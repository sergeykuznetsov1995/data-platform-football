"""Competition-catalog adapter for canonical WhoScored scopes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import math
import os
from pathlib import Path
import re
import unicodedata
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import urlsplit

import yaml

from . import runtime_contract as _runtime_contract
from .domain import (
    SeasonFormat,
    TournamentClassification,
    TournamentEligibility,
    WhoScoredScope,
)


DEFAULT_COMPETITIONS_PATH = (
    Path(__file__).resolve().parents[2] / "configs" / "medallion" / "competitions.yaml"
)


class CatalogError(ValueError):
    """The competition catalog cannot provide an unambiguous source scope."""


@dataclass(frozen=True)
class CatalogSeason:
    scope: WhoScoredScope
    start: Optional[date] = None
    end: Optional[date] = None
    source_season_id: Optional[int] = None
    stage_ids: tuple[int, ...] = ()
    is_active: Optional[bool] = None
    eligibility: TournamentEligibility = TournamentEligibility.INCLUDED
    classification_reason: str = "static_configuration"
    source_label: Optional[str] = None
    source_url: Optional[str] = None
    raw_json: Optional[str] = None
    schema_fingerprint: Optional[str] = None


@dataclass(frozen=True)
class CatalogCompetition:
    competition_id: str
    seasons: tuple[CatalogSeason, ...]
    whoscored_enabled: bool
    region_id: Optional[int] = None
    tournament_id: Optional[int] = None
    region_name: Optional[str] = None
    region_code: Optional[str] = None
    tournament_name: Optional[str] = None
    tournament_url: Optional[str] = None
    source_sex: Optional[int] = None
    eligibility: TournamentEligibility = TournamentEligibility.INCLUDED
    classification_reason: str = "static_configuration"
    classifier_version: str = "static-v1"
    override_version: Optional[str] = None
    raw_json: Optional[str] = None
    schema_fingerprint: Optional[str] = None


CATALOG_CLASSIFIER_VERSION = "senior-men-v3"
DEFAULT_OVERRIDE_VERSION = "2026-07-11-v1"
TECHNICAL_EXCLUSION_AUDIT_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class TournamentOverride:
    """Versioned decision for a genuinely ambiguous source tournament.

    Overrides are keyed by the source tournament id, never by display name, so
    a rename cannot silently change coverage.  An override is an exception to
    classification, not an allow-list for discovery.
    """

    tournament_id: int
    eligibility: TournamentEligibility
    reason: str
    version: str = DEFAULT_OVERRIDE_VERSION
    canonical_competition_id: Optional[str] = None

    def __post_init__(self) -> None:
        if isinstance(self.tournament_id, bool):
            raise CatalogError("Tournament override id must be a positive integer")
        try:
            source_id = int(self.tournament_id)
        except (TypeError, ValueError) as exc:
            raise CatalogError(
                f"Tournament override id must be an integer, got {self.tournament_id!r}"
            ) from exc
        if source_id <= 0:
            raise CatalogError("Tournament override id must be a positive integer")

        try:
            eligibility = TournamentEligibility.coerce(self.eligibility)
        except ValueError as exc:
            raise CatalogError(str(exc)) from exc
        reason = "" if self.reason is None else str(self.reason).strip()
        version = "" if self.version is None else str(self.version).strip()
        if not reason:
            raise CatalogError(f"Tournament override {source_id} has no reason")
        if not version:
            raise CatalogError(f"Tournament override {source_id} has no version")

        canonical_competition_id = self.canonical_competition_id
        if canonical_competition_id is not None:
            canonical_competition_id = str(canonical_competition_id).strip()
            if not canonical_competition_id:
                raise CatalogError(
                    f"Tournament override {source_id} has an empty competition id"
                )
        if (
            eligibility is TournamentEligibility.EXCLUDED_TECHNICAL
            and canonical_competition_id is None
        ):
            raise CatalogError(
                f"Tournament override {source_id} has no canonical competition id"
            )

        object.__setattr__(self, "tournament_id", source_id)
        object.__setattr__(self, "eligibility", eligibility)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "canonical_competition_id", canonical_competition_id)


# These five source ids were audited against their source tournament/stage
# identity on 2026-07-11.  WhoScored exposes no ``sex`` observation for them
# because the selected stage calendars are structurally valid but empty.  The
# names and source identities are senior men's competitions; none is a
# women-only or youth-only tournament (the EFL Trophy may invite academy sides,
# but is a senior men's competition).  Keeping the exception keyed by immutable
# source id makes the decision reviewable without turning discovery into an
# allow-list.  Every new ambiguous id still remains quarantined and fail-closed.
DEFAULT_TOURNAMENT_OVERRIDES: tuple[TournamentOverride, ...] = (
    TournamentOverride(
        599,
        TournamentEligibility.INCLUDED,
        "audited senior men: Japan Football League; empty source calendar",
    ),
    TournamentOverride(
        416,
        TournamentEligibility.INCLUDED,
        "audited senior men: Serbia Prva Liga; empty source calendar",
    ),
    TournamentOverride(
        480,
        TournamentEligibility.INCLUDED,
        "audited senior men: Belarus Premier League Qualification; empty source calendar",
    ),
    TournamentOverride(
        252,
        TournamentEligibility.INCLUDED,
        "audited senior men: Zambia 1 Division; empty source calendar",
    ),
    TournamentOverride(
        23,
        TournamentEligibility.INCLUDED,
        "audited senior men: EFL Trophy; empty future source calendar",
    ),
)


def _technical_name_key(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return " ".join(re.sub(r"[^\w]+", " ", normalized).split())


def _technical_link_key(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlsplit(raw)
    path = re.sub(r"/+", "/", parsed.path).rstrip("/").casefold()
    return path


def build_technical_exclusion_audit(
    rows: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    source_snapshot_sha256: str,
    overrides: Sequence[TournamentOverride] = DEFAULT_TOURNAMENT_OVERRIDES,
) -> dict[str, Any]:
    """Build a reproducible duplicate-candidate review for one source snapshot.

    The detector never silently classifies a tournament as technical.  A
    candidate is considered reviewed only when a versioned immutable source-ID
    override excludes every duplicate shell while leaving exactly one canonical
    tournament.  Every other candidate blocks catalog publication upstream.
    """

    source_sha256 = str(source_snapshot_sha256 or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", source_sha256) is None:
        raise CatalogError("technical audit requires a source snapshot SHA-256")
    technical_overrides = {
        int(item.tournament_id): item
        for item in overrides
        if item.eligibility is TournamentEligibility.EXCLUDED_TECHNICAL
    }
    competition_rows = [dict(item) for item in rows.get("competitions", ())]
    competition_by_id = {
        int(item["tournament_id"]): item
        for item in competition_rows
        if item.get("tournament_id") is not None
        and not isinstance(item.get("tournament_id"), bool)
    }
    candidate_groups: list[tuple[str, str, set[int]]] = []

    def collect(
        candidate_type: str,
        keyed_ids: Mapping[str, set[int]],
    ) -> None:
        for key, source_ids in sorted(keyed_ids.items()):
            if key and len(source_ids) > 1:
                candidate_groups.append((candidate_type, key, set(source_ids)))

    names: dict[str, set[int]] = {}
    links: dict[str, set[int]] = {}
    for source_id, row in competition_by_id.items():
        region_id = row.get("region_id")
        name_key = _technical_name_key(row.get("tournament_name"))
        if region_id is not None and name_key:
            names.setdefault(f"{region_id}:{name_key}", set()).add(source_id)
        link_key = _technical_link_key(row.get("tournament_url"))
        if link_key:
            links.setdefault(link_key, set()).add(source_id)
    collect("normalized_name_within_region", names)
    collect("canonical_tournament_link", links)

    stages: dict[str, set[int]] = {}
    competition_id_to_source_id = {
        str(row.get("competition_id")): source_id
        for source_id, row in competition_by_id.items()
        if row.get("competition_id")
    }
    for row in rows.get("stages", ()):
        stage_id = row.get("stage_id")
        source_id = row.get("tournament_id")
        if source_id is None:
            source_id = competition_id_to_source_id.get(
                str(row.get("competition_id") or "")
            )
        if (
            stage_id is None
            or isinstance(stage_id, bool)
            or source_id is None
            or isinstance(source_id, bool)
        ):
            continue
        stages.setdefault(str(int(stage_id)), set()).add(int(source_id))
    collect("stage_id_overlap", stages)

    parent: dict[int, int] = {}

    def find(source_id: int) -> int:
        parent.setdefault(source_id, source_id)
        while parent[source_id] != source_id:
            parent[source_id] = parent[parent[source_id]]
            source_id = parent[source_id]
        return source_id

    def union(source_ids: set[int]) -> None:
        ordered = sorted(source_ids)
        if not ordered:
            return
        root = find(ordered[0])
        for source_id in ordered[1:]:
            other = find(source_id)
            if other != root:
                parent[other] = root

    for _candidate_type, _key, source_ids in candidate_groups:
        union(source_ids)

    present_override_ids = set(competition_by_id) & technical_overrides.keys()
    orphan_override_ids = sorted(present_override_ids - parent.keys())
    for source_id in orphan_override_ids:
        candidate_groups.append(
            (
                "technical_override_without_duplicate_evidence",
                str(source_id),
                {source_id},
            )
        )
        union({source_id})

    component_ids: dict[int, str] = {}
    component_members: dict[int, set[int]] = {}
    for source_id in sorted(parent):
        root = find(source_id)
        component_members.setdefault(root, set()).add(source_id)
    for index, (root, _members) in enumerate(
        sorted(component_members.items(), key=lambda item: sorted(item[1])),
        start=1,
    ):
        component_ids[root] = f"component-{index:04d}"

    component_state: dict[int, dict[str, Any]] = {}
    components: list[dict[str, Any]] = []
    for root, source_ids in sorted(
        component_members.items(), key=lambda item: sorted(item[1])
    ):
        excluded_ids = sorted(source_ids & technical_overrides.keys())
        canonical_ids = sorted(source_ids - technical_overrides.keys())
        failures: list[str] = []
        if not excluded_ids:
            failures.append("component_has_no_technical_override")
        if len(canonical_ids) != 1:
            failures.append(
                f"component_has_{len(canonical_ids)}_canonical_tournaments"
            )
            canonical_competition_id = None
        else:
            canonical_row = competition_by_id[canonical_ids[0]]
            canonical_competition_id = str(
                canonical_row.get("competition_id") or ""
            )
            if not canonical_competition_id:
                failures.append("canonical_tournament_has_no_competition_id")
            if canonical_row.get("eligibility") != TournamentEligibility.INCLUDED.value:
                failures.append("canonical_tournament_is_not_included")
        for source_id in excluded_ids:
            if (
                canonical_competition_id is None
                or technical_overrides[source_id].canonical_competition_id
                != canonical_competition_id
            ):
                failures.append(
                    f"override_{source_id}_canonical_competition_mismatch"
                )
        resolved = not failures
        signals = [
            {"candidate_type": candidate_type, "candidate_key": key}
            for candidate_type, key, candidate_ids in sorted(candidate_groups)
            if candidate_ids <= source_ids
        ]
        state = {
            "component_id": component_ids[root],
            "source_tournament_ids": sorted(source_ids),
            "technical_override_ids": excluded_ids,
            "canonical_tournament_ids": canonical_ids,
            "canonical_competition_id": canonical_competition_id,
            "signals": signals,
            "review_disposition": (
                "resolved_by_versioned_source_id_override"
                if resolved
                else "requires_versioned_source_id_review"
            ),
            "validation_failures": sorted(set(failures)),
        }
        component_state[root] = state
        components.append(state)

    candidates: list[dict[str, Any]] = []
    unresolved = 0
    for candidate_type, key, source_ids in sorted(candidate_groups):
        root = find(min(source_ids))
        state = component_state[root]
        if state["review_disposition"] != "resolved_by_versioned_source_id_override":
            unresolved += 1
        ordered_ids = sorted(source_ids)
        candidates.append(
            {
                "candidate_type": candidate_type,
                "candidate_key": key,
                "source_tournament_ids": ordered_ids,
                "competition_ids": sorted(
                    str(competition_by_id[source_id].get("competition_id") or "")
                    for source_id in ordered_ids
                ),
                "component_id": state["component_id"],
                "component_source_tournament_ids": state[
                    "source_tournament_ids"
                ],
                "technical_override_ids": state["technical_override_ids"],
                "canonical_tournament_ids": state["canonical_tournament_ids"],
                "canonical_competition_id": state["canonical_competition_id"],
                "review_disposition": state["review_disposition"],
                "validation_failures": state["validation_failures"],
            }
        )

    return {
        "schema_version": TECHNICAL_EXCLUSION_AUDIT_SCHEMA_VERSION,
        "audit_type": "whoscored_technical_exclusion_audit",
        "source_snapshot_sha256": source_sha256,
        "classifier_version": CATALOG_CLASSIFIER_VERSION,
        "technical_override_registry": [
            {
                "tournament_id": source_id,
                "reason": item.reason,
                "version": item.version,
                "canonical_competition_id": item.canonical_competition_id,
                "present_in_source_snapshot": source_id in competition_by_id,
                "component_id": (
                    component_ids[find(source_id)] if source_id in parent else None
                ),
            }
            for source_id, item in sorted(technical_overrides.items())
        ],
        "candidate_count": len(candidates),
        "unresolved_candidate_count": unresolved,
        "component_count": len(components),
        "unresolved_component_count": sum(
            item["review_disposition"]
            != "resolved_by_versioned_source_id_override"
            for item in components
        ),
        "candidates": candidates,
        "components": components,
    }


_WOMEN_MARKERS = re.compile(
    r"(?:\bwomen(?:'s)?\b|\bwoman\b|\bf[ée]min(?:ine|ino|ina|in)?\b|"
    r"\bfemminile\b|\bfrauen\b|\bdamen\b|\bfemale\b|\bladies\b|"
    r"\bliga[ -]?f\b|\bnwsl\b|женск|女子|女足)",
    re.IGNORECASE,
)
_YOUTH_MARKERS = re.compile(
    r"(?:\b(?:u|under)[ -]?(?:[5-9]|1[0-9]|2[0-3])\b|\byouth\b|\bjunior(?:s)?\b|"
    r"\bacademy\b|\bprimavera\b|\bjuvenil\b|\bsub[ -]?(?:[5-9]|1[0-9]|2[0-3])\b|"
    r"\bolympic(?:s| games)?\b|\bcolts?\b|"
    r"молод[её]ж|юнош|青年)",
    re.IGNORECASE,
)
_RESERVE_MARKERS = re.compile(
    r"(?:\breserves?\b|\bb[ -]?team\b|\bdevelopment league\b|"
    r"\bpremier league 2\b)",
    re.IGNORECASE,
)


def _coerce_source_sex(value: Any) -> tuple[Optional[int], Optional[str]]:
    if value in (None, ""):
        return None, None
    if isinstance(value, bool):
        return None, "source_sex_is_boolean"
    if isinstance(value, str):
        token = value.strip().casefold()
        if token in {"male", "men", "man", "m"}:
            return 1, None
        if token in {"female", "women", "woman", "f"}:
            return 0, None
        if token in {"0", "1"}:
            return int(token), None
        return None, "source_sex_is_unknown"
    if isinstance(value, int):
        numeric = value
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            return None, "source_sex_is_unknown"
        numeric = int(value)
    elif isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            return None, "source_sex_is_unknown"
        numeric = int(value)
    else:
        return None, "source_sex_is_unknown"
    if numeric not in {0, 1}:
        return None, "source_sex_is_unknown"
    return numeric, None


def classify_tournament(
    *,
    tournament_id: int,
    tournament_name: str,
    region_name: Optional[str] = None,
    source_sex: Any = None,
    overrides: Sequence[TournamentOverride] = DEFAULT_TOURNAMENT_OVERRIDES,
) -> TournamentClassification:
    """Classify one discovered tournament without ever defaulting to men.

    Source ``sex`` is authoritative when present.  Name markers still exclude
    youth and reserve tournaments and detect contradictory metadata.  Missing
    sex without an explicit marker is quarantined until schedule metadata
    resolves it.  Technical exclusions are never inferred from a display name;
    they require a versioned override keyed by immutable source id.
    """

    source_id = _optional_int(tournament_id, "tournament_id")
    if source_id is None:
        return TournamentClassification(
            TournamentEligibility.QUARANTINED,
            "missing_tournament_id",
            CATALOG_CLASSIFIER_VERSION,
        )
    override_by_id = {int(item.tournament_id): item for item in overrides}
    override = override_by_id.get(source_id)

    label = " ".join(
        part for part in (str(region_name or ""), str(tournament_name or "")) if part
    )
    women_marker = bool(_WOMEN_MARKERS.search(label))
    youth_marker = bool(_YOUTH_MARKERS.search(label))
    reserve_marker = bool(_RESERVE_MARKERS.search(label))
    sex, sex_error = _coerce_source_sex(source_sex)
    if sex_error:
        return TournamentClassification(
            TournamentEligibility.QUARANTINED,
            sex_error,
            CATALOG_CLASSIFIER_VERSION,
        )
    if reserve_marker:
        reason = "name_marks_reserve"
        if women_marker and youth_marker:
            reason = "name_marks_women_youth_and_reserve"
        elif women_marker:
            reason = "name_marks_women_and_reserve"
        elif youth_marker:
            reason = "name_marks_youth_and_reserve"
        return TournamentClassification(
            TournamentEligibility.EXCLUDED_RESERVE,
            reason,
            CATALOG_CLASSIFIER_VERSION,
            sex,
        )
    if women_marker and youth_marker:
        return TournamentClassification(
            TournamentEligibility.EXCLUDED_YOUTH,
            "name_marks_women_and_youth",
            CATALOG_CLASSIFIER_VERSION,
            sex,
        )
    if sex == 1 and women_marker:
        return TournamentClassification(
            TournamentEligibility.QUARANTINED,
            "male_source_sex_conflicts_with_women_marker",
            CATALOG_CLASSIFIER_VERSION,
            sex,
        )
    if sex == 0:
        return TournamentClassification(
            TournamentEligibility.EXCLUDED_WOMEN,
            "source_sex_female",
            CATALOG_CLASSIFIER_VERSION,
            sex,
        )
    if youth_marker:
        return TournamentClassification(
            TournamentEligibility.EXCLUDED_YOUTH,
            "name_marks_youth",
            CATALOG_CLASSIFIER_VERSION,
            sex,
        )
    if women_marker:
        return TournamentClassification(
            TournamentEligibility.EXCLUDED_WOMEN,
            "name_marks_women",
            CATALOG_CLASSIFIER_VERSION,
            sex,
        )
    if override is not None:
        return TournamentClassification(
            override.eligibility,
            f"explicit_override:{override.reason}",
            CATALOG_CLASSIFIER_VERSION,
            sex,
            override.version,
        )
    if sex == 1:
        return TournamentClassification(
            TournamentEligibility.INCLUDED,
            "source_sex_male_no_youth_marker",
            CATALOG_CLASSIFIER_VERSION,
            sex,
        )
    return TournamentClassification(
        TournamentEligibility.QUARANTINED,
        "source_sex_not_yet_observed",
        CATALOG_CLASSIFIER_VERSION,
    )


def apply_schedule_classification(
    rows: Mapping[str, Sequence[Mapping[str, Any]]],
    schedule_rows: Sequence[Mapping[str, Any]],
    *,
    overrides: Sequence[TournamentOverride] = DEFAULT_TOURNAMENT_OVERRIDES,
) -> dict[str, tuple[dict[str, Any], ...]]:
    """Resolve provisional allRegions decisions with schedule ``sex`` metadata.

    Conflicting sex observations quarantine the tournament.  Seasons with an
    independently invalid identity remain quarantined even when their parent
    tournament becomes eligible.
    """

    observed: dict[tuple[Optional[int], int], set[int]] = {}
    for index, schedule in enumerate(schedule_rows):
        tournament_id = _optional_int(
            schedule.get("tournament_id"), f"schedule[{index}].tournament_id"
        )
        if tournament_id is None:
            continue
        region_id = _optional_int(
            schedule.get("region_id"), f"schedule[{index}].region_id"
        )
        sex, error = _coerce_source_sex(schedule.get("source_sex"))
        if error or sex is None:
            continue
        observed.setdefault((region_id, tournament_id), set()).add(sex)

    competitions: list[dict[str, Any]] = []
    disposition_by_competition: dict[str, tuple[str, str]] = {}
    for index, source in enumerate(rows.get("competitions", ())):
        row = dict(source)
        tournament_id = _optional_int(
            row.get("tournament_id"), f"competitions[{index}].tournament_id"
        )
        region_id = _optional_int(
            row.get("region_id"), f"competitions[{index}].region_id"
        )
        values = observed.get((region_id, tournament_id), set())
        if not values:
            # Some feeds omit region metadata; only use tournament-only
            # evidence when it is unique across every matching observation.
            candidates = {
                value
                for (candidate_region, candidate_tournament), sexes in observed.items()
                if candidate_tournament == tournament_id
                for value in sexes
            }
            values = candidates
        if len(values) > 1:
            row.update(
                {
                    "source_sex": None,
                    "eligibility": TournamentEligibility.QUARANTINED.value,
                    "classification_reason": "conflicting_schedule_source_sex",
                    "classifier_version": CATALOG_CLASSIFIER_VERSION,
                    "override_version": None,
                }
            )
        elif len(values) == 1 and tournament_id is not None:
            source_sex = next(iter(values))
            decision = classify_tournament(
                tournament_id=tournament_id,
                tournament_name=str(row.get("tournament_name") or ""),
                region_name=_optional_text(row.get("region_name")),
                source_sex=source_sex,
                overrides=overrides,
            )
            row.update(
                {
                    "source_sex": source_sex,
                    "eligibility": decision.eligibility.value,
                    "classification_reason": decision.reason,
                    "classifier_version": decision.classifier_version,
                    "override_version": decision.override_version,
                }
            )
        competition_id = str(row.get("competition_id") or "")
        disposition_by_competition[competition_id] = (
            str(row.get("eligibility") or TournamentEligibility.QUARANTINED.value),
            str(row.get("classification_reason") or "tournament_unclassified"),
        )
        competitions.append(row)

    def inherit(
        source_rows: Sequence[Mapping[str, Any]], *, preserve_invalid_season: bool
    ) -> tuple[dict[str, Any], ...]:
        inherited: list[dict[str, Any]] = []
        for source in source_rows:
            row = dict(source)
            competition_id = str(row.get("competition_id") or "")
            disposition = disposition_by_competition.get(competition_id)
            eligibility = str(
                row.get("eligibility") or TournamentEligibility.QUARANTINED.value
            )
            reason = str(row.get("classification_reason") or "")
            provisional_parent_quarantine = (
                eligibility == TournamentEligibility.QUARANTINED.value
                and reason
                in {
                    "source_sex_not_yet_observed",
                    "parent:source_sex_not_yet_observed",
                }
            )
            independently_invalid = (
                eligibility == TournamentEligibility.SOURCE_UNAVAILABLE.value
                or (
                    eligibility == TournamentEligibility.QUARANTINED.value
                    and not provisional_parent_quarantine
                )
                or (
                    preserve_invalid_season
                    and (
                        row.get("season_id") in (None, "")
                        or row.get("source_season_id") in (None, "")
                        or row.get("season_format") in (None, "")
                    )
                )
            )
            if disposition is not None and not independently_invalid:
                row["eligibility"], parent_reason = disposition
                row["classification_reason"] = f"parent:{parent_reason}"
            inherited.append(row)
        return tuple(inherited)

    seasons = inherit(rows.get("seasons", ()), preserve_invalid_season=True)
    stages = list(inherit(rows.get("stages", ()), preserve_invalid_season=False))
    exceptional_seasons = {
        (
            str(row.get("competition_id") or ""),
            _optional_int(row.get("source_season_id"), "season.source_season_id"),
        ): (
            str(row.get("eligibility")),
            str(row.get("classification_reason") or "season_unclassified"),
        )
        for row in seasons
        if row.get("eligibility")
        in {
            TournamentEligibility.QUARANTINED.value,
            TournamentEligibility.SOURCE_UNAVAILABLE.value,
        }
    }
    for stage in stages:
        key = (
            str(stage.get("competition_id") or ""),
            _optional_int(stage.get("source_season_id"), "stage.source_season_id"),
        )
        disposition = exceptional_seasons.get(key)
        if disposition is not None:
            stage["eligibility"], reason = disposition
            stage["classification_reason"] = f"season:{reason}"

    return {
        "competitions": tuple(competitions),
        "seasons": seasons,
        "stages": tuple(stages),
    }


def _optional_int(value: Any, field: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        raise CatalogError(f"{field} must be an integer, not boolean")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CatalogError(f"{field} must be an integer, got {value!r}") from exc


def _optional_date(value: Any, field: str) -> Optional[date]:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise CatalogError(f"{field} must be an ISO date, got {value!r}") from exc


def _required_discovery_int(value: Any, field: str) -> int:
    parsed = _optional_int(value, field)
    if parsed is None:
        raise CatalogError(f"{field} is required")
    return parsed


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _optional_bool(value: Any, field: str) -> Optional[bool]:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    if value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().casefold()
        if token in {"true", "1"}:
            return True
        if token in {"false", "0"}:
            return False
    raise CatalogError(f"{field} must be a boolean, got {value!r}")


def _row_eligibility(row: Mapping[str, Any]) -> TournamentEligibility:
    raw = row.get("eligibility", TournamentEligibility.QUARANTINED.value)
    try:
        return TournamentEligibility.coerce(raw)
    except ValueError as exc:
        raise CatalogError(str(exc)) from exc


def _technical_override_provenance_error(
    row: Mapping[str, Any],
) -> Optional[str]:
    if _row_eligibility(row) is not TournamentEligibility.EXCLUDED_TECHNICAL:
        return None
    tournament_id = _optional_int(row.get("tournament_id"), "tournament_id")
    if tournament_id is None or tournament_id <= 0:
        return "excluded_technical_without_source_tournament_id"
    reason = str(row.get("classification_reason") or "").strip()
    if not reason.startswith("explicit_override:") or not reason.partition(":")[2]:
        return "excluded_technical_without_override_reason"
    if _optional_text(row.get("override_version")) is None:
        return "excluded_technical_without_override_version"
    return None


class WhoScoredCatalog:
    """Strict read-only projection of ``competitions.yaml``.

    Source identifiers live in the source-owned catalog mapping::

        sources:
          whoscored: {region_id: 252, tournament_id: 2}

    A season may similarly carry ``source_season_id`` once it has been
    discovered.  Missing source ids do not prevent scope validation; callers
    that need discovery can test the explicit ``None`` values.
    """

    def __init__(
        self,
        competitions: tuple[CatalogCompetition, ...],
        *,
        discovery_rows: Optional[Mapping[str, Sequence[Mapping[str, Any]]]] = None,
    ) -> None:
        by_id: dict[str, CatalogCompetition] = {}
        for competition in competitions:
            if competition.competition_id in by_id:
                raise CatalogError(
                    f"Duplicate competition id {competition.competition_id!r}"
                )
            seasons_by_id: dict[str, CatalogSeason] = {}
            for season in competition.seasons:
                season_id = season.scope.season_id
                previous = seasons_by_id.get(season_id)
                if previous is not None:
                    raise CatalogError(
                        f"Duplicate canonical scope {competition.competition_id}="
                        f"{season_id!s}: source seasons "
                        f"{previous.source_season_id!r} and "
                        f"{season.source_season_id!r}"
                    )
                seasons_by_id[season_id] = season
            by_id[competition.competition_id] = competition
        self._competitions = by_id
        self._discovery_rows = {
            name: tuple(dict(row) for row in rows)
            for name, rows in (discovery_rows or {}).items()
        }

    @classmethod
    def from_file(
        cls, path: str | Path = DEFAULT_COMPETITIONS_PATH
    ) -> "WhoScoredCatalog":
        if Path(os.path.abspath(path)) == Path(os.path.abspath(DEFAULT_COMPETITIONS_PATH)):
            payload = _runtime_contract.read_attested_static_runtime_file(
                "configs/medallion/competitions.yaml"
            )
            try:
                document = yaml.safe_load(payload.decode("utf-8")) or {}
            except UnicodeDecodeError as exc:
                raise CatalogError("Competition catalog must be UTF-8") from exc
            return cls.from_mapping(document)
        with Path(path).open("r", encoding="utf-8") as handle:
            document = yaml.safe_load(handle) or {}
        return cls.from_mapping(document)

    @classmethod
    def from_mapping(cls, document: Mapping[str, Any]) -> "WhoScoredCatalog":
        raw_competitions = document.get("competitions")
        if not isinstance(raw_competitions, list):
            raise CatalogError("Catalog must contain a 'competitions' list")

        competitions: list[CatalogCompetition] = []
        for raw_competition in raw_competitions:
            if not isinstance(raw_competition, Mapping):
                raise CatalogError("Each competition entry must be a mapping")
            competition_id = str(raw_competition.get("id", "")).strip()
            if not competition_id:
                raise CatalogError("Each competition entry must have a non-empty id")

            sources = raw_competition.get("sources") or {}
            if not isinstance(sources, Mapping):
                raise CatalogError(f"{competition_id}: sources must be a mapping")
            enabled_sources = set(sources.get("primary") or ()) | set(
                sources.get("fallback") or ()
            )

            whoscored_ids = sources.get("whoscored") or {}
            if not isinstance(whoscored_ids, Mapping):
                raise CatalogError(
                    f"{competition_id}: sources.whoscored must be a mapping"
                )

            seasons: list[CatalogSeason] = []
            seen_seasons: set[str] = set()
            raw_seasons = raw_competition.get("seasons") or []
            if not isinstance(raw_seasons, list):
                raise CatalogError(f"{competition_id}: seasons must be a list")
            for raw_season in raw_seasons:
                if not isinstance(raw_season, Mapping) or "id" not in raw_season:
                    raise CatalogError(
                        f"{competition_id}: every season must be a mapping with an id"
                    )
                fmt = SeasonFormat.coerce(
                    raw_season.get("season_format", SeasonFormat.SPLIT_YEAR.value)
                )
                try:
                    scope = WhoScoredScope(competition_id, str(raw_season["id"]), fmt)
                except ValueError as exc:
                    raise CatalogError(f"{competition_id}: {exc}") from exc
                if scope.season_id in seen_seasons:
                    raise CatalogError(
                        f"{competition_id}: duplicate season {scope.season_id!r}"
                    )
                seen_seasons.add(scope.season_id)
                start = _optional_date(raw_season.get("start"), f"{scope.spec}.start")
                end = _optional_date(raw_season.get("end"), f"{scope.spec}.end")
                if start is not None and end is not None and end < start:
                    raise CatalogError(f"{scope.spec}: end date precedes start date")
                seasons.append(
                    CatalogSeason(
                        scope=scope,
                        start=start,
                        end=end,
                        source_season_id=_optional_int(
                            raw_season.get("source_season_id"),
                            f"{scope.spec}.source_season_id",
                        ),
                    )
                )

            competitions.append(
                CatalogCompetition(
                    competition_id=competition_id,
                    seasons=tuple(seasons),
                    whoscored_enabled=(
                        "whoscored" in enabled_sources or bool(whoscored_ids)
                    ),
                    region_id=_optional_int(
                        whoscored_ids.get("region_id"),
                        f"{competition_id}.region_id",
                    ),
                    tournament_id=_optional_int(
                        whoscored_ids.get("tournament_id"),
                        f"{competition_id}.tournament_id",
                    ),
                )
            )
        return cls(tuple(competitions))

    @classmethod
    def from_discovery_rows(
        cls,
        competition_rows: Sequence[Mapping[str, Any]],
        season_rows: Sequence[Mapping[str, Any]],
        stage_rows: Sequence[Mapping[str, Any]] = (),
    ) -> "WhoScoredCatalog":
        """Reconstruct the runtime catalog from persisted Bronze rows.

        The wire contract is the exact output of :meth:`to_rows`.  Invalid or
        unclassified source rows are retained under ``quarantined`` and never
        disappear merely because they cannot form a :class:`WhoScoredScope`.
        """

        raw_competitions = [dict(row) for row in competition_rows]
        raw_seasons = [dict(row) for row in season_rows]
        raw_stages = [dict(row) for row in stage_rows]
        stages_by_season: dict[tuple[int, int, int], set[int]] = {}
        for index, row in enumerate(raw_stages):
            try:
                key = (
                    _required_discovery_int(
                        row.get("region_id"), f"stages[{index}].region_id"
                    ),
                    _required_discovery_int(
                        row.get("tournament_id"), f"stages[{index}].tournament_id"
                    ),
                    _required_discovery_int(
                        row.get("source_season_id"),
                        f"stages[{index}].source_season_id",
                    ),
                )
                stage_id = _required_discovery_int(
                    row.get("stage_id"), f"stages[{index}].stage_id"
                )
            except CatalogError:
                # Retained in discovery_rows and exposed through quarantined.
                continue
            stages_by_season.setdefault(key, set()).add(stage_id)

        seasons_by_competition: dict[str, list[CatalogSeason]] = {}
        seen_source_seasons: set[tuple[str, int]] = set()
        for index, row in enumerate(raw_seasons):
            competition_id = str(row.get("competition_id") or "").strip()
            source_season_id = _optional_int(
                row.get("source_season_id"), f"seasons[{index}].source_season_id"
            )
            canonical = str(row.get("season_id") or "").strip()
            raw_format = row.get("season_format")
            eligibility = _row_eligibility(row)
            if (
                not competition_id
                or source_season_id is None
                or not canonical
                or raw_format in (None, "")
            ):
                continue
            try:
                fmt = SeasonFormat.coerce(raw_format)
                scope = WhoScoredScope(competition_id, canonical, fmt)
            except ValueError:
                continue
            dedupe_key = (competition_id, source_season_id)
            if dedupe_key in seen_source_seasons:
                raise CatalogError(
                    f"Duplicate source season {source_season_id} for {competition_id}"
                )
            seen_source_seasons.add(dedupe_key)
            region_id = _optional_int(row.get("region_id"), "season.region_id")
            tournament_id = _optional_int(
                row.get("tournament_id"), "season.tournament_id"
            )
            stage_key = (
                region_id if region_id is not None else -1,
                tournament_id if tournament_id is not None else -1,
                source_season_id,
            )
            seasons_by_competition.setdefault(competition_id, []).append(
                CatalogSeason(
                    scope=scope,
                    start=_optional_date(row.get("start"), f"{scope.spec}.start"),
                    end=_optional_date(row.get("end"), f"{scope.spec}.end"),
                    source_season_id=source_season_id,
                    stage_ids=tuple(sorted(stages_by_season.get(stage_key, ()))),
                    is_active=_optional_bool(
                        row.get("is_active"), f"{scope.spec}.is_active"
                    ),
                    eligibility=eligibility,
                    classification_reason=str(
                        row.get("classification_reason") or "inherited_from_tournament"
                    ),
                    source_label=_optional_text(row.get("source_label")),
                    source_url=_optional_text(row.get("source_url")),
                    raw_json=_optional_text(
                        row.get("source_raw_json", row.get("raw_json"))
                    ),
                    schema_fingerprint=_optional_text(
                        row.get(
                            "source_schema_fingerprint",
                            row.get("schema_fingerprint"),
                        )
                    ),
                )
            )

        competitions: list[CatalogCompetition] = []
        seen_ids: set[str] = set()
        for index, row in enumerate(raw_competitions):
            region_id = _optional_int(
                row.get("region_id"), f"competitions[{index}].region_id"
            )
            tournament_id = _optional_int(
                row.get("tournament_id"), f"competitions[{index}].tournament_id"
            )
            fallback_id = (
                f"WS-{region_id}-{tournament_id}"
                if region_id is not None and tournament_id is not None
                else f"WS-quarantine-{index}"
            )
            competition_id = str(row.get("competition_id") or fallback_id).strip()
            if competition_id in seen_ids:
                raise CatalogError(f"Duplicate competition id {competition_id!r}")
            seen_ids.add(competition_id)
            eligibility = _row_eligibility(row)
            technical_provenance_error = _technical_override_provenance_error(row)
            if technical_provenance_error is not None:
                eligibility = TournamentEligibility.QUARANTINED
            competitions.append(
                CatalogCompetition(
                    competition_id=competition_id,
                    seasons=tuple(
                        sorted(
                            seasons_by_competition.get(competition_id, ()),
                            key=lambda item: item.scope.season_id,
                        )
                    ),
                    whoscored_enabled=eligibility is TournamentEligibility.INCLUDED,
                    region_id=region_id,
                    tournament_id=tournament_id,
                    region_name=_optional_text(row.get("region_name")),
                    region_code=_optional_text(row.get("region_code")),
                    tournament_name=_optional_text(row.get("tournament_name")),
                    tournament_url=_optional_text(row.get("tournament_url")),
                    source_sex=_optional_int(row.get("source_sex"), "source_sex"),
                    eligibility=eligibility,
                    classification_reason=str(
                        technical_provenance_error
                        or row.get("classification_reason")
                        or "missing_classification_reason"
                    ),
                    classifier_version=str(
                        row.get("classifier_version") or CATALOG_CLASSIFIER_VERSION
                    ),
                    override_version=_optional_text(row.get("override_version")),
                    raw_json=_optional_text(
                        row.get("source_raw_json", row.get("raw_json"))
                    ),
                    schema_fingerprint=_optional_text(
                        row.get(
                            "source_schema_fingerprint",
                            row.get("schema_fingerprint"),
                        )
                    ),
                )
            )

        known = {item.competition_id for item in competitions}
        orphan_competitions = sorted(set(seasons_by_competition) - known)
        if orphan_competitions:
            raise CatalogError(
                "Season rows reference missing competitions: "
                + ", ".join(orphan_competitions)
            )
        return cls(
            tuple(competitions),
            discovery_rows={
                "competitions": raw_competitions,
                "seasons": raw_seasons,
                "stages": raw_stages,
            },
        )

    @classmethod
    def from_rows(
        cls, rows: Mapping[str, Sequence[Mapping[str, Any]]]
    ) -> "WhoScoredCatalog":
        """Deserialize the stable mapping returned by :meth:`to_rows`."""

        if not isinstance(rows, Mapping):
            raise CatalogError("Discovery catalog rows must be a mapping")
        return cls.from_discovery_rows(
            rows.get("competitions", ()),
            rows.get("seasons", ()),
            rows.get("stages", ()),
        )

    @property
    def competitions(self) -> tuple[CatalogCompetition, ...]:
        return tuple(self._competitions.values())

    def competition(self, competition_id: str) -> CatalogCompetition:
        try:
            return self._competitions[competition_id]
        except KeyError as exc:
            raise CatalogError(f"Unknown competition {competition_id!r}") from exc

    def resolve_scope(self, competition_id: str, season_id: str | int) -> CatalogSeason:
        competition = self.competition(competition_id)
        token = str(season_id).strip()
        matches = tuple(
            season for season in competition.seasons if season.scope.season_id == token
        )
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise CatalogError(f"Season {token!r} is ambiguous for {competition_id!r}")
        raise CatalogError(f"Season {token!r} is not configured for {competition_id!r}")

    def parse_scope_spec(self, spec: str) -> CatalogSeason:
        if not isinstance(spec, str) or spec.count("=") != 1:
            raise CatalogError(
                "WhoScored scope must have the form '<competition>=<season-id>'"
            )
        competition_id, season_id = (part.strip() for part in spec.split("=", 1))
        return self.resolve_scope(competition_id, season_id)

    def enabled_scopes(self) -> tuple[CatalogSeason, ...]:
        return tuple(
            season
            for competition in self._competitions.values()
            if competition.whoscored_enabled
            and competition.eligibility is TournamentEligibility.INCLUDED
            for season in competition.seasons
            if season.eligibility is TournamentEligibility.INCLUDED
        )

    def all_scopes(self) -> tuple[CatalogSeason, ...]:
        """All source scopes that have a valid canonical season identity."""

        return tuple(
            season
            for competition in self._competitions.values()
            for season in competition.seasons
        )

    def active_scopes(self, *, on: Optional[date] = None) -> tuple[CatalogSeason, ...]:
        """Eligible active scopes; fail closed when activity is unresolved."""

        current = on or date.today()
        active: list[CatalogSeason] = []
        unresolved: list[str] = []
        for season in self.enabled_scopes():
            if season.is_active is True:
                active.append(season)
                continue
            if season.is_active is False:
                continue
            if season.start is not None or season.end is not None:
                if (season.start is None or season.start <= current) and (
                    season.end is None or season.end >= current
                ):
                    active.append(season)
                continue
            unresolved.append(season.scope.spec)
        if unresolved:
            raise CatalogError(
                "Cannot determine active status for eligible scopes: "
                + ", ".join(sorted(unresolved))
            )
        return tuple(active)

    def eligible_scopes(
        self, *, active_only: bool = False, on: Optional[date] = None
    ) -> tuple[CatalogSeason, ...]:
        return self.active_scopes(on=on) if active_only else self.enabled_scopes()

    @property
    def quarantined(self) -> tuple[dict[str, Any], ...]:
        rows: list[dict[str, Any]] = []
        for kind in ("competitions", "seasons", "stages"):
            for source in self._discovery_rows.get(kind, ()):
                try:
                    eligibility = _row_eligibility(source)
                    technical_provenance_error = (
                        _technical_override_provenance_error(source)
                        if kind == "competitions"
                        else None
                    )
                except CatalogError:
                    eligibility = TournamentEligibility.QUARANTINED
                    technical_provenance_error = None
                if (
                    eligibility is TournamentEligibility.QUARANTINED
                    or technical_provenance_error is not None
                ):
                    record = {"record_type": kind[:-1], **dict(source)}
                    if technical_provenance_error is not None:
                        record.update(
                            {
                                "eligibility": TournamentEligibility.QUARANTINED.value,
                                "classification_reason": technical_provenance_error,
                            }
                        )
                    rows.append(record)
        return tuple(rows)

    def to_rows(self) -> dict[str, tuple[dict[str, Any], ...]]:
        """Return the persisted discovery wire contract without dropping rows."""

        if self._discovery_rows:
            return {
                name: tuple(dict(row) for row in self._discovery_rows.get(name, ()))
                for name in ("competitions", "seasons", "stages")
            }
        competition_rows: list[dict[str, Any]] = []
        season_rows: list[dict[str, Any]] = []
        for competition in self.competitions:
            competition_rows.append(
                {
                    "competition_id": competition.competition_id,
                    "region_id": competition.region_id,
                    "region_name": competition.region_name,
                    "region_code": competition.region_code,
                    "tournament_id": competition.tournament_id,
                    "tournament_name": competition.tournament_name,
                    "tournament_url": competition.tournament_url,
                    "source_sex": competition.source_sex,
                    "eligibility": competition.eligibility.value,
                    "classification_reason": competition.classification_reason,
                    "classifier_version": competition.classifier_version,
                    "override_version": competition.override_version,
                    "source_raw_json": competition.raw_json,
                    "source_schema_fingerprint": competition.schema_fingerprint,
                }
            )
            for season in competition.seasons:
                season_rows.append(
                    {
                        "competition_id": competition.competition_id,
                        "region_id": competition.region_id,
                        "tournament_id": competition.tournament_id,
                        "season_id": season.scope.season_id,
                        "source_season_id": season.source_season_id,
                        "source_label": season.source_label,
                        "season_format": season.scope.season_format.value,
                        "source_url": season.source_url,
                        "start": season.start.isoformat() if season.start else None,
                        "end": season.end.isoformat() if season.end else None,
                        "is_active": season.is_active,
                        "eligibility": season.eligibility.value,
                        "classification_reason": season.classification_reason,
                        "source_raw_json": season.raw_json,
                        "source_schema_fingerprint": season.schema_fingerprint,
                    }
                )
        return {
            "competitions": tuple(competition_rows),
            "seasons": tuple(season_rows),
            "stages": (),
        }
