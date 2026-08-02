"""Pure, network-free discovery for the ESPN soccer competition catalog.

Discovery is intentionally observational: it creates candidate snapshots and
diffs only.  It never writes or mutates the promoted source registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping, Optional, Sequence

from .models import (
    AgeClass,
    CanonicalModel,
    CapabilityState,
    EntityCapabilities,
    Gender,
)


class DiscoveryError(ValueError):
    """A frozen catalog/detail payload cannot be identified safely."""


class DiscoveryChangeKind(str, Enum):
    ADDED = "added"
    REMOVED = "removed"
    SLUG = "slug"
    GENDER = "gender"
    CURRENT_EDITION = "current_edition"
    CAPABILITIES = "capabilities"


UNKNOWN_CAPABILITIES = EntityCapabilities(
    CapabilityState.UNKNOWN,
    CapabilityState.UNKNOWN,
    CapabilityState.UNKNOWN,
)


@dataclass(frozen=True, slots=True)
class CompetitionDetail(CanonicalModel):
    espn_id: Optional[int]
    slug: str
    name: str
    gender: Gender
    source_season_year: Optional[int]
    edition_display_name: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    capabilities: EntityCapabilities
    gender_evidence: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CatalogCandidate(CanonicalModel):
    espn_id: Optional[int]
    slug: str
    name: str
    group: str
    source_order: int
    gender: Gender = Gender.UNKNOWN
    # ESPN discovery does not provide an authoritative age classification.
    # This remains UNKNOWN until an operator supplies manual evidence.
    age_class: AgeClass = AgeClass.UNKNOWN
    source_season_year: Optional[int] = None
    edition_display_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    capabilities: EntityCapabilities = UNKNOWN_CAPABILITIES
    gender_evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.espn_id is not None and (
            type(self.espn_id) is not int or self.espn_id <= 0
        ):
            raise ValueError("espn_id must be a positive integer when present")
        if type(self.source_order) is not int or self.source_order < 0:
            raise ValueError("source_order must be a non-negative integer")
        if self.source_season_year is not None and (
            type(self.source_season_year) is not int or self.source_season_year < 1800
        ):
            raise ValueError("source_season_year must be an ESPN season year")
        if not self.slug or not self.name or not self.group:
            raise ValueError("candidate slug, name and group must not be empty")
        if not isinstance(self.gender, Gender):
            object.__setattr__(self, "gender", Gender(self.gender))
        if not isinstance(self.age_class, AgeClass):
            object.__setattr__(self, "age_class", AgeClass(self.age_class))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CatalogCandidate":
        if not isinstance(value, Mapping):
            raise DiscoveryError("catalog candidate must be an object")
        allowed = {
            "espn_id",
            "slug",
            "name",
            "group",
            "source_order",
            "gender",
            "age_class",
            "source_season_year",
            "edition_display_name",
            "start_date",
            "end_date",
            "capabilities",
            "gender_evidence",
        }
        extras = set(value) - allowed
        if extras:
            raise DiscoveryError(f"catalog candidate has unknown keys {sorted(extras)}")
        if "capabilities" in value:
            capabilities = value["capabilities"]
            if not isinstance(capabilities, Mapping):
                raise DiscoveryError("catalog candidate capabilities must be an object")
            if set(capabilities) != {"schedule", "lineup", "matchsheet"}:
                raise DiscoveryError(
                    "catalog candidate capabilities must define exactly "
                    "schedule, lineup and matchsheet"
                )
        else:
            capabilities = {
                "schedule": CapabilityState.UNKNOWN.value,
                "lineup": CapabilityState.UNKNOWN.value,
                "matchsheet": CapabilityState.UNKNOWN.value,
            }
        raw_espn_id = value.get("espn_id")
        source_order = value.get("source_order")
        raw_source_year = value.get("source_season_year")
        evidence = value.get("gender_evidence") or []
        if isinstance(source_order, bool) or not isinstance(source_order, int):
            raise DiscoveryError("catalog candidate source_order must be integer")
        espn_id = (
            _native_positive_int(raw_espn_id, "catalog candidate espn_id")
            if raw_espn_id is not None
            else None
        )
        source_year = (
            _native_positive_int(
                raw_source_year,
                "catalog candidate source_season_year",
                minimum=1800,
            )
            if raw_source_year is not None
            else None
        )
        if not isinstance(evidence, list) or not all(
            isinstance(item, str) and item.strip() for item in evidence
        ):
            raise DiscoveryError(
                "catalog candidate gender_evidence must be a string list"
            )
        try:
            candidate = cls(
                espn_id=espn_id,
                slug=_required_candidate_string(value.get("slug"), "slug"),
                name=_required_candidate_string(value.get("name"), "name"),
                group=_required_candidate_string(value.get("group"), "group"),
                source_order=source_order,
                gender=Gender(value.get("gender", Gender.UNKNOWN.value)),
                age_class=AgeClass(value.get("age_class", AgeClass.UNKNOWN.value)),
                source_season_year=source_year,
                edition_display_name=value.get("edition_display_name"),
                start_date=value.get("start_date"),
                end_date=value.get("end_date"),
                capabilities=EntityCapabilities(
                    capabilities.get("schedule", CapabilityState.UNKNOWN.value),
                    capabilities.get("lineup", CapabilityState.UNKNOWN.value),
                    capabilities.get("matchsheet", CapabilityState.UNKNOWN.value),
                ),
                gender_evidence=tuple(evidence),
            )
        except (TypeError, ValueError) as exc:
            raise DiscoveryError(f"invalid catalog candidate: {exc}") from exc
        if candidate.source_order < 0:
            raise DiscoveryError("catalog candidate source_order must be non-negative")
        for field_name in ("start_date", "end_date"):
            raw_date = getattr(candidate, field_name)
            if raw_date is not None:
                _date_part(raw_date, field_name)
        if (
            candidate.start_date is not None
            and candidate.end_date is not None
            and candidate.start_date > candidate.end_date
        ):
            raise DiscoveryError("catalog candidate has an invalid date window")
        return candidate


@dataclass(frozen=True, slots=True)
class CatalogSnapshot(CanonicalModel):
    captured_at: str
    candidates: tuple[CatalogCandidate, ...]
    schema_version: int = 1
    source: str = "ESPN soccer dropdown"

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise DiscoveryError("catalog snapshot schema_version must be 1")
        _required_candidate_string(self.captured_at, "captured_at")
        _required_candidate_string(self.source, "source")
        if not isinstance(self.candidates, (list, tuple)) or not all(
            isinstance(candidate, CatalogCandidate) for candidate in self.candidates
        ):
            raise DiscoveryError(
                "catalog snapshot candidates must contain catalog candidates"
            )
        object.__setattr__(self, "candidates", tuple(self.candidates))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CatalogSnapshot":
        if not isinstance(value, Mapping):
            raise DiscoveryError("catalog snapshot must be an object")
        allowed = {"schema_version", "captured_at", "source", "candidates"}
        extras = set(value) - allowed
        if extras:
            raise DiscoveryError(f"catalog snapshot has unknown keys {sorted(extras)}")
        if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
            raise DiscoveryError("catalog snapshot schema_version must be 1")
        rows = value.get("candidates")
        if not isinstance(rows, list):
            raise DiscoveryError("catalog snapshot candidates must be a list")
        return cls(
            captured_at=_required_candidate_string(
                value.get("captured_at"), "captured_at"
            ),
            candidates=tuple(CatalogCandidate.from_dict(row) for row in rows),
            schema_version=1,
            source=_required_candidate_string(value.get("source"), "source"),
        )


@dataclass(frozen=True, slots=True)
class DiscoveryChange(CanonicalModel):
    kind: DiscoveryChangeKind
    espn_id: Optional[int]
    before: Any
    after: Any


@dataclass(frozen=True, slots=True)
class CatalogDiff(CanonicalModel):
    previous_captured_at: str
    current_captured_at: str
    changes: tuple[DiscoveryChange, ...]


def _required_candidate_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DiscoveryError(f"catalog candidate {field} must be a non-empty string")
    return value.strip()


def _native_positive_int(value: Any, field: str, *, minimum: int = 1) -> int:
    if type(value) is int:
        parsed = value
    elif isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value):
        parsed = int(value)
    else:
        raise DiscoveryError(f"{field} must be a canonical positive integer")
    if parsed < minimum:
        raise DiscoveryError(f"{field} must be at least {minimum}")
    return parsed


def _find_league_teams(value: Any) -> Optional[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        if isinstance(value.get("leagueTeams"), Mapping):
            return value["leagueTeams"]
        for child in value.values():
            found = _find_league_teams(child)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_league_teams(child)
            if found is not None:
                return found
    return None


def _dropdown_mapping(payload: str | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(payload, Mapping):
        if isinstance(payload.get("leagues"), list):
            return payload
        if isinstance(payload.get("groups"), list):
            return payload
        result = _find_league_teams(payload)
        if result is None:
            raise DiscoveryError("dropdown payload contains no groups")
        return result
    if not isinstance(payload, str):
        raise DiscoveryError("dropdown payload must be HTML or an object")
    marker = "window['__espnfitt__']="
    position = payload.find(marker)
    if position < 0:
        raise DiscoveryError("ESPN HTML contains no __espnfitt__ payload")
    try:
        document, _ = json.JSONDecoder().raw_decode(payload[position + len(marker) :])
    except json.JSONDecodeError as exc:
        raise DiscoveryError("ESPN __espnfitt__ payload is not valid JSON") from exc
    result = _find_league_teams(document)
    if result is None:
        raise DiscoveryError("ESPN payload contains no leagueTeams groups")
    return result


_SLUG_RE = re.compile(r"/league/(?:_/name/)?([A-Za-z0-9._-]+)(?:[/?#]|$)")


def _row_slug(row: Mapping[str, Any]) -> str:
    links = row.get("lk")
    if not isinstance(links, list):
        raise DiscoveryError("dropdown competition has no links")
    for link in links:
        if not isinstance(link, Mapping):
            continue
        match = _SLUG_RE.search(str(link.get("u") or ""))
        if match:
            return match.group(1)
    raise DiscoveryError(f"dropdown competition {row.get('id')!r} has no league slug")


def parse_soccer_dropdown(
    payload: str | Mapping[str, Any],
) -> tuple[CatalogCandidate, ...]:
    """Parse all catalog occurrences without deduplicating or classifying."""

    dropdown = _dropdown_mapping(payload)
    direct_rows = dropdown.get("leagues")
    if isinstance(direct_rows, list):
        candidates: list[CatalogCandidate] = []
        for source_order, row in enumerate(direct_rows):
            if not isinstance(row, Mapping):
                raise DiscoveryError("dropdown league must be an object")
            raw_id = row.get("id")
            try:
                espn_id = (
                    _native_positive_int(raw_id, "dropdown league numeric id")
                    if raw_id is not None
                    else None
                )
            except DiscoveryError as exc:
                raise DiscoveryError("dropdown league has invalid numeric id") from exc
            slug = str(row.get("slug") or "").strip()
            name = str(row.get("name") or "").strip()
            if not slug or not name:
                raise DiscoveryError(
                    f"dropdown league {espn_id!r} is missing slug or name"
                )
            candidates.append(
                CatalogCandidate(
                    espn_id=espn_id,
                    slug=slug,
                    name=name,
                    group="ESPN soccer dropdown",
                    source_order=source_order,
                )
            )
        if not candidates:
            raise DiscoveryError("ESPN soccer dropdown contains no competitions")
        return tuple(candidates)
    groups = dropdown.get("groups")
    if not isinstance(groups, list):
        raise DiscoveryError("leagueTeams.groups must be a list")
    candidates: list[CatalogCandidate] = []
    source_order = 0
    for group in groups:
        if not isinstance(group, Mapping):
            raise DiscoveryError("dropdown group must be an object")
        group_name = str(group.get("title") or group.get("name") or "").strip()
        if not group_name:
            raise DiscoveryError("dropdown group has no name")
        columns = group.get("columns")
        if not isinstance(columns, list):
            raise DiscoveryError(f"dropdown group {group_name!r} has no columns")
        for column in columns:
            if not isinstance(column, Mapping):
                raise DiscoveryError("dropdown column must be an object")
            teams = column.get("teams")
            if not isinstance(teams, list):
                raise DiscoveryError("dropdown column teams must be a list")
            for row in teams:
                if not isinstance(row, Mapping):
                    raise DiscoveryError("dropdown competition must be an object")
                try:
                    espn_id = _native_positive_int(
                        row["id"], "dropdown competition numeric id"
                    )
                except (KeyError, DiscoveryError) as exc:
                    raise DiscoveryError(
                        "dropdown competition has invalid numeric id"
                    ) from exc
                name = str(row.get("n") or "").strip()
                if not name:
                    raise DiscoveryError(f"dropdown competition {espn_id} has no name")
                candidates.append(
                    CatalogCandidate(
                        espn_id=espn_id,
                        slug=_row_slug(row),
                        name=name,
                        group=group_name,
                        source_order=source_order,
                    )
                )
                source_order += 1
    if not candidates:
        raise DiscoveryError("ESPN soccer dropdown contains no competitions")
    return tuple(candidates)


def _date_part(value: Any, field: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) < 10:
        raise DiscoveryError(f"competition detail {field} must be an ISO date")
    token = value[:10]
    try:
        from datetime import date

        date.fromisoformat(token)
    except ValueError as exc:
        raise DiscoveryError(f"competition detail {field} must be an ISO date") from exc
    return token


def _parse_gender(value: Any) -> Gender:
    if value is None:
        return Gender.UNKNOWN
    if not isinstance(value, str):
        raise DiscoveryError(f"unknown competition detail gender {value!r}")
    try:
        return Gender(value)
    except ValueError as exc:
        raise DiscoveryError(f"unknown competition detail gender {value!r}") from exc


def parse_competition_detail(payload: Mapping[str, Any]) -> CompetitionDetail:
    """Parse only competition-level classification and edition metadata."""

    if not isinstance(payload, Mapping):
        raise DiscoveryError("competition detail must be an object")
    raw_id = payload.get("id")
    if raw_id is None:
        espn_id = None
    else:
        try:
            espn_id = _native_positive_int(raw_id, "competition detail numeric id")
        except DiscoveryError as exc:
            raise DiscoveryError("competition detail has invalid numeric id") from exc
    slug = str(payload.get("slug") or "").strip()
    name = str(payload.get("name") or payload.get("displayName") or "").strip()
    if not slug or not name:
        raise DiscoveryError("competition detail is missing slug or name")
    gender = _parse_gender(payload.get("gender"))
    evidence_value = payload.get("genderEvidence")
    if evidence_value is None:
        evidence = ()
    elif isinstance(evidence_value, list):
        evidence = tuple(
            str(item).strip() for item in evidence_value if str(item).strip()
        )
    else:
        evidence = (str(evidence_value).strip(),) if str(evidence_value).strip() else ()
    if gender is not Gender.UNKNOWN and not evidence:
        evidence = (f"detail.gender={gender.value}",)

    season = payload.get("season") or {}
    if not isinstance(season, Mapping):
        raise DiscoveryError("competition detail season must be an object")
    year_value = season.get("year")
    source_year = (
        _native_positive_int(year_value, "competition detail season.year", minimum=1800)
        if year_value is not None
        else None
    )
    if "capabilities" in payload:
        capabilities_value = payload["capabilities"]
        if not isinstance(capabilities_value, Mapping):
            raise DiscoveryError("competition detail capabilities must be an object")
        if set(capabilities_value) != {"schedule", "lineup", "matchsheet"}:
            raise DiscoveryError(
                "competition detail capabilities must define exactly "
                "schedule, lineup and matchsheet"
            )
    else:
        capabilities_value = {}
    try:
        capabilities = EntityCapabilities(
            capabilities_value.get("schedule", CapabilityState.UNKNOWN.value),
            capabilities_value.get("lineup", CapabilityState.UNKNOWN.value),
            capabilities_value.get("matchsheet", CapabilityState.UNKNOWN.value),
        )
    except ValueError as exc:
        raise DiscoveryError(f"unknown competition detail capability: {exc}") from exc
    return CompetitionDetail(
        espn_id=espn_id,
        slug=slug,
        name=name,
        gender=gender,
        source_season_year=source_year,
        edition_display_name=season.get("displayName"),
        start_date=_date_part(season.get("startDate"), "season.startDate"),
        end_date=_date_part(season.get("endDate"), "season.endDate"),
        capabilities=capabilities,
        gender_evidence=evidence,
    )


def discover_catalog(
    dropdown_payload: str | Mapping[str, Any],
    *,
    details_by_slug: Mapping[str, Mapping[str, Any]],
    captured_at: str,
) -> CatalogSnapshot:
    """Enrich every dropdown occurrence while leaving age/manual state unset."""

    details = {
        slug: parse_competition_detail(payload)
        for slug, payload in details_by_slug.items()
    }
    candidates: list[CatalogCandidate] = []
    for row in parse_soccer_dropdown(dropdown_payload):
        detail = details.get(row.slug)
        if detail is None:
            candidates.append(row)
            continue
        if (
            row.espn_id is not None and detail.espn_id != row.espn_id
        ) or detail.slug != row.slug:
            raise DiscoveryError(
                f"detail identity mismatch for dropdown competition {row.espn_id}:{row.slug}"
            )
        candidates.append(
            CatalogCandidate(
                espn_id=detail.espn_id,
                slug=row.slug,
                name=detail.name,
                group=row.group,
                source_order=row.source_order,
                gender=detail.gender,
                age_class=AgeClass.UNKNOWN,
                source_season_year=detail.source_season_year,
                edition_display_name=detail.edition_display_name,
                start_date=detail.start_date,
                end_date=detail.end_date,
                capabilities=detail.capabilities,
                gender_evidence=detail.gender_evidence,
            )
        )
    return CatalogSnapshot(captured_at=captured_at, candidates=tuple(candidates))


def _representatives(
    candidates: Sequence[CatalogCandidate],
) -> dict[tuple[str, object], CatalogCandidate]:
    result: dict[tuple[str, object], CatalogCandidate] = {}
    for candidate in candidates:
        identity = (
            ("id", candidate.espn_id)
            if candidate.espn_id is not None
            else ("slug", candidate.slug)
        )
        previous = result.get(identity)
        if previous is None:
            result[identity] = candidate
            continue
        compared = (
            "slug",
            "gender",
            "source_season_year",
            "edition_display_name",
            "start_date",
            "end_date",
            "capabilities",
        )
        if any(
            getattr(previous, field) != getattr(candidate, field) for field in compared
        ):
            raise DiscoveryError(f"conflicting duplicate discovery rows for {identity}")
    return result


def diff_catalogs(previous: CatalogSnapshot, current: CatalogSnapshot) -> CatalogDiff:
    before = _representatives(previous.candidates)
    after = _representatives(current.candidates)
    changes: list[DiscoveryChange] = []
    for identity in sorted(before.keys() - after.keys(), key=str):
        candidate = before[identity]
        changes.append(
            DiscoveryChange(
                DiscoveryChangeKind.REMOVED, candidate.espn_id, candidate.slug, None
            )
        )
    for identity in sorted(after.keys() - before.keys(), key=str):
        candidate = after[identity]
        changes.append(
            DiscoveryChange(
                DiscoveryChangeKind.ADDED, candidate.espn_id, None, candidate.slug
            )
        )
    simple_fields = (
        (DiscoveryChangeKind.SLUG, "slug"),
        (DiscoveryChangeKind.GENDER, "gender"),
        (DiscoveryChangeKind.CAPABILITIES, "capabilities"),
    )
    for identity in sorted(before.keys() & after.keys(), key=str):
        old_candidate = before[identity]
        new_candidate = after[identity]
        espn_id = new_candidate.espn_id
        for kind, field_name in simple_fields:
            old = getattr(old_candidate, field_name)
            new = getattr(new_candidate, field_name)
            if old != new:
                changes.append(DiscoveryChange(kind, espn_id, old, new))
        old_edition = {
            field_name: getattr(old_candidate, field_name)
            for field_name in (
                "source_season_year",
                "edition_display_name",
                "start_date",
                "end_date",
            )
        }
        new_edition = {
            field_name: getattr(new_candidate, field_name) for field_name in old_edition
        }
        if old_edition != new_edition:
            changes.append(
                DiscoveryChange(
                    DiscoveryChangeKind.CURRENT_EDITION,
                    espn_id,
                    old_edition,
                    new_edition,
                )
            )
    return CatalogDiff(previous.captured_at, current.captured_at, tuple(changes))


def save_catalog_snapshot(snapshot: CatalogSnapshot, path: str | Path) -> Path:
    """Atomically persist candidates without touching the promoted registry."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(snapshot.canonical_json())
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return target


def load_catalog_snapshot(path: str | Path) -> CatalogSnapshot:
    source = Path(path)
    try:
        with source.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise DiscoveryError(f"cannot read catalog snapshot {source}: {exc}") from exc
    return CatalogSnapshot.from_dict(document)


def quarantine_new_editions(snapshot: CatalogSnapshot, registry: Any) -> set[str]:
    """Identify source rollovers that require explicit registry promotion."""

    quarantined: set[str] = set()
    by_id = registry.by_id
    for candidate in _representatives(snapshot.candidates).values():
        if candidate.espn_id is None:
            continue
        competition = by_id.get(candidate.espn_id)
        if competition is None or candidate.source_season_year is None:
            continue
        if (
            candidate.source_season_year
            != competition.current_edition.source_season_year
        ):
            quarantined.add(f"{candidate.espn_id}:{candidate.source_season_year}")
    return quarantined


__all__ = [
    "CatalogCandidate",
    "CatalogDiff",
    "CatalogSnapshot",
    "CompetitionDetail",
    "DiscoveryChange",
    "DiscoveryChangeKind",
    "DiscoveryError",
    "diff_catalogs",
    "discover_catalog",
    "load_catalog_snapshot",
    "parse_competition_detail",
    "parse_soccer_dropdown",
    "quarantine_new_editions",
    "save_catalog_snapshot",
]
