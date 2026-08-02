"""Strict loader and explicit promotion helpers for the ESPN registry."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml

from .discovery import CatalogCandidate, CatalogSnapshot
from .models import (
    ADMITTED_AGE_CLASSES,
    AgeClass,
    CanonicalModel,
    CapabilityState,
    Competition,
    Edition,
    EntityCapabilities,
    Gender,
    LegacyAliases,
)


SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS = frozenset({SCHEMA_VERSION})
DEFAULT_REGISTRY_PATH = (
    Path(__file__).resolve().parents[2] / "configs" / "espn" / "registry.yaml"
)


class RegistryError(ValueError):
    """The source registry is missing, malformed or unsafe to activate."""


def _reject_unknown_keys(
    value: Mapping[str, Any], allowed: set[str], field: str
) -> None:
    extras = set(value) - allowed
    if extras:
        raise RegistryError(f"{field} has unknown keys {sorted(extras)}")


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryError(f"{field} must be a non-empty string")
    return value.strip()


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RegistryError(f"{field} must be a positive integer")
    return value


def _iso_date(value: Any, field: str) -> date:
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise RegistryError(f"{field} must be an ISO-8601 date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RegistryError(f"{field} must be an ISO-8601 date") from exc


def _string_tuple(value: Any, field: str, *, nonempty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise RegistryError(f"{field} must be a string list")
    result = tuple(item.strip() for item in value)
    if nonempty and not result:
        raise RegistryError(f"{field} must contain evidence")
    return result


def _enum(enum_type, value: Any, field: str):
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        choices = ", ".join(item.value for item in enum_type)
        label = "capability" if enum_type is CapabilityState else field
        raise RegistryError(
            f"unknown {label} {value!r}; expected one of {choices}"
        ) from exc


def _capabilities(value: Any, field: str) -> EntityCapabilities:
    if not isinstance(value, Mapping):
        raise RegistryError(f"{field} must be an object")
    required = {"schedule", "lineup", "matchsheet"}
    if set(value) != required:
        raise RegistryError(
            f"{field} must define exactly schedule, lineup and matchsheet"
        )
    return EntityCapabilities(
        _enum(CapabilityState, value["schedule"], f"{field}.schedule"),
        _enum(CapabilityState, value["lineup"], f"{field}.lineup"),
        _enum(CapabilityState, value["matchsheet"], f"{field}.matchsheet"),
    )


def _edition(value: Any, field: str) -> Edition:
    if not isinstance(value, Mapping):
        raise RegistryError(f"{field} must be an object")
    _reject_unknown_keys(
        value,
        {
            "source_season_year",
            "display_name",
            "start_date",
            "end_date",
            "current",
            "capabilities",
        },
        field,
    )
    current = value.get("current")
    if not isinstance(current, bool):
        raise RegistryError(f"{field}.current must be boolean")
    start = _iso_date(value.get("start_date"), f"{field}.start_date")
    end = _iso_date(value.get("end_date"), f"{field}.end_date")
    if start > end:
        raise RegistryError(f"{field} has an invalid date window")
    try:
        return Edition(
            source_season_year=_positive_int(
                value.get("source_season_year"),
                f"{field}.source_season_year",
            ),
            display_name=_required_string(
                value.get("display_name"), f"{field}.display_name"
            ),
            start_date=start,
            end_date=end,
            current=current,
            capabilities=_capabilities(
                value.get("capabilities"), f"{field}.capabilities"
            ),
        )
    except ValueError as exc:
        raise RegistryError(f"{field}: {exc}") from exc


def _legacy(value: Any, field: str) -> Optional[LegacyAliases]:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise RegistryError(f"{field} must be an object")
    _reject_unknown_keys(value, {"league", "league_aliases", "season_aliases"}, field)
    league = _required_string(value.get("league"), f"{field}.league")
    league_aliases = _string_tuple(
        value.get("league_aliases", []), f"{field}.league_aliases"
    )
    raw_seasons = value.get("season_aliases", {})
    if not isinstance(raw_seasons, Mapping):
        raise RegistryError(f"{field}.season_aliases must be an object")
    season_aliases: dict[int, tuple[str, ...]] = {}
    for raw_year, aliases in raw_seasons.items():
        try:
            year = int(raw_year)
        except (TypeError, ValueError) as exc:
            raise RegistryError(
                f"{field}.season_aliases key {raw_year!r} must be a year"
            ) from exc
        season_aliases[year] = _string_tuple(
            aliases, f"{field}.season_aliases[{year}]", nonempty=True
        )
    return LegacyAliases(league, league_aliases, season_aliases)


def _competition(value: Any, index: int) -> Competition:
    field = f"competitions[{index}]"
    if not isinstance(value, Mapping):
        raise RegistryError(f"{field} must be an object")
    _reject_unknown_keys(
        value,
        {
            "espn_id",
            "slug",
            "name",
            "enabled",
            "gender",
            "age_class",
            "gender_evidence",
            "age_class_evidence",
            "legacy",
            "editions",
        },
        field,
    )
    enabled = value.get("enabled")
    if not isinstance(enabled, bool):
        raise RegistryError(f"{field}.enabled must be boolean")
    gender = _enum(Gender, value.get("gender"), f"{field}.gender")
    age_class = _enum(AgeClass, value.get("age_class"), f"{field}.age_class")
    raw_editions = value.get("editions")
    if not isinstance(raw_editions, list) or not raw_editions:
        raise RegistryError(f"{field}.editions must contain current edition metadata")
    editions = tuple(
        _edition(item, f"{field}.editions[{edition_index}]")
        for edition_index, item in enumerate(raw_editions)
    )
    years = [edition.source_season_year for edition in editions]
    if len(years) != len(set(years)):
        raise RegistryError(f"{field} has duplicate source season years")
    if sum(edition.current for edition in editions) != 1:
        raise RegistryError(f"{field} must have exactly one current edition")
    gender_evidence = _string_tuple(
        value.get("gender_evidence", []), f"{field}.gender_evidence"
    )
    age_evidence = _string_tuple(
        value.get("age_class_evidence", []), f"{field}.age_class_evidence"
    )
    if enabled:
        if gender is not Gender.MALE:
            raise RegistryError(f"{field} promotion requires explicit MALE gender")
        if not gender_evidence:
            raise RegistryError(f"{field} promotion requires gender evidence")
        if age_class is not AgeClass.UNKNOWN and age_class not in ADMITTED_AGE_CLASSES:
            raise RegistryError(f"{field} promotion requires explicit age_class")
        if age_class is not AgeClass.UNKNOWN and not age_evidence:
            raise RegistryError(f"{field} promotion requires age class evidence")
        for edition in editions:
            if edition.capabilities.schedule not in {
                CapabilityState.PROVEN,
                CapabilityState.UNKNOWN,
            }:
                raise RegistryError(
                    f"{field} schedule capability must be proven or unknown "
                    "for every edition"
                )
            for entity in ("lineup", "matchsheet"):
                state = getattr(edition.capabilities, entity)
                if state not in {
                    CapabilityState.PROVEN,
                    CapabilityState.PARTIAL,
                    CapabilityState.ABSENT,
                    CapabilityState.UNKNOWN,
                }:
                    raise RegistryError(
                        f"{field} {entity} capability must be explicit "
                        "(proven, partial, absent or unknown)"
                    )
    return Competition(
        espn_id=_positive_int(value.get("espn_id"), f"{field}.espn_id"),
        slug=_required_string(value.get("slug"), f"{field}.slug"),
        name=_required_string(value.get("name"), f"{field}.name"),
        gender=gender,
        age_class=age_class,
        enabled=enabled,
        editions=editions,
        gender_evidence=gender_evidence,
        age_class_evidence=age_evidence,
        legacy=_legacy(value.get("legacy"), f"{field}.legacy"),
    )


@dataclass(frozen=True, slots=True)
class Registry(CanonicalModel):
    schema_version: int
    registry_version: str
    as_of: date
    competitions: tuple[Competition, ...]

    @property
    def by_id(self) -> dict[int, Competition]:
        return {competition.espn_id: competition for competition in self.competitions}

    @property
    def by_slug(self) -> dict[str, Competition]:
        return {competition.slug: competition for competition in self.competitions}

    @property
    def promoted(self) -> tuple[Competition, ...]:
        return tuple(item for item in self.competitions if item.enabled)


def validate_registry_document(document: Mapping[str, Any]) -> Registry:
    if not isinstance(document, Mapping):
        raise RegistryError("registry root must be an object")
    _reject_unknown_keys(
        document,
        {"schema_version", "registry_version", "as_of", "competitions"},
        "registry",
    )
    schema_version = document.get("schema_version")
    if (
        type(schema_version) is not int
        or schema_version <= 0
        or schema_version not in SUPPORTED_SCHEMA_VERSIONS
    ):
        raise RegistryError(
            f"schema_version must be one of {sorted(SUPPORTED_SCHEMA_VERSIONS)}"
        )
    raw_competitions = document.get("competitions")
    if not isinstance(raw_competitions, list):
        raise RegistryError("registry competitions must be a list")
    competitions = tuple(
        _competition(item, index) for index, item in enumerate(raw_competitions)
    )
    seen_ids: set[int] = set()
    seen_slugs: set[str] = set()
    for competition in competitions:
        if competition.espn_id in seen_ids:
            raise RegistryError(f"duplicate espn_id {competition.espn_id}")
        if competition.slug in seen_slugs:
            raise RegistryError(f"duplicate slug {competition.slug!r}")
        seen_ids.add(competition.espn_id)
        seen_slugs.add(competition.slug)
    return Registry(
        schema_version=schema_version,
        registry_version=_required_string(
            document.get("registry_version"), "registry_version"
        ),
        as_of=_iso_date(document.get("as_of"), "as_of"),
        competitions=competitions,
    )


def load_registry(path: str | Path = DEFAULT_REGISTRY_PATH) -> Registry:
    source = Path(path)
    try:
        with source.open("r", encoding="utf-8") as handle:
            document = yaml.safe_load(handle)
    except FileNotFoundError as exc:
        raise RegistryError(f"registry not found: {source}") from exc
    except (OSError, yaml.YAMLError) as exc:
        raise RegistryError(f"cannot read registry {source}: {exc}") from exc
    return validate_registry_document(document)


def _captured_date(captured_at: str) -> date:
    if not isinstance(captured_at, str):
        raise RegistryError("discovery captured_at must be an ISO-8601 timestamp")
    try:
        return datetime.fromisoformat(captured_at.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise RegistryError(
            "discovery captured_at must be an ISO-8601 timestamp"
        ) from exc


def _validate_explicit_male_candidate(candidate: CatalogCandidate) -> None:
    try:
        if candidate.gender is not Gender.MALE:
            raise ValueError("gender is not MALE")
        _positive_int(candidate.espn_id, "espn_id")
        _required_string(candidate.slug, "slug")
        _required_string(candidate.name, "name")
        if not candidate.gender_evidence:
            raise ValueError("gender evidence is missing")
        _positive_int(candidate.source_season_year, "source_season_year")
        _required_string(candidate.edition_display_name, "edition_display_name")
        _iso_date(candidate.start_date, "start_date")
        _iso_date(candidate.end_date, "end_date")
        if candidate.start_date > candidate.end_date:
            raise ValueError("date window is invalid")
        if not isinstance(candidate.capabilities, EntityCapabilities):
            raise ValueError("capabilities are invalid")
        if candidate.capabilities.schedule not in {
            CapabilityState.PROVEN,
            CapabilityState.UNKNOWN,
        }:
            raise ValueError("schedule capability is unsafe")
        for entity in ("lineup", "matchsheet"):
            if getattr(candidate.capabilities, entity) not in {
                CapabilityState.PROVEN,
                CapabilityState.PARTIAL,
                CapabilityState.ABSENT,
                CapabilityState.UNKNOWN,
            }:
                raise ValueError(f"{entity} capability is unsafe")
    except (RegistryError, TypeError, ValueError) as exc:
        raise RegistryError("explicit MALE candidate has incomplete identity") from exc


def _matching_competition(
    registry: Registry, candidate: CatalogCandidate
) -> Competition | None:
    by_id = registry.by_id.get(candidate.espn_id)
    by_slug = registry.by_slug.get(candidate.slug)
    if by_id is None and by_slug is None:
        return None
    if by_id is None or by_slug is None or by_id is not by_slug:
        raise RegistryError("explicit MALE candidate identity conflicts with registry")
    return by_id


def _discovered_edition(candidate: CatalogCandidate) -> Edition:
    return Edition(
        source_season_year=candidate.source_season_year,
        display_name=candidate.edition_display_name,
        start_date=_iso_date(candidate.start_date, "start_date"),
        end_date=_iso_date(candidate.end_date, "end_date"),
        current=True,
        capabilities=candidate.capabilities,
    )


def _discovered_competition(
    candidate: CatalogCandidate,
    overlay: Registry,
    legacy_registry: Registry,
) -> Competition:
    prior = _matching_competition(overlay, candidate)
    manual = _matching_competition(legacy_registry, candidate)
    discovered_edition = _discovered_edition(candidate)
    if prior is not None:
        legacy = manual.legacy if manual is not None else None
        if prior.current_edition.source_season_year == candidate.source_season_year:
            editions = prior.editions
        else:
            editions = tuple(
                Edition(
                    source_season_year=edition.source_season_year,
                    display_name=edition.display_name,
                    start_date=edition.start_date,
                    end_date=edition.end_date,
                    current=False,
                    capabilities=edition.capabilities,
                )
                for edition in prior.editions
            ) + (discovered_edition,)
        return Competition(
            espn_id=candidate.espn_id,
            slug=candidate.slug,
            name=candidate.name,
            gender=Gender.MALE,
            age_class=prior.age_class,
            enabled=True,
            editions=editions,
            gender_evidence=candidate.gender_evidence,
            age_class_evidence=prior.age_class_evidence,
            legacy=legacy,
        )
    return Competition(
        espn_id=candidate.espn_id,
        slug=candidate.slug,
        name=candidate.name,
        gender=Gender.MALE,
        age_class=AgeClass.UNKNOWN,
        enabled=True,
        editions=(discovered_edition,),
        gender_evidence=candidate.gender_evidence,
        age_class_evidence=(),
        legacy=manual.legacy if manual is not None else None,
    )


def build_discovered_male_registry(
    snapshot: CatalogSnapshot,
    *,
    legacy_registry: Registry,
    previous_registry: Registry | None = None,
) -> Registry:
    """Build a deterministic enabled registry from explicit ESPN male evidence."""

    if not isinstance(snapshot, CatalogSnapshot):
        raise TypeError("snapshot must be a CatalogSnapshot")
    if not isinstance(legacy_registry, Registry):
        raise TypeError("legacy_registry must be a Registry")
    if previous_registry is not None and not isinstance(previous_registry, Registry):
        raise TypeError("previous_registry must be a Registry or None")
    overlay = previous_registry or legacy_registry
    rows: list[Competition] = []
    seen_ids: set[int] = set()
    seen_slugs: set[str] = set()
    for candidate in snapshot.candidates:
        if candidate.gender is not Gender.MALE:
            continue
        _validate_explicit_male_candidate(candidate)
        if candidate.espn_id in seen_ids or candidate.slug in seen_slugs:
            raise RegistryError("explicit MALE candidate identity is duplicated")
        seen_ids.add(candidate.espn_id)
        seen_slugs.add(candidate.slug)
        rows.append(_discovered_competition(candidate, overlay, legacy_registry))
    if not rows:
        raise RegistryError("discovery contains no explicit MALE competitions")
    if len(rows) > 300:
        raise RegistryError("explicit MALE competition cap exceeded")
    return Registry(
        schema_version=SCHEMA_VERSION,
        registry_version=f"discovery-male-{snapshot.signature()[:16]}",
        as_of=_captured_date(snapshot.captured_at),
        competitions=tuple(sorted(rows, key=lambda row: (row.espn_id, row.slug))),
    )


def _legacy_season_aliases(candidate: CatalogCandidate) -> list[str]:
    year = candidate.source_season_year
    if year is None:
        return []
    aliases = [str(year)]
    if candidate.start_date and candidate.end_date:
        start_year = int(candidate.start_date[:4])
        end_year = int(candidate.end_date[:4])
        if start_year == year and end_year == year + 1:
            aliases.insert(0, f"{year % 100:02d}{(year + 1) % 100:02d}")
    return aliases


def promote_candidate(
    document: Mapping[str, Any],
    candidate: CatalogCandidate,
    *,
    age_class: AgeClass,
    age_class_evidence: tuple[str, ...],
    legacy_league: str,
) -> dict[str, Any]:
    """Return an explicitly promoted copy; the discovery snapshot is untouched."""

    if candidate.espn_id is None:
        raise RegistryError("promotion requires an ESPN numeric id")
    if candidate.gender is not Gender.MALE:
        raise RegistryError(
            "promotion requires explicit MALE gender from detail metadata"
        )
    if not candidate.gender_evidence:
        raise RegistryError("promotion requires explicit gender evidence")
    if age_class not in ADMITTED_AGE_CLASSES:
        raise RegistryError("promotion requires explicit age_class")
    if not age_class_evidence:
        raise RegistryError("promotion requires age class evidence")
    if candidate.source_season_year is None or not all(
        (candidate.edition_display_name, candidate.start_date, candidate.end_date)
    ):
        raise RegistryError(
            "promotion requires current edition metadata and date window"
        )
    if candidate.capabilities.schedule is not CapabilityState.PROVEN:
        raise RegistryError("promotion requires proven schedule capability")

    promoted = deepcopy(dict(document))
    competitions = promoted.get("competitions")
    if not isinstance(competitions, list):
        raise RegistryError("registry competitions must be a list")
    new_edition = {
        "source_season_year": candidate.source_season_year,
        "display_name": candidate.edition_display_name,
        "start_date": candidate.start_date,
        "end_date": candidate.end_date,
        "current": True,
        "capabilities": candidate.capabilities.to_dict(),
    }
    existing = [
        row
        for row in competitions
        if row.get("espn_id") == candidate.espn_id or row.get("slug") == candidate.slug
    ]
    if existing:
        if len(existing) != 1:
            raise RegistryError("promotion matches multiple existing competitions")
        row = existing[0]
        if row.get("espn_id") != candidate.espn_id or row.get("slug") != candidate.slug:
            raise RegistryError("promotion identity conflicts with existing id or slug")
        if row.get("name") != candidate.name:
            raise RegistryError("promotion name conflicts with existing competition")
        legacy = row.get("legacy") or {}
        if legacy and legacy.get("league") != legacy_league:
            raise RegistryError(
                "promotion legacy league conflicts with existing mapping"
            )
        if not legacy:
            legacy = {
                "league": legacy_league,
                "league_aliases": [legacy_league],
                "season_aliases": {},
            }
            row["legacy"] = legacy
        editions = row.get("editions")
        if not isinstance(editions, list):
            raise RegistryError("existing competition editions must be a list")
        replaced = False
        for index, edition in enumerate(editions):
            if edition.get("source_season_year") == candidate.source_season_year:
                editions[index] = new_edition
                replaced = True
            else:
                edition["current"] = False
        if not replaced:
            editions.append(new_edition)
        row.update(
            enabled=True,
            gender=candidate.gender.value,
            age_class=age_class.value,
            gender_evidence=list(candidate.gender_evidence),
            age_class_evidence=list(age_class_evidence),
        )
        season_aliases = legacy.setdefault("season_aliases", {})
        season_aliases[str(candidate.source_season_year)] = _legacy_season_aliases(
            candidate
        )
    else:
        competitions.append(
            {
                "espn_id": candidate.espn_id,
                "slug": candidate.slug,
                "name": candidate.name,
                "enabled": True,
                "gender": candidate.gender.value,
                "age_class": age_class.value,
                "gender_evidence": list(candidate.gender_evidence),
                "age_class_evidence": list(age_class_evidence),
                "legacy": {
                    "league": legacy_league,
                    "league_aliases": [legacy_league],
                    "season_aliases": {
                        str(candidate.source_season_year): _legacy_season_aliases(
                            candidate
                        )
                    },
                },
                "editions": [new_edition],
            }
        )
    validate_registry_document(promoted)
    return promoted


__all__ = [
    "DEFAULT_REGISTRY_PATH",
    "Registry",
    "RegistryError",
    "SCHEMA_VERSION",
    "SUPPORTED_SCHEMA_VERSIONS",
    "build_discovered_male_registry",
    "load_registry",
    "promote_candidate",
    "validate_registry_document",
]
