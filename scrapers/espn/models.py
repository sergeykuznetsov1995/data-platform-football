"""Source-native contracts shared by the ESPN Raw/Bronze v2 pipeline.

Identity is deliberately independent from display labels and legacy medallion
aliases.  An ESPN scope is always ``<numeric competition id>:<source year>``.
The models contain no transport, parsing or persistence behavior.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from enum import Enum
import hashlib
import json
from typing import Any, Mapping, Optional


class Gender(str, Enum):
    MALE = "MALE"
    FEMALE = "FEMALE"
    MIXED = "MIXED"
    UNKNOWN = "UNKNOWN"


class AgeClass(str, Enum):
    SENIOR = "SENIOR"
    YOUTH = "YOUTH"
    UNKNOWN = "UNKNOWN"


class CapabilityState(str, Enum):
    """Evidence level for one edition/entity pair."""

    PROVEN = "proven"
    PARTIAL = "partial"
    ABSENT = "absent"
    UNKNOWN = "unknown"
    QUARANTINED = "quarantined"


class ManifestState(str, Enum):
    PLANNED = "planned"
    INCOMPLETE = "incomplete"
    COMPLETE = "complete"
    FAILED = "failed"


class DispositionState(str, Enum):
    PLANNED = "planned"
    CAPTURED = "captured"
    VALID_EMPTY = "valid_empty"
    FAILED = "failed"
    SKIPPED = "skipped"
    QUARANTINED = "quarantined"


def _utc_string(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime values must be timezone-aware")
    normalized = value.astimezone(timezone.utc).isoformat(timespec="seconds")
    return normalized.replace("+00:00", "Z")


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return _utc_string(value)
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value):
        return {
            field.name: _canonical_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_canonical_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported canonical JSON value {type(value).__name__}")


class CanonicalModel:
    """Mixin providing byte-stable JSON and SHA-256 signatures."""

    def to_dict(self) -> dict[str, Any]:
        result = _canonical_value(self)
        if not isinstance(result, dict):  # pragma: no cover - mixin is dataclass-only
            raise TypeError("CanonicalModel must be used with a dataclass")
        return result

    def canonical_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    def signature(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class EntityCapabilities(CanonicalModel):
    schedule: CapabilityState
    lineup: CapabilityState
    matchsheet: CapabilityState

    def __post_init__(self) -> None:
        for field_name in ("schedule", "lineup", "matchsheet"):
            value = getattr(self, field_name)
            if not isinstance(value, CapabilityState):
                object.__setattr__(self, field_name, CapabilityState(value))


@dataclass(frozen=True, slots=True)
class Edition(CanonicalModel):
    source_season_year: int
    display_name: str
    start_date: date
    end_date: date
    current: bool
    capabilities: EntityCapabilities

    def __post_init__(self) -> None:
        if isinstance(self.source_season_year, bool) or self.source_season_year < 1800:
            raise ValueError("source_season_year must be an ESPN season year")
        if not self.display_name.strip():
            raise ValueError("edition display_name must not be empty")
        if self.start_date > self.end_date:
            raise ValueError("edition date window starts after it ends")

    @property
    def scope_suffix(self) -> str:
        return str(self.source_season_year)


@dataclass(frozen=True, slots=True)
class LegacyAliases(CanonicalModel):
    league: str
    league_aliases: tuple[str, ...] = ()
    season_aliases: Mapping[int, tuple[str, ...]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if not self.league.strip():
            raise ValueError("legacy league must not be empty")
        if self.season_aliases is None:
            object.__setattr__(self, "season_aliases", {})


@dataclass(frozen=True, slots=True)
class Competition(CanonicalModel):
    espn_id: int
    slug: str
    name: str
    gender: Gender
    age_class: AgeClass
    enabled: bool
    editions: tuple[Edition, ...]
    gender_evidence: tuple[str, ...] = ()
    age_class_evidence: tuple[str, ...] = ()
    legacy: Optional[LegacyAliases] = None

    def __post_init__(self) -> None:
        if isinstance(self.espn_id, bool) or self.espn_id <= 0:
            raise ValueError("espn_id must be a positive integer")
        if not self.slug.strip() or not self.name.strip():
            raise ValueError("competition slug and name must not be empty")
        if not isinstance(self.gender, Gender):
            object.__setattr__(self, "gender", Gender(self.gender))
        if not isinstance(self.age_class, AgeClass):
            object.__setattr__(self, "age_class", AgeClass(self.age_class))

    def scope_id(self, edition: Edition | int) -> str:
        year = (
            edition.source_season_year
            if isinstance(edition, Edition)
            else int(edition)
        )
        return f"{self.espn_id}:{year}"

    @property
    def current_edition(self) -> Edition:
        current = tuple(edition for edition in self.editions if edition.current)
        if len(current) != 1:
            raise ValueError("competition must have exactly one current edition")
        return current[0]


@dataclass(frozen=True, slots=True)
class ScopePlan(CanonicalModel):
    scope_id: str
    espn_id: int
    slug: str
    source_season_year: int
    start_date: date
    end_date: date
    capabilities: EntityCapabilities

    def __post_init__(self) -> None:
        expected = f"{self.espn_id}:{self.source_season_year}"
        if self.scope_id != expected:
            raise ValueError(f"scope_id must be {expected!r}")
        if self.start_date > self.end_date:
            raise ValueError("scope plan has an invalid date window")


@dataclass(frozen=True, slots=True)
class IngestPlan(CanonicalModel):
    schema_version: int
    run_id: str
    as_of: date
    registry_signature: str
    scopes: tuple[ScopePlan, ...]
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class RequestDisposition(CanonicalModel):
    endpoint: str
    state: DispositionState
    detail: str
    event_id: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, DispositionState):
            object.__setattr__(self, "state", DispositionState(self.state))
        if not self.endpoint.strip() or not self.detail.strip():
            raise ValueError("disposition endpoint and detail must not be empty")


@dataclass(frozen=True, slots=True)
class ScopeManifest(CanonicalModel):
    schema_version: int
    run_id: str
    scope_id: str
    registry_signature: str
    plan_signature: str
    state: ManifestState
    generated_at: datetime
    dispositions: tuple[RequestDisposition, ...]
    row_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        if not isinstance(self.state, ManifestState):
            object.__setattr__(self, "state", ManifestState(self.state))
        _utc_string(self.generated_at)
        if any(value < 0 for value in self.row_counts.values()):
            raise ValueError("manifest row counts must be non-negative")


__all__ = [
    "AgeClass",
    "CanonicalModel",
    "CapabilityState",
    "Competition",
    "DispositionState",
    "Edition",
    "EntityCapabilities",
    "Gender",
    "IngestPlan",
    "LegacyAliases",
    "ManifestState",
    "RequestDisposition",
    "ScopeManifest",
    "ScopePlan",
]
