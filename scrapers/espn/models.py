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
import re
from types import MappingProxyType
from typing import Any, Mapping, Optional


class Gender(str, Enum):
    MALE = "MALE"
    FEMALE = "FEMALE"
    MIXED = "MIXED"
    UNKNOWN = "UNKNOWN"


class AgeClass(str, Enum):
    SENIOR = "SENIOR"
    YOUTH = "YOUTH"
    U17 = "U17"
    U19 = "U19"
    U20 = "U20"
    U21 = "U21"
    U23 = "U23"
    COLLEGE = "COLLEGE"
    UNKNOWN = "UNKNOWN"


ADMITTED_AGE_CLASSES = frozenset(
    {
        AgeClass.SENIOR,
        AgeClass.U17,
        AgeClass.U19,
        AgeClass.U20,
        AgeClass.U21,
        AgeClass.U23,
        AgeClass.COLLEGE,
    }
)

MODEL_SCHEMA_VERSION = 1
_SIGNATURE_RE = re.compile(r"[0-9a-f]{64}")
_SCOPE_ID_RE = re.compile(r"([1-9][0-9]*):([1-9][0-9]*)")


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
    NOT_APPLICABLE = "not_applicable"
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


def _positive_int(value: Any, field_name: str, *, minimum: int = 1) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{field_name} must be an integer of at least {minimum}")
    return value


def _required_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _string_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(f"{field_name} must contain non-empty strings")
    return tuple(value)


def _signature(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _SIGNATURE_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 signature")
    return value


def _scope_identity(value: Any) -> tuple[int, int]:
    if not isinstance(value, str):
        raise ValueError("scope_id must be '<espn_id>:<source_season_year>'")
    match = _SCOPE_ID_RE.fullmatch(value)
    if match is None:
        raise ValueError("scope_id must be '<espn_id>:<source_season_year>'")
    espn_id, source_year = (int(part) for part in match.groups())
    _positive_int(espn_id, "scope_id espn_id")
    _positive_int(source_year, "scope_id source_season_year", minimum=1800)
    return espn_id, source_year


def _freeze_json(value: Any, field_name: str) -> Any:
    """Copy JSON-like nested values into immutable containers."""

    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{field_name} keys must be strings")
            frozen[key] = _freeze_json(item, f"{field_name}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item, field_name) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"{field_name} must contain only JSON-compatible values")


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
        _positive_int(self.source_season_year, "source_season_year", minimum=1800)
        _required_string(self.display_name, "edition display_name")
        if type(self.start_date) is not date or type(self.end_date) is not date:
            raise TypeError("edition dates must be date values")
        if type(self.current) is not bool:
            raise TypeError("edition current must be boolean")
        if not isinstance(self.capabilities, EntityCapabilities):
            raise TypeError("edition capabilities must be EntityCapabilities")
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
        _required_string(self.league, "legacy league")
        object.__setattr__(
            self,
            "league_aliases",
            _string_tuple(self.league_aliases, "legacy league_aliases"),
        )
        raw_aliases = self.season_aliases if self.season_aliases is not None else {}
        if not isinstance(raw_aliases, Mapping):
            raise TypeError("legacy season_aliases must be a mapping")
        normalized: dict[int, tuple[str, ...]] = {}
        for year, aliases in raw_aliases.items():
            _positive_int(year, "legacy season_aliases year", minimum=1800)
            normalized[year] = _string_tuple(aliases, f"legacy season_aliases[{year}]")
        object.__setattr__(self, "season_aliases", MappingProxyType(normalized))


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
        _positive_int(self.espn_id, "espn_id")
        _required_string(self.slug, "competition slug")
        _required_string(self.name, "competition name")
        if not isinstance(self.gender, Gender):
            object.__setattr__(self, "gender", Gender(self.gender))
        if not isinstance(self.age_class, AgeClass):
            object.__setattr__(self, "age_class", AgeClass(self.age_class))
        if type(self.enabled) is not bool:
            raise TypeError("competition enabled must be boolean")
        if not isinstance(self.editions, (list, tuple)) or not all(
            isinstance(edition, Edition) for edition in self.editions
        ):
            raise TypeError("competition editions must contain Edition values")
        object.__setattr__(self, "editions", tuple(self.editions))
        object.__setattr__(
            self,
            "gender_evidence",
            _string_tuple(self.gender_evidence, "competition gender_evidence"),
        )
        object.__setattr__(
            self,
            "age_class_evidence",
            _string_tuple(self.age_class_evidence, "competition age_class_evidence"),
        )
        if self.legacy is not None and not isinstance(self.legacy, LegacyAliases):
            raise TypeError("competition legacy must be LegacyAliases or None")

    def scope_id(self, edition: Edition | int) -> str:
        _positive_int(self.espn_id, "espn_id")
        year = edition.source_season_year if isinstance(edition, Edition) else edition
        _positive_int(year, "source_season_year", minimum=1800)
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
        _positive_int(self.espn_id, "espn_id")
        _positive_int(self.source_season_year, "source_season_year", minimum=1800)
        _required_string(self.slug, "scope slug")
        expected = f"{self.espn_id}:{self.source_season_year}"
        if self.scope_id != expected:
            raise ValueError(f"scope_id must be {expected!r}")
        if type(self.start_date) is not date or type(self.end_date) is not date:
            raise TypeError("scope plan dates must be date values")
        if self.start_date > self.end_date:
            raise ValueError("scope plan has an invalid date window")
        if not isinstance(self.capabilities, EntityCapabilities):
            raise TypeError("scope plan capabilities must be EntityCapabilities")


@dataclass(frozen=True, slots=True)
class IngestPlan(CanonicalModel):
    schema_version: int
    run_id: str
    as_of: date
    registry_signature: str
    scopes: tuple[ScopePlan, ...]
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != MODEL_SCHEMA_VERSION
        ):
            raise ValueError(f"schema_version must be {MODEL_SCHEMA_VERSION}")
        _required_string(self.run_id, "run_id")
        if type(self.as_of) is not date:
            raise TypeError("as_of must be a date")
        _signature(self.registry_signature, "registry_signature")
        if not isinstance(self.scopes, (list, tuple)) or not all(
            isinstance(scope, ScopePlan) for scope in self.scopes
        ):
            raise TypeError("scopes must contain ScopePlan values")
        scopes = tuple(self.scopes)
        if len({scope.scope_id for scope in scopes}) != len(scopes):
            raise ValueError("scopes must have unique scope_id values")
        if not isinstance(self.metadata, Mapping):
            raise TypeError("metadata must be a mapping")
        object.__setattr__(self, "scopes", scopes)
        object.__setattr__(self, "metadata", _freeze_json(self.metadata, "metadata"))


@dataclass(frozen=True, slots=True)
class RequestDisposition(CanonicalModel):
    endpoint: str
    state: DispositionState
    detail: str
    event_id: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, DispositionState):
            object.__setattr__(self, "state", DispositionState(self.state))
        _required_string(self.endpoint, "disposition endpoint")
        _required_string(self.detail, "disposition detail")
        if self.event_id is not None:
            _positive_int(self.event_id, "disposition event_id")


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
        if (
            type(self.schema_version) is not int
            or self.schema_version != MODEL_SCHEMA_VERSION
        ):
            raise ValueError(f"schema_version must be {MODEL_SCHEMA_VERSION}")
        _required_string(self.run_id, "run_id")
        _scope_identity(self.scope_id)
        _signature(self.registry_signature, "registry_signature")
        _signature(self.plan_signature, "plan_signature")
        if not isinstance(self.state, ManifestState):
            object.__setattr__(self, "state", ManifestState(self.state))
        _utc_string(self.generated_at)
        if not isinstance(self.dispositions, (list, tuple)) or not all(
            isinstance(item, RequestDisposition) for item in self.dispositions
        ):
            raise TypeError("dispositions must contain RequestDisposition values")
        if not isinstance(self.row_counts, Mapping):
            raise TypeError("manifest row_counts must be a mapping")
        counts: dict[str, int] = {}
        for entity, count in self.row_counts.items():
            _required_string(entity, "manifest row_counts key")
            if type(count) is not int:
                raise TypeError("manifest row count must be an integer")
            if count < 0:
                raise ValueError("manifest row counts must be non-negative")
            counts[entity] = count
        object.__setattr__(self, "dispositions", tuple(self.dispositions))
        object.__setattr__(self, "row_counts", MappingProxyType(counts))


__all__ = [
    "ADMITTED_AGE_CLASSES",
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
    "MODEL_SCHEMA_VERSION",
    "RequestDisposition",
    "ScopeManifest",
    "ScopePlan",
]
