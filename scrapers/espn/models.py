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


MODEL_SCHEMA_VERSION = 2


class CapabilityState(str, Enum):
    """Evidence level for one edition/entity pair."""

    PROVEN = "proven"
    PARTIAL = "partial"
    ABSENT = "absent"
    UNKNOWN = "unknown"
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
class SeasonType(CanonicalModel):
    """One ESPN season stage (``seasons/{year}/types/{id}``).

    A bare ``$ref`` list carries only the id; name and dates come with the
    embedded form (league detail, ``seasons/{year}``).
    """

    id: int
    name: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None

    def __post_init__(self) -> None:
        _positive_int(self.id, "season type id")
        if self.name is not None:
            _required_string(self.name, "season type name")
        for value in (self.start_date, self.end_date):
            if value is not None and type(value) is not date:
                raise TypeError("season type dates must be date values")


@dataclass(frozen=True, slots=True)
class Edition(CanonicalModel):
    source_season_year: int
    display_name: str
    start_date: date
    end_date: date
    current: bool
    capabilities: EntityCapabilities
    types: tuple[SeasonType, ...] = ()

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
        if not isinstance(self.types, (list, tuple)) or not all(
            isinstance(item, SeasonType) for item in self.types
        ):
            raise TypeError("edition types must contain SeasonType values")
        object.__setattr__(self, "types", tuple(self.types))
        if len({item.id for item in self.types}) != len(self.types):
            raise ValueError("edition types repeat a type id")

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

    def open_editions(self) -> tuple[Edition, ...]:
        """Every open edition: a new season opens before the old one closes (#1501)."""

        current = tuple(edition for edition in self.editions if edition.current)
        if not current:
            raise ValueError("competition must have at least one open edition")
        return current


__all__ = [
    "AgeClass",
    "CanonicalModel",
    "CapabilityState",
    "Competition",
    "Edition",
    "EntityCapabilities",
    "Gender",
    "LegacyAliases",
    "MODEL_SCHEMA_VERSION",
    "SeasonType",
]
