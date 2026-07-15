"""Fail-closed Transfermarkt competition and edition registry.

The registry deliberately separates source discovery from data crawling.  A
competition is crawlable only when non-name source evidence proves every
classification dimension.  Pagination is reconciled into a complete snapshot
before any deterministic ``competition x edition`` scopes are emitted.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Sequence, Type, TypeVar


class RegistryError(ValueError):
    """Base error for invalid or unsafe registry input."""


class IncompleteSnapshotError(RegistryError):
    """Raised when discovery pagination is incomplete."""


class RegistryConflictError(RegistryError):
    """Raised when the same source identity has conflicting records."""


class UnsafeCrawlError(RegistryError):
    """Raised when an unknown/conflicting classification would reach crawl."""


class UnknownCompetitionError(RegistryError):
    """Raised when a caller cannot resolve a competition unambiguously."""


class CompetitionType(str, Enum):
    DOMESTIC_LEAGUE = "domestic_league"
    DOMESTIC_CUP = "domestic_cup"
    CONTINENTAL_CLUB = "continental_club"
    NATIONAL_TEAM_TOURNAMENT = "national_team_tournament"
    UNKNOWN = "unknown"


class Gender(str, Enum):
    MEN = "men"
    WOMEN = "women"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class TeamType(str, Enum):
    CLUB = "club"
    NATIONAL_TEAM = "national_team"
    RESERVE = "reserve"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class AgeCategory(str, Enum):
    SENIOR = "senior"
    YOUTH = "youth"
    UXX = "uxx"
    UNKNOWN = "unknown"


class SeasonFormat(str, Enum):
    SPLIT_YEAR = "split_year"
    SINGLE_YEAR = "single_year"
    UNKNOWN = "unknown"


class EvidenceOrigin(str, Enum):
    STRUCTURED = "structured"
    SOURCE_PAGE = "source_page"
    NAME = "name"


class ClassificationStatus(str, Enum):
    ELIGIBLE = "eligible"
    EXCLUDED = "excluded"
    UNKNOWN = "unknown"
    CONFLICT = "conflict"


EnumT = TypeVar("EnumT", bound=Enum)


def _enum_value(enum_type: Type[EnumT], value: Any) -> EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).strip().lower())
    except (TypeError, ValueError) as exc:
        raise RegistryError(
            f"invalid {enum_type.__name__}: {value!r}"
        ) from exc


def _aware_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            result = datetime.fromisoformat(text)
        except ValueError as exc:
            raise RegistryError(f"invalid discovered_at: {value!r}") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise RegistryError("discovered_at must include a timezone")
    return result


def _optional_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise RegistryError(f"invalid date: {value!r}") from exc


def _required_text(name: str, value: Any) -> str:
    text = str(value).strip()
    if not text:
        raise RegistryError(f"{name} is required")
    return text


def _compact_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _split_year_bounds(raw: str) -> tuple[int, int]:
    """The two calendar years a split-year edition spans."""

    # The oldest leagues in the registry reach back into the 1890s.
    pair = re.fullmatch(
        r"(?P<start>(?:18|19|20|21)?\d{2})\s*[/\-]\s*"
        r"(?P<end>(?:18|19|20|21)?\d{2})",
        raw,
    )
    if pair is not None:
        start_text = pair.group("start")
        end_text = pair.group("end")
        if len(start_text) == 2:
            start_year = 2000 + int(start_text)
        else:
            start_year = int(start_text)
        if len(end_text) == 2:
            century = (start_year // 100) * 100
            end_year = century + int(end_text)
            if end_year < start_year:
                end_year += 100
        else:
            end_year = int(end_text)
        if end_year != start_year + 1:
            raise RegistryError(
                f"split-year edition must span one year: {raw!r}"
            )
        return start_year, end_year

    if re.fullmatch(r"(19|20|21)\d{2}", raw):
        start_year = int(raw)
        return start_year, start_year + 1

    raise RegistryError(f"invalid split-year edition: {raw!r}")


def canonical_season(
    edition_id_or_label: Any,
    season_format: SeasonFormat | str,
) -> str:
    """Return the canonical season without guessing its format.

    A four-digit source year becomes a split season only when
    ``season_format=split_year``.  Therefore ``2026`` is ``2026`` for a World
    Cup and ``2627`` for an explicitly split-year competition.
    """

    fmt = _enum_value(SeasonFormat, season_format)
    raw = str(edition_id_or_label).strip()
    if fmt is SeasonFormat.UNKNOWN:
        raise RegistryError("season_format=unknown cannot produce a season")

    if fmt is SeasonFormat.SINGLE_YEAR:
        match = re.fullmatch(r"(18|19|20|21)\d{2}", raw)
        if match is None:
            raise RegistryError(f"invalid single-year edition: {raw!r}")
        return raw

    start_year, end_year = _split_year_bounds(raw)
    return f"{start_year % 100:02d}{end_year % 100:02d}"


def season_window_year(
    edition_id_or_label: Any,
    season_format: SeasonFormat | str,
    canonical: Optional[str] = None,
) -> int:
    """The calendar year a season's date window opens in.

    A split-year season is named by the year it opens in, and that year is its
    saison_id.  A calendar-year season is named by the year the source prints
    on it — from which the source offsets some saison_ids — so for those the
    registered season, not the edition id, states the window.
    """

    fmt = _enum_value(SeasonFormat, season_format)
    if fmt is SeasonFormat.SINGLE_YEAR:
        return int(canonical or canonical_season(edition_id_or_label, fmt))
    start_year, _ = _split_year_bounds(str(edition_id_or_label).strip())
    return start_year


@dataclass(frozen=True)
class ClassificationEvidence:
    """One source-backed classification statement."""

    source_field: str
    source_value: str
    source_url: str
    origin: EvidenceOrigin
    competition_type: Optional[CompetitionType] = None
    gender: Optional[Gender] = None
    team_type: Optional[TeamType] = None
    age_category: Optional[AgeCategory] = None
    season_format: Optional[SeasonFormat] = None
    # How narrowly the source made this statement. The catalogue lists one
    # competition under several headings — the World Cup sits both in the broad
    # "International cup competitions" rubric and in "National Team
    # Competitions" — and the narrower statement is the one it means.
    precedence: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source_field", _required_text("source_field", self.source_field)
        )
        object.__setattr__(
            self, "source_value", _required_text("source_value", self.source_value)
        )
        object.__setattr__(
            self, "source_url", _required_text("source_url", self.source_url)
        )
        object.__setattr__(self, "origin", _enum_value(EvidenceOrigin, self.origin))
        enum_fields = (
            ("competition_type", CompetitionType),
            ("gender", Gender),
            ("team_type", TeamType),
            ("age_category", AgeCategory),
            ("season_format", SeasonFormat),
        )
        for name, enum_type in enum_fields:
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _enum_value(enum_type, value))
        if not any(getattr(self, name) is not None for name, _ in enum_fields):
            raise RegistryError("classification evidence has no signals")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ClassificationEvidence":
        signals = value.get("signals") or {}
        if not isinstance(signals, Mapping):
            raise RegistryError("evidence signals must be an object")
        return cls(
            source_field=value.get("source_field", value.get("field", "")),
            source_value=value.get("source_value", value.get("value", "")),
            source_url=value.get("source_url", ""),
            origin=value.get("origin", EvidenceOrigin.STRUCTURED.value),
            competition_type=signals.get("competition_type"),
            gender=signals.get("gender"),
            team_type=signals.get("team_type"),
            age_category=signals.get("age_category"),
            season_format=signals.get("season_format"),
            precedence=int(value.get("precedence", 0)),
        )

    def as_dict(self) -> dict[str, Any]:
        signals = {}
        for name in (
            "competition_type",
            "gender",
            "team_type",
            "age_category",
            "season_format",
        ):
            value = getattr(self, name)
            if value is not None:
                signals[name] = value.value
        return {
            "origin": self.origin.value,
            "precedence": self.precedence,
            "signals": signals,
            "source_field": self.source_field,
            "source_url": self.source_url,
            "source_value": self.source_value,
        }


_DIMENSIONS: tuple[tuple[str, Type[Enum]], ...] = (
    ("competition_type", CompetitionType),
    ("gender", Gender),
    ("team_type", TeamType),
    ("age_category", AgeCategory),
    ("season_format", SeasonFormat),
)


def narrowest_signals(
    evidence: Iterable[ClassificationEvidence],
    dimension: str,
) -> set[Any]:
    """The values stated at the source's narrowest level for one dimension.

    A broad rubric that also brackets the competition does not contradict the
    narrow statement; two statements at the same level still do.
    """
    stated = [item for item in evidence if getattr(item, dimension) is not None]
    if not stated:
        return set()
    narrowest = max(item.precedence for item in stated)
    return {
        getattr(item, dimension)
        for item in stated
        if item.precedence == narrowest
    }


@dataclass(frozen=True)
class CompetitionRecord:
    """A discovered competition with evidence-derived crawl eligibility."""

    competition_id: str
    slug: str
    name: str
    country: str
    confederation: str
    competition_type: CompetitionType
    gender: Gender
    team_type: TeamType
    age_category: AgeCategory
    season_format: SeasonFormat
    active: bool
    source_url: str
    discovered_at: datetime
    canonical_competition_id: Optional[str] = None
    evidence: tuple[ClassificationEvidence, ...] = field(default_factory=tuple)
    registry_snapshot_id: str = ""
    source_body_hash: str = ""
    parser_revision: str = "registry-v1"
    schema_revision: str = "1"
    aliases: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        for name in (
            "competition_id",
            "slug",
            "name",
            "country",
            "confederation",
            "source_url",
        ):
            object.__setattr__(self, name, _required_text(name, getattr(self, name)))
        for name, enum_type in _DIMENSIONS:
            object.__setattr__(self, name, _enum_value(enum_type, getattr(self, name)))
        object.__setattr__(self, "active", bool(self.active))
        object.__setattr__(self, "discovered_at", _aware_datetime(self.discovered_at))
        evidence = tuple(self.evidence)
        if not all(isinstance(item, ClassificationEvidence) for item in evidence):
            raise RegistryError("evidence must contain ClassificationEvidence")
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(
            self,
            "aliases",
            tuple(sorted({_required_text("alias", item) for item in self.aliases})),
        )
        if self.canonical_competition_id is not None:
            canonical_id = _required_text(
                "canonical_competition_id", self.canonical_competition_id
            )
            object.__setattr__(self, "canonical_competition_id", canonical_id)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        registry_snapshot_id: Optional[str] = None,
        source_body_hash: Optional[str] = None,
    ) -> "CompetitionRecord":
        raw_evidence = value.get("classification_evidence", value.get("evidence", ()))
        if isinstance(raw_evidence, str):
            try:
                raw_evidence = json.loads(raw_evidence)
            except json.JSONDecodeError as exc:
                raise RegistryError("invalid classification_evidence JSON") from exc
        return cls(
            competition_id=value.get("competition_id", ""),
            slug=value.get("slug", ""),
            name=value.get("name", ""),
            country=value.get("country", ""),
            confederation=value.get("confederation", ""),
            competition_type=value.get("competition_type", CompetitionType.UNKNOWN.value),
            gender=value.get("gender", Gender.UNKNOWN.value),
            team_type=value.get("team_type", TeamType.UNKNOWN.value),
            age_category=value.get("age_category", AgeCategory.UNKNOWN.value),
            season_format=value.get("season_format", SeasonFormat.UNKNOWN.value),
            active=value.get("active", False),
            source_url=value.get("source_url", ""),
            discovered_at=value.get("discovered_at", ""),
            canonical_competition_id=value.get("canonical_competition_id"),
            evidence=tuple(
                item
                if isinstance(item, ClassificationEvidence)
                else ClassificationEvidence.from_mapping(item)
                for item in raw_evidence
            ),
            registry_snapshot_id=(
                registry_snapshot_id
                if registry_snapshot_id is not None
                else value.get("registry_snapshot_id", "")
            ),
            source_body_hash=(
                source_body_hash
                if source_body_hash is not None
                else value.get("source_body_hash", "")
            ),
            parser_revision=value.get("parser_revision", "registry-v1"),
            schema_revision=value.get("schema_revision", "1"),
            aliases=tuple(value.get("aliases", ())),
        )

    def _classification(self) -> tuple[ClassificationStatus, Optional[str]]:
        # The source only marks age structurally for league competitions, under
        # its "Youth league" group; for tournaments it says so in the name and
        # nowhere else, which is why the U17 World Cup sits in the very section
        # that certifies its entrants as national teams. A name may therefore
        # exclude a competition from the crawl — it can never admit one.
        named = [
            item for item in self.evidence if item.origin is EvidenceOrigin.NAME
        ]
        name_exclusions = [
            f"{dimension}={getattr(item, dimension).value} (name)"
            for item in named
            for dimension, excluded in (
                ("gender", {Gender.WOMEN, Gender.MIXED}),
                ("age_category", {AgeCategory.YOUTH, AgeCategory.UXX}),
                ("team_type", {TeamType.RESERVE, TeamType.MIXED}),
            )
            if getattr(item, dimension) in excluded
        ]
        if name_exclusions:
            return (
                ClassificationStatus.EXCLUDED,
                "; ".join(sorted(set(name_exclusions))),
            )

        conflicts: list[str] = []
        missing: list[str] = []
        for name, enum_type in _DIMENSIONS:
            declared = getattr(self, name)
            stated = [
                item
                for item in self.evidence
                if item.origin is not EvidenceOrigin.NAME
                and getattr(item, name) is not None
                and getattr(item, name) != enum_type("unknown")
            ]
            signals = narrowest_signals(stated, name)
            if len(signals) > 1 or (signals and declared not in signals):
                conflicts.append(name)
            elif declared == enum_type("unknown") or not signals:
                missing.append(name)

        club_types = {
            CompetitionType.DOMESTIC_LEAGUE,
            CompetitionType.DOMESTIC_CUP,
            CompetitionType.CONTINENTAL_CLUB,
        }
        if (
            self.competition_type in club_types
            and self.team_type is TeamType.NATIONAL_TEAM
        ) or (
            self.competition_type is CompetitionType.NATIONAL_TEAM_TOURNAMENT
            and self.team_type is TeamType.CLUB
        ):
            conflicts.append("competition_type/team_type")

        if conflicts:
            return (
                ClassificationStatus.CONFLICT,
                "conflicting source evidence: " + ", ".join(sorted(set(conflicts))),
            )

        exclusions = []
        if self.gender in {Gender.WOMEN, Gender.MIXED}:
            exclusions.append(f"gender={self.gender.value}")
        if self.age_category in {AgeCategory.YOUTH, AgeCategory.UXX}:
            exclusions.append(f"age_category={self.age_category.value}")
        if self.team_type in {TeamType.RESERVE, TeamType.MIXED}:
            exclusions.append(f"team_type={self.team_type.value}")
        if exclusions:
            return ClassificationStatus.EXCLUDED, "; ".join(exclusions)

        if missing:
            return (
                ClassificationStatus.UNKNOWN,
                "missing non-name source evidence: "
                + ", ".join(sorted(set(missing))),
            )

        if self.gender is not Gender.MEN or self.age_category is not AgeCategory.SENIOR:
            return ClassificationStatus.UNKNOWN, "not proven senior men's competition"
        if self.competition_type in club_types and self.team_type is not TeamType.CLUB:
            return ClassificationStatus.CONFLICT, "club competition has non-club teams"
        if (
            self.competition_type is CompetitionType.NATIONAL_TEAM_TOURNAMENT
            and self.team_type is not TeamType.NATIONAL_TEAM
        ):
            return ClassificationStatus.CONFLICT, "national tournament has non-national teams"
        return ClassificationStatus.ELIGIBLE, None

    @property
    def classification_status(self) -> ClassificationStatus:
        return self._classification()[0]

    @property
    def crawl_block_reason(self) -> Optional[str]:
        if not self.active:
            return "competition is inactive"
        return self._classification()[1]

    @property
    def crawl_eligible(self) -> bool:
        return self.active and self.classification_status is ClassificationStatus.ELIGIBLE

    def as_dict(self) -> dict[str, Any]:
        """Return the exact Bronze registry row contract."""

        return {
            "competition_id": self.competition_id,
            "slug": self.slug,
            "name": self.name,
            "country": self.country,
            "confederation": self.confederation,
            "competition_type": self.competition_type.value,
            "gender": self.gender.value,
            "team_type": self.team_type.value,
            "age_category": self.age_category.value,
            "season_format": self.season_format.value,
            "active": self.active,
            "source_url": self.source_url,
            "discovered_at": self.discovered_at.isoformat(),
            "canonical_competition_id": self.canonical_competition_id,
            "classification_status": self.classification_status.value,
            "classification_evidence": _compact_json(
                [item.as_dict() for item in self.evidence]
            ),
            "registry_snapshot_id": self.registry_snapshot_id,
            "source_body_hash": self.source_body_hash,
            "parser_revision": self.parser_revision,
            "schema_revision": self.schema_revision,
        }


@dataclass(frozen=True)
class EditionRecord:
    """A source edition with explicit split/single-year semantics."""

    competition_id: str
    edition_id: str
    edition_label: str
    canonical_season: str
    season_format: SeasonFormat
    start_date: Optional[date]
    end_date: Optional[date]
    active: bool
    current: bool
    participant_count: Optional[int]
    participant_hash: Optional[str]
    source_url: str
    discovered_at: datetime
    registry_snapshot_id: str = ""
    source_body_hash: str = ""
    parser_revision: str = "registry-v1"
    schema_revision: str = "1"

    def __post_init__(self) -> None:
        for name in ("competition_id", "edition_id", "edition_label", "source_url"):
            object.__setattr__(self, name, _required_text(name, getattr(self, name)))
        object.__setattr__(
            self, "season_format", _enum_value(SeasonFormat, self.season_format)
        )
        if self.season_format is SeasonFormat.UNKNOWN:
            raise RegistryError("edition season_format cannot be unknown")
        expected = canonical_season(self.edition_label, self.season_format)
        if str(self.canonical_season) != expected:
            raise RegistryConflictError(
                "canonical season mismatch for "
                f"{self.competition_id}/{self.edition_id}: "
                f"{self.canonical_season!r} != {expected!r}"
            )
        object.__setattr__(self, "canonical_season", expected)
        object.__setattr__(self, "start_date", _optional_date(self.start_date))
        object.__setattr__(self, "end_date", _optional_date(self.end_date))
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise RegistryError("edition end_date precedes start_date")
        if self.participant_count is not None:
            count = int(self.participant_count)
            if count < 0:
                raise RegistryError("participant_count cannot be negative")
            object.__setattr__(self, "participant_count", count)
        if self.participant_hash is not None:
            object.__setattr__(
                self,
                "participant_hash",
                _required_text("participant_hash", self.participant_hash),
            )
        object.__setattr__(self, "active", bool(self.active))
        object.__setattr__(self, "current", bool(self.current))
        object.__setattr__(self, "discovered_at", _aware_datetime(self.discovered_at))

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        registry_snapshot_id: Optional[str] = None,
        source_body_hash: Optional[str] = None,
    ) -> "EditionRecord":
        season_format = _enum_value(
            SeasonFormat, value.get("season_format", SeasonFormat.UNKNOWN.value)
        )
        label = value.get("edition_label", value.get("label", value.get("edition_id", "")))
        computed = canonical_season(label, season_format)
        supplied = value.get("canonical_season", computed)
        return cls(
            competition_id=value.get("competition_id", ""),
            edition_id=str(value.get("edition_id", "")),
            edition_label=str(label),
            canonical_season=str(supplied),
            season_format=season_format,
            start_date=value.get("start_date"),
            end_date=value.get("end_date"),
            active=value.get("active", False),
            current=value.get("current", False),
            participant_count=value.get("participant_count"),
            participant_hash=value.get("participant_hash"),
            source_url=value.get("source_url", ""),
            discovered_at=value.get("discovered_at", ""),
            registry_snapshot_id=(
                registry_snapshot_id
                if registry_snapshot_id is not None
                else value.get("registry_snapshot_id", "")
            ),
            source_body_hash=(
                source_body_hash
                if source_body_hash is not None
                else value.get("source_body_hash", "")
            ),
            parser_revision=value.get("parser_revision", "registry-v1"),
            schema_revision=value.get("schema_revision", "1"),
        )

    def as_dict(self) -> dict[str, Any]:
        """Return the exact Bronze edition row contract."""

        return {
            "competition_id": self.competition_id,
            "edition_id": self.edition_id,
            "edition_label": self.edition_label,
            "canonical_season": self.canonical_season,
            "season_format": self.season_format.value,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "active": self.active,
            "current": self.current,
            "participant_count": self.participant_count,
            "participant_hash": self.participant_hash,
            "source_url": self.source_url,
            "discovered_at": self.discovered_at.isoformat(),
            "registry_snapshot_id": self.registry_snapshot_id,
            "source_body_hash": self.source_body_hash,
            "parser_revision": self.parser_revision,
            "schema_revision": self.schema_revision,
        }


def deterministic_scope_id(competition_id: str, edition_id: str) -> str:
    identity = _compact_json(
        {
            "competition_id": _required_text("competition_id", competition_id),
            "edition_id": _required_text("edition_id", edition_id),
        }
    )
    return "tm-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class CrawlScope:
    scope_id: str
    competition_id: str
    edition_id: str
    canonical_season: str
    competition_type: CompetitionType
    source_url: str
    registry_snapshot_id: str

    @classmethod
    def from_records(
        cls,
        competition: CompetitionRecord,
        edition: EditionRecord,
    ) -> "CrawlScope":
        if competition.competition_id != edition.competition_id:
            raise RegistryConflictError("edition belongs to another competition")
        if not competition.crawl_eligible:
            raise UnsafeCrawlError(
                f"{competition.competition_id} is not crawlable: "
                f"{competition.crawl_block_reason}"
            )
        if not edition.active:
            raise UnsafeCrawlError(
                f"{competition.competition_id}/{edition.edition_id} is inactive"
            )
        # A competition's format is the one its current edition runs on, and
        # older editions legitimately keep theirs (Australia played 1977 as a
        # calendar year); each edition's canonical_season already carries its
        # own format, so the two need not agree.
        if (
            competition.registry_snapshot_id
            and edition.registry_snapshot_id
            and competition.registry_snapshot_id != edition.registry_snapshot_id
        ):
            raise RegistryConflictError("competition/edition snapshot mismatch")
        snapshot_id = edition.registry_snapshot_id or competition.registry_snapshot_id
        return cls(
            scope_id=deterministic_scope_id(
                competition.competition_id, edition.edition_id
            ),
            competition_id=competition.competition_id,
            edition_id=edition.edition_id,
            canonical_season=edition.canonical_season,
            competition_type=competition.competition_type,
            source_url=edition.source_url,
            registry_snapshot_id=snapshot_id,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "scope_id": self.scope_id,
            "competition_id": self.competition_id,
            "edition_id": self.edition_id,
            "canonical_season": self.canonical_season,
            "competition_type": self.competition_type.value,
            "source_url": self.source_url,
            "registry_snapshot_id": self.registry_snapshot_id,
        }


@dataclass(frozen=True)
class RegistryPage:
    snapshot_id: str
    page_number: int
    page_count: int
    source_url: str
    source_body_hash: str
    competitions: tuple[CompetitionRecord, ...]
    editions: tuple[EditionRecord, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "snapshot_id", _required_text("snapshot_id", self.snapshot_id))
        object.__setattr__(self, "source_url", _required_text("source_url", self.source_url))
        object.__setattr__(
            self,
            "source_body_hash",
            _required_text("source_body_hash", self.source_body_hash),
        )
        if self.page_count < 1 or not 1 <= self.page_number <= self.page_count:
            raise RegistryError("invalid registry pagination")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RegistryPage":
        snapshot_id = _required_text("snapshot_id", value.get("snapshot_id", ""))
        body_hash = _required_text(
            "source_body_hash", value.get("source_body_hash", "")
        )
        return cls(
            snapshot_id=snapshot_id,
            page_number=int(value.get("page_number", 0)),
            page_count=int(value.get("page_count", 0)),
            source_url=value.get("source_url", ""),
            source_body_hash=body_hash,
            competitions=tuple(
                CompetitionRecord.from_mapping(
                    item,
                    registry_snapshot_id=snapshot_id,
                    source_body_hash=body_hash,
                )
                for item in value.get("competitions", ())
            ),
            editions=tuple(
                EditionRecord.from_mapping(
                    item,
                    registry_snapshot_id=snapshot_id,
                    source_body_hash=body_hash,
                )
                for item in value.get("editions", ())
            ),
        )


@dataclass(frozen=True)
class RegistrySnapshot:
    snapshot_id: str
    page_count: int
    source_body_hashes: tuple[str, ...]
    competitions: tuple[CompetitionRecord, ...]
    editions: tuple[EditionRecord, ...]
    snapshot_hash: str

    @property
    def blocked_competition_ids(self) -> tuple[str, ...]:
        return tuple(
            record.competition_id
            for record in self.competitions
            if record.active
            and record.classification_status
            in {ClassificationStatus.UNKNOWN, ClassificationStatus.CONFLICT}
        )

    @property
    def promotable(self) -> bool:
        return not self.blocked_competition_ids

    def crawl_scopes(self, *, strict: bool = True) -> tuple[CrawlScope, ...]:
        blocked = self.blocked_competition_ids
        if strict and blocked:
            raise UnsafeCrawlError(
                "active competitions require classification: " + ", ".join(blocked)
            )
        competitions = {
            item.competition_id: item
            for item in self.competitions
            if item.crawl_eligible
        }
        scopes = [
            CrawlScope.from_records(competitions[item.competition_id], item)
            for item in self.editions
            if item.active and item.competition_id in competitions
        ]
        return tuple(sorted(scopes, key=lambda item: (item.competition_id, item.edition_id)))


def _insert_unique(
    target: dict[Any, Any],
    key: Any,
    value: Any,
    label: str,
) -> None:
    previous = target.get(key)
    if previous is not None and previous != value:
        raise RegistryConflictError(f"conflicting {label}: {key!r}")
    target[key] = value


def reconcile_registry_pages(
    pages: Iterable[RegistryPage],
    *,
    expected_page_count: Optional[int] = None,
    expected_competition_ids: Optional[Iterable[str]] = None,
) -> RegistrySnapshot:
    """Reconcile a complete discovery snapshot or fail without partial output."""

    materialized = tuple(pages)
    if not materialized:
        raise IncompleteSnapshotError("registry snapshot has no pages")
    snapshot_ids = {item.snapshot_id for item in materialized}
    page_counts = {item.page_count for item in materialized}
    if len(snapshot_ids) != 1:
        raise RegistryConflictError("registry pages use different snapshot IDs")
    if len(page_counts) != 1:
        raise RegistryConflictError("registry pages disagree on page_count")
    declared_count = next(iter(page_counts))
    if expected_page_count is not None and declared_count != expected_page_count:
        raise IncompleteSnapshotError(
            f"page_count mismatch: source={declared_count}, expected={expected_page_count}"
        )

    by_page: dict[int, RegistryPage] = {}
    for page in materialized:
        _insert_unique(by_page, page.page_number, page, "registry page")
    expected_pages = set(range(1, declared_count + 1))
    actual_pages = set(by_page)
    if actual_pages != expected_pages:
        missing = sorted(expected_pages - actual_pages)
        extra = sorted(actual_pages - expected_pages)
        raise IncompleteSnapshotError(
            f"incomplete pagination: missing={missing}, extra={extra}"
        )

    competitions: dict[str, CompetitionRecord] = {}
    editions: dict[tuple[str, str], EditionRecord] = {}
    for page_number in sorted(by_page):
        page = by_page[page_number]
        for record in page.competitions:
            _insert_unique(
                competitions, record.competition_id, record, "competition"
            )
        for record in page.editions:
            _insert_unique(
                editions,
                (record.competition_id, record.edition_id),
                record,
                "edition",
            )

    missing_parents = sorted(
        {record.competition_id for record in editions.values()} - set(competitions)
    )
    if missing_parents:
        raise RegistryConflictError(
            "editions reference undiscovered competitions: "
            + ", ".join(missing_parents)
        )
    if expected_competition_ids is not None:
        expected_ids = {_required_text("competition_id", item) for item in expected_competition_ids}
        actual_ids = set(competitions)
        if actual_ids != expected_ids:
            raise IncompleteSnapshotError(
                "competition inventory mismatch: "
                f"missing={sorted(expected_ids - actual_ids)}, "
                f"unexpected={sorted(actual_ids - expected_ids)}"
            )

    competition_rows = tuple(competitions[key] for key in sorted(competitions))
    edition_rows = tuple(editions[key] for key in sorted(editions))
    page_hashes = tuple(by_page[number].source_body_hash for number in sorted(by_page))
    digest_payload = {
        "competitions": [item.as_dict() for item in competition_rows],
        "editions": [item.as_dict() for item in edition_rows],
        "page_hashes": page_hashes,
        "snapshot_id": next(iter(snapshot_ids)),
    }
    snapshot_hash = hashlib.sha256(
        _compact_json(digest_payload).encode("utf-8")
    ).hexdigest()
    return RegistrySnapshot(
        snapshot_id=next(iter(snapshot_ids)),
        page_count=declared_count,
        source_body_hashes=page_hashes,
        competitions=competition_rows,
        editions=edition_rows,
        snapshot_hash=snapshot_hash,
    )


def _bootstrap_evidence(
    *,
    source_url: str,
    competition_type: CompetitionType,
    gender: Gender,
    team_type: TeamType,
    age_category: AgeCategory,
    season_format: SeasonFormat,
) -> tuple[ClassificationEvidence, ...]:
    return (
        ClassificationEvidence(
            source_field="competition_context",
            source_value=competition_type.value,
            source_url=source_url,
            origin=EvidenceOrigin.SOURCE_PAGE,
            competition_type=competition_type,
            team_type=team_type,
        ),
        ClassificationEvidence(
            source_field="competition_audience",
            source_value=f"{gender.value}:{age_category.value}",
            source_url=source_url,
            origin=EvidenceOrigin.STRUCTURED,
            gender=gender,
            age_category=age_category,
        ),
        ClassificationEvidence(
            source_field="season_selector",
            source_value=season_format.value,
            source_url=source_url,
            origin=EvidenceOrigin.STRUCTURED,
            season_format=season_format,
        ),
    )


def _bootstrap_record(
    *,
    competition_id: str,
    slug: str,
    name: str,
    country: str,
    confederation: str,
    competition_type: CompetitionType,
    team_type: TeamType,
    season_format: SeasonFormat,
    source_url: str,
    canonical_competition_id: Optional[str],
    aliases: Sequence[str] = (),
) -> CompetitionRecord:
    body_hash = hashlib.sha256(source_url.encode("utf-8")).hexdigest()
    return CompetitionRecord(
        competition_id=competition_id,
        slug=slug,
        name=name,
        country=country,
        confederation=confederation,
        competition_type=competition_type,
        gender=Gender.MEN,
        team_type=team_type,
        age_category=AgeCategory.SENIOR,
        season_format=season_format,
        active=True,
        source_url=source_url,
        discovered_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
        canonical_competition_id=canonical_competition_id,
        evidence=_bootstrap_evidence(
            source_url=source_url,
            competition_type=competition_type,
            gender=Gender.MEN,
            team_type=team_type,
            age_category=AgeCategory.SENIOR,
            season_format=season_format,
        ),
        registry_snapshot_id="bootstrap-fixtures-v1",
        source_body_hash=body_hash,
        aliases=tuple(aliases),
    )


# This is an offline compatibility seed, not a production crawl list.  Runtime
# callers pass records from a reconciled discovery snapshot to
# ``resolve_competition``.  Only the five source-verified regression fixtures
# are intentionally present here.
BOOTSTRAP_COMPETITIONS: tuple[CompetitionRecord, ...] = (
    _bootstrap_record(
        competition_id="GB1",
        slug="premier-league",
        name="Premier League",
        country="England",
        confederation="UEFA",
        competition_type=CompetitionType.DOMESTIC_LEAGUE,
        team_type=TeamType.CLUB,
        season_format=SeasonFormat.SPLIT_YEAR,
        source_url="https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1",
        canonical_competition_id="ENG-Premier League",
        aliases=("English Premier League",),
    ),
    _bootstrap_record(
        competition_id="CL",
        slug="uefa-champions-league",
        name="UEFA Champions League",
        country="Europe",
        confederation="UEFA",
        competition_type=CompetitionType.CONTINENTAL_CLUB,
        team_type=TeamType.CLUB,
        season_format=SeasonFormat.SPLIT_YEAR,
        source_url="https://www.transfermarkt.com/uefa-champions-league/startseite/pokalwettbewerb/CL",
        canonical_competition_id="UEFA-Champions League",
    ),
    _bootstrap_record(
        competition_id="AFCN",
        slug="afrika-cup",
        name="Africa Cup of Nations",
        country="Africa",
        confederation="CAF",
        competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
        team_type=TeamType.NATIONAL_TEAM,
        season_format=SeasonFormat.SINGLE_YEAR,
        source_url="https://www.transfermarkt.com/afrika-cup/startseite/pokalwettbewerb/AFCN",
        canonical_competition_id=None,
    ),
    _bootstrap_record(
        competition_id="UNLA",
        slug="uefa-nations-league-a",
        name="UEFA Nations League A",
        country="Europe",
        confederation="UEFA",
        competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
        team_type=TeamType.NATIONAL_TEAM,
        season_format=SeasonFormat.SPLIT_YEAR,
        source_url="https://www.transfermarkt.com/uefa-nations-league-a/startseite/pokalwettbewerb/UNLA",
        canonical_competition_id=None,
    ),
    _bootstrap_record(
        competition_id="FIWC",
        slug="world-cup",
        name="FIFA World Cup",
        country="World",
        confederation="FIFA",
        competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
        team_type=TeamType.NATIONAL_TEAM,
        season_format=SeasonFormat.SINGLE_YEAR,
        source_url="https://www.transfermarkt.com/world-cup/startseite/wettbewerb/FIWC",
        canonical_competition_id="INT-World Cup",
    ),
)


def _lookup_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).casefold())


def resolve_competition(
    value: str,
    records: Optional[Iterable[CompetitionRecord]] = None,
) -> CompetitionRecord:
    """Resolve a source/canonical alias without consulting a static Top-5 map."""

    query = _lookup_key(_required_text("competition", value))
    candidates: dict[str, CompetitionRecord] = {}
    for record in BOOTSTRAP_COMPETITIONS if records is None else tuple(records):
        identities = (
            record.competition_id,
            f"TM-{record.competition_id}",
            record.slug,
            record.name,
            record.canonical_competition_id,
            *record.aliases,
        )
        if query in {_lookup_key(item) for item in identities if item}:
            candidates[record.competition_id] = record
    if not candidates:
        raise UnknownCompetitionError(f"unknown competition: {value!r}")
    if len(candidates) > 1:
        raise UnknownCompetitionError(
            f"ambiguous competition {value!r}: {sorted(candidates)}"
        )
    return next(iter(candidates.values()))


__all__ = [
    "AgeCategory",
    "BOOTSTRAP_COMPETITIONS",
    "ClassificationEvidence",
    "ClassificationStatus",
    "CompetitionRecord",
    "CompetitionType",
    "CrawlScope",
    "EditionRecord",
    "EvidenceOrigin",
    "Gender",
    "IncompleteSnapshotError",
    "RegistryConflictError",
    "RegistryError",
    "RegistryPage",
    "RegistrySnapshot",
    "SeasonFormat",
    "TeamType",
    "UnknownCompetitionError",
    "UnsafeCrawlError",
    "canonical_season",
    "deterministic_scope_id",
    "reconcile_registry_pages",
    "resolve_competition",
]
