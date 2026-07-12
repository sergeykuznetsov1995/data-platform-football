"""Strict read-only access to the versioned SofaScore tournament registry."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional

from scrapers.sofascore.registry import (
    SCHEMA_VERSION,
    SUPPORTED_SCHEMA_VERSIONS,
    ActivationEligibility,
    activation_eligibility,
)

DEFAULT_REGISTRY_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "sofascore"
    / "tournaments.json"
)


class CatalogError(ValueError):
    """The registry is missing, malformed, or internally ambiguous."""


@dataclass(frozen=True)
class CatalogSeason:
    season_id: int
    name: str
    source_name: str
    year: str
    format: str
    season_format: str
    source_canonical_season: Optional[str]
    canonical_season: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    aliases: tuple[str, ...]

    @property
    def activatable(self) -> bool:
        return (
            self.format in {"split_year", "calendar_year", "named"}
            and self.canonical_season is not None
        )


@dataclass(frozen=True)
class CatalogTournament:
    unique_tournament_id: int
    name: str
    slug: str
    category_id: Optional[int]
    category_name: str
    category_slug: str
    sport_slug: str
    page_path: str
    canonical_id: Optional[str]
    enabled: bool
    registry_schema_version: int
    classification: Mapping[str, Any]
    review: Mapping[str, Any]
    seasons: tuple[CatalogSeason, ...]

    @property
    def activation_eligibility(self) -> ActivationEligibility:
        return activation_eligibility({
            "canonical_id": self.canonical_id,
            "classification": self.classification,
            "review": self.review,
            "seasons": [
                {"canonical_season": season.canonical_season}
                for season in self.seasons
            ],
        })

    @property
    def capture_allowed(self) -> bool:
        return self.enabled and self.activation_eligibility.allowed


def registry_path() -> Path:
    """Resolve the registry from the explicit runtime path or the repo."""

    configured = os.environ.get("SOFASCORE_REGISTRY_PATH", "").strip()
    return Path(configured) if configured else DEFAULT_REGISTRY_PATH


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CatalogError(f"{field} must be a non-empty string")
    return value.strip()


def _optional_string(value: Any, field: str) -> Optional[str]:
    if value is None:
        return None
    return _required_string(value, field)


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CatalogError(f"{field} must be a positive integer")
    return value


def _optional_positive_int(value: Any, field: str) -> Optional[int]:
    if value is None:
        return None
    return _positive_int(value, field)


def _optional_date(value: Any, field: str) -> Optional[str]:
    token = _optional_string(value, field)
    if token is None:
        return None
    try:
        date.fromisoformat(token)
    except ValueError as exc:
        raise CatalogError(f"{field} must be an ISO-8601 date") from exc
    return token


def _validate_classification(value: Mapping[str, Any], prefix: str) -> None:
    allowed = {
        "gender": {"male", "female", "mixed", "unknown"},
        "age_group": {"adult", "youth", "unknown"},
        "team_level": {"first_team", "reserve", "unknown"},
        "status": {
            "source_confirmed_adult_men",
            "review_required",
            "unknown",
            "excluded",
        },
    }
    _required_string(
        value.get("sport"), f"{prefix}.classification.sport"
    )
    for field, choices in allowed.items():
        token = value.get(field)
        if token not in choices:
            raise CatalogError(
                f"{prefix}.classification.{field} must be one of "
                f"{sorted(choices)}"
            )
    exclusions = value.get("exclusion_reasons")
    evidence = value.get("evidence")
    if not isinstance(exclusions, list) or not all(
        isinstance(item, str) and item.strip() for item in exclusions
    ):
        raise CatalogError(
            f"{prefix}.classification.exclusion_reasons must be a string list"
        )
    if not isinstance(evidence, list) or not evidence:
        raise CatalogError(
            f"{prefix}.classification.evidence must be a non-empty list"
        )
    if not all(isinstance(item, Mapping) for item in evidence):
        raise CatalogError(
            f"{prefix}.classification.evidence entries must be objects"
        )
    if any(
        not isinstance(item.get("type"), str)
        or not item["type"].strip()
        or not isinstance(item.get("endpoint"), str)
        or not item["endpoint"].strip()
        for item in evidence
    ):
        raise CatalogError(
            f"{prefix}.classification.evidence must identify type and endpoint"
        )


def _validate_review(value: Mapping[str, Any], prefix: str) -> None:
    status = value.get("status")
    if status not in {"pending", "approved", "rejected"}:
        raise CatalogError(
            f"{prefix}.review.status must be pending, approved, or rejected"
        )
    confirmed = value.get("confirmed")
    if not isinstance(confirmed, Mapping):
        raise CatalogError(f"{prefix}.review.confirmed must be an object")
    evidence = value.get("evidence")
    if not isinstance(evidence, list) or not all(
        isinstance(item, Mapping) for item in evidence
    ):
        raise CatalogError(f"{prefix}.review.evidence must be an object list")
    if status == "approved":
        expected = {
            "sport": "football",
            "gender": "male",
            "age_group": "adult",
            "team_level": "first_team",
        }
        if any(confirmed.get(key) != val for key, val in expected.items()):
            raise CatalogError(
                f"{prefix}.review approved confirmation must be adult men's "
                "first-team football"
            )
        meaningful_evidence = any(
            any(
                isinstance(item.get(field), str) and item[field].strip()
                for field in ("reference", "url", "note", "value")
            )
            for item in evidence
        )
        if not meaningful_evidence:
            raise CatalogError(
                f"{prefix}.review approved evidence must not be empty"
            )
        _required_string(
            value.get("reviewed_by"), f"{prefix}.review.reviewed_by"
        )
        _required_string(
            value.get("reviewed_at"), f"{prefix}.review.reviewed_at"
        )


class SofaScoreCatalog:
    """Validated projection of ``configs/sofascore/tournaments.json``."""

    def __init__(self, tournaments: tuple[CatalogTournament, ...]) -> None:
        by_source_id: dict[int, CatalogTournament] = {}
        by_canonical_id: dict[str, CatalogTournament] = {}
        for tournament in tournaments:
            source_id = tournament.unique_tournament_id
            if source_id in by_source_id:
                raise CatalogError(
                    f"duplicate unique_tournament_id {source_id}"
                )
            by_source_id[source_id] = tournament
            canonical_id = tournament.canonical_id
            if canonical_id is not None:
                if canonical_id in by_canonical_id:
                    raise CatalogError(
                        f"duplicate canonical_id {canonical_id!r}"
                    )
                by_canonical_id[canonical_id] = tournament
            if tournament.enabled and canonical_id is None:
                raise CatalogError(
                    f"tournament {source_id} is enabled without canonical_id"
                )
        self._tournaments = tuple(tournaments)
        self._by_source_id = by_source_id
        self._by_canonical_id = by_canonical_id

    @classmethod
    def load(cls, path: str | Path | None = None) -> "SofaScoreCatalog":
        return cls.from_file(path or registry_path())

    @classmethod
    def from_file(cls, path: str | Path) -> "SofaScoreCatalog":
        source = Path(path)
        try:
            with source.open("r", encoding="utf-8") as handle:
                document = json.load(handle)
        except FileNotFoundError as exc:
            raise CatalogError(f"registry not found: {source}") from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise CatalogError(f"cannot read registry {source}: {exc}") from exc
        return cls.from_mapping(document)

    @classmethod
    def from_mapping(cls, document: Mapping[str, Any]) -> "SofaScoreCatalog":
        if not isinstance(document, Mapping):
            raise CatalogError("registry root must be an object")
        schema_version = document.get("schema_version")
        if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
            raise CatalogError(
                "schema_version must be one of "
                f"{sorted(SUPPORTED_SCHEMA_VERSIONS)}"
            )
        raw_tournaments = document.get("tournaments")
        if not isinstance(raw_tournaments, list):
            raise CatalogError("registry must contain a tournaments list")

        tournaments: list[CatalogTournament] = []
        for index, raw in enumerate(raw_tournaments):
            if not isinstance(raw, Mapping):
                raise CatalogError(f"tournaments[{index}] must be an object")
            prefix = f"tournaments[{index}]"
            source_id = _positive_int(
                raw.get("unique_tournament_id"),
                f"{prefix}.unique_tournament_id",
            )
            canonical_id = _optional_string(
                raw.get("canonical_id"), f"{prefix}.canonical_id"
            )
            enabled = raw.get("enabled")
            if not isinstance(enabled, bool):
                raise CatalogError(f"{prefix}.enabled must be boolean")

            raw_classification = raw.get("classification")
            raw_review = raw.get("review")
            if schema_version == SCHEMA_VERSION:
                if not isinstance(raw_classification, Mapping):
                    raise CatalogError(
                        f"{prefix}.classification must be an object"
                    )
                if not isinstance(raw_review, Mapping):
                    raise CatalogError(f"{prefix}.review must be an object")
                classification = dict(raw_classification)
                review = dict(raw_review)
                _validate_classification(classification, prefix)
                _validate_review(review, prefix)
            else:
                # Schema v1 remains readable for rollback/replay, but it has no
                # evidence and is therefore never production-capture eligible.
                classification = {}
                review = {}

            raw_category = raw.get("category")
            if not isinstance(raw_category, Mapping):
                raise CatalogError(f"{prefix}.category must be an object")
            category_slug = _required_string(
                raw_category.get("slug"), f"{prefix}.category.slug"
            )
            sport_slug = _required_string(
                raw.get("sport_slug"), f"{prefix}.sport_slug"
            )
            slug = _required_string(raw.get("slug"), f"{prefix}.slug")
            page_path = _required_string(
                raw.get("page_path"), f"{prefix}.page_path"
            )
            expected_page_path = f"{sport_slug}/{category_slug}/{slug}"
            if page_path != expected_page_path:
                raise CatalogError(
                    f"{prefix}.page_path must equal {expected_page_path!r}"
                )

            raw_seasons = raw.get("seasons")
            if not isinstance(raw_seasons, list):
                raise CatalogError(f"{prefix}.seasons must be a list")
            seasons: list[CatalogSeason] = []
            seen_season_ids: set[int] = set()
            seen_years: dict[str, int] = {}
            seen_canonical_seasons: dict[str, tuple[int, str, str]] = {}
            seen_resolution_tokens: dict[str, int] = {}
            for season_index, raw_season in enumerate(raw_seasons):
                season_prefix = f"{prefix}.seasons[{season_index}]"
                if not isinstance(raw_season, Mapping):
                    raise CatalogError(f"{season_prefix} must be an object")
                season_id = _positive_int(
                    raw_season.get("season_id"), f"{season_prefix}.season_id"
                )
                if season_id in seen_season_ids:
                    raise CatalogError(
                        f"{prefix} has duplicate season_id {season_id}"
                    )
                seen_season_ids.add(season_id)
                year = _required_string(
                    raw_season.get("year"), f"{season_prefix}.year"
                )
                # Parallel divisions share one year label (tournament 65 ships
                # "2nd Division East 14/15" and "... West 14/15"). Both are real
                # source seasons, so loading the registry must not reject them.
                # resolve_source_season still fails closed when a token cannot
                # name exactly one season.
                seen_years.setdefault(year, season_id)
                season_format = _required_string(
                    raw_season.get("season_format"),
                    f"{season_prefix}.season_format",
                )
                if season_format not in {
                    "split_year",
                    "single_year",
                    "unknown",
                }:
                    raise CatalogError(
                        f"{season_prefix}.season_format is invalid"
                    )
                raw_format = raw_season.get("format")
                if schema_version == SCHEMA_VERSION:
                    season_kind = _required_string(
                        raw_format, f"{season_prefix}.format"
                    )
                    if season_kind not in {
                        "split_year",
                        "calendar_year",
                        "named",
                        "unknown",
                    }:
                        raise CatalogError(
                            f"{season_prefix}.format is invalid"
                        )
                    expected_legacy = {
                        "split_year": "split_year",
                        "calendar_year": "single_year",
                        "named": "unknown",
                        "unknown": "unknown",
                    }[season_kind]
                    if season_format != expected_legacy:
                        raise CatalogError(
                            f"{season_prefix}.season_format must equal "
                            f"{expected_legacy!r} for format {season_kind!r}"
                        )
                else:
                    season_kind = {
                        "split_year": "split_year",
                        "single_year": "calendar_year",
                        "unknown": "unknown",
                    }[season_format]
                source_canonical_season = _optional_string(
                    raw_season.get("canonical_season"),
                    f"{season_prefix}.canonical_season",
                )
                canonical_override = _optional_string(
                    raw_season.get("canonical_season_override"),
                    f"{season_prefix}.canonical_season_override",
                )
                canonical_season = (
                    canonical_override or source_canonical_season
                )
                if season_kind == "unknown" and canonical_season is not None:
                    raise CatalogError(
                        f"{season_prefix} unknown format cannot be activatable"
                    )
                if canonical_season is not None:
                    # Two labels of the *same* season format collapsing into one
                    # canonical season is corruption and stays fail-closed. A
                    # league that migrated formats owns both "20/21" and "2021"
                    # (tournament 278), and parallel divisions share one year
                    # (tournament 65) — both are real, and an ambiguous token
                    # fails closed in resolve_source_season instead.
                    previous_canonical = seen_canonical_seasons.get(
                        canonical_season
                    )
                    if (
                        previous_canonical is not None
                        and previous_canonical[0] != season_id
                        and previous_canonical[1] == season_kind
                        and previous_canonical[2] != year
                    ):
                        raise CatalogError(
                            f"{prefix} has ambiguous canonical_season "
                            f"{canonical_season!r}"
                        )
                    seen_canonical_seasons.setdefault(
                        canonical_season, (season_id, season_kind, year)
                    )

                if schema_version == SCHEMA_VERSION:
                    source_name = _required_string(
                        raw_season.get("source_name"),
                        f"{season_prefix}.source_name",
                    )
                    start_date = _optional_date(
                        raw_season.get("start_date"),
                        f"{season_prefix}.start_date",
                    )
                    end_date = _optional_date(
                        raw_season.get("end_date"),
                        f"{season_prefix}.end_date",
                    )
                    if start_date and end_date and end_date < start_date:
                        raise CatalogError(
                            f"{season_prefix}.end_date precedes start_date"
                        )
                    raw_aliases = raw_season.get("aliases")
                    if not isinstance(raw_aliases, list):
                        raise CatalogError(
                            f"{season_prefix}.aliases must be a list"
                        )
                    aliases = tuple(
                        _required_string(alias, f"{season_prefix}.aliases")
                        for alias in raw_aliases
                    )
                    if len(set(aliases)) != len(aliases):
                        raise CatalogError(
                            f"{season_prefix}.aliases contains duplicates"
                        )
                    raw_season_evidence = raw_season.get("evidence")
                    if (
                        not isinstance(raw_season_evidence, list)
                        or not raw_season_evidence
                        or not all(
                            isinstance(item, Mapping)
                            for item in raw_season_evidence
                        )
                    ):
                        raise CatalogError(
                            f"{season_prefix}.evidence must be a non-empty "
                            "object list"
                        )
                else:
                    source_name = _required_string(
                        raw_season.get("name"), f"{season_prefix}.name"
                    )
                    start_date = None
                    end_date = None
                    aliases = tuple(dict.fromkeys(filter(None, (
                        year,
                        canonical_season,
                        source_name,
                    ))))

                resolution_tokens = set(aliases) | {year, source_name}
                resolution_tokens.add(
                    _required_string(
                        raw_season.get("name"), f"{season_prefix}.name"
                    )
                )
                if canonical_season is not None:
                    resolution_tokens.add(canonical_season)
                # Parallel divisions and format migrations make some tokens name
                # more than one real season (tournaments 65 and 278). Rejecting
                # the whole registry over that would strand every unambiguous
                # season with it; resolve_source_season already refuses to guess
                # when a token matches more than one season.
                for token in resolution_tokens:
                    seen_resolution_tokens.setdefault(token, season_id)
                seasons.append(
                    CatalogSeason(
                        season_id=season_id,
                        name=_required_string(
                            raw_season.get("name"), f"{season_prefix}.name"
                        ),
                        source_name=source_name,
                        year=year,
                        format=season_kind,
                        season_format=season_format,
                        source_canonical_season=source_canonical_season,
                        canonical_season=canonical_season,
                        start_date=start_date,
                        end_date=end_date,
                        aliases=aliases,
                    )
                )

            tournaments.append(
                CatalogTournament(
                    unique_tournament_id=source_id,
                    name=_required_string(raw.get("name"), f"{prefix}.name"),
                    slug=slug,
                    category_id=_optional_positive_int(
                        raw_category.get("id"), f"{prefix}.category.id"
                    ),
                    category_name=_required_string(
                        raw_category.get("name"), f"{prefix}.category.name"
                    ),
                    category_slug=category_slug,
                    sport_slug=sport_slug,
                    page_path=page_path,
                    canonical_id=canonical_id,
                    enabled=enabled,
                    registry_schema_version=int(schema_version),
                    classification=classification,
                    review=review,
                    seasons=tuple(seasons),
                )
            )
        return cls(tuple(tournaments))

    @property
    def tournaments(self) -> tuple[CatalogTournament, ...]:
        return self._tournaments

    def competition(self, canonical_id: str) -> CatalogTournament:
        try:
            return self._by_canonical_id[canonical_id]
        except KeyError as exc:
            raise CatalogError(
                f"unknown canonical competition {canonical_id!r}"
            ) from exc

    def tournament(self, unique_tournament_id: int) -> CatalogTournament:
        try:
            return self._by_source_id[int(unique_tournament_id)]
        except (KeyError, TypeError, ValueError) as exc:
            raise CatalogError(
                f"unknown unique_tournament_id {unique_tournament_id!r}"
            ) from exc

    def enabled_competition_ids(self) -> tuple[str, ...]:
        return tuple(sorted(
            tournament.canonical_id
            for tournament in self._tournaments
            if tournament.capture_allowed
            and tournament.canonical_id is not None
        ))

    def tournament_map(self, *, enabled_only: bool = False) -> dict[str, int]:
        return {
            tournament.canonical_id: tournament.unique_tournament_id
            for tournament in self._tournaments
            if tournament.canonical_id is not None
            and (tournament.capture_allowed or not enabled_only)
        }

    def slug_map(self, *, enabled_only: bool = False) -> dict[str, str]:
        return {
            tournament.canonical_id: tournament.page_path
            for tournament in self._tournaments
            if tournament.canonical_id is not None
            and (tournament.capture_allowed or not enabled_only)
        }

    def resolve_source_season(
        self,
        unique_tournament_id: int,
        season: str | int,
    ) -> Optional[CatalogSeason]:
        try:
            tournament = self.tournament(unique_tournament_id)
        except CatalogError:
            return None
        token = str(season).strip()
        matches = [
            item
            for item in tournament.seasons
            if (
                item.year == token
                or item.name == token
                or item.source_name == token
                or item.canonical_season == token
                or token in item.aliases
            )
        ]
        if not matches:
            return None
        season_ids = {item.season_id for item in matches}
        if len(season_ids) != 1:
            raise CatalogError(
                f"ambiguous season {token!r} for tournament "
                f"{unique_tournament_id}"
            )
        return matches[0]

    def resolve_season_id(
        self,
        unique_tournament_id: int,
        season: str | int,
    ) -> Optional[int]:
        resolved = self.resolve_source_season(unique_tournament_id, season)
        return resolved.season_id if resolved is not None else None


__all__ = [
    "CatalogError",
    "CatalogSeason",
    "CatalogTournament",
    "DEFAULT_REGISTRY_PATH",
    "SCHEMA_VERSION",
    "SofaScoreCatalog",
    "registry_path",
]
