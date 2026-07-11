"""Strict read-only access to the versioned SofaScore tournament registry."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional


SCHEMA_VERSION = 1
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
    year: str
    season_format: str
    canonical_season: Optional[str]

    @property
    def activatable(self) -> bool:
        return (
            self.season_format in {"split_year", "single_year"}
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
    seasons: tuple[CatalogSeason, ...]


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
        if document.get("schema_version") != SCHEMA_VERSION:
            raise CatalogError(
                f"schema_version must be {SCHEMA_VERSION}"
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
            seen_canonical_seasons: dict[str, int] = {}
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
                previous = seen_years.get(year)
                if previous is not None and previous != season_id:
                    raise CatalogError(
                        f"{prefix} has ambiguous year {year!r}"
                    )
                seen_years[year] = season_id
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
                canonical_season = _optional_string(
                    raw_season.get("canonical_season"),
                    f"{season_prefix}.canonical_season",
                )
                if season_format == "unknown" and canonical_season is not None:
                    raise CatalogError(
                        f"{season_prefix} unknown format cannot be activatable"
                    )
                if canonical_season is not None:
                    previous_canonical = seen_canonical_seasons.get(
                        canonical_season
                    )
                    if (
                        previous_canonical is not None
                        and previous_canonical != season_id
                    ):
                        raise CatalogError(
                            f"{prefix} has ambiguous canonical_season "
                            f"{canonical_season!r}"
                        )
                    seen_canonical_seasons[canonical_season] = season_id
                seasons.append(
                    CatalogSeason(
                        season_id=season_id,
                        name=_required_string(
                            raw_season.get("name"), f"{season_prefix}.name"
                        ),
                        year=year,
                        season_format=season_format,
                        canonical_season=canonical_season,
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
            if tournament.enabled and tournament.canonical_id is not None
        ))

    def tournament_map(self, *, enabled_only: bool = False) -> dict[str, int]:
        return {
            tournament.canonical_id: tournament.unique_tournament_id
            for tournament in self._tournaments
            if tournament.canonical_id is not None
            and (tournament.enabled or not enabled_only)
        }

    def slug_map(self, *, enabled_only: bool = False) -> dict[str, str]:
        return {
            tournament.canonical_id: tournament.page_path
            for tournament in self._tournaments
            if tournament.canonical_id is not None
            and (tournament.enabled or not enabled_only)
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
            if item.year == token or item.canonical_season == token
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
