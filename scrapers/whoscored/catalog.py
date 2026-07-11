"""Competition-catalog adapter for canonical WhoScored scopes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Optional

import yaml

from .domain import SeasonFormat, WhoScoredScope


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


@dataclass(frozen=True)
class CatalogCompetition:
    competition_id: str
    seasons: tuple[CatalogSeason, ...]
    whoscored_enabled: bool
    region_id: Optional[int] = None
    tournament_id: Optional[int] = None


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


class WhoScoredCatalog:
    """Strict read-only projection of ``competitions.yaml``.

    Source identifiers live in the source-owned catalog mapping::

        sources:
          whoscored: {region_id: 252, tournament_id: 2}

    A season may similarly carry ``source_season_id`` once it has been
    discovered.  Missing source ids do not prevent scope validation; callers
    that need discovery can test the explicit ``None`` values.
    """

    def __init__(self, competitions: tuple[CatalogCompetition, ...]) -> None:
        by_id: dict[str, CatalogCompetition] = {}
        for competition in competitions:
            if competition.competition_id in by_id:
                raise CatalogError(
                    f"Duplicate competition id {competition.competition_id!r}"
                )
            by_id[competition.competition_id] = competition
        self._competitions = by_id

    @classmethod
    def from_file(cls, path: str | Path = DEFAULT_COMPETITIONS_PATH) -> "WhoScoredCatalog":
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
        for season in competition.seasons:
            if season.scope.season_id == token:
                return season
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
            for season in competition.seasons
        )
