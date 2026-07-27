"""League mappings and source-driven season discovery for Understat."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
import math
from types import MappingProxyType
from typing import Any, Mapping, Optional

from .client import UnderstatPayloadError


@dataclass(frozen=True)
class LeagueDefinition:
    league: str
    source_league: str
    source_league_id: int
    source_labels: tuple[str, ...]


LEAGUES: tuple[LeagueDefinition, ...] = (
    LeagueDefinition("ENG-Premier League", "EPL", 1, ("EPL",)),
    LeagueDefinition("ESP-La Liga", "La_liga", 4, ("La liga", "La Liga", "La_liga")),
    LeagueDefinition("GER-Bundesliga", "Bundesliga", 3, ("Bundesliga",)),
    LeagueDefinition("ITA-Serie A", "Serie_A", 2, ("Serie A", "Serie_A")),
    LeagueDefinition("FRA-Ligue 1", "Ligue_1", 5, ("Ligue 1", "Ligue_1")),
    LeagueDefinition("RUS-Premier League", "RFPL", 6, ("RFPL",)),
)

PRODUCTION_LEAGUES: tuple[str, ...] = tuple(item.league for item in LEAGUES)
LEAGUE_BY_CANONICAL: Mapping[str, LeagueDefinition] = MappingProxyType(
    {item.league: item for item in LEAGUES}
)
LEAGUE_BY_SOURCE: Mapping[str, LeagueDefinition] = MappingProxyType(
    {
        label: item
        for item in LEAGUES
        for label in (item.source_league, *item.source_labels)
    }
)

_STAT_REQUIRED_ROW_FIELDS = frozenset(
    {"league", "h", "a", "hxg", "axg", "year", "month", "matches"}
)
_STAT_ROW_FIELDS = frozenset(
    {"league_id", "league", "h", "a", "hxg", "axg", "year", "month", "matches"}
)


def _payload_error(path: str, message: str) -> UnderstatPayloadError:
    return UnderstatPayloadError(f"getStatData.{path}: {message}")


def _integer_field(row: Mapping[str, Any], field: str, path: str) -> int:
    if field not in row:
        raise _payload_error(path, f"missing required field {field!r}")
    value = row[field]
    if isinstance(value, bool):
        raise _payload_error(path, f"{field} must be an integer, got bool")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise _payload_error(path, f"{field} must be an integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise _payload_error(path, f"{field} must be an integer")
    if isinstance(value, str) and str(parsed) != value.strip():
        raise _payload_error(path, f"{field} must be a canonical integer string")
    return parsed


def _number_field(row: Mapping[str, Any], field: str, path: str) -> float:
    if field not in row:
        raise _payload_error(path, f"missing required field {field!r}")
    value = row[field]
    if isinstance(value, bool):
        raise _payload_error(path, f"{field} must be numeric, got bool")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise _payload_error(path, f"{field} must be numeric") from exc
    if not math.isfinite(parsed):
        raise _payload_error(path, f"{field} must be finite")
    return parsed


def season_slug(source_season_id: int) -> str:
    """Return the platform's unambiguous four-digit season slug."""

    if isinstance(source_season_id, bool) or not isinstance(source_season_id, int):
        raise TypeError("source_season_id must be an integer")
    if not 1900 <= source_season_id <= 2198:
        raise ValueError("source_season_id is outside the supported range")
    return f"{source_season_id % 100:02d}{(source_season_id + 1) % 100:02d}"


def source_season_id_from_slug(value: str) -> int:
    """Parse a canonical slug and reject ambiguous year-like values."""

    if not isinstance(value, str) or len(value) != 4 or not value.isdigit():
        raise ValueError("season must be a four-digit string such as '2526'")
    start = int(value[:2])
    end = int(value[2:])
    if end != (start + 1) % 100:
        raise ValueError(f"season {value!r} does not describe consecutive years")
    return 2000 + start if start < 90 else 1900 + start


def current_source_season_id(today: Optional[date] = None) -> int:
    """Return the season starting in July of the supplied calendar year."""

    today = today or date.today()
    return today.year if today.month >= 7 else today.year - 1


@dataclass(frozen=True, order=True)
class UnderstatScope:
    league: str
    source_league: str
    source_league_id: int
    season: str
    source_season_id: int
    is_closed: bool
    discovered: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class UnderstatCatalog:
    """Translate ``getStatData`` rows into canonical league-season scopes."""

    def __init__(self, client: Any, *, today: Optional[date] = None):
        self.client = client
        self.today = today or date.today()

    def discover_scopes(self, *, force_refresh: bool = True) -> tuple[UnderstatScope, ...]:
        payload = self.client.get_stat_data(force_refresh=force_refresh)
        if not isinstance(payload, Mapping):
            raise _payload_error("root", "payload must be an object")
        if set(payload) != {"stat"}:
            missing = sorted({"stat"} - set(payload))
            extra = sorted(set(payload) - {"stat"})
            raise _payload_error(
                "root",
                f"expected only the stat field; missing={missing}, extra={extra}",
            )
        rows = payload["stat"]
        if not isinstance(rows, list):
            raise _payload_error("stat", "must be a list")
        if not rows:
            raise _payload_error("stat", "must contain discovery rows")

        current = current_source_season_id(self.today)
        found: dict[tuple[str, int], UnderstatScope] = {}
        covered_leagues: set[str] = set()

        for index, row in enumerate(rows):
            path = f"stat[{index}]"
            if not isinstance(row, Mapping):
                raise _payload_error(path, "row must be an object")
            missing = sorted(_STAT_REQUIRED_ROW_FIELDS - set(row))
            unknown = sorted(set(row) - _STAT_ROW_FIELDS)
            if missing or unknown:
                raise _payload_error(
                    path,
                    f"field contract mismatch; missing={missing}, extra={unknown}",
                )
            label = row["league"]
            if not isinstance(label, str) or not label.strip():
                raise _payload_error(path, "league must be a non-empty string")
            definition = LEAGUE_BY_SOURCE.get(label)

            source_league_id = (
                _integer_field(row, "league_id", path)
                if "league_id" in row
                else None
            )
            year = _integer_field(row, "year", path)
            month = _integer_field(row, "month", path)
            matches = _integer_field(row, "matches", path)
            for field in ("h", "a", "hxg", "axg"):
                _number_field(row, field, path)
            if not 1900 <= year <= 2198:
                raise _payload_error(path, f"year {year} is outside the supported range")
            if not 1 <= month <= 12:
                raise _payload_error(path, f"month must be in 1..12, got {month}")
            if matches < 0:
                raise _payload_error(path, "matches must be non-negative")

            if definition is None:
                continue
            if (
                source_league_id is not None
                and source_league_id != definition.source_league_id
            ):
                raise _payload_error(
                    path,
                    f"league_id={source_league_id} does not match {label!r} "
                    f"(expected {definition.source_league_id})",
                )
            covered_leagues.add(definition.league)
            source_year = year if month >= 7 else year - 1
            key = (definition.league, source_year)
            found[key] = UnderstatScope(
                league=definition.league,
                source_league=definition.source_league,
                source_league_id=definition.source_league_id,
                season=season_slug(source_year),
                source_season_id=source_year,
                is_closed=source_year < current,
                discovered=True,
            )

        missing_leagues = sorted(set(PRODUCTION_LEAGUES) - covered_leagues)
        if missing_leagues:
            raise _payload_error(
                "stat", f"production leagues absent from discovery: {missing_leagues}"
            )

        return tuple(sorted(found.values()))

    def rolling_scopes(
        self,
        *,
        window: int = 2,
        probe_next: bool = True,
        force_refresh: bool = True,
    ) -> tuple[UnderstatScope, ...]:
        """Return current scopes even before Understat publishes match data.

        ``window=2`` means the previous and current season. ``probe_next`` is
        useful during the June/July rollover: it also probes the following
        source id until the scheduler's calendar has caught up.
        """

        if window <= 0:
            raise ValueError("window must be positive")
        discovered = {
            (scope.league, scope.source_season_id): scope
            for scope in self.discover_scopes(force_refresh=force_refresh)
        }
        current = current_source_season_id(self.today)
        years = list(range(current - window + 1, current + 1))
        # Before the July rollover, probe the source id matching this calendar
        # year. After rollover it is already the current season; do not invent
        # a 2027/28 probe a full year early.
        if probe_next and self.today.year not in years:
            years.append(self.today.year)

        result: list[UnderstatScope] = []
        for definition in LEAGUES:
            for source_year in years:
                result.append(
                    discovered.get(
                        (definition.league, source_year),
                        UnderstatScope(
                            league=definition.league,
                            source_league=definition.source_league,
                            source_league_id=definition.source_league_id,
                            season=season_slug(source_year),
                            source_season_id=source_year,
                            is_closed=source_year < current,
                            discovered=False,
                        ),
                    )
                )
        return tuple(sorted(result))


__all__ = [
    "LEAGUES",
    "LEAGUE_BY_CANONICAL",
    "LEAGUE_BY_SOURCE",
    "PRODUCTION_LEAGUES",
    "LeagueDefinition",
    "UnderstatCatalog",
    "UnderstatScope",
    "current_source_season_id",
    "season_slug",
    "source_season_id_from_slug",
]
