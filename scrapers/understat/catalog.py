"""League mappings and source-driven season discovery for Understat."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta
import math
import json
import logging
from pathlib import Path
import uuid
from types import MappingProxyType
from typing import Any, Mapping, Optional

from .client import UnderstatHTTPError, UnderstatPayloadError, save_schema_drift_payload
from .cache_state import atomic_json, digest, read_json


logger = logging.getLogger(__name__)


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
        try:
            scopes = self._scopes_from(payload)
            cache_dir = getattr(self.client, "cache_dir", None)
            if cache_dir is not None:
                target = Path(cache_dir) / "stat.last-success.json"
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
                try:
                    temporary.write_text(json.dumps(payload), encoding="utf-8")
                    temporary.replace(target)
                finally:
                    temporary.unlink(missing_ok=True)
            return scopes
        except UnderstatPayloadError:
            # #1428 (R-02): keep the rejected discovery response.
            save_schema_drift_payload(
                getattr(self.client, "cache_dir", None), "stat.json", payload
            )
            raise

    def _scopes_from(self, payload: Any) -> tuple[UnderstatScope, ...]:
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
        confirmed = {item.league: self._confirmed(item) for item in LEAGUES}
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
            matching = [int(y) for y, state in confirmed[definition.league].items()
                        if f"{year:04d}-{month:02d}" in state.get("months", [])]
            if matching:
                source_year = max(matching)
            key = (definition.league, source_year)
            found[key] = UnderstatScope(
                league=definition.league,
                source_league=definition.source_league,
                source_league_id=definition.source_league_id,
                season=season_slug(source_year),
                source_season_id=source_year,
                is_closed=confirmed[definition.league].get(str(source_year), {}).get(
                    "is_closed", source_year < current
                ),
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
        return self._rolling_window(
            self.discover_scopes(force_refresh=force_refresh),
            window=window, probe_next=probe_next,
        )

    def calendar_scopes(
        self, *, window: int = 2, probe_next: bool = True,
    ) -> tuple[UnderstatScope, ...]:
        """Calendar window using only the last validated registry, never HTTP."""
        cache_dir = getattr(self.client, "cache_dir", None)
        target = Path(cache_dir) / "stat.last-success.json" if cache_dir is not None else None
        discovered = ()
        if target is not None and target.exists():
            try:
                discovered = self._scopes_from(json.loads(target.read_text(encoding="utf-8")))
            except (OSError, ValueError) as exc:
                raise UnderstatPayloadError(f"Invalid last successful registry: {exc}") from exc
        return self._rolling_window(discovered, window=window, probe_next=probe_next, live=False)

    def _rolling_window(
        self, scopes: tuple[UnderstatScope, ...], *, window: int, probe_next: bool,
        live: bool = True,
    ) -> tuple[UnderstatScope, ...]:
        if window <= 0:
            raise ValueError("window must be positive")
        discovered = {
            (scope.league, scope.source_season_id): scope
            for scope in scopes
        }
        current = current_source_season_id(self.today)
        result: list[UnderstatScope] = []
        for definition in LEAGUES:
            known = [scope.source_season_id for scope in scopes if scope.league == definition.league]
            confirmed = self._confirmed(definition)
            published = [int(year) for year, state in confirmed.items() if state.get("published")]
            latest = max([*known, *published], default=current)
            # Stat rows are only a bootstrap hint. Keep a continuing season
            # across July until its own league schedule says it has finished.
            latest = max(latest, current - 1)
            years = list(range(latest - window + 1, latest + 1))
            getter = getattr(self.client, "get_league_data", None) if live else None
            failed_probes = set()

            def probe(year: int, *, force: bool = False) -> None:
                try:
                    self._probe(definition, year, confirmed, force=force)
                except Exception:
                    # The scope task repeats the request and classifies the
                    # error; a league probe must not block the other leagues.
                    failed_probes.add(year)
                    logger.warning("Understat probe failed: league=%s year=%s",
                                   definition.league, year, exc_info=True)

            if callable(getter):
                active = [int(y) for y, state in confirmed.items()
                          if state.get("published") and not state["is_closed"]]
                if not confirmed.get(str(latest), {}).get("is_closed"):
                    active.append(latest)
                if not confirmed and latest >= self.today.year:
                    # A July stat row may still belong to last year's source
                    # season. Confirm both candidates during cold bootstrap.
                    active.append(latest - 1)
                for year in sorted(set(active)):
                    probe(year, force=year in known and not confirmed.get(str(year), {}).get("published"))
                # Probe only around the actual end of the latest schedule.
                # Negative responses are cached for seven days.
                state = confirmed.get(str(latest), {})
                end = state.get("last_match")
                if (probe_next and end and
                        -14 <= (self.today - date.fromisoformat(end)).days <= 90):
                    probe(latest + 1)
                published = [int(y) for y, state in confirmed.items() if state.get("published")]
                if published:
                    if not confirmed.get(str(latest), {}).get("published") and latest == self.today.year:
                        latest = max(published)
                    latest = max(latest, max(published))
                    years = list(range(latest - window + 1, latest + 1))
                    years.extend(int(y) for y, state in confirmed.items()
                                 if state.get("published") and not state["is_closed"]
                                 and int(y) not in years)
            elif not confirmed and probe_next and self.today.month == 6 and self.today.year not in years:
                # Offline/bootstrap fallback has no league evidence yet.
                years.append(self.today.year)
            years.extend(int(y) for y, state in confirmed.items()
                         if state.get("published") and not state["is_closed"]
                         and int(y) not in years)
            years.extend(year for year in failed_probes if year not in years)
            for source_year in sorted(years):
                scope = discovered.get(
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
                state = confirmed.get(str(source_year))
                if state is not None and state.get("published"):
                    scope = replace(scope, is_closed=state["is_closed"], discovered=True)
                if source_year in failed_probes:
                    scope = replace(scope, is_closed=False, discovered=True)
                result.append(scope)
        return tuple(sorted(result))

    def _confirmed(self, definition: LeagueDefinition) -> dict[str, Any]:
        cache_dir = getattr(self.client, "cache_dir", None)
        if cache_dir is None:
            return {}
        folder = Path(cache_dir) / "confirmed"
        return {path.stem.rsplit("_", 1)[1]: state
                for path in folder.glob(f"{definition.source_league}_*.json")
                if isinstance(state := read_json(path), dict)}

    def record_league(self, definition: LeagueDefinition, year: int,
                      payload: Mapping[str, Any]) -> dict[str, Any]:
        from .parsers import _boolean
        dates = payload.get("dates", [])
        previous = self._confirmed(definition).get(str(year), {})
        last_match = max((str(row["datetime"])[:10] for row in dates), default=None)
        state = {
            "published": bool(dates), "checked_on": self.today.isoformat(),
            "last_match": last_match,
            "months": sorted({str(row["datetime"])[:7] for row in dates}),
            "schedule_closed": bool(dates) and all(
                _boolean(row["isResult"], "getLeagueData.dates[].isResult") for row in dates
            ) and date.fromisoformat(last_match) < self.today - timedelta(days=2),
            "league_hash": digest(payload),
            "quality_ready": previous.get("quality_ready", False)
            and previous.get("league_hash") == digest(payload),
            "quarantine_until": previous.get("quarantine_until"),
        }
        state["is_closed"] = self._can_close(state)
        self._save_confirmation(definition, year, state)
        return state

    def _can_close(self, state: Mapping[str, Any]) -> bool:
        until = state.get("quarantine_until")
        return bool(state.get("schedule_closed") and state.get("quality_ready")
                    and (not until or self.today > date.fromisoformat(until[:10])))

    def _save_confirmation(self, definition: LeagueDefinition, year: int, state: Any) -> None:
        cache_dir = getattr(self.client, "cache_dir", None)
        if cache_dir is not None:
            atomic_json(Path(cache_dir) / "confirmed" / f"{definition.source_league}_{year}.json", state)

    def note_quality(self, league: str, year: int, report: Any, *,
                     publication_ready: bool = True) -> None:
        definition = LEAGUE_BY_CANONICAL[league]
        cache_dir = getattr(self.client, "cache_dir", None)
        if not report.passed and cache_dir is not None:
            # DQ can implicate an unchanged, previously confirmed team page.
            # Keep confirmed matches; only teams of this scope need a reset.
            for path in (Path(cache_dir) / "validated").glob("team_*.json"):
                record = read_json(path)
                if isinstance(record, dict) and record.get("identity", [])[:2] == [league, year]:
                    path.unlink(missing_ok=True)
        state = self._confirmed(definition).get(str(year))
        if state is None:
            return
        state["quality_ready"] = report.passed and publication_ready
        state["quarantine_until"] = max(
            (item.retry_until for item in report.match_quarantine if item.state == "waiting_data"),
            default=None,
        )
        state["is_closed"] = self._can_close(state)
        self._save_confirmation(definition, year, state)

    def _probe(self, definition: LeagueDefinition, year: int,
               confirmed: dict[str, Any], *, force: bool = False) -> None:
        previous = confirmed.get(str(year), {})
        checked = previous.get("checked_on")
        ttl = 1 if previous.get("published") else 7
        if not force and checked and (self.today - date.fromisoformat(checked)).days < ttl:
            return
        try:
            payload = self.client.get_league_data(definition.source_league, year, force_refresh=True)
        except UnderstatHTTPError as exc:
            if exc.status_code != 404:
                raise
            payload = {"dates": [], "players": [], "teams": {}}
        # Imported here because parsers use the catalog's scope definitions.
        from .parsers import validate_league_payload
        try:
            validate_league_payload(payload)
        except Exception:
            save_schema_drift_payload(getattr(self.client, "cache_dir", None),
                                      f"league_{definition.source_league}_{year}.json", payload)
            raise
        state = self.record_league(definition, year, payload)
        # A transient empty response must not erase a confirmed continuing
        # season. The runner still fails on an empty discovered scope.
        if previous.get("published") and not state["published"]:
            state = {**previous, "checked_on": self.today.isoformat()}
            cache_dir = getattr(self.client, "cache_dir", None)
            if cache_dir is not None:
                atomic_json(Path(cache_dir) / "confirmed" / f"{definition.source_league}_{year}.json", state)
        confirmed[str(year)] = state


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
