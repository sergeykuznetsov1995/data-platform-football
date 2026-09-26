"""Explicit WhoScored daily denominator (#1474, epic #1457 decision 2).

The daily run no longer walks every active scope of the catalog.  It covers:

* the tournaments of class A — WhoScored publishes matchCentre data for them
  (review table C2, 24.09.2026).  The class belongs to the source tournament
  id, so every new season inherits it without a code change;
* per class-A tournament, the current season(s) and the season that has just
  finished;
* the scopes whose availability is still unknown and must be probed by the
  regular daily run (``PROBE_SCOPE_SPECS``).  A probe proven available is
  promoted into ``CLASS_A_TOURNAMENT_IDS`` by a follow-up commit.

Friendlies and pre-season exhibitions stay outside the denominator and outside
collection (``EXCLUDED_TOURNAMENT_IDS``).
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

# Source tournament id -> tournament (review C2 table, class A: publishes).
CLASS_A_TOURNAMENTS: dict[int, str] = {
    2: "England / Premier League",
    3: "Germany / Bundesliga",
    4: "Spain / LaLiga",
    5: "Italy / Serie A",
    6: "Germany / 2. Bundesliga",
    7: "England / Championship",
    8: "England / League One",
    9: "England / League Two",
    12: "Europe / Champions League",
    13: "Netherlands / Eredivisie",
    17: "Turkey / Super Lig",
    18: "Belgium / Jupiler Pro League",
    20: "Scotland / Premiership",
    21: "Portugal / Liga Portugal",
    22: "France / Ligue 1",
    29: "England / League Cup",
    30: "Europe / Europa League",
    36: "International / FIFA World Cup",
    77: "Russia / Premier League",
    85: "USA / Major League Soccer",
    95: "Brazil / Brasileirao",
    721: "International / World Cup Qualification UEFA",
}
CLASS_A_TOURNAMENT_IDS: frozenset[int] = frozenset(CLASS_A_TOURNAMENTS)

# Never part of the denominator nor of daily collection.
EXCLUDED_TOURNAMENTS: dict[int, str] = {
    27: "International / Int. Friendly",
    57: "International / Club Friendlies",
    613: "Europe / The Atlantic Cup",
    635: "Asia / Premier League Asia Trophy",
}
EXCLUDED_TOURNAMENT_IDS: frozenset[int] = frozenset(EXCLUDED_TOURNAMENTS)

# Unknown availability: probed by the regular daily run (grill 24.09).
PROBE_SCOPE_SPECS: tuple[str, ...] = (
    "WS-206-63=2526",  # Spain / Segunda Division
    "WS-108-19=2526",  # Italy / Serie B
    "WS-194-282=2526",  # Saudi Arabia / Pro League
    "WS-250-715=2526",  # Europe / Conference League
    "WS-249-287=2526",  # Asia / AFC Champions League
    "WS-248-290=2526",  # Africa / CAF Champions League
    "WS-64-277=2526",  # Egypt / Premier League
    "WS-149-291=2526",  # Morocco / Botola Pro
    "WS-239-392=2526",  # Vietnam / V.League 1
    "WS-265-105=2026",  # South America / Copa Libertadores
    "WS-264-222=2026",  # North & Central America / CONCACAF Champions Cup
)


def _season_key(season: Any) -> str:
    return str(season.scope.season_id)


def _is_finished(season: Any, today: date) -> bool:
    end = getattr(season, "end", None)
    start = getattr(season, "start", None)
    if end is not None:
        return end < today
    # Legacy catalog rows of past seasons carry no dates at all.
    return start is None


def denominator_scopes(catalog: Any, *, on: Optional[date] = None) -> list[Any]:
    """Return catalog scopes of the daily run, sorted by scope spec.

    Class-A tournaments contribute their active season(s) plus the latest
    finished season before them; probe scopes are added when the catalog
    knows them.  Excluded tournaments never appear.
    """

    today = on or date.today()
    active_specs = {season.scope.spec for season in catalog.active_scopes(on=today)}
    selected: dict[str, Any] = {}
    by_competition: dict[str, list[Any]] = {}
    for season in catalog.enabled_scopes():
        by_competition.setdefault(season.scope.competition_id, []).append(season)
    for competition_id, seasons in by_competition.items():
        tournament_id = catalog.competition(competition_id).tournament_id
        if tournament_id is None:
            continue
        tournament_id = int(tournament_id)
        if (
            tournament_id in EXCLUDED_TOURNAMENT_IDS
            or tournament_id not in CLASS_A_TOURNAMENT_IDS
        ):
            continue
        active = [season for season in seasons if season.scope.spec in active_specs]
        finished = [
            season
            for season in seasons
            if season.scope.spec not in active_specs and _is_finished(season, today)
        ]
        if active:
            first_active = min(_season_key(season) for season in active)
            finished = [
                season for season in finished if _season_key(season) < first_active
            ]
        for season in active:
            selected[season.scope.spec] = season
        if finished:
            latest = max(finished, key=_season_key)
            selected[latest.scope.spec] = latest
    enabled = {season.scope.spec: season for season in catalog.enabled_scopes()}
    for spec in PROBE_SCOPE_SPECS:
        season = enabled.get(spec)
        if season is None:
            continue
        tournament_id = catalog.competition(season.scope.competition_id).tournament_id
        if tournament_id is not None and int(tournament_id) in EXCLUDED_TOURNAMENT_IDS:
            continue
        selected[spec] = season
    return [selected[spec] for spec in sorted(selected)]


def missing_probe_specs(catalog: Any) -> list[str]:
    """Probe specs the catalog does not know (visible in the run log)."""

    enabled = {season.scope.spec for season in catalog.enabled_scopes()}
    return [spec for spec in PROBE_SCOPE_SPECS if spec not in enabled]
