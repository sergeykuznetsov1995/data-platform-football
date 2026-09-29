"""WhoScored "played -> Bronze within 24 h" meter and its red gates (#1476).

Definition (grill 24.09, roadmap task 6; the morning report copies the SQL
text below verbatim, so change it only together with that copy):

* Denominator: games of the class-A tournaments (``CLASS_A_TOURNAMENTS`` in
  ``scrapers/whoscored/catalog.py``; probe scopes are not in it) from the
  latest schedule row per game, ``status NOT IN (5, 7)`` (not postponed /
  cancelled: a stale schedule never says "played"), in a stage that is not
  ``unavailable``.  Stage availability is the daily candidate rule of
  ``WhoScoredRepository.list_match_candidates``: >= 1 success -> available,
  >= 2 "not available" with a confirmed lineup and 0 success -> unavailable,
  otherwise unknown; here per (league, season, stage_id).
* Deadline = ``schedule.date`` (UTC) + 2 h + 24 h.  Window: deadlines in the
  7 days before ``{now}`` (a UTC ``YYYY-MM-DD HH:MM:SS`` literal).
* Collected: first V2 success (``state = 'success'``, ``batch_id LIKE
  'ws2-%'``, ``raw_uri IS NOT NULL`` - the ``*_latest_success`` filter) with
  ``events_count > 0`` and ``lineups_count > 0``; its ``completed_at``.
* Ceiling ("not at the source"): no such success and the latest manifest
  state is ``not_available`` after its last re-probe (>= 2 verdicts), or in a
  stage that is not yet ``available`` (its re-probe is the 30-day stage
  probe).  Counted apart and removed from the percentage base.
* ok = collected by the deadline; late = collected after it; missing = not
  collected and not ceiling.  pct = ok / (due - ceiling).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable, Iterable, Optional, Sequence

from scrapers.whoscored.catalog import CLASS_A_TOURNAMENT_IDS, PROBE_SCOPE_SPECS

TARGET_PCT = Decimal("99")
WINDOW_DAYS = 7
DEADLINE_HOURS = 26
SCHEDULE_MAX_AGE_HOURS = 48
CONTENT_MAX_AGE_HOURS = 48
MISSED_LIMIT = 10

_CLASS_A_SQL = ", ".join(str(value) for value in sorted(CLASS_A_TOURNAMENT_IDS))

_DENOMINATOR_CTES = (
    """
sched AS (
    SELECT league, season, game_id, game, stage_id, status, date,
           is_lineup_confirmed, tournament_id, region_name, tournament_name
    FROM (
        SELECT s.*, ROW_NUMBER() OVER (
            PARTITION BY league, season, game_id ORDER BY _ingested_at DESC
        ) AS rn
        FROM iceberg.bronze.whoscored_schedule_current s
        WHERE tournament_id IN ("""
    + _CLASS_A_SQL
    + """)
    )
    WHERE rn = 1 AND game_id IS NOT NULL
),
latest AS (
    SELECT league, season, game_id, state
    FROM iceberg.bronze.whoscored_match_ingest_latest
),
stage_availability AS (
    SELECT s.league, s.season, s.stage_id,
           CASE
               WHEN COUNT_IF(l.state = 'success') > 0 THEN 'available'
               WHEN COUNT_IF(
                   l.state = 'not_available' AND s.is_lineup_confirmed = TRUE
               ) >= 2 THEN 'unavailable'
               ELSE 'unknown'
           END AS availability
    FROM sched s
    JOIN latest l
      ON l.league = s.league AND l.season = s.season AND l.game_id = s.game_id
    GROUP BY s.league, s.season, s.stage_id
),
first_success AS (
    SELECT league, season, game_id, MIN(completed_at) AS collected_at
    FROM iceberg.bronze.whoscored_match_ingest_manifest
    WHERE state = 'success' AND batch_id LIKE 'ws2-%' AND raw_uri IS NOT NULL
      AND events_count > 0 AND lineups_count > 0
    GROUP BY league, season, game_id
),
na_verdicts AS (
    SELECT league, season, game_id, COUNT(*) AS verdicts
    FROM iceberg.bronze.whoscored_match_ingest_manifest
    WHERE state = 'not_available'
    GROUP BY league, season, game_id
),
due AS (
    SELECT s.league, s.season, s.game_id, s.game, s.region_name,
           s.tournament_name,
           s.date + INTERVAL '26' HOUR AS deadline,
           f.collected_at,
           f.collected_at IS NULL
               AND COALESCE(l.state = 'not_available', FALSE)
               AND (
                   COALESCE(n.verdicts, 0) >= 2
                   OR COALESCE(st.availability, 'unknown') <> 'available'
               ) AS ceiling
    FROM sched s
    LEFT JOIN stage_availability st
      ON st.league = s.league AND st.season = s.season
     AND st.stage_id IS NOT DISTINCT FROM s.stage_id
    LEFT JOIN latest l
      ON l.league = s.league AND l.season = s.season AND l.game_id = s.game_id
    LEFT JOIN first_success f
      ON f.league = s.league AND f.season = s.season AND f.game_id = s.game_id
    LEFT JOIN na_verdicts n
      ON n.league = s.league AND n.season = s.season AND n.game_id = s.game_id
    WHERE s.status NOT IN (5, 7)
      AND COALESCE(st.availability, 'unknown') <> 'unavailable'
      AND s.date + INTERVAL '26' HOUR <= TIMESTAMP '{now}'
      AND s.date + INTERVAL '26' HOUR > TIMESTAMP '{now}' - INTERVAL '7' DAY
)"""
)

# One row per UTC deadline day of the 7-day window.
DAILY_CRITERION_SQL = (
    "WITH"
    + _DENOMINATOR_CTES
    + """
SELECT CAST(CAST(deadline AS date) AS varchar) AS day,
       COUNT(*) AS due,
       COUNT_IF(collected_at IS NOT NULL AND collected_at <= deadline) AS ok,
       COUNT_IF(collected_at IS NOT NULL AND collected_at > deadline) AS late,
       COUNT_IF(collected_at IS NULL AND NOT ceiling) AS missing,
       COUNT_IF(ceiling) AS ceiling
FROM due
GROUP BY CAST(CAST(deadline AS date) AS varchar)
ORDER BY day"""
)

# Up to ten games of the window that missed the deadline (late or missing).
MISSED_SQL = (
    "WITH"
    + _DENOMINATOR_CTES
    + """
SELECT region_name || ' / ' || tournament_name AS tournament, league, season,
       game_id, game, deadline, collected_at
FROM due
WHERE NOT ceiling AND (collected_at IS NULL OR collected_at > deadline)
ORDER BY deadline DESC, game_id DESC
LIMIT 10"""
)

# validate_data: games past their deadline that are still not collected.
OVERDUE_SQL = (
    "WITH"
    + _DENOMINATOR_CTES
    + """
SELECT league, season, game_id, game, deadline
FROM due
WHERE collected_at IS NULL AND NOT ceiling
ORDER BY deadline, game_id"""
)

# Last schedule write per denominator partition; {partitions} is a list of
# ('league', 'season') row literals.
SCHEDULE_FRESHNESS_SQL = """SELECT league, season, MAX(_ingested_at) AS refreshed_at
FROM iceberg.bronze.whoscored_schedule
WHERE (league, season) IN ({partitions})
GROUP BY league, season"""

# Last content write over the denominator partitions (one row per table).
CONTENT_FRESHNESS_SQL = """SELECT '{table}' AS table_name, MAX(_ingested_at) AS refreshed_at
FROM iceberg.bronze.{table}
WHERE (league, season) IN ({partitions})"""

CONTENT_TABLES = ("whoscored_matches", "whoscored_events")


def _now_literal(now: datetime) -> str:
    if now.tzinfo is not None:
        raise ValueError("now must be a naive UTC datetime")
    return now.strftime("%Y-%m-%d %H:%M:%S")


def render_daily_criterion_sql(now: datetime) -> str:
    return DAILY_CRITERION_SQL.replace("{now}", _now_literal(now))


def render_missed_sql(now: datetime) -> str:
    return MISSED_SQL.replace("{now}", _now_literal(now))


def render_overdue_sql(now: datetime) -> str:
    return OVERDUE_SQL.replace("{now}", _now_literal(now))


def _quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _partitions_sql(partitions: Sequence[tuple[str, str]]) -> str:
    if not partitions:
        raise ValueError("denominator partitions are empty")
    return ", ".join(
        f"({_quote(league)}, {_quote(season)})" for league, season in partitions
    )


def render_schedule_freshness_sql(partitions: Sequence[tuple[str, str]]) -> str:
    return SCHEDULE_FRESHNESS_SQL.replace("{partitions}", _partitions_sql(partitions))


def render_content_freshness_sql(
    table: str, partitions: Sequence[tuple[str, str]]
) -> str:
    if table not in CONTENT_TABLES:
        raise ValueError(f"unexpected content table {table!r}")
    return CONTENT_FRESHNESS_SQL.replace("{table}", table).replace(
        "{partitions}", _partitions_sql(partitions)
    )


def denominator_partitions(report: dict[str, Any]) -> list[tuple[str, str]]:
    """(league, season) of the run's denominator scopes; probes excluded."""
    probes = set(PROBE_SCOPE_SPECS)
    return [
        (str(scope["competition_id"]), str(scope["season_id"]))
        for scope in report.get("scopes") or []
        if str(scope.get("scope") or "") not in probes
    ]


def pct(ok: int, due: int, ceiling: int = 0) -> Optional[Decimal]:
    """Percentage with one decimal, ROUND_HALF_UP; ``None`` when nothing is due."""
    base = due - ceiling
    if base <= 0:
        return None
    return (Decimal(100) * Decimal(ok) / Decimal(base)).quantize(
        Decimal("0.1"), rounding=ROUND_HALF_UP
    )


@dataclass(frozen=True)
class DayResult:
    day: str
    due: int
    ok: int
    late: int
    missing: int
    ceiling: int

    @property
    def pct(self) -> Optional[Decimal]:
        return pct(self.ok, self.due, self.ceiling)

    @property
    def meets_target(self) -> Optional[bool]:
        base = self.due - self.ceiling
        if base <= 0:
            return None
        return Decimal(100) * self.ok >= TARGET_PCT * base


def day_results(rows: Iterable[Sequence[Any]]) -> list[DayResult]:
    return [
        DayResult(
            day=str(row[0]),
            due=int(row[1]),
            ok=int(row[2]),
            late=int(row[3]),
            missing=int(row[4]),
            ceiling=int(row[5]),
        )
        for row in rows
    ]


def _age_hours(value: Any, now: datetime) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is not None:
        value = value.replace(tzinfo=None)
    return (now - value).total_seconds() / 3600.0


def stale_schedule_partitions(
    query: Callable[[str], Sequence[Sequence[Any]]],
    partitions: Sequence[tuple[str, str]],
    now: datetime,
) -> list[str]:
    """Denominator partitions whose schedule is older than 48 h or absent."""
    rows = query(render_schedule_freshness_sql(partitions))
    refreshed = {(str(row[0]), str(row[1])): row[2] for row in rows}
    stale = []
    for league, season in partitions:
        age = _age_hours(refreshed.get((league, season)), now)
        if age is None or age > SCHEDULE_MAX_AGE_HOURS:
            shown = "never" if age is None else f"{age:.0f}h"
            stale.append(f"{league}={season} ({shown})")
    return stale


def content_age_hours(
    query: Callable[[str], Sequence[Sequence[Any]]],
    table: str,
    partitions: Sequence[tuple[str, str]],
    now: datetime,
) -> Optional[float]:
    rows = query(render_content_freshness_sql(table, partitions))
    return _age_hours(rows[0][1] if rows else None, now)


__all__ = [
    "CONTENT_MAX_AGE_HOURS",
    "CONTENT_TABLES",
    "DAILY_CRITERION_SQL",
    "DEADLINE_HOURS",
    "MISSED_LIMIT",
    "MISSED_SQL",
    "OVERDUE_SQL",
    "SCHEDULE_FRESHNESS_SQL",
    "SCHEDULE_MAX_AGE_HOURS",
    "TARGET_PCT",
    "WINDOW_DAYS",
    "DayResult",
    "content_age_hours",
    "day_results",
    "denominator_partitions",
    "pct",
    "render_content_freshness_sql",
    "render_daily_criterion_sql",
    "render_missed_sql",
    "render_overdue_sql",
    "render_schedule_freshness_sql",
    "stale_schedule_partitions",
]
