"""SQL and the morning-report line of the ESPN history lane (#1509).

* ``LIVE_DEBT_SQL`` — the pause rule of the history lane: a match of a live
  target (``Denominator.live_targets()``) with kickoff 14…72 h ago that the
  freshness meter (``criterion.DAILY_CRITERION_SQL``) would count as due and
  that is still not published.  Out of the debt, exactly as out of ``due``:
  a duplicate, ``disposition = 'withdrawn'`` and a match confirmed not played
  after kickoff (``terminal_nonplayed`` or ``STATUS_POSTPONED`` with
  ``status_checked_at > kickoff``) — a postponed match has no Summary to wait
  for.  Any debt stops the history lane until its next run.
* ``HISTORY_CAPABILITY_SQL`` — what a history season can legitimately lack,
  from its own data (C3-F5, C3-F9): among its played finals the share of
  captured lineups and team statistics.  A part captured in fewer than half of
  them is absent for the season ("без составов" / "без статов"); its
  ``valid_empty`` rows are then no defect.  Nothing is kept by hand.
* ``HISTORY_QUEUE_SQL`` / ``HISTORY_JOURNAL_SQL`` — the report line: seasons
  finished during the day and in the queue, red seasons and red inventories
  (a slug whose season list failed), matches written,
  pauses for the live debt (run rows of the queue) and the network requests
  and bytes of the ``history`` lane (request journal).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence

from .criterion import _iso_day, targets_literal

# Kickoff window of the live debt: kickoff + 2 h (end) + 12 h ... 72 h ago.
DEBT_FROM = timedelta(hours=72)
DEBT_TO = timedelta(hours=14)
# A part captured in fewer than this share of played finals is absent.
CAPTURED_SHARE = 0.5
FULL = "полный"
NO_LINEUPS = "без составов"
NO_STATS = "без статов"
NO_BOTH = "без составов и статов"

_LIVE_DEBT_WHERE = """duplicate_of IS NULL
  AND competition_slug IN ({targets})
  AND kickoff > TIMESTAMP '{since}'
  AND kickoff < TIMESTAMP '{until}'
  AND first_published_at IS NULL
  AND NOT coalesce(disposition = 'withdrawn', false)
  AND NOT coalesce(
      (terminal_nonplayed OR status = 'STATUS_POSTPONED') AND status_checked_at > kickoff,
      false
  )"""

LIVE_DEBT_SQL = f"""SELECT count(*) AS debt
FROM iceberg.bronze.espn_match
WHERE {_LIVE_DEBT_WHERE}"""

HISTORY_CAPABILITY_SQL = """WITH seasons AS (
    SELECT DISTINCT slug, season_year
    FROM iceberg.ops.espn_history_queue_v1
    WHERE slug <> '(run)' AND season_year > 0
),
parts AS (
    SELECT m.competition_slug, m.season_year,
           count(*) AS played,
           count_if(m.lineup_state = 'captured') AS lineup_captured,
           count_if(m.lineup_state = 'valid_empty') AS lineup_empty,
           count_if(m.team_stats_state = 'captured') AS stats_captured,
           count_if(m.team_stats_state = 'valid_empty') AS stats_empty
    FROM iceberg.bronze.espn_match m
    JOIN seasons s ON s.slug = m.competition_slug AND s.season_year = m.season_year
    WHERE m.duplicate_of IS NULL AND m.played_final
    GROUP BY m.competition_slug, m.season_year
)
SELECT competition_slug, season_year, played,
       lineup_captured, lineup_empty, stats_captured, stats_empty,
       CASE
           WHEN lineup_captured < {share} * played AND stats_captured < {share} * played
               THEN '{no_both}'
           WHEN lineup_captured < {share} * played THEN '{no_lineups}'
           WHEN stats_captured < {share} * played THEN '{no_stats}'
           ELSE '{full}'
       END AS capability
FROM parts
ORDER BY competition_slug, season_year"""

_DAY = "TIMESTAMP '{day} 00:00:00'"
_IN_DAY = f"updated_at >= {_DAY} AND updated_at < {_DAY} + INTERVAL '1' DAY"

HISTORY_QUEUE_SQL = f"""WITH seasons AS (
    SELECT slug, season_year,
           bool_and(state IN ('done', 'empty')) AS finished,
           bool_or(state = 'red') AS red,
           max(updated_at) AS last_at
    FROM iceberg.ops.espn_history_queue_v1
    WHERE slug <> '(run)' AND season_year > 0
    GROUP BY slug, season_year
),
runs AS (
    SELECT state, matches
    FROM iceberg.ops.espn_history_queue_v1
    WHERE slug = '(run)' AND {_IN_DAY}
)
SELECT (SELECT count(*) FROM seasons) AS seasons,
       (SELECT count_if(finished AND last_at >= {_DAY}
                        AND last_at < {_DAY} + INTERVAL '1' DAY) FROM seasons) AS finished_day,
       (SELECT count_if(finished) FROM seasons) AS finished,
       (SELECT count_if(red) FROM seasons)
         + (SELECT count_if(state = 'red') FROM iceberg.ops.espn_history_queue_v1
            WHERE slug <> '(run)' AND season_year = 0) AS red,
       (SELECT coalesce(sum(matches), 0) FROM runs) AS matches,
       (SELECT count_if(state = 'live_debt') FROM runs) AS pauses"""

# Network requests only: a raw-store hit or a closed lane has no attempt.
HISTORY_JOURNAL_SQL = """SELECT count(*) AS requests, coalesce(sum(direct_bytes), 0) AS bytes
FROM iceberg.ops.espn_request_journal_v1
WHERE lane = 'history' AND attempts > 0 AND request_date = DATE '{day}'"""


def _ts(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("time must be timezone-aware")
    return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")


def render_live_debt_sql(targets: Iterable[str], now: datetime) -> str:
    return LIVE_DEBT_SQL.format(
        targets=targets_literal(targets),
        since=_ts(now - DEBT_FROM),
        until=_ts(now - DEBT_TO),
    )


def render_live_debt_rows_sql(
    columns: Iterable[str], targets: Iterable[str], now: datetime
) -> str:
    """Debt rows under the exact same predicate as ``render_live_debt_sql``."""

    selected = ", ".join(columns)
    if not selected:
        raise ValueError("live-debt row query needs columns")
    return f"SELECT {selected} FROM iceberg.bronze.espn_match WHERE {_LIVE_DEBT_WHERE}".format(
        targets=targets_literal(targets),
        since=_ts(now - DEBT_FROM),
        until=_ts(now - DEBT_TO),
    )


def render_capability_sql() -> str:
    return HISTORY_CAPABILITY_SQL.format(
        share=CAPTURED_SHARE,
        full=FULL,
        no_lineups=NO_LINEUPS,
        no_stats=NO_STATS,
        no_both=NO_BOTH,
    )


def render_queue_sql(day: str) -> str:
    return HISTORY_QUEUE_SQL.format(day=_iso_day(day))


def render_journal_sql(day: str) -> str:
    return HISTORY_JOURNAL_SQL.format(day=_iso_day(day))


def empty_defects(rows: Sequence[Sequence]) -> int:
    """``valid_empty`` parts of seasons that do have that part (capability rows).

    A season "без составов" has no lineup defect however many empty lineups
    it has; the same for team statistics.
    """

    defects = 0
    for row in rows:
        lineup_empty, stats_empty, capability = int(row[4]), int(row[6]), row[7]
        if capability not in (NO_LINEUPS, NO_BOTH):
            defects += lineup_empty
        if capability not in (NO_STATS, NO_BOTH):
            defects += stats_empty
    return defects


def render_history_line(
    day: str,
    queue: Sequence | None,
    journal: Sequence | None,
    defects: int | None = None,
) -> str:
    """The history line of the morning report for UTC day ``day``."""

    dd = f"{day[8:10]}.{day[5:7]}"
    if queue is None or (int(queue[0]) == 0 and int(queue[3]) == 0):
        return f"• ESPN история {dd}: очередь пуста"
    seasons, finished_day, finished, red, matches, pauses = (int(value) for value in queue)
    requests, size = (int(value) for value in journal) if journal else (0, 0)
    per_match = (
        f" (≈ {requests / matches:.1f} запр. и {size / 1024 / matches:.0f} КБ на матч)"
        if matches
        else ""
    )
    line = (
        f"• ESPN история {dd}: сезонов готово за сутки {finished_day} "
        f"(всего готово {finished} из {seasons}), красных {red}, матчей {matches}, "
        f"запросов {requests}, {size / 1024:.0f} КБ{per_match}, "
        f"пауз из-за актуалки {pauses}"
    )
    if defects is not None:
        line += f"; пустых частей в полных сезонах {defects}"
    flag = "‼️" if red else "✅"
    return f"{line} {flag}"


__all__ = [
    "CAPTURED_SHARE",
    "DEBT_FROM",
    "DEBT_TO",
    "HISTORY_CAPABILITY_SQL",
    "HISTORY_JOURNAL_SQL",
    "HISTORY_QUEUE_SQL",
    "LIVE_DEBT_SQL",
    "empty_defects",
    "render_capability_sql",
    "render_history_line",
    "render_journal_sql",
    "render_live_debt_sql",
    "render_live_debt_rows_sql",
    "render_queue_sql",
]
