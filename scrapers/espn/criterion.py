"""Daily "played -> Bronze within 24 h" meter for ESPN (#1505).

One rule for the morning report, the watchdogs and the acceptance of
milestone 1 (roadmap task 10; R-02, R-09, R-18, R-19, R-40).  The morning
report copies the SQL texts below verbatim, so change them only together with
that copy (package ``/root/espn-deliveries/1505/summary``).

* The denominator is ``iceberg.bronze.espn_match``: every row of a target
  tournament (``senior_official`` of ``configs/espn/denominator.tsv``,
  ``Denominator.targets()``) with ``duplicate_of IS NULL``.  The failure unit
  is the match, not the tournament (R-40): a tournament without matches adds
  nothing.  Youth, olympic, friendly and women tournaments are outside.
* ``kickoff`` is the latest known one (a moved match counts from its new
  kickoff); the deadline is ``kickoff + 26 h`` (the match ends ~2 h after
  kickoff, plus 24 h).  The day key D is the UTC day of the deadline.
* Out of ``due``: ``disposition = 'withdrawn'`` (core answers 404; counted on
  its own line) and a match that is not played, confirmed after kickoff:
  ``terminal_nonplayed`` (CANCELED, ABANDONED, FORFEIT, WALKOVER) or
  ``STATUS_POSTPONED`` with ``status_checked_at > kickoff``.
* *On time* (``ok``): ``played_final``, ``lineup_state`` and
  ``team_stats_state`` in (captured, valid_empty) and ``first_published_at``
  (start of the match commit of the batch that first published the match
  that way, after its children; a republication never moves it)
  ``<= deadline``.
* Everything else in ``due`` is a miss: no data, late, a Summary still
  pending or malformed, and a match whose status was not read after
  ``kickoff + 2 h`` (``unchecked`` — a stopped collection shows up here
  instead of an empty denominator).
* The streak counts consecutive days with deadlines whose ``ok / due`` is at
  least 99 % (exact ratio, not the rounded display); days without deadlines
  (``due = 0``) are neutral and do not break it.  Milestone 1 = a streak of 3.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Iterable, Optional, Sequence

from .bronze_schema import BRONZE_DATABASE, MATCH_TABLE
from .wave_log import WAVE_LOG_TABLE, WAVE_ROW

TARGET_PCT = Decimal("99")
MILESTONE_STREAK = 3
# Wave duration criterion of #1504: p95 (end - start) <= 60 min.
WAVE_P95_LIMIT_S = 3600
MATCH_RELATION = f"iceberg.{BRONZE_DATABASE}.{MATCH_TABLE}"

DAILY_CRITERION_SQL = """WITH
m AS (
    SELECT competition_slug, event_id, kickoff, status, played_final,
           terminal_nonplayed, disposition, lineup_state, team_stats_state,
           first_published_at, status_checked_at,
           kickoff + INTERVAL '26' HOUR AS deadline
    FROM iceberg.bronze.espn_match
    WHERE duplicate_of IS NULL
      AND competition_slug IN ({targets})
      AND kickoff + INTERVAL '26' HOUR >= TIMESTAMP '{day} 00:00:00'
      AND kickoff + INTERVAL '26' HOUR < TIMESTAMP '{day} 00:00:00' + INTERVAL '1' DAY
),
graded AS (
    SELECT competition_slug,
           coalesce(disposition = 'withdrawn', false) AS withdrawn,
           NOT coalesce(disposition = 'withdrawn', false)
               AND coalesce(
                   (terminal_nonplayed OR status = 'STATUS_POSTPONED')
                   AND status_checked_at > kickoff,
                   false
               ) AS nonplayed,
           coalesce(
               played_final
               AND lineup_state IN ('captured', 'valid_empty')
               AND team_stats_state IN ('captured', 'valid_empty')
               AND first_published_at <= deadline,
               false
           ) AS ok,
           coalesce(
               NOT played_final
               AND coalesce(status_checked_at < kickoff + INTERVAL '2' HOUR, true),
               false
           ) AS unchecked
    FROM m
)
SELECT competition_slug,
       count_if(NOT withdrawn AND NOT nonplayed) AS due,
       count_if(NOT withdrawn AND NOT nonplayed AND ok) AS ok,
       count_if(withdrawn) AS withdrawn,
       count_if(nonplayed) AS nonplayed,
       count_if(NOT withdrawn AND NOT nonplayed AND unchecked) AS unchecked
FROM graded
GROUP BY competition_slug
ORDER BY competition_slug"""

# Waves of the UTC day D (the wave row of the wave log, #1504 criterion 3).
WAVE_DURATION_SQL = """SELECT count(*) AS waves,
       count_if(state = 'red') AS red_waves,
       CAST(approx_percentile(duration_s, 0.95) AS bigint) AS p95_s,
       max(duration_s) AS max_s
FROM (
    SELECT state,
           date_diff('second', wave_started_at, wave_finished_at) AS duration_s
    FROM iceberg.ops.espn_wave_tournament_v1
    WHERE slug = '(wave)'
      AND wave_started_at >= TIMESTAMP '{day} 00:00:00'
      AND wave_started_at < TIMESTAMP '{day} 00:00:00' + INTERVAL '1' DAY
) w"""

assert MATCH_RELATION in DAILY_CRITERION_SQL
assert WAVE_LOG_TABLE in WAVE_DURATION_SQL and f"'{WAVE_ROW}'" in WAVE_DURATION_SQL


def _iso_day(day: str) -> str:
    parts = day.split("-")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise ValueError(f"day must be YYYY-MM-DD, got {day!r}")
    return day


def targets_literal(targets: Iterable[str]) -> str:
    """``'a', 'b'`` for the ``IN`` list; an empty target set is an error."""

    slugs = sorted(set(targets))
    if not slugs:
        raise ValueError("the target set is empty")
    for slug in slugs:
        if not slug or "'" in slug:
            raise ValueError(f"bad slug {slug!r}")
    return ", ".join(f"'{slug}'" for slug in slugs)


def render_daily_criterion_sql(day: str, targets: Iterable[str]) -> str:
    """SQL for deadline day ``day`` (ISO ``YYYY-MM-DD``, UTC)."""

    return DAILY_CRITERION_SQL.format(day=_iso_day(day), targets=targets_literal(targets))


def render_wave_duration_sql(day: str) -> str:
    return WAVE_DURATION_SQL.format(day=_iso_day(day))


def pct(ok: int, due: int) -> Optional[Decimal]:
    """Percentage with one decimal, ROUND_HALF_UP; ``None`` when nothing is due."""

    if due <= 0:
        return None
    return (Decimal(100) * Decimal(ok) / Decimal(due)).quantize(
        Decimal("0.1"), rounding=ROUND_HALF_UP
    )


def meets(ok: int, due: int) -> bool:
    """The 99 % threshold on the exact ratio (95/96 rounds to 99.0 but misses)."""

    return due > 0 and Decimal(100) * ok >= TARGET_PCT * due


@dataclass(frozen=True)
class DayResult:
    day: str
    due: int
    ok: int
    withdrawn: int = 0
    nonplayed: int = 0
    unchecked: int = 0

    @property
    def pct(self) -> Optional[Decimal]:
        return pct(self.ok, self.due)


def day_result(day: str, rows: Sequence[Sequence]) -> DayResult:
    """Rows of ``DAILY_CRITERION_SQL`` (per tournament) -> one day."""

    sums = [sum(int(row[index]) for row in rows) for index in range(1, 6)]
    return DayResult(day, *sums)


def summarize_days(days: Iterable[DayResult]) -> int:
    """Streak of consecutive days with deadlines meeting the target.

    Walks back from the newest day; days with ``due = 0`` are neutral
    (skipped), the first day with deadlines below ``TARGET_PCT`` ends it.
    """

    streak = 0
    for result in sorted(days, key=lambda item: item.day, reverse=True):
        if result.due <= 0:
            continue
        if not meets(result.ok, result.due):
            break
        streak += 1
    return streak


def render_summary_line(result: DayResult, streak: int) -> str:
    """The ESPN line of the morning report for one deadline day."""

    dd = f"{result.day[8:10]}.{result.day[5:7]}"
    extra = []
    if result.withdrawn:
        extra.append(f"снято с календаря {result.withdrawn}")
    if result.nonplayed:
        extra.append(f"перенесено/отменено {result.nonplayed}")
    tail = f" ({'; '.join(extra)})" if extra else ""
    if result.due <= 0:
        return (
            f"• ESPN за сутки {dd}: нет сыгранных в новых таблицах ⚠️ "
            f"(серия {streak}){tail}"
        )
    flag = "✅" if meets(result.ok, result.due) else "‼️"
    line = (
        f"• ESPN: сыгранных за сутки {dd} {result.due}, ≤ 24 ч {result.pct:.1f} % "
        f"({result.ok}/{result.due}), серия {streak} дн. "
        f"(веха 1: {MILESTONE_STREAK} дня ≥ {TARGET_PCT} %) {flag}"
    )
    if result.unchecked:
        line += f"; статус не перепроверен после kickoff + 2 ч: {result.unchecked}"
    return line + tail


def render_wave_line(day: str, row: Sequence | None) -> str:
    """p95 duration of the waves of ``day`` (``WAVE_DURATION_SQL`` row)."""

    dd = f"{day[8:10]}.{day[5:7]}"
    if row is None or int(row[0]) == 0:
        return f"• ESPN волны {dd}: волн нет"
    waves, red, p95, longest = (int(value) for value in row)
    flag = "✅" if p95 <= WAVE_P95_LIMIT_S else "‼️"
    return (
        f"• ESPN волны {dd}: {waves}, красных {red}, p95 {p95 // 60} мин "
        f"(макс. {longest // 60} мин; порог {WAVE_P95_LIMIT_S // 60}) {flag}"
    )


__all__ = [
    "DAILY_CRITERION_SQL",
    "MILESTONE_STREAK",
    "TARGET_PCT",
    "WAVE_DURATION_SQL",
    "WAVE_P95_LIMIT_S",
    "DayResult",
    "day_result",
    "meets",
    "pct",
    "render_daily_criterion_sql",
    "render_summary_line",
    "render_wave_duration_sql",
    "render_wave_line",
    "summarize_days",
    "targets_literal",
]
