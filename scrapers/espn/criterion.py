"""Daily "played -> Bronze within 24 h" meter for ESPN (#1505).

One rule for the morning report, the watchdogs and the acceptance of
milestone 1 (roadmap task 10; R-02, R-09, R-18, R-19, R-40).  The morning
report and the acceptance reader call the same closed-day calculation.

* The closed day uses the independent archived scoreboards and the nightly
  core reconciliation for the target tournaments. Missing source proof makes
  the day unmeasured. A source match missing from Bronze remains due.
  Bronze duplicates stay outside the denominator.
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
  ``<= deadline``. If the source corrected the kickoff, a final result
  published before the corrected kickoff cannot establish timely coverage.
* Everything else in ``due`` is a miss: no data, late, a Summary still
  pending or malformed, and a match whose status was not read after
  ``kickoff + 2 h`` (``unchecked`` — a stopped collection shows up here
  instead of an empty denominator).
* The streak counts consecutive days with deadlines whose ``ok / due`` is at
  least 99 % (exact ratio, not the rounded display); measured days without
  deadlines (``due = 0``) are neutral and do not break it, an unmeasured day
  does.  Milestone 1 = a streak of 3.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Iterable, Optional, Sequence

from .bronze_schema import BRONZE_DATABASE, MATCH_TABLE
from .freshness_source import SourceDay, read_source_day
from .schedule_parser import STATUS_MAP
from .wave_log import WAVE_LOG_TABLE, WAVE_ROW

TARGET_PCT = Decimal("99")
MILESTONE_STREAK = 3
# Wave duration criterion of #1504: p95 (end - start) <= 60 min.
WAVE_P95_LIMIT_S = 3600
MATCH_RELATION = f"iceberg.{BRONZE_DATABASE}.{MATCH_TABLE}"

DAILY_CRITERION_SQL = """WITH
source (slug, id, source_kickoff, observed, source_played, source_nonplayed) AS (
    {source}
),
joined AS (
    SELECT coalesce(b.competition_slug, s.slug) AS competition_slug,
           coalesce(b.event_id, s.id) AS event_id, b.event_id IS NULL AS missing,
           b.kickoff AS bronze_kickoff, s.source_kickoff, s.observed,
           s.observed IS NOT NULL AND
               (b.status_checked_at IS NULL OR s.observed >= b.status_checked_at) AS use_source,
           s.source_played, s.source_nonplayed, b.status, b.played_final, b.terminal_nonplayed,
           b.disposition, b.lineup_state, b.team_stats_state, b.first_published_at, b.status_checked_at
    FROM (SELECT * FROM iceberg.bronze.espn_match
          WHERE competition_slug IN ({targets})
            AND ((kickoff + INTERVAL '26' HOUR >= TIMESTAMP '{day} 00:00:00'
                  AND kickoff + INTERVAL '26' HOUR < TIMESTAMP '{day} 00:00:00' + INTERVAL '1' DAY)
                 OR event_id IN (SELECT id FROM source))) b
    FULL JOIN source s ON b.competition_slug = s.slug AND b.event_id = s.id
    WHERE b.duplicate_of IS NULL
),
m AS (
    SELECT *, CASE WHEN use_source THEN source_kickoff ELSE bronze_kickoff END AS kickoff,
           coalesce(disposition = 'withdrawn', false) AND NOT use_source AS withdrawn,
           CASE WHEN use_source THEN source_nonplayed AND observed > source_kickoff
                ELSE coalesce((terminal_nonplayed OR status = 'STATUS_POSTPONED')
                              AND status_checked_at > bronze_kickoff, false) END AS nonplayed
    FROM joined
),
graded AS (
    SELECT *,
           NOT withdrawn AND NOT nonplayed AND kickoff IS NOT NULL AS due,
           coalesce(played_final AND (NOT use_source OR source_played)
                    AND (bronze_kickoff = kickoff OR first_published_at >= kickoff)
                    AND lineup_state IN ('captured', 'valid_empty')
                    AND team_stats_state IN ('captured', 'valid_empty')
                    AND first_published_at <= kickoff + INTERVAL '26' HOUR, false) AS ok,
           coalesce(NOT played_final, true) AND
               coalesce(status_checked_at < kickoff + INTERVAL '2' HOUR, true) AS unchecked
    FROM m
    WHERE kickoff IS NULL OR (kickoff + INTERVAL '26' HOUR >= TIMESTAMP '{day} 00:00:00'
      AND kickoff + INTERVAL '26' HOUR < TIMESTAMP '{day} 00:00:00' + INTERVAL '1' DAY)
)
SELECT competition_slug, count_if(due) AS due, count_if(due AND ok) AS ok,
       count_if(withdrawn) AS withdrawn, count_if(NOT withdrawn AND nonplayed) AS nonplayed,
       count_if(due AND unchecked) AS unchecked,
       array_agg(event_id ORDER BY event_id) FILTER (WHERE due AND missing) AS missing_ids,
       count_if(kickoff IS NULL) AS unlocated
FROM graded GROUP BY competition_slug ORDER BY competition_slug"""

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


def _timestamp(value):
    return "TIMESTAMP '{}'".format(
        value.astimezone(timezone.utc).replace(tzinfo=None).isoformat(" ")
    )


def render_daily_criterion_sql(
    day: str, targets: Iterable[str], source: SourceDay | None = None
) -> str:
    """Grade Bronze and the supplied source IDs for the same UTC deadline day."""
    targets = set(targets)
    values = []
    if source is not None:
        for fixture in source.fixtures.values():
            if fixture.slug not in targets:
                continue
            state = STATUS_MAP[fixture.status]
            nonplayed = state.terminal_nonplayed or fixture.status == "STATUS_POSTPONED"
            values.append(
                f"('{fixture.slug}', {int(fixture.event_id)}, {_timestamp(fixture.kickoff)}, "
                f"{_timestamp(fixture.observed)}, {state.played_final}, {nonplayed})"
            )
        for slug, eid in sorted(source.unlocated):
            if slug in targets:
                values.append(
                    f"('{slug}', {int(eid)}, CAST(NULL AS timestamp), "
                    "CAST(NULL AS timestamp), false, false)"
                )
    source_sql = (
        "VALUES " + ", ".join(values)
        if values
        else "SELECT CAST(NULL AS varchar), CAST(NULL AS bigint), CAST(NULL AS timestamp), "
        "CAST(NULL AS timestamp), false, false WHERE false"
    )
    return DAILY_CRITERION_SQL.format(
        day=_iso_day(day), targets=targets_literal(targets), source=source_sql
    )


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
    coverage_gaps: tuple[str, ...] = ()
    missing_ids: tuple[int, ...] = ()

    @property
    def pct(self) -> Optional[Decimal]:
        return None if self.coverage_gaps else pct(self.ok, self.due)

    @property
    def state(self) -> str:
        if self.coverage_gaps:
            return "unknown"
        return "measured" if self.due else "neutral"

    @property
    def eligible(self) -> bool:
        return not self.coverage_gaps and meets(self.ok, self.due)


def day_result(
    day: str, rows: Sequence[Sequence], source: SourceDay | None = None
) -> DayResult:
    """Only an explicitly verified source list can certify a measured day."""
    sums = [sum(int(row[index]) for row in rows) for index in range(1, 6)]
    gaps = set(source.gaps) if source is not None else {"нет сверки источника"}
    gaps.update(row[0] for row in rows if row[7])
    missing = sorted({int(eid) for row in rows for eid in (row[6] or ())})
    return DayResult(day, *sums, tuple(sorted(gaps)), tuple(missing))


def read_day(trino, day: str, targets) -> DayResult:
    targets = set(targets)
    source = read_source_day(trino, date.fromisoformat(day), targets)
    rows = trino.execute_query(render_daily_criterion_sql(day, targets, source))
    return day_result(day, rows, source)


def summarize_days(days: Iterable[DayResult]) -> int:
    """Streak of consecutive days with deadlines meeting the target.

    Walks back from the newest day; measured days with ``due = 0`` are
    neutral (skipped), the first day with deadlines below ``TARGET_PCT`` ends
    it, and so does a missing day: an unmeasured day is not a neutral one.
    """

    streak = 0
    expected: date | None = None
    for result in sorted(days, key=lambda item: item.day, reverse=True):
        day = date.fromisoformat(result.day)
        if expected is not None and day != expected:
            break
        expected = day - timedelta(days=1)
        if result.coverage_gaps:
            break
        if result.due <= 0:
            continue
        if not meets(result.ok, result.due):
            break
        streak += 1
    return streak


def _display_items(items, limit=8):
    text = ", ".join(map(str, items[:limit]))
    return text if len(items) <= limit else f"{text} и ещё {len(items) - limit}"


def render_summary_line(result: DayResult, streak: int) -> str:
    """The ESPN line of the morning report for one deadline day."""

    dd = f"{result.day[8:10]}.{result.day[5:7]}"
    extra = []
    if result.withdrawn:
        extra.append(f"снято с календаря {result.withdrawn}")
    if result.nonplayed:
        extra.append(f"перенесено/отменено {result.nonplayed}")
    tail = f" ({'; '.join(extra)})" if extra else ""
    if result.missing_ids:
        tail += " · нет в Bronze: " + _display_items(result.missing_ids)
    if result.coverage_gaps:
        return (
            f"• ESPN за сутки {dd}: список матчей не подтверждён "
            f"({_display_items(result.coverage_gaps)}), вовремя по известным "
            f"{result.ok}/{result.due}, серия 0 ⚠️{tail}"
        )
    if result.due <= 0:
        return (
            f"• ESPN за сутки {dd}: нет матчей со сроком публикации "
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
    "read_day",
    "report_packet",
    "render_daily_criterion_sql",
    "render_summary_line",
    "render_wave_duration_sql",
    "render_wave_line",
    "summarize_days",
    "targets_literal",
]


def report_packet(result: DayResult, prior_streak: int) -> dict:
    """One decision for the host cache and the milestone evidence."""
    state = result.state
    streak = 0
    if state == "neutral":
        streak = prior_streak
    elif result.eligible:
        streak = prior_streak + 1
    return {
        "day": result.day,
        "state": state,
        "streak": streak,
        "due": result.due,
        "ok": result.ok,
        "coverage_gaps": result.coverage_gaps,
        "missing_ids": result.missing_ids,
        "line": render_summary_line(result, streak),
    }


if __name__ == "__main__":
    import argparse
    import json
    from .denominator import load_denominator
    from .trino_manager import EspnTrinoTableManager

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("day", type=date.fromisoformat)
    parser.add_argument("prior_streak", type=int, nargs="?", default=0)
    args = parser.parse_args()
    if args.prior_streak < 0:
        parser.error("prior streak must be nonnegative")
    if args.day >= datetime.now(timezone.utc).date():
        parser.error("deadline day must be closed")
    with EspnTrinoTableManager() as trino:
        result = read_day(trino, args.day.isoformat(), load_denominator().targets())
    print(json.dumps(report_packet(result, args.prior_streak), ensure_ascii=False))
