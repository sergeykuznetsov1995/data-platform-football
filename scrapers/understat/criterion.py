"""Daily "played -> Bronze within 24 h" meter for Understat (#1429).

Definition (grill 23.09, roadmap assumptions; the morning report copies the
SQL text below verbatim, so change it only together with that copy):

* A match is *played* when the schedule of the latest ``complete`` attempt of
  the scope (``_batch_id`` equals its ``batch_id``) has ``is_result = true``,
  or when any attempt completed by its deadline listed it in
  ``site_result_game_ids`` (a complete attempt or a failures-journal row with
  ``site_result_known = true``) and that schedule has its row (kickoff).
  Not the consumers' fence (latest row must be ``complete``): the season
  schedule is known in advance, so a failed latest attempt must not drop its
  matches from ``due`` -- they stay due and count as our delay.
  ``date`` is UTC for the current seasons, so the deadline is
  ``date + 26 h`` (24 h after a ~2 h match).
* The day key D is the UTC day of the deadline.
* *On time* (``ok``): a ``complete`` manifest attempt of the scope with
  ``kickoff < completed_at <= deadline`` covered the match
  (``quality_json.covered_game_ids`` = games with shots and player rows).
* *Site delay* (``site_late``): the match is not on time, at least one
  ``complete`` attempt ran between kickoff and deadline, and no attempt in
  that window saw the match marked played by the site
  (``site_result_game_ids`` of complete manifest attempts or of failures
  journal rows with ``site_result_known = true``).  Site delays
  are subtracted from ``due`` and reported separately.  No complete attempt
  in that window at all means the delay is ours.
* Transitional rule: attempts written before the lists existed have neither
  list; for them "covered" and "seen by the site" both fall back to
  ``completed_game_count >= ordinal of the match by date in its scope``.
* The streak counts consecutive days with deadlines whose ``pct >= 99``;
  days without deadlines (``due = 0``) are neutral and do not break it.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Iterable, Optional

from scrapers.understat.manifest import FAILURES_TABLE, MANIFEST_SCHEMA


TARGET_PCT = Decimal("99")
FAILURES_RELATION = f"iceberg.{MANIFEST_SCHEMA}.{FAILURES_TABLE}"

_FENCED_SCHEDULE = """
m AS (
    SELECT league, season, batch_id, status
    FROM (
        SELECT league, season, batch_id, status,
               ROW_NUMBER() OVER (
                   PARTITION BY league, season
                   ORDER BY completed_at DESC, attempt_id DESC
               ) AS rn
        FROM iceberg.ops.understat_ingest_manifest_v1
        WHERE contract_version = 'understat-bronze-v2'
    )
    WHERE rn = 1
),
played AS (
    SELECT s.league, s.season, CAST(s.game_id AS varchar) AS game_id,
           s.date AS kickoff,
           count(*) OVER (
               PARTITION BY s.league, s.season ORDER BY s.date
           ) AS ordinal
    FROM iceberg.bronze.understat_schedule s
    JOIN m ON m.league = s.league AND m.season = s.season
    WHERE m.status = 'complete' AND s._batch_id = m.batch_id AND s.is_result
)"""

DAILY_CRITERION_SQL = """WITH
lc AS (
    SELECT league, season, batch_id
    FROM (
        SELECT league, season, batch_id,
               ROW_NUMBER() OVER (
                   PARTITION BY league, season
                   ORDER BY completed_at DESC, attempt_id DESC
               ) AS rn
        FROM iceberg.ops.understat_ingest_manifest_v1
        WHERE contract_version = 'understat-bronze-v2' AND status = 'complete'
    )
    WHERE rn = 1
),
sched AS (
    SELECT s.league, s.season, CAST(s.game_id AS varchar) AS game_id,
           s.date AS kickoff, s.is_result,
           CASE WHEN s.is_result THEN count_if(s.is_result) OVER (
               PARTITION BY s.league, s.season ORDER BY s.date
           ) END AS ordinal
    FROM iceberg.bronze.understat_schedule s
    JOIN lc ON lc.league = s.league AND lc.season = s.season
    WHERE s._batch_id = lc.batch_id
),
cand AS (
    SELECT league, season, game_id, kickoff, is_result, ordinal,
           kickoff + INTERVAL '26' HOUR AS deadline
    FROM sched
    WHERE kickoff + INTERVAL '26' HOUR >= TIMESTAMP '{day} 00:00:00'
      AND kickoff + INTERVAL '26' HOUR < TIMESTAMP '{day} 00:00:00' + INTERVAL '1' DAY
),
att AS (
    SELECT league, season,
           CAST(from_iso8601_timestamp(completed_at) AT TIME ZONE 'UTC'
                AS timestamp(6)) AS done,
           CAST(json_extract_scalar(quality_json, '$.completed_game_count')
                AS integer) AS cg,
           CAST(json_extract(quality_json, '$.covered_game_ids')
                AS array(varchar)) AS covered,
           CAST(json_extract(quality_json, '$.site_result_game_ids')
                AS array(varchar)) AS site
    FROM iceberg.ops.understat_ingest_manifest_v1
    WHERE contract_version = 'understat-bronze-v2' AND status = 'complete'
),
journal AS (
    SELECT f.league, f.season,
           CAST(from_iso8601_timestamp(f.completed_at) AT TIME ZONE 'UTC'
                AS timestamp(6)) AS done,
           CAST(json_extract(f.quality_json, '$.site_result_game_ids')
                AS array(varchar)) AS site
    FROM {failures} f
    WHERE f.contract_version = 'understat-bronze-v2'
      AND json_extract_scalar(f.quality_json, '$.site_result_known') = 'true'
),
seen AS (
    SELECT league, season, done, site FROM att WHERE site IS NOT NULL
    UNION ALL
    SELECT league, season, done, site FROM journal
),
seen_by_deadline AS (
    SELECT DISTINCT c.league, c.season, c.game_id
    FROM cand c
    JOIN seen v
      ON v.league = c.league AND v.season = c.season
     AND v.done <= c.deadline AND contains(v.site, c.game_id)
),
due AS (
    SELECT c.league, c.season, c.game_id, c.kickoff, c.ordinal, c.deadline
    FROM cand c
    LEFT JOIN seen_by_deadline sd
      ON sd.league = c.league AND sd.season = c.season AND sd.game_id = c.game_id
    WHERE c.is_result OR sd.game_id IS NOT NULL
),
journal_seen AS (
    SELECT d.league, d.game_id, count(*) AS hits
    FROM due d
    JOIN journal j
      ON j.league = d.league AND j.season = d.season
     AND j.done > d.kickoff AND j.done <= d.deadline
     AND contains(j.site, d.game_id)
    GROUP BY d.league, d.game_id
),
per_game AS (
    SELECT d.league, d.game_id,
           count_if(a.done IS NOT NULL) AS attempts,
           count_if(coalesce(
               CASE WHEN a.covered IS NOT NULL
                    THEN contains(a.covered, d.game_id)
                    ELSE a.cg >= d.ordinal END,
               false
           )) AS covered_hits,
           count_if(coalesce(
               CASE WHEN a.site IS NOT NULL
                    THEN contains(a.site, d.game_id)
                    ELSE a.cg >= d.ordinal END,
               false
           )) AS site_hits
    FROM due d
    LEFT JOIN att a
      ON a.league = d.league AND a.season = d.season
     AND a.done > d.kickoff AND a.done <= d.deadline
    GROUP BY d.league, d.game_id
),
graded AS (
    SELECT p.league,
           p.covered_hits > 0 AS ok,
           p.covered_hits = 0 AND p.attempts > 0
               AND p.site_hits = 0 AND coalesce(js.hits, 0) = 0 AS site_late
    FROM per_game p
    LEFT JOIN journal_seen js
      ON js.league = p.league AND js.game_id = p.game_id
)
SELECT league,
       count_if(NOT site_late) AS due,
       count_if(ok) AS ok,
       count_if(site_late) AS site_late
FROM graded
GROUP BY league
ORDER BY league"""

COMPLETENESS_SQL = (
    "WITH"
    + _FENCED_SCHEDULE
    + """,
covered AS (
    SELECT DISTINCT sh.league, CAST(sh.game_id AS varchar) AS game_id
    FROM iceberg.bronze.understat_shots sh
    JOIN m ON m.league = sh.league AND m.season = sh.season
    WHERE m.status = 'complete' AND sh._batch_id = m.batch_id
      AND sh.season = '{season}'
    INTERSECT
    SELECT DISTINCT pm.league, CAST(pm.game_id AS varchar) AS game_id
    FROM iceberg.bronze.understat_player_match_stats pm
    JOIN m ON m.league = pm.league AND m.season = pm.season
    WHERE m.status = 'complete' AND pm._batch_id = m.batch_id
      AND pm.season = '{season}'
)
SELECT p.league,
       count(*) AS played,
       count(c.game_id) AS have
FROM played p
LEFT JOIN covered c ON c.league = p.league AND c.game_id = p.game_id
WHERE p.season = '{season}'
GROUP BY p.league
ORDER BY p.league"""
)


def render_daily_criterion_sql(day: str, failures: str = FAILURES_RELATION) -> str:
    """SQL for deadline day ``day`` (ISO ``YYYY-MM-DD``, UTC).

    ``failures`` is the failures-journal relation; a caller may pass an empty
    derived table with the manifest columns while the journal does not exist.
    """
    return DAILY_CRITERION_SQL.format(day=_iso_day(day), failures=failures)


def render_completeness_sql(season: str) -> str:
    """SQL for the current-season completeness line (``season`` like 2627)."""
    if not (len(season) == 4 and season.isdigit()):
        raise ValueError(f"season must be a four-digit slug, got {season!r}")
    return COMPLETENESS_SQL.format(season=season)


def _iso_day(day: str) -> str:
    parts = day.split("-")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise ValueError(f"day must be YYYY-MM-DD, got {day!r}")
    return day


def pct(ok: int, due: int) -> Optional[Decimal]:
    """Percentage with one decimal, ROUND_HALF_UP; ``None`` when nothing is due."""
    if due <= 0:
        return None
    return (Decimal(100) * Decimal(ok) / Decimal(due)).quantize(
        Decimal("0.1"), rounding=ROUND_HALF_UP
    )


@dataclass(frozen=True)
class DayResult:
    day: str
    due: int
    ok: int
    site_late: int = 0

    @property
    def pct(self) -> Optional[Decimal]:
        return pct(self.ok, self.due)


def summarize_days(days: Iterable[DayResult]) -> int:
    """Streak of consecutive days with deadlines meeting the target.

    Walks back from the newest day; days with ``due = 0`` are neutral (skipped),
    the first day with deadlines below ``TARGET_PCT`` ends the streak.  The
    threshold is checked on the exact ratio, not on the rounded display value
    (95/96 = 98.96 % rounds to 99.0 but misses the target).
    """
    streak = 0
    for result in sorted(days, key=lambda item: item.day, reverse=True):
        if result.due <= 0:
            continue
        if Decimal(100) * result.ok < TARGET_PCT * result.due:
            break
        streak += 1
    return streak


__all__ = [
    "COMPLETENESS_SQL",
    "DAILY_CRITERION_SQL",
    "FAILURES_RELATION",
    "TARGET_PCT",
    "DayResult",
    "pct",
    "render_completeness_sql",
    "render_daily_criterion_sql",
    "summarize_days",
]
