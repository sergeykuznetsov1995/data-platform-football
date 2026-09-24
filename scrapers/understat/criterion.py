"""Daily "played -> Bronze within 24 h" meter for Understat (#1429).

Definition (grill 23.09, roadmap assumptions; the morning report copies the
SQL text below verbatim, so change it only together with that copy):

* A match is *played* when the published schedule (manifest fence: the latest
  manifest row of the scope is ``complete`` and ``_batch_id`` equals its
  ``batch_id``) has ``is_result = true``.  ``date`` is UTC for the current
  seasons, so the deadline is ``date + 26 h`` (24 h after a ~2 h match).
* The day key D is the UTC day of the deadline.
* *On time* (``ok``): a ``complete`` manifest attempt of the scope with
  ``kickoff < completed_at <= deadline`` covered the match
  (``quality_json.covered_game_ids`` = games with shots and player rows).
* *Site delay* (``site_late``): the match is not on time, at least one
  ``complete`` attempt ran between kickoff and deadline, and none of them saw
  the match marked played by the site (``site_result_game_ids``).  Site delays
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


TARGET_PCT = Decimal("99")

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

DAILY_CRITERION_SQL = (
    "WITH"
    + _FENCED_SCHEDULE
    + """,
due AS (
    SELECT league, season, game_id, kickoff, ordinal,
           kickoff + INTERVAL '26' HOUR AS deadline
    FROM played
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
per_game AS (
    SELECT d.league, d.game_id,
           count_if(a.done IS NOT NULL) AS attempts,
           count_if(
               CASE WHEN a.covered IS NOT NULL
                    THEN contains(a.covered, d.game_id)
                    ELSE a.cg >= d.ordinal END
           ) AS covered_hits,
           count_if(
               CASE WHEN a.site IS NOT NULL
                    THEN contains(a.site, d.game_id)
                    ELSE a.cg >= d.ordinal END
           ) AS site_hits
    FROM due d
    LEFT JOIN att a
      ON a.league = d.league AND a.season = d.season
     AND a.done > d.kickoff AND a.done <= d.deadline
    GROUP BY d.league, d.game_id
),
graded AS (
    SELECT league,
           covered_hits > 0 AS ok,
           covered_hits = 0 AND attempts > 0 AND site_hits = 0 AS site_late
    FROM per_game
)
SELECT league,
       count_if(NOT site_late) AS due,
       count_if(ok) AS ok,
       count_if(site_late) AS site_late
FROM graded
GROUP BY league
ORDER BY league"""
)

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


def render_daily_criterion_sql(day: str) -> str:
    """SQL for deadline day ``day`` (ISO ``YYYY-MM-DD``, UTC)."""
    return DAILY_CRITERION_SQL.format(day=_iso_day(day))


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
    the first day with deadlines below ``TARGET_PCT`` ends the streak.
    """
    streak = 0
    for result in sorted(days, key=lambda item: item.day, reverse=True):
        value = result.pct
        if value is None:
            continue
        if value < TARGET_PCT:
            break
        streak += 1
    return streak


__all__ = [
    "COMPLETENESS_SQL",
    "DAILY_CRITERION_SQL",
    "TARGET_PCT",
    "DayResult",
    "pct",
    "render_completeness_sql",
    "render_daily_criterion_sql",
    "summarize_days",
]
