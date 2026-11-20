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
* If the complete schedule has been replaced, only its successor's matched
  physical rows can supply kickoffs. Played IDs come from source lists.
  Missing evidence makes the affected day unmeasured and interrupts the streak.
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
-- Bound undated evidence by the ordinary July-to-June season. Actual dates
-- also include matches in extended seasons.
seasons AS (
    SELECT DISTINCT substr(CAST(year(k - INTERVAL '6' MONTH) AS varchar), 3, 2)
                  || substr(CAST(year(k + INTERVAL '6' MONTH) AS varchar), 3, 2) AS season
    FROM (VALUES (TIMESTAMP '{day} 00:00:00' - INTERVAL '26' HOUR),
                 (TIMESTAMP '{day} 00:00:00' - INTERVAL '2' HOUR)) AS bounds(k)
),
dated_scopes AS (
    SELECT DISTINCT league, season
    FROM iceberg.bronze.understat_schedule
    WHERE date >= TIMESTAMP '{day} 00:00:00' - INTERVAL '26' HOUR
      AND date < TIMESTAMP '{day} 00:00:00' - INTERVAL '2' HOUR
),
manifest AS (
    SELECT m.*,
           CAST(from_iso8601_timestamp(completed_at) AT TIME ZONE 'UTC'
                AS timestamp(6)) AS done
    FROM iceberg.ops.understat_ingest_manifest_v1 m
    LEFT JOIN dated_scopes d ON d.league = m.league AND d.season = m.season
    WHERE contract_version = 'understat-bronze-v2'
      AND (m.season IN (SELECT season FROM seasons) OR d.league IS NOT NULL)
),
ranked AS (
    SELECT m.*, row_number() OVER (PARTITION BY league, season
                   ORDER BY done DESC, attempt_id DESC) AS latest,
           row_number() OVER (PARTITION BY league, season, batch_id
                   ORDER BY done DESC, attempt_id DESC) AS in_batch,
           max(CASE WHEN status = 'complete' THEN done END)
               OVER (PARTITION BY league, season) AS last_complete
    FROM manifest m
),
observations AS (
    SELECT league, season, batch_id, attempt_id, done, status = 'complete' AS complete,
           CAST(json_extract_scalar(quality_json, '$.completed_game_count')
                AS integer) AS cg,
           CAST(json_extract(quality_json, '$.covered_game_ids')
                AS array(varchar)) AS covered,
           CASE WHEN status = 'upstream_pending' THEN CAST(ARRAY[] AS array(varchar))
                WHEN coalesce(json_extract_scalar(quality_json, '$.site_result_known'), 'true') = 'true'
                THEN CAST(json_extract(quality_json, '$.site_result_game_ids') AS array(varchar))
           END AS site
    FROM manifest WHERE status IN ('complete', 'upstream_pending')
    UNION ALL
    SELECT f.league, f.season, f.batch_id, f.attempt_id,
           CAST(from_iso8601_timestamp(f.completed_at) AT TIME ZONE 'UTC'
                AS timestamp(6)) AS done,
           false AS complete, NULL AS cg, NULL AS covered,
           CAST(json_extract(f.quality_json, '$.site_result_game_ids')
                AS array(varchar)) AS site
    FROM {failures} f
    LEFT JOIN dated_scopes d ON d.league = f.league AND d.season = f.season
    WHERE f.contract_version = 'understat-bronze-v2'
      AND (f.season IN (SELECT season FROM seasons) OR d.league IS NOT NULL)
      AND json_extract_scalar(f.quality_json, '$.site_result_known') = 'true'
),
first_seen AS (
    SELECT league, season, game_id, min(done) AS seen_at
    FROM observations CROSS JOIN UNNEST(site) AS ids(game_id)
    GROUP BY league, season, game_id
),
known_attempts AS (
    SELECT league, season, batch_id,
           CASE WHEN NOT bool_or(complete) AND min(attempt_id) = max(attempt_id)
                     AND count(DISTINCT site) = 1 THEN min(attempt_id) END AS unpublished_attempt,
           max(max(done)) OVER (PARTITION BY league, season) AS last_list
    FROM observations WHERE site IS NOT NULL GROUP BY league, season, batch_id
),
physical AS (
    SELECT m.league, m.season, m.done, j.last_list, m.status = 'complete' AS certified,
           s._batch_id IS NOT NULL AND (m.status = 'complete' OR j.unpublished_attempt IS NOT NULL)
               AND min(s._batch_id) OVER (PARTITION BY m.league, m.season)
                 = max(s._batch_id) OVER (PARTITION BY m.league, m.season) AS usable,
           CAST(s.game_id AS varchar) AS game_id, s.date AS kickoff, s.is_result,
           dense_rank() OVER (PARTITION BY m.league, m.season ORDER BY
               CASE WHEN m.status = 'complete' AND s._batch_id IS NOT NULL THEN 0
                    WHEN m.latest = 1 THEN 1 ELSE 2 END) AS preference
    FROM ranked m
    LEFT JOIN iceberg.bronze.understat_schedule s
      ON s.league = m.league AND s.season = m.season AND s._batch_id = m.batch_id
    LEFT JOIN known_attempts j
      ON j.league = m.league AND j.season = m.season AND j.batch_id = m.batch_id
     -- A surviving write marker and its terminal failure have different attempt IDs.
     AND (j.unpublished_attempt = m.attempt_id OR m.status = 'in_progress')
    -- Ignore stale rows from generations preceding the last publication.
    -- Several surviving candidate generations cannot establish a single calendar.
    WHERE m.in_batch = 1 AND (m.latest = 1 OR (s._batch_id IS NOT NULL
          AND (m.last_complete IS NULL OR m.done >= m.last_complete)))
),
games AS (
    SELECT league, season, game_id, certified, usable, done, last_list,
           min(kickoff) AS kickoff, max(kickoff) AS last_kickoff,
           count(*) = 1 AND count(kickoff) = 1 AND game_id IS NOT NULL AS unique_kickoff,
           count(kickoff) < count(*) AS missing_kickoff,
           count_if(kickoff >= TIMESTAMP '{day} 00:00:00' - INTERVAL '26' HOUR
                AND kickoff < TIMESTAMP '{day} 00:00:00' - INTERVAL '2' HOUR) > 0 AS in_day,
           certified AND bool_or(coalesce(is_result, false)) AS played
    FROM physical WHERE preference = 1
    GROUP BY league, season, game_id, certified, usable, done, last_list
),
schedule AS (
    SELECT g.*, CASE WHEN played THEN count_if(played) OVER (
               PARTITION BY league, season ORDER BY kickoff
           ) END AS ordinal
    FROM games g
),
facts AS (
    SELECT coalesce(g.league, v.league) AS league, coalesce(g.season, v.season) AS season,
           coalesce(g.game_id, v.game_id) AS game_id, g.kickoff, g.ordinal,
           g.kickoff + INTERVAL '26' HOUR AS deadline,
           g.usable, g.played, g.certified, g.last_list, v.seen_at, g.in_day,
           CASE WHEN g.played AND (v.seen_at IS NULL OR g.done < v.seen_at)
                THEN g.done ELSE v.seen_at END AS first_played,
           coalesce(g.missing_kickoff, true) OR
               (NOT g.certified AND v.seen_at < g.last_kickoff) AS date_unknown,
           coalesce(g.unique_kickoff, false)
               AND (g.certified OR v.seen_at IS NULL OR v.seen_at >= g.kickoff) AS located
    FROM schedule g FULL JOIN first_seen v
      ON v.league = g.league AND v.season = g.season AND v.game_id = g.game_id
),
assessment AS (
    SELECT f.*,
           coalesce(usable AND located AND in_day AND (played OR seen_at <= deadline),
                    false) AS due_game,
           CASE
               WHEN NOT usable THEN true
               WHEN NOT certified AND in_day AND last_list < kickoff THEN true
               -- A missing ID/date can belong to any earlier deadline day in
               -- this season, up to the first reliable observation plus 26 h.
               WHEN NOT located AND (seen_at IS NOT NULL OR played OR game_id IS NULL)
               THEN in_day OR (date_unknown AND (first_played IS NULL OR
                                TIMESTAMP '{day} 00:00:00' <= first_played + INTERVAL '26' HOUR))
               ELSE false
           END AS uncertain
    FROM facts f
),
per_game AS (
    SELECT d.league, d.season, d.game_id, d.due_game, d.uncertain,
           count_if(coalesce(a.complete, false)) AS attempts,
           count_if(coalesce(a.complete AND CASE WHEN a.covered IS NOT NULL
               THEN contains(a.covered, d.game_id) ELSE a.cg >= d.ordinal END, false)) AS covered_hits,
           count_if(coalesce(CASE WHEN a.site IS NOT NULL
               THEN contains(a.site, d.game_id) ELSE a.cg >= d.ordinal END, false)) AS site_hits,
           count_if(coalesce(NOT d.certified AND a.complete AND a.cg > 0
                    AND (a.covered IS NULL OR (a.site IS NULL AND NOT d.due_game)),
                    false)) AS uncertain_legacy
    FROM assessment d
    LEFT JOIN observations a ON d.in_day AND d.located AND d.usable
      AND a.league = d.league AND a.season = d.season
      AND a.done > d.kickoff AND a.done <= d.deadline
    WHERE d.due_game OR d.uncertain OR (NOT d.certified AND d.in_day)
    GROUP BY d.league, d.season, d.game_id, d.due_game, d.uncertain
),
graded AS (
    SELECT p.*,
           p.covered_hits > 0 AS ok,
           p.covered_hits = 0 AND p.attempts > 0 AND p.site_hits = 0 AS site_late
    FROM per_game p
)
SELECT league,
       count_if(due_game AND NOT site_late) AS due,
       count_if(due_game AND ok) AS ok,
       count_if(due_game AND site_late) AS site_late,
       CASE WHEN count_if(uncertain OR (uncertain_legacy > 0 AND NOT (due_game AND ok))) > 0
            THEN 0 ELSE 1 END AS measured
FROM graded
GROUP BY league
UNION ALL
-- No publication evidence at all cannot establish a neutral day.
SELECT CAST(NULL AS varchar), 0, 0, 0, 0
WHERE NOT EXISTS (SELECT 1 FROM manifest)
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

QUARANTINE_SQL = """WITH
latest AS (
    SELECT league, status, quality_json
    FROM (
        SELECT league, season, status, quality_json,
               ROW_NUMBER() OVER (
                   PARTITION BY league, season
                   ORDER BY completed_at DESC, attempt_id DESC
               ) AS rn
        FROM iceberg.ops.understat_ingest_manifest_v1
        WHERE contract_version = 'understat-bronze-v2'
          AND season = '{season}'
    )
    WHERE rn = 1 AND status = 'complete'
),
quarantine AS (
    SELECT latest.league,
           json_extract_scalar(item, '$.game_id') AS game_id,
           json_extract_scalar(item, '$.state') AS state,
           json_extract_scalar(item, '$.first_seen_at') AS first_seen_at,
           json_extract_scalar(item, '$.last_checked_at') AS last_checked_at,
           json_extract_scalar(item, '$.retry_until') AS retry_until
    FROM latest
    CROSS JOIN UNNEST(
        COALESCE(
            CAST(
                json_extract(quality_json, '$.match_quarantine')
                AS array(json)
            ),
            CAST(ARRAY[] AS array(json))
        )
    ) AS records (item)
)
SELECT league, game_id, state, first_seen_at, last_checked_at, retry_until
FROM quarantine
ORDER BY league, game_id"""


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


def render_quarantine_sql(season: str) -> str:
    """SQL for current quarantine records in ``season`` (such as ``2627``)."""
    if not (len(season) == 4 and season.isdigit()):
        raise ValueError(f"season must be a four-digit slug, got {season!r}")
    return QUARANTINE_SQL.format(season=season)


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
    measured: bool = True

    @property
    def pct(self) -> Optional[Decimal]:
        return pct(self.ok, self.due) if self.measured else None


def summarize_days(days: Iterable[DayResult]) -> int:
    """Streak of consecutive days with deadlines meeting the target.

    Walks back from the newest day. Measured days with ``due = 0`` are neutral.
    An unmeasured day or a day below ``TARGET_PCT`` ends the streak. The
    threshold is checked on the exact ratio, not on the rounded display value
    (95/96 = 98.96 % rounds to 99.0 but misses the target).
    """
    streak = 0
    for result in sorted(days, key=lambda item: item.day, reverse=True):
        if not result.measured:
            break
        if result.due <= 0:
            continue
        if Decimal(100) * result.ok < TARGET_PCT * result.due:
            break
        streak += 1
    return streak


__all__ = [
    "compare_fotmob",
    "render_anomaly_sql",
    "render_fotmob_diagnostic_sql",
    "source_diagnostic_summary",
    "COMPLETENESS_SQL",
    "DAILY_CRITERION_SQL",
    "FAILURES_RELATION",
    "QUARANTINE_SQL",
    "TARGET_PCT",
    "DayResult",
    "pct",
    "render_completeness_sql",
    "render_daily_criterion_sql",
    "render_quarantine_sql",
    "summarize_days",
]


# Independent diagnostics: do not alter the daily deadline/completeness meter.
FOTMOB_COMPETITIONS = {
    'ENG-Premier League': 47, 'ESP-La Liga': 87, 'GER-Bundesliga': 54,
    'ITA-Serie A': 55, 'FRA-Ligue 1': 53, 'RUS-Premier League': 63,
}


def render_anomaly_sql(entity: str, season: str) -> str:
    """Published rows across all six leagues, fenced by the latest attempt."""
    allowed = ('understat_schedule', 'understat_player_match_stats', 'understat_shots')
    if entity not in allowed:
        raise ValueError('unsupported diagnostic entity')
    render_completeness_sql(season)  # shared season validation
    return f"""WITH latest AS (
        SELECT league, season, batch_id, status, completed_at,
               row_number() OVER (PARTITION BY league, season
                   ORDER BY completed_at DESC, attempt_id DESC) AS rn
        FROM iceberg.ops.understat_ingest_manifest_v1
        WHERE contract_version = 'understat-bronze-v2' AND season = '{season}'
    )
    SELECT s.*, m.completed_at AS diagnostic_snapshot_at
    FROM iceberg.bronze.{entity} s
    JOIN latest m ON s.league = m.league AND s.season = m.season
        AND s._batch_id = m.batch_id
    WHERE m.rn = 1 AND m.status = 'complete'
      AND s.league IN ({', '.join(repr(league) for league in FOTMOB_COMPETITIONS)})"""


def render_fotmob_diagnostic_sql(season: str) -> str:
    """Read FotMob's published current view, including snapshot timestamps."""
    render_completeness_sql(season)
    year = 2000 + int(season[:2])
    return f"""SELECT competition_id, source_season_key, match_id, utc_time,
        finished, home_team_name, away_team_name, home_score, away_score,
        _ingested_at AS diagnostic_snapshot_at
    FROM iceberg.bronze.fotmob_matches_current
    WHERE CAST(competition_id AS varchar) IN ('47', '87', '54', '55', '53', '63')
      AND source_season_key = '{year}/{year + 1}'"""


def compare_fotmob(schedule, fotmob, *, team_aliases=None) -> str:
    """Format counts and scores from DataFrames; None means unavailable.

    Match only unique league/season/UTC-date/home/away keys. Exact normalized
    names are the default; reviewed aliases may map (league, source name) to
    a shared team name. Unmatched or ambiguous rows remain visible.
    """
    from collections import defaultdict
    import pandas as pd

    aliases = team_aliases or {}

    def name(league, value):
        if pd.isna(value):
            return None
        return str(aliases.get((league, str(value)), value)).strip().casefold()

    def key(row, other=False):
        league = row.get('league')
        if other:
            league = next((key for key, value in FOTMOB_COMPETITIONS.items()
                           if str(value) == str(row.get('competition_id'))), None)
        stamp = pd.to_datetime(row.get('utc_time' if other else 'date'), utc=True, errors='coerce')
        season = str(row.get('source_season_key')) if other else str(row.get('season'))
        if other:
            parts = season.split('/')
            season = ''.join(part[-2:] for part in parts) if len(parts) == 2 else None
        home = name(league, row.get('home_team_name' if other else 'home_team'))
        away = name(league, row.get('away_team_name' if other else 'away_team'))
        if league is None or pd.isna(stamp) or not home or not away or season is None:
            return None
        return league, season, str(stamp.date()), home, away

    def played(frame, flag):
        if frame is None or frame.empty:
            return None
        return [row for row in frame.to_dict('records')
                if not pd.isna(row.get(flag)) and row.get(flag) == True]

    under = played(schedule, 'is_result')
    other = played(fotmob, 'finished')

    def snapshot(frame):
        if frame is None or frame.empty or 'diagnostic_snapshot_at' not in frame:
            return 'n/a'
        values = frame['diagnostic_snapshot_at'].dropna().astype(str)
        return ','.join(sorted(set(values))) if len(values) else 'n/a'

    expected = set(FOTMOB_COMPETITIONS)
    u_scopes = set() if schedule is None or 'league' not in schedule else set(schedule['league'].dropna())
    f_scopes = set() if fotmob is None or 'competition_id' not in fotmob else {
        league for league, competition in FOTMOB_COMPETITIONS.items()
        if str(competition) in set(fotmob['competition_id'].dropna().astype(str))}
    missing_scopes = f"; missing_scopes U={sorted(expected - u_scopes)}, F={sorted(expected - f_scopes)}"
    prefix = f"FotMob: played Understat={len(under) if under is not None else 'n/a'}, FotMob={len(other) if other is not None else 'n/a'}"
    times = f"; snapshots U={snapshot(schedule)}, F={snapshot(fotmob)}"
    if under is None or other is None or not under or not other:
        return prefix + '; delta=n/a; scores=n/a; unmatched=n/a' + times + missing_scopes
    indexes = []
    for rows, is_other in ((under, False), (other, True)):
        index = defaultdict(list)
        for row in rows:
            index[key(row, is_other)].append(row)
        indexes.append(index)
    left, right = indexes
    matched = 0
    mismatches = []
    unavailable = 0
    for identity, rows in left.items():
        candidates = right.get(identity, [])
        if identity is None or len(rows) != 1 or len(candidates) != 1:
            continue
        matched += 1
        u, f = rows[0], candidates[0]
        us = [pd.to_numeric(u.get(col), errors='coerce') for col in ('home_goals', 'away_goals')]
        fs = [pd.to_numeric(f.get(col), errors='coerce') for col in ('home_score', 'away_score')]
        if any(pd.isna(value) for value in (*us, *fs)):
            unavailable += 1
        elif [int(value) for value in us] != [int(value) for value in fs]:
            mismatches.append(f"{u['game_id']}/{f['match_id']} {us}/{fs}")
    complete = expected <= u_scopes and expected <= f_scopes
    delta = f"{len(under) - len(other):+d}" if complete else 'n/a'
    per_league = {}
    for league, competition in FOTMOB_COMPETITIONS.items():
        if league not in u_scopes or league not in f_scopes:
            per_league[league] = 'n/a'
        else:
            uc = sum(row.get('league') == league for row in under)
            fc = sum(str(row.get('competition_id')) == str(competition) for row in other)
            per_league[league] = f"U={uc},F={fc},delta={uc - fc:+d}"
    return (prefix + f'; delta={delta}; matched={matched}; '
            f'scores={len(mismatches)} [{", ".join(mismatches)}]; scores_n/a={unavailable}; '
            f'unmatched U={len(under) - matched}, F={len(other) - matched}' + times
            + missing_scopes + f'; leagues={per_league}')


def source_diagnostic_summary(query_frame, season: str, *, team_aliases=None) -> str:
    """Morning-report entry point; query_frame(SELECT) returns a DataFrame.

    Failed/unavailable reads produce n/a, never a fabricated zero. No writes,
    DDL or source HTTP requests. All-league clone detection runs once.
    """
    import pandas as pd
    from scrapers.understat.anomalies import diagnose_matches

    render_completeness_sql(season)
    frames = {}
    missing = []
    for entity in ('understat_schedule', 'understat_player_match_stats', 'understat_shots'):
        try:
            frames[entity] = query_frame(render_anomaly_sql(entity, season))
            if frames[entity] is None or frames[entity].empty:
                missing.append(entity)
        except Exception:
            missing.append(entity)
    try:
        fotmob = query_frame(render_fotmob_diagnostic_sql(season))
    except Exception:
        fotmob = None
    # Each SELECT resolves the manifest independently. Never diagnose mixed
    # publications if a scope advances between reads (or metadata is absent).
    batches = {}
    for entity, frame in frames.items():
        scopes = {}
        if frame is not None and not frame.empty:
            for row in frame.to_dict('records'):
                scope = (str(row.get('league')), str(row.get('season')))
                batch = row.get('_batch_id')
                scopes.setdefault(scope, set()).add(None if pd.isna(batch) else str(batch))
        batches[entity] = scopes
    inconsistent = []
    for scope in sorted({scope for scopes in batches.values() for scope in scopes}):
        versions = [scopes.get(scope, set()) for scopes in batches.values()]
        if (any(len(version) != 1 or None in version for version in versions)
                or any(version != versions[0] for version in versions)):
            inconsistent.append(scope)
    if missing or inconsistent:
        return (compare_fotmob(frames.get('understat_schedule'), fotmob, team_aliases=team_aliases)
                + '; anomalies=n/a; inconsistent_batches=' + str(inconsistent)
                + '; unavailable=' + (','.join(missing) or 'none'))
    signals = diagnose_matches(frames)
    from collections import Counter
    counts = dict(sorted(Counter(signal['code'] for signal in signals).items()))
    affected = sorted({tuple(match) for signal in signals
                       for match in signal['details'].get('matches',
                           [signal['details']['match']] if 'match' in signal['details'] else [])})
    # Keep the morning line bounded; complete warnings remain in scope quality.
    diagnostic = f"{counts}; affected={affected[:20]} ({len(affected)} total)"
    return (compare_fotmob(frames.get('understat_schedule'), fotmob, team_aliases=team_aliases)
            + '; anomalies=' + diagnostic + '; unavailable=' + (','.join(missing) or 'none'))
