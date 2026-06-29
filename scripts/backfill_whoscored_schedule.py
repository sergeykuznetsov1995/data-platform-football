#!/usr/bin/env python3
"""
Backfill ``iceberg.bronze.whoscored_schedule`` from ``iceberg.bronze.whoscored_events``.

Closes #102 — silver.xref_match has 0 rows for source='whoscored', season='2526'
because the underlying bronze.whoscored_schedule never received post-2021 data
(soccerdata read_schedule path silently broken; validator threshold key missing
in MIN_ROW_THRESHOLDS hid the regression).

Strategy: events already carry the metadata we need to reconstruct schedule
rows. The ``game`` column has the form ``"YYYY-MM-DD Home-Away"`` (full team
names, identical to what live ``read_schedule`` writes — confirmed via 2021
sample). REGEXP non-greedy split on ``-`` correctly handles both
``"Manchester City-Tottenham"`` and ``"Manchester City-Manchester United"``.

Idempotent: DELETEs the targeted partitions before INSERT, so re-runs replace
rather than append.

Usage (inside Airflow container):
    docker compose exec -T airflow-scheduler python /opt/airflow/scripts/backfill_whoscored_schedule.py
    # optional: --dry-run, --seasons 2526,2425, --league "ENG-Premier League"
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('backfill_whoscored_schedule')

DEFAULT_LEAGUE = 'ENG-Premier League'
DEFAULT_SEASONS = ['2526', '2425', '2324', '2223', '2122']
BATCH_ID = 'backfill_issue_102'


def _quote_csv(values: List[str]) -> str:
    return ', '.join("'" + v.replace("'", "''") + "'" for v in values)


def _build_count_sql(league: str, seasons: List[str]) -> str:
    return (
        "SELECT season, COUNT(*) AS rows "
        "FROM iceberg.bronze.whoscored_schedule "
        f"WHERE league = '{league}' AND season IN ({_quote_csv(seasons)}) "
        "GROUP BY season ORDER BY season DESC"
    )


def _build_delete_sql(league: str, seasons: List[str]) -> str:
    return (
        "DELETE FROM iceberg.bronze.whoscored_schedule "
        f"WHERE league = '{league}' AND season IN ({_quote_csv(seasons)})"
    )


def _build_insert_sql(league: str, seasons: List[str]) -> str:
    """One-shot CTE → INSERT. Only the columns we can reliably reconstruct are
    populated; the rest stay NULL (schema allows it). Downstream xref_team /
    xref_match join on home_team/away_team strings, not on *_id BIGINTs, so
    leaving the id columns NULL is safe."""
    return f"""
INSERT INTO iceberg.bronze.whoscored_schedule (
    league, season, game, game_id,
    home_team, away_team, date,
    _source, _entity_type, _ingested_at, _batch_id
)
WITH distinct_games AS (
    SELECT DISTINCT league, season, game_id, game
    FROM iceberg.bronze.whoscored_events
    WHERE league = '{league}'
      AND season IN ({_quote_csv(seasons)})
      AND game_id IS NOT NULL
      AND game IS NOT NULL
      AND LENGTH(game) > 11
),
parsed AS (
    SELECT
        league,
        season,
        game,
        game_id,
        TRY_CAST(SUBSTR(game, 1, 10) AS timestamp(6)) AS date,
        TRIM(REGEXP_EXTRACT(SUBSTR(game, 12), '^(.+?)-(.+)$', 1)) AS home_team,
        TRIM(REGEXP_EXTRACT(SUBSTR(game, 12), '^(.+?)-(.+)$', 2)) AS away_team
    FROM distinct_games
)
SELECT
    league,
    season,
    game,
    game_id,
    home_team,
    away_team,
    date,
    'whoscored',
    'schedule',
    CAST(current_timestamp AS timestamp(6)),
    '{BATCH_ID}'
FROM parsed
WHERE date IS NOT NULL
  AND home_team IS NOT NULL AND home_team <> ''
  AND away_team IS NOT NULL AND away_team <> ''
""".strip()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description='Backfill whoscored_schedule from events (issue #102)')
    parser.add_argument('--league', default=DEFAULT_LEAGUE)
    parser.add_argument(
        '--seasons',
        default=','.join(DEFAULT_SEASONS),
        help='CSV of season codes in soccerdata YYZZ form (e.g. "2526,2425")',
    )
    parser.add_argument('--dry-run', action='store_true', help='Print SQL + counts but do not modify')
    args = parser.parse_args(argv)

    seasons = [s.strip() for s in args.seasons.split(',') if s.strip()]
    if not seasons:
        logger.error("No seasons provided")
        return 2

    # Imports here so the script can be run with --help even without airflow env.
    from utils.silver_tasks import _execute, _get_trino_connection

    logger.info(f"League: {args.league}")
    logger.info(f"Seasons: {seasons}")
    logger.info(f"Dry run: {args.dry_run}")

    conn = _get_trino_connection()
    try:
        # ---- Before counts ----
        logger.info("Pre-backfill counts in bronze.whoscored_schedule:")
        rows = _execute(conn, _build_count_sql(args.league, seasons), fetch=True) or []
        for r in rows:
            logger.info(f"  season={r[0]}: {r[1]} rows")
        if not rows:
            logger.info("  (no rows for any of the target seasons — fresh backfill)")

        # ---- Source counts (events) ----
        src_rows = _execute(
            conn,
            (
                "SELECT season, COUNT(DISTINCT game_id) AS games "
                "FROM iceberg.bronze.whoscored_events "
                f"WHERE league = '{args.league}' AND season IN ({_quote_csv(seasons)}) "
                "GROUP BY season ORDER BY season DESC"
            ),
            fetch=True,
        ) or []
        logger.info("Source distinct game_id counts in bronze.whoscored_events:")
        for r in src_rows:
            logger.info(f"  season={r[0]}: {r[1]} games")
        if not src_rows:
            logger.warning("  (no events for any of the target seasons — nothing to backfill)")
            return 0

        if args.dry_run:
            logger.info("--- DELETE SQL ---")
            print(_build_delete_sql(args.league, seasons))
            logger.info("--- INSERT SQL ---")
            print(_build_insert_sql(args.league, seasons))
            return 0

        # ---- DELETE existing partitions for idempotency ----
        delete_sql = _build_delete_sql(args.league, seasons)
        logger.info("Executing DELETE for target partitions...")
        _execute(conn, delete_sql)
        logger.info("DELETE done")

        # ---- INSERT ----
        insert_sql = _build_insert_sql(args.league, seasons)
        logger.info("Executing INSERT from events...")
        _execute(conn, insert_sql)
        logger.info("INSERT done")

        # ---- After counts ----
        logger.info("Post-backfill counts in bronze.whoscored_schedule:")
        after_rows = _execute(conn, _build_count_sql(args.league, seasons), fetch=True) or []
        for r in after_rows:
            logger.info(f"  season={r[0]}: {r[1]} rows")

        # ---- Sanity: every source game must have a backfilled row ----
        src_by_season = {r[0]: r[1] for r in src_rows}
        after_by_season = {r[0]: r[1] for r in after_rows}
        bad = []
        for s in seasons:
            src_cnt = src_by_season.get(s, 0)
            got = after_by_season.get(s, 0)
            if src_cnt and got < src_cnt:
                bad.append((s, src_cnt, got))
        if bad:
            logger.error(f"Coverage mismatch (season, events_games, schedule_rows): {bad}")
            return 3

        logger.info("Backfill complete; coverage matches source events 1:1")
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
