#!/usr/bin/env python3
"""
One-shot dedup of iceberg.bronze.espn_schedule.

Background: the ESPN schedule writer used to APPEND on every run, so the
active season (re-scraped daily) accumulated many duplicate rows per match
(~31x for season 2526; older seasons stay clean at 1x). The code path now
uses partition-replace (replace_partitions=['league','season'], #347), but
the rows already written still need a one-off cleanup.

Strategy:
  1. CTAS into staging table, keeping the freshest row per game_id
  2. Verify the unique-game_id count did not unexpectedly shrink
  3. DROP original, RENAME staging -> espn_schedule

Run inside the airflow container (so it can talk to Trino on the docker
network):

    docker compose exec airflow-webserver \
        python /opt/airflow/scripts/dedup_espn_schedule.py
"""

import logging
import os
import sys
import warnings

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('dedup')


def get_conn():
    import trino
    password = os.environ.get('TRINO_PASSWORD', '')
    user = os.environ.get('TRINO_USER', 'airflow')
    kw = dict(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8443)),
        user=user,
        catalog='iceberg',
    )
    if password:
        kw.update(
            http_scheme='https',
            auth=trino.auth.BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(**kw)


def execute(c, sql):
    logger.info(f"EXEC: {sql[:120]}{'…' if len(sql) > 120 else ''}")
    c.execute(sql)
    return c.fetchall()


def main():
    conn = get_conn()
    c = conn.cursor()

    # Baseline counts
    rows = execute(
        c,
        'SELECT COUNT(*), COUNT(DISTINCT game_id) '
        'FROM iceberg.bronze.espn_schedule'
    )
    total, uniq = rows[0]
    logger.info(f"Before: total={total} unique_game_ids={uniq}")

    if uniq == 0:
        logger.error('Empty unique set — refusing to proceed')
        sys.exit(1)

    if total == uniq:
        logger.info('Already deduplicated — nothing to do')
        return

    # Existing partitioning is (league, season). Keep it on staging.
    # ROW_NUMBER over PARTITION BY game_id ORDER BY _ingested_at DESC
    # picks the freshest row per match. Clean seasons (1x) are untouched.
    staging = 'iceberg.bronze.espn_schedule_dedup_staging'

    # Drop staging if a previous attempt left it behind
    try:
        execute(c, f'DROP TABLE IF EXISTS {staging}')
    except Exception as e:
        logger.warning(f"Could not drop pre-existing staging: {e}")

    execute(c, f"""
        CREATE TABLE {staging}
        WITH (partitioning = ARRAY['league', 'season'])
        AS
        SELECT
            league, season, game, match_date, home_team, away_team,
            game_id, league_id, _source, _entity_type, _ingested_at, _batch_id
        FROM (
            SELECT s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id
                    ORDER BY _ingested_at DESC
                ) AS rn
            FROM iceberg.bronze.espn_schedule s
            WHERE game_id IS NOT NULL
        )
        WHERE rn = 1
    """)

    rows = execute(c, f'SELECT COUNT(*), COUNT(DISTINCT game_id) FROM {staging}')
    s_total, s_uniq = rows[0]
    logger.info(f"Staging: total={s_total} unique_game_ids={s_uniq}")

    if s_uniq < uniq * 0.95:
        logger.error(
            f"Staging has fewer unique game_ids than expected "
            f"({s_uniq} < {uniq} × 0.95). Aborting — staging table left for "
            f"manual inspection."
        )
        sys.exit(2)

    # Atomic-ish swap: drop original, rename staging
    execute(c, 'DROP TABLE iceberg.bronze.espn_schedule')
    execute(c, f'ALTER TABLE {staging} RENAME TO iceberg.bronze.espn_schedule')

    # Verify final counts
    rows = execute(
        c,
        'SELECT COUNT(*), COUNT(DISTINCT game_id) '
        'FROM iceberg.bronze.espn_schedule'
    )
    f_total, f_uniq = rows[0]
    logger.info(f"After: total={f_total} unique_game_ids={f_uniq}")
    logger.info(
        f"Removed {total - f_total} duplicate rows "
        f"({(1 - f_total / total) * 100:.1f}% reduction)"
    )


if __name__ == '__main__':
    main()
