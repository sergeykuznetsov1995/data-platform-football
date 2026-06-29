#!/usr/bin/env python3
"""
One-shot dedup of iceberg.bronze.fbref_schedule.

Background: schedule writers used to APPEND on every run, producing many
duplicate rows per match (~5x for older seasons, ~10x for active ones).
The newer code path uses partition-replace, but existing rows still need
cleanup.

Strategy:
  1. CTAS into staging table, keeping latest row per match_url
  2. Verify counts match expectation
  3. DROP original, RENAME staging → fbref_schedule

Run inside the airflow container (so it can talk to Trino on the docker
network):

    docker compose exec airflow-webserver \
        python /opt/airflow/scripts/dedup_fbref_schedule.py
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
        'SELECT COUNT(*), COUNT(DISTINCT match_url) '
        'FROM iceberg.bronze.fbref_schedule'
    )
    total, uniq = rows[0]
    logger.info(f"Before: total={total} unique_match_urls={uniq}")

    if uniq == 0:
        logger.error('Empty unique set — refusing to proceed')
        sys.exit(1)

    if total == uniq:
        logger.info('Already deduplicated — nothing to do')
        return

    # Existing partitioning is (league, season). Keep it on staging.
    # ROW_NUMBER over PARTITION BY match_url ORDER BY _ingested_at DESC
    # picks the freshest row per match.
    staging = 'iceberg.bronze.fbref_schedule_dedup_staging'

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
            wk, day, date, time, home, score, away, attendance, venue,
            referee, "match report", notes, match_url, league, season,
            _source, _entity_type, _ingested_at, _batch_id
        FROM (
            SELECT s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY match_url
                    ORDER BY _ingested_at DESC
                ) AS rn
            FROM iceberg.bronze.fbref_schedule s
            WHERE match_url IS NOT NULL
        )
        WHERE rn = 1
    """)

    rows = execute(c, f'SELECT COUNT(*), COUNT(DISTINCT match_url) FROM {staging}')
    s_total, s_uniq = rows[0]
    logger.info(f"Staging: total={s_total} unique_match_urls={s_uniq}")

    if s_uniq < uniq * 0.95:
        logger.error(
            f"Staging has fewer unique urls than expected "
            f"({s_uniq} < {uniq} × 0.95). Aborting — staging table left for "
            f"manual inspection."
        )
        sys.exit(2)

    # Atomic-ish swap: drop original, rename staging
    execute(c, 'DROP TABLE iceberg.bronze.fbref_schedule')
    execute(c, f'ALTER TABLE {staging} RENAME TO iceberg.bronze.fbref_schedule')

    # Verify final counts
    rows = execute(
        c,
        'SELECT COUNT(*), COUNT(DISTINCT match_url) '
        'FROM iceberg.bronze.fbref_schedule'
    )
    f_total, f_uniq = rows[0]
    logger.info(f"After: total={f_total} unique_match_urls={f_uniq}")
    logger.info(
        f"Removed {total - f_total} duplicate rows "
        f"({(1 - f_total / total) * 100:.1f}% reduction)"
    )


if __name__ == '__main__':
    main()
