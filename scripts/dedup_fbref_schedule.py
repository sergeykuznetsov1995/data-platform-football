#!/usr/bin/env python3
"""
One-shot cleanup of iceberg.bronze.fbref_schedule (issue #892, P2).

Background
----------
Schedule writers used to APPEND on every run, so this script was originally a
dedup over ``match_url``. The writers moved to partition-replace and the
duplicates are gone — a 2026-07-08 audit found **zero** duplicate ``match_url``
values (20 981 rows / 18 835 distinct urls / 2 146 rows with ``match_url IS
NULL``).

What is left is a different kind of bloat: 2 146 rows where *every* field is
NULL — blank separator rows the FBref HTML parser captures between gameweeks.
Silver already discards them (``fbref_match_enriched.sql`` filters
``sch.date IS NOT NULL``), so this is pure Bronze hygiene.

Why row-level DELETE instead of the previous CTAS + DROP + RENAME
-----------------------------------------------------------------
The CTAS spelled out an explicit column list. Bronze schema evolves (the
``round`` column appeared 2026-07-06 during the Top-5 backfill and carries the
German/French relegation play-off labels), so any column added after the script
was written would be silently destroyed by the DROP + RENAME. A DELETE names no
columns and cannot lose them. The table is ``format_version = 2``, so Trino
does a merge-on-read delete.

Run inside the airflow container (so it can talk to Trino on the docker
network):

    docker compose exec airflow-webserver \
        python /opt/airflow/scripts/dedup_fbref_schedule.py

Flags:
    --dry-run   only report what would be deleted
"""

import argparse
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

TABLE = 'iceberg.bronze.fbref_schedule'


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
    logger.info(f"EXEC: {sql[:140]}{'…' if len(sql) > 140 else ''}")
    c.execute(sql)
    return c.fetchall()


def purge_null_url_rows(c, dry_run: bool) -> None:
    rows = execute(
        c,
        f'SELECT COUNT(*), COUNT(match_url), COUNT(DISTINCT match_url) FROM {TABLE}'
    )
    total, with_url, uniq = rows[0]
    null_url = total - with_url
    logger.info(f"Before: total={total} with_url={with_url} unique_urls={uniq}")

    if uniq == 0:
        logger.error('Empty unique set — refusing to proceed')
        sys.exit(1)

    # Duplicate match_urls are out of scope: choosing a survivor needs a
    # score-aware ORDER BY (a null-score placeholder can be ingested *after*
    # the played row), so `ORDER BY _ingested_at DESC` would silently drop
    # results. Refuse rather than guess.
    if with_url != uniq:
        logger.error(
            f"Found {with_url - uniq} duplicate match_url rows. This script "
            f"only purges all-NULL rows; deduplication needs a score-aware "
            f"survivor rule — aborting for manual review."
        )
        sys.exit(3)

    if null_url == 0:
        logger.info('No NULL-match_url rows — nothing to do')
        return

    logger.info(f"Rows to delete (match_url IS NULL): {null_url}")
    if dry_run:
        logger.info('--dry-run, no changes')
        return

    execute(c, f'DELETE FROM {TABLE} WHERE match_url IS NULL')

    rows = execute(c, f'SELECT COUNT(*) FROM {TABLE}')
    final = rows[0][0]
    logger.info(f"After: total={final}")
    if final != uniq:
        logger.error(f"Expected {uniq} rows after delete, got {final}")
        sys.exit(4)
    logger.info(
        f"Removed {total - final} all-NULL rows "
        f"({(1 - final / total) * 100:.1f}% reduction)"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true',
                        help='only report what would be deleted')
    args = parser.parse_args()

    conn = get_conn()
    c = conn.cursor()
    purge_null_url_rows(c, args.dry_run)


if __name__ == '__main__':
    main()
