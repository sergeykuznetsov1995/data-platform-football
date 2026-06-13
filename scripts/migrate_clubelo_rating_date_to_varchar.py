#!/usr/bin/env python3
"""One-off remediation for #554 (split from #470 bug 5): dedup the daily
clubelo_ratings append bloat and recast rating_date timestamp(6) -> varchar.

Before the fix, the daily DAG wrote ``rating_date = datetime.now()`` WITH a
time component as the partition key and appended without replace_partitions, so
every same-day rerun / Airflow retry added a second full 44-row snapshot. The
table column was therefore created as ``timestamp(6)``.

The code fix writes a date-only ISO string and passes
``replace_partitions=['rating_date']``, but the replace-DELETE predicate
(``rating_date = '2026-06-13'``) is a varchar literal — it raises
``TYPE_MISMATCH`` against a timestamp(6) column. clubelo_ratings_historical
works only because its column is varchar. ``rating_date`` is not consumed
downstream (silver xref_team reads only team/league), so recasting is safe.

This script keeps the LATEST snapshot per calendar day and rewrites
rating_date to a date-only varchar 'YYYY-MM-DD', matching the fixed writer.
Same CTAS + DROP + RENAME strategy as scripts/dedup_fbref_single_stat_bloat.py.
Idempotent: if rating_date is already varchar the table is left untouched.

Usage (inside a container that can reach Trino, e.g. airflow-scheduler):
    python scripts/migrate_clubelo_rating_date_to_varchar.py --dry-run
    python scripts/migrate_clubelo_rating_date_to_varchar.py            # apply
"""

import argparse
import os
import sys
import warnings

import trino

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

TABLE = 'clubelo_ratings'


def _connect():
    user = os.environ.get('TRINO_USER', 'airflow')
    return trino.dbapi.connect(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8443)),
        user=user,
        catalog='iceberg',
        http_scheme='https',
        auth=trino.auth.BasicAuthentication(
            user, os.environ.get('TRINO_PASSWORD', '')
        ),
        verify=False,
    )


def _q(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def _select_list(cur):
    """Original column order, double-quoted (``from``/``to`` are reserved),
    with rating_date recast to a date-only varchar."""
    parts = []
    for (name, *_rest) in _q(cur, f'DESCRIBE iceberg.bronze.{TABLE}'):
        if name == 'rating_date':
            parts.append("CAST(date(rating_date) AS varchar) AS rating_date")
        else:
            parts.append(f'"{name}"')
    return ', '.join(parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    conn = _connect()
    cur = conn.cursor()
    src = f'iceberg.bronze.{TABLE}'

    rd_type = next(
        (c[1] for c in _q(cur, f'SHOW COLUMNS FROM {src}') if c[0] == 'rating_date'),
        None,
    )
    print(f'#554 clubelo_ratings migration — rating_date is currently {rd_type!r}')
    if rd_type == 'varchar':
        print('  already varchar — nothing to do (idempotent no-op)')
        return 0

    before = _q(cur, f'SELECT count(*) FROM {src}')[0][0]
    # latest snapshot per calendar day = rows whose timestamp is that day's max
    target = _q(cur, f'''
        SELECT count(*) FROM {src}
        WHERE rating_date IN (
            SELECT max(rating_date) FROM {src} GROUP BY date(rating_date)
        )''')[0][0]
    days = _q(cur, f'SELECT count(DISTINCT date(rating_date)) FROM {src}')[0][0]
    print(f'  before={before} -> target={target} ({days} days, '
          f'removing {before - target} dup rows; recast timestamp(6)->varchar)')
    if args.dry_run:
        return 0

    twin = f'iceberg.bronze.{TABLE}__mig554'
    backup = f'iceberg.bronze.{TABLE}_backup554'
    cols = _select_list(cur)
    _q(cur, f'DROP TABLE IF EXISTS {twin}')
    _q(cur, f'''
        CREATE TABLE {twin} WITH (partitioning = ARRAY['rating_date']) AS
        SELECT {cols} FROM {src}
        WHERE rating_date IN (
            SELECT max(rating_date) FROM {src} GROUP BY date(rating_date)
        )''')
    twin_cnt = _q(cur, f'SELECT count(*) FROM {twin}')[0][0]
    twin_days = _q(cur, f'SELECT count(DISTINCT rating_date) FROM {twin}')[0][0]
    if twin_cnt != target or twin_days != days:
        _q(cur, f'DROP TABLE IF EXISTS {twin}')
        raise RuntimeError(
            f'twin count/days ({twin_cnt}/{twin_days}) != target ({target}/{days}); aborted'
        )
    # keep the pre-migration table as a backup until verified, then swap
    _q(cur, f'DROP TABLE IF EXISTS {backup}')
    _q(cur, f'ALTER TABLE {src} RENAME TO {backup}')
    _q(cur, f'ALTER TABLE {twin} RENAME TO {src}')
    after = _q(cur, f'SELECT count(*) FROM {src}')[0][0]
    print(f'  done -> {after} rows, varchar date-only key; backup at {backup}')
    print('  (drop the backup once the fixed daily run is verified idempotent)')
    cur.close()
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
