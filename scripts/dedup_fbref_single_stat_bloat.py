#!/usr/bin/env python3
"""One-off remediation for #536: collapse pre-fix append bloat in FBref
season-grain stat tables.

Before the scrape_single_stat_type replace_partitions fix (#536), the weekly
dag_ingest_fbref single_stat tasks plain-appended a full copy of each
(league, season) on every run, bloating these Bronze tables 45-50x:

    fbref_player_stats / fbref_player_misc / fbref_player_shooting /
    fbref_player_playingtime / fbref_team_stats / fbref_team_misc /
    fbref_team_shooting / fbref_team_playingtime /
    fbref_keeper_keeper / fbref_keeper_keeper_adv

Each historical run wrote one full copy tagged with its own _batch_id. The
latest batch per (league, season) is a complete copy (verified: latest row
count == max batch row count for every partition), so the dedup keeps only
the newest batch per (league, season) and drops the rest. CTAS + DROP +
RENAME strategy, with the column list read from DESCRIBE so schema evolution
cannot silently drop a column.

Usage (inside a container that can reach Trino, e.g. airflow-scheduler):
    python scripts/dedup_fbref_single_stat_bloat.py --dry-run
    python scripts/dedup_fbref_single_stat_bloat.py            # apply
    python scripts/dedup_fbref_single_stat_bloat.py --tables fbref_team_stats
"""

import argparse
import os
import sys
import warnings

import trino

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

BLOATED_TABLES = [
    'fbref_team_stats',
    'fbref_player_stats',
    'fbref_keeper_keeper',
    'fbref_keeper_keeper_adv',
    'fbref_player_misc',
    'fbref_player_shooting',
    'fbref_player_playingtime',
    'fbref_team_misc',
    'fbref_team_shooting',
    'fbref_team_playingtime',
]


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


def _columns(cur, table):
    """Original column order, double-quoted for Trino (names contain
    chars like '# pl', 'g+a', '90s', 'g-pk')."""
    cols = [r[0] for r in _q(cur, f'DESCRIBE iceberg.bronze.{table}')]
    return ', '.join(f'"{c}"' for c in cols)


def _target_count(cur, table):
    """Expected clean rows = sum over partitions of the latest batch size."""
    rows = _q(cur, f'''
        SELECT sum(latest_cnt) FROM (
            SELECT league, season, max_by(cnt, ing) AS latest_cnt
            FROM (
                SELECT league, season, _batch_id,
                       count(*) cnt, max(_ingested_at) ing
                FROM iceberg.bronze.{table}
                GROUP BY league, season, _batch_id
            ) GROUP BY league, season
        )
    ''')
    return rows[0][0] or 0


def dedup_table(cur, table, dry_run):
    quoted = _columns(cur, table)
    before = _q(cur, f'SELECT count(*) FROM iceberg.bronze.{table}')[0][0]
    target = _target_count(cur, table)
    print(f'  {table}: before={before} target={target} '
          f'(removing {before - target} dup rows)')
    if dry_run:
        return
    if before == target:
        print(f'  {table}: already clean, skipping')
        return

    twin = f'{table}__dd536'
    _q(cur, f'DROP TABLE IF EXISTS iceberg.bronze.{twin}')
    # Keep all rows of the newest batch per (league, season). bing is constant
    # within a batch; DENSE_RANK over (bing, _batch_id) gives the latest batch
    # rank 1 and ties break on _batch_id.
    _q(cur, f'''
        CREATE TABLE iceberg.bronze.{twin}
        WITH (partitioning = ARRAY['league', 'season']) AS
        SELECT {quoted} FROM (
            SELECT {quoted}, DENSE_RANK() OVER (
                PARTITION BY league, season
                ORDER BY bing DESC, "_batch_id" DESC
            ) AS _brank
            FROM (
                SELECT {quoted}, max("_ingested_at") OVER (
                    PARTITION BY league, season, "_batch_id"
                ) AS bing
                FROM iceberg.bronze.{table}
            )
        ) WHERE _brank = 1
    ''')
    twin_cnt = _q(cur, f'SELECT count(*) FROM iceberg.bronze.{twin}')[0][0]
    if twin_cnt != target:
        _q(cur, f'DROP TABLE IF EXISTS iceberg.bronze.{twin}')
        raise RuntimeError(
            f'{table}: dedup twin count {twin_cnt} != target {target}; aborted'
        )
    _q(cur, f'DROP TABLE iceberg.bronze.{table}')
    _q(cur, f'ALTER TABLE iceberg.bronze.{twin} RENAME TO iceberg.bronze.{table}')
    after = _q(cur, f'SELECT count(*) FROM iceberg.bronze.{table}')[0][0]
    print(f'  {table}: done -> {after}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--tables', nargs='*', default=BLOATED_TABLES)
    args = ap.parse_args()

    conn = _connect()
    cur = conn.cursor()
    mode = 'DRY-RUN' if args.dry_run else 'APPLY'
    print(f'#536 FBref single_stat bloat dedup [{mode}]')
    for t in args.tables:
        dedup_table(cur, t, args.dry_run)
    cur.close()
    conn.close()
    print('Done.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
