#!/usr/bin/env python3
"""
One-shot purge of the EPL 2010/2015 probe-scrape leftovers from Bronze FBref
(issue #892, P3).

Background
----------
`bronze.fbref_schedule` carries two seasons nobody asked for: ENG-Premier
League 2010 and 2015. Each has a *complete* 380-fixture schedule but only
**three** matches of actual data — the residue of exploratory scrapes. Because
Silver builds `fbref_match_enriched` straight off the schedule, those 760
fixtures reach Gold: on 2026-07-08 `gold.dim_match` held 760 rows whose season
(`1011` / `1516`) has no `gold.dim_season` parent — a standing FK violation.

The platform's season scope is 2016/17 onwards (`configs/medallion/
competitions.yaml`), so the fix is to delete the out-of-scope rows at the root
rather than to filter them in every downstream transform.

Row counts measured 2026-07-08 (3 698 rows total).

Match-level (both seasons):

    fbref_schedule            852
    fbref_match_player_stats  167
    fbref_lineups             108
    fbref_match_events         66
    fbref_match_managers       12
    fbref_match_keeper_stats   12
    fbref_match_team_stats      6
    fbref_match_officials       6

Season-level (2015 only — the 2010 probe never reached these):

    fbref_player_playingtime  659
    fbref_player_stats        561
    fbref_player_shooting     561
    fbref_player_misc         561
    fbref_keeper_keeper        47
    fbref_team_stats           20
    fbref_team_shooting        20
    fbref_team_misc            20
    fbref_team_playingtime     20

Deletes are row-level (Iceberg `format_version = 2`, merge-on-read), so no
table is rebuilt and no column can be lost. Every table's current snapshot_id
is logged before the first delete — recover with

    CALL iceberg.system.rollback_to_snapshot('bronze', '<table>', <snapshot_id>)

After this script, re-run the FBref Silver transforms so `fbref_match_enriched`
drops the 760 phantom fixtures.

Run inside the airflow container (so it can talk to Trino on the docker
network). Deleting rows is irreversible in practice, so --apply is required:

    docker compose exec airflow-webserver \
        python /opt/airflow/scripts/purge_fbref_probe_seasons.py            # dry-run
    docker compose exec airflow-webserver \
        python /opt/airflow/scripts/purge_fbref_probe_seasons.py --apply
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
logger = logging.getLogger('purge_probe_seasons')

LEAGUE = 'ENG-Premier League'
# `season` is bigint in every fbref_* Bronze table (verified via
# information_schema on 2026-07-08).
SEASONS = (2010, 2015)

TABLES = [
    # match-level
    'fbref_schedule',
    'fbref_match_player_stats',
    'fbref_lineups',
    'fbref_match_events',
    'fbref_match_managers',
    'fbref_match_keeper_stats',
    'fbref_match_team_stats',
    'fbref_match_officials',
    # season-level — feed silver.fbref_{player,team}_season_profile /
    # fbref_keeper_profile, which would otherwise gain a phantom '1516' season
    # on the next Silver run.
    'fbref_player_stats',
    'fbref_player_shooting',
    'fbref_player_misc',
    'fbref_player_playingtime',
    'fbref_team_stats',
    'fbref_team_shooting',
    'fbref_team_misc',
    'fbref_team_playingtime',
    'fbref_keeper_keeper',
]

PREDICATE = (
    f"league = '{LEAGUE}' AND season IN ({', '.join(str(s) for s in SEASONS)})"
)


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


def log_snapshot(c, table: str) -> None:
    """Record the pre-delete snapshot so the table can be rolled back."""
    rows = execute(
        c,
        f'SELECT snapshot_id FROM iceberg.bronze."{table}$snapshots" '
        f'ORDER BY committed_at DESC LIMIT 1'
    )
    snapshot_id = rows[0][0] if rows else None
    logger.info(f"{table}: rollback snapshot_id={snapshot_id}")


def purge_table(c, table: str, dry_run: bool) -> int:
    """Delete out-of-scope probe rows from one Bronze table. Returns row count."""
    fqtn = f'iceberg.bronze.{table}'

    rows = execute(c, f'SELECT COUNT(*) FROM {fqtn} WHERE {PREDICATE}')
    doomed = rows[0][0]
    if doomed == 0:
        logger.info(f"{table}: no probe rows — already clean")
        return 0

    logger.info(f"{table}: {doomed} rows to delete")
    if dry_run:
        return doomed

    log_snapshot(c, table)
    execute(c, f'DELETE FROM {fqtn} WHERE {PREDICATE}')

    rows = execute(c, f'SELECT COUNT(*) FROM {fqtn} WHERE {PREDICATE}')
    left = rows[0][0]
    if left != 0:
        logger.error(f"{table}: {left} probe rows survived the delete — abort")
        sys.exit(2)

    logger.info(f"{table}: deleted {doomed} rows")
    return doomed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true',
                        help='actually delete (default: dry-run report only)')
    args = parser.parse_args()
    dry_run = not args.apply

    if dry_run:
        logger.info('DRY-RUN — pass --apply to delete')

    conn = get_conn()
    c = conn.cursor()

    total = sum(purge_table(c, table, dry_run) for table in TABLES)
    verb = 'would delete' if dry_run else 'deleted'
    logger.info(f"Total: {verb} {total} rows across {len(TABLES)} tables")


if __name__ == '__main__':
    main()
