#!/usr/bin/env python3
"""
SoFIFA Scraper Runner Script
============================

Standalone script to run SoFIFA scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.
"""

import argparse
import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Replace-partitions completeness guard (#513 → #583): refuse a save that would
# shrink a sofifa_* partition below this share of its existing rows, so a
# partial/failed scrape (Turnstile/FlareSolverr death mid heavy player_ratings)
# can't wipe a good FIFA edition. COUNT(*) (full-state per fifa_edition — no
# replace_guard_key needed). Armed only when replace_partitions is set
# (dynamic `part` is None when the column is absent → guard stays off, else
# min_replace_ratio without replace_partitions raises ValueError).
# ReplaceGuardError → exit 3; bypass with --force-replace for a first backfill.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'SOFIFA_REPLACE_GUARD'


def _trino_connect():
    """Open a Trino dbapi connection from env. Returns None on import error.

    Mirrors run_sofascore_scraper._trino_connect (#69 skip-existing path).
    """
    try:
        import trino
        import trino.auth as trino_auth
    except ImportError as e:
        logger.error("trino client unavailable: %s", e)
        return None

    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')
    if password:
        return trino.dbapi.connect(
            host=os.environ.get('TRINO_HOST', 'trino'),
            port=int(os.environ.get('TRINO_PORT', 8443)),
            user=user,
            catalog='iceberg',
            http_scheme='https',
            auth=trino_auth.BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8080)),
        user=user,
        catalog='iceberg',
    )


def _bronze_up_to_date(latest_vid: int, fifa_edition: str, update: str) -> bool:
    """True when Bronze already carries the latest sofifa roster update.

    Two probes, both must pass:
    - ``MAX(version_id)`` in ``sofifa_player_ratings`` equals the homepage's
      latest version id (the heavy ~546-page step is current);
    - ``sofifa_players`` has rows for (fifa_edition, update) — guards the rare
      run where player_ratings succeeded but the earlier players step failed
      on every retry.

    Any error (table absent, version_id column absent before the first
    post-deploy write, Trino down) → False → full scrape. Fail-open by
    design: a wasted full run is safe, a wrong skip lets Bronze go stale.
    """
    conn = _trino_connect()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT MAX(version_id) FROM iceberg.bronze.sofifa_player_ratings"
        )
        row = cur.fetchone()
        if not row or row[0] is None or int(row[0]) != latest_vid:
            return False
        # "update" is a Trino reserved word — must stay quoted.
        cur.execute(
            'SELECT COUNT(*) FROM iceberg.bronze.sofifa_players '
            'WHERE fifa_edition = ? AND "update" = ?',
            (fifa_edition, update),
        )
        row = cur.fetchone()
        return bool(row and row[0])
    except Exception as e:
        logger.warning(
            "incremental probe on Bronze failed (%s) — running full scrape", e
        )
        return False


def main():
    parser = argparse.ArgumentParser(description='Run SoFIFA scraper')
    parser.add_argument(
        '--leagues',
        type=str,
        default='ENG-Premier League',
        help='Comma-separated list of leagues'
    )
    parser.add_argument(
        '--versions',
        type=str,
        default='latest',
        help='FIFA versions: "latest", "all", or comma-separated version IDs'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/sofifa_result.json',
        help='Output file for results'
    )
    parser.add_argument(
        '--force-replace',
        action='store_true',
        help='Bypass the completeness guard — write even if the scraped frame '
             'shrinks the existing partition. Use for a deliberate first '
             'backfill or a known legitimate shrink.'
    )
    parser.add_argument(
        '--force-full',
        action='store_true',
        help='Bypass the incremental version_id check and always run the '
             'full scrape (players/teams/team_ratings/player_ratings).'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    # versions: "latest"/"all" pass through; explicit comma-separated version
    # IDs must become list[int] — soccerdata.SoFIFA accepts 'latest'|'all'|int|
    # list[int] and raises ValueError on a raw digit string. (#665)
    if args.versions in ('latest', 'all'):
        versions = args.versions
    else:
        versions = [int(v.strip()) for v in args.versions.split(',')]
    logger.info(f"Starting SoFIFA scraper: leagues={leagues}, versions={versions}")

    results = {
        'tables': [],
        'players_rows': 0,
        'teams_rows': 0,
        'team_ratings_rows': 0,
        'versions_rows': 0,
        'leagues_rows': 0,
        'player_ratings_rows': 0,
        'errors': [],
        # Issue #616 — FlareSolverr proxy-traffic audit for this run.
        'traffic': {},
    }
    # #583: a refused completeness guard (partial scrape would shrink a
    # partition) maps to exit 3 — distinct from the exit-1 hard-failure path.
    guard_refused = False

    try:
        from scrapers.base.base_scraper import ReplaceGuardError
        from scrapers.sofifa import SoFIFAScraper

        with SoFIFAScraper(leagues=leagues, versions=versions) as scraper:
            # Incremental skip: sofifa content only changes when a new roster
            # update (version_id) is published — every scraped URL is keyed by
            # ?r=<version_id>. When Bronze already carries the latest id, the
            # heavy steps (23-request read_players bootstrap + ~546 player
            # pages, ~2h) would re-download identical data, so they are
            # skipped and only the two 1-request lookups (versions, leagues)
            # refresh. 'latest'-mode only: an explicit version list is a
            # deliberate backfill.
            skip_heavy = False
            latest_vid = None
            if versions == 'latest' and not args.force_full:
                try:
                    reader = scraper._get_reader()  # 1 homepage request
                    latest_vid = int(reader.versions.index.max())
                    vrow = reader.versions.loc[latest_vid]
                    skip_heavy = _bronze_up_to_date(
                        latest_vid,
                        str(vrow['fifa_edition']),
                        str(vrow['update']),
                    )
                except Exception as e:
                    logger.warning(
                        "incremental version check failed (%s) — full scrape", e
                    )
            if skip_heavy:
                results['skipped'] = {
                    'reason': 'version_unchanged',
                    'version_id': latest_vid,
                }
                logger.info(
                    "Bronze already at sofifa version %s — skipping "
                    "players/teams/team_ratings/player_ratings",
                    latest_vid,
                )

            # Scrape players
            try:
                df = None if skip_heavy else scraper.read_players()
                if df is not None and not df.empty:
                    df = scraper.filter_by_league(df)
                    df = scraper._process_player_data(df)

                    if not df.empty:
                        part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='sofifa_players',
                            partition_cols=part,
                            replace_partitions=part,
                            min_replace_ratio=(
                                None if (args.force_replace or part is None)
                                else _MIN_REPLACE_RATIO
                            ),
                        )
                        results['tables'].append(table_path)
                        results['players_rows'] = len(df)
                        logger.info(f"Saved {len(df)} player records")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Players scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape teams
            try:
                df = None if skip_heavy else scraper.read_teams()
                if df is not None and not df.empty:
                    part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofifa_teams',
                        partition_cols=part,
                        replace_partitions=part,
                        min_replace_ratio=(
                            None if (args.force_replace or part is None)
                            else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['teams_rows'] = len(df)
                    logger.info(f"Saved {len(df)} team records")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Teams scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape per-team ratings (overall/attack/midfield/defence + subs).
            # Single league-level page — cheap.
            try:
                df = None if skip_heavy else scraper.read_team_ratings()
                if df is not None and not df.empty:
                    part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofifa_team_ratings',
                        partition_cols=part,
                        replace_partitions=part,
                        min_replace_ratio=(
                            None if (args.force_replace or part is None)
                            else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['team_ratings_rows'] = len(df)
                    logger.info(f"Saved {len(df)} team rating records")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Team ratings scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape FIFA editions catalogue (version_id per edition). Cheap.
            try:
                df = scraper.read_versions()
                if df is not None and not df.empty:
                    part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofifa_versions',
                        partition_cols=part,
                        replace_partitions=part,
                        min_replace_ratio=(
                            None if (args.force_replace or part is None)
                            else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['versions_rows'] = len(df)
                    logger.info(f"Saved {len(df)} version records")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Versions scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape league -> sofifa league_id lookup. Cheap. No fifa_edition,
            # so replace on the `league` key to stay idempotent across runs.
            try:
                df = scraper.read_leagues()
                if df is not None and not df.empty:
                    repl = ['league'] if 'league' in df.columns else None
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofifa_leagues',
                        partition_cols=None,
                        replace_partitions=repl,
                        min_replace_ratio=(
                            None if (args.force_replace or repl is None)
                            else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['leagues_rows'] = len(df)
                    logger.info(f"Saved {len(df)} league records")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Leagues scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape per-player attribute ratings (issue #42).
            # ~545 player pages per APL edition — slowest step by far.
            try:
                df = None if skip_heavy else scraper.read_player_ratings()
                if df is not None and not df.empty:
                    df = scraper._process_rating_data(df)
                    if not df.empty:
                        part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='sofifa_player_ratings',
                            partition_cols=part,
                            replace_partitions=part,
                            min_replace_ratio=(
                                None if (args.force_replace or part is None)
                                else _MIN_REPLACE_RATIO
                            ),
                        )
                        results['tables'].append(table_path)
                        results['player_ratings_rows'] = len(df)
                        logger.info(f"Saved {len(df)} player rating records")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Player ratings scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Issue #616: surface the FlareSolverr proxy-traffic audit for this
            # run (per-match proxy MB baseline; player_ratings dominates).
            try:
                results['traffic'] = scraper.get_traffic_stats()
            except Exception as e:
                logger.warning(f"get_traffic_stats failed: {e}")

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        with open(args.output, 'w') as f:
            json.dump(results, f)
        sys.exit(1)

    # Write results
    with open(args.output, 'w') as f:
        json.dump(results, f)

    total_rows = (
        results['players_rows']
        + results['teams_rows']
        + results['team_ratings_rows']
        + results['versions_rows']
        + results['leagues_rows']
        + results['player_ratings_rows']
    )
    logger.info(f"Scraper complete: {total_rows} total rows")
    print(json.dumps(results))
    # Issue #466: non-zero exit when any scrape step failed — otherwise the
    # BashOperator stays green while team_ratings/versions/leagues/
    # player_ratings silently go stale for weeks. #583: a refused completeness
    # guard takes priority as exit 3 (distinct from a hard failure's exit 1).
    return 3 if guard_refused else (1 if results.get('errors') else 0)


if __name__ == '__main__':
    sys.exit(main())
