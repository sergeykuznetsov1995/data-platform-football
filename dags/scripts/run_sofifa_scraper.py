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
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


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

    try:
        from scrapers.sofifa import SoFIFAScraper

        with SoFIFAScraper(leagues=leagues, versions=versions) as scraper:
            # Scrape players
            try:
                df = scraper.read_players()
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
                        )
                        results['tables'].append(table_path)
                        results['players_rows'] = len(df)
                        logger.info(f"Saved {len(df)} player records")
            except Exception as e:
                error_msg = f"Players scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape teams
            try:
                df = scraper.read_teams()
                if df is not None and not df.empty:
                    part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofifa_teams',
                        partition_cols=part,
                        replace_partitions=part,
                    )
                    results['tables'].append(table_path)
                    results['teams_rows'] = len(df)
                    logger.info(f"Saved {len(df)} team records")
            except Exception as e:
                error_msg = f"Teams scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape per-team ratings (overall/attack/midfield/defence + subs).
            # Single league-level page — cheap.
            try:
                df = scraper.read_team_ratings()
                if df is not None and not df.empty:
                    part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofifa_team_ratings',
                        partition_cols=part,
                        replace_partitions=part,
                    )
                    results['tables'].append(table_path)
                    results['team_ratings_rows'] = len(df)
                    logger.info(f"Saved {len(df)} team rating records")
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
                    )
                    results['tables'].append(table_path)
                    results['versions_rows'] = len(df)
                    logger.info(f"Saved {len(df)} version records")
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
                    )
                    results['tables'].append(table_path)
                    results['leagues_rows'] = len(df)
                    logger.info(f"Saved {len(df)} league records")
            except Exception as e:
                error_msg = f"Leagues scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape per-player attribute ratings (issue #42).
            # ~545 player pages per APL edition — slowest step by far.
            try:
                df = scraper.read_player_ratings()
                if df is not None and not df.empty:
                    df = scraper._process_rating_data(df)
                    if not df.empty:
                        part = ['fifa_edition'] if 'fifa_edition' in df.columns else None
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='sofifa_player_ratings',
                            partition_cols=part,
                            replace_partitions=part,
                        )
                        results['tables'].append(table_path)
                        results['player_ratings_rows'] = len(df)
                        logger.info(f"Saved {len(df)} player rating records")
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
    # player_ratings silently go stale for weeks.
    return 1 if results.get('errors') else 0


if __name__ == '__main__':
    sys.exit(main())
