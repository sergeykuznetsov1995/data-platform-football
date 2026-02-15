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
    # versions can be "latest", "all", or comma-separated IDs
    versions = args.versions
    logger.info(f"Starting SoFIFA scraper: leagues={leagues}, versions={versions}")

    results = {
        'tables': [],
        'players_rows': 0,
        'teams_rows': 0,
        'errors': []
    }

    try:
        from scrapers.sofifa_scraper import SoFIFAScraper

        with SoFIFAScraper(leagues=leagues, versions=versions) as scraper:
            # Scrape players
            try:
                df = scraper.read_players()
                if df is not None and not df.empty:
                    df = scraper.filter_by_league(df)
                    df = scraper._process_player_data(df)

                    if not df.empty:
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='sofifa_players',
                            partition_cols=['version'] if 'version' in df.columns else None,
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
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='sofifa_teams',
                        partition_cols=['version'] if 'version' in df.columns else None,
                    )
                    results['tables'].append(table_path)
                    results['teams_rows'] = len(df)
                    logger.info(f"Saved {len(df)} team records")
            except Exception as e:
                error_msg = f"Teams scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        with open(args.output, 'w') as f:
            json.dump(results, f)
        sys.exit(1)

    # Write results
    with open(args.output, 'w') as f:
        json.dump(results, f)

    total_rows = results['players_rows'] + results['teams_rows']
    logger.info(f"Scraper complete: {total_rows} total rows")
    print(json.dumps(results))
    return 0


if __name__ == '__main__':
    sys.exit(main())
