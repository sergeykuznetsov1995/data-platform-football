#!/usr/bin/env python3
"""
MatchHistory Scraper Runner Script
==================================

Standalone script to run MatchHistory scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Supports Selenium with xvfb for headless browser operation.
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
    parser = argparse.ArgumentParser(description='Run MatchHistory scraper')
    parser.add_argument(
        '--leagues',
        type=str,
        default='ENG-Premier League',
        help='Comma-separated list of leagues'
    )
    parser.add_argument(
        '--season',
        type=int,
        default=2024,
        help='Season year'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/matchhistory_result.json',
        help='Output file for results'
    )
    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Run browser in headless mode'
    )
    parser.add_argument(
        '--use-xvfb',
        action='store_true',
        default=True,
        help='Use xvfb for virtual display'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    logger.info(f"Starting MatchHistory scraper: leagues={leagues}, season={args.season}")
    logger.info(f"Headless: {args.headless}, use_xvfb: {args.use_xvfb}")

    results = {
        'tables': [],
        'rows': 0,
        'errors': [],
        'league_details': {}
    }

    try:
        import pandas as pd
        from scrapers.matchhistory import MatchHistoryScraper

        with MatchHistoryScraper(
            leagues=leagues,
            seasons=[args.season],
            headless=args.headless,
            use_xvfb=args.use_xvfb,
        ) as scraper:
            all_matches = []

            for league in leagues:
                try:
                    df = scraper.read_games(league, args.season)
                    if df is not None and not df.empty:
                        # Calculate odds statistics
                        df = scraper.calculate_odds_stats(df)
                        all_matches.append(df)
                        results['league_details'][league] = len(df)
                        results['rows'] += len(df)
                        logger.info(f"Fetched {len(df)} matches for {league}")
                    else:
                        error_msg = f"No data for {league}"
                        logger.warning(error_msg)
                        results['errors'].append(error_msg)
                except Exception as e:
                    error_msg = f"Error scraping {league}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

            # Save combined results
            if all_matches:
                combined_df = pd.concat(all_matches, ignore_index=True)
                table_path = scraper.save_to_iceberg(
                    df=combined_df,
                    table_name='matchhistory_results',
                    partition_cols=['league', 'season'],
                    replace_partitions=['league', 'season'],
                )
                results['tables'].append(table_path)
                logger.info(f"Saved {len(combined_df)} total rows")

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        with open(args.output, 'w') as f:
            json.dump(results, f)
        sys.exit(1)

    # Write results
    with open(args.output, 'w') as f:
        json.dump(results, f)

    logger.info(f"Scraper complete: {results['rows']} total rows")
    print(json.dumps(results))
    return 0


if __name__ == '__main__':
    sys.exit(main())
