#!/usr/bin/env python3
"""
ClubElo Scraper Runner Script
==============================

Standalone script to run ClubElo scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.
"""

import argparse
import json
import logging
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Run ClubElo scraper')
    parser.add_argument(
        '--leagues',
        type=str,
        default='ENG-Premier League',
        help='Comma-separated list of leagues'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/clubelo_result.json',
        help='Output file for results'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    logger.info(f"Starting ClubElo scraper with leagues: {leagues}")

    results = {
        'tables': [],
        'rows': 0,
        'errors': [],
        'rating_date': None
    }

    try:
        # Import here to avoid issues at module load time
        from scrapers.clubelo import ClubEloScraper

        with ClubEloScraper(leagues=leagues) as scraper:
            today = datetime.now()
            logger.info(f"Fetching ratings for {today.date()}")

            df = scraper.read_by_date(today)

            if df is not None and not df.empty:
                table_path = scraper.save_to_iceberg(
                    df=df,
                    table_name='clubelo_ratings',
                    partition_cols=['rating_date'],
                )
                results['tables'].append(table_path)
                results['rows'] = len(df)
                results['rating_date'] = today.strftime('%Y-%m-%d')
                logger.info(f"Saved {len(df)} ELO ratings for {today.date()}")
            else:
                logger.warning("No data returned from scraper")

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        # Write results even on failure
        with open(args.output, 'w') as f:
            json.dump(results, f)
        sys.exit(1)

    # Write results
    with open(args.output, 'w') as f:
        json.dump(results, f)

    logger.info(f"Scraper complete: {results['rows']} rows")
    print(json.dumps(results))  # Also print for Airflow logs
    return 0


if __name__ == '__main__':
    sys.exit(main())
