#!/usr/bin/env python3
"""
ESPN Scraper Runner Script
==========================

Standalone script to run ESPN scraper.
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
    parser = argparse.ArgumentParser(description='Run ESPN scraper')
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
        default='/tmp/espn_result.json',
        help='Output file for results'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    logger.info(f"Starting ESPN scraper: leagues={leagues}, season={args.season}")

    results = {
        'tables': [],
        'schedule_rows': 0,
        'errors': []
    }

    try:
        from scrapers.espn import ESPNScraper

        with ESPNScraper(leagues=leagues, seasons=[args.season]) as scraper:
            # Scrape schedule
            try:
                df = scraper.read_schedule()
                if df is not None and not df.empty:
                    df = scraper._standardize_schedule(df)
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='espn_schedule',
                        partition_cols=['league', 'season'],
                        replace_partitions=['league', 'season'],
                    )
                    results['tables'].append(table_path)
                    results['schedule_rows'] = len(df)
                    logger.info(f"Saved {len(df)} schedule rows")
            except Exception as e:
                error_msg = f"Schedule scraping failed: {e}"
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

    logger.info(f"Scraper complete: {results['schedule_rows']} total rows")
    print(json.dumps(results))
    return 0


if __name__ == '__main__':
    sys.exit(main())
