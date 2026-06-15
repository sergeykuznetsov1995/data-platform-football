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

# Replace-partitions completeness guard (#513 → #583): refuse a save that would
# shrink bronze.espn_schedule below this share of the existing (league, season)
# partition, so a partial/failed scrape can't wipe a good partition. COUNT(*)
# (one row per match — no replace_guard_key needed). ReplaceGuardError → exit 3;
# bypass with --force-replace for a deliberate first backfill / known shrink.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'ESPN_REPLACE_GUARD'


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
    parser.add_argument(
        '--force-replace',
        action='store_true',
        help='Bypass the completeness guard — write even if the scraped frame '
             'shrinks the existing partition. Use for a deliberate first '
             'backfill or a known legitimate shrink.'
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
        from scrapers.base.base_scraper import ReplaceGuardError
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
                        min_replace_ratio=(
                            None if args.force_replace else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['schedule_rows'] = len(df)
                    logger.info(f"Saved {len(df)} schedule rows")
            except ReplaceGuardError as e:
                # Guard refused the save (partial scrape would shrink the
                # partition) — nothing written. Distinct exit 3 so an operator
                # can tell a refused guard from a hard scrape failure (#583).
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                with open(args.output, 'w') as f:
                    json.dump(results, f)
                return 3
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
    # Issue #466: non-zero exit when any scrape step failed — otherwise the
    # BashOperator stays green while bronze.espn_schedule silently goes stale.
    return 1 if results.get('errors') else 0


if __name__ == '__main__':
    sys.exit(main())
