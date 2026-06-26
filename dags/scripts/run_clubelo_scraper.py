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

# Replace-partitions completeness guard (#513 → #583): refuse a save that would
# shrink a clubelo_ratings* partition below this share of its existing rows, so
# a partial/failed scrape can't wipe a good partition. COUNT(*) (full-state
# snapshot per rating_date — no replace_guard_key needed). ReplaceGuardError →
# exit 3 (current ratings, critical); the historical stage records the marker
# but stays non-fatal. Bypass with --force-replace for a deliberate backfill.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'CLUBELO_REPLACE_GUARD'


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
    parser.add_argument(
        '--mode',
        choices=['daily', 'full'],
        default='daily',
        help="daily = current ratings only (fast, 1 HTTP call); "
             "full = + historical ratings (heavy, weekly cadence)"
    )
    parser.add_argument(
        '--force-replace',
        action='store_true',
        help='Bypass the completeness guard — write even if the scraped frame '
             'shrinks the existing partition. Use for a deliberate first '
             'backfill or a known legitimate shrink.'
    )
    parser.add_argument(
        '--days-back',
        type=int,
        default=365,
        help='How many days of history to weekly-sample in --mode full '
             '(default 365 = the recurring weekly refresh). Use a large value '
             'for a one-time deep backfill, e.g. 3650 ≈ 10 seasons of APL '
             '(issue #716). Ignored in --mode daily.'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    logger.info(f"Starting ClubElo scraper (mode={args.mode}) with leagues: {leagues}")

    results = {
        'tables': [],
        'rows': 0,
        'history_rows': 0,
        'errors': [],
        'rating_date': None
    }
    # Only the current-ratings stage is critical: its failure fails the DAG.
    # The heavy historical stage only runs in --mode full and its failure is
    # recorded as a warning, not a non-zero exit.
    critical_failed = False

    try:
        # Import here to avoid issues at module load time
        from scrapers.base.base_scraper import ReplaceGuardError
        from scrapers.clubelo import ClubEloScraper

        with ClubEloScraper(leagues=leagues) as scraper:
            today = datetime.now()
            logger.info(f"Fetching ratings for {today.date()}")

            # --- Stage 1: current ratings (CRITICAL, every mode) ---
            df = scraper.read_by_date(today)

            if df is not None and not df.empty:
                try:
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='clubelo_ratings',
                        partition_cols=['rating_date'],
                        # Replace the day's partition so a same-day rerun / Airflow
                        # retry overwrites instead of appending a duplicate snapshot.
                        # read_by_date now yields a date-only ISO rating_date and the
                        # table column is varchar, so the delete predicate is valid
                        # (#554, split from #470 bug 5). This runner inlines the save
                        # instead of calling scrape_current_ratings(), so the fix must
                        # live here too — not only on the scraper method.
                        replace_partitions=['rating_date'],
                        min_replace_ratio=(
                            None if args.force_replace else _MIN_REPLACE_RATIO
                        ),
                    )
                except ReplaceGuardError as e:
                    # Guard refused: a partial scrape would shrink today's
                    # snapshot partition — nothing written. Distinct exit 3 so an
                    # operator can tell a refused guard from a hard failure (#583).
                    msg = f"{REPLACE_GUARD_MARKER}: {e}"
                    logger.error(msg)
                    results['errors'].append(msg)
                    with open(args.output, 'w') as f:
                        json.dump(results, f)
                    return 3
                results['tables'].append(table_path)
                results['rows'] = len(df)
                results['rating_date'] = today.strftime('%Y-%m-%d')
                logger.info(f"Saved {len(df)} ELO ratings for {today.date()}")
            else:
                critical_failed = True
                results['errors'].append('current_ratings: no data returned')
                logger.warning("No data returned from scraper")

            if args.mode == 'full':
                # --- Stage 2: historical ratings (non-critical) ---
                try:
                    hist = scraper.scrape_historical_ratings(
                        days_back=args.days_back,
                        force_replace=args.force_replace,
                    )
                    if hist.get('historical_ratings'):
                        results['tables'].append(hist['historical_ratings'])
                        results['history_rows'] = hist.get('rows', 0)
                        logger.info(
                            f"Saved {results['history_rows']} historical rows"
                        )
                except ReplaceGuardError as e:
                    # Non-critical stage: a refused historical guard is a
                    # warning, not a DAG failure (exit stays 0). The marker
                    # still surfaces it to validate_data / Telegram.
                    msg = f"{REPLACE_GUARD_MARKER}: {e}"
                    logger.error(msg)
                    results['errors'].append(msg)
                except Exception as e:
                    logger.error(f"historical_ratings failed: {e}", exc_info=True)
                    results['errors'].append(f'historical: {e}')

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        critical_failed = True

    # Write results (even on failure)
    with open(args.output, 'w') as f:
        json.dump(results, f)

    logger.info(f"Scraper complete: {results['rows']} ratings rows")
    print(json.dumps(results))  # Also print for Airflow logs
    return 1 if critical_failed else 0


if __name__ == '__main__':
    sys.exit(main())
