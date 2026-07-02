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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Replace-partitions completeness guard (#513 → #583): the scraper methods
# refuse a save that would shrink a clubelo_ratings* partition below 90% of its
# existing rows, so a partial/failed scrape can't wipe a good partition.
# ReplaceGuardError → exit 3 (current ratings, critical); the historical stage
# records the marker but stays non-fatal. Bypass with --force-replace for a
# deliberate backfill.
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
             "full = historical ratings ONLY (heavy, weekly cadence; the "
             "daily task in the DAG chain already covers current ratings)"
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
            if args.mode == 'daily':
                # --- Current ratings (CRITICAL) ---
                try:
                    current = scraper.scrape_current_ratings(
                        force_replace=args.force_replace,
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

                if current.get('current_ratings'):
                    results['tables'].append(current['current_ratings'])
                    results['rows'] = current.get('rows', 0)
                    results['rating_date'] = current.get('rating_date')
                    logger.info(
                        f"Saved {results['rows']} ELO ratings "
                        f"for {results['rating_date']}"
                    )
                else:
                    critical_failed = True
                    results['errors'].append('current_ratings: no data returned')
                    logger.warning("No data returned from scraper")
            else:
                # --- Historical ratings ONLY (--mode full, non-critical) ---
                # The daily task already scraped current ratings earlier in the
                # DAG chain (scrape_ratings >> gate >> full); re-scraping them
                # here was a duplicate HTTP call + duplicate partition write.
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
                    else:
                        # Not silent (#716 keeps the stage non-critical, exit 0,
                        # but the error must surface in the results JSON).
                        results['errors'].append('historical: no data returned')
                        logger.error("historical_ratings returned no data")
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
