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

# Replace-partitions completeness guard (#513 → #583): refuse a save that would
# shrink bronze.matchhistory_results below this share of the existing
# (league, season) partition, so a partial/failed scrape can't wipe a good
# partition. COUNT(*) (one row per match — no replace_guard_key needed).
# ReplaceGuardError → exit 3; bypass with --force-replace for a deliberate
# first backfill / known legitimate shrink.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'MATCHHISTORY_REPLACE_GUARD'


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
    parser.add_argument(
        '--force-replace',
        action='store_true',
        help='Bypass the completeness guard — write even if the scraped frame '
             'shrinks the existing partition. Use for a deliberate first '
             'backfill or a known legitimate shrink.'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    logger.info(f"Starting MatchHistory scraper: leagues={leagues}, season={args.season}")
    logger.info(f"Headless: {args.headless}, use_xvfb: {args.use_xvfb}")

    results = {
        'tables': [],
        'rows': 0,
        'errors': [],
        'league_details': {},
        'skipped_not_modified': [],
    }

    try:
        import pandas as pd
        from scrapers.base.base_scraper import ReplaceGuardError
        from scrapers.matchhistory import NOT_MODIFIED, MatchHistoryScraper

        with MatchHistoryScraper(
            leagues=leagues,
            seasons=[args.season],
            headless=args.headless,
            use_xvfb=args.use_xvfb,
            # A deliberate re-ingest must also bypass the 304 short-circuit,
            # not just the completeness guard.
            force_refresh=args.force_replace,
        ) as scraper:
            all_matches = []

            for league in leagues:
                try:
                    df = scraper.read_games(league, args.season)
                    if df is NOT_MODIFIED:
                        # Season CSV unchanged since the last successful
                        # ingest — the partition already holds this data.
                        results['skipped_not_modified'].append(league)
                        logger.info(f"{league}: CSV not modified — skipping")
                        continue
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
                try:
                    table_path = scraper.save_to_iceberg(
                        df=combined_df,
                        table_name='matchhistory_results',
                        partition_cols=['league', 'season'],
                        replace_partitions=['league', 'season'],
                        min_replace_ratio=(
                            None if args.force_replace else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    logger.info(f"Saved {len(combined_df)} total rows")
                    # Data landed — now it is safe to persist the ETag/
                    # Last-Modified validators so the next run can 304-skip.
                    scraper.commit_http_meta()
                except ReplaceGuardError as e:
                    # Guard refused the save (partial scrape would shrink the
                    # partition) — nothing written. Distinct exit 3 so an
                    # operator can tell a refused guard from a hard scrape
                    # failure (#583). Meta NOT committed: next run refetches.
                    msg = f"{REPLACE_GUARD_MARKER}: {e}"
                    logger.error(msg)
                    results['errors'].append(msg)
                    with open(args.output, 'w') as f:
                        json.dump(results, f)
                    return 3
            elif results['skipped_not_modified'] and not results['errors']:
                # Every league answered 304 — clean no-op, nothing to write.
                results['status'] = 'no_op'
                logger.info(
                    "All leagues not modified "
                    f"({len(results['skipped_not_modified'])}) — no-op run"
                )

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
