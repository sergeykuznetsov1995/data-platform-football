#!/usr/bin/env python3
"""
Understat Scraper Runner Script
===============================

Standalone script to run Understat scraper.
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
# shrink a bronze.understat_* (league, season) partition below this share of its
# existing rows, so a partial/failed scrape can't wipe a good partition.
# COUNT(*) (no replace_guard_key) — each (league, season) is scraped full-state.
# ReplaceGuardError → exit 3; bypass with --force-replace.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'UNDERSTAT_REPLACE_GUARD'


def main():
    parser = argparse.ArgumentParser(description='Run Understat scraper')
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
        default='/tmp/understat_result.json',
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
    logger.info(f"Starting Understat scraper: leagues={leagues}, season={args.season}")

    results = {
        'tables': [],
        'schedule_rows': 0,
        'shots_rows': 0,
        'player_stats_rows': 0,
        'team_match_stats_rows': 0,
        'player_match_stats_rows': 0,
        'errors': []
    }
    guard_refused = False

    try:
        from scrapers.base.base_scraper import ReplaceGuardError
        from scrapers.understat import UnderstatScraper

        with UnderstatScraper(leagues=leagues, seasons=[args.season]) as scraper:
            # Scrape schedule
            try:
                df = scraper.read_schedule()
                if df is not None and not df.empty:
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='understat_schedule',
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
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Schedule scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape shot events
            try:
                df = scraper.read_shot_events()
                if df is not None and not df.empty:
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='understat_shots',
                        partition_cols=['league', 'season'],
                        replace_partitions=['league', 'season'],
                        min_replace_ratio=(
                            None if args.force_replace else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['shots_rows'] = len(df)
                    logger.info(f"Saved {len(df)} shot events")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Shots scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape player stats
            try:
                df = scraper.read_player_season_stats()
                if df is not None and not df.empty:
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='understat_players',
                        partition_cols=['league', 'season'],
                        replace_partitions=['league', 'season'],
                        min_replace_ratio=(
                            None if args.force_replace else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['player_stats_rows'] = len(df)
                    logger.info(f"Saved {len(df)} player stats")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Player stats scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape team match-level stats (xG/xGA per match)
            try:
                df = scraper.read_team_match_stats()
                if df is not None and not df.empty:
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='understat_team_match_stats',
                        partition_cols=['league', 'season'],
                        replace_partitions=['league', 'season'],
                        min_replace_ratio=(
                            None if args.force_replace else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['team_match_stats_rows'] = len(df)
                    logger.info(f"Saved {len(df)} team match stats")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Team match stats scraping failed: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)

            # Scrape per-match player stats (Silver layer requires this for xG joins)
            try:
                df = scraper.read_player_match_stats()
                if df is not None and not df.empty:
                    table_path = scraper.save_to_iceberg(
                        df=df,
                        table_name='understat_player_match_stats',
                        partition_cols=['league', 'season'],
                        replace_partitions=['league', 'season'],
                        min_replace_ratio=(
                            None if args.force_replace else _MIN_REPLACE_RATIO
                        ),
                    )
                    results['tables'].append(table_path)
                    results['player_match_stats_rows'] = len(df)
                    logger.info(f"Saved {len(df)} player match stats")
            except ReplaceGuardError as e:
                msg = f"{REPLACE_GUARD_MARKER}: {e}"
                logger.error(msg)
                results['errors'].append(msg)
                guard_refused = True
            except Exception as e:
                error_msg = f"Player match stats scraping failed: {e}"
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

    total_rows = (
        results['schedule_rows'] +
        results['shots_rows'] +
        results['player_stats_rows'] +
        results['team_match_stats_rows'] +
        results['player_match_stats_rows']
    )
    logger.info(f"Scraper complete: {total_rows} total rows")
    print(json.dumps(results))
    # Exit 3 when the completeness guard refused any save (distinct from the
    # exit-1 hard-failure path) so an operator can spot a refused guard (#583).
    return 3 if guard_refused else (1 if results.get('errors') else 0)


if __name__ == '__main__':
    sys.exit(main())
