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
    parser.add_argument(
        '--mode',
        choices=['daily', 'full'],
        default='daily',
        help="daily = current ratings only (fast, 1 HTTP call); "
             "full = + historical + team_history (heavy, weekly cadence)"
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    logger.info(f"Starting ClubElo scraper (mode={args.mode}) with leagues: {leagues}")

    results = {
        'tables': [],
        'rows': 0,
        'history_rows': 0,
        'team_history_rows': 0,
        'errors': [],
        'rating_date': None
    }
    # Only the current-ratings stage is critical: its failure fails the DAG.
    # Heavy stages (historical/team_history) only run in --mode full and their
    # failures are recorded as warnings, not a non-zero exit.
    critical_failed = False

    try:
        # Import here to avoid issues at module load time
        from scrapers.clubelo import ClubEloScraper

        with ClubEloScraper(leagues=leagues) as scraper:
            today = datetime.now()
            logger.info(f"Fetching ratings for {today.date()}")

            # --- Stage 1: current ratings (CRITICAL, every mode) ---
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
                critical_failed = True
                results['errors'].append('current_ratings: no data returned')
                logger.warning("No data returned from scraper")

            if args.mode == 'full':
                # --- Stage 2: historical ratings (non-critical) ---
                try:
                    hist = scraper.scrape_historical_ratings()
                    if hist.get('historical_ratings'):
                        results['tables'].append(hist['historical_ratings'])
                        results['history_rows'] = hist.get('rows', 0)
                        logger.info(
                            f"Saved {results['history_rows']} historical rows"
                        )
                except Exception as e:
                    logger.error(f"historical_ratings failed: {e}", exc_info=True)
                    results['errors'].append(f'historical: {e}')

                # --- Stage 3: team histories (non-critical) ---
                try:
                    teams = None
                    if df is not None and 'team' in df.columns:
                        teams = df['team'].dropna().astype(str).unique().tolist()
                    th = scraper.scrape_team_histories(teams=teams)
                    if th.get('team_history'):
                        results['tables'].append(th['team_history'])
                        results['team_history_rows'] = th.get('rows', 0)
                        logger.info(
                            f"Saved {results['team_history_rows']} team-history rows"
                        )
                except Exception as e:
                    logger.error(f"team_histories failed: {e}", exc_info=True)
                    results['errors'].append(f'team_history: {e}')

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
