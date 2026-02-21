#!/usr/bin/env python3
"""
FotMob Scraper Runner Script
============================

Standalone script to run FotMob scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Uses Selenium with Cloudflare bypass for data collection.
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
    parser = argparse.ArgumentParser(description='Run FotMob scraper')
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
        default='/tmp/fotmob_result.json',
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
    logger.info(f"Starting FotMob scraper: leagues={leagues}, season={args.season}")
    logger.info(f"Headless: {args.headless}, use_xvfb: {args.use_xvfb}")

    results = {
        'tables': [],
        'schedule_rows': 0,
        'team_stats_rows': 0,
        'player_stats_rows': 0,
        'errors': []
    }

    try:
        from scrapers.fotmob import FotMobScraper

        with FotMobScraper(
            leagues=leagues,
            seasons=[args.season],
            headless=args.headless,
            use_xvfb=args.use_xvfb,
        ) as scraper:
            # Scrape schedule for each league
            for league in leagues:
                try:
                    df = scraper.read_schedule(league, args.season)
                    if df is not None and not df.empty:
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='fotmob_schedule',
                            partition_cols=['league', 'season'],
                        )
                        results['tables'].append(table_path)
                        results['schedule_rows'] += len(df)
                        logger.info(f"Saved {len(df)} schedule rows for {league}")
                except Exception as e:
                    error_msg = f"Schedule scraping for {league} failed: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

            # Scrape team stats for each league
            for league in leagues:
                try:
                    df = scraper.read_team_season_stats(league, args.season)
                    if df is not None and not df.empty:
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='fotmob_team_stats',
                            partition_cols=['league', 'season'],
                        )
                        results['tables'].append(table_path)
                        results['team_stats_rows'] += len(df)
                        logger.info(f"Saved {len(df)} team stats for {league}")
                except Exception as e:
                    error_msg = f"Team stats scraping for {league} failed: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

            # Scrape player stats for each league
            for league in leagues:
                try:
                    df = scraper.read_player_season_stats('goals', league, args.season)
                    if df is not None and not df.empty:
                        table_path = scraper.save_to_iceberg(
                            df=df,
                            table_name='fotmob_player_stats',
                            partition_cols=['league', 'season'],
                        )
                        results['tables'].append(table_path)
                        results['player_stats_rows'] += len(df)
                        logger.info(f"Saved {len(df)} player stats for {league}")
                except Exception as e:
                    error_msg = f"Player stats scraping for {league} failed: {e}"
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

    total_rows = results['schedule_rows'] + results['team_stats_rows'] + results['player_stats_rows']
    logger.info(f"Scraper complete: {total_rows} total rows")
    print(json.dumps(results))
    return 0


if __name__ == '__main__':
    sys.exit(main())
