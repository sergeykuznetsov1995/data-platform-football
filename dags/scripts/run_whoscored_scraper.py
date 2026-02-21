#!/usr/bin/env python3
"""
WhoScored Scraper Runner Script
===============================

Standalone script to run WhoScored scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Uses Selenium with Cloudflare bypass for data collection.

IMPORTANT: WhoScored uses aggressive Cloudflare protection.
Recommended to run with headless=False for better success rate.
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
    parser = argparse.ArgumentParser(description='Run WhoScored scraper')
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
        default='/tmp/whoscored_result.json',
        help='Output file for results'
    )
    parser.add_argument(
        '--match-urls',
        type=str,
        default='',
        help='Comma-separated list of specific match URLs to scrape'
    )
    parser.add_argument(
        '--headless',
        action='store_true',
        default=False,
        help='Run browser in headless mode (not recommended for WhoScored)'
    )
    parser.add_argument(
        '--use-xvfb',
        action='store_true',
        default=True,
        help='Use xvfb for virtual display'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    match_urls = [u.strip() for u in args.match_urls.split(',') if u.strip()] if args.match_urls else []

    logger.info(f"Starting WhoScored scraper: leagues={leagues}, season={args.season}")
    logger.info(f"Headless: {args.headless}, use_xvfb: {args.use_xvfb}")
    if match_urls:
        logger.info(f"Specific match URLs: {len(match_urls)}")

    results = {
        'tables': [],
        'rows': 0,
        'errors': [],
        'matches_scraped': 0
    }

    try:
        import pandas as pd
        from scrapers.whoscored import WhoScoredScraper

        with WhoScoredScraper(
            leagues=leagues,
            seasons=[args.season],
            headless=args.headless,
            use_xvfb=args.use_xvfb,
        ) as scraper:
            all_events = []

            # If specific match URLs provided, use them
            if match_urls:
                urls_to_scrape = match_urls
            else:
                # Try to get URLs from scraper (if implemented)
                urls_to_scrape = []
                for league in leagues:
                    try:
                        league_urls = scraper.get_match_urls(league, args.season)
                        urls_to_scrape.extend([
                            (url, league, args.season) for url in league_urls
                        ])
                        logger.info(f"Found {len(league_urls)} match URLs for {league}")
                    except Exception as e:
                        logger.warning(f"Could not get match URLs for {league}: {e}")

            # Scrape each match
            for item in urls_to_scrape:
                if isinstance(item, tuple):
                    url, league, season = item
                else:
                    url = item
                    league = leagues[0] if leagues else 'Unknown'
                    season = args.season

                try:
                    df = scraper.read_match_events(url, league, season)
                    if df is not None and not df.empty:
                        all_events.append(df)
                        results['matches_scraped'] += 1
                        logger.info(f"Scraped {len(df)} events from {url}")

                except Exception as e:
                    error_msg = f"Error scraping {url}: {e}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)

            # Save combined events
            if all_events:
                combined_df = pd.concat(all_events, ignore_index=True)
                table_path = scraper.save_to_iceberg(
                    df=combined_df,
                    table_name='whoscored_events_spadl',
                    partition_cols=['league', 'season'],
                )
                results['tables'].append(table_path)
                results['rows'] = len(combined_df)
                logger.info(f"Saved {len(combined_df)} total events")

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        with open(args.output, 'w') as f:
            json.dump(results, f)
        sys.exit(1)

    # Write results
    with open(args.output, 'w') as f:
        json.dump(results, f)

    logger.info(f"Scraper complete: {results['rows']} rows from {results['matches_scraped']} matches")
    print(json.dumps(results))
    return 0


if __name__ == '__main__':
    sys.exit(main())
