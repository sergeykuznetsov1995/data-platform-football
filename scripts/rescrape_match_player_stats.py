#!/usr/bin/env python3
"""
Re-scrape match player stats for all seasons with player_id extraction.

Runs up to WORKERS parallel processes (one per season) for ~3x speedup.

Usage (inside airflow container):
    python /opt/airflow/scripts/rescrape_match_player_stats.py

    # Single season:
    python /opt/airflow/scripts/rescrape_match_player_stats.py --season 2025

    # Custom parallelism:
    python /opt/airflow/scripts/rescrape_match_player_stats.py --workers 2
"""

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

for name in ['nodriver', 'uc', 'urllib3', 'websockets', 'asyncio',
             'selenium', 'undetected_chromedriver', 'hpack', 'httpx']:
    logging.getLogger(name).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

ALL_SEASONS = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
LEAGUE = 'ENG-Premier League'
PROXY_FILE = '/opt/airflow/proxys.txt'


def scrape_season(season: int) -> dict:
    """Scrape one season in a separate process."""
    # Re-configure logging per process
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s [S{season}] %(levelname)s %(name)s: %(message)s',
        datefmt='%H:%M:%S',
        force=True,
    )
    for name in ['nodriver', 'uc', 'urllib3', 'websockets', 'asyncio',
                 'selenium', 'undetected_chromedriver', 'hpack', 'httpx']:
        logging.getLogger(name).setLevel(logging.WARNING)

    log = logging.getLogger(f'season.{season}')
    log.info(f"Starting season {season}")
    t0 = time.time()

    try:
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=[LEAGUE],
            seasons=[season],
            headless=True,
            use_xvfb=True,
            use_nodriver=True,
            proxy_file=PROXY_FILE,
        )

        with scraper:
            results = scraper.scrape_combined_match_data(
                max_matches=None,
                incremental=True,
            )

        stats = scraper._stats
        elapsed = time.time() - t0
        log.info(
            f"Season {season} done in {elapsed/60:.1f}m: "
            f"successes={stats.get('successes', 0)}, "
            f"failures={stats.get('failures', 0)}, "
            f"tables={list(results.keys())}"
        )
        return {
            'season': season,
            'ok': True,
            'successes': stats.get('successes', 0),
            'failures': stats.get('failures', 0),
            'elapsed_min': round(elapsed / 60, 1),
        }

    except Exception as e:
        elapsed = time.time() - t0
        log.error(f"Season {season} failed after {elapsed/60:.1f}m: {e}", exc_info=True)
        return {
            'season': season,
            'ok': False,
            'error': str(e),
            'elapsed_min': round(elapsed / 60, 1),
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--season', type=int, default=None, help='Single season to scrape')
    parser.add_argument('--workers', type=int, default=3, help='Parallel workers (default: 3)')
    args = parser.parse_args()

    seasons = [args.season] if args.season else ALL_SEASONS
    workers = min(args.workers, len(seasons))

    logger.info(f"Scraping {len(seasons)} seasons with {workers} parallel workers")
    t0 = time.time()

    results = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(scrape_season, s): s for s in seasons}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            status = "OK" if result['ok'] else f"FAIL: {result.get('error', '?')}"
            logger.info(
                f"Season {result['season']}: {status} "
                f"({result['elapsed_min']}m)"
            )

    elapsed = time.time() - t0
    ok = sum(1 for r in results if r['ok'])
    logger.info(f"All done in {elapsed/60:.1f}m: {ok}/{len(seasons)} seasons OK")

    for r in sorted(results, key=lambda x: x['season']):
        s = r['season']
        if r['ok']:
            logger.info(f"  {s}: {r['successes']} matches, {r['failures']} failures, {r['elapsed_min']}m")
        else:
            logger.info(f"  {s}: FAILED - {r.get('error', '?')}")


if __name__ == '__main__':
    main()
