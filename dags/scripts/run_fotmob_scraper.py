#!/usr/bin/env python3
"""
FotMob Scraper Runner Script
============================

Standalone script to run the FotMob scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Pure HTTP — FotMob's public ``/api/data`` endpoints require no browser, no
Cloudflare bypass and no cookies.
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


# (results_key, iceberg_table, read_callable) for every Bronze entity.
# read_callable signature: (scraper, league, season) -> Optional[DataFrame]
ENTITIES = [
    ('schedule', 'fotmob_schedule',
     lambda s, lg, se: s.read_schedule(lg, se)),
    ('team_stats', 'fotmob_team_stats',
     lambda s, lg, se: s.read_team_season_stats(lg, se)),
    ('player_stats', 'fotmob_player_stats',
     lambda s, lg, se: s.read_player_season_stats('goals', lg, se)),
    ('team_profile', 'fotmob_team_profile',
     lambda s, lg, se: s.read_team_profile(lg, se)),
    ('team_squad', 'fotmob_team_squad',
     lambda s, lg, se: s.read_team_squad(lg, se)),
    ('team_leaderboards', 'fotmob_team_leaderboards',
     lambda s, lg, se: s.read_team_leaderboards(lg, se)),
    ('transfers', 'fotmob_transfers',
     lambda s, lg, se: s.read_transfers(lg, se)),
    ('match_details', 'fotmob_match_details',
     lambda s, lg, se: s.read_match_details(lg, se)),
    ('player_details', 'fotmob_player_details',
     lambda s, lg, se: s.read_player_details(lg, se)),
]


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
        default=2025,
        help='Season year'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/fotmob_result.json',
        help='Output file for results'
    )
    parser.add_argument(
        '--entities',
        type=str,
        default='',
        help='Optional comma-separated subset of entity keys to scrape '
             '(default: all). e.g. "team_profile,team_squad"'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    selected = {e.strip() for e in args.entities.split(',') if e.strip()}
    entities = [e for e in ENTITIES if not selected or e[0] in selected]

    logger.info(f"Starting FotMob scraper: leagues={leagues}, season={args.season}")
    logger.info(f"Entities: {[e[0] for e in entities]}")

    results = {
        'tables': [],
        'rows': {},
        'errors': [],
    }

    try:
        from scrapers.fotmob import FotMobScraper

        with FotMobScraper(leagues=leagues, seasons=[args.season]) as scraper:
            for key, table_name, read_fn in entities:
                row_count = 0
                for league in leagues:
                    try:
                        df = read_fn(scraper, league, args.season)
                        if df is not None and not df.empty:
                            table_path = scraper.save_to_iceberg(
                                df=df,
                                table_name=table_name,
                                partition_cols=['league', 'season'],
                                replace_partitions=['league', 'season'],
                            )
                            results['tables'].append(table_path)
                            row_count += len(df)
                            logger.info(f"Saved {len(df)} {key} rows for {league}")
                    except Exception as e:
                        error_msg = f"{key} scraping for {league} failed: {e}"
                        logger.error(error_msg)
                        results['errors'].append(error_msg)
                results['rows'][key] = row_count
                # Legacy flat keys (kept for backward-compatible consumers)
                results[f'{key}_rows'] = row_count

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        with open(args.output, 'w') as f:
            json.dump(results, f)
        sys.exit(1)

    # Write results
    with open(args.output, 'w') as f:
        json.dump(results, f)

    total_rows = sum(results['rows'].values())
    logger.info(f"Scraper complete: {total_rows} total rows across "
                f"{len(results['rows'])} entities")
    print(json.dumps(results))
    return 0


if __name__ == '__main__':
    sys.exit(main())
