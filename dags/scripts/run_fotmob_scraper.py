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

# Replace-partitions completeness guard (#513 → #583): refuse a save that would
# shrink a bronze.fotmob_* (league, season) partition below this share of its
# existing rows, so a partial/failed scrape can't wipe a good partition.
# COUNT(*) (no replace_guard_key) — each (league, season) is scraped full-state.
# ReplaceGuardError → exit 3; bypass with --force-replace.
_MIN_REPLACE_RATIO = 0.9
REPLACE_GUARD_MARKER = 'FOTMOB_REPLACE_GUARD'


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
    parser.add_argument(
        '--force-replace',
        action='store_true',
        help='Bypass the completeness guard — write even if the scraped frame '
             'shrinks the existing partition. Use for a deliberate first '
             'backfill or a known legitimate shrink.'
    )
    parser.add_argument(
        '--full-players',
        action='store_true',
        help='Re-fetch every squad player in player_details instead of only '
             'players missing from Bronze. Run periodically to refresh stale '
             'career/market-value payloads.'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]
    selected = {e.strip() for e in args.entities.split(',') if e.strip()}
    entities = [e for e in ENTITIES if not selected or e[0] in selected]

    # #920 bridge: single_year tournaments must never inherit the club-formula
    # season (July 2026 -> 2025): FotMob answers a season='2025' WC request
    # with CURRENT tournament data, silently mislabelling the partition.
    # Mixed club+WC calls can't carry two seasons -> WC is dropped (dedicated
    # call), mirroring run_whoscored_scraper / run_fbref_scraper.
    if 'INT-World Cup' in leagues:
        from utils.medallion_config import get_active_single_year_season
        _wc_season = get_active_single_year_season('INT-World Cup')
        if len(leagues) > 1:
            logger.warning(
                "INT-World Cup dropped from mixed call (needs its own season; "
                f"leagues={leagues}). Scrape it with --leagues 'INT-World Cup'.")
            leagues = [l for l in leagues if l != 'INT-World Cup']
        elif _wc_season is None:
            logger.warning(
                "INT-World Cup is out of its tournament window — nothing to "
                "scrape; exiting 0.")
            with open(args.output, 'w') as f:
                json.dump({'tables': [], 'rows': {}, 'errors': [],
                           'skipped': 'out_of_window'}, f)
            return 0
        elif int(args.season) != int(_wc_season):
            logger.info(
                f"INT-World Cup: overriding --season {args.season} -> "
                f"{_wc_season} (active single_year season, #920 bridge).")
            args.season = _wc_season

    logger.info(f"Starting FotMob scraper: leagues={leagues}, season={args.season}")
    logger.info(f"Entities: {[e[0] for e in entities]}")

    results = {
        'tables': [],
        'rows': {},
        'errors': [],
    }
    guard_refused = False

    try:
        from scrapers.base.base_scraper import ReplaceGuardError
        from scrapers.fotmob import FotMobScraper

        with FotMobScraper(leagues=leagues, seasons=[args.season],
                           full_players=args.full_players) as scraper:
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
                                min_replace_ratio=(
                                    None if args.force_replace
                                    else _MIN_REPLACE_RATIO
                                ),
                            )
                            results['tables'].append(table_path)
                            row_count += len(df)
                            logger.info(f"Saved {len(df)} {key} rows for {league}")
                    except ReplaceGuardError as e:
                        # Guard refused this (entity, league) save — a partial
                        # scrape would shrink the partition. Record + continue
                        # other entities/leagues; exit 3 at the end (#583).
                        msg = f"{REPLACE_GUARD_MARKER}: {key}/{league}: {e}"
                        logger.error(msg)
                        results['errors'].append(msg)
                        guard_refused = True
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
    # Exit 3 when the completeness guard refused any save (distinct from the
    # exit-0 path) so an operator can spot a refused guard in the BashOperator.
    return 3 if guard_refused else 0


if __name__ == '__main__':
    sys.exit(main())
