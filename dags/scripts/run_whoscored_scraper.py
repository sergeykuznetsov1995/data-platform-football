#!/usr/bin/env python3
"""
WhoScored Scraper Runner Script
===============================

Standalone script to run :class:`WhoScoredScraper`. Called from Airflow via
BashOperator to avoid memory issues with PythonOperator.

The WhoScoredScraper exposes these high-level methods:
    * scrape_schedule()         — fixtures (full N seasons)
    * scrape_missing_players()  — pre-match injury / suspension list
    * scrape_season_stages()    — cup vs league stage metadata
    * scrape_events()           — per-match Opta events + lineups/ratings for
                                  ALL configured seasons; skip-existing per
                                  match keeps re-runs cheap (append-only).

W3 contract:
    --leagues       CSV (default: "ENG-Premier League")
    --seasons       CSV (default: "2024")
    --season        legacy single int alias for --seasons
    --skip-events   skip the heaviest task (`scrape_events`)
    --output        JSON output path (default: /tmp/whoscored_result.json)

JSON output (stable contract):
    {
      "rows":             int,        # totals (best-effort; tables remains the source of truth)
      "errors":           [str, ...],
      "tables":           [str, ...],
      "tables_by_entity": {entity: table_path, ...},
      "traffic":          {events: {...}, schedule: {...}},  # issue #616 audit
    }
"""

import argparse
import json
import logging
import os
import sys
from typing import List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def _parse_seasons(args: argparse.Namespace) -> List[int]:
    if args.seasons:
        return [int(s.strip()) for s in args.seasons.split(',') if s.strip()]
    return [int(args.season)]


def main() -> int:
    parser = argparse.ArgumentParser(description='Run WhoScored scraper')
    parser.add_argument(
        '--leagues',
        type=str,
        default='ENG-Premier League',
        help='Comma-separated list of leagues',
    )
    parser.add_argument(
        '--seasons',
        type=str,
        default='',
        help='Comma-separated list of season start years (e.g. "2021,2022,2023,2024,2025")',
    )
    parser.add_argument(
        '--season',
        type=int,
        default=2024,
        help='[Legacy] Single season — used only if --seasons is not provided',
    )
    parser.add_argument(
        '--skip-events',
        action='store_true',
        default=False,
        help='Skip the heaviest task (scrape_events). Useful for fast smoke runs.',
    )
    parser.add_argument(
        '--events-only',
        action='store_true',
        default=False,
        help=(
            'Run ONLY scrape_events (skip schedule/missing_players/season_stages). '
            'scrape_events reads game_ids from already-populated '
            'iceberg.bronze.whoscored_schedule, so this is safe when schedule has been '
            'ingested in a prior run and the soccerdata read_schedule path is failing.'
        ),
    )
    parser.add_argument(
        '--output',
        type=str,
        default='/tmp/whoscored_result.json',
        help='Output file for results',
    )
    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Run browser in headless mode (Discovery confirmed headless=True works)',
    )
    parser.add_argument(
        '--max-matches',
        type=int,
        default=None,
        help='Cap events scrape to N matches (smoke / verification runs)',
    )
    parser.add_argument(
        '--proxy-file',
        type=str,
        default='/opt/airflow/proxys.txt',
        help=(
            'Path to file with proxies (format: host:port:user:pass). '
            'Required for events scraping — WhoScored Cloudflare blocks per-IP.'
        ),
    )
    parser.add_argument(
        '--flaresolverr-url',
        type=str,
        default=os.environ.get('FLARESOLVERR_URL', 'http://flaresolverr:8191'),
        help='Base URL of FlareSolverr instance.',
    )
    parser.add_argument(
        '--player-profile',
        action='store_true',
        default=False,
        help=(
            'Run ONLY scrape_player_profile — biographical /Players/{id} '
            'snapshot. Reads player_ids from bronze.whoscored_events, so safe '
            'only after events have been ingested. Skips schedule/events.'
        ),
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Cap player_profile to N players (smoke / verification runs).',
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',') if l.strip()]
    seasons = _parse_seasons(args)
    logger.info(
        f"Starting WhoScored scraper: leagues={leagues}, seasons={seasons}, "
        f"skip_events={args.skip_events}, headless={args.headless}, "
        f"proxy_file={args.proxy_file}, flaresolverr_url={args.flaresolverr_url}"
    )

    results = {
        'rows': 0,
        'errors': [],
        'tables': [],
        'tables_by_entity': {},
        # Issue #616 — FlareSolverr proxy-traffic audit ({events, schedule}).
        'traffic': {},
    }

    try:
        # Lazy import to avoid pulling scrapers/__init__.py side-effects at parse time.
        from scrapers.whoscored import WhoScoredScraper

        with WhoScoredScraper(
            leagues=leagues,
            seasons=seasons,
            headless=args.headless,
            proxy_file=args.proxy_file,
            flaresolverr_url=args.flaresolverr_url,
        ) as scraper:
            if args.player_profile:
                logger.info("--player-profile set: running scrape_player_profile only")
                try:
                    out = scraper.scrape_player_profile(limit=args.limit) or {}
                    _merge(results, out)
                except Exception as e:
                    logger.error(f"scrape_player_profile failed: {e}", exc_info=True)
                    results['errors'].append(f"player_profile: {e}")
            elif args.events_only:
                logger.info("--events-only set: skipping schedule/missing/stages")
            else:
                # 1. Schedule (cheap, required)
                try:
                    out = scraper.scrape_schedule() or {}
                    _merge(results, out)
                except Exception as e:
                    logger.error(f"scrape_schedule failed: {e}", exc_info=True)
                    results['errors'].append(f"schedule: {e}")

                # 2. Missing players (cheap)
                try:
                    out = scraper.scrape_missing_players() or {}
                    _merge(results, out)
                except Exception as e:
                    logger.error(f"scrape_missing_players failed: {e}", exc_info=True)
                    results['errors'].append(f"missing_players: {e}")

                # 3. Season stages (cheap)
                try:
                    out = scraper.scrape_season_stages() or {}
                    _merge(results, out)
                except Exception as e:
                    logger.error(f"scrape_season_stages failed: {e}", exc_info=True)
                    results['errors'].append(f"season_stages: {e}")

            # 4. Events (heavy — only latest season; can be skipped)
            if args.player_profile:
                pass  # player-profile-only run: events deliberately skipped
            elif args.skip_events:
                logger.info("--skip-events set: not calling scrape_events()")
            else:
                try:
                    out = scraper.scrape_events(
                        max_matches=args.max_matches,
                    ) or {}
                    _merge(results, out)
                except Exception as e:
                    logger.error(f"scrape_events failed: {e}", exc_info=True)
                    results['errors'].append(f"events: {e}")

            # Issue #616: surface the FlareSolverr proxy-traffic audit for this
            # run (per-match proxy MB baseline; events + schedule sessions).
            try:
                results['traffic'] = scraper.get_traffic_stats()
            except Exception as e:
                logger.warning(f"get_traffic_stats failed: {e}")

    except Exception as e:
        logger.error(f"Scraper failed: {e}", exc_info=True)
        results['errors'].append(str(e))
        with open(args.output, 'w') as f:
            json.dump(results, f)
        sys.exit(1)

    # `rows` cannot be precisely known per task without an extra Trino round-trip;
    # downstream validators rely on Trino COUNT(*) checks against MIN_ROW_THRESHOLDS,
    # so we leave `rows` at 0 and surface the table list instead.
    with open(args.output, 'w') as f:
        json.dump(results, f)

    logger.info(
        f"Scraper complete: tables={len(results['tables'])}, errors={len(results['errors'])}"
    )
    print(json.dumps(results))
    return 1 if results.get('errors') else 0


def _merge(results: dict, entity_to_path: dict) -> None:
    """Fold a {entity: table_path} dict from a scrape_* method into the runner result."""
    for entity, path in entity_to_path.items():
        if not path:
            continue
        results['tables_by_entity'][entity] = path
        if path not in results['tables']:
            results['tables'].append(path)


if __name__ == '__main__':
    sys.exit(main())
