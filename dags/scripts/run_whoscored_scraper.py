#!/usr/bin/env python3
"""
WhoScored Scraper Runner Script
===============================

Standalone script to run :class:`WhoScoredScraper`. Called from Airflow via
BashOperator to avoid memory issues with PythonOperator.

The WhoScoredScraper exposes these high-level methods:
    * scrape_schedule()         — fixtures (full N seasons); ~10-14 fetches
                                  per (league, season) — calendar + monthly
                                  fixture JSONs, NOT per-match
    * scrape_missing_players()  — pre-match injury / suspension list. HEAVY:
                                  soccerdata fetches one /Matches/{id}/Preview
                                  page PER MATCH (~380 pages per league/season
                                  through FlareSolverr, ~5 s each) — same
                                  order of cost as scrape_events (#878)
    * scrape_season_stages()    — cup vs league stage metadata (~1 fetch,
                                  calendar comes from the soccerdata cache)
    * scrape_events()           — per-match Opta events + lineups/ratings for
                                  ALL configured seasons; skip-existing per
                                  match keeps re-runs cheap (append-only).

W3 contract:
    --leagues       CSV (default: "ENG-Premier League")
    --seasons       CSV (default: "2024")
    --season        legacy single int alias for --seasons
    --skip-events   skip the heaviest task (`scrape_events`)
    --skip-missing-players  skip the per-match `scrape_missing_players` (#878);
                    with --skip-events this is the fast schedule path
                    (schedule + season_stages, ~15 requests, minutes)
    --skip-existing skip (league, season) pairs already complete in bronze
                    (#878; fbref #877 pattern). Only honored together with
                    --skip-events --skip-missing-players. Completeness =
                    bronze.whoscored_schedule >= WHOSCORED_SCHEDULE_MIN_ROWS
                    (default 270) AND bronze.whoscored_season_stages >=
                    WHOSCORED_STAGES_MIN_ROWS (default 1) — BOTH tables,
                    because the runner order is schedule → missing_players →
                    season_stages and a timed-out unit has schedule written
                    but no stages. Fail-open on Trino errors. The current
                    season is never skipped.
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
from datetime import datetime
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


def _trino_connect():
    """Open a Trino dbapi connection from env. Returns None on import error.

    Same pattern as run_fbref_scraper.py (#877 skip-existing probe):
    TRINO_PASSWORD set -> https:8443 BasicAuth, else plain http:8080.
    """
    try:
        import trino
        import trino.auth as trino_auth
    except ImportError as e:
        logger.error("trino client unavailable: %s", e)
        return None

    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')
    if password:
        return trino.dbapi.connect(
            host=os.environ.get('TRINO_HOST', 'trino'),
            port=int(os.environ.get('TRINO_PORT', 8443)),
            user=user,
            catalog='iceberg',
            http_scheme='https',
            auth=trino_auth.BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8080)),
        user=user,
        catalog='iceberg',
    )


def _season_to_bronze_str(season) -> str:
    """Normalize a 4-digit season token to the bronze 'YYZZ' slug.

    Local copy of ``scrapers.whoscored.scraper._season_to_soccerdata_str`` —
    NOT imported from there because the unit tests stub the whole
    ``scrapers.whoscored`` module with a MagicMock (heavy browser deps), which
    would turn the imported function into a mock too. Keep the two in sync.
    """
    s = str(season)
    if len(s) != 4 or not s.isdigit():
        raise ValueError(f"Unrecognized season token: {season!r}")
    if (int(s[:2]) + 1) % 100 == int(s[2:]):
        return s
    if s[2:] == "99":
        return "9900"
    return s[-2:] + f"{(int(s[-2:]) + 1) % 100:02d}"


def _season_start_year(season) -> int:
    """Season start year for both input forms: 2016 -> 2016, 2526 -> 2025."""
    s = str(season)
    if len(s) != 4 or not s.isdigit():
        raise ValueError(f"Unrecognized season token: {season!r}")
    if s == "9900":
        return 1999
    if (int(s[:2]) + 1) % 100 == int(s[2:]):
        return 2000 + int(s[:2])
    return int(s)


def _completed_schedule_pairs(leagues: List[str], season_strs: List[str]) -> set:
    """Return the (league, season_str) pairs already complete in bronze.

    A pair is complete only when BOTH ``bronze.whoscored_schedule`` has
    >= WHOSCORED_SCHEDULE_MIN_ROWS rows (default 270 — GER full season is
    306, the rest of the top-5 380, covid seasons stay above 270) AND
    ``bronze.whoscored_season_stages`` has >= WHOSCORED_STAGES_MIN_ROWS
    (default 1). The stages check matters: the runner order is schedule →
    missing_players → season_stages, so a unit killed by the backfill
    timeout (#878, rc=124 inside missing_players) has schedule written but
    NO stages — a schedule-only probe would no-op forever and stages would
    never backfill.

    Fail-open: any Trino error returns an empty set (scrape everything) —
    a false "not complete" costs ~40 s of re-scrape (replace_partitions is
    idempotent), a false "complete" would silently lose data.
    """
    sched_floor = int(os.environ.get('WHOSCORED_SCHEDULE_MIN_ROWS', '270'))
    stages_floor = int(os.environ.get('WHOSCORED_STAGES_MIN_ROWS', '1'))
    conn = _trino_connect()
    if conn is None:
        return set()
    try:
        cur = conn.cursor()
        leagues_ph = ', '.join('?' for _ in leagues)
        seasons_ph = ', '.join('?' for _ in season_strs)

        def _counts(table: str) -> dict:
            sql = (
                "SELECT league, season, COUNT(*) "
                f"FROM iceberg.bronze.{table} "
                f"WHERE league IN ({leagues_ph}) AND season IN ({seasons_ph}) "
                "GROUP BY league, season"
            )
            cur.execute(sql, (*leagues, *season_strs))
            return {
                (r[0], r[1]): r[2]
                for r in cur.fetchall()
                if r and r[0] is not None and r[1] is not None
            }

        sched = _counts('whoscored_schedule')
        stages = _counts('whoscored_season_stages')
        done = {
            pair for pair, cnt in sched.items()
            if cnt >= sched_floor and stages.get(pair, 0) >= stages_floor
        }
        logger.info(
            "skip-existing probe (schedule floor=%s, stages floor=%s): "
            "schedule counts=%s stages counts=%s -> complete=%s",
            sched_floor, stages_floor, sched, stages, sorted(done),
        )
        return done
    except Exception as e:
        logger.warning(
            "skip-existing probe on bronze whoscored tables failed (%s) — "
            "scraping all requested pairs.", e,
        )
        return set()


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
        '--skip-missing-players',
        action='store_true',
        default=False,
        help=(
            'Skip scrape_missing_players (#878). It is per-match — one '
            '/Matches/{id}/Preview page per fixture (~380 pages per '
            'league/season through FlareSolverr), NOT cheap. Combined with '
            '--skip-events this is the fast schedule path '
            '(schedule + season_stages, ~15 requests).'
        ),
    )
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        default=False,
        help=(
            'Skip (league, season) pairs already complete in bronze (#878; '
            'fbref #877 pattern). Only honored together with --skip-events '
            '--skip-missing-players. Complete = whoscored_schedule >= '
            'WHOSCORED_SCHEDULE_MIN_ROWS (default 270) AND '
            'whoscored_season_stages >= WHOSCORED_STAGES_MIN_ROWS (default 1). '
            'Fail-open on Trino errors; the current season is never skipped.'
        ),
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
            'Empty string = proxy-less (prod default since #616 §5c: '
            'FlareSolverr solves CF itself, 0 residential MB). Fallback if '
            'CF failures return: PROXY_FILTER_URL=http://proxy_filter:8899 '
            '(#652) or a non-empty proxy file.'
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

    # #913 Phase 1: WhoScored (5th source). The DAG always passes multi-year
    # SEASONS_STR. For INT-World Cup we must use single-year season ('2026')
    # so that soccerdata and Bronze get the correct partition. Separate task
    # per league helps, but we still override here.
    if 'INT-World Cup' in leagues:
        seasons = [2026]

    logger.info(
        f"Starting WhoScored scraper: leagues={leagues}, seasons={seasons}, "
        f"skip_events={args.skip_events}, "
        f"skip_missing_players={args.skip_missing_players}, "
        f"skip_existing={args.skip_existing}, headless={args.headless}, "
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

    # #878: --skip-existing — don't re-fetch (league, season) pairs already
    # complete in bronze. Runs BEFORE the scraper is created, so a full skip
    # never opens a FlareSolverr session. Only meaningful on the fast
    # schedule path: with events or missing_players still due, a bronze
    # schedule probe says nothing about what remains to scrape.
    if args.skip_existing:
        if (
            args.player_profile
            or args.events_only
            or not (args.skip_events and args.skip_missing_players)
        ):
            logger.warning(
                "--skip-existing is only honored together with --skip-events "
                "--skip-missing-players; ignoring it for this run"
            )
        elif leagues and seasons:
            _now = datetime.now()
            _current_start = _now.year if _now.month >= 8 else _now.year - 1
            past = [s for s in seasons if _season_start_year(s) < _current_start]
            season_str_map = {s: _season_to_bronze_str(s) for s in past}
            done_pairs = (
                _completed_schedule_pairs(
                    leagues, sorted(set(season_str_map.values()))
                )
                if past else set()
            )
            # Per-league reduction (fbref #877 pattern): a league is dropped
            # only when ALL its requested seasons are past AND complete.
            # Non-rectangular partial sets are not optimized — re-scraping a
            # complete pair is idempotent (replace_partitions) and costs ~40 s.
            skipped = [
                lg for lg in leagues
                if len(past) == len(seasons)
                and all((lg, season_str_map[s]) in done_pairs for s in past)
            ]
            if skipped:
                leagues = [lg for lg in leagues if lg not in skipped]
                results['skipped_pairs'] = sorted(
                    [lg, season_str_map[s]] for lg in skipped for s in past
                )
                logger.info(
                    "skip-existing: schedule+season_stages already complete "
                    f"for {skipped} seasons={sorted(season_str_map.values())}; "
                    f"remaining leagues: {leagues}"
                )
            if not leagues:
                # Full no-op: zeroed traffic mirrors
                # FlareSolverrClient.get_traffic_stats() so downstream
                # traffic consumers see a valid shape instead of a stale run.
                results['skip_existing'] = True
                results['traffic'] = {
                    'events': {},
                    'schedule': {
                        'fs_response_bytes': 0,
                        'fs_response_mb': 0.0,
                        'requests': 0,
                        'sessions_created': 0,
                        'cf_challenge_failures': 0,
                        'top_traffic_urls': [],
                    },
                }
                with open(args.output, 'w') as f:
                    json.dump(results, f)
                logger.info(
                    "skip-existing: nothing to scrape (all requested pairs "
                    "complete in bronze); exiting 0"
                )
                print(json.dumps(results))
                return 0

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
                # 1. Schedule (light: ~10-14 fetches per (league, season) —
                # calendar + monthly fixture JSONs, ~40 s)
                try:
                    out = scraper.scrape_schedule() or {}
                    _merge(results, out)
                except Exception as e:
                    logger.error(f"scrape_schedule failed: {e}", exc_info=True)
                    results['errors'].append(f"schedule: {e}")

                # 2. Missing players (HEAVY: one /Matches/{id}/Preview page
                # PER MATCH — ~380 pages per (league, season) through
                # FlareSolverr at ~5 s each; this, not schedule, is what
                # blew the 30-min backfill unit cap in #878)
                if args.skip_missing_players:
                    logger.info(
                        "--skip-missing-players set: not calling "
                        "scrape_missing_players()"
                    )
                else:
                    try:
                        out = scraper.scrape_missing_players() or {}
                        _merge(results, out)
                    except Exception as e:
                        logger.error(
                            f"scrape_missing_players failed: {e}", exc_info=True
                        )
                        results['errors'].append(f"missing_players: {e}")

                # 3. Season stages (cheap: ~1 fetch, calendar from cache)
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
