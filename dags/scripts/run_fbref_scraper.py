#!/usr/bin/env python3
"""
FBref Scraper Runner Script
===========================

Standalone script to run FBref scraper.
Called from Airflow via BashOperator to avoid memory issues with PythonOperator.

Supports three scraper types:
1. nodriver (default, recommended) - Browser-based with Cloudflare Turnstile bypass
2. soccerdata - Lightweight HTTP-based scraper (DEPRECATED: blocked by Cloudflare)
3. selenium - Browser-based with undetected-chromedriver

Usage:
    # Using nodriver (recommended for Cloudflare Turnstile)
    python run_fbref_scraper.py --scraper-type nodriver --proxy-file /path/to/proxys.txt

    # Using nodriver with specific settings
    python run_fbref_scraper.py --scraper-type nodriver --cloudflare-wait 120 --cf-verify-retries 15

NOTE: As of 2025-2026, FBref uses Cloudflare Turnstile CAPTCHA.
      soccerdata (curl_cffi) does NOT work because it cannot execute JavaScript.
      nodriver with cf-verify plugin is the recommended solution.
"""

import argparse
import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def check_flaresolverr_health(url: str, timeout: int = 30) -> bool:
    """Check if FlareSolverr is healthy before starting."""
    import requests
    try:
        response = requests.get(f"{url.rstrip('/')}/health", timeout=timeout)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'ok':
                logger.info(f"FlareSolverr health check passed: {data}")
                return True
        logger.warning(f"FlareSolverr health check failed: {response.text}")
        return False
    except Exception as e:
        logger.error(f"FlareSolverr health check error: {e}")
        return False


def check_tor_health(host: str = 'tor', port: int = 9050, timeout: int = 30) -> bool:
    """Check if Tor proxy is healthy before starting."""
    import requests
    try:
        proxies = {
            'http': f'socks5h://{host}:{port}',
            'https': f'socks5h://{host}:{port}',
        }
        response = requests.get(
            'https://check.torproject.org/api/ip',
            proxies=proxies,
            timeout=timeout
        )
        if response.status_code == 200:
            data = response.json()
            if data.get('IsTor', False):
                logger.info(f"Tor health check passed: IP={data.get('IP')}")
                return True
        logger.warning(f"Tor health check failed: {response.text}")
        return False
    except Exception as e:
        logger.error(f"Tor health check error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Run FBref scraper')

    # === Scraper type selection ===
    parser.add_argument(
        '--scraper-type',
        type=str,
        choices=['nodriver', 'soccerdata', 'selenium'],
        default='nodriver',
        help='Scraper type: nodriver (recommended, Cloudflare Turnstile bypass), '
             'soccerdata (deprecated, blocked by Cloudflare), '
             'selenium (undetected-chromedriver)'
    )
    parser.add_argument(
        '--use-tor',
        action='store_true',
        help='Use Tor proxy for anonymization (soccerdata scraper)'
    )
    parser.add_argument(
        '--tor-host',
        type=str,
        default='tor',
        help='Tor service hostname (default: tor for Docker)'
    )
    parser.add_argument(
        '--tor-port',
        type=int,
        default=9050,
        help='Tor SOCKS5 port (default: 9050)'
    )

    # === Mode selection ===
    parser.add_argument(
        '--mode',
        type=str,
        choices=['full', 'single_stat', 'match_data', 'combined_match_data'],
        default='full',
        help='Scraping mode: full (all data), single_stat (one stat_type), '
             'match_data (one match data type), combined_match_data (all match data in one pass)'
    )
    parser.add_argument(
        '--stat-type',
        type=str,
        default=None,
        help='Stat type for single_stat mode (stats, shooting, passing, passing_types, gca, defense, possession, playingtime, misc, keeper, keeper_adv)'
    )
    parser.add_argument(
        '--data-category',
        type=str,
        choices=['player', 'team', 'keeper'],
        default='player',
        help='Data category for single_stat mode (player, team, keeper)'
    )
    parser.add_argument(
        '--match-data-type',
        type=str,
        choices=['schedule', 'shot_events', 'match_events', 'lineups'],
        default=None,
        help='Match data type for match_data mode'
    )

    # === Common arguments ===
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
        default='/tmp/fbref_result.json',
        help='Output file for results'
    )
    parser.add_argument(
        '--proxy-file',
        type=str,
        default=None,
        help='Path to file with proxies (format: host:port:user:pass)'
    )

    # === Selenium-specific arguments ===
    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Run browser in headless mode (selenium scraper)'
    )
    parser.add_argument(
        '--use-xvfb',
        action='store_true',
        default=True,
        help='Use xvfb for virtual display (selenium scraper)'
    )
    parser.add_argument(
        '--use-flaresolverr',
        action='store_true',
        help='Use FlareSolverr for Cloudflare bypass instead of Selenium'
    )
    parser.add_argument(
        '--flaresolverr-url',
        type=str,
        default='http://flaresolverr:8191',
        help='FlareSolverr service URL'
    )
    parser.add_argument(
        '--use-nodriver',
        action='store_true',
        help='Use nodriver instead of undetected-chromedriver (better Cloudflare bypass)'
    )
    parser.add_argument(
        '--nodriver-cloudflare-wait',
        type=float,
        default=30.0,
        help='Time to wait for Cloudflare challenge when using nodriver (seconds)'
    )
    parser.add_argument(
        '--cloudflare-wait',
        type=float,
        default=90.0,
        help='Time to wait for Cloudflare challenge (nodriver scraper)'
    )
    parser.add_argument(
        '--cf-verify-retries',
        type=int,
        default=12,
        help='Maximum retries for cf-verify plugin (nodriver scraper)'
    )
    parser.add_argument(
        '--max-retries',
        type=int,
        default=5,
        help='Maximum page load retries (nodriver scraper)'
    )
    parser.add_argument(
        '--content-timeout',
        type=float,
        default=45.0,
        help='Timeout for content extraction in seconds (default 45).'
    )

    # === Full mode specific arguments ===
    parser.add_argument(
        '--extended-stats',
        action='store_true',
        default=True,
        help='[full mode] Collect extended player stats (all stat_types merged)'
    )
    parser.add_argument(
        '--no-extended-stats',
        action='store_true',
        help='[full mode] Disable extended player stats collection'
    )
    parser.add_argument(
        '--match-stats',
        action='store_true',
        help='[full mode] Collect per-match player stats (slow, selenium only)'
    )
    parser.add_argument(
        '--keeper-stats',
        action='store_true',
        default=True,
        help='[full mode] Collect goalkeeper statistics'
    )
    parser.add_argument(
        '--no-keeper-stats',
        action='store_true',
        help='[full mode] Disable goalkeeper stats collection'
    )
    parser.add_argument(
        '--shot-events',
        action='store_true',
        default=True,
        help='[full mode] Collect shot events with xG data (selenium only)'
    )
    parser.add_argument(
        '--no-shot-events',
        action='store_true',
        help='[full mode] Disable shot events collection'
    )
    parser.add_argument(
        '--match-events',
        action='store_true',
        default=True,
        help='[full mode] Collect match events (goals, cards, substitutions, selenium only)'
    )
    parser.add_argument(
        '--no-match-events',
        action='store_true',
        help='[full mode] Disable match events collection'
    )
    parser.add_argument(
        '--lineups',
        action='store_true',
        default=True,
        help='[full mode] Collect team lineups (selenium only)'
    )
    parser.add_argument(
        '--no-lineups',
        action='store_true',
        help='[full mode] Disable lineups collection'
    )
    parser.add_argument(
        '--team-match-stats',
        action='store_true',
        help='[full mode] Collect team-level match statistics (slow, selenium only)'
    )
    parser.add_argument(
        '--team-stats-extended',
        action='store_true',
        default=True,
        help='[full mode] Collect extended team stats (all stat_types merged)'
    )
    parser.add_argument(
        '--no-team-stats-extended',
        action='store_true',
        help='[full mode] Disable extended team stats collection'
    )
    parser.add_argument(
        '--max-matches',
        type=int,
        default=50,
        help='Maximum matches to scrape per league/season (0 = no limit, selenium only)'
    )
    args = parser.parse_args()

    leagues = [l.strip() for l in args.leagues.split(',')]

    logger.info(f"Starting FBref scraper: scraper_type={args.scraper_type}, mode={args.mode}")
    logger.info(f"Leagues: {leagues}, Season: {args.season}")
    logger.info(f"Use Tor: {args.use_tor}, Tor host: {args.tor_host}:{args.tor_port}")
    logger.info(f"Proxy file: {args.proxy_file}")

    results = {
        'mode': args.mode,
        'scraper_type': args.scraper_type,
        'tables': [],
        'errors': [],
        'diagnostics': {}
    }

    # max_matches=0 means no limit (None)
    max_matches = args.max_matches if args.max_matches > 0 else None

    # ==========================================================================
    # Nodriver scraper (recommended, Cloudflare Turnstile bypass)
    # ==========================================================================
    if args.scraper_type == 'nodriver':
        logger.info("Using nodriver scraper (Cloudflare Turnstile bypass)")
        bronze_dir = os.environ.get('FBREF_BRONZE_DIR', '/tmp/bronze/fbref')
        os.makedirs(bronze_dir, exist_ok=True)
        logger.info(f"Headless: {args.headless}, use_xvfb: {args.use_xvfb}")
        logger.info(f"Cloudflare wait: {args.cloudflare_wait}s, cf-verify retries: {args.cf_verify_retries}")
        logger.info(f"Content timeout: {args.content_timeout}s")
        logger.info(f"Proxy file: {args.proxy_file}")

        results['diagnostics']['nodriver_enabled'] = True
        results['diagnostics']['cloudflare_wait'] = args.cloudflare_wait
        results['diagnostics']['cf_verify_retries'] = args.cf_verify_retries
        results['diagnostics']['content_timeout'] = args.content_timeout

        try:
            from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

            with NodriverFBrefScraper(
                leagues=leagues,
                seasons=[args.season],
                proxy_file=args.proxy_file,
                headless=args.headless,
                use_xvfb=args.use_xvfb,
                cloudflare_wait=args.cloudflare_wait,
                max_retries=args.max_retries,
                cf_verify_max_retries=args.cf_verify_retries,
                content_timeout=args.content_timeout,
            ) as scraper:

                # =============================================================
                # MODE: single_stat
                # =============================================================
                if args.mode == 'single_stat':
                    if not args.stat_type:
                        raise ValueError("--stat-type is required for single_stat mode")

                    logger.info(
                        f"Single stat mode: category={args.data_category}, "
                        f"stat_type={args.stat_type}"
                    )

                    scrape_result = scraper.scrape_single_stat_type(
                        stat_type=args.stat_type,
                        data_category=args.data_category,
                    )

                    if scrape_result.get('data') is not None:
                        # Save to HDFS/file
                        output_path = f"{bronze_dir}/{args.data_category}_{args.stat_type}"
                        df = scrape_result['data']
                        df.to_parquet(f"{output_path}.parquet", index=False)
                        results['tables'] = [output_path]
                        results['rows'] = scrape_result['rows']
                        logger.info(f"Saved {scrape_result['rows']} rows to {output_path}")
                    else:
                        results['tables'] = []
                        results['rows'] = 0

                    results['stat_type'] = args.stat_type
                    results['data_category'] = args.data_category
                    results['diagnostics']['scraper_stats'] = scrape_result.get('stats', {})

                    if scrape_result['rows'] == 0:
                        error_msg = (
                            f"No data collected for {args.data_category}_{args.stat_type}. "
                            f"Stats: {scrape_result.get('stats', {})}"
                        )
                        logger.error(error_msg)
                        results['errors'].append(error_msg)

                # =============================================================
                # MODE: match_data (schedule)
                # =============================================================
                elif args.mode == 'match_data':
                    if not args.match_data_type:
                        raise ValueError("--match-data-type is required for match_data mode")

                    if args.match_data_type != 'schedule':
                        logger.warning(
                            f"Match data type '{args.match_data_type}' not yet supported by nodriver scraper. "
                            f"Use --scraper-type selenium for detailed match data."
                        )
                        results['tables'] = []
                        results['match_data_type'] = args.match_data_type
                    else:
                        logger.info("Scraping schedule...")
                        scrape_result = scraper.scrape_schedule()

                        if scrape_result.get('data') is not None:
                            output_path = f"{bronze_dir}/schedule"
                            df = scrape_result['data']
                            df.to_parquet(f"{output_path}.parquet", index=False)
                            results['tables'] = [output_path]
                            results['rows'] = scrape_result['rows']
                            logger.info(f"Saved {scrape_result['rows']} schedule rows")
                        else:
                            results['tables'] = []
                            results['rows'] = 0
                            error_msg = "No schedule data collected"
                            logger.error(error_msg)
                            results['errors'].append(error_msg)

                        results['match_data_type'] = args.match_data_type
                        results['diagnostics']['scraper_stats'] = scrape_result.get('stats', {})

                # =============================================================
                # MODE: full (not recommended for nodriver - use single_stat)
                # =============================================================
                else:  # mode == 'full'
                    logger.warning(
                        "Full mode with nodriver is not recommended due to memory usage. "
                        "Using sequential single_stat collection instead."
                    )

                    from scrapers.fbref.constants import PLAYER_STAT_TYPES, TEAM_STAT_TYPES, KEEPER_STAT_TYPES

                    all_tables = []

                    # Collect schedule
                    logger.info("Collecting schedule...")
                    sched_result = scraper.scrape_schedule()
                    if sched_result.get('data') is not None:
                        output_path = f"{bronze_dir}/schedule"
                        sched_result['data'].to_parquet(f"{output_path}.parquet", index=False)
                        all_tables.append(output_path)

                    # Collect player stats (one at a time)
                    for stat_type in PLAYER_STAT_TYPES:
                        logger.info(f"Collecting player_{stat_type}...")
                        try:
                            stat_result = scraper.scrape_single_stat_type(stat_type, 'player')
                            if stat_result.get('data') is not None:
                                output_path = f"{bronze_dir}/player_{stat_type}"
                                stat_result['data'].to_parquet(f"{output_path}.parquet", index=False)
                                all_tables.append(output_path)
                        except Exception as e:
                            logger.error(f"Error collecting player_{stat_type}: {e}")
                            continue

                    # Collect team stats
                    for stat_type in TEAM_STAT_TYPES:
                        logger.info(f"Collecting team_{stat_type}...")
                        try:
                            stat_result = scraper.scrape_single_stat_type(stat_type, 'team')
                            if stat_result.get('data') is not None:
                                output_path = f"{bronze_dir}/team_{stat_type}"
                                stat_result['data'].to_parquet(f"{output_path}.parquet", index=False)
                                all_tables.append(output_path)
                        except Exception as e:
                            logger.error(f"Error collecting team_{stat_type}: {e}")
                            continue

                    # Collect keeper stats
                    for stat_type in KEEPER_STAT_TYPES:
                        logger.info(f"Collecting keeper_{stat_type}...")
                        try:
                            stat_result = scraper.scrape_single_stat_type(stat_type, 'keeper')
                            if stat_result.get('data') is not None:
                                output_path = f"{bronze_dir}/keeper_{stat_type}"
                                stat_result['data'].to_parquet(f"{output_path}.parquet", index=False)
                                all_tables.append(output_path)
                        except Exception as e:
                            logger.error(f"Error collecting keeper_{stat_type}: {e}")
                            continue

                    results['tables'] = all_tables
                    results['diagnostics']['scraper_stats'] = scraper.get_stats()

                    logger.info(f"Full scrape completed: {len(all_tables)} tables saved")

        except ImportError as e:
            logger.error(f"Failed to import NodriverFBrefScraper: {e}")
            logger.info("Falling back to selenium scraper...")
            args.scraper_type = 'selenium'
            # Fall through to selenium scraper below

        except Exception as e:
            logger.error(f"Nodriver scraper failed: {e}", exc_info=True)
            results['errors'].append(str(e))
            with open(args.output, 'w') as f:
                json.dump(results, f)
            sys.exit(1)

    # ==========================================================================
    # Soccerdata scraper (DEPRECATED - blocked by Cloudflare)
    # ==========================================================================
    elif args.scraper_type == 'soccerdata':
        logger.warning(
            "WARNING: soccerdata scraper is DEPRECATED and will fail on FBref. "
            "Cloudflare Turnstile requires JavaScript execution. "
            "Use --scraper-type nodriver instead."
        )
        logger.info("Using soccerdata scraper (lightweight, DEPRECATED)")

        # Pre-flight check for Tor if enabled
        if args.use_tor:
            logger.info(f"Checking Tor health at {args.tor_host}:{args.tor_port}...")
            if not check_tor_health(args.tor_host, args.tor_port):
                error_msg = f"Tor is not healthy at {args.tor_host}:{args.tor_port}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['diagnostics']['tor_healthy'] = False
                with open(args.output, 'w') as f:
                    json.dump(results, f)
                sys.exit(1)
            results['diagnostics']['tor_healthy'] = True

        try:
            from scrapers.soccerdata_fbref_scraper import SoccerdataFBrefScraper

            with SoccerdataFBrefScraper(
                leagues=leagues,
                seasons=[args.season],
                use_tor=args.use_tor,
                tor_host=args.tor_host,
                tor_port=args.tor_port,
                proxy_file=args.proxy_file,
            ) as scraper:

                # =============================================================
                # MODE: single_stat
                # =============================================================
                if args.mode == 'single_stat':
                    if not args.stat_type:
                        raise ValueError("--stat-type is required for single_stat mode")

                    logger.info(
                        f"Single stat mode: category={args.data_category}, "
                        f"stat_type={args.stat_type}"
                    )

                    scrape_results = scraper.scrape_single_stat_type(
                        stat_type=args.stat_type,
                        data_category=args.data_category,
                    )

                    results['tables'] = list(scrape_results.values())
                    results['stat_type'] = args.stat_type
                    results['data_category'] = args.data_category

                    results['diagnostics']['scraper_stats'] = {
                        'successes': scraper._stats.get('successes', 0),
                        'failures': scraper._stats.get('failures', 0),
                    }

                    logger.info(f"Single stat scrape completed: {list(scrape_results.keys())}")

                    if not scrape_results:
                        error_msg = (
                            f"No data collected for {args.data_category}_{args.stat_type}. "
                            f"Scraper stats: successes={scraper._stats.get('successes', 0)}, "
                            f"failures={scraper._stats.get('failures', 0)}"
                        )
                        logger.error(error_msg)
                        results['errors'].append(error_msg)

                # =============================================================
                # MODE: match_data
                # =============================================================
                elif args.mode == 'match_data':
                    if not args.match_data_type:
                        raise ValueError("--match-data-type is required for match_data mode")

                    logger.info(f"Match data mode: type={args.match_data_type}")

                    scrape_results = scraper.scrape_match_data(
                        data_type=args.match_data_type,
                        max_matches=max_matches,
                    )

                    results['tables'] = list(scrape_results.values())
                    results['match_data_type'] = args.match_data_type

                    results['diagnostics']['scraper_stats'] = {
                        'successes': scraper._stats.get('successes', 0),
                        'failures': scraper._stats.get('failures', 0),
                    }

                    logger.info(f"Match data scrape completed: {list(scrape_results.keys())}")

                    if not scrape_results:
                        # Check if we should treat this as an error
                        failures = scraper._stats.get('failures', 0)
                        if failures > 0 or args.match_data_type == 'schedule':
                            # schedule should always return data; failures indicate real errors
                            error_msg = (
                                f"No data collected for match_data/{args.match_data_type}. "
                                f"Scraper stats: successes={scraper._stats.get('successes', 0)}, "
                                f"failures={failures}"
                            )
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
                        else:
                            # For soccerdata, some match data types are not supported
                            logger.warning(
                                f"Match data type '{args.match_data_type}' not supported by soccerdata. "
                                f"Use --scraper-type selenium for detailed match data."
                            )

                # =============================================================
                # MODE: full
                # =============================================================
                else:  # mode == 'full'
                    include_extended = args.extended_stats and not args.no_extended_stats
                    include_keeper = args.keeper_stats and not args.no_keeper_stats
                    include_team_stats_extended = args.team_stats_extended and not args.no_team_stats_extended

                    logger.info(
                        f"Full mode (soccerdata): extended={include_extended}, "
                        f"keeper={include_keeper}, team_extended={include_team_stats_extended}"
                    )

                    scrape_results = scraper.scrape_all(
                        include_extended_stats=include_extended,
                        include_keeper_stats=include_keeper,
                        include_team_stats_extended=include_team_stats_extended,
                    )

                    results['tables'] = list(scrape_results.values())
                    logger.info(f"Full scrape completed. Tables saved: {list(scrape_results.keys())}")

        except Exception as e:
            logger.error(f"Soccerdata scraper failed: {e}", exc_info=True)
            results['errors'].append(str(e))
            with open(args.output, 'w') as f:
                json.dump(results, f)
            sys.exit(1)

    # ==========================================================================
    # Selenium scraper (browser-based, Cloudflare bypass)
    # ==========================================================================
    else:  # scraper_type == 'selenium'
        logger.info("Using Selenium scraper (browser-based)")
        logger.info(f"Headless: {args.headless}, use_xvfb: {args.use_xvfb}")
        logger.info(f"Use FlareSolverr: {args.use_flaresolverr}, URL: {args.flaresolverr_url}")
        logger.info(f"Use nodriver: {args.use_nodriver}, cloudflare_wait: {args.nodriver_cloudflare_wait}s")

        # Pre-flight check for FlareSolverr if enabled
        if args.use_flaresolverr:
            logger.info(f"Checking FlareSolverr health at {args.flaresolverr_url}...")
            if not check_flaresolverr_health(args.flaresolverr_url):
                error_msg = f"FlareSolverr is not healthy at {args.flaresolverr_url}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['diagnostics']['flaresolverr_healthy'] = False
                with open(args.output, 'w') as f:
                    json.dump(results, f)
                sys.exit(1)
            results['diagnostics']['flaresolverr_healthy'] = True

        # Add nodriver diagnostics
        if args.use_nodriver:
            results['diagnostics']['nodriver_enabled'] = True
            results['diagnostics']['nodriver_cloudflare_wait'] = args.nodriver_cloudflare_wait
            logger.info("Nodriver mode enabled - using advanced Cloudflare bypass")

        try:
            from scrapers.fbref import FBrefScraper

            with FBrefScraper(
                leagues=leagues,
                seasons=[args.season],
                headless=args.headless,
                use_xvfb=args.use_xvfb,
                proxy_file=args.proxy_file,
                use_flaresolverr=args.use_flaresolverr,
                flaresolverr_url=args.flaresolverr_url,
                use_nodriver=args.use_nodriver,
                nodriver_cloudflare_wait=args.nodriver_cloudflare_wait,
            ) as scraper:

                # =============================================================
                # MODE: single_stat
                # =============================================================
                if args.mode == 'single_stat':
                    if not args.stat_type:
                        raise ValueError("--stat-type is required for single_stat mode")

                    logger.info(
                        f"Single stat mode: category={args.data_category}, "
                        f"stat_type={args.stat_type}"
                    )

                    scrape_results = scraper.scrape_single_stat_type(
                        stat_type=args.stat_type,
                        data_category=args.data_category,
                    )

                    results['tables'] = list(scrape_results.values())
                    results['stat_type'] = args.stat_type
                    results['data_category'] = args.data_category

                    results['diagnostics']['scraper_stats'] = {
                        'successes': scraper._stats.get('successes', 0),
                        'failures': scraper._stats.get('failures', 0),
                    }

                    logger.info(f"Single stat scrape completed: {list(scrape_results.keys())}")

                    if not scrape_results:
                        error_msg = (
                            f"No data collected for {args.data_category}_{args.stat_type}. "
                            f"Scraper stats: successes={scraper._stats.get('successes', 0)}, "
                            f"failures={scraper._stats.get('failures', 0)}"
                        )
                        logger.error(error_msg)
                        results['errors'].append(error_msg)

                # =============================================================
                # MODE: match_data
                # =============================================================
                elif args.mode == 'match_data':
                    if not args.match_data_type:
                        raise ValueError("--match-data-type is required for match_data mode")

                    logger.info(
                        f"Match data mode: type={args.match_data_type}, "
                        f"max_matches={max_matches}"
                    )

                    scrape_results = scraper.scrape_match_data(
                        data_type=args.match_data_type,
                        max_matches=max_matches,
                    )

                    results['tables'] = list(scrape_results.values())
                    results['match_data_type'] = args.match_data_type

                    results['diagnostics']['scraper_stats'] = {
                        'successes': scraper._stats.get('successes', 0),
                        'failures': scraper._stats.get('failures', 0),
                    }

                    logger.info(f"Match data scrape completed: {list(scrape_results.keys())}")

                    if not scrape_results:
                        error_msg = (
                            f"No data collected for match_data type '{args.match_data_type}'. "
                            f"Scraper stats: successes={scraper._stats.get('successes', 0)}, "
                            f"failures={scraper._stats.get('failures', 0)}"
                        )
                        logger.error(error_msg)
                        results['errors'].append(error_msg)

                # =============================================================
                # MODE: combined_match_data (3x efficiency optimization)
                # =============================================================
                elif args.mode == 'combined_match_data':
                    logger.info(
                        f"Combined match data mode: max_matches={max_matches}. "
                        f"Collecting shot_events, match_events, lineups in one pass (3x efficiency)"
                    )

                    scrape_results = scraper.scrape_combined_match_data(
                        max_matches=max_matches,
                    )

                    results['tables'] = list(scrape_results.values())
                    results['mode'] = 'combined_match_data'

                    results['diagnostics']['scraper_stats'] = {
                        'successes': scraper._stats.get('successes', 0),
                        'failures': scraper._stats.get('failures', 0),
                    }
                    results['diagnostics']['optimization'] = '3x reduction in HTTP requests'

                    logger.info(
                        f"Combined match data scrape completed: {list(scrape_results.keys())}"
                    )

                    if not scrape_results:
                        error_msg = (
                            f"No data collected for combined_match_data. "
                            f"Scraper stats: successes={scraper._stats.get('successes', 0)}, "
                            f"failures={scraper._stats.get('failures', 0)}"
                        )
                        logger.error(error_msg)
                        results['errors'].append(error_msg)

                # =============================================================
                # MODE: full
                # =============================================================
                else:  # mode == 'full'
                    include_extended = args.extended_stats and not args.no_extended_stats
                    include_keeper = args.keeper_stats and not args.no_keeper_stats
                    include_shot_events = args.shot_events and not args.no_shot_events
                    include_match_events = args.match_events and not args.no_match_events
                    include_lineups = args.lineups and not args.no_lineups
                    include_team_stats_extended = args.team_stats_extended and not args.no_team_stats_extended

                    logger.info(
                        f"Full mode (selenium): extended={include_extended}, keeper={include_keeper}, "
                        f"shot_events={include_shot_events}, match_events={include_match_events}, "
                        f"lineups={include_lineups}, team_extended={include_team_stats_extended}"
                    )
                    logger.info(f"Max matches per league: {max_matches if max_matches else 'unlimited'}")

                    scrape_results = scraper.scrape_all(
                        include_extended_stats=include_extended,
                        include_match_stats=args.match_stats,
                        include_keeper_stats=include_keeper,
                        include_shot_events=include_shot_events,
                        include_match_events=include_match_events,
                        include_lineups=include_lineups,
                        include_team_match_stats=args.team_match_stats,
                        include_team_stats_extended=include_team_stats_extended,
                        max_matches_per_league=max_matches,
                    )

                    results['tables'] = list(scrape_results.values())

                    # Add row count placeholders for backwards compatibility
                    results['schedule_rows'] = 1 if 'schedule' in scrape_results else 0
                    results['team_stats_rows'] = 1 if 'team_stats' in scrape_results else 0
                    results['team_stats_extended_rows'] = 1 if 'team_stats_extended' in scrape_results else 0
                    results['player_stats_rows'] = 1 if 'player_stats' in scrape_results else 0
                    results['player_stats_extended_rows'] = 1 if 'player_stats_extended' in scrape_results else 0
                    results['keeper_stats_rows'] = 1 if 'keeper_stats' in scrape_results else 0
                    results['match_stats_rows'] = 1 if 'player_match_stats' in scrape_results else 0
                    results['shot_events_rows'] = 1 if 'shot_events' in scrape_results else 0
                    results['match_events_rows'] = 1 if 'match_events' in scrape_results else 0
                    results['lineups_rows'] = 1 if 'lineups' in scrape_results else 0
                    results['team_match_stats_rows'] = 1 if 'team_match_stats' in scrape_results else 0

                    logger.info(f"Full scrape completed. Tables saved: {list(scrape_results.keys())}")

        except Exception as e:
            logger.error(f"Selenium scraper failed: {e}", exc_info=True)
            results['errors'].append(str(e))
            with open(args.output, 'w') as f:
                json.dump(results, f)
            sys.exit(1)

    # Write results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)

    total_tables = len(results['tables'])
    total_errors = len(results['errors'])

    logger.info(f"Scraper complete: {total_tables} tables saved, {total_errors} errors")
    print(json.dumps(results, indent=2))

    # Exit with error if no data was collected and there were errors
    if total_tables == 0:
        if total_errors > 0:
            logger.error(
                f"Scraper finished with no data and {total_errors} errors. "
                f"Errors: {results['errors']}"
            )
            return 1
        else:
            # No data but also no errors - might be expected for some stat types
            logger.warning(
                f"Scraper finished with no data but no errors. "
                f"This may be expected for some stat types or leagues."
            )
            return 0

    return 0


if __name__ == '__main__':
    sys.exit(main())
