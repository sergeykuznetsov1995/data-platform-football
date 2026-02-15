"""
FBref Data Ingestion DAG (Refactored + Optimized)
==================================================

Memory-efficient DAG for scraping football statistics from FBref.

Supports three scraper backends:
1. nodriver (default, recommended) - Browser-based with Cloudflare Turnstile bypass
2. soccerdata (deprecated) - HTTP-based, blocked by Cloudflare Turnstile
3. selenium - Browser-based with undetected-chromedriver

Architecture:
- TaskGroup player_stats: 9 SEQUENTIAL tasks (one per stat_type)
- TaskGroup team_stats: 9 SEQUENTIAL tasks (one per stat_type)
- TaskGroup keeper_stats: 2 SEQUENTIAL tasks (keeper, keeper_adv)
- TaskGroup match_data: schedule → match_all_data (OPTIMIZED)
- validate_all_data: final validation

OPTIMIZATION (Feb 2026):
- Before: schedule → shot_events → match_events → lineups (4 tasks, 3N page loads)
- After:  schedule → match_all_data (2 tasks, N page loads)
- HTTP requests reduction: 3x (e.g., 1141 → 381 for 380 matches)
- Time reduction: ~2-4 hours → ~15-25 minutes

Each stat_type is saved to a separate Iceberg table:
- fbref_player_{stat_type} (9 tables)
- fbref_team_{stat_type} (9 tables)
- fbref_keeper_{stat_type} (2 tables)
- fbref_schedule, fbref_shot_events, fbref_match_events, fbref_lineups

Scraper types:
- nodriver + cf-verify: Recommended for Cloudflare Turnstile bypass (2025-2026)
- soccerdata: DEPRECATED - blocked by Cloudflare (curl_cffi cannot execute JS)
- selenium: Alternative with undetected-chromedriver

NOTE: As of 2025-2026, FBref uses Cloudflare Turnstile CAPTCHA.
      Only nodriver with cf-verify plugin can bypass this automatically.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow.operators.empty import EmptyOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SELENIUM_ARGS


# Stat types for each data category
PLAYER_STAT_TYPES = [
    'stats', 'shooting', 'passing', 'passing_types',
    'gca', 'defense', 'possession', 'playingtime', 'misc'
]

TEAM_STAT_TYPES = [
    'stats', 'shooting', 'passing', 'passing_types',
    'gca', 'defense', 'possession', 'playingtime', 'misc'
]

KEEPER_STAT_TYPES = ['keeper', 'keeper_adv']

MATCH_DATA_TYPES = ['schedule', 'shot_events', 'match_events', 'lineups']

# =============================================================================
# SCRAPER CONFIGURATION
# =============================================================================
#
# FBref uses Cloudflare Turnstile CAPTCHA (2025-2026).
#
# Solution approach:
# 1. nodriver (recommended) - Full browser with cf-verify plugin
# 2. Residential proxies from proxys.txt with rotation
# 3. Xvfb for headless detection bypass
# 4. Sequential execution to prevent OOM
#
# IMPORTANT: 2captcha and paid services are NOT used.
#            soccerdata/curl_cffi does NOT work (cannot execute JavaScript).
# =============================================================================

# Scraper type: 'nodriver' (recommended), 'soccerdata' (deprecated), 'selenium'
# NOTE: As of Feb 2026, FBref uses Cloudflare Turnstile CAPTCHA.
# Only nodriver with cf-verify plugin can bypass this automatically.
# soccerdata (curl_cffi) DOES NOT work - it cannot execute JavaScript.
DEFAULT_SCRAPER_TYPE = 'nodriver'  # cf-verify plugin for Turnstile bypass

# Tor proxy settings (blocked by FBref Cloudflare)
USE_TOR = False  # Tor is blocked by FBref
TOR_HOST = 'tor'
TOR_PORT = 9050

# FlareSolverr settings (deprecated - cannot solve modern Cloudflare)
USE_FLARESOLVERR = False  # Disabled: FlareSolverr is outdated
FLARESOLVERR_URL = 'http://flaresolverr:8191'

# Selenium browser settings
USE_XVFB = True  # Xvfb virtual display to bypass headless detection
HEADLESS = True  # Headless mode with Xvfb

# Nodriver settings (PRIMARY for Cloudflare Turnstile bypass)
USE_NODRIVER = True  # Use nodriver with cf-verify plugin
NODRIVER_CLOUDFLARE_WAIT = 30.0  # Wait time for Cloudflare challenge (successful bypass takes ~10s)
NODRIVER_CONTENT_TIMEOUT = 45.0  # Timeout for content extraction (increased for CDP DOM fallback)
NODRIVER_MAX_RETRIES = 2  # Per-proxy retries (fail fast, rotate proxy instead)
NODRIVER_CF_VERIFY_RETRIES = 6  # Maximum cf-verify plugin retries

# =============================================================================
# NODRIVER-CF-VERIFY PLUGIN SETTINGS
# =============================================================================
# Active Turnstile bypass via nodriver-cf-verify plugin
# Automatically clicks Turnstile checkbox instead of passive waiting
# https://github.com/KlozetLabs/nodriver-cf-bypass
USE_CF_VERIFY_PLUGIN = True  # Enable automatic Turnstile checkbox clicking
CF_VERIFY_MAX_RETRIES = 3    # Max retries (less aggressive to avoid triggering new challenges)
CF_VERIFY_INTERVAL = 1.5     # Interval between cf-verify retries (seconds, reduced from 3.0)

# Cookie injection settings (fallback for HTTP scraper after cf-verify)
# Extracts cf_clearance cookies via nodriver and injects into HTTP session
USE_CF_COOKIE_INJECTION = True
CF_COOKIE_CACHE_TTL_MINUTES = 25  # Slightly less than 30min CF cookie lifetime

# =============================================================================
# CF COOKIE PREWARM SETTINGS
# =============================================================================
# Pre-solve Cloudflare Turnstile before starting scraper tasks
# Reduces 403 errors by having valid cookies ready
CF_COOKIE_PREWARM = False  # Disabled for soccerdata mode to prevent OOM
CF_COOKIE_PREWARM_ATTEMPTS = 5

# Proxy configuration
PROXY_FILE = '/opt/airflow/proxys.txt'  # Path to proxy file in container


def prewarm_cf_cookies(**context) -> Dict[str, Any]:
    """
    Pre-solve Cloudflare Turnstile before starting scraper tasks.

    Uses nodriver + cf-verify plugin to obtain cf_clearance cookies,
    then stores them in XCom for use by scraper tasks.

    Returns:
        Dictionary with success status and cookie info
    """
    import logging

    logger = logging.getLogger(__name__)
    logger.info("Pre-warming CF cookies for FBref...")

    result = {
        'success': False,
        'cookie_count': 0,
        'cookies': {},
        'error': None,
    }

    try:
        from scrapers.base.browser.cf_cookie_manager import CFCookieManager
        from scrapers.utils.proxy_manager import ProxyManager, ProxyType
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        result['error'] = str(e)
        return result

    try:
        # Load proxies
        proxy_manager = ProxyManager()
        proxy_manager.load_from_file_custom_format(PROXY_FILE, ProxyType.HTTP)
        logger.info(f"Loaded {proxy_manager.total_count} proxies for CF prewarm")

        # Create CF cookie manager with optimized settings
        manager = CFCookieManager(
            cache_ttl_minutes=CF_COOKIE_CACHE_TTL_MINUTES,
            use_cf_verify=USE_CF_VERIFY_PLUGIN,
            cf_verify_max_retries=CF_VERIFY_MAX_RETRIES,
            cf_verify_interval=CF_VERIFY_INTERVAL,
            use_xvfb=USE_XVFB,
        )

        # Get cookies with retry across different proxies
        cookies = manager.get_cookies_with_retry_sync(
            url="https://fbref.com/en/",
            proxy_manager=proxy_manager,
            max_attempts=CF_COOKIE_PREWARM_ATTEMPTS,
        )

        if cookies and 'cf_clearance' in cookies:
            # Success - push cookies to XCom for scraper tasks
            context['ti'].xcom_push(key='cf_cookies', value=cookies)
            result['success'] = True
            result['cookie_count'] = len(cookies)
            result['cookies'] = list(cookies.keys())
            logger.info(
                f"CF prewarm successful: {len(cookies)} cookies obtained: "
                f"{list(cookies.keys())}"
            )
        else:
            logger.warning(
                f"CF prewarm failed - no cf_clearance cookie obtained. "
                f"Got cookies: {list(cookies.keys()) if cookies else 'none'}"
            )
            result['error'] = 'No cf_clearance cookie obtained'

    except Exception as e:
        logger.error(f"CF prewarm failed with exception: {e}")
        result['error'] = str(e)

    return result


def validate_all_data(**context) -> Dict[str, Any]:
    """
    Validate all scraped data from all TaskGroups.

    Returns:
        Validation results
    """
    import json
    import logging
    import os
    from pathlib import Path

    logger = logging.getLogger(__name__)

    validation = {
        'status': 'success',
        'warnings': [],
        'tables_collected': [],
        'errors': [],
    }

    # Check all result files
    result_files_pattern = '/tmp/fbref_*.json'
    result_dir = Path('/tmp')

    for result_file in result_dir.glob('fbref_*.json'):
        try:
            with open(result_file, 'r') as f:
                result = json.load(f)

            tables = result.get('tables', [])
            errors = result.get('errors', [])

            if tables:
                validation['tables_collected'].extend(tables)

            if errors:
                validation['errors'].extend(errors)
                validation['warnings'].append(
                    f"{result_file.name}: {len(errors)} error(s)"
                )

        except (FileNotFoundError, json.JSONDecodeError) as e:
            validation['warnings'].append(f"Error reading {result_file}: {e}")

    # Check minimum data thresholds
    total_tables = len(validation['tables_collected'])

    # We expect at least:
    # - 9 player tables + 9 team tables + 2 keeper tables + 4 match data tables = 24 tables
    # But some may fail, so we accept >= 10 as partial success
    if total_tables == 0:
        validation['status'] = 'failed'
        validation['warnings'].append('No tables were collected')
    elif total_tables < 10:
        validation['status'] = 'partial_success'
        validation['warnings'].append(
            f"Only {total_tables} tables collected (expected ~24)"
        )
    else:
        logger.info(f"Collected {total_tables} tables successfully")

    if validation['errors']:
        if validation['status'] == 'success':
            validation['status'] = 'partial_success'

    logger.info(f"Validation complete: {validation['status']}")
    logger.info(f"Tables collected: {total_tables}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation}")

    return validation


def create_single_stat_task(
    stat_type: str,
    data_category: str,
    leagues_str: str,
    season: int,
    scraper_type: str = DEFAULT_SCRAPER_TYPE,
    use_tor: bool = USE_TOR,
    use_flaresolverr: bool = USE_FLARESOLVERR,
    flaresolverr_url: str = FLARESOLVERR_URL,
    use_xvfb: bool = USE_XVFB,
    headless: bool = HEADLESS,
    use_nodriver: bool = USE_NODRIVER,
    nodriver_cloudflare_wait: float = NODRIVER_CLOUDFLARE_WAIT,
    nodriver_content_timeout: float = NODRIVER_CONTENT_TIMEOUT,
    nodriver_max_retries: int = NODRIVER_MAX_RETRIES,
    nodriver_cf_verify_retries: int = NODRIVER_CF_VERIFY_RETRIES,
    proxy_file: str = None,
) -> BashOperator:
    """
    Create a BashOperator task for collecting a single stat_type.

    Args:
        stat_type: The stat type to collect
        data_category: player, team, or keeper
        leagues_str: Comma-separated leagues string
        season: Season year
        scraper_type: 'nodriver' (recommended), 'soccerdata' (deprecated), 'selenium'
        use_tor: Use Tor proxy for soccerdata scraper (deprecated)
        use_flaresolverr: Use FlareSolverr for Cloudflare bypass (deprecated)
        flaresolverr_url: FlareSolverr service URL (deprecated)
        use_xvfb: Use Xvfb virtual display
        headless: Run browser in headless mode
        use_nodriver: Use nodriver (for selenium scraper type)
        nodriver_cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
        nodriver_max_retries: Maximum page load retries
        nodriver_cf_verify_retries: Maximum cf-verify plugin retries
        proxy_file: Path to file with proxy list (format: host:port:user:pass)

    Returns:
        BashOperator task
    """
    task_id = f'{data_category}_{stat_type}'
    output_file = f'/tmp/fbref_{task_id}.json'

    # Build command based on scraper type
    if scraper_type == 'nodriver':
        # nodriver: Browser-based with cf-verify plugin for Turnstile bypass
        nodriver_args = []
        if headless:
            nodriver_args.append('--headless')
        if use_xvfb:
            nodriver_args.append('--use-xvfb')
        if proxy_file:
            nodriver_args.append(f'--proxy-file {proxy_file}')
        nodriver_args.append(f'--cloudflare-wait {nodriver_cloudflare_wait}')
        nodriver_args.append(f'--content-timeout {nodriver_content_timeout}')
        nodriver_args.append(f'--max-retries {nodriver_max_retries}')
        nodriver_args.append(f'--cf-verify-retries {nodriver_cf_verify_retries}')

        bash_command = f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type nodriver \\
    {' '.join(nodriver_args)} \\
    --mode single_stat \\
    --stat-type {stat_type} \\
    --data-category {data_category} \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --output {output_file}
"""

    elif scraper_type == 'soccerdata':
        # soccerdata: DEPRECATED - blocked by Cloudflare Turnstile
        tor_args = f'--use-tor --tor-host {TOR_HOST} --tor-port {TOR_PORT}' if use_tor else ''
        proxy_args = f'--proxy-file {proxy_file}' if proxy_file else ''
        bash_command = f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type soccerdata \\
    {tor_args} \\
    {proxy_args} \\
    --mode single_stat \\
    --stat-type {stat_type} \\
    --data-category {data_category} \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --output {output_file}
"""
    else:
        # selenium: Browser-based scraper with undetected-chromedriver
        if use_flaresolverr:
            selenium_args = f'--use-flaresolverr --flaresolverr-url {flaresolverr_url}'
        else:
            selenium_args_list = []
            if headless:
                selenium_args_list.append('--headless')
            if use_xvfb:
                selenium_args_list.append('--use-xvfb')
            if use_nodriver:
                selenium_args_list.append('--use-nodriver')
                selenium_args_list.append(f'--nodriver-cloudflare-wait {nodriver_cloudflare_wait}')
            if proxy_file:
                selenium_args_list.append(f'--proxy-file {proxy_file}')
            selenium_args = ' '.join(selenium_args_list)

        bash_command = f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type selenium \\
    {selenium_args} \\
    --mode single_stat \\
    --stat-type {stat_type} \\
    --data-category {data_category} \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --output {output_file}
"""

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
            'DISPLAY': ':99',
        },
    )


def create_match_data_task(
    data_type: str,
    leagues_str: str,
    season: int,
    max_matches: int = 0,
    scraper_type: str = DEFAULT_SCRAPER_TYPE,
    use_tor: bool = USE_TOR,
    use_flaresolverr: bool = USE_FLARESOLVERR,
    flaresolverr_url: str = FLARESOLVERR_URL,
    use_xvfb: bool = USE_XVFB,
    headless: bool = HEADLESS,
    use_nodriver: bool = USE_NODRIVER,
    nodriver_cloudflare_wait: float = NODRIVER_CLOUDFLARE_WAIT,
    nodriver_content_timeout: float = NODRIVER_CONTENT_TIMEOUT,
    nodriver_max_retries: int = NODRIVER_MAX_RETRIES,
    nodriver_cf_verify_retries: int = NODRIVER_CF_VERIFY_RETRIES,
    proxy_file: str = None,
) -> BashOperator:
    """
    Create a BashOperator task for collecting match-level data.

    Args:
        data_type: schedule, shot_events, match_events, or lineups
        leagues_str: Comma-separated leagues string
        season: Season year
        max_matches: Maximum matches per league (0 = unlimited)
        scraper_type: 'nodriver' (recommended), 'soccerdata' (deprecated), 'selenium'
        use_tor: Use Tor proxy for soccerdata scraper (deprecated)
        use_flaresolverr: Use FlareSolverr for Cloudflare bypass (deprecated)
        flaresolverr_url: FlareSolverr service URL (deprecated)
        use_xvfb: Use Xvfb virtual display
        headless: Run browser in headless mode
        use_nodriver: Use nodriver (for selenium scraper type)
        nodriver_cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
        nodriver_max_retries: Maximum page load retries
        nodriver_cf_verify_retries: Maximum cf-verify plugin retries
        proxy_file: Path to file with proxy list (format: host:port:user:pass)

    Returns:
        BashOperator task

    Note:
        For match data types other than 'schedule', nodriver scraper only
        supports 'schedule'. Use selenium for detailed match-level data
        (shot_events, match_events, lineups).
    """
    task_id = f'match_{data_type}'
    output_file = f'/tmp/fbref_{task_id}.json'

    # For detailed match data, fall back to selenium (nodriver only supports schedule)
    effective_scraper = scraper_type
    if data_type in ['shot_events', 'match_events', 'lineups']:
        if scraper_type in ['nodriver', 'soccerdata']:
            effective_scraper = 'selenium'

    # Build command based on scraper type
    if effective_scraper == 'nodriver':
        # nodriver: Browser-based with cf-verify plugin for Turnstile bypass
        nodriver_args = []
        if headless:
            nodriver_args.append('--headless')
        if use_xvfb:
            nodriver_args.append('--use-xvfb')
        if proxy_file:
            nodriver_args.append(f'--proxy-file {proxy_file}')
        nodriver_args.append(f'--cloudflare-wait {nodriver_cloudflare_wait}')
        nodriver_args.append(f'--content-timeout {nodriver_content_timeout}')
        nodriver_args.append(f'--max-retries {nodriver_max_retries}')
        nodriver_args.append(f'--cf-verify-retries {nodriver_cf_verify_retries}')

        bash_command = f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type nodriver \\
    {' '.join(nodriver_args)} \\
    --mode match_data \\
    --match-data-type {data_type} \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --output {output_file}
"""

    elif effective_scraper == 'soccerdata':
        # soccerdata: DEPRECATED - blocked by Cloudflare Turnstile
        tor_args = f'--use-tor --tor-host {TOR_HOST} --tor-port {TOR_PORT}' if use_tor else ''
        proxy_args = f'--proxy-file {proxy_file}' if proxy_file else ''
        bash_command = f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type soccerdata \\
    {tor_args} \\
    {proxy_args} \\
    --mode match_data \\
    --match-data-type {data_type} \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --output {output_file}
"""
    else:
        # selenium: Browser-based scraper with undetected-chromedriver
        if use_flaresolverr:
            selenium_args = f'--use-flaresolverr --flaresolverr-url {flaresolverr_url}'
        else:
            selenium_args_list = []
            if headless:
                selenium_args_list.append('--headless')
            if use_xvfb:
                selenium_args_list.append('--use-xvfb')
            if use_nodriver:
                selenium_args_list.append('--use-nodriver')
                selenium_args_list.append(f'--nodriver-cloudflare-wait {nodriver_cloudflare_wait}')
            if proxy_file:
                selenium_args_list.append(f'--proxy-file {proxy_file}')
            selenium_args = ' '.join(selenium_args_list)

        bash_command = f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type selenium \\
    {selenium_args} \\
    --mode match_data \\
    --match-data-type {data_type} \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --max-matches {max_matches} \\
    --output {output_file}
"""

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
            'DISPLAY': ':99',
        },
    )


def create_combined_match_data_task(
    leagues_str: str,
    season: int,
    max_matches: int = 50,
    use_xvfb: bool = USE_XVFB,
    headless: bool = HEADLESS,
    use_nodriver: bool = USE_NODRIVER,
    nodriver_cloudflare_wait: float = NODRIVER_CLOUDFLARE_WAIT,
    proxy_file: str = None,
) -> BashOperator:
    """
    Create a BashOperator task for collecting ALL match-level data in one pass.

    This task collects shot_events, match_events, and lineups simultaneously,
    reducing HTTP requests by 3x compared to separate tasks.

    Optimization:
    - Before: schedule → shot_events → match_events → lineups (3 separate passes)
    - After: schedule → match_all_data (1 combined pass)

    HTTP requests reduction:
    - Before: 1 + 380 + 380 + 380 = 1141 requests (for 380 matches)
    - After: 1 + 380 = 381 requests (3x reduction)

    Args:
        leagues_str: Comma-separated leagues string
        season: Season year
        max_matches: Maximum matches per league (default 50 for reasonable runtime)
        use_xvfb: Use Xvfb virtual display
        headless: Run browser in headless mode
        use_nodriver: Use nodriver (for selenium scraper type)
        nodriver_cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
        proxy_file: Path to file with proxy list (format: host:port:user:pass)

    Returns:
        BashOperator task
    """
    task_id = 'match_all_data'
    output_file = '/tmp/fbref_match_all_data.json'

    # Build selenium args (combined_match_data uses selenium with nodriver)
    selenium_args_list = []
    if headless:
        selenium_args_list.append('--headless')
    if use_xvfb:
        selenium_args_list.append('--use-xvfb')
    if use_nodriver:
        selenium_args_list.append('--use-nodriver')
        selenium_args_list.append(f'--nodriver-cloudflare-wait {nodriver_cloudflare_wait}')
    if proxy_file:
        selenium_args_list.append(f'--proxy-file {proxy_file}')
    selenium_args = ' '.join(selenium_args_list)

    bash_command = f"""
cd /opt/airflow && \\
python dags/scripts/run_fbref_scraper.py \\
    --scraper-type selenium \\
    {selenium_args} \\
    --mode combined_match_data \\
    --leagues "{leagues_str}" \\
    --season {season} \\
    --max-matches {max_matches} \\
    --output {output_file}
"""

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
            'DISPLAY': ':99',
        },
    )


# Build arguments
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_fbref',
    default_args=SELENIUM_ARGS,
    description='Memory-efficient FBref data ingestion with TaskGroup architecture',
    schedule=SCHEDULES.get('dag_ingest_fbref', '0 6 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('fbref', ['scraping', 'fbref', 'bronze', 'football', 'selenium']),
    max_active_runs=1,
    concurrency=1,  # Reduced from 3 to prevent OOM - only one task at a time
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
        'max_matches': 0,  # 0 = no limit
        'scraper_type': DEFAULT_SCRAPER_TYPE,  # 'soccerdata' or 'selenium'
        'use_tor': USE_TOR,  # Use Tor proxy for soccerdata
    },
    doc_md="""
    ## FBref Data Ingestion (Refactored + Optimized)

    Memory-efficient DAG using TaskGroup architecture with 3x optimization
    for match-level data collection.

    ### Match Data Optimization (Feb 2026)

    **Problem:** Separate tasks for shot_events, match_events, lineups
    each iterate through ALL matches, causing 3x redundant page loads.

    **Solution:** Combined `match_all_data` task collects all three data types
    in a single pass through matches.

    | Metric | Before | After |
    |--------|--------|-------|
    | HTTP requests | 1144 | 381 |
    | Execution time | 2-4 hours | 15-25 min |
    | Duplication | 3x | 0 |

    ### Scraper Types

    This DAG supports two scraper backends:

    **1. nodriver (default, recommended)**
    - Browser-based scraper with Cloudflare Turnstile bypass
    - Uses cf-verify plugin for automatic CAPTCHA solving
    - Works for all data types including match-level data

    **2. selenium + nodriver** - Alternative for match-level data
    - Browser-based scraper using nodriver (successor to undetected-chromedriver)
    - Better fingerprint evasion and Cloudflare Turnstile bypass
    - Uses Xvfb virtual display to avoid headless detection

    **Note:** Tor and FlareSolverr are no longer effective against FBref's
    Cloudflare protection. Residential proxies are now required.

    ### Configuration

    Set in DAG params or edit constants in source:
    - `DEFAULT_SCRAPER_TYPE`: 'nodriver' (recommended) or 'selenium'
    - `USE_TOR`: False (Tor is blocked by FBref)
    - `PROXY_FILE`: Path to proxy file (format: host:port:user:pass)
    - `USE_NODRIVER`: True - Use nodriver for selenium scraper
    - `NODRIVER_CLOUDFLARE_WAIT`: 90.0 - Time to wait for Cloudflare challenge
    - `USE_XVFB`: True/False - Xvfb for Selenium (recommended: True)
    - `HEADLESS`: True/False - Chrome headless mode

    ### Architecture (Optimized)

    ```
    dag_ingest_fbref
    ├── TaskGroup: player_stats (9 SEQUENTIAL tasks)
    │   ├── player_stats, player_shooting, player_passing
    │   ├── player_passing_types, player_gca, player_defense
    │   └── player_possession, player_playingtime, player_misc
    ├── TaskGroup: team_stats (9 SEQUENTIAL tasks)
    │   └── (same stat_types as player)
    ├── TaskGroup: keeper_stats (2 SEQUENTIAL tasks)
    │   ├── keeper_keeper
    │   └── keeper_keeper_adv
    ├── TaskGroup: match_data (OPTIMIZED)
    │   ├── match_schedule
    │   └── match_all_data (combined: shot_events + match_events + lineups)
    └── validate_all_data
    ```

    ### Tables Created (24 tables)

    **Player Stats (9 tables):**
    - fbref_player_stats, fbref_player_shooting, fbref_player_passing
    - fbref_player_passing_types, fbref_player_gca, fbref_player_defense
    - fbref_player_possession, fbref_player_playingtime, fbref_player_misc

    **Team Stats (9 tables):**
    - fbref_team_stats, fbref_team_shooting, fbref_team_passing
    - etc.

    **Keeper Stats (2 tables):**
    - fbref_keeper_keeper, fbref_keeper_keeper_adv

    **Match Data (4 tables):**
    - fbref_schedule, fbref_shot_events, fbref_match_events, fbref_lineups

    ### Testing with soccerdata + residential proxies (recommended)

    Test the scraper with soccerdata and residential proxy rotation:
    ```bash
    docker compose exec airflow-webserver python -c "
    from scrapers.soccerdata_fbref_scraper import SoccerdataFBrefScraper

    s = SoccerdataFBrefScraper(
        leagues=['ENG-Premier League'],
        seasons=[2025],
        use_tor=False,
        proxy_file='/opt/airflow/proxys.txt',
        no_cache=True
    )
    df = s.read_schedule()
    print(f'Schedule rows: {len(df) if df is not None else 0}')
    "
    ```

    ### Testing proxy connectivity

    ```bash
    docker compose exec airflow-webserver python -c "
    from scrapers.utils.proxy_manager import ProxyManager, ProxyType

    pm = ProxyManager()
    pm.load_from_file_custom_format('/opt/airflow/proxys.txt', ProxyType.HTTP)
    print(f'Loaded {pm.total_count} proxies')
    print(f'First proxy URL: {pm.get_http_proxy_url()}')
    "
    ```

    ### Testing with nodriver + proxy (fallback for match data)

    ```bash
    docker compose exec airflow-webserver python dags/scripts/run_fbref_scraper.py \\
        --scraper-type selenium \\
        --use-nodriver \\
        --nodriver-cloudflare-wait 45 \\
        --proxy-file /opt/airflow/proxys.txt \\
        --headless \\
        --use-xvfb \\
        --mode match_data \\
        --match-data-type shot_events \\
        --leagues "ENG-Premier League" \\
        --season 2024 \\
        --output /tmp/test_nodriver.json
    ```
    """,
) as dag:

    # =========================================================================
    # Start Task (заменяет FlareSolverrSensor, т.к. используем undetected-chromedriver)
    # =========================================================================
    # FlareSolverr теперь не используется напрямую - вместо него
    # используется undetected-chromedriver + Xvfb для обхода Cloudflare
    start = EmptyOperator(task_id='start')

    # =========================================================================
    # Prewarm CF Cookies Task
    # =========================================================================
    # Pre-solve Cloudflare Turnstile to get cf_clearance cookies
    # before starting any scraper tasks
    prewarm_task = PythonOperator(
        task_id='prewarm_cf_cookies',
        python_callable=prewarm_cf_cookies,
        retries=2,
        retry_delay=timedelta(seconds=120),
    ) if CF_COOKIE_PREWARM else None

    # =========================================================================
    # TaskGroup: Player Stats (9 SEQUENTIAL tasks to prevent OOM)
    # =========================================================================
    with TaskGroup(group_id='player_stats') as player_stats_group:
        player_tasks = []
        prev_task = None
        for stat_type in PLAYER_STAT_TYPES:
            task = create_single_stat_task(
                stat_type=stat_type,
                data_category='player',
                leagues_str=leagues_str,
                season=CURRENT_SEASON,
                scraper_type=DEFAULT_SCRAPER_TYPE,
                use_tor=USE_TOR,
                use_flaresolverr=USE_FLARESOLVERR,
                flaresolverr_url=FLARESOLVERR_URL,
                use_xvfb=USE_XVFB,
                headless=HEADLESS,
                use_nodriver=USE_NODRIVER,
                nodriver_cloudflare_wait=NODRIVER_CLOUDFLARE_WAIT,
                nodriver_content_timeout=NODRIVER_CONTENT_TIMEOUT,
                nodriver_max_retries=NODRIVER_MAX_RETRIES,
                nodriver_cf_verify_retries=NODRIVER_CF_VERIFY_RETRIES,
                proxy_file=PROXY_FILE,
            )
            # Chain tasks sequentially to prevent OOM
            if prev_task is not None:
                prev_task >> task
            prev_task = task
            player_tasks.append(task)

    # =========================================================================
    # TaskGroup: Team Stats (9 SEQUENTIAL tasks to prevent OOM)
    # =========================================================================
    with TaskGroup(group_id='team_stats') as team_stats_group:
        team_tasks = []
        prev_task = None
        for stat_type in TEAM_STAT_TYPES:
            task = create_single_stat_task(
                stat_type=stat_type,
                data_category='team',
                leagues_str=leagues_str,
                season=CURRENT_SEASON,
                scraper_type=DEFAULT_SCRAPER_TYPE,
                use_tor=USE_TOR,
                use_flaresolverr=USE_FLARESOLVERR,
                flaresolverr_url=FLARESOLVERR_URL,
                use_xvfb=USE_XVFB,
                headless=HEADLESS,
                use_nodriver=USE_NODRIVER,
                nodriver_cloudflare_wait=NODRIVER_CLOUDFLARE_WAIT,
                nodriver_content_timeout=NODRIVER_CONTENT_TIMEOUT,
                nodriver_max_retries=NODRIVER_MAX_RETRIES,
                nodriver_cf_verify_retries=NODRIVER_CF_VERIFY_RETRIES,
                proxy_file=PROXY_FILE,
            )
            # Chain tasks sequentially to prevent OOM
            if prev_task is not None:
                prev_task >> task
            prev_task = task
            team_tasks.append(task)

    # =========================================================================
    # TaskGroup: Keeper Stats (2 SEQUENTIAL tasks to prevent OOM)
    # =========================================================================
    with TaskGroup(group_id='keeper_stats') as keeper_stats_group:
        keeper_tasks = []
        prev_task = None
        for stat_type in KEEPER_STAT_TYPES:
            task = create_single_stat_task(
                stat_type=stat_type,
                data_category='keeper',
                leagues_str=leagues_str,
                season=CURRENT_SEASON,
                scraper_type=DEFAULT_SCRAPER_TYPE,
                use_tor=USE_TOR,
                use_flaresolverr=USE_FLARESOLVERR,
                flaresolverr_url=FLARESOLVERR_URL,
                use_xvfb=USE_XVFB,
                headless=HEADLESS,
                use_nodriver=USE_NODRIVER,
                nodriver_cloudflare_wait=NODRIVER_CLOUDFLARE_WAIT,
                nodriver_content_timeout=NODRIVER_CONTENT_TIMEOUT,
                nodriver_max_retries=NODRIVER_MAX_RETRIES,
                nodriver_cf_verify_retries=NODRIVER_CF_VERIFY_RETRIES,
                proxy_file=PROXY_FILE,
            )
            # Chain tasks sequentially to prevent OOM
            if prev_task is not None:
                prev_task >> task
            prev_task = task
            keeper_tasks.append(task)

    # =========================================================================
    # TaskGroup: Match Data (OPTIMIZED - combined task for 3x efficiency)
    # =========================================================================
    # OPTIMIZATION: Instead of 3 separate tasks (shot_events, match_events, lineups)
    # that each iterate through all matches (3N page loads), we use ONE combined task
    # that collects all data in a single pass (N page loads = 3x reduction).
    #
    # Before: schedule → shot_events → match_events → lineups
    #         HTTP requests: 1 + 380 + 380 + 380 = 1141 (for 380 matches)
    #
    # After:  schedule → match_all_data
    #         HTTP requests: 1 + 380 = 381 (3x reduction)
    # =========================================================================
    with TaskGroup(group_id='match_data') as match_data_group:
        # Schedule task uses nodriver
        schedule_task = create_match_data_task(
            data_type='schedule',
            leagues_str=leagues_str,
            season=CURRENT_SEASON,
            scraper_type=DEFAULT_SCRAPER_TYPE,
            use_tor=USE_TOR,
            use_flaresolverr=USE_FLARESOLVERR,
            flaresolverr_url=FLARESOLVERR_URL,
            use_xvfb=USE_XVFB,
            headless=HEADLESS,
            use_nodriver=USE_NODRIVER,
            nodriver_cloudflare_wait=NODRIVER_CLOUDFLARE_WAIT,
            nodriver_max_retries=NODRIVER_MAX_RETRIES,
            nodriver_cf_verify_retries=NODRIVER_CF_VERIFY_RETRIES,
            proxy_file=PROXY_FILE,
        )

        # Combined match data task: collects shot_events, match_events, lineups
        # in ONE pass through matches (3x efficiency vs separate tasks)
        match_all_task = create_combined_match_data_task(
            leagues_str=leagues_str,
            season=CURRENT_SEASON,
            max_matches=0,  # 0 = no limit, process all matches
            use_xvfb=USE_XVFB,
            headless=HEADLESS,
            use_nodriver=USE_NODRIVER,
            nodriver_cloudflare_wait=NODRIVER_CLOUDFLARE_WAIT,
            proxy_file=PROXY_FILE,
        )

        # schedule → match_all_data
        schedule_task >> match_all_task

    # =========================================================================
    # Validation Task
    # =========================================================================
    validate_task = PythonOperator(
        task_id='validate_all_data',
        python_callable=validate_all_data,
        trigger_rule='all_done',  # Run even if some tasks fail
    )

    # =========================================================================
    # Dependencies: Start -> Prewarm -> TaskGroups SEQUENTIAL -> Validate
    # Changed from parallel to sequential execution to prevent OOM
    # =========================================================================
    if CF_COOKIE_PREWARM and prewarm_task:
        start >> prewarm_task >> player_stats_group >> team_stats_group >> keeper_stats_group >> match_data_group >> validate_task
    else:
        start >> player_stats_group >> team_stats_group >> keeper_stats_group >> match_data_group >> validate_task
