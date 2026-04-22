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
- TaskGroup match_data: schedule -> match_all_data (OPTIMIZED)
- validate_all_data: final validation

OPTIMIZATION (Feb 2026):
- Before: schedule -> shot_events -> match_events -> lineups (4 tasks, 3N page loads)
- After:  schedule -> match_all_data (2 tasks, N page loads)
- HTTP requests reduction: 3x (e.g., 1141 -> 381 for 380 matches)
- Time reduction: ~2-4 hours -> ~15-25 minutes

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

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SELENIUM_ARGS
from utils.fbref_tasks import (
    create_single_stat_task,
    create_match_data_task,
    create_combined_match_data_task,
    create_trino_health_check_task,
)
from utils.fbref_callbacks import (
    prewarm_cf_cookies,
    validate_all_data,
    check_traffic_guard,
)

from scrapers.fbref.constants import (
    PLAYER_STAT_TYPES,
    TEAM_STAT_TYPES,
    KEEPER_STAT_TYPES,
    MATCH_DATA_TYPES,
)

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

# Selenium browser settings
USE_XVFB = True  # Xvfb virtual display to bypass headless detection
HEADLESS = True  # Headless mode with Xvfb

# Nodriver settings (PRIMARY for Cloudflare Turnstile bypass)
USE_NODRIVER = True  # Use nodriver with cf-verify plugin
NODRIVER_CLOUDFLARE_WAIT = 30.0  # Wait time for Cloudflare challenge
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

# =============================================================================
# COMMON TASK KWARGS (passed to all task factory functions)
# =============================================================================
# Bundled into a dict to avoid repeating the same kwargs in every call.
COMMON_TASK_KWARGS = dict(
    leagues_str=','.join(LEAGUES),
    season="{{ params.season }}",
    scraper_type=DEFAULT_SCRAPER_TYPE,
    use_tor=USE_TOR,
    tor_host=TOR_HOST,
    tor_port=TOR_PORT,
    use_xvfb=USE_XVFB,
    headless=HEADLESS,
    use_nodriver=USE_NODRIVER,
    nodriver_cloudflare_wait=NODRIVER_CLOUDFLARE_WAIT,
    nodriver_content_timeout=NODRIVER_CONTENT_TIMEOUT,
    nodriver_max_retries=NODRIVER_MAX_RETRIES,
    nodriver_cf_verify_retries=NODRIVER_CF_VERIFY_RETRIES,
    proxy_file=PROXY_FILE,
)

# Subset of kwargs for combined match data task (doesn't use all options)
COMBINED_MATCH_KWARGS = dict(
    leagues_str=','.join(LEAGUES),
    season="{{ params.season }}",
    use_xvfb=USE_XVFB,
    headless=HEADLESS,
    use_nodriver=USE_NODRIVER,
    nodriver_cloudflare_wait=NODRIVER_CLOUDFLARE_WAIT,
    proxy_file=PROXY_FILE,
)


def _build_sequential_stat_group(stat_types, data_category):
    """Build a list of sequential stat tasks within a TaskGroup context."""
    tasks = []
    prev_task = None
    for stat_type in stat_types:
        task = create_single_stat_task(
            stat_type=stat_type,
            data_category=data_category,
            **COMMON_TASK_KWARGS,
        )
        # Chain tasks sequentially to prevent OOM
        if prev_task is not None:
            prev_task >> task
        prev_task = task
        tasks.append(task)
    return tasks


# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id='dag_ingest_fbref',
    default_args=SELENIUM_ARGS,
    description='Memory-efficient FBref data ingestion with TaskGroup architecture',
    schedule=SCHEDULES.get('dag_ingest_fbref', '0 6 * * *'),
    start_date=datetime(2026, 2, 16),
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

    **Note:** Tor is no longer effective against FBref's Cloudflare protection.
    Residential proxies are now required.

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
    │   └── match_all_data (combined: 5 data types in single pass)
    └── validate_all_data
    ```

    ### Tables Created (26 tables)

    **Player Stats (9 tables):**
    - fbref_player_stats, fbref_player_shooting, fbref_player_passing
    - fbref_player_passing_types, fbref_player_gca, fbref_player_defense
    - fbref_player_possession, fbref_player_playingtime, fbref_player_misc

    **Team Stats (9 tables):**
    - fbref_team_stats, fbref_team_shooting, fbref_team_passing
    - etc.

    **Keeper Stats (2 tables):**
    - fbref_keeper_keeper, fbref_keeper_keeper_adv

    **Match Data (6 tables):**
    - fbref_schedule, fbref_shot_events, fbref_match_events, fbref_lineups
    - fbref_match_team_stats, fbref_match_player_stats

    ### Testing with soccerdata + residential proxies (recommended)

    Test the scraper with soccerdata and residential proxy rotation:
    ```bash
    docker compose exec airflow-webserver python -c "
    from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

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
    # Start Task
    # =========================================================================
    start = EmptyOperator(task_id='start')

    # =========================================================================
    # Prewarm CF Cookies Task
    # =========================================================================
    # Pre-solve Cloudflare Turnstile to get cf_clearance cookies
    # before starting any scraper tasks
    prewarm_task = PythonOperator(
        task_id='prewarm_cf_cookies',
        python_callable=prewarm_cf_cookies,
        op_kwargs=dict(
            proxy_file=PROXY_FILE,
            cache_ttl_minutes=CF_COOKIE_CACHE_TTL_MINUTES,
            use_cf_verify=USE_CF_VERIFY_PLUGIN,
            cf_verify_max_retries=CF_VERIFY_MAX_RETRIES,
            cf_verify_interval=CF_VERIFY_INTERVAL,
            use_xvfb=USE_XVFB,
            max_attempts=CF_COOKIE_PREWARM_ATTEMPTS,
        ),
        retries=2,
        retry_delay=timedelta(seconds=120),
    ) if CF_COOKIE_PREWARM else None

    # =========================================================================
    # TaskGroup: Player Stats (9 SEQUENTIAL tasks to prevent OOM)
    # =========================================================================
    with TaskGroup(group_id='player_stats') as player_stats_group:
        player_tasks = _build_sequential_stat_group(PLAYER_STAT_TYPES, 'player')

    # =========================================================================
    # TaskGroup: Team Stats (9 SEQUENTIAL tasks to prevent OOM)
    # =========================================================================
    with TaskGroup(group_id='team_stats') as team_stats_group:
        team_tasks = _build_sequential_stat_group(TEAM_STAT_TYPES, 'team')

    # =========================================================================
    # TaskGroup: Keeper Stats (2 SEQUENTIAL tasks to prevent OOM)
    # =========================================================================
    with TaskGroup(group_id='keeper_stats') as keeper_stats_group:
        keeper_tasks = _build_sequential_stat_group(KEEPER_STAT_TYPES, 'keeper')

    # =========================================================================
    # TaskGroup: Match Data (OPTIMIZED - combined task for 5x efficiency)
    # =========================================================================
    # OPTIMIZATION: Instead of 5 separate tasks (shot_events, match_events,
    # lineups, match_team_stats, match_player_stats) that would each iterate
    # through all matches (5N page loads), we use ONE combined task that
    # collects all 5 data types from a single HTML page per match.
    #
    # Before (naive): 5N page loads for 5 data types
    # After (combined): N page loads, HTML parsed 5 times in memory
    #
    # Real numbers for 380 matches:
    #   schedule + match_all_data = 1 + 380 = 381 HTTP requests
    # =========================================================================
    with TaskGroup(group_id='match_data') as match_data_group:
        # Pre-flight: verify Trino is reachable before schedule/match tasks
        trino_check = create_trino_health_check_task()

        # Schedule task uses nodriver
        schedule_task = create_match_data_task(
            data_type='schedule',
            **COMMON_TASK_KWARGS,
        )

        # Second Trino check: verify Trino is still up after schedule task
        # (Trino may have crashed between schedule and match_all_data)
        trino_check_2 = create_trino_health_check_task(
            task_id='check_trino_before_match',
        )

        # Combined match data task: collects shot_events, match_events, lineups
        # in ONE pass through matches (3x efficiency vs separate tasks)
        match_all_task = create_combined_match_data_task(
            max_matches=0,  # 0 = no limit, process all matches
            **COMBINED_MATCH_KWARGS,
        )

        # trino_check -> schedule -> trino_check_2 -> match_all_data -> traffic_guard
        #
        # traffic_guard reads /tmp/fbref_traffic_match_all_data.json written by
        # match_all_task and fails if real_proxy_mb exceeds the Airflow
        # Variable `fbref_proxy_mb_threshold` (default 500). Push XCom keys:
        # real_proxy_mb, real_proxy_requests, matches_scraped.
        traffic_guard = PythonOperator(
            task_id='traffic_guard',
            python_callable=check_traffic_guard,
            trigger_rule='all_done',  # always inspect traffic, even on upstream fail
        )

        trino_check >> schedule_task >> trino_check_2 >> match_all_task >> traffic_guard

    # =========================================================================
    # Validation Task
    # =========================================================================
    validate_task = PythonOperator(
        task_id='validate_all_data',
        python_callable=validate_all_data,
        trigger_rule='all_done',  # Run even if some tasks fail
    )

    # =========================================================================
    # Trigger Silver DAG after ingestion completes
    # =========================================================================
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_transform',
        trigger_dag_id='dag_transform_fbref_silver',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # =========================================================================
    # Dependencies: Start -> Prewarm -> TaskGroups SEQUENTIAL -> Validate -> Trigger Silver
    # Changed from parallel to sequential execution to prevent OOM
    # =========================================================================
    if CF_COOKIE_PREWARM and prewarm_task:
        start >> prewarm_task >> player_stats_group >> team_stats_group >> keeper_stats_group >> match_data_group >> validate_task >> trigger_silver
    else:
        start >> player_stats_group >> team_stats_group >> keeper_stats_group >> match_data_group >> validate_task >> trigger_silver
