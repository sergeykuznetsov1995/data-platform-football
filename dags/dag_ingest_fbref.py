"""
FBref Data Ingestion DAG (Refactored + Optimized)
==================================================

Memory-efficient DAG for scraping football statistics from FBref.

Supports two scraper backends:
1. nodriver (default, recommended) - Browser-based with Cloudflare Turnstile bypass
2. selenium - Browser-based with undetected-chromedriver (used for combined
   match-level data only)

Architecture:
- season_stats_all: ONE combined task for all player/team/keeper season stats
- TaskGroup match_data: schedule -> match_all_data (OPTIMIZED)
- validate_all_data: final validation

OPTIMIZATION (Feb 2026):
- Before: schedule -> shot_events -> match_events -> lineups (4 tasks, 3N page loads)
- After:  schedule -> match_all_data (2 tasks, N page loads)
- HTTP requests reduction: 3x (e.g., 1141 -> 381 for 380 matches)
- Time reduction: ~2-4 hours -> ~15-25 minutes

OPTIMIZATION (Jul 2026):
- Before: 9 single_stat tasks (player x4, team x4, keeper x1), each a separate
  process with its own browser + CF bypass; player and team stats pages
  downloaded twice (same URL for stats/shooting/misc).
- After: ONE season_stats_all task — 5 unique pages per (league, season),
  both player and squad tables parsed from the same HTML, single CF bypass,
  HTTP fast-path for the rest.
- Proxy traffic per run (1 league): ~24 MB -> ~3.5 MB.

CLEANUP (Apr 2026):
- Removed 5 player/team stat_types (passing, passing_types, gca, defense,
  possession) — FBref restricted these stats; tables were 100% empty.
- Removed deprecated soccerdata branch — curl_cffi cannot bypass Cloudflare
  Turnstile, so the scraper was non-functional.

Each stat_type is saved to a separate Iceberg table:
- fbref_player_{stat_type} (4 tables)
- fbref_team_{stat_type} (4 tables)
- fbref_keeper_{stat_type} (2 tables)
- fbref_schedule, fbref_match_events, fbref_lineups,
  fbref_match_team_stats, fbref_match_player_stats, fbref_match_managers

NOTE: As of 2025-2026, FBref uses Cloudflare Turnstile CAPTCHA.
      Only nodriver with cf-verify plugin can bypass this automatically.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SELENIUM_ARGS
from utils.fbref_tasks import (
    create_match_data_task,
    create_combined_match_data_task,
    create_combined_season_stats_task,
    create_trino_health_check_task,
)
from utils.fbref_callbacks import (
    validate_all_data,
    check_traffic_guard,
    report_proxy_traffic,
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
# =============================================================================

# Scraper type: 'nodriver' (recommended) or 'selenium'
# Only nodriver with cf-verify plugin can bypass Cloudflare Turnstile
# automatically. selenium is used for combined match-level data
# (shot_events, match_events, lineups, match_team_stats, match_player_stats).
DEFAULT_SCRAPER_TYPE = 'nodriver'

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


def _make_traffic_guard(label: str, group_suffix: str = '') -> PythonOperator:
    """Create a parameterized traffic_guard task for one entity.

    Issue #44: every traffic-heavy task gets its own guard reading
    `/tmp/fbref_traffic_<label>.json`. Per-task threshold can be set via
    `fbref_proxy_mb_threshold_<label>` Airflow Variable; falls back to
    the global `fbref_proxy_mb_threshold`.
    """
    task_id = f'traffic_guard_{label}{group_suffix}'
    return PythonOperator(
        task_id=task_id,
        python_callable=check_traffic_guard,
        op_kwargs={
            'traffic_path': f'/tmp/fbref_traffic_{label}.json',
            'label': label,
        },
        trigger_rule='all_done',  # always inspect traffic, even on upstream fail
    )


def _gate_scrape(**context) -> bool:
    """ShortCircuitOperator hook — TRUE means "run the FBref scrape".

    The FBref run is the most anti-bot-fragile path in the platform (nodriver
    Cloudflare-Turnstile bypass, ~15-25 min per run). The DAG's own cron is
    weekly (Monday), but ``dag_master_pipeline`` triggers this DAG daily via
    ``TriggerDagRunOperator``, which ignores the child cron — without a gate
    that means 7 CF bypasses a week instead of 1. Mirrors the sofascore /
    clubelo gates. Runs only when:

      - a manual "Trigger DAG w/ config" sets ``run_scrape=True`` (on demand); or
      - this is the DAG's OWN weekly scheduled run.

    Skipped on any external trigger (e.g. ``dag_master_pipeline``). Returning
    False short-circuits every downstream task to ``skipped``.
    """
    import logging

    logger = logging.getLogger(__name__)

    params = context.get('params') or {}
    if params.get('run_scrape'):
        logger.info("run_scrape=True → running FBref scrape on demand.")
        return True

    dag_run = context.get('dag_run')
    if getattr(dag_run, 'external_trigger', False):
        logger.info(
            "External trigger (e.g. dag_master_pipeline) → skip the heavy "
            "CF-bypass scrape; the DAG's own weekly cron covers FBref."
        )
        return False

    logger.info("Own scheduled run → running weekly FBref scrape.")
    return True


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
        'scraper_type': DEFAULT_SCRAPER_TYPE,  # 'nodriver' or 'selenium'
        'run_scrape': False,  # True = force the scrape on a manual/external trigger
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
    - `PROXY_FILE`: Path to proxy file (format: host:port:user:pass)
    - `USE_NODRIVER`: True - Use nodriver for selenium scraper
    - `NODRIVER_CLOUDFLARE_WAIT`: 90.0 - Time to wait for Cloudflare challenge
    - `USE_XVFB`: True/False - Xvfb for Selenium (recommended: True)
    - `HEADLESS`: True/False - Chrome headless mode

    ### Architecture (Optimized)

    ```
    dag_ingest_fbref
    ├── season_stats_all (combined: 9 bronze tables from 5 pages per league/season)
    │   └── traffic_guard_season_stats
    ├── TaskGroup: match_data (OPTIMIZED)
    │   ├── match_schedule
    │   └── match_all_data (combined: 6 data types in single pass)
    └── validate_all_data
    ```

    Note (Apr 2026): passing, passing_types, gca, defense, possession were
    removed — FBref restricted these stats and the tables were 100% empty.

    ### Tables Created (16 tables)

    **Player Stats (4 tables):**
    - fbref_player_stats, fbref_player_shooting
    - fbref_player_playingtime, fbref_player_misc

    **Team Stats (4 tables):**
    - fbref_team_stats, fbref_team_shooting
    - fbref_team_playingtime, fbref_team_misc

    **Keeper Stats (1 table):**
    - fbref_keeper_keeper

    **Match Data (7 tables):**
    - fbref_schedule, fbref_match_events, fbref_lineups
    - fbref_match_team_stats, fbref_match_player_stats, fbref_match_managers
    - fbref_match_keeper_stats

    ### Testing nodriver scraper with residential proxies

    Test the scraper end-to-end via the runner CLI (preferred — exercises the
    same code path as the DAG):
    ```bash
    docker compose exec airflow-webserver python dags/scripts/run_fbref_scraper.py \\
        --scraper-type nodriver \\
        --proxy-file /opt/airflow/proxys.txt \\
        --mode single_stat \\
        --stat-type stats \\
        --data-category player \\
        --leagues "ENG-Premier League" \\
        --season 2025 \\
        --output /tmp/test_nodriver_player_stats.json
    ```

    ### Testing proxy connectivity

    ```bash
    docker compose exec airflow-webserver python -c "
    from scrapers.utils.proxy_manager import ProxyManager, ProxyType

    pm = ProxyManager()
    pm.load_from_file_custom_format('/opt/airflow/proxys.txt', ProxyType.HTTP)
    print(f'Loaded {pm.total_count} proxies')
    print(f'First proxy: {pm.get_proxy().masked_url}')  # creds masked — do not log full URLs
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

    # Gate the whole scrape on external triggers (dag_master_pipeline runs
    # daily; FBref must stay weekly — see _gate_scrape). Short-circuiting here
    # skips every downstream task, including the all_done validators.
    gate_scrape = ShortCircuitOperator(
        task_id='gate_scrape',
        python_callable=_gate_scrape,
    )

    # =========================================================================
    # Season Stats: ONE combined task (replaces 9 single_stat tasks)
    # =========================================================================
    # OPTIMIZATION (Jul 2026): player and team stats share the same season
    # page for stats/shooting/misc (squad tables in the DOM, player table in
    # an HTML comment), so one process fetches 5 unique pages per
    # (league, season) instead of 9 — and pays ONE CF bypass instead of nine
    # (each of the old task processes bootstrapped its own browser at
    # ~2.7 MB proxy traffic per CF challenge). Uses the FBrefScraper HTTP
    # fast-path stack (same as match_all_data).
    #
    # Proxy traffic per run (1 league): ~24 MB -> ~3.5 MB.
    # =========================================================================
    season_stats_task = create_combined_season_stats_task(
        **COMBINED_MATCH_KWARGS,
    )

    # Guard reads /tmp/fbref_traffic_season_stats.json; per-task threshold via
    # Airflow Variable `fbref_proxy_mb_threshold_season_stats`.
    season_guard = _make_traffic_guard(label='season_stats')

    season_stats_task >> season_guard

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

        # Schedule task runs through the selenium scraper-type so it uses
        # FBrefScraper (and thus the Camoufox transport via FBREF_TRANSPORT);
        # the nodriver NodriverFBrefScraper can no longer pass fbref's
        # Cloudflare interstitial (#CF-2026-07).
        schedule_task = create_match_data_task(
            data_type='schedule',
            **{**COMMON_TASK_KWARGS, 'scraper_type': 'selenium'},
        )

        # Issue #44: guard on schedule's own CF/asset traffic. The schedule
        # task starts a fresh browser, so its CF bypass dominates the
        # per-task MB and was previously unattributed.
        schedule_guard = _make_traffic_guard(label='match_schedule')

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

        # traffic_guard reads /tmp/fbref_traffic_match_all_data.json written by
        # match_all_task and fails if real_proxy_mb exceeds the Airflow
        # Variable `fbref_proxy_mb_threshold_match_all_data` (or the global
        # `fbref_proxy_mb_threshold`, default 500). Push XCom keys:
        # real_proxy_mb, real_proxy_requests, matches_scraped, cf_*, restart_*.
        traffic_guard = _make_traffic_guard(label='match_all_data')

        trino_check >> schedule_task >> schedule_guard >> trino_check_2 >> match_all_task >> traffic_guard

    # =========================================================================
    # Validation Task
    # =========================================================================
    validate_task = PythonOperator(
        task_id='validate_all_data',
        python_callable=validate_all_data,
        trigger_rule='all_done',  # Run even if some tasks fail
    )

    # =========================================================================
    # Residential-proxy traffic report (#789)
    # =========================================================================
    # Aggregate the per-task /tmp/fbref_traffic_*.json byte counters into one
    # "PROXY_TRAFFIC source=fbref total=… MB" log line so the ~$4/GB residential
    # spend is visible per run. Passive (never raises), runs even if tasks fail.
    report_traffic = PythonOperator(
        task_id='report_proxy_traffic',
        python_callable=report_proxy_traffic,
        trigger_rule='all_done',
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
    # Dependencies: Start -> Gate -> Season Stats -> Match Data -> Validate -> Report -> Trigger Silver
    # Sequential execution to prevent OOM
    # =========================================================================
    start >> gate_scrape >> season_stats_task
    season_guard >> match_data_group >> validate_task >> report_traffic >> trigger_silver
