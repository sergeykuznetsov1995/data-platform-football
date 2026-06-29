"""
WhoScored Data Ingestion DAG
============================

W3 (Wave 3) — soccerdata-backed ingest with Selenium under the hood.

Schedule: weekly (Monday 04:00 UTC) — events table is heavy (~600k rows /
season for APL), so daily ingest is overkill.

The new :class:`WhoScoredScraper` exposes 4 high-level methods which are
called sequentially inside ONE BashOperator (single browser session — keeps
Cloudflare cookies hot, avoids re-bypass cost):

    * scrape_schedule()         — fixtures + integer game_id  (full N seasons)
    * scrape_missing_players()  — pre-match injuries / suspensions
    * scrape_season_stages()    — cup/league stage metadata
    * scrape_events()           — Opta events for the LATEST season only
                                  (`match_ids=None` → reader picks max(seasons))

Validation runs as TWO Trino COUNT(*) tasks against MIN_ROW_THRESHOLDS so a
crash in events doesn't mask a healthy schedule write.
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.bronze_validation import validate_table
from utils.config import (
    DAG_TAGS,
    LEAGUES,
    SCHEDULES,
    SEASONS_STR,
)
from utils.default_args import SELENIUM_ARGS


# Extended timeout for WhoScored due to Cloudflare + heavy events scrape.
WHOSCORED_ARGS = {
    **SELENIUM_ARGS,
    'execution_timeout': timedelta(hours=6),
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}


def validate_schedule(**context) -> Dict[str, Any]:
    """Hard threshold check for whoscored_schedule (5 seasons APL ~ 1900 rows)."""
    return validate_table('whoscored_schedule', 'whoscored_schedule')


def validate_events(**context) -> Dict[str, Any]:
    """Hard threshold check for whoscored_events (1 latest season ~ 500k rows)."""
    return validate_table('whoscored_events', 'whoscored_events')


def validate_player_profile(**context) -> Dict[str, Any]:
    """Hard threshold check for whoscored_player_profile (~531 players/season, #37)."""
    return validate_table('whoscored_player_profile', 'whoscored_player_profile')


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

# player_profile reads player_ids from bronze.whoscored_events (latest season).
# Use the latest single short-form season token so the resolver matches Bronze
# exactly and tags the partition correctly (see scraper resolver note).
whoscored_pp_season = SEASONS_STR.split(',')[-1]

# DAG definition
with DAG(
    dag_id='dag_ingest_whoscored',
    default_args=WHOSCORED_ARGS,
    description='Ingest WhoScored fixtures + Opta events (weekly, latest season events only)',
    schedule=SCHEDULES.get('dag_ingest_whoscored', '0 4 * * 1'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('whoscored', ['scraping', 'whoscored', 'bronze', 'selenium', 'spadl']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'seasons': SEASONS_STR,
    },
    doc_md=f"""
    ## WhoScored Data Ingestion (W3, weekly)

    Soccerdata-backed scraper running under headless Chromium 120 inside the
    Airflow container. Schedule: `{SCHEDULES.get('dag_ingest_whoscored')}`.

    ### Pipeline

    A single BashOperator runs `run_whoscored_scraper.py` which sequentially
    calls (one browser session — keeps Cloudflare cookies hot):

    1. `scrape_schedule()`         — full {SEASONS_STR} fixtures
    2. `scrape_missing_players()`  — pre-match injuries
    3. `scrape_season_stages()`    — cup/league stages
    4. `scrape_events()`           — **latest season only** (heaviest task)

    ### Why latest-season events only

    Events are ~600k rows / season for APL — 5 seasons would push past 3M and
    take many hours under Cloudflare-throttled scraping. `scrape_events()`
    with `match_ids=None` automatically picks `max(seasons)`.

    Threshold `whoscored_events = 500_000` is sized for one season + buffer.

    ### Validation

    Row counts are checked via Trino COUNT(*) (NOT via JSON output) so a crash
    in `scrape_events` doesn't mask a healthy schedule write:

    * `validate_schedule` — hard threshold ~1700 rows (5 seasons)
    * `validate_events`   — hard threshold 500k rows (latest season)
    """,
) as dag:

    # Proxy-less by design (#616, decision 2026-06-18): empty --proxy-file +
    # unset PROXY_FILTER_URL means no residential proxy — FlareSolverr solves
    # Cloudflare itself (probe: 30/30 pages, 0 CF failures). To re-enable a
    # proxy as a fallback, set PROXY_FILTER_URL=http://proxy_filter:8899
    # (ad-tech filter, #652) or pass a non-empty --proxy-file. See
    # docs/research/flaresolverr-proxy-traffic-audit.md.
    scrape_task = BashOperator(
        task_id='scrape_whoscored',
        bash_command=(
            'cd /opt/airflow && '
            f'python dags/scripts/run_whoscored_scraper.py '
            f'--leagues "{leagues_str}" '
            f'--seasons "{SEASONS_STR}" '
            f'--proxy-file "" '
            f'--flaresolverr-url http://flaresolverr:8191 '
            f'--output /tmp/whoscored_result.json'
        ),
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    validate_schedule_task = PythonOperator(
        task_id='validate_schedule',
        python_callable=validate_schedule,
        trigger_rule='all_done',
    )

    validate_events_task = PythonOperator(
        task_id='validate_events',
        python_callable=validate_events,
        trigger_rule='all_done',
    )

    # player_profile — biographical /Players/{id} snapshot (issue #37). Runs
    # AFTER the main scrape because it resolves player_ids from the freshly
    # written bronze.whoscored_events (latest season only). Separate FlareSolverr
    # session; proxy-less by default like the main scrape.
    scrape_player_profile_task = BashOperator(
        task_id='scrape_player_profile',
        bash_command=(
            'cd /opt/airflow && '
            f'python dags/scripts/run_whoscored_scraper.py '
            f'--player-profile '
            f'--leagues "{leagues_str}" '
            f'--seasons "{whoscored_pp_season}" '
            f'--proxy-file "" '
            f'--flaresolverr-url http://flaresolverr:8191 '
            f'--output /tmp/whoscored_player_profile_result.json'
        ),
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    validate_player_profile_task = PythonOperator(
        task_id='validate_player_profile',
        python_callable=validate_player_profile,
        trigger_rule='all_done',
    )

    scrape_task >> [validate_schedule_task, validate_events_task]
    scrape_task >> scrape_player_profile_task >> validate_player_profile_task
