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

import requests.exceptions as _req_exc
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from trino.exceptions import TrinoConnectionError

from utils.config import (
    DAG_TAGS,
    LEAGUES,
    MIN_ROW_THRESHOLDS,
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


def _bronze_count(table_name: str) -> int:
    """Count rows in iceberg.bronze.{table_name} via Trino."""
    from utils.silver_tasks import _get_trino_connection, _validate_identifier

    _validate_identifier(table_name, "table")
    conn = _get_trino_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM iceberg.bronze.{table_name}")
            row = cur.fetchall()
            return int(row[0][0]) if row else 0
        finally:
            cur.close()
    finally:
        conn.close()


def _validate_table(table_name: str, threshold_key: str) -> Dict[str, Any]:
    """Run a row-count check against MIN_ROW_THRESHOLDS for one Bronze table."""
    import logging

    logger = logging.getLogger(__name__)
    try:
        threshold = MIN_ROW_THRESHOLDS[threshold_key]
    except KeyError as e:
        raise AirflowException(
            f"MIN_ROW_THRESHOLDS missing key '{threshold_key}' — refusing silent-pass. "
            f"Add a threshold in dags/utils/config.py before re-running."
        ) from e

    try:
        rows = _bronze_count(table_name)
    except (TrinoConnectionError, _req_exc.ConnectionError) as e:
        # Trino unreachable (container down, DNS not resolving, network) — infra
        # issue, not data. Distinct message helps on-call separate scope from a
        # missing/empty table. Airflow retries (3×15min via WHOSCORED_ARGS) cover
        # the recovery window once `restart: unless-stopped` brings Trino back.
        logger.error(f"Trino unreachable while counting {table_name}: {e}")
        raise AirflowException(
            f"Trino unreachable (infra issue, not data): {e}"
        ) from e
    except Exception as e:
        # If the Bronze table doesn't exist (first run, cancelled subtask), the
        # COUNT(*) raises. Surface as a hard validation failure.
        logger.error(f"COUNT(*) failed for {table_name}: {e}")
        raise AirflowException(
            f"Bronze table iceberg.bronze.{table_name} unavailable: {e}"
        ) from e

    summary = {'table': table_name, 'rows': rows, 'threshold': threshold}
    logger.info(f"Validation: {summary}")

    if rows < threshold:
        raise AirflowException(
            f"{table_name}: {rows} rows < threshold {threshold} (seasons={SEASONS_STR})"
        )
    return summary


def validate_schedule(**context) -> Dict[str, Any]:
    """Hard threshold check for whoscored_schedule (5 seasons APL ~ 1900 rows)."""
    return _validate_table('whoscored_schedule', 'whoscored_schedule')


def validate_events(**context) -> Dict[str, Any]:
    """Hard threshold check for whoscored_events (1 latest season ~ 500k rows)."""
    return _validate_table('whoscored_events', 'whoscored_events')


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

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

    scrape_task >> [validate_schedule_task, validate_events_task]
