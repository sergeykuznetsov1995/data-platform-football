"""
FotMob Data Ingestion DAG
=========================

Airflow DAG for scraping football statistics from FotMob.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Pure HTTP — FotMob's public /api/data/leagues endpoint requires no auth.

Schedules daily at 7 AM UTC (after FBref to avoid overlapping).

Data collected:
- Match schedules and results
- Team season statistics
- Player season statistics

All data is written to Iceberg Bronze layer tables.
"""

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import DEFAULT_ARGS


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality.

    Hard-fails when zero rows were ingested (regardless of whether the
    scraper script reported explicit errors), so DAG runs go red whenever
    FotMob changes their API again.

    Returns:
        Validation results
    """
    import json
    import logging

    logger = logging.getLogger(__name__)

    try:
        with open('/tmp/fotmob_result.json', 'r') as f:
            scrape_result = json.load(f)
    except FileNotFoundError:
        logger.error("Results file not found - scraping may have failed")
        raise AirflowException("Results file not found - scraping failed")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in results: {e}")
        raise AirflowException(f"Invalid JSON in results: {e}")

    summary = {
        'schedule_rows': scrape_result.get('schedule_rows', 0),
        'team_stats_rows': scrape_result.get('team_stats_rows', 0),
        'player_stats_rows': scrape_result.get('player_stats_rows', 0),
        'tables': scrape_result.get('tables', []),
    }
    total_rows = (
        summary['schedule_rows']
        + summary['team_stats_rows']
        + summary['player_stats_rows']
    )

    validation = {
        'status': 'success',
        'warnings': list(scrape_result.get('errors') or []),
        'summary': summary,
    }

    # Hard-fail on empty ingest — prevents silent green DAGs when the API
    # path changes (the bug that caused this validation to be tightened).
    if total_rows == 0:
        validation['status'] = 'failed'
        logger.error(f"FotMob ingest produced zero rows. Warnings: {validation['warnings']}")
        raise AirflowException(
            f"FotMob ingest produced zero rows. "
            f"Errors from scraper: {validation['warnings']}"
        )

    if scrape_result.get('errors'):
        validation['status'] = 'partial_success'

    if summary['schedule_rows'] < 100:
        validation['warnings'].append("Low schedule row count - possible scraping issue")
    if summary['team_stats_rows'] < 50:
        validation['warnings'].append("Low team stats row count - possible scraping issue")

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {summary}")
    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    return validation


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

with DAG(
    dag_id='dag_ingest_fotmob',
    default_args=DEFAULT_ARGS,
    description='Ingest football statistics from FotMob (public /api/data JSON)',
    schedule=SCHEDULES.get('dag_ingest_fotmob', '0 7 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('fotmob', ['scraping', 'fotmob', 'bronze', 'football']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
    },
    doc_md="""
    ## FotMob Data Ingestion

    Scrapes football statistics from FotMob's public ``/api/data/leagues``
    endpoint — no Cloudflare bypass, no Selenium, no cookies required.

    ### Data Collected

    - **Schedule**: match dates, teams, scores
    - **Team Stats**: season standings (wins, draws, losses, points, goals)
    - **Player Stats**: top-player categories (goals, assists, rating, ...)

    ### Architecture

    BashOperator → ``run_fotmob_scraper.py`` → ``FotMobScraper`` (HTTP only).

    ### Failure Mode

    The scraper raises on any 4xx/5xx after retries, and ``validate_data``
    hard-fails on ``total_rows == 0`` — this is the lesson from the 2025
    breakage where FotMob renamed ``/api/leagues`` → ``/api/data/leagues``
    and the DAG silently kept publishing zero rows.
    """,
) as dag:

    scrape_data_task = BashOperator(
        task_id='scrape_fotmob_data',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_fotmob_scraper.py \\
    --leagues "{leagues_str}" \\
    --season {CURRENT_SEASON} \\
    --output /tmp/fotmob_result.json
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    scrape_data_task >> validate_data_task
