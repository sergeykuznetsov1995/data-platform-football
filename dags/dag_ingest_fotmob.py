"""
FotMob Data Ingestion DAG
=========================

Airflow DAG for scraping football statistics from FotMob.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Uses Selenium with Cloudflare bypass for data collection.

Schedules daily at 7 AM UTC (after FBref to avoid overlapping).

Data collected:
- Match schedules and results
- Team season statistics
- Player season statistics

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SELENIUM_ARGS


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality.

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

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'schedule_rows': scrape_result.get('schedule_rows', 0),
            'team_stats_rows': scrape_result.get('team_stats_rows', 0),
            'player_stats_rows': scrape_result.get('player_stats_rows', 0),
            'tables': scrape_result.get('tables', []),
        }
    }

    # Check for errors
    if scrape_result.get('errors'):
        validation['warnings'] = scrape_result['errors']
        total_rows = sum([
            validation['summary']['schedule_rows'],
            validation['summary']['team_stats_rows'],
            validation['summary']['player_stats_rows'],
        ])
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # Check minimum data thresholds
    if validation['summary']['schedule_rows'] < 100:
        validation['warnings'].append("Low schedule row count - possible scraping issue")

    if validation['summary']['team_stats_rows'] < 50:
        validation['warnings'].append("Low team stats row count - possible scraping issue")

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_fotmob',
    default_args=SELENIUM_ARGS,
    description='Ingest football statistics from FotMob using Selenium',
    schedule=SCHEDULES.get('dag_ingest_fotmob', '0 7 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('fotmob', ['scraping', 'fotmob', 'bronze', 'football', 'selenium']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
    },
    doc_md="""
    ## FotMob Data Ingestion

    This DAG scrapes football statistics from FotMob.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Data Collected

    - **Schedule**: Match dates, teams, scores, venues
    - **Team Stats**: Season-level team statistics
    - **Player Stats**: Season-level player statistics (goals, assists, etc.)

    ### Notes

    - Uses Selenium with Cloudflare bypass
    - Uses xvfb for headless browser operation
    - API + cookies from browser for authentication
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_data_task = BashOperator(
        task_id='scrape_fotmob_data',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_fotmob_scraper.py \\
    --leagues "{leagues_str}" \\
    --season {CURRENT_SEASON} \\
    --output /tmp/fotmob_result.json \\
    --headless \\
    --use-xvfb
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
            'DISPLAY': ':99',
        },
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        
        trigger_rule='all_done',
    )

    scrape_data_task >> validate_data_task
