"""
ClubElo Data Ingestion DAG
==========================

Airflow DAG for scraping ELO ratings from ClubElo.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules daily at 1 PM UTC.

Data collected:
- Current ELO ratings for all clubs

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import LEAGUES, SCHEDULES, DAG_TAGS
from utils.default_args import LIGHT_ARGS


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
        with open('/tmp/clubelo_result.json', 'r') as f:
            ratings_result = json.load(f)
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
            'ratings_rows': ratings_result.get('rows', 0),
            'history_rows': ratings_result.get('history_rows', 0),
            'rating_date': ratings_result.get('rating_date'),
            'tables': ratings_result.get('tables', []),
        }
    }

    if ratings_result.get('errors'):
        validation['warnings'] = ratings_result['errors']
        validation['status'] = (
            'partial_success'
            if validation['summary']['ratings_rows'] > 0
            else 'failed'
        )

    # ClubElo should have ratings for many clubs
    if validation['summary']['ratings_rows'] < 100:
        validation['warnings'].append("Low ratings count - possible scraping issue")

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation


# Build leagues argument for bash command
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_clubelo',
    default_args=LIGHT_ARGS,
    description='Ingest ELO ratings from ClubElo',
    schedule=SCHEDULES.get('dag_ingest_clubelo', '0 13 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('clubelo', ['scraping', 'clubelo', 'bronze', 'elo']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
    },
    doc_md="""
    ## ClubElo Data Ingestion

    This DAG scrapes ELO ratings for football clubs from ClubElo.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### ELO Rating System

    ClubElo uses a chess-like rating system adapted for football:
    - Initial rating: 1500
    - Updated after each match based on expected vs actual result
    - Higher rating = stronger team

    ### Data Collected

    - Club name and country
    - Current ELO rating
    - Rating date

    ### Notes

    - Simple, fast scraper (no rate limiting issues)
    - Data is partitioned by rating date
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_ratings_task = BashOperator(
        task_id='scrape_current_ratings',
        bash_command=f"""
cd /opt/airflow && \
python dags/scripts/run_clubelo_scraper.py \
    --leagues "{leagues_str}" \
    --output /tmp/clubelo_result.json
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        
        trigger_rule='all_done',
    )

    scrape_ratings_task >> validate_data_task
