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

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.clubelo_tasks import validate_data
from utils.config import LEAGUES, SCHEDULES, DAG_TAGS
from utils.default_args import LIGHT_ARGS

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
