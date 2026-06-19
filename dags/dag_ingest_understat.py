"""
Understat Data Ingestion DAG
============================

Airflow DAG for scraping xG statistics from Understat.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules daily at 9 AM UTC.

Data collected:
- Match schedules with xG
- Shot events with coordinates and xG
- Player season xG/xA statistics
- Team season xG statistics

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.bronze_validation import validate_table
from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SCRAPER_ARGS

# Issue #466: every Bronze table this DAG writes gets a fail-closed Trino
# COUNT(*) floor (threshold key == table name in MIN_ROW_THRESHOLDS).
UNDERSTAT_BRONZE_TABLES = [
    'understat_schedule',
    'understat_players',
    'understat_shots',
    'understat_team_match_stats',
    'understat_player_match_stats',
]


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
        with open('/tmp/understat_result.json', 'r') as f:
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
            'shots_rows': scrape_result.get('shots_rows', 0),
            'player_stats_rows': scrape_result.get('player_stats_rows', 0),
            'team_stats_rows': scrape_result.get('team_stats_rows', 0),
            'tables': scrape_result.get('tables', []),
        }
    }

    if scrape_result.get('errors'):
        validation['warnings'] = scrape_result['errors']
        total_rows = sum([
            validation['summary']['schedule_rows'],
            validation['summary']['shots_rows'],
            validation['summary']['player_stats_rows'],
            validation['summary']['team_stats_rows'],
        ])
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # Check minimum thresholds
    if validation['summary']['schedule_rows'] < 100:
        validation['warnings'].append("Low schedule row count - possible scraping issue")

    if validation['summary']['shots_rows'] < 500:
        validation['warnings'].append("Low shots row count - possible scraping issue")

    if validation['summary']['player_stats_rows'] < 100:
        validation['warnings'].append("Low player stats row count - possible scraping issue")

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
    dag_id='dag_ingest_understat',
    default_args=SCRAPER_ARGS,
    description='Ingest xG statistics from Understat',
    schedule=SCHEDULES.get('dag_ingest_understat', '0 9 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('understat', ['scraping', 'understat', 'bronze']),
    max_active_runs=1,
    # issue #530: cap run wall-clock so a stuck/abandoned run auto-fails instead
    # of lingering forever. Scrape execution_timeout is 2h (DEFAULT_ARGS) — 3h
    # leaves headroom for the scrape + downstream validation tasks.
    dagrun_timeout=timedelta(hours=3),
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
    },
    doc_md="""
    ## Understat Data Ingestion

    This DAG scrapes xG (expected goals) statistics from Understat.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Data Collected

    - **Schedule**: Match dates, teams, scores with xG
    - **Shots**: Individual shot events with coordinates and xG
    - **Player Stats**: Season-level player xG/xA statistics
    - **Team Stats**: Season-level team xG statistics

    ### xG Data

    Understat provides expected goals (xG) for:
    - Individual shots based on shot location and type
    - Aggregated player and team statistics

    ### Notes

    - Uses soccerdata library wrapper
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_data_task = BashOperator(
        task_id='scrape_understat_data',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_understat_scraper.py \\
    --leagues "{leagues_str}" \\
    --season {CURRENT_SEASON} \\
    --output /tmp/understat_result.json
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

    # Issue #466: hard Trino COUNT(*) floors — run even if the scrape task
    # failed (trigger_rule='all_done'), so an empty/wiped Bronze table can
    # never pass silently.
    validate_bronze_tasks = [
        PythonOperator(
            task_id=f'validate_{table}',
            python_callable=validate_table,
            op_args=[table, table],
            trigger_rule='all_done',
        )
        for table in UNDERSTAT_BRONZE_TABLES
    ]

    scrape_data_task >> [validate_data_task, *validate_bronze_tasks]
