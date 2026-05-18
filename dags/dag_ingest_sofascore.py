"""
SofaScore Data Ingestion DAG
============================

Airflow DAG for scraping football statistics from SofaScore.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules daily at 11 AM UTC.

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
from utils.default_args import DEFAULT_ARGS


SCHEDULE_RESULT_PATH = '/tmp/sofascore_result.json'
PLAYER_RATINGS_RESULT_PATH = '/tmp/sofascore_player_ratings_result.json'


def _load_result(path: str, logger) -> Dict[str, Any]:
    """Load a runner JSON output. Missing file → empty dict (treated as failure)."""
    import json

    try:
        with open(path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Results file %s not found", path)
        return {}
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in %s: %s", path, e)
        return {}


def validate_data(**context) -> Dict[str, Any]:
    """
    Validate scraped data quality across both scrape tasks (schedule+league_table
    and player_ratings).
    """
    import logging

    logger = logging.getLogger(__name__)

    schedule_result = _load_result(SCHEDULE_RESULT_PATH, logger)
    ratings_result = _load_result(PLAYER_RATINGS_RESULT_PATH, logger)

    if not schedule_result:
        raise AirflowException(
            f"Schedule results file {SCHEDULE_RESULT_PATH} missing or unreadable"
        )

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'schedule_rows': schedule_result.get('schedule_rows', 0),
            'league_table_rows': schedule_result.get('league_table_rows', 0),
            'player_ratings_rows': ratings_result.get('rows', 0),
            'player_ratings_matches': ratings_result.get('matches_with_ratings', 0),
            'player_ratings_fallback': ratings_result.get('fallback', False),
            'tables': (
                schedule_result.get('tables', []) + ratings_result.get('tables', [])
            ),
        }
    }

    errors: List[str] = []
    errors.extend(schedule_result.get('errors', []) or [])
    errors.extend(ratings_result.get('errors', []) or [])
    if errors:
        validation['warnings'] = errors
        total_rows = sum([
            validation['summary']['schedule_rows'],
            validation['summary']['league_table_rows'],
            validation['summary']['player_ratings_rows'],
        ])
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # Minimum thresholds
    if validation['summary']['schedule_rows'] < 100:
        validation['warnings'].append("Low schedule row count - possible scraping issue")

    if validation['summary']['league_table_rows'] < 10:
        validation['warnings'].append("Low league_table row count - possible scraping issue")

    # APL has ~300 matches/season; ratings emit ~25K rows. Anything < 300 rows
    # means we scraped at most a handful of matches → DAG defect or hard CF block.
    if validation['summary']['player_ratings_rows'] < 300:
        if validation['summary']['player_ratings_fallback']:
            validation['warnings'].append(
                f"player_ratings R0.2B_FALLBACK: rows="
                f"{validation['summary']['player_ratings_rows']} matches="
                f"{validation['summary']['player_ratings_matches']}"
            )
            # Fallback is a soft failure — keep status non-failed so dependent
            # DAGs see partial_success, not hard-fail.
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low player_ratings row count: "
                f"{validation['summary']['player_ratings_rows']} < 300"
            )

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
    dag_id='dag_ingest_sofascore',
    default_args=DEFAULT_ARGS,
    description='Ingest football statistics from SofaScore',
    schedule=SCHEDULES.get('dag_ingest_sofascore', '0 11 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('sofascore', ['scraping', 'sofascore', 'bronze']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
    },
    doc_md="""
    ## SofaScore Data Ingestion

    This DAG scrapes football statistics from SofaScore.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Data Collected

    - **Schedule**: Match dates, teams, scores, venues
    - **Team Stats**: Season-level team statistics
    - **Player Stats**: Season-level player statistics

    ### Notes

    - Uses soccerdata library wrapper
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_data_task = BashOperator(
        task_id='scrape_sofascore_data',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofascore_scraper.py \\
    --leagues "{leagues_str}" \\
    --season {CURRENT_SEASON} \\
    --output {SCHEDULE_RESULT_PATH}
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
    )

    # R0.2B player_ratings: depends on freshly written bronze.sofascore_schedule
    # (runner reads finished match_ids from there). Exit code 2 = graceful
    # R0.2B_FALLBACK; treat as success at the bash level so validate_data runs.
    scrape_player_ratings_task = BashOperator(
        task_id='scrape_player_ratings',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity player_ratings \\
    --league "{LEAGUES[0]}" \\
    --season {CURRENT_SEASON} \\
    --output {PLAYER_RATINGS_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 — propagating as soft success."
    exit 0
fi
exit $rc
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    scrape_data_task >> scrape_player_ratings_task >> validate_data_task
