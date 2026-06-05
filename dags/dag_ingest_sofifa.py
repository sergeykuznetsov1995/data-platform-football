"""
SoFIFA Data Ingestion DAG
=========================

Airflow DAG for scraping FIFA video game player ratings from SoFIFA.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules weekly on Sunday at 6 AM UTC.

NOTE: SoFIFA data doesn't change frequently, so weekly scraping is sufficient.

Data collected:
- Player overall ratings and attributes
- Team data

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import LEAGUES, SOFIFA_VERSIONS, SCHEDULES, DAG_TAGS
from utils.default_args import WEEKLY_ARGS


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
        with open('/tmp/sofifa_result.json', 'r') as f:
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
            'players_rows': scrape_result.get('players_rows', 0),
            'teams_rows': scrape_result.get('teams_rows', 0),
            'team_ratings_rows': scrape_result.get('team_ratings_rows', 0),
            'versions_rows': scrape_result.get('versions_rows', 0),
            'leagues_rows': scrape_result.get('leagues_rows', 0),
            'player_ratings_rows': scrape_result.get('player_ratings_rows', 0),
            'tables': scrape_result.get('tables', []),
        }
    }

    if scrape_result.get('errors'):
        validation['warnings'] = scrape_result['errors']
        total_rows = validation['summary']['players_rows'] + validation['summary']['teams_rows']
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # Fail closed: пустой Bronze (CF-блок / "tab crashed") должен ВАЛИТЬ задачу,
    # а не молча warn'ить — иначе DAG зелёный, пока Bronze протухает днями.
    # См. docs/bronze_audit_2026-05-13.md §Phase 5 + issue #136.
    if (validation['summary']['players_rows'] == 0
            and validation['summary']['teams_rows'] == 0):
        raise AirflowException(
            "Zero rows scraped (players=0, teams=0) — SoFIFA Bronze is empty"
        )

    # SoFIFA has lots of players, so we expect significant data
    if validation['summary']['players_rows'] < 1000:
        validation['warnings'].append("Low player count - possible scraping issue")

    if validation['summary']['teams_rows'] < 100:
        validation['warnings'].append("Low team count - possible scraping issue")

    # Player ratings (issue #42): ~545 per APL edition. A near-empty table
    # usually means FlareSolverr could not clear the Turnstile this run.
    if validation['summary']['player_ratings_rows'] < 100:
        validation['warnings'].append(
            "Low player_ratings count - possible FlareSolverr/Turnstile issue"
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
versions_str = SOFIFA_VERSIONS  # "latest", "all", or comma-separated IDs

# DAG definition
with DAG(
    dag_id='dag_ingest_sofifa',
    default_args=WEEKLY_ARGS,
    description='Ingest FIFA video game ratings from SoFIFA (weekly)',
    schedule=SCHEDULES.get('dag_ingest_sofifa', '0 6 * * 0'),  # Sunday 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('sofifa', ['scraping', 'sofifa', 'bronze', 'fifa']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'versions': SOFIFA_VERSIONS,
    },
    doc_md="""
    ## SoFIFA Data Ingestion

    This DAG scrapes FIFA video game player ratings and attributes from SoFIFA.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Schedule

    Runs **weekly on Sunday** at 6 AM UTC because FIFA data updates infrequently.

    ### Parameters

    - `leagues`: List of leagues to filter players by
    - `versions`: List of FIFA versions to scrape (e.g., ['24', '25'])

    ### Data Collected

    - **Players**: Overall ratings, potential, attributes (pace, shooting, etc.),
      positions, work rates, wages, values
    - **Teams**: Team-level data

    ### Notes

    - Uses `versions` instead of `seasons` (FIFA game versions)
    - Large dataset (~20k+ players per version)
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_data_task = BashOperator(
        task_id='scrape_sofifa_data',
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_sofifa_scraper.py \\
    --leagues "{leagues_str}" \\
    --versions "{versions_str}" \\
    --output /tmp/sofifa_result.json
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        # Inherit container env (TRINO_PASSWORD/TRINO_PORT) so the Iceberg writer
        # connects via HTTPS:8443 with auth instead of falling back to HTTP:8080.
        append_env=True,
        # player_ratings fetches ~545 player pages through FlareSolverr with
        # session rotation (~12s + occasional ~15s rotation each) → up to ~2h.
        execution_timeout=timedelta(hours=4),
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        
        trigger_rule='all_done',
    )

    scrape_data_task >> validate_data_task
