"""
ESPN Data Ingestion DAG
=======================

Airflow DAG for scraping football data from ESPN.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Schedules daily at 12 PM UTC.

Data collected:
- Match schedules and results

All data is written to Iceberg Bronze layer tables (via Parquet fallback).
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.bronze_validation import validate_table
from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SCRAPER_ARGS


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
        with open('/tmp/espn_result.json', 'r') as f:
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
            'tables': scrape_result.get('tables', []),
        }
    }

    if scrape_result.get('errors'):
        validation['warnings'] = scrape_result['errors']
        # Only fail if the schedule scrape produced no rows.
        if validation['summary']['schedule_rows'] == 0:
            validation['status'] = 'failed'
        else:
            validation['status'] = 'partial_success'

    # Check minimum thresholds
    if validation['summary']['schedule_rows'] < 100:
        validation['warnings'].append("Low schedule row count - possible scraping issue")

    logger.info(f"Data validation complete: {validation['status']}")
    logger.info(f"Summary: {validation['summary']}")

    if validation['warnings']:
        logger.warning(f"Warnings: {validation['warnings']}")

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")

    return validation


def validate_schedule(**context) -> Dict[str, Any]:
    """Fail-closed Trino COUNT(*) floor for espn_schedule (issue #466).

    #920 Phase 2: per-league — each league in LEAGUES must clear its own
    competitions.yaml-derived floor (APL ~380 fixtures vs WC 104).
    """
    return validate_table('espn_schedule', 'espn_schedule', leagues=LEAGUES)


def validate_lineup(**context) -> Dict[str, Any]:
    """Fail-closed COUNT(*) wipe-floor for espn_lineup — the per-match tables
    were the unguarded half of the #466 class (only the schedule had a floor,
    so a wiped/frozen lineup table passed silently)."""
    return validate_table('espn_lineup', 'espn_lineup')


def validate_matchsheet(**context) -> Dict[str, Any]:
    """Fail-closed COUNT(*) wipe-floor for espn_matchsheet (see validate_lineup)."""
    return validate_table('espn_matchsheet', 'espn_matchsheet')


# Build arguments for bash command
leagues_str = ','.join(LEAGUES)

# DAG definition
with DAG(
    dag_id='dag_ingest_espn',
    default_args=SCRAPER_ARGS,
    description='Ingest football data from ESPN',
    schedule=SCHEDULES.get('dag_ingest_espn', '0 12 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('espn', ['scraping', 'espn', 'bronze']),
    max_active_runs=1,
    # issue #530: cap run wall-clock so a stuck/abandoned run auto-fails instead
    # of lingering forever. Scrape execution_timeout is 2h (DEFAULT_ARGS) — 3h
    # leaves headroom for the scrape + downstream validation tasks.
    dagrun_timeout=timedelta(hours=3),
    params={
        'leagues': LEAGUES,
        # season UI-configurable: the daily scheduled run uses the default
        # (CURRENT_SEASON); to backfill a past season use "Trigger DAG w/
        # config" and set season (e.g. 2016 = 2016/17). New (league, season)
        # partitions are written via replace_partitions — backfilling an early
        # season leaves all other partitions untouched (#713, pattern #710).
        'season': Param(
            default=CURRENT_SEASON,
            type='integer',
            minimum=2000,
            maximum=CURRENT_SEASON,
            title='Season (start year)',
            description=(
                'APL season start year (2016 = 2016/17). Default = current '
                'season for the daily run. Override to backfill early history '
                '(2016…2020 → 10-season corpus, #713).'
            ),
        ),
    },
    doc_md="""
    ## ESPN Data Ingestion

    This DAG scrapes football data from ESPN.

    ### Architecture

    Uses BashOperator to run scraper in isolated subprocess,
    preventing LocalExecutor fork memory issues.

    ### Data Collected

    One DAG = one source: the runner writes all three ESPN Bronze tables in a
    single run (#713):

    - **Schedule** (`bronze.espn_schedule`): match dates, teams + result
      columns (goals/status/venue/attendance) enriched from the scoreboard
      JSONs the schedule fetch already downloaded (zero extra traffic)
    - **Lineup** (`bronze.espn_lineup`): one row per player per game
    - **Matchsheet** (`bronze.espn_matchsheet`): match-level team stats + venue

    Lineup/matchsheet are incremental: unplayed matches are deferred (a
    pre-kickoff Summary would be cached forever as a stub) and games already
    in bronze are skipped, so the steady-state daily run fetches only new
    matches. Saves replace per (league, season, game). A full re-scrape needs
    the runner's `--force-replace`.

    ### Notes

    - Uses soccerdata library wrapper
    - Written to Parquet fallback (PyIceberg disabled for stability)
    """,
) as dag:

    scrape_data_task = BashOperator(
        task_id='scrape_espn_data',
        # --season is rendered at runtime from params.season (Jinja), so the
        # season is configurable from the UI ("Trigger DAG w/ config") without
        # a separate backfill DAG (#713, pattern #710). The f-string escapes
        # {{ }} as {{{{ }}}} so the literal Jinja tag survives into the command.
        bash_command=f"""
cd /opt/airflow && \\
python dags/scripts/run_espn_scraper.py \\
    --leagues "{leagues_str}" \\
    --season {{{{ params.season }}}} \\
    --output /tmp/espn_result.json
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

    # Issue #466: hard Trino COUNT(*) floor — runs even if the scrape task
    # failed (trigger_rule='all_done'), so an empty/wiped Bronze table can
    # never pass silently. Lineup/matchsheet get the same wipe-floor (they
    # were the unguarded half of the #466 class).
    validate_schedule_task = PythonOperator(
        task_id='validate_schedule',
        python_callable=validate_schedule,
        trigger_rule='all_done',
    )

    validate_lineup_task = PythonOperator(
        task_id='validate_lineup',
        python_callable=validate_lineup,
        trigger_rule='all_done',
    )

    validate_matchsheet_task = PythonOperator(
        task_id='validate_matchsheet',
        python_callable=validate_matchsheet,
        trigger_rule='all_done',
    )

    scrape_data_task >> [
        validate_data_task,
        validate_schedule_task,
        validate_lineup_task,
        validate_matchsheet_task,
    ]
