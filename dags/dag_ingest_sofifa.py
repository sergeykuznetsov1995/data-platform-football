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

from utils.bronze_validation import validate_table
from utils.config import (
    LEAGUES,
    NON_INTERNATIONAL_LEAGUES,
    PER_LEAGUE_FLOOR_BASES,
    SOFIFA_VERSIONS,
    SCHEDULES,
    DAG_TAGS,
)
from utils.default_args import WEEKLY_ARGS

# Issue #466: every Bronze table this DAG writes gets a fail-closed Trino
# COUNT(*) floor (threshold key == table name in MIN_ROW_THRESHOLDS). Before
# this, only players+teams==0 failed validate_data — team_ratings / versions /
# leagues / player_ratings could go stale for weeks behind a green DAG.
SOFIFA_BRONZE_TABLES = [
    'sofifa_players',
    'sofifa_teams',
    'sofifa_team_ratings',
    'sofifa_versions',
    'sofifa_leagues',
    'sofifa_player_ratings',
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

    # Инкрементальный no-op: sofifa version_id не изменился с прошлого рана,
    # скрейпер осознанно пропустил тяжёлые шаги (players/teams/team_ratings/
    # player_ratings). Нулевые *_rows здесь — норма, а полноту Bronze всё равно
    # сторожат validate_<table> floor-задачи (whole-table COUNT, #466).
    if scrape_result.get('skipped'):
        validation['status'] = 'skipped'
        validation['summary']['skipped'] = scrape_result['skipped']
        logger.info(f"Scrape skipped (no-op): {scrape_result['skipped']}")
        return validation

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

    # Пороги масштабируются от числа лиг: ~546 игроков и 20 клубов на лигу
    # (floor'ы = MIN_ROW_THRESHOLDS на лигу). Прежние константы (1000/100)
    # были рассчитаны на несколько лиг и шумели warning'ом на КАЖДОМ успешном
    # APL-ране (546 players / 20 teams). Считаем только клубные лиги —
    # SoFIFA не покрывает международные турниры (INT-World Cup и т.п., #913),
    # иначе их появление в LEAGUES задвоило бы floor и зашумило здоровый
    # однолиговый ран (тот же фильтр, что уже применяется к
    # MIN_ROW_THRESHOLDS['sofifa_*'] в utils/config.py).
    n_leagues = max(1, len([lg for lg in LEAGUES if not lg.startswith('INT-')]))
    if validation['summary']['players_rows'] < 450 * n_leagues:
        validation['warnings'].append("Low player count - possible scraping issue")

    if validation['summary']['teams_rows'] < 18 * n_leagues:
        validation['warnings'].append("Low team count - possible scraping issue")

    # Player ratings (issue #42): ~545 per league edition. A near-empty table
    # usually means FlareSolverr could not clear the Turnstile this run.
    if validation['summary']['player_ratings_rows'] < 450 * n_leagues:
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

    # Proxy-less by design (#616, decision 2026-06-18): no proxy flag + unset
    # PROXY_FILTER_URL means the FlareSolverr reader runs without a residential
    # proxy and solves Cloudflare itself. SoFIFA probe was 30/30 pages with
    # 5/30 CF "warnings" (vs 0 with proxy) — acceptable, but if a full ~545-page
    # run starts hitting CF failures, re-enable a proxy fallback by setting
    # PROXY_FILTER_URL=http://proxy_filter:8899 (ad-tech filter, #652). See
    # docs/research/flaresolverr-proxy-traffic-audit.md.
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
        # player_ratings fetches ~545 player pages per league through
        # FlareSolverr with session rotation (~12s + occasional ~15s rotation
        # each) → ~2h per league, serial across LEAGUES. 12h covers Big-5;
        # with the incremental version_id skip a full crawl only happens on
        # roster-update weeks, so the loose bound costs nothing on no-op runs.
        execution_timeout=timedelta(hours=12),
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,

        trigger_rule='all_done',
    )

    # Issue #466: hard Trino COUNT(*) floors — run even if the scrape task
    # failed (trigger_rule='all_done'), so an empty/wiped Bronze table can
    # never pass silently. #920 Phase 2: per-league floors over the club
    # leagues (sofifa never covers INT-*); sofifa_versions and
    # sofifa_player_ratings have no league column in bronze — they opt out
    # of the per-league scope EXPLICITLY (validate_table refuses a silent
    # whole-table downgrade for unregistered keys).
    validate_bronze_tasks = [
        PythonOperator(
            task_id=f'validate_{table}',
            python_callable=validate_table,
            op_args=[
                table,
                table,
                NON_INTERNATIONAL_LEAGUES
                if table in PER_LEAGUE_FLOOR_BASES else None,
            ],
            trigger_rule='all_done',
        )
        for table in SOFIFA_BRONZE_TABLES
    ]

    scrape_data_task >> [validate_data_task, *validate_bronze_tasks]
