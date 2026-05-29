"""
FotMob Data Ingestion DAG
=========================

Airflow DAG for scraping football statistics from FotMob.
Uses BashOperator to run scraper in isolated subprocess,
avoiding LocalExecutor memory issues.

Pure HTTP — FotMob's public /api/data/leagues endpoint requires no auth.

Schedules daily at 7 AM UTC (after FBref to avoid overlapping).

Data collected (9 Bronze tables):
- Match schedules and results        (fotmob_schedule)
- Team season standings              (fotmob_team_stats)
- Player stat leaderboards (top)     (fotmob_player_stats)
- Team profiles                      (fotmob_team_profile)
- Team squads                        (fotmob_team_squad)
- Team stat leaderboards             (fotmob_team_leaderboards)
- Transfers                          (fotmob_transfers)
- Per-match details                  (fotmob_match_details)
- Per-player details                 (fotmob_player_details)

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

    # Per-entity soft-warn minimums (APL single-season baselines). Hard-fail is
    # reserved for a fully empty ingest; individual low counts only warn.
    MIN_ROWS = {
        'schedule': 300,
        'team_stats': 18,
        'player_stats': 50,
        'team_profile': 18,
        'team_squad': 400,
        'team_leaderboards': 400,
        'transfers': 1,
        'match_details': 200,
        'player_details': 400,
    }

    # New runner emits a 'rows' dict; fall back to legacy flat '<key>_rows'.
    rows = scrape_result.get('rows') or {
        k: scrape_result.get(f'{k}_rows', 0) for k in MIN_ROWS
    }

    summary = {
        'rows': rows,
        'tables': scrape_result.get('tables', []),
    }
    total_rows = sum(rows.values())

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

    for key, minimum in MIN_ROWS.items():
        if rows.get(key, 0) < minimum:
            validation['warnings'].append(
                f"Low {key} row count ({rows.get(key, 0)} < {minimum}) "
                f"- possible scraping issue"
            )

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

    ### Data Collected (9 Bronze tables)

    - **Schedule**: match dates, teams, scores
    - **Team Stats**: season standings (wins, draws, losses, points, goals)
    - **Player Stats**: top-player categories (goals, assists, rating, ...)
    - **Team Profile**: venue, country, league position (per team)
    - **Team Squad**: squad members + coach (per team)
    - **Team Leaderboards**: team-side stat leaderboards (all categories)
    - **Transfers**: league transfer list
    - **Match Details**: lineups, events, stats, shotmap (per finished match)
    - **Player Details**: career, market values, trophies (per squad player)

    ### Architecture

    BashOperator → ``run_fotmob_scraper.py`` → ``FotMobScraper`` (HTTP only).
    All 9 entities use ``replace_partitions=['league','season']`` so daily
    re-runs overwrite the season partition rather than accumulating duplicates.

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
