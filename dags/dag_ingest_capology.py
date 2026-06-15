"""
Capology Data Ingestion DAG
============================

Weekly Bronze ingest of Capology player salaries (issue #43).

Architecture mirrors ``dag_ingest_sofascore.py`` / ``dag_ingest_transfermarkt.py``:
a single BashOperator runs the scraper in an isolated subprocess, then
``validate_data`` aggregates row counts and flags CAPOLOGY_FALLBACK soft
exits.

Schedule: weekly, Monday 05:00 UTC (one hour after Transfermarkt).
"""

from datetime import datetime
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.config import CURRENT_SEASON, DAG_TAGS, LEAGUES, SCHEDULES
from utils.default_args import DEFAULT_ARGS


SALARIES_RESULT_PATH = '/tmp/capology_player_salaries_result.json'

# Capology ships the whole APL season (~526 rows) in one response — no smoke
# cap needed; we want the full snapshot on every weekly run.
SALARIES_DAILY_LIMIT: int = None

# MVP currency scope per issue #43. EUR/USD lift is a separate followup.
DEFAULT_CURRENCY: str = 'GBP'


def _load_result(path: str, logger) -> Dict[str, Any]:
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
    """Aggregate per-entity row counts + flag soft fallbacks."""
    import logging

    logger = logging.getLogger(__name__)
    salaries = _load_result(SALARIES_RESULT_PATH, logger)

    if not salaries:
        raise AirflowException(
            f"Salaries results file {SALARIES_RESULT_PATH} missing"
        )

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'salary_rows': salaries.get('rows', 0),
            'unique_players': salaries.get('players_with_rows', 0),
            'currency': salaries.get('currency', DEFAULT_CURRENCY),
            'fallback': salaries.get('fallback', False),
            'tables': salaries.get('tables', []),
        },
    }

    errors: List[str] = list(salaries.get('errors', []) or [])
    if errors:
        validation['warnings'] = errors
        validation['status'] = (
            'partial_success' if validation['summary']['salary_rows'] > 0 else 'failed'
        )

    # APL has 20 clubs × ~25 players ≈ 500 salaries — soft floor at 400.
    if validation['summary']['salary_rows'] < 400:
        if validation['summary']['fallback']:
            validation['warnings'].append(
                f"player_salaries CAPOLOGY_FALLBACK: rows={validation['summary']['salary_rows']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low salary row count: {validation['summary']['salary_rows']} < 400"
            )

    logger.info("Validation: status=%s summary=%s", validation['status'], validation['summary'])
    if validation['warnings']:
        logger.warning("Warnings: %s", validation['warnings'])

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")
    return validation


with DAG(
    dag_id='dag_ingest_capology',
    default_args=DEFAULT_ARGS,
    description='Ingest Capology player salaries Bronze (issue #43)',
    schedule=SCHEDULES.get('dag_ingest_capology', '0 5 * * 1'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get(
        'capology',
        ['scraping', 'capology', 'bronze', 'football', 'salaries'],
    ),
    max_active_runs=1,
    params={'leagues': LEAGUES, 'season': CURRENT_SEASON},
    doc_md=f"""
    ## Capology Data Ingestion (Issue #43)

    Weekly Bronze ingest of Capology salary snapshots. MVP currency: GBP.

    ### Architecture

    - BashOperator runs the scraper in an isolated subprocess.
    - ``validate_data`` (PythonOperator, ``trigger_rule='all_done'``)
      summarises row counts and flags soft CAPOLOGY_FALLBACK exits.

    ### Bronze table
    - ``iceberg.bronze.capology_player_salaries``
      partition: (league, season, currency); write semantics: replace.

    ### Notes
    - Capology ships the entire season roster in one HTML; rate-limit
      ≤10 req/min keeps us below the bursty CF flare threshold.
    - Default currency: {DEFAULT_CURRENCY}; EUR/USD are a followup.
    """,
) as dag:

    league = LEAGUES[0]
    season = CURRENT_SEASON

    salaries_limit_arg = (
        f' --limit {SALARIES_DAILY_LIMIT}' if SALARIES_DAILY_LIMIT else ''
    )

    scrape_salaries_task = BashOperator(
        task_id='scrape_player_salaries',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {SALARIES_RESULT_PATH} && \\
python dags/scripts/run_capology_scraper.py \\
    --entity player_salaries \\
    --league "{league}" \\
    --season {season} \\
    --currency {DEFAULT_CURRENCY}{salaries_limit_arg} \\
    --output {SALARIES_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "CAPOLOGY_FALLBACK exit-code 2 — propagating as soft success."
    exit 0
fi
exit $rc
""",
        env={
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
        },
        append_env=True,
    )

    # The other three APL data products (issue #321): same runner, one
    # BashOperator each, partition (league, season). All soft-fall back on a
    # CAPOLOGY_FALLBACK exit-code-2 like salaries do.
    PRODUCT_ENTITIES = [
        'team_payrolls', 'contract_extensions', 'transfer_window',
    ]
    product_tasks = []
    for _entity in PRODUCT_ENTITIES:
        _task = BashOperator(
            task_id=f'scrape_{_entity}',
            bash_command=f"""
cd /opt/airflow && \\
rm -f /tmp/capology_{_entity}_result.json && \\
python dags/scripts/run_capology_scraper.py \\
    --entity {_entity} \\
    --league "{league}" \\
    --season {season} \\
    --output /tmp/capology_{_entity}_result.json
rc=$?
if [ $rc -eq 2 ]; then
    echo "CAPOLOGY_FALLBACK exit-code 2 — propagating as soft success."
    exit 0
fi
exit $rc
""",
            env={
                'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
                'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
                'HOME': '/home/airflow',
            },
            append_env=True,
        )
        product_tasks.append(_task)

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    def _validate_bronze_quality(**ctx) -> None:
        """Trino-level CHECK gate over bronze.capology_player_salaries.

        row_count / no_duplicates are ERROR-severity (promoted after green
        weekly runs, issue #48); no_nulls / freshness stay WARNING so a
        CAPOLOGY_FALLBACK soft-exit doesn't hard-fail the gate.
        """
        from utils.data_quality import CHECK, run_checks

        season_short = (
            f"{str(CURRENT_SEASON)[2:4]}"
            f"{(int(str(CURRENT_SEASON)[2:4]) + 1) % 100:02d}"
        )
        where = (
            f"league = '{LEAGUES[0]}' AND season = '{season_short}' "
            f"AND currency = '{DEFAULT_CURRENCY}'"
        )
        checks = [
            CHECK.row_count(
                'bronze.capology_player_salaries',
                min_rows=400, where=where, severity='ERROR',
            ),
            CHECK.no_duplicates(
                'bronze.capology_player_salaries',
                pk=['league', 'season', 'currency', 'player_slug', 'club_slug'],
                where=where, severity='ERROR',
            ),
            CHECK.no_nulls(
                'bronze.capology_player_salaries',
                cols=['player_slug', 'player_name'],
                where=where, severity='WARNING',
            ),
            CHECK.freshness(
                'bronze.capology_player_salaries',
                ts_col='_ingested_at', max_age_hours=48,
                where=where, severity='WARNING',
            ),
        ]
        report = run_checks(checks, raise_on_error=True)
        import logging
        logging.getLogger(__name__).info(
            "validate_bronze_quality: %s", report.summary(),
        )

    validate_bronze_quality_task = PythonOperator(
        task_id='validate_bronze_quality',
        python_callable=_validate_bronze_quality,
        trigger_rule='all_done',
    )

    # Cascade Bronze→Silver: triggers dag_transform_capology_silver
    # (issue #63). wait_for_completion=False keeps Bronze DAG short; the
    # Silver DAG runs its own DQ gate.
    trigger_silver_task = TriggerDagRunOperator(
        task_id='trigger_silver',
        trigger_dag_id='dag_transform_capology_silver',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    [scrape_salaries_task, *product_tasks] >> validate_task \
        >> validate_bronze_quality_task >> trigger_silver_task
