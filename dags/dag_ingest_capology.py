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
from utils.default_args import SCRAPER_ARGS


SALARIES_RESULT_TMPL = '/tmp/capology_player_salaries_{slug}_result.json'

# Capology ships the whole season (~500 rows) in one response — no smoke
# cap needed; we want the full snapshot on every weekly run.
SALARIES_DAILY_LIMIT: int = None

# MVP currency scope per issue #43. EUR/USD lift is a separate followup.
DEFAULT_CURRENCY: str = 'GBP'

# Per-league soft floor on salary rows: clubs × ~25 players, floored at ~80%.
# 20-club leagues → 400; 18-club (Bundesliga / Ligue 1) → 360.
LEAGUE_ROW_FLOORS = {
    'ENG-Premier League': 400,
    'ESP-La Liga': 400,
    'ITA-Serie A': 400,
    'GER-Bundesliga': 360,
    'FRA-Ligue 1': 360,
}
DEFAULT_ROW_FLOOR = 360


def _league_slug(league: str) -> str:
    """``'ENG-Premier League'`` → ``'eng_premier_league'`` (task-id / path safe)."""
    return league.lower().replace(' ', '_').replace('-', '_')


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
    """Aggregate per-league salary row counts + flag soft fallbacks."""
    import logging

    logger = logging.getLogger(__name__)

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'salary_rows': 0,
            'unique_players': 0,
            'currency': DEFAULT_CURRENCY,
            'leagues': {},
        },
    }
    files_found = 0

    for league in LEAGUES:
        path = SALARIES_RESULT_TMPL.format(slug=_league_slug(league))
        salaries = _load_result(path, logger)
        if not salaries:
            validation['warnings'].append(
                f"{league}: salaries results file {path} missing"
            )
            validation['summary']['leagues'][league] = {'salary_rows': 0}
            continue
        files_found += 1

        rows = salaries.get('rows', 0)
        fallback = salaries.get('fallback', False)
        validation['summary']['salary_rows'] += rows
        validation['summary']['unique_players'] += salaries.get(
            'players_with_rows', 0,
        )
        validation['summary']['leagues'][league] = {
            'salary_rows': rows,
            'fallback': fallback,
            'tables': salaries.get('tables', []),
        }

        errors: List[str] = list(salaries.get('errors', []) or [])
        validation['warnings'].extend(f"{league}: {e}" for e in errors)

        floor = LEAGUE_ROW_FLOORS.get(league, DEFAULT_ROW_FLOOR)
        if rows < floor:
            if fallback:
                validation['warnings'].append(
                    f"{league}: player_salaries CAPOLOGY_FALLBACK rows={rows}"
                )
            else:
                validation['warnings'].append(
                    f"{league}: low salary row count {rows} < {floor}"
                )

    if files_found == 0:
        raise AirflowException(
            f"No salaries results files found for any league in {LEAGUES}"
        )

    if validation['warnings']:
        validation['status'] = (
            'partial_success'
            if validation['summary']['salary_rows'] > 0 else 'failed'
        )

    logger.info("Validation: status=%s summary=%s", validation['status'], validation['summary'])
    if validation['warnings']:
        logger.warning("Warnings: %s", validation['warnings'])

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")
    return validation


with DAG(
    dag_id='dag_ingest_capology',
    default_args=SCRAPER_ARGS,
    description='Ingest Capology player salaries Bronze (issue #43)',
    schedule=SCHEDULES.get('dag_ingest_capology', '0 5 * * 1'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get(
        'capology',
        ['scraping', 'capology', 'bronze', 'football', 'salaries'],
    ),
    max_active_runs=1,
    # Fan-out is per (league, product); cap concurrent tasks so parallel
    # subprocesses can't burst-request Capology (>5 req/s trips CF).
    max_active_tasks=4,
    params={'leagues': LEAGUES, 'season': CURRENT_SEASON},
    doc_md=f"""
    ## Capology Data Ingestion (Issue #43)

    Weekly Bronze ingest of Capology salary snapshots. MVP currency: GBP.

    ### Architecture

    - One BashOperator per (league, product) runs the scraper in an
      isolated subprocess — fan-out over ``utils.config.LEAGUES``.
    - ``validate_data`` (PythonOperator, ``trigger_rule='all_done'``)
      summarises per-league row counts and flags soft CAPOLOGY_FALLBACK
      exits (row floor per league via ``LEAGUE_ROW_FLOORS``).

    ### Bronze table
    - ``iceberg.bronze.capology_player_salaries``
      partition: (league, season, currency); write semantics: replace.

    ### Notes
    - Capology ships the entire season roster in one HTML; rate-limit
      ≤10 req/min keeps us below the bursty CF flare threshold.
    - Default currency: {DEFAULT_CURRENCY}; EUR/USD are a followup.
    """,
) as dag:

    season = CURRENT_SEASON

    salaries_limit_arg = (
        f' --limit {SALARIES_DAILY_LIMIT}' if SALARIES_DAILY_LIMIT else ''
    )

    _TASK_ENV = {
        'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
        'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
        'HOME': '/home/airflow',
    }

    # Fan-out: one BashOperator per (league, product). Salaries + the three
    # club/contract products (issue #321); every league in utils.config
    # LEAGUES is scraped (previously only LEAGUES[0]). All tasks soft-fall
    # back on a CAPOLOGY_FALLBACK exit-code-2.
    PRODUCT_ENTITIES = [
        'team_payrolls', 'contract_extensions', 'transfer_window',
    ]
    scrape_tasks = []
    for _league in LEAGUES:
        _slug = _league_slug(_league)
        _salaries_result = SALARIES_RESULT_TMPL.format(slug=_slug)
        scrape_tasks.append(BashOperator(
            task_id=f'scrape_player_salaries_{_slug}',
            bash_command=f"""
cd /opt/airflow && \\
rm -f {_salaries_result} && \\
python dags/scripts/run_capology_scraper.py \\
    --entity player_salaries \\
    --league "{_league}" \\
    --season {season} \\
    --currency {DEFAULT_CURRENCY}{salaries_limit_arg} \\
    --output {_salaries_result}
rc=$?
if [ $rc -eq 2 ]; then
    echo "CAPOLOGY_FALLBACK exit-code 2 — propagating as soft success."
    exit 0
fi
exit $rc
""",
            env=_TASK_ENV,
            append_env=True,
        ))
        for _entity in PRODUCT_ENTITIES:
            _result = f'/tmp/capology_{_entity}_{_slug}_result.json'
            scrape_tasks.append(BashOperator(
                task_id=f'scrape_{_entity}_{_slug}',
                bash_command=f"""
cd /opt/airflow && \\
rm -f {_result} && \\
python dags/scripts/run_capology_scraper.py \\
    --entity {_entity} \\
    --league "{_league}" \\
    --season {season} \\
    --output {_result}
rc=$?
if [ $rc -eq 2 ]; then
    echo "CAPOLOGY_FALLBACK exit-code 2 — propagating as soft success."
    exit 0
fi
exit $rc
""",
                env=_TASK_ENV,
                append_env=True,
            ))

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    def _validate_bronze_quality(**ctx) -> None:
        """Trino-level CHECK gate over bronze.capology_player_salaries.

        One check set per league in LEAGUES (row floor from
        LEAGUE_ROW_FLOORS). row_count / no_duplicates are ERROR-severity
        (promoted after green weekly runs, issue #48); no_nulls / freshness
        stay WARNING so a CAPOLOGY_FALLBACK soft-exit doesn't hard-fail the
        gate.
        """
        from utils.data_quality import CHECK, run_checks

        season_short = (
            f"{str(CURRENT_SEASON)[2:4]}"
            f"{(int(str(CURRENT_SEASON)[2:4]) + 1) % 100:02d}"
        )
        checks = []
        for league in LEAGUES:
            where = (
                f"league = '{league}' AND season = '{season_short}' "
                f"AND currency = '{DEFAULT_CURRENCY}'"
            )
            checks += [
                CHECK.row_count(
                    'bronze.capology_player_salaries',
                    min_rows=LEAGUE_ROW_FLOORS.get(league, DEFAULT_ROW_FLOOR),
                    where=where, severity='ERROR',
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

    scrape_tasks >> validate_task \
        >> validate_bronze_quality_task >> trigger_silver_task
