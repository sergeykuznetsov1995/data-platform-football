"""
SofaScore Per-Player Ingestion DAG (weekly)
==========================================

Issue #751 PR3. Captures the per-player **profile** (biographical snapshot) from
SofaScore in ONE Camoufox navigation per player:

  - ``bronze.sofascore_player_profile`` — bio SSR'd in ``__NEXT_DATA__``

Season-aggregate stats (``sofascore_player_season_stats``) are deferred to PR3b:
for transferred/multi-competition players the player page's default Season tab is
a non-EPL competition, so a season-picker must be driven — see
``memory/feedback_sofascore_player_page_capture``.

Why a SEPARATE weekly DAG (not the daily ``dag_ingest_sofascore``): the player
universe is ~526 players, each a full browser navigation behind a residential
proxy, so a run is hours long and far too heavy for the 11GB VM to carry daily.
Profiles change rarely, so weekly cadence is plenty. Player ids are resolved from
``bronze.sofascore_player_ratings`` (written by the daily ``match_capture``), so
this DAG runs independently after that table is populated.

The table is written full-state (``replace_partitions=['league', 'season']`` +
completeness guard). Exit 2 = graceful R0.2B_FALLBACK (page didn't render / proxy
dead) → soft success so ``validate_data`` still runs; exit 3 = completeness-guard
refusal → real failure.
"""

import os
from datetime import datetime
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import LEAGUES, CURRENT_SEASON, SCHEDULES, DAG_TAGS
from utils.default_args import SCRAPER_ARGS


PLAYER_CAPTURE_RESULT_PATH = '/tmp/sofascore_player_capture_result.json'


def _env_int(name: str):
    """Read a positive int from ENV; empty/unparseable/non-positive → None."""
    raw = os.environ.get(name, '').strip()
    if not raw:
        return None
    try:
        v = int(raw)
    except ValueError:
        return None
    return v if v > 0 else None


# Smoke/dev cap — None = full coverage (all ~526 players). Issue #69 convention.
PLAYER_CAPTURE_LIMIT = _env_int('SS_PLAYER_CAPTURE_LIMIT')


def _limit_arg(limit) -> str:
    return f"--limit {int(limit)}" if limit else ""


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
    """Row-floor + fallback validation for the consolidated player capture."""
    import logging

    logger = logging.getLogger(__name__)
    result = _load_result(PLAYER_CAPTURE_RESULT_PATH, logger)

    if not result:
        raise AirflowException(
            f"player_capture results file {PLAYER_CAPTURE_RESULT_PATH} "
            f"missing or unreadable"
        )

    validation = {
        'status': 'success',
        'warnings': [],
        'summary': {
            'player_profile_rows': result.get('rows', 0),
            'player_profile_players': result.get('profile_players', 0),
            'fallback': result.get('fallback', False),
            'tables': result.get('tables', []),
        },
    }

    errors: List[str] = result.get('errors', []) or []
    if errors:
        validation['warnings'] = list(errors)
        total_rows = validation['summary']['player_profile_rows']
        validation['status'] = 'partial_success' if total_rows > 0 else 'failed'

    # APL ≈ 526 active players → 1 profile row each. WARN-only floor = 400 (issue
    # #69); a fallback keeps the DAG non-failed (soft).
    rows = validation['summary']['player_profile_rows']
    if rows < 400:
        if validation['summary']['fallback']:
            validation['warnings'].append(
                f"player_profile R0.2B_FALLBACK: rows={rows} "
                f"players={validation['summary']['player_profile_players']}"
            )
            if validation['status'] == 'success':
                validation['status'] = 'partial_success'
        else:
            validation['warnings'].append(
                f"Low player_profile row count: {rows} < 400")

    logger.info("Data validation complete: %s", validation['status'])
    logger.info("Summary: %s", validation['summary'])
    if validation['warnings']:
        logger.warning("Warnings: %s", validation['warnings'])

    if validation['status'] == 'failed':
        raise AirflowException(f"Validation failed: {validation.get('warnings', [])}")
    return validation


def validate_bronze_freshness(**context) -> None:
    """Telegram-alert when the player bronze tables stop refreshing (#751).

    The scrape task soft-exits (R0.2B_FALLBACK, exit 2) when SofaScore's anti-bot
    returns 403, so the DAG stays green while data silently goes stale. A direct
    MAX(_ingested_at) check surfaces a multi-week stall. WARNING-severity (not
    ERROR) for now — promote after the capture path yields green weekly runs
    (PR4). 8-day window gives one missed weekly run of grace.
    """
    import logging

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CHECK, run_checks

    logger = logging.getLogger(__name__)

    checks = [
        CHECK.freshness(
            'bronze.sofascore_player_profile',
            ts_col='_ingested_at', max_age_hours=192, severity='WARNING',
        ),
    ]
    report = run_checks(checks, raise_on_error=False)
    logger.info("validate_bronze_freshness: %s", report.summary())
    telegram_dq_summary(report, header='SofaScore player Bronze freshness')


with DAG(
    dag_id='dag_ingest_sofascore_players',
    default_args=SCRAPER_ARGS,
    description='Weekly per-player SofaScore capture (profile + season stats)',
    schedule=SCHEDULES.get('dag_ingest_sofascore_players', '0 15 * * 6'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('sofascore', ['scraping', 'sofascore', 'bronze']),
    max_active_runs=1,
    params={
        'leagues': LEAGUES,
        'season': CURRENT_SEASON,
    },
    doc_md="""
    ## SofaScore Per-Player Ingestion (weekly)

    ONE Camoufox navigation per player → ``bronze.sofascore_player_profile``
    (bio from ``__NEXT_DATA__``). Replaces the dead tls pass (Turnstile-blocked,
    #751). Season-aggregate stats are deferred to PR3b (need a season-picker).

    ### Why weekly + separate DAG

    ~526 player navigations/run (hours long, residential proxy) — too heavy for
    the daily ingest on the 11GB VM. Profiles move slowly.

    ### Dependency

    Player ids come from ``bronze.sofascore_player_ratings`` (written by the daily
    ``match_capture`` task) — keep the daily DAG healthy for full coverage.

    ### Dev cap

    `SS_PLAYER_CAPTURE_LIMIT` (positive int) caps the number of players (smoke).
    """,
) as dag:

    leagues_str = ','.join(LEAGUES)

    scrape_player_capture_task = BashOperator(
        task_id='scrape_player_capture',
        bash_command=f"""
cd /opt/airflow && \\
rm -f {PLAYER_CAPTURE_RESULT_PATH} && \\
python dags/scripts/run_sofascore_scraper.py \\
    --entity player_capture \\
    --league "{LEAGUES[0]}" \\
    --season {CURRENT_SEASON} \\
    {_limit_arg(PLAYER_CAPTURE_LIMIT)} \\
    --output {PLAYER_CAPTURE_RESULT_PATH}
rc=$?
if [ $rc -eq 2 ]; then
    echo "R0.2B_FALLBACK exit-code 2 (player_capture) — propagating as soft success."
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

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule='all_done',
    )

    validate_bronze_freshness_task = PythonOperator(
        task_id='validate_bronze_freshness',
        python_callable=validate_bronze_freshness,
        trigger_rule='all_done',
    )

    scrape_player_capture_task >> validate_data_task >> validate_bronze_freshness_task
