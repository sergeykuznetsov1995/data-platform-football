"""Daily source-native Understat ingestion.

The DAG owns the single 09:00 UTC current-data schedule.  Source discovery is
performed at task runtime and returns the rolling two-season source window
plus the next season when Understat starts publishing it.  Each discovered
``(league, season)`` scope is executed and validated independently; one
subprocess can therefore never hide another league's empty or partial result.

Historical seasons are deliberately not accepted as a UI override here.  The
paused, self-draining ``dag_backfill_understat`` owns history and resumes from
the durable Understat publication manifest.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import DAG_TAGS, SCHEDULES, UNDERSTAT_LEAGUES
from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils.understat_tasks import (
    _close_understat_client,
    _deduplicate_scopes,
    _scope_value,
    scope_environment,
    validate_scope_result,
)


logger = logging.getLogger(__name__)

DAG_ID = "dag_ingest_understat"
CURRENT_PRIORITY = 100


def plan_current_scopes(**context: Any) -> list[dict[str, str]]:
    """Discover the bounded current window and prepare mapped runner inputs."""

    # Lazy import is intentional: DAG parsing must not open a source session or
    # require the scraper-only dependency set in the scheduler process.
    from scrapers.understat import UnderstatCatalog, UnderstatClient

    client = UnderstatClient()
    try:
        scopes = UnderstatCatalog(client).rolling_scopes(
            window=2,
            probe_next=True,
        )
    finally:
        _close_understat_client(client)
    configured = frozenset(UNDERSTAT_LEAGUES)
    scopes = _deduplicate_scopes(
        scope
        for scope in scopes
        if str(_scope_value(scope, "league")) in configured
    )
    if not scopes:
        raise AirflowException("Understat current discovery returned no scopes")

    run_id = str(context.get("run_id") or "manual")
    plan = [
        scope_environment(
            scope,
            mode="current",
            run_id=run_id,
        )
        for scope in scopes
    ]
    logger.info(
        "Understat current plan contains %d scope(s): %s",
        len(plan),
        [
            (item["UNDERSTAT_LEAGUE"], item["UNDERSTAT_SEASON_SLUG"])
            for item in plan
        ],
    )
    return plan


RUN_SCOPE_COMMAND = """
set -euo pipefail
cd /opt/airflow
/opt/legacy-scraper-venv/bin/python dags/scripts/run_understat_scraper.py \\
    --mode "${UNDERSTAT_MODE}" \\
    --league "${UNDERSTAT_LEAGUE}" \\
    --season-slug "${UNDERSTAT_SEASON_SLUG}" \\
    --source-season-id "${UNDERSTAT_SOURCE_SEASON_ID}" \\
    --source-discovered "${UNDERSTAT_SOURCE_DISCOVERED}" \\
    --output "${UNDERSTAT_RESULT_PATH}"
"""


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Discover and ingest the rolling Understat current window",
    schedule=SCHEDULES.get(DAG_ID, "0 9 * * *"),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=12),
    render_template_as_native_obj=True,
    tags=DAG_TAGS.get("understat", ["scraping", "understat", "bronze"]),
    doc_md="""
    ## Understat current ingestion

    The source catalog is discovered at runtime. One dynamically mapped task
    ingests one exact league-season scope through the shared single-slot
    scraper pool. The rolling two-season window automatically admits a new
    season when Understat starts publishing it; expected pre-publication is an
    explicit result state, never an empty successful partition.
    """,
) as dag:
    plan_scopes = PythonOperator(
        task_id="plan_current_scopes",
        python_callable=plan_current_scopes,
        op_kwargs={
            "run_id": "{{ run_id }}",
        },
        pool=INGEST_SCRAPER_POOL,
        priority_weight=CURRENT_PRIORITY,
        retries=1,
        execution_timeout=timedelta(minutes=5),
    )

    run_scope = BashOperator.partial(
        task_id="run_current_scope",
        bash_command=RUN_SCOPE_COMMAND,
        append_env=True,
        pool=INGEST_SCRAPER_POOL,
        priority_weight=CURRENT_PRIORITY,
        execution_timeout=timedelta(hours=3),
    ).expand(env=plan_scopes.output)

    validate_scope = PythonOperator.partial(
        task_id="validate_current_scope",
        python_callable=validate_scope_result,
        retries=0,
    ).expand(op_kwargs=plan_scopes.output)

    plan_scopes >> run_scope >> validate_scope


__all__ = [
    "dag",
    "plan_current_scopes",
    "scope_environment",
    "validate_scope_result",
]
