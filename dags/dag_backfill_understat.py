"""On-demand source-native Understat historical backfill.

Runs once a day at 12:00 UTC (after the 09:00 daily DAG). The plan reads only
the durable manifest: one SQL query returns the oldest closed league-seasons
that have no complete attempt for the current contract. No site request and no
physical scope verification happen in the plan, so a day without history work
costs one query and never occupies the shared scraper pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import DAG_TAGS
from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils.understat_tasks import (
    RUNNER,
    scope_environment,
    validate_scope_result,
)


logger = logging.getLogger(__name__)

DAG_ID = "dag_backfill_understat"
BACKFILL_PRIORITY = 10
HISTORY_SCOPES_PER_RUN = 12


def plan_history_scope(**context: Any) -> list[dict[str, str]]:
    """Return up to ``HISTORY_SCOPES_PER_RUN`` oldest incomplete closed scopes."""

    from scrapers.understat.catalog import current_source_season_id
    from scrapers.understat.manifest import (
        CONTRACT_VERSION,
        UnderstatManifestRepository,
    )

    repository = UnderstatManifestRepository.from_env()
    scopes = repository.incomplete_closed_scopes(
        before_source_season_id=current_source_season_id(),
        limit=HISTORY_SCOPES_PER_RUN,
    )
    if not scopes:
        logger.info(
            "Understat history drained: no incomplete closed scope for %s",
            CONTRACT_VERSION,
        )
        return []

    run_id = str(context.get("run_id") or "manual")
    environments = [
        scope_environment(
            {**key.to_dict(), "discovered": True},
            mode="backfill",
            run_id=run_id,
        )
        for key in scopes
    ]
    for environment in environments:
        logger.info(
            "Understat history selected incomplete scope: %s/%s (%s)",
            environment["UNDERSTAT_LEAGUE"],
            environment["UNDERSTAT_SEASON_SLUG"],
            environment["UNDERSTAT_SOURCE_SEASON_ID"],
        )
    return environments


RUN_HISTORY_SCOPE_COMMAND = f"""
set -euo pipefail
cd /opt/airflow
/opt/legacy-scraper-venv/bin/python {RUNNER} \\
    --mode backfill \\
    --league "${{UNDERSTAT_LEAGUE}}" \\
    --season-slug "${{UNDERSTAT_SEASON_SLUG}}" \\
    --source-season-id "${{UNDERSTAT_SOURCE_SEASON_ID}}" \\
    --source-discovered "${{UNDERSTAT_SOURCE_DISCOVERED}}" \\
    --output "${{UNDERSTAT_RESULT_PATH}}"
"""


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Drain incomplete closed Understat league-seasons on demand",
    schedule="0 12 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=5),
    render_template_as_native_obj=True,
    tags=[*DAG_TAGS.get("understat", ["understat"]), "backfill"],
    doc_md="""
    ## Understat full-history backfill

    Paused on creation. Daily at 12:00 UTC the plan asks the manifest (one SQL,
    no site request) for the oldest closed league-seasons without a complete
    attempt for the current contract, up to 12 per run. No work: the mapped
    runner is skipped and the shared scraper pool is untouched. The daily DAG
    shares the same one-slot scraper pool with a higher priority.
    """,
) as dag:
    plan_scope = PythonOperator(
        task_id="plan_history_scope",
        python_callable=plan_history_scope,
        priority_weight=BACKFILL_PRIORITY,
        retries=1,
        execution_timeout=timedelta(minutes=5),
    )

    run_scope = BashOperator.partial(
        task_id="run_history_scope",
        bash_command=RUN_HISTORY_SCOPE_COMMAND,
        append_env=True,
        pool=INGEST_SCRAPER_POOL,
        priority_weight=BACKFILL_PRIORITY,
        execution_timeout=timedelta(hours=3),
    ).expand(env=plan_scope.output)

    validate_scope = PythonOperator.partial(
        task_id="validate_history_scope",
        python_callable=validate_scope_result,
        retries=0,
    ).expand(op_kwargs=plan_scope.output)

    plan_scope >> run_scope >> validate_scope


__all__ = [
    "dag",
    "plan_history_scope",
]
