"""Self-draining source-native Understat historical backfill.

Unpause this DAG to drain every closed league-season exposed by Understat.
Each continuous run selects the oldest scope that has no complete manifest for
the current contract, executes exactly that scope, validates its result, and
then cools down.  The durable manifest is the cursor, so scheduler restarts do
not lose progress and completed scopes are never re-scraped accidentally.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from utils.config import DAG_TAGS
from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils.understat_tasks import (
    RUNNER,
    _close_understat_client,
    _deduplicate_scopes,
    _load_result,
    _scope_value,
    scope_environment,
    validate_scope_result,
)


logger = logging.getLogger(__name__)

DAG_ID = "dag_backfill_understat"
BACKFILL_PRIORITY = 10
ACTIVE_COOLDOWN = timedelta(minutes=5)
IDLE_COOLDOWN = timedelta(minutes=30)
HISTORY_TASK_IDS = frozenset(
    {
        "plan_history_scope",
        "run_history_scope",
        "validate_history_scope",
        "finalize_history_run",
        "wait_before_next_continuous_run",
    }
)


def _scope_key(scope: Any) -> Any:
    """Build the exact manifest key without importing it at DAG parse time."""

    from scrapers.understat.manifest import ScopeKey

    return ScopeKey(
        league=str(_scope_value(scope, "league")),
        season=str(_scope_value(scope, "season")),
        source_league=str(_scope_value(scope, "source_league")),
        source_season_id=int(_scope_value(scope, "source_season_id")),
    )


def _contract_version() -> str:
    from scrapers.understat.manifest import CONTRACT_VERSION

    return str(CONTRACT_VERSION)


def plan_history_scope(**context: Any) -> list[dict[str, str]]:
    """Return zero or one oldest incomplete closed source scope."""

    from scrapers.understat import UnderstatCatalog, UnderstatClient
    from scrapers.understat.manifest import UnderstatManifestRepository

    client = UnderstatClient()
    try:
        discovered = UnderstatCatalog(client).discover_scopes(force_refresh=True)
    finally:
        _close_understat_client(client)
    closed = [
        scope
        for scope in _deduplicate_scopes(discovered)
        if bool(_scope_value(scope, "is_closed"))
    ]
    closed.sort(
        key=lambda scope: (
            int(_scope_value(scope, "source_season_id")),
            str(_scope_value(scope, "league")),
        )
    )

    repository = UnderstatManifestRepository.from_env()
    repository.ensure_table()
    contract_version = _contract_version()
    selected = next(
        (
            scope
            for scope in closed
            if not repository.is_scope_complete(
                _scope_key(scope),
                contract_version=contract_version,
            )
        ),
        None,
    )
    if selected is None:
        logger.info(
            "Understat history is drained: %d closed scope(s) complete for %s",
            len(closed),
            contract_version,
        )
        return []

    environment = scope_environment(
        selected,
        mode="backfill",
        run_id=str(context.get("run_id") or "manual"),
    )
    logger.info(
        "Understat history selected oldest incomplete scope: %s/%s (%s)",
        environment["UNDERSTAT_LEAGUE"],
        environment["UNDERSTAT_SEASON_SLUG"],
        environment["UNDERSTAT_SOURCE_SEASON_ID"],
    )
    return [environment]


def finalize_history_run(**context: Any) -> dict[str, Any]:
    """Record progress and choose the continuous-run cooldown."""

    planned = context["ti"].xcom_pull(task_ids="plan_history_scope") or []
    report: dict[str, Any] = {}
    did_work = False
    if planned:
        result_path = planned[0].get("UNDERSTAT_RESULT_PATH")
        try:
            report = _load_result(str(result_path))
            did_work = str(report.get("status") or "").casefold() == "complete"
        except AirflowException as exc:
            # The mapped runner/validator already carries the failed verdict.
            # The finalizer must still choose a safe idle cooldown rather than
            # hiding that original task failure with a second exception.
            logger.warning("Understat history result unavailable: %s", exc)

    target = datetime.now(timezone.utc) + (
        ACTIVE_COOLDOWN if did_work else IDLE_COOLDOWN
    )
    context["ti"].xcom_push(key="next_poll_at", value=target.isoformat())
    summary = {
        "did_work": did_work,
        "status": report.get("status", "idle"),
        "next_poll_at": target.isoformat(),
    }
    logger.info("Understat history run finalized: %s", summary)
    return summary


def history_poll_ready(**context: Any) -> bool:
    raw_target = context["ti"].xcom_pull(
        task_ids="finalize_history_run",
        key="next_poll_at",
    )
    if not raw_target:
        return True
    try:
        target = datetime.fromisoformat(str(raw_target)).astimezone(timezone.utc)
    except ValueError as exc:
        raise AirflowException("Understat backfill next-poll timestamp is invalid") from exc
    return datetime.now(timezone.utc) >= target


def propagate_history_status(**context: Any) -> dict[str, Any]:
    """Preserve upstream failures after the mandatory cooldown finishes.

    Finalization and cooldown intentionally use ``all_done`` so a source
    failure still backs off before ``@continuous`` retries it. This terminal
    watcher prevents the successful cooldown from hiding a failed mapped
    runner or validator as the DAG's only leaf.
    """

    dag_run = context.get("dag_run")
    if dag_run is None:
        raise AirflowException("Understat history watcher has no current DagRun")

    failures: list[str] = []
    observed = 0
    for task_instance in dag_run.get_task_instances():
        if task_instance.task_id not in HISTORY_TASK_IDS:
            continue
        observed += 1
        state = getattr(task_instance.state, "value", task_instance.state)
        normalized = str(state or "none").casefold().split(".")[-1]
        if normalized not in {"failed", "upstream_failed"}:
            continue
        map_index = getattr(task_instance, "map_index", -1)
        suffix = f"[{map_index}]" if map_index is not None and map_index >= 0 else ""
        failures.append(f"{task_instance.task_id}{suffix}={normalized}")

    if observed == 0:
        raise AirflowException(
            "Understat history watcher found no task instances for this DagRun"
        )
    if failures:
        raise AirflowException(
            "Understat history attempt failed before/while cooling down: "
            + "; ".join(sorted(failures))
        )
    logger.info(
        "Understat history terminal watcher accepted %d task instance(s)",
        observed,
    )
    return {"status": "success", "observed_task_instances": observed}


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
    description="Self-drain all closed Understat league-season scopes",
    schedule="@continuous",
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

    Paused on creation. Unpause to drain every closed source-discovered season
    oldest-first. One continuous run publishes at most one league-season and
    the current contract manifest is the durable resume cursor. The daily DAG
    shares the same one-slot scraper pool with a higher priority.
    """,
) as dag:
    plan_scope = PythonOperator(
        task_id="plan_history_scope",
        python_callable=plan_history_scope,
        pool=INGEST_SCRAPER_POOL,
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

    finalize = PythonOperator(
        task_id="finalize_history_run",
        python_callable=finalize_history_run,
        trigger_rule="all_done",
        retries=0,
    )

    cooldown = PythonSensor(
        task_id="wait_before_next_continuous_run",
        python_callable=history_poll_ready,
        mode="reschedule",
        poke_interval=60,
        timeout=int(IDLE_COOLDOWN.total_seconds()) + 600,
        trigger_rule="all_done",
        retries=0,
    )

    propagate_status = PythonOperator(
        task_id="propagate_history_status",
        python_callable=propagate_history_status,
        trigger_rule="all_done",
        retries=0,
    )

    (
        plan_scope
        >> run_scope
        >> validate_scope
        >> finalize
        >> cooldown
        >> propagate_status
    )


__all__ = [
    "dag",
    "finalize_history_run",
    "history_poll_ready",
    "plan_history_scope",
    "propagate_history_status",
]
