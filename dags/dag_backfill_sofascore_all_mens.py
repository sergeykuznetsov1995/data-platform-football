"""Continuous breadth-first SofaScore history for the frozen all-men scope."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from scrapers.sofascore.workload_plan import load_verified_workload_policy

from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils import sofascore_all_mens_state as state


DAG_ID = "dag_backfill_sofascore_all_mens"
SNAPSHOT_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_SNAPSHOT",
    "/opt/airflow/runtime/sofascore/all-men/snapshot.json",
)
POLICY_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_POLICY",
    "/opt/airflow/configs/sofascore/all_mens_campaign.json",
)
STATE_PATH = os.environ.get(
    "SOFASCORE_ALL_MENS_STATE",
    "/opt/airflow/logs/sofascore-all-men/state.json",
)
RESULT_DIR = os.environ.get(
    "SOFASCORE_ALL_MENS_RESULT_DIR",
    "/opt/airflow/logs/sofascore-all-men/results",
)
WORKLOAD_ARTIFACT = os.environ.get(
    "SOFASCORE_PROXY_BUDGET_ARTIFACT",
    "/opt/airflow/runtime/sofascore/proxy_budget_canary.json",
)
ACTIVE_COOLDOWN = timedelta(minutes=1)
IDLE_COOLDOWN = timedelta(minutes=30)
HISTORY_BLACKOUT_START_HOUR_UTC = 8
HISTORY_BLACKOUT_END_HOUR_UTC = 17
HISTORY_TASK_IDS = frozenset({
    "plan_historical_batch",
    "run_historical_scope",
    "validate_historical_scope",
    "finalize_historical_run",
    "wait_before_next_continuous_run",
})


def _history_start_allowed(
    *, now: datetime, production_active: bool
) -> bool:
    if production_active:
        return False
    hour = now.astimezone(timezone.utc).hour
    return not (
        HISTORY_BLACKOUT_START_HOUR_UTC
        <= hour
        < HISTORY_BLACKOUT_END_HOUR_UTC
    )


def _production_dag_active() -> bool:
    """Check at task runtime; no Airflow metadata query happens at import."""

    from airflow.models.dagrun import DagRun
    from airflow.utils.session import create_session

    with create_session() as session:
        return bool(session.query(DagRun).filter(
            DagRun.dag_id == "dag_ingest_sofascore",
            DagRun.state.in_(("queued", "running")),
        ).count())


def _plan_historical_batch(**context: Any) -> list[dict[str, str]]:
    if not _history_start_allowed(
        now=datetime.now(timezone.utc),
        production_active=_production_dag_active(),
    ):
        return []
    snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
    campaign_id = str(snapshot.get("campaign_id") or "")
    completed = state.read_completed(STATE_PATH, campaign_id=campaign_id)
    workload_policy = load_verified_workload_policy(WORKLOAD_ARTIFACT)
    return state.plan_historical_batch(
        snapshot,
        completed=completed,
        batch_size=1,
        snapshot_path=SNAPSHOT_PATH,
        policy_path=POLICY_PATH,
        result_dir=RESULT_DIR,
        workload_artifact=WORKLOAD_ARTIFACT,
        dag_run_id=str(context.get("run_id") or "manual"),
        authorized_season_classes={
            name: budget.measured_tournament_ids
            for name, budget in workload_policy.classes.items()
            if budget.scope == "season"
        },
    )


def _validate_historical_scope(**environment: str) -> dict[str, Any]:
    result_path = Path(environment["SOFASCORE_SCOPE_RESULT_PATH"])
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f"scope result is unreadable: {exc}") from exc
    if result.get("status") != "success":
        raise AirflowException("scope cycle did not finish successfully")
    action = environment.get("SOFASCORE_CAMPAIGN_ACTION")
    if action == "metadata":
        snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
        if (
            snapshot.get("campaign_id")
            != environment["SOFASCORE_EXPECTED_CAMPAIGN_ID"]
            or result.get("campaign_id") != snapshot.get("campaign_id")
            or result.get("snapshot_id") != snapshot.get("snapshot_id")
        ):
            raise AirflowException("metadata result provenance mismatch")
        return {
            "status": "metadata_complete",
            "wave": environment["SOFASCORE_METADATA_WAVE"],
            "snapshot_id": result["snapshot_id"],
        }
    if action != "capture":
        raise AirflowException("unknown SofaScore campaign action")
    campaign_id, tournament_id, season_id = environment[
        "SOFASCORE_SCOPE_KEY"
    ].split(":")
    if (
        result.get("snapshot_id") != environment["SOFASCORE_EXPECTED_SNAPSHOT_ID"]
        or result.get("campaign_id") != campaign_id
        or int(result.get("tournament_id", 0)) != int(tournament_id)
        or int(result.get("source_season_id", 0)) != int(season_id)
    ):
        raise AirflowException("scope result provenance mismatch")
    state.mark_completed(
        STATE_PATH,
        campaign_id=campaign_id,
        scope_key=environment["SOFASCORE_SCOPE_KEY"],
    )
    return {"status": "complete", "scope_key": environment["SOFASCORE_SCOPE_KEY"]}


def _finalize_historical_run(**context: Any) -> dict[str, Any]:
    planned = context["ti"].xcom_pull(task_ids="plan_historical_batch") or []
    did_work = bool(planned)
    target = datetime.now(timezone.utc) + (
        ACTIVE_COOLDOWN if did_work else IDLE_COOLDOWN
    )
    context["ti"].xcom_push(key="next_poll_at", value=target.isoformat())
    return {"did_work": did_work, "next_poll_at": target.isoformat()}


def _poll_ready(**context: Any) -> bool:
    raw = context["ti"].xcom_pull(
        task_ids="finalize_historical_run", key="next_poll_at"
    )
    if not raw:
        return True
    try:
        target = datetime.fromisoformat(str(raw)).astimezone(timezone.utc)
    except ValueError as exc:
        raise AirflowException("campaign next-poll timestamp is invalid") from exc
    return datetime.now(timezone.utc) >= target


def _propagate_status(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise AirflowException("campaign watcher has no DagRun")
    failures = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.task_id not in HISTORY_TASK_IDS:
            continue
        value = getattr(task_instance.state, "value", task_instance.state)
        normalized = str(value or "none").casefold().split(".")[-1]
        if normalized in {"failed", "upstream_failed"}:
            failures.append(task_instance.task_id)
    if failures:
        raise AirflowException(
            "SofaScore history attempt failed: " + ", ".join(sorted(failures))
        )
    return {"status": "success"}


RUN_SCOPE_COMMAND = """
set -euo pipefail
cd /opt/airflow
case "${SOFASCORE_CAMPAIGN_ACTION}" in
  metadata)
    /opt/legacy-scraper-venv/bin/python \
      scripts/enrich_sofascore_all_mens_snapshot.py \
      --snapshot "${SOFASCORE_CAMPAIGN_SNAPSHOT}" \
      --policy "${SOFASCORE_ALL_MENS_POLICY}" \
      --output "${SOFASCORE_CAMPAIGN_SNAPSHOT}" \
      --report "${SOFASCORE_SCOPE_RESULT_PATH}" \
      --expected-snapshot-id "${SOFASCORE_EXPECTED_SNAPSHOT_ID}" \
      --dag-id "${AIRFLOW_CTX_DAG_ID}" \
      --run-id "${AIRFLOW_CTX_DAG_RUN_ID}" \
      --task-id "${AIRFLOW_CTX_TASK_ID}" \
      --wave-start-year "${SOFASCORE_METADATA_WAVE}" \
      --budget-cap-bytes "${SOFASCORE_METADATA_BUDGET_BYTES}"
    ;;
  capture)
    /opt/legacy-scraper-venv/bin/python \
      dags/scripts/run_sofascore_scope_cycle.py \
      --snapshot "${SOFASCORE_CAMPAIGN_SNAPSHOT}" \
      --tournament-id "${SOFASCORE_TOURNAMENT_ID}" \
      --source-season-id "${SOFASCORE_SOURCE_SEASON_ID}" \
      --expected-snapshot-id "${SOFASCORE_EXPECTED_SNAPSHOT_ID}" \
      --expected-campaign-id "${SOFASCORE_EXPECTED_CAMPAIGN_ID}" \
      --phase all \
      --output-dir "${SOFASCORE_SCOPE_OUTPUT_DIR}" \
      --output "${SOFASCORE_SCOPE_RESULT_PATH}" \
      --workload-artifact "${SOFASCORE_WORKLOAD_ARTIFACT}" \
      --run-id "${SOFASCORE_SCOPE_RUN_ID}"
    ;;
  *)
    echo "unknown SofaScore campaign action" >&2
    exit 64
    ;;
esac
"""


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="All adult-men SofaScore history, newest season across all first",
    schedule="@continuous",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=6),
    render_template_as_native_obj=True,
    tags=["sofascore", "backfill", "all-men"],
) as dag:
    plan = PythonOperator(
        task_id="plan_historical_batch",
        python_callable=_plan_historical_batch,
        retries=0,
    )
    run = BashOperator.partial(
        task_id="run_historical_scope",
        bash_command=RUN_SCOPE_COMMAND,
        append_env=True,
        pool=INGEST_SCRAPER_POOL,
        priority_weight=1,
        do_xcom_push=False,
        max_active_tis_per_dag=1,
        retries=0,
        execution_timeout=timedelta(hours=4),
    ).expand(env=plan.output)
    validate = PythonOperator.partial(
        task_id="validate_historical_scope",
        python_callable=_validate_historical_scope,
        retries=0,
    ).expand(op_kwargs=plan.output)
    finalize = PythonOperator(
        task_id="finalize_historical_run",
        python_callable=_finalize_historical_run,
        trigger_rule="all_done",
        retries=0,
    )
    cooldown = PythonSensor(
        task_id="wait_before_next_continuous_run",
        python_callable=_poll_ready,
        mode="reschedule",
        poke_interval=60,
        timeout=int(IDLE_COOLDOWN.total_seconds()) + 600,
        trigger_rule="all_done",
        retries=0,
    )
    propagate = PythonOperator(
        task_id="propagate_historical_status",
        python_callable=_propagate_status,
        trigger_rule="all_done",
        retries=0,
    )

    plan >> run >> validate
    [plan, validate] >> finalize >> cooldown >> propagate


__all__ = ["DAG_ID", "dag"]
