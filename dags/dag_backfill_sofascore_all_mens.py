"""Continuous breadth-first SofaScore history for the frozen all-men scope."""

from __future__ import annotations

import json
import hashlib
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from scrapers.sofascore.workload_plan import load_static_workload_policy
from scrapers.sofascore import history_controller, history_inventory

from utils.default_args import DEFAULT_ARGS
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
FAILURES_PATH = str(Path(STATE_PATH).with_name("failures.json"))
CONTROLLER_PATH = str(Path(STATE_PATH).with_name("history-controller.json"))
REGISTRY_PATH = str(Path(__file__).resolve().parents[1] / "configs/sofascore/tournaments.json")
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


# History uses three independently refilled workers on its dedicated pool.
# The legacy batch knob stays valid for older callers; it does not cap the queue.
# An explicit smaller max-active value remains a safety restriction.
HISTORY_BATCH_SIZE = state.env_int("SOFASCORE_HISTORY_BATCH_SIZE", 1, 1, 64)
HISTORY_POOL = (
    os.environ.get("SOFASCORE_HISTORY_POOL", "").strip() or "sofascore_history_pool"
)
HISTORY_MAX_ACTIVE_TASKS = state.env_int("SOFASCORE_HISTORY_MAX_ACTIVE_TASKS", 3, 1, 16)
HISTORY_FIRST_START_YEAR = state.env_int(
    "SOFASCORE_HISTORY_FIRST_START_YEAR", state.DEFAULT_FIRST_START_YEAR, 2000, 2100
)
# A scope that failed this many DagRuns is parked (failures.json) instead of
# retrying at the head of the queue forever; a validated success clears it.
HISTORY_MAX_SCOPE_ATTEMPTS = state.env_int(
    "SOFASCORE_HISTORY_MAX_SCOPE_ATTEMPTS", state.DEFAULT_MAX_SCOPE_ATTEMPTS, 1, 100
)
# Forwarded into every planned task only when set: the scope cycle reads
# SOFASCORE_PROXY_CONTROL_URL for its gateway and SOFASCORE_RATE_LIMIT_PER_MINUTE
# for its source rate limit.
HISTORY_TASK_ENV: dict[str, str] = {}
_rate_limit = state.env_int("SOFASCORE_HISTORY_RATE_LIMIT_PER_MINUTE", None, 1, 60)
if _rate_limit is not None:
    HISTORY_TASK_ENV["SOFASCORE_RATE_LIMIT_PER_MINUTE"] = str(_rate_limit)
_control_url = os.environ.get("SOFASCORE_HISTORY_PROXY_CONTROL_URL", "").strip()
if _control_url:
    HISTORY_TASK_ENV["SOFASCORE_PROXY_CONTROL_URL"] = _control_url
HISTORY_TASK_IDS = frozenset({
    "plan_historical_batch",
    "run_historical_scope",
    "validate_historical_scope",
    "finalize_historical_run",
    "wait_before_next_continuous_run",
})


def _recover_history_reservation(snapshot, run_id):
    if not Path(CONTROLLER_PATH).exists():
        return
    report = history_controller.read_summary(CONTROLLER_PATH, snapshot["campaign_id"])
    previous = report.get("run")
    if not previous or previous["finalized"] or previous["run_id"] == run_id:
        return
    # A timed-out DagRun may have lost its finalizer. Reconcile its accounting
    # only after Airflow confirms it terminal; a restart cannot steal work
    # from a still-running reservation.
    from airflow.models.dagrun import DagRun
    from airflow.utils.session import create_session
    with create_session() as session:
        old = session.query(DagRun).filter(
            DagRun.dag_id == DAG_ID, DagRun.run_id == previous["run_id"],
        ).one_or_none()
        if old is None or _task_state(old) not in {"success", "failed"}:
            raise AirflowException("previous history reservation has no terminal DagRun")
        class RecoveredTI:
            def xcom_pull(self, **kwargs):
                return previous["plan"]
            def xcom_push(self, **kwargs):
                pass
        _finalize_historical_run(ti=RecoveredTI(), dag_run=old, run_id=previous["run_id"])


def _plan_historical_batch(**context: Any) -> list[dict[str, str]]:
    snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
    campaign_id = str(snapshot.get("campaign_id") or "")
    _recover_history_reservation(snapshot, str(context.get("run_id") or "manual"))
    completed = state.read_completed(STATE_PATH, campaign_id=campaign_id)
    failures = state.read_failures(FAILURES_PATH, campaign_id=campaign_id)
    workload_policy = load_static_workload_policy(WORKLOAD_ARTIFACT)
    inventory = history_inventory.collect(
        snapshot, result_dir=RESULT_DIR, checkpoint_path=CONTROLLER_PATH,
        registry_path=REGISTRY_PATH,
    )
    planned = state.plan_historical_batch(
        snapshot,
        history_inventory=inventory, controller_path=CONTROLLER_PATH,
        history_slots=min(3, HISTORY_MAX_ACTIVE_TASKS),
        history_deadline=(context["dag_run"].start_date + timedelta(hours=6)).isoformat()
        if context.get("dag_run") and context["dag_run"].start_date else None,
        completed=completed,
        failures=failures,
        max_scope_attempts=HISTORY_MAX_SCOPE_ATTEMPTS,
        batch_size=HISTORY_BATCH_SIZE,
        first_start_year=HISTORY_FIRST_START_YEAR,
        snapshot_path=SNAPSHOT_PATH,
        policy_path=POLICY_PATH,
        result_dir=RESULT_DIR,
        workload_artifact=WORKLOAD_ARTIFACT,
        dag_run_id=str(context.get("run_id") or "manual"),
        authorized_season_classes=[
            name
            for name, budget in workload_policy.classes.items()
            if budget.scope == "season"
        ],
        task_env={**HISTORY_TASK_ENV, "SOFASCORE_HISTORY_STATE": STATE_PATH,
                  "SOFASCORE_HISTORY_FAILURES": FAILURES_PATH,
                  "SOFASCORE_HISTORY_POOL": HISTORY_POOL},
    )
    if context.get("ti") is not None:
        report = history_controller.read_summary(CONTROLLER_PATH, campaign_id)
        context["ti"].xcom_push(key="history_mode", value="slots")
        context["ti"].xcom_push(key="history_groups", value=report["groups"])
        context["ti"].xcom_push(key="history_summary", value=report["summary"])
    return planned


def _validate_historical_scope(**environment: str) -> dict[str, Any]:
    if "SOFASCORE_HISTORY_SLOT" in environment:
        run = history_controller.read_summary(
            environment["SOFASCORE_HISTORY_CONTROLLER"],
            environment["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
        )["run"]
        slot = environment["SOFASCORE_HISTORY_SLOT"]
        if (run.get("mode") != "slots" or run["run_id"] != environment["SOFASCORE_HISTORY_RUN_ID"]
                or run["slots"].get(slot) is not None
                or any(not item["accounted"] for item in run["items"].values() if item["slot"] == slot)):
            raise AirflowException("history slot accounting incomplete")
        return {"status": "slot_accounted", "slot": slot}
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
    # The result's snapshot_id is the revision the scope actually ran on; the
    # refresh lane's metadata enrichment may have advanced it after planning
    # (#1354), which leaves the campaign identity and this exact scope intact.
    if (
        result.get("campaign_id") != campaign_id
        or int(result.get("tournament_id", 0)) != int(tournament_id)
        or int(result.get("source_season_id", 0)) != int(season_id)
    ):
        raise AirflowException("scope result provenance mismatch")
    if environment.get("SOFASCORE_HISTORY_GROUP") != "july":
        state.mark_completed(
            STATE_PATH, campaign_id=campaign_id,
            scope_key=environment["SOFASCORE_SCOPE_KEY"],
        )
    state.clear_failed(
        FAILURES_PATH,
        campaign_id=campaign_id,
        scope_key=environment["SOFASCORE_SCOPE_KEY"],
    )
    return {"status": "complete", "scope_key": environment["SOFASCORE_SCOPE_KEY"]}


def _task_state(task_instance: Any) -> str:
    value = getattr(task_instance.state, "value", task_instance.state)
    return str(value or "none").casefold().split(".")[-1]


def _finalize_historical_run(**context: Any) -> dict[str, Any]:
    planned = context["ti"].xcom_pull(task_ids="plan_historical_batch") or []
    dag_run = context.get("dag_run")
    release = state.current_release()
    if Path(CONTROLLER_PATH).exists():
        snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
        report = history_controller.read_summary(CONTROLLER_PATH, snapshot["campaign_id"])
        reservation = report.get("run")
        if reservation and reservation.get("mode") == "slots" and reservation["run_id"] == str(context.get("run_id") or "manual"):
            return _finalize_slots(reservation, snapshot, context, release)
    for index, environment in enumerate(planned):
        scope_key = environment.get("SOFASCORE_SCOPE_KEY")
        if not scope_key or dag_run is None:
            continue
        # BOTH mapped tasks, not just the bash one.  ``validate`` expands over
        # the same plan, so the map indexes line up.  A scope whose bash step
        # succeeded but whose validation failed (provenance mismatch, result
        # JSON absent or unreadable) was neither completed NOR failed: it came
        # back first in the next @continuous run, was paid for in full again,
        # failed validation again, and ``HISTORY_MAX_SCOPE_ATTEMPTS`` never grew
        # — an unbounded paid loop on one broken scope (code review of PR
        # #1216).
        states = set()
        for task_id in ("run_historical_scope", "validate_historical_scope"):
            task_instance = dag_run.get_task_instance(task_id, map_index=index)
            if task_instance is not None:
                states.add(_task_state(task_instance))
        if not states & {"failed", "upstream_failed"}:
            if states == {"success"}:
                # #1352: a green scope that journaled rejects is replayed
                # from raw once per new release (rejects_await_release).
                state.mark_completed_rejects(
                    FAILURES_PATH,
                    campaign_id=environment["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
                    scope_key=scope_key,
                    rejected_endpoints=state.read_scope_rejects(
                        environment.get("SOFASCORE_SCOPE_RESULT_PATH")
                    ),
                    run_id=str(context.get("run_id") or "manual"),
                    release=release,
                )
            continue
        reason, source_requests = state.read_scope_outcome(
            environment.get("SOFASCORE_SCOPE_RESULT_PATH")
        )
        state.mark_failed(
            FAILURES_PATH,
            campaign_id=environment["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
            scope_key=scope_key,
            run_id=str(context.get("run_id") or "manual"),
            reason=reason,
            source_requests=source_requests,
            release=release,
            season_identity=environment.get("SOFASCORE_SEASON_ALIGNMENT_IDENTITY"),
        )
    checkpoint = Path(CONTROLLER_PATH)
    if checkpoint.exists():
        snapshot = state.read_snapshot(SNAPSHOT_PATH, policy_path=POLICY_PATH)
        history_controller.finalize(
            checkpoint, campaign_id=snapshot["campaign_id"],
            run_id=str(context.get("run_id") or "manual"),
        )
    did_work = bool(planned)
    target = datetime.now(timezone.utc) + (
        ACTIVE_COOLDOWN if did_work else IDLE_COOLDOWN
    )
    context["ti"].xcom_push(key="next_poll_at", value=target.isoformat())
    return {"did_work": did_work, "next_poll_at": target.isoformat()}


def _finalize_slots(reservation, snapshot, context, release):
    from scrapers.sofascore.history_worker import stamp_result
    campaign, run_id = snapshot["campaign_id"], reservation["run_id"]
    for index, item in reservation["items"].items():
        if item["accounted"]:
            continue
        # Called only by all_done finalizer, or terminal-DagRun recovery.
        dag_run = context.get("dag_run")
        ti = dag_run.get_task_instance("run_historical_scope", map_index=int(item["slot"])) if dag_run else None
        if ti is None or _task_state(ti) not in {"success", "failed", "upstream_failed", "skipped", "removed"}:
            raise AirflowException("history slot is not terminal; cannot reconcile")
        environment = reservation["plan"][item["plan_index"]]
        outcome = item["outcome"] or {"status": "not_started" if item["attempts"] == 0 else "failed",
                                    "reason": "history worker interrupted before accounting",
                                    "source_requests": 0 if item["attempts"] == 0 else None}
        history_controller.record_scope(CONTROLLER_PATH, campaign_id=campaign, run_id=run_id,
                                        slot=item["slot"], index=int(index), outcome=outcome)
        current = history_controller.read_summary(CONTROLLER_PATH, campaign)["run"]["items"][index]
        stamp_result(environment, current, outcome, run_id)
        history_controller.account_scope(environment, outcome, state_path=STATE_PATH,
                                         failures_path=FAILURES_PATH, release=release)
        history_controller.record_scope(CONTROLLER_PATH, campaign_id=campaign, run_id=run_id,
                                        slot=item["slot"], index=int(index), accounted=True)
    reservation = history_controller.read_summary(CONTROLLER_PATH, campaign)["run"]
    receipt = {"history_slots_receipt": True, "dag_run_id": run_id, "campaign_id": campaign,
               "finalized": True, "claimed_count": reservation["cursor"],
               "plan_digest": reservation["plan_digest"], "plan_length": len(reservation["plan"]),
               "items": [dict(item, index=int(index), environment=reservation["plan"][item["plan_index"]])
                                              for index, item in reservation["items"].items()]}
    destination = Path(RESULT_DIR) / ("history-slots-" + hashlib.sha256(run_id.encode()).hexdigest()[:20] + ".json")
    receipt["receipt_digest"] = hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest()
    state._write_document_atomically(destination, receipt)
    descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    history_controller.finalize(CONTROLLER_PATH, campaign_id=campaign, run_id=run_id)
    target = datetime.now(timezone.utc) + (ACTIVE_COOLDOWN if reservation["items"] else IDLE_COOLDOWN)
    context["ti"].xcom_push(key="next_poll_at", value=target.isoformat())
    return {"did_work": bool(reservation["items"]), "next_poll_at": target.isoformat(), "receipt": str(destination)}


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
        if _task_state(task_instance) in {"failed", "upstream_failed"}:
            failures.append(task_instance.task_id)
    if failures:
        raise AirflowException(
            "SofaScore history attempt failed: " + ", ".join(sorted(failures))
        )
    return {"status": "success"}


RUN_SCOPE_COMMAND = """
set -euo pipefail
cd /opt/airflow
if [ -n "${SOFASCORE_HISTORY_SLOT:-}" ]; then
  exec /opt/legacy-scraper-venv/bin/python -m scrapers.sofascore.history_worker \
    --checkpoint "${SOFASCORE_HISTORY_CONTROLLER}" \
    --campaign-id "${SOFASCORE_EXPECTED_CAMPAIGN_ID}" \
    --run-id "${AIRFLOW_CTX_DAG_RUN_ID}" --slot "${SOFASCORE_HISTORY_SLOT}" \
    --state "${SOFASCORE_HISTORY_STATE}" --failures "${SOFASCORE_HISTORY_FAILURES}" \
    --dag-id "${AIRFLOW_CTX_DAG_ID}" --pool "${SOFASCORE_HISTORY_POOL}"
fi
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
      --phase "${SOFASCORE_HISTORY_PHASE:-all}" \
      --season-evidence "${SOFASCORE_HISTORY_SEASON_EVIDENCE:-pages}" \
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
    description="Three independent SofaScore history slots in gated breadth-first order",
    schedule="@continuous",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=HISTORY_MAX_ACTIVE_TASKS,
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
        pool=HISTORY_POOL,
        priority_weight=1,
        do_xcom_push=False,
        max_active_tis_per_dag=min(3, HISTORY_MAX_ACTIVE_TASKS),
        # One retry keeps the same run_id: the gateway reuses the signed plan,
        # finished allocations replay from raw, a latched lease is re-claimed
        # once the reaper grace (30 s) has passed.
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=5, minutes=50),
    ).expand(env=plan.output)
    validate = PythonOperator.partial(
        task_id="validate_historical_scope",
        python_callable=_validate_historical_scope,
        # A batch is a batch of independent scopes: ``validate`` expands over the
        # same plan but lives outside the mapped group, so the default
        # ``all_success`` made one failed ``run[i]`` mark EVERY ``validate`` as
        # upstream_failed — and ``finalize`` then booked two paid, healthy scopes
        # as failures.  ``all_done`` lets each validation reach its own check: it
        # decides on its own result whether its scope succeeded.
        trigger_rule="all_done",
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
