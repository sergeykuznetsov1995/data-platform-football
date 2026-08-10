"""Single fair schedule owner for automatic adult-men FotMob collection."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Mapping

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.default_args import DEFAULT_ARGS
from utils.fotmob_orchestration import (
    FotMobLane,
    FotMobSchedulerState,
    advance_after_success,
    build_child_conf,
    choose_lane,
)
from utils.fotmob_publication import (
    FOTMOB_DEPLOYMENT_REPORT_PATH_ENV,
    FOTMOB_PUBLICATION_SOURCE,
    attest_fotmob_isolated_runtime,
    fotmob_ceremony_configured,
    initialize_fotmob_publication,
)


UTC = timezone.utc
ISOLATED_STACK_ENV = "FOTMOB_ISOLATED_STACK"
SCHEDULER_STATE_VARIABLE = "fotmob.scheduler.state.v1"
INGEST_DAG_ID = "dag_ingest_fotmob"
OWNER_DAG_ID = "dag_orchestrate_fotmob"
AUTOMATIC_ADMITTED_DAGS = frozenset(
    {OWNER_DAG_ID, "dag_ingest_fotmob", "dag_transform_fotmob_silver"}
)
LEGACY_PAUSED_DAGS = frozenset(
    {"dag_trigger_fotmob_daily", "dag_refresh_fotmob", "dag_backfill_fotmob"}
)
DECISION_TASK_ID = "choose_fotmob_lane"
INITIALIZER_TASK_ID = "initialize_fotmob_publication"
TRIGGER_TASK_ID = "trigger_fotmob_ingest"
LAUNCH_REJECTED_XCOM_KEY = "fotmob_launch_rejected"

GENERATION_TEMPLATE = (
    "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
    "['generation_id'] }}"
)
BINDING_TEMPLATE = {
    key: (
        "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
        f"['binding']['{key}'] }}}}"
    )
    for key in (
        "schema",
        "source",
        "owner",
        "data_interval_start",
        "data_interval_end",
        "runtime_fingerprint",
    )
}
_CONF_KEYS = (
    "mode",
    "scope",
    "catalog_contract",
    "entities",
    "max_requests",
    "max_direct_mib",
    "max_proxy_mib",
    "competition_limit",
    "season_limit",
    "requests_per_minute",
    "deadline",
)
CHILD_CONF_TEMPLATE = {
    key: (
        "{{ ti.xcom_pull(task_ids='choose_fotmob_lane')"
        f"['conf']['{key}'] }}}}"
    )
    for key in _CONF_KEYS
}


def _utc_now(value: datetime | None = None) -> datetime:
    now = value or datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise AirflowException("FotMob owner time must include a timezone")
    return now.astimezone(UTC)


def _load_state() -> FotMobSchedulerState:
    # Airflow applies json.loads to a default when deserialize_json=True, so
    # the first-run default must be JSON text rather than a Python dict.
    default = json.dumps(
        FotMobSchedulerState.initial().to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    )
    raw = Variable.get(
        SCHEDULER_STATE_VARIABLE,
        default_var=default,
        deserialize_json=True,
    )
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise AirflowException("FotMob scheduler state is invalid JSON") from exc
    try:
        return FotMobSchedulerState.from_dict(raw)
    except (TypeError, ValueError) as exc:
        raise AirflowException("FotMob scheduler state is invalid") from exc


def _store_state(state: FotMobSchedulerState) -> None:
    Variable.set(SCHEDULER_STATE_VARIABLE, state.to_dict(), serialize_json=True)


def _ingest_child_active() -> bool:
    # Lazy import keeps host DAG unit tests independent of a full Airflow DB.
    from airflow.models import DagRun

    if DagRun.find(dag_id=INGEST_DAG_ID, state="queued"):
        return True
    return bool(DagRun.find(dag_id=INGEST_DAG_ID, state="running"))


def _context_utc_now(context: Mapping[str, Any]) -> datetime:
    """Read the real clock at the call site, with deterministic test hooks."""

    clock = context.get("utcnow")
    if callable(clock):
        return _utc_now(clock())
    supplied_now = context.get("now_utc")
    return _utc_now(supplied_now if isinstance(supplied_now, datetime) else None)


def _attest_owner_runtime(**context: Any) -> dict[str, Any]:
    """Retain the existing admission while naming this new scheduled owner."""

    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", None)
    run_type = getattr(dag_run, "run_type", None)
    normalized_run_type = str(getattr(run_type, "value", run_type) or "").casefold()
    if dag_id != OWNER_DAG_ID or normalized_run_type != "scheduled":
        raise AirflowException("FotMob orchestrator requires its scheduled DagRun")
    # The historic admission helper knows the three rollback owners.  This
    # task performs the exact new-owner check above and asks it to validate all
    # remaining deployment/runtime evidence without the legacy owner allowlist.
    attestation = attest_fotmob_isolated_runtime(
        require_scheduled_owner=False,
        **context,
    )
    supplied_environ = context.get("environ")
    runtime_env = supplied_environ if isinstance(supplied_environ, Mapping) else os.environ
    if fotmob_ceremony_configured(runtime_env):
        report_path = str(
            runtime_env.get(FOTMOB_DEPLOYMENT_REPORT_PATH_ENV, "")
        ).strip()
        try:
            with open(report_path, "r", encoding="utf-8") as stream:
                report = json.load(stream)
        except (OSError, json.JSONDecodeError) as exc:
            raise AirflowException(
                "FotMob automatic owner deployment admission is unavailable"
            ) from exc
        if (
            not isinstance(report, Mapping)
            or report.get("activation_state") != "active"
            or report.get("unpaused") != sorted(AUTOMATIC_ADMITTED_DAGS)
            or report.get("paused") != sorted(LEGACY_PAUSED_DAGS)
        ):
            # Task 7 rotates deployment admission from the legacy daily owner
            # to this exact set.  Until that evidence exists, remain paused
            # and fail closed instead of borrowing the legacy report.
            raise AirflowException(
                "FotMob deployment admission does not name the automatic owner"
            )
    return attestation


def select_fotmob_lane(**context: Any) -> dict[str, Any] | bool:
    """Short-circuit an idle tick, otherwise publish one immutable decision."""

    now = _context_utc_now(context)
    state = _load_state()
    child_running = bool(
        context.get("child_running")
        if "child_running" in context
        else _ingest_child_active()
    )
    decision = choose_lane(now, state, child_running)
    if decision.lane is None:
        return False
    return {
        "lane": decision.lane.value,
        "reason": decision.reason,
        "selected_date": now.date().isoformat(),
        "state": state.to_dict(),
        "state_generation": state.generation,
        "conf": build_child_conf(decision.lane, now),
    }


def _selected_launch(
    context: Mapping[str, Any],
) -> tuple[Mapping[str, Any], FotMobSchedulerState, FotMobLane]:
    ti = context.get("ti")
    raw = ti.xcom_pull(task_ids=DECISION_TASK_ID) if ti is not None else None
    if not isinstance(raw, Mapping):
        raise AirflowException("FotMob scheduler decision XCom is missing")
    try:
        selected_state = FotMobSchedulerState.from_dict(raw["state"])
        selected_lane = FotMobLane(str(raw["lane"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise AirflowException("FotMob scheduler decision XCom is invalid") from exc
    return raw, selected_state, selected_lane


def _launch_still_admitted(context: Mapping[str, Any]) -> bool:
    raw, selected_state, selected_lane = _selected_launch(context)
    current = _load_state()
    if current != selected_state:
        raise AirflowException("FotMob scheduler state changed before launch")

    deadline = None
    if selected_lane is not FotMobLane.DAILY:
        conf = raw.get("conf")
        if not isinstance(conf, Mapping):
            raise AirflowException("FotMob background launch conf is invalid")
        raw_deadline = conf.get("deadline")
        try:
            deadline = datetime.fromisoformat(str(raw_deadline))
        except (TypeError, ValueError) as exc:
            raise AirflowException("FotMob background deadline is invalid") from exc
        if deadline.tzinfo is None or deadline.utcoffset() is None:
            raise AirflowException("FotMob background deadline is invalid")
        deadline = deadline.astimezone(UTC)

    child_active = bool(
        context.get("child_running")
        if "child_running" in context
        else _ingest_child_active()
    )
    # This is deliberately the last potentially changing observation.  State
    # and DagRun queries above may be slow; after this clock read only pure
    # comparisons remain before TriggerDagRunOperator performs its insert.
    now = _context_utc_now(context)
    if choose_lane(now, current, child_active).lane is not selected_lane:
        return False
    if deadline is not None and now >= deadline:
        return False
    return True


def initialize_admitted_publication(**context: Any) -> dict[str, Any] | bool:
    """Recheck the launch boundary before creating a publication generation."""

    if not _launch_still_admitted(context):
        return False
    raw, _selected_state, lane = _selected_launch(context)
    publication_context = dict(context)
    if lane is FotMobLane.DAILY:
        # The shared consumer is a 14:00 UTC daily DAG.  A five-minute owner
        # tick must mint the exact same 24-hour binding, otherwise the shared
        # consumer can never claim the ready generation.
        selected_date = date.fromisoformat(str(raw["selected_date"]))
        interval_end = datetime.combine(selected_date, time(14, 0), tzinfo=UTC)
        publication_context["data_interval_start"] = interval_end - timedelta(days=1)
        publication_context["data_interval_end"] = interval_end
    return initialize_fotmob_publication(
        publication_owner="isolated",
        require_scheduled_owner=False,
        **publication_context,
    )


def _release_unstarted_publication(context: Mapping[str, Any]) -> dict[str, Any]:
    """Fail and safely unlock a generation before any child was triggered."""

    ti = context.get("ti")
    publication = (
        ti.xcom_pull(task_ids=INITIALIZER_TASK_ID) if ti is not None else None
    )
    if not isinstance(publication, Mapping) or not publication.get("generation_id"):
        raise AirflowException("FotMob initialized publication XCom is missing")
    if not fotmob_ceremony_configured():
        return {
            "status": "released",
            "ceremony": "disabled",
            "generation_id": publication["generation_id"],
        }
    from scrapers.fbref.control import ControlStore

    return ControlStore.from_env().fail_publication_generation(
        publication["generation_id"],
        safe_to_release=True,
        source=FOTMOB_PUBLICATION_SOURCE,
    )


class BoundaryCheckedTriggerDagRunOperator(TriggerDagRunOperator):
    """Guarantee a fresh boundary check at the child-trigger operation."""

    @staticmethod
    def _release_and_mark_rejected(
        context: Mapping[str, Any], *, verdict: str
    ) -> None:
        released = _release_unstarted_publication(context)
        if fotmob_ceremony_configured() and released.get("released") is not True:
            raise AirflowException(
                "FotMob unstarted publication generation was not released"
            )
        ti = context.get("ti")
        if ti is not None:
            ti.xcom_push(
                key=LAUNCH_REJECTED_XCOM_KEY,
                value={
                    "safe_release": True,
                    "state_advanced": False,
                    "verdict": verdict,
                },
            )

    def execute(self, context: Mapping[str, Any]):
        try:
            admitted = _launch_still_admitted(context)
        except Exception:
            self._release_and_mark_rejected(context, verdict="failed")
            raise
        if not admitted:
            self._release_and_mark_rejected(context, verdict="skipped")
            raise AirflowSkipException(
                "FotMob launch window closed before child trigger"
            )
        return super().execute(context)


def finalize_or_skip_rejected_launch(**context: Any) -> dict[str, Any]:
    """Preserve the terminal verdict of a safely rejected child launch."""

    ti = context.get("ti")
    rejected = (
        ti.xcom_pull(task_ids=TRIGGER_TASK_ID, key=LAUNCH_REJECTED_XCOM_KEY)
        if ti is not None
        else None
    )
    if isinstance(rejected, Mapping) and rejected.get("state_advanced") is False:
        verdict = rejected.get("verdict")
        if verdict == "skipped":
            raise AirflowSkipException("FotMob child launch was safely rejected")
        if verdict == "failed":
            raise AirflowException("FotMob child launch failed before child trigger")
        raise AirflowException("FotMob rejected launch verdict is invalid")
    ti = context.get("ti")
    publication = (
        ti.xcom_pull(task_ids=INITIALIZER_TASK_ID) if ti is not None else None
    )
    if not isinstance(publication, Mapping) or not publication.get("generation_id"):
        raise AirflowException("FotMob initialized publication XCom is missing")
    states = {
        str(getattr(item, "task_id", "")): str(
            getattr(getattr(item, "state", None), "value", getattr(item, "state", ""))
            or "missing"
        ).casefold()
        for item in (
            context.get("dag_run").get_task_instances()
            if context.get("dag_run") is not None
            else ()
        )
    }
    if states.get(TRIGGER_TASK_ID) != "success":
        # Preserve the existing terminal-red behavior and the ambiguous child
        # lock.  Pass the exact initialized generation instead of recomputing
        # it from the owner's five-minute interval.
        if not fotmob_ceremony_configured():
            raise AirflowException("FotMob generation did not reach ready")
        from scrapers.fbref.control import ControlStore

        state = ControlStore.from_env().fail_publication_generation(
            publication["generation_id"],
            safe_to_release=False,
            source=FOTMOB_PUBLICATION_SOURCE,
        )
        raise AirflowException(
            "FotMob generation did not reach ready; lock retained because "
            f"child-writer state is ambiguous (state={state})"
        )

    raw, _selected_state, lane = _selected_launch(context)
    if lane is FotMobLane.DAILY or not fotmob_ceremony_configured():
        return {
            "status": "ready",
            "generation_id": publication["generation_id"],
            "lane": lane.value,
        }

    # Refresh/backfill has no shared consumer.  It must release its ready
    # source lock before the next owner tick; otherwise one background run
    # stalls every later run until the long publication TTL expires.
    from scrapers.fbref.control import ControlStore

    abandoned = ControlStore.from_env().complete_publication_generation(
        publication["generation_id"],
        published=False,
        source=FOTMOB_PUBLICATION_SOURCE,
    )
    if (
        abandoned.get("phase") != "abandoned"
        or abandoned.get("active") is not False
        or abandoned.get("released") is not True
        or abandoned.get("published") is not False
    ):
        raise AirflowException("FotMob background generation was not abandoned safely")
    return {
        "status": "abandoned",
        "generation_id": publication["generation_id"],
        "lane": lane.value,
        "publication_state": abandoned,
        "selected_date": raw.get("selected_date"),
    }


def advance_fotmob_scheduler_state(**context: Any) -> dict[str, Any]:
    """Persist alternation only after the synchronous child/finalizer succeeds."""

    ti = context.get("ti")
    raw = ti.xcom_pull(task_ids=DECISION_TASK_ID) if ti is not None else None
    if not isinstance(raw, Mapping):
        raise AirflowException("FotMob scheduler decision XCom is missing")
    try:
        selected_state = FotMobSchedulerState.from_dict(raw["state"])
        lane = FotMobLane(str(raw["lane"]))
        selected_date = date.fromisoformat(str(raw["selected_date"]))
        selected_generation = int(raw["state_generation"])
    except (KeyError, TypeError, ValueError) as exc:
        raise AirflowException("FotMob scheduler decision XCom is invalid") from exc
    if selected_state.generation != selected_generation:
        raise AirflowException("FotMob scheduler decision generation is inconsistent")

    current = _load_state()
    if current.generation == selected_generation + 1:
        # Airflow may retry this task after Variable.set succeeded.  Never
        # toggle a background lane twice for the same child publication.
        expected_next = selected_state.next_background_lane
        expected_daily = selected_state.daily_date
        if lane is FotMobLane.DAILY:
            expected_daily = selected_date
        elif lane is FotMobLane.REFRESH:
            expected_next = FotMobLane.BACKFILL
        elif lane is FotMobLane.BACKFILL:
            expected_next = FotMobLane.REFRESH
        if (
            current.next_background_lane is not expected_next
            or current.daily_date != expected_daily
        ):
            raise AirflowException(
                "FotMob scheduler retry found an unrelated state generation"
            )
        return current.to_dict()
    if current != selected_state:
        raise AirflowException("FotMob scheduler state changed during publication")

    completed_at = _utc_now(
        context.get("now_utc")
        if isinstance(context.get("now_utc"), datetime)
        else None
    )
    advanced = advance_after_success(
        current,
        lane,
        completed_at,
        daily_date=selected_date if lane is FotMobLane.DAILY else None,
    )
    _store_state(advanced)
    return advanced.to_dict()


dag = None
if os.environ.get(ISOLATED_STACK_ENV) == "1":
    with DAG(
        dag_id=OWNER_DAG_ID,
        description="Fair automatic adult-men FotMob daily/refresh/backfill owner",
        schedule="*/5 * * * *",
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        render_template_as_native_obj=True,
        default_args={**DEFAULT_ARGS, "retries": 0},
        tags=["fotmob", "orchestrator", "bronze"],
    ) as dag:
        attest_runtime = PythonOperator(
            task_id="attest_isolated_runtime",
            python_callable=_attest_owner_runtime,
            retries=0,
        )

        choose_lane_task = ShortCircuitOperator(
            task_id=DECISION_TASK_ID,
            python_callable=select_fotmob_lane,
            retries=0,
        )

        initialize_publication = ShortCircuitOperator(
            task_id=INITIALIZER_TASK_ID,
            python_callable=initialize_admitted_publication,
            ignore_downstream_trigger_rules=True,
            retries=0,
        )

        trigger_ingest = BoundaryCheckedTriggerDagRunOperator(
            task_id=TRIGGER_TASK_ID,
            trigger_dag_id=INGEST_DAG_ID,
            trigger_run_id="fotmob_orchestrated__" + GENERATION_TEMPLATE,
            logical_date="{{ logical_date.isoformat() }}",
            conf={
                **CHILD_CONF_TEMPLATE,
                "fotmob_publication": {
                    "generation_id": GENERATION_TEMPLATE,
                    "binding": BINDING_TEMPLATE,
                },
            },
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=["success"],
            failed_states=["failed"],
            reset_dag_run=False,
            execution_timeout=timedelta(hours=14),
            retries=0,
        )

        finalize_publication = PythonOperator(
            task_id="finalize_fotmob_publication",
            python_callable=finalize_or_skip_rejected_launch,
            trigger_rule="all_done",
            retries=0,
        )

        advance_state = PythonOperator(
            task_id="advance_fotmob_scheduler_state",
            python_callable=advance_fotmob_scheduler_state,
            retries=0,
        )

        (
            attest_runtime
            >> choose_lane_task
            >> initialize_publication
            >> trigger_ingest
            >> finalize_publication
            >> advance_state
        )


__all__ = [
    "FotMobLane",
    "FotMobSchedulerState",
    "SCHEDULER_STATE_VARIABLE",
    "advance_fotmob_scheduler_state",
    "dag",
    "initialize_admitted_publication",
    "select_fotmob_lane",
]
