"""Drain the full FotMob men's-competition history in the isolated stack.

Companion to ``dag_trigger_fotmob_daily`` (which owns the 14:00 UTC current
window): this DAG self-drains every historical season FotMob exposes for the
included senior-men catalog.  Each continuous run mints one exact publication
generation, triggers ``dag_ingest_fotmob`` in ``backfill`` mode over a bounded
chunk, then cools down — short while the chunk still moved rows, long once the
plan is drained so an idle DAG does not hot-spin the shared scheduler.

Resume is durable and free: the runner skips scopes already terminal in
``bronze.fotmob_ingest_manifest``, so successive bounded chunks advance the
plan without any DAG-side cursor.  The men's-only guarantee is upstream and
automatic — ``scrapers/fotmob/catalog.classify_competition`` marks
women/youth/reserve competitions EXCLUDED and ``planner.plan_seasons`` only
plans INCLUDED identities, so an empty ``scopes`` value drains the whole
men's catalog and never a women's competition.

Like the daily owner, this must run through the parent ingest DAG (never a
writer child directly) so the publication writer fence stays intact, and it
only materializes under ``FOTMOB_ISOLATED_STACK=1``.  Traffic is direct-only
(proxy bytes are fenced to zero).  Paused on creation: unpause to start
draining, pause to stop.

Trade-off (documented): routing through ``dag_ingest_fotmob`` means every chunk
also rebuilds Silver (``_should_transform`` is true for backfill).  Chunks are
therefore deliberately coarse (``BACKFILL_SEASON_LIMIT``) with a multi-minute
active cooldown so Silver is rebuilt at most a few times per hour while
history drains, not once per season.
"""

from datetime import datetime, timedelta, timezone
import logging
import os
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor

from utils.default_args import DEFAULT_ARGS
from utils.fotmob_publication import (
    attest_fotmob_isolated_runtime,
    fail_unsealed_fotmob_publication,
    initialize_fotmob_publication,
)

logger = logging.getLogger(__name__)

ISOLATED_STACK_ENV = "FOTMOB_ISOLATED_STACK"
INGEST_DAG_ID = "dag_ingest_fotmob"
INITIALIZER_TASK_ID = "initialize_fotmob_publication"
TRIGGER_TASK_ID = "trigger_fotmob_backfill"

# One admitted backfill profile.  Values mirror the daily production budget
# ceilings (``utils.fotmob_publication``: 10k requests, 512 MiB direct, 60
# rpm max) but bound each continuous run to a coarse, resumable slice.
#   * ``BACKFILL_SEASON_LIMIT`` truncates the planner to N new season-scopes so
#     one run is bounded and the manifest resumes the rest on the next run.
#   * proxy bytes stay 0 — FotMob is direct-only by design.
BACKFILL_ENTITIES = "season,leaderboards,matches,teams,players,transfers"
BACKFILL_MAX_REQUESTS = 10_000
BACKFILL_MAX_DIRECT_MIB = 512
BACKFILL_REQUESTS_PER_MINUTE = 45
BACKFILL_SEASON_LIMIT = 6

# Cooldown between continuous runs: coarse on purpose because each chunk also
# rebuilds Silver.  Short-ish while draining, long once the plan is empty.
ACTIVE_COOLDOWN = timedelta(minutes=5)
IDLE_COOLDOWN = timedelta(minutes=30)

# The exact generation minted by ``initialize_fotmob_publication`` is handed to
# the ingest child through the DagRun conf, identical to the daily owner.
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


def _rows_total(report: Any) -> int:
    """Sum committed Bronze rows the validated ingest chunk reported."""

    if not isinstance(report, dict):
        return 0
    rows = report.get("rows")
    if isinstance(rows, dict):
        total = 0
        for value in rows.values():
            try:
                total += int(value or 0)
            except (TypeError, ValueError):
                continue
        return total
    try:
        return int(rows or 0)
    except (TypeError, ValueError):
        return 0


def schedule_next_poll(**context: Any) -> None:
    """Set the next continuous-run cooldown from the child chunk's progress.

    Reads the exact ingest child's ``validate_data`` result.  A chunk that
    committed rows means history is still draining (short cooldown); zero rows
    (plan exhausted) or any lookup failure backs off to the long idle cooldown
    so a drained or wedged DAG never hot-spins the shared scheduler.
    """

    did_work = False
    try:
        generation = context["ti"].xcom_pull(task_ids=INITIALIZER_TASK_ID)[
            "generation_id"
        ]
        ingest_run_id = f"fotmob_backfill__{generation}"
        from airflow.models.xcom import XCom

        report = XCom.get_one(
            run_id=ingest_run_id,
            dag_id=INGEST_DAG_ID,
            task_id="validate_data",
            key="return_value",
        )
        did_work = _rows_total(report) > 0
        logger.info(
            "FotMob backfill chunk: run=%s rows=%d draining=%s",
            ingest_run_id,
            _rows_total(report),
            did_work,
        )
    except Exception as exc:  # noqa: BLE001 - any lookup failure = safe backoff
        logger.warning(
            "FotMob backfill cooldown: child progress unavailable (%s); "
            "backing off to idle",
            exc,
        )
        did_work = False
    now = datetime.now(timezone.utc)
    target = now + (ACTIVE_COOLDOWN if did_work else IDLE_COOLDOWN)
    context["ti"].xcom_push(key="next_poll_at", value=target.isoformat())


def _poll_ready(**context: Any) -> bool:
    raw_target = context["ti"].xcom_pull(
        task_ids="schedule_next_poll", key="next_poll_at"
    )
    if not raw_target:
        return True
    try:
        target = datetime.fromisoformat(str(raw_target)).astimezone(timezone.utc)
    except ValueError as exc:
        raise AirflowException("backfill next-poll timestamp is invalid") from exc
    return datetime.now(timezone.utc) >= target


dag = None
if os.environ.get(ISOLATED_STACK_ENV) == "1":
    with DAG(
        dag_id="dag_backfill_fotmob",
        description="Self-draining source-native FotMob men's-history backfill",
        default_args={**DEFAULT_ARGS, "retries": 0},
        schedule="@continuous",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["fotmob", "backfill", "orchestrator", "bronze"],
        params={
            "scopes": Param(
                default="",
                type="string",
                title="Exact backfill scopes",
                description=(
                    "Optional comma-separated FotMob ID=season keys "
                    "(e.g. 47=2024/2025,87=2023/2024) to narrow the drain. "
                    "Empty = the whole included men's catalog, oldest-to-newest."
                ),
            ),
        },
    ) as dag:
        attest_runtime = PythonOperator(
            task_id="attest_isolated_runtime",
            python_callable=attest_fotmob_isolated_runtime,
            retries=0,
        )

        initialize_publication = PythonOperator(
            task_id=INITIALIZER_TASK_ID,
            python_callable=initialize_fotmob_publication,
            op_kwargs={"publication_owner": "isolated"},
            retries=0,
        )

        trigger_backfill = TriggerDagRunOperator(
            task_id=TRIGGER_TASK_ID,
            trigger_dag_id=INGEST_DAG_ID,
            trigger_run_id="fotmob_backfill__" + GENERATION_TEMPLATE,
            logical_date="{{ logical_date.isoformat() }}",
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=["success"],
            failed_states=["failed"],
            reset_dag_run=False,
            conf={
                "mode": "backfill",
                "scope": "{{ params.scopes }}",
                "entities": BACKFILL_ENTITIES,
                "max_requests": BACKFILL_MAX_REQUESTS,
                "max_direct_mib": BACKFILL_MAX_DIRECT_MIB,
                "max_proxy_mib": 0,
                "competition_limit": 0,
                "season_limit": BACKFILL_SEASON_LIMIT,
                "requests_per_minute": BACKFILL_REQUESTS_PER_MINUTE,
                "fotmob_publication": {
                    "generation_id": GENERATION_TEMPLATE,
                    "binding": BINDING_TEMPLATE,
                },
            },
            execution_timeout=timedelta(hours=14),
            retries=0,
        )

        finalize_publication = PythonOperator(
            task_id="finalize_fotmob_publication",
            python_callable=fail_unsealed_fotmob_publication,
            op_kwargs={
                "publication_owner": "isolated",
                "success_task_id": TRIGGER_TASK_ID,
                "writer_task_ids": [TRIGGER_TASK_ID],
            },
            trigger_rule="all_done",
            retries=0,
        )

        schedule_cooldown = PythonOperator(
            task_id="schedule_next_poll",
            python_callable=schedule_next_poll,
            trigger_rule="all_done",
            retries=0,
        )

        wait_next_run = PythonSensor(
            task_id="wait_before_next_continuous_run",
            python_callable=_poll_ready,
            mode="reschedule",
            poke_interval=60,
            timeout=int(IDLE_COOLDOWN.total_seconds()) + 600,
        )

        (
            attest_runtime
            >> initialize_publication
            >> trigger_backfill
            >> finalize_publication
            >> schedule_cooldown
            >> wait_next_run
        )
