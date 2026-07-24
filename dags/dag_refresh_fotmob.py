"""Keep the whole FotMob men's catalog's current seasons fresh, on rotation.

The pinned daily owner (``dag_trigger_fotmob_daily``) only refreshes the 21
core competitions every 14:00 — the full ~494-competition men's catalog cannot
finish inside one direct-only slot.  This continuous companion closes that gap:
each run refreshes the CURRENT (selected/latest) season of the ``season_limit``
STALEST included men's competitions, then cools down, so over a couple of days
every men's competition's current season is re-pulled and the cycle repeats.

It reuses the runner's ``refresh`` mode, which plans and writes exactly like
``daily`` (current seasons, stalest-first, men's-only via
``classify_competition``) but bounded by ``--season-limit`` and validated
best-effort — no pinned 21-competition contract, no requirement that a single
run finish the whole catalog.  Coverage is dynamic: new competitions FotMob
advertises are picked up automatically.

Like the other owners it runs through the parent ingest DAG (never a writer
child directly) so the publication writer fence stays intact, mints one exact
generation per run, is direct-only (proxy bytes fenced to zero), and only
materializes under ``FOTMOB_ISOLATED_STACK=1``.  Paused on creation: unpause to
start the rotation, pause to stop.

Trade-off (documented): routing through ``dag_ingest_fotmob`` rebuilds Silver
each chunk, and every run re-discovers the full catalog to rank staleness.
Chunks are therefore deliberately coarse (``REFRESH_SEASON_LIMIT``) with a
multi-minute cooldown so Silver and discovery cost are amortized, not paid per
competition.
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
TRIGGER_TASK_ID = "trigger_fotmob_refresh"
RUN_ID_PREFIX = "fotmob_refresh__"

# One admitted refresh profile.  Every run re-discovers the full catalog
# (~494 competitions, needed to rank staleness) and then syncs the current
# season of the ``REFRESH_SEASON_LIMIT`` stalest, so the budget covers both.
# Coarser chunks amortize the fixed ~494-request discovery cost over more
# useful sync work and rebuild Silver less often, at the price of a longer
# single run.  All values are ceilings/tunables — raise the cooldown to lower
# sustained direct-only traffic (a full ~494 rotation lands in ~2-3 days at
# these settings), shrink the chunk if a single run struggles to finish.
# Proxy bytes stay 0 — FotMob is direct-only by design.
REFRESH_ENTITIES = "season,leaderboards,matches,teams,players,transfers"
REFRESH_MAX_REQUESTS = 15_000
REFRESH_MAX_DIRECT_MIB = 1024
REFRESH_REQUESTS_PER_MINUTE = 60
REFRESH_SEASON_LIMIT = 20

# Cooldown between continuous runs: there is almost always a stalest
# competition to refresh, so ACTIVE is the normal cadence; IDLE only guards the
# rare empty chunk.  Raise ACTIVE to reduce sustained load.
ACTIVE_COOLDOWN = timedelta(minutes=10)
IDLE_COOLDOWN = timedelta(minutes=30)

# The exact generation minted by ``initialize_fotmob_publication`` is handed to
# the ingest child through the DagRun conf, identical to the other owners.
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
    committed rows keeps the short rotation cadence; zero rows or any lookup
    failure backs off to the long idle cooldown so a wedged DAG never hot-spins
    the shared scheduler.
    """

    did_work = False
    try:
        generation = context["ti"].xcom_pull(task_ids=INITIALIZER_TASK_ID)[
            "generation_id"
        ]
        ingest_run_id = f"{RUN_ID_PREFIX}{generation}"
        from airflow.models.xcom import XCom

        report = XCom.get_one(
            run_id=ingest_run_id,
            dag_id=INGEST_DAG_ID,
            task_id="validate_data",
            key="return_value",
        )
        did_work = _rows_total(report) > 0
        logger.info(
            "FotMob refresh chunk: run=%s rows=%d active=%s",
            ingest_run_id,
            _rows_total(report),
            did_work,
        )
    except Exception as exc:  # noqa: BLE001 - any lookup failure = safe backoff
        logger.warning(
            "FotMob refresh cooldown: child progress unavailable (%s); "
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
        raise AirflowException("refresh next-poll timestamp is invalid") from exc
    return datetime.now(timezone.utc) >= target


dag = None
if os.environ.get(ISOLATED_STACK_ENV) == "1":
    with DAG(
        dag_id="dag_refresh_fotmob",
        description="Continuous stalest-first refresh of the men's catalog's current seasons",
        default_args={**DEFAULT_ARGS, "retries": 0},
        schedule="@continuous",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["fotmob", "refresh", "orchestrator", "bronze"],
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

        trigger_refresh = TriggerDagRunOperator(
            task_id=TRIGGER_TASK_ID,
            trigger_dag_id=INGEST_DAG_ID,
            trigger_run_id=RUN_ID_PREFIX + GENERATION_TEMPLATE,
            logical_date="{{ logical_date.isoformat() }}",
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=["success"],
            failed_states=["failed"],
            reset_dag_run=False,
            conf={
                "mode": "refresh",
                # Empty scope: the runner plans the current season of every
                # included men's competition and the stalest-first sort +
                # season_limit bound this run to one rotating chunk.
                "scope": "",
                "entities": REFRESH_ENTITIES,
                "max_requests": REFRESH_MAX_REQUESTS,
                "max_direct_mib": REFRESH_MAX_DIRECT_MIB,
                "max_proxy_mib": 0,
                "competition_limit": 0,
                "season_limit": REFRESH_SEASON_LIMIT,
                "requests_per_minute": REFRESH_REQUESTS_PER_MINUTE,
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
            >> trigger_refresh
            >> finalize_publication
            >> schedule_cooldown
            >> wait_next_run
        )
