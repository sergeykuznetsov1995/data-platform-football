"""ESPN current-data waves: 00/06/12/18 UTC, publication per tournament (#1504).

Lives in ``deploy/espn/dags/`` — the DAG folder of the ``espn-airflow``
project (mounted by #1507), not ``dags/``: a new file there changes the
directory the WhoScored runtime guard pins and the shared stack does not
collect ESPN (roadmap assumption 8).  Paused on creation all the same.

prepare -> plan_wave -> run_tournament (mapped, one tournament-season each)
-> wave_summary (all_done).  The logic is ``scrapers.espn.wave``; this file
only wires Airflow, the transport and Trino.  ``wave_summary`` writes the wave
log (``scrapers.espn.wave_log``, #1505) before it turns a red wave red.
``prepare`` also creates the recheck journal (``scrapers.espn.recheck``, #1506).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DAG_ID = "dag_espn_current"
SCHEDULE = "0 0,6,12,18 * * *"
# Pool of the live lane; created by the espn-airflow delivery (#1507).
LIVE_POOL = "espn_live"
RUN_TASK_ID = "run_tournament"
PLAN_TASK_ID = "plan_wave"
OUTCOME_KEY = "outcome"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def _trino():
    from scrapers.base.trino_manager import TrinoTableManager

    return TrinoTableManager()


def _client():
    from scrapers.espn.gate import TransportGate
    from scrapers.espn.transport import EspnHttpClient

    # One gate per task process; the pace is shared through its flock file,
    # the step comes from ESPN_GATE_STEP_CEILING (S0 by default).
    return EspnHttpClient(gate=TransportGate(lane="live"))


def _gate_closed(exc: BaseException) -> bool:
    from scrapers.espn.transport_contracts import AllOriginsBlocked, LaneClosed

    return isinstance(exc, (AllOriginsBlocked, LaneClosed))


def _no_retry(exc: BaseException) -> bool:
    """Gate closed or planning already failed: a retry would change nothing."""

    from scrapers.espn.wave import WavePlanError

    return _gate_closed(exc) or isinstance(exc, WavePlanError)


def _task_name(ti) -> str:
    map_index = getattr(ti, "map_index", -1)
    return f"{ti.task_id}[{map_index}]" if map_index is not None and map_index >= 0 else ti.task_id


def prepare(**_: Any) -> None:
    from scrapers.base.iceberg_writer import IcebergWriter
    from scrapers.espn.bronze_schema import ensure_bronze_tables
    from scrapers.espn.journal import ensure_journal_table
    from scrapers.espn.recheck import ensure_recheck_table
    from scrapers.espn.wave_log import ensure_wave_log_table

    ensure_bronze_tables(IcebergWriter())
    connection = _trino().connection
    ensure_journal_table(connection)
    ensure_wave_log_table(connection)
    ensure_recheck_table(connection)


def plan_wave(**context: Any) -> list[dict[str, Any]]:
    from scrapers.espn import editions_store, wave
    from scrapers.espn.denominator import load_denominator

    now = datetime.now(timezone.utc)
    boundary = context.get("data_interval_end") or now
    midnight = boundary.astimezone(timezone.utc).hour == 0
    trino = _trino()
    client = _client()
    failure: BaseException | None = None
    try:
        plan = wave.plan_wave(
            client=client,
            trino=trino,
            rows=wave.live_rows(load_denominator()),
            state_path=editions_store.default_state_path(),
            now=now,
            # Once a day, in the 00 wave: stuck POSTPONED/SUSPENDED matches
            # and the core event list against bronze (#1505, R-09).
            check_stale=midnight,
            check_core=midnight,
            # Once a day too: rechecks at kickoff + 7…10 days and the 5 %
            # sample at +24/72 h, under the "not worse" rule (#1506).
            check_recheck=midnight,
        )
    except BaseException as exc:
        failure = exc
        if _gate_closed(exc):
            raise AirflowFailException(f"ESPN gate closed: {type(exc).__name__}: {exc}") from exc
        raise
    finally:
        try:
            wave.finish_requests(
                client,
                trino.connection,
                run_id=str(context.get("run_id") or "manual"),
                task_id=PLAN_TASK_ID,
            )
        except Exception:
            if failure is None:
                raise
            logger.exception("ESPN plan request journal not written")
    return [{"work": work.to_xcom()} for work in plan.works]


def run_tournament(work: dict[str, Any], **context: Any) -> dict[str, Any]:
    from scrapers.espn import wave

    ti = context["ti"]
    item = wave.TournamentWork.from_xcom(work)
    try:
        trino = _trino()
        outcome = wave.run_tournament(
            item,
            client=_client(),
            trino=trino,
            conn=trino.connection,
            run_id=str(context.get("run_id") or "manual"),
            task_id=_task_name(ti),
        )
    except Exception as exc:
        ti.xcom_push(key=OUTCOME_KEY, value=wave.failed_outcome(item, exc).as_dict())
        if _no_retry(exc):
            raise AirflowFailException(f"ESPN {item.slug}: {type(exc).__name__}: {exc}") from exc
        raise
    result = outcome.as_dict()
    ti.xcom_push(key=OUTCOME_KEY, value=result)
    return result


def _state(ti) -> str:
    state = getattr(ti.state, "value", ti.state)
    return str(state or "none").casefold().split(".")[-1]


def wave_summary(**context: Any) -> dict[str, Any]:
    from scrapers.espn import wave, wave_log

    dag_run = context["dag_run"]
    ti = context["ti"]
    outcomes: list[dict[str, Any]] = []
    failed: list[str] = []
    plan_error = None
    for instance in dag_run.get_task_instances():
        state = _state(instance)
        if instance.task_id == PLAN_TASK_ID and state != "success":
            plan_error = state
        if instance.task_id != RUN_TASK_ID or state in {"skipped", "removed"}:
            continue
        outcome = ti.xcom_pull(
            task_ids=RUN_TASK_ID, map_indexes=instance.map_index, key=OUTCOME_KEY
        )
        if outcome:
            if state != "success" and outcome.get("state") != wave.RED:
                outcome = {**outcome, "state": wave.RED, "first_error": f"task {state}"}
            outcomes.append(outcome)
        else:
            failed.append(f"{_task_name(instance)}={state}")
    started = getattr(dag_run, "start_date", None)
    finished = datetime.now(timezone.utc)
    duration = (finished - started).total_seconds() if started is not None else None
    summary = wave.summarize_wave(
        outcomes, failed, plan_error=plan_error, duration_s=duration
    )
    # The log goes first: a red wave leaves its trace too (#1505).
    try:
        wave_log.write_wave_log(
            _trino().connection,
            wave_log.wave_log_rows(
                outcomes,
                failed,
                summary,
                run_id=str(context.get("run_id") or "manual"),
                started_at=started,
                finished_at=finished,
            ),
        )
    except Exception:
        if not summary.red:
            raise
        logger.exception("ESPN wave log not written")
    for line in summary.table:
        logger.info("ESPN wave: %s", line)
    for warning in summary.warnings:
        logger.warning("ESPN wave: %s", warning)
    logger.info(
        "ESPN wave: %d tournament(s), %d red, withdrawn %d, moved %d, duration %s s",
        summary.tournaments,
        summary.red_tournaments,
        summary.withdrawn_count,
        summary.moved_count,
        None if duration is None else round(duration),
    )
    if summary.red:
        raise AirflowException(f"ESPN wave red: {summary.reason}")
    return summary.as_dict()


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="ESPN current data: four waves a day, one batch per tournament",
    schedule=SCHEDULE,
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=55),
    tags=["espn", "bronze", "scraping"],
    doc_md=__doc__,
) as dag:
    prepare_task = PythonOperator(
        task_id="prepare",
        python_callable=prepare,
        retries=2,
        execution_timeout=timedelta(minutes=10),
    )
    plan_task = PythonOperator(
        task_id=PLAN_TASK_ID,
        python_callable=plan_wave,
        pool=LIVE_POOL,
        retries=2,
        execution_timeout=timedelta(minutes=20),
    )
    run_task = PythonOperator.partial(
        task_id=RUN_TASK_ID,
        python_callable=run_tournament,
        pool=LIVE_POOL,
        retries=2,
        retry_delay=timedelta(minutes=3),
        max_active_tis_per_dag=4,
        execution_timeout=timedelta(minutes=20),
    ).expand(op_kwargs=plan_task.output)
    summary_task = PythonOperator(
        task_id="wave_summary",
        python_callable=wave_summary,
        trigger_rule="all_done",
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )

    prepare_task >> plan_task >> run_task >> summary_task
