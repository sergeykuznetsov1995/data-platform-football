"""ESPN history lane: past seasons on what the live lane leaves, every 30 min (#1509).

Lives in ``deploy/espn/dags/`` next to ``dag_espn_current`` (the espn-live DAG
folder, #1507), never in ``dags/``.  Paused on creation; the owner unpauses it.

prepare -> run_history.  The logic is ``scrapers.espn.history``: one run walks
the queue ``iceberg.ops.espn_history_queue_v1`` until it is empty, the time
budget (12 min from the DAG run start) ends, the live lane has a debt, the gate closes the
``history`` lane or the stop file appears; each of those is a clean end.
Never ahead of the live lane: its own pool ``espn_history`` (1 slot),
``priority_weight=1`` with ``weight_rule="absolute"`` and the ``history``
lane of the gate (only what the live share leaves, frozen first).

Stop file: ``$AIRFLOW_HOME/state/espn/history.off`` (the ``espn_live_state``
volume) — while it exists a run does nothing; it is also checked between
matches.  The scope is ``configs/espn/history_scope.json``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DAG_ID = "dag_espn_history"
SCHEDULE = "*/30 * * * *"
# Pool of the history lane; created by the espn-live delivery from pools.json.
HISTORY_POOL = "espn_history"
RUN_TASK_ID = "run_history"
# The whole DAG run ends within DAGRUN_TIMEOUT of its start: >= 10 min (2 ticks
# of the */5 delivery cron) before the next run, else busy_reason starves the
# espn-live delivery.  The budget counts from the DAG run start (prepare and
# queueing included) and leaves room for the write after it; the timeouts are
# the backstop.
BUDGET = timedelta(minutes=12)
TASK_TIMEOUT = timedelta(minutes=18)
DAGRUN_TIMEOUT = timedelta(minutes=20)

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 0,
}


def _trino():
    # Without dynamic filtering: the tombstone MERGE loses NULL rows (#1557).
    from scrapers.espn.trino_manager import EspnTrinoTableManager

    return EspnTrinoTableManager()


def _client():
    from scrapers.espn.gate import TransportGate
    from scrapers.espn.transport import EspnHttpClient

    # The history lane of the one VM gate: the rest of the live share.
    return EspnHttpClient(gate=TransportGate(lane="history"))


def prepare(**_: Any) -> None:
    from scrapers.base.iceberg_writer import IcebergWriter
    from scrapers.espn.bronze_schema import ensure_bronze_tables
    from scrapers.espn.history import ensure_queue_table
    from scrapers.espn.journal import ensure_journal_table

    ensure_bronze_tables(IcebergWriter())
    connection = _trino().connection
    ensure_journal_table(connection)
    ensure_queue_table(connection)


def _run_start(context: dict[str, Any]) -> datetime:
    # The budget is for the whole DAG run, not for this task.
    start = getattr(context.get("dag_run"), "start_date", None)
    return start or datetime.now(timezone.utc)


def run_history(**context: Any) -> dict[str, Any]:
    from scrapers.espn import history
    from scrapers.espn.denominator import load_denominator

    stop_file = history.default_stop_file()
    if history.history_stopped(stop_file):
        logger.info("ESPN history: stop file %s present, nothing to do", stop_file)
        return {"reason": history.STOPPED}
    trino = _trino()
    client = _client()
    try:
        run = history.run_history(
            client=client,
            trino=trino,
            conn=trino.connection,
            denominator=load_denominator(),
            scope=history.load_scope(),
            run_id=str(context.get("run_id") or "manual"),
            deadline=_run_start(context) + BUDGET,
            stop_file=stop_file,
            task_id=RUN_TASK_ID,
        )
    finally:
        client.close()
    logger.info("ESPN history: %s", run.as_dict())
    return run.as_dict()


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="ESPN history: past seasons on the rest of the live share",
    schedule=SCHEDULE,
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=DAGRUN_TIMEOUT,
    tags=["espn", "bronze", "scraping", "history"],
    doc_md=__doc__,
) as dag:
    prepare_task = PythonOperator(
        task_id="prepare",
        python_callable=prepare,
        retries=1,
        execution_timeout=timedelta(minutes=5),
    )
    run_task = PythonOperator(
        task_id=RUN_TASK_ID,
        python_callable=run_history,
        pool=HISTORY_POOL,
        priority_weight=1,
        weight_rule="absolute",
        retries=0,
        execution_timeout=TASK_TIMEOUT,
    )

    prepare_task >> run_task
