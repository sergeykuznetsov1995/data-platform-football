"""
00:20 UTC tail of the SofaScore daily for its registry leagues (#1359).

The 14:00 daily (``dag_trigger_sofascore_daily``) collects yesterday's evening
matches of its registry leagues only the next afternoon — past their 24 h
deadline.  The refresh lane skips these leagues (they are configured for the
daily), so this DAG re-runs ``dag_ingest_sofascore`` just after midnight for
the same leagues, matches only (``run_players=false``), under its own run_id
(no ``reset_dag_run`` of the daily) and its own season freshness key, so
the schedule pages are read again, not reused from the daily's day key.  Same pool and gateway as the daily; the
refresh lane is not touched.

The league list is read from ``configs/sofascore/denominator.tsv`` (rows whose
basis is ``DAILY_TAIL_BASIS``), not repeated here.  ``dag_ingest_sofascore``
accepts ``conf.competition_ids`` only as a subset of its enabled catalog.

Removed together with the daily in #1370.
"""

from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.default_args import DEFAULT_ARGS

from scrapers.sofascore.denominator import load_denominator

DAILY_DAG_ID = "dag_ingest_sofascore"
DAILY_TAIL_BASIS = "registry:tournaments.json (дейли до #1370)"
# A daily run still going (or waiting for its slot) owns the same leagues;
# ``max_active_runs=1`` would only queue the tail behind it.
BUSY_STATES = ("running", "queued")

TAIL_ARGS = {
    **DEFAULT_ARGS,
    # A retried blocking trigger would start a second child run.
    "retries": 0,
}


def daily_tail_competition_ids() -> List[str]:
    """The daily's registry leagues, as the denominator file names them."""

    leagues = sorted(
        row.capture_key
        for row in load_denominator().rows.values()
        if row.basis == DAILY_TAIL_BASIS
    )
    if not leagues:
        raise AirflowException(
            f"denominator has no rows with basis {DAILY_TAIL_BASIS!r}"
        )
    return leagues


def _skip_if_daily_running(**_context) -> None:
    import logging

    from airflow.models import DagRun

    busy = [
        run.run_id
        for state in BUSY_STATES
        for run in DagRun.find(dag_id=DAILY_DAG_ID, state=state)
    ]
    if busy:
        message = (
            f"{DAILY_DAG_ID} is still running ({', '.join(busy)}); "
            "the daily tail is skipped this night"
        )
        logging.getLogger(__name__).warning(message)
        raise AirflowSkipException(message)


with DAG(
    dag_id="dag_trigger_sofascore_daily_tail",
    description=(
        "00:20 UTC matches-only tail of dag_ingest_sofascore for the daily's "
        "registry leagues (#1359, removed with the daily in #1370)"
    ),
    schedule="20 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    # The contour creates new DAGs paused (DAGS_ARE_PAUSED_AT_CREATION) and the
    # night delivery unpauses nothing: a paused tail would ship green and never
    # run (Astra #1359, finding 1).
    is_paused_upon_creation=False,
    default_args=TAIL_ARGS,
    tags=["sofascore", "orchestrator", "bronze"],
) as dag:
    skip_if_daily_running = PythonOperator(
        task_id="skip_if_daily_running",
        python_callable=_skip_if_daily_running,
    )

    trigger_daily_tail = TriggerDagRunOperator(
        task_id="trigger_sofascore_daily_tail",
        trigger_dag_id=DAILY_DAG_ID,
        trigger_run_id="tail__{{ ts_nodash }}",
        conf={
            "competition_ids": daily_tail_competition_ids(),
            "run_players": False,
        },
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
        execution_timeout=timedelta(hours=12),
        retries=0,
    )

    skip_if_daily_running >> trigger_daily_tail
