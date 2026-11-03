"""Network-disabled FBref re-parse of raw left behind by a parser bump.

Each run takes the newest raw of every target that only an older parser
version has parsed, from both lanes, and writes it to Bronze in large groups
(#1332).  Like replay it has no fetch task and zero request/byte budgets; the
run counts in neither current nor history metrics.  It queues for the
publication lock behind current refresh, does nothing in a current start or
history admission window, and stops early enough before the next one.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from scrapers.fbref.settings import DEFAULT_REQUEST_RESERVATION_BYTES, MIB

from utils.default_args import DEFAULT_ARGS
from utils.fbref_pipeline_tasks import (
    FBREF_REPARSE_DAG_ID,
    FBREF_REPARSE_PAGE_KINDS,
    FBREF_SCRAPER_POOL,
    drain_fbref_reparse,
    fbref_dag_failure_callback,
    finalize_fbref_reparse_lock,
    initialize_fbref_run,
    validate_fbref_production_readiness,
    validate_fbref_run,
    wait_fbref_reparse_lock,
)


REPARSE_PAGE_KINDS = FBREF_REPARSE_PAGE_KINDS

REPARSE_SHARD_SIZE = 25
REPARSE_LOCK_TTL_SECONDS = 3 * 60 * 60
# Gives the hour back before the next run queues afresh.
REPARSE_LOCK_WAIT_SECONDS = 50 * 60

AIRFLOW_RUN_ID = "{{ run_id }}"
DAG_ID = "{{ dag.dag_id }}"


with DAG(
    dag_id=FBREF_REPARSE_DAG_ID,
    default_args={**DEFAULT_ARGS, "pool": FBREF_SCRAPER_POOL,
                  "priority_weight": 1, "weight_rule": "absolute"},
    description="Raw-only, zero-network FBref parser-version re-parse",
    schedule="40 * * * *",
    start_date=datetime(2026, 11, 3),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(hours=5),
    on_failure_callback=fbref_dag_failure_callback,
    tags=["fbref", "bronze", "reparse", "network-disabled"],
    doc_md="""
    ## FBref version re-parse (#1332)

    Paused on creation: the owner switches it on. Each hourly run creates a
    zero-budget replay control run (`execution_mode=version_reparse`), waits
    for the publication lock behind current refresh, re-parses the newest
    version-lagging raw of each target from storage in large groups until
    its window closes, validates and releases the lock. It cannot construct
    the FBref transport.
    """,
) as dag:
    validate_production_readiness = PythonOperator(
        task_id="validate_production_readiness",
        python_callable=validate_fbref_production_readiness,
        op_kwargs={
            "run_type": "replay",
            "request_limit": 0,
            "byte_limit_mb": 0,
            "shard_size": REPARSE_SHARD_SIZE,
        },
    )

    initialize_run = PythonOperator(
        task_id="initialize_run",
        python_callable=initialize_fbref_run,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "run_type": "replay",
            "request_limit": 0,
            "byte_limit_mb": 0,
            "shard_size": REPARSE_SHARD_SIZE,
            "reservation_mb": DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
            "version_reparse": True,
        },
    )

    acquire_publication_lock = PythonSensor(
        task_id="acquire_publication_lock",
        python_callable=wait_fbref_reparse_lock,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID,
                   "ttl_seconds": REPARSE_LOCK_TTL_SECONDS},
        retries=0,
        mode="reschedule",
        poke_interval=60,
        timeout=REPARSE_LOCK_WAIT_SECONDS,
        soft_fail=True,
    )

    drain_reparse = PythonOperator(
        task_id="drain_reparse",
        python_callable=drain_fbref_reparse,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "page_kinds": REPARSE_PAGE_KINDS,
            "shard_size": REPARSE_SHARD_SIZE,
            "reservation_mb": DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
            "lock_ttl_seconds": REPARSE_LOCK_TTL_SECONDS,
        },
    )

    validate_run = PythonOperator(
        task_id="validate_run",
        python_callable=validate_fbref_run,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
    )

    release_publication_lock = PythonOperator(
        task_id="release_publication_lock",
        python_callable=finalize_fbref_reparse_lock,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        retries=0,
        trigger_rule="all_done",
    )

    (
        validate_production_readiness
        >> initialize_run
        >> acquire_publication_lock
        >> drain_reparse
        >> validate_run
        >> release_publication_lock
    )


__all__ = ["dag"]
