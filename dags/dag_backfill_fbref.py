"""Manual, bounded FBref historical backfill DAG.

The current-refresh DAG owns registry discovery.  This DAG takes the next
unfinished page of historical seasons from that durable registry and advances
their frontier for at most 25 total requests.  Repeated manual runs resume
automatically from PostgreSQL and immutable raw storage; no league list,
operator cursor, or filesystem handoff is accepted.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils.fbref_pipeline_tasks import (
    fetch_fbref_wave,
    fbref_dag_failure_callback,
    initialize_fbref_run,
    parse_fbref_wave,
    seed_fbref_historical_seasons,
    validate_fbref_run,
)


BACKFILL_PAGE_KINDS = (
    "season",
    "season_stats",
    "schedule",
    "standings",
    "squad",
    "player",
    "matchlog",
    "match",
)
BACKFILL_REQUEST_LIMIT = 25
BACKFILL_BYTE_LIMIT_MB = 100
MIN_LIVE_REQUEST_LIMIT = 22  # 20 browser bootstrap + 2 bounded HTTP attempts
MIN_LIVE_BYTE_LIMIT_MB = 7   # one complete per-target reservation
DEFAULT_SHARD_SIZE = 2
MAX_SHARD_SIZE = 2
# One-target shards can still consume the complete bounded request allowance.
BACKFILL_WAVE_COUNT = BACKFILL_REQUEST_LIMIT

AIRFLOW_RUN_ID = "{{ run_id }}"
DAG_ID = "{{ dag.dag_id }}"


with DAG(
    dag_id="dag_backfill_fbref",
    default_args=DEFAULT_ARGS,
    description="Manual bounded FBref historical backfill",
    schedule=None,
    start_date=datetime(2026, 7, 11),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    on_failure_callback=fbref_dag_failure_callback,
    render_template_as_native_obj=True,
    tags=["fbref", "bronze", "backfill", "raw-first"],
    params={
        "request_limit": Param(
            BACKFILL_REQUEST_LIMIT,
            type="integer",
            minimum=MIN_LIVE_REQUEST_LIMIT,
            maximum=BACKFILL_REQUEST_LIMIT,
            description="Hard request cap for one manual backfill batch",
        ),
        "byte_limit_mb": Param(
            BACKFILL_BYTE_LIMIT_MB,
            type="integer",
            minimum=MIN_LIVE_BYTE_LIMIT_MB,
            maximum=BACKFILL_BYTE_LIMIT_MB,
            description="Hard provider-billed byte cap in MiB",
        ),
        "shard_size": Param(
            DEFAULT_SHARD_SIZE,
            type="integer",
            minimum=1,
            maximum=MAX_SHARD_SIZE,
            description="Maximum historical targets claimed by one task",
        ),
    },
    doc_md="""
    ## FBref historical backfill

    Manual only. The DAG selects the next bounded unfinished page of
    non-current seasons from the source-discovered male registry, then runs up
    to 25 sequential raw-first waves under a 25-request hard cap. Completed
    historical targets are never requeued; verified raw-v2 or raw-v1 content
    is imported into a new run without a network request. Run again to resume
    the next remaining cohort automatically.
    """,
) as dag:
    initialize_run = PythonOperator(
        task_id="initialize_run",
        python_callable=initialize_fbref_run,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "run_type": "backfill",
            "request_limit": "{{ params.request_limit }}",
            "byte_limit_mb": "{{ params.byte_limit_mb }}",
            "shard_size": "{{ params.shard_size }}",
            "reservation_mb": 7,
            "domain_interval_seconds": 3.0,
        },
        trigger_rule="all_success",
    )

    seed_historical_seasons = PythonOperator(
        task_id="seed_historical_seasons",
        python_callable=seed_fbref_historical_seasons,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "request_limit": "{{ params.request_limit }}",
            "byte_limit_mb": "{{ params.byte_limit_mb }}",
            "shard_size": "{{ params.shard_size }}",
            "reservation_mb": 7,
        },
        trigger_rule="all_success",
    )

    initialize_run >> seed_historical_seasons
    previous = seed_historical_seasons
    for wave_number in range(1, BACKFILL_WAVE_COUNT + 1):
        fetch = PythonOperator(
            task_id=f"fetch_wave_{wave_number:02d}",
            python_callable=fetch_fbref_wave,
            op_kwargs={
                "airflow_run_id": AIRFLOW_RUN_ID,
                "dag_id": DAG_ID,
                "worker_id": (
                    f"backfill-wave-{wave_number:02d}:{{{{ run_id }}}}"
                ),
                "page_kinds": BACKFILL_PAGE_KINDS,
                "run_type": "backfill",
                "request_limit": "{{ params.request_limit }}",
                "byte_limit_mb": "{{ params.byte_limit_mb }}",
                "shard_size": "{{ params.shard_size }}",
                "reservation_mb": 7,
                "domain_interval_seconds": 3.0,
            },
            pool=INGEST_SCRAPER_POOL,
            trigger_rule="all_success",
        )
        parse = PythonOperator(
            task_id=f"parse_wave_{wave_number:02d}",
            python_callable=parse_fbref_wave,
            op_kwargs={
                "airflow_run_id": AIRFLOW_RUN_ID,
                "dag_id": DAG_ID,
                "page_kinds": BACKFILL_PAGE_KINDS,
                "run_type": "backfill",
                "request_limit": "{{ params.request_limit }}",
                "byte_limit_mb": "{{ params.byte_limit_mb }}",
                "shard_size": "{{ params.shard_size }}",
                "reservation_mb": 7,
            },
            trigger_rule="all_success",
        )
        previous >> fetch >> parse
        previous = parse

    validate_run = PythonOperator(
        task_id="validate_run",
        python_callable=validate_fbref_run,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        trigger_rule="all_success",
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="dag_transform_fbref_silver",
        trigger_run_id="fbref_silver__{{ dag.dag_id }}__{{ run_id }}",
        logical_date="{{ ti.start_date }}",
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        execution_timeout=timedelta(hours=12),
        retries=0,
        trigger_rule="all_success",
    )

    previous >> validate_run >> trigger_silver


__all__ = ["dag"]
