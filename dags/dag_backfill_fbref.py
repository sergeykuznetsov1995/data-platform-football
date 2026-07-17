"""Manual, bounded FBref historical backfill DAG.

The current-refresh DAG owns registry discovery.  This DAG takes the next
unfinished page of historical seasons from that durable registry and advances
their frontier under the same hard 200-request/100-MiB production budget as
current ingestion (or the hard 100/50 canary profile). Repeated manual runs
resume automatically from PostgreSQL and immutable raw storage; no league list,
operator cursor, or filesystem handoff is accepted.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scrapers.fbref.settings import DEFAULT_DOMAIN_INTERVAL_SECONDS

from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils.fbref_pipeline_tasks import (
    FBREF_CANARY_BYTE_LIMIT_MB,
    FBREF_CANARY_REQUEST_LIMIT,
    FBREF_MAX_WARM_SESSION_TARGETS,
    FBREF_PRODUCTION_BYTE_LIMIT_MB,
    FBREF_PRODUCTION_REQUEST_LIMIT,
    acquire_fbref_publication_lock,
    audit_fbref_raw_integrity,
    capture_fbref_raw_baseline,
    choose_fbref_backfill_mode,
    export_fbref_publication_scope,
    fbref_dag_failure_callback,
    finalize_fbref_publication_lock,
    initialize_fbref_run,
    plan_fbref_backfill,
    run_recovery_wave,
    run_fbref_live_waves,
    seed_fbref_historical_seasons,
    validate_fbref_current_scope_freshness,
    validate_fbref_production_readiness,
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
BACKFILL_REQUEST_LIMIT = FBREF_PRODUCTION_REQUEST_LIMIT
BACKFILL_BYTE_LIMIT_MB = FBREF_PRODUCTION_BYTE_LIMIT_MB
DEFAULT_SHARD_SIZE = FBREF_MAX_WARM_SESSION_TARGETS
MAX_SHARD_SIZE = FBREF_MAX_WARM_SESSION_TARGETS
BACKFILL_MAX_BATCHES = 16

AIRFLOW_RUN_ID = "{{ run_id }}"
DAG_ID = "{{ dag.dag_id }}"
REQUEST_LIMIT = (
    "{{ dag_run.conf.get('request_limit', params.request_limit) }}"
)
BYTE_LIMIT_MB = (
    "{{ dag_run.conf.get('byte_limit_mb', params.byte_limit_mb) }}"
)
SHARD_SIZE = "{{ dag_run.conf.get('shard_size', params.shard_size) }}"
DRY_RUN = "{{ dag_run.conf.get('dry_run', params.dry_run) }}"


with DAG(
    dag_id="dag_backfill_fbref",
    default_args=DEFAULT_ARGS,
    description="Manual bounded FBref historical backfill",
    schedule=None,
    start_date=datetime(2026, 7, 11),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=18),
    on_failure_callback=fbref_dag_failure_callback,
    render_template_as_native_obj=True,
    tags=["fbref", "bronze", "backfill", "raw-first"],
    params={
        "dry_run": Param(
            False,
            type="boolean",
            description=(
                "Plan the next cohort without creating a run or using proxy"
            ),
        ),
        "request_limit": Param(
            BACKFILL_REQUEST_LIMIT,
            type="integer",
            enum=[FBREF_CANARY_REQUEST_LIMIT, BACKFILL_REQUEST_LIMIT],
            description="Hard canary (100) or production (200) request cap",
        ),
        "byte_limit_mb": Param(
            BACKFILL_BYTE_LIMIT_MB,
            type="integer",
            enum=[FBREF_CANARY_BYTE_LIMIT_MB, BACKFILL_BYTE_LIMIT_MB],
            description="Hard canary (50) or production (100) MiB cap",
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
    non-current seasons from the source-discovered male registry, then runs
    up to sixteen raw-first batches in one warm process under a
    200-request/100-MiB hard cap.
    A `100/50` canary profile is available through Params or DagRun conf.
    Set `dry_run=true` to inspect the exact next cohort without creating a
    control run, opening a proxy session, or changing frontier state.
    Live mode checks current-scope freshness immediately after run creation,
    before seeding, raw recovery, or any paid fetch, and checks it again after
    all batches to catch drift during the run.
    Completed
    historical targets are never requeued; verified raw-v2 or raw-v1 content
    is imported into a new run without a network request. Run again to resume
    the next remaining cohort automatically. A pre-run content inventory and
    post-run raw-integrity artifact are mandatory before Silver publication.
    """,
) as dag:
    choose_mode = BranchPythonOperator(
        task_id="choose_backfill_mode",
        python_callable=choose_fbref_backfill_mode,
        op_kwargs={"dry_run": DRY_RUN},
        trigger_rule="all_success",
    )

    plan_backfill = PythonOperator(
        task_id="plan_backfill",
        python_callable=plan_fbref_backfill,
        op_kwargs={
            "request_limit": REQUEST_LIMIT,
            "byte_limit_mb": BYTE_LIMIT_MB,
            "shard_size": SHARD_SIZE,
        },
        trigger_rule="all_success",
    )

    validate_production_readiness = PythonOperator(
        task_id="validate_production_readiness",
        python_callable=validate_fbref_production_readiness,
        op_kwargs={
            "run_type": "backfill",
            "request_limit": REQUEST_LIMIT,
            "byte_limit_mb": BYTE_LIMIT_MB,
            "shard_size": SHARD_SIZE,
        },
        trigger_rule="all_success",
    )

    initialize_run = PythonOperator(
        task_id="initialize_run",
        python_callable=initialize_fbref_run,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "run_type": "backfill",
            "request_limit": REQUEST_LIMIT,
            "byte_limit_mb": BYTE_LIMIT_MB,
            "shard_size": SHARD_SIZE,
            "reservation_mb": 3,
            "domain_interval_seconds": DEFAULT_DOMAIN_INTERVAL_SECONDS,
        },
        trigger_rule="all_success",
    )

    acquire_publication_lock = PythonOperator(
        task_id="acquire_publication_lock",
        python_callable=acquire_fbref_publication_lock,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        retries=0,
        trigger_rule="all_success",
    )

    validate_freshness_preflight = PythonOperator(
        task_id="validate_current_scope_freshness_preflight",
        python_callable=validate_fbref_current_scope_freshness,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "run_type": "backfill",
            "fail_fast": False,
        },
        trigger_rule="all_success",
    )

    seed_historical_seasons = PythonOperator(
        task_id="seed_historical_seasons",
        python_callable=seed_fbref_historical_seasons,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "request_limit": REQUEST_LIMIT,
            "byte_limit_mb": BYTE_LIMIT_MB,
            "shard_size": SHARD_SIZE,
            "reservation_mb": 3,
        },
        trigger_rule="all_success",
    )

    recover_raw = PythonOperator(
        task_id="recover_raw_before_fetch",
        python_callable=run_recovery_wave,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "page_kinds": BACKFILL_PAGE_KINDS,
            "run_type": "backfill",
            "request_limit": REQUEST_LIMIT,
            "byte_limit_mb": BYTE_LIMIT_MB,
            "shard_size": SHARD_SIZE,
            "reservation_mb": 3,
        },
        trigger_rule="all_success",
    )

    capture_raw_baseline = PythonOperator(
        task_id="capture_raw_baseline",
        python_callable=capture_fbref_raw_baseline,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        trigger_rule="all_success",
    )

    choose_mode >> plan_backfill
    choose_mode >> validate_production_readiness
    validate_production_readiness >> initialize_run
    initialize_run >> validate_freshness_preflight
    validate_freshness_preflight >> acquire_publication_lock
    acquire_publication_lock >> seed_historical_seasons
    seed_historical_seasons >> capture_raw_baseline >> recover_raw
    live_waves = PythonOperator(
        task_id="run_live_waves",
        python_callable=run_fbref_live_waves,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "worker_id": "backfill-live:{{ run_id }}",
            "page_kinds": BACKFILL_PAGE_KINDS,
            "run_type": "backfill",
            "request_limit": REQUEST_LIMIT,
            "byte_limit_mb": BYTE_LIMIT_MB,
            "shard_size": SHARD_SIZE,
            "reservation_mb": 3,
            "domain_interval_seconds": DEFAULT_DOMAIN_INTERVAL_SECONDS,
            "max_batches": BACKFILL_MAX_BATCHES,
        },
        pool=INGEST_SCRAPER_POOL,
        execution_timeout=timedelta(minutes=120),
        retries=0,
        trigger_rule="all_success",
    )
    recover_raw >> live_waves
    audit_raw_integrity = PythonOperator(
        task_id="audit_raw_integrity",
        python_callable=audit_fbref_raw_integrity,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "run_type": "backfill",
        },
        trigger_rule="all_success",
    )
    live_waves >> audit_raw_integrity
    previous = audit_raw_integrity

    validate_freshness = PythonOperator(
        task_id="validate_current_scope_freshness",
        python_callable=validate_fbref_current_scope_freshness,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "run_type": "backfill",
            "fail_fast": True,
        },
        trigger_rule="all_success",
    )

    export_publication_scope = PythonOperator(
        task_id="export_publication_scope",
        python_callable=export_fbref_publication_scope,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        trigger_rule="all_success",
    )

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
        execution_date="{{ ti.start_date }}",
        conf={
            "fbref_source_dag_id": DAG_ID,
            "fbref_source_run_id": AIRFLOW_RUN_ID,
            "fbref_control_run_id": (
                "{{ ti.xcom_pull(task_ids='initialize_run') }}"
            ),
            "publication_scope": "fbref_silver_only",
            "trigger_xref": False,
        },
        wait_for_completion=True,
        reset_dag_run=False,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        execution_timeout=timedelta(hours=12),
        retries=0,
        trigger_rule="all_success",
    )

    release_publication_lock = PythonOperator(
        task_id="release_publication_lock",
        python_callable=finalize_fbref_publication_lock,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        retries=0,
        trigger_rule="all_done",
    )

    previous >> validate_freshness >> validate_run
    validate_run >> export_publication_scope >> trigger_silver
    trigger_silver >> release_publication_lock


__all__ = ["dag"]
