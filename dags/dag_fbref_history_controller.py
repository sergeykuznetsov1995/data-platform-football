"""One paused-by-default Airflow controller for the durable FBref history campaign."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator

from airflow.sensors.python import PythonSensor

from scrapers.fbref.settings import (
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    DEFAULT_REQUEST_RESERVATION_BYTES,
    MIB,
)

from utils.default_args import DEFAULT_ARGS
from utils.fbref_pipeline_tasks import (
    FBREF_CANARY_BYTE_LIMIT_MB,
    FBREF_CANARY_REQUEST_LIMIT,
    FBREF_PRODUCTION_BYTE_LIMIT_MB,
    FBREF_PRODUCTION_REQUEST_LIMIT,
    FBREF_SCRAPER_POOL,
    wait_fbref_publication_lock,
    audit_fbref_raw_integrity,
    capture_fbref_raw_baseline,
    choose_fbref_backfill_mode,
    choose_fbref_backfill_publication_path,
    export_fbref_publication_scope,
    fbref_dag_failure_callback,
    checkpoint_fbref_history_campaign,
    prepare_fbref_history_campaign,
    guard_fbref_history_slice,
    initialize_fbref_run,
    plan_fbref_backfill,
    run_recovery_wave,
    run_fbref_live_waves,
    seed_fbref_historical_seasons,
    validate_fbref_current_scope_freshness,
    validate_fbref_production_readiness,
    validate_fbref_run,
)


# History follows the same decision 3 as the daily lane: player pages and their
# match logs are out of scope, so the backfill must not claim or fetch them
# either (#1321).
BACKFILL_PAGE_KINDS = (
    "season",
    "season_stats",
    "schedule",
    "standings",
    "squad",
    "match",
)
BACKFILL_REQUEST_LIMIT = FBREF_PRODUCTION_REQUEST_LIMIT
BACKFILL_BYTE_LIMIT_MB = FBREF_PRODUCTION_BYTE_LIMIT_MB
DEFAULT_SHARD_SIZE = 1
MAX_SHARD_SIZE = 1
BACKFILL_MAX_BATCHES = 1

AIRFLOW_RUN_ID = "{{ run_id }}"
DAG_ID = "{{ dag.dag_id }}"
REQUEST_LIMIT = "{{ dag_run.conf.get('request_limit', params.request_limit) }}"
BYTE_LIMIT_MB = "{{ dag_run.conf.get('byte_limit_mb', params.byte_limit_mb) }}"
SHARD_SIZE = "{{ dag_run.conf.get('shard_size', params.shard_size) }}"
DRY_RUN = "{{ dag_run.conf.get('dry_run', params.dry_run) }}"
PUBLISH = "{{ dag_run.conf.get('publish', params.publish) }}"
MAX_BATCHES = "{{ dag_run.conf.get('max_batches', params.max_batches) }}"


with DAG(
    dag_id="dag_fbref_history_controller",
    default_args={
        **DEFAULT_ARGS,
        "pool": FBREF_SCRAPER_POOL,
        "priority_weight": 10,
        "weight_rule": "absolute",
    },
    description="Durable bounded FBref history controller",
    schedule="30 22 * * *",
    is_paused_upon_creation=True,
    start_date=datetime(2026, 7, 11, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=20),
    on_failure_callback=fbref_dag_failure_callback,
    render_template_as_native_obj=True,
    tags=["fbref", "bronze", "backfill", "raw-first"],
    params={
        "dry_run": Param(
            False,
            type="boolean",
            description=("Plan the next cohort without creating a run or using proxy"),
        ),
        "request_limit": Param(
            BACKFILL_REQUEST_LIMIT,
            type="integer",
            enum=[FBREF_CANARY_REQUEST_LIMIT, BACKFILL_REQUEST_LIMIT],
            description="Canary (100) or production safety circuit (4096)",
        ),
        "byte_limit_mb": Param(
            BACKFILL_BYTE_LIMIT_MB,
            type="integer",
            enum=[FBREF_CANARY_BYTE_LIMIT_MB, BACKFILL_BYTE_LIMIT_MB],
            description="Canary (50) or production safety circuit (2048 MiB)",
        ),
        "shard_size": Param(
            DEFAULT_SHARD_SIZE,
            type="integer",
            minimum=1,
            maximum=MAX_SHARD_SIZE,
            description="Maximum historical targets claimed by one task",
        ),
        "publish": Param(
            False,
            type="boolean",
            enum=[False],
            description=(
                "Keep false for the isolated historical lane. Set true "
                "explicitly only when this run should export scope and "
                "publish after validation"
            ),
        ),
        "max_batches": Param(
            BACKFILL_MAX_BATCHES,
            type="integer",
            minimum=1,
            maximum=BACKFILL_MAX_BATCHES,
            description="Hard ceiling of live batches for one run",
        ),
    },
    doc_md="""
    ## FBref durable history controller

    One new controller, initially paused and disabled by FBREF_HISTORY_CONTROLLER_ENABLED.
    No launch before accepted current prerequisites and explicit authorization.
    UTC slices at 22:30 advance one season and at most one live page
    plus one recovery page. Start-year order: 2026 through 2017 across the full
    adult men's registry, then deeper. Missing and current-owned seasons remain
    visible; current refresh owns its pages. Never launch the old driver alongside
    this DAG. Retries retain pinned membership and recover committed raw.
    Every writer shares the one-slot FBref pool. Current tasks and lock waiters
    precede history; waiting sensors reschedule. Admission rechecks after waiting,
    refuses unknown timing and includes measured fetch+parse duration.
    Twenty-minute run/lock limits and a 45-minute margin protect four current
    reservations. History stays nonpublishing. Silver is outside this controller.
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

    assert_history_window = PythonOperator(
        task_id="assert_history_window",
        python_callable=guard_fbref_history_slice,
        op_kwargs={},
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
            "reservation_mb": DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
            "domain_interval_seconds": DEFAULT_DOMAIN_INTERVAL_SECONDS,
            "publishing": PUBLISH,
        },
        trigger_rule="all_success",
    )

    acquire_publication_lock = PythonSensor(
        task_id="acquire_publication_lock",
        python_callable=wait_fbref_publication_lock,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "ttl_seconds": 20 * 60,
        },
        retries=0,
        trigger_rule="all_success",
        mode="reschedule",
        poke_interval=30,
        timeout=20 * 60,
        pool=FBREF_SCRAPER_POOL,
    )

    validate_freshness_preflight = PythonOperator(
        task_id="validate_current_scope_freshness_preflight",
        python_callable=validate_fbref_current_scope_freshness,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "run_type": "backfill",
            "fail_fast": False,
            "enforce": PUBLISH,
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
            "reservation_mb": DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
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
            "reservation_mb": DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
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
    validate_production_readiness >> assert_history_window >> initialize_run
    initialize_run >> validate_freshness_preflight
    validate_freshness_preflight >> acquire_publication_lock
    # Recovery drains raw BEFORE the seed, never after it.  The seed reopens a
    # scope-quarantined season (reconcile_frontier_scope) and gives it a
    # 'pending' run_target; that same step unhides the target's stale raw for
    # the recovery drain, which can retire it on bytes captured days earlier.
    # The retirement leaves the run_target open -- the contract quarantine only
    # touches page_frontier -- so the wave gate counted an unclaimable target as
    # unfinished and raised before the first request, turning the whole run into
    # zero work.  Draining first keeps the seed from handing the drain a target
    # it has just unhidden: raw still behind a quarantine at drain time stays
    # hidden from the drain, so no run_target is open when the verdict lands.
    # The live wave separately closes #1186 by refusing cross-run raw adoption
    # for season roots. Exact logical-refresh raw remains eligible so a crash
    # after raw commit still recovers without another paid request.
    prepare_campaign = PythonOperator(
        task_id="prepare_history_campaign",
        python_callable=prepare_fbref_history_campaign,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        trigger_rule="all_success",
    )
    acquire_publication_lock >> prepare_campaign >> capture_raw_baseline >> recover_raw
    recover_raw >> seed_historical_seasons
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
            "reservation_mb": DEFAULT_REQUEST_RESERVATION_BYTES // MIB,
            "domain_interval_seconds": DEFAULT_DOMAIN_INTERVAL_SECONDS,
            "max_batches": MAX_BATCHES,
            "deadline_seconds": 10 * 60,
        },
        pool=FBREF_SCRAPER_POOL,
        execution_timeout=timedelta(minutes=15),
        retries=0,
        trigger_rule="all_success",
    )
    seed_historical_seasons >> live_waves
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
            "enforce": PUBLISH,
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
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "publication_eligible": PUBLISH,
        },
        trigger_rule="all_success",
    )

    choose_publication_path = BranchPythonOperator(
        task_id="choose_publication_path",
        python_callable=choose_fbref_backfill_publication_path,
        op_kwargs={"publish": PUBLISH},
        trigger_rule="all_success",
    )

    release_publication_lock = PythonOperator(
        task_id="release_publication_lock",
        python_callable=checkpoint_fbref_history_campaign,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        retries=0,
        trigger_rule="all_done",
    )

    previous >> validate_freshness >> validate_run >> choose_publication_path
    choose_publication_path >> export_publication_scope >> release_publication_lock
    # A non-publishing historical run holds the lock for its own batches only,
    # then releases it without launching a downstream transform.
    choose_publication_path >> release_publication_lock
    for writer in (
        choose_mode,
        plan_backfill,
        validate_production_readiness,
        assert_history_window,
        initialize_run,
        acquire_publication_lock,
        validate_freshness_preflight,
        prepare_campaign,
        capture_raw_baseline,
        recover_raw,
        seed_historical_seasons,
        live_waves,
        audit_raw_integrity,
        validate_freshness,
        validate_run,
    ):
        writer >> release_publication_lock


__all__ = ["dag"]
