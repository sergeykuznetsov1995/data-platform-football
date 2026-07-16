"""Build the scheduled and manual-only FBref current raw-first DAGs."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils.fbref_pipeline_tasks import (
    FBREF_CANARY_BYTE_LIMIT_MB,
    FBREF_CANARY_REQUEST_LIMIT,
    FBREF_BOOTSTRAP_DAG_ID,
    FBREF_MAX_WARM_SESSION_TARGETS,
    FBREF_PRODUCTION_BYTE_LIMIT_MB,
    FBREF_PRODUCTION_REQUEST_LIMIT,
    acquire_fbref_publication_lock,
    audit_fbref_raw_integrity,
    capture_fbref_raw_baseline,
    choose_fbref_publication_path,
    export_fbref_publication_scope,
    fbref_dag_failure_callback,
    finalize_fbref_publication_lock,
    initialize_fbref_run,
    release_fbref_publication_lock,
    run_fbref_live_waves,
    run_recovery_wave,
    seed_fbref_competition_index,
    validate_fbref_bootstrap_run,
    validate_fbref_current_scope_freshness,
    validate_fbref_production_readiness,
    validate_fbref_run,
)


INGEST_DAG_ID = "dag_ingest_fbref"
BOOTSTRAP_DAG_ID = FBREF_BOOTSTRAP_DAG_ID

PAGE_KINDS = (
    "competition_index",
    "competition",
    "season",
    "season_stats",
    "schedule",
    "standings",
    "squad",
    "player",
    "matchlog",
    "match",
)

# One unforked process advances bounded raw-first batches while retaining the
# same clearance and proxy quarantine for the run.
CURRENT_MAX_BATCHES = 16
CURRENT_REQUEST_LIMIT = FBREF_PRODUCTION_REQUEST_LIMIT
CURRENT_BYTE_LIMIT_MB = FBREF_PRODUCTION_BYTE_LIMIT_MB
DEFAULT_SHARD_SIZE = FBREF_MAX_WARM_SESSION_TARGETS
MAX_SHARD_SIZE = FBREF_MAX_WARM_SESSION_TARGETS

AIRFLOW_RUN_ID = "{{ run_id }}"
DAG_ID = "{{ dag.dag_id }}"
DAG_RUN_TYPE = "{{ dag_run.run_type }}"
REQUEST_LIMIT = (
    "{{ dag_run.conf.get('request_limit', params.request_limit) }}"
)
BYTE_LIMIT_MB = (
    "{{ dag_run.conf.get('byte_limit_mb', params.byte_limit_mb) }}"
)
SHARD_SIZE = "{{ dag_run.conf.get('shard_size', params.shard_size) }}"


def _scheduled_params() -> dict:
    return {
        "request_limit": Param(
            CURRENT_REQUEST_LIMIT,
            type="integer",
            enum=[FBREF_CANARY_REQUEST_LIMIT, CURRENT_REQUEST_LIMIT],
            description="Hard canary (100) or production (200) request cap",
        ),
        "byte_limit_mb": Param(
            CURRENT_BYTE_LIMIT_MB,
            type="integer",
            enum=[FBREF_CANARY_BYTE_LIMIT_MB, CURRENT_BYTE_LIMIT_MB],
            description="Hard canary (50) or production (100) MiB cap",
        ),
        "shard_size": Param(
            DEFAULT_SHARD_SIZE,
            type="integer",
            minimum=1,
            maximum=MAX_SHARD_SIZE,
            description="Maximum frontier targets claimed by one task",
        ),
    }


def build_fbref_current_dag(*, bootstrap_only: bool) -> DAG:
    """Create one current-scope DAG without duplicating its raw-first spine."""

    if bootstrap_only:
        dag_id = BOOTSTRAP_DAG_ID
        schedule = None
        dagrun_timeout = timedelta(hours=3)
        request_limit = FBREF_PRODUCTION_REQUEST_LIMIT
        byte_limit_mb = FBREF_PRODUCTION_BYTE_LIMIT_MB
        shard_size = FBREF_MAX_WARM_SESSION_TARGETS
        description = "Manual non-publishing FBref current bootstrap"
        tags = ["fbref", "bronze", "raw-first", "bootstrap", "manual"]
        doc_md = """
        ## FBref manual bootstrap

        This DAG has no schedule and is safe to leave unpaused. It always uses
        the exact production `200 requests / 100 MiB / shard 25` profile. It
        performs raw recovery, live fetch, parse, and integrity validation,
        then releases the publication lock. Freshness, scope export, canary,
        and Silver tasks do not exist in this DAG.
        """
    else:
        dag_id = INGEST_DAG_ID
        schedule = "0 6 * * *"
        dagrun_timeout = timedelta(hours=18)
        request_limit = REQUEST_LIMIT
        byte_limit_mb = BYTE_LIMIT_MB
        shard_size = SHARD_SIZE
        description = "Durable raw-first FBref current refresh"
        tags = ["fbref", "bronze", "raw-first", "discovery"]
        doc_md = """
        ## FBref current refresh

        The source-discovered competition registry decides scope. Female
        competitions are recorded but never added to the crawl frontier;
        unknown gender is quarantined. Every network task is bounded by the
        shared PostgreSQL request/byte budget and commits raw bytes before
        parsing. Silver starts only after final completeness/traffic
        validation passes. DagRun conf may select only the measured `100/50`
        canary profile or the default `200/100` production profile; every warm
        session claims at most 25 targets. A content-hashed raw inventory is
        captured before recovery/fetch, and publication is gated by a
        persisted integrity artifact. ALERT_ENV must be `prod` before the
        control run is created.
        """

    dag_kwargs = {
        "dag_id": dag_id,
        "default_args": DEFAULT_ARGS,
        "description": description,
        "schedule": schedule,
        "start_date": datetime(2026, 7, 11),
        "catchup": False,
        "max_active_runs": 1,
        "max_active_tasks": 1,
        "dagrun_timeout": dagrun_timeout,
        "on_failure_callback": fbref_dag_failure_callback,
        "render_template_as_native_obj": True,
        "tags": tags,
        "doc_md": doc_md,
    }
    if bootstrap_only:
        # schedule=None makes unpausing safe: no automatic DagRun can appear.
        dag_kwargs["is_paused_upon_creation"] = False
    else:
        dag_kwargs["params"] = _scheduled_params()

    with DAG(**dag_kwargs) as dag:
        readiness_kwargs = {
            "run_type": "current",
            "request_limit": request_limit,
            "byte_limit_mb": byte_limit_mb,
            "shard_size": shard_size,
        }
        initialize_kwargs = {
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "run_type": "current",
            "request_limit": request_limit,
            "byte_limit_mb": byte_limit_mb,
            "shard_size": shard_size,
            "reservation_mb": 3,
            "domain_interval_seconds": 3.0,
        }
        if bootstrap_only:
            readiness_kwargs.update(
                bootstrap_only=True,
                dag_run_type=DAG_RUN_TYPE,
            )
            initialize_kwargs.update(
                bootstrap_only=True,
                dag_run_type=DAG_RUN_TYPE,
            )

        validate_production_readiness = PythonOperator(
            task_id="validate_production_readiness",
            python_callable=validate_fbref_production_readiness,
            op_kwargs=readiness_kwargs,
            trigger_rule="all_success",
        )

        initialize_run = PythonOperator(
            task_id="initialize_run",
            python_callable=initialize_fbref_run,
            op_kwargs=initialize_kwargs,
            trigger_rule="all_success",
        )

        acquire_publication_lock = PythonOperator(
            task_id="acquire_publication_lock",
            python_callable=acquire_fbref_publication_lock,
            op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
            retries=0,
            trigger_rule="all_success",
        )

        seed_competition_index = PythonOperator(
            task_id="seed_competition_index",
            python_callable=seed_fbref_competition_index,
            op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
            trigger_rule="all_success",
        )

        capture_raw_baseline = PythonOperator(
            task_id="capture_raw_baseline",
            python_callable=capture_fbref_raw_baseline,
            op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
            trigger_rule="all_success",
        )

        recover_raw = PythonOperator(
            task_id="recover_raw_before_fetch",
            python_callable=run_recovery_wave,
            op_kwargs={
                "airflow_run_id": AIRFLOW_RUN_ID,
                "dag_id": DAG_ID,
                "page_kinds": PAGE_KINDS,
                "run_type": "current",
                "request_limit": request_limit,
                "byte_limit_mb": byte_limit_mb,
                "shard_size": shard_size,
                "reservation_mb": 3,
            },
            trigger_rule="all_success",
        )

        live_waves = PythonOperator(
            task_id="run_live_waves",
            python_callable=run_fbref_live_waves,
            op_kwargs={
                "airflow_run_id": AIRFLOW_RUN_ID,
                "dag_id": DAG_ID,
                "worker_id": "current-live:{{ run_id }}",
                "page_kinds": PAGE_KINDS,
                "run_type": "current",
                "request_limit": request_limit,
                "byte_limit_mb": byte_limit_mb,
                "shard_size": shard_size,
                "reservation_mb": 3,
                "domain_interval_seconds": 3.0,
                "max_batches": CURRENT_MAX_BATCHES,
            },
            pool=INGEST_SCRAPER_POOL,
            execution_timeout=timedelta(minutes=120),
            retries=0,
            trigger_rule="all_success",
        )

        audit_raw_integrity = PythonOperator(
            task_id="audit_raw_integrity",
            python_callable=audit_fbref_raw_integrity,
            op_kwargs={
                "airflow_run_id": AIRFLOW_RUN_ID,
                "dag_id": DAG_ID,
                "run_type": "current",
            },
            trigger_rule="all_success",
        )

        validate_production_readiness >> initialize_run
        initialize_run >> acquire_publication_lock >> seed_competition_index
        seed_competition_index >> capture_raw_baseline >> recover_raw
        recover_raw >> live_waves >> audit_raw_integrity

        release_publication_lock = PythonOperator(
            task_id="release_publication_lock",
            python_callable=finalize_fbref_publication_lock,
            op_kwargs={
                "airflow_run_id": AIRFLOW_RUN_ID,
                "dag_id": DAG_ID,
                **({"bootstrap_only": True} if bootstrap_only else {}),
            },
            retries=0,
            trigger_rule="all_done",
        )

        if bootstrap_only:
            validate_bootstrap = PythonOperator(
                task_id="validate_bootstrap_run",
                python_callable=validate_fbref_bootstrap_run,
                op_kwargs={
                    "airflow_run_id": AIRFLOW_RUN_ID,
                    "dag_id": DAG_ID,
                    "bootstrap_only": True,
                    "dag_run_type": DAG_RUN_TYPE,
                    "request_limit": request_limit,
                    "byte_limit_mb": byte_limit_mb,
                    "shard_size": shard_size,
                },
                retries=0,
                trigger_rule="all_success",
            )
            release_bootstrap_lock = PythonOperator(
                task_id="release_bootstrap_publication_lock",
                python_callable=release_fbref_publication_lock,
                op_kwargs={
                    "airflow_run_id": AIRFLOW_RUN_ID,
                    "dag_id": DAG_ID,
                },
                trigger_rule="all_success",
            )
            audit_raw_integrity >> validate_bootstrap >> release_bootstrap_lock
            release_bootstrap_lock >> release_publication_lock
        else:
            choose_path = BranchPythonOperator(
                task_id="choose_publication_path",
                python_callable=choose_fbref_publication_path,
                op_kwargs={
                    "request_limit": request_limit,
                    "byte_limit_mb": byte_limit_mb,
                },
                trigger_rule="all_success",
            )
            validate_canary = PythonOperator(
                task_id="validate_canary_run",
                python_callable=validate_fbref_run,
                op_kwargs={
                    "airflow_run_id": AIRFLOW_RUN_ID,
                    "dag_id": DAG_ID,
                    "publication_eligible": False,
                },
                retries=0,
                trigger_rule="all_success",
            )
            release_canary_lock = PythonOperator(
                task_id="release_canary_publication_lock",
                python_callable=release_fbref_publication_lock,
                op_kwargs={
                    "airflow_run_id": AIRFLOW_RUN_ID,
                    "dag_id": DAG_ID,
                },
                trigger_rule="all_success",
            )
            validate_freshness = PythonOperator(
                task_id="validate_current_scope_freshness",
                python_callable=validate_fbref_current_scope_freshness,
                op_kwargs={
                    "airflow_run_id": AIRFLOW_RUN_ID,
                    "dag_id": DAG_ID,
                    "run_type": "current",
                    "fail_fast": True,
                },
                trigger_rule="all_success",
            )
            validate_run = PythonOperator(
                task_id="validate_run",
                python_callable=validate_fbref_run,
                op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
                trigger_rule="all_success",
            )
            export_publication_scope = PythonOperator(
                task_id="export_publication_scope",
                python_callable=export_fbref_publication_scope,
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

            audit_raw_integrity >> choose_path
            choose_path >> validate_canary >> release_canary_lock
            choose_path >> validate_freshness >> validate_run
            validate_run >> export_publication_scope >> trigger_silver
            trigger_silver >> release_publication_lock
            release_canary_lock >> release_publication_lock

    return dag


__all__ = [
    "BOOTSTRAP_DAG_ID",
    "CURRENT_MAX_BATCHES",
    "INGEST_DAG_ID",
    "PAGE_KINDS",
    "build_fbref_current_dag",
]
