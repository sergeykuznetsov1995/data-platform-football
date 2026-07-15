"""Network-disabled FBref parser replay DAG.

Replay reads immutable raw manifests from one completed source control run.
There are deliberately no fetch or discovery-seed tasks in this graph, and
the replay control run has zero request and byte budgets.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.default_args import DEFAULT_ARGS
from utils.fbref_pipeline_tasks import (
    acquire_fbref_publication_lock,
    audit_fbref_raw_integrity,
    capture_fbref_raw_baseline,
    export_fbref_publication_scope,
    fbref_dag_failure_callback,
    finalize_fbref_publication_lock,
    initialize_fbref_run,
    parse_fbref_wave,
    validate_fbref_production_readiness,
    validate_fbref_run,
)


REPLAY_PAGE_KINDS = (
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

# Current production runs are capped at 200 fetches. Eight offline shards of
# 25 drain one complete source run while memory stays bounded by one shard.
REPLAY_WAVE_COUNT = 8
REPLAY_SHARD_SIZE = 25

AIRFLOW_RUN_ID = "{{ run_id }}"
DAG_ID = "{{ dag.dag_id }}"
SOURCE_CONTROL_RUN_ID = (
    "{{ dag_run.conf.get('source_control_run_id', "
    "params.source_control_run_id) }}"
)


with DAG(
    dag_id="dag_replay_fbref",
    default_args=DEFAULT_ARGS,
    description="Raw-only, zero-network FBref parser replay",
    schedule=None,
    start_date=datetime(2026, 7, 11),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=18),
    on_failure_callback=fbref_dag_failure_callback,
    render_template_as_native_obj=True,
    tags=["fbref", "bronze", "replay", "network-disabled"],
    params={
        "source_control_run_id": Param(
            # Airflow 2.7 validates every Param while building DagBag and
            # cannot represent a no-default required field.  Nullable None is
            # import-safe; parse_fbref_wave rejects it before any work starts.
            default=None,
            type=["null", "string"],
            minLength=36,
            maxLength=36,
            pattern=(
                "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
                "[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
            ),
            description="Required crawl_run UUID whose raw manifests are replayed",
        ),
    },
    doc_md="""
    ## FBref parser replay

    `source_control_run_id` is required. The DAG creates a separate replay
    control run with request and byte limits set to zero, then processes only
    raw manifests missing the current parser version. It contains no fetch
    task and cannot construct the FBref transport. A before/after inventory
    gate proves replay created, deleted, or rewrote zero raw objects.
    """,
) as dag:
    validate_production_readiness = PythonOperator(
        task_id="validate_production_readiness",
        python_callable=validate_fbref_production_readiness,
        op_kwargs={
            "run_type": "replay",
            "request_limit": 0,
            "byte_limit_mb": 0,
            "shard_size": REPLAY_SHARD_SIZE,
        },
        trigger_rule="all_success",
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
            "shard_size": REPLAY_SHARD_SIZE,
            "reservation_mb": 3,
            "domain_interval_seconds": 3.0,
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

    capture_raw_baseline = PythonOperator(
        task_id="capture_raw_baseline",
        python_callable=capture_fbref_raw_baseline,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        trigger_rule="all_success",
    )

    validate_production_readiness >> initialize_run
    initialize_run >> acquire_publication_lock >> capture_raw_baseline
    previous = capture_raw_baseline
    for wave_number in range(1, REPLAY_WAVE_COUNT + 1):
        parse = PythonOperator(
            task_id=f"parse_wave_{wave_number:02d}",
            python_callable=parse_fbref_wave,
            op_kwargs={
                "airflow_run_id": AIRFLOW_RUN_ID,
                "dag_id": DAG_ID,
                "page_kinds": REPLAY_PAGE_KINDS,
                "run_type": "replay",
                "source_control_run_id": SOURCE_CONTROL_RUN_ID,
                "request_limit": 0,
                "byte_limit_mb": 0,
                "shard_size": REPLAY_SHARD_SIZE,
                "reservation_mb": 3,
            },
            trigger_rule="all_success",
        )
        previous >> parse
        previous = parse

    audit_raw_integrity = PythonOperator(
        task_id="audit_raw_integrity",
        python_callable=audit_fbref_raw_integrity,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "run_type": "replay",
            "source_control_run_id": SOURCE_CONTROL_RUN_ID,
        },
        trigger_rule="all_success",
    )
    previous >> audit_raw_integrity
    previous = audit_raw_integrity

    validate_run = PythonOperator(
        task_id="validate_run",
        python_callable=validate_fbref_run,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "source_control_run_id": SOURCE_CONTROL_RUN_ID,
        },
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
            "replay_source_control_run_id": SOURCE_CONTROL_RUN_ID,
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

    previous >> validate_run >> export_publication_scope >> trigger_silver
    trigger_silver >> release_publication_lock


__all__ = ["dag"]
