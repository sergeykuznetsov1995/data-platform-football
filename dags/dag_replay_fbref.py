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
    fbref_dag_failure_callback,
    initialize_fbref_run,
    parse_fbref_wave,
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


with DAG(
    dag_id="dag_replay_fbref",
    default_args=DEFAULT_ARGS,
    description="Raw-only, zero-network FBref parser replay",
    schedule=None,
    start_date=datetime(2026, 7, 11),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
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
    task and cannot construct the FBref transport.
    """,
) as dag:
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
            "reservation_mb": 7,
            "domain_interval_seconds": 3.0,
        },
        trigger_rule="all_success",
    )

    previous = initialize_run
    for wave_number in range(1, REPLAY_WAVE_COUNT + 1):
        parse = PythonOperator(
            task_id=f"parse_wave_{wave_number:02d}",
            python_callable=parse_fbref_wave,
            op_kwargs={
                "airflow_run_id": AIRFLOW_RUN_ID,
                "dag_id": DAG_ID,
                "page_kinds": REPLAY_PAGE_KINDS,
                "run_type": "replay",
                "source_control_run_id": (
                    "{{ params.source_control_run_id }}"
                ),
                "request_limit": 0,
                "byte_limit_mb": 0,
                "shard_size": REPLAY_SHARD_SIZE,
                "reservation_mb": 7,
            },
            trigger_rule="all_success",
        )
        previous >> parse
        previous = parse

    validate_run = PythonOperator(
        task_id="validate_run",
        python_callable=validate_fbref_run,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "source_control_run_id": "{{ params.source_control_run_id }}",
        },
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
