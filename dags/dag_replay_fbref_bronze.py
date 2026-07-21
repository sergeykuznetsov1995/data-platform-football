"""Manual zero-network replay of one successful FBref acceptance run."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from utils.default_args import DEFAULT_ARGS
from utils.fbref_bronze_acceptance_tasks import (
    acquire_fbref_acceptance_publication_lock,
    initialize_fbref_acceptance_replay_run,
    parse_fbref_acceptance_replay,
    validate_fbref_acceptance_replay_readiness,
    validate_fbref_acceptance_run,
)
from utils.fbref_pipeline_tasks import (
    audit_fbref_raw_integrity,
    capture_fbref_raw_baseline,
    fbref_dag_failure_callback,
    release_fbref_publication_lock,
)


AIRFLOW_RUN_ID = "{{ run_id }}"
DAG_ID = "{{ dag.dag_id }}"
SOURCE_CONTROL_RUN_ID = (
    "{{ dag_run.conf.get('source_control_run_id', "
    "params.source_control_run_id) }}"
)


with DAG(
    dag_id="dag_replay_fbref_bronze",
    default_args=DEFAULT_ARGS,
    description="Zero-network replay of one FBref Bronze acceptance run",
    schedule=None,
    start_date=datetime(2026, 7, 17),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=3),
    on_failure_callback=fbref_dag_failure_callback,
    render_template_as_native_obj=True,
    tags=["fbref", "raw", "bronze", "acceptance", "replay", "offline"],
    params={
        "source_control_run_id": Param(
            default=None,
            type=["null", "string"],
            minLength=36,
            maxLength=36,
            pattern=(
                "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
                "[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
            ),
            description="Required successful acceptance crawl_run UUID",
        )
    },
    doc_md="""
    ## FBref Bronze acceptance replay

    `source_control_run_id` is required and must identify a successful live
    acceptance run. This DAG has zero request and byte budgets, contains no
    fetch or seed tasks, parses the frozen source cohort once, and requires a
    zero-delta Raw inventory audit. It contains no Silver or Gold trigger.
    """,
) as dag:
    validate_production_readiness = PythonOperator(
        task_id="validate_production_readiness",
        python_callable=validate_fbref_acceptance_replay_readiness,
        retries=0,
        trigger_rule="all_success",
    )

    initialize_run = PythonOperator(
        task_id="initialize_run",
        python_callable=initialize_fbref_acceptance_replay_run,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "source_control_run_id": SOURCE_CONTROL_RUN_ID,
        },
        retries=0,
        trigger_rule="all_success",
    )

    acquire_publication_lock = PythonOperator(
        task_id="acquire_publication_lock",
        python_callable=acquire_fbref_acceptance_publication_lock,
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

    parse_source_cohort = PythonOperator(
        task_id="parse_source_cohort",
        python_callable=parse_fbref_acceptance_replay,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "source_control_run_id": SOURCE_CONTROL_RUN_ID,
        },
        retries=0,
        trigger_rule="all_success",
    )

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

    validate_acceptance_run = PythonOperator(
        task_id="validate_acceptance_run",
        python_callable=validate_fbref_acceptance_run,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "source_control_run_id": SOURCE_CONTROL_RUN_ID,
            "replay": True,
        },
        retries=0,
        trigger_rule="all_success",
    )

    release_publication_lock = PythonOperator(
        task_id="release_publication_lock",
        python_callable=release_fbref_publication_lock,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        retries=0,
        trigger_rule="all_done",
    )

    validate_production_readiness >> initialize_run
    initialize_run >> acquire_publication_lock >> capture_raw_baseline
    capture_raw_baseline >> parse_source_cohort >> audit_raw_integrity
    audit_raw_integrity >> validate_acceptance_run >> release_publication_lock


__all__ = ["dag"]
