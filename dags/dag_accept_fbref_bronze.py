"""Manual, non-publishing live acceptance for FBref Raw and Bronze."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from utils.default_args import DEFAULT_ARGS, INGEST_SCRAPER_POOL
from utils.fbref_bronze_acceptance_tasks import (
    acquire_fbref_acceptance_publication_lock,
    audit_fbref_acceptance_raw,
    initialize_fbref_acceptance_run,
    prepare_fbref_acceptance_cohort,
    run_fbref_acceptance_live_wave,
    validate_fbref_acceptance_readiness,
    validate_fbref_acceptance_run,
)
from utils.fbref_pipeline_tasks import (
    capture_fbref_raw_baseline,
    fbref_dag_failure_callback,
    release_fbref_publication_lock,
)


AIRFLOW_RUN_ID = "{{ run_id }}"
DAG_ID = "{{ dag.dag_id }}"
SCOPE = "{{ dag_run.conf.get('scope', params.scope) }}"
EXPECTED_COHORT = "{{ ti.xcom_pull(task_ids='select_acceptance_cohort') }}"


with DAG(
    dag_id="dag_accept_fbref_bronze",
    default_args=DEFAULT_ARGS,
    description="Manual non-publishing FBref Raw/Bronze acceptance",
    schedule=None,
    start_date=datetime(2026, 7, 17),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=3),
    on_failure_callback=fbref_dag_failure_callback,
    render_template_as_native_obj=True,
    tags=["fbref", "raw", "bronze", "acceptance", "manual"],
    params={
        "scope": Param(
            "current",
            type="string",
            enum=["current", "history"],
            description="Live current sample or one historical male season",
        )
    },
    doc_md="""
    ## FBref Raw/Bronze acceptance

    Manual and non-publishing. It freezes one deterministic male cohort of at
    most 25 targets, then runs exactly one live batch under the immutable
    `100 requests / 50 MiB / shard 25` profile. `current` covers every
    supported page kind and season-stat route plus populated/empty players and
    full/sparse matches. `history` requires that evidence within one historical
    male season. This graph contains no Silver or Gold trigger.
    """,
) as dag:
    validate_production_readiness = PythonOperator(
        task_id="validate_production_readiness",
        python_callable=validate_fbref_acceptance_readiness,
        op_kwargs={"scope": SCOPE},
        retries=0,
        trigger_rule="all_success",
    )

    initialize_run = PythonOperator(
        task_id="initialize_run",
        python_callable=initialize_fbref_acceptance_run,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "scope": SCOPE,
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

    select_acceptance_cohort = PythonOperator(
        task_id="select_acceptance_cohort",
        python_callable=prepare_fbref_acceptance_cohort,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "scope": SCOPE,
        },
        retries=0,
        trigger_rule="all_success",
    )

    capture_raw_baseline = PythonOperator(
        task_id="capture_raw_baseline",
        python_callable=capture_fbref_raw_baseline,
        op_kwargs={"airflow_run_id": AIRFLOW_RUN_ID, "dag_id": DAG_ID},
        trigger_rule="all_success",
    )

    run_live_wave = PythonOperator(
        task_id="run_live_wave",
        python_callable=run_fbref_acceptance_live_wave,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "scope": SCOPE,
        },
        pool=INGEST_SCRAPER_POOL,
        execution_timeout=timedelta(minutes=120),
        retries=0,
        trigger_rule="all_success",
    )

    audit_raw_integrity = PythonOperator(
        task_id="audit_raw_integrity",
        python_callable=audit_fbref_acceptance_raw,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "scope": SCOPE,
        },
        trigger_rule="all_success",
    )

    validate_acceptance_run = PythonOperator(
        task_id="validate_acceptance_run",
        python_callable=validate_fbref_acceptance_run,
        op_kwargs={
            "airflow_run_id": AIRFLOW_RUN_ID,
            "dag_id": DAG_ID,
            "expected_cohort": EXPECTED_COHORT,
            "replay": False,
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
    initialize_run >> acquire_publication_lock >> select_acceptance_cohort
    select_acceptance_cohort >> capture_raw_baseline >> run_live_wave
    run_live_wave >> audit_raw_integrity >> validate_acceptance_run
    validate_acceptance_run >> release_publication_lock


__all__ = ["dag"]
