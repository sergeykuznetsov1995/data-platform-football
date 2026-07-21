"""Own the 14:00 UTC FotMob schedule in the isolated production stack.

``dag_ingest_fotmob`` is intentionally trigger-only because the shared stack
normally schedules it through the master DAG.  The isolated FotMob stack does
not load that master DAG, so this minimal owner is the only scheduled object.
It must be paused before moving the workload back to the shared scheduler.
"""

from datetime import datetime, timedelta, timezone
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.default_args import DEFAULT_ARGS
from utils.fotmob_publication import (
    attest_fotmob_isolated_runtime,
    fail_unsealed_fotmob_publication,
    fotmob_daily_trigger_conf,
    initialize_fotmob_publication,
)


INITIALIZER_TASK_ID = "initialize_fotmob_publication"
RUNTIME_ATTESTATION_TASK_ID = "attest_isolated_runtime"
ISOLATED_STACK_ENV = "FOTMOB_ISOLATED_STACK"
GENERATION_TEMPLATE = (
    "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
    "['generation_id'] }}"
)
BINDING_TEMPLATE = {
    "schema": (
        "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
        "['binding']['schema'] }}"
    ),
    "source": (
        "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
        "['binding']['source'] }}"
    ),
    "owner": (
        "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
        "['binding']['owner'] }}"
    ),
    "data_interval_start": (
        "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
        "['binding']['data_interval_start'] }}"
    ),
    "data_interval_end": (
        "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
        "['binding']['data_interval_end'] }}"
    ),
    "runtime_fingerprint": (
        "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
        "['binding']['runtime_fingerprint'] }}"
    ),
}


dag = None
if os.environ.get(ISOLATED_STACK_ENV) == "1":
    with DAG(
        dag_id="dag_trigger_fotmob_daily",
        description="Daily 14:00 UTC source-native FotMob trigger",
        schedule="0 14 * * *",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        default_args={**DEFAULT_ARGS, "retries": 0},
        tags=["fotmob", "orchestrator", "bronze"],
    ) as dag:
        attest_runtime = PythonOperator(
            task_id=RUNTIME_ATTESTATION_TASK_ID,
            python_callable=attest_fotmob_isolated_runtime,
            retries=0,
        )

        initialize_publication = PythonOperator(
            task_id=INITIALIZER_TASK_ID,
            python_callable=initialize_fotmob_publication,
            op_kwargs={"publication_owner": "isolated"},
            retries=0,
        )

        trigger_ingest = TriggerDagRunOperator(
            task_id="trigger_fotmob_ingest",
            trigger_dag_id="dag_ingest_fotmob",
            trigger_run_id="fotmob_ingest__" + GENERATION_TEMPLATE,
            logical_date="{{ logical_date.isoformat() }}",
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=["success"],
            failed_states=["failed"],
            reset_dag_run=False,
            conf={
                **fotmob_daily_trigger_conf(),
                "fotmob_publication": {
                    "generation_id": GENERATION_TEMPLATE,
                    "binding": BINDING_TEMPLATE,
                },
            },
            execution_timeout=timedelta(hours=14),
            retries=0,
        )

        finalize_publication = PythonOperator(
            task_id="finalize_fotmob_publication",
            python_callable=fail_unsealed_fotmob_publication,
            op_kwargs={
                "publication_owner": "isolated",
                "success_task_id": "trigger_fotmob_ingest",
                "writer_task_ids": ["trigger_fotmob_ingest"],
            },
            trigger_rule="all_done",
            retries=0,
        )

        (
            attest_runtime
            >> initialize_publication
            >> trigger_ingest
            >> finalize_publication
        )
