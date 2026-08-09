"""Airflow DAG owning the isolated 14:00 UTC ESPN daily child run."""

from datetime import datetime, timedelta, timezone
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scrapers.espn.daily_owner import (
    ESPN_ISOLATED_V1,
    ISOLATED_STACK_ENV,
    daily_child_run_id,
    daily_parent_envelope,
    validate_scheduled_owner,
)
from utils.default_args import DEFAULT_ARGS


PARENT_RUN_ID_TEMPLATE = "{{ run_id }}"
CHILD_RUN_ID_TEMPLATE = daily_child_run_id(PARENT_RUN_ID_TEMPLATE)
PARENT_ENVELOPE_TEMPLATE = daily_parent_envelope(
    parent_run_id=PARENT_RUN_ID_TEMPLATE,
    logical_date="{{ logical_date.isoformat() }}",
    data_interval_start="{{ data_interval_start.isoformat() }}",
    data_interval_end="{{ data_interval_end.isoformat() }}",
)


dag = None
if os.environ.get(ISOLATED_STACK_ENV) == "1":
    with DAG(
        dag_id=ESPN_ISOLATED_V1.parent_dag_id,
        description="Daily 14:00 UTC isolated ESPN Native Bronze owner",
        schedule="0 14 * * *",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        default_args={**DEFAULT_ARGS, "retries": 0},
        tags=["espn", "orchestrator", "bronze", "isolated"],
    ) as dag:
        validate_owner = PythonOperator(
            task_id="validate_scheduled_owner",
            python_callable=validate_scheduled_owner,
            retries=0,
        )

        trigger_ingest = TriggerDagRunOperator(
            task_id=ESPN_ISOLATED_V1.trigger_task_id,
            trigger_dag_id=ESPN_ISOLATED_V1.child_dag_id,
            trigger_run_id=CHILD_RUN_ID_TEMPLATE,
            logical_date="{{ logical_date.isoformat() }}",
            conf={"espn_parent": PARENT_ENVELOPE_TEMPLATE},
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=["success"],
            failed_states=["failed"],
            reset_dag_run=False,
            execution_timeout=timedelta(hours=12),
            retries=0,
        )

        validate_owner >> trigger_ingest
