"""Publish C1 inputs separately from the morning SELECT; bootstrap is manual."""
from datetime import datetime, timedelta, timezone
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from scrapers.fotmob.c1_inputs import run_refresh
from utils.default_args import DEFAULT_ARGS


dag = None
if os.environ.get("FOTMOB_ISOLATED_STACK") == "1":
    with DAG(
        dag_id="dag_refresh_fotmob_c1",
        description="Incrementally publish complete FotMob C1 day inputs",
        default_args={**DEFAULT_ARGS, "retries": 0},
        schedule="15 * * * *",
        start_date=datetime(2026, 10, 28, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["fotmob", "bronze", "c1"],
    ) as dag:
        PythonOperator(
            task_id="refresh_c1_inputs",
            python_callable=run_refresh,
            execution_timeout=timedelta(minutes=30),
            retries=0,
        )
