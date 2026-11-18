"""One disposable research task; never add this file to scheduler delivery."""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dag_ingest_clubelo",
    description="Isolated one-run ClubElo solver experiment",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["isolated-research"],
) as dag:
    check_watch = BashOperator(
        task_id="check_watch",
        bash_command="exec python -m scripts.research.clubelo_solver_canary run-once",
        retries=0,
        execution_timeout=timedelta(minutes=10),
        do_xcom_push=False,
        append_env=True,
    )
