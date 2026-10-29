"""Daily bounded schedule compaction in the isolated SofaScore scheduler."""
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

from utils.default_args import DEFAULT_ARGS

with DAG(
    dag_id="dag_sofascore_schedule_maintenance",
    schedule="0 3 * * *",  # 06:00 Moscow, fixed UTC+3.
    start_date=datetime(2026, 10, 28, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    default_args={**DEFAULT_ARGS, "retries": 0},
    tags=["sofascore", "maintenance", "iceberg"],
) as dag:
    compact_schedule = BashOperator(
        task_id="compact_schedule",
        bash_command="cd /opt/airflow && python -m scrapers.sofascore.maintenance",
        execution_timeout=timedelta(minutes=59),
        do_xcom_push=False,
    )
