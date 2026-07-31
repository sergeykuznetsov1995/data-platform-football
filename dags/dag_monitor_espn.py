"""Network-free ESPN freshness and durable-health monitor."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.espn_native_tasks import check_36h_freshness_and_alerts


with DAG(
    dag_id="dag_monitor_espn",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["espn", "monitoring"],
) as dag:
    PythonOperator(
        task_id="check_36h_freshness_and_alerts",
        python_callable=check_36h_freshness_and_alerts,
        retries=0,
    )
