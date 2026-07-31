"""Weekly observational ESPN registry discovery; never auto-promotes."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.espn_native_tasks import (
    HTTP_POOL,
    fetch_discovery_catalog,
    write_reviewable_discovery_diff,
)


with DAG(
    dag_id="dag_discover_espn_registry",
    schedule="0 7 * * 1",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["espn", "discovery"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_discovery_catalog",
        python_callable=fetch_discovery_catalog,
        pool=HTTP_POOL,
        pool_slots=1,
        retries=1,
        multiple_outputs=True,
    )
    review = PythonOperator(
        task_id="write_reviewable_diff",
        python_callable=write_reviewable_discovery_diff,
        op_kwargs={"discovery_raw_ref": fetch.output["discovery_raw_ref"]},
        retries=0,
    )
    fetch >> review
