"""Weekly observational ESPN registry discovery; never auto-promotes."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.espn_native_tasks import (
    HTTP_POOL,
    fetch_discovery_catalog,
    fetch_discovery_detail_batch,
    MAX_DISCOVERY_DETAIL_BATCH_MAP_ITEMS,
    plan_discovery_detail_batches,
    select_mapping_descriptors,
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
    detail_plan = PythonOperator(
        task_id="plan_discovery_detail_batches",
        python_callable=plan_discovery_detail_batches,
        op_kwargs={"discovery_raw_ref": fetch.output["discovery_raw_ref"]},
        retries=0,
        multiple_outputs=True,
    )
    detail_selector = PythonOperator(
        task_id="select_discovery_detail_batches",
        python_callable=select_mapping_descriptors,
        op_kwargs={
            "source": detail_plan.output,
            "source_key": "discovery_detail_batch_refs",
            "descriptor_key": "discovery_detail_batch_ref",
            "max_items": MAX_DISCOVERY_DETAIL_BATCH_MAP_ITEMS,
        },
        retries=0,
    )
    detail_fetch = PythonOperator.partial(
        task_id="fetch_discovery_detail_batches",
        python_callable=fetch_discovery_detail_batch,
        pool=HTTP_POOL,
        pool_slots=1,
        retries=1,
    ).expand(op_kwargs=detail_selector.output)
    review = PythonOperator(
        task_id="write_reviewable_diff",
        python_callable=write_reviewable_discovery_diff,
        op_kwargs={
            "discovery_detail_index_ref": detail_plan.output[
                "discovery_detail_index_ref"
            ],
            "discovery_detail_phase_refs": detail_fetch.output,
        },
        trigger_rule="none_failed",
        retries=0,
    )
    fetch >> detail_plan >> detail_selector >> detail_fetch >> review
