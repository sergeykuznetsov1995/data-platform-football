"""
FBref Stage Janitor (post-window)
=================================

The fail-closed FBref stage audit moved out of the nightly bronze
compaction window into this standalone DAG (owner decision 14.11.2026,
TRINO-GRILL 14.11): janitor retries burned 02:00–07:08 UTC on 11–14.11
and blocked the 03:00–05:00 UTC direct window. Running here, after the
window, a red janitor can never block compaction. retries=0 keeps the
attempt bounded — attention findings are an alert, not a retry loop —
and the janitor session caps each query (FBREF_STAGE_JANITOR_QUERY_CAP
in `utils.maintenance_tasks`).

Schedule: 05:30 UTC daily — safely after the 03:00–05:00 UTC compaction
window of `dag_iceberg_maintenance_daily`.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.sensors.python import PythonSensor

from utils.default_args import SILVER_ARGS

# Local copy — never mutate the shared SILVER_ARGS constant. retries=0:
# attention findings are an alert, not a retry loop (TRINO-GRILL 14.11).
FBREF_JANITOR_ARGS: Dict[str, Any] = {**SILVER_ARGS, "retries": 0}


def _maintain_fbref_stages(**_ctx) -> Dict[str, Any] | bool:
    from utils.fbref_maintenance import maintain_fbref_stages_with_lock_wait

    result = maintain_fbref_stages_with_lock_wait(reschedule_on_busy=True)
    if result is not False:
        from airflow.sensors.base import PokeReturnValue

        return PokeReturnValue(is_done=True, xcom_value=result)
    return result


with DAG(
    dag_id="dag_fbref_stage_janitor",
    default_args=FBREF_JANITOR_ARGS,
    description="Fail-closed FBref stage audit after the compaction window",
    schedule="30 5 * * *",  # 05:30 UTC, after the 03:00–05:00 UTC window
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "iceberg", "fbref", "cleanup"],
    doc_md=__doc__,
) as dag:
    janitor_fbref_generic_stages = PythonSensor(
        task_id="janitor_fbref_generic_stages",
        python_callable=_maintain_fbref_stages,
        mode="reschedule",
        poke_interval=30,
        timeout=45 * 60,
        # The whole bounded attempt, including lock waits, stays inside the
        # window-free budget; attention findings alert instead of retrying.
        execution_timeout=timedelta(minutes=50),
        retries=0,
        pool="fbref_scraper_pool",
        priority_weight=10,
        weight_rule="absolute",
    )
