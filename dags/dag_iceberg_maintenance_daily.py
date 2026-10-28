"""
Iceberg Maintenance — Daily (high-churn bronze)
================================================

Daily `expire_snapshots` and `remove_orphan_files` for a curated allow-list of
high-churn Bronze tables (delete-then-insert pattern from daily ingest DAGs).
A separate serial pass compacts physical Bronze tables at 02:00–05:00 UTC,
with at most 120 portions / 8 GiB. Other feeds retain 3d; WhoScored retains
14d for rollback. Compaction does not repeat retention.
Any per-table failure fails its Airflow task and reaches the normal alerting
callback instead of producing a false-green maintenance run.

The full bronze/silver/gold sweep stays weekly (`dag_iceberg_maintenance.py`,
retention 30d) — this DAG is a tighter loop only for the worst offenders
identified during the 2026-04-30 disk-fill incident.

Schedule: 02:00 UTC daily — well before the 14:00 UTC master pipeline.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from utils.default_args import SILVER_ARGS


def _fail_on_partial_maintenance(
    result: Dict[str, Any], *, group: str
) -> Dict[str, Any]:
    failures = result.get("failures") or []
    if failures:
        from airflow.exceptions import AirflowFailException

        raise AirflowFailException(
            f"Iceberg maintenance for {group} had {len(failures)} "
            "table failure(s); see task logs"
        )
    return result


def _maintain_whoscored(**_ctx) -> Dict[str, Any]:
    from utils.maintenance_tasks import (
        WHOSCORED_DAILY_RETENTION,
        WHOSCORED_HIGH_CHURN,
        maintain_iceberg_tables,
    )

    result = maintain_iceberg_tables(
        schemas=("bronze",),
        retention_threshold=WHOSCORED_DAILY_RETENTION,
        table_filter=WHOSCORED_HIGH_CHURN,
        compact_live_files=False,
    )
    return _fail_on_partial_maintenance(result, group="WhoScored")


def _cleanup_whoscored_dq_stage(**_ctx) -> Dict[str, Any]:
    from utils.maintenance_tasks import cleanup_whoscored_dq_stage_partitions

    return cleanup_whoscored_dq_stage_partitions()


def _maintain_other_high_churn(**_ctx) -> Dict[str, Any]:
    from utils.maintenance_tasks import (
        NON_WHOSCORED_HIGH_CHURN,
        OTHER_HIGH_CHURN_DAILY_RETENTION,
        maintain_iceberg_tables,
    )

    result = maintain_iceberg_tables(
        schemas=("bronze",),
        retention_threshold=OTHER_HIGH_CHURN_DAILY_RETENTION,
        table_filter=NON_WHOSCORED_HIGH_CHURN,
        compact_live_files=False,
    )
    return _fail_on_partial_maintenance(result, group="other high-churn feeds")


def _compact_bronze(**context):
    from utils.bronze_compaction import compact_nightly

    result = compact_nightly(reschedule_on_busy=True, **context)
    if result is False:
        return False
    _fail_on_partial_maintenance(result, group="Bronze compaction")
    from airflow.sensors.base import PokeReturnValue
    return PokeReturnValue(is_done=True, xcom_value=result)


def _maintain_fbref_stages(*, reschedule_on_busy=False, **_ctx):
    from utils.fbref_maintenance import maintain_fbref_stages_with_lock_wait

    result = maintain_fbref_stages_with_lock_wait(
        **({"reschedule_on_busy": True} if reschedule_on_busy else {})
    )
    if reschedule_on_busy and result is not False:
        from airflow.sensors.base import PokeReturnValue
        return PokeReturnValue(is_done=True, xcom_value=result)
    return result


with DAG(
    dag_id="dag_iceberg_maintenance_daily",
    default_args=SILVER_ARGS,
    description="Daily expire_snapshots+remove_orphan_files for high-churn bronze",
    schedule="0 2 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["maintenance", "iceberg", "cleanup", "daily"],
    doc_md=__doc__,
) as dag:
    compact_bronze = PythonSensor(
        task_id="compact_bronze",
        retries=0,
        python_callable=_compact_bronze,
        mode="reschedule",
        poke_interval=5,
        timeout=3 * 60 * 60,
        execution_timeout=timedelta(hours=3),
        pool="fbref_scraper_pool",
        priority_weight=10,
        weight_rule="absolute",
    )

    cleanup_whoscored_dq_stage = PythonOperator(
        task_id="cleanup_whoscored_dq_stage_partitions",
        python_callable=_cleanup_whoscored_dq_stage,
        # Retained FBref stages need attention, but must not suppress this
        # unrelated cleanup wave.
        trigger_rule="all_done",
    )

    maintain_whoscored = PythonOperator(
        task_id="maintain_whoscored_bronze",
        python_callable=_maintain_whoscored,
    )

    maintain_other_high_churn = PythonOperator(
        task_id="maintain_other_high_churn_bronze",
        python_callable=_maintain_other_high_churn,
        # A retained FBref stage needs attention, but must not suppress
        # unrelated snapshot/orphan maintenance.
        trigger_rule="all_done",
    )

    fbref_stage_janitor = PythonSensor(
        task_id="janitor_fbref_generic_stages",
        python_callable=_maintain_fbref_stages,
        op_kwargs={"reschedule_on_busy": True},
        mode="reschedule",
        poke_interval=30,
        timeout=90 * 60,
        # Waiting pokes release the single FBref pool so ingest's tail can
        # release publication ownership. Actual cleanup retains its time limit.
        execution_timeout=timedelta(minutes=120),
        pool="fbref_scraper_pool",
        priority_weight=10,
        weight_rule="absolute",
    )

    fbref_stage_janitor >> cleanup_whoscored_dq_stage
    fbref_stage_janitor >> maintain_other_high_churn
    cleanup_whoscored_dq_stage >> maintain_whoscored

    compact_bronze >> fbref_stage_janitor
