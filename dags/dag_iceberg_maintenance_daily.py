"""
Iceberg Maintenance — Daily (high-churn bronze)
================================================

Daily `expire_snapshots` and `remove_orphan_files` for a curated allow-list of
high-churn Bronze tables (delete-then-insert pattern from daily ingest DAGs).
A separate serial pass compacts physical Bronze tables at 03:00–05:00 UTC
(06:00–08:00 MSK, owner decision 08.11), with at most 120 portions / 8 GiB. Other feeds retain 3d; WhoScored retains
14d for rollback. Compaction does not repeat retention. The `iceberg.ops` source journals get a
full `optimize` + `expire_snapshots('3d')` (never orphan cleanup: live writers)
via `maintain_ops`; a table that stays commit-busy is skipped, not failed.
Any per-table failure fails its Airflow task and reaches the normal alerting
callback instead of producing a false-green maintenance run.

The full bronze/silver/gold sweep stays weekly (`dag_iceberg_maintenance.py`,
retention 30d) — this DAG is a tighter loop only for the worst offenders
identified during the 2026-04-30 disk-fill incident.

The fail-closed FBref stage janitor no longer runs here: its retries burned
02:00–07:08 UTC on 11–14.11 and blocked the 03:00–05:00 UTC direct compaction
window. It moved to its own DAG (`dag_fbref_stage_janitor`) scheduled after
the window (owner decision 14.11.2026, TRINO-GRILL 14.11).

Schedule: 03:00 UTC (06:00 MSK) daily — well before the 14:00 UTC master pipeline.
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


def _check_maintenance_run(**context):
    from airflow.exceptions import AirflowFailException

    failures = [
        ti.task_id
        for ti in context["dag_run"].get_task_instances()
        if ti.task_id != context["task"].task_id
        and ti.state in {"failed", "upstream_failed"}
    ]
    if failures:
        raise AirflowFailException(f"Iceberg maintenance failed: {', '.join(failures)}")


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


def _maintain_ops(**_ctx) -> Dict[str, Any]:
    from datetime import timezone

    from utils.maintenance_tasks import _get_trino_connection
    from utils.ops_maintenance import maintain_ops_tables

    # Stay inside the task's 1 h execution_timeout; busy tables do not fail.
    result = maintain_ops_tables(
        _get_trino_connection,
        deadline=datetime.now(timezone.utc) + timedelta(minutes=55),
        dry_run=False,
    )
    if result["exit_code"]:
        from airflow.exceptions import AirflowFailException

        raise AirflowFailException(
            f"ops maintenance failed: {result['stop_reason']}, "
            f"{result.get('failed', 0)} failed / {result.get('busy', 0)} busy table(s)"
        )
    return result


with DAG(
    dag_id="dag_iceberg_maintenance_daily",
    default_args=SILVER_ARGS,
    description="Daily expire_snapshots+remove_orphan_files for high-churn bronze",
    schedule="0 3 * * *",  # 06:00 MSK, owner window 03:00–05:00 UTC
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
    )

    maintain_whoscored = PythonOperator(
        task_id="maintain_whoscored_bronze",
        python_callable=_maintain_whoscored,
    )

    maintain_other_high_churn = PythonOperator(
        task_id="maintain_other_high_churn_bronze",
        python_callable=_maintain_other_high_churn,
    )

    maintain_ops = PythonOperator(
        task_id="maintain_ops",
        python_callable=_maintain_ops,
        execution_timeout=timedelta(hours=1),
        # Ops journals are independent of Bronze retention outcomes.
        trigger_rule="all_done",
    )

    cleanup_whoscored_dq_stage >> maintain_whoscored
    maintain_other_high_churn >> maintain_ops

    compact_bronze >> cleanup_whoscored_dq_stage
    compact_bronze >> maintain_other_high_churn

    check_maintenance_run = PythonOperator(
        task_id="check_maintenance_run",
        python_callable=_check_maintenance_run,
        trigger_rule="all_done",
    )
    for task in (
        compact_bronze,
        cleanup_whoscored_dq_stage,
        maintain_whoscored,
        maintain_other_high_churn,
        maintain_ops,
    ):
        task >> check_maintenance_run
