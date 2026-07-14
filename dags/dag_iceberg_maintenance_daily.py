"""
Iceberg Maintenance — Daily (high-churn bronze)
================================================

Daily `expire_snapshots` and `remove_orphan_files` for a curated allow-list of
high-churn Bronze tables (delete-then-insert pattern from daily ingest DAGs).
The same run performs one bounded compaction invocation, prioritizing
WhoScored; the weekly all-table DAG compacts the other feeds. This keeps the
whole daily DagRun within four tables / 2 GiB selected input. Other feeds
retain 3d; WhoScored retains 14d so a bad publication has a practical Iceberg
rollback window in addition to raw replay.
Any per-table failure fails its Airflow task and reaches the normal alerting
callback instead of producing a false-green maintenance run.

The full bronze/silver/gold sweep stays weekly (`dag_iceberg_maintenance.py`,
retention 30d) — this DAG is a tighter loop only for the worst offenders
identified during the 2026-04-30 disk-fill incident.

Schedule: 02:00 UTC daily — well before the 14:00 UTC master pipeline.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.default_args import SILVER_ARGS


def _compaction_rotation(context: Dict[str, Any]) -> int | None:
    """Stable daily sequence: retries keep the same rotation key."""

    logical_date = context.get("logical_date")
    return logical_date.date().toordinal() if logical_date is not None else None


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
        compact_live_files=True,
        compaction_rotation=_compaction_rotation(_ctx),
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
    cleanup_whoscored_dq_stage = PythonOperator(
        task_id="cleanup_whoscored_dq_stage_partitions",
        python_callable=_cleanup_whoscored_dq_stage,
    )

    maintain_whoscored = PythonOperator(
        task_id="maintain_whoscored_bronze",
        python_callable=_maintain_whoscored,
    )

    PythonOperator(
        task_id="maintain_other_high_churn_bronze",
        python_callable=_maintain_other_high_churn,
    )

    cleanup_whoscored_dq_stage >> maintain_whoscored
