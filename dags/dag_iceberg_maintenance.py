"""
Iceberg Maintenance DAG
=======================

Weekly bounded small-file compaction plus `expire_snapshots` and
`remove_orphan_files` over every table in `iceberg.{bronze,silver,gold}`.

Why
---
Iceberg never garbage-collects old snapshots on its own. DAGs that do
delete-then-insert (e.g. `dag_ingest_whoscored`) accumulate old warehouse
objects indefinitely — `bronze.whoscored_events` reached a 12 538-file / 26 GB
warehouse footprint while current data was only 49 MB, exhausting the host
disk.

Retention threshold: 30d for the weekly sweep — страховка для time-travel
rollback на time-window E1-E7 этапов medallion-redesign'а (см.
`docs/MEDALLION_REDESIGN_ROADMAP.md` E0). Высокочастотные bronze-таблицы
дополнительно подметаются ежедневно из `dag_iceberg_maintenance_daily.py`:
WhoScored сохраняет 14d для оперативного rollback, остальные источники — 3d.
Это ограничивает metadata churn между weekly run'ами.

Schedule: Sundays 05:00 UTC — chosen to avoid `dag_master_pipeline` (14:00 UTC)
and Silver/Gold trigger chains. 9 hours of slack before the master pipeline
starts.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.default_args import SILVER_ARGS


def _maintain(**_ctx) -> Dict[str, Any]:
    # Lazy import so DAG parse stays cheap and never pulls scrapers/__init__.
    from utils.maintenance_tasks import maintain_iceberg_tables

    logical_date = _ctx.get("logical_date")
    rotation = (
        logical_date.date().toordinal() // 7 if logical_date is not None else None
    )
    result = maintain_iceberg_tables(
        compact_live_files=True,
        compaction_rotation=rotation,
    )
    if result["failures"]:
        # Any partial failure must reach the normal Airflow alert callback;
        # returning a green maintenance DagRun would hide accumulating metadata.
        from airflow.exceptions import AirflowFailException

        raise AirflowFailException(
            f"Iceberg maintenance had {len(result['failures'])} table failure(s); see logs"
        )
    return result


with DAG(
    dag_id="dag_iceberg_maintenance",
    default_args=SILVER_ARGS,
    description="Weekly expire_snapshots + remove_orphan_files for all Iceberg tables",
    schedule="0 5 * * 0",  # Sunday 05:00 UTC — outside serve_predictions window
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "iceberg", "cleanup"],
    doc_md=__doc__,
) as dag:
    PythonOperator(
        task_id="maintain_iceberg_tables",
        python_callable=_maintain,
    )
