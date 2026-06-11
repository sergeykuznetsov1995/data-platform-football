"""
Iceberg Maintenance DAG
=======================

Weekly sweep of `expire_snapshots` + `remove_orphan_files` over every table
in `iceberg.{bronze,silver,gold}`.

Why
---
Iceberg never garbage-collects old snapshots on its own. DAGs that do
delete-then-insert (e.g. `dag_ingest_whoscored`) accumulate metadata files
indefinitely — `bronze.whoscored_events` reached 12 538 files / 26 GB of
manifest+metadata for 49 MB of actual data, exhausting the host disk.

Retention threshold: 30d for the weekly sweep — страховка для time-travel
rollback на time-window E1-E7 этапов medallion-redesign'а (см.
`docs/MEDALLION_REDESIGN_ROADMAP.md` E0). Высокочастотные bronze-таблицы
дополнительно подметаются ежедневно с 3d retention из
`dag_iceberg_maintenance_daily.py` — это контролирует таблицы вроде
`whoscored_events`, где 30 daily ingest commits оставили бы 30+ stale
snapshots между weekly run'ами.

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

    result = maintain_iceberg_tables()
    if result["failures"]:
        # Soft-fail policy: surface failures to logs/Telegram via the DEFAULT_ARGS
        # callback, but don't raise — partial cleanup is better than none, and
        # the next weekly run will retry. If you want hard-fail, raise here.
        from airflow.exceptions import AirflowFailException
        raise AirflowFailException(
            f"Iceberg maintenance had {len(result['failures'])} table failure(s); see logs"
        )
    return result


with DAG(
    dag_id='dag_iceberg_maintenance',
    default_args=SILVER_ARGS,
    description='Weekly expire_snapshots + remove_orphan_files for all Iceberg tables',
    schedule='0 5 * * 0',  # Sunday 05:00 UTC — outside serve_predictions window
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'iceberg', 'cleanup'],
    doc_md=__doc__,
) as dag:

    PythonOperator(
        task_id='maintain_iceberg_tables',
        python_callable=_maintain,
    )
