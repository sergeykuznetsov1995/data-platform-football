"""
Iceberg Maintenance — Daily (high-churn bronze)
================================================

Daily sweep of `expire_snapshots` + `remove_orphan_files` for a curated
allow-list of high-churn Bronze tables (delete-then-insert pattern from daily
ingest DAGs). Retention 3d — short enough to keep metadata under a few hundred
files at all times, long enough that an SQL mistake on Tuesday is still
recoverable on Thursday via time-travel.

The full bronze/silver/gold sweep stays weekly (`dag_iceberg_maintenance.py`,
retention 7d) — this DAG is a tighter loop only for the worst offenders
identified during the 2026-04-30 disk-fill incident.

Schedule: 02:00 UTC daily — well before the 14:00 UTC master pipeline.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.default_args import SILVER_ARGS


def _maintain_high_churn(**_ctx) -> Dict[str, Any]:
    from utils.maintenance_tasks import HIGH_CHURN_BRONZE, maintain_iceberg_tables

    return maintain_iceberg_tables(
        schemas=("bronze",),
        retention_threshold="3d",
        table_filter=HIGH_CHURN_BRONZE,
    )


def _maintain_fbref_stages(**_ctx) -> Dict[str, Any]:
    from utils.maintenance_tasks import maintain_fbref_generic_stages

    return maintain_fbref_generic_stages()


with DAG(
    dag_id='dag_iceberg_maintenance_daily',
    default_args=SILVER_ARGS,
    description='Daily expire_snapshots+remove_orphan_files for high-churn bronze',
    schedule='0 2 * * *',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=['maintenance', 'iceberg', 'cleanup', 'daily'],
    doc_md=__doc__,
) as dag:

    fbref_stage_janitor = PythonOperator(
        task_id='janitor_fbref_generic_stages',
        python_callable=_maintain_fbref_stages,
    )

    iceberg_maintenance = PythonOperator(
        task_id='maintain_high_churn_bronze',
        python_callable=_maintain_high_churn,
        # Retained FBref stages need operator attention, but must not suppress
        # unrelated snapshot/orphan maintenance when the janitor fails closed.
        trigger_rule='all_done',
    )

    fbref_stage_janitor >> iceberg_maintenance
