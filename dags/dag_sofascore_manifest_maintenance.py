"""
Isolated weekly maintenance for iceberg.ops.sofascore_capture_manifest (#999 п.1).

Полный dag_iceberg_maintenance из master обслуживает ВСЕ таблицы bronze/silver/gold —
запускать его с изолированного sofascore-стека нельзя (чужие таблицы, чужие писатели).
Этот мини-DAG вызывает ТОЛЬКО выделенный lifecycle манифеста
(utils.maintenance_tasks.maintain_sofascore_capture_manifest): полный optimize +
expire_snapshots('7d'), remove_orphan_files намеренно НЕ выполняется (активные писатели).
Файл вне fingerprint-скоупа. При возврате под общий scheduler (там танцует
dag_iceberg_maintenance) — поставить на паузу, иначе двойное обслуживание.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.default_args import DEFAULT_ARGS


def _maintain_manifest(**_ctx):
    from utils.maintenance_tasks import maintain_sofascore_capture_manifest

    return maintain_sofascore_capture_manifest()


with DAG(
    dag_id="dag_sofascore_manifest_maintenance",
    description=(
        "Weekly optimize + expire_snapshots(7d) for "
        "iceberg.ops.sofascore_capture_manifest (isolated stack, #999)"
    ),
    # Воскресенье 05:00 UTC — как у dag_iceberg_maintenance в master: вне окна
    # ежедневника 14:00 UTC и ночного провала резидентного пула.
    schedule="0 5 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["sofascore", "maintenance", "iceberg"],
) as dag:
    maintain_sofascore_manifest = PythonOperator(
        task_id="maintain_sofascore_manifest",
        python_callable=_maintain_manifest,
        execution_timeout=timedelta(hours=1),
    )
