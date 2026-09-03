"""
Isolated daily trigger for dag_ingest_sofascore (#951, bronze-only scope).

dag_ingest_sofascore имеет schedule=None, а dag_sofascore_pipeline в изолированной БД
непригоден (ExternalTaskSensor на dag_ingest_fbref + рантайм-импорт dag_master_pipeline).
Этот DAG несёт ТОЛЬКО триггер-оператор пайплайна, скопированный 1:1 (conf
master_data_interval_end, execution_date=ds, reset_dag_run) — субботняя player-ротация
получает ту же границу интервала, что и при мастере. Файл вне fingerprint-скоупа.
При возврате на общий scheduler с dag_sofascore_pipeline — поставить на паузу
(иначе двойной триггер).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils.default_args import DEFAULT_ARGS

TRIGGER_ARGS = {
    **DEFAULT_ARGS,
    # Ретрай блокирующего триггера сдублирует/сбросит прогон ребёнка —
    # тот же контракт, что в dag_sofascore_pipeline.
    "retries": 0,
}

with DAG(
    dag_id="dag_trigger_sofascore_daily",
    description=(
        "Daily 14:00 UTC bronze-only trigger for dag_ingest_sofascore "
        "(isolated stack, #951)"
    ),
    schedule="0 14 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=TRIGGER_ARGS,
    tags=["sofascore", "orchestrator", "bronze"],
) as dag:
    trigger_sofascore_ingest = TriggerDagRunOperator(
        task_id="trigger_sofascore_ingest",
        trigger_dag_id="dag_ingest_sofascore",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
        execution_date="{{ ds }}",
        conf={"master_data_interval_end": "{{ data_interval_end }}"},
        execution_timeout=timedelta(hours=12),
        retries=0,
    )
