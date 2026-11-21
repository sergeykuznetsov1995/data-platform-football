"""Paused-at-creation bounded achievements ingest for all recipient types."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG

from utils.default_args import SCRAPER_ARGS
from utils.transfermarkt_bash_operator import TransfermarktBashOperator

with DAG(
    dag_id="dag_ingest_transfermarkt_achievements",
    default_args=SCRAPER_ARGS,
    description="Monthly and post-season Transfermarkt player/club/coach achievements",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["transfermarkt", "bronze", "achievements"],
) as dag:
    run_achievement_portion = TransfermarktBashOperator(
        task_id="run_achievement_portion",
        bash_command="exec python dags/scripts/run_transfermarkt_achievements.py",
        append_env=True,
        env={
            "TM_DAG_ID": "dag_ingest_transfermarkt_achievements",
            "TM_RUN_ID": "{{ run_id }}",
            "TM_REQUIRE_METERED_PROXY": "true",
            "TRANSFERMARKT_REQUIRE_RAW_STORE": "true",
            "TM_ACHIEVEMENT_STATE": "/opt/airflow/logs/transfermarkt-achievements/state.sqlite3",
            "TM_ACHIEVEMENT_REPORT": "/opt/airflow/logs/transfermarkt-achievements/last-report.json",
            "TM_ACHIEVEMENT_MAX_RECIPIENTS": "180",
            "TM_ACHIEVEMENT_PORTION_SECONDS": "2700",
        },
        execution_timeout=timedelta(minutes=45),
        retries=0,
        pool="transfermarkt_proxy",
        pool_slots=1,
        max_active_tis_per_dag=1,
        do_xcom_push=False,
    )
