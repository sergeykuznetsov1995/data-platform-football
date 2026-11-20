"""Daily read-only physical DQ for referee profiles and season aggregates."""
from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from utils.alerts import telegram_on_failure
from utils.config import DAG_TAGS
from utils.default_args import SCRAPER_ARGS

STATE_DIR = Path('/opt/airflow/logs/transfermarkt-referees/dq')


def run_daily_referee_dq(**context):
    from dags.utils import transfermarkt_native_v2 as native
    from dags.utils.transfermarkt_referees import run_referee_physical_dq

    connection = native.connect()
    try:
        report = run_referee_physical_dq(connection)
    finally:
        connection.close()
    logical = context.get('logical_date') or context.get('data_interval_start')
    if isinstance(logical, datetime):
        day = logical.date().isoformat()
    else:
        day = datetime.now().date().isoformat()
    report['day'] = day
    report['dag_run_id'] = str(context['run_id'])
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = STATE_DIR / f'{day}.json'
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(report, sort_keys=True, ensure_ascii=False))
    temporary.replace(path)
    if not report.get('completed') or report.get('status') != 'GREEN':
        raise AirflowException('daily referee physical DQ is not GREEN')
    return report


with DAG(
    dag_id='dag_dq_transfermarkt_referees',
    default_args={**SCRAPER_ARGS, 'pool': 'transfermarkt_control'},
    description='Daily read-only referee lineage and natural-key DQ',
    schedule='0 1 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=DAG_TAGS.get('transfermarkt', ['transfermarkt', 'bronze', 'dq']),
) as dag:
    audit = PythonOperator(
        task_id='daily_referee_physical_dq', python_callable=run_daily_referee_dq,
        execution_timeout=timedelta(minutes=10), on_failure_callback=telegram_on_failure,
    )
