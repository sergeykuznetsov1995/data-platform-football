"""Resumable historical referee directory backfill without match discovery."""
from __future__ import annotations

from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from utils.alerts import telegram_on_failure
from utils.config import DAG_TAGS
from utils.default_args import SCRAPER_ARGS
from utils.transfermarkt_bash_operator import TransfermarktBashOperator

STATE_DIR = '/opt/airflow/logs/transfermarkt-referees'


def _operator_kwargs(environment):
    return {'env': environment}


def _plan_referee_history(**context):
    params = context.get('params') or {}
    scopes = params.get('scopes') or []
    if not isinstance(scopes, list) or not all(isinstance(value, str) for value in scopes):
        raise AirflowException('params.scopes must contain exact historical scope IDs')
    return [{
        'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
        'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
        'TM_DAG_ID': 'dag_backfill_transfermarkt_referees',
        'TM_RUN_ID': str(context['run_id']),
        'TM_REFEREE_STATE_DIR': STATE_DIR,
        'TM_REFEREE_SCOPES_JSON': json.dumps(scopes, separators=(',', ':')),
        'TM_REFEREE_MAX_SCOPES': str(params.get('max_scopes', 100)),
        'TM_REFEREE_MAX_SECONDS': str(params.get('max_seconds', 3600)),
        'TM_STANDING_POLICY_ENABLED': 'true',
        'TM_STANDING_POLICY_PATH': '/opt/airflow/dags/configs/transfermarkt/standing_approval_policy.json',
        'TM_WRITE_MODE': 'native-only',
    }]


def _validate_referee_history(**context):
    with open(f'{STATE_DIR}/history-last-report.json') as handle:
        report = json.load(handle)
    if report.get('run_id') != str(context['run_id']):
        raise AirflowException('history coverage report belongs to another run')
    if report.get('status') == 'error':
        raise AirflowException('historical referee backfill has unresolved scopes')
    from dags.utils import transfermarkt_native_v2 as native
    from dags.utils.transfermarkt_referees import run_referee_physical_dq

    connection = native.connect()
    try:
        dq = run_referee_physical_dq(connection, run_id=str(context['run_id']))
    finally:
        connection.close()
    if not dq.get('completed') or dq.get('status') != 'GREEN':
        raise AirflowException('historical referee physical DQ is not GREEN')
    report['physical_dq'] = dq
    return report


with DAG(
    dag_id='dag_backfill_transfermarkt_referees',
    default_args={**SCRAPER_ARGS, 'pool': 'transfermarkt_control'},
    description='Historical referee season aggregates from directory pages only',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=DAG_TAGS.get('transfermarkt', ['transfermarkt', 'bronze', 'backfill']),
    params={
        'scopes': Param(default=[], type='array'),
        'max_scopes': Param(default=100, type='integer', minimum=1, maximum=1000),
        'max_seconds': Param(default=3600, type='integer', minimum=60, maximum=3600),
    },
) as dag:
    plan = PythonOperator(
        task_id='plan_referee_history', python_callable=_plan_referee_history,
    )
    capture = TransfermarktBashOperator.partial(
        task_id='capture_and_write_referee_history',
        bash_command=r'''set -euo pipefail
cd /opt/airflow
exec python dags/scripts/run_transfermarkt_referees.py \
  --mode history --run-id "$TM_RUN_ID" --state-dir "$TM_REFEREE_STATE_DIR" \
  --scopes-json "$TM_REFEREE_SCOPES_JSON" --max-scopes "$TM_REFEREE_MAX_SCOPES" \
  --max-seconds "$TM_REFEREE_MAX_SECONDS"''',
        append_env=True,
        retries=0,
        pool='transfermarkt_proxy',
        pool_slots=1,
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(seconds=4200),
        do_xcom_push=False,
        on_failure_callback=None,
    ).expand_kwargs(plan.output.map(_operator_kwargs), strict=False)
    validate = PythonOperator(
        task_id='validate_referee_history_coverage', python_callable=_validate_referee_history,
        trigger_rule='all_done', execution_timeout=timedelta(minutes=10),
        on_failure_callback=telegram_on_failure,
    )
    plan >> capture >> validate
