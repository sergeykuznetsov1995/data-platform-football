"""Primary Bronze DAG for the isolated Transfermarkt deployment."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from utils.config import SCHEDULES


DAG_ID = 'dag_ingest_transfermarkt'
STATE_ROOT = Path('/opt/airflow/logs/transfermarkt-core')
POLICY_FILE = Path('/opt/airflow/dags/configs/transfermarkt/standing_approval_policy.json')


def _manual_rehearsal(run):
    kind = getattr(run.run_type, 'value', run.run_type)
    if kind == 'manual':
        return True
    if kind != 'scheduled' or run.conf:
        raise ValueError('Only a scheduled run without overrides can publish Transfermarkt Bronze')
    return False


def wait_for_cycle_inputs(**context):
    run = context['dag_run']
    if _manual_rehearsal(run):
        return True

    from dags.scripts.run_transfermarkt_bronze_cycle import prepare_scheduled_plan, require_scheduled_writes

    require_scheduled_writes()
    plan = prepare_scheduled_plan(STATE_ROOT, cycle_id=run.run_id, started_at=run.start_date)
    return plan.with_name('captures.json').is_file()


def run_bronze_cycle(**context):
    run = context['dag_run']
    if _manual_rehearsal(run):
        from dags.utils.transfermarkt_core_rehearsal import run_rehearsal

        params = context['params']
        return run_rehearsal(STATE_ROOT / 'replays', params['package'], params['league'], params['season'])

    from dags.scripts.run_transfermarkt_bronze_cycle import publish_cycle

    return publish_cycle(STATE_ROOT, POLICY_FILE, cycle_id=run.run_id, started_at=run.start_date)


def build_core_dag():
    with DAG(
        dag_id=DAG_ID,
        description='Transfermarkt Bronze core and sections',
        schedule=SCHEDULES[DAG_ID],
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        tags=['transfermarkt', 'bronze'],
        params={
            'package': Param('', type='string', description='Saved input package for a manual rehearsal'),
            'league': Param('', type='string'),
            'season': Param('', type='string'),
        },
    ) as dag:
        ready = PythonSensor(
            task_id='wait_for_cycle_inputs',
            python_callable=wait_for_cycle_inputs,
            mode='reschedule',
            poke_interval=60,
            timeout=3600,
            pool='transfermarkt_control',
            retries=0,
        )
        publish = PythonOperator(
            task_id='run_bronze_cycle',
            python_callable=run_bronze_cycle,
            pool='transfermarkt_control',
            retries=0,
            execution_timeout=timedelta(hours=1),
        )
        ready >> publish
    return dag
