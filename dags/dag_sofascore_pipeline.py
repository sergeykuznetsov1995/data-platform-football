"""
SofaScore Daily Pipeline (orchestrator)
=======================================

Daily bronze -> silver -> gold chain for the SofaScore source while
``dag_master_pipeline`` stays paused (manual-ops mode since #847).

Why a separate orchestrator instead of unpausing the master: the master would
re-trigger every source at once, and unpausing it is an operational decision
that belongs to the owner.  This DAG carries ONLY the SofaScore slice using
the exact master patterns:

- ``dag_ingest_sofascore`` is triggered with ``master_data_interval_end`` in
  conf (the Saturday player-branch gate reads it), mirroring
  ``dag_master_pipeline.py`` ingestion_triggers.
- ``dag_transform_xref`` needs a pinned FBref publication generation:
  the ``resolve_scheduled_fbref_control_run`` helper is imported from the
  master module (single source of truth) and requires the scheduled 06:00
  ``dag_ingest_fbref`` run to be current+succeeded with an active publication
  lock.  This DAG therefore runs in the master's own 14:00 UTC slot so the
  ``logical_date - 8h`` math resolves the same source run.
- E3/E4 run strictly after xref (fresh ``silver.xref_*`` identities).

The FBref publication lock is NOT released here: with the master paused the
lock is re-acquired by every scheduled FBref run (idempotent acquire,
TTL 8 days) — the same steady state production has been in since the pause.

Mutual exclusion contract: ``dag_ingest_sofascore`` was removed from the
master's ``TRIGGERED_INGESTION_DAGS`` (#951) — if the master is ever
unpaused, pause THIS DAG in the same change; running both would double the
xref/e3/e4 publication triggers in the same daily slot.

One source = one DAG (#782) stays intact: this file adds no scraping tasks,
it only sequences existing DAGs.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

from utils.config import SCHEDULES
from utils.default_args import DEFAULT_ARGS


def resolve_fbref_publication_scope(**context):
    """Pin the scheduled 06:00 FBref generation for xref publication.

    The import is deferred into the callable on purpose: importing
    ``dag_master_pipeline`` at module top level executes that DAG file during
    DagBag parsing and re-registers its ``dag_master_pipeline`` DAG under this
    file, raising ``AirflowDagDuplicatedIdException``. DAG files must not import
    each other at parse time. Resolving at task runtime is collision-free and
    keeps the master helper the single source of truth.
    """

    from dag_master_pipeline import resolve_scheduled_fbref_control_run

    return resolve_scheduled_fbref_control_run(**context)

PIPELINE_ARGS = {
    **DEFAULT_ARGS,
    # Retrying a blocking child trigger can reset or duplicate publication
    # (same contract as the master pipeline).
    'retries': 0,
}


with DAG(
    dag_id='dag_sofascore_pipeline',
    description=(
        'Daily SofaScore chain: ingest -> xref -> E3 -> E4 while the master '
        'pipeline is paused'
    ),
    schedule=SCHEDULES.get('dag_sofascore_pipeline'),
    # Explicit, like dag_master_pipeline / dag_ingest_sofascore — DEFAULT_ARGS
    # carries no start_date, and Airflow requires one at DAG construction.
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=PIPELINE_ARGS,
    tags=['sofascore', 'orchestrator', 'medallion'],
) as dag:

    # Bronze capture is independent from the FBref generation pin and can run
    # while the sensor below is still waiting.
    trigger_sofascore_ingest = TriggerDagRunOperator(
        task_id='trigger_sofascore_ingest',
        trigger_dag_id='dag_ingest_sofascore',
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        conf={"master_data_interval_end": "{{ data_interval_end }}"},
        execution_timeout=timedelta(hours=12),
        retries=0,
    )

    # Same source-run pinning as the master: the 06:00 FBref run must be
    # terminal before xref may publish against its generation.
    wait_for_scheduled_fbref = ExternalTaskSensor(
        task_id='wait_for_scheduled_fbref',
        external_dag_id='dag_ingest_fbref',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed'],
        execution_delta=timedelta(hours=8),
        mode='reschedule',
        poke_interval=60,
        timeout=timedelta(hours=12).total_seconds(),
        check_existence=True,
    )

    resolve_fbref_scope = PythonOperator(
        task_id='resolve_fbref_publication_scope',
        python_callable=resolve_fbref_publication_scope,
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )

    trigger_xref_transforms = TriggerDagRunOperator(
        task_id='trigger_xref_transforms',
        trigger_dag_id='dag_transform_xref',
        trigger_run_id='sofascore_xref__{{ dag.dag_id }}__{{ run_id }}',
        logical_date='{{ ti.start_date }}',
        conf={
            'publication_owner': 'dag_sofascore_pipeline',
            'master_run_id': '{{ run_id }}',
            'fbref_source_dag_id': 'dag_ingest_fbref',
            'fbref_control_run_id': (
                "{{ ti.xcom_pull(task_ids="
                "'resolve_fbref_publication_scope') }}"
            ),
        },
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=False,
        # Child has a 4h DagRun timeout; leave one hour for scheduler handoff
        # (same sizing as the master pipeline).
        execution_timeout=timedelta(hours=5),
        retries=0,
        trigger_rule='all_success',
    )

    trigger_e3_transforms = TriggerDagRunOperator(
        task_id='trigger_e3_transforms',
        trigger_dag_id='dag_transform_e3',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        execution_timeout=timedelta(hours=12),
        retries=0,
        trigger_rule='all_success',
    )

    trigger_e4_transforms = TriggerDagRunOperator(
        task_id='trigger_e4_transforms',
        trigger_dag_id='dag_transform_e4',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        execution_timeout=timedelta(hours=12),
        retries=0,
        trigger_rule='all_success',
    )

    wait_for_scheduled_fbref >> resolve_fbref_scope
    trigger_sofascore_ingest >> trigger_xref_transforms
    resolve_fbref_scope >> trigger_xref_transforms
    trigger_xref_transforms >> trigger_e3_transforms
    trigger_e3_transforms >> trigger_e4_transforms
