"""
Master Pipeline DAG
===================

Airflow DAG for orchestrating all data ingestion DAGs.
Uses TriggerDagRunOperator to run child DAGs in sequence.

Schedules daily at 2 PM UTC (after all individual DAGs).

This DAG:
1. Triggers all ingestion DAGs in sequence
2. Waits for each to complete before proceeding
3. Validates overall pipeline success
4. Logs completion summary
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from utils.config import SCHEDULES, DAG_TAGS
from utils.default_args import DEFAULT_ARGS


# List of ingestion DAGs in execution order
INGESTION_DAGS = [
    'dag_ingest_fbref',
    'dag_ingest_fotmob',
    'dag_ingest_matchhistory',
    'dag_ingest_understat',
    'dag_ingest_whoscored',
    'dag_ingest_sofascore',
    'dag_ingest_espn',
    'dag_ingest_clubelo',
]

# Weekly DAGs (run separately)
WEEKLY_DAGS = [
    'dag_ingest_sofifa',
]

# Extended default args for master pipeline
MASTER_ARGS = {
    **DEFAULT_ARGS,
    'execution_timeout': timedelta(hours=12),  # Long timeout for full pipeline
    'retries': 1,
}


def check_pipeline_success(**context) -> Dict[str, Any]:
    """
    Check overall pipeline success by examining triggered DAG runs.

    Returns:
        Pipeline status summary
    """
    import logging
    from airflow.models import DagRun
    from airflow.utils.state import State

    logger = logging.getLogger(__name__)

    ti = context['ti']
    # Airflow 3.x uses logical_date instead of execution_date
    logical_date = context.get('logical_date') or context.get('data_interval_end')

    results = {
        'status': 'success',
        'dag_statuses': {},
        'failed_dags': [],
        'successful_dags': [],
    }

    for dag_id in INGESTION_DAGS:
        try:
            # Get the most recent run for this DAG
            dag_runs = DagRun.find(dag_id=dag_id)
            if dag_runs:
                latest_run = max(dag_runs, key=lambda x: x.logical_date or x.start_date)
                state = latest_run.state

                results['dag_statuses'][dag_id] = state

                if state == State.SUCCESS:
                    results['successful_dags'].append(dag_id)
                elif state == State.FAILED:
                    results['failed_dags'].append(dag_id)
            else:
                results['dag_statuses'][dag_id] = 'not_found'
                logger.warning(f"No runs found for {dag_id}")

        except Exception as e:
            logger.error(f"Error checking status for {dag_id}: {e}")
            results['dag_statuses'][dag_id] = 'error'

    # Determine overall status
    if results['failed_dags']:
        results['status'] = 'partial_success' if results['successful_dags'] else 'failed'

    logger.info(f"Pipeline check complete: {results['status']}")
    logger.info(f"Successful: {len(results['successful_dags'])}/{len(INGESTION_DAGS)}")

    if results['failed_dags']:
        logger.warning(f"Failed DAGs: {results['failed_dags']}")

    return results


def generate_pipeline_report(**context) -> Dict[str, Any]:
    """
    Generate a summary report of the pipeline execution.

    Returns:
        Pipeline report
    """
    import logging
    from datetime import datetime as dt

    logger = logging.getLogger(__name__)

    ti = context['ti']
    check_result = ti.xcom_pull(task_ids='check_pipeline_success')

    report = {
        'timestamp': dt.utcnow().isoformat(),
        'pipeline_status': check_result.get('status', 'unknown') if check_result else 'unknown',
        'total_dags': len(INGESTION_DAGS),
        'successful_dags': len(check_result.get('successful_dags', [])) if check_result else 0,
        'failed_dags': len(check_result.get('failed_dags', [])) if check_result else 0,
        'dag_details': check_result.get('dag_statuses', {}) if check_result else {},
    }

    # Log report
    logger.info("=" * 60)
    logger.info("MASTER PIPELINE REPORT")
    logger.info("=" * 60)
    logger.info(f"Timestamp: {report['timestamp']}")
    logger.info(f"Status: {report['pipeline_status']}")
    logger.info(f"Successful: {report['successful_dags']}/{report['total_dags']}")
    logger.info("-" * 60)

    for dag_id, status in report['dag_details'].items():
        logger.info(f"  {dag_id}: {status}")

    logger.info("=" * 60)

    return report


# DAG definition
with DAG(
    dag_id='dag_master_pipeline',
    default_args=MASTER_ARGS,
    description='Master pipeline orchestrating all data ingestion DAGs',
    schedule=SCHEDULES.get('dag_master_pipeline', '0 14 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=DAG_TAGS.get('master', ['orchestration', 'master', 'pipeline']),
    max_active_runs=1,
    doc_md="""
    ## Master Pipeline

    This DAG orchestrates all data ingestion DAGs in the correct sequence.

    ### Execution Order

    1. **FBref** (6:00 UTC) - Selenium-based scraper
    2. **FotMob** (7:00 UTC) - Selenium-based scraper
    3. **MatchHistory** (8:00 UTC) - Direct HTTP scraper
    4. **Understat** (9:00 UTC) - soccerdata library
    5. **WhoScored** (10:00 UTC) - Selenium with SPADL conversion
    6. **SofaScore** (11:00 UTC) - soccerdata library
    7. **ESPN** (12:00 UTC) - soccerdata library
    8. **ClubElo** (13:00 UTC) - ELO ratings

    ### Notes

    - Each DAG is triggered with `wait_for_completion=True`
    - Pipeline continues even if some DAGs fail
    - Final report summarizes all DAG statuses
    - SoFIFA runs weekly (Sunday) and is not included here
    """,
) as dag:

    # Create trigger tasks for each ingestion DAG
    trigger_tasks = []

    with TaskGroup(group_id='ingestion_triggers') as triggers_group:
        prev_task = None

        for dag_id in INGESTION_DAGS:
            trigger_task = TriggerDagRunOperator(
                task_id=f'trigger_{dag_id.replace("dag_ingest_", "")}',
                trigger_dag_id=dag_id,
                wait_for_completion=True,
                poke_interval=60,  # Check every minute
                allowed_states=['success', 'failed'],  # Continue on failure
                failed_states=[],  # Don't fail master if child fails
                reset_dag_run=True,  # Reset if already running
                execution_date='{{ ds }}',  # Airflow 2.x uses execution_date
            )

            if prev_task:
                prev_task >> trigger_task

            prev_task = trigger_task
            trigger_tasks.append(trigger_task)

    # Check overall pipeline success
    check_success_task = PythonOperator(
        task_id='check_pipeline_success',
        python_callable=check_pipeline_success,
        
        trigger_rule='all_done',
    )

    # Generate summary report
    generate_report_task = PythonOperator(
        task_id='generate_pipeline_report',
        python_callable=generate_pipeline_report,
        
    )

    # Dependencies
    triggers_group >> check_success_task >> generate_report_task
