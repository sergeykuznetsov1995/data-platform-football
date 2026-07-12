"""
Master Pipeline DAG
===================

Airflow DAG for orchestrating all data ingestion DAGs.
Uses TriggerDagRunOperator for child DAGs and a fail-closed sensor for the
independently scheduled 06:00 FBref run.

Schedules daily at 2 PM UTC (after all individual DAGs).

This DAG:
1. Triggers non-FBref ingestion DAGs in sequence
2. Waits for each plus the scheduled FBref chain to complete before proceeding
3. Validates overall pipeline success
4. Logs completion summary
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from utils.config import SCHEDULES, DAG_TAGS
from utils.default_args import DEFAULT_ARGS


# Bronze DAGs actively triggered by this master run.
TRIGGERED_INGESTION_DAGS = [
    'dag_ingest_fotmob',
    'dag_ingest_matchhistory',
    'dag_ingest_understat',
    'dag_ingest_whoscored',
    'dag_ingest_sofascore',
    'dag_ingest_espn',
    'dag_ingest_clubelo',
]

# FBref owns its 06:00 schedule and its daily request/byte budget.  Master only
# waits for that run; triggering it again at 14:00 would create a second
# control run and could double paid proxy traffic.
SCHEDULED_INGESTION_DAGS = ['dag_ingest_fbref']

# Complete reporting scope (both master-triggered and externally scheduled).
INGESTION_DAGS = [*TRIGGERED_INGESTION_DAGS, *SCHEDULED_INGESTION_DAGS]

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

    1. Other Bronze ingestion DAGs run in sequence.
    2. Master waits for the successful scheduled **FBref 06:00** run; it does
       not launch a second crawl or consume a second traffic budget.
    3. E3/E4 and auxiliary Silver transforms run on the validated xref spine.
    4. A single final FBref Gold run consumes all successful prerequisites.

    ### E1: Silver xref step

    The scheduled FBref ingestion waits for `dag_transform_fbref_silver`;
    FBref Silver in turn waits for `dag_transform_xref` and its DQ gate. The
    master uses a fail-closed external-DAG sensor for that completed chain,
    so `xref_player` cannot race `silver.fbref_player_identity` and FBref is
    never crawled twice for one daily promotion.

    ### E3: Core event facts (Silver + Gold)

    After the sensed FBref Silver/xref chain finishes, `dag_transform_e3` runs the E3
    medallion-redesign chain: Silver `whoscored_events_spadl` +
    `espn_lineup` → Gold `fct_event` / `fct_shot` / `fct_lineup` →
    `validate_e3`. It depends on `silver.xref_match`, `silver.xref_team`,
    `silver.xref_player` (produced by E1) and runs sequentially
    (`max_active_tasks=1`) for OOM safety. E3 facts feed the Gold star
    schema (fct_team_season_stats CTE inline и далее) — materialising them
    here keeps the daily cadence consistent.

    ### E4: Narrow event facts (Silver + Gold)

    After `dag_transform_e3` finishes, `dag_transform_e4` materialises the
    E4 narrow facts: Silver `matchhistory_match_odds` /
    `sofascore_player_ratings` → Gold `fct_match_odds` /
    `fct_match_rating` → `validate_e4` (fct_goal/fct_card/fct_substitution
    dropped in #448 — superseded by `gold.fct_match_timeline`). It depends
    on `silver.xref_*` (E1) and runs sequentially
    (`max_active_tasks=1`) for OOM safety. The Sofascore Silver task is
    bronze-guarded — it skips if `bronze.sofascore_player_ratings` is
    absent (R0.2B partial-backfill scenarios) so the rest of the chain
    keeps moving.

    ### Transfermarkt + Capology Silver (issue #64)

    After E4, the master pipeline kicks off `dag_transform_transfermarkt_silver`
    and `dag_transform_capology_silver` in parallel. Both are trigger-only
    (`schedule=None`) and re-materialise Silver tables from already-fetched
    Bronze rows — they do NOT re-scrape upstream. Bronze ingest runs on its
    own weekly Monday cron (`dag_ingest_transfermarkt` 04:00 UTC,
    `dag_ingest_capology` 05:00 UTC); the daily Silver refresh here picks up
    fresh `silver.xref_player` rows from E1 so canonical_id coverage stays
    aligned even when xref grows mid-week. These prerequisite triggers must
    succeed before Gold becomes eligible.

    ### FBref Gold layer (issue #39)

    After the TM/Capology/SoFIFA Silver block, the master pipeline triggers
    `dag_transform_fbref_gold`, which materialises the analytical Gold star
    schema (dimensions + season/base facts; the derived feat_*/mart_*/ML tier
    was dropped in #478). It is placed LAST in the chain because it
    consumes outputs from every earlier step:
      * `silver.xref_*` (E1) for identity resolution;
      * `silver.sofascore_player_profile` / `sofascore_player_season_aggregate`
        (E3) -> `gold.dim_player_attributes` / `fct_player_season_stats`
        (CTAS fails on a missing table if E3 has not run);
      * `gold.fct_shot` / `fct_event` (E3);
      * `silver.transfermarkt_players` / `capology_player_salaries`
        (TM/Cap Silver) -> `gold.fct_team_season_stats`
        (`squad_market_value_eur` / `total_wage_bill_gbp`), which is why it runs
        AFTER the TM/Capology Silver block rather than in parallel with it.
    The Gold DAG runs sequentially (`max_active_tasks=1`) for OOM safety on the
    dev Trino (3.5 GB heap). The handoff is fail-closed: a failed prerequisite
    or Gold CTAS cannot be hidden by the reporting tasks.

    ### Notes

    - Triggered children use `wait_for_completion=True`
    - Scheduled FBref is sensed at the matching 06:00 logical date
    - FBref Silver, xref, Gold, and direct Gold prerequisites are fail-closed
    - Final report is generated only after successful promotion
    - SoFIFA runs weekly (Sunday) and is not included here
    - Transfermarkt/Capology Bronze run weekly (Monday); master only re-
      materialises their Silver tables daily (idempotent CTAS, no re-scrape)
    """,
) as dag:

    # Create trigger tasks for each ingestion DAG
    trigger_tasks = []

    with TaskGroup(group_id='ingestion_triggers') as triggers_group:
        prev_task = None

        for dag_id in TRIGGERED_INGESTION_DAGS:
            trigger_task = TriggerDagRunOperator(
                task_id=f'trigger_{dag_id.replace("dag_ingest_", "")}',
                trigger_dag_id=dag_id,
                wait_for_completion=True,
                poke_interval=60,  # Check every minute
                allowed_states=['success', 'failed'],
                failed_states=[],
                reset_dag_run=True,  # Reset if already running
                execution_date='{{ ds }}',  # Airflow 2.x uses execution_date
                conf=(
                    {"master_data_interval_end": "{{ data_interval_end }}"}
                    if dag_id == "dag_ingest_sofascore"
                    else {}
                ),
            )

            if prev_task:
                prev_task >> trigger_task

            prev_task = trigger_task
            trigger_tasks.append(trigger_task)

    # FBref runs once per day on its own 06:00 schedule.  The master DAG's
    # 14:00 logical date is eight hours later, so execution_delta maps to the
    # exact same daily data interval without launching another paid crawl.
    # Waiting for the whole external DAG is intentional: dag_ingest_fbref
    # cannot succeed until its blocking Silver -> xref -> DQ chain succeeds.
    wait_for_scheduled_fbref = ExternalTaskSensor(
        task_id='wait_for_scheduled_fbref',
        external_dag_id='dag_ingest_fbref',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed'],
        execution_delta=timedelta(hours=8),
        mode='reschedule',
        poke_interval=60,
        timeout=timedelta(hours=6).total_seconds(),
        check_existence=True,
    )

    # =========================================================================
    # E3 medallion-redesign: Core event facts (Silver + Gold)
    # =========================================================================
    # Runs AFTER `dag_transform_xref` (E1) so Silver `whoscored_events_spadl`
    # / `espn_lineup` and the downstream Gold `fct_event` / `fct_shot` /
    # `fct_lineup` builders can resolve identities through fresh
    # `silver.xref_match` / `silver.xref_team` / `silver.xref_player` rows.
    #
    # The E3 DAG itself runs sequentially (`max_active_tasks=1`) for OOM
    # safety. Its validation is a hard prerequisite for final Gold.
    trigger_e3_transforms = TriggerDagRunOperator(
        task_id='trigger_e3_transforms',
        trigger_dag_id='dag_transform_e3',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        trigger_rule='all_success',
    )

    # =========================================================================
    # E4 medallion-redesign: Narrow event facts (Silver + Gold)
    # =========================================================================
    # Runs AFTER `dag_transform_e3` (E3) so the E4 builders can read fresh
    # `silver.xref_match` / `silver.xref_team` / `silver.xref_player` rows
    # (produced by xref step).
    #
    # The E4 DAG itself runs sequentially (`max_active_tasks=1`) for OOM
    # safety. Its validation is a hard prerequisite for final Gold.
    trigger_e4_transforms = TriggerDagRunOperator(
        task_id='trigger_e4_transforms',
        trigger_dag_id='dag_transform_e4',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        trigger_rule='all_success',
    )

    # =========================================================================
    # Transfermarkt + Capology Silver (issue #64)
    # =========================================================================
    # Bronze ingest runs weekly (Mon 04:00/05:00 UTC); the master pipeline
    # only re-materialises Silver tables daily. CTAS is idempotent (DROP +
    # CREATE) and reads from `iceberg.bronze.*` + fresh `silver.xref_player`
    # from E1, so canonical_id coverage stays aligned even on days when
    # Bronze did not refresh.
    #
    # Triggered in parallel after E4 (TM/Capology Silver does not depend on
    # gold.fct_* outputs from E3/E4) but kept downstream of E1 because both
    # DAGs LEFT JOIN `silver.xref_player` for canonical_id.
    trigger_silver_transfermarkt = TriggerDagRunOperator(
        task_id='trigger_silver_transfermarkt',
        trigger_dag_id='dag_transform_transfermarkt_silver',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        trigger_rule='all_success',
    )

    trigger_silver_capology = TriggerDagRunOperator(
        task_id='trigger_silver_capology',
        trigger_dag_id='dag_transform_capology_silver',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        trigger_rule='all_success',
    )

    # SoFIFA Silver (issue #42) — same dependency profile as TM/Capology:
    # reads bronze.sofifa_* + fresh silver.xref_player (source='sofifa') from
    # E1, so it is kept downstream of E1 and runs in parallel with TM/Capology.
    # Bronze ingest is weekly (Sunday); CTAS is idempotent so daily re-
    # materialisation keeps canonical_id coverage aligned.
    trigger_silver_sofifa = TriggerDagRunOperator(
        task_id='trigger_silver_sofifa',
        trigger_dag_id='dag_transform_sofifa_silver',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        trigger_rule='all_success',
    )

    # =========================================================================
    # FBref Gold layer (issue #39)
    # =========================================================================
    # Runs AFTER E4 + TM/Capology/SoFIFA Silver so the Gold builders can read:
    #   * silver.xref_* (E1)
    #   * silver.sofascore_player_profile / _season_aggregate (E3) ->
    #     gold.dim_player_attributes / fct_player_season_stats
    #   * gold.fct_shot / fct_event (E3)
    #   * silver.transfermarkt_players / capology_player_salaries (TM/Cap Silver)
    #     -> gold.fct_team_season_stats (squad_market_value_eur /
    #        total_wage_bill_gbp)
    #
    # The Gold DAG itself runs sequentially (max_active_tasks=1) for OOM safety.
    # Child failure propagates so reporting cannot turn a failed promotion green.
    trigger_fbref_gold = TriggerDagRunOperator(
        task_id='trigger_fbref_gold',
        trigger_dag_id='dag_transform_fbref_gold',
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

    # Check overall pipeline success
    check_success_task = PythonOperator(
        task_id='check_pipeline_success',
        python_callable=check_pipeline_success,

        trigger_rule='all_success',
    )

    # Generate summary report
    generate_report_task = PythonOperator(
        task_id='generate_pipeline_report',
        python_callable=generate_pipeline_report,

    )

    # Dependencies
    # The sensor waits for the scheduled FBref run, whose child DAG does not
    # complete until Silver DQ and xref DQ have both passed.
    triggers_group >> wait_for_scheduled_fbref >> trigger_e3_transforms
    trigger_e3_transforms >> trigger_e4_transforms
    trigger_e4_transforms >> [
        trigger_silver_transfermarkt,
        trigger_silver_capology,
        trigger_silver_sofifa,
    ]
    [
        trigger_silver_transfermarkt,
        trigger_silver_capology,
        trigger_silver_sofifa,
    ] >> trigger_fbref_gold
    trigger_fbref_gold >> check_success_task >> generate_report_task
