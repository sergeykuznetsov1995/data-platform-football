"""
Master Pipeline DAG
===================

Airflow DAG for orchestrating all data ingestion DAGs.
Uses TriggerDagRunOperator to run child DAGs in sequence.

Schedules daily at 2 PM UTC and is the sole schedule owner for trigger-only
sources such as FotMob.

This DAG:
1. Triggers all ingestion DAGs in sequence
2. Waits for each to complete before proceeding
3. Validates overall pipeline success
4. Logs completion summary
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from utils.config import SCHEDULES, DAG_TAGS
from utils.default_args import DEFAULT_ARGS
from utils import transfermarkt_native_v2 as tm_v2


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

# A failed optional source may still be reported as degraded without blocking
# the historical master pipeline. WhoScored feeds xref/E3 event facts, while
# FotMob publishes a strict source-native completeness manifest. Publishing
# downstream from a failed or partial required source would mix generations.
REQUIRED_SOURCE_TASKS = {
    'dag_ingest_fotmob': 'ingestion_triggers.trigger_fotmob',
    'dag_ingest_whoscored': 'ingestion_triggers.trigger_whoscored',
}

# Publication children whose failure must make the current master DagRun fail.
# These form the Bronze -> source Silver -> xref/E3 -> Gold path; accepting a
# failed child as a successful trigger would publish a mixed generation and let
# the terminal report turn green despite failed DQ.
REQUIRED_PUBLICATION_TASKS = {
    'dag_transform_fbref_silver': 'trigger_fbref_silver',
    'dag_transform_xref': 'trigger_silver_xref',
    'dag_transform_e3': 'trigger_e3_transforms',
    'dag_transform_fbref_gold': 'trigger_fbref_gold',
}

# Extended default args for master pipeline
MASTER_ARGS = {
    **DEFAULT_ARGS,
    'execution_timeout': timedelta(hours=12),  # Long timeout for full pipeline
    'retries': 1,
}


def enforce_required_source_success(**context) -> Dict[str, str]:
    """Fail unless every required source trigger completed successfully.

    The check deliberately reads task instances from *this* master DagRun.
    Looking up the latest child DagRun is racy because a separately scheduled
    child run can finish while the master is still executing. The
    TriggerDagRunOperator is configured to fail when its WhoScored child fails,
    so its task-instance state is the exact publication evidence we need.
    """
    from airflow.exceptions import AirflowException

    dag_run = context.get('dag_run')
    if dag_run is None:
        raise AirflowException('required-source gate has no current DagRun')

    task_states = {}
    for task_instance in dag_run.get_task_instances():
        state = getattr(task_instance.state, 'value', task_instance.state)
        task_states[task_instance.task_id] = (
            str(state or 'none').lower().split('.')[-1]
        )

    required_states = {
        dag_id: task_states.get(task_id, 'missing')
        for dag_id, task_id in REQUIRED_SOURCE_TASKS.items()
    }
    invalid = {
        dag_id: state
        for dag_id, state in required_states.items()
        if state != 'success'
    }
    if invalid:
        details = ', '.join(
            f'{dag_id}={state}' for dag_id, state in sorted(invalid.items())
        )
        raise AirflowException(
            'Required ingestion source did not publish a complete successful '
            f'run; downstream transforms are blocked: {details}'
        )
    return required_states


def enforce_required_publication_success(**context) -> Dict[str, str]:
    """Fail unless the current master's required transform triggers succeeded."""
    from airflow.exceptions import AirflowException

    dag_run = context.get('dag_run')
    if dag_run is None:
        raise AirflowException('required-publication gate has no current DagRun')

    task_states = {}
    for task_instance in dag_run.get_task_instances():
        state = getattr(task_instance.state, 'value', task_instance.state)
        task_states[task_instance.task_id] = (
            str(state or 'none').lower().split('.')[-1]
        )

    required_states = {
        dag_id: task_states.get(task_id, 'missing')
        for dag_id, task_id in REQUIRED_PUBLICATION_TASKS.items()
    }
    invalid = {
        dag_id: state
        for dag_id, state in required_states.items()
        if state != 'success'
    }
    if invalid:
        details = ', '.join(
            f'{dag_id}={state}' for dag_id, state in sorted(invalid.items())
        )
        raise AirflowException(
            'Required publication transform did not complete successfully; '
            f'the master cannot report a published generation: {details}'
        )
    return required_states


def check_pipeline_success(**context) -> Dict[str, Any]:
    """
    Check overall pipeline success by examining triggered DAG runs.

    Returns:
        Pipeline status summary
    """
    import logging

    # This second check is intentional. The pre-transform gate prevents
    # publication, while the terminal check prevents an ``all_done`` summary
    # task from turning the master DagRun green after that gate failed.
    required_source_states = enforce_required_source_success(**context)
    required_publication_states = enforce_required_publication_success(**context)

    from airflow.models import DagRun
    from airflow.utils.state import State

    logger = logging.getLogger(__name__)

    results = {
        'status': 'success',
        'required_source_states': required_source_states,
        'required_publication_states': required_publication_states,
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


def _transfermarkt_gold_gate(**context) -> Dict[str, Any]:
    """Block Gold when the currently routed native scope set is no longer ready."""
    from airflow.exceptions import AirflowException

    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        state = tm_v2.read_reader_state(cur, allow_missing=True)
        result = state.to_dict()
        if state.active_version != 'v2':
            result['status'] = 'legacy_soft_gate'
            return result
        if not all((
            state.approved_cycle_id,
            state.approved_scope_set_id,
            state.approved_model_revision is not None,
            state.active_slot,
        )):
            raise AirflowException(
                'active TM v2 reader has incomplete scope-set evidence'
            )
        marker = tm_v2.readiness(
            cur,
            str(state.approved_cycle_id),
            expected_revision=int(state.approved_model_revision),
            scope_set_id=str(state.approved_scope_set_id),
            parent_cycle_id=str(state.approved_cycle_id),
            candidate_slot_override=str(state.active_slot),
            require_fresh=False,
            require_current_snapshots=False,
        )
        if (
            marker.get('scope_set_id') != state.approved_scope_set_id
            or marker.get('candidate_slot') != state.active_slot
            or int(marker.get('expected_state_revision', -1))
            != int(state.approved_model_revision)
            or not marker.get('ready')
        ):
            raise AirflowException(
                'active TM v2 scope-set readiness does not match the approved '
                f'cycle/slot/revision: {marker}'
            )
        views = tm_v2.verify_reader_views(
            cur,
            expected_version='v2',
            expected_revision=state.revision,
            expected_slot=state.active_slot,
            allow_static_slot=state.cleanup_completed_at is not None,
        )
        if not views['passed']:
            raise AirflowException(
                f'active TM v2 canonical reader verification failed: {views}'
            )
        result.update(
            status='v2_scope_set_and_views_ready',
            readiness=marker,
            reader_views=views,
        )
        return result
    finally:
        cur.close()
        conn.close()


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
    2. **FotMob** - source-native direct JSON discovery (trigger-only)
    3. **MatchHistory** (8:00 UTC) - Direct HTTP scraper
    4. **Understat** (9:00 UTC) - soccerdata library
    5. **WhoScored** (10:00 UTC) - Selenium with SPADL conversion
    6. **SofaScore** (11:00 UTC) - soccerdata library
    7. **ESPN** (12:00 UTC) - soccerdata library
    8. **ClubElo** (13:00 UTC) - ELO ratings

    ### E1: Silver xref step

    After all 8 ingestion DAGs finish, `dag_transform_xref` materialises the
    five Silver cross-reference tables (`xref_team`, `xref_match`,
    `xref_referee`, `xref_manager`, `xref_player`). It is fast (~1-2 min)
    and blocks before the daily summary so the T6 dual-run parity validator
    runs on fresh xref data.

    ### E3: Core event facts (Silver + Gold)

    After `dag_transform_xref` finishes, `dag_transform_e3` runs the E3
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

    Transfermarkt native-v2 writes are owned by its exact registry-driven
    ingest/transform cycle. The daily master performs one read-only route gate;
    legacy remains a soft dependency, while an active-v2 readiness failure
    blocks downstream Gold. Capology remains a normal trigger-only transform.

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
    dev Trino (3.5 GB heap). Capology/SoFIFA remain soft inputs; the dedicated
    Transfermarkt state gate is blocking only while canonical readers use v2.

    ### Notes

    - Each DAG is triggered with `wait_for_completion=True`
    - Optional source failures are reported as degraded; a failed/partial
      WhoScored child blocks all downstream publication and fails the master
    - Final report summarizes all DAG statuses
    - SoFIFA runs weekly (Sunday) and is not included here
    - Transfermarkt/Capology Bronze run weekly (Monday); master only re-
      materialises their Silver tables daily (idempotent CTAS, no re-scrape)
    """,
) as dag:

    # Create trigger tasks for each ingestion DAG
    trigger_tasks = []

    with TaskGroup(group_id='ingestion_triggers') as triggers_group:
        prev_task = None

        for dag_id in INGESTION_DAGS:
            required_source = dag_id in REQUIRED_SOURCE_TASKS
            trigger_task = TriggerDagRunOperator(
                task_id=f'trigger_{dag_id.replace("dag_ingest_", "")}',
                trigger_dag_id=dag_id,
                wait_for_completion=True,
                poke_interval=60,  # Check every minute
                allowed_states=(['success'] if required_source else ['success', 'failed']),
                failed_states=(['failed'] if required_source else []),
                reset_dag_run=True,  # Reset if already running
                execution_date='{{ ds }}',  # Airflow 2.x uses execution_date
                # Keep independent later sources running after a required
                # source failure. The explicit gate below blocks transforms.
                trigger_rule=('all_done' if prev_task else 'all_success'),
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

    required_sources_gate = PythonOperator(
        task_id='validate_required_sources',
        python_callable=enforce_required_source_success,
        # It must execute (and raise) when a required trigger failed.
        trigger_rule='all_done',
    )
    trigger_tasks >> required_sources_gate

    # =========================================================================
    # FBref source Silver publication
    # =========================================================================
    # This master-owned blocking trigger is the only automatic owner of the
    # FBref Silver generation. The ingest DAG publishes Bronze only, and the
    # Silver DAG never launches Gold. Keeping this step before xref/E3 prevents
    # the old nested asynchronous Silver/Gold path from racing the ordered
    # master publication chain.
    trigger_fbref_silver = TriggerDagRunOperator(
        task_id='trigger_fbref_silver',
        trigger_dag_id='dag_transform_fbref_silver',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        trigger_rule='all_success',
    )

    # =========================================================================
    # E1 medallion-redesign: Silver xref tables
    # =========================================================================
    # Runs AFTER all Bronze ingestion and the blocking FBref Silver generation.
    # xref_team/match/referee/manager/player read from iceberg.bronze.*; E3 then
    # consumes both fresh xref and source-Silver tables.
    #
    # `wait_for_completion=True` — the xref DAG is fast (~1-2 min) and
    # blocking lets the T6 dual-run parity validator run on freshly
    # written xref tables.
    #
    # The required-source gate is blocking. A failed WhoScored run must not
    # reach xref/E3/Gold with a mixed or stale Bronze snapshot.
    trigger_xref_task = TriggerDagRunOperator(
        task_id='trigger_silver_xref',
        trigger_dag_id='dag_transform_xref',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        trigger_rule='all_success',
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
    # safety; here we trigger it with the same fail-closed policy as the xref
    # step. A failed E3 DQ task blocks downstream Gold publication.
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
    # safety; here we simply trigger it with the same wait/parity policy as
    # the xref / E3 steps. `failed_states=[]` keeps master pipeline
    # resilient: an E4 failure surfaces via `validate_e4`'s
    # on_failure_callback (Telegram) but does not block the daily summary.
    trigger_e4_transforms = TriggerDagRunOperator(
        task_id='trigger_e4_transforms',
        trigger_dag_id='dag_transform_e4',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success', 'failed'],
        failed_states=[],
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
    trigger_silver_transfermarkt = PythonOperator(
        task_id='trigger_silver_transfermarkt',
        python_callable=_transfermarkt_gold_gate,
        trigger_rule='all_success',
    )

    trigger_silver_capology = TriggerDagRunOperator(
        task_id='trigger_silver_capology',
        trigger_dag_id='dag_transform_capology_silver',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success', 'failed'],
        failed_states=[],
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
        allowed_states=['success', 'failed'],
        failed_states=[],
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
    # The Gold DAG itself runs sequentially (max_active_tasks=1) for OOM safety;
    # Capology/SoFIFA remain soft. An active-v2 Transfermarkt readiness failure
    # is blocking because the canonical readers would otherwise be unsafe.
    trigger_fbref_gold = TriggerDagRunOperator(
        task_id='trigger_fbref_gold',
        trigger_dag_id='dag_transform_fbref_gold',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        reset_dag_run=True,
        execution_date='{{ ds }}',
        trigger_rule='none_failed_min_one_success',
    )

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
    (required_sources_gate >> trigger_fbref_silver >> trigger_xref_task
     >> trigger_e3_transforms >> trigger_e4_transforms)
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
