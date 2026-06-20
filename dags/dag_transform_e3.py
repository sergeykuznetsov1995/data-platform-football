"""
E3 Core-Facts Transformation DAG  (Medallion E3 / Wave 1+2)
============================================================

Materialises the three Gold-layer **core fact tables** that complete the
E3 wave of the medallion redesign, plus the two Silver-layer prerequisite
tables they depend on:

    iceberg.silver.whoscored_events_spadl   -- 39 WS types -> 24 SPADL actions
    iceberg.silver.espn_lineup              -- ESPN lineup, schema parity w/ FBref

    iceberg.gold.fct_event                  -- xref-resolved SPADL event spine
    iceberg.gold.fct_shot                   -- shot-level facts (xG / xA capable)
    iceberg.gold.fct_lineup                 -- starting XI + sub-on / sub-off

Topology
--------
::

    start_marker
        |
        v
    TaskGroup: silver_e3
        |-- whoscored_events_spadl   (run_silver_transform)
        |-- espn_lineup              (run_silver_transform)
        |
        v
    TaskGroup: gold_e3
        |-- fct_event    (run_gold_transform, partitions=['league','season'])
        |-- fct_shot     (run_gold_transform, partitions=['league','season'])
        |-- fct_lineup   (run_gold_transform, partitions=['league','season'])
        |
        v
    validate_e3      (DQ checks + Telegram summary)
        |
        v
    end_marker

Tasks inside each TaskGroup are wired with explicit ``>>`` dependencies AND
``max_active_tasks=1`` is set on the DAG. Either alone would serialise the
group; both together keep the topology readable in the UI and provide
defence-in-depth against Trino/HDFS overload (the same OOM-safety pattern
used by ``dag_transform_xref`` and the FBref Silver/Gold DAGs).

Trigger model
-------------
``schedule=None`` -- the DAG is triggered by ``dag_master_pipeline`` after
``dag_transform_xref`` has produced the Silver xref spine that all three
Gold facts depend on (E3.10 will wire the ``TriggerDagRunOperator``).

Re-running E3 standalone is safe -- ``run_silver_transform`` /
``run_gold_transform`` use DROP+CTAS, so each rebuild is atomic at the
table level and idempotent.

Upstream dependencies
---------------------
* Silver: ``silver.xref_match``, ``silver.xref_team``, ``silver.xref_player``
  (built by ``dag_transform_xref``).
* Bronze: ``bronze.whoscored_events``, ``bronze.understat_shot_events``,
  ``bronze.espn_lineup``, ``bronze.fbref_match_lineups`` (ingested by the
  per-source ingest DAGs).

Known limitations (carried over from the SQL-level ADRs)
--------------------------------------------------------
* **fct_event.match_id_canonical = NULL** for WhoScored events: WhoScored
  does NOT publish FBref-compatible match identifiers, so the xref bridge
  cannot resolve the match. We retain the row with the SPADL action and
  team_id_canonical, leaving downstream joins on match_id deferred until
  E3.5 ships an ESPN-mediated fuzzy bridge. Tracked in
  ``dags/sql/gold/fct_event.sql`` ADR.
* **fct_shot rejection ~1.8%** -- shots whose (date, home_canonical, away_canonical)
  do not resolve to a Gold dim_match row are dropped (INNER JOIN). Acceptable
  per E3.4 spec.
* **fct_lineup ESPN bridge ~90.8% success** -- the xxhash64-based name match
  leaves ~9% of ESPN players orphan. The orphan rows are NOT inserted (the
  bridge is INNER JOIN); see ADR in ``dags/sql/gold/fct_lineup.sql``.

DQ wiring (validate_e3)
-----------------------
DQ builders live in ``utils.e3_dq`` (E3.8). The validator imports
``build_all_e3_checks`` (39 standard checks) and the standalone
``parity_check_event_counts`` (Bronze->Silver->Gold row-count parity gate)
from that module. ERROR-severity failures raise ``AirflowException`` after
the Telegram summary is posted.

Notes for maintainers
---------------------
* All callables defer their imports inside the function body. The DAG
  parser must NOT pull ``scrapers/__init__.py`` (~1.5 GB RAM) -- the
  Silver / Gold runners use ``import trino`` directly.
* ``start_date=datetime(2026, 5, 9)`` is one day after ``today=2026-05-08``
  -- ``catchup=False`` plus ``schedule=None`` means no historical runs
  will ever be created.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-stage task registries
# ---------------------------------------------------------------------------
# (task_id, sql_file (relative to /opt/airflow/), target table name)
SILVER_E3_TRANSFORMS = [
    (
        'whoscored_events_spadl',
        'dags/sql/silver/whoscored_events_spadl.sql',
        'whoscored_events_spadl',
    ),
    (
        # issue #693: per-(match, player) lineup INFERRED from bronze.whoscored_events
        # (appeared & not subbed-on ⇒ starter) + whoscored_schedule team-name bridge.
        # Feeds the WhoScored branch of gold.fct_lineup (player_id + is_starter only;
        # position/captain/jersey NULL — WhoScored events have no lineup block).
        'whoscored_lineup',
        'dags/sql/silver/whoscored_lineup.sql',
        'whoscored_lineup',
    ),
    (
        'espn_lineup',
        'dags/sql/silver/espn_lineup.sql',
        'espn_lineup',
    ),
    (
        # B1 extension: per-(canonical_id, league, season) aggregate of
        # WhoScored event-level metrics. Reads silver.whoscored_events_spadl
        # so MUST run after the spadl task (sequential silver_e3 group
        # guarantees this).
        'whoscored_player_season_aggregate',
        'dags/sql/silver/whoscored_player_season_aggregate.sql',
        'whoscored_player_season_aggregate',
    ),
    (
        # issue #46: per-(canonical_id, match_id, league, season) WhoScored
        # match-level aggregate. Aggregates bronze.whoscored_events by
        # (game_id, player_id) — shots/passes/tackles/interceptions/fouls/
        # duels/dribbles via COUNT FILTER. Feeds the WhoScored block of
        # gold.fct_player_match. MUST run after dag_transform_xref (joins
        # silver.xref_player / xref_match) and after the spadl task.
        'whoscored_player_match_aggregate',
        'dags/sql/silver/whoscored_player_match_aggregate.sql',
        'whoscored_player_match_aggregate',
    ),
    (
        # T6.3 (#92): per-(match_id, team_id) WhoScored team match-aggregate.
        # GROUP BY on silver.whoscored_events_spadl — pass/take-on/tackle/
        # interception/shot/foul/spatial counters via COUNT_IF. Feeds the
        # WhoScored block of gold.fct_team_match v2 (#95). MUST run after
        # whoscored_events_spadl. Season rollup инлайнен CTE в
        # gold.fct_team_season_stats (#478).
        'whoscored_team_match',
        'dags/sql/silver/whoscored_team_match.sql',
        'whoscored_team_match',
    ),
    (
        # Per-(canonical_id, league, season) aggregate of Understat player
        # season metrics. Reads bronze.understat_players directly and joins
        # silver.xref_player for canonical_id resolution -- so it must run
        # AFTER xref_player has been materialised by dag_transform_xref.
        # Provides the Understat-side feed for gold.fct_player_season_stats.
        'understat_player_season_aggregate',
        'dags/sql/silver/understat_player_season_aggregate.sql',
        'understat_player_season_aggregate',
    ),
    (
        # issue #46: per-(canonical_id, match_id, league, season) Understat
        # match-level aggregate. Passthrough из bronze.understat_player_match_stats
        # с dedup ROW_NUMBER. Feeds Understat block of gold.fct_player_match.
        # MUST run after dag_transform_xref (joins silver.xref_player / xref_match).
        'understat_player_match_aggregate',
        'dags/sql/silver/understat_player_match_aggregate.sql',
        'understat_player_match_aggregate',
    ),
    (
        # T6.2 (#91): per-(match_id, team_id_canonical) Understat team
        # match-facts (xG/NPxG/PPDA/deep/points/xPts). UNION ALL home+away из
        # bronze.understat_team_match_stats + JOIN silver.xref_team
        # (source='understat'). Feeds Understat block of gold.fct_team_match.
        # MUST run after dag_transform_xref. Season rollup инлайнен CTE в
        # gold.fct_team_season_stats (#478).
        'understat_team_match',
        'dags/sql/silver/understat_team_match.sql',
        'understat_team_match',
    ),
    (
        # T6: SofaScore per-(canonical_id, league, season) aggregate.
        # Reads bronze.sofascore_player_season_stats + silver.xref_player
        # (source='sofascore') — MUST run after dag_transform_xref has
        # materialised the SofaScore bridge rows. Provides the SofaScore
        # feed for gold.fct_player_season_stats.
        'sofascore_player_season_aggregate',
        'dags/sql/silver/sofascore_player_season_aggregate.sql',
        'sofascore_player_season_aggregate',
    ),
    (
        # issue #46: per-(canonical_id, match_id, league, season) SofaScore
        # match-level aggregate. Passthrough из bronze.sofascore_event_player_stats
        # (statistics struct уже flattened) с dedup ROW_NUMBER. Feeds SofaScore
        # block of gold.fct_player_match (rating, xg, xa, shots, touches и т.п.).
        # MUST run after dag_transform_xref (joins silver.xref_player / xref_match).
        'sofascore_player_match_aggregate',
        'dags/sql/silver/sofascore_player_match_aggregate.sql',
        'sofascore_player_match_aggregate',
    ),
    (
        # Time-invariant атрибуты игрока (height, foot, dob, nationality)
        # из SofaScore. Снепшот-таблица per (player_id, league, season)
        # с подтянутым canonical_id из silver.xref_player — feeds the
        # SofaScore-block в gold.dim_player_attributes.
        'sofascore_player_profile',
        'dags/sql/silver/sofascore_player_profile.sql',
        'sofascore_player_profile',
    ),
    (
        # T6.4 (#93): per-(match_id, team_id) SofaScore team match-aggregate.
        # Single-source conform: PIVOT bronze.sofascore_match_stats (period='ALL')
        # + JOIN bronze.sofascore_schedule (goals_for/against). No silver.* reads
        # (#367 removed the cross-entity minutes/assists rollup — Silver Charter R2).
        # Feeds SofaScore block of gold.fct_team_match v2 (#95).
        # Season rollup инлайнен CTE в gold.fct_team_season_stats (#478).
        'sofascore_team_match',
        'dags/sql/silver/sofascore_team_match.sql',
        'sofascore_team_match',
    ),
    (
        # issue #602: shot-grained SofaScore shotmap projection, canonicalised
        # to fct_shot's match/team/player IDs. Reads bronze.sofascore_event_shotmap
        # + sofascore_schedule + silver.xref_{match,team,player} (source='sofascore')
        # — MUST run after dag_transform_xref. Consumers: gold.fct_shot_audit
        # (cross-source xG/SoT validation vs Understat) AND gold.fct_shot (#699,
        # match-level fallback source — runs in gold_e3, after this silver group).
        'sofascore_shots',
        'dags/sql/silver/sofascore_shots.sql',
        'sofascore_shots',
    ),
]

GOLD_E3_TRANSFORMS = [
    (
        'fct_event',
        'dags/sql/gold/fct_event.sql',
        'fct_event',
    ),
    (
        'fct_shot',
        'dags/sql/gold/fct_shot.sql',
        'fct_shot',
    ),
    (
        # issue #602: cross-source shot audit (Understat fct_shot vs SofaScore
        # silver.sofascore_shots). MUST run after fct_shot (reads it as the
        # Understat spine). WARNING-only DQ table, grain (match_id, team_id).
        'fct_shot_audit',
        'dags/sql/gold/fct_shot_audit.sql',
        'fct_shot_audit',
    ),
    (
        'fct_lineup',
        'dags/sql/gold/fct_lineup.sql',
        'fct_lineup',
    ),
]

# Partition columns common to all three Gold facts (per the SQL files'
# SELECT lists -- league/season are emitted as last columns to feed the
# Iceberg ``partitioning`` clause set up by ``run_silver_transform``).
GOLD_E3_PARTITION_COLUMNS = ['league', 'season']


# ---------------------------------------------------------------------------
# Task callables -- imports are inside the callables so DAG parse stays cheap
# (the DAG parser must NOT pull ``scrapers/__init__.py`` ~1.5 GB).
# ---------------------------------------------------------------------------

def _run_silver_e3(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """Run an E3 Silver CTAS via :func:`utils.silver_tasks.run_silver_transform`.

    Thin wrapper -- exists only so the DAG can hand a uniform callable to
    every PythonOperator in ``silver_e3`` (and so the row-count returned
    by the runner is logged in a consistent format).
    """
    from utils.silver_tasks import run_silver_transform

    result = run_silver_transform(
        sql_file=sql_file,
        table_name=table_name,
        schema='silver',
    )
    logger.info(
        "silver_e3.%s CTAS complete: %d rows in %s",
        table_name,
        result.get('rows', 0),
        result.get('table'),
    )
    return result


def _run_gold_e3(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """Run an E3 Gold CTAS via :func:`utils.gold_tasks.run_gold_transform`.

    Always partitions by ``(league, season)`` -- all three E3 facts are
    APL-only at MVP but partitioning future-proofs them for E8 multi-
    competition expansion. The partition columns MUST be the last columns
    in the SELECT (which they are -- see SQL files).
    """
    from utils.gold_tasks import run_gold_transform

    result = run_gold_transform(
        sql_file=sql_file,
        table_name=table_name,
        partition_columns=list(GOLD_E3_PARTITION_COLUMNS),
    )
    logger.info(
        "gold_e3.%s CTAS complete: %d rows in %s (partitions=%s)",
        table_name,
        result.get('rows', 0),
        result.get('table'),
        GOLD_E3_PARTITION_COLUMNS,
    )
    return result


def _validate_e3(**context) -> Dict[str, Any]:
    """Run E3-scoped DQ checks and post a Telegram summary.

    DQ list comes from :func:`utils.e3_dq.build_all_e3_checks` (E3.8) — 39
    standard checks across silver.whoscored_events_spadl, silver.espn_lineup,
    gold.fct_event/fct_shot/fct_lineup. The standalone
    :func:`utils.e3_dq.parity_check_event_counts` adds a Bronze->Silver->Gold
    row-count parity gate (ERROR severity, runs after standard checks).

    Severity model — ERROR-severity failures raise ``AirflowException``
    after the Telegram summary is posted. WARNING-severity failures are
    logged + reported but do NOT fail the DAG.
    """
    from airflow.exceptions import AirflowException

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CheckResult, run_checks
    from utils.e3_dq import build_all_e3_checks, parity_check_event_counts

    all_checks = build_all_e3_checks()
    logger.info("E3 DQ: running %d standard checks from utils.e3_dq", len(all_checks))

    # Standard CHECK.* primitives. raise_on_error=False so Telegram fires
    # before we re-raise on ERROR-severity failures.
    report = run_checks(all_checks, raise_on_error=False)

    # Bronze->Silver->Gold parity gate (custom — not expressible via
    # standard CHECK.* registry). Append result to the same report so the
    # Telegram summary covers it too.
    try:
        parity_result = parity_check_event_counts()
        report.results.append(parity_result)
    except Exception as e:
        logger.exception("parity_check_event_counts crashed; recording WARNING")
        report.results.append(CheckResult(
            name='parity_check_event_counts',
            kind='custom',
            severity='WARNING',
            passed=False,
            error=str(e),
        ))

    logger.info("E3 DQ: %s", report.summary())

    telegram_dq_summary(report, header="E3 Core Facts DQ")

    if report.errors:
        raise AirflowException(
            f"E3 DQ failed: {len(report.errors)} error(s). "
            + "; ".join(
                f"{r.name}: {r.details or r.error}"
                for r in report.errors[:5]
            )
        )

    return {
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='dag_transform_e3',
    default_args=SILVER_ARGS,
    description=(
        'Materialise E3 core facts: silver.whoscored_events_spadl + '
        'silver.espn_lineup -> gold.fct_event / fct_shot / fct_lineup. '
        'Triggered after dag_transform_xref by master pipeline.'
    ),
    schedule=None,                 # Triggered by dag_master_pipeline (E3.10)
    start_date=datetime(2026, 5, 9),
    catchup=False,
    tags=['silver', 'gold', 'medallion-e3', 'transform', 'core-facts'],
    max_active_runs=1,
    max_active_tasks=1,            # Sequential -- OOM-safety, same as Silver/xref/fbref
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id='start_marker')

    # =========================================================================
    # TaskGroup: silver_e3 (sequential pure-SQL CTAS, max_active_tasks=1)
    # =========================================================================
    with TaskGroup(group_id='silver_e3') as silver_group:
        prev = None
        for task_id, sql_file, table_name in SILVER_E3_TRANSFORMS:
            t = PythonOperator(
                task_id=task_id,
                python_callable=_run_silver_e3,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
            )
            if prev is not None:
                prev >> t
            prev = t

    # =========================================================================
    # TaskGroup: gold_e3 (sequential pure-SQL CTAS, max_active_tasks=1)
    # =========================================================================
    with TaskGroup(group_id='gold_e3') as gold_group:
        prev = None
        for task_id, sql_file, table_name in GOLD_E3_TRANSFORMS:
            t = PythonOperator(
                task_id=task_id,
                python_callable=_run_gold_e3,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
            )
            if prev is not None:
                prev >> t
            prev = t

    # =========================================================================
    # Validation -- DQ checks + Telegram summary
    # =========================================================================
    validate_task = PythonOperator(
        task_id='validate_e3',
        python_callable=_validate_e3,
        trigger_rule='all_success',  # Skip validation if any transform failed
    )

    end = EmptyOperator(task_id='end_marker')

    # =========================================================================
    # Dependencies
    # =========================================================================
    start >> silver_group >> gold_group >> validate_task >> end
