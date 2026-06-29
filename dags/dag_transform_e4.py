"""
E4 Narrow-Facts Transformation DAG  (Medallion E4 / Wave 1+2)
=============================================================

Materialises the two Gold-layer **narrow fact tables** that remain from the
E4 wave of the medallion redesign, plus their two Silver-layer
prerequisite tables:

    iceberg.silver.matchhistory_match_odds        -- pre-match + closing odds
    iceberg.silver.sofascore_player_ratings       -- per-match player ratings

    iceberg.gold.fct_match_odds                   -- passthrough silver
    iceberg.gold.fct_match_rating                 -- passthrough silver

(``fct_goal`` / ``fct_card`` / ``fct_substitution`` were dropped in #448 --
superseded by ``gold.fct_match_timeline``, built in
``dag_transform_fbref_gold`` -- #427.)

Topology
--------
::

    start_marker
        |
        v
    TaskGroup: silver_e4
        |-- matchhistory_match_odds      (run_silver_transform)
        |-- sofascore_player_ratings     (run_silver_transform; skipped
        |                                 if bronze missing)
        |
        v
    TaskGroup: gold_e4
        |-- fct_match_odds     (run_gold_transform, partitions=['league','season'])
        |-- fct_match_rating   (run_gold_transform, partitions=['league','season'])
        |
        v
    validate_e4      (DQ checks + Telegram summary; build_all_e4_checks)
        |
        v
    end_marker

Tasks inside each TaskGroup are wired with explicit ``>>`` dependencies AND
``max_active_tasks=1`` is set on the DAG. Either alone would serialise the
group; both together keep the topology readable in the UI and provide
defence-in-depth against Trino/HDFS overload (the same OOM-safety pattern
used by ``dag_transform_e3`` / ``dag_transform_xref``).

Trigger model
-------------
``schedule=None`` -- the DAG is triggered by ``dag_master_pipeline`` after
``dag_transform_e3`` succeeds (E4.10). Re-running E4 standalone is safe --
``run_silver_transform`` / ``run_gold_transform`` use DROP+CTAS, so each
rebuild is atomic at the table level and idempotent.

Upstream dependencies
---------------------
* Silver: ``silver.xref_match``, ``silver.xref_team``, ``silver.xref_player``
  (built by ``dag_transform_xref``).
* Bronze: ``bronze.matchhistory_match_odds``,
  ``bronze.sofascore_player_ratings`` (E4.1 smoke; defensively guarded
  via ``check_bronze_table_exists`` so an empty/absent Bronze yields a
  task SKIP rather than a hard failure).

Known limitations (carried over from the SQL-level ADRs)
--------------------------------------------------------
* ``fct_match_odds`` /
  ``fct_match_rating`` ref_integrity to ``gold.dim_match`` is WARNING-only
  pending Phase B bridging via ``xref_match`` for non-FBref sources
  (matchhistory bookmaker fixture IDs, sofascore game IDs, whoscored
  numeric event IDs). Tracked in ``utils.e4_dq``.
* ``sofascore_player_ratings`` thresholds are smoke-tolerant (50-row floor)
  until E4 Phase 1.5 backfill -- see ``utils.e4_dq``.

DQ wiring (validate_e4)
-----------------------
DQ builders live in ``utils.e4_dq`` (E4.7). The validator imports
``build_all_e4_checks`` across
the two Silver tables and two Gold facts. ERROR-severity failures raise
``AirflowException`` after the Telegram summary is posted. WARNING-severity
failures are logged + reported but do NOT fail the DAG.

Notes for maintainers
---------------------
* All callables defer their imports inside the function body. The DAG
  parser must NOT pull ``scrapers/__init__.py`` (~1.5 GB RAM) -- the
  Silver / Gold runners use ``import trino`` directly.
* ``start_date=datetime(2026, 5, 9)`` is one day after ``today=2026-05-08``
  -- ``catchup=False`` plus ``schedule=None`` means no historical runs
  will ever be created.
* ``sofascore_player_ratings`` is bronze-guarded: if
  ``iceberg.bronze.sofascore_player_ratings`` is missing (R0.2B fallback
  not yet deployed), the task raises ``AirflowSkipException`` so the
  rest of the DAG keeps moving. Once E4.1 is fully shipped, the guard
  is a cheap no-op (one ``SHOW TABLES`` query).
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
SILVER_E4_TRANSFORMS = [
    # match_cards / match_substitutions were folded into the Gold facts (#382),
    # which were in turn superseded by gold.fct_match_timeline (#448).
    (
        'matchhistory_match_odds',
        'dags/sql/silver/matchhistory_match_odds.sql',
        'matchhistory_match_odds',
    ),
    (
        'sofascore_player_ratings',
        'dags/sql/silver/sofascore_player_ratings.sql',
        'sofascore_player_ratings',
    ),
]

GOLD_E4_TRANSFORMS = [
    # fct_goal / fct_card / fct_substitution dropped in #448 — superseded by
    # gold.fct_match_timeline (#427, built in dag_transform_fbref_gold).
    (
        'fct_match_odds',
        'dags/sql/gold/fct_match_odds.sql',
        'fct_match_odds',
    ),
    (
        'fct_match_rating',
        'dags/sql/gold/fct_match_rating.sql',
        'fct_match_rating',
    ),
]

# Partition columns common to both Gold facts (per the SQL files'
# SELECT lists -- league/season are emitted as last columns to feed the
# Iceberg ``partitioning`` clause set up by ``run_silver_transform``).
GOLD_E4_PARTITION_COLUMNS = ['league', 'season']

# Silver tables whose Bronze upstream may be ABSENT in the current
# environment (R0.2B fallback / partial-backfill scenarios). The DAG
# runs ``check_bronze_table_exists`` before invoking the runner; on a
# miss the task raises AirflowSkipException so the rest of the DAG
# keeps moving and a Telegram alert is NOT fired (skip is benign).
#
# NB: a present-but-stale Bronze is still treated as a normal upstream;
# this guard only addresses the "table simply does not exist" race.
OPTIONAL_BRONZE_FOR_E4_SILVER: Dict[str, str] = {
    # silver task_id -> bronze table name (in iceberg.bronze.*)
    'sofascore_player_ratings': 'sofascore_player_ratings',
}


# ---------------------------------------------------------------------------
# Task callables -- imports are inside the callables so DAG parse stays cheap
# (the DAG parser must NOT pull ``scrapers/__init__.py`` ~1.5 GB).
# ---------------------------------------------------------------------------

def _run_silver_e4(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """Run an E4 Silver CTAS via :func:`utils.silver_tasks.run_silver_transform`.

    Optional-Bronze guard: if the silver task is registered in
    :data:`OPTIONAL_BRONZE_FOR_E4_SILVER` and its Bronze upstream is
    missing, raise ``AirflowSkipException`` rather than letting Trino
    emit a ``TableNotFoundException`` mid-CTAS. This keeps the DAG
    green during R0.2B partial-backfill phases.
    """
    from airflow.exceptions import AirflowSkipException

    from utils.silver_tasks import (
        check_bronze_table_exists,
        run_silver_transform,
    )

    # Defensive optional-Bronze guard.
    bronze_dep = OPTIONAL_BRONZE_FOR_E4_SILVER.get(table_name)
    if bronze_dep is not None:
        if not check_bronze_table_exists(table_name=bronze_dep, schema='bronze'):
            msg = (
                f"silver_e4.{table_name}: bronze.{bronze_dep} is absent — "
                "skipping (R0.2B fallback). Re-enable once E4.1 backfill ships."
            )
            logger.warning(msg)
            raise AirflowSkipException(msg)

    result = run_silver_transform(
        sql_file=sql_file,
        table_name=table_name,
        schema='silver',
    )
    logger.info(
        "silver_e4.%s CTAS complete: %d rows in %s",
        table_name,
        result.get('rows', 0),
        result.get('table'),
    )
    return result


def _run_gold_e4(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """Run an E4 Gold CTAS via :func:`utils.gold_tasks.run_gold_transform`.

    Always partitions by ``(league, season)`` -- both E4 facts are
    APL-only at MVP but partitioning future-proofs them for E8 multi-
    competition expansion. The partition columns MUST be the last columns
    in the SELECT (which they are -- see SQL files).
    """
    from utils.gold_tasks import run_gold_transform

    result = run_gold_transform(
        sql_file=sql_file,
        table_name=table_name,
        partition_columns=list(GOLD_E4_PARTITION_COLUMNS),
    )
    logger.info(
        "gold_e4.%s CTAS complete: %d rows in %s (partitions=%s)",
        table_name,
        result.get('rows', 0),
        result.get('table'),
        GOLD_E4_PARTITION_COLUMNS,
    )
    return result


def _validate_e4(**context) -> Dict[str, Any]:
    """Run E4-scoped DQ checks and post a Telegram summary.

    DQ list comes from :func:`utils.e4_dq.build_all_e4_checks` (E4.7) —
    standard checks across silver.matchhistory_match_odds /
    sofascore_player_ratings and gold.fct_match_odds / fct_match_rating
    (fct_goal/fct_card/fct_substitution dropped in #448).

    Severity model — ERROR-severity failures raise ``AirflowException``
    after the Telegram summary is posted. WARNING-severity failures are
    logged + reported but do NOT fail the DAG.
    """
    from airflow.exceptions import AirflowException

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import run_checks
    from utils.e4_dq import build_all_e4_checks

    all_checks = build_all_e4_checks()
    logger.info("E4 DQ: running %d standard checks from utils.e4_dq", len(all_checks))

    # raise_on_error=False so Telegram fires before we re-raise on
    # ERROR-severity failures.
    report = run_checks(all_checks, raise_on_error=False)

    logger.info("E4 DQ: %s", report.summary())

    telegram_dq_summary(report, header="E4 Narrow Facts DQ")

    if report.errors:
        raise AirflowException(
            f"E4 DQ failed: {len(report.errors)} error(s). "
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
    dag_id='dag_transform_e4',
    default_args=SILVER_ARGS,
    description=(
        'Materialise E4 narrow facts: silver.matchhistory_match_odds / '
        'sofascore_player_ratings -> gold.fct_match_odds / fct_match_rating '
        '(fct_goal/fct_card/fct_substitution dropped in #448). '
        'Triggered after dag_transform_e3 by master pipeline.'
    ),
    schedule=None,                 # Triggered by dag_master_pipeline (E4.10)
    start_date=datetime(2026, 5, 9),
    catchup=False,
    tags=['silver', 'gold', 'medallion-e4', 'transform', 'narrow-facts'],
    max_active_runs=1,
    max_active_tasks=1,            # Sequential -- OOM-safety, same as Silver/xref/e3
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id='start_marker')

    # =========================================================================
    # TaskGroup: silver_e4 (sequential pure-SQL CTAS, max_active_tasks=1)
    # =========================================================================
    with TaskGroup(group_id='silver_e4') as silver_group:
        prev = None
        for task_id, sql_file, table_name in SILVER_E4_TRANSFORMS:
            t = PythonOperator(
                task_id=task_id,
                python_callable=_run_silver_e4,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
            )
            if prev is not None:
                prev >> t
            prev = t

    # =========================================================================
    # TaskGroup: gold_e4 (sequential pure-SQL CTAS, max_active_tasks=1)
    # =========================================================================
    # ``trigger_rule='none_failed'`` lets gold_e4 run even when the
    # optional sofascore Silver task SKIPS. A failure (not a skip) still
    # propagates and short-circuits the chain.
    with TaskGroup(group_id='gold_e4') as gold_group:
        prev = None
        for task_id, sql_file, table_name in GOLD_E4_TRANSFORMS:
            t = PythonOperator(
                task_id=task_id,
                python_callable=_run_gold_e4,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
                # First gold task tolerates a skipped optional silver
                # upstream; the rest chain off it normally (all_success).
                trigger_rule='none_failed' if prev is None else 'all_success',
            )
            if prev is not None:
                prev >> t
            prev = t

    # =========================================================================
    # Validation -- DQ checks + Telegram summary
    # =========================================================================
    # ``trigger_rule='none_failed'`` so a SKIP on sofascore Silver doesn't
    # block validation of the other 4 silver / 5 gold tables. A failure
    # still short-circuits.
    validate_task = PythonOperator(
        task_id='validate_e4',
        python_callable=_validate_e4,
        trigger_rule='none_failed',
    )

    end = EmptyOperator(task_id='end_marker')

    # =========================================================================
    # Dependencies
    # =========================================================================
    start >> silver_group >> gold_group >> validate_task >> end
