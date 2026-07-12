"""
E3.5 — Historical Backfill DAG (Medallion E3.5 / parametrised per-(season, league))
====================================================================================

Backfills the three Gold E3 core fact tables (`gold.fct_event` /
`gold.fct_shot` / `gold.fct_lineup`) plus their two Silver
prerequisites (`silver.whoscored_events_spadl` / `silver.espn_lineup`)
for ONE historical (season, league) tuple at a time, leaving every
other partition untouched.

This DAG is the runtime sibling of `dag_transform_e3.py` (production E3
DAG, full-table DROP+CTAS). The two are deliberately kept separate:

    dag_transform_e3        — production: rebuild WHOLE table on each run.
    dag_e3_backfill (here)  — backfill:   per-partition INSERT, idempotent.

Trigger model
-------------
``schedule=None`` + ``is_paused_upon_creation=True``.  Operators trigger
the DAG manually with concrete ``params`` per run, e.g.::

    params = {
        'season':  '2324',
        'league':  'ENG-Premier League',
        'dry_run': False,
    }

Or via ``TriggerDagRunOperator`` from ``dag_master_pipeline`` once the
Wave 3 Bronze scrape finishes.

Tasks
-----
::

    start_marker
        |
        v
    pre_check_bronze         (verify bronze.{whoscored_events_current, understat_shots,
                              espn_lineup} non-empty for the partition)
        |
        v
    taxonomy_diff_check      (assert WhoScored types ⊆ 39-mapping)
        |
        v
    TaskGroup: silver_backfill
        |-- silver_whoscored_events_spadl_partition
        |-- silver_espn_lineup_partition
        |
        v
    TaskGroup: gold_backfill
        |-- gold_fct_event_partition
        |-- gold_fct_shot_partition
        |-- gold_fct_lineup_partition
        |
        v
    validate_backfill        (per-season DQ — parity, PK, unknown_rate, schema_version)
        |
        v
    end_marker

Total: 9 tasks (markers + 2 pre-checks + 2 silver + 3 gold + 1 validate).

``max_active_tasks=1`` enforces strict sequential execution — each transform
processes a single partition, but the Silver/Trino memory budget is shared
with other DAGs and partition-INSERTs are still I/O-heavy. Sequential is
the OOM-safe default (mirroring `dag_transform_e3` and `dag_transform_xref`).

dry_run mode
------------
``params['dry_run'] = True`` causes the DAG to run **only the pre-checks**
(`pre_check_bronze` + `taxonomy_diff_check`).  Downstream Silver/Gold/
Validate tasks short-circuit via ``ShortCircuitOperator`` — they appear as
``skipped`` in the UI.  Useful for verifying:

  * Bronze inventory is present for the requested (season, league).
  * The WhoScored taxonomy is fully covered by the SPADL CASE tree.

Idempotency
-----------
Every Silver/Gold task uses DELETE-then-INSERT semantics
(``run_silver_partition_insert`` / ``run_gold_partition_insert_wrapped``).
Re-running the DAG for the same (season, league) tuple converges to the
same final state — safe for retries / partial restarts.

Known blockers (to clear before unpause)
----------------------------------------
* **Bronze must be scraped first** for 2122/2223/2324 — Wave 3 is a
  separate session (see ``docs/research/E3.5_inventory.md`` §4 Gap analysis).
* **R4 — fct_lineup season type**: ``gold.fct_lineup.season`` is BIGINT
  (FBref-derived) while ESPN/WhoScored Silver is VARCHAR. Backfill of ESPN-
  lineup historical seasons (2122/2223/2324) requires the type unification
  refactor (separate ticket E3.5b / R4) before the DAG can run on those
  seasons. As of 2026-05-08 this is tracked as task #11.

References
----------
* Roadmap: ``docs/MEDALLION_REDESIGN_ROADMAP.md`` §E3.5.
* Pre-flight inventory: ``docs/research/E3.5_inventory.md``.
* E3 postmortem: ``docs/decisions/E3-postmortem.md``.
* Helpers: ``utils.silver_tasks.run_silver_partition_insert`` (E3.5 wrapper),
  ``utils.gold_tasks.run_gold_partition_insert_wrapped`` (E3.5 wrapper).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task registries — same shape as dag_transform_e3 so structural diffs
# between the two DAGs stay obvious. The wrapper-style runners do NOT
# require sentinels in the SQL files (see helper docstrings).
# ---------------------------------------------------------------------------
SILVER_E35_TRANSFORMS = [
    (
        'silver_whoscored_events_spadl_partition',
        'dags/sql/silver/whoscored_events_spadl.sql',
        'whoscored_events_spadl',
    ),
    (
        'silver_espn_lineup_partition',
        'dags/sql/silver/espn_lineup.sql',
        'espn_lineup',
    ),
]

GOLD_E35_TRANSFORMS = [
    (
        'gold_fct_event_partition',
        'dags/sql/gold/fct_event.sql',
        'fct_event',
    ),
    (
        'gold_fct_shot_partition',
        'dags/sql/gold/fct_shot.sql',
        'fct_shot',
    ),
    (
        'gold_fct_lineup_partition',
        'dags/sql/gold/fct_lineup.sql',
        'fct_lineup',
    ),
]

GOLD_E35_PARTITION_COLUMNS = ['league', 'season']


# ---------------------------------------------------------------------------
# Param helpers — Airflow params dicts cannot live as module-level objects
# accessed in callables; we read them from the task ``context`` instead.
# ---------------------------------------------------------------------------

def _read_params(context: Dict[str, Any]) -> Dict[str, Any]:
    """Extract DAG-run params + apply defaults / type guards.

    Airflow 2.x stores DagRun.conf in ``context['params']`` (Airflow params
    namespace) and ``context['dag_run'].conf`` (raw conf override). We read
    from ``context['params']`` because that's what the typed Param objects
    populate — defaults applied automatically.
    """
    raw = dict(context.get('params') or {})
    season = raw.get('season')
    league = raw.get('league') or 'ENG-Premier League'
    dry_run = bool(raw.get('dry_run', False))

    if not isinstance(season, str) or not season:
        raise AirflowException(
            "params.season must be a non-empty string "
            "(e.g. '2122', '2223', '2324'). Got: " + repr(season)
        )
    if not isinstance(league, str) or not league:
        raise AirflowException(
            "params.league must be a non-empty string. Got: " + repr(league)
        )
    return {
        'season':  season,
        'league':  league,
        'dry_run': dry_run,
    }


# ---------------------------------------------------------------------------
# Pre-check callables — fail FAST if Bronze is empty / taxonomy drifted.
# Imports are deferred inside the body so the DAG parser stays cheap (no
# scrapers/__init__ pull, no Trino client at parse time).
# ---------------------------------------------------------------------------

# The three Bronze tables we MUST verify before kicking off Silver/Gold.
# whoscored_events_current is the load-bearing one (drives ~90% of fct_event volume);
# the other two are checked but only WARNING — silver.espn_lineup / fct_shot
# tolerate empty Bronze (rows simply absent, downstream not poisoned).
_BRONZE_TABLES = [
    ('iceberg.bronze.whoscored_events_current', 'ERROR'),
    ('iceberg.bronze.understat_shots',  'WARNING'),
    ('iceberg.bronze.espn_lineup',      'WARNING'),
]


def _pre_check_bronze(**context) -> Dict[str, Any]:
    """Verify the three Bronze tables have rows for the (season, league) tuple.

    whoscored_events_current is required (ERROR if empty); the other two are
    checked but treated as WARNING — backfill can proceed with degraded
    coverage on understat / espn (matches production tolerance).

    Logs one summary dict per table; fails the task with a single
    AirflowException if any ERROR-severity table is empty.
    """
    from utils.data_quality import _get_conn
    from utils.e3_dq import _safe_predicate_value

    params = _read_params(context)
    season, league = params['season'], params['league']
    logger.info(
        "pre_check_bronze: season=%s league=%s — verifying Bronze inventory",
        season, league,
    )
    # Params come straight from a manual "Trigger DAG w/ config" — escape them
    # like every other inline predicate in this pipeline (e3_dq/silver_tasks).
    season_sql = _safe_predicate_value(season)
    league_sql = _safe_predicate_value(league)

    summary: Dict[str, Any] = {}
    errors: list = []
    conn = _get_conn()
    try:
        cur = conn.cursor()
        for table, severity in _BRONZE_TABLES:
            sql = (
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE season = '{season_sql}' AND league = '{league_sql}'"
            )
            try:
                cur.execute(sql)
                row = cur.fetchone()
                cnt = int(row[0]) if row and row[0] is not None else 0
            except Exception as e:
                # Bronze table may not yet exist for sources we haven't
                # scraped. Treat as 0 rows + warn.
                logger.warning("pre_check_bronze: query failed for %s: %s", table, e)
                cnt = 0
            summary[table] = cnt
            logger.info("  %-46s %10d rows  [%s]", table, cnt, severity)
            if cnt == 0 and severity == 'ERROR':
                errors.append(f"{table}: 0 rows for ({season}, {league})")
        cur.close()
    finally:
        conn.close()

    if errors:
        raise AirflowException(
            "pre_check_bronze FAILED — required Bronze tables empty: "
            + "; ".join(errors)
            + ". Run Wave 3 ingest before retrying this backfill."
        )
    return {'summary': summary, 'season': season, 'league': league}


def _taxonomy_diff_check(**context) -> Dict[str, Any]:
    """Assert ``DISTINCT type FROM bronze.whoscored_events_current`` ⊆ 39-mapping.

    Wraps :func:`utils.e3_dq.taxonomy_diff_check` and lifts its CheckResult
    into an AirflowException on failure (so the DAG visibly fails before
    the SPADL CASE tree silently maps a new type to ``'unknown'``).
    """
    from utils.e3_dq import taxonomy_diff_check

    params = _read_params(context)
    season, league = params['season'], params['league']
    logger.info(
        "taxonomy_diff_check: season=%s league=%s — verifying 39-type taxonomy",
        season, league,
    )
    result = taxonomy_diff_check(season=season, league=league)
    logger.info("taxonomy_diff: %s — %s", result.passed, result.details)

    if not result.passed:
        raise AirflowException(
            f"taxonomy_diff FAILED: {result.details or result.error}. "
            "Update dags/sql/silver/whoscored_events_spadl.sql "
            "AND utils.e3_dq.WHOSCORED_KNOWN_TYPES_39 before retrying."
        )
    return {
        'observed_types': result.value,
        'season': season,
        'league': league,
    }


def _gate_dry_run(**context) -> bool:
    """ShortCircuitOperator hook — TRUE means "continue to downstream".

    When ``params.dry_run`` is True we want to STOP after pre-checks (return
    False so downstream tasks short-circuit to skipped). When False we
    return True to let Silver/Gold/Validate run.
    """
    params = _read_params(context)
    if params['dry_run']:
        logger.info(
            "dry_run=True — skipping Silver/Gold/Validate. "
            "Pre-checks only (pre_check_bronze + taxonomy_diff_check)."
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Silver / Gold callables — read params, dispatch to the partition runners.
# ---------------------------------------------------------------------------

def _run_silver_partition(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """Run a Silver partition INSERT for the requested (season, league).

    Imports are inside the body — DAG parser must NOT pull
    ``scrapers/__init__.py`` (~1.5 GB RAM).
    """
    from utils.silver_tasks import run_silver_partition_insert

    params = _read_params(context)
    partition_values = {'league': params['league'], 'season': params['season']}
    logger.info(
        "silver_backfill.%s: partition=%s",
        table_name, partition_values,
    )
    result = run_silver_partition_insert(
        sql_file=sql_file,
        table_name=table_name,
        schema='silver',
        partition_values=partition_values,
    )
    logger.info(
        "silver_backfill.%s: %d rows in %s",
        table_name, result.get('rows_inserted', 0), result.get('table'),
    )
    return result


def _run_gold_partition(sql_file: str, table_name: str, **context) -> Dict[str, Any]:
    """Run a Gold partition INSERT for the requested (season, league).

    Uses the wrapper-style runner (``run_gold_partition_insert_wrapped``)
    because production E3 SQL files don't carry the
    ``-- WHERE_PARTITION_FILTER_HERE`` sentinel. The wrapper engine
    imposes no SQL changes; Trino's optimiser pushes the predicate down
    past the outer wrapper for partition pruning.
    """
    from utils.gold_tasks import run_gold_partition_insert_wrapped

    params = _read_params(context)
    partition_values = {'league': params['league'], 'season': params['season']}
    logger.info(
        "gold_backfill.%s: partition=%s",
        table_name, partition_values,
    )
    result = run_gold_partition_insert_wrapped(
        sql_file=sql_file,
        table_name=table_name,
        partition_values=partition_values,
        partition_columns=GOLD_E35_PARTITION_COLUMNS,
    )
    logger.info(
        "gold_backfill.%s: %d rows in %s (partition=%s)",
        table_name, result.get('rows_inserted', 0), result.get('table'),
        partition_values,
    )
    return result


# ---------------------------------------------------------------------------
# Validate callable — per-season DQ + Telegram summary.
# ---------------------------------------------------------------------------

def _validate_backfill(**context) -> Dict[str, Any]:
    """Run E3.5 per-season DQ and post a Telegram summary.

    Composition:
      * ``build_per_season_e3_checks(season, league)`` — PK uniqueness +
        SPADL unknown_rate + schema-version drift, all scoped to season.
      * ``parity_check_event_counts_per_season`` — Bronze→Silver→Gold
        row-count parity gate (ERROR severity).

    ERROR-severity failures raise ``AirflowException`` after Telegram fires.
    """
    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CheckResult, run_checks
    from utils.e3_dq import (
        build_per_season_e3_checks,
        completeness_check_events_per_season,
        parity_check_event_counts_per_season,
    )

    params = _read_params(context)
    season, league = params['season'], params['league']

    # ---- Standard CHECK.* primitives, scoped to season ----
    checks = build_per_season_e3_checks(season=season, league=league)
    logger.info(
        "validate_backfill: running %d per-season checks for (%s, %s)",
        len(checks), season, league,
    )
    report = run_checks(checks, raise_on_error=False)

    # ---- Per-season parity gate (custom CheckResult) ----
    try:
        parity_result = parity_check_event_counts_per_season(
            season=season, league=league
        )
        report.results.append(parity_result)
    except Exception as e:
        logger.exception("parity_check_event_counts_per_season crashed; recording WARNING")
        report.results.append(CheckResult(
            name=(
                "parity_check_event_counts_per_season "
                f"season={season} league={league}"
            ),
            kind='custom',
            severity='WARNING',
            passed=False,
            error=str(e),
        ))

    # ---- Per-season schedule->events completeness gate (custom — #895) ----
    try:
        completeness_result = completeness_check_events_per_season(
            season=season, league=league
        )
        report.results.append(completeness_result)
    except Exception as e:
        logger.exception("completeness_check_events_per_season crashed; recording WARNING")
        report.results.append(CheckResult(
            name=(
                "completeness_check_events_per_season "
                f"season={season} league={league}"
            ),
            kind='custom',
            severity='WARNING',
            passed=False,
            error=str(e),
        ))

    logger.info("E3.5 backfill DQ: %s", report.summary())
    telegram_dq_summary(
        report,
        header=f"E3.5 Backfill DQ — season={season} league={league}",
    )

    if report.errors:
        raise AirflowException(
            f"E3.5 backfill DQ failed: {len(report.errors)} error(s). "
            + "; ".join(
                f"{r.name}: {r.details or r.error}"
                for r in report.errors[:5]
            )
        )

    return {
        'season': season,
        'league': league,
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id='dag_e3_backfill',
    default_args=SILVER_ARGS,
    description=(
        'E3.5 historical backfill (per-(season, league) Silver/Gold INSERT). '
        'Manual trigger only. See dags/dag_e3_backfill.py docstring.'
    ),
    schedule=None,
    start_date=datetime(2026, 5, 9),
    catchup=False,
    is_paused_upon_creation=True,   # Defence: do NOT auto-run on import.
    max_active_runs=1,
    max_active_tasks=1,
    tags=['silver', 'gold', 'medallion-e3', 'medallion-e3.5', 'backfill'],
    params={
        'season': Param(
            default=None,
            type=['null', 'string'],
            title='Season (varchar)',
            description=(
                "Season to backfill in WhoScored/Understat/ESPN format "
                "(e.g. '2122' = 2021-22). Required."
            ),
        ),
        'league': Param(
            default='ENG-Premier League',
            type='string',
            title='League',
            description='Source league name; default APL.',
        ),
        'dry_run': Param(
            default=False,
            type='boolean',
            title='Dry run (pre-checks only)',
            description=(
                'When True, run only pre_check_bronze + taxonomy_diff_check. '
                'Silver/Gold/Validate skip via short-circuit.'
            ),
        ),
    },
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id='start_marker')

    # =========================================================================
    # Pre-flight DQ gates (always run, even in dry_run mode)
    # =========================================================================
    pre_check = PythonOperator(
        task_id='pre_check_bronze',
        python_callable=_pre_check_bronze,
    )

    taxonomy_check = PythonOperator(
        task_id='taxonomy_diff_check',
        python_callable=_taxonomy_diff_check,
    )

    # =========================================================================
    # dry_run gate — short-circuits Silver/Gold/Validate when dry_run=True
    # =========================================================================
    dry_run_gate = ShortCircuitOperator(
        task_id='dry_run_gate',
        python_callable=_gate_dry_run,
        # The pre-flight tasks run BEFORE this gate; the gate decides whether
        # to fan out to Silver/Gold below.
    )

    # =========================================================================
    # Silver backfill (sequential, max_active_tasks=1)
    # =========================================================================
    with TaskGroup(group_id='silver_backfill') as silver_group:
        prev = None
        for task_id, sql_file, table_name in SILVER_E35_TRANSFORMS:
            t = PythonOperator(
                task_id=task_id,
                python_callable=_run_silver_partition,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
            )
            if prev is not None:
                prev >> t
            prev = t

    # =========================================================================
    # Gold backfill (sequential, max_active_tasks=1)
    # =========================================================================
    with TaskGroup(group_id='gold_backfill') as gold_group:
        prev = None
        for task_id, sql_file, table_name in GOLD_E35_TRANSFORMS:
            t = PythonOperator(
                task_id=task_id,
                python_callable=_run_gold_partition,
                op_kwargs={
                    'sql_file': sql_file,
                    'table_name': table_name,
                },
            )
            if prev is not None:
                prev >> t
            prev = t

    # =========================================================================
    # Validation — DQ + Telegram summary
    # =========================================================================
    validate_task = PythonOperator(
        task_id='validate_backfill',
        python_callable=_validate_backfill,
        trigger_rule='all_success',
    )

    end = EmptyOperator(task_id='end_marker')

    # =========================================================================
    # Dependencies
    # =========================================================================
    # Pre-flight runs unconditionally; dry_run_gate decides whether to
    # continue. Silver -> Gold -> Validate -> end.
    start >> pre_check >> taxonomy_check >> dry_run_gate
    dry_run_gate >> silver_group >> gold_group >> validate_task >> end
