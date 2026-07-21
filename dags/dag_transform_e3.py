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
    validate_fotmob_publication_consumer
        |
        v
    TaskGroup: silver_e3
        |-- whoscored_events_spadl   (partition-staged Silver transform)
        |-- espn_lineup              (run_silver_transform)
        |
        v
    TaskGroup: gold_e3
        |-- fct_event    (run_gold_transform, partitions=['league','season'])
        |-- fct_shot     (run_gold_transform, partitions=['league','season'])
        |-- fct_lineup   (run_gold_transform, partitions=['league','season'])
        |
        v
    validate_sofascore_committed_state
        |               (active-registry endpoint/table DQ, fail closed)
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
``schedule=None`` -- the DAG is triggered only by ``dag_master_pipeline`` or
``dag_sofascore_pipeline`` after ``dag_transform_xref`` has produced the
Silver xref spine. Direct/manual runs have no exact active FotMob consumer
binding and fail before the first transform.

Normal transforms use atomic ``CREATE OR REPLACE``. The 28M-row SPADL
transform first materialises bounded
league/season scopes in a retry-cleaned staging table, proves the ordered
manifest commit set and exact Bronze parity stayed unchanged, then atomically
replaces the live table with a window-free stage scan.

Upstream dependencies
---------------------
* Silver: ``silver.xref_match``, ``silver.xref_team``, ``silver.xref_player``
  (built by ``dag_transform_xref``).
* Bronze: ``bronze.whoscored_events_current``, ``bronze.whoscored_lineups_current``,
  ``bronze.understat_shot_events``,
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
* **fct_lineup ESPN bridge** -- #867: resolves through ``silver.xref_match``
  (source='espn'). Games outside the FBref spine keep their ``espn_<hash>``
  pseudo-id (LEFT JOIN, rows preserved); see ADR in
  ``dags/sql/gold/fct_lineup.sql``.

DQ wiring (validate_e3)
-----------------------
``validate_sofascore_committed_state`` first executes the versioned
SofaScore endpoint/table contract for every capture-allowed registry
competition-season. SQL/planning failures and threshold violations fail the
DAG before the cross-source E3 summary can publish a false success.

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
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS
from utils.fotmob_publication import validate_fotmob_consumer_fence

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
        # Real per-(match, player) lineup from the latest successful WhoScored
        # match batch; includes unused substitutes, position and jersey number.
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
        # issue #735 (Gold one-hop, аудит #704): conform-only venue-проекция ESPN
        # matchsheet. Dedup ROW_NUMBER перенесён сюда из gold.dim_venue, который
        # читал bronze.espn_matchsheet напрямую. Single bronze source, без xref —
        # НЕ зависит от dag_transform_xref. Consumer: gold.dim_venue (ESPN-сторона
        # UNION), строится в dag_transform_fbref_gold (после e3).
        'espn_matchsheet',
        'dags/sql/silver/espn_matchsheet.sql',
        'espn_matchsheet',
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
        # match-level aggregate. Aggregates bronze.whoscored_events_current by
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
    (
        # issue #704 (Gold one-hop): shot-grained Understat shotmap conform,
        # canonicalised to fct_shot's team/player IDs + enum domains. Reads
        # bronze.understat_shots + bronze.understat_players (assist name dict)
        # + silver.xref_{team,player} (source='understat') — MUST run after
        # dag_transform_xref. Consumer: gold.fct_shot (#699) primary branch,
        # which now reads this instead of bronze (keeping only the
        # understat_schedule → fbref match bridge). The match_id bridge stays
        # in Gold (cross-source; xref_match has no understat rows yet).
        'understat_shots',
        'dags/sql/silver/understat_shots.sql',
        'understat_shots',
    ),
    (
        # issue #702: conform-only SofaScore standings snapshot. Reads
        # bronze.sofascore_league_table (dedup ROW_NUMBER, cast, season-slug as-is)
        # — NO silver.xref reads (canonical resolve deferred to Gold per charter §5),
        # so it does NOT depend on dag_transform_xref. Consumer: gold.fct_standings
        # (primary source; FotMob silver.fotmob_team_standings is the fallback).
        # Placed in e3 so it materialises before dag_transform_fbref_gold.
        'sofascore_league_table',
        'dags/sql/silver/sofascore_league_table.sql',
        'sofascore_league_table',
    ),
    (
        # issue #753: conform-only SofaScore venue snapshot from the per-match
        # event-capture (bronze.sofascore_venue, dedup ROW_NUMBER to one row per
        # stadium, season-slug as-is). NO silver.xref reads (canonical resolve
        # deferred to Gold per charter §5) → no dag_transform_xref dependency.
        # Consumer: gold.dim_venue (sofascore_venue enrichment CTE — fills
        # country FotMob lacks + city/coords for moved grounds).
        'sofascore_venue',
        'dags/sql/silver/sofascore_venue.sql',
        'sofascore_venue',
    ),
]

# Graceful-degrade for Silver tables whose Bronze source is OPTIONAL (#812).
# Keyed by table_name → required Bronze table(s) + an empty-schema fallback SQL.
# When any required Bronze table is absent, `_run_silver_e3` runs the fallback
# (identical schema, zero rows) instead of failing on TABLE_NOT_FOUND — which
# otherwise cascades to the whole Gold layer (gold_e3 here AND
# dag_transform_fbref_gold's dim_venue). Mirrors the run_gold_transform
# require_silver/fallback_sql_file pattern used across dag_transform_fbref_gold.
SILVER_E3_FALLBACKS = {
    # bronze.sofascore_venue is written only when a SofaScore capture pass
    # carries a `.venue` block (#753) — absent until then.
    'sofascore_venue': {
        'require_bronze': ['sofascore_venue'],
        'fallback_sql_file': 'dags/sql/silver/sofascore_venue_empty.sql',
    },
}

# Full-corpus SPADL has one necessary per-game ROW_NUMBER window (legacy event
# id compatibility).  At 28M rows, evaluating it as one CTAS can exhaust the
# Trino heap even though each match partition is small.  Route only this table
# through a staging runner that evaluates league/season scopes sequentially,
# verifies the complete manifest commit set and exact source parity, then
# atomically replaces the live table from a window-free staging scan. Other E3
# Silver transforms keep the normal CTAS.
WHOSCORED_EVENTS_SOURCE_VERSION_SQL = """
SELECT league, season, game_id, batch_id
FROM iceberg.bronze.whoscored_match_ingest_latest_success
ORDER BY league, season, game_id, batch_id
""".strip()

SILVER_E3_PARTITION_STAGED = {
    'whoscored_events_spadl': {
        'partition_source_table': 'iceberg.bronze.whoscored_events_current',
        'source_version_sql': WHOSCORED_EVENTS_SOURCE_VERSION_SQL,
    },
}

SILVER_E3_TASK_OVERRIDES = {
    # Historical partition builds have taken up to 7h41m. The bounded runner
    # trades peak heap for sequential work, so it needs an explicit long-task
    # limit instead of the normal 30-minute single-CTAS timeout.
    'whoscored_events_spadl': {
        'execution_timeout': timedelta(hours=8),
    },
}

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
        # #876: source-explicit SofaScore xGOT. Keep this fact separate from
        # cross-provider psxg/xG models so a COALESCE can never silently mix
        # incompatible provider definitions.
        'fct_sofascore_team_match_post_shot_xg',
        'dags/sql/gold/fct_sofascore_team_match_post_shot_xg.sql',
        'fct_sofascore_team_match_post_shot_xg',
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

def _run_silver_e3(
    sql_file: str,
    table_name: str,
    require_bronze=None,
    fallback_sql_file: str = None,
    partition_source_table: str = None,
    source_version_sql: str = None,
    **context,
) -> Dict[str, Any]:
    """Run an E3 Silver transform with the configured execution strategy.

    Most tables use :func:`utils.silver_tasks.run_silver_transform`. The SPADL
    corpus uses the partition-staged runner to bound its necessary legacy-id
    window without introducing a new event-id formula or format.

    Graceful-degrade (#812): when ``require_bronze`` is given and any of those
    Bronze tables is absent (optional source not yet populated, e.g.
    bronze.sofascore_venue per #753), build ``fallback_sql_file`` instead — an
    identical-schema, zero-row table — so the contract stays intact and the
    Gold layer does not fail on TABLE_NOT_FOUND.
    """
    from utils.silver_tasks import check_bronze_table_exists, run_silver_transform

    if require_bronze and fallback_sql_file:
        missing = [
            t for t in require_bronze
            if not check_bronze_table_exists(table_name=t, schema='bronze')
        ]
        if missing:
            logger.warning(
                "silver_e3.%s: required Bronze table(s) %s not found — "
                "falling back to '%s' (empty table, identical schema).",
                table_name, missing, fallback_sql_file,
            )
            sql_file = fallback_sql_file

    if partition_source_table:
        from utils.silver_tasks import run_silver_transform_partition_staged

        if not source_version_sql:
            raise ValueError(
                f"partition-staged transform {table_name} requires "
                "source_version_sql"
            )
        result = run_silver_transform_partition_staged(
            sql_file=sql_file,
            table_name=table_name,
            source_table=partition_source_table,
            source_version_sql=source_version_sql,
            schema='silver',
        )
    else:
        result = run_silver_transform(
            sql_file=sql_file,
            table_name=table_name,
            schema='silver',
        )
    logger.info(
        "silver_e3.%s transform complete: %d rows in %s",
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


def _validate_sofascore_committed_state(**context) -> Dict[str, Any]:
    """Fail closed on committed SofaScore partition quality.

    Scope is derived from the versioned registry: only enabled, evidenced,
    operator-approved adult-men competitions and their activatable seasons are
    checked. The callable runs after all E3 SofaScore Silver/Gold CTAS tasks, so
    duplicate, season, schedule-skeleton, endpoint completeness, profile,
    rating and Silver-to-Gold lineup attachment gates observe one committed
    state. Any SQL error or threshold violation fails this Airflow task.
    """

    from airflow.exceptions import AirflowException

    from utils.sofascore_dq import (
        SofaScoreDQViolation,
        run_active_registry_committed_dq,
    )

    try:
        result = run_active_registry_committed_dq()
    except SofaScoreDQViolation as exc:
        raise AirflowException(
            f"SofaScore committed-state DQ failed: {exc}"
        ) from exc
    except Exception as exc:
        raise AirflowException(
            f"SofaScore committed-state DQ crashed fail-closed: {exc}"
        ) from exc
    logger.info(
        "SofaScore committed-state DQ passed: %d checks across %d partitions",
        result['checks'],
        result['partitions'],
    )
    return result


def _validate_e3(**context) -> Dict[str, Any]:
    """Run E3-scoped DQ checks and post a Telegram summary.

    DQ list comes from :func:`utils.e3_dq.build_all_e3_checks` (E3.8). Custom
    fail-closed checks add scale-aware WhoScored SPADL unknown coverage,
    Bronze->Silver->Gold parity and schedule->events completeness.

    Severity model — ERROR-severity failures raise ``AirflowException``
    after the Telegram summary is posted. WARNING-severity failures are
    logged + reported but do NOT fail the DAG.
    """
    from airflow.exceptions import AirflowException

    from utils.alerts import telegram_dq_summary
    from utils.data_quality import CheckResult, run_checks
    from utils.e3_dq import (
        build_all_e3_checks,
        completeness_check_events,
        parity_check_event_counts,
        spadl_unknown_coverage_check,
    )

    all_checks = build_all_e3_checks()
    logger.info("E3 DQ: running %d standard checks from utils.e3_dq", len(all_checks))

    # Standard CHECK.* primitives. raise_on_error=False so Telegram fires
    # before we re-raise on ERROR-severity failures.
    report = run_checks(all_checks, raise_on_error=False)

    # Whole-table SPADL unknown coverage needs a combined absolute+ratio cap,
    # which is not expressible via the standard CHECK registry. Any execution
    # error is returned as an ERROR CheckResult by the helper itself.
    unknown_coverage_result = spadl_unknown_coverage_check()
    report.results.append(unknown_coverage_result)

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

    # Schedule->events completeness gate (custom — #895). ERROR on any
    # scheduled fixture missing events that is NOT in the sanctioned floor.
    try:
        completeness_result = completeness_check_events()
        report.results.append(completeness_result)
    except Exception as e:
        logger.exception("completeness_check_events crashed; recording WARNING")
        report.results.append(CheckResult(
            name='completeness_check_events',
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

    publication_preflight = PythonOperator(
        task_id='validate_fotmob_publication_consumer',
        python_callable=validate_fotmob_consumer_fence,
        retries=0,
    )

    # =========================================================================
    # TaskGroup: silver_e3 (sequential pure-SQL CTAS, max_active_tasks=1)
    # =========================================================================
    with TaskGroup(group_id='silver_e3') as silver_group:
        prev = None
        for task_id, sql_file, table_name in SILVER_E3_TRANSFORMS:
            op_kwargs = {
                'sql_file': sql_file,
                'table_name': table_name,
            }
            # Optional Bronze source → empty-schema fallback when absent (#812).
            op_kwargs.update(SILVER_E3_FALLBACKS.get(table_name, {}))
            op_kwargs.update(SILVER_E3_PARTITION_STAGED.get(table_name, {}))
            t = PythonOperator(
                task_id=task_id,
                python_callable=_run_silver_e3,
                op_kwargs=op_kwargs,
                **SILVER_E3_TASK_OVERRIDES.get(table_name, {}),
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
    # Validation -- source-specific committed state, then general E3 summary
    # =========================================================================
    sofascore_committed_dq_task = PythonOperator(
        task_id='validate_sofascore_committed_state',
        python_callable=_validate_sofascore_committed_state,
        trigger_rule='all_success',
    )

    validate_task = PythonOperator(
        task_id='validate_e3',
        python_callable=_validate_e3,
        trigger_rule='all_success',  # Skip validation if any transform failed
    )

    end = EmptyOperator(task_id='end_marker')

    # =========================================================================
    # Dependencies
    # =========================================================================
    (
        start
        >> publication_preflight
        >> silver_group
        >> gold_group
        >> sofascore_committed_dq_task
        >> validate_task
        >> end
    )
