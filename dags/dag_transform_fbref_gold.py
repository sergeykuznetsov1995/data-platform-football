"""
FBref Gold Layer Transformation DAG
====================================

Builds the analytical Gold feature store from Silver FBref tables.

Architecture
------------

    Triggered by dag_transform_fbref_silver (or manual trigger)
        |
        v
    dim_team, dim_player, dim_match (read silver.xref_* since E1.5;
                                     parallel-safe, but sequential to save RAM)
        |
        v
    fct_team_match, fct_player_match, match_outcomes
        |
        v
    feat_team_form, feat_team_h2h, feat_player_form
        |
        v
    fct_match (wide form with ML targets + joined features)
        |
        v
    validate_gold_row_counts  — row-count sanity
        |
        v
    validate_gold_quality     — PK uniqueness, ref integrity, point-in-time

All tasks are executed SEQUENTIALLY (``max_active_tasks=1``) to keep memory
usage predictable on a dev-sized Trino (5 GB container / 3.5 GB heap).

Gold Tables
-----------
- ``gold.dim_team``           — team dimension (reads silver.xref_team since E1.5)
- ``gold.dim_player``         — player dimension (canonical_id format
                                'fb_<player_id>' since E1.5)
- ``gold.dim_match``          — match attributes + ML targets
                                (reads silver.xref_team since E1.5)
- ``gold.dim_venue``          — venue master-data (E2)
- ``gold.dim_referee``        — referee master-data (E2)
- ``gold.dim_standings``      — SofaScore league-table snapshot (E2)
                                (reads silver.xref_team(source='sofascore') since E1.5)
- ``gold.dim_competition``    — competition master-data from leagues.yaml (E2)
- ``gold.dim_season``         — season master-data with valid_from/valid_to (E2)
- ``gold.fct_team_match``     — long-form team metrics per match
- ``gold.fct_team_season_stats`` — cross-source per-season team stats (T6.4 #94;
                                FBref+Understat+WhoScored+SofaScore via xref_team)
- ``gold.fct_team_season_stats_audit`` — DQ-audit diff'ы для fct_team_season_stats
- ``gold.fct_player_match``   — player metrics per match
- ``gold.fct_player_unavailable`` — confirmed absences (E5; from WhoScored)
- ``gold.fct_player_market_value`` — FotMob market_value timeline per player×date
                                     (issue #11; bridge via silver.xref_player)
- ``gold.match_outcomes``     — labels-only target table (1X2/BTTS/totals) for backtesting
- ``gold.fct_match``          — wide form with pre-match features (ready for ML)
- ``gold.feat_team_form``     — rolling team form (last 5, point-in-time safe)
- ``gold.feat_team_xg_form``  — rolling team xG / PSxG (L5 + L10, point-in-time safe)
- ``gold.feat_team_h2h``      — rolling head-to-head (last 5)
- ``gold.feat_player_form``   — rolling player form (last 5)
- ``gold.feat_referee_bias``  — rolling referee bias (cards/pens per match, L10/L20; E6)
- ``gold.feat_team_event_style`` — rolling team event-style profile from SPADL (L5; E6)
- ``gold.fct_match_train``    — ML train split (earliest ~80% completed per season)
- ``gold.fct_match_test``     — ML test split (latest ~20% completed per season)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS

# ---------------------------------------------------------------------------
# Transform definitions
# ---------------------------------------------------------------------------
# (task_id, sql_file, table_name, partition_cols)
#
# Order matters: dim_* must be built before fct_* which reference them;
# feat_* depend on fct_team_match; fct_match depends on feat_*.

# Stage 1.5: per-source season aggregates (migrated from Silver, issue #370).
# True match→season rollups that the Silver Charter assigns to Gold. They read
# silver.*_team_match directly (legitimate Gold→Silver one-hop) and feed the
# cross-source fct_team_season_stats / _audit in Stage 2 — so they MUST build
# first. Pure GROUP BY rollups (uniqueness guaranteed by the group key), so no
# extra DQ here; the cross-source facts downstream carry the validation.
STAGE_1_5_SOURCE_SEASON_AGG = [
    ('fotmob_team_season',    'dags/sql/gold/fotmob_team_season.sql',    'fotmob_team_season',    ['league', 'season']),
    ('understat_team_season', 'dags/sql/gold/understat_team_season.sql', 'understat_team_season', ['league', 'season']),
    ('whoscored_team_season', 'dags/sql/gold/whoscored_team_season.sql', 'whoscored_team_season', ['league', 'season']),
    ('sofascore_team_season', 'dags/sql/gold/sofascore_team_season.sql', 'sofascore_team_season', ['league', 'season']),
]

STAGE_2_DIMS = [
    ('dim_team',   'dags/sql/gold/dim_team.sql',   'dim_team',   ['league', 'season']),
    ('dim_player', 'dags/sql/gold/dim_player.sql', 'dim_player', ['league', 'season']),
    ('dim_match',  'dags/sql/gold/dim_match.sql',  'dim_match',  ['league', 'season']),
    # T5 wiring restore + T6 SofaScore. Player-season block ОТСЮДА depends
    # on Silver per-source aggregates (FBref/FotMob/WS/US/SofaScore) which
    # are produced by dag_transform_e3 ahead of this DAG via master pipeline.
    # dim_player_attributes — snapshot-grain (один row per canonical_id),
    # season не в SELECT. Без partition cols.
    ('dim_player_attributes',     'dags/sql/gold/dim_player_attributes.sql',
     'dim_player_attributes',     None),
    ('fct_player_season_stats',   'dags/sql/gold/fct_player_season_stats.sql',
     'fct_player_season_stats',   ['league', 'season']),
    # #175 — keeper-variant per-season facts. Restored lost wiring (SQL + DQ
    # checks shipped in T5 but never registered in a STAGE list). Reads
    # silver.fotmob_keeper_profile (per-90 cols pass through as-is), so the
    # #174 FotMob count-drift does not apply here. Main facts precede both
    # audits (audit ref_integrity → main).
    ('fct_keeper_season_stats',   'dags/sql/gold/fct_keeper_season_stats.sql',
     'fct_keeper_season_stats',   ['league', 'season']),
    ('fct_player_season_stats_audit',
     'dags/sql/gold/fct_player_season_stats_audit.sql',
     'fct_player_season_stats_audit', ['league', 'season']),
    ('fct_keeper_season_stats_audit',
     'dags/sql/gold/fct_keeper_season_stats_audit.sql',
     'fct_keeper_season_stats_audit', ['league', 'season']),
    # T6.4 (#94) — cross-source team season facts + audit.
    # 4-source (FBref spine + Understat + WhoScored + SofaScore) merged via
    # silver.xref_team. Audit is WARNING-only and isolates HARD_FACT diffs
    # from the business mart. Mirrors fct_player_season_stats pattern above.
    ('fct_team_season_stats',     'dags/sql/gold/fct_team_season_stats.sql',
     'fct_team_season_stats',     ['league', 'season']),
    ('fct_team_season_stats_audit',
     'dags/sql/gold/fct_team_season_stats_audit.sql',
     'fct_team_season_stats_audit', ['league', 'season']),
]

# E2: master-data dims that are NOT partitioned by (league, season).
# Three are read straight from a static .sql file (Bronze-sourced); the other
# two (dim_competition, dim_season) are rendered from config via
# utils.dim_loaders before CTAS — see STAGE_2B_MASTER_DIMS_INLINE below.
#
# dim_standings IS partitioned by (league, season) because Bronze emits one
# snapshot per league/season. The other four are tiny static reference tables
# (~5-30 rows) so partitioning would just create empty manifest noise.
STAGE_2B_MASTER_DIMS_SQL = [
    # (task_id, sql_file, table_name, partition_cols)
    ('dim_referee',   'dags/sql/gold/dim_referee.sql',   'dim_referee',   None),
    ('dim_manager',   'dags/sql/gold/dim_manager.sql',   'dim_manager',   None),
    ('dim_standings', 'dags/sql/gold/dim_standings.sql', 'dim_standings', ['league', 'season']),
]

# Inline-rendered master dims (Jinja .sql.j2 templates -> tempfile -> CTAS).
# Renderer functions are looked up by name at TaskGroup-build time so the
# top-level DAG body stays import-light (no eager import of dim_loaders -> yaml).
STAGE_2B_MASTER_DIMS_INLINE = [
    # (task_id, renderer_name, template_path, table_name, partition_cols)
    # dim_venue (issue #145): curated alias-identity from venue_aliases.yaml.
    ('dim_venue',       'render_dim_venue_sql',
     'dags/sql/gold/dim_venue.sql.j2',       'dim_venue',       None),
    ('dim_competition', 'render_dim_competition_sql',
     'dags/sql/gold/dim_competition.sql.j2', 'dim_competition', None),
    ('dim_season',      'render_dim_season_sql',
     'dags/sql/gold/dim_season.sql.j2',      'dim_season',      None),
]

STAGE_3_FACTS = [
    ('fct_team_match',   'dags/sql/gold/fct_team_match.sql',   'fct_team_match',   ['league', 'season']),
    # issue #95: cross-source DQ-audit для fct_team_match. INNER FBref ∩ Understat,
    # LEFT WhoScored / SofaScore с diff-колонками. WARNING-only DQ.
    # Pattern: fct_player_match_audit (issue #46).
    ('fct_team_match_audit', 'dags/sql/gold/fct_team_match_audit.sql',
     'fct_team_match_audit', ['league', 'season']),
    ('fct_player_match', 'dags/sql/gold/fct_player_match.sql', 'fct_player_match', ['league', 'season']),
    # issue #46: cross-source DQ-audit для fct_player_match. INNER JOIN всех
    # 4 источников (FBref/SofaScore/Understat/WhoScored) с diff-колонками
    # `<metric>_diff_<source>` для anomaly detection. WARNING-only DQ.
    # Same pattern as fct_player_season_stats_audit (STAGE_2_DIMS).
    ('fct_player_match_audit', 'dags/sql/gold/fct_player_match_audit.sql',
     'fct_player_match_audit', ['league', 'season']),
    # E5: must run before Stage 4 — feat_team_form joins it for unavailable_count_l5.
    ('fct_player_unavailable', 'dags/sql/gold/fct_player_unavailable.sql',
     'fct_player_unavailable', ['league', 'season']),
    # issue #11: FotMob market_value timeline (per-player, per-date).
    # Источник — silver.fotmob_player_market_value_history (Bronze UNNEST).
    # Bridge FotMob→canonical через silver.xref_player(source='fotmob').
    ('fct_player_market_value', 'dags/sql/gold/fct_player_market_value.sql',
     'fct_player_market_value', ['league', 'season']),
    # Labels-only target table — kept separate from `fct_match` so backtesting
    # can join targets to features without leakage.
    ('match_outcomes',   'dags/sql/gold/match_outcomes.sql',   'match_outcomes',   ['league', 'season']),
]

# Tables in STAGE_3 with optional Silver sources. Same mechanism as
# STAGE_4_FALLBACKS — runner routes CTAS to fallback SQL when source is absent.
STAGE_3_FALLBACKS = {
    'fct_player_unavailable': {
        'fallback_sql_file': 'dags/sql/gold/fct_player_unavailable_empty.sql',
        'require_silver':    ['whoscored_player_unavailable'],
    },
    # issue #11: market_value Silver source может отсутствовать в MVP env
    # без FotMob ingest. Fallback держит контракт пустой таблицы.
    'fct_player_market_value': {
        'fallback_sql_file': 'dags/sql/gold/fct_player_market_value_empty.sql',
        'require_silver':    ['fotmob_player_market_value_history'],
    },
}

STAGE_4_FEATS = [
    ('feat_team_form',    'dags/sql/gold/feat_team_form.sql',    'feat_team_form',    ['league', 'season']),
    # xG features depend on silver.fbref_shot_events (optional Bronze source).
    # When that Silver table is missing (MVP envs without shot_events ingestion),
    # run_gold_transform automatically falls back to feat_team_xg_form_empty.sql
    # which emits an identical schema with NULL xG columns so downstream
    # fct_match LEFT JOINs continue to resolve.
    ('feat_team_xg_form', 'dags/sql/gold/feat_team_xg_form.sql', 'feat_team_xg_form', ['league', 'season']),
    ('feat_team_h2h',     'dags/sql/gold/feat_team_h2h.sql',     'feat_team_h2h',     ['league', 'season']),
    ('feat_player_form',  'dags/sql/gold/feat_player_form.sql',  'feat_player_form',  ['league', 'season']),
    # E6 W6: referee-bias rolling features. Reads ONLY gold tables
    # (dim_match.referee + fct_card / fct_goal); has no Silver dependency, so
    # it is intentionally NOT registered in STAGE_4_FALLBACKS — the
    # *_empty.sql exists on disk as a manual safety-net for the rare case
    # where dim_match.referee is wholly empty, but the runner never reaches
    # for it automatically. Partitioned by season only (referee identity
    # crosses leagues, so a per-league partition would shard each ref's
    # rolling window).
    ('feat_referee_bias',
     'dags/sql/gold/feat_referee_bias.sql',
     'feat_referee_bias',
     ['season']),
    # E6 W6: team event-style rolling features. Reads
    # silver.whoscored_events_spadl (optional Silver source — falls back to
    # feat_team_event_style_empty.sql when whoscored Silver is absent).
    ('feat_team_event_style',
     'dags/sql/gold/feat_team_event_style.sql',
     'feat_team_event_style',
     ['league', 'season']),
]

# Tables in STAGE_4 that depend on optional Silver sources. When the listed
# Silver tables are missing, run_gold_transform routes the CTAS to the
# fallback SQL (NULL placeholders, identical schema).
STAGE_4_FALLBACKS = {
    'feat_team_xg_form': {
        'fallback_sql_file': 'dags/sql/gold/feat_team_xg_form_empty.sql',
        'require_silver':    ['fbref_shot_events'],
    },
    'feat_team_event_style': {
        'fallback_sql_file': 'dags/sql/gold/feat_team_event_style_empty.sql',
        'require_silver':    ['whoscored_events_spadl'],
    },
}

STAGE_5_MARTS = [
    ('fct_match', 'dags/sql/gold/fct_match.sql', 'fct_match', ['league', 'season']),
]

# T4.1: ML train/test split. Depends on fct_match (features) AND match_outcomes
# (extended targets). Partitioned by season — natural unit for time-series CV.
STAGE_6_ML_SPLITS = [
    ('fct_match_train', 'dags/sql/gold/fct_match_train.sql', 'fct_match_train', ['season']),
    ('fct_match_test',  'dags/sql/gold/fct_match_test.sql',  'fct_match_test',  ['season']),
]

# E7 T3: BI / dashboard-facing marts. Sit AFTER all dims/facts/features
# because they cross-join Gold facts (fct_shot, fct_event, fct_card,
# fct_goal, fct_player_match, dim_match, dim_player, dim_team, dim_referee).
# E3/E4 facts (fct_shot/event/card/goal) are produced by sister DAGs
# (dag_transform_e3, dag_transform_e4) which the master pipeline runs
# *before* dag_transform_fbref_gold — so by the time STAGE_7 fires they
# are guaranteed present. Each mart has an `_empty.sql` fallback for
# environments missing the underlying Silver source (e.g. WhoScored
# events for the heatmap).
STAGE_7_DASHBOARD_MARTS = [
    # (task_id, sql_file, table_name, partition_cols, fallback_sql, require_silver)
    ('mart_scouting_radar',
     'dags/sql/gold/mart_scouting_radar.sql',
     'mart_scouting_radar',
     ['league', 'season'],
     'dags/sql/gold/mart_scouting_radar_empty.sql',
     ['fbref_shot_events']),
    ('mart_referee_dashboard',
     'dags/sql/gold/mart_referee_dashboard.sql',
     'mart_referee_dashboard',
     ['season'],
     'dags/sql/gold/mart_referee_dashboard_empty.sql',
     None),
    ('mart_event_heatmap',
     'dags/sql/gold/mart_event_heatmap.sql',
     'mart_event_heatmap',
     ['league', 'season'],
     'dags/sql/gold/mart_event_heatmap_empty.sql',
     ['whoscored_events_spadl']),
]


def _run_transform(
    sql_file: str,
    table_name: str,
    partition_cols=None,
    fallback_sql_file: str = None,
    require_silver=None,
    add_timestamp: bool = True,
    **_ctx,
) -> Dict[str, Any]:
    from utils.gold_tasks import run_gold_transform

    return run_gold_transform(
        sql_file=sql_file,
        table_name=table_name,
        partition_columns=partition_cols,
        fallback_sql_file=fallback_sql_file,
        require_silver=require_silver,
        add_timestamp=add_timestamp,
    )


def _row_counts(**_ctx) -> Dict[str, Any]:
    from utils.gold_tasks import validate_gold_row_counts

    return validate_gold_row_counts()


def _quality(**_ctx) -> Dict[str, Any]:
    from utils.gold_tasks import validate_gold_quality

    return validate_gold_quality()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id='dag_transform_fbref_gold',
    default_args=SILVER_ARGS,
    description='Build Gold feature store from Silver FBref tables',
    schedule=None,  # Trigger-only (called after Silver)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['transform', 'fbref', 'gold', 'football', 'trino', 'feature-store'],
    max_active_runs=1,
    max_active_tasks=1,  # Sequential — predictable RAM on dev Trino
    doc_md=__doc__,
) as dag:

    # Stage 1.5: per-source season aggregates migrated from Silver (#370).
    # Must precede s2_dimensions because fct_team_season_stats (in g2) reads them.
    with TaskGroup(group_id='s1_5_source_season_agg') as g1_5:
        for task_id, sql_file, table_name, pcols in STAGE_1_5_SOURCE_SEASON_AGG:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={'sql_file': sql_file, 'table_name': table_name,
                           'partition_cols': pcols},
            )

    # Stage 2: dimensions (read silver.xref_* directly since E1.5)
    with TaskGroup(group_id='s2_dimensions') as g2:
        for task_id, sql_file, table_name, pcols in STAGE_2_DIMS:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={'sql_file': sql_file, 'table_name': table_name,
                           'partition_cols': pcols},
            )

    # Stage 2b: master-data dims (E2). Three Bronze-sourced + two
    # config-rendered. NOT in s2_dimensions because:
    #   * dim_team / dim_player / dim_match are FBref-driven and partitioned
    #     by (league, season); the master dims have a different shape (mostly
    #     un-partitioned, some come from YAML / Python config).
    #   * Keeping a separate group makes the Airflow UI mirror the medallion
    #     plan (E2 is "master-data dims") and keeps blast radius tight if
    #     Phase B's SQL needs to be re-run independently.
    with TaskGroup(group_id='s2b_master_dims') as g2b:
        for task_id, sql_file, table_name, pcols in STAGE_2B_MASTER_DIMS_SQL:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={'sql_file': sql_file, 'table_name': table_name,
                           'partition_cols': pcols},
            )

        # Inline-rendered dims: lazy-import the renderer registry inside the
        # TaskGroup body (NOT at module top) so DAG parse stays cheap and
        # doesn't pull in PyYAML for unrelated DAGs in the same DagBag.
        from utils.dim_loaders import (
            render_dim_competition_sql,
            render_dim_season_sql,
            render_dim_venue_sql,
            run_inline_ctas,
        )
        _RENDERERS = {
            'render_dim_venue_sql':       render_dim_venue_sql,
            'render_dim_competition_sql': render_dim_competition_sql,
            'render_dim_season_sql':      render_dim_season_sql,
        }
        for task_id, renderer_name, tpl, table_name, pcols in STAGE_2B_MASTER_DIMS_INLINE:
            PythonOperator(
                task_id=task_id,
                python_callable=run_inline_ctas,
                op_kwargs={
                    'renderer':     _RENDERERS[renderer_name],
                    'template_sql': tpl,
                    'table_name':   table_name,
                    'partition_cols': pcols,
                },
            )

    # Stage 3: base facts (long-form). Some tables degrade gracefully via
    # STAGE_3_FALLBACKS when their optional Silver source is missing.
    with TaskGroup(group_id='s3_facts') as g3:
        for task_id, sql_file, table_name, pcols in STAGE_3_FACTS:
            kwargs = {
                'sql_file': sql_file,
                'table_name': table_name,
                'partition_cols': pcols,
            }
            fb = STAGE_3_FALLBACKS.get(task_id)
            if fb:
                kwargs['fallback_sql_file'] = fb['fallback_sql_file']
                kwargs['require_silver']    = fb['require_silver']
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs=kwargs,
            )

    # Stage 4: rolling features (depend on fct_team_match, fct_player_match).
    # STAGE_4_FALLBACKS handles the same graceful-degrade pattern as Stage 3.
    with TaskGroup(group_id='s4_features') as g4:
        for task_id, sql_file, table_name, pcols in STAGE_4_FEATS:
            kwargs = {
                'sql_file': sql_file,
                'table_name': table_name,
                'partition_cols': pcols,
            }
            fb = STAGE_4_FALLBACKS.get(task_id)
            if fb:
                kwargs['fallback_sql_file'] = fb['fallback_sql_file']
                kwargs['require_silver']    = fb['require_silver']
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs=kwargs,
            )

    # Stage 5: wide mart (depends on feat_team_form + feat_team_h2h)
    with TaskGroup(group_id='s5_marts') as g5:
        for task_id, sql_file, table_name, pcols in STAGE_5_MARTS:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={'sql_file': sql_file, 'table_name': table_name,
                           'partition_cols': pcols},
            )

    # Stage 6: ML train/test split (depends on fct_match + match_outcomes).
    # add_timestamp=False because SELECT m.* from gold.fct_match already carries
    # _silver_created_at — re-adding it raises DUPLICATE_COLUMN_NAME in Trino.
    with TaskGroup(group_id='s6_ml_splits') as g6:
        for task_id, sql_file, table_name, pcols in STAGE_6_ML_SPLITS:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={'sql_file': sql_file, 'table_name': table_name,
                           'partition_cols': pcols, 'add_timestamp': False},
            )

    # Stage 7: dashboard-facing marts (E7). Built last because they read
    # cross-cutting Gold facts (incl. E3/E4 outputs from sister DAGs).
    with TaskGroup(group_id='s7_dashboard_marts') as g7:
        for task_id, sql_file, table_name, pcols, fb_sql, req_silver in STAGE_7_DASHBOARD_MARTS:
            kwargs = {
                'sql_file': sql_file,
                'table_name': table_name,
                'partition_cols': pcols,
            }
            if fb_sql:
                kwargs['fallback_sql_file'] = fb_sql
            if req_silver:
                kwargs['require_silver'] = req_silver
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs=kwargs,
            )

    validate_row_counts = PythonOperator(
        task_id='validate_gold_row_counts',
        python_callable=_row_counts,
    )

    validate_quality = PythonOperator(
        task_id='validate_gold_quality',
        python_callable=_quality,
    )

    g1_5 >> g2 >> g2b >> g3 >> g4 >> g5 >> g6 >> g7 >> validate_row_counts >> validate_quality
