"""
FBref Gold Layer Transformation DAG
====================================

Builds the analytical Gold feature store from Silver FBref tables.

Architecture
------------

    Triggered by dag_transform_fbref_silver (or manual trigger)
        |
        v
    entity_xref (teams, players, matches)
        |
        v
    dim_team, dim_player, dim_match (parallel-safe, but sequential to save RAM)
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
- ``gold.entity_xref``        — cross-source ID map (MVP: FBref only)
- ``gold.dim_team``           — team dimension
- ``gold.dim_player``         — player dimension
- ``gold.dim_match``          — match attributes + ML targets
- ``gold.fct_team_match``     — long-form team metrics per match
- ``gold.fct_player_match``   — player metrics per match
- ``gold.match_outcomes``     — labels-only target table (1X2/BTTS/totals) for backtesting
- ``gold.fct_match``          — wide form with pre-match features (ready for ML)
- ``gold.feat_team_form``     — rolling team form (last 5, point-in-time safe)
- ``gold.feat_team_xg_form``  — rolling team xG / PSxG (L5 + L10, point-in-time safe)
- ``gold.feat_team_h2h``      — rolling head-to-head (last 5)
- ``gold.feat_player_form``   — rolling player form (last 5)
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
# Order matters: dim_* and entity_xref must be built before fct_* which
# reference them; feat_* depend on fct_team_match; fct_match depends on feat_*.

STAGE_1_XREF = [
    ('entity_xref', 'dags/sql/gold/entity_xref.sql', 'entity_xref', ['league', 'season']),
]

STAGE_2_DIMS = [
    ('dim_team',   'dags/sql/gold/dim_team.sql',   'dim_team',   ['league', 'season']),
    ('dim_player', 'dags/sql/gold/dim_player.sql', 'dim_player', ['league', 'season']),
    ('dim_match',  'dags/sql/gold/dim_match.sql',  'dim_match',  ['league', 'season']),
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
    ('dim_venue',     'dags/sql/gold/dim_venue.sql',     'dim_venue',     None),
    ('dim_referee',   'dags/sql/gold/dim_referee.sql',   'dim_referee',   None),
    ('dim_standings', 'dags/sql/gold/dim_standings.sql', 'dim_standings', ['league', 'season']),
]

# Inline-rendered master dims (Jinja .sql.j2 templates -> tempfile -> CTAS).
# Renderer functions are looked up by name at TaskGroup-build time so the
# top-level DAG body stays import-light (no eager import of dim_loaders -> yaml).
STAGE_2B_MASTER_DIMS_INLINE = [
    # (task_id, renderer_name, template_path, table_name, partition_cols)
    ('dim_competition', 'render_dim_competition_sql',
     'dags/sql/gold/dim_competition.sql.j2', 'dim_competition', None),
    ('dim_season',      'render_dim_season_sql',
     'dags/sql/gold/dim_season.sql.j2',      'dim_season',      None),
]

STAGE_3_FACTS = [
    ('fct_team_match',   'dags/sql/gold/fct_team_match.sql',   'fct_team_match',   ['league', 'season']),
    ('fct_player_match', 'dags/sql/gold/fct_player_match.sql', 'fct_player_match', ['league', 'season']),
    # Labels-only target table — kept separate from `fct_match` so backtesting
    # can join targets to features without leakage.
    ('match_outcomes',   'dags/sql/gold/match_outcomes.sql',   'match_outcomes',   ['league', 'season']),
]

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
]

# Tables in STAGE_4 that depend on optional Silver sources. When the listed
# Silver tables are missing, run_gold_transform routes the CTAS to the
# fallback SQL (NULL placeholders, identical schema).
STAGE_4_FALLBACKS = {
    'feat_team_xg_form': {
        'fallback_sql_file': 'dags/sql/gold/feat_team_xg_form_empty.sql',
        'require_silver':    ['fbref_shot_events'],
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

    # Stage 1: cross-reference table (built first — dims depend on it)
    with TaskGroup(group_id='s1_entity_xref') as g1:
        for task_id, sql_file, table_name, pcols in STAGE_1_XREF:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={'sql_file': sql_file, 'table_name': table_name,
                           'partition_cols': pcols},
            )

    # Stage 2: dimensions
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
            run_inline_ctas,
        )
        _RENDERERS = {
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

    # Stage 3: base facts (long-form)
    with TaskGroup(group_id='s3_facts') as g3:
        for task_id, sql_file, table_name, pcols in STAGE_3_FACTS:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={'sql_file': sql_file, 'table_name': table_name,
                           'partition_cols': pcols},
            )

    # Stage 4: rolling features (depend on fct_team_match, fct_player_match)
    with TaskGroup(group_id='s4_features') as g4:
        for task_id, sql_file, table_name, pcols in STAGE_4_FEATS:
            kwargs = {
                'sql_file': sql_file,
                'table_name': table_name,
                'partition_cols': pcols,
            }
            # Optional graceful-degrade for transforms whose Silver source
            # may be absent (e.g. feat_team_xg_form ← silver.fbref_shot_events).
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

    validate_row_counts = PythonOperator(
        task_id='validate_gold_row_counts',
        python_callable=_row_counts,
    )

    validate_quality = PythonOperator(
        task_id='validate_gold_quality',
        python_callable=_quality,
    )

    g1 >> g2 >> g2b >> g3 >> g4 >> g5 >> g6 >> validate_row_counts >> validate_quality
