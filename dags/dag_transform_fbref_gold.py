"""
FBref Gold Layer Transformation DAG
====================================

Builds the analytical Gold star schema (dims + narrow facts) from Silver tables.
The derived gold-on-gold tier (feat_*, fct_match, train/test splits, mart_*,
match_outcomes, per-source *_team_season rollups) was dropped in #478.

Architecture
------------

    Triggered once by dag_master_pipeline after FBref Silver/xref and all
    direct Gold prerequisites complete (or by an operator manually)
        |
        v
    dim_competition, dim_season, dim_venue   (config-driven, no dependencies)
        |
        v
    dim_player, dim_team, dim_referee, dim_manager   (from silver.xref_*)
        |
        v
    dim_match   (star centre — carries FKs to ALL dims above, issue #425)
        |
        v
    dim_player_attributes, fct/dim season blocks
        |
        v
    fct_team_match, fct_player_match + остальные base facts (Stage 3)
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
Star-schema dims (issue #425 — unpartitioned, design grains):
- ``gold.dim_team``           — one row per club (attrs from team_aliases.yaml)
- ``gold.dim_player``         — one row per player (multi-source COALESCE)
- ``gold.dim_match``          — match passport: FKs to team/referee/venue/manager
- ``gold.dim_venue``          — one row per stadium (venue_aliases.yaml identity)
- ``gold.dim_referee``        — one row per referee (silver.xref_referee canonical)
- ``gold.dim_manager``        — one row per manager (stints -> fct_manager_stint, #429)
- ``gold.dim_competition``    — one row per league (configs/medallion/competitions.yaml)
- ``gold.dim_season``         — one row per season slug (same YAML)
Other:
- ``gold.fct_standings``      — SofaScore league-table snapshot (E2; renamed
                                from dim_standings in #428 — командный снапшот
                                это факт). Reads silver.xref_team(source='sofascore').
- ``gold.fct_team_match``     — long-form team metrics per match
- ``gold.fct_team_season_stats`` — cross-source per-season team stats (T6.4 #94;
                                FBref+Understat+WhoScored+SofaScore via xref_team)
- ``gold.fct_team_season_stats_audit`` — DQ-audit diff'ы для fct_team_season_stats
- ``gold.fct_player_match``   — player metrics per match
- ``gold.fct_player_unavailable`` — confirmed absences (E5; from WhoScored)
- ``gold.fct_player_market_value`` — FotMob market_value timeline per player×date
                                     (issue #11; bridge via silver.xref_player)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from utils.default_args import SILVER_ARGS
from utils import transfermarkt_native_v2 as tm_v2

# ---------------------------------------------------------------------------
# Transform definitions
# ---------------------------------------------------------------------------
# (task_id, sql_file, table_name, partition_cols)
#
# Order matters: dim_* must be built before fct_* which reference them.

# Star-schema dims (issue #425) build in the design's dependency order:
#   2a config dims (no dependencies) -> 2b xref dims -> 2c dim_match (centre).
# ALL star dims are unpartitioned (design rule: dims carry no partitions;
# dim_match is ~1.9k rows for APL×5 — partitioning is manifest noise).

# Stage 2a: config-driven dims, rendered from configs/medallion/*.yaml via
# utils.dim_loaders (Jinja .sql.j2 -> tempfile -> CTAS).
STAGE_2A_CONFIG_DIMS_INLINE = [
    # (task_id, renderer_name, template_path, table_name, partition_cols)
    ('dim_competition', 'render_dim_competition_sql',
     'dags/sql/gold/dim_competition.sql.j2', 'dim_competition', None),
    ('dim_season',      'render_dim_season_sql',
     'dags/sql/gold/dim_season.sql.j2',      'dim_season',      None),
    # dim_venue (issue #145): curated alias-identity from venue_aliases.yaml.
    ('dim_venue',       'render_dim_venue_sql',
     'dags/sql/gold/dim_venue.sql.j2',       'dim_venue',       None),
]

# Stage 2b: dims built from silver.xref_* canonical identities.
STAGE_2B_XREF_DIMS_SQL = [
    # (task_id, sql_file, table_name, partition_cols)
    ('dim_referee', 'dags/sql/gold/dim_referee.sql', 'dim_referee', None),
    (
        'dim_manager', 'dags/sql/gold/dim_manager.sql',
        'dim_manager_legacy', None,
    ),
]
STAGE_2B_XREF_DIMS_INLINE = [
    # dim_team: xref_team spine + country/short_name from team_aliases.yaml.
    ('dim_team', 'render_dim_team_sql',
     'dags/sql/gold/dim_team.sql.j2', 'dim_team', None),
    # dim_player (#435): xref_player spine + FBref FIFA-code->name map from
    # country_codes.yaml via {{ country_map_values_sql }}.
    ('dim_player', 'render_dim_player_sql',
     'dags/sql/gold/dim_player.sql.j2', 'dim_player', None),
]

# Stage 2c: the centre of the star — needs every dim above (FK targets) and
# resolves referee/venue/manager ids itself via xref + venue alias VALUES.
STAGE_2C_DIM_MATCH_INLINE = [
    ('dim_match', 'render_dim_match_sql',
     'dags/sql/gold/dim_match.sql.j2', 'dim_match', None),
]

# Stage 2d: snapshot/per-season blocks that historically shared the dim stage.
# fct_standings (#428, ex-dim_standings) IS partitioned by (league, season)
# because Bronze emits one snapshot per league/season.
STAGE_2D_SEASON_BLOCKS = [
    ('fct_standings', 'dags/sql/gold/fct_standings.sql', 'fct_standings', ['league', 'season']),
    # T5 wiring restore + T6 SofaScore. Player-season block depends on Silver
    # per-source aggregates (FBref/FotMob/WS/US/SofaScore) which are produced
    # by dag_transform_e3 ahead of this DAG via master pipeline.
    # dim_player_attributes — snapshot-grain (один row per canonical_id),
    # season не в SELECT. Без partition cols.
    ('dim_player_attributes',     'dags/sql/gold/dim_player_attributes.sql',
     'dim_player_attributes',     None),
    ('fct_player_season_stats',   'dags/sql/gold/fct_player_season_stats.sql.j2',
     'fct_player_season_stats',   ['league', 'season']),
    # #175 — keeper-variant per-season facts. Restored lost wiring (SQL + DQ
    # checks shipped in T5 but never registered in a STAGE list). Reads
    # silver.fotmob_keeper_profile (per-90 cols pass through as-is), so the
    # #174 FotMob count-drift does not apply here. Main facts precede both
    # audits (audit ref_integrity → main).
    ('fct_keeper_season_stats',   'dags/sql/gold/fct_keeper_season_stats.sql.j2',
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
    # #478: per-source season rollups (бывшие gold.*_team_season, ex Stage 1.5)
    # инлайнены в оба SQL как CTE — читают silver.*_team_match напрямую.
    ('fct_team_season_stats',     'dags/sql/gold/fct_team_season_stats.sql.j2',
     'fct_team_season_stats',     ['league', 'season']),
    ('fct_team_season_stats_audit',
     'dags/sql/gold/fct_team_season_stats_audit.sql',
     'fct_team_season_stats_audit', ['league', 'season']),
]

STAGE_3_FACTS = [
    ('fct_team_match',   'dags/sql/gold/fct_team_match.sql.j2',   'fct_team_match',   ['league', 'season']),
    # issue #95: cross-source DQ-audit для fct_team_match. INNER FBref ∩ Understat,
    # LEFT WhoScored / SofaScore с diff-колонками. WARNING-only DQ.
    # Pattern: fct_player_match_audit (issue #46).
    ('fct_team_match_audit', 'dags/sql/gold/fct_team_match_audit.sql',
     'fct_team_match_audit', ['league', 'season']),
    ('fct_player_match', 'dags/sql/gold/fct_player_match.sql.j2', 'fct_player_match', ['league', 'season']),
    # issue #46: cross-source DQ-audit для fct_player_match. INNER JOIN всех
    # 4 источников (FBref/SofaScore/Understat/WhoScored) с diff-колонками
    # `<metric>_diff_<source>` для anomaly detection. WARNING-only DQ.
    # Same pattern as fct_player_season_stats_audit (STAGE_2_DIMS).
    ('fct_player_match_audit', 'dags/sql/gold/fct_player_match_audit.sql',
     'fct_player_match_audit', ['league', 'season']),
    # E5: confirmed absences — самостоятельный narrow fact (WhoScored Silver).
    ('fct_player_unavailable', 'dags/sql/gold/fct_player_unavailable.sql',
     'fct_player_unavailable', ['league', 'season']),
    # issue #430: market_value timeline from TWO sources (fotmob +
    # transfermarkt, source in PK). Pointwise off-field fact — a career-long
    # timeline with no season key → unpartitioned (None), like fct_team_elo.
    ('fct_player_market_value', 'dags/sql/gold/fct_player_market_value.sql',
     'fct_player_market_value_legacy', None),
    # issue #427: unified per-event match chronicle (goals/cards/subs in ONE
    # timeline, PK (match_id, event_seq)). silver.fbref_match_events primary
    # + silver.whoscored_events_spadl per-match fallback. Runs in s3 — after g2c,
    # so dim_match (FK target) already exists. No STAGE_3_FALLBACKS entry:
    # silver.fbref_match_events is a core table, and the require_silver
    # mechanism can't guard bronze sources anyway (fct_card precedent).
    ('fct_match_timeline', 'dags/sql/gold/fct_match_timeline.sql',
     'fct_match_timeline', ['league', 'season']),
    # issue #429: SCD-2 manager employment history (stint = continuous run of
    # matches, islands-and-gaps over bronze.fbref_match_managers). Resurrected
    # from the pre-#433 dim_manager. Unpartitioned: ~50-200 rows, a stint may
    # span seasons. No STAGE_3_FALLBACKS entry: bronze sources can't be
    # guarded by require_silver (fct_match_timeline precedent).
    ('fct_manager_stint', 'dags/sql/gold/fct_manager_stint.sql',
     'fct_manager_stint', None),
    # issue #429: player transfers — pure projection of
    # silver.transfermarkt_transfers with 'tm_'-prefixed orphan ids (≈18%
    # players, most clubs unresolved). Unpartitioned: ~750 rows APL '2526'.
    ('fct_transfer', 'dags/sql/gold/fct_transfer.sql',
     'fct_transfer_legacy_source', None),
    # issue #431: external team-strength ELO from ClubElo. One row per team
    # per date (PK team_id, elo_date). Reads bronze.clubelo_ratings ∪
    # clubelo_ratings_historical + silver.xref_team — no silver ClubElo layer
    # (mirrors fct_standings reading bronze directly). Unpartitioned
    # (design §6 r5 — small off-field fact, no season key). No STAGE_3_FALLBACKS
    # entry: bronze sources can't be guarded by require_silver (fct_standings
    # precedent).
    ('fct_team_elo', 'dags/sql/gold/fct_team_elo.sql',
     'fct_team_elo', None),
    # issue #430: player wages from Capology (APL only). canonical_id is
    # resolved in Silver → pure projection with 'cap_'-prefixed orphan ids.
    # Partitioned by (league, season) — a contract is season-bound.
    ('fct_player_salary', 'dags/sql/gold/fct_player_salary.sql',
     'fct_player_salary', ['league', 'season']),
    # issue #430: EA Sports FC game ratings from SoFIFA. canonical_id resolved
    # in Silver → 'sf_'-prefixed orphan ids. Pointwise off-field fact keyed by
    # fifa_edition (1:1 with season) → unpartitioned (None), like fct_team_elo.
    ('fct_player_fifa_rating', 'dags/sql/gold/fct_player_fifa_rating.sql',
     'fct_player_fifa_rating', None),
    # issue #613: per-match officiating crew (referee/ar1/ar2/4th/var). referee_id
    # resolved best-effort via silver.xref_referee. Optional Silver source →
    # STAGE_3_FALLBACKS empty contract below. Runs in s3 (after g2c) so
    # dim_match / dim_referee (FK targets) already exist.
    ('fct_match_officials', 'dags/sql/gold/fct_match_officials.sql',
     'fct_match_officials', ['league', 'season']),
]

# Tables in STAGE_3 with optional Silver sources — runner routes CTAS to
# fallback SQL when source is absent.
STAGE_3_FALLBACKS = {
    'fct_player_unavailable': {
        'fallback_sql_file': 'dags/sql/gold/fct_player_unavailable_empty.sql',
        'require_silver':    ['whoscored_player_unavailable'],
    },
    # issue #430: market_value needs BOTH Silver sources; either may be absent
    # in an MVP env without FotMob / Transfermarkt ingest. Fallback holds the
    # empty contract.
    'fct_player_market_value': {
        'fallback_sql_file': 'dags/sql/gold/fct_player_market_value_empty.sql',
        'require_silver':    ['fotmob_player_market_value_history',
                              'transfermarkt_market_value_history'],
    },
    # issue #429: Transfermarkt Silver строится отдельным DAG'ом
    # (dag_transform_transfermarkt_silver) и может отсутствовать в env без
    # TM ingest. Fallback держит контракт пустой таблицы.
    'fct_transfer': {
        'fallback_sql_file': 'dags/sql/gold/fct_transfer_empty.sql',
        'require_silver':    ['transfermarkt_transfers'],
    },
    # issue #430: Capology / SoFIFA Silver may be absent in env without ingest.
    'fct_player_salary': {
        'fallback_sql_file': 'dags/sql/gold/fct_player_salary_empty.sql',
        'require_silver':    ['capology_player_salaries'],
    },
    'fct_player_fifa_rating': {
        'fallback_sql_file': 'dags/sql/gold/fct_player_fifa_rating_empty.sql',
        'require_silver':    ['sofifa_player_profile'],
    },
    # issue #613: officials Silver may be absent (combined_match_data hasn't
    # populated bronze.fbref_match_officials yet). Empty contract keeps the
    # Gold star intact.
    'fct_match_officials': {
        'fallback_sql_file': 'dags/sql/gold/fct_match_officials_empty.sql',
        'require_silver':    ['fbref_match_officials'],
    },
}

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

    if table_name in {
        'dim_manager_legacy', 'fct_player_market_value_legacy',
        'fct_transfer_legacy_source',
    }:
        ti = _ctx.get('ti')
        state = (
            ti.xcom_pull(task_ids='transfermarkt_reader_precondition')
            if ti is not None else None
        ) or {}
        if state.get('legacy_writers_disabled_at') is not None:
            return {
                'status': 'legacy_writer_disabled', 'table': table_name,
                'state_revision': state.get('revision'),
            }

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


def _tm_reader_precondition(**context) -> Dict[str, Any]:
    """Gate direct Gold triggers when canonical Transfermarkt readers are v2."""
    from airflow.exceptions import AirflowException

    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        state = tm_v2.read_reader_state(cur, allow_missing=True)
        result = state.to_dict()
        if state.active_version == 'v2':
            dag_run = context.get('dag_run')
            conf = getattr(dag_run, 'conf', None) or {}
            cycle_id = state.approved_cycle_id
            model_revision = conf.get('transfermarkt_model_revision')
            approved_model_revision = state.approved_model_revision
            if not all((
                cycle_id,
                approved_model_revision is not None,
                state.active_slot,
            )):
                raise AirflowException(
                    'TM v2 state has no complete approved cycle/slot/revision'
                )
            if (
                model_revision is not None
                and int(model_revision) != approved_model_revision
            ):
                raise AirflowException(
                    'explicit TM model revision differs from approved revision'
                )
            model_revision = approved_model_revision
            explicit_cycle = (
                conf.get('transfermarkt_parent_cycle_id')
                or conf.get('transfermarkt_cycle_id')
            )
            if explicit_cycle is not None and str(explicit_cycle) != cycle_id:
                raise AirflowException(
                    'explicit TM Gold cycle differs from ops-approved cycle'
                )
            explicit_slot = conf.get('transfermarkt_active_slot')
            if explicit_slot is not None and explicit_slot != state.active_slot:
                raise AirflowException(
                    'explicit TM active slot differs from ops-approved slot'
                )
            if state.approved_scope_set_id:
                explicit_scope_set = conf.get('transfermarkt_scope_set_id')
                if (
                    explicit_scope_set is not None
                    and str(explicit_scope_set) != state.approved_scope_set_id
                ):
                    raise AirflowException(
                        'explicit TM Gold scope set differs from ops-approved set'
                    )
                report = tm_v2.readiness(
                    cur,
                    cycle_id,
                    expected_revision=int(model_revision),
                    scope_set_id=state.approved_scope_set_id,
                    parent_cycle_id=cycle_id,
                    candidate_slot_override=state.active_slot,
                    require_fresh=False,
                    require_current_snapshots=False,
                )
                result['readiness_scope_set_id'] = state.approved_scope_set_id
            else:
                league = state.approved_league
                season = state.approved_season
                if not all((league, season is not None)):
                    raise AirflowException(
                        'TM v2 state has neither scope-set nor legacy scope evidence'
                    )
                report = tm_v2.readiness(
                    cur,
                    cycle_id,
                    league=league,
                    season=int(season),
                    expected_revision=int(model_revision),
                    require_fresh=False,
                    require_current_snapshots=False,
                )
            views = tm_v2.verify_reader_views(
                cur,
                expected_version='v2',
                expected_revision=state.revision,
                expected_slot=state.active_slot,
                allow_static_slot=state.cleanup_completed_at is not None,
            )
            if not report['ready'] or not views['passed']:
                raise AirflowException(
                    f'TM v2 direct Gold precondition failed: '
                    f'readiness={report} views={views}'
                )
            result['readiness_cycle_id'] = cycle_id
            result['model_revision'] = int(model_revision)
            result['approved_slot'] = state.active_slot
        return result
    finally:
        cur.close()
        conn.close()


def _tm_reader_postcondition(**context) -> Dict[str, Any]:
    """Reject a reader revision change during the long sequential Gold run."""
    ti = context.get('ti')
    captured = (
        ti.xcom_pull(task_ids='transfermarkt_reader_precondition')
        if ti is not None else None
    )
    if not captured:
        raise RuntimeError('Transfermarkt reader precondition XCom is missing')
    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        state = tm_v2.assert_reader_revision(cur, int(captured['revision']))
        if (
            state.active_version != captured['active_version']
            or state.active_slot != captured.get('active_slot')
        ):
            raise tm_v2.RevisionConflict(
                'Transfermarkt reader version changed without revision change'
            )
        return state.to_dict()
    finally:
        cur.close()
        conn.close()


def _tm_legacy_physical_dq(**context) -> Dict[str, Any]:
    """Keep the physical rollback branch green throughout retention."""
    ti = context.get('ti')
    captured = (
        ti.xcom_pull(task_ids='transfermarkt_reader_precondition')
        if ti is not None else None
    ) or {}
    if captured.get('legacy_writers_disabled_at') is not None:
        return {'status': 'persistently_disabled', 'passed': True}
    if not all((
        captured.get('approved_league'),
        captured.get('approved_season') is not None,
    )):
        return {'status': 'pre_cutover_not_required', 'passed': True}
    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        report = tm_v2._rollback_dq_report(
            cur, league=captured['approved_league'],
            season=int(captured['approved_season']),
        )
    finally:
        cur.close()
        conn.close()
    if not report['passed']:
        from airflow.exceptions import AirflowException
        raise AirflowException(
            f'physical Transfermarkt legacy retention DQ failed: {report}'
        )
    report['status'] = 'physical_legacy_ready'
    return report


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id='dag_transform_fbref_gold',
    default_args=SILVER_ARGS,
    description='Build Gold star schema (dims + narrow facts) from Silver tables',
    schedule=None,  # Trigger-only (called once by the master pipeline)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['transform', 'fbref', 'gold', 'football', 'trino', 'star-schema'],
    max_active_runs=1,
    max_active_tasks=1,  # Sequential — predictable RAM on dev Trino
    doc_md=__doc__,
) as dag:

    transfermarkt_reader_precondition = PythonOperator(
        task_id='transfermarkt_reader_precondition',
        python_callable=_tm_reader_precondition,
    )

    # Inline-rendered dims: import the renderer registry inside the DAG body
    # (NOT at module top) so DAG parse stays cheap for unrelated DAGs in the
    # same DagBag.
    from utils.dim_loaders import (
        render_dim_competition_sql,
        render_dim_match_sql,
        render_dim_player_sql,
        render_dim_season_sql,
        render_dim_team_sql,
        render_dim_venue_sql,
        run_inline_ctas,
    )
    _RENDERERS = {
        'render_dim_venue_sql':       render_dim_venue_sql,
        'render_dim_competition_sql': render_dim_competition_sql,
        'render_dim_season_sql':      render_dim_season_sql,
        'render_dim_team_sql':        render_dim_team_sql,
        'render_dim_player_sql':      render_dim_player_sql,
        'render_dim_match_sql':       render_dim_match_sql,
    }

    def _add_inline_dims(stage):
        for task_id, renderer_name, tpl, table_name, pcols in stage:
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

    # Stage 2a: config-driven dims (design §7 step 1 — no dependencies).
    with TaskGroup(group_id='s2a_config_dims') as g2a:
        _add_inline_dims(STAGE_2A_CONFIG_DIMS_INLINE)

    # Stage 2b: dims from silver.xref_* canonical identities (design step 2).
    with TaskGroup(group_id='s2b_xref_dims') as g2b:
        for task_id, sql_file, table_name, pcols in STAGE_2B_XREF_DIMS_SQL:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={'sql_file': sql_file, 'table_name': table_name,
                           'partition_cols': pcols},
            )
        _add_inline_dims(STAGE_2B_XREF_DIMS_INLINE)

    # Stage 2c: dim_match — the star centre (design step 3). Group chaining
    # (not intra-group deps) guarantees every FK-target dim exists first.
    with TaskGroup(group_id='s2c_dim_match') as g2c:
        _add_inline_dims(STAGE_2C_DIM_MATCH_INLINE)

    # Stage 2d: snapshot/per-season blocks (dim_player_attributes,
    # fct_*_season_stats, fct_standings ex-dim_standings #428).
    with TaskGroup(group_id='s2d_season_blocks') as g2d:
        for task_id, sql_file, table_name, pcols in STAGE_2D_SEASON_BLOCKS:
            PythonOperator(
                task_id=task_id,
                python_callable=_run_transform,
                op_kwargs={'sql_file': sql_file, 'table_name': table_name,
                           'partition_cols': pcols},
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

    validate_row_counts = PythonOperator(
        task_id='validate_gold_row_counts',
        python_callable=_row_counts,
    )

    validate_quality = PythonOperator(
        task_id='validate_gold_quality',
        python_callable=_quality,
    )

    transfermarkt_reader_postcondition = PythonOperator(
        task_id='transfermarkt_reader_postcondition',
        python_callable=_tm_reader_postcondition,
    )

    transfermarkt_legacy_physical_dq = PythonOperator(
        task_id='transfermarkt_legacy_physical_dq',
        python_callable=_tm_legacy_physical_dq,
    )

    (transfermarkt_reader_precondition >> g2a >> g2b >> g2c >> g2d >> g3
     >> validate_row_counts >> validate_quality
     >> transfermarkt_legacy_physical_dq
     >> transfermarkt_reader_postcondition)
