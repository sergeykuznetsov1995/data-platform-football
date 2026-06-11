#!/usr/bin/env python
# =============================================================================
# Superset dashboard: "Сводный обзор лиги — игроки"
# =============================================================================
# Aggregated player statistics dashboard for the current APL season.
#
# Layout (top-to-bottom):
#   1. Markdown header
#   2. KPI row: Players / Goals / xG / Avg rating
#   3-4. Атака: Top scorers, Top xG, Top assistants, Goals vs xG
#   5-6. Защита: Top tackles+interceptions, DF/MF table
#   7-8. Дисциплина: Top cards, Top penalties
#   9-10. Минуты и форма: Top minutes, Top SofaScore rating
#   11-12. Per-match timeline (Saka-кейс): xG/xA, рейтинг, ключевые передачи
#         — связаны с фильтром «Игрок» (multi-source fct_player_match, issue #46)
#   13. Full player summary table
#
# Season is hard-coded in virtual datasets — column type differs by source:
#   - FBref facts         -> bigint 2025
#   - SofaScore rating    -> bigint 2526 (sofascore short-form)
#   - Understat shots     -> varchar '2526' (understat short-form)
# dim_player (#425) is per-player (no season) — JOINs by player_id only.
# Single-season MVP; regenerate virtuals when a new season's data lands.
#
# All `superset.*` imports happen INSIDE `create_dashboard()` because the
# Superset model layer touches Flask globals at import time and demands an
# active `app.app_context()`.
# =============================================================================
from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger("dashboard.player_overview")

DASHBOARD_TITLE = "Сводный обзор лиги — игроки"
DASHBOARD_SLUG = "player-overview-league"
DATABASE_NAME = "trino_iceberg"
LEAGUE = "ENG-Premier League"
SEASON_FBREF = 2025          # fct_player_match / dim_player / dim_match
SEASON_OTHER_STR = "2526"    # fct_shot + fct_match_rating (varchar slug convention)


# ---------------------------------------------------------------------------
# Context wrapper: lets helpers stay top-level while receiving Superset model
# classes resolved lazily inside `create_dashboard()`.
# ---------------------------------------------------------------------------
class _Ctx:
    def __init__(
        self,
        db: Any,
        SqlaTable: Any,
        Database: Any,
        Dashboard: Any,
        Slice: Any,
    ) -> None:
        self.db = db
        self.SqlaTable = SqlaTable
        self.Database = Database
        self.Dashboard = Dashboard
        self.Slice = Slice


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _find_table(ctx: _Ctx, database: Any, schema: str, table_name: str) -> Any:
    table = (
        ctx.db.session.query(ctx.SqlaTable)
        .filter_by(database_id=database.id, schema=schema, table_name=table_name)
        .one_or_none()
    )
    if table is None:
        raise RuntimeError(
            f"dataset {schema}.{table_name} not found in database "
            f"'{database.database_name}'. Run import_datasources.py first."
        )
    return table


def _ensure_virtual_dataset(
    ctx: _Ctx, database: Any, schema: str, name: str, sql: str
) -> Any:
    """Create-or-update a virtual (SQL-backed) Superset dataset."""
    existing = (
        ctx.db.session.query(ctx.SqlaTable)
        .filter_by(database_id=database.id, schema=schema, table_name=name)
        .one_or_none()
    )
    if existing is not None:
        sql_changed = (existing.sql or "").strip() != sql.strip()
        if sql_changed:
            existing.sql = sql
            ctx.db.session.commit()
        # Refresh metadata when SQL changes OR when columns weren't populated on
        # a previous run (a transient fetch_metadata failure can leave the
        # dataset with 0 columns, which then breaks every chart built on it).
        if sql_changed or not list(existing.columns):
            try:
                existing.fetch_metadata()
                ctx.db.session.commit()
            except Exception as exc:  # noqa: BLE001
                log.warning("fetch_metadata failed for %s.%s: %s", schema, name, exc)
                ctx.db.session.rollback()
            log.info("refreshed virtual dataset %s.%s", schema, name)
        return existing

    table = ctx.SqlaTable(
        database_id=database.id,
        schema=schema,
        table_name=name,
        sql=sql,
    )
    ctx.db.session.add(table)
    ctx.db.session.commit()
    try:
        table.fetch_metadata()
        ctx.db.session.commit()
    except Exception as exc:  # noqa: BLE001
        log.warning("fetch_metadata failed for %s.%s: %s", schema, name, exc)
        ctx.db.session.rollback()
    log.info("created virtual dataset %s.%s", schema, name)
    return table


def _make_slice(
    ctx: _Ctx,
    name: str,
    viz_type: str,
    table: Any,
    params: dict[str, Any],
) -> Any:
    """Create-or-update a Slice by (name, dataset). Force-updates params."""
    full_params = dict(params)
    full_params.setdefault("datasource", f"{table.id}__table")
    full_params.setdefault("viz_type", viz_type)
    encoded = json.dumps(full_params)

    existing = (
        ctx.db.session.query(ctx.Slice)
        .filter_by(slice_name=name, datasource_id=table.id, datasource_type="table")
        .one_or_none()
    )
    if existing is not None:
        existing.viz_type = viz_type
        existing.params = encoded
        ctx.db.session.commit()
        log.info("updated slice '%s' (id=%s)", name, existing.id)
        return existing

    slc = ctx.Slice(
        slice_name=name,
        viz_type=viz_type,
        datasource_type="table",
        datasource_id=table.id,
        datasource_name=f"{table.schema}.{table.table_name}",
        params=encoded,
    )
    ctx.db.session.add(slc)
    ctx.db.session.commit()
    log.info("created slice '%s' (id=%s, viz=%s)", name, slc.id, viz_type)
    return slc


def _metric(label: str, agg: str, col: str | None, sql: str | None = None) -> dict:
    if sql is not None:
        return {
            "label": label,
            "expressionType": "SQL",
            "sqlExpression": sql,
        }
    return {
        "label": label,
        "expressionType": "SIMPLE",
        "aggregate": agg,
        "column": {"column_name": col},
    }


# ---------------------------------------------------------------------------
# Virtual datasets — все агрегации сезона захардкожены в SQL
# ---------------------------------------------------------------------------
def _build_virtual_datasets(ctx: _Ctx, database: Any) -> dict[str, Any]:
    schema = "gold"

    # NB: post issue #426, fct_player_match.PK = (match_id, player_id); the
    # star-aligned fact no longer carries denormalized team/position columns —
    # team_name comes from dim_team, position from dim_player.
    # Для cross-source-only игроков (SofaScore/Understat-only) JOIN dim_player
    # вернёт NULL → COALESCE покажет сырой canonical id.
    v_player_match_named = f"""\
SELECT
  fpm.player_id,
  COALESCE(dp.player_name, fpm.player_id) AS player_name,
  dp.primary_position AS position,
  REGEXP_EXTRACT(dp.primary_position, '^([A-Z]{{2}})') AS position_primary,
  dt.team_name,
  fpm.match_id,
  fpm.minutes_played AS minutes, fpm.goals, fpm.assists,
  fpm.shots, fpm.shots_on_target, fpm.penalty_goals, fpm.penalty_attempts,
  fpm.yellow_cards, fpm.red_cards, fpm.tackles_won, fpm.interceptions,
  fpm.fouls_committed, fpm.fouls_drawn, fpm.offsides, fpm.own_goals,
  fpm.league, fpm.season
FROM iceberg.gold.fct_player_match fpm
LEFT JOIN iceberg.gold.dim_player dp
  ON dp.player_id = fpm.player_id
LEFT JOIN iceberg.gold.dim_team dt
  ON dt.team_id = fpm.team_id
WHERE fpm.season = {SEASON_FBREF}
  AND fpm.league = '{LEAGUE}'"""

    v_player_xg = f"""\
SELECT
  s.player_id,
  COALESCE(dp.player_name, s.player_id) AS player_name,
  dp.primary_position,
  REGEXP_EXTRACT(dp.primary_position, '^([A-Z]{{2}})') AS position_primary,
  s.xg, s.is_goal, s.body_part, s.situation,
  s.league, s.season
FROM iceberg.gold.fct_shot s
LEFT JOIN iceberg.gold.dim_player dp
  ON dp.player_id = s.player_id
WHERE s.season = '{SEASON_OTHER_STR}'
  AND s.league = '{LEAGUE}'
  AND s.player_id IS NOT NULL"""

    v_player_goals_vs_xg = f"""\
WITH fbref_agg AS (
  SELECT player_id, SUM(goals) AS goals, SUM(minutes_played) AS minutes,
         SUM(shots) AS shots, ARBITRARY(team_id) AS team_id
  FROM iceberg.gold.fct_player_match
  WHERE season = {SEASON_FBREF} AND league = '{LEAGUE}'
  GROUP BY player_id
),
xg_agg AS (
  SELECT player_id, SUM(xg) AS xg
  FROM iceberg.gold.fct_shot
  WHERE season = '{SEASON_OTHER_STR}' AND league = '{LEAGUE}'
    AND player_id IS NOT NULL
  GROUP BY player_id
)
SELECT
  COALESCE(f.player_id, x.player_id) AS player_id,
  COALESCE(dp.player_name, COALESCE(f.player_id, x.player_id)) AS player_name,
  dp.primary_position,
  REGEXP_EXTRACT(dp.primary_position, '^([A-Z]{{2}})') AS position_primary,
  dt.team_name,
  COALESCE(f.goals, 0) AS goals,
  COALESCE(x.xg, 0.0)  AS xg,
  COALESCE(f.minutes, 0) AS minutes,
  COALESCE(f.shots, 0)   AS shots
FROM fbref_agg f
FULL OUTER JOIN xg_agg x ON f.player_id = x.player_id
LEFT JOIN iceberg.gold.dim_player dp
  ON dp.player_id = COALESCE(f.player_id, x.player_id)
LEFT JOIN iceberg.gold.dim_team dt
  ON dt.team_id = f.team_id
WHERE COALESCE(f.minutes, 0) >= 270"""

    # NB: fct_match_rating identifies players by ss_* canonical id (SofaScore).
    # The R2 player resolver does not currently bridge ss_* to fb_* (FBref), so a
    # JOIN with dim_player returns no rows. We expose the raw ss_id as
    # player_name and keep the chart honest about that. Filter is by rating
    # count (>= 5 matches) using a self-join CTE.
    v_player_rating = f"""\
SELECT
  fmr.player_id_canonical AS player_id,
  COALESCE(dp.player_name, fmr.player_id_canonical) AS player_name,
  COALESCE(dp.primary_position, 'unknown') AS position,
  REGEXP_EXTRACT(COALESCE(dp.primary_position, ''), '^([A-Z]{{2}})') AS position_primary,
  fmr.rating,
  fmr.team_side
FROM iceberg.gold.fct_match_rating fmr
LEFT JOIN iceberg.gold.dim_player dp
  ON dp.player_id = fmr.player_id_canonical
WHERE fmr.season = '{SEASON_OTHER_STR}'
  AND fmr.league = '{LEAGUE}'
  AND fmr.rating IS NOT NULL"""

    v_player_summary = f"""\
WITH fbref AS (
  SELECT player_id,
         COUNT(DISTINCT match_id) AS matches, SUM(minutes_played) AS minutes,
         SUM(goals) AS goals, SUM(assists) AS assists, SUM(shots) AS shots,
         SUM(shots_on_target) AS sot, SUM(tackles_won) AS tackles_won,
         SUM(interceptions) AS interceptions, SUM(yellow_cards) AS yellow_cards,
         SUM(red_cards) AS red_cards, SUM(penalty_goals) AS penalty_goals,
         SUM(penalty_attempts) AS penalty_attempts,
         ARBITRARY(team_id) AS team_id
  FROM iceberg.gold.fct_player_match
  WHERE season = {SEASON_FBREF} AND league = '{LEAGUE}'
  GROUP BY player_id
),
xg_cte AS (
  SELECT player_id, SUM(xg) AS total_xg
  FROM iceberg.gold.fct_shot
  WHERE season = '{SEASON_OTHER_STR}' AND league = '{LEAGUE}'
  GROUP BY player_id
),
rating_cte AS (
  -- avg_rating omitted: fct_match_rating.player_id_canonical is ss_* (SofaScore)
  -- and silver.xref_player has NO sofascore rows yet (only fbref/understat/
  -- whoscored). No deterministic way to bridge ss_* to fb_*, so we'd produce
  -- 100% NULLs. The standalone "Топ-10 по среднему рейтингу SofaScore" slice
  -- (id=44) still works because it operates on raw ss_* ids.
  SELECT
    CAST(NULL AS varchar) AS player_id,
    CAST(NULL AS double)  AS avg_rating
  WHERE FALSE
)
SELECT
  f.player_id,
  COALESCE(dp.player_name, f.player_id) AS player_name,
  dp.primary_position,
  REGEXP_EXTRACT(dp.primary_position, '^([A-Z]{{2}})') AS position_primary,
  dt.team_name, f.matches, f.minutes, f.goals, f.assists,
  COALESCE(x.total_xg, 0.0) AS xg,
  f.shots, f.sot,
  (f.tackles_won + f.interceptions) AS tackles_int,
  f.yellow_cards, f.red_cards, f.penalty_goals, f.penalty_attempts,
  r.avg_rating
FROM fbref f
LEFT JOIN xg_cte     x ON x.player_id = f.player_id
LEFT JOIN rating_cte r ON r.player_id = f.player_id
LEFT JOIN iceberg.gold.dim_player dp
  ON dp.player_id = f.player_id
LEFT JOIN iceberg.gold.dim_team dt
  ON dt.team_id = f.team_id"""

    # Per-match timeline (Saka-кейс): x = дата матча, y = xG/xA/rating/key_passes
    # из multi-source fct_player_match (post issue #46).
    # JOIN с dim_match даёт ось времени; player_name берём из dim_player по
    # FBref-canonical (для cross-source-only игроков покажем сырой canonical id).
    v_player_match_timeline = f"""\
SELECT
  fpm.player_id,
  COALESCE(dp.player_name, fpm.player_id) AS player_name,
  dp.primary_position AS position,
  REGEXP_EXTRACT(dp.primary_position, '^([A-Z]{{2}})') AS position_primary,
  dt.team_name,
  fpm.match_id,
  dm.match_date,
  fpm.minutes_played AS minutes,
  fpm.goals,
  fpm.assists,
  fpm.xg AS expected_goals,
  fpm.xa AS expected_assists,
  fpm.rating,
  fpm.key_passes,
  fpm.duels_won,
  fpm.league,
  fpm.season
FROM iceberg.gold.fct_player_match fpm
LEFT JOIN iceberg.gold.dim_match dm
  ON dm.match_id = fpm.match_id
LEFT JOIN iceberg.gold.dim_player dp
  ON dp.player_id = fpm.player_id
LEFT JOIN iceberg.gold.dim_team dt
  ON dt.team_id = fpm.team_id
WHERE fpm.season = {SEASON_FBREF}
  AND fpm.league = '{LEAGUE}'"""

    return {
        "v_player_match_named": _ensure_virtual_dataset(
            ctx, database, schema, "v_player_match_named", v_player_match_named
        ),
        "v_player_xg_2025": _ensure_virtual_dataset(
            ctx, database, schema, "v_player_xg_2025", v_player_xg
        ),
        "v_player_goals_vs_xg_2025": _ensure_virtual_dataset(
            ctx, database, schema, "v_player_goals_vs_xg_2025", v_player_goals_vs_xg
        ),
        "v_player_rating_2025": _ensure_virtual_dataset(
            ctx, database, schema, "v_player_rating_2025", v_player_rating
        ),
        "v_player_season_summary_2025": _ensure_virtual_dataset(
            ctx, database, schema, "v_player_season_summary_2025", v_player_summary
        ),
        "v_player_match_timeline_2025": _ensure_virtual_dataset(
            ctx, database, schema, "v_player_match_timeline_2025", v_player_match_timeline
        ),
    }


# ---------------------------------------------------------------------------
# Slices
# ---------------------------------------------------------------------------
def _build_slices(ctx: _Ctx, database: Any) -> list[Any]:
    vds = _build_virtual_datasets(ctx, database)

    slices: list[Any] = []

    # --- KPIs ---------------------------------------------------------------
    # Все KPI завязаны на virtual datasets (а не на физические fct_*), чтобы
    # dashboard-фильтры по `position_primary` / `team_name` находили колонки и
    # применялись к KPI тоже. SQL virtuals уже фильтруют season+league, никаких
    # adhoc_filters в KPI не нужно.
    # Common no-time-filter param for big_number_total — without it Superset
    # injects a default 7-day or 1-year window that filters everything out
    # if the dataset has no datetime column or none of its timestamps fall in
    # the default window.
    _kpi_time = {"time_range": "No filter"}

    slices.append(_make_slice(ctx,
        "Игроков в сезоне", "big_number_total", vds["v_player_match_named"],
        {
            **_kpi_time,
            "metric": _metric("COUNT(DISTINCT player_id)", "COUNT_DISTINCT", "player_id"),
            "subheader": "Сезон 2025/26",
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Голов суммарно", "big_number_total", vds["v_player_match_named"],
        {
            **_kpi_time,
            "metric": _metric("SUM(goals)", "SUM", "goals"),
            "subheader": "ENG-Premier League",
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Суммарный xG", "big_number_total", vds["v_player_xg_2025"],
        {
            **_kpi_time,
            "metric": _metric("SUM(xg)", "SUM", "xg"),
            "subheader": "Understat",
            "y_axis_format": ".1f",
        },
    ))

    slices.append(_make_slice(ctx,
        "Средний рейтинг лиги", "big_number_total", vds["v_player_rating_2025"],
        {
            **_kpi_time,
            "metric": _metric("AVG(rating)", "AVG", "rating"),
            "subheader": "SofaScore",
            "y_axis_format": ".2f",
        },
    ))

    # --- Атака -------------------------------------------------------------
    slices.append(_make_slice(ctx,
        "Топ-10 бомбардиров", "dist_bar", vds["v_player_match_named"],
        {
            "metrics": [_metric("SUM(goals)", "SUM", "goals")],
            "groupby": ["player_name"],
            "row_limit": 10, "order_desc": True,
            "show_legend": False, "show_bar_value": True,
            "x_ticks_layout": "45°", "bottom_margin": 100,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-10 по xG", "dist_bar", vds["v_player_xg_2025"],
        {
            "metrics": [_metric("SUM(xg)", "SUM", "xg")],
            "groupby": ["player_name"],
            "row_limit": 10, "order_desc": True,
            "show_legend": False, "show_bar_value": True,
            "x_ticks_layout": "45°", "bottom_margin": 100,
            "y_axis_format": ".2f",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-10 ассистентов", "dist_bar", vds["v_player_match_named"],
        {
            "metrics": [_metric("SUM(assists)", "SUM", "assists")],
            "groupby": ["player_name"],
            "row_limit": 10, "order_desc": True,
            "show_legend": False, "show_bar_value": True,
            "x_ticks_layout": "45°", "bottom_margin": 100,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Голы vs xG", "bubble_v2", vds["v_player_goals_vs_xg_2025"],
        {
            "entity": "player_name",
            "series": "position_primary",
            "x": _metric("SUM(xg)", "SUM", "xg"),
            "y": _metric("SUM(goals)", "SUM", "goals"),
            "size": _metric("SUM(minutes)", "SUM", "minutes"),
            "row_limit": 500,
            "max_bubble_size": "25",
            "x_axis_label": "xG (Understat)",
            "y_axis_label": "Голы (FBref)",
            "x_axis_format": ".1f",
            "y_axis_format": "SMART_NUMBER",
            "show_legend": True,
        },
    ))

    # --- Защита ------------------------------------------------------------
    slices.append(_make_slice(ctx,
        "Топ-10 отборы+перехваты", "dist_bar", vds["v_player_match_named"],
        {
            "metrics": [_metric(
                "tackles+interceptions", "SUM", None,
                sql="SUM(tackles_won + interceptions)",
            )],
            "groupby": ["player_name"],
            "row_limit": 10, "order_desc": True,
            "show_legend": False, "show_bar_value": True,
            "x_ticks_layout": "45°", "bottom_margin": 100,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Защитники и опорники — детально", "table", vds["v_player_match_named"],
        {
            "query_mode": "aggregate",
            "groupby": ["player_name", "position", "team_name"],
            "metrics": [
                _metric("Отборы", "SUM", "tackles_won"),
                _metric("Перехваты", "SUM", "interceptions"),
                _metric("Фолы", "SUM", "fouls_committed"),
                _metric("Минуты", "SUM", "minutes"),
            ],
            "adhoc_filters": [{
                "expressionType": "SQL", "clause": "WHERE",
                "sqlExpression": "position LIKE '%DF%' OR position LIKE '%MF%'",
            }],
            "row_limit": 25,
            "order_by_cols": ['["Отборы", false]'],
            "include_search": True,
            "show_cell_bars": True,
        },
    ))

    # --- Дисциплина --------------------------------------------------------
    slices.append(_make_slice(ctx,
        "Топ-10 нарушений (ЖК + 2×КК)", "dist_bar", vds["v_player_match_named"],
        {
            "metrics": [_metric(
                "card_score", "SUM", None,
                sql="SUM(yellow_cards + 2 * red_cards)",
            )],
            "groupby": ["player_name"],
            "row_limit": 10, "order_desc": True,
            "show_legend": False, "show_bar_value": True,
            "x_ticks_layout": "45°", "bottom_margin": 100,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-10 по пенальти", "dist_bar", vds["v_player_match_named"],
        {
            "metrics": [
                _metric("Удары с пенальти", "SUM", "penalty_attempts"),
                _metric("Голы с пенальти", "SUM", "penalty_goals"),
            ],
            "groupby": ["player_name"],
            "row_limit": 10, "order_desc": True,
            "show_legend": True, "show_bar_value": True,
            "x_ticks_layout": "45°", "bottom_margin": 100,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    # --- Минуты и форма ----------------------------------------------------
    slices.append(_make_slice(ctx,
        "Топ-10 по минутам на поле", "dist_bar", vds["v_player_match_named"],
        {
            "metrics": [_metric("SUM(minutes)", "SUM", "minutes")],
            "groupby": ["player_name"],
            "row_limit": 10, "order_desc": True,
            "show_legend": False, "show_bar_value": True,
            "x_ticks_layout": "45°", "bottom_margin": 100,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-10 по среднему рейтингу SofaScore", "dist_bar", vds["v_player_rating_2025"],
        {
            "metrics": [_metric("AVG(rating)", "AVG", "rating")],
            "groupby": ["player_name"],
            "row_limit": 10, "order_desc": True,
            "show_legend": False, "show_bar_value": True,
            "x_ticks_layout": "45°", "bottom_margin": 100,
            "y_axis_format": ".2f",
        },
    ))

    # --- Per-match timeline (Saka-кейс) -----------------------------------
    # Эти чарты осмысленны только в связке с filter'ом "Игрок": без него на
    # одном line-графике будут сотни кривых. См. _build_native_filters.
    slices.append(_make_slice(ctx,
        "xG / xA по матчам", "echarts_timeseries_line",
        vds["v_player_match_timeline_2025"],
        {
            "x_axis": "match_date",
            "metrics": [
                _metric("SUM(xG)", "SUM", "expected_goals"),
                _metric("SUM(xA)", "SUM", "expected_assists"),
            ],
            "groupby": ["player_name"],
            "row_limit": 5000,
            "show_legend": True,
            "rich_tooltip": True,
            "x_axis_title": "Дата матча",
            "y_axis_title": "xG / xA",
            "y_axis_format": ".2f",
        },
    ))

    slices.append(_make_slice(ctx,
        "Рейтинг по матчам (SofaScore)", "echarts_timeseries_line",
        vds["v_player_match_timeline_2025"],
        {
            "x_axis": "match_date",
            "metrics": [_metric("AVG(rating)", "AVG", "rating")],
            "groupby": ["player_name"],
            "row_limit": 5000,
            "show_legend": True,
            "rich_tooltip": True,
            "x_axis_title": "Дата матча",
            "y_axis_title": "Рейтинг (0-10)",
            "y_axis_format": ".2f",
            "adhoc_filters": [{
                "expressionType": "SIMPLE",
                "subject": "rating",
                "operator": "IS NOT NULL",
                "clause": "WHERE",
            }],
        },
    ))

    slices.append(_make_slice(ctx,
        "Ключевые передачи по матчам", "dist_bar",
        vds["v_player_match_timeline_2025"],
        {
            "metrics": [_metric("SUM(key_passes)", "SUM", "key_passes")],
            "groupby": ["match_date"],
            "row_limit": 100,
            "order_desc": False,
            "show_legend": False,
            "show_bar_value": True,
            "x_ticks_layout": "45°",
            "bottom_margin": 80,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    # --- Сводка ------------------------------------------------------------
    slices.append(_make_slice(ctx,
        "Сводная таблица игроков", "table", vds["v_player_season_summary_2025"],
        {
            "query_mode": "raw",
            "all_columns": [
                "player_name", "position", "team_name", "matches", "minutes",
                "goals", "assists", "xg", "shots", "sot",
                "tackles_int", "yellow_cards", "red_cards",
                "penalty_goals", "penalty_attempts",
            ],
            "row_limit": 200,
            "order_by_cols": ['["minutes", false]'],
            "include_search": True,
            "show_cell_bars": True,
            "table_timestamp_format": "smart_date",
        },
    ))

    return slices


# ---------------------------------------------------------------------------
# Layout (position_json) — grid 12-wide rows
# ---------------------------------------------------------------------------
def _build_position_json(slices: list[Any]) -> dict[str, Any]:
    pos: dict[str, Any] = {"DASHBOARD_VERSION_KEY": "v2"}

    grid_id = "GRID_ID"
    root_id = "ROOT_ID"
    pos["ROOT_ID"] = {"type": "ROOT", "id": root_id, "children": [grid_id]}
    pos[grid_id] = {
        "type": "GRID",
        "id": grid_id,
        "children": [],
        "parents": [root_id],
        "meta": {},
    }

    def _markdown(text: str, height: int = 4) -> str:
        mid = f"MARKDOWN-{abs(hash(text)) % (10**8)}"
        pos[mid] = {
            "type": "MARKDOWN",
            "id": mid,
            "children": [],
            "parents": [root_id, grid_id],
            "meta": {"width": 12, "height": height, "code": text, "background": "BACKGROUND_TRANSPARENT"},
        }
        return mid

    def _row(child_ids: list[str]) -> str:
        rid = f"ROW-{abs(hash(tuple(child_ids))) % (10**8)}"
        pos[rid] = {
            "type": "ROW",
            "id": rid,
            "children": child_ids,
            "parents": [root_id, grid_id],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        return rid

    def _chart(slc: Any, width: int, height: int = 50) -> str:
        cid = f"CHART-{slc.id}"
        pos[cid] = {
            "type": "CHART",
            "id": cid,
            "children": [],
            "parents": [root_id, grid_id, "row_placeholder"],
            "meta": {
                "width": width,
                "height": height,
                "chartId": slc.id,
                "sliceName": slc.slice_name,
            },
        }
        return cid

    children: list[str] = []
    children.append(_markdown(
        "# Сводный обзор лиги — игроки\n\n"
        "**ENG-Premier League** · Сезон **2025/26** · Источники: FBref + Understat (xG) + SofaScore (рейтинг)",
        height=10,
    ))
    children.append(_row([_chart(slices[i], 3, height=25) for i in range(0, 4)]))

    children.append(_markdown("### Атака — голы, удары, xG", height=3))
    children.append(_row([_chart(slices[4], 6), _chart(slices[5], 6)]))
    children.append(_row([_chart(slices[6], 6), _chart(slices[7], 6, height=60)]))

    children.append(_markdown("### Защита — отборы, перехваты, фолы", height=3))
    children.append(_row([_chart(slices[8], 6), _chart(slices[9], 6, height=60)]))

    children.append(_markdown("### Дисциплина — карточки и пенальти", height=3))
    children.append(_row([_chart(slices[10], 6), _chart(slices[11], 6)]))

    children.append(_markdown("### Минуты и форма", height=3))
    children.append(_row([_chart(slices[12], 6), _chart(slices[13], 6)]))

    children.append(_markdown(
        "### Per-match timeline\n"
        "Выберите игрока через фильтр «Игрок» в шапке, чтобы увидеть его "
        "форму матч за матчем (xG/xA, рейтинг SofaScore, ключевые передачи).",
        height=4,
    ))
    children.append(_row([_chart(slices[14], 6, height=55), _chart(slices[15], 6, height=55)]))
    children.append(_row([_chart(slices[16], 12, height=50)]))

    children.append(_row([_chart(slices[17], 12, height=80)]))

    pos[grid_id]["children"] = children
    return pos


# ---------------------------------------------------------------------------
# Native filters
# ---------------------------------------------------------------------------
def _build_native_filters(
    slices: list[Any],
    filter_dataset_id: int,
) -> list[dict[str, Any]]:
    """Three dashboard-level filters: Position, Team, Player.

    Scope is set per-filter based on which slices have the column. KPI slices
    on physical fct_* tables don't have `position_primary` (a synthesised
    virtual-dataset column) — applying the position filter to them would 500.

    Mapping (slice index → dataset → has_position_primary / has_team_name):
      0 dim_player          (— / —)
      1 fct_player_match    (— / ✓)
      2 fct_shot            (— / —)
      3 fct_match_rating    (— / —)
      4..12 v_player_match_named            (✓ / ✓)  (subset of indices)
      5 v_player_xg_2025                    (✓ / —)
      7 v_player_goals_vs_xg_2025           (✓ / ✓)
      13 v_player_rating_2025               (✓ / —)
      14 v_player_season_summary_2025       (✓ / ✓)
    """
    chart_ids_all = [slc.id for slc in slices]
    # All slices now use virtual datasets that expose `position_primary`, so
    # the position filter applies everywhere. `team_name` is missing only on
    # the xG/rating-only virtuals (slices 5, 13). Timeline virtuals (14, 15, 16)
    # expose `team_name` (v_player_match_timeline_2025).
    # "Игрок" фильтр применяется ТОЛЬКО к timeline-чартам (14, 15, 16) — на
    # сезон-агрегатах он не имеет смысла (там и так всё разбито по player_name).
    team_idxs = [0, 1, 4, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17]
    player_idxs = [14, 15, 16]
    position_chart_ids = chart_ids_all
    team_chart_ids = [slices[i].id for i in team_idxs]
    player_chart_ids = [slices[i].id for i in player_idxs]

    def _filter(
        fid: str, name: str, column: str, multiple: bool,
        target_chart_ids: list[int],
    ) -> dict[str, Any]:
        return {
            "id": fid,
            "name": name,
            "filterType": "filter_select",
            "targets": [{"column": {"name": column}, "datasetId": filter_dataset_id}],
            "controlValues": {
                "multiSelect": multiple,
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "inverseSelection": False,
            },
            "defaultDataMask": {"filterState": {}, "extraFormData": {}},
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": [
                    cid for cid in chart_ids_all if cid not in target_chart_ids
                ],
            },
            "type": "NATIVE_FILTER",
            "description": "",
            "chartsInScope": target_chart_ids,
            "tabsInScope": [],
        }

    return [
        _filter("NATIVE_FILTER-position", "Позиция", "position_primary",
                multiple=True, target_chart_ids=position_chart_ids),
        _filter("NATIVE_FILTER-team", "Команда", "team_name",
                multiple=True, target_chart_ids=team_chart_ids),
        _filter("NATIVE_FILTER-player", "Игрок", "player_name",
                multiple=True, target_chart_ids=player_chart_ids),
    ]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def create_dashboard() -> Any:
    # Lazy imports — require an active Flask app context
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        ctx = _Ctx(db=db, SqlaTable=SqlaTable, Database=Database,
                   Dashboard=Dashboard, Slice=Slice)

        database = (
            db.session.query(Database).filter_by(database_name=DATABASE_NAME).one_or_none()
        )
        if database is None:
            raise RuntimeError(
                f"database '{DATABASE_NAME}' not found. Run import_datasources.py first."
            )

        slices = _build_slices(ctx, database)
        position_json = _build_position_json(slices)
        # v_player_match_named has both `position_primary` and `team_name`
        # columns — use it as the values source for both filters.
        filter_ds = (
            db.session.query(SqlaTable)
            .filter_by(database_id=database.id, schema="gold",
                       table_name="v_player_match_named")
            .one()
        )
        native_filters = _build_native_filters(slices, filter_ds.id)

        metadata = {
            "native_filter_configuration": native_filters,
            "chart_configuration": {},
            "global_chart_configuration": {
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "chartsInScope": [slc.id for slc in slices],
            },
            "color_scheme": "supersetColors",
            "label_colors": {},
            "shared_label_colors": {},
            "default_filters": "{}",
            "refresh_frequency": 0,
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "filter_scopes": {},
        }

        existing = (
            db.session.query(Dashboard).filter_by(slug=DASHBOARD_SLUG).one_or_none()
        )
        if existing is not None:
            existing.dashboard_title = DASHBOARD_TITLE
            existing.position_json = json.dumps(position_json)
            existing.json_metadata = json.dumps(metadata)
            existing.slices = slices
            existing.published = True
            db.session.commit()
            log.info("updated dashboard '%s' (id=%s, slices=%d)",
                     DASHBOARD_SLUG, existing.id, len(slices))
            return existing

        dashboard = Dashboard(
            dashboard_title=DASHBOARD_TITLE,
            slug=DASHBOARD_SLUG,
            published=True,
            slices=slices,
            position_json=json.dumps(position_json),
            json_metadata=json.dumps(metadata),
        )
        db.session.add(dashboard)
        db.session.commit()
        log.info("created dashboard '%s' (id=%s, slices=%d)",
                 DASHBOARD_TITLE, dashboard.id, len(slices))
        return dashboard


if __name__ == "__main__":
    logging.basicConfig(
        level="INFO",
        format="[dashboard.player_overview] %(levelname)s %(message)s",
    )
    create_dashboard()
