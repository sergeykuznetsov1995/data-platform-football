#!/usr/bin/env python
# =============================================================================
# Superset dashboard: "Обзор лиги + игроки"
# =============================================================================
# Мультилиговый обзорный дашборд: таблица лиги (Understat-стиль), командные
# метрики (xG/xGA, реализация, PPDA, деньги), динамика Elo и игроцкая
# аналитика (топы, scatter'ы, сводная таблица).
#
# Layout (top-to-bottom):
#   1. Markdown header
#   2. KPI row: Команд / Голов / Средний xG / Средний рейтинг
#   3. Таблица лиги (full width, cell bars, «сверх-очки» = points − xPTS)
#   4. Команды 2×2: xG vs xGA · голы − xG · PPDA · стоимость состава vs очки
#   5. Elo по времени (линия = команда, недельное среднее)
#   6. Игроки — топ-15 ×6: голы · xG · ассисты · рейтинг · стоимость · зарплата
#   7. Аналитика: xG vs голы · стоимость vs Г+А · big chances · голы − xG
#   8. Сводная таблица игроков с поиском
#
# В отличие от player_overview.py здесь НЕТ хардкода league/season: все
# виртуальные датасеты отдают все сезоны с колонками league/season, фильтрация
# целиком на native filters (Лига → Сезон → Команда → Игрок каскадом; Сезон
# required с дефолтом из dim_season.is_current).
#
# Данные: зарплаты/contract_status (Capology) есть только для EPL 2025/26 —
# на других сезонах зарплатный бар и колонки зарплат пусты (дисклеймер в
# markdown). PPDA/xPTS (Understat) сейчас только EPL.
#
# All `superset.*` imports happen INSIDE `create_dashboard()` because the
# Superset model layer touches Flask globals at import time and demands an
# active `app.app_context()`.
# =============================================================================
from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger("dashboard.league_overview")

DASHBOARD_TITLE = "Обзор лиги + игроки"
DASHBOARD_SLUG = "league-overview"
DATABASE_NAME = "trino_iceberg"
FALLBACK_SEASON = "2526"     # если dim_season.is_current недоступен при импорте


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
# Helpers (как в player_overview.py)
# ---------------------------------------------------------------------------
def _apply_column_labels(
    ctx: _Ctx, table: Any, labels: dict[str, str] | None
) -> None:
    """Русские verbose_name для колонок — заголовки raw-таблиц в дашборде."""
    if not labels:
        return
    changed = False
    for col in table.columns:
        wanted = labels.get(col.column_name)
        if wanted and col.verbose_name != wanted:
            col.verbose_name = wanted
            changed = True
    if changed:
        ctx.db.session.commit()


def _ensure_virtual_dataset(
    ctx: _Ctx, database: Any, schema: str, name: str, sql: str,
    labels: dict[str, str] | None = None,
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
        _apply_column_labels(ctx, existing, labels)
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
    _apply_column_labels(ctx, table, labels)
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


def _sql_where(expression: str) -> dict:
    return {
        "expressionType": "SQL",
        "clause": "WHERE",
        "sqlExpression": expression,
    }


# ---------------------------------------------------------------------------
# Virtual datasets — БЕЗ хардкода league/season: фильтрация на native filters
# ---------------------------------------------------------------------------
def _build_virtual_datasets(ctx: _Ctx, database: Any) -> dict[str, Any]:
    schema = "gold"

    # Грейн (league, season, team_id) = fct_standings; xG/xPTS/PPDA/деньги —
    # LEFT JOIN fct_team_season_stats (у orphan-команд ss_*/fm_* будет NULL).
    v_team_season = """\
SELECT
  st.league,
  st.season,
  ds.season_name,
  st.team_id,
  COALESCE(dt.team_name, st.team_name_raw) AS team_name,
  st.position,
  st.played, st.wins, st.draws, st.losses,
  st.goals_for, st.goals_against, st.goal_diff,
  st.points, st.points_per_game,
  tss.expected_goals AS xg,
  tss.expected_goals_against AS xga,
  tss.xpts,
  st.points - tss.xpts AS over_points,
  tss.goals AS goals_fbref,
  tss.goals - tss.expected_goals AS finishing_delta,
  tss.ppda, tss.oppda, tss.deep_completions,
  tss.possession_pct, tss.big_chances,
  tss.squad_market_value_eur, tss.total_wage_bill_eur
FROM iceberg.gold.fct_standings st
LEFT JOIN iceberg.gold.fct_team_season_stats tss
  ON  tss.team_id = st.team_id
  AND tss.league  = st.league
  AND tss.season  = st.season
LEFT JOIN iceberg.gold.dim_team   dt ON dt.team_id = st.team_id
LEFT JOIN iceberg.gold.dim_season ds ON ds.season  = st.season"""

    # fct_team_elo не несёт league/season — приклеиваем через окно dim_season +
    # membership команды в fct_standings. ROW_NUMBER — guard от фан-аута, если
    # команда окажется в standings двух лиг в одном сезоне. Летние даты вне
    # окон сезонов отсекаются JOIN-ом (разрывы линий в межсезонье — ожидаемо).
    v_team_elo = """\
WITH joined AS (
  SELECT
    e.team_id,
    COALESCE(dt.team_name, e.team_name_raw) AS team_name,
    st.league,
    st.season,
    ds.season_name,
    e.elo_date,
    e.elo,
    e."rank" AS elo_rank,
    ROW_NUMBER() OVER (
      PARTITION BY e.team_id, e.elo_date
      ORDER BY st.league
    ) AS rn
  FROM iceberg.gold.fct_team_elo e
  JOIN iceberg.gold.dim_season ds
    ON e.elo_date BETWEEN ds.start_date AND ds.end_date
  JOIN iceberg.gold.fct_standings st
    ON  st.team_id = e.team_id
    AND st.season  = ds.season
  LEFT JOIN iceberg.gold.dim_team dt ON dt.team_id = e.team_id
)
SELECT team_id, team_name, league, season, season_name, elo_date, elo, elo_rank
FROM joined
WHERE rn = 1"""

    # Грейн (player_id, league, season). Все JOIN строго с (league, season) —
    # иначе фан-аут (см. CLAUDE.md footguns). Стоимость — as-of конец сезона
    # (последняя TM-оценка до dim_season.end_date); contract_until — из silver
    # Transfermarkt (шире, чем Capology contract_status = только EPL 2526).
    v_player_season = """\
WITH ps AS (
  SELECT * FROM iceberg.gold.fct_player_season_stats
),
mv_asof AS (
  SELECT
    k.player_id, k.league, k.season, mv.market_value_eur,
    ROW_NUMBER() OVER (
      PARTITION BY k.player_id, k.league, k.season
      ORDER BY mv.valuation_date DESC
    ) AS rn
  FROM (SELECT DISTINCT player_id, league, season FROM ps) k
  JOIN iceberg.gold.dim_season ds ON ds.season = k.season
  JOIN iceberg.gold.fct_player_market_value mv
    ON  mv.player_id      = k.player_id
    AND mv.source         = 'transfermarkt'
    AND mv.valuation_date <= ds.end_date
),
tm AS (
  SELECT canonical_id, league, season, contract_until,
         ROW_NUMBER() OVER (
           PARTITION BY canonical_id, league, season
           ORDER BY _bronze_ingested_at DESC
         ) AS rn
  FROM iceberg.silver.transfermarkt_players
  WHERE canonical_id IS NOT NULL
)
SELECT
  ps.player_id,
  COALESCE(dp.player_name, ps.player_id) AS player_name,
  dp.primary_position,
  REGEXP_EXTRACT(dp.primary_position, '^([A-Z]{2})') AS position_primary,
  COALESCE(dt.team_name, ps.team_id) AS team_name,
  ps.league, ps.season,
  ps.matches, ps.minutes,
  ps.goals, ps.assists, ps.non_penalty_goals, ps.shots, ps.key_passes,
  ps.expected_goals, ps.expected_assists,
  ps.goals - ps.expected_goals AS finishing_delta,
  ps.rating_sofascore,
  ps.successful_dribbles, ps.take_on_pct, ps.total_duels_won_pct,
  ps.big_chances_created,
  mv.market_value_eur,
  sal.annual_gross_eur, sal.weekly_gross_eur, sal.contract_status,
  tm.contract_until
FROM ps
LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = ps.player_id
LEFT JOIN iceberg.gold.dim_team   dt ON dt.team_id  = ps.team_id
LEFT JOIN mv_asof mv
  ON  mv.player_id = ps.player_id AND mv.league = ps.league
  AND mv.season    = ps.season    AND mv.rn = 1
LEFT JOIN iceberg.gold.fct_player_salary sal
  ON  sal.player_id = ps.player_id AND sal.league = ps.league
  AND sal.season    = ps.season
LEFT JOIN tm
  ON  tm.canonical_id = ps.player_id AND tm.league = ps.league
  AND tm.season       = ps.season    AND tm.rn = 1"""

    team_labels = {
        "position": "Место", "team_name": "Команда", "played": "И",
        "wins": "В", "draws": "Н", "losses": "П",
        "goals_for": "Забито", "goals_against": "Пропущено",
        "goal_diff": "Разница", "points": "Очки",
        "xg": "xG", "xga": "xGA", "xpts": "xPTS",
        "over_points": "Сверх-очки",
    }
    player_labels = {
        "player_name": "Игрок", "team_name": "Клуб",
        "primary_position": "Позиция", "matches": "Матчи",
        "minutes": "Минуты", "goals": "Голы", "assists": "Ассисты",
        "expected_goals": "xG", "expected_assists": "xA",
        "rating_sofascore": "Рейтинг",
        "total_duels_won_pct": "% единоборств", "take_on_pct": "% обводок",
        "market_value_eur": "Стоимость (€)",
        "annual_gross_eur": "Зарплата (€/год)",
        "contract_status": "Контракт (статус)", "contract_until": "Контракт до",
    }

    return {
        "v_lo_team_season": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_team_season", v_team_season,
            labels=team_labels,
        ),
        "v_lo_team_elo": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_team_elo", v_team_elo
        ),
        "v_lo_player_season": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_player_season", v_player_season,
            labels=player_labels,
        ),
    }


# ---------------------------------------------------------------------------
# Slices
# ---------------------------------------------------------------------------
def _build_slices(ctx: _Ctx, vds: dict[str, Any]) -> list[Any]:
    team = vds["v_lo_team_season"]
    elo = vds["v_lo_team_elo"]
    player = vds["v_lo_player_season"]

    slices: list[Any] = []

    # Common no-time-filter param for big_number_total — without it Superset
    # injects a default time window that filters everything out.
    _kpi_time = {"time_range": "No filter"}

    # Общие параметры топ-баров (dist_bar — легаси, но проверен этим же
    # инстансом в player_overview.py; TODO мигрировать на echarts к Superset 5).
    _bar = {
        "order_desc": True,
        "show_legend": False,
        "show_bar_value": True,
        "x_ticks_layout": "45°",
        "bottom_margin": 120,
    }

    # --- 0..3 KPI -----------------------------------------------------------
    slices.append(_make_slice(ctx,
        "Команд в лиге", "big_number_total", team,
        {
            **_kpi_time,
            "metric": _metric("COUNT(DISTINCT team_name)", "COUNT_DISTINCT", "team_name"),
            "subheader": "в выбранном срезе",
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Всего голов", "big_number_total", team,
        {
            **_kpi_time,
            "metric": _metric("SUM(goals_for)", "SUM", "goals_for"),
            "subheader": "по турнирной таблице",
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Средний xG команды", "big_number_total", team,
        {
            **_kpi_time,
            "metric": _metric("AVG(xg)", "AVG", "xg"),
            "subheader": "за сезон",
            "y_axis_format": ".1f",
        },
    ))

    slices.append(_make_slice(ctx,
        "Средний рейтинг игроков", "big_number_total", player,
        {
            **_kpi_time,
            "metric": _metric("AVG(rating)", "AVG", "rating_sofascore"),
            "subheader": "SofaScore",
            "y_axis_format": ".2f",
        },
    ))

    # --- 4 Таблица лиги ------------------------------------------------------
    slices.append(_make_slice(ctx,
        "Таблица лиги", "table", team,
        {
            "query_mode": "raw",
            "all_columns": [
                "position", "team_name", "played", "wins", "draws", "losses",
                "goals_for", "goals_against", "goal_diff", "points",
                "xg", "xga", "xpts", "over_points",
            ],
            "order_by_cols": ['["position", true]'],
            "row_limit": 50,
            "include_search": False,
            "show_cell_bars": True,
            "conditional_formatting": [
                {"operator": ">", "targetValue": 0,
                 "column": "over_points", "colorScheme": "#ACE1C4"},
                {"operator": "<", "targetValue": 0,
                 "column": "over_points", "colorScheme": "#EFA1AA"},
            ],
            "column_config": {
                "xg": {"d3NumberFormat": ".1f"},
                "xga": {"d3NumberFormat": ".1f"},
                "xpts": {"d3NumberFormat": ".1f"},
                "over_points": {"d3NumberFormat": "+.1f"},
            },
            "table_timestamp_format": "smart_date",
        },
    ))

    # --- 5..8 Команды 2×2 ----------------------------------------------------
    slices.append(_make_slice(ctx,
        "Стиль игры: xG vs xGA", "bubble_v2", team,
        {
            "entity": "team_name",
            "x": _metric("xG", "SUM", "xg"),
            "y": _metric("xGA", "SUM", "xga"),
            "size": _metric("Очки", "SUM", "points"),
            "row_limit": 100,
            "max_bubble_size": "25",
            "x_axis_label": "xG (атака)",
            "y_axis_label": "xGA (оборона, меньше = лучше)",
            "x_axis_format": ".1f",
            "y_axis_format": ".1f",
            "show_legend": False,
        },
    ))

    slices.append(_make_slice(ctx,
        "Реализация: голы − xG", "dist_bar", team,
        {
            "metrics": [_metric(
                "Голы − xG", "SUM", None,
                sql="SUM(goals_fbref) - SUM(xg)",
            )],
            "groupby": ["team_name"],
            "adhoc_filters": [_sql_where("xg IS NOT NULL")],
            "row_limit": 25,
            **_bar,
            "y_axis_format": "+.1f",
        },
    ))

    slices.append(_make_slice(ctx,
        "PPDA: интенсивность прессинга", "dist_bar", team,
        {
            "metrics": [_metric("PPDA", "AVG", "ppda")],
            "groupby": ["team_name"],
            "adhoc_filters": [_sql_where("ppda IS NOT NULL")],
            "row_limit": 25,
            **{**_bar, "order_desc": False},  # меньше = агрессивнее, слева
            "y_axis_format": ".1f",
        },
    ))

    slices.append(_make_slice(ctx,
        "Стоимость состава vs очки", "bubble_v2", team,
        {
            "entity": "team_name",
            "x": _metric("Стоимость состава (€)", "SUM", "squad_market_value_eur"),
            "y": _metric("Очки", "SUM", "points"),
            "size": _metric("Голы", "SUM", "goals_fbref"),
            "adhoc_filters": [_sql_where("squad_market_value_eur IS NOT NULL")],
            "row_limit": 100,
            "max_bubble_size": "25",
            "x_axis_label": "Стоимость состава (€, Transfermarkt)",
            "y_axis_label": "Очки",
            "x_axis_format": ".2s",
            "show_legend": False,
        },
    ))

    # --- 9 Elo ---------------------------------------------------------------
    slices.append(_make_slice(ctx,
        "Elo-рейтинг по времени", "echarts_timeseries_line", elo,
        {
            "time_range": "No filter",
            "x_axis": "elo_date",
            "time_grain_sqla": "P1W",
            "metrics": [_metric("Elo", "AVG", "elo")],
            "groupby": ["team_name"],
            "row_limit": 50000,
            "show_legend": True,
            "rich_tooltip": True,
            "truncateYAxis": True,  # без этого ось от 0 сжимает линии в полосу
            "x_axis_title": "Дата",
            "y_axis_title": "Elo (ClubElo)",
            "y_axis_format": ".0f",
        },
    ))

    # --- 10..15 Игроки: топ-15 ----------------------------------------------
    slices.append(_make_slice(ctx,
        "Топ-15 бомбардиров", "dist_bar", player,
        {
            "metrics": [_metric("Голы", "SUM", "goals")],
            "groupby": ["player_name"],
            "row_limit": 15,
            **_bar,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 по xG", "dist_bar", player,
        {
            "metrics": [_metric("xG", "SUM", "expected_goals")],
            "groupby": ["player_name"],
            "adhoc_filters": [_sql_where("expected_goals IS NOT NULL")],
            "row_limit": 15,
            **_bar,
            "y_axis_format": ".1f",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 ассистентов", "dist_bar", player,
        {
            "metrics": [_metric("Ассисты", "SUM", "assists")],
            "groupby": ["player_name"],
            "row_limit": 15,
            **_bar,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 по рейтингу SofaScore", "dist_bar", player,
        {
            "metrics": [_metric("Рейтинг", "AVG", "rating_sofascore")],
            "groupby": ["player_name"],
            "adhoc_filters": [_sql_where(
                "minutes >= 450 AND rating_sofascore IS NOT NULL"
            )],
            "row_limit": 15,
            **_bar,
            "y_axis_format": ".2f",
        },
    ))

    # MAX, не SUM: при выборе нескольких сезонов SUM суммировал бы оценки
    # игрока по сезонам.
    slices.append(_make_slice(ctx,
        "Топ-15 по трансферной стоимости (€)", "dist_bar", player,
        {
            "metrics": [_metric("Стоимость (€)", "MAX", "market_value_eur")],
            "groupby": ["player_name"],
            "adhoc_filters": [_sql_where("market_value_eur IS NOT NULL")],
            "row_limit": 15,
            **_bar,
            "y_axis_format": ".2s",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 по зарплате (€/год)", "dist_bar", player,
        {
            # IS NOT NULL обязателен: на сезонах без Capology (всё, кроме 2526)
            # метрика из сплошных NULL роняет legacy dist_bar 500-й ошибкой;
            # с фильтром пустой сезон даёт чистое «No results».
            "metrics": [_metric("Зарплата (€/год)", "MAX", "annual_gross_eur")],
            "groupby": ["player_name"],
            "adhoc_filters": [_sql_where("annual_gross_eur IS NOT NULL")],
            "row_limit": 15,
            **_bar,
            "y_axis_format": ".2s",
        },
    ))

    # --- 16..19 Аналитика игроков ---------------------------------------------
    slices.append(_make_slice(ctx,
        "xG vs голы (игроки)", "bubble_v2", player,
        {
            "entity": "player_name",
            "series": "position_primary",
            "x": _metric("xG", "SUM", "expected_goals"),
            "y": _metric("Голы", "SUM", "goals"),
            "size": _metric("Минуты", "SUM", "minutes"),
            "adhoc_filters": [_sql_where("minutes >= 270")],
            "row_limit": 1000,
            "max_bubble_size": "25",
            "x_axis_label": "xG",
            "y_axis_label": "Голы",
            "x_axis_format": ".1f",
            "y_axis_format": "SMART_NUMBER",
            "show_legend": True,
        },
    ))

    slices.append(_make_slice(ctx,
        "Стоимость vs гол+пас", "bubble_v2", player,
        {
            "entity": "player_name",
            "series": "position_primary",
            "x": _metric("Стоимость (€)", "MAX", "market_value_eur"),
            "y": _metric("Гол+пас", "SUM", None, sql="SUM(goals + assists)"),
            "size": _metric("Минуты", "SUM", "minutes"),
            "adhoc_filters": [_sql_where(
                "minutes >= 270 AND market_value_eur IS NOT NULL"
            )],
            "row_limit": 1000,
            "max_bubble_size": "25",
            "x_axis_label": "Трансферная стоимость (€)",
            "y_axis_label": "Гол+пас",
            "x_axis_format": ".2s",
            "y_axis_format": "SMART_NUMBER",
            "show_legend": True,
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 по big chances created", "dist_bar", player,
        {
            "metrics": [_metric("Big chances created", "SUM", "big_chances_created")],
            "groupby": ["player_name"],
            "adhoc_filters": [_sql_where("big_chances_created IS NOT NULL")],
            "row_limit": 15,
            **_bar,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Финишеры: голы − xG", "dist_bar", player,
        {
            "metrics": [_metric(
                "Голы − xG", "SUM", None,
                sql="SUM(goals) - SUM(expected_goals)",
            )],
            "groupby": ["player_name"],
            "adhoc_filters": [_sql_where(
                "minutes >= 450 AND expected_goals IS NOT NULL"
            )],
            "row_limit": 15,
            **_bar,
            "y_axis_format": "+.1f",
        },
    ))

    # --- 20 Сводная таблица ---------------------------------------------------
    slices.append(_make_slice(ctx,
        "Сводная таблица игроков", "table", player,
        {
            "query_mode": "raw",
            "all_columns": [
                "player_name", "team_name", "primary_position",
                "matches", "minutes", "goals", "assists",
                "expected_goals", "expected_assists", "rating_sofascore",
                "total_duels_won_pct", "take_on_pct",
                "market_value_eur", "annual_gross_eur",
                "contract_status", "contract_until",
            ],
            "order_by_cols": ['["minutes", false]'],
            "row_limit": 1000,
            "include_search": True,
            "show_cell_bars": True,
            "column_config": {
                "expected_goals": {"d3NumberFormat": ".1f"},
                "expected_assists": {"d3NumberFormat": ".1f"},
                "rating_sofascore": {"d3NumberFormat": ".2f"},
                "total_duels_won_pct": {"d3NumberFormat": ".1f"},
                "take_on_pct": {"d3NumberFormat": ".1f"},
                "market_value_eur": {"d3NumberFormat": ".2s"},
                "annual_gross_eur": {"d3NumberFormat": ".2s"},
                "contract_until": {"d3TimeFormat": "%Y-%m-%d"},
            },
            "table_timestamp_format": "%Y-%m-%d",
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
        "# Обзор лиги + игроки\n\n"
        "Мультилиговый обзор: таблица, командные метрики, Elo и игроки. "
        "Источники: FBref · Understat (xG, xPTS, PPDA) · SofaScore (рейтинги, дуэли) · "
        "FotMob · Transfermarkt (стоимость, контракты) · Capology (зарплаты) · ClubElo.",
        height=10,
    ))
    children.append(_row([_chart(slices[i], 3, height=25) for i in range(0, 4)]))

    children.append(_markdown(
        "## Таблица лиги\n"
        "«Сверх-очки» = очки − xPTS (Understat): плюс — команда набирает больше, "
        "чем заслуживает по качеству моментов (везёт), минус — недобирает.",
        height=4,
    ))
    children.append(_row([_chart(slices[4], 12, height=70)]))

    children.append(_markdown("## Команды", height=3))
    children.append(_row([_chart(slices[5], 6, height=55), _chart(slices[6], 6, height=55)]))
    children.append(_row([_chart(slices[7], 6, height=55), _chart(slices[8], 6, height=55)]))

    children.append(_markdown(
        "## Динамика силы — Elo\n"
        "Недельное среднее ClubElo в границах выбранного сезона.",
        height=3,
    ))
    children.append(_row([_chart(slices[9], 12, height=60)]))

    children.append(_markdown(
        "## Игроки — топы\n"
        "⚠️ Зарплаты (Capology) есть только для сезона 2025/26 — "
        "на других сезонах бар зарплат и колонки зарплат пусты.",
        height=4,
    ))
    children.append(_row([_chart(slices[i], 4, height=50) for i in range(10, 13)]))
    children.append(_row([_chart(slices[i], 4, height=50) for i in range(13, 16)]))

    children.append(_markdown(
        "## Аналитика игроков\n"
        "Scatter'ы — минимум 270 сыгранных минут; рейтинг и «финишеры» — минимум 450.",
        height=3,
    ))
    children.append(_row([_chart(slices[16], 6, height=60), _chart(slices[17], 6, height=60)]))
    children.append(_row([_chart(slices[18], 6, height=50), _chart(slices[19], 6, height=50)]))

    children.append(_row([_chart(slices[20], 12, height=90)]))

    pos[grid_id]["children"] = children
    return pos


# ---------------------------------------------------------------------------
# Native filters
# ---------------------------------------------------------------------------
def _resolve_current_season(database: Any) -> str:
    """Сезон по умолчанию — dim_season.is_current; fallback на константу."""
    try:
        df = database.get_df(
            "SELECT season FROM iceberg.gold.dim_season WHERE is_current"
        )
        if not df.empty:
            return str(df["season"].iloc[0])
        log.warning("dim_season.is_current is empty; falling back to %s",
                    FALLBACK_SEASON)
    except Exception as exc:  # noqa: BLE001
        log.warning("failed to resolve current season (%s); falling back to %s",
                    exc, FALLBACK_SEASON)
    return FALLBACK_SEASON


def _build_native_filters(
    slices: list[Any],
    vds: dict[str, Any],
    current_season: str,
) -> list[dict[str, Any]]:
    """Пять фильтров: Лига → Сезон → Команда → (Позиция) → Игрок.

    Лига/Сезон/Команда применяются ко всем чартам (колонки league/season/
    team_name есть во всех трёх виртуалках). Позиция/Игрок — только к чартам
    на v_lo_player_season: scope собираем по datasource_id (а не по индексам —
    перестановка слайсов не сломает scope), иначе team/elo-чарты падали бы
    500 на несуществующей колонке.
    """
    team_ds_id = vds["v_lo_team_season"].id
    player_ds_id = vds["v_lo_player_season"].id

    all_chart_ids = [slc.id for slc in slices]
    player_chart_ids = [
        slc.id for slc in slices if slc.datasource_id == player_ds_id
    ]

    def _filter(
        fid: str, name: str, column: str, dataset_id: int, multiple: bool,
        target_chart_ids: list[int],
        cascade_parent_ids: list[str] | None = None,
        required: bool = False,
        default_value: str | None = None,
    ) -> dict[str, Any]:
        default_data_mask: dict[str, Any] = {"filterState": {}, "extraFormData": {}}
        if default_value is not None:
            default_data_mask = {
                "extraFormData": {
                    "filters": [{"col": column, "op": "IN", "val": [default_value]}]
                },
                "filterState": {"value": [default_value], "label": default_value},
            }
        return {
            "id": fid,
            "name": name,
            "filterType": "filter_select",
            "targets": [{"column": {"name": column}, "datasetId": dataset_id}],
            "cascadeParentIds": cascade_parent_ids or [],
            "controlValues": {
                "multiSelect": multiple,
                "enableEmptyFilter": required,
                "defaultToFirstItem": False,
                "inverseSelection": False,
            },
            "defaultDataMask": default_data_mask,
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": [
                    cid for cid in all_chart_ids if cid not in target_chart_ids
                ],
            },
            "type": "NATIVE_FILTER",
            "description": "",
            "chartsInScope": target_chart_ids,
            "tabsInScope": [],
        }

    f_league = "NATIVE_FILTER-lo-league"
    f_season = "NATIVE_FILTER-lo-season"
    f_team = "NATIVE_FILTER-lo-team"
    f_position = "NATIVE_FILTER-lo-position"

    return [
        _filter(f_league, "Лига", "league", team_ds_id,
                multiple=False, target_chart_ids=all_chart_ids),
        _filter(f_season, "Сезон", "season", team_ds_id,
                multiple=False, target_chart_ids=all_chart_ids,
                cascade_parent_ids=[f_league],
                required=True, default_value=current_season),
        _filter(f_team, "Команда", "team_name", team_ds_id,
                multiple=True, target_chart_ids=all_chart_ids,
                cascade_parent_ids=[f_league, f_season]),
        _filter(f_position, "Позиция", "position_primary",
                vds["v_lo_player_season"].id,
                multiple=True, target_chart_ids=player_chart_ids),
        _filter("NATIVE_FILTER-lo-player", "Игрок", "player_name",
                vds["v_lo_player_season"].id,
                multiple=True, target_chart_ids=player_chart_ids,
                cascade_parent_ids=[f_league, f_season, f_team, f_position]),
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

        vds = _build_virtual_datasets(ctx, database)
        slices = _build_slices(ctx, vds)
        position_json = _build_position_json(slices)
        current_season = _resolve_current_season(database)
        native_filters = _build_native_filters(slices, vds, current_season)

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
        format="[dashboard.league_overview] %(levelname)s %(message)s",
    )
    create_dashboard()
