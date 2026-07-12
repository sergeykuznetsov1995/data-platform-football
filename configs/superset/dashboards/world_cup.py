#!/usr/bin/env python
# =============================================================================
# Superset dashboard: "Чемпионаты мира"
# =============================================================================
# Тематический дашборд INT-World Cup на шести вкладках (TABS в position_json),
# макет «C — гибрид» из WC1 (docs/research/WC1_wc_dashboard_viz_audit.md):
#   1. Паспорт турнира — 2 KPI-ряда, голы по стадиям, ритм по дням,
#                        рекорды турнира, стадионы (город/страна)
#   2. Групповой этап  — таблицы 12 групп, очки vs разница, лучшие третьи
#   3. Плей-офф        — сетка (таблица), серии пенальти, голы по минутам/типам
#   4. Сборные         — стиль (владение×xG), xG vs xGA, реализация, дисциплина
#   5. Игроки          — гол+пас, xG vs голы, возраст×вклад, рейтинг, радар
#   6. Сравнение ЧМ    — «эпохи»: голы/матч, посещаемость, пенальти, формат,
#                        чемпионы, матрица прогресса, пороги выхода из группы
#
# «Контракт деградации» (фильтр «ЧМ (год)» — MULTI-select):
#   * bubble-чарты — entity = team_label/player_label («Бразилия 2022»), чтобы
#     одна сборная на двух ЧМ не слипалась в точку;
#   * стадийные/минутные бары — series = season (grouped bars по турнирам);
#   * таблицы — колонка «ЧМ» первой;
#   * оси «внутри турнира» относительные (stage_label/minute_bucket), а не
#     календарные — годы разных ЧМ ложатся на общую X-сетку ECharts без дыр.
# Вкладка 6 работает ВНЕ фильтра года (все турниры сразу) и построена только
# на T1-метриках (счёт/стадии/посещаемость из dim_match) — заполняется по мере
# бэкфила исторических ЧМ (S-эпохи: только FBref schedule).
#
# Лига зашита ('INT-World Cup') — дашборд тематический; сезон ('2026', '2022',
# …) НЕ зашит: это и есть фильтр «ЧМ (год)». Все JOIN фактов к dim_match несут
# league/season (отсекают FK-сирот fct_lineup/fct_match_timeline — WC1
# Находка 2). На спарсовые метрики (xG, рейтинг) — adhoc IS NOT NULL; ratio-
# и per-90-метрики гейтятся по минутам (короткий турнир: порог 90/180 минут).
#
# All `superset.*` imports happen INSIDE `create_dashboard()` because the
# Superset model layer touches Flask globals at import time and demands an
# active `app.app_context()`.
# =============================================================================
from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger("dashboard.world_cup")

DASHBOARD_TITLE = "Чемпионаты мира"
DASHBOARD_SLUG = "world-cup"
DATABASE_NAME = "trino_iceberg"
FALLBACK_YEAR = "2026"       # если dim_match недоступен при импорте


# ---------------------------------------------------------------------------
# Context wrapper (как в league_overview.py)
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
# Helpers (как в league_overview.py)
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
        # Протухший query_context прячет обновлённые params (см. league_overview).
        existing.query_context = None
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


def _ru_plural(expr: str, one: str, few: str, many: str) -> str:
    """SQL-склонение существительного после числа: 1 гол / 3 гола / 8 голов."""
    return (f"CASE WHEN {expr} % 100 BETWEEN 11 AND 14 THEN '{many}' "
            f"WHEN {expr} % 10 = 1 THEN '{one}' "
            f"WHEN {expr} % 10 BETWEEN 2 AND 4 THEN '{few}' "
            f"ELSE '{many}' END")


# Порядок и подписи стадий: FBref `round` → сортируемый префикс. ELSE-ветка
# оставляет сырые строки исторических форматов ('First round', 'Final round'…)
# с order=0 — чарты по стадиям их не теряют, а «эпохи» опираются на is_knockout.
_STAGE_ORDER_SQL = """CASE {col}
    WHEN 'Group stage' THEN 1
    WHEN 'Round of 32' THEN 2
    WHEN 'Round of 16' THEN 3
    WHEN 'Quarter-finals' THEN 4
    WHEN 'Semi-finals' THEN 5
    WHEN 'Third-place match' THEN 6
    WHEN 'Final' THEN 7
    ELSE 0
  END"""

_STAGE_LABEL_SQL = """CASE {col}
    WHEN 'Group stage' THEN '1. Группы'
    WHEN 'Round of 32' THEN '2. 1/16 финала'
    WHEN 'Round of 16' THEN '3. 1/8 финала'
    WHEN 'Quarter-finals' THEN '4. 1/4 финала'
    WHEN 'Semi-finals' THEN '5. Полуфиналы'
    WHEN 'Third-place match' THEN '6. Матч за 3-е'
    WHEN 'Final' THEN '7. Финал'
    ELSE COALESCE({col}, '—')
  END"""


# ---------------------------------------------------------------------------
# Virtual datasets — league зашита, season = фильтр «ЧМ (год)»
# ---------------------------------------------------------------------------
def _build_virtual_datasets(ctx: _Ctx, database: Any) -> dict[str, Any]:
    schema = "gold"

    stage_order = _STAGE_ORDER_SQL.format(col="m.stage")
    stage_label = _STAGE_LABEL_SQL.format(col="m.stage")

    # ESPN шлёт арены ЧМ с суффиксом « (Neutral Site)» и тремя устаревшими/
    # спонсорскими именами — из-за этого enrichment dim_venue не сматчился
    # (city/country/capacity = NULL, venue_source='orphan'). Канонизируем имя
    # здесь: оно же — ключ моста к silver.sofascore_venue (город/страна).
    venue_canon = """CASE regexp_replace(dv.venue_name, ' \\(Neutral Site\\)$', '')
    WHEN 'Reliant Stadium' THEN 'NRG Stadium'
    WHEN 'Estadio Banorte' THEN 'Estadio Azteca'
    WHEN 'GEHA Field at Arrowhead Stadium' THEN 'Arrowhead Stadium'
    WHEN 'BC Place Stadium' THEN 'BC Place'
    ELSE regexp_replace(dv.venue_name, ' \\(Neutral Site\\)$', '')
  END"""

    # Грейн — матч. Хребет дашборда: счёт, стадии, пенальти, посещаемость.
    # Будущие матчи (полуфиналы/финал до розыгрыша) несут NULL-счёт —
    # is_completed отсекает их в метриках. Карточки/автоголы/пенальти —
    # матчевые агрегаты фактов для KPI паспорта; у «эпох» (только расписание
    # FBref) они NULL — AVG/SUM их честно пропускают, а не рисуют нули.
    v_match = f"""\
WITH m AS (
  SELECT
    dm.match_id, dm.season, dm.stage, dm.is_knockout, dm.match_date,
    dm.attendance, dm.home_score, dm.away_score,
    dm.home_penalty, dm.away_penalty, dm.is_completed,
    dm.venue_id, dm.referee_id,
    COALESCE(th.team_name, dm.home_team_id) AS home_team,
    COALESCE(ta.team_name, dm.away_team_id) AS away_team
  FROM iceberg.gold.dim_match dm
  LEFT JOIN iceberg.gold.dim_team th ON th.team_id = dm.home_team_id
  LEFT JOIN iceberg.gold.dim_team ta ON ta.team_id = dm.away_team_id
  WHERE dm.league = 'INT-World Cup'
),
sv AS (
  SELECT season, stadium, MAX(city) AS city, MAX(country) AS country
  FROM iceberg.silver.sofascore_venue
  WHERE league = 'INT-World Cup'
  GROUP BY season, stadium
),
cards AS (
  SELECT match_id, season,
         SUM(yellow_cards) AS yellow_cards,
         SUM(red_cards)    AS red_cards
  FROM iceberg.gold.fct_team_match
  WHERE league = 'INT-World Cup'
  GROUP BY match_id, season
),
og AS (
  SELECT match_id, season, SUM(own_goals) AS own_goals
  FROM iceberg.gold.fct_player_match
  WHERE league = 'INT-World Cup'
  GROUP BY match_id, season
),
pens AS (
  SELECT match_id, season,
         COUNT(*) AS pens_awarded,
         SUM(CASE WHEN result = 'goal' THEN 1 ELSE 0 END) AS pens_scored
  FROM iceberg.gold.fct_shot
  WHERE league = 'INT-World Cup'
    AND situation = 'penalty' AND minute <= 120
  GROUP BY match_id, season
)
SELECT
  m.match_id, m.season, m.stage, m.is_knockout, m.match_date, m.is_completed,
  CASE WHEN m.is_knockout THEN 'Плей-офф' ELSE 'Группы' END AS phase_label,
  {stage_order} AS stage_order,
  {stage_label} AS stage_label,
  date_diff('day',
            MIN(m.match_date) OVER (PARTITION BY m.season),
            m.match_date) + 1 AS tournament_day,
  m.home_team, m.away_team,
  m.home_team || ' — ' || m.away_team AS match_label,
  m.home_score, m.away_score,
  m.home_score + m.away_score AS total_goals,
  m.home_penalty, m.away_penalty,
  CASE WHEN m.home_penalty IS NOT NULL THEN 1 ELSE 0 END AS went_to_pens,
  CASE
    WHEN m.home_score IS NULL THEN '—'
    WHEN m.home_penalty IS NOT NULL THEN
      CAST(m.home_score AS varchar) || ':' || CAST(m.away_score AS varchar)
      || ' (пен. ' || CAST(m.home_penalty AS varchar) || ':'
      || CAST(m.away_penalty AS varchar) || ')'
    ELSE CAST(m.home_score AS varchar) || ':' || CAST(m.away_score AS varchar)
  END AS score_label,
  m.attendance,
  {venue_canon} AS venue_name,
  COALESCE(dv.city, sv.city)       AS city,
  COALESCE(dv.country, sv.country) AS country,
  dv.capacity,
  CASE WHEN dv.capacity > 0
       THEN CAST(m.attendance AS DOUBLE) / dv.capacity END AS fill_pct,
  c.yellow_cards, c.red_cards,
  c.yellow_cards + c.red_cards AS cards_total,
  og.own_goals,
  pens.pens_awarded, pens.pens_scored,
  dr.referee_name
FROM m
LEFT JOIN iceberg.gold.dim_venue   dv ON dv.venue_id   = m.venue_id
LEFT JOIN sv    ON sv.season = m.season AND sv.stadium = {venue_canon}
LEFT JOIN cards c    ON c.match_id    = m.match_id AND c.season    = m.season
LEFT JOIN og         ON og.match_id   = m.match_id AND og.season   = m.season
LEFT JOIN pens       ON pens.match_id = m.match_id AND pens.season = m.season
LEFT JOIN iceberg.gold.dim_referee dr ON dr.referee_id = m.referee_id"""

    v_match_labels = {
        "season": "ЧМ", "stage_label": "Стадия", "stage_order": "Этап №",
        "match_date": "Дата", "match_label": "Матч", "score_label": "Счёт",
        "home_team": "Хозяева", "away_team": "Гости",
        "attendance": "Зрители", "venue_name": "Стадион", "city": "Город",
        "country": "Страна",
        "capacity": "Вместимость", "fill_pct": "Заполняемость",
        "referee_name": "Судья", "total_goals": "Голов",
    }

    # Грейн — (season, group, team). `advanced` = сыграла ли сборная хоть один
    # матч плей-офф этого ЧМ (для порогов выхода из группы).
    v_standings = """\
SELECT
  st.season, st.group_id, st.position,
  COALESCE(dt.team_name, st.team_name_raw) AS team_name,
  COALESCE(dt.team_name, st.team_name_raw) || ' ' || st.season AS team_label,
  st.played, st.wins, st.draws, st.losses,
  st.goals_for, st.goals_against, st.goal_diff, st.points,
  st.points_per_game,
  CASE WHEN ko.team_id IS NOT NULL THEN 1 ELSE 0 END AS advanced
FROM iceberg.gold.fct_standings st
LEFT JOIN iceberg.gold.dim_team dt ON dt.team_id = st.team_id
LEFT JOIN (
  SELECT DISTINCT season, team_id FROM (
    SELECT season, home_team_id AS team_id
    FROM iceberg.gold.dim_match
    WHERE league = 'INT-World Cup' AND is_knockout
    UNION ALL
    SELECT season, away_team_id
    FROM iceberg.gold.dim_match
    WHERE league = 'INT-World Cup' AND is_knockout
  )
) ko ON ko.season = st.season AND ko.team_id = st.team_id
WHERE st.league = 'INT-World Cup'"""

    v_standings_labels = {
        "season": "ЧМ", "group_id": "Группа", "position": "Место",
        "team_name": "Сборная", "played": "И", "wins": "В", "draws": "Н",
        "losses": "П", "goals_for": "ГЗ", "goals_against": "ГП",
        "goal_diff": "РМ", "points": "О", "points_per_game": "О/матч",
        "advanced": "Вышла в плей-офф",
    }

    tm_stage_order = _STAGE_ORDER_SQL.format(col="dm.stage")
    tm_stage_label = _STAGE_LABEL_SQL.format(col="dm.stage")

    # Грейн — (season, team): агрегат fct_team_match по сыгранным матчам.
    # W/D/L считаются от счёта матча (с ОТ; серия пенальти в счёте — ничья),
    # points у knockout-матчей NULL by design → group_points = очки групп.
    # Стоимость состава: последняя рыночная оценка игрока (любой источник,
    # свежая по дате) не позже старта турнира, суммированная по заявке
    # (fct_player_season_stats). Покрытие ЧМ-2026: 895/968 игроков.
    v_team_tournament = f"""\
WITH tm AS (
  SELECT
    t.*,
    {tm_stage_order} AS stage_order,
    {tm_stage_label} AS stage_label
  FROM iceberg.gold.fct_team_match t
  JOIN iceberg.gold.dim_match dm
    ON  dm.match_id = t.match_id
    AND dm.league   = t.league
    AND dm.season   = t.season
  WHERE t.league = 'INT-World Cup' AND t.is_completed
),
mv_asof AS (
  SELECT player_id, season, market_value_eur FROM (
    SELECT ps.player_id, ps.season, mv.market_value_eur,
           ROW_NUMBER() OVER (
             PARTITION BY ps.player_id, ps.season
             ORDER BY mv.valuation_date DESC
           ) AS rn
    FROM (SELECT DISTINCT player_id, season
          FROM iceberg.gold.fct_player_season_stats
          WHERE league = 'INT-World Cup') ps
    JOIN iceberg.gold.dim_season ds ON ds.season = ps.season
    JOIN iceberg.gold.fct_player_market_value mv
      ON  mv.player_id      = ps.player_id
      AND mv.valuation_date <= ds.start_date
  ) WHERE rn = 1
),
squad AS (
  SELECT ps.season, ps.team_id,
         SUM(mv.market_value_eur) AS squad_value_eur,
         COUNT(mv.market_value_eur) AS valued_players
  FROM iceberg.gold.fct_player_season_stats ps
  LEFT JOIN mv_asof mv
    ON mv.player_id = ps.player_id AND mv.season = ps.season
  WHERE ps.league = 'INT-World Cup'
  GROUP BY ps.season, ps.team_id
)
SELECT
  tm.season,
  COALESCE(dt.team_name, tm.team_id) AS team_name,
  COALESCE(dt.team_name, tm.team_id) || ' ' || tm.season AS team_label,
  COUNT(*) AS matches,
  SUM(CASE WHEN tm.goals_for > tm.goals_against THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN tm.goals_for = tm.goals_against THEN 1 ELSE 0 END) AS draws,
  SUM(CASE WHEN tm.goals_for < tm.goals_against THEN 1 ELSE 0 END) AS losses,
  SUM(tm.goals_for) AS goals_for,
  SUM(tm.goals_against) AS goals_against,
  SUM(tm.goals_for) - SUM(tm.goals_against) AS goal_diff,
  SUM(tm.points) AS group_points,
  SUM(tm.xg)  AS xg,
  SUM(tm.xga) AS xga,
  SUM(tm.xgot) AS xgot,
  SUM(tm.xg)  / COUNT(*) AS xg_per_match,
  SUM(tm.xga) / COUNT(*) AS xga_per_match,
  AVG(tm.possession_pct) AS possession_pct,
  SUM(tm.big_chances) AS big_chances,
  SUM(tm.fouls)   AS fouls,
  SUM(tm.corners) AS corners,
  SUM(tm.yellow_cards) AS yellow_cards,
  SUM(tm.red_cards)    AS red_cards,
  AVG(tm.ground_duels_won_pct) AS ground_duels_won_pct,
  AVG(tm.aerial_duels_won_pct) AS aerial_duels_won_pct,
  SUM(tm.shots) AS shots,
  SUM(tm.shots_on_target) AS shots_on_target,
  MAX(tm.stage_order) AS furthest_stage_order,
  MAX_BY(tm.stage_label, tm.stage_order) AS furthest_stage_label,
  MAX(sq.squad_value_eur) AS squad_value_eur,
  MAX(sq.valued_players)  AS valued_players
FROM tm
LEFT JOIN squad sq ON sq.season = tm.season AND sq.team_id = tm.team_id
LEFT JOIN iceberg.gold.dim_team dt ON dt.team_id = tm.team_id
GROUP BY tm.season, COALESCE(dt.team_name, tm.team_id)"""

    v_team_tournament_labels = {
        "season": "ЧМ", "team_name": "Сборная", "matches": "И",
        "wins": "В", "draws": "Н", "losses": "П",
        "goals_for": "ГЗ", "goals_against": "ГП", "goal_diff": "РМ",
        "group_points": "Очки (группа)", "xg": "xG", "xga": "xGA",
        "xgot": "xGOT", "xg_per_match": "xG/матч",
        "xga_per_match": "xGA/матч", "possession_pct": "Владение %",
        "big_chances": "Явные шансы", "fouls": "Фолы", "corners": "Угловые",
        "yellow_cards": "ЖК", "red_cards": "КК",
        "ground_duels_won_pct": "Дуэли низ %",
        "aerial_duels_won_pct": "Дуэли верх %", "shots": "Удары",
        "shots_on_target": "В створ", "furthest_stage_label": "Дошла до",
        "squad_value_eur": "Стоимость состава (€)",
        "valued_players": "Игроков с оценкой",
    }

    # Грейн — (season, player): fct_player_season_stats уже турнирный.
    # per-90 гейтится 180 минутами (короткий турнир, лиговый порог 270 жёсток).
    # Рейтинг: fct_player_season_stats.rating_sofascore для ЧМ пуст (дефект
    # gold-агрегата, 968/0 — заметка в WC1) — среднее берём напрямую из
    # fct_match_rating (грейн игрок-матч, для ЧМ заполнен).
    # ВРЕМЕННЫЙ мост рейтинга (до пересборки xref): fct_match_rating для ЧМ
    # несёт СЫРЫЕ ss_-id (sofascore-ветки нет в xref_player — для ЧМ не
    # скрейпился её якорь sofascore_player_season_stats). Мостим через
    # silver.sofascore_player_profile.canonical_id — профили от КЛУБНЫХ
    # скрейпов уже канонизированы (покрытие 645/1248 игроков ЧМ, топы
    # покрыты; superset НЕ имеет прав на bronze — только silver/gold).
    # Убрать мост после каноничной пересборки xref/gold для ЧМ.
    v_player_tournament = """\
WITH prof AS (
  SELECT player_id, MAX(canonical_id) AS canonical_id
  FROM iceberg.silver.sofascore_player_profile
  WHERE canonical_id IS NOT NULL
  GROUP BY player_id
),
mr AS (
  SELECT prof.canonical_id AS player_id, r.season,
         AVG(r.rating) AS rating, COUNT(*) AS rated_matches
  FROM iceberg.gold.fct_match_rating r
  JOIN prof
    ON 'ss_' || CAST(prof.player_id AS varchar) = r.player_id
  WHERE r.league = 'INT-World Cup'
  GROUP BY prof.canonical_id, r.season
),
pmv AS (
  SELECT player_id, season, market_value_eur FROM (
    SELECT k.player_id, k.season, mv.market_value_eur,
           ROW_NUMBER() OVER (
             PARTITION BY k.player_id, k.season
             ORDER BY mv.valuation_date DESC
           ) AS rn
    FROM (SELECT DISTINCT player_id, season
          FROM iceberg.gold.fct_player_season_stats
          WHERE league = 'INT-World Cup') k
    JOIN iceberg.gold.dim_season ds ON ds.season = k.season
    JOIN iceberg.gold.fct_player_market_value mv
      ON  mv.player_id      = k.player_id
      AND mv.valuation_date <= ds.start_date
  ) WHERE rn = 1
),
pm AS (
  -- key_passes/dribbles в fct_player_season_stats для ЧМ пусты (0/968 — их
  -- источник-агрегат sofascore не скрейпился), но fct_player_match несёт их
  -- на каноничных id (whoscored, 2892/3028) — суммируем сами.
  SELECT player_id, season,
         SUM(key_passes)   AS key_passes,
         SUM(dribbles_won) AS dribbles_won
  FROM iceberg.gold.fct_player_match
  WHERE league = 'INT-World Cup'
  GROUP BY player_id, season
)
SELECT
  ps.season,
  dp.player_name,
  dp.player_name || ' ' || ps.season AS player_label,
  COALESCE(dt.team_name, ps.team_id) AS team_name,
  dp.nationality,
  CASE
    WHEN dp.primary_position = 'GK' THEN 'GK'
    WHEN dp.primary_position IN ('DF','CB','RB','LB','LWB','RWB','WB') THEN 'DF'
    WHEN dp.primary_position IN ('MF','CM','RM','LM','CAM','CDM','DM','AM') THEN 'MF'
    WHEN dp.primary_position IN ('FW','ST','RW','LW','CF','SS') THEN 'FW'
  END AS position_group,
  FLOOR(date_diff('day', dp.dob, ds.start_date) / 365.25) AS age_at_start,
  ps.matches, ps.minutes, ps.starts,
  ps.goals, ps.assists,
  COALESCE(ps.goals, 0) + COALESCE(ps.assists, 0) AS goal_contrib,
  ps.expected_goals, ps.expected_assists,
  ps.shots, ps.shots_on_target,
  COALESCE(ps.key_passes, pm.key_passes) AS key_passes,
  ps.big_chances_created,
  COALESCE(ps.successful_dribbles, pm.dribbles_won) AS successful_dribbles,
  ps.touches_in_box,
  ps.yellow_cards, ps.red_cards,
  COALESCE(ps.rating_sofascore, mr.rating) AS rating_sofascore,
  pmv.market_value_eur,
  CASE WHEN ps.minutes >= 180
       THEN ps.goals * 90.0 / ps.minutes END AS goals_p90,
  CASE WHEN ps.minutes >= 180
       THEN ps.expected_goals * 90.0 / ps.minutes END AS xg_p90,
  CASE WHEN ps.minutes >= 180
       THEN ps.shots * 90.0 / ps.minutes END AS shots_p90,
  CASE WHEN ps.minutes >= 180
       THEN COALESCE(ps.key_passes, pm.key_passes) * 90.0 / ps.minutes
       END AS key_passes_p90,
  CASE WHEN ps.minutes >= 180
       THEN COALESCE(ps.successful_dribbles, pm.dribbles_won) * 90.0
            / ps.minutes
       END AS dribbles_p90
FROM iceberg.gold.fct_player_season_stats ps
LEFT JOIN mr  ON mr.player_id  = ps.player_id AND mr.season  = ps.season
LEFT JOIN pmv ON pmv.player_id = ps.player_id AND pmv.season = ps.season
LEFT JOIN pm  ON pm.player_id  = ps.player_id AND pm.season  = ps.season
LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = ps.player_id
LEFT JOIN iceberg.gold.dim_team   dt ON dt.team_id   = ps.team_id
LEFT JOIN iceberg.gold.dim_season ds ON ds.season    = ps.season
WHERE ps.league = 'INT-World Cup'"""

    v_player_tournament_labels = {
        "season": "ЧМ", "player_name": "Игрок", "team_name": "Сборная",
        "nationality": "Гражданство", "position_group": "Позиция",
        "age_at_start": "Возраст", "matches": "И", "minutes": "Минуты",
        "starts": "В старте", "goals": "Голы", "assists": "Ассисты",
        "goal_contrib": "Г+А", "expected_goals": "xG",
        "expected_assists": "xA", "shots": "Удары",
        "shots_on_target": "В створ", "key_passes": "Ключ. передачи",
        "big_chances_created": "Созданные шансы",
        "successful_dribbles": "Обводки", "touches_in_box": "Касания в штрафной",
        "yellow_cards": "ЖК", "red_cards": "КК",
        "rating_sofascore": "Рейтинг",
        "market_value_eur": "Стоимость (€)",
    }

    shot_stage_label = _STAGE_LABEL_SQL.format(col="dm.stage")

    # Грейн — удар. JOIN dim_match с league/season отсекает FK-сирот.
    # Имена: fct_shot для ЧМ несёт сырые ss_-id (см. мост рейтинга) —
    # фолбэк на имя из silver-профиля sofascore.
    v_shot = f"""\
WITH prof AS (
  SELECT player_id, MAX(player_name) AS player_name
  FROM iceberg.silver.sofascore_player_profile
  WHERE player_name IS NOT NULL
  GROUP BY player_id
)
SELECT
  s.shot_id, s.season, s.match_id, s.minute,
  CASE
    WHEN s.minute <= 15 THEN '1. 0–15'
    WHEN s.minute <= 30 THEN '2. 16–30'
    WHEN s.minute <= 45 THEN '3. 31–45'
    WHEN s.minute <= 60 THEN '4. 46–60'
    WHEN s.minute <= 75 THEN '5. 61–75'
    WHEN s.minute <= 90 THEN '6. 76–90'
    ELSE '7. 91+ (ОТ)'
  END AS minute_bucket,
  s.x, s.y, s.xg, s.psxg, s.body_part, s.situation,
  CASE s.situation
    WHEN 'open_play'  THEN 'С игры'
    WHEN 'corner'     THEN 'После углового'
    WHEN 'free_kick'  THEN 'Прямой штрафной'
    WHEN 'set_piece'  THEN 'Со стандарта'
    WHEN 'penalty'    THEN 'Пенальти'
    ELSE 'Прочее'
  END AS situation_label,
  -- Попытки послематчевых серий пенальти: SofaScore пишет их в shotmap с
  -- minute 121+ и NULL situation (ЧМ-2026: 40 попыток / 25 голов в 4 матчах).
  -- Они НЕ игровые голы — чарты минут/типов их исключают.
  CASE WHEN s.minute >= 121 THEN 1 ELSE 0 END AS is_shootout_attempt,
  CASE WHEN s.is_goal THEN 1 ELSE 0 END AS is_goal_int,
  s.is_goal, s.result,
  CASE s.result
    WHEN 'goal'       THEN 'Гол'
    WHEN 'saved'      THEN 'Сейв'
    WHEN 'blocked'    THEN 'Заблокирован'
    WHEN 'off_target' THEN 'Мимо'
    WHEN 'post'       THEN 'Каркас'
    ELSE COALESCE(s.result, '—')
  END AS result_label,
  {shot_stage_label} AS stage_label,
  dm.is_knockout,
  COALESCE(th.team_name, dm.home_team_id) || ' — '
    || COALESCE(ta.team_name, dm.away_team_id) AS match_label,
  COALESCE(dt.team_name, s.team_id) AS team_name,
  COALESCE(dp.player_name, prof.player_name, s.player_id) AS player_name
FROM iceberg.gold.fct_shot s
JOIN iceberg.gold.dim_match dm
  ON  dm.match_id = s.match_id
  AND dm.league   = s.league
  AND dm.season   = s.season
LEFT JOIN iceberg.gold.dim_team   dt ON dt.team_id   = s.team_id
LEFT JOIN iceberg.gold.dim_team   th ON th.team_id   = dm.home_team_id
LEFT JOIN iceberg.gold.dim_team   ta ON ta.team_id   = dm.away_team_id
LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = s.player_id
LEFT JOIN prof
  ON 'ss_' || CAST(prof.player_id AS varchar) = s.player_id
WHERE s.league = 'INT-World Cup'"""

    v_shot_labels = {
        "season": "ЧМ", "minute_bucket": "Минуты", "situation_label": "Тип",
        "team_name": "Сборная", "player_name": "Игрок",
        "stage_label": "Стадия", "match_label": "Матч",
        "result": "Исход (raw)", "result_label": "Исход",
    }

    # Грейн — season (хребет «эпох»). Всё из dim_match — работает для любого
    # ЧМ, у которого есть хотя бы FBref-расписание (ярус T1). Карточки — из
    # fct_team_match (для эпох будет NULL: там только счёт). Хозяйка — мода
    # стран стадионов.
    v_tournament = """\
WITH m AS (
  SELECT * FROM iceberg.gold.dim_match WHERE league = 'INT-World Cup'
),
played AS (
  SELECT * FROM m WHERE is_completed
),
teams AS (
  SELECT season, COUNT(DISTINCT team_id) AS teams
  FROM (
    SELECT season, home_team_id AS team_id FROM m
    UNION ALL
    SELECT season, away_team_id FROM m
  )
  GROUP BY season
),
host AS (
  SELECT season, country AS host_country,
         ROW_NUMBER() OVER (PARTITION BY season ORDER BY cnt DESC) AS rn
  FROM (
    SELECT m.season, dv.country, COUNT(*) AS cnt
    FROM m
    JOIN iceberg.gold.dim_venue dv ON dv.venue_id = m.venue_id
    GROUP BY m.season, dv.country
  )
),
cards AS (
  SELECT season,
         SUM(COALESCE(yellow_cards, 0) + COALESCE(red_cards, 0)) AS cards
  FROM iceberg.gold.fct_team_match
  WHERE league = 'INT-World Cup'
  GROUP BY season
)
SELECT
  p.season,
  COUNT(*) AS matches_played,
  SUM(p.home_score + p.away_score) AS goals,
  CAST(SUM(p.home_score + p.away_score) AS DOUBLE) / COUNT(*) AS goals_per_match,
  MAX(t.teams) AS teams,
  AVG(p.attendance) AS avg_attendance,
  SUM(p.attendance) AS total_attendance,
  SUM(CASE WHEN p.home_penalty IS NOT NULL THEN 1 ELSE 0 END) AS shootouts,
  CAST(SUM(CASE WHEN NOT p.is_knockout
                 AND p.home_score = p.away_score THEN 1 ELSE 0 END) AS DOUBLE)
    / NULLIF(SUM(CASE WHEN NOT p.is_knockout THEN 1 ELSE 0 END), 0)
    AS group_draws_share,
  CAST(MAX(c.cards) AS DOUBLE) / COUNT(*) AS cards_per_match,
  MAX(h.host_country) AS host_country
FROM played p
LEFT JOIN teams t ON t.season = p.season
LEFT JOIN host  h ON h.season = p.season AND h.rn = 1
LEFT JOIN cards c ON c.season = p.season
GROUP BY p.season"""

    v_tournament_labels = {
        "season": "ЧМ", "matches_played": "Матчей", "goals": "Голов",
        "goals_per_match": "Голов/матч", "teams": "Сборных",
        "avg_attendance": "Ср. посещаемость",
        "total_attendance": "Всего зрителей", "shootouts": "Серий пенальти",
        "group_draws_share": "Доля ничьих (группы)",
        "cards_per_match": "Карточек/матч", "host_country": "Хозяйка",
    }

    # Грейн — season: финал, чемпион и его турнирные показатели («паспорт
    # чемпионов»). Спайн — все сезоны dim_match, финал LEFT JOIN'ом: пока финал
    # не сыгран, строка сезона существует с NULL-чемпионом (иначе пустой
    # результат оставляет датасет без колонок — fetch_metadata Superset не
    # выводит схему из 0 строк).
    v_final = """\
WITH m AS (
  SELECT * FROM iceberg.gold.dim_match WHERE league = 'INT-World Cup'
),
seasons AS (
  SELECT DISTINCT season FROM m
),
fin AS (
  SELECT
    season, match_date, home_team_id, away_team_id,
    home_score, away_score, home_penalty, away_penalty,
    CASE
      WHEN home_penalty IS NOT NULL AND home_penalty > away_penalty
        THEN home_team_id
      WHEN home_penalty IS NOT NULL THEN away_team_id
      WHEN home_score > away_score THEN home_team_id
      ELSE away_team_id
    END AS champion_id,
    CASE
      WHEN home_penalty IS NOT NULL AND home_penalty > away_penalty
        THEN away_team_id
      WHEN home_penalty IS NOT NULL THEN home_team_id
      WHEN home_score > away_score THEN away_team_id
      ELSE home_team_id
    END AS runner_up_id,
    CASE WHEN home_penalty IS NOT NULL THEN 1 ELSE 0 END AS went_to_pens,
    CASE
      WHEN home_penalty IS NOT NULL THEN
        CAST(home_score AS varchar) || ':' || CAST(away_score AS varchar)
        || ' (пен. ' || CAST(home_penalty AS varchar) || ':'
        || CAST(away_penalty AS varchar) || ')'
      ELSE CAST(home_score AS varchar) || ':' || CAST(away_score AS varchar)
    END AS final_score
  FROM m
  WHERE stage = 'Final' AND is_completed
),
team_matches AS (
  SELECT season, home_team_id AS team_id,
         home_score AS gf, away_score AS ga
  FROM m WHERE is_completed
  UNION ALL
  SELECT season, away_team_id, away_score, home_score
  FROM m WHERE is_completed
),
champ_stats AS (
  SELECT
    f.season,
    COUNT(*) AS champ_matches,
    CAST(SUM(t.gf) AS DOUBLE) / COUNT(*) AS champ_goals_per_match,
    SUM(t.gf) - SUM(t.ga) AS champ_goal_diff,
    CAST(SUM(CASE WHEN t.gf > t.ga THEN 1 ELSE 0 END) AS DOUBLE)
      / COUNT(*) AS champ_win_share
  FROM fin f
  JOIN team_matches t
    ON t.season = f.season AND t.team_id = f.champion_id
  GROUP BY f.season
)
SELECT
  s.season,
  f.match_date AS final_date,
  COALESCE(ct.team_name, f.champion_id)  AS champion,
  COALESCE(rt.team_name, f.runner_up_id) AS runner_up,
  f.final_score,
  f.went_to_pens,
  cs.champ_matches,
  cs.champ_goals_per_match,
  cs.champ_goal_diff,
  cs.champ_win_share
FROM seasons s
LEFT JOIN fin f ON f.season = s.season
LEFT JOIN champ_stats cs ON cs.season = s.season
LEFT JOIN iceberg.gold.dim_team ct ON ct.team_id = f.champion_id
LEFT JOIN iceberg.gold.dim_team rt ON rt.team_id = f.runner_up_id"""

    v_final_labels = {
        "season": "ЧМ", "final_date": "Дата финала", "champion": "Чемпион",
        "runner_up": "Финалист", "final_score": "Счёт финала",
        "went_to_pens": "По пенальти", "champ_matches": "Матчей чемпиона",
        "champ_goals_per_match": "Голов/матч чемпиона",
        "champ_goal_diff": "РМ чемпиона", "champ_win_share": "Доля побед",
    }

    prog_stage_order = _STAGE_ORDER_SQL.format(col="x.stage")

    # Грейн — (season, team): максимальная достигнутая стадия (по участию в
    # матчах dim_match — работает для эпох без standings/fct_team_match);
    # победитель финала получает 8 «Чемпион».
    v_team_progress = f"""\
WITH m AS (
  SELECT * FROM iceberg.gold.dim_match
  WHERE league = 'INT-World Cup' AND is_completed
),
champions AS (
  SELECT season,
    CASE
      WHEN home_penalty IS NOT NULL AND home_penalty > away_penalty
        THEN home_team_id
      WHEN home_penalty IS NOT NULL THEN away_team_id
      WHEN home_score > away_score THEN home_team_id
      ELSE away_team_id
    END AS champion_id
  FROM m WHERE stage = 'Final'
),
participation AS (
  SELECT x.season, x.team_id, MAX({prog_stage_order}) AS max_stage
  FROM (
    SELECT season, home_team_id AS team_id, stage FROM m
    UNION ALL
    SELECT season, away_team_id, stage FROM m
  ) x
  GROUP BY x.season, x.team_id
)
SELECT
  p.season,
  COALESCE(dt.team_name, p.team_id) AS team_name,
  CASE WHEN c.champion_id IS NOT NULL THEN 8 ELSE p.max_stage END
    AS furthest_stage_order,
  CASE
    WHEN c.champion_id IS NOT NULL THEN 'Чемпион'
    WHEN p.max_stage = 7 THEN 'Финал'
    WHEN p.max_stage = 6 THEN 'Матч за 3-е'
    WHEN p.max_stage = 5 THEN 'Полуфинал'
    WHEN p.max_stage = 4 THEN '1/4'
    WHEN p.max_stage = 3 THEN '1/8'
    WHEN p.max_stage = 2 THEN '1/16'
    WHEN p.max_stage = 1 THEN 'Группы'
    ELSE '—'
  END AS furthest_stage_label
FROM participation p
LEFT JOIN champions c
  ON c.season = p.season AND c.champion_id = p.team_id
LEFT JOIN iceberg.gold.dim_team dt ON dt.team_id = p.team_id"""

    v_team_progress_labels = {
        "season": "ЧМ", "team_name": "Сборная",
        "furthest_stage_order": "Стадия №", "furthest_stage_label": "Дошла до",
    }

    # Грейн — (season, record): «визитная карточка» для паспорта. Одна строка
    # на рекорд; ties режутся детерминированно (счёт → дата), хвост уходит в
    # detail («ещё N»). Для эпох строки из фактов (быстрый гол, бомбардир…)
    # просто не появятся — таблица деградирует до счёта/посещаемости.
    v_records = f"""\
WITH m AS (
  SELECT dm.*,
         COALESCE(th.team_name, dm.home_team_id) AS home_team,
         COALESCE(ta.team_name, dm.away_team_id) AS away_team
  FROM iceberg.gold.dim_match dm
  LEFT JOIN iceberg.gold.dim_team th ON th.team_id = dm.home_team_id
  LEFT JOIN iceberg.gold.dim_team ta ON ta.team_id = dm.away_team_id
  WHERE dm.league = 'INT-World Cup' AND dm.is_completed
    AND dm.home_score IS NOT NULL
),
scored AS (
  SELECT season, match_id, match_date, attendance, venue_id,
         home_team || ' ' || CAST(home_score AS varchar) || ':'
           || CAST(away_score AS varchar) || ' ' || away_team AS match_text,
         ABS(home_score - away_score) AS margin,
         home_score + away_score AS total_goals
  FROM m
),
biggest_win AS (
  SELECT season, 1 AS record_order, 'Крупнейшая победа' AS record,
         match_text AS holder,
         '+' || CAST(margin AS varchar) AS value_label,
         date_format(CAST(match_date AS timestamp), '%d.%m.%Y') AS detail
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY season
          ORDER BY margin DESC, total_goals DESC, match_date) AS rn
        FROM scored)
  WHERE rn = 1 AND margin > 0
),
highest_score AS (
  SELECT season, 2, 'Самый результативный матч',
         match_text,
         CAST(total_goals AS varchar) || ' '
           || {_ru_plural("total_goals", "гол", "гола", "голов")},
         date_format(CAST(match_date AS timestamp), '%d.%m.%Y')
  FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY season
          ORDER BY total_goals DESC, match_date) AS rn
        FROM scored)
  WHERE rn = 1 AND total_goals > 0
),
attendance_rec AS (
  SELECT season, 3, 'Рекорд посещаемости',
         match_text || COALESCE(' · ' || venue, ''),
         replace(format('%,d', attendance), ',', ' '),
         date_format(CAST(match_date AS timestamp), '%d.%m.%Y')
           || CASE WHEN ties > 1
                THEN ' · ещё ' || CAST(ties - 1 AS varchar) || ' с тем же числом'
                ELSE '' END
  FROM (
    SELECT s.*, {venue_canon} AS venue,
           ROW_NUMBER() OVER (PARTITION BY s.season
             ORDER BY s.attendance DESC, s.match_date) AS rn,
           COUNT(*) OVER (PARTITION BY s.season, s.attendance) AS ties
    FROM scored s
    LEFT JOIN iceberg.gold.dim_venue dv ON dv.venue_id = s.venue_id
    WHERE s.attendance IS NOT NULL
  )
  WHERE rn = 1
),
fastest_goal AS (
  SELECT season, 4, 'Самый быстрый гол',
         player || ' — ' || match_text,
         CAST(minute AS varchar) || '-я минута',
         date_format(CAST(match_date AS timestamp), '%d.%m.%Y')
           || CASE WHEN ties > 1
                THEN ' · ещё ' || CAST(ties - 1 AS varchar) || ' на той же минуте'
                ELSE '' END
  FROM (
    SELECT sc.season, sc.match_text, sc.match_date, sh.minute,
           COALESCE(dp.player_name, sh.player_id) AS player,
           ROW_NUMBER() OVER (PARTITION BY sc.season
             ORDER BY sh.minute, sc.match_date) AS rn,
           COUNT(*) OVER (PARTITION BY sc.season, sh.minute) AS ties
    FROM iceberg.gold.fct_shot sh
    JOIN scored sc ON sc.match_id = sh.match_id AND sc.season = sh.season
    LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = sh.player_id
    WHERE sh.league = 'INT-World Cup'
      AND sh.result = 'goal' AND sh.minute <= 120
  )
  WHERE rn = 1
),
top_scorer AS (
  SELECT season, 5, 'Лучший бомбардир',
         array_join(array_agg(player ORDER BY player), ', '),
         CAST(MAX(goals) AS varchar) || ' '
           || {_ru_plural("MAX(goals)", "гол", "гола", "голов")},
         ''
  FROM (
    SELECT ps.season, ps.goals,
           COALESCE(dp.player_name, ps.player_id) AS player,
           RANK() OVER (PARTITION BY ps.season ORDER BY ps.goals DESC) AS rk
    FROM iceberg.gold.fct_player_season_stats ps
    LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = ps.player_id
    WHERE ps.league = 'INT-World Cup' AND ps.goals > 0
  )
  WHERE rk = 1
  GROUP BY season
),
hat_tricks AS (
  SELECT season, 6, 'Хет-трики',
         array_join(array_agg(DISTINCT player ORDER BY player), ', '),
         CAST(COUNT(*) AS varchar),
         ''
  FROM (
    SELECT pm.season, COALESCE(dp.player_name, pm.player_id) AS player
    FROM iceberg.gold.fct_player_match pm
    LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = pm.player_id
    WHERE pm.league = 'INT-World Cup' AND pm.goals >= 3
  )
  GROUP BY season
),
own_goals_rec AS (
  SELECT season, 7, 'Автоголы', '',
         CAST(SUM(own_goals) AS varchar), ''
  FROM iceberg.gold.fct_player_match
  WHERE league = 'INT-World Cup'
  GROUP BY season
  HAVING SUM(own_goals) > 0
),
zero_draws AS (
  SELECT season, 8, 'Нулевые ничьи (0:0)', '',
         CAST(COUNT(*) AS varchar) || ' '
           || {_ru_plural("COUNT(*)", "матч", "матча", "матчей")}, ''
  FROM scored
  WHERE total_goals = 0
  GROUP BY season
)
SELECT * FROM biggest_win
UNION ALL SELECT * FROM highest_score
UNION ALL SELECT * FROM attendance_rec
UNION ALL SELECT * FROM fastest_goal
UNION ALL SELECT * FROM top_scorer
UNION ALL SELECT * FROM hat_tricks
UNION ALL SELECT * FROM own_goals_rec
UNION ALL SELECT * FROM zero_draws"""

    v_records_labels = {
        "season": "ЧМ", "record_order": "№", "record": "Рекорд",
        "holder": "Кто", "value_label": "Значение", "detail": "Детали",
    }

    return {
        "v_wc_match": _ensure_virtual_dataset(
            ctx, database, schema, "v_wc_match", v_match, v_match_labels),
        "v_wc_standings": _ensure_virtual_dataset(
            ctx, database, schema, "v_wc_standings", v_standings,
            v_standings_labels),
        "v_wc_team_tournament": _ensure_virtual_dataset(
            ctx, database, schema, "v_wc_team_tournament", v_team_tournament,
            v_team_tournament_labels),
        "v_wc_player_tournament": _ensure_virtual_dataset(
            ctx, database, schema, "v_wc_player_tournament",
            v_player_tournament, v_player_tournament_labels),
        "v_wc_shot": _ensure_virtual_dataset(
            ctx, database, schema, "v_wc_shot", v_shot, v_shot_labels),
        "v_wc_tournament": _ensure_virtual_dataset(
            ctx, database, schema, "v_wc_tournament", v_tournament,
            v_tournament_labels),
        "v_wc_final": _ensure_virtual_dataset(
            ctx, database, schema, "v_wc_final", v_final, v_final_labels),
        "v_wc_records": _ensure_virtual_dataset(
            ctx, database, schema, "v_wc_records", v_records,
            v_records_labels),
        "v_wc_team_progress": _ensure_virtual_dataset(
            ctx, database, schema, "v_wc_team_progress", v_team_progress,
            v_team_progress_labels),
    }


# ---------------------------------------------------------------------------
# Slices
# ---------------------------------------------------------------------------
def _build_slices(ctx: _Ctx, vds: dict[str, Any]) -> list[Any]:
    match = vds["v_wc_match"]
    standings = vds["v_wc_standings"]
    team = vds["v_wc_team_tournament"]
    player = vds["v_wc_player_tournament"]
    shot = vds["v_wc_shot"]
    tournament = vds["v_wc_tournament"]
    final = vds["v_wc_final"]
    progress = vds["v_wc_team_progress"]
    records = vds["v_wc_records"]

    slices: list[Any] = []

    _kpi_time = {"time_range": "No filter"}
    _hbar = {
        "orientation": "horizontal",
        "show_legend": False,
        "show_value": True,
        "rich_tooltip": True,
    }
    _line_tt = {
        "rich_tooltip": True,
        "showTooltipTotal": False,
        "showTooltipPercentage": False,
        "tooltipSortByMetric": True,
        "sort_series_type": "sum",
        "sort_series_ascending": False,
        "x_axis_title_margin": 30,
    }
    _played = _sql_where("is_completed")

    # === Таб 1. Паспорт турнира: 0..5 KPI + 6..7 =============================
    slices.append(_make_slice(ctx,
        "Матчей сыграно", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Матчей", "COUNT", "match_id"),
            "adhoc_filters": [_played],
            "subheader": "в выбранном срезе",
            "y_axis_format": ",d",
        },
    ))
    slices.append(_make_slice(ctx,
        "Голов", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Голов", "SUM", "total_goals"),
            "adhoc_filters": [_played],
            "subheader": "в выбранном срезе",
            "y_axis_format": ",d",
        },
    ))
    slices.append(_make_slice(ctx,
        "Голов за матч", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Голов/матч", "AVG", "total_goals"),
            "adhoc_filters": [_played],
            "subheader": "главная цифра жанра",
            "y_axis_format": ".2f",
        },
    ))
    slices.append(_make_slice(ctx,
        "Сборных", "big_number_total", standings,
        {
            **_kpi_time,
            "metric": _metric("Сборных", "COUNT_DISTINCT", "team_name"),
            "subheader": "в выбранном срезе",
            "y_axis_format": ",d",
        },
    ))
    slices.append(_make_slice(ctx,
        "Средняя посещаемость", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Зрителей/матч", "AVG", "attendance"),
            "adhoc_filters": [_sql_where("attendance IS NOT NULL")],
            "subheader": "на матч",
            "y_axis_format": "SMART_NUMBER",
        },
    ))
    slices.append(_make_slice(ctx,
        "Серий пенальти", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Серий", "SUM", "went_to_pens"),
            "subheader": "в плей-офф",
            "y_axis_format": ",d",
        },
    ))

    # series=season — контракт деградации: несколько ЧМ дают grouped bars.
    slices.append(_make_slice(ctx,
        "Голы за матч по стадиям", "echarts_timeseries_bar", match,
        {
            "x_axis": "stage_label",
            "metrics": [_metric("Голов/матч", "AVG", "total_goals")],
            "groupby": ["season"],
            "adhoc_filters": [_played],
            "row_limit": 200,
            "show_legend": True,
            "show_value": False,
            "rich_tooltip": True,
            "y_axis_format": ".2f",
            "x_axis_title_margin": 30,
            "x_axis_title": "Стадия",
        },
    ))
    # Город/страна — SofaScore-мост в v_wc_match (dim_venue для арен ЧМ пуст).
    # capacity/fill_pct по-прежнему вне чарта: вместимость есть только в
    # bronze.sofascore_venue (Superset bronze не читает) — вернуть, когда
    # silver-трансформ спроецирует capacity или арены попадут в venue_aliases.
    slices.append(_make_slice(ctx,
        "Стадионы турнира", "table", match,
        {
            "query_mode": "aggregate",
            "groupby": ["season", "venue_name", "city", "country"],
            "metrics": [
                _metric("Матчей", "COUNT", "match_id"),
                _metric("Ср. зрителей", "AVG", "attendance"),
            ],
            "adhoc_filters": [_sql_where("attendance IS NOT NULL")],
            "row_limit": 200,
            "order_desc": True,
            "timeseries_limit_metric": _metric("Ср. зрителей", "AVG", "attendance"),
            "include_search": False,
            "show_cell_bars": True,
            "column_config": {
                "Ср. зрителей": {"d3NumberFormat": ",.0f"},
            },
        },
    ))

    # === Таб 2. Групповой этап: 8..10 ========================================
    slices.append(_make_slice(ctx,
        "Таблицы групп", "table", standings,
        {
            "query_mode": "raw",
            "all_columns": [
                "season", "group_id", "position", "team_name",
                "played", "wins", "draws", "losses",
                "goals_for", "goals_against", "goal_diff", "points",
                "advanced",
            ],
            "order_by_cols": ['["group_id", true]', '["position", true]'],
            "row_limit": 500,
            "include_search": True,
            "show_cell_bars": False,
            "conditional_formatting": [
                {"operator": "=", "targetValue": 1,
                 "column": "advanced", "colorScheme": "#ACE1C4"},
            ],
        },
    ))
    slices.append(_make_slice(ctx,
        "Группы: очки vs разница мячей", "bubble_v2", standings,
        {
            "entity": "team_label",
            "x": _metric("Очки", "SUM", "points"),
            "y": _metric("Разница мячей", "SUM", "goal_diff"),
            "size": _metric("Голы", "SUM", "goals_for"),
            "series": "group_id",
            "row_limit": 200,
            "max_bubble_size": "15",
            "x_axis_label": "Очки в группе",
            "y_axis_label": "Разница мячей",
            "show_legend": False,
        },
    ))
    slices.append(_make_slice(ctx,
        "Лучшие третьи места", "table", standings,
        {
            "query_mode": "raw",
            "all_columns": [
                "season", "group_id", "team_name",
                "points", "goal_diff", "goals_for", "advanced",
            ],
            "adhoc_filters": [_sql_where("position = 3")],
            "order_by_cols": ['["points", false]', '["goal_diff", false]'],
            "row_limit": 100,
            "include_search": False,
            "show_cell_bars": True,
            "conditional_formatting": [
                {"operator": "=", "targetValue": 1,
                 "column": "advanced", "colorScheme": "#ACE1C4"},
            ],
        },
    ))

    # === Таб 3. Плей-офф: 11..14 =============================================
    slices.append(_make_slice(ctx,
        "Сетка плей-офф", "table", match,
        {
            "query_mode": "raw",
            "all_columns": [
                "season", "stage_order", "stage_label", "match_date",
                "match_label", "score_label",
            ],
            "adhoc_filters": [_sql_where("is_knockout")],
            "order_by_cols": ['["stage_order", false]', '["match_date", true]'],
            "row_limit": 200,
            "include_search": True,
            "show_cell_bars": False,
            "table_timestamp_format": "%d.%m.%Y",
        },
    ))
    slices.append(_make_slice(ctx,
        "Серии пенальти: матчи", "table", match,
        {
            "query_mode": "raw",
            "all_columns": [
                "season", "stage_label", "match_date",
                "match_label", "score_label",
            ],
            "adhoc_filters": [_sql_where("went_to_pens = 1")],
            "order_by_cols": ['["match_date", true]'],
            "row_limit": 100,
            "include_search": False,
            "show_cell_bars": False,
            "table_timestamp_format": "%d.%m.%Y",
        },
    ))
    slices.append(_make_slice(ctx,
        "Голы по минутам", "echarts_timeseries_bar", shot,
        {
            "x_axis": "minute_bucket",
            "metrics": [_metric("Голов", "SUM", "is_goal_int")],
            "groupby": ["season"],
            "adhoc_filters": [_sql_where("is_shootout_attempt = 0")],
            "row_limit": 200,
            "show_legend": True,
            "show_value": False,
            "rich_tooltip": True,
            "y_axis_format": ",d",
            "x_axis_title_margin": 30,
            "x_axis_title": "Минуты (15-мин корзины)",
        },
    ))
    slices.append(_make_slice(ctx,
        "Голы по типам атаки", "echarts_timeseries_bar", shot,
        {
            "x_axis": "season",
            "metrics": [_metric("Голов", "SUM", "is_goal_int")],
            "groupby": ["situation_label"],
            "stack": True,
            "adhoc_filters": [_sql_where("is_shootout_attempt = 0")],
            "row_limit": 200,
            "show_legend": True,
            "show_value": False,
            "rich_tooltip": True,
            "y_axis_format": ",d",
            "x_axis_title_margin": 30,
            "x_axis_title": "ЧМ",
        },
    ))

    # === Таб 4. Сборные: 15..18 + 19 таблица =================================
    slices.append(_make_slice(ctx,
        "Стиль: владение vs xG за матч", "bubble_v2", team,
        {
            "entity": "team_label",
            "x": _metric("Владение %", "AVG", "possession_pct"),
            "y": _metric("xG/матч", "AVG", "xg_per_match"),
            "size": _metric("Дошла до (стадия №)", "MAX", "furthest_stage_order"),
            "adhoc_filters": [_sql_where(
                "possession_pct IS NOT NULL AND xg_per_match IS NOT NULL")],
            "row_limit": 200,
            "max_bubble_size": "15",
            "x_axis_label": "Владение, %",
            "y_axis_label": "xG за матч",
            "x_axis_format": ".0f",
            "y_axis_format": ".2f",
            "show_legend": False,
        },
    ))
    slices.append(_make_slice(ctx,
        "xG vs xGA за матч", "bubble_v2", team,
        {
            "entity": "team_label",
            "x": _metric("xG/матч", "AVG", "xg_per_match"),
            "y": _metric("xGA/матч", "AVG", "xga_per_match"),
            "size": _metric("Матчей", "SUM", "matches"),
            "adhoc_filters": [_sql_where("xg_per_match IS NOT NULL")],
            "row_limit": 200,
            "max_bubble_size": "15",
            "x_axis_label": "xG за матч (атака)",
            "y_axis_label": "xGA за матч (оборона, меньше = лучше)",
            "x_axis_format": ".2f",
            "y_axis_format": ".2f",
            "show_legend": False,
        },
    ))
    slices.append(_make_slice(ctx,
        "Реализация: голы − xG", "echarts_timeseries_bar", team,
        {
            "x_axis": "team_label",
            "x_axis_sort": "Голы − xG",
            "x_axis_sort_asc": True,
            "metrics": [_metric(
                "Голы − xG", "SUM", None,
                sql="SUM(goals_for) - SUM(xg)",
            )],
            "adhoc_filters": [_sql_where("xg IS NOT NULL")],
            "row_limit": 25,
            **_hbar,
            "y_axis_format": "+.1f",
        },
    ))
    slices.append(_make_slice(ctx,
        # Одна метрика — иначе ось не сортируется (см. «Топ-15: гол + пас»).
        # Разбивка ЖК/КК — в «Сводной таблице сборных».
        "Дисциплина: карточки", "echarts_timeseries_bar", team,
        {
            "x_axis": "team_label",
            "x_axis_sort": "Карточки (ЖК+КК)",
            "x_axis_sort_asc": True,
            "metrics": [_metric(
                "Карточки (ЖК+КК)", "SUM", None,
                sql="SUM(COALESCE(yellow_cards,0) + COALESCE(red_cards,0))",
            )],
            "adhoc_filters": [_sql_where("yellow_cards IS NOT NULL")],
            "row_limit": 25,
            **_hbar,
            "y_axis_format": ",d",
        },
    ))
    slices.append(_make_slice(ctx,
        "Сводная таблица сборных", "table", team,
        {
            "query_mode": "raw",
            "all_columns": [
                "season", "team_name", "matches", "wins", "draws", "losses",
                "goals_for", "goals_against", "goal_diff", "group_points",
                "xg", "xga", "possession_pct", "big_chances",
                "fouls", "corners", "yellow_cards", "red_cards",
                "squad_value_eur", "furthest_stage_label",
            ],
            "order_by_cols": ['["goal_diff", false]'],
            "row_limit": 200,
            "include_search": True,
            "show_cell_bars": True,
            "column_config": {
                "xg": {"d3NumberFormat": ".1f"},
                "xga": {"d3NumberFormat": ".1f"},
                "possession_pct": {"d3NumberFormat": ".0f"},
                "squad_value_eur": {"d3NumberFormat": "SMART_NUMBER"},
            },
        },
    ))

    # === Таб 5. Игроки: 20..24 + 25 таблица ==================================
    slices.append(_make_slice(ctx,
        # Одна метрика: сортировка оси (x_axis_sort) в echarts-барах работает
        # только для single-metric чартов (проверено на стеке Голы+Ассисты —
        # ось выходила хаотичной). Разбивка Г/А — в «Сводной таблице игроков».
        "Топ-15: гол + пас", "echarts_timeseries_bar", player,
        {
            "x_axis": "player_label",
            "x_axis_sort": "Г+А",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Г+А", "SUM", "goal_contrib")],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": ",d",
        },
    ))
    slices.append(_make_slice(ctx,
        "xG vs голы", "bubble_v2", player,
        {
            "entity": "player_label",
            "x": _metric("xG", "SUM", "expected_goals"),
            "y": _metric("Голы", "SUM", "goals"),
            "size": _metric("Минуты", "SUM", "minutes"),
            "series": "position_group",
            "adhoc_filters": [_sql_where(
                "minutes >= 90 AND expected_goals IS NOT NULL")],
            "row_limit": 500,
            "max_bubble_size": "15",
            "x_axis_label": "xG",
            "y_axis_label": "Голы",
            "x_axis_format": ".1f",
            "show_legend": True,
        },
    ))
    slices.append(_make_slice(ctx,
        "Возраст и вклад: молодые звёзды", "bubble_v2", player,
        {
            "entity": "player_label",
            "x": _metric("Возраст", "AVG", "age_at_start"),
            "y": _metric("Г+А", "SUM", "goal_contrib"),
            "size": _metric("Минуты", "SUM", "minutes"),
            "series": "position_group",
            "adhoc_filters": [_sql_where(
                "minutes >= 90 AND age_at_start IS NOT NULL")],
            "row_limit": 500,
            "max_bubble_size": "15",
            "x_axis_label": "Возраст на старте турнира",
            "y_axis_label": "Гол + пас",
            "x_axis_format": ".0f",
            "show_legend": True,
        },
    ))
    slices.append(_make_slice(ctx,
        "Топ-15 по рейтингу SofaScore", "echarts_timeseries_bar", player,
        {
            "x_axis": "player_label",
            "x_axis_sort": "Рейтинг",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Рейтинг", "AVG", "rating_sofascore")],
            "adhoc_filters": [_sql_where(
                "minutes >= 180 AND rating_sofascore IS NOT NULL")],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": ".2f",
        },
    ))
    slices.append(_make_slice(ctx,
        "Радар: профиль per-90", "radar", player,
        {
            "groupby": ["player_label"],
            "metrics": [
                _metric("Голы/90", "AVG", "goals_p90"),
                _metric("xG/90", "AVG", "xg_p90"),
                _metric("Удары/90", "AVG", "shots_p90"),
                _metric("Ключ. передачи/90", "AVG", "key_passes_p90"),
                _metric("Обводки/90", "AVG", "dribbles_p90"),
            ],
            "timeseries_limit_metric": _metric("Г+А", "SUM", "goal_contrib"),
            # Гейт по всем осям: игрок без КП/обводок рисует ломаный контур
            # с подписью «null» (жалоба юзера). show_labels=False — цифры на
            # вершинах налезали друг на друга, значения видны в тултипе.
            "adhoc_filters": [_sql_where(
                "minutes >= 180 AND goals_p90 IS NOT NULL "
                "AND xg_p90 IS NOT NULL AND shots_p90 IS NOT NULL "
                "AND key_passes_p90 IS NOT NULL "
                "AND dribbles_p90 IS NOT NULL")],
            "row_limit": 5,
            "show_legend": True,
            "show_labels": False,
        },
    ))
    slices.append(_make_slice(ctx,
        "Сводная таблица игроков", "table", player,
        {
            "query_mode": "raw",
            "all_columns": [
                "season", "player_name", "team_name", "position_group",
                "age_at_start", "matches", "minutes", "goals", "assists",
                "goal_contrib", "expected_goals", "expected_assists",
                "shots", "shots_on_target", "key_passes",
                "successful_dribbles", "big_chances_created",
                "rating_sofascore", "market_value_eur",
                "yellow_cards", "red_cards",
            ],
            "order_by_cols": ['["goal_contrib", false]'],
            "row_limit": 1500,
            "include_search": True,
            "show_cell_bars": True,
            "column_config": {
                "expected_goals": {"d3NumberFormat": ".1f"},
                "expected_assists": {"d3NumberFormat": ".1f"},
                "rating_sofascore": {"d3NumberFormat": ".2f"},
                "market_value_eur": {"d3NumberFormat": "SMART_NUMBER"},
            },
        },
    ))

    # === Таб 6. Сравнение ЧМ («эпохи», вне фильтра года): 26..34 =============
    slices.append(_make_slice(ctx,
        "Голы за матч по турнирам", "echarts_timeseries_line", tournament,
        {
            "x_axis": "season",
            "metrics": [_metric("Голов/матч", "AVG", "goals_per_match")],
            "row_limit": 50,
            "show_legend": False,
            **_line_tt,
            "markerEnabled": True,
            "y_axis_format": ".2f",
            "x_axis_title_margin": 30,
            "x_axis_title": "ЧМ",
        },
    ))
    slices.append(_make_slice(ctx,
        "Группа vs плей-офф: голы за матч", "echarts_timeseries_line", match,
        {
            "x_axis": "season",
            "metrics": [_metric("Голов/матч", "AVG", "total_goals")],
            "groupby": ["phase_label"],
            "adhoc_filters": [_played],
            "row_limit": 200,
            "show_legend": True,
            **_line_tt,
            "markerEnabled": True,
            "y_axis_format": ".2f",
            "x_axis_title_margin": 30,
            "x_axis_title": "ЧМ",
        },
    ))
    slices.append(_make_slice(ctx,
        "Посещаемость на матч по турнирам", "echarts_timeseries_bar", tournament,
        {
            "x_axis": "season",
            "metrics": [_metric("Зрителей/матч", "AVG", "avg_attendance")],
            "row_limit": 50,
            "show_legend": False,
            "show_value": False,
            "rich_tooltip": True,
            "y_axis_format": "SMART_NUMBER",
            "x_axis_title_margin": 30,
            "x_axis_title": "ЧМ",
        },
    ))
    slices.append(_make_slice(ctx,
        "Зрителей за турнир", "echarts_timeseries_bar", tournament,
        {
            "x_axis": "season",
            "metrics": [_metric("Всего зрителей", "SUM", "total_attendance")],
            "row_limit": 50,
            "show_legend": False,
            "show_value": False,
            "rich_tooltip": True,
            "y_axis_format": "SMART_NUMBER",
            "x_axis_title_margin": 30,
            "x_axis_title": "ЧМ",
        },
    ))
    slices.append(_make_slice(ctx,
        "Серии пенальти по турнирам", "echarts_timeseries_bar", tournament,
        {
            "x_axis": "season",
            "metrics": [_metric("Серий пенальти", "SUM", "shootouts")],
            "row_limit": 50,
            "show_legend": False,
            "show_value": True,
            "rich_tooltip": True,
            "y_axis_format": ",d",
            "x_axis_title_margin": 30,
            "x_axis_title": "ЧМ",
        },
    ))
    slices.append(_make_slice(ctx,
        "Рост формата: сборные и матчи", "echarts_timeseries_bar", tournament,
        {
            "x_axis": "season",
            "metrics": [
                _metric("Сборных", "AVG", "teams"),
                _metric("Матчей", "AVG", "matches_played"),
            ],
            "row_limit": 50,
            "show_legend": True,
            "show_value": False,
            "rich_tooltip": True,
            "y_axis_format": ",d",
            "x_axis_title_margin": 30,
            "x_axis_title": "ЧМ",
        },
    ))
    slices.append(_make_slice(ctx,
        "Чемпионы и финалы", "table", final,
        {
            "query_mode": "raw",
            "all_columns": [
                "season", "champion", "runner_up", "final_score",
                "went_to_pens", "champ_goals_per_match", "champ_goal_diff",
                "champ_win_share",
            ],
            "adhoc_filters": [_sql_where("champion IS NOT NULL")],
            "order_by_cols": ['["season", false]'],
            "row_limit": 50,
            "include_search": False,
            "show_cell_bars": True,
            "column_config": {
                "champ_goals_per_match": {"d3NumberFormat": ".2f"},
                "champ_win_share": {"d3NumberFormat": ".0%"},
            },
        },
    ))
    slices.append(_make_slice(ctx,
        "Матрица прогресса сборных", "heatmap_v2", progress,
        {
            "x_axis": "season",
            "groupby": "team_name",
            "metric": _metric("Стадия №", "MAX", "furthest_stage_order"),
            "row_limit": 1000,
            "linear_color_scheme": "superset_seq_1",
            "left_margin": "auto",
            "bottom_margin": "auto",
            "y_axis_format": ",d",
            "sort_x_axis": "alpha_asc",
            "sort_y_axis": "value_desc",
            "show_legend": True,
            "show_values": False,
            "normalize_across": "heatmap",
        },
    ))
    slices.append(_make_slice(ctx,
        "Очки и выход из группы", "echarts_timeseries_bar", standings,
        {
            "x_axis": "points",
            "metrics": [_metric(
                "% вышедших", "AVG", None,
                sql="AVG(CAST(advanced AS DOUBLE)) * 100",
            )],
            "groupby": ["season"],
            "row_limit": 200,
            "show_legend": True,
            "show_value": False,
            "rich_tooltip": True,
            "y_axis_format": ".0f",
            "x_axis_title_margin": 30,
            "x_axis_title": "Очки в группе",
        },
    ))

    # === Дозаказ WC1-полировки (индексы 35..40 — в конец, чтобы не сдвигать
    # ссылки layout на 0..34) ==================================================
    slices.append(_make_slice(ctx,  # 35
        "Стоимость составов (€)", "echarts_timeseries_bar", team,
        {
            "x_axis": "team_label",
            "x_axis_sort": "Стоимость состава",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Стоимость состава", "SUM", "squad_value_eur")],
            "adhoc_filters": [_sql_where("squad_value_eur IS NOT NULL")],
            "row_limit": 25,
            **_hbar,
            "y_axis_format": "SMART_NUMBER",
        },
    ))
    slices.append(_make_slice(ctx,  # 36
        "Стоимость состава vs путь", "bubble_v2", team,
        {
            "entity": "team_label",
            "x": _metric("Стоимость состава (€)", "SUM", "squad_value_eur"),
            "y": _metric("Дошла до (стадия №)", "MAX", "furthest_stage_order"),
            "size": _metric("Голы", "SUM", "goals_for"),
            "adhoc_filters": [_sql_where("squad_value_eur IS NOT NULL")],
            "row_limit": 200,
            "max_bubble_size": "15",
            "x_axis_label": "Стоимость состава (€, оценки на старт турнира)",
            "y_axis_label": "Стадия (1=группы … 7=финал)",
            "x_axis_format": "SMART_NUMBER",
            "show_legend": False,
        },
    ))
    slices.append(_make_slice(ctx,  # 37
        "Серии: конверсия по сборным", "echarts_timeseries_bar", shot,
        {
            "x_axis": "team_name",
            "x_axis_sort": "Конверсия %",
            "x_axis_sort_asc": True,
            "metrics": [_metric(
                "Конверсия %", "AVG", None,
                sql="SUM(is_goal_int) * 100.0 / COUNT(*)",
            )],
            "adhoc_filters": [_sql_where("is_shootout_attempt = 1")],
            "row_limit": 30,
            **_hbar,
            "y_axis_format": ".0f",
        },
    ))
    slices.append(_make_slice(ctx,  # 38
        "Серии пенальти: все попытки", "table", shot,
        {
            "query_mode": "raw",
            "all_columns": [
                "season", "stage_label", "match_label",
                "team_name", "player_name", "result_label",
            ],
            "adhoc_filters": [_sql_where("is_shootout_attempt = 1")],
            "order_by_cols": ['["match_label", true]'],
            "row_limit": 300,
            "include_search": True,
            "show_cell_bars": False,
        },
    ))
    slices.append(_make_slice(ctx,  # 39
        "Карта ударов (ворота справа)", "bubble_v2", shot,
        {
            "entity": "shot_id",
            # координаты SofaScore: чужие ворота у x=0 — инвертируем, чтобы
            # атака шла слева направо (ворота справа, как в заголовке)
            "x": _metric("Длина поля (0→1)", None, None, sql="MIN(1 - x)"),
            "y": _metric("Ширина поля (0→1)", "MIN", "y"),
            "size": _metric("xG", "SUM", "xg"),
            "series": "result_label",
            "adhoc_filters": [_sql_where("is_shootout_attempt = 0")],
            "row_limit": 5000,
            "max_bubble_size": "10",
            "x_axis_label": "Длина поля (0 = свои ворота, 1 = чужие)",
            "y_axis_label": "Ширина поля",
            "x_axis_format": ".2f",
            "y_axis_format": ".2f",
            "show_legend": True,
        },
    ))
    slices.append(_make_slice(ctx,  # 40
        "Разброс голов: один ЧМ — одна коробка", "box_plot", match,
        {
            "query_mode": "aggregate",
            "groupby": ["season"],
            "columns": ["match_id"],
            "metrics": [_metric("Голы в матче", "SUM", "total_goals")],
            "adhoc_filters": [_played],
            "whiskerOptions": "Tukey",
            "row_limit": 20000,
            "x_axis_title_margin": 30,
            "x_axis_title": "ЧМ",
            "y_axis_format": ",d",
        },
    ))

    # === Паспорт v2 (индексы 41..48 — в конец, чтобы не сдвигать layout) =====
    slices.append(_make_slice(ctx,  # 41
        "Зрителей всего", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Зрителей", "SUM", "attendance"),
            "adhoc_filters": [_played],
            "subheader": "суммарно на трибунах",
            "y_axis_format": "SMART_NUMBER",
        },
    ))
    # NULLIF: у эпох venue/страна = NULL у всех матчей — голый COUNT_DISTINCT
    # рисовал бы ложный «0» вместо пустой карточки (контракт деградации).
    slices.append(_make_slice(ctx,  # 42
        "Стадионов", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Стадионов", None, None,
                              sql="NULLIF(COUNT(DISTINCT venue_name), 0)"),
            "subheader": "принимали матчи",
            "y_axis_format": ",d",
        },
    ))
    slices.append(_make_slice(ctx,  # 43
        "Стран-хозяек", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Стран", None, None,
                              sql="NULLIF(COUNT(DISTINCT country), 0)"),
            "subheader": "география турнира",
            "y_axis_format": ",d",
        },
    ))
    # cards_total NULL у матчей без командной статистики (эпохи) — AVG их
    # пропускает, а не подмешивает нули.
    slices.append(_make_slice(ctx,  # 44
        "Карточек за матч", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Карточек/матч", "AVG", "cards_total"),
            "adhoc_filters": [_played],
            "subheader": "жёлтые + красные",
            "y_axis_format": ".2f",
        },
    ))
    slices.append(_make_slice(ctx,  # 45
        "Пенальти с игры", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Пенальти", "SUM", "pens_awarded"),
            "adhoc_filters": [_played],
            "subheader": "назначено (серии не в счёт)",
            "y_axis_format": ",d",
        },
    ))
    slices.append(_make_slice(ctx,  # 46
        "Автоголов", "big_number_total", match,
        {
            **_kpi_time,
            "metric": _metric("Автоголов", "SUM", "own_goals"),
            "adhoc_filters": [_played],
            "subheader": "за турнир",
            "y_axis_format": ",d",
        },
    ))
    # tournament_day — относительная ось контракта деградации: дни разных ЧМ
    # ложатся на общую X-сетку, series=season даёт grouped bars.
    slices.append(_make_slice(ctx,  # 47
        "Ритм турнира: голы по дням", "echarts_timeseries_bar", match,
        {
            "x_axis": "tournament_day",
            "metrics": [_metric("Голов", "SUM", "total_goals")],
            "groupby": ["season"],
            "adhoc_filters": [_played],
            "row_limit": 2000,
            "show_legend": True,
            "show_value": False,
            "rich_tooltip": True,
            "y_axis_format": ",d",
            "x_axis_title_margin": 30,
            "x_axis_title": "День турнира",
        },
    ))
    slices.append(_make_slice(ctx,  # 48
        "Рекорды турнира", "table", records,
        {
            "query_mode": "raw",
            "all_columns": ["season", "record", "holder", "value_label",
                            "detail"],
            "order_by_cols": ['["season", true]', '["record_order", true]'],
            "row_limit": 500,
            "include_search": False,
            "show_cell_bars": False,
        },
    ))

    return slices


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
def _build_position_json(slices: list[Any]) -> dict[str, Any]:
    """Grid v2 с вкладками: шапка над TABS, шесть TAB-контейнеров внутри."""
    pos: dict[str, Any] = {"DASHBOARD_VERSION_KEY": "v2"}

    root_id = "ROOT_ID"
    grid_id = "GRID_ID"
    tabs_id = "TABS-wc"
    pos[root_id] = {"type": "ROOT", "id": root_id, "children": [grid_id]}
    pos[grid_id] = {
        "type": "GRID",
        "id": grid_id,
        "children": [],
        "parents": [root_id],
        "meta": {},
    }
    pos[tabs_id] = {
        "type": "TABS",
        "id": tabs_id,
        "children": [],
        "parents": [root_id, grid_id],
        "meta": {},
    }

    def _markdown(parents: list[str], text: str, height: int = 12) -> str:
        mid = f"MARKDOWN-{abs(hash(text)) % (10**8)}"
        pos[mid] = {
            "type": "MARKDOWN",
            "id": mid,
            "children": [],
            "parents": parents,
            "meta": {"width": 12, "height": height, "code": text,
                     "background": "BACKGROUND_TRANSPARENT"},
        }
        return mid

    def _row(parents: list[str], child_ids: list[str]) -> str:
        rid = f"ROW-{abs(hash(tuple(child_ids))) % (10**8)}"
        pos[rid] = {
            "type": "ROW",
            "id": rid,
            "children": child_ids,
            "parents": parents,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        return rid

    def _chart(slc: Any, width: int, height: int = 50) -> str:
        cid = f"CHART-{slc.id}"
        pos[cid] = {
            "type": "CHART",
            "id": cid,
            "children": [],
            "parents": [root_id, grid_id, tabs_id],
            "meta": {
                "width": width,
                "height": height,
                "chartId": slc.id,
                "sliceName": slc.slice_name,
            },
        }
        return cid

    def _tab(key: str, title: str) -> tuple[list[str], list[str]]:
        tid = f"TAB-wc-{key}"
        pos[tid] = {
            "type": "TAB",
            "id": tid,
            "children": [],
            "parents": [root_id, grid_id, tabs_id],
            "meta": {"text": title, "defaultText": "Tab title",
                     "placeholder": "Tab title"},
        }
        pos[tabs_id]["children"].append(tid)
        return pos[tid]["children"], [root_id, grid_id, tabs_id, tid]

    header = _markdown(
        [root_id, grid_id],
        "# Чемпионаты мира\n\n"
        "Сравнение турниров и обзор каждого ЧМ. Фильтр «ЧМ (год)» — "
        "мультивыбор: выбери несколько турниров, чтобы сравнить их на "
        "вкладках 1–5; вкладка «Сравнение ЧМ» показывает все турниры сразу. "
        "Источники: FBref (счёт, стадии, посещаемость) · SofaScore (удары, "
        "xG, рейтинги, фолы, дуэли) · FotMob (xG, явные шансы) · WhoScored "
        "(события) · ESPN (составы).",
        height=10,
    )
    pos[grid_id]["children"] = [header, tabs_id]

    # --- Таб 1. Паспорт турнира ---------------------------------------------
    c, p = _tab("passport", "Паспорт турнира")
    c.append(_row(p, [_chart(slices[i], 2, height=25) for i in range(0, 6)]))
    c.append(_row(p, [_chart(slices[i], 2, height=25) for i in range(41, 47)]))
    c.append(_markdown(p,
        "## Профиль турнира\n\n"
        "Голы по стадиям — среднее за матч (нормировка per-match: у разных "
        "ЧМ разное число матчей). Ритм — голы по дням турнира (день 1 = "
        "матч открытия): видно плотность группового этапа и паузы плей-офф.",
        height=16,
    ))
    c.append(_row(p, [_chart(slices[6], 6, height=55), _chart(slices[47], 6, height=55)]))
    c.append(_markdown(p,
        "## Рекорды и география\n\n"
        "Рекорды считаются по выбранным ЧМ (строка на турнир). Город и "
        "страна стадионов — данные SofaScore; имена арен приведены к "
        "каноническим (ESPN шлёт устаревшие: Reliant = NRG Stadium, "
        "Estadio Banorte = Estadio Azteca).",
        height=16,
    ))
    c.append(_row(p, [_chart(slices[48], 6, height=70), _chart(slices[7], 6, height=70)]))

    # --- Таб 2. Групповой этап ------------------------------------------------
    c, p = _tab("groups", "Групповой этап")
    c.append(_markdown(p,
        "## Групповой этап\n\n"
        "Зелёная подсветка — сборная вышла в плей-офф. «Лучшие третьи» — "
        "формат-2026: 8 из 12 третьих мест проходят дальше.",
    ))
    c.append(_row(p, [_chart(slices[8], 12, height=90)]))
    c.append(_row(p, [_chart(slices[9], 6, height=55), _chart(slices[10], 6, height=55)]))

    # --- Таб 3. Плей-офф -------------------------------------------------------
    c, p = _tab("knockout", "Плей-офф")
    c.append(_markdown(p,
        "## Плей-офф\n\n"
        "Сетка — таблицей (нативного bracket-виза в Superset нет): стадии "
        "сверху вниз от финала. Счёт — основной + серия пенальти в скобках. "
        "«Голы по минутам» и «по типам» — из шот-данных (современные ЧМ).",
        height=16,
    ))
    c.append(_row(p, [_chart(slices[11], 7, height=70), _chart(slices[12], 5, height=70)]))
    c.append(_row(p, [_chart(slices[13], 6, height=55), _chart(slices[14], 6, height=55)]))
    c.append(_markdown(p,
        "## Серии пенальти — поударно\n\n"
        "Каждая попытка послематчевой серии из шот-данных SofaScore "
        "(минуты 121+). Конверсия — % реализованных попыток сборной.",
    ))
    c.append(_row(p, [_chart(slices[37], 5, height=55), _chart(slices[38], 7, height=55)]))

    # --- Таб 4. Сборные --------------------------------------------------------
    c, p = _tab("teams", "Сборные")
    c.append(_markdown(p,
        "## Сборные\n\n"
        "Точка = сборная-турнир («Бразилия 2022» и «Бразилия 2026» — разные "
        "точки). Размер пузыря в «стиле» — насколько далеко сборная дошла.",
    ))
    c.append(_row(p, [_chart(slices[15], 6, height=55), _chart(slices[16], 6, height=55)]))
    c.append(_row(p, [_chart(slices[17], 6, height=55), _chart(slices[18], 6, height=55)]))
    c.append(_markdown(p,
        "## Деньги\n\n"
        "Стоимость состава = сумма последних рыночных оценок игроков заявки "
        "на старт турнира (Transfermarkt/FotMob, покрытие ~92% игроков).",
    ))
    c.append(_row(p, [_chart(slices[35], 6, height=55), _chart(slices[36], 6, height=55)]))
    c.append(_row(p, [_chart(slices[19], 12, height=80)]))

    # --- Таб 5. Игроки ---------------------------------------------------------
    c, p = _tab("players", "Игроки")
    c.append(_markdown(p,
        "## Игроки\n\n"
        "Scatter'ы — минимум 90 минут; рейтинг и per-90 — минимум 180. "
        "Радар — топ-5 по Г+А; выбери игроков в фильтре «Игрок», чтобы "
        "сравнить любых (в т.ч. с разных ЧМ). ⚠️ Рейтинг SofaScore пока "
        "подтянут мостом по имени (покрытие ~60% игроков, топы покрыты) — "
        "станет полным после пересборки xref.",
        height=20,
    ))
    c.append(_row(p, [_chart(slices[20], 6, height=55), _chart(slices[23], 6, height=55)]))
    c.append(_row(p, [_chart(slices[21], 6, height=55), _chart(slices[22], 6, height=55)]))
    c.append(_row(p, [_chart(slices[24], 6, height=60), _chart(slices[25], 6, height=60)]))
    c.append(_markdown(p,
        "## Карта ударов\n\n"
        "Все удары выбранного среза (кроме серий пенальти): размер = xG, "
        "цвет = исход. Подложки поля в Superset нет — атака слева направо, "
        "чужие ворота у x = 1. Выбери игрока или сборную в фильтрах.",
        height=16,
    ))
    c.append(_row(p, [_chart(slices[39], 12, height=70)]))

    # --- Таб 6. Сравнение ЧМ ---------------------------------------------------
    c, p = _tab("epochs", "Сравнение ЧМ")
    c.append(_markdown(p,
        "## Сравнение ЧМ («эпохи»)\n\n"
        "Вкладка НЕ зависит от фильтра «ЧМ (год)» — показывает все турниры, "
        "загруженные в платформу. Метрики нормированы на матч: форматы "
        "росли с 13 сборных (1930) до 48 (2026), абсолюты несравнимы. "
        "Наполняется по мере бэкфила исторических ЧМ.",
        height=16,
    ))
    c.append(_row(p, [_chart(slices[26], 6, height=55), _chart(slices[27], 6, height=55)]))
    c.append(_row(p, [_chart(slices[28], 6, height=50), _chart(slices[29], 6, height=50)]))
    c.append(_row(p, [_chart(slices[30], 6, height=50), _chart(slices[31], 6, height=50)]))
    c.append(_row(p, [_chart(slices[32], 12, height=55)]))
    c.append(_row(p, [_chart(slices[33], 7, height=90), _chart(slices[34], 5, height=90)]))
    c.append(_row(p, [_chart(slices[40], 12, height=55)]))

    return pos


# ---------------------------------------------------------------------------
# Native filters
# ---------------------------------------------------------------------------
def _resolve_default_year(database: Any) -> str:
    """Дефолт «ЧМ (год)» — последний турнир в данных; fallback на константу."""
    try:
        df = database.get_df(
            "SELECT MAX(season) AS season FROM iceberg.gold.dim_match "
            "WHERE league = 'INT-World Cup'"
        )
        if not df.empty and df["season"].iloc[0]:
            return str(df["season"].iloc[0])
        log.warning("no INT-World Cup seasons in dim_match; falling back to %s",
                    FALLBACK_YEAR)
    except Exception as exc:  # noqa: BLE001
        log.warning("failed to resolve default WC year (%s); falling back to %s",
                    exc, FALLBACK_YEAR)
    return FALLBACK_YEAR


# Чарты «эпох» живут вне фильтра «ЧМ (год)» — иначе на них одна точка.
_EPOCH_SLICES = {
    "Голы за матч по турнирам",
    "Группа vs плей-офф: голы за матч",
    "Посещаемость на матч по турнирам",
    "Зрителей за турнир",
    "Серии пенальти по турнирам",
    "Рост формата: сборные и матчи",
    "Чемпионы и финалы",
    "Матрица прогресса сборных",
    "Очки и выход из группы",
    "Разброс голов: один ЧМ — одна коробка",
}


def _build_native_filters(
    slices: list[Any],
    vds: dict[str, Any],
    default_year: str,
) -> list[dict[str, Any]]:
    """Пять фильтров: ЧМ (год, multi) → Стадия / Группа / Сборная → Игрок.

    Scope — по фактическим колонкам датасета слайса (фильтр по отсутствующей
    колонке = 500). «ЧМ (год)» — multi-select (контракт деградации C);
    чарты «эпох» исключены вручную.
    """
    tournament_ds_id = vds["v_wc_tournament"].id
    match_ds_id = vds["v_wc_match"].id
    standings_ds_id = vds["v_wc_standings"].id
    player_ds_id = vds["v_wc_player_tournament"].id

    ds_columns = {
        t.id: {c.column_name for c in t.columns} for t in vds.values()
    }

    def _charts_with(column: str, skip_names: set[str] | None = None) -> list[int]:
        skip = skip_names or set()
        return [
            slc.id for slc in slices
            if column in ds_columns.get(slc.datasource_id, set())
            and slc.slice_name not in skip
        ]

    all_chart_ids = [slc.id for slc in slices]

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

    f_year = "NATIVE_FILTER-wc-year"
    f_stage = "NATIVE_FILTER-wc-stage"
    f_group = "NATIVE_FILTER-wc-group"
    f_team = "NATIVE_FILTER-wc-team"

    return [
        # Год — MULTI-select (сравнение турниров выбором нескольких лет);
        # required с дефолтом на последний ЧМ, эпохи исключены.
        _filter(f_year, "ЧМ (год)", "season", tournament_ds_id,
                multiple=True,
                target_chart_ids=_charts_with("season", _EPOCH_SLICES),
                required=True, default_value=default_year),
        _filter(f_stage, "Стадия", "stage_label", match_ds_id,
                multiple=True,
                target_chart_ids=_charts_with("stage_label", _EPOCH_SLICES),
                cascade_parent_ids=[f_year]),
        _filter(f_group, "Группа", "group_id", standings_ds_id,
                multiple=True,
                target_chart_ids=_charts_with("group_id", _EPOCH_SLICES),
                cascade_parent_ids=[f_year]),
        _filter(f_team, "Сборная", "team_name", standings_ds_id,
                multiple=True,
                target_chart_ids=_charts_with("team_name", _EPOCH_SLICES),
                cascade_parent_ids=[f_year]),
        _filter("NATIVE_FILTER-wc-player", "Игрок", "player_name",
                player_ds_id,
                multiple=True,
                target_chart_ids=_charts_with("player_name", _EPOCH_SLICES),
                cascade_parent_ids=[f_year, f_team]),
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
        default_year = _resolve_default_year(database)
        native_filters = _build_native_filters(slices, vds, default_year)

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
        format="[dashboard.world_cup] %(levelname)s %(message)s",
    )
    create_dashboard()
