#!/usr/bin/env python
# =============================================================================
# Superset dashboard: "Обзор лиги + игроки"
# =============================================================================
# Мультилиговый дашборд на пяти вкладках (TABS в position_json):
#   1. Обзор лиги   — KPI, таблица лиги (cell bars, «сверх-очки» = points −
#                     xPTS), команды 2×2 (xG/xGA, реализация, PPDA, деньги
#                     vs очки), Elo по времени
#   2. Игроки       — топ-15 ×6, scatter'ы (xG/голы, стоимость/Г+А),
#                     big chances, финишеры, сводная таблица с поиском
#   3. Трансферы и деньги — топ покупок, баланс клубов, динамика стоимости
#                     (топ-10 + фильтр «Игрок»), стоимость состава и фонд ЗП
#                     по сезонам, таблица всех сделок
#   4. Форма по турам — гонка за титул (кумулятивные очки), скользящий
#                     xG-баланс за 5 матчей, гонка бомбардиров (кумулятив
#                     Г+А), рейтинг игрока за 5 матчей
#   5. Вратари      — % сейвов, сухие матчи, PSxG − GA, сводная таблица
#
# В отличие от player_overview.py здесь НЕТ хардкода league/season: все
# виртуальные датасеты отдают все сезоны с колонками league/season, фильтрация
# целиком на native filters (Лига → Сезон → Команда → Позиция → Игрок
# каскадом + Тип сделки и Куда/Откуда (клуб) для трансферов; Сезон required
# с дефолтом из dim_season.is_current).
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
        # Сохранённый query_context имеет приоритет над form_data в
        # data_for_slices (payload дашборда): протухший контекст прячет
        # добавленные колонки (сырые имена вместо verbose_name). Сбрасываем —
        # Superset пересоберёт его из свежих params.
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
  tss.squad_market_value_eur, tss.total_wage_bill_eur,
  tss.transfer_income_eur, tss.transfer_expense_eur, tss.transfer_balance_eur,
  pay.annual_gross_eur AS payroll_annual_gross_eur,
  ds.start_date AS season_start
FROM iceberg.gold.fct_standings st
LEFT JOIN iceberg.gold.fct_team_season_stats tss
  ON  tss.team_id = st.team_id
  AND tss.league  = st.league
  AND tss.season  = st.season
LEFT JOIN (
  SELECT canonical_id, league, season, annual_gross_eur,
         ROW_NUMBER() OVER (
           PARTITION BY canonical_id, league, season
           ORDER BY _bronze_ingested_at DESC
         ) AS rn
  FROM iceberg.silver.capology_team_payrolls
) pay
  ON  pay.canonical_id = st.team_id
  AND pay.league = st.league AND pay.season = st.season AND pay.rn = 1
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

    # Позиции в dim_player — смесь таксономий источников (FBref MF/DF/FW +
    # гранулярные CAM/CDM/ST/RW…). Прежний REGEXP первых двух букв давал 14
    # пересекающихся кодов в фильтре «Позиция» и легендах scatter'ов (CAM
    # резался до «CA», CDM до «CD», выбор FW не включал ST/LW/RW) — сводим
    # в 4 группы. ELSE NULL: неизвестный код не создаёт мусорной группы.
    pos_case = """CASE
    WHEN dp.primary_position = 'GK' THEN 'GK'
    WHEN dp.primary_position IN ('DF','CB','RB','LB') THEN 'DF'
    WHEN dp.primary_position IN ('MF','CM','RM','LM','CAM','CDM') THEN 'MF'
    WHEN dp.primary_position IN ('FW','ST','RW','LW') THEN 'FW'
  END"""

    # Грейн (player_id, league, season). Все JOIN строго с (league, season) —
    # иначе фан-аут (см. CLAUDE.md footguns). Стоимость — as-of конец сезона
    # (последняя TM-оценка до dim_season.end_date); contract_until — из silver
    # Transfermarkt (шире, чем Capology contract_status = только EPL 2526).
    v_player_season = f"""\
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
  {pos_case} AS position_primary,
  COALESCE(dt.team_name, ps.team_id) AS team_name,
  ps.league, ps.season,
  ps.matches, ps.minutes,
  ps.goals, ps.assists, ps.non_penalty_goals, ps.shots, ps.shots_on_target,
  ps.key_passes, ps.expected_goals, ps.expected_assists,
  ps.goals - ps.expected_goals AS finishing_delta,
  -- Ratio-метрики только при 270+ минут: у сыгравшего 1 минуту "100%
  -- единоборств" — мусор, который всплывает при сортировке таблицы.
  CASE WHEN ps.minutes >= 270 THEN ps.rating_sofascore END AS rating_sofascore,
  CASE WHEN ps.minutes >= 270 THEN ps.take_on_pct END AS take_on_pct,
  CASE WHEN ps.minutes >= 270 THEN ps.total_duels_won_pct END AS total_duels_won_pct,
  CASE WHEN ps.minutes >= 270
       THEN (ps.goals + ps.assists) * 90.0 / NULLIF(ps.minutes, 0)
  END AS ga_per90,
  CASE WHEN ps.minutes >= 270
       THEN ps.expected_goals * 90.0 / NULLIF(ps.minutes, 0)
  END AS xg_per90,
  CASE WHEN ps.minutes >= 270
       THEN ps.expected_assists * 90.0 / NULLIF(ps.minutes, 0)
  END AS xa_per90,
  ps.successful_dribbles,
  ps.big_chances_created,
  ps.tackles_won, ps.interceptions, ps.yellow_cards, ps.red_cards,
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

    # Трансферы (грейн = сделка). team_name-колонки нет — фильтр «Команда»
    # к этим чартам не применяется (generic scope по колонкам).
    # fct_transfer.season = окно СКРЕЙПА (TM отдаёт всю карьеру игрока), не
    # сезон сделки — season здесь пересчитан из transfer_date (июль—июнь);
    # иначе «Топ покупок сезона» показывал топ сделок за всю карьеру.
    # Имена tm_*-орфанов (U21 и т.п., нет в dim_player) добираются из
    # silver.transfermarkt_players по сырому TM id (покрытие 1324/1325).
    # is_incoming = to_club играет в лиге в сезон сделки (join к standings):
    # без него «Топ покупок» показывал и продажи в чужие лиги (Diaz → Bayern).
    # Сделки будущего сезона (нет standings) дают false — они и так вне
    # фильтра «Сезон»; join по (team_id, league, season) → 0..1 строка, без
    # фан-аута.
    v_transfer = """\
WITH tm_names AS (
  SELECT player_id, name,
         ROW_NUMBER() OVER (
           PARTITION BY player_id ORDER BY _bronze_ingested_at DESC
         ) AS rn
  FROM iceberg.silver.transfermarkt_players
),
base AS (
  SELECT
    t.player_id,
    COALESCE(dp.player_name, tn.name, t.player_id) AS player_name,
    t.transfer_date,
    COALESCE(dtf.team_name, t.from_club_name) AS from_club,
    COALESCE(dtt.team_name, t.to_club_name) AS to_club,
    t.fee_eur,
    t.market_value_at_transfer_eur,
    CASE WHEN t.is_loan THEN 'аренда' ELSE 'трансфер' END AS transfer_type,
    t.league,
    t.to_team_id,
    CASE WHEN MONTH(t.transfer_date) >= 7
         THEN LPAD(CAST(YEAR(t.transfer_date) % 100 AS varchar), 2, '0')
              || LPAD(CAST((YEAR(t.transfer_date) + 1) % 100 AS varchar), 2, '0')
         ELSE LPAD(CAST((YEAR(t.transfer_date) - 1) % 100 AS varchar), 2, '0')
              || LPAD(CAST(YEAR(t.transfer_date) % 100 AS varchar), 2, '0')
    END AS season
  FROM iceberg.gold.fct_transfer t
  LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = t.player_id
  LEFT JOIN tm_names tn
    ON  tn.rn = 1
    AND SUBSTR(t.player_id, 1, 3) = 'tm_'
    AND tn.player_id = SUBSTR(t.player_id, 4)
  LEFT JOIN iceberg.gold.dim_team dtf ON dtf.team_id = t.from_team_id
  LEFT JOIN iceberg.gold.dim_team dtt ON dtt.team_id = t.to_team_id
  WHERE NOT t.is_upcoming
)
SELECT b.*, b.player_name || ' → ' || b.to_club AS deal,
       (st.team_id IS NOT NULL) AS is_incoming
FROM base b
LEFT JOIN iceberg.gold.fct_standings st
  ON  st.team_id = b.to_team_id
  AND st.league  = b.league
  AND st.season  = b.season"""

    # Источник ОПЦИЙ для фильтров «Куда/Откуда» = клубы самой лиги (≈20 на
    # сезон), НЕ все пункты назначения из fct_transfer. TM отдаёт всю карьеру,
    # и to_club/from_club за сезон = 600+ клубов со всего мира (аренды/продажи
    # молодёжи АПЛ в мелкие клубы) — выпадашка была замусорена «AC Bellinzona»
    # и т.п. Колонки названы to_club/from_club, чтобы фильтр применялся к
    # трансферным чартам (там эти колонки); league/season — для каскада.
    v_league_club = """\
SELECT DISTINCT
  ts.league, ts.season,
  COALESCE(dt.team_name, ts.team_id) AS to_club,
  COALESCE(dt.team_name, ts.team_id) AS from_club
FROM iceberg.gold.fct_team_season_stats ts
LEFT JOIN iceberg.gold.dim_team dt ON dt.team_id = ts.team_id"""

    # Таймлайн стоимости (TM) в окне сезона. Чарт режет серии series-limit'ом
    # (топ-10 по пиковой стоимости) — лимит применяется ПОСЛЕ фильтров, так
    # что выбранный в фильтре «Игрок» показывается, даже если он не из топа.
    # As-of на МЕСЯЧНОЙ сетке (forward-fill), не сырые даты оценок: TM
    # оценивает игроков в разные дни 2-4 раза за сезон, а ECharts выравнивает
    # серии на общую X-сетку — у игрока с 1-2 оценками линии не было вовсе
    # (одинокие маркеры, «игроки пропадают»).
    v_player_mv = f"""\
WITH months AS (
  SELECT ds.season, m AS month_date
  FROM iceberg.gold.dim_season ds
  CROSS JOIN UNNEST(SEQUENCE(
    DATE_TRUNC('month', ds.start_date),
    DATE_TRUNC('month', ds.end_date),
    INTERVAL '1' MONTH
  )) AS t(m)
  WHERE m <= CURRENT_DATE
),
pts AS (
  SELECT DISTINCT player_id, valuation_date, market_value_eur
  FROM iceberg.gold.fct_player_market_value
  WHERE source = 'transfermarkt'
),
grid AS (
  SELECT
    ps.player_id, ps.team_id, ps.league, ps.season, m.month_date,
    p.market_value_eur,
    ROW_NUMBER() OVER (
      PARTITION BY ps.player_id, ps.league, ps.season, m.month_date
      ORDER BY p.valuation_date DESC
    ) AS rn
  FROM iceberg.gold.fct_player_season_stats ps
  JOIN months m ON m.season = ps.season
  LEFT JOIN pts p
    ON  p.player_id = ps.player_id
    AND p.valuation_date <= LAST_DAY_OF_MONTH(m.month_date)
)
SELECT
  g.player_id,
  COALESCE(dp.player_name, g.player_id) AS player_name,
  {pos_case} AS position_primary,
  COALESCE(dt.team_name, g.team_id) AS team_name,
  g.league, g.season, g.month_date, g.market_value_eur
FROM grid g
LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = g.player_id
LEFT JOIN iceberg.gold.dim_team   dt ON dt.team_id = g.team_id
WHERE g.rn = 1 AND g.market_value_eur IS NOT NULL"""

    # По-туровая форма команд: кумулятивные очки + скользящий xG-баланс.
    # Окна строго ORDER BY gameweek (не по дате!): ось чартов — тур, и
    # перенесённый матч, сыгранный позже своего тура, при date-порядке давал
    # немонотонный кумулятив («провал очков» на графике).
    v_team_form = """\
SELECT
  tm.team_id,
  COALESCE(dt.team_name, tm.team_id) AS team_name,
  tm.league, tm.season, tm.gameweek,
  tm.date AS match_date,
  tm.points,
  SUM(tm.points) OVER (
    PARTITION BY tm.team_id, tm.league, tm.season
    ORDER BY tm.gameweek
  ) AS cum_points,
  tm.goals_for, tm.goals_against, tm.result,
  tm.xg, tm.xga,
  AVG(tm.xg - tm.xga) OVER (
    PARTITION BY tm.team_id, tm.league, tm.season
    ORDER BY tm.gameweek
    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
  ) AS xg_diff_rolling5
FROM iceberg.gold.fct_team_match tm
LEFT JOIN iceberg.gold.dim_team dt ON dt.team_id = tm.team_id
WHERE tm.is_completed AND tm.gameweek IS NOT NULL"""

    # По-матчевая форма игрока. Сырые per-match значения (рейтинг, xG/xA)
    # линиями не читались (шум 6–8 у рейтинга, пила у xG) — чарт строится на
    # рейтинге, сглаженном окном 5 матчей. Окно строго ORDER BY gameweek (см.
    # v_team_form); season_minutes — порог «только регулярные игроки».
    # Кумулятив Г+А («гонка бомбардиров») вынесен в v_player_race (плотная
    # сетка туров + forward-fill — сплошные линии без разрывов на пропусках).
    v_player_form = f"""\
SELECT
  fpm.player_id,
  COALESCE(dp.player_name, fpm.player_id) AS player_name,
  {pos_case} AS position_primary,
  COALESCE(dt.team_name, fpm.team_id) AS team_name,
  fpm.league, fpm.season,
  dm.match_date, dm.gameweek,
  fpm.minutes_played AS minutes,
  fpm.goals, fpm.assists,
  fpm.xg AS expected_goals,
  fpm.xa AS expected_assists,
  fpm.rating, fpm.key_passes,
  AVG(fpm.rating) OVER (
    PARTITION BY fpm.player_id, fpm.league, fpm.season
    ORDER BY dm.gameweek
    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
  ) AS rating_rolling5,
  SUM(fpm.minutes_played) OVER (
    PARTITION BY fpm.player_id, fpm.league, fpm.season
  ) AS season_minutes
FROM iceberg.gold.fct_player_match fpm
JOIN iceberg.gold.dim_match dm ON dm.match_id = fpm.match_id
LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = fpm.player_id
LEFT JOIN iceberg.gold.dim_team   dt ON dt.team_id = fpm.team_id
WHERE dm.gameweek IS NOT NULL"""

    # «Гонка бомбардиров»: кумулятивные Г+А по турам. Ось чарта — тур (общая
    # сетка всех игроков), но строка есть только за сыгранные туры → на
    # пропущенных турах (травма/ротация/нет матча) линия рвалась, а у игрока
    # без матча в последнем туре не доходила до края. Фикс как у v_lo_player_mv:
    # плотная сетка туров (SEQUENCE от первого сыгранного тура игрока до макс.
    # тура лиги/сезона) + forward-fill кумулятива (SUM с COALESCE(0) на пустых
    # турах) → монотонные сплошные линии. Отдельный датасет (не v_player_form):
    # там грейн «сыгранный матч», плотная сетка сломала бы рейтинг-чарт.
    # team_name — команда игрока в последнем туре (для фильтра «Команда»).
    v_player_race = f"""\
WITH pm AS (
  SELECT fpm.player_id, fpm.team_id, fpm.league, fpm.season, dm.gameweek,
         COALESCE(fpm.goals, 0) + COALESCE(fpm.assists, 0) AS ga
  FROM iceberg.gold.fct_player_match fpm
  JOIN iceberg.gold.dim_match dm ON dm.match_id = fpm.match_id
  WHERE dm.gameweek IS NOT NULL
),
bounds AS (
  SELECT league, season, MAX(gameweek) AS max_gw FROM pm GROUP BY league, season
),
perweek AS (
  SELECT player_id, league, season, gameweek, SUM(ga) AS ga_week
  FROM pm GROUP BY player_id, league, season, gameweek
),
span AS (
  SELECT p.player_id, p.league, p.season, MIN(p.gameweek) AS first_gw, b.max_gw,
         MAX_BY(p.team_id, p.gameweek) AS team_id
  FROM pm p JOIN bounds b ON b.league = p.league AND b.season = p.season
  GROUP BY p.player_id, p.league, p.season, b.max_gw
),
grid AS (
  SELECT s.player_id, s.team_id, s.league, s.season, g AS gameweek
  FROM span s CROSS JOIN UNNEST(SEQUENCE(s.first_gw, s.max_gw)) AS t(g)
)
SELECT
  g.player_id,
  COALESCE(dp.player_name, g.player_id) AS player_name,
  {pos_case} AS position_primary,
  COALESCE(dt.team_name, g.team_id) AS team_name,
  g.league, g.season, g.gameweek,
  SUM(COALESCE(pw.ga_week, 0)) OVER (
    PARTITION BY g.player_id, g.league, g.season
    ORDER BY g.gameweek
  ) AS cum_goal_contrib
FROM grid g
LEFT JOIN perweek pw
  ON  pw.player_id = g.player_id AND pw.league = g.league
  AND pw.season = g.season AND pw.gameweek = g.gameweek
LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = g.player_id
LEFT JOIN iceberg.gold.dim_team   dt ON dt.team_id = g.team_id"""

    v_keeper_season = """\
SELECT
  k.player_id,
  COALESCE(dp.player_name, k.player_id) AS player_name,
  COALESCE(dt.team_name, k.team_id) AS team_name,
  k.league, k.season,
  k.matches, k.minutes,
  k.saves, k.save_pct, k.saves_per_90,
  k.clean_sheets, k.clean_sheet_pct,
  k.goals_against, k.goals_against_per90,
  k.pk_faced, k.pk_saved, k.pk_save_pct,
  k.psxg_minus_ga, k.fotmob_rating
FROM iceberg.gold.fct_keeper_season_stats k
LEFT JOIN iceberg.gold.dim_player dp ON dp.player_id = k.player_id
LEFT JOIN iceberg.gold.dim_team   dt ON dt.team_id = k.team_id"""

    team_labels = {
        "position": "Место", "team_name": "Команда", "played": "И",
        "wins": "В", "draws": "Н", "losses": "П",
        "goals_for": "Забито", "goals_against": "Пропущено",
        "goal_diff": "Разница", "points": "Очки",
        "xg": "xG", "xga": "xGA", "xpts": "xPTS",
        "over_points": "Сверх-очки",
        "transfer_balance_eur": "Трансферный баланс (€)",
        "payroll_annual_gross_eur": "Фонд ЗП (€/год)",
    }
    player_labels = {
        "player_name": "Игрок", "team_name": "Клуб",
        "primary_position": "Позиция", "matches": "Матчи",
        "minutes": "Минуты", "goals": "Голы", "assists": "Ассисты",
        "expected_goals": "xG", "expected_assists": "xA",
        "ga_per90": "Г+А/90", "xg_per90": "xG/90", "xa_per90": "xA/90",
        "shots": "Удары", "shots_on_target": "В створ",
        "key_passes": "Ключевые передачи",
        "big_chances_created": "Big chances",
        "successful_dribbles": "Обводки",
        "tackles_won": "Отборы", "interceptions": "Перехваты",
        "yellow_cards": "ЖК", "red_cards": "КК",
        "rating_sofascore": "Рейтинг",
        "total_duels_won_pct": "% единоборств", "take_on_pct": "% обводок",
        "market_value_eur": "Стоимость (€)",
        "annual_gross_eur": "Зарплата (€/год)",
        "contract_status": "Контракт (статус)", "contract_until": "Контракт до",
    }

    transfer_labels = {
        "player_name": "Игрок", "transfer_date": "Дата",
        "from_club": "Откуда", "to_club": "Куда",
        "fee_eur": "Сумма (€)",
        "market_value_at_transfer_eur": "Стоимость на момент (€)",
        "transfer_type": "Тип", "deal": "Сделка",
    }
    mv_labels = {
        "player_name": "Игрок", "team_name": "Клуб",
        "month_date": "Месяц", "market_value_eur": "Стоимость (€)",
    }
    race_labels = {
        "player_name": "Игрок", "team_name": "Клуб",
        "gameweek": "Тур", "cum_goal_contrib": "Г+А (кумулятивно)",
    }
    keeper_labels = {
        "player_name": "Вратарь", "team_name": "Клуб",
        "matches": "Матчи", "minutes": "Минуты",
        "saves": "Сейвы", "save_pct": "% сейвов",
        "saves_per_90": "Сейвов/90",
        "clean_sheets": "Сухие", "clean_sheet_pct": "% сухих",
        "goals_against": "Пропущено", "goals_against_per90": "Пропущено/90",
        "pk_faced": "Пен. против", "pk_saved": "Пен. отбито",
        "pk_save_pct": "% отбитых пен.",
        "psxg_minus_ga": "PSxG − GA", "fotmob_rating": "Рейтинг FotMob",
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
        "v_lo_transfer": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_transfer", v_transfer,
            labels=transfer_labels,
        ),
        "v_lo_league_club": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_league_club", v_league_club
        ),
        "v_lo_player_mv": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_player_mv", v_player_mv,
            labels=mv_labels,
        ),
        "v_lo_team_form": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_team_form", v_team_form
        ),
        "v_lo_player_form": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_player_form", v_player_form
        ),
        "v_lo_player_race": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_player_race", v_player_race,
            labels=race_labels,
        ),
        "v_lo_keeper_season": _ensure_virtual_dataset(
            ctx, database, schema, "v_lo_keeper_season", v_keeper_season,
            labels=keeper_labels,
        ),
    }


# ---------------------------------------------------------------------------
# Slices
# ---------------------------------------------------------------------------
def _build_slices(ctx: _Ctx, vds: dict[str, Any]) -> list[Any]:
    team = vds["v_lo_team_season"]
    elo = vds["v_lo_team_elo"]
    player = vds["v_lo_player_season"]
    transfer = vds["v_lo_transfer"]
    player_mv = vds["v_lo_player_mv"]
    team_form = vds["v_lo_team_form"]
    player_form = vds["v_lo_player_form"]
    player_race = vds["v_lo_player_race"]
    keeper = vds["v_lo_keeper_season"]

    slices: list[Any] = []

    # Common no-time-filter param for big_number_total — without it Superset
    # injects a default time window that filters everything out.
    _kpi_time = {"time_range": "No filter"}

    # Общие параметры топ-баров: горизонтальный echarts-бар — имена целиком,
    # значения справа не наезжают (вертикальный dist_bar сливал подписи).
    # Сортировка по метрике задаётся per-чарт через x_axis_sort(_asc).
    _hbar = {
        "orientation": "horizontal",
        "show_legend": False,
        "show_value": True,
        "rich_tooltip": True,
    }

    # Тултип line-чартов: только абсолютные значения. Дефолтные Total и «доля
    # от Total» бессмысленны для Elo/кумулятивных очков, а на метриках с
    # суммой около нуля (скользящий xG-баланс) доля взрывалась до 10^17 %.
    _line_tt = {
        "rich_tooltip": True,
        "showTooltipTotal": False,
        "showTooltipPercentage": False,
        # строки тултипа — по убыванию значения в наведённой точке (иначе
        # порядок = порядок серий, и на конкретную дату значения вразнобой)
        "tooltipSortByMetric": True,
        # легенда и порядок серий — по убыванию суммы серии (лидер сверху),
        # а не в порядке выборки
        "sort_series_type": "sum",
        "sort_series_ascending": False,
        # с дефолтным отступом заголовок оси X («Тур», «Дата») наезжал на тики
        "x_axis_title_margin": 30,
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
            # ",d": счётчик голов целым числом (SMART_NUMBER давал «1.05k»)
            "y_axis_format": ",d",
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

    # goals_for (standings), не goals_fbref: FBref-голы — сумма голов игроков
    # БЕЗ автоголов, на 2-4 меньше турнирных у каждой команды — весь чарт
    # уезжал в минус (19/20 команд «недобирали») и расходился с таблицей лиги.
    slices.append(_make_slice(ctx,
        "Реализация: голы − xG", "echarts_timeseries_bar", team,
        {
            "x_axis": "team_name",
            "x_axis_sort": "Голы − xG",
            "x_axis_sort_asc": True,   # horizontal: лучшие сверху
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
        "PPDA: интенсивность прессинга", "echarts_timeseries_bar", team,
        {
            "x_axis": "team_name",
            "x_axis_sort": "PPDA",
            "x_axis_sort_asc": False,  # меньше = агрессивнее, сверху
            "metrics": [_metric("PPDA", "AVG", "ppda")],
            "adhoc_filters": [_sql_where("ppda IS NOT NULL")],
            "row_limit": 25,
            **_hbar,
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
            # SMART_NUMBER: d3 ".2s" рисовал миллиарды как «1.4G»
            "x_axis_format": "SMART_NUMBER",
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
            **_line_tt,
            "truncateYAxis": True,  # без этого ось от 0 сжимает линии в полосу
            "x_axis_title": "Дата",
            "y_axis_title": "Elo (ClubElo)",
            "y_axis_format": ".0f",
        },
    ))

    # --- 10..15 Игроки: топ-15 ----------------------------------------------
    slices.append(_make_slice(ctx,
        "Топ-15 бомбардиров", "echarts_timeseries_bar", player,
        {
            "x_axis": "player_name",
            "x_axis_sort": "Голы",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Голы", "SUM", "goals")],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 по xG", "echarts_timeseries_bar", player,
        {
            "x_axis": "player_name",
            "x_axis_sort": "xG",
            "x_axis_sort_asc": True,
            "metrics": [_metric("xG", "SUM", "expected_goals")],
            "adhoc_filters": [_sql_where("expected_goals IS NOT NULL")],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": ".1f",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 ассистентов", "echarts_timeseries_bar", player,
        {
            "x_axis": "player_name",
            "x_axis_sort": "Ассисты",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Ассисты", "SUM", "assists")],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 по рейтингу SofaScore", "echarts_timeseries_bar", player,
        {
            "x_axis": "player_name",
            "x_axis_sort": "Рейтинг",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Рейтинг", "AVG", "rating_sofascore")],
            "adhoc_filters": [_sql_where(
                "minutes >= 450 AND rating_sofascore IS NOT NULL"
            )],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": ".2f",
        },
    ))

    # MAX, не SUM: при выборе нескольких сезонов SUM суммировал бы оценки
    # игрока по сезонам.
    slices.append(_make_slice(ctx,
        "Топ-15 по трансферной стоимости (€)", "echarts_timeseries_bar", player,
        {
            "x_axis": "player_name",
            "x_axis_sort": "Стоимость (€)",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Стоимость (€)", "MAX", "market_value_eur")],
            "adhoc_filters": [_sql_where("market_value_eur IS NOT NULL")],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": ".2s",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 по зарплате (€/год)", "echarts_timeseries_bar", player,
        {
            # IS NOT NULL: на сезонах без Capology (всё, кроме 2526) метрика —
            # сплошные NULL; с фильтром пустой сезон даёт чистое «No results».
            "x_axis": "player_name",
            "x_axis_sort": "Зарплата (€/год)",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Зарплата (€/год)", "MAX", "annual_gross_eur")],
            "adhoc_filters": [_sql_where("annual_gross_eur IS NOT NULL")],
            "row_limit": 15,
            **_hbar,
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
        "Топ-15 по big chances created", "echarts_timeseries_bar", player,
        {
            "x_axis": "player_name",
            "x_axis_sort": "Big chances created",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Big chances created", "SUM", "big_chances_created")],
            "adhoc_filters": [_sql_where("big_chances_created IS NOT NULL")],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Финишеры: голы − xG", "echarts_timeseries_bar", player,
        {
            "x_axis": "player_name",
            "x_axis_sort": "Голы − xG",
            "x_axis_sort_asc": True,
            "metrics": [_metric(
                "Голы − xG", "SUM", None,
                sql="SUM(goals) - SUM(expected_goals)",
            )],
            "adhoc_filters": [_sql_where(
                "minutes >= 450 AND expected_goals IS NOT NULL"
            )],
            "row_limit": 15,
            **_hbar,
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
                "expected_goals", "expected_assists",
                "ga_per90", "xg_per90", "xa_per90",
                "shots", "shots_on_target", "key_passes",
                "big_chances_created", "successful_dribbles",
                "tackles_won", "interceptions",
                "yellow_cards", "red_cards", "rating_sofascore",
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
                "ga_per90": {"d3NumberFormat": ".2f"},
                "xg_per90": {"d3NumberFormat": ".2f"},
                "xa_per90": {"d3NumberFormat": ".2f"},
                "big_chances_created": {"d3NumberFormat": ".0f"},
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

    # --- 21..26 Вкладка «Трансферы и деньги» -----------------------------------
    # Ось — «Игрок → Клуб» (deal), не игрок: два перехода одного игрока в
    # одном окне (лето + зима) остаются двумя отдельными планками.
    slices.append(_make_slice(ctx,
        "Топ-15 покупок сезона (€)", "echarts_timeseries_bar", transfer,
        {
            "x_axis": "deal",
            "x_axis_sort": "Сумма (€)",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Сумма (€)", "MAX", "fee_eur")],
            # is_incoming: без него в «покупки» попадали продажи в чужие лиги
            "adhoc_filters": [_sql_where("fee_eur IS NOT NULL AND is_incoming")],
            "row_limit": 15,
            **_hbar,
            # .3s, не .2s: суммы сделок некруглые (145M превращалось в «150M»)
            "y_axis_format": ".3s",
        },
    ))

    slices.append(_make_slice(ctx,
        "Трансферный баланс клубов (€)", "echarts_timeseries_bar", team,
        {
            "x_axis": "team_name",
            "x_axis_sort": "Баланс (€)",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Баланс (€)", "SUM", "transfer_balance_eur")],
            "adhoc_filters": [_sql_where("transfer_balance_eur IS NOT NULL")],
            "row_limit": 25,
            **_hbar,
            "y_axis_format": "+.3s",
        },
    ))

    # Series limit (не adhoc-фильтр): топ-10 серий по пиковой стоимости
    # считается ПОСЛЕ фильтров дашборда — выбранный в фильтре «Игрок»
    # показывается, даже если он не из топ-10.
    slices.append(_make_slice(ctx,
        "Динамика стоимости: топ-10 сезона (€)", "echarts_timeseries_line",
        player_mv,
        {
            "time_range": "No filter",
            "x_axis": "month_date",
            "metrics": [_metric("Стоимость (€)", "MAX", "market_value_eur")],
            "groupby": ["player_name"],
            "limit": 10,
            "timeseries_limit_metric": _metric(
                "Пик стоимости", "MAX", "market_value_eur"
            ),
            "row_limit": 50000,
            "show_legend": True,
            **_line_tt,
            "truncateYAxis": True,
            "x_axis_title": "Месяц (as-of оценка Transfermarkt)",
            "y_axis_format": ".2s",
        },
    ))

    # По сезонам (ось = дата начала сезона) — эти 2 чарта исключены из
    # фильтра «Сезон» (см. _build_native_filters), иначе была бы одна точка.
    # Series limit топ-10 (после фильтров): без него — все команды всех
    # сезонов, ~34 линии и легенда на 9 страниц; любая команда доступна
    # через фильтр «Команда».
    slices.append(_make_slice(ctx,
        "Стоимость состава по сезонам (€)", "echarts_timeseries_line", team,
        {
            "time_range": "No filter",
            "x_axis": "season_start",
            "metrics": [_metric("Стоимость состава (€)", "MAX", "squad_market_value_eur")],
            "groupby": ["team_name"],
            "limit": 10,
            "timeseries_limit_metric": _metric(
                "Пик стоимости состава", "MAX", "squad_market_value_eur"
            ),
            "adhoc_filters": [_sql_where("squad_market_value_eur IS NOT NULL")],
            "row_limit": 5000,
            "show_legend": True,
            **_line_tt,
            "x_axis_title": "Сезон (дата старта)",
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Зарплатный фонд по сезонам (€/год)", "echarts_timeseries_line", team,
        {
            "time_range": "No filter",
            "x_axis": "season_start",
            "metrics": [_metric("Фонд ЗП (€/год)", "MAX", "payroll_annual_gross_eur")],
            "groupby": ["team_name"],
            "limit": 10,
            "timeseries_limit_metric": _metric(
                "Пик фонда ЗП", "MAX", "payroll_annual_gross_eur"
            ),
            "adhoc_filters": [_sql_where("payroll_annual_gross_eur IS NOT NULL")],
            "row_limit": 5000,
            "show_legend": True,
            **_line_tt,
            "x_axis_title": "Сезон (дата старта)",
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "Все трансферы", "table", transfer,
        {
            "query_mode": "raw",
            "all_columns": [
                "transfer_date", "player_name", "from_club", "to_club",
                "transfer_type", "fee_eur", "market_value_at_transfer_eur",
            ],
            "order_by_cols": ['["transfer_date", false]'],
            "row_limit": 2000,
            "include_search": True,
            "show_cell_bars": True,
            "column_config": {
                "fee_eur": {"d3NumberFormat": ".2s"},
                "market_value_at_transfer_eur": {"d3NumberFormat": ".2s"},
                "transfer_date": {"d3TimeFormat": "%Y-%m-%d"},
            },
            "table_timestamp_format": "%Y-%m-%d",
        },
    ))

    # --- 27..30 Вкладка «Форма по турам» ---------------------------------------
    # Ось X = тур (gameweek), не дата: серии в ECharts выравниваются на общую
    # сетку X, а даты матчей у команд разные → null-разрывы в линиях. Сетка
    # туров одинакова у всех — линии сплошные.
    slices.append(_make_slice(ctx,
        "Гонка за титул: очки по турам", "echarts_timeseries_line", team_form,
        {
            "time_range": "No filter",
            "x_axis": "gameweek",
            "metrics": [_metric("Очки (кумулятивно)", "MAX", "cum_points")],
            "groupby": ["team_name"],
            "adhoc_filters": [_sql_where("gameweek IS NOT NULL")],
            "row_limit": 50000,
            "show_legend": True,
            **_line_tt,
            "x_axis_title": "Тур",
            "y_axis_format": ".0f",
        },
    ))

    slices.append(_make_slice(ctx,
        "xG-форма: скользящий баланс за 5 матчей", "echarts_timeseries_line",
        team_form,
        {
            "time_range": "No filter",
            "x_axis": "gameweek",
            "metrics": [_metric(
                "xG − xGA (среднее за 5 матчей)", "AVG", "xg_diff_rolling5"
            )],
            "groupby": ["team_name"],
            "adhoc_filters": [_sql_where("xg IS NOT NULL AND gameweek IS NOT NULL")],
            "row_limit": 50000,
            "show_legend": True,
            **_line_tt,
            "truncateYAxis": True,
            "x_axis_title": "Тур",
            "y_axis_format": "+.2f",
        },
    ))

    # Замена сырых per-match линий (шумели, не читались): «гонка бомбардиров»
    # — кумулятивные Г+А по турам, монотонные линии как у «Гонки за титул».
    # Series limit топ-8 по пиковому кумулятиву; фильтр «Игрок» покажет любого.
    slices.append(_make_slice(ctx,
        "Гонка бомбардиров: Г+А по турам", "echarts_timeseries_line",
        player_race,
        {
            "time_range": "No filter",
            "x_axis": "gameweek",
            "metrics": [_metric("Г+А (кумулятивно)", "MAX", "cum_goal_contrib")],
            "groupby": ["player_name"],
            "limit": 8,
            "timeseries_limit_metric": _metric(
                "Пик Г+А", "MAX", "cum_goal_contrib"
            ),
            "row_limit": 50000,
            "show_legend": True,
            **_line_tt,
            "x_axis_title": "Тур",
            "y_axis_format": ".0f",
        },
    ))

    # Рейтинг, сглаженный окном 5 матчей: видны спады/подъёмы формы вместо
    # пилы 6–8. Топ-8 по СРЕДНЕМУ рейтингу (не по минутам — там сплошь
    # вратари), порог season_minutes >= 900 отсекает малые выборки.
    slices.append(_make_slice(ctx,
        "Форма: рейтинг за 5 матчей", "echarts_timeseries_line",
        player_form,
        {
            "time_range": "No filter",
            "x_axis": "gameweek",
            "metrics": [_metric(
                "Рейтинг (ср. за 5 матчей)", "AVG", "rating_rolling5"
            )],
            "groupby": ["player_name"],
            "limit": 8,
            "timeseries_limit_metric": _metric("Средний рейтинг", "AVG", "rating"),
            "adhoc_filters": [_sql_where(
                "rating_rolling5 IS NOT NULL AND season_minutes >= 900"
            )],
            "row_limit": 50000,
            "show_legend": True,
            **_line_tt,
            "truncateYAxis": True,
            "x_axis_title": "Тур",
            "y_axis_format": ".2f",
        },
    ))

    # --- 31..34 Вкладка «Вратари» -----------------------------------------------
    slices.append(_make_slice(ctx,
        "Топ-15 по % сейвов", "echarts_timeseries_bar", keeper,
        {
            "x_axis": "player_name",
            "x_axis_sort": "% сейвов",
            "x_axis_sort_asc": True,
            "metrics": [_metric("% сейвов", "AVG", "save_pct")],
            "adhoc_filters": [_sql_where(
                "minutes >= 900 AND save_pct IS NOT NULL"
            )],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": ".1f",
        },
    ))

    slices.append(_make_slice(ctx,
        "Топ-15 по сухим матчам", "echarts_timeseries_bar", keeper,
        {
            "x_axis": "player_name",
            "x_axis_sort": "Сухие матчи",
            "x_axis_sort_asc": True,
            "metrics": [_metric("Сухие матчи", "SUM", "clean_sheets")],
            "adhoc_filters": [_sql_where("clean_sheets IS NOT NULL")],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": "SMART_NUMBER",
        },
    ))

    slices.append(_make_slice(ctx,
        "PSxG − пропущено: кто тащит", "echarts_timeseries_bar", keeper,
        {
            "x_axis": "player_name",
            "x_axis_sort": "PSxG − GA",
            "x_axis_sort_asc": True,
            "metrics": [_metric("PSxG − GA", "SUM", "psxg_minus_ga")],
            "adhoc_filters": [_sql_where("psxg_minus_ga IS NOT NULL")],
            "row_limit": 15,
            **_hbar,
            "y_axis_format": "+.1f",
        },
    ))

    slices.append(_make_slice(ctx,
        "Сводная таблица вратарей", "table", keeper,
        {
            "query_mode": "raw",
            "all_columns": [
                "player_name", "team_name", "matches", "minutes",
                "saves", "save_pct", "saves_per_90",
                "clean_sheets", "clean_sheet_pct",
                "goals_against", "goals_against_per90",
                "pk_faced", "pk_saved", "pk_save_pct",
                "psxg_minus_ga", "fotmob_rating",
            ],
            "order_by_cols": ['["minutes", false]'],
            "row_limit": 200,
            "include_search": True,
            "show_cell_bars": True,
            "column_config": {
                "save_pct": {"d3NumberFormat": ".1f"},
                "saves_per_90": {"d3NumberFormat": ".2f"},
                "clean_sheet_pct": {"d3NumberFormat": ".1f"},
                "goals_against_per90": {"d3NumberFormat": ".2f"},
                "pk_save_pct": {"d3NumberFormat": ".1f"},
                "psxg_minus_ga": {"d3NumberFormat": "+.1f"},
                "fotmob_rating": {"d3NumberFormat": ".2f"},
            },
        },
    ))

    return slices


# ---------------------------------------------------------------------------
# Layout (position_json) — grid 12-wide rows
# ---------------------------------------------------------------------------
def _build_position_json(slices: list[Any]) -> dict[str, Any]:
    """Grid v2 с вкладками: шапка над TABS, пять TAB-контейнеров внутри."""
    pos: dict[str, Any] = {"DASHBOARD_VERSION_KEY": "v2"}

    root_id = "ROOT_ID"
    grid_id = "GRID_ID"
    tabs_id = "TABS-lo"
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
        # Высоты: 1 юнит ≈ 8px; H2 + строка текста не влезают в 3-4 юнита
        # (текст обрезался) — блокам с подзаголовком нужно ~12.
        mid = f"MARKDOWN-{abs(hash(text)) % (10**8)}"
        pos[mid] = {
            "type": "MARKDOWN",
            "id": mid,
            "children": [],
            "parents": parents,
            "meta": {"width": 12, "height": height, "code": text, "background": "BACKGROUND_TRANSPARENT"},
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
        tid = f"TAB-lo-{key}"
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

    # --- Шапка над вкладками -------------------------------------------------
    header = _markdown(
        [root_id, grid_id],
        "# Обзор лиги + игроки\n\n"
        "Мультилиговый обзор: таблица, командные метрики, Elo, игроки, "
        "трансферы, форма и вратари. Источники: FBref · Understat (xG, xPTS, PPDA) · "
        "SofaScore (рейтинги, дуэли) · FotMob · Transfermarkt (стоимость, контракты) · "
        "Capology (зарплаты) · ClubElo.",
        height=10,
    )
    pos[grid_id]["children"] = [header, tabs_id]

    # --- Таб 1. Обзор лиги -----------------------------------------------------
    c, p = _tab("overview", "Обзор лиги")
    c.append(_row(p, [_chart(slices[i], 3, height=25) for i in range(0, 4)]))
    c.append(_markdown(p,
        "## Таблица лиги\n\n"
        "«Сверх-очки» = очки − xPTS (Understat): плюс — команда набирает больше, "
        "чем заслуживает по качеству моментов (везёт), минус — недобирает.",
    ))
    c.append(_row(p, [_chart(slices[4], 12, height=70)]))
    c.append(_markdown(p, "## Команды", height=8))
    c.append(_row(p, [_chart(slices[5], 6, height=55), _chart(slices[6], 6, height=55)]))
    c.append(_row(p, [_chart(slices[7], 6, height=55), _chart(slices[8], 6, height=55)]))
    c.append(_markdown(p,
        "## Динамика силы — Elo\n\n"
        "Недельное среднее ClubElo в границах выбранного сезона.",
    ))
    c.append(_row(p, [_chart(slices[9], 12, height=60)]))

    # --- Таб 2. Игроки -----------------------------------------------------------
    c, p = _tab("players", "Игроки")
    c.append(_markdown(p,
        "## Игроки — топы\n\n"
        "⚠️ Зарплаты (Capology) есть только для сезона 2025/26 — "
        "на других сезонах бар зарплат и колонки зарплат пусты.",
    ))
    c.append(_row(p, [_chart(slices[i], 4, height=50) for i in range(10, 13)]))
    c.append(_row(p, [_chart(slices[i], 4, height=50) for i in range(13, 16)]))
    c.append(_markdown(p,
        "## Аналитика игроков\n\n"
        "Scatter'ы — минимум 270 сыгранных минут; рейтинг и «финишеры» — минимум 450. "
        "В таблице %-метрики, рейтинг и per-90 показываются только при 270+ минут.",
    ))
    c.append(_row(p, [_chart(slices[16], 6, height=60), _chart(slices[17], 6, height=60)]))
    c.append(_row(p, [_chart(slices[18], 6, height=50), _chart(slices[19], 6, height=50)]))
    c.append(_row(p, [_chart(slices[20], 12, height=90)]))

    # --- Таб 3. Трансферы и деньги -----------------------------------------------
    c, p = _tab("money", "Трансферы и деньги")
    c.append(_markdown(p,
        "## Трансферы и деньги\n\n"
        "Сезон сделки = трансферное окно июль—июнь (переход 1 июля 2026 — "
        "уже сезон 2026/27). Суммы сделок публичны примерно у каждой шестой "
        "(Transfermarkt); баланс клуба = доход − расход за сезонное окно. "
        "Топ покупок — только входящие в лигу сделки; клуб и тип сделки — "
        "фильтры «Куда/Откуда (клуб)» и «Тип сделки».",
    ))
    c.append(_row(p, [_chart(slices[21], 6, height=55), _chart(slices[22], 6, height=55)]))
    c.append(_markdown(p,
        "## Динамика стоимости\n\n"
        "Топ-10 сезона по пиковой стоимости; выбери игрока в фильтре «Игрок», "
        "чтобы посмотреть его линию. Точка месяца = последняя оценка "
        "Transfermarkt на его конец. Графики «по сезонам» не зависят от фильтра «Сезон».",
    ))
    c.append(_row(p, [_chart(slices[23], 12, height=55)]))
    c.append(_row(p, [_chart(slices[24], 6, height=55), _chart(slices[25], 6, height=55)]))
    c.append(_row(p, [_chart(slices[26], 12, height=85)]))

    # --- Таб 4. Форма по турам -----------------------------------------------------
    c, p = _tab("form", "Форма по турам")
    c.append(_markdown(p,
        "## Форма команд\n\n"
        "Гонка за титул — кумулятивные очки; xG-форма — скользящий баланс "
        "xG − xGA за последние 5 матчей (выше нуля = создаёт больше, чем допускает).",
    ))
    c.append(_row(p, [_chart(slices[27], 12, height=60)]))
    c.append(_row(p, [_chart(slices[28], 12, height=55)]))
    c.append(_markdown(p,
        "## Форма игрока\n\n"
        "Гонка бомбардиров — кумулятивные гол+пас по турам (топ-8 сезона). "
        "Форма — рейтинг SofaScore, сглаженный за последние 5 матчей "
        "(топ-8 по среднему рейтингу, минимум 900 минут). "
        "Выбери игрока в фильтре «Игрок», чтобы посмотреть любого.",
    ))
    c.append(_row(p, [_chart(slices[29], 6, height=55), _chart(slices[30], 6, height=55)]))

    # --- Таб 5. Вратари ---------------------------------------------------------------
    c, p = _tab("keepers", "Вратари")
    c.append(_markdown(p,
        "## Вратари\n\n"
        "PSxG − GA (FBref) — «вытащенные» голы сверх ожидаемого; "
        "метрика заполнена не для всех сезонов.",
    ))
    c.append(_row(p, [_chart(slices[31], 4, height=50), _chart(slices[32], 4, height=50), _chart(slices[33], 4, height=50)]))
    c.append(_row(p, [_chart(slices[34], 12, height=70)]))

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
    """Восемь фильтров: Лига → Сезон → Команда → Позиция → Игрок +
    трансферные (Тип сделки, Куда/Откуда (клуб)).

    Scope каждого фильтра — только чарты, в чьём датасете есть колонка
    фильтра (фильтр по отсутствующей колонке = 500). Собирается по фактическим
    колонкам SqlaTable, а не по индексам слайсов. Чарты «по сезонам» вручную
    исключены из фильтра «Сезон» (иначе на графике одна точка).

    Значения «Позиция»/«Игрок» — из v_lo_player_form (по-матчевый грейн),
    а не v_lo_player_season: в fct_player_season_stats нет вратарей, из-за
    чего GK отсутствовал в опциях, а на вкладке «Вратари» фильтр «Игрок»
    был бесполезен (вратаря не выбрать, полевой обнулял чарты).
    """
    team_ds_id = vds["v_lo_team_season"].id
    player_ds_id = vds["v_lo_player_form"].id
    transfer_ds_id = vds["v_lo_transfer"].id
    league_club_ds_id = vds["v_lo_league_club"].id

    ds_columns = {
        t.id: {c.column_name for c in t.columns} for t in vds.values()
    }
    by_season_slices = {
        "Стоимость состава по сезонам (€)",
        "Зарплатный фонд по сезонам (€/год)",
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

    f_league = "NATIVE_FILTER-lo-league"
    f_season = "NATIVE_FILTER-lo-season"
    f_team = "NATIVE_FILTER-lo-team"
    f_position = "NATIVE_FILTER-lo-position"

    return [
        _filter(f_league, "Лига", "league", team_ds_id,
                multiple=False, target_chart_ids=_charts_with("league")),
        _filter(f_season, "Сезон", "season", team_ds_id,
                multiple=False,
                target_chart_ids=_charts_with("season", by_season_slices),
                cascade_parent_ids=[f_league],
                required=True, default_value=current_season),
        _filter(f_team, "Команда", "team_name", team_ds_id,
                multiple=True, target_chart_ids=_charts_with("team_name"),
                cascade_parent_ids=[f_league, f_season]),
        _filter(f_position, "Позиция", "position_primary", player_ds_id,
                multiple=True,
                target_chart_ids=_charts_with("position_primary"),
                cascade_parent_ids=[f_league, f_season]),
        _filter("NATIVE_FILTER-lo-player", "Игрок", "player_name",
                player_ds_id,
                multiple=True, target_chart_ids=_charts_with("player_name"),
                cascade_parent_ids=[f_league, f_season, f_team, f_position]),
        # Трансферные: «Команда» к сделкам неприменима (нет team_name),
        # клуб выбирается через Куда/Откуда; тип отсекает арендные возвраты,
        # которыми забит верх «Всех трансферов» (30 июня, суммы N/A).
        _filter("NATIVE_FILTER-lo-ttype", "Тип сделки", "transfer_type",
                transfer_ds_id,
                multiple=True, target_chart_ids=_charts_with("transfer_type")),
        # Опции берём из v_lo_league_club (клубы лиги, ≈20), а не из v_lo_transfer
        # (600+ клубов со всего мира). Колонка to_club/from_club та же → фильтр
        # по-прежнему применяется к трансферным чартам; каскад по league/season.
        _filter("NATIVE_FILTER-lo-toclub", "Куда (клуб)", "to_club",
                league_club_ds_id,
                multiple=True, target_chart_ids=_charts_with("to_club"),
                cascade_parent_ids=[f_league, f_season]),
        _filter("NATIVE_FILTER-lo-fromclub", "Откуда (клуб)", "from_club",
                league_club_ds_id,
                multiple=True, target_chart_ids=_charts_with("from_club"),
                cascade_parent_ids=[f_league, f_season]),
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
