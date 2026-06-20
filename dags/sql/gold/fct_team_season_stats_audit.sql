-- =============================================================================
-- Gold: fct_team_season_stats_audit
-- =============================================================================
--
-- DQ-audit таблица для cross-source согласованности по HARD_FACT метрикам в
-- `gold.fct_team_season_stats`. НЕ business-витрина: содержит ТОЛЬКО
-- технические diff-колонки + PK.
--
-- Convention: diff = FBref - <source> для каждого HARD_FACT (FBref — primary
-- spine на team-grain). Для MODELED xG diff'ы вычисляются между Understat и
-- SofaScore (Understat = primary per RX2; FBref не отдаёт xG на team-level).
--
-- Зерно: (team_id, league, season). INNER JOIN FBref ∩ Understat
-- (Understat = primary secondary с coverage 100% APL). WhoScored / SofaScore /
-- FotMob (#97) — LEFT JOIN → diff = NULL когда источник отсутствует. FotMob xref
-- season — slug '2526' (#404); fm_team_season.season — slug '2526'.
--
-- Использование:
--   1. DQ coverage WARNING — ABS(diff) <= threshold у ≥95% rows.
--   2. Engineer-debug при «не сходятся goals/shots в дашборде».
--   3. R1 калибровка thresholds → docs/research/R1_cross_source_thresholds.md.
--
-- Audit-таблица читает Silver заново (НЕ gold.fct_team_season_stats):
--   one-hop правило (project_gold_cleanup_2026-05-12).
--
-- #478: производный gold-этаж удалён — per-source season rollups (бывшие
-- gold.*_team_season, #370) инлайнены ниже как УСЕЧЁННЫЕ CTE (только колонки,
-- нужные diff'ам; выражения дословно из main-файла). Без ws_penalties — аудит
-- пенальти не сравнивает и bronze.whoscored_events не сканирует.
-- ⚠️ Синхронизировать вручную с fct_team_season_stats.sql.
--
-- #556: остаётся inline .sql (НЕ мигрирован на source_priority.yaml) — per-source
-- diff-layout несовместим с single-COALESCE эмиттером. Sync усечённых CTE выше
-- закреплён тестом TestAuditCteSync. Решение: docs/decisions/season-audit-inline.md
-- =============================================================================

WITH
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_team_name,
        league,
        -- #404: silver.xref_team.season is slug now → season_year & season_slug
        -- are both the same slug ('2425'); the legacy year-start build is gone.
        season                                            AS season_year,
        season                                            AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

xref_ws AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS ws_team_name,
        league,
        season                                            AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'whoscored'
      AND confidence <> 'orphan'
),

xref_ss AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS ss_team_name,
        league,
        season                                            AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
),

-- FotMob xref (#97). season is slug '2526' after #404 → JOIN on
-- CAST(season_year AS varchar) (slug now); fm_team_season.season is slug too.
xref_fm AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fm_team_name,
        league,
        season                                            AS season_year_str
    FROM iceberg.silver.xref_team
    WHERE source = 'fotmob'
      AND confidence <> 'orphan'
),

-- WhoScored numeric↔name mapping (see main fct file header for KNOWN GAP).
ws_name_to_id AS (
    SELECT DISTINCT CAST(home_team_id AS varchar) AS ws_team_id,
                    home_team                     AS ws_team_name,
                    league, season
    FROM iceberg.bronze.whoscored_schedule
    WHERE home_team_id IS NOT NULL
    UNION
    SELECT DISTINCT CAST(away_team_id AS varchar) AS ws_team_id,
                    away_team                     AS ws_team_name,
                    league, season
    FROM iceberg.bronze.whoscored_schedule
    WHERE away_team_id IS NOT NULL
),

-- ===== Inlined per-source season aggregates (#478, усечены до diff-колонок) =====

us_team_season AS (
    SELECT
        team_id_canonical,
        CAST(COUNT(*) AS INTEGER)           AS games_played,
        CAST(SUM(goals)         AS INTEGER) AS goals,
        CAST(SUM(goals_against) AS INTEGER) AS goals_against,
        SUM(xg)                             AS xg,
        SUM(xg_against)                     AS xg_against,
        league,
        season
    FROM iceberg.silver.understat_team_match
    GROUP BY team_id_canonical, league, season
),

ws_season_rollup AS (
    SELECT
        team_id,
        COUNT(*)                   AS matches_seen,
        SUM(shots_total)           AS shots_total,
        SUM(shots_on_target_proxy) AS shots_on_target_proxy,
        SUM(interceptions)         AS interceptions,
        SUM(tackle_won)            AS tackle_won,
        SUM(fouls_committed)       AS fouls_committed,
        league,
        season
    FROM iceberg.silver.whoscored_team_match
    GROUP BY team_id, league, season
),

ss_team_season AS (
    SELECT
        team_id,
        league,
        season,
        CAST(COUNT(*)              AS INTEGER) AS appearances,
        CAST(SUM(goals_for)        AS INTEGER) AS goals,
        CAST(SUM(goals_against)    AS INTEGER) AS goals_conceded,
        CAST(SUM(assists)          AS INTEGER) AS assists,
        CAST(SUM(yellow_cards)     AS INTEGER) AS yellow_cards,
        CAST(SUM(red_cards)        AS INTEGER) AS red_cards,
        CAST(SUM(total_shots)      AS INTEGER) AS total_shots,
        CAST(SUM(shots_on_target)  AS INTEGER) AS shots_on_target,
        CAST(SUM(interceptions)    AS INTEGER) AS interceptions,
        CAST(SUM(tackles_won)      AS INTEGER) AS tackles_won,
        CAST(SUM(fouls)            AS INTEGER) AS fouls_committed,
        CAST(SUM(offsides)         AS INTEGER) AS offsides,
        CAST(SUM(total_crosses)    AS INTEGER) AS total_crosses,
        ROUND(SUM(expected_goals),         2)  AS expected_goals,
        ROUND(SUM(expected_goals_against), 2)  AS expected_goals_against
    FROM iceberg.silver.sofascore_team_match
    WHERE team_id IS NOT NULL
    GROUP BY team_id, league, season
),

fm_team_season AS (
    SELECT
        team_id,
        CAST(COUNT(*)              AS INTEGER) AS appearances,
        CAST(SUM(goals_for)        AS INTEGER) AS goals,
        CAST(SUM(goals_against)    AS INTEGER) AS goals_conceded,
        CAST(SUM(total_shots)      AS INTEGER) AS total_shots,
        CAST(SUM(shots_on_target)  AS INTEGER) AS shots_on_target,
        ROUND(SUM(expected_goals), 4)          AS expected_goals,
        league,
        season
    FROM iceberg.silver.fotmob_team_match
    GROUP BY team_id, league, season
)

SELECT
    -- ========= PK (грейн совпадает с fct_team_season_stats) =========
    xf.canonical_id                                      AS team_id,
    xf.league                                            AS league,
    xf.season_year                                       AS season,

    -- ========= Understat diff (INNER JOIN — всегда non-NULL) =========
    (CAST(fb.mp                AS DOUBLE) - CAST(us.games_played AS DOUBLE)) AS matches_diff_understat,
    (CAST(fb.goals             AS DOUBLE) - CAST(us.goals         AS DOUBLE)) AS goals_diff_understat,
    (CAST(fb.gk_goals_against  AS DOUBLE) - CAST(us.goals_against AS DOUBLE)) AS goals_against_diff_understat,
    -- Understat не агрегирует shots в season-grain → no shots_diff_understat.

    -- ========= WhoScored diff (LEFT JOIN → NULL when absent) =========
    (CAST(fb.mp              AS DOUBLE) - CAST(ws.matches_seen        AS DOUBLE)) AS matches_diff_whoscored,
    (CAST(fb.total_shots     AS DOUBLE) - CAST(ws.shots_total         AS DOUBLE)) AS shots_diff_whoscored,
    (CAST(fb.shots_on_target AS DOUBLE) - CAST(ws.shots_on_target_proxy AS DOUBLE)) AS shots_on_target_diff_whoscored,
    (CAST(fb.interceptions   AS DOUBLE) - CAST(ws.interceptions       AS DOUBLE)) AS interceptions_diff_whoscored,
    (CAST(fb.tackles_won     AS DOUBLE) - CAST(ws.tackle_won          AS DOUBLE)) AS tackles_won_diff_whoscored,
    (CAST(fb.fouls_committed AS DOUBLE) - CAST(ws.fouls_committed     AS DOUBLE)) AS fouls_committed_diff_whoscored,

    -- ========= SofaScore diff (LEFT JOIN → NULL when absent) =========
    (CAST(fb.mp                AS DOUBLE) - CAST(ss.appearances       AS DOUBLE)) AS matches_diff_sofascore,
    (CAST(fb.goals             AS DOUBLE) - CAST(ss.goals             AS DOUBLE)) AS goals_diff_sofascore,
    (CAST(fb.gk_goals_against  AS DOUBLE) - CAST(ss.goals_conceded    AS DOUBLE)) AS goals_against_diff_sofascore,
    (CAST(fb.assists           AS DOUBLE) - CAST(ss.assists           AS DOUBLE)) AS assists_diff_sofascore,
    (CAST(fb.yellow_cards      AS DOUBLE) - CAST(ss.yellow_cards      AS DOUBLE)) AS yellow_cards_diff_sofascore,
    (CAST(fb.red_cards         AS DOUBLE) - CAST(ss.red_cards         AS DOUBLE)) AS red_cards_diff_sofascore,
    (CAST(fb.total_shots       AS DOUBLE) - CAST(ss.total_shots       AS DOUBLE)) AS shots_diff_sofascore,
    (CAST(fb.shots_on_target   AS DOUBLE) - CAST(ss.shots_on_target   AS DOUBLE)) AS shots_on_target_diff_sofascore,
    (CAST(fb.interceptions     AS DOUBLE) - CAST(ss.interceptions     AS DOUBLE)) AS interceptions_diff_sofascore,
    (CAST(fb.tackles_won       AS DOUBLE) - CAST(ss.tackles_won       AS DOUBLE)) AS tackles_won_diff_sofascore,
    (CAST(fb.fouls_committed   AS DOUBLE) - CAST(ss.fouls_committed   AS DOUBLE)) AS fouls_committed_diff_sofascore,
    (CAST(fb.offsides          AS DOUBLE) - CAST(ss.offsides          AS DOUBLE)) AS offsides_diff_sofascore,
    (CAST(fb.crosses           AS DOUBLE) - CAST(ss.total_crosses     AS DOUBLE)) AS crosses_diff_sofascore,

    -- ========= MODELED xG diff (different models, expected to disagree) =========
    -- FBref не отдаёт team-level xG → primary = Understat, secondary = SofaScore.
    ROUND(CAST(us.xg         AS DOUBLE) - CAST(ss.expected_goals         AS DOUBLE), 4) AS xg_diff_us_vs_ss,
    ROUND(CAST(us.xg_against AS DOUBLE) - CAST(ss.expected_goals_against AS DOUBLE), 4) AS xg_against_diff_us_vs_ss,

    -- ========= FotMob diff (LEFT JOIN → NULL when absent; #97) =========
    (CAST(fb.mp                AS DOUBLE) - CAST(fm.appearances    AS DOUBLE)) AS matches_diff_fotmob,
    (CAST(fb.goals             AS DOUBLE) - CAST(fm.goals          AS DOUBLE)) AS goals_diff_fotmob,
    (CAST(fb.gk_goals_against  AS DOUBLE) - CAST(fm.goals_conceded AS DOUBLE)) AS goals_against_diff_fotmob,
    (CAST(fb.total_shots       AS DOUBLE) - CAST(fm.total_shots    AS DOUBLE)) AS shots_diff_fotmob,
    (CAST(fb.shots_on_target   AS DOUBLE) - CAST(fm.shots_on_target AS DOUBLE)) AS shots_on_target_diff_fotmob,
    -- xG cross-model: Understat (primary) vs FotMob.
    ROUND(CAST(us.xg           AS DOUBLE) - CAST(fm.expected_goals AS DOUBLE), 4) AS xg_diff_us_vs_fm,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                     AS _gold_created_at

FROM xref_fbref xf
INNER JOIN iceberg.silver.fbref_team_season_profile fb
    ON  fb.team    = xf.fbref_team_name
    AND fb.league  = xf.league
    AND fb.season  = xf.season_year
INNER JOIN us_team_season us
    ON  us.team_id_canonical = xf.canonical_id
    AND us.league            = xf.league
    AND us.season            = xf.season_slug
LEFT JOIN xref_ws xw
    ON  xw.canonical_id = xf.canonical_id
    AND xw.league       = xf.league
    AND xw.season_slug  = xf.season_slug
LEFT JOIN ws_name_to_id wn
    ON  wn.ws_team_name = xw.ws_team_name
    AND wn.league       = xw.league
    AND wn.season       = xw.season_slug
LEFT JOIN ws_season_rollup ws
    ON  ws.team_id = wn.ws_team_id
    AND ws.league  = wn.league
    AND ws.season  = wn.season
LEFT JOIN xref_ss xs
    ON  xs.canonical_id = xf.canonical_id
    AND xs.league       = xf.league
    AND xs.season_slug  = xf.season_slug
LEFT JOIN ss_team_season ss
    ON  ss.team_id = xs.ss_team_name
    AND ss.league  = xs.league
    AND ss.season  = xs.season_slug
LEFT JOIN xref_fm xfm
    ON  xfm.canonical_id    = xf.canonical_id
    AND xfm.league          = xf.league
    AND xfm.season_year_str = CAST(xf.season_year AS varchar)
LEFT JOIN fm_team_season fm
    ON  fm.team_id = xfm.fm_team_name
    AND fm.league  = xf.league
    AND fm.season  = xf.season_slug
