-- =============================================================================
-- Gold: fct_team_match_audit
-- =============================================================================
--
-- DQ-audit таблица для cross-source согласованности по HARD_FACT метрикам в
-- `gold.fct_team_match`. НЕ business-витрина: содержит ТОЛЬКО технические
-- diff-колонки + PK (без COALESCE, без UNIQUE_*).
--
-- Design contract: docs/decisions/T6_team_facts_schema.md §8.3 (issue #95).
--
-- Convention:
--   diff = FBref - <source> где FBref имеет метрику (primary spine).
--   Для метрик, которых нет у FBref на team-match-grain (corners / passes
--   total / duels / fouls), diff = SS - <source> (SofaScore secondary).
--   xG / xA — кросс-моделированные diff'ы между US ↔ SS.
--
-- Grain: (match_id, team_id). Один row per канонический
-- матч × команда — **только когда обе стороны spine имеют запись** (INNER JOIN
-- FBref ∩ Understat). WhoScored / SofaScore — LEFT JOIN → diff = NULL когда
-- источник отсутствует. WS-блок ожидаемо NULL для current seasons (см.
-- feedback_whoscored_team_id_numeric_vs_xref_name.md, followup #120).
--
-- Использование:
--   1. DQ coverage WARNING — ABS(diff) <= threshold у ≥95% rows.
--   2. Engineer-debug при «голы по матчу не сходятся в дашборде».
--   3. Калибровка thresholds → docs/research/R1_cross_source_thresholds.md.
--
-- xref-JOIN footgun (feedback_xref_join_season_predicate.md):
--   silver.xref_team / xref_match хранят per-(source, source_id, season) rows
--   → ВСЕ xref JOIN обязаны иметь (league, season) predicate.
--
-- Season normalization (feedback_xref_team_source_id_format.md):
--   #404 unified every silver/xref/gold season onto the slug form 'YYNN' ('2526'):
--   xref_team / xref_match / silver.fbref_match_enriched / silver.fotmob_team_match
--   all expose slug now (bronze fotmob_schedule.season is still bigint year-start).
--   → all bridge + fact JOINs are direct slug = slug (no LPAD/MOD conversion).
--
-- PK: (match_id, team_id) — natural composite, без xxhash64
-- (оба компонента non-NULL по конструкции INNER spine FBref ∩ Understat).
--
-- Audit-таблица читает Silver заново (НЕ gold.fct_team_match) per one-hop
-- правило (memory: project_gold_cleanup_2026-05-12).
-- =============================================================================

WITH
-- ===== Spine: FBref — flatten home/away to long form =====
fb_home AS (
    SELECT
        m.match_id,
        m.league,
        m.season                              AS season_year,
        m.home                                AS fb_team_name,
        m.away                                AS fb_opponent_name,
        m.home_score                          AS goals_for,
        m.away_score                          AS goals_against,
        m.home_shots                          AS shots,
        m.home_sot                            AS shots_on_target,
        m.home_possession                     AS possession,
        m.home_yellow_cards                   AS yellow_cards,
        m.home_red_cards                      AS red_cards,
        m.home_saves                          AS saves
    FROM iceberg.silver.fbref_match_enriched m
),
fb_away AS (
    SELECT
        m.match_id,
        m.league,
        m.season                              AS season_year,
        m.away                                AS fb_team_name,
        m.home                                AS fb_opponent_name,
        m.away_score                          AS goals_for,
        m.home_score                          AS goals_against,
        m.away_shots                          AS shots,
        m.away_sot                            AS shots_on_target,
        m.away_possession                     AS possession,
        m.away_yellow_cards                   AS yellow_cards,
        m.away_red_cards                      AS red_cards,
        m.away_saves                          AS saves
    FROM iceberg.silver.fbref_match_enriched m
),
fb_team AS (
    SELECT *,
        -- #404: silver.fbref_match_enriched.season is slug now → season_year is
        -- already slug, so season_slug is the same value (no year-start build).
        season_year AS season_slug
    FROM (
        SELECT * FROM fb_home
        UNION ALL
        SELECT * FROM fb_away
    )
),

-- ===== xref bridges =====
xref_team_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                            AS fbref_team_name,
        league,
        season                                               AS fbref_season_year_str
    FROM iceberg.silver.xref_team
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

xref_team_us AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                            AS us_team_name,
        league,
        season                                               AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'understat'
      AND confidence <> 'orphan'
),

xref_match_us AS (
    SELECT DISTINCT
        canonical_id                                         AS match_id_canonical,
        source_id                                            AS us_match_id,
        league,
        season                                               AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'understat'
      AND confidence <> 'orphan'
),

xref_team_ws AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                            AS ws_team_name,
        league,
        season                                               AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'whoscored'
      AND confidence <> 'orphan'
),

xref_match_ws AS (
    SELECT DISTINCT
        canonical_id                                         AS match_id_canonical,
        source_id                                            AS ws_match_id,
        league,
        season                                               AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'whoscored'
      AND confidence <> 'orphan'
),

xref_team_ss AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                            AS ss_team_name,
        league,
        season                                               AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
),

xref_match_ss AS (
    SELECT DISTINCT
        canonical_id                                         AS match_id_canonical,
        source_id                                            AS ss_match_id,
        league,
        season                                               AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
),

-- FotMob xref (#97). season is slug '2526' after #404 — JOIN below on
-- CAST(season_year AS varchar) (slug now); the fact season is slug '2526' too.
xref_team_fm AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                            AS fm_team_name,
        league,
        season                                               AS season_year
    FROM iceberg.silver.xref_team
    WHERE source = 'fotmob'
      AND confidence <> 'orphan'
),

xref_match_fm AS (
    SELECT DISTINCT
        canonical_id                                         AS match_id_canonical,
        source_id                                            AS fm_match_id,
        league,
        season                                               AS season_year
    FROM iceberg.silver.xref_match
    WHERE source = 'fotmob'
      AND confidence <> 'orphan'
),

-- WS team_id NUMERIC ↔ NAME bridge (same fail-soft pattern as fct_team_match)
ws_team_name_bridge AS (
    SELECT DISTINCT
        CAST(CAST(home_team_id AS BIGINT) AS varchar) AS ws_team_id_numeric,
        home_team                                     AS ws_team_name,
        league,
        season
    FROM iceberg.bronze.whoscored_schedule
    WHERE home_team_id IS NOT NULL

    UNION

    SELECT DISTINCT
        CAST(CAST(away_team_id AS BIGINT) AS varchar),
        away_team,
        league,
        season
    FROM iceberg.bronze.whoscored_schedule
    WHERE away_team_id IS NOT NULL
)

SELECT
    -- ========= PK (natural composite, same names as fct_team_match; #442) =========
    fb.match_id                                          AS match_id,
    xtf.canonical_id                                     AS team_id,

    -- ========= vs Understat (INNER spine — always non-NULL) =========
    (CAST(fb.goals_for         AS DOUBLE) - CAST(us.goals          AS DOUBLE)) AS goals_for_diff_us,
    (CAST(fb.goals_against     AS DOUBLE) - CAST(us.goals_against  AS DOUBLE)) AS goals_against_diff_us,
    -- xG cross-model diff (Understat vs SofaScore); diff = US - SS per RX2 convention.
    ROUND(CAST(us.xg          AS DOUBLE) - CAST(ss.expected_goals         AS DOUBLE), 4) AS xg_diff_us_ss,
    ROUND(CAST(us.xg_against  AS DOUBLE) - CAST(ss.expected_goals_against AS DOUBLE), 4) AS xga_diff_us_ss,

    -- ========= vs SofaScore (LEFT — NULL if absent) =========
    (CAST(fb.goals_for         AS DOUBLE) - CAST(ss.goals_for       AS DOUBLE)) AS goals_for_diff_ss,
    (CAST(fb.goals_against     AS DOUBLE) - CAST(ss.goals_against   AS DOUBLE)) AS goals_against_diff_ss,
    (CAST(fb.shots             AS DOUBLE) - CAST(ss.total_shots     AS DOUBLE)) AS shots_diff_ss,
    (CAST(fb.shots_on_target   AS DOUBLE) - CAST(ss.shots_on_target AS DOUBLE)) AS shots_on_target_diff_ss,
    (CAST(fb.yellow_cards      AS DOUBLE) - CAST(ss.yellow_cards    AS DOUBLE)) AS yellow_cards_diff_ss,
    (CAST(fb.red_cards         AS DOUBLE) - CAST(ss.red_cards       AS DOUBLE)) AS red_cards_diff_ss,
    (CAST(fb.possession        AS DOUBLE) - CAST(ss.possession_pct  AS DOUBLE)) AS possession_diff_ss,

    -- ========= vs WhoScored (LEFT — NULL expected for current seasons; #120) =========
    (CAST(fb.shots             AS DOUBLE) - CAST(ws.shots_total            AS DOUBLE)) AS shots_diff_ws,
    (CAST(fb.shots_on_target   AS DOUBLE) - CAST(ws.shots_on_target_proxy  AS DOUBLE)) AS shots_on_target_diff_ws,
    -- FBref does not expose team-grain fouls; use SS as baseline for SS↔WS comparison.
    (CAST(ss.fouls             AS DOUBLE) - CAST(ws.fouls_committed         AS DOUBLE)) AS fouls_diff_ss_ws,

    -- ========= vs FotMob (LEFT — NULL if absent; #97) =========
    (CAST(fb.goals_for         AS DOUBLE) - CAST(fm.goals_for       AS DOUBLE)) AS goals_for_diff_fm,
    (CAST(fb.goals_against     AS DOUBLE) - CAST(fm.goals_against   AS DOUBLE)) AS goals_against_diff_fm,
    (CAST(fb.shots             AS DOUBLE) - CAST(fm.total_shots     AS DOUBLE)) AS shots_diff_fm,
    (CAST(fb.shots_on_target   AS DOUBLE) - CAST(fm.shots_on_target AS DOUBLE)) AS shots_on_target_diff_fm,
    (CAST(fb.possession        AS DOUBLE) - CAST(fm.possession_pct  AS DOUBLE)) AS possession_diff_fm,
    (CAST(fb.yellow_cards      AS DOUBLE) - CAST(fm.yellow_cards    AS DOUBLE)) AS yellow_cards_diff_fm,
    (CAST(fb.red_cards         AS DOUBLE) - CAST(fm.red_cards       AS DOUBLE)) AS red_cards_diff_fm,
    -- xG cross-model diff (Understat vs FotMob); diff = US - FM.
    ROUND(CAST(us.xg           AS DOUBLE) - CAST(fm.expected_goals  AS DOUBLE), 4) AS xg_diff_us_fm,
    -- passes / corners / offsides: FBref has no team-grain → SS baseline vs FotMob.
    (CAST(ss.total_passes      AS DOUBLE) - CAST(fm.total_passes    AS DOUBLE)) AS passes_diff_ss_fm,
    (CAST(ss.corner_kicks      AS DOUBLE) - CAST(fm.corner_kicks    AS DOUBLE)) AS corners_diff_ss_fm,
    (CAST(ss.fouls             AS DOUBLE) - CAST(fm.fouls           AS DOUBLE)) AS fouls_diff_ss_fm,
    (CAST(ss.offsides          AS DOUBLE) - CAST(fm.offsides        AS DOUBLE)) AS offsides_diff_ss_fm,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                     AS _gold_created_at,

    -- ========= Partition keys (LAST in SELECT) =========
    fb.league                                             AS league,
    fb.season_slug                                        AS season

FROM fb_team fb

-- ===== FBref → canonical team_id (xref_team source='fbref') =====
INNER JOIN xref_team_fbref xtf
    ON  xtf.fbref_team_name        = fb.fb_team_name
    AND xtf.league                 = fb.league
    AND xtf.fbref_season_year_str  = CAST(fb.season_year AS varchar)

-- ===== Understat INNER spine =====
INNER JOIN xref_team_us xtu
    ON  xtu.canonical_id = xtf.canonical_id
    AND xtu.league       = fb.league
    AND xtu.season_slug  = fb.season_slug
INNER JOIN xref_match_us xmu
    ON  xmu.match_id_canonical = fb.match_id
    AND xmu.league             = fb.league
    AND xmu.season_slug        = fb.season_slug
INNER JOIN iceberg.silver.understat_team_match us
    ON  us.match_id          = xmu.us_match_id
    AND us.team_id_canonical = xtf.canonical_id
    AND us.league            = fb.league
    AND us.season            = fb.season_slug

-- ===== SofaScore LEFT =====
LEFT JOIN xref_team_ss xts
    ON  xts.canonical_id = xtf.canonical_id
    AND xts.league       = fb.league
    AND xts.season_slug  = fb.season_slug
LEFT JOIN xref_match_ss xms
    ON  xms.match_id_canonical = fb.match_id
    AND xms.league             = fb.league
    AND xms.season_slug        = fb.season_slug
LEFT JOIN iceberg.silver.sofascore_team_match ss
    ON  ss.match_id = xms.ss_match_id
    AND ss.team_id  = xts.ss_team_name
    AND ss.league   = fb.league
    AND ss.season   = fb.season_slug

-- ===== WhoScored LEFT (fail-soft) =====
LEFT JOIN xref_team_ws xtw
    ON  xtw.canonical_id = xtf.canonical_id
    AND xtw.league       = fb.league
    AND xtw.season_slug  = fb.season_slug
LEFT JOIN ws_team_name_bridge wsb
    ON  wsb.ws_team_name = xtw.ws_team_name
    AND wsb.league       = fb.league
    AND wsb.season       = fb.season_slug
LEFT JOIN xref_match_ws xmw
    ON  xmw.match_id_canonical = fb.match_id
    AND xmw.league             = fb.league
    AND xmw.season_slug        = fb.season_slug
LEFT JOIN iceberg.silver.whoscored_team_match ws
    ON  ws.match_id = xmw.ws_match_id
    AND ws.team_id  = wsb.ws_team_id_numeric
    AND ws.league   = fb.league
    AND ws.season   = fb.season_slug

-- ===== FotMob LEFT (#97) — xref season slug now (#404), fact season slug =====
LEFT JOIN xref_team_fm xtfm
    ON  xtfm.canonical_id = xtf.canonical_id
    AND xtfm.league       = fb.league
    AND xtfm.season_year  = CAST(fb.season_year AS varchar)
LEFT JOIN xref_match_fm xmfm
    ON  xmfm.match_id_canonical = fb.match_id
    AND xmfm.league             = fb.league
    AND xmfm.season_year        = CAST(fb.season_year AS varchar)
LEFT JOIN iceberg.silver.fotmob_team_match fm
    ON  fm.match_id = xmfm.fm_match_id
    AND fm.team_id  = xtfm.fm_team_name
    AND fm.league   = fb.league
    AND fm.season   = fb.season_slug

WHERE fb.match_id IS NOT NULL
