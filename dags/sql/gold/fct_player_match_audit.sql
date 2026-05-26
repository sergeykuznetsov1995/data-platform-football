-- =============================================================================
-- Gold: fct_player_match_audit
-- =============================================================================
--
-- DQ-audit таблица для cross-source согласованности по HARD_FACT метрикам в
-- `gold.fct_player_match`. НЕ business-витрина: содержит ТОЛЬКО технические
-- diff-колонки + PK (без COALESCE, без UNIQUE_*).
--
-- Convention:
--   diff = FBref - <source> где FBref имеет метрику (primary spine).
--   Для метрик, которых нет у FBref на match-grain (key_passes / tackles /
--   passes / clearances / ball_recoveries / dribbles_attempted / aerial_duels /
--   touches), diff = SS - <source> (SofaScore = secondary spine, INNER JOIN).
--   xG / xA — кросс-моделированные diff'ы между US ↔ SS ↔ FB-derived.
--
-- Grain: (match_id_canonical, player_id_canonical). Один row per канонический
-- матч × игрок, **только когда обе стороны spine имеют запись** (INNER JOIN
-- FBref ∩ SofaScore). Understat / WhoScored — LEFT JOIN → diff = NULL когда
-- источник отсутствует.
--
-- Использование:
--   1. DQ coverage WARNING — ABS(diff) <= threshold у ≥95% rows.
--   2. Engineer-debug при «голы по матчу не сходятся в дашборде».
--   3. Калибровка thresholds → docs/research/R1_cross_source_thresholds.md.
--
-- xref-JOIN footgun (feedback_xref_join_season_predicate.md):
--   silver.xref_player и silver.xref_match хранят per-(source, source_id,
--   season) rows → ВСЕ xref JOIN обязаны иметь (league, season) predicate,
--   иначе fan-out 1.5-4×.
--
-- Season normalization (feedback_xref_season_type):
--   xref.season = varchar '2526'; fbref_player_match_stats.season = bigint 2025
--   `2000 + CAST(SUBSTR(season,1,2) AS BIGINT)` приводит slug → bigint.
--
-- PK: (match_id_canonical, player_id_canonical) — natural composite, без
-- xxhash64 (оба компонента non-NULL по конструкции INNER spine).
--
-- Audit-таблица читает Silver заново (НЕ gold.fct_player_match) per one-hop
-- правило (memory: project_gold_cleanup_2026-05-12).
-- =============================================================================

WITH
-- ===== Spine: FBref subset из xref_player =====
xref_fbref_player AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                          AS fbref_player_id,
        league,
        season                                             AS season_slug,
        2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)        AS season_year
    FROM iceberg.silver.xref_player
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

-- ===== FBref match bridge =====
xref_match_fbref AS (
    SELECT DISTINCT
        canonical_id                                       AS match_id_canonical,
        source_id                                          AS fbref_match_id,
        league,
        season                                             AS season_slug,
        2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)        AS season_year
    FROM iceberg.silver.xref_match
    WHERE source = 'fbref'
),

-- ===== SofaScore bridge (secondary INNER spine) =====
xref_ss_player AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                          AS ss_player_id,
        league,
        season                                             AS season_slug
    FROM iceberg.silver.xref_player
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
),

xref_ss_match AS (
    SELECT DISTINCT
        canonical_id                                       AS match_id_canonical,
        source_id                                          AS ss_match_id,
        league,
        season                                             AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
),

-- ===== Understat bridge =====
xref_us_player AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                          AS us_player_id,
        league,
        season                                             AS season_slug
    FROM iceberg.silver.xref_player
    WHERE source = 'understat'
      AND confidence <> 'orphan'
),

xref_us_match AS (
    SELECT DISTINCT
        canonical_id                                       AS match_id_canonical,
        source_id                                          AS us_match_id,
        league,
        season                                             AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'understat'
      AND confidence <> 'orphan'
),

-- ===== WhoScored bridge =====
xref_ws_player AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                          AS ws_player_id,
        league,
        season                                             AS season_slug
    FROM iceberg.silver.xref_player
    WHERE source = 'whoscored'
      AND confidence <> 'orphan'
),

xref_ws_match AS (
    SELECT DISTINCT
        canonical_id                                       AS match_id_canonical,
        source_id                                          AS ws_match_id,
        league,
        season                                             AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'whoscored'
      AND confidence <> 'orphan'
)

SELECT
    -- ========= PK (грейн совпадает с fct_player_match) =========
    COALESCE(xmf.match_id_canonical, fb.match_id)         AS match_id_canonical,
    xfp.canonical_id                                       AS player_id_canonical,

    -- ========= SofaScore diff (INNER JOIN — всегда non-NULL) =========
    -- HARD_FACT pairs с FBref spine
    (CAST(fb.minutes          AS DOUBLE) - CAST(ss.minutes_played   AS DOUBLE)) AS minutes_diff_ss,
    (CAST(fb.goals            AS DOUBLE) - CAST(ss.goals            AS DOUBLE)) AS goals_diff_ss,
    (CAST(fb.assists          AS DOUBLE) - CAST(ss.assists          AS DOUBLE)) AS assists_diff_ss,
    (CAST(fb.own_goals        AS DOUBLE) - CAST(ss.own_goals        AS DOUBLE)) AS own_goals_diff_ss,
    (CAST(fb.shots            AS DOUBLE) - CAST(ss.shots            AS DOUBLE)) AS shots_diff_ss,
    (CAST(fb.shots_on_target  AS DOUBLE) - CAST(ss.shots_on_target  AS DOUBLE)) AS shots_on_target_diff_ss,
    (CAST(fb.yellow_cards     AS DOUBLE) - CAST(ss.yellow_cards     AS DOUBLE)) AS yellow_cards_diff_ss,
    (CAST(fb.red_cards        AS DOUBLE) - CAST(ss.red_cards        AS DOUBLE)) AS red_cards_diff_ss,
    (CAST(fb.crosses          AS DOUBLE) - CAST(ss.crosses          AS DOUBLE)) AS crosses_diff_ss,
    (CAST(fb.fouls_committed  AS DOUBLE) - CAST(ss.fouls_committed  AS DOUBLE)) AS fouls_committed_diff_ss,
    (CAST(fb.fouls_drawn      AS DOUBLE) - CAST(ss.fouls_drawn      AS DOUBLE)) AS fouls_drawn_diff_ss,
    (CAST(fb.offsides         AS DOUBLE) - CAST(ss.offsides         AS DOUBLE)) AS offsides_diff_ss,
    (CAST(fb.tackles_won      AS DOUBLE) - CAST(ss.tackles_won      AS DOUBLE)) AS tackles_won_diff_ss,
    (CAST(fb.interceptions    AS DOUBLE) - CAST(ss.interceptions    AS DOUBLE)) AS interceptions_diff_ss,
    (CAST(fb.penalty_goals    AS DOUBLE) - CAST(ss.penalties_won    AS DOUBLE)) AS penalty_goals_diff_ss,
    (CAST(fb.penalty_attempts AS DOUBLE) - CAST(ss.penalties_missed AS DOUBLE)) AS penalty_attempts_diff_ss,
    (CAST(fb.penalties_won      AS DOUBLE) - CAST(ss.penalties_won      AS DOUBLE)) AS penalties_won_diff_ss,
    (CAST(fb.penalties_conceded AS DOUBLE) - CAST(ss.penalties_conceded AS DOUBLE)) AS penalties_conceded_diff_ss,

    -- ========= Understat diff (LEFT JOIN → NULL if absent) =========
    -- HARD_FACT pairs с FBref spine
    (CAST(fb.minutes      AS DOUBLE) - CAST(us.minutes_played AS DOUBLE)) AS minutes_diff_us,
    (CAST(fb.goals        AS DOUBLE) - CAST(us.goals          AS DOUBLE)) AS goals_diff_us,
    (CAST(fb.assists      AS DOUBLE) - CAST(us.assists        AS DOUBLE)) AS assists_diff_us,
    (CAST(fb.own_goals    AS DOUBLE) - CAST(us.own_goals      AS DOUBLE)) AS own_goals_diff_us,
    (CAST(fb.shots        AS DOUBLE) - CAST(us.shots          AS DOUBLE)) AS shots_diff_us,
    (CAST(fb.yellow_cards AS DOUBLE) - CAST(us.yellow_cards   AS DOUBLE)) AS yellow_cards_diff_us,
    (CAST(fb.red_cards    AS DOUBLE) - CAST(us.red_cards      AS DOUBLE)) AS red_cards_diff_us,
    -- key_passes: FBref на match-grain не отдаёт → diff = SS - US
    (CAST(ss.key_passes   AS DOUBLE) - CAST(us.key_passes     AS DOUBLE)) AS key_passes_diff_ss_us,

    -- ========= WhoScored diff (LEFT JOIN → NULL if absent) =========
    -- HARD_FACT pairs с FBref spine
    (CAST(fb.goals            AS DOUBLE) - CAST(ws.goals            AS DOUBLE)) AS goals_diff_ws,
    (CAST(fb.shots            AS DOUBLE) - CAST(ws.shots            AS DOUBLE)) AS shots_diff_ws,
    (CAST(fb.shots_on_target  AS DOUBLE) - CAST(ws.shots_on_target  AS DOUBLE)) AS shots_on_target_diff_ws,
    (CAST(fb.yellow_cards     AS DOUBLE) - CAST(ws.yellow_cards     AS DOUBLE)) AS yellow_cards_diff_ws,
    (CAST(fb.red_cards        AS DOUBLE) - CAST(ws.red_cards        AS DOUBLE)) AS red_cards_diff_ws,
    (CAST(fb.crosses          AS DOUBLE) - CAST(ws.crosses          AS DOUBLE)) AS crosses_diff_ws,
    (CAST(fb.fouls_committed  AS DOUBLE) - CAST(ws.fouls_committed  AS DOUBLE)) AS fouls_committed_diff_ws,
    (CAST(fb.fouls_drawn      AS DOUBLE) - CAST(ws.fouls_drawn      AS DOUBLE)) AS fouls_drawn_diff_ws,
    (CAST(fb.offsides         AS DOUBLE) - CAST(ws.offsides         AS DOUBLE)) AS offsides_diff_ws,
    (CAST(fb.tackles_won      AS DOUBLE) - CAST(ws.tackles_won      AS DOUBLE)) AS tackles_won_diff_ws,
    (CAST(fb.interceptions    AS DOUBLE) - CAST(ws.interceptions    AS DOUBLE)) AS interceptions_diff_ws,
    -- FBref не отдаёт key_passes / tackles / passes / clearances / blocks /
    -- ball_recoveries / dribbles / touches на match-grain → diff = SS - WS
    (CAST(ss.key_passes         AS DOUBLE) - CAST(ws.key_passes         AS DOUBLE)) AS key_passes_diff_ss_ws,
    (CAST(ss.passes             AS DOUBLE) - CAST(ws.passes             AS DOUBLE)) AS passes_diff_ss_ws,
    (CAST(ss.passes_completed   AS DOUBLE) - CAST(ws.passes_completed   AS DOUBLE)) AS passes_completed_diff_ss_ws,
    (CAST(ss.tackles            AS DOUBLE) - CAST(ws.tackles            AS DOUBLE)) AS tackles_diff_ss_ws,
    (CAST(ss.clearances         AS DOUBLE) - CAST(ws.clearances         AS DOUBLE)) AS clearances_diff_ss_ws,
    (CAST(ss.ball_recoveries    AS DOUBLE) - CAST(ws.ball_recoveries    AS DOUBLE)) AS ball_recoveries_diff_ss_ws,
    (CAST(ss.dribbles_attempted AS DOUBLE) - CAST(ws.dribbles_attempted AS DOUBLE)) AS dribbles_attempted_diff_ss_ws,
    (CAST(ss.dribbles_won       AS DOUBLE) - CAST(ws.dribbles_won       AS DOUBLE)) AS dribbles_won_diff_ss_ws,
    (CAST(ss.aerial_duels_won   AS DOUBLE) - CAST(ws.aerials_won        AS DOUBLE)) AS aerials_won_diff_ss_ws,
    (CAST(ss.touches            AS DOUBLE) - CAST(ws.touches            AS DOUBLE)) AS touches_diff_ss_ws,
    (CAST(ss.dispossessed       AS DOUBLE) - CAST(ws.dispossessed       AS DOUBLE)) AS dispossessed_diff_ss_ws,

    -- ========= MODELED xG / xA diff (different models, expected to disagree) =========
    -- xG: Understat = primary (RX2), SofaScore = secondary. Кросс-моделирование.
    ROUND(CAST(us.xg AS DOUBLE) - CAST(ss.xg AS DOUBLE), 4) AS xg_diff_us_ss,
    ROUND(CAST(us.xa AS DOUBLE) - CAST(ss.xa AS DOUBLE), 4) AS xa_diff_us_ss,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                      AS _gold_created_at,

    -- ========= Partition keys (must be last for Iceberg partitioning) =========
    xfp.league                                             AS league,
    xfp.season_slug                                        AS season

FROM xref_fbref_player xfp
INNER JOIN iceberg.silver.fbref_player_match_stats fb
    ON  fb.player_id = xfp.fbref_player_id
    AND fb.league    = xfp.league
    AND fb.season    = xfp.season_year
-- Bridge FBref match_id → canonical (mostly identity для source='fbref')
LEFT JOIN xref_match_fbref xmf
    ON  xmf.fbref_match_id = fb.match_id
    AND xmf.league         = xfp.league
    AND xmf.season_year    = xfp.season_year

-- ===== SofaScore bridge (INNER — secondary spine) =====
INNER JOIN xref_ss_player xsp
    ON  xsp.canonical_id = xfp.canonical_id
    AND xsp.league       = xfp.league
    AND xsp.season_slug  = xfp.season_slug
INNER JOIN xref_ss_match xsm
    ON  xsm.match_id_canonical = COALESCE(xmf.match_id_canonical, fb.match_id)
    AND xsm.league             = xfp.league
    AND xsm.season_slug        = xfp.season_slug
INNER JOIN iceberg.silver.sofascore_player_match_aggregate ss
    ON  ss.match_id  = xsm.ss_match_id
    AND ss.player_id = xsp.ss_player_id
    AND ss.league    = xfp.league
    AND ss.season    = xfp.season_slug

-- ===== Understat bridge (LEFT) =====
LEFT JOIN xref_us_player xup
    ON  xup.canonical_id = xfp.canonical_id
    AND xup.league       = xfp.league
    AND xup.season_slug  = xfp.season_slug
LEFT JOIN xref_us_match xum
    ON  xum.match_id_canonical = COALESCE(xmf.match_id_canonical, fb.match_id)
    AND xum.league             = xfp.league
    AND xum.season_slug        = xfp.season_slug
LEFT JOIN iceberg.silver.understat_player_match_aggregate us
    ON  us.match_id  = xum.us_match_id
    AND us.player_id = xup.us_player_id
    AND us.league    = xfp.league
    AND us.season    = xfp.season_slug

-- ===== WhoScored bridge (LEFT) =====
LEFT JOIN xref_ws_player xwp
    ON  xwp.canonical_id = xfp.canonical_id
    AND xwp.league       = xfp.league
    AND xwp.season_slug  = xfp.season_slug
LEFT JOIN xref_ws_match xwm
    ON  xwm.match_id_canonical = COALESCE(xmf.match_id_canonical, fb.match_id)
    AND xwm.league             = xfp.league
    AND xwm.season_slug        = xfp.season_slug
LEFT JOIN iceberg.silver.whoscored_player_match_aggregate ws
    ON  ws.match_id  = xwm.ws_match_id
    AND ws.player_id = xwp.ws_player_id
    AND ws.league    = xfp.league
    AND ws.season    = xfp.season_slug

WHERE fb.match_id  IS NOT NULL
  AND fb.player_id IS NOT NULL
