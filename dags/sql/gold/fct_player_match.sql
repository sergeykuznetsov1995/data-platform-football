-- =============================================================================
-- Gold: fct_player_match
-- =============================================================================
--
-- Per-match cross-source player performance: FBref + SofaScore + Understat +
-- WhoScored объединены через silver.xref_player + silver.xref_match. Spine —
-- FBref (silver.fbref_player_match_stats), остальные источники LEFT JOIN.
--
-- Source-selection rule (mirrors fct_player_season_stats / RX2):
--   * HARD_FACT (counters, identical definition) → COALESCE(fb → ss → ws → us)
--     CAST AS BIGINT. FBref — primary; cross-source diff-колонки идут в
--     `fct_player_match_audit`.
--   * MODELED:
--       - xG / xA: Understat primary (RX2 — coverage 99% vs 82-85%), then
--         SofaScore fallback. Single column через COALESCE(us → ss).
--       - rating: SofaScore (Opta) единственный источник на match-grain
--         (FotMob match-level rating не собираем).
--   * UNIQUE_<source> метрики (ground_duels_won, aerials_won, dribbles_*) —
--     single column из соответствующего источника.
--
-- Grain: (match_id_canonical, player_id_canonical).
-- PK: natural composite — оба компонента non-NULL по конструкции INNER spine.
--
-- xref JOIN footgun (feedback_xref_join_season_predicate.md):
--   silver.xref_player и silver.xref_match хранят per-(source, source_id,
--   season) rows → ВСЕ xref JOIN обязаны иметь (league, season) predicate.
--   Без него fan-out 1.5-4×.
--
-- Season normalization (feedback_xref_season_type):
--   * silver.xref_player.season              = varchar '2526'
--   * silver.fbref_player_match_stats.season = bigint 2025
--   * silver.{ss,us,ws}_player_match_aggregate.season = varchar slug
--   xref slug '2526' → bigint 2025: `2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)`.
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

-- ===== FBref match bridge (mostly identity для source='fbref') =====
xref_match_fbref AS (
    SELECT DISTINCT
        canonical_id                                       AS match_id_canonical,
        source_id                                          AS fbref_match_id,
        league,
        season                                             AS season_slug
    FROM iceberg.silver.xref_match
    WHERE source = 'fbref'
),

-- ===== FBref team bridge =====
xref_team_fbref AS (
    SELECT DISTINCT
        canonical_id                                       AS team_id_canonical,
        source_id                                          AS fbref_team_id,
        league,
        season                                             AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'fbref'
),

-- ===== SofaScore bridges =====
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

-- ===== Understat bridges =====
-- Issue #70: silver.xref_player resolver dedups duplicate source_ids per
-- (canonical, league, season) at source, so this CTE matches the symmetric
-- SofaScore / WhoScored / FotMob bridges below.
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

-- ===== WhoScored bridges =====
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
    -- ========= PK (natural composite) =========
    COALESCE(xmf.match_id_canonical, fb.match_id)        AS match_id_canonical,
    xfp.canonical_id                                     AS player_id_canonical,
    xtf.team_id_canonical                                AS team_id_canonical,

    -- ========= Identity / context =========
    fb.team                                              AS team_name_fbref,
    fb.team_side,
    fb.pos                                               AS position_fbref,
    fme.date                                             AS match_date,

    -- ========= HARD_FACT (COALESCE fb → ss → ws → us, CAST AS BIGINT) =========
    -- COALESCE между гетерогенными source types (FBref integer, SS DOUBLE,
    -- WS BIGINT) промотит в DOUBLE — CAST AS BIGINT даёт чистый integer
    -- counter в BI. Cross-source diff'ы хранятся в fct_player_match_audit.
    CAST(COALESCE(fb.minutes,           ss.minutes_played)                          AS BIGINT) AS minutes,
    CAST(COALESCE(fb.goals,             ss.goals,            ws.goals,    us.goals) AS BIGINT) AS goals,
    CAST(COALESCE(fb.assists,           ss.assists,                       us.assists) AS BIGINT) AS assists,
    CAST(COALESCE(fb.shots,             ss.shots,            ws.shots,    us.shots) AS BIGINT) AS shots,
    CAST(COALESCE(fb.shots_on_target,   ss.shots_on_target,  ws.shots_on_target)    AS BIGINT) AS shots_on_target,
    CAST(COALESCE(fb.yellow_cards,      ss.yellow_cards,     ws.yellow_cards, us.yellow_cards) AS BIGINT) AS yellow_cards,
    CAST(COALESCE(fb.red_cards,         ss.red_cards,        ws.red_cards,    us.red_cards)    AS BIGINT) AS red_cards,
    CAST(COALESCE(fb.crosses,           ss.crosses,          ws.crosses)            AS BIGINT) AS crosses,
    CAST(COALESCE(fb.fouls_committed,   ss.fouls_committed,  ws.fouls_committed)    AS BIGINT) AS fouls_committed,
    CAST(COALESCE(fb.fouls_drawn,       ss.fouls_drawn,      ws.fouls_drawn)        AS BIGINT) AS fouls_drawn,
    CAST(COALESCE(fb.offsides,          ss.offsides,         ws.offsides)           AS BIGINT) AS offsides,
    CAST(COALESCE(fb.tackles_won,       ss.tackles_won,      ws.tackles_won)        AS BIGINT) AS tackles_won,
    CAST(COALESCE(fb.interceptions,     ss.interceptions,    ws.interceptions)      AS BIGINT) AS interceptions,
    CAST(COALESCE(fb.own_goals,         ss.own_goals,                        us.own_goals) AS BIGINT) AS own_goals,
    CAST(COALESCE(fb.penalty_goals,     ss.penalty_goals)                           AS BIGINT) AS penalty_goals,
    CAST(COALESCE(fb.penalty_attempts,  ss.penalties_missed + ss.penalty_goals)     AS BIGINT) AS penalty_attempts,
    CAST(COALESCE(fb.penalties_won,     ss.penalties_won)                           AS BIGINT) AS penalties_won,
    CAST(COALESCE(fb.penalties_conceded, ss.penalties_conceded)                     AS BIGINT) AS penalties_conceded,

    -- FBref does NOT expose key_passes/tackles/passes/etc on match-grain.
    -- COALESCE SS → WS для этих метрик.
    CAST(COALESCE(ss.key_passes,        ws.key_passes)                              AS BIGINT) AS key_passes,
    CAST(COALESCE(ss.passes,            ws.passes)                                  AS BIGINT) AS passes,
    CAST(COALESCE(ss.passes_completed,  ws.passes_completed)                        AS BIGINT) AS passes_completed,
    CAST(COALESCE(ss.tackles,           ws.tackles)                                 AS BIGINT) AS tackles,
    CAST(COALESCE(ss.clearances,        ws.clearances)                              AS BIGINT) AS clearances,
    CAST(COALESCE(ss.ball_recoveries,   ws.ball_recoveries)                         AS BIGINT) AS ball_recoveries,
    CAST(COALESCE(ss.dribbles_attempted, ws.dribbles_attempted)                     AS BIGINT) AS dribbles_attempted,
    CAST(COALESCE(ss.dribbles_won,      ws.dribbles_won)                            AS BIGINT) AS dribbles_won,
    CAST(COALESCE(ss.touches,           ws.touches)                                 AS BIGINT) AS touches,
    CAST(COALESCE(ss.dispossessed,      ws.dispossessed)                            AS BIGINT) AS dispossessed,
    CAST(COALESCE(ss.aerial_duels_won,  ws.aerials_won)                             AS BIGINT) AS aerial_duels_won,

    -- ========= UNIQUE_SOFASCORE =========
    CAST(ss.ground_duels_won                                                        AS BIGINT) AS ground_duels_won,
    CAST(ss.errors_lead_to_goal                                                     AS BIGINT) AS errors_lead_to_goal,
    CAST(ss.errors_lead_to_shot                                                     AS BIGINT) AS errors_lead_to_shot,
    CAST(ss.blocks                                                                  AS BIGINT) AS blocks,
    CAST(ss.accurate_crosses                                                        AS BIGINT) AS accurate_crosses,
    CAST(ss.accurate_long_balls                                                     AS BIGINT) AS accurate_long_balls,
    CAST(ss.total_long_balls                                                        AS BIGINT) AS total_long_balls,

    -- ========= UNIQUE_WHOSCORED (event-derived dribble = SPADL take_on success) =========
    CAST(ws.dribbles_won                                                            AS BIGINT) AS dribbles_ws,
    CAST(ws.blocks                                                                  AS BIGINT) AS blocks_ws,

    -- ========= MODELED — xG / xA / rating =========
    -- xG/xA: Understat primary (RX2 — coverage 99% vs 82-85%; r≥0.989).
    -- COALESCE us → ss закрывает ~1% Understat-gap (U21 / backup minutes).
    -- Cross-source diff'ы (us vs ss) хранятся в fct_player_match_audit.
    ROUND(COALESCE(us.xg, ss.xg), 4)                     AS expected_goals,
    ROUND(COALESCE(us.xa, ss.xa), 4)                     AS expected_assists,
    ROUND(us.non_penalty_xg, 4)                          AS non_penalty_xg_understat,
    ROUND(us.xg_chain, 4)                                AS xg_chain_understat,
    ROUND(us.xg_buildup, 4)                              AS xg_buildup_understat,
    -- Rating — SofaScore (Opta) единственный источник на match-grain.
    ROUND(ss.rating, 2)                                  AS rating,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                    AS _gold_created_at,

    -- ========= Partition keys (LAST in SELECT) =========
    xfp.league                                           AS league,
    xfp.season_slug                                      AS season

FROM xref_fbref_player xfp
INNER JOIN iceberg.silver.fbref_player_match_stats fb
    ON  fb.player_id = xfp.fbref_player_id
    AND fb.league    = xfp.league
    AND fb.season    = xfp.season_year

-- ===== FBref match canonicalisation =====
LEFT JOIN xref_match_fbref xmf
    ON  xmf.fbref_match_id = fb.match_id
    AND xmf.league         = xfp.league
    AND xmf.season_slug    = xfp.season_slug

-- ===== FBref team canonicalisation =====
LEFT JOIN xref_team_fbref xtf
    ON  xtf.fbref_team_id = fb.team
    AND xtf.league        = xfp.league
    AND xtf.season_slug   = xfp.season_slug

-- ===== Match-date enrichment =====
LEFT JOIN iceberg.silver.fbref_match_enriched fme
    ON  fme.match_id = fb.match_id
    AND fme.league   = xfp.league
    AND fme.season   = xfp.season_year

-- ===== SofaScore bridge (LEFT) =====
LEFT JOIN xref_ss_player xsp
    ON  xsp.canonical_id = xfp.canonical_id
    AND xsp.league       = xfp.league
    AND xsp.season_slug  = xfp.season_slug
LEFT JOIN xref_ss_match xsm
    ON  xsm.match_id_canonical = COALESCE(xmf.match_id_canonical, fb.match_id)
    AND xsm.league             = xfp.league
    AND xsm.season_slug        = xfp.season_slug
LEFT JOIN iceberg.silver.sofascore_player_match_aggregate ss
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
