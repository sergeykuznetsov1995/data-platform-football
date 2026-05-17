-- =============================================================================
-- Gold: fct_player_season_stats
-- =============================================================================
--
-- Per-season cross-source stats per игрок: FBref+FotMob объединены через
-- silver.xref_player. Wide single-column для overlap (FBref primary,
-- COALESCE FotMob); UNIQUE_FBREF / UNIQUE_FOTMOB — own column; audit
-- `<metric>_diff_fotmob` для калибровки расхождений (R1-followup).
--
-- Зерно: (player_id_canonical, league, season). Один row per канонический
-- игрок × лига × сезон. Spine — FBref subset из xref_player.
--
-- Источники:
--   silver.xref_player                          — bridge canonical_id ↔ source_id
--   silver.fbref_player_season_profile          — FBref Silver wide (fb.player_id)
--   silver.fotmob_player_season_profile         — FotMob Silver wide (fm.player_id)
--
-- Cross-source season type:
--   * silver.xref_player.season       = varchar '2526'
--   * silver.fbref_player_season_profile.season  = bigint 2025
--   * silver.fotmob_player_season_profile.season = bigint 2025
--   xref slug '2526' → bigint 2025 идиомой
--   `2000 + CAST(SUBSTR(season, 1, 2) AS bigint)` (см. fct_card.sql:51).
--   Output season хранится bigint для symmetry с dim_player / fct_player_match.
--
-- ⚠️ xref JOIN MUST include (league, season) predicate (CLAUDE.md):
--   silver.xref_player имеет per-(source, source_id, season) rows;
--   без season-condition будет fan-out 1.5-4×.
--
-- ⚠️ Cross-source season type — spine CTE emits BOTH `season_year` (bigint
--   2025) for FBref/FotMob Silver и `season_slug` (varchar '2526') for
--   WhoScored/Understat Silver. JOIN'ы должны использовать correct field.
--
-- HARD_FACT (8): matches, minutes, goals, assists, yellow_cards, red_cards,
--                penalties_won, penalties_conceded — overlap, COALESCE
--                FBref→FotMob→WhoScored→Understat (WhoScored event-aggregate
--                contributes only to `matches`; minutes/goals/assists/cards
--                are NOT present in WS season-aggregate).
-- UNIQUE_FBREF (12): complete_matches, starts, subs, plus_minus, points_per_match,
--                    on_off_impact, shots, shots_on_target, goals_per_shot,
--                    crosses, offsides, own_goals.
-- UNIQUE_FOTMOB (14): expected_goals, expected_assists, expected_goals_on_target,
--                     big_chances_created/missed, chances_created, fotmob_rating,
--                     defensive_actions_per_90, clearances_per_90, recoveries_per_90,
--                     blocks_per_90, accurate_passes_per_90, accurate_long_balls_per_90,
--                     successful_dribbles_per_90.
-- UNIQUE_WHOSCORED (13): dribbles, take_on_pct, bad_touches, pass_pct, tackles_won,
--                        tackle_pct, interceptions, ball_recoveries, clearances,
--                        fouls_committed, touches_in_box, avg_x, avg_y.
-- UNIQUE_UNDERSTAT (8): expected_goals_understat, expected_assists_understat,
--                       non_penalty_goals_understat, non_penalty_xg, xg_chain,
--                       xg_buildup, key_passes_understat, shots_understat.
--
-- Cross-source audit-diff (FBref - FotMob per HARD_FACT) вынесены в отдельную
-- таблицу `gold.fct_player_season_stats_audit` чтобы не загромождать business-витрину
-- технической DQ-метаданой.
-- =============================================================================

WITH
-- FBref-spine: одна строка per (canonical_id, league, season).
-- FBref игроки в xref_player всегда confidence='exact', но фильтр оставлен
-- для symmetry с dim_player_attributes на случай будущей логики.
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_player_id,
        league,
        season                                            AS season_slug,  -- varchar '2526' (для WS/US JOIN)
        2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)      AS season_year   -- '2526' → 2025 (для FBref/FotMob)
    FROM iceberg.silver.xref_player
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

-- FotMob bridge: per-(canonical_id, league, season) → fotmob_player_id.
-- Берём только non-orphan строки (orphan = U21/резервы без FBref-pair, для
-- которых canonical_id не совпадает с FBref-spine; они в любом случае
-- отфильтруются JOIN-ом).
xref_fotmob AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fotmob_player_id,
        league,
        2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)      AS season_year
    FROM iceberg.silver.xref_player
    WHERE source = 'fotmob'
      AND confidence <> 'orphan'
)

SELECT
    -- ========= Identity (per-season) =========
    xf.canonical_id                                      AS player_id_canonical,
    xf.league                                            AS league,
    xf.season_year                                       AS season,
    COALESCE(fb.squad, fm.primary_team_name)             AS primary_team_name,
    fb.pos                                               AS position_fbref,
    fm.primary_position                                  AS position_fotmob,

    -- ========= HARD_FACT overlap (FBref→FotMob→WhoScored→Understat) =========
    -- WhoScored event-aggregate отдаёт только matches_seen (нет
    -- minutes/goals/assists/cards/penalties на season-уровне).
    -- Understat покрывает matches/minutes/goals/assists/cards.
    COALESCE(fb.mp,                  fm.matches_played, ws.matches_seen, us.games_played) AS matches,
    COALESCE(fb.minutes,             fm.minutes_played, us.minutes_played)               AS minutes,
    COALESCE(fb.goals,               fm.goals,          us.goals)                        AS goals,
    COALESCE(fb.assists,             fm.assists,        us.assists)                      AS assists,
    COALESCE(fb.yellow_cards,        fm.yellow_cards,   us.yellow_cards)                 AS yellow_cards,
    COALESCE(fb.red_cards,           fm.red_cards,      us.red_cards)                    AS red_cards,
    COALESCE(fb.penalties_won,       fm.penalties_won)        AS penalties_won,
    COALESCE(fb.penalties_conceded,  fm.penalties_conceded)   AS penalties_conceded,

    -- ========= UNIQUE_FBREF (12) =========
    fb.complete_matches,
    fb.starts,
    fb.subs,
    fb.plus_minus,
    fb.points_per_match,
    fb.on_off_impact,
    fb.shots,
    fb.shots_on_target,
    fb.goals_per_shot,
    fb.crosses,
    fb.offsides,
    fb.own_goals,

    -- ========= UNIQUE_FOTMOB (14) =========
    fm.expected_goals,
    fm.expected_assists,
    fm.expected_goals_on_target,
    fm.big_chances_created,
    fm.big_chances_missed,
    fm.chances_created,
    fm.fotmob_rating,
    fm.defensive_actions_per_90,
    fm.clearances_per_90,
    fm.recoveries_per_90,
    fm.blocks_per_90,
    fm.accurate_passes_per_90,
    fm.accurate_long_balls_per_90,
    fm.successful_dribbles_per_90,

    -- ========= UNIQUE_WHOSCORED (13) =========
    ws.dribbles                                          AS dribbles_whoscored,
    ws.takeon_pct                                        AS take_on_pct_whoscored,
    ws.bad_touches                                       AS bad_touches_whoscored,
    ws.pass_pct                                          AS pass_pct_whoscored,
    ws.tackle_won                                        AS tackles_won_whoscored,
    ws.tackle_pct                                        AS tackle_pct_whoscored,
    ws.interceptions                                     AS interceptions_whoscored,
    ws.ball_recoveries                                   AS ball_recoveries_whoscored,
    ws.clearances                                        AS clearances_whoscored,
    ws.fouls_committed                                   AS fouls_committed_whoscored,
    ws.touches_in_box                                    AS touches_in_box_whoscored,
    ws.avg_x                                             AS avg_x_whoscored,
    ws.avg_y                                             AS avg_y_whoscored,

    -- ========= UNIQUE_UNDERSTAT (8) =========
    -- xG/xA дублируются с FotMob, поэтому suffix `_understat` для явности.
    -- npxG/xg_chain/xg_buildup — Understat-exclusive, без суффикса.
    us.expected_goals                                    AS expected_goals_understat,
    us.expected_assists                                  AS expected_assists_understat,
    us.non_penalty_goals                                 AS non_penalty_goals_understat,
    us.non_penalty_xg                                    AS non_penalty_xg,
    us.xg_chain                                          AS xg_chain,
    us.xg_buildup                                        AS xg_buildup,
    us.key_passes                                        AS key_passes_understat,
    us.shots                                             AS shots_understat,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                    AS _gold_created_at

FROM xref_fbref xf
INNER JOIN iceberg.silver.fbref_player_season_profile fb
    ON  fb.player_id = xf.fbref_player_id
    AND fb.league    = xf.league
    AND fb.season    = xf.season_year
LEFT JOIN xref_fotmob xfm
    ON  xfm.canonical_id = xf.canonical_id
    AND xfm.league       = xf.league
    AND xfm.season_year  = xf.season_year
LEFT JOIN iceberg.silver.fotmob_player_season_profile fm
    ON  fm.player_id = xfm.fotmob_player_id
    AND fm.league    = xfm.league
    AND fm.season    = xfm.season_year
-- WhoScored event-aggregate (silver). canonical_id уже cross-source через
-- xref_player; JOIN на (league, season_slug). MUST use season_slug ('2526'),
-- НЕ season_year (bigint), иначе type mismatch.
LEFT JOIN iceberg.silver.whoscored_player_season_aggregate ws
    ON  ws.canonical_id = xf.canonical_id
    AND ws.league       = xf.league
    AND ws.season       = xf.season_slug
-- Understat season-aggregate (silver). Аналогично — varchar slug JOIN.
LEFT JOIN iceberg.silver.understat_player_season_aggregate us
    ON  us.canonical_id = xf.canonical_id
    AND us.league       = xf.league
    AND us.season       = xf.season_slug
-- Outfield-only: exclude вратарей (они в fct_keeper_season_stats).
-- silver.fbref_player_season_profile содержит ВСЕХ игроков включая GK
-- (фильтра по pos нет), поэтому GK исключаем явно. NULL pos редкость, но на
-- всякий случай не пропускаем (treated as outfield по умолчанию).
WHERE fb.pos IS NULL OR fb.pos NOT LIKE '%GK%'
