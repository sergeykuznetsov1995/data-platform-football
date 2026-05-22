-- =============================================================================
-- Gold: fct_player_season_stats
-- =============================================================================
--
-- Per-season cross-source stats per игрок: FBref+FotMob+WhoScored+Understat+
-- SofaScore объединены через silver.xref_player. Правило выбора источников
-- зафиксировано в docs/research/RX_cross_source_player_profile.md (D3) и
-- memory/feedback_audit_in_separate_table.md:
--
--   * HARD_FACT (счётные event-метрики, identical definition) →
--     single column через COALESCE(fb→fm→ws→us→ss). FBref — primary spine.
--     Cross-source diff'ы выносятся в `fct_player_season_stats_audit`.
--   * MODELED (разные модели) → суффикс `_<source>` оставляется
--     (xG: FotMob vs Understat vs SofaScore — три разные модели;
--      rating: fotmob_rating vs rating_sofascore — две разные шкалы).
--   * UNIQUE_<source> (метрика отсутствует у других) → single column,
--     без суффикса.
--
-- Зерно: (player_id_canonical, league, season). Spine — FBref subset из
-- xref_player.
--
-- Cross-source season type:
--   * silver.xref_player.season          = varchar '2526'
--   * silver.fbref_player_season_profile.season  = bigint 2025
--   * silver.fotmob_player_season_profile.season = bigint 2025
--   * silver.whoscored_player_season_aggregate.season = varchar slug
--   * silver.understat_player_season_aggregate.season = varchar slug
--   * silver.sofascore_player_season_aggregate.season = varchar slug
--   xref slug '2526' → bigint 2025: `2000 + CAST(SUBSTR(season, 1, 2) AS bigint)`.
--
-- ⚠️ xref JOIN MUST include (league, season) predicate (CLAUDE.md):
--   silver.xref_player имеет per-(source, source_id, season) rows;
--   без season-condition будет fan-out 1.5-4×.
-- =============================================================================

WITH
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_player_id,
        league,
        season                                            AS season_slug,  -- varchar '2526' (для WS/US/SS JOIN)
        2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)      AS season_year   -- '2526' → 2025 (для FBref/FotMob)
    FROM iceberg.silver.xref_player
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

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

    -- ========= HARD_FACT (single column, COALESCE fb→fm→ws→us→ss) =========
    -- Integer counters get CAST(... AS BIGINT) — COALESCE между гетерогенными
    -- source types (FBref varchar, FotMob bigint, US bigint) иначе промотит
    -- в double и BI показывает `90.0` / `3.0`. Cross-source diff'ы (FBref - Х)
    -- хранятся в fct_player_season_stats_audit.
    CAST(COALESCE(fb.mp,                  fm.matches_played, ws.matches_seen, us.games_played) AS BIGINT) AS matches,
    CAST(COALESCE(fb.minutes,             fm.minutes_played, us.minutes_played)               AS BIGINT) AS minutes,
    CAST(COALESCE(fb.goals,               fm.goals,          us.goals)                        AS BIGINT) AS goals,
    CAST(COALESCE(fb.assists,             fm.assists,        us.assists)                      AS BIGINT) AS assists,
    CAST(COALESCE(fb.yellow_cards,        fm.yellow_cards,   us.yellow_cards)                 AS BIGINT) AS yellow_cards,
    CAST(COALESCE(fb.red_cards,           fm.red_cards,      us.red_cards)                    AS BIGINT) AS red_cards,
    CAST(COALESCE(fb.penalty_goals,                                                   ss.penalty_goals)  AS BIGINT) AS penalty_goals,
    CAST(COALESCE(fb.penalty_attempts,                                                ss.penalties_taken) AS BIGINT) AS penalty_attempts,
    CAST(COALESCE(fb.penalties_won,       fm.penalties_won,                           ss.penalty_won)    AS BIGINT) AS penalties_won,
    CAST(COALESCE(fb.penalties_conceded,  fm.penalties_conceded,                      ss.penalty_conceded) AS BIGINT) AS penalties_conceded,
    CAST(COALESCE(fb.shots,               fm.shots,          ws.shots_total,         us.shots,           ss.total_shots) AS BIGINT) AS shots,
    CAST(COALESCE(fb.shots_on_target,     fm.shots_on_target, ws.shots_on_target_proxy, ss.shots_on_target) AS BIGINT) AS shots_on_target,
    CAST(COALESCE(fb.interceptions,       fm.interceptions,  ws.interceptions,        ss.interceptions)  AS BIGINT) AS interceptions,
    CAST(COALESCE(fb.tackles_won,         ws.tackle_won,                              ss.tackles_won)    AS BIGINT) AS tackles_won,
    CAST(COALESCE(fm.tackles,             ws.tackle_att,                              ss.tackles)        AS BIGINT) AS tackles_attempted,
    CAST(COALESCE(fb.fouls_committed,     fm.fouls_committed, ws.fouls_committed,    ss.fouls)          AS BIGINT) AS fouls_committed,
    CAST(COALESCE(fb.fouls_drawn,                                                     ss.was_fouled)     AS BIGINT) AS fouls_drawn,
    CAST(COALESCE(fb.offsides,                                                        ss.offsides)       AS BIGINT) AS offsides,
    CAST(COALESCE(fm.clearances,          ws.clearances,                              ss.clearances)     AS BIGINT) AS clearances,
    CAST(COALESCE(fm.ball_recoveries,     ws.ball_recoveries,                         ss.ball_recoveries) AS BIGINT) AS ball_recoveries,
    CAST(COALESCE(fm.blocks,                                                          ss.blocks)         AS BIGINT) AS blocks,
    CAST(COALESCE(fm.successful_dribbles, ws.takeon_won,                              ss.dribbles)       AS BIGINT) AS successful_dribbles,
    CAST(ws.takeon_att                                                                                  AS BIGINT) AS dribbles_attempted,
    CAST(ws.dribbles                                                                                    AS BIGINT) AS dribbles_completed_ws,  -- WS-specific SPADL "take_on" count (semantically different from takeon_won)
    CAST(COALESCE(ws.pass_total,                                                      ss.total_passes)   AS BIGINT) AS pass_total,
    CAST(COALESCE(fm.accurate_passes,     ws.pass_ok,                                 ss.accurate_passes) AS BIGINT) AS accurate_passes,
    CAST(COALESCE(fm.accurate_long_balls,                                             ss.accurate_long_balls) AS BIGINT) AS accurate_long_balls,
    CAST(ss.total_long_balls                                                                            AS BIGINT) AS total_long_balls,
    CAST(COALESCE(fb.crosses,                                                         ss.total_crosses)  AS BIGINT) AS crosses,
    CAST(ss.accurate_crosses                                                                            AS BIGINT) AS accurate_crosses,
    CAST(COALESCE(us.key_passes,                                                      ss.key_passes)     AS BIGINT) AS key_passes,
    CAST(fb.own_goals                                                                                   AS BIGINT) AS own_goals,
    CAST(fb.second_yellow                                                                               AS BIGINT) AS second_yellow,
    CAST(COALESCE(us.non_penalty_goals,
                  CAST(fb.goals AS BIGINT) - CAST(fb.penalty_goals AS BIGINT))                          AS BIGINT) AS non_penalty_goals,

    -- ========= Percentages (single column, COALESCE) =========
    -- Платформы вычисляют по-разному, но разница ≤2% — приемлемо для single
    -- column. FotMob → WhoScored → SofaScore приоритет (sample-size order).
    ROUND(COALESCE(CAST(ws.pass_pct    AS DOUBLE), ss.accurate_passes_pct), 2)        AS pass_pct,
    ROUND(COALESCE(CAST(ws.takeon_pct  AS DOUBLE), ss.dribbles_pct), 2)               AS take_on_pct,
    ROUND(COALESCE(CAST(ws.tackle_pct  AS DOUBLE), ss.tackles_won_pct), 2)            AS tackle_pct,
    ROUND(ss.accurate_crosses_pct, 2)                                                  AS accurate_crosses_pct,
    ROUND(ss.accurate_long_balls_pct, 2)                                               AS accurate_long_balls_pct,

    -- ========= MODELED (разные модели у разных платформ → суффиксы) =========
    -- xG: FotMob (StatsBomb-derived), Understat (own), SofaScore (Opta-derived).
    -- xA: FotMob, Understat. Rating: FotMob (own 0-10), SofaScore (Opta 0-10).
    ROUND(fm.expected_goals, 2)                          AS expected_goals_fotmob,
    ROUND(us.expected_goals, 2)                          AS expected_goals_understat,
    ROUND(ss.expected_goals, 2)                          AS expected_goals_sofascore,
    ROUND(fm.expected_assists, 2)                        AS expected_assists_fotmob,
    ROUND(us.expected_assists, 2)                        AS expected_assists_understat,
    ROUND(fm.expected_goals_on_target, 2)                AS expected_goals_on_target,
    ROUND(us.non_penalty_xg, 2)                          AS non_penalty_xg_understat,
    ROUND(us.xg_chain, 2)                                AS xg_chain_understat,
    ROUND(us.xg_buildup, 2)                              AS xg_buildup_understat,
    ROUND(fm.fotmob_rating, 2)                           AS rating_fotmob,
    ROUND(ss.rating, 2)                                  AS rating_sofascore,

    -- ========= UNIQUE_FBREF =========
    -- complete_matches/starts/subs/unused_sub — playing-time breakdown (FBref-only)
    fb.complete_matches,
    fb.starts,
    fb.subs,
    fb.unused_sub,
    fb.plus_minus,
    ROUND(fb.points_per_match, 2)                        AS points_per_match,
    ROUND(fb.on_off_impact, 2)                           AS on_off_impact,
    ROUND(fb.goals_per_shot, 2)                          AS goals_per_shot,

    -- ========= UNIQUE_FOTMOB =========
    -- defensive_actions — FotMob composite (нет в других). big_chances_*/
    -- chances_created — FotMob proprietary. poss_won_final_third — FotMob
    -- pressing-метрика (SS даёт att_third, но определения отличаются).
    fm.defensive_actions,
    ROUND(fm.big_chances_created, 2)                     AS big_chances_created,
    ROUND(fm.big_chances_missed, 2)                      AS big_chances_missed,
    ROUND(fm.chances_created, 2)                         AS chances_created,
    fm.poss_won_final_third,

    -- ========= UNIQUE_WHOSCORED =========
    -- bad_touches/touches_in_box/avg_x/avg_y — WS-specific event-aggregates,
    -- нет аналогов у других источников.
    ws.bad_touches,
    ws.touches_in_box,
    ROUND(ws.avg_x, 2)                                   AS avg_x,
    ROUND(ws.avg_y, 2)                                   AS avg_y,

    -- ========= UNIQUE_SOFASCORE =========
    -- Aerial/ground/total duels — нет ни у одного другого источника.
    -- Errors lead to goal/shot — критическая дефенсивная метрика SofaScore.
    -- Touches/dispossessed/possession_lost — SofaScore-specific event-counts.
    -- Structure of goals (inside/outside box, headed/L/R-foot) — SofaScore-only.
    ss.ground_duels_won,
    ROUND(ss.ground_duels_won_pct, 2)                    AS ground_duels_won_pct,
    ss.aerial_duels_won,
    ROUND(ss.aerial_duels_won_pct, 2)                    AS aerial_duels_won_pct,
    ss.total_duels_won,
    ROUND(ss.total_duels_won_pct, 2)                     AS total_duels_won_pct,
    ss.errors_lead_to_goal,
    ss.errors_lead_to_shot,
    ss.touches,
    ss.dispossessed,
    ss.possession_lost,
    ss.poss_won_att_third                                AS poss_won_att_third_sofascore,
    ss.totw_appearances,
    ss.matches_started,
    ss.appearances,
    ss.dribbled_past,
    ss.secondary_assists,
    ss.final_third_passes,
    ss.shots_off_target,
    ss.shots_inside_box,
    ss.shots_outside_box,
    ss.blocked_shots,
    ss.hit_woodwork,
    ROUND(ss.goal_conversion_pct, 2)                     AS goal_conversion_pct,
    ss.goals_inside_box,
    ss.goals_outside_box,
    ss.headed_goals,
    ss.left_foot_goals,
    ss.right_foot_goals,
    ss.set_piece_shots,
    ss.free_kick_goals,

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
LEFT JOIN iceberg.silver.whoscored_player_season_aggregate ws
    ON  ws.canonical_id = xf.canonical_id
    AND ws.league       = xf.league
    AND ws.season       = xf.season_slug
LEFT JOIN iceberg.silver.understat_player_season_aggregate us
    ON  us.canonical_id = xf.canonical_id
    AND us.league       = xf.league
    AND us.season       = xf.season_slug
LEFT JOIN iceberg.silver.sofascore_player_season_aggregate ss
    ON  ss.canonical_id = xf.canonical_id
    AND ss.league       = xf.league
    AND ss.season       = xf.season_slug
-- Outfield-only: exclude вратарей (они в fct_keeper_season_stats).
WHERE fb.pos IS NULL OR fb.pos NOT LIKE '%GK%'
