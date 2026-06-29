-- =============================================================================
-- Silver: sofascore_player_season_aggregate
-- =============================================================================
--
-- One row per (canonical_id, league, season) — SofaScore season-level player
-- stats bridged through `silver.xref_player` (source='sofascore'). Mirrors
-- shape of `silver.understat_player_season_aggregate` and
-- `silver.whoscored_player_season_aggregate` so Gold layer can LEFT JOIN all
-- three on (canonical_id, league, season) varchar-slug.
--
-- Bronze `bronze.sofascore_player_season_stats` is already season-grain
-- (PK = (player_id, season)) — no aggregation needed.
--
-- Sources:
--   bronze.sofascore_player_season_stats
--   silver.xref_player WHERE source='sofascore'
--
-- Notes:
--   * (league, season) JOIN predicate against xref_player MANDATORY
--     (CLAUDE.md / feedback_xref_join_season_predicate.md).
--   * Orphan rows filtered out — they can't bridge to FBref spine in Gold.
--   * Season convention: varchar slug ('2526' for 2025/26), matches xref_player.
--   * bronze.sofascore_player_season_stats.player_id is VARCHAR; xref_player
--     source_id is varchar — direct = compare.
--   * Bronze is full-state replace_partitions, so ROW_NUMBER dedup is
--     defensive (against future ingest-mode regression), realistically 1:1.
-- =============================================================================

WITH xp AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_player
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
),

bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.sofascore_player_season_stats b
        WHERE player_id IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    canonical_id,
    rating, tackles, tackles_won, tackles_won_pct, interceptions, dribbles,
    dribbles_pct, ground_duels_won, ground_duels_won_pct, aerial_duels_won,
    aerial_duels_won_pct, total_duels_won, total_duels_won_pct, total_passes,
    accurate_passes, accurate_passes_pct, final_third_passes, key_passes,
    secondary_assists, total_crosses, accurate_crosses, accurate_crosses_pct,
    total_long_balls, accurate_long_balls, accurate_long_balls_pct, total_shots,
    shots_on_target, shots_off_target, shots_inside_box, shots_outside_box,
    blocked_shots, hit_woodwork, offsides, expected_goals, penalties_taken,
    penalty_goals, penalty_won, penalty_conceded, was_fouled, fouls,
    dribbled_past, errors_lead_to_goal, errors_lead_to_shot, clearances,
    ball_recoveries, blocks, poss_won_att_third, touches, dispossessed,
    possession_lost, totw_appearances, matches_started, appearances,
    goal_conversion_pct, goals_inside_box, goals_outside_box, headed_goals,
    left_foot_goals, right_foot_goals, set_piece_shots, free_kick_goals,
    league, season
FROM (
SELECT
    xp.canonical_id,

    -- ========= UNIQUE_SOFASCORE (consumed by Gold fct_player_season_stats) =========
    -- Bronze `successful_dribbles` aliased as `dribbles` для symmetry с WS.
    b.rating                              AS rating,
    b.tackles                             AS tackles,
    b.tackles_won                         AS tackles_won,
    b.tackles_won_percentage              AS tackles_won_pct,
    b.interceptions                       AS interceptions,
    b.successful_dribbles                 AS dribbles,
    b.successful_dribbles_percentage      AS dribbles_pct,

    -- Дуэли (ground+aerial+total) — нет ни у одного из 4-х других источников.
    b.ground_duels_won                    AS ground_duels_won,
    b.ground_duels_won_percentage         AS ground_duels_won_pct,
    b.aerial_duels_won                    AS aerial_duels_won,
    b.aerial_duels_won_percentage         AS aerial_duels_won_pct,
    b.total_duels_won                     AS total_duels_won,
    b.total_duels_won_percentage          AS total_duels_won_pct,

    -- Passing (SofaScore-точные totals + pct)
    b.total_passes                        AS total_passes,
    b.accurate_passes                     AS accurate_passes,
    b.accurate_passes_percentage          AS accurate_passes_pct,
    b.accurate_final_third_passes         AS final_third_passes,
    b.key_passes                          AS key_passes,
    b.pass_to_assist                      AS secondary_assists,

    -- Crosses / long balls
    b.total_cross                         AS total_crosses,
    b.accurate_crosses                    AS accurate_crosses,
    b.accurate_crosses_percentage         AS accurate_crosses_pct,
    b.total_long_balls                    AS total_long_balls,
    b.accurate_long_balls                 AS accurate_long_balls,
    b.accurate_long_balls_percentage      AS accurate_long_balls_pct,

    -- Shots (SofaScore breakdown)
    b.total_shots                         AS total_shots,
    b.shots_on_target                     AS shots_on_target,
    b.shots_off_target                    AS shots_off_target,
    b.shots_from_inside_the_box           AS shots_inside_box,
    b.shots_from_outside_the_box          AS shots_outside_box,
    b.blocked_shots                       AS blocked_shots,
    b.hit_woodwork                        AS hit_woodwork,
    b.offsides                            AS offsides,
    b.expected_goals                      AS expected_goals,

    -- Penalties (SofaScore — самый полный breakdown)
    b.penalties_taken                     AS penalties_taken,
    b.penalty_goals                       AS penalty_goals,
    b.penalty_won                         AS penalty_won,
    b.penalty_conceded                    AS penalty_conceded,

    -- Фолы (двусторонние)
    b.was_fouled                          AS was_fouled,
    b.fouls                               AS fouls,
    b.dribbled_past                       AS dribbled_past,

    -- Грубые ошибки — критическая дефенсивная метрика
    b.error_lead_to_goal                  AS errors_lead_to_goal,
    b.error_lead_to_shot                  AS errors_lead_to_shot,

    -- Defensive volumes (третий источник параллельно FotMob/WhoScored)
    b.clearances                          AS clearances,
    b.ball_recovery                       AS ball_recoveries,
    b.outfielder_blocks                   AS blocks,
    b.possession_won_att_third            AS poss_won_att_third,

    -- Касания / потери владения
    b.touches                             AS touches,
    b.dispossessed                        AS dispossessed,
    b.possession_lost                     AS possession_lost,

    -- Proprietary SofaScore сигнал
    b.totw_appearances                    AS totw_appearances,
    b.matches_started                     AS matches_started,
    b.appearances                         AS appearances,

    -- Конверсия + структура голов
    b.goal_conversion_percentage          AS goal_conversion_pct,
    b.goals_from_inside_the_box           AS goals_inside_box,
    b.goals_from_outside_the_box          AS goals_outside_box,
    b.headed_goals                        AS headed_goals,
    b.left_foot_goals                     AS left_foot_goals,
    b.right_foot_goals                    AS right_foot_goals,
    b.shot_from_set_piece                 AS set_piece_shots,
    b.free_kick_goal                      AS free_kick_goals,

    -- ========= Partition keys =========
    b.league,
    b.season,

    -- Dedup AFTER the xref JOIN — one canonical_id can map to >1 sofascore
    -- source_id within a (league, season), fanning the JOIN out. Keep the most
    -- "active" record (most appearances); mirrors understat canonical_rn (#464).
    ROW_NUMBER() OVER (
        PARTITION BY xp.canonical_id, b.league, b.season
        ORDER BY b.appearances DESC NULLS LAST, b.player_id
    ) AS canonical_rn

FROM bronze_dedup b
JOIN xp
  ON b.player_id = xp.source_id
 AND b.league    = xp.league
 AND b.season    = xp.season
)
WHERE canonical_rn = 1
