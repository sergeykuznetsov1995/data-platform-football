-- =============================================================================
-- Gold: feat_player_form
-- =============================================================================
-- Rolling per-player features (last 5 appearances).
-- Point-in-time safe: window excludes current match.
--
-- Sources: iceberg.gold.fct_player_match
-- PK: (match_id, player_id)
-- Partitioning: (league, season)
-- =============================================================================

WITH base AS (
    SELECT
        match_id,
        player_id,
        team_id,
        league,
        season,
        minutes,
        goals,
        assists,
        shots,
        shots_on_target,
        yellow_cards,
        red_cards,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, season
            ORDER BY match_id
        ) AS appearance_rn
    FROM iceberg.gold.fct_player_match
),
rolled AS (
    SELECT
        *,
        AVG(CAST(minutes          AS DOUBLE)) OVER w AS l5_minutes_avg_raw,
        AVG(CAST(goals            AS DOUBLE)) OVER w AS l5_goals_avg_raw,
        AVG(CAST(assists          AS DOUBLE)) OVER w AS l5_assists_avg_raw,
        AVG(CAST(shots            AS DOUBLE)) OVER w AS l5_shots_avg_raw,
        AVG(CAST(shots_on_target  AS DOUBLE)) OVER w AS l5_sot_avg_raw,
        SUM(goals)   OVER w                          AS l5_goals_sum_raw,
        SUM(assists) OVER w                          AS l5_assists_sum_raw,
        SUM(yellow_cards) OVER w                     AS l5_yellows_sum_raw,
        SUM(red_cards)    OVER w                     AS l5_reds_sum_raw
    FROM base
    WINDOW w AS (
        PARTITION BY player_id, season
        ORDER BY match_id
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    )
)
SELECT
    match_id,
    player_id,
    team_id,
    CASE WHEN appearance_rn > 5 THEN l5_minutes_avg_raw  END AS l5_minutes_avg,
    CASE WHEN appearance_rn > 5 THEN l5_goals_avg_raw    END AS l5_goals_avg,
    CASE WHEN appearance_rn > 5 THEN l5_assists_avg_raw  END AS l5_assists_avg,
    CASE WHEN appearance_rn > 5 THEN l5_shots_avg_raw    END AS l5_shots_avg,
    CASE WHEN appearance_rn > 5 THEN l5_sot_avg_raw      END AS l5_sot_avg,
    CASE WHEN appearance_rn > 5 THEN l5_goals_sum_raw    END AS l5_goals_sum,
    CASE WHEN appearance_rn > 5 THEN l5_assists_sum_raw  END AS l5_assists_sum,
    CASE WHEN appearance_rn > 5 THEN l5_yellows_sum_raw  END AS l5_yellows_sum,
    CASE WHEN appearance_rn > 5 THEN l5_reds_sum_raw     END AS l5_reds_sum,
    appearance_rn                                           AS appearances_so_far,
    league,
    season
FROM rolled
