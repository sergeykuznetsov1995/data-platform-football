-- =============================================================================
-- Gold: sofascore_team_season (per-source season aggregate; migrated from Silver, #370)
-- =============================================================================
--
-- One row per (team_id, league, season) — season-level SofaScore team
-- aggregates. Pure rollup of `silver.sofascore_team_match` via SUM/AVG.
--
-- Sources:
--   silver.sofascore_team_match  — match-grain team facts (this PR).
--
-- Why a single-source rollup:
--   `team_match` already merges all SofaScore inputs (match_stats PIVOT +
--   schedule outcome + player_match_aggregate rollup for minutes/assists),
--   so a season rollup is just one GROUP BY. No need for a second JOIN on
--   `player_season_aggregate` — duplication risk avoided.
--
-- Coverage gaps (NULL on purpose):
--   * `expected_assists`, `was_fouled`, `penalties_won`, `penalties_conceded`
--     — not available in `bronze.sofascore_match_stats`; downstream Gold
--     (#94 fct_team_season_stats) COALESCEs them from FBref / Understat.
-- =============================================================================

WITH match_rollup AS (
    SELECT
        team_id,
        league,
        season,

        -- ===== Identity / appearances =====
        COUNT(*)                  AS appearances,
        SUM(minutes)              AS minutes_played,

        -- ===== HARD_FACT — outcome =====
        SUM(goals_for)            AS goals,
        SUM(goals_against)        AS goals_conceded,
        SUM(assists)              AS assists,

        -- ===== HARD_FACT — discipline =====
        SUM(yellow_cards)         AS yellow_cards,
        SUM(red_cards)            AS red_cards,
        SUM(fouls)                AS fouls_committed,
        SUM(offsides)             AS offsides,

        -- ===== HARD_FACT — shots =====
        SUM(total_shots)          AS total_shots,
        SUM(shots_on_target)      AS shots_on_target,

        -- ===== HARD_FACT — defending =====
        SUM(interceptions)        AS interceptions,
        SUM(tackles_won)          AS tackles_won,
        SUM(total_tackles)        AS total_tackles,

        -- ===== HARD_FACT — passing =====
        SUM(total_passes)         AS total_passes,
        SUM(accurate_passes)      AS accurate_passes,

        -- ===== Possession =====
        AVG(possession_pct)       AS possession_pct_avg,

        -- ===== MODELED =====
        SUM(expected_goals)         AS expected_goals,
        SUM(expected_goals_against) AS expected_goals_against,

        -- ===== UNIQUE_SOFASCORE — corners =====
        SUM(corner_kicks)         AS corner_kicks,

        -- ===== UNIQUE_SOFASCORE — duels (totals for pct derivation) =====
        SUM(ground_duels_won)     AS ground_duels_won,
        SUM(ground_duels_total)   AS ground_duels_total,
        SUM(aerial_duels_won)     AS aerial_duels_won,
        SUM(aerial_duels_total)   AS aerial_duels_total,

        -- ===== UNIQUE_SOFASCORE — long balls / crosses =====
        SUM(accurate_long_balls)  AS accurate_long_balls,
        SUM(total_long_balls)     AS total_long_balls,
        SUM(accurate_crosses)     AS accurate_crosses,
        SUM(total_crosses)        AS total_crosses,

        -- ===== Lineage =====
        MAX(_bronze_ingested_at)  AS _bronze_ingested_at

    FROM iceberg.silver.sofascore_team_match
    WHERE team_id IS NOT NULL
    GROUP BY team_id, league, season
)

SELECT
    -- ===== Identity =====
    team_id,

    -- ===== Appearances =====
    CAST(appearances    AS INTEGER) AS appearances,
    minutes_played,

    -- ===== HARD_FACT — outcome =====
    CAST(goals          AS INTEGER) AS goals,
    CAST(goals_conceded AS INTEGER) AS goals_conceded,
    CAST(assists        AS INTEGER) AS assists,

    -- ===== HARD_FACT — discipline =====
    CAST(yellow_cards    AS INTEGER) AS yellow_cards,
    CAST(red_cards       AS INTEGER) AS red_cards,
    CAST(fouls_committed AS INTEGER) AS fouls_committed,
    CAST(NULL AS INTEGER)            AS was_fouled,           -- Not in SS match_stats; Gold uses Understat
    CAST(offsides        AS INTEGER) AS offsides,

    -- ===== HARD_FACT — shots =====
    CAST(total_shots     AS INTEGER) AS total_shots,
    CAST(shots_on_target AS INTEGER) AS shots_on_target,

    -- ===== HARD_FACT — defending =====
    CAST(interceptions AS INTEGER) AS interceptions,
    CAST(tackles_won   AS INTEGER) AS tackles_won,
    CAST(total_tackles AS INTEGER) AS total_tackles,
    CAST(NULL AS INTEGER)          AS penalty_won,            -- Not in SS match_stats
    CAST(NULL AS INTEGER)          AS penalty_conceded,       -- Not in SS match_stats

    -- ===== HARD_FACT — passing =====
    CAST(total_passes    AS INTEGER) AS total_passes,
    CAST(accurate_passes AS INTEGER) AS accurate_passes,
    CASE
        WHEN total_passes > 0
            THEN ROUND(100.0 * accurate_passes / total_passes, 2)
        ELSE NULL
    END                              AS accurate_passes_pct,

    -- ===== Possession =====
    ROUND(possession_pct_avg, 2) AS possession_pct_avg,

    -- ===== MODELED =====
    ROUND(expected_goals,         2) AS expected_goals,
    ROUND(expected_goals_against, 2) AS expected_goals_against,
    CAST(NULL AS DOUBLE)             AS expected_assists,     -- Not in SS match_stats

    -- ===== UNIQUE_SOFASCORE — corners =====
    CAST(corner_kicks AS INTEGER) AS corner_kicks,

    -- ===== UNIQUE_SOFASCORE — duels =====
    CAST(ground_duels_won   AS INTEGER) AS ground_duels_won,
    CAST(ground_duels_total AS INTEGER) AS ground_duels_total,
    CASE
        WHEN ground_duels_total > 0
            THEN ROUND(100.0 * ground_duels_won / ground_duels_total, 2)
        ELSE NULL
    END                                 AS ground_duels_won_pct,
    CAST(aerial_duels_won   AS INTEGER) AS aerial_duels_won,
    CAST(aerial_duels_total AS INTEGER) AS aerial_duels_total,
    CASE
        WHEN aerial_duels_total > 0
            THEN ROUND(100.0 * aerial_duels_won / aerial_duels_total, 2)
        ELSE NULL
    END                                 AS aerial_duels_won_pct,
    CASE
        WHEN (ground_duels_total + aerial_duels_total) > 0
            THEN ROUND(
                100.0 * (ground_duels_won + aerial_duels_won)
                      / (ground_duels_total + aerial_duels_total), 2
            )
        ELSE NULL
    END                                 AS total_duels_won_pct,

    -- ===== UNIQUE_SOFASCORE — long balls / crosses =====
    CAST(accurate_long_balls AS INTEGER) AS accurate_long_balls,
    CAST(total_long_balls    AS INTEGER) AS total_long_balls,
    CASE
        WHEN total_long_balls > 0
            THEN ROUND(100.0 * accurate_long_balls / total_long_balls, 2)
        ELSE NULL
    END                                  AS accurate_long_balls_pct,
    CAST(accurate_crosses AS INTEGER) AS accurate_crosses,
    CAST(total_crosses    AS INTEGER) AS total_crosses,

    -- ===== Lineage =====
    _bronze_ingested_at,

    -- ===== Partition keys =====
    league,
    season

FROM match_rollup
