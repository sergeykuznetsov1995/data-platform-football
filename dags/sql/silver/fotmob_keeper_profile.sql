-- =============================================================================
-- Silver: fotmob_keeper_profile
-- =============================================================================
--
-- One wide row per goalkeeper / league / season.
--
-- Симметрично silver.fbref_keeper_profile, но из FotMob Bronze.
--
-- Filter: LOWER(primary_position) = 'keeper' AND NOT is_coach
--          (вратари из FotMob /api/playerData, без тренеров).
--
-- Содержит:
--   * Identity (per-season): player_id, player_name, primary_team_*
--   * Volume:        matches_played, minutes_played, fotmob_rating
--   * Goalkeeping:   clean_sheets, goals_conceded_per_90, save_percentage,
--                    saves_per_90, goals_prevented
--   * Distribution:  accurate_passes_per_90, accurate_long_balls_per_90
--   * Discipline:    yellow_cards, red_cards, fouls_per_90
--
-- Time-invariant атрибуты (birth_date/height/foot/country) НЕ хранятся здесь —
-- уйдут в silver.fotmob_player_profile (snapshot, T4 backlog).
-- =============================================================================

WITH details_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fotmob_player_details
),

stats_dedup AS (
    -- Dedup long-format stats by (participant_id, league, season, stat_name)
    -- BEFORE the MAX-pivot — otherwise MAX(stat_value) over multiple snapshots
    -- returns the historical high for non-monotonic metrics (rating,
    -- save_percentage), not the latest value. _batch_id breaks ties (#464).
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY participant_id, league, season, stat_name
                ORDER BY _ingested_at DESC, _batch_id DESC
            ) AS rn
        FROM iceberg.bronze.fotmob_player_stats s
    )
    WHERE rn = 1
),

stats_pivoted AS (
    SELECT
        CAST(participant_id AS VARCHAR) AS player_id,
        league,
        season,
        MAX(_ingested_at) AS _stats_ingested_at,
        MAX(matches_played) AS matches_played,
        -- Volume
        MAX(CASE WHEN stat_name = 'mins_played' THEN stat_value END) AS minutes_played,
        MAX(CASE WHEN stat_name = 'rating' THEN stat_value END) AS fotmob_rating,
        -- Goalkeeping
        MAX(CASE WHEN stat_name = 'clean_sheet' THEN stat_value END) AS clean_sheets,
        MAX(CASE WHEN stat_name = 'goals_conceded' THEN stat_value END) AS goals_conceded_per_90,
        MAX(CASE WHEN stat_name = '_save_percentage' THEN stat_value END) AS save_percentage,
        MAX(CASE WHEN stat_name = 'saves' THEN stat_value END) AS saves_per_90,
        MAX(CASE WHEN stat_name = '_goals_prevented' THEN stat_value END) AS goals_prevented,
        -- Distribution (GK passing)
        MAX(CASE WHEN stat_name = 'accurate_pass' THEN stat_value END) AS accurate_passes_per_90,
        MAX(CASE WHEN stat_name = 'accurate_long_balls' THEN stat_value END) AS accurate_long_balls_per_90,
        -- Discipline
        MAX(CASE WHEN stat_name = 'yellow_card' THEN stat_value END) AS yellow_cards,
        MAX(CASE WHEN stat_name = 'red_card' THEN stat_value END) AS red_cards,
        MAX(CASE WHEN stat_name = 'fouls' THEN stat_value END) AS fouls_per_90
    FROM stats_dedup
    GROUP BY participant_id, league, season
)

SELECT
    -- ========= Identity (per-season attributes only) =========
    d.player_id,
    d.name                                                            AS player_name,
    d.primary_team_id,
    d.primary_team_name,

    -- ========= Volume =========
    s.matches_played,
    s.minutes_played,
    s.fotmob_rating,

    -- ========= Goalkeeping =========
    s.clean_sheets,
    s.goals_conceded_per_90,
    s.save_percentage,
    s.saves_per_90,
    s.goals_prevented,

    -- ========= Distribution =========
    s.accurate_passes_per_90,
    s.accurate_long_balls_per_90,

    -- ========= Discipline =========
    s.yellow_cards,
    s.red_cards,
    s.fouls_per_90,

    -- ========= Lineage =========
    GREATEST(d._ingested_at, COALESCE(s._stats_ingested_at, d._ingested_at)) AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season → slug ('2425'); FotMob bronze stores year-start bigint (2024).
    d.league,
    LPAD(CAST(MOD(d.season,     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(d.season + 1, 100) AS varchar), 2, '0') AS season

FROM details_dedup d
LEFT JOIN stats_pivoted s
    ON  d.player_id = s.player_id
    AND d.league    = s.league
    AND d.season    = s.season
WHERE d.rn = 1
  AND NOT d.is_coach
  AND LOWER(d.position_description) = 'keeper'
