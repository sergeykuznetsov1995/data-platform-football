-- =============================================================================
-- Silver: espn_player_match_aggregate (native v2)
-- =============================================================================
-- Grain/PK: one row per (event_id, team_id, athlete_id), played final only.
-- Sources (native v2): compact6 canonical lineup rows (event_id/team_id/athlete_id bigint,
-- starter/substitution booleans, counters numeric, JSON varchars); schedule event
-- rows (event_id bigint, status varchar, played_final boolean, partition fields).
-- Notes: schedule supplies played status and partitions; lineup remains the player
-- grain and is deduplicated before any JSON array operation.
-- Footguns: player ids may occur for both teams in a match; JSON jersey outranks
-- bronze jersey; empty statistics means unavailable, while zero remains a fact.
-- didScore/didAssist comes from per-player plays. Live calibration across all 316
-- STATUS_FINAL_PEN events confirmed didScore does not include shootout kicks and
-- never exceeds regulation scoreboard goals, so it is counted as-is (no cap/filter).
-- Minutes are best-effort because ESPN stores summed stoppage minutes.
-- DAG integration: run_silver_transform wraps this pure SELECT in CTAS.
-- =============================================================================

WITH espn_downstream_scope (
    scope_id, espn_id, source_season_year, platform_league,
    platform_season_slug, convention, effective_start_date, effective_end_date
) AS (VALUES
__ESPN_DOWNSTREAM_SCOPE_VALUES__
),
bronze_src_schedule AS (
    SELECT
        es_source.*,
        espn_scope.platform_league AS platform_league,
        espn_scope.platform_season_slug AS platform_season_slug
    FROM iceberg.bronze.espn_schedule AS es_source
    JOIN espn_downstream_scope espn_scope ON
__ESPN_DOWNSTREAM_SCOPE_FILTER__
),
schedule_dedup AS (
    SELECT * FROM (
        SELECT b.*, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingested_at DESC) AS rn
        FROM bronze_src_schedule b
    ) WHERE rn = 1
),
bronze_src_lineup AS (
    SELECT es_source.*
    FROM iceberg.bronze.espn_lineup AS es_source
    JOIN espn_downstream_scope espn_scope ON
__ESPN_DOWNSTREAM_SCOPE_FILTER__
),
lineup_dedup AS (
    SELECT * FROM (
        SELECT b.*, ROW_NUMBER() OVER (
            PARTITION BY event_id, team_id, athlete_id ORDER BY _ingested_at DESC
        ) AS rn
        FROM bronze_src_lineup b
    ) WHERE rn = 1
),
lineup_played AS (
    SELECT
        l.*,
        s.status,
        s.platform_league,
        s.platform_season_slug
    FROM lineup_dedup l
    JOIN schedule_dedup s ON s.event_id = l.event_id
    WHERE s.played_final
),
player_modeled AS (
    SELECT
        l.*,
        starter AS is_starter,
        TRY_CAST(sub_in AS integer) AS sub_in_minute,
        TRY_CAST(sub_out AS integer) AS sub_out_minute,
        NULLIF(NULLIF(position, ''), 'Substitute') AS clean_position
    FROM lineup_played l
)
SELECT
    -- ===== Identity / Attributes =====
    event_id,
    team_id,
    team,
    athlete_id,
    player AS player_name,
    TRY_CAST(COALESCE(json_extract_scalar(extra_json, '$.jersey'), NULLIF(jersey, '')) AS integer) AS jersey,
    clean_position AS position,
    CASE
        WHEN clean_position = 'Goalkeeper' THEN 'GK'
        WHEN clean_position LIKE '%Back%' OR clean_position LIKE '%Defender%' OR clean_position = 'Sweeper' THEN 'DF'
        WHEN clean_position LIKE '%Midfielder%' THEN 'MF'
        WHEN clean_position LIKE '%Forward%' THEN 'FW'
    END AS position_group,
    NULLIF(TRY_CAST(formation_place AS integer), 0) AS formation_place,
    is_starter,
    CAST(NULL AS boolean) AS is_captain,
    subbed_in,
    subbed_out,
    sub_in_minute,
    sub_out_minute,
    TRY_CAST(json_extract_scalar(extra_json, '$.subbedInFor.athlete.id') AS bigint) AS sub_in_for_athlete_id,
    TRY_CAST(json_extract_scalar(extra_json, '$.subbedOutFor.athlete.id') AS bigint) AS sub_out_for_athlete_id,
    CASE
        WHEN is_starter OR subbed_in THEN GREATEST(
            COALESCE(sub_out_minute, CASE WHEN status IN ('STATUS_FINAL_AET', 'STATUS_FINAL_PEN') THEN 120 ELSE 90 END)
            - COALESCE(sub_in_minute, 0),
            0
        )
    END AS minutes_played,
    statistics_json IS NOT NULL AND statistics_json <> '' AND statistics_json <> '[]' AS has_stats,

    -- ===== HARD_FACT =====
    TRY_CAST(appearances AS integer) AS appearances,
    TRY_CAST(total_goals AS integer) AS goals,
    TRY_CAST(goal_assists AS integer) AS assists,
    TRY_CAST(total_shots AS integer) AS shots,
    TRY_CAST(shots_on_target AS integer) AS shots_on_target,
    TRY_CAST(saves AS integer) AS saves,
    TRY_CAST(goals_conceded AS integer) AS goals_conceded,
    TRY_CAST(shots_faced AS integer) AS shots_faced,
    TRY_CAST(fouls_committed AS integer) AS fouls_committed,
    TRY_CAST(fouls_suffered AS integer) AS fouls_drawn,
    TRY_CAST(offsides AS integer) AS offsides,
    TRY_CAST(own_goals AS integer) AS own_goals,
    TRY_CAST(red_cards AS integer) AS red_cards,
    TRY_CAST(yellow_cards AS integer) AS yellow_cards,
    TRY_CAST(sub_ins AS integer) AS sub_ins,

    -- ===== MODELED =====
    COALESCE(cardinality(filter(
        CAST(json_extract(substitutions_json, '$.plays') AS array<json>),
        p -> json_extract_scalar(p, '$.didScore') = 'true'
    )), 0) AS goals_events,
    COALESCE(cardinality(filter(
        CAST(json_extract(substitutions_json, '$.plays') AS array<json>),
        p -> json_extract_scalar(p, '$.didAssist') = 'true'
    )), 0) AS assists_events,

    -- ===== Cross-source player-match parity (not in ESPN) =====
    CAST(NULL AS double) AS rating,
    CAST(NULL AS double) AS xg,
    CAST(NULL AS double) AS xa,
    CAST(NULL AS double) AS shots_blocked,
    CAST(NULL AS double) AS shots_off_target,
    CAST(NULL AS double) AS crosses,
    CAST(NULL AS double) AS accurate_crosses,
    CAST(NULL AS double) AS tackles,
    CAST(NULL AS double) AS tackles_won,
    CAST(NULL AS double) AS interceptions,
    CAST(NULL AS double) AS clearances,
    CAST(NULL AS double) AS blocks,
    CAST(NULL AS double) AS ball_recoveries,
    CAST(NULL AS double) AS errors_lead_to_goal,
    CAST(NULL AS double) AS errors_lead_to_shot,
    CAST(NULL AS double) AS passes,
    CAST(NULL AS double) AS passes_completed,
    CAST(NULL AS double) AS key_passes,
    CAST(NULL AS double) AS accurate_long_balls,
    CAST(NULL AS double) AS total_long_balls,
    CAST(NULL AS double) AS dribbles_attempted,
    CAST(NULL AS double) AS dribbles_won,
    CAST(NULL AS double) AS total_duels_won,
    CAST(NULL AS double) AS total_duels_lost,
    CAST(NULL AS double) AS aerial_duels_won,
    CAST(NULL AS double) AS aerial_duels_lost,
    CAST(NULL AS double) AS challenge_lost,
    CAST(NULL AS double) AS touches,
    CAST(NULL AS double) AS dispossessed,
    CAST(NULL AS double) AS possession_lost,
    CAST(NULL AS double) AS penalties_won,
    CAST(NULL AS double) AS penalties_conceded,
    CAST(NULL AS double) AS penalties_missed,
    CAST(NULL AS double) AS penalty_goals,
    CAST(NULL AS double) AS penalty_saves,
    CAST(NULL AS double) AS ground_duels_won,

    -- ===== Lineage =====
    _ingested_at AS _bronze_ingested_at,

    -- ===== Partition keys =====
    platform_league AS league,
    platform_season_slug AS season
FROM player_modeled
