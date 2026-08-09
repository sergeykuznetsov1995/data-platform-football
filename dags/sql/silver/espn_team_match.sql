-- =============================================================================
-- Silver: espn_team_match (native v2)
-- =============================================================================
-- Grain/PK: one row per (event_id, team_id), played final matches only.
-- Sources (native v2): compact6 canonical schedule event rows (event_id/team ids bigint, score
-- integers, played_final boolean, extra_json varchar); matchsheet team rows
-- (event_id/team_id bigint, score integer, measured stat varchars).
-- Notes: matchsheet is authoritative for both sides' score when available; the
-- schedule score is a per-side fallback. Scoreboard JSON is used only for events
-- with no matchsheet at all.
-- Matchsheet wins wholesale per event; scoreboard statistics are a fallback only.
-- Footguns: ESPN 0 is a genuine measured value, not missing data; arg.2 carries
-- cards only; per-league stat availability differs. Bronze percentage fields are
-- coarse fragments and are intentionally recalculated from counters. Dead Bronze
-- fields (capacity, total_goals, goal_assists, goals_conceded, referee_id and the
-- duplicate corner_kicks) are deliberately not projected.
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
bronze_src_matchsheet AS (
    SELECT es_source.*
    FROM iceberg.bronze.espn_matchsheet AS es_source
    JOIN espn_downstream_scope espn_scope ON
__ESPN_DOWNSTREAM_SCOPE_FILTER__
),
matchsheet_dedup AS (
    SELECT * FROM (
        SELECT b.*, ROW_NUMBER() OVER (
            PARTITION BY event_id, team_id ORDER BY _ingested_at DESC
        ) AS rn
        FROM bronze_src_matchsheet b
    ) WHERE rn = 1
),
matches_with_matchsheet AS (
    SELECT DISTINCT event_id FROM matchsheet_dedup
),
matchsheet_rows AS (
    SELECT
        s.event_id, m.team_id,
        CASE WHEN m.team_id = s.home_team_id THEN s.home_team ELSE s.away_team END AS team,
        CASE WHEN m.team_id = s.home_team_id THEN s.away_team_id ELSE s.home_team_id END AS opponent_team_id,
        m.team_id = s.home_team_id AS is_home,
        COALESCE(m.score, CASE WHEN m.team_id = s.home_team_id THEN s.home_score ELSE s.away_score END) AS goals_for,
        COALESCE(
            opponent.score,
            CASE WHEN m.team_id = s.home_team_id THEN s.away_score ELSE s.home_score END
        ) AS goals_against,
        TRY_CAST(m.possession_pct AS double) AS possession_pct,
        TRY_CAST(m.total_shots AS integer) AS total_shots,
        TRY_CAST(m.shots_on_target AS integer) AS shots_on_target,
        TRY_CAST(m.blocked_shots AS integer) AS blocked_shots,
        TRY_CAST(m.penalty_kick_goals AS integer) AS penalty_kick_goals,
        TRY_CAST(m.penalty_kick_shots AS integer) AS penalty_kick_shots,
        TRY_CAST(m.total_passes AS integer) AS total_passes,
        TRY_CAST(m.accurate_passes AS integer) AS accurate_passes,
        TRY_CAST(m.total_crosses AS integer) AS total_crosses,
        TRY_CAST(m.accurate_crosses AS integer) AS accurate_crosses,
        TRY_CAST(m.total_long_balls AS integer) AS total_long_balls,
        TRY_CAST(m.accurate_long_balls AS integer) AS accurate_long_balls,
        TRY_CAST(m.total_tackles AS integer) AS total_tackles,
        TRY_CAST(m.effective_tackles AS integer) AS effective_tackles,
        TRY_CAST(m.interceptions AS integer) AS interceptions,
        TRY_CAST(m.effective_clearance AS integer) AS effective_clearance,
        TRY_CAST(m.total_clearance AS integer) AS total_clearance,
        TRY_CAST(m.saves AS integer) AS saves,
        TRY_CAST(m.fouls_committed AS integer) AS fouls_committed,
        TRY_CAST(m.offsides AS integer) AS offsides,
        TRY_CAST(m.won_corners AS integer) AS corner_kicks,
        TRY_CAST(m.yellow_cards AS integer) AS yellow_cards,
        TRY_CAST(m.red_cards AS integer) AS red_cards,
        'matchsheet' AS stats_source,
        m._ingested_at AS _bronze_ingested_at,
        s.platform_league,
        s.platform_season_slug
    FROM matchsheet_dedup m
    JOIN schedule_dedup s ON s.event_id = m.event_id
    LEFT JOIN matchsheet_dedup opponent
      ON opponent.event_id = m.event_id
     AND opponent.team_id = CASE
         WHEN m.team_id = s.home_team_id THEN s.away_team_id
         ELSE s.home_team_id
     END
    WHERE s.played_final
),
scoreboard_home_flat AS (
    SELECT s.event_id, s._ingested_at, s.platform_league, s.platform_season_slug,
           s.home_team_id AS team_id, s.home_team AS team, s.away_team_id AS opponent_team_id,
           TRUE AS is_home, s.home_score AS goals_for, s.away_score AS goals_against,
           json_extract_scalar(stat, '$.name') AS stat_name,
           json_extract_scalar(stat, '$.displayValue') AS stat_value
    FROM schedule_dedup s
    CROSS JOIN UNNEST(CAST(json_extract(s.extra_json, '$.sides.home.competitor.statistics') AS array<json>)) AS u(stat)
    LEFT JOIN matches_with_matchsheet m ON m.event_id = s.event_id
    WHERE s.played_final AND m.event_id IS NULL
),
scoreboard_away_flat AS (
    SELECT s.event_id, s._ingested_at, s.platform_league, s.platform_season_slug,
           s.away_team_id AS team_id, s.away_team AS team, s.home_team_id AS opponent_team_id,
           FALSE AS is_home, s.away_score AS goals_for, s.home_score AS goals_against,
           json_extract_scalar(stat, '$.name') AS stat_name,
           json_extract_scalar(stat, '$.displayValue') AS stat_value
    FROM schedule_dedup s
    CROSS JOIN UNNEST(CAST(json_extract(s.extra_json, '$.sides.away.competitor.statistics') AS array<json>)) AS u(stat)
    LEFT JOIN matches_with_matchsheet m ON m.event_id = s.event_id
    WHERE s.played_final AND m.event_id IS NULL
),
scoreboard_rows AS (
    SELECT
        event_id, team_id, MAX(team) AS team, MAX(opponent_team_id) AS opponent_team_id,
        MAX(is_home) AS is_home, MAX(goals_for) AS goals_for, MAX(goals_against) AS goals_against,
        MAX(IF(stat_name = 'possessionPct', TRY_CAST(stat_value AS double))) AS possession_pct,
        MAX(IF(stat_name = 'totalShots', TRY_CAST(stat_value AS integer))) AS total_shots,
        MAX(IF(stat_name = 'shotsOnTarget', TRY_CAST(stat_value AS integer))) AS shots_on_target,
        CAST(NULL AS integer) AS blocked_shots,
        CAST(NULL AS integer) AS penalty_kick_goals,
        CAST(NULL AS integer) AS penalty_kick_shots,
        CAST(NULL AS integer) AS total_passes,
        CAST(NULL AS integer) AS accurate_passes,
        CAST(NULL AS integer) AS total_crosses,
        CAST(NULL AS integer) AS accurate_crosses,
        CAST(NULL AS integer) AS total_long_balls,
        CAST(NULL AS integer) AS accurate_long_balls,
        CAST(NULL AS integer) AS total_tackles,
        CAST(NULL AS integer) AS effective_tackles,
        CAST(NULL AS integer) AS interceptions,
        CAST(NULL AS integer) AS effective_clearance,
        CAST(NULL AS integer) AS total_clearance,
        CAST(NULL AS integer) AS saves,
        MAX(IF(stat_name = 'foulsCommitted', TRY_CAST(stat_value AS integer))) AS fouls_committed,
        CAST(NULL AS integer) AS offsides,
        MAX(IF(stat_name = 'wonCorners', TRY_CAST(stat_value AS integer))) AS corner_kicks,
        CAST(NULL AS integer) AS yellow_cards,
        CAST(NULL AS integer) AS red_cards,
        'scoreboard' AS stats_source,
        MAX(_ingested_at) AS _bronze_ingested_at,
        MAX(platform_league) AS platform_league,
        MAX(platform_season_slug) AS platform_season_slug
    FROM (
        SELECT * FROM scoreboard_home_flat
        UNION ALL
        SELECT * FROM scoreboard_away_flat
    ) x
    GROUP BY event_id, team_id
),
unioned AS (
    SELECT * FROM matchsheet_rows
    UNION ALL
    SELECT * FROM scoreboard_rows
)
SELECT
    -- ===== Identity =====
    event_id, team_id, team, opponent_team_id, is_home, goals_for, goals_against,
    -- ===== HARD_FACT =====
    possession_pct, total_shots, shots_on_target, blocked_shots, penalty_kick_goals,
    penalty_kick_shots, total_passes, accurate_passes, total_crosses, accurate_crosses,
    total_long_balls, accurate_long_balls, total_tackles, effective_tackles, interceptions,
    effective_clearance, total_clearance, saves, fouls_committed, offsides, corner_kicks,
    yellow_cards, red_cards,
    -- ===== MODELED =====
    ROUND(100.0 * accurate_passes / NULLIF(total_passes, 0), 2) AS pass_pct,
    ROUND(100.0 * shots_on_target / NULLIF(total_shots, 0), 2) AS shot_pct,
    ROUND(100.0 * accurate_crosses / NULLIF(total_crosses, 0), 2) AS cross_pct,
    ROUND(100.0 * accurate_long_balls / NULLIF(total_long_balls, 0), 2) AS longball_pct,
    ROUND(100.0 * effective_tackles / NULLIF(total_tackles, 0), 2) AS tackle_pct,
    stats_source,
    -- ===== Lineage =====
    _bronze_ingested_at,
    -- ===== Partition keys =====
    platform_league AS league,
    platform_season_slug AS season
FROM unioned
