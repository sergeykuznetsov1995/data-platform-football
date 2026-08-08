-- =============================================================================
-- Silver: espn_match (native v2)
-- =============================================================================
-- Grain/PK: one row per ESPN event_id. Native v2 schedule is append-only and is
-- deduplicated before any JSON parsing; matchsheet is joined only through the
-- per-event referee aggregate, so it cannot fan out a match.
-- Sources (native v2): schedule event rows (event_id bigint, status varchar,
-- played_final boolean, score integers, extra_json varchar); matchsheet team rows
-- (event_id/team_id bigint, referee varchar, extra_json varchar).
-- Notes: schedule supplies the match grain and all partition keys; matchsheet is
-- reduced to one referee value per event before the join.
-- Footguns: non-played ESPN fixtures carry literal 0:0; attendance=0 means
-- unknown; shootout score is separate from regulation score; season_slug is
-- sometimes a cup stage. season_slug_platform parses displayName, not dates.
-- DAG integration: run_silver_transform wraps this pure SELECT in CTAS.
-- =============================================================================

WITH bronze_src_schedule AS (
    SELECT *
    FROM iceberg.bronze.espn_schedule_generation_v2
),

schedule_dedup AS (
    SELECT *
    FROM (
        SELECT b.*, ROW_NUMBER() OVER (
            PARTITION BY event_id ORDER BY _ingested_at DESC
        ) AS rn
        FROM bronze_src_schedule b
    )
    WHERE rn = 1
),

bronze_src_matchsheet AS (
    SELECT *
    FROM iceberg.bronze.espn_matchsheet_generation_v2
),

matchsheet_dedup AS (
    SELECT *
    FROM (
        SELECT b.*, ROW_NUMBER() OVER (
            PARTITION BY event_id, team_id ORDER BY _ingested_at DESC
        ) AS rn
        FROM bronze_src_matchsheet b
    )
    WHERE rn = 1
),

referee_by_event AS (
    SELECT
        event_id,
        COALESCE(
            MAX(NULLIF(trim(referee), '')),
            MAX(NULLIF(trim(json_extract_scalar(extra_json, '$.summaryGameInfo.officials[0].fullName')), ''))
        ) AS referee
    FROM matchsheet_dedup
    GROUP BY event_id
),

schedule_parsed AS (
    SELECT
        s.*,
        json_extract_scalar(extra_json, '$.source.league.name') AS competition_name,
        json_extract_scalar(extra_json, '$.season.slug') AS season_slug,
        TRY_CAST(json_extract_scalar(extra_json, '$.season.type') AS integer) AS season_type,
        json_extract_scalar(extra_json, '$.competition.altGameNote') AS alt_game_note,
        TRY_CAST(json_extract_scalar(extra_json, '$.competition.leg.value') AS integer) AS leg,
        TRY_CAST(TRY_CAST(json_extract_scalar(extra_json, '$.sides.home.competitor.aggregateScore') AS double) AS integer) AS home_aggregate_score,
        TRY_CAST(TRY_CAST(json_extract_scalar(extra_json, '$.sides.away.competitor.aggregateScore') AS double) AS integer) AS away_aggregate_score,
        TRY_CAST(json_extract_scalar(extra_json, '$.competition.series.completed') AS boolean) AS is_series_completed,
        TRY_CAST(json_extract_scalar(extra_json, '$.sides.home.competitor.advance') AS boolean) AS home_advanced,
        TRY_CAST(json_extract_scalar(extra_json, '$.sides.away.competitor.advance') AS boolean) AS away_advanced,
        TRY_CAST(json_extract_scalar(extra_json, '$.sides.home.competitor.shootoutScore') AS integer) AS home_shootout_score,
        TRY_CAST(json_extract_scalar(extra_json, '$.sides.away.competitor.shootoutScore') AS integer) AS away_shootout_score,
        TRY_CAST(json_extract_scalar(extra_json, '$.sides.home.competitor.winner') AS boolean) AS home_winner,
        TRY_CAST(json_extract_scalar(extra_json, '$.sides.away.competitor.winner') AS boolean) AS away_winner,
        json_extract_scalar(extra_json, '$.venue.address.city') AS venue_city,
        json_extract_scalar(extra_json, '$.venue.address.country') AS venue_country,
        TRY_CAST(json_extract_scalar(extra_json, '$.sides.home.team.venue.id') AS bigint) AS home_team_venue_id,
        TRY_CAST(json_extract_scalar(extra_json, '$.sides.away.team.venue.id') AS bigint) AS away_team_venue_id,
        TRY_CAST(substr(json_extract_scalar(extra_json, '$.source.league.season.startDate'), 1, 10) AS date) AS season_start_date,
        TRY_CAST(substr(json_extract_scalar(extra_json, '$.source.league.season.endDate'), 1, 10) AS date) AS season_end_date,
        json_extract_scalar(extra_json, '$.source.league.season.displayName') AS source_season_display_name
    FROM schedule_dedup s
),

match_modeled AS (
    SELECT
        p.*,
        CASE
            WHEN home_winner THEN home_team_id
            WHEN away_winner THEN away_team_id
            WHEN played_final AND home_score > away_score THEN home_team_id
            WHEN played_final AND away_score > home_score THEN away_team_id
            WHEN played_final AND home_shootout_score > away_shootout_score THEN home_team_id
            WHEN played_final AND away_shootout_score > home_shootout_score THEN away_team_id
        END AS winner_team_id,
        CASE
            WHEN lower(COALESCE(season_slug, '')) LIKE '%round%'
              OR lower(COALESCE(season_slug, '')) LIKE '%final%'
              OR lower(COALESCE(season_slug, '')) = 'group-stage'
              OR lower(COALESCE(season_slug, '')) = 'league-phase'
              OR lower(COALESCE(season_slug, '')) LIKE 'knockout%'
              OR lower(COALESCE(season_slug, '')) LIKE 'playoff%'
            THEN season_slug
        END AS stage,
        CASE
            WHEN regexp_extract(source_season_display_name, '(\d{4})\s*[-/]\s*(\d{2}|\d{4})', 1) <> ''
            THEN substr(regexp_extract(source_season_display_name, '(\d{4})\s*[-/]\s*(\d{2}|\d{4})', 1), 3, 2)
                 || right(regexp_extract(source_season_display_name, '(\d{4})\s*[-/]\s*(\d{2}|\d{4})', 2), 2)
            WHEN regexp_extract(source_season_display_name, '(\d{4})', 1) <> ''
            THEN regexp_extract(source_season_display_name, '(\d{4})', 1)
            ELSE CAST(source_season_year AS varchar)
        END AS season_slug_platform
    FROM schedule_parsed p
)

SELECT
    -- ===== Identity =====
    event_id,
    scope_id,
    competition_id,

    -- ===== Attributes =====
    competition_name,
    source_season_year,
    kickoff,
    status,
    played_final AS is_played,
    terminal AS is_terminal,
    terminal_nonplayed AS is_cancelled,
    home_team_id,
    away_team_id,
    home_team,
    away_team,
    CASE WHEN played_final THEN home_score END AS home_score,
    CASE WHEN played_final THEN away_score END AS away_score,
    home_shootout_score,
    away_shootout_score,
    winner_team_id,
    season_slug,
    season_type,
    stage,
    regexp_extract(alt_game_note, '(Group\s+[A-Z0-9]+)\s*$', 1) AS group_name,
    alt_game_note,
    leg,
    home_aggregate_score,
    away_aggregate_score,
    is_series_completed,
    home_advanced,
    away_advanced,
    venue_id,
    venue,
    venue_city,
    venue_country,
    home_team_venue_id,
    away_team_venue_id,
    NULLIF(attendance_value, 0) AS attendance,
    r.referee,
    season_start_date,
    season_end_date,

    -- ===== MODELED =====
    season_slug_platform,

    -- ===== Lineage =====
    m._ingested_at AS _bronze_ingested_at,

    -- ===== Partition keys =====
    competition_slug AS league,
    CAST(source_season_year AS varchar) AS season
FROM match_modeled m
LEFT JOIN referee_by_event r ON r.event_id = m.event_id
