-- =============================================================================
-- Silver: espn_substitutions (native v2)
-- =============================================================================
-- Grain/PK: one row per (event_id, team_id, player_in_id), played final only.
-- Sources (native v2): lineup player rows (event_id/team_id/athlete_id bigint,
-- subbed_in boolean, sub_in varchar, jersey varchar, extra_json varchar,
-- _ingested_at timestamp(6)); schedule rows (event_id bigint, played_final
-- boolean, competition_slug varchar, source_season_year bigint, _ingested_at).
-- Notes: inbound players are the authoritative substitution fact, then the
-- canonical schedule supplies played status and clean native-v2 partitions.
-- Footguns: outgoing-only ESPN rows are deliberately excluded: 239 players left
-- without a recorded replacement. Outgoing jersey is a sibling of athlete.
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

bronze_src_lineup AS (
    SELECT *
    FROM iceberg.bronze.espn_lineup_generation_v2
),

lineup_dedup AS (
    SELECT *
    FROM (
        SELECT b.*, ROW_NUMBER() OVER (
            PARTITION BY event_id, team_id, athlete_id ORDER BY _ingested_at DESC
        ) AS rn
        FROM bronze_src_lineup b
    )
    WHERE rn = 1
),

inbound_substitutions AS (
    SELECT
        l.*,
        s.competition_slug AS schedule_competition_slug,
        s.source_season_year AS schedule_source_season_year
    FROM lineup_dedup l
    JOIN schedule_dedup s ON s.event_id = l.event_id
    WHERE s.played_final AND l.subbed_in
)

SELECT
    -- ===== Identity / Attributes =====
    l.event_id,
    l.team_id,
    l.team,
    TRY_CAST(l.sub_in AS integer) AS minute,
    l.athlete_id AS player_in_id,
    l.player AS player_in_name,
    TRY_CAST(json_extract_scalar(l.extra_json, '$.subbedInFor.athlete.id') AS bigint) AS player_out_id,
    json_extract_scalar(l.extra_json, '$.subbedInFor.athlete.displayName') AS player_out_name,
    TRY_CAST(COALESCE(json_extract_scalar(l.extra_json, '$.jersey'), NULLIF(l.jersey, '')) AS integer) AS player_in_jersey,
    TRY_CAST(json_extract_scalar(l.extra_json, '$.subbedInFor.jersey') AS integer) AS player_out_jersey,

    -- ===== Lineage =====
    l._ingested_at AS _bronze_ingested_at,

    -- ===== Partition keys =====
    l.schedule_competition_slug AS league,
    CAST(l.schedule_source_season_year AS varchar) AS season
FROM inbound_substitutions l
