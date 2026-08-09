-- =============================================================================
-- Silver: espn_venue (native v2)
-- =============================================================================
-- Grain/PK: one row per ESPN venue_id, latest canonical event observation.
-- Sources (native v2): schedule event rows (event_id/venue_id bigint, venue
-- varchar, competition_slug varchar, source_season_year bigint, extra_json
-- varchar, _ingested_at timestamp(6)).
-- Notes: event snapshots are deduplicated first, then venue observations are
-- reduced to their latest row; venue name is the Bronze column, not JSON.
-- Footguns: venue address JSON has no name; a venue's league/season describes
-- its latest observed match and is retained for the standard Silver partitions.
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

venue_dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY venue_id ORDER BY _ingested_at DESC
            ) AS venue_rn
        FROM schedule_dedup s
        WHERE venue_id IS NOT NULL
    )
    WHERE venue_rn = 1
)

SELECT
    -- ===== Identity / Attributes =====
    venue_id,
    trim(venue) AS venue_name,
    json_extract_scalar(extra_json, '$.venue.address.city') AS city,
    json_extract_scalar(extra_json, '$.venue.address.country') AS country,

    -- ===== Lineage =====
    _ingested_at AS _bronze_ingested_at,

    -- ===== Partition keys =====
    competition_slug AS league,
    CAST(source_season_year AS varchar) AS season
FROM venue_dedup
