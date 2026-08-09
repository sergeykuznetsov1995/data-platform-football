-- =============================================================================
-- Silver: espn_venue (native v2)
-- =============================================================================
-- Grain/PK: one row per ESPN venue_id, latest canonical event observation.
-- Sources (native v2): compact6 canonical schedule rows (event_id/venue_id bigint, venue
-- varchar, competition_slug varchar, source_season_year bigint, extra_json
-- varchar, _ingested_at timestamp(6)).
-- Notes: event snapshots are deduplicated first, then venue observations are
-- reduced to their latest row; venue name is the Bronze column, not JSON.
-- Footguns: venue address JSON has no name; a venue's league/season describes
-- its latest observed match and is retained for the standard Silver partitions.
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
    platform_league AS league,
    platform_season_slug AS season
FROM venue_dedup
