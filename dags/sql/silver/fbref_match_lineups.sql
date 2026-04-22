-- =============================================================================
-- Silver: fbref_match_lineups
-- =============================================================================
--
-- Detailed per-player lineup entries for each match.
--
-- Source:
--   iceberg.bronze.fbref_lineups
--
-- Deduplication:
--   ROW_NUMBER() OVER (PARTITION BY match_id, player_id
--                       ORDER BY _ingested_at DESC)  =>  rn = 1
--
-- Notes:
--   * is_starter is already BOOLEAN in Bronze.
--   * jersey_number is TRY_CAST to INTEGER (source is VARCHAR).
--   * Partitioning by (league, season) is applied externally by Python CTAS.
-- =============================================================================

WITH src AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, player_id
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_lineups
)

SELECT
    match_id,
    team,
    player,
    player_id,
    is_starter,
    position,
    TRY_CAST(number AS INTEGER)    AS jersey_number,

    -- ========= Lineage =========
    _ingested_at                   AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    league,
    season

FROM src
WHERE rn = 1
