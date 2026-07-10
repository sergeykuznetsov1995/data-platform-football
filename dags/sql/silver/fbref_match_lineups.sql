-- =============================================================================
-- Silver: fbref_match_lineups
-- =============================================================================
--
-- Detailed per-player lineup entries for each match.
--
-- Source:
--   iceberg.bronze.fbref_lineups
--
-- Deduplication (#463):
--   ROW_NUMBER() OVER (PARTITION BY match_id, COALESCE(player_id, player), team
--                       ORDER BY _ingested_at DESC, _batch_id DESC)  =>  rn = 1
--   COALESCE guards legacy rows without player_id (otherwise every NULL row
--   of a match collapses into one partition); team guards same-named players
--   on opposite sides (mirrors fbref_player_match_stats.sql).
--
-- Notes:
--   * is_starter is already BOOLEAN in Bronze.
--   * jersey_number is TRY_CAST to INTEGER (source is VARCHAR).
--   * Partitioning by (league, season) is applied externally by Python CTAS.
-- =============================================================================

WITH src AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, COALESCE(player_id, player), team
               ORDER BY _ingested_at DESC, _batch_id DESC
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
    -- season → slug ('2425'); FBref bronze stores year-start bigint (2024).
    league,
    -- #913 Phase 2
    CASE WHEN league = 'INT-World Cup'
         THEN LPAD(CAST(season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0')
    END AS season

FROM src
WHERE rn = 1
