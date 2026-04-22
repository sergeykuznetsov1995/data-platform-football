-- =============================================================================
-- Silver: fbref_match_events
-- =============================================================================
--
-- Detailed per-event rows (goals, cards, substitutions) for each match.
--
-- Source:
--   iceberg.bronze.fbref_match_events
--
-- Deduplication:
--   ROW_NUMBER() OVER (PARTITION BY match_id, minute, player_id, event_type
--                       ORDER BY _ingested_at DESC)  =>  rn = 1
--
-- Notes:
--   * event_type values: goal, penalty, own_goal, yellow_card,
--     second_yellow_card, red_card, substitution.
--   * All columns remain VARCHAR — no TRY_CAST needed.
--   * Partitioning by (league, season) is applied externally by Python CTAS.
-- =============================================================================

WITH src AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, minute, player_id, event_type
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_match_events
)

SELECT
    match_id,
    minute,
    event_type,
    player,
    player_id,
    team,
    team_side,
    secondary_player,
    secondary_player_id,

    -- ========= Lineage =========
    _ingested_at                   AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    league,
    season

FROM src
WHERE rn = 1
