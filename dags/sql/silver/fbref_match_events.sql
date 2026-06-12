-- =============================================================================
-- Silver: fbref_match_events
-- =============================================================================
--
-- Detailed per-event rows (goals, cards, substitutions) for each match.
--
-- Source:
--   iceberg.bronze.fbref_match_events
--
-- Deduplication (#463):
--   ROW_NUMBER() OVER (PARTITION BY match_id, minute,
--                                   COALESCE(player_id, player), event_type
--                       ORDER BY _ingested_at DESC, _batch_id DESC)  =>  rn = 1
--   COALESCE guards rows without player_id (two carded NULL-id players in the
--   same minute must NOT collapse). Key-based dedup is kept deliberately:
--   live bronze (2026-06-12) contains 4 bit-identical duplicate yellow_card
--   rows (parse artifacts) that this key correctly collapses.
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
               PARTITION BY match_id, minute, COALESCE(player_id, player), event_type
               ORDER BY _ingested_at DESC, _batch_id DESC
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
    -- season → slug ('2425'); FBref bronze stores year-start bigint (2024).
    league,
    LPAD(CAST(MOD(season,     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0') AS season

FROM src
WHERE rn = 1
