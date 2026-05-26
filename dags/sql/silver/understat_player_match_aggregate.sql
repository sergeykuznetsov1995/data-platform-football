-- =============================================================================
-- Silver: understat_player_match_aggregate
-- =============================================================================
--
-- One row per (match_id, player_id, league, season) — passthrough from
-- `bronze.understat_player_match_stats` with cross-source-aligned column
-- aliases and `game_id` renamed to `match_id` for Gold-layer alignment.
--
-- Bronze is already match-grain (PK = (player_id, game_id, season)) — no
-- aggregation needed, only dedup + rename.
--
-- Sources:
--   bronze.understat_player_match_stats
--
-- Notes:
--   * Understat exposes only forward + midfielder metrics (no defence/duels)
--     → no rating column.
--   * Bronze dedup: defensive ROW_NUMBER on (game_id, player_id, league,
--     season). Understat ingest historically had append-mode duplicates
--     (see CLAUDE.md / feedback_replace_partitions_required.md), keep dedup
--     even after replace_partitions=True fix.
--   * Numeric columns from soccerdata are object-typed strings — TRY_CAST
--     into the matching numeric type defensively.
--   * `time` → `minutes_played` rename to match FBref / SofaScore / WhoScored.
--   * `yellow_card`/`red_card` (singular) → `yellow_cards`/`red_cards` plural
--     for cross-source alignment.
--   * Season convention: passthrough varchar slug (matches xref_player).
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY game_id, player_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.understat_player_match_stats b
        WHERE game_id   IS NOT NULL
          AND player_id IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    -- ========= Identification =========
    CAST(game_id AS varchar)               AS match_id,
    CAST(player_id AS varchar)             AS player_id,
    player,
    CAST(team_id AS varchar)               AS team_id,
    h_a                                    AS team_side,
    position,

    -- ========= HARD_FACT (FBref-aligned names) =========
    TRY_CAST(time AS INTEGER)              AS minutes_played,
    TRY_CAST(goals AS INTEGER)             AS goals,
    TRY_CAST(own_goals AS INTEGER)         AS own_goals,
    TRY_CAST(shots AS INTEGER)             AS shots,
    TRY_CAST(yellow_card AS INTEGER)       AS yellow_cards,
    TRY_CAST(red_card AS INTEGER)          AS red_cards,
    TRY_CAST(assists AS INTEGER)           AS assists,
    TRY_CAST(key_passes AS INTEGER)        AS key_passes,

    -- ========= MODELED (xG / xA / build-up) =========
    TRY_CAST(xg AS DOUBLE)                 AS xg,
    TRY_CAST(xa AS DOUBLE)                 AS xa,
    TRY_CAST(npg AS INTEGER)               AS non_penalty_goals,
    TRY_CAST(npxg AS DOUBLE)               AS non_penalty_xg,
    TRY_CAST(xgchain AS DOUBLE)            AS xg_chain,
    TRY_CAST(xgbuildup AS DOUBLE)          AS xg_buildup,

    -- ========= Lineage =========
    _ingested_at                           AS _bronze_ingested_at,

    -- ========= Partition keys =========
    league,
    season

FROM bronze_dedup
