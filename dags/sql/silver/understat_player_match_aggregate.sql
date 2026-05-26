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
    CAST(NULL AS varchar)                  AS team_side,
    position,

    -- ========= HARD_FACT (FBref-aligned names) =========
    CAST(minutes AS INTEGER)               AS minutes_played,
    CAST(goals AS INTEGER)                 AS goals,
    CAST(own_goals AS INTEGER)             AS own_goals,
    CAST(shots AS INTEGER)                 AS shots,
    CAST(yellow_cards AS INTEGER)          AS yellow_cards,
    CAST(red_cards AS INTEGER)             AS red_cards,
    CAST(assists AS INTEGER)               AS assists,
    CAST(key_passes AS INTEGER)            AS key_passes,

    -- ========= MODELED (xG / xA / build-up) =========
    xg                                     AS xg,
    xa                                     AS xa,
    CAST(NULL AS INTEGER)                  AS non_penalty_goals,
    CAST(NULL AS DOUBLE)                   AS non_penalty_xg,
    xg_chain,
    xg_buildup,

    -- ========= Lineage =========
    _ingested_at                           AS _bronze_ingested_at,

    -- ========= Partition keys =========
    league,
    season

FROM bronze_dedup
