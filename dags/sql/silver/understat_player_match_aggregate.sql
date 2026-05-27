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
--   bronze.understat_player_match_stats — per-(match, player) aggregate
--   bronze.understat_shots              — shot-grain, used to derive penalty info
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
--   * `non_penalty_xg` / `non_penalty_goals` (issue #103): soccerdata's
--     `read_player_match_stats` does NOT extract `npxG` (only `read_player_
--     season_stats` does), so Bronze has no native field. Compute via
--     `bronze.understat_shots`: soccerdata `SHOT_SITUATIONS` dict omits the
--     'Penalty' key → penalty shots arrive with `situation IS NULL`, all
--     pinned at xg=0.7612. We filter `situation IS NULL AND xg > 0.7` to be
--     resilient against future NULL-situation drift (other categories).
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
),

shot_penalty_aggr AS (
    SELECT
        game_id,
        player_id,
        SUM(xg)        AS penalty_xg,
        COUNT(*) FILTER (WHERE result = 'Goal') AS penalty_goals
    FROM iceberg.bronze.understat_shots
    WHERE game_id   IS NOT NULL
      AND player_id IS NOT NULL
      AND situation IS NULL           -- soccerdata maps 'Penalty' → NULL
      AND xg > 0.7                    -- penalty xG fixed at 0.7612 in Understat
    GROUP BY 1, 2
)

SELECT
    -- ========= Identification =========
    CAST(b.game_id AS varchar)             AS match_id,
    CAST(b.player_id AS varchar)           AS player_id,
    b.player,
    CAST(b.team_id AS varchar)             AS team_id,
    CAST(NULL AS varchar)                  AS team_side,
    b.position,

    -- ========= HARD_FACT (FBref-aligned names) =========
    CAST(b.minutes AS INTEGER)             AS minutes_played,
    CAST(b.goals AS INTEGER)               AS goals,
    CAST(b.own_goals AS INTEGER)           AS own_goals,
    CAST(b.shots AS INTEGER)               AS shots,
    CAST(b.yellow_cards AS INTEGER)        AS yellow_cards,
    CAST(b.red_cards AS INTEGER)           AS red_cards,
    CAST(b.assists AS INTEGER)             AS assists,
    CAST(b.key_passes AS INTEGER)          AS key_passes,

    -- ========= MODELED (xG / xA / build-up) =========
    b.xg                                                                   AS xg,
    b.xa                                                                   AS xa,
    CAST(b.goals AS INTEGER) - COALESCE(spa.penalty_goals, 0)              AS non_penalty_goals,
    GREATEST(0.0, b.xg - COALESCE(spa.penalty_xg, 0.0))                    AS non_penalty_xg,
    b.xg_chain,
    b.xg_buildup,

    -- ========= Lineage =========
    b._ingested_at                         AS _bronze_ingested_at,

    -- ========= Partition keys =========
    b.league,
    b.season

FROM bronze_dedup b
LEFT JOIN shot_penalty_aggr spa
       ON spa.game_id   = b.game_id
      AND spa.player_id = b.player_id
