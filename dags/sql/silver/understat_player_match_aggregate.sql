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
--   * Numeric casts remain defensive for pre-native Bronze partitions.
--   * `minutes` → `minutes_played` rename to match FBref / SofaScore / WhoScored.
--   * Season convention: passthrough varchar slug (matches xref_player).
--   * `non_penalty_xg` / `non_penalty_goals` (issue #103) are derived via
--     `bronze.understat_shots`. Native rows preserve `situation='Penalty'`;
--     the high-xG NULL branch only keeps pre-native partitions compatible.
-- =============================================================================

WITH understat_manifest_latest AS (
    SELECT league, season, batch_id, status
    FROM (
        SELECT
            league,
            season,
            batch_id,
            status,
            ROW_NUMBER() OVER (
                PARTITION BY league, season
                ORDER BY completed_at DESC, attempt_id DESC
            ) AS rn
        FROM iceberg.ops.understat_ingest_manifest_v1
        WHERE contract_version = 'understat-bronze-v2'
    )
    WHERE rn = 1
),

bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.game_id, b.player_id, b.league, b.season
                ORDER BY b._ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.understat_player_match_stats b
        LEFT JOIN understat_manifest_latest m
          ON m.league = b.league
         AND m.season = CAST(b.season AS varchar)
        WHERE b.game_id   IS NOT NULL
          AND b.player_id IS NOT NULL
          AND b.league IN (
              'ENG-Premier League', 'ESP-La Liga', 'GER-Bundesliga',
              'ITA-Serie A', 'FRA-Ligue 1'
          )
          AND (
              m.league IS NULL
              OR (m.status = 'complete' AND b._batch_id = m.batch_id)
          )
    )
    WHERE rn = 1
),

-- Dedup bronze.understat_shots by shot_id BEFORE the penalty SUM/COUNT —
-- otherwise a re-ingest / replace→append regression double-counts a penalty
-- shot and poisons non_penalty_xg / non_penalty_goals (#464). Mirrors
-- gold/fct_shot.sql shots_dedup; _batch_id breaks _ingested_at ties.
shots_dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.shot_id
                ORDER BY _ingested_at DESC, _batch_id DESC
            ) AS rn
        FROM iceberg.bronze.understat_shots s
        LEFT JOIN understat_manifest_latest m
          ON m.league = s.league
         AND m.season = CAST(s.season AS varchar)
        WHERE s.shot_id IS NOT NULL
          AND s.league IN (
              'ENG-Premier League', 'ESP-La Liga', 'GER-Bundesliga',
              'ITA-Serie A', 'FRA-Ligue 1'
          )
          AND (
              m.league IS NULL
              OR (m.status = 'complete' AND s._batch_id = m.batch_id)
          )
    )
    WHERE rn = 1
),

shot_penalty_aggr AS (
    SELECT
        game_id,
        player_id,
        SUM(xg)        AS penalty_xg,
        COUNT(*) FILTER (WHERE result = 'Goal') AS penalty_goals
    FROM shots_dedup
    WHERE game_id   IS NOT NULL
      AND player_id IS NOT NULL
      AND (
          situation = 'Penalty'       -- native parser
          OR (situation IS NULL AND xg > 0.7) -- pre-native migration fallback
      )
    GROUP BY 1, 2
)

SELECT
    -- ========= Identification =========
    CAST(b.game_id AS varchar)             AS match_id,
    CAST(b.player_id AS varchar)           AS player_id,
    b.player,
    CAST(b.team_id AS varchar)             AS team_id,
    CAST(b.team_side AS varchar)           AS team_side,
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
