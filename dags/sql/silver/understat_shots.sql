-- =============================================================================
-- Silver: understat_shots
-- =============================================================================
-- Shot-grained projection of the Understat shotmap, conformed + canonicalised to
-- gold.fct_shot's match/team/player IDs and enum domains. One row per shot.
--
-- Issue #704 — Gold one-hop: lift the Understat shot conform out of
-- gold.fct_shot (which read bronze.understat_shots + bronze.understat_players
-- directly) into Silver. Mirrors silver.sofascore_shots (#602), which already
-- feeds the fct_shot SofaScore fallback branch. After this, the fct_shot
-- Understat branch reads THIS table; the only bronze read it keeps is the
-- understat_schedule → fbref match bridge (a sanctioned *_schedule bridge).
--
-- Single source = Understat (shots + players). The shot→fbref match_id bridge is
-- a CROSS-SOURCE join (understat_schedule × silver.fbref_match_enriched) and so
-- by charter §5 stays in Gold (xref_match has no source='understat' rows yet —
-- Phase B). This table therefore carries `understat_game_id` raw for Gold to
-- bridge, NOT a resolved match_id.
--
-- Source:
--   iceberg.bronze.understat_shots    — shot-level events with xG (primary feed)
--   iceberg.bronze.understat_players  — name→player_id dictionary for assists
--   iceberg.silver.xref_team          — understat team NAME → canonical id
--   iceberg.silver.xref_player        — understat player_id → canonical id
--
-- PK: shot_id (Understat shot_id is globally unique). Gold adds match_id.
-- Partitioning: (league, season) — applied externally by run_silver_transform.
--
-- =============================================================================
-- Bronze schema (bronze.understat_shots — verified via DESCRIBE 2026-05-08)
-- =============================================================================
--   shot_id            BIGINT      -- Understat-assigned, globally unique
--   game_id            BIGINT      -- Understat match id
--   team_id            BIGINT      -- Understat team id (UNUSED; team via name)
--   player_id          BIGINT      -- Understat player id
--   assist_player      VARCHAR     -- assister NAME (no per-shot numeric id, #444)
--   minute             BIGINT
--   location_x         DOUBLE      -- 0..1 normalized field coords
--   location_y         DOUBLE
--   xg                 DOUBLE      -- 0..1
--   body_part          VARCHAR     -- {Right Foot, Left Foot, Head?, Other Body Part?, NULL}
--   situation          VARCHAR     -- {Open Play, From Corner, Set Piece, Direct Freekick, Penalty?}
--   result             VARCHAR     -- {Goal, Saved Shot, Blocked Shot, Missed Shot, Shot On Post, Own Goal}
--   team, player       VARCHAR     -- raw display names (team = xref source_id)
--   league, season     VARCHAR
--   _ingested_at       TIMESTAMP(6)
--
-- =============================================================================
-- Enum normalisation → fct_shot domain (identical to the old Gold CTEs)
-- =============================================================================
--   body_part: Right Foot|Left Foot → foot; Head → head; else → other; NULL → NULL
--   situation: Open Play → open_play; From Corner → corner; Direct Freekick →
--     free_kick; Set Piece → set_piece; Penalty → penalty; else → NULL
--   result:    Goal → goal; Saved Shot → saved; Blocked Shot → blocked;
--     Missed Shot → off_target; Shot On Post → post; Own Goal → own_goal
--   is_goal = result IN ('goal','own_goal')  (own goal ended in net)
--
-- Footguns:
--   * ALL xref JOINs carry the (league, season) predicate — else 1.5-4× fan-out
--     (feedback_xref_join_season_predicate). Full (source, source_id, league,
--     season) key returns exactly one row → no per-season fan-out, no dedup CTE.
--   * Understat ids are BIGINT (NOT DOUBLE like whoscored) → single CAST to
--     varchar; no double-cast scientific-notation issue here.
--   * Team xref filters confidence <> 'orphan' (xref_team.sql.j2 contract: orphan
--     'us_<slug>' canonicals must not leak as a resolved team_id, #506). Player
--     xref is orphan-tolerant (xref_player emits us_* canonicals for orphans,
--     rejection ≤6.94% per E1 — kept).
--   * assist_player has NO numeric id in bronze; resolved by NAME via the
--     understat_players dictionary, then xref_player (#444).
-- =============================================================================

WITH

-- 1) Dedup bronze.understat_shots (idempotent re-scrape protection) -----------
shots_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY shot_id
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.understat_shots
    WHERE shot_id IS NOT NULL
),

-- 2) Normalize enums + cast scalars ------------------------------------------
shots_norm AS (
    SELECT
        CAST(s.shot_id  AS varchar)    AS shot_id,
        s.game_id                      AS understat_game_id,
        -- xref_team.source_id for source='understat' is the team NAME
        -- (e.g. 'Tottenham'), NOT the numeric team_id.
        s.team                         AS understat_team_source_id,
        -- xref_player.source_id for source='understat' is player_id as varchar.
        CAST(s.player_id AS varchar)   AS understat_player_source_id,
        -- #444: resolve the assister by NAME (bronze has no per-shot assist id).
        LOWER(s.assist_player)         AS assist_player_norm,
        CAST(s.minute    AS integer)   AS minute,
        s.location_x                   AS x,
        s.location_y                   AS y,
        s.xg                           AS xg,

        CASE
            WHEN s.body_part IN ('Right Foot', 'Left Foot')          THEN 'foot'
            WHEN s.body_part = 'Head'                                 THEN 'head'
            WHEN s.body_part IN ('Other Body Part', 'OtherBodyPart')  THEN 'other'
            WHEN s.body_part IS NULL                                  THEN NULL
            ELSE 'other'
        END                            AS body_part,

        CASE
            WHEN s.situation = 'Open Play'        THEN 'open_play'
            WHEN s.situation = 'From Corner'      THEN 'corner'
            WHEN s.situation = 'Direct Freekick'  THEN 'free_kick'
            WHEN s.situation = 'Set Piece'        THEN 'set_piece'
            WHEN s.situation = 'Penalty'          THEN 'penalty'
            ELSE NULL
        END                            AS situation,

        CASE
            WHEN s.result = 'Goal'         THEN 'goal'
            WHEN s.result = 'Saved Shot'   THEN 'saved'
            WHEN s.result = 'Blocked Shot' THEN 'blocked'
            WHEN s.result = 'Missed Shot'  THEN 'off_target'
            WHEN s.result = 'Shot On Post' THEN 'post'
            WHEN s.result = 'Own Goal'     THEN 'own_goal'
            ELSE NULL
        END                            AS result,

        s._ingested_at                 AS _bronze_ingested_at,
        s.league                       AS league,
        CAST(s.season AS varchar)      AS season
    FROM shots_dedup s
    WHERE s.rn = 1
),

-- 3) Understat name→player_id dictionary (#444) ------------------------------
--    Understat has NO per-shot numeric assist id; the assister NAME is correct,
--    so recover the player_id from bronze.understat_players (same source that
--    feeds xref_player) keyed on (LOWER(name), league, season). Deduped with
--    ROW_NUMBER (not GROUP BY) to keep the output shot-grain and avoid an R1
--    ROLLUP false-positive in the charter checker. Identical names within one
--    league-season (rare) tie-break to the most-recently-ingested row.
us_player_dim AS (
    SELECT player_norm, league, season, understat_player_id
    FROM (
        SELECT
            LOWER(player)                AS player_norm,
            league,
            CAST(season AS varchar)      AS season,
            CAST(player_id AS varchar)   AS understat_player_id,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(player), league, CAST(season AS varchar)
                ORDER BY _ingested_at DESC, player_id
            )                            AS rn
        FROM iceberg.bronze.understat_players
        WHERE player IS NOT NULL AND player_id IS NOT NULL
    )
    WHERE rn = 1
)

-- 4) Final projection — conform + canonical resolve --------------------------
SELECT
    sn.shot_id,
    sn.understat_game_id,

    xt.canonical_id                                AS team_id,
    xp.canonical_id                                AS player_id,
    xa.canonical_id                                AS assist_player_id,

    sn.minute,
    sn.x,
    sn.y,

    sn.body_part,
    sn.situation,

    sn.xg,
    sn.result,
    (sn.result IN ('goal', 'own_goal'))            AS is_goal,

    CAST('understat_v1' AS varchar)                AS shot_source,

    sn._bronze_ingested_at,
    sn.league,
    sn.season

FROM shots_norm sn

-- Team: name-based, orphan-EXCLUDED (orphan 'us_<slug>' must not leak, #506).
LEFT JOIN iceberg.silver.xref_team xt
       ON xt.source     = 'understat'
      AND xt.source_id  = sn.understat_team_source_id
      AND xt.league     = sn.league
      AND xt.season     = sn.season
      AND xt.confidence <> 'orphan'

-- Player: orphan-tolerant (xref_player emits us_* canonical for orphans).
LEFT JOIN iceberg.silver.xref_player xp
       ON xp.source    = 'understat'
      AND xp.source_id = sn.understat_player_source_id
      AND xp.league    = sn.league
      AND xp.season    = sn.season

-- Assister: name → understat player_id (dict), then xref_player → canonical.
LEFT JOIN us_player_dim upd
       ON upd.player_norm = sn.assist_player_norm
      AND upd.league      = sn.league
      AND upd.season      = sn.season

LEFT JOIN iceberg.silver.xref_player xa
       ON xa.source    = 'understat'
      AND xa.source_id = upd.understat_player_id
      AND xa.league    = sn.league
      AND xa.season    = sn.season
