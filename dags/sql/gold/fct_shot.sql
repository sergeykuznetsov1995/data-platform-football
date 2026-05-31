-- =============================================================================
-- Gold: fct_shot
-- =============================================================================
-- Shot-grained xG/xA fact table built on top of Understat shot events.
-- One row per shot, resolved to canonical match/team/player IDs via E1 xref.
--
-- Source:
--   iceberg.bronze.understat_shots          — shot-level events with xG
--   iceberg.bronze.understat_schedule       — game_id → (date, home, away) lookup
--   iceberg.silver.fbref_match_enriched     — fbref match_id + date/home/away
--   iceberg.silver.xref_match               — fbref-spine canonical match_id
--   iceberg.silver.xref_team                — team alias resolution (8 sources)
--   iceberg.silver.xref_player              — player resolution (orphan-tolerant)
--
-- PK: (match_id_canonical, shot_id)
-- Partitioning: (league, season) — applied externally by Python CTAS.
--
-- =============================================================================
-- Bronze schema (verified via DESCRIBE on 2026-05-08)
-- =============================================================================
--   shot_id            BIGINT      -- Understat-assigned, globally unique
--   game_id            BIGINT      -- Understat match id
--   team_id            BIGINT      -- Understat team id
--   player_id          BIGINT      -- Understat player id
--   minute             BIGINT
--   location_x         DOUBLE      -- 0..1 normalized field coords
--   location_y         DOUBLE
--   xg                 DOUBLE      -- 0..1
--   body_part          VARCHAR     -- {Right Foot, Left Foot, Head?, Other Body Part?, NULL}
--   situation          VARCHAR     -- {Open Play, From Corner, Set Piece, Direct Freekick, Penalty?}
--   result             VARCHAR     -- {Goal, Saved Shot, Blocked Shot, Missed Shot, Shot On Post, Own Goal}
--   team, player       VARCHAR     -- raw display names
--   league, season     VARCHAR
--   date               TIMESTAMP(6)
--
-- Notes on Understat raw values (sample 143k rows on 2026-05-08):
--   * `xa` is NOT present in bronze.understat_shots; Understat exposes the
--     assister via `assist_player_id`/`assist_player`. We therefore expose the
--     assister-pointer columns instead of `xa`. xA aggregation lives in
--     fct_player_match (cross-source via in-match name-match).
--   * `body_part` ∈ {Right Foot, Left Foot} only in current sample (no Head /
--     Other Body Part observed yet). Mapping branches included for forward
--     compatibility — see CASE in body_part_canonical below.
--   * `situation = 'Penalty'` not observed in current sample but documented in
--     soccerdata; we map it explicitly. 'Set Piece' is a generic non-corner
--     non-FK dead-ball — kept as `set_piece`.
--
-- =============================================================================
-- ADR — match_id bridging (Option A: date + canonical home/away)
-- =============================================================================
-- E1 MVP `silver.xref_match` only emits `source='fbref'` rows; Understat
-- bridging is a Phase B follow-up (see header of `silver/xref_match.sql`).
-- Until that lands we resolve `understat.game_id → fbref.match_id` directly
-- in this CTAS via `understat_schedule` JOIN `fbref_match_enriched` on
-- `(date, home_canonical_id, away_canonical_id)`. Both sides resolve teams
-- through `silver.xref_team`, which guarantees alias-consistent matching
-- (Wolves↔Wolverhampton Wanderers, Spurs↔Tottenham Hotspur, etc.).
--
-- Why date instead of timestamp:
--   * Understat `understat_schedule.date` is timestamp(6) at kickoff.
--   * FBref `fbref_match_enriched.date` is DATE (calendar day).
--   * Late-night EU games shifting dates between TZs are rare in APL and
--     would surface in DQ as orphan match rows — acceptable for E3.4 MVP.
--
-- Strict INNER JOIN: shots without a resolved fbref match_id are DROPPED.
-- Rationale: a shot with NULL canonical match makes the row useless for
-- downstream joins (fct_match / dim_match keyed on canonical_id). DQ in
-- E3.8 will track the rejection rate and gate ERROR if >5%.
--
-- Cutover plan: once `xref_match` Phase B materialises understat→fbref
-- bridging, replace the `match_bridge` CTE below with a direct LEFT JOIN
-- onto `xref_match` filtered by source='understat'.
--
-- =============================================================================
-- Output schema (frozen)
-- =============================================================================
--   shot_id                  varchar
--   match_id_canonical       varchar    -- fbref match_id (8-char hex)
--   team_id_canonical        varchar    -- xref_team(source='understat')
--   player_id_canonical      varchar    -- xref_player(source='understat'); orphan us_* possible
--   assist_player_id_canon   varchar    -- xref_player(source='understat'); NULL when no assist
--   minute                   integer
--   x, y                     double     -- 0..1 normalized
--   xg                       double     -- 0..1
--   body_part_canonical      varchar    -- foot | head | other | NULL
--   situation_canonical      varchar    -- open_play | from_corner | from_free_kick | set_piece | penalty
--   result_canonical         varchar    -- goal | saved | blocked | missed | post | own_goal
--   is_goal                  boolean    -- result_canonical IN ('goal','own_goal')
--   shot_source              varchar    -- literal 'understat_v1'
--   shot_version             varchar    -- literal 'v1'
--   league                   varchar
--   season                   varchar    -- normalised to varchar (bronze stores varchar)
--
-- Testable invariants (E3.8):
--   * PK = (match_id_canonical, shot_id) is unique.
--   * shot_source = 'understat_v1' for every row.
--   * xg ∈ [0, 1] (value_range check).
--   * is_goal = TRUE  iff  result_canonical IN ('goal', 'own_goal').
--   * match_id_canonical is non-null (INNER JOIN guarantees it).
--   * team_id_canonical / player_id_canonical may be NULL — orphan-tolerant.
-- =============================================================================

WITH

-- 0) De-dup xref_team across seasons -----------------------------------------
--    xref_team PK = (source, source_id, league, season). Joining on
--    source+source_id alone produces N-row fan-out per season (5-10× blow-up
--    observed on 2026-05-08). For team-resolution the canonical_id is
--    season-stable (alias mapping doesn't change across years), so we collapse
--    to one row per (source, source_id) using ANY canonical_id.
xref_team_dedup AS (
    SELECT source, source_id, league, ARBITRARY(canonical_id) AS canonical_id
    FROM iceberg.silver.xref_team
    GROUP BY source, source_id, league
),

-- 0b) De-dup xref_player across seasons --------------------------------------
--    Same footgun as xref_team: xref_player PK = (source, source_id, season).
--    Joining on source+source_id alone fans out ~2-4× per player-season
--    (issue #168: 2.87× → 109k dup PK). canonical_id is resolver-stable per
--    player across seasons (1/1092 understat ids drift — ARBITRARY tolerated,
--    same as xref_team_dedup), so collapse to one row per (source, source_id).
xref_player_dedup AS (
    SELECT source, source_id, ARBITRARY(canonical_id) AS canonical_id
    FROM iceberg.silver.xref_player
    GROUP BY source, source_id
),

-- 1) Bridge: understat game_id → fbref match_id ------------------------------
--    Resolve via (date, home_canonical_id, away_canonical_id).
us_sched AS (
    -- bronze.understat_schedule re-ingests every run → ~10 rows per game_id.
    -- Dedup with ROW_NUMBER() to guarantee one row per game_id, taking the
    -- most recently ingested copy.
    SELECT game_id, match_date, home_team, away_team, league, season
    FROM (
        SELECT
            s.game_id,
            CAST(s.date AS DATE)            AS match_date,
            s.home_team,
            s.away_team,
            s.league,
            CAST(s.season AS varchar)       AS season,
            ROW_NUMBER() OVER (
                PARTITION BY s.game_id
                ORDER BY s._ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.understat_schedule s
        WHERE s.game_id IS NOT NULL
    )
    WHERE rn = 1
),

us_sched_resolved AS (
    SELECT
        us.game_id,
        us.match_date,
        us.league,
        us.season,
        xt_home.canonical_id           AS home_canonical_id,
        xt_away.canonical_id           AS away_canonical_id
    FROM us_sched us
    LEFT JOIN xref_team_dedup xt_home
           ON xt_home.source    = 'understat'
          AND xt_home.source_id = us.home_team
          AND xt_home.league    = us.league
    LEFT JOIN xref_team_dedup xt_away
           ON xt_away.source    = 'understat'
          AND xt_away.source_id = us.away_team
          AND xt_away.league    = us.league
),

fb_sched_resolved AS (
    -- Resolve fbref home/away (raw FBref names) → canonical team_id via
    -- xref_team. We intentionally do NOT join xref_team on league/season here:
    -- fbref_match_enriched does not carry a stable varchar season column and
    -- (source, source_id) is unique enough for APL where team aliases are
    -- season-stable. If multi-league bridging is added in Phase B, revisit.
    SELECT
        fb.match_id                    AS fbref_match_id,
        fb.date                        AS match_date,
        xt_home.canonical_id           AS home_canonical_id,
        xt_away.canonical_id           AS away_canonical_id
    FROM iceberg.silver.fbref_match_enriched fb
    LEFT JOIN xref_team_dedup xt_home
           ON xt_home.source    = 'fbref'
          AND xt_home.source_id = fb.home
    LEFT JOIN xref_team_dedup xt_away
           ON xt_away.source    = 'fbref'
          AND xt_away.source_id = fb.away
),

match_bridge AS (
    -- One row per understat game_id mapped to fbref match_id.
    SELECT
        us.game_id            AS understat_game_id,
        fb.fbref_match_id     AS match_id_canonical
    FROM us_sched_resolved us
    INNER JOIN fb_sched_resolved fb
           ON fb.match_date          = us.match_date
          AND fb.home_canonical_id   = us.home_canonical_id
          AND fb.away_canonical_id   = us.away_canonical_id
    WHERE us.home_canonical_id IS NOT NULL
      AND us.away_canonical_id IS NOT NULL
),

-- 2) Dedup bronze.understat_shots (idempotent re-scrape protection) ----------
shots_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY shot_id
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.understat_shots
    WHERE shot_id IS NOT NULL
),

-- 3) Normalize enums + cast scalars -----------------------------------------
shots_norm AS (
    SELECT
        CAST(s.shot_id  AS varchar)    AS shot_id,
        CAST(s.game_id  AS varchar)    AS understat_game_id_str,
        s.game_id                      AS understat_game_id,
        -- xref_team.source_id for source='understat' is the team NAME
        -- (e.g. 'Tottenham'), NOT the numeric team_id. Verified 2026-05-08.
        s.team                         AS understat_team_source_id,
        -- xref_player.source_id for source='understat' is player_id as varchar
        -- (e.g. '5613'). Bronze stores BIGINT → cast to varchar for join.
        CAST(s.player_id AS varchar)   AS understat_player_source_id,
        CAST(s.assist_player_id AS varchar) AS understat_assist_player_source_id,
        CAST(s.minute    AS integer)   AS minute,
        s.location_x                   AS x,
        s.location_y                   AS y,
        s.xg                           AS xg,

        -- body_part: {Right Foot, Left Foot, Head, Other Body Part, NULL}
        CASE
            WHEN s.body_part IN ('Right Foot', 'Left Foot')      THEN 'foot'
            WHEN s.body_part = 'Head'                             THEN 'head'
            WHEN s.body_part IN ('Other Body Part', 'OtherBodyPart') THEN 'other'
            WHEN s.body_part IS NULL                              THEN NULL
            ELSE 'other'
        END                            AS body_part_canonical,

        -- situation: {Open Play, From Corner, Set Piece, Direct Freekick, Penalty}
        CASE
            WHEN s.situation = 'Open Play'        THEN 'open_play'
            WHEN s.situation = 'From Corner'      THEN 'from_corner'
            WHEN s.situation = 'Direct Freekick'  THEN 'from_free_kick'
            WHEN s.situation = 'Set Piece'        THEN 'set_piece'
            WHEN s.situation = 'Penalty'          THEN 'penalty'
            ELSE NULL
        END                            AS situation_canonical,

        -- result: {Goal, Saved Shot, Blocked Shot, Missed Shot, Shot On Post, Own Goal}
        CASE
            WHEN s.result = 'Goal'         THEN 'goal'
            WHEN s.result = 'Saved Shot'   THEN 'saved'
            WHEN s.result = 'Blocked Shot' THEN 'blocked'
            WHEN s.result = 'Missed Shot'  THEN 'missed'
            WHEN s.result = 'Shot On Post' THEN 'post'
            WHEN s.result = 'Own Goal'     THEN 'own_goal'
            ELSE NULL
        END                            AS result_canonical,

        s.league                       AS league,
        CAST(s.season AS varchar)      AS season
    FROM shots_dedup s
    WHERE s.rn = 1
)

-- 4) Final projection — INNER JOIN on match bridge, LEFT JOIN on team/player
SELECT
    sn.shot_id,
    mb.match_id_canonical,

    xt.canonical_id                                AS team_id_canonical,
    xp.canonical_id                                AS player_id_canonical,
    xa.canonical_id                                AS assist_player_id_canonical,

    sn.minute,
    sn.x,
    sn.y,
    sn.xg,

    sn.body_part_canonical,
    sn.situation_canonical,
    sn.result_canonical,

    -- own_goal counts as a goal for is_goal semantics (it ended in net).
    -- DQ note: own_goal credits scoring team via match outcome, NOT shooter.
    (sn.result_canonical IN ('goal', 'own_goal'))  AS is_goal,

    CAST('understat_v1' AS varchar)                AS shot_source,
    CAST('v1'           AS varchar)                AS shot_version,

    sn.league,
    sn.season

FROM shots_norm sn

-- Strict bridge — drops shots whose game_id can't be mapped to fbref match_id.
INNER JOIN match_bridge mb
       ON mb.understat_game_id = sn.understat_game_id

-- Orphan-tolerant team lookup. xref_team.source_id for source='understat'
-- carries the team NAME (e.g. 'Tottenham'), so we join on `team` not `team_id`.
-- Use season-deduped view (xref_team_dedup) to avoid N-row fan-out.
LEFT JOIN xref_team_dedup xt
       ON xt.source    = 'understat'
      AND xt.source_id = sn.understat_team_source_id
      AND xt.league    = sn.league

-- Orphan-tolerant player lookup (xref_player emits us_* canonical for orphans).
-- xref_player rejection rate ≤6.94% per E1 verdict — orphan rows allowed.
LEFT JOIN xref_player_dedup xp
       ON xp.source    = 'understat'
      AND xp.source_id = sn.understat_player_source_id

LEFT JOIN xref_player_dedup xa
       ON xa.source    = 'understat'
      AND xa.source_id = sn.understat_assist_player_source_id
