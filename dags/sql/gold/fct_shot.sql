-- =============================================================================
-- Gold: fct_shot
-- =============================================================================
-- Shot-grained xG/xA fact table. Multi-source (#699): Understat is the primary
-- feed, SofaScore is a match-level fallback. One row per shot, resolved to
-- canonical match/team/player IDs via E1 xref.
--
-- Source selection is per-match, NOT per-shot: the two feeds share no shot key
-- (Understat shot_id != SofaScore shot_id for the same physical shot), so a
-- match keeps ALL shots from exactly ONE source — the lowest source_priority
-- present (Understat=1, SofaScore=2). SofaScore only fills matches Understat is
-- missing or failed to bridge to the FBref spine. See the match_winner CTE.
--
-- Source:
--   iceberg.bronze.understat_shots          — shot-level events with xG (primary)
--   iceberg.bronze.understat_schedule       — game_id → (date, home, away) lookup
--   iceberg.silver.fbref_match_enriched     — fbref match_id + date/home/away
--   iceberg.silver.sofascore_shots          — canonicalised SofaScore shotmap (#602),
--                                             fallback source, already on fct_shot IDs
--   iceberg.silver.xref_match               — fbref-spine canonical match_id
--   iceberg.silver.xref_team                — team alias resolution (8 sources)
--   iceberg.silver.xref_player              — player resolution (orphan-tolerant)
--
-- Design contract: docs/design/gold-star-schema.md §4.3 (issue #426).
-- PK: (match_id, shot_id)
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
--     assister by NAME only (`assist_player`). There is no per-shot numeric
--     assist id — `bronze.assist_player_id` is a soccerdata roster-row id (#444)
--     and is IGNORED here. The assister is resolved name → bronze.understat_players
--     → xref_player (see us_player_dim CTE). xA aggregation lives in
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
-- Output schema (frozen — star design §4.3)
-- =============================================================================
--   shot_id                  varchar
--   match_id                 varchar    -- fbref match_id (8-char hex)
--   team_id                  varchar    -- xref_team(source='understat')
--   player_id                varchar    -- xref_player(source='understat'); orphan us_* possible
--   assist_player_id         varchar    -- name-resolved canonical (#444); NULL when no assist
--   minute                   integer
--   x, y                     double     -- 0..1 normalized
--   body_part                varchar    -- foot | head | other | NULL
--   situation                varchar    -- open_play | corner | free_kick | set_piece | penalty | NULL
--   xg                       double     -- 0..1
--   psxg                     double     -- post-shot xG. NULL for understat_v1 rows
--                                       --   (Understat has none); sofascore_v1 rows
--                                       --   carry SofaScore xGOT (#699).
--   result                   varchar    -- goal | saved | blocked | off_target | post | own_goal
--                                       --   (own_goal — deviation from design §4.3, kept for
--                                       --    shooter attribution; design doc amended)
--   is_goal                  boolean    -- result IN ('goal','own_goal')
--   shot_source              varchar    -- 'understat_v1' (primary) | 'sofascore_v1' (fallback)
--   shot_version             varchar    -- literal 'v1'
--   league                   varchar
--   season                   varchar    -- normalised to varchar (bronze stores varchar)
--
-- Testable invariants (E3.8):
--   * PK = (match_id, shot_id) is unique.
--   * shot_source ∈ {'understat_v1', 'sofascore_v1'} for every row.
--   * each match_id has exactly ONE shot_source (match-level fallback).
--   * xg ∈ [0, 1] (value_range check).
--   * is_goal = TRUE  iff  result IN ('goal', 'own_goal').
--   * match_id is non-null (INNER JOIN / fbref-spine filter guarantees it).
--   * team_id / player_id may be NULL — orphan-tolerant.
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
    -- #506: drop confidence='orphan' rows before the GROUP BY — they carry a
    -- non-NULL source-prefixed canonical ('us_<slug>') and would leak through
    -- as a resolved team_id. xref_team.sql.j2 contract: orphans excluded from
    -- every cross-source Gold JOIN.
    WHERE confidence <> 'orphan'
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

-- 0c) Understat name→player_id dictionary (#444) -----------------------------
--    Understat has NO per-shot numeric assist id. soccerdata 1.8.8 fills
--    bronze.understat_shots.assist_player_id with the roster-ROW id instead of
--    the player id, so it never matches xref_player and assist resolution was
--    100% NULL. The assister NAME (`assist_player`) IS correct, so we recover
--    the true understat player_id from bronze.understat_players (the same source
--    that feeds xref_player) keyed on (LOWER(name), league, season). The player
--    id is stable across mid-season transfers, so ARBITRARY is safe when a name
--    appears under two teams; identical names within one league-season (rare)
--    are the only residual collision risk, tracked in DQ.
us_player_dim AS (
    SELECT LOWER(player)                          AS player_norm,
           league,
           CAST(season AS varchar)                AS season,
           ARBITRARY(CAST(player_id AS varchar))  AS understat_player_id
    FROM iceberg.bronze.understat_players
    WHERE player IS NOT NULL AND player_id IS NOT NULL
    GROUP BY LOWER(player), league, CAST(season AS varchar)
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
        -- #444: resolve the assister by NAME (bronze.assist_player_id is a
        -- soccerdata roster-row id, not a player id). Lower-cased for the join
        -- against the understat name→id dictionary; same source, so case is the
        -- only normalisation needed.
        LOWER(s.assist_player)         AS assist_player_norm,
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
        END                            AS body_part,

        -- situation: {Open Play, From Corner, Set Piece, Direct Freekick, Penalty}
        -- Domain per star design §4.3: open_play | corner | free_kick | set_piece | penalty
        CASE
            WHEN s.situation = 'Open Play'        THEN 'open_play'
            WHEN s.situation = 'From Corner'      THEN 'corner'
            WHEN s.situation = 'Direct Freekick'  THEN 'free_kick'
            WHEN s.situation = 'Set Piece'        THEN 'set_piece'
            WHEN s.situation = 'Penalty'          THEN 'penalty'
            ELSE NULL
        END                            AS situation,

        -- result: {Goal, Saved Shot, Blocked Shot, Missed Shot, Shot On Post, Own Goal}
        -- Domain per star design §4.3: goal | saved | blocked | off_target | post
        -- (+ own_goal — kept beyond design, see header).
        CASE
            WHEN s.result = 'Goal'         THEN 'goal'
            WHEN s.result = 'Saved Shot'   THEN 'saved'
            WHEN s.result = 'Blocked Shot' THEN 'blocked'
            WHEN s.result = 'Missed Shot'  THEN 'off_target'
            WHEN s.result = 'Shot On Post' THEN 'post'
            WHEN s.result = 'Own Goal'     THEN 'own_goal'
            ELSE NULL
        END                            AS result,

        s.league                       AS league,
        CAST(s.season AS varchar)      AS season
    FROM shots_dedup s
    WHERE s.rn = 1
),

-- 4) Understat branch (primary, source_priority=1) ---------------------------
--    INNER JOIN on match bridge, LEFT JOIN on team/player. Logic unchanged
--    from the single-source v1 — only wrapped in a CTE + tagged source_priority.
understat_final AS (
    SELECT
        sn.shot_id,
        mb.match_id_canonical                          AS match_id,

        xt.canonical_id                                AS team_id,
        xp.canonical_id                                AS player_id,
        xa.canonical_id                                AS assist_player_id,

        sn.minute,
        sn.x,
        sn.y,

        sn.body_part,
        sn.situation,

        sn.xg,
        -- psxg: Understat carries no post-shot xG (the FBref feed that did is
        -- dead since 2026-02) → typed NULL. SofaScore branch fills it from xgot.
        CAST(NULL AS double)                           AS psxg,

        sn.result,

        -- own_goal counts as a goal for is_goal semantics (it ended in net).
        -- DQ note: own_goal credits scoring team via match outcome, NOT shooter.
        (sn.result IN ('goal', 'own_goal'))            AS is_goal,

        CAST('understat_v1' AS varchar)                AS shot_source,
        CAST('v1'           AS varchar)                AS shot_version,

        sn.league,
        sn.season,

        1                                              AS source_priority

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

    -- Assister resolution (#444): recover the true understat player_id from the
    -- name dictionary (Understat has no per-shot assist id), then xref to canonical.
    -- NULL assist_player_norm (no assist) → no match → NULL assist_player_id.
    LEFT JOIN us_player_dim upd
           ON upd.player_norm = sn.assist_player_norm
          AND upd.league      = sn.league
          AND upd.season      = sn.season

    LEFT JOIN xref_player_dedup xa
           ON xa.source    = 'understat'
          AND xa.source_id = upd.understat_player_id
),

-- 5) SofaScore branch (fallback, source_priority=2) --------------------------
--    silver.sofascore_shots (#602) is ALREADY canonicalised to the same
--    match/team/player IDs and the same enum domains (result/body_part/
--    situation) and coordinate scale (0..1) as the Understat branch, so we read
--    it directly — no xref re-join here (the (league,season) footgun is already
--    handled inside the silver projection). We DROP:
--      * non-fbref matches — silver emits orphan ids ('sofascore_<raw>' when the
--        game is absent from xref_match, 'ss_<id>' when xref_match has it but it
--        didn't bridge to the FBref spine). Neither is on the fbref spine; the
--        first also fails ref_integrity. fct_shot is fbref-spine-keyed (Understat
--        branch INNER-JOINs the bridge), so SofaScore must match the same shape.
--        No fbref match_id (8-hex) starts with 's', so the two prefix filters are
--        collision-free.
--      * NULL-xg shots — fct_shot.xg is NOT NULL / [0,1] at ERROR severity.
--    psxg := xgot (#699): SofaScore xGOT is post-shot xG — the metric FBref's
--    dead feed used to provide. assist_player_id has no SofaScore shotmap source.
sofascore_final AS (
    SELECT
        ss.shot_id,
        ss.match_id,

        ss.team_id,
        ss.player_id,
        CAST(NULL AS varchar)                          AS assist_player_id,

        ss.minute,
        ss.x,
        ss.y,

        ss.body_part,
        ss.situation,

        ss.xg,
        ss.xgot                                        AS psxg,

        ss.result,
        ss.is_goal,

        ss.shot_source,                                -- literal 'sofascore_v1'
        CAST('v1' AS varchar)                          AS shot_version,

        ss.league,
        ss.season,

        2                                              AS source_priority

    FROM iceberg.silver.sofascore_shots ss
    WHERE ss.match_id NOT LIKE 'sofascore_%'
      AND ss.match_id NOT LIKE 'ss_%'
      AND ss.xg IS NOT NULL
),

-- 6) Union both sources ------------------------------------------------------
all_shots AS (
    SELECT shot_id, match_id, team_id, player_id, assist_player_id, minute,
           x, y, body_part, situation, xg, psxg, result, is_goal,
           shot_source, shot_version, league, season, source_priority
    FROM understat_final
    UNION ALL
    SELECT shot_id, match_id, team_id, player_id, assist_player_id, minute,
           x, y, body_part, situation, xg, psxg, result, is_goal,
           shot_source, shot_version, league, season, source_priority
    FROM sofascore_final
),

-- 7) Per-match source winner (match-level fallback) --------------------------
--    The two sources share NO shot key (Understat shot_id != SofaScore shot_id
--    for the same physical shot), so individual shots cannot be COALESCE'd or
--    deduped across sources (#602). Instead we pick ONE source per canonical
--    match: the lowest source_priority present. A match covered by Understat
--    keeps ALL its Understat shots; SofaScore only fills matches Understat is
--    missing (or failed to bridge). No double-count, no cross-source coordinate
--    mixing (orientation is not guaranteed equal between the two feeds).
match_winner AS (
    SELECT match_id, MIN(source_priority) AS win_priority
    FROM all_shots
    GROUP BY match_id
)

-- 8) Final projection — keep only the winning source's shots per match --------
SELECT
    a.shot_id,
    a.match_id,
    a.team_id,
    a.player_id,
    a.assist_player_id,
    a.minute,
    a.x,
    a.y,
    a.body_part,
    a.situation,
    a.xg,
    a.psxg,
    a.result,
    a.is_goal,
    a.shot_source,
    a.shot_version,
    a.league,
    a.season

FROM all_shots a
JOIN match_winner w
       ON w.match_id     = a.match_id
      AND w.win_priority = a.source_priority
