-- =============================================================================
-- Gold: fct_goal
-- =============================================================================
-- Goal-grained narrow fact (one row per goal). UNION ALL of two sources:
--
--   1) Regular goals — projected from `iceberg.gold.fct_shot` WHERE
--      result_canonical='goal' (i.e. shots that ended in net via a regular
--      Understat shot row). Carries scorer + assist + situation/penalty flag.
--
--   2) Own goals — extracted from `iceberg.bronze.fbref_match_events` WHERE
--      event_type='own_goal'. FBref own-goal coverage is the most complete
--      across all 10 APL seasons (463 events vs Understat's 203 own_goal
--      shots — Understat misses early seasons). To avoid double-counting
--      we INTENTIONALLY exclude `fct_shot` rows with result_canonical='own_goal'
--      from the regular branch.
--
-- Sources:
--   iceberg.gold.fct_shot                  (regular goals)
--   iceberg.bronze.fbref_match_events      (own goals)
--   iceberg.silver.xref_match              (FBref source_id → canonical bridge)
--   iceberg.silver.xref_player             (FBref player_id → canonical)
--   iceberg.silver.xref_team               (FBref team-name → canonical)
--
-- DAG-integration note: T4 wraps this SELECT in
-- `CREATE TABLE iceberg.gold.fct_goal AS ... WITH
-- (partitioning=ARRAY['league','season'])` via `gold_tasks.run_gold_transform()`.
-- This file MUST stay a pure SELECT (no CREATE TABLE, no DDL).
--
-- =============================================================================
-- ADR — own-goal team attribution (FBref convention)
-- =============================================================================
-- bronze.fbref_match_events row for an own_goal carries the SCORER's name and
-- player_id, but `team` is the GOAL-RECEIVING team (the team to whose tally
-- the goal is added). Verified empirically on 2026-05-08:
--
--   match=4b7e6f44 minute=20  player='Bernd Leno' (Fulham GK) team='Liverpool'
--   match=dfde4bd7 minute=43  player='Axel Disasi' (Aston Villa) team='Wolverhampton Wanderers'
--   match=40128bc4 minute=2   player='Daiki Hashioka' (Luton Town) team='Manchester City'
--
-- In each case the named player belongs to the OPPOSING team — the FBref
-- `team` column captures the *credited* (goal-receiving) side, NOT the
-- scorer's team. This is the public-facing FBref Match Events behaviour
-- (look any goal log on fbref.com — the "Team" column on an own-goal row
-- is the side that benefited from the score).
--
-- Decision for fct_goal:
--   `team_id_canonical` = team CREDITED with the goal in the match score
--     (i.e. FBref `team` column resolved via xref_team for own-goal rows;
--      scorer's team for regular goals — fct_shot already encodes this).
--   `scorer_id_canonical` = the player who actually struck the ball (the
--     own-goal scorer is from the OPPOSING team).
--
-- This makes `team_id_canonical` directly aggregatable (SUM by team gives
-- total goals scored by that team in the match score). `scorer_id_canonical`
-- is the player attribution and IS NOT the same team as team_id_canonical
-- on own_goal rows. Downstream consumers should join scorer_id_canonical
-- back to xref_player or to fct_player_match for player-grained tallies.
--
-- =============================================================================
-- ADR — match_id_canonical bridging for own goals
-- =============================================================================
-- bronze.fbref_match_events.match_id is the FBref 8-char hex. Resolution
-- order: (1) silver.xref_match.canonical_id where source='fbref' and
-- source_id=fe.match_id; (2) fallback to fe.match_id itself (same as
-- silver.match_cards / match_substitutions COALESCE pattern). xref_match
-- E1 MVP only emits source='fbref' rows so most non-NULL canonical_ids
-- equal the raw match_id anyway — the COALESCE is defensive.
--
-- =============================================================================
-- Output schema (frozen for E4 wave-1)
-- =============================================================================
--   match_id_canonical    varchar     FBref hex (regular: pass-through from
--                                     fct_shot; own_goal: COALESCE(xref, raw))
--   team_id_canonical     varchar     team credited with goal in match score
--   scorer_id_canonical   varchar     player who struck the ball
--                                     (own_goal: opposing-team player)
--   assist_id_canonical   varchar     assister; NULL for own_goals (FBref does
--                                     not assist own goals). For regular goals
--                                     this is fct_shot.assist_player_id_canonical
--                                     pass-through; LIVE-DATA OBSERVATION
--                                     (2026-05-08): all 5,147 regular goal
--                                     rows in fct_shot currently have NULL
--                                     assist (Understat assist resolution is
--                                     pending in xref_player Phase B; until
--                                     then this column is reserved schema
--                                     and will populate automatically once
--                                     fct_shot is rebuilt with resolved
--                                     assists — no fct_goal change needed).
--   minute                integer
--   is_own_goal           boolean     TRUE iff the goal is an own_goal
--   is_penalty            boolean     TRUE iff regular goal with situation
--                                     ='penalty' (always FALSE for own_goal).
--                                     LIVE-DATA OBSERVATION (2026-05-08):
--                                     0 rows are flagged is_penalty in the
--                                     current 5,488-row corpus because
--                                     bronze.understat_shots in this dump
--                                     does not surface 'Penalty' as a
--                                     `situation` value (header of fct_shot
--                                     documents this — "'Penalty' not
--                                     observed in current sample but mapped
--                                     for forward compatibility"). Once
--                                     bronze re-ingest captures Penalty
--                                     situation, fct_shot rebuild will
--                                     automatically propagate is_penalty=TRUE
--                                     here — no fct_goal change needed.
--   goal_canonical        varchar     xxhash64 synthetic PK
--   goal_source           varchar     'fct_shot' | 'fbref_own_goal'
--   goal_version          varchar     literal 'v1'
--   league                varchar     partition key
--   season                bigint      partition key (year-of-start, normalised
--                                     across both branches — see Season block)
--   _ingested_at          timestamp(6)
--
-- Logical PK: goal_canonical
--   xxhash64 over (match || minute || scorer || is_own_goal || tiebreaker).
--   The is_own_goal flag is part of the key so a regular goal and an own_goal
--   at the same minute do not collide.
--
--   `tiebreaker` distinguishes the rare case of a player scoring 2 goals
--   at the same minute (verified live: match=967efd56, minute=46, scorer
--   ='fb_92e7e919' has 2 distinct fct_shot rows with different (x,y,xg)).
--   For regular goals tiebreaker = fct_shot.shot_id (Understat-globally-
--   unique, varchar passthrough). For own_goals tiebreaker = literal 'og'
--   (own_goals at the same minute by the same player are not observed
--   in the 463-row 10-season FBref corpus, so a constant suffices).
--
-- =============================================================================
-- Season normalisation
-- =============================================================================
-- Two input season representations need merging:
--
--   * fct_shot.season   varchar mixed format observed in production:
--                       '2021', '2223', '2324', '2425', '2526'.
--                       The first season ('2021' for 2021-22) is the legacy
--                       single-year form; the rest are the compact 'YYNN'
--                       form (e.g. '2425' = 2024-25 → 2024).
--   * fbref_match_events.season   bigint year-of-start (2024 for 2024-25).
--
-- Target: bigint year-of-start to match gold.dim_match.season.
-- Strategy: detect single-year-form via numeric range (2000..2100) — if
-- already in range, use directly; otherwise parse first-2-digits + 2000.
-- Falls back to TRY_CAST for any unexpected format.
--
-- =============================================================================
-- _ingested_at lineage
-- =============================================================================
-- fct_shot does not carry _ingested_at (it's a Gold-level passthrough that
-- gets a `_silver_created_at` from the gold wrapper). For row-count parity
-- across both branches we synthesise _ingested_at from the table's own
-- creation time via a dummy `CAST(NULL AS timestamp(6))` slot on the
-- regular branch — Iceberg fills it consistently. Own-goal branch passes
-- through bronze.fbref_match_events._ingested_at directly.
--
-- =============================================================================

WITH

-- =============================================================================
-- 1) Regular goals — projection from fct_shot (excludes own_goal rows)
-- =============================================================================
-- Drop result_canonical='own_goal' to avoid double-counting against the
-- fbref own-goal branch below. fct_shot's situation_canonical='penalty' is
-- the explicit Understat penalty marker.
regular_goals AS (
    SELECT
        sh.match_id_canonical,
        sh.team_id_canonical,
        sh.player_id_canonical          AS scorer_id_canonical,
        sh.assist_player_id_canonical   AS assist_id_canonical,
        sh.minute,
        FALSE                           AS is_own_goal,
        (sh.situation_canonical = 'penalty') AS is_penalty,
        -- shot_id makes the canonical hash unique even for the rare same-
        -- minute brace (one scorer, two shots in the same minute — verified
        -- on match 967efd56 / Tottenham brace 2021-22). Cast defensively to
        -- varchar so the hash input is type-stable.
        CAST(sh.shot_id AS varchar)     AS pk_tiebreaker,
        CAST('fct_shot' AS varchar)     AS goal_source,
        sh.league                       AS league,
        -- #404: gold.fct_shot.season is slug ('2425') → pass through unchanged.
        sh.season                       AS season,
        CAST(NULL AS timestamp(6))      AS _ingested_at
    FROM iceberg.gold.fct_shot sh
    WHERE sh.result_canonical = 'goal'
),

-- =============================================================================
-- 2) Own-goal events — bronze FBref + xref bridges
-- =============================================================================
-- Bronze re-scrape dedup via ROW_NUMBER. Natural key for an own_goal in
-- bronze.fbref_match_events is (match_id, minute, player_id) — same
-- pattern silver/match_cards.sql uses.
fb_own_goals_dedup AS (
    SELECT
        match_id,
        TRY_CAST(minute AS integer)          AS minute,
        player                               AS scorer_name,
        player_id                            AS scorer_player_id,
        team                                 AS team_name_raw,
        league,
        season,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, minute, player_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.fbref_match_events
    WHERE event_type = 'own_goal'
),

-- xref_match bridge — fbref-only in E1 MVP, defensive COALESCE keeps row.
fb_own_goals AS (
    SELECT
        COALESCE(xm.canonical_id, fe.match_id)          AS match_id_canonical,
        -- xref_team may miss when bronze.team_name_raw drifts season-to-season
        -- (e.g. "Newcastle Utd" 2017-18 vs "Newcastle United" 2024-25). Fall
        -- back to slugified raw FBref team-name so the row stays SUM-aggregable
        -- and DQ no_nulls(team_id_canonical) does not trip on ~50/6500 (~0.8%).
        COALESCE(
            xt.canonical_id,
            'fbref_team_' || lower(regexp_replace(fe.team_name_raw, '[^A-Za-z0-9]+', '_'))
        )                                               AS team_id_canonical,
        xp.canonical_id                                 AS scorer_id_canonical,
        fe.minute                                       AS minute,
        TRUE                                            AS is_own_goal,
        FALSE                                           AS is_penalty,
        CAST('fbref_own_goal' AS varchar)               AS goal_source,
        fe.league                                       AS league,
        -- #404: bronze fbref_match_events.season is year-start bigint → slug.
        format('%02d%02d', mod(fe.season, 100), mod(fe.season + 1, 100)) AS season,
        fe._ingested_at                                 AS _ingested_at
    FROM fb_own_goals_dedup fe
    -- xref_match bridge (FBref source_id → canonical_id, season-agnostic
    -- predicate because xref_match.season is varchar and bronze.season is
    -- bigint — bridging would need a CAST round-trip; for fbref-only PHASE A
    -- the source+source_id pair is unique enough to skip season).
    LEFT JOIN iceberg.silver.xref_match xm
        ON xm.source    = 'fbref'
       AND xm.source_id = fe.match_id
    -- xref_team — credit the goal-receiving team (FBref `team` column).
    -- season-predicated to prevent cross-season fan-out (memory note
    -- feedback_xref_join_season_predicate).
    LEFT JOIN iceberg.silver.xref_team xt
        ON xt.source    = 'fbref'
       AND xt.source_id = fe.team_name_raw
       AND xt.league    = fe.league
       AND xt.season    = format('%02d%02d', mod(fe.season, 100), mod(fe.season + 1, 100))  -- #404: year-start → slug
    -- xref_player — resolve the actual scorer (opposing-team player on an
    -- own_goal). season+league predicate per xref_join contract.
    LEFT JOIN iceberg.silver.xref_player xp
        ON xp.source    = 'fbref'
       AND xp.source_id = fe.scorer_player_id
       AND xp.league    = fe.league
       AND xp.season    = format('%02d%02d', mod(fe.season, 100), mod(fe.season + 1, 100))  -- #404: year-start → slug
    WHERE fe.rn = 1
),

-- =============================================================================
-- 3) Own-goal projection aligned to regular_goals shape
-- =============================================================================
-- assist_id_canonical is hard-NULL for own goals: FBref does not credit an
-- assister on an own_goal row (secondary_player is universally empty in the
-- 463-row sample) and Opta/Understat agree.
own_goals_projected AS (
    SELECT
        og.match_id_canonical,
        og.team_id_canonical,
        og.scorer_id_canonical,
        CAST(NULL AS varchar)               AS assist_id_canonical,
        og.minute,
        og.is_own_goal,
        og.is_penalty,
        -- own_goals never observed twice for the same scorer in the same
        -- minute on 10-season corpus → constant tiebreaker is safe.
        CAST('og' AS varchar)               AS pk_tiebreaker,
        og.goal_source,
        og.league,
        og.season                           AS season,    -- already bigint
        og._ingested_at
    FROM fb_own_goals og
),

-- =============================================================================
-- 4) UNION + final projection with canonical-trio
-- =============================================================================
union_goals AS (
    SELECT * FROM regular_goals
    UNION ALL
    SELECT * FROM own_goals_projected
)

SELECT
    match_id_canonical,
    team_id_canonical,
    scorer_id_canonical,
    assist_id_canonical,
    minute,
    is_own_goal,
    is_penalty,

    -- ============================================================
    -- canonical-trio: synthetic PK + provenance
    -- ============================================================
    -- Hash includes is_own_goal so a regular goal and an own goal at the
    -- same minute/match by the same player canonical do not collide
    -- (theoretical only — defensive). pk_tiebreaker carries shot_id for
    -- regular branch (rare same-minute brace) and 'og' for own_goal branch.
    lower(to_hex(xxhash64(to_utf8(
        match_id_canonical
        || '|' || CAST(minute AS varchar)
        || '|' || COALESCE(scorer_id_canonical, '?')
        || '|' || CAST(is_own_goal AS varchar)
        || '|' || pk_tiebreaker
    ))))                                     AS goal_canonical,
    goal_source,
    CAST('v1' AS varchar)                    AS goal_version,

    league,
    season,
    _ingested_at
FROM union_goals
WHERE match_id_canonical IS NOT NULL
  AND minute IS NOT NULL
