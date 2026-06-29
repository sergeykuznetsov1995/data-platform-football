-- =============================================================================
-- Gold: fct_match_timeline
-- =============================================================================
-- Unified per-event match chronicle (#427, star-schema design §4.2): goals,
-- cards and substitutions in ONE timeline — "what happened and on which
-- minute" answered by a single `SELECT ... ORDER BY event_seq`, no UNION of
-- fct_goal / fct_card / fct_substitution needed. Those three narrow facts
-- were dropped in #448 (this table is their replacement).
--
-- Sources:
--   iceberg.silver.fbref_match_events     — PRIMARY (already deduped, season
--                                           already compact slug '2425')
--   iceberg.silver.whoscored_events_spadl — FALLBACK, per-match all-or-nothing:
--                                           a match takes WhoScored events ONLY
--                                           when it has zero FBref events. No
--                                           event-level cross-source dedup —
--                                           minute mismatches between sources
--                                           would double goals and break the
--                                           running score / dense event_seq.
--                                           Gate is two-legged (#459): by
--                                           match_id AND by physical identity
--                                           (league, season, date, canonical
--                                           teams) so an unbridged WS twin of
--                                           an FBref-covered match can't
--                                           double-enter under the raw id.
--                                           #736: reads SILVER (one-hop), not
--                                           bronze — original WhoScored type via
--                                           `_action_source_note`, raw JSON via
--                                           `qualifiers_raw`; silver is already
--                                           deduped + double-ids pre-cast.
--   iceberg.bronze.whoscored_schedule     — bridge spine (game_id → date/home/away)
--   iceberg.silver.fbref_match_enriched   — bridge target (FBref hex match_id)
--   iceberg.silver.xref_match/team/player — canonical resolvers
--
-- ADRs folded into this file:
--   * event_type dictionary (8 values): goal, own_goal, penalty_goal,
--     penalty_missed, yellow_card, second_yellow, red_card, substitution.
--     FBref bronze 'penalty' → 'penalty_goal' (scored penalty). Since #447 the
--     scraper splits the FBref penalty_miss / yellow_red_card sprites into
--     their own bronze values, so 'penalty_missed' and 'second_yellow' are now
--     populated from BOTH sources (previously penalty_missed was WhoScored-only
--     and FBref missed penalties leaked in as penalty_goal, inflating score).
--   * substitution: player_id = player going OFF (main actor, Opta/StatsBomb
--     convention per issue spec), related_player_id = player coming ON.
--     FBref source is INVERTED (player_id=ON, secondary_player_id=OFF) — the
--     columns are swapped here. WhoScored SubstitutionOff rows natively carry
--     related_player_id = ON player (100% coverage on live corpus), so the
--     SubstitutionOn twin rows are dropped entirely — no pair-matching dance.
--   * own_goal: team_id = team CREDITED with the goal; player_id = actual
--     striker from the opposite team. FBref rows already sit on the credited
--     side (team/team_side = beneficiary — see the former fct_goal.sql ADR,
--     removed in #448, kept in git history), so
--     team_side feeds the running score directly. Opta attributes the event
--     to the STRIKER's team → the WhoScored branch flips the credited side
--     (and takes team_id from the bridge's opposite-team canonical).
--   * goal assist: related_player_id. FBref secondary_player_id; WhoScored
--     Goal.related_player_id (verified = assist on live corpus). NULL where
--     not applicable (cards, own goals, penalty miss) — by design.
--   * time: minute = base minute, minute_added = stoppage ("90+4" → 90/4).
--     FBref minute is a 1-based display varchar with optional '+'. WhoScored
--     (Opta) minute is 0-based CUMULATIVE within half (probed live: FirstHalf
--     0–59, SecondHalf 45–105) → normalised +1 to FBref's 1-based convention
--     (#521), THEN clamped: FirstHalf >45 ⇒ 45+(m−45), SecondHalf >90 ⇒
--     90+(m−90). period: '1H' | '2H' | 'ET' (FBref base >90 ⇒ ET; league
--     corpus has no ET in practice). WhoScored non-half periods
--     (PreMatch/PostGame/shootout) are dropped.
--   * event_seq: ROW_NUMBER per match ordered by (period, minute, added,
--     second, event-priority, player) — dense, 1-based, deterministic. True
--     intra-minute order is unknowable from FBref granularity, so the
--     priority tiebreaker (goals before cards before subs) is stable-but-
--     arbitrary; the running score may be transiently off-by-one on the
--     non-goal row of a same-minute pair.
--   * score_home_after/score_away_after: running SUM of goal-ish events
--     (goal, own_goal, penalty_goal) by credited side, materialised over
--     event_seq from a LOWER CTE level — Trino rejects same-level SELECT
--     aliases inside OVER() (COLUMN_NOT_FOUND; DuckDB unit tests don't catch
--     this — EXPLAIN (TYPE VALIDATE) on live Trino does).
--
-- DAG-integration note: STAGE_3_FACTS wraps this SELECT in
-- `CREATE TABLE iceberg.gold.fct_match_timeline AS ...
--  WITH (partitioning=ARRAY['league','season'])` via
-- `gold_tasks.run_gold_transform()`. This file MUST stay a pure SELECT.
-- `_silver_created_at` lineage column is appended by the wrapper — do NOT
-- add it here.
--
-- =============================================================================
-- Output schema
-- =============================================================================
--   match_id            varchar     FBref hex when bridged;
--                                   'whoscored_raw_<game_id>' fallback
--   event_seq           integer     dense 1..N within match — PK with match_id
--   period              varchar     '1H' | '2H' | 'ET'
--   minute              integer     base minute (90 for "90+4")
--   minute_added        integer     stoppage minutes, NULL when none
--   event_type          varchar     8-value dictionary (see ADR above)
--   team_id             varchar     credited team for goal-ish events,
--                                   actor's team otherwise — orphan-tolerant
--   player_id           varchar     main actor (scorer / carded / going OFF)
--   related_player_id   varchar     assist on goals, coming ON for subs
--   score_home_after    integer     running score after this event
--   score_away_after    integer     running score after this event
--   event_source        varchar     'fbref' | 'whoscored'
--   league              varchar     partition key
--   season              varchar     partition key (compact slug '2425')
--   _ingested_at        timestamp(6) bronze provenance
-- =============================================================================

WITH
-- ============================================================================
-- Branch A: FBref (primary) — silver is already deduped, season already slug
-- ============================================================================
fb_raw AS (
    SELECT
        fe.match_id                                          AS src_match_id,
        TRY_CAST(split_part(fe.minute, '+', 1) AS integer)   AS minute,
        TRY_CAST(split_part(fe.minute, '+', 2) AS integer)   AS minute_added,
        CASE fe.event_type
            WHEN 'goal'                THEN 'goal'
            WHEN 'penalty'             THEN 'penalty_goal'
            WHEN 'penalty_missed'      THEN 'penalty_missed'
            WHEN 'own_goal'            THEN 'own_goal'
            WHEN 'yellow_card'         THEN 'yellow_card'
            WHEN 'second_yellow_card'  THEN 'second_yellow'
            WHEN 'red_card'            THEN 'red_card'
            WHEN 'substitution'        THEN 'substitution'
        END                                                  AS event_type,
        -- main actor; FBref sub convention is inverted (player_id = ON) —
        -- swap so the actor is the player going OFF
        CASE WHEN fe.event_type = 'substitution'
             THEN fe.secondary_player_id
             ELSE fe.player_id
        END                                                  AS actor_raw,
        CASE
            WHEN fe.event_type = 'substitution'
                THEN fe.player_id                            -- coming ON
            WHEN fe.event_type IN ('goal', 'penalty')
                THEN fe.secondary_player_id                  -- assist
            ELSE NULL                                        -- cards, own_goal
        END                                                  AS related_raw,
        fe.team                                              AS team_name_raw,
        -- own_goal rows already sit on the credited side (former fct_goal.sql
        -- ADR, removed in #448);
        -- silver values are lowercase 'home'/'away' — lower() guards drift
        lower(fe.team_side)                                  AS credited_side,
        fe.league                                            AS league,
        fe.season                                            AS season,
        fe._bronze_ingested_at                               AS _ingested_at
    FROM iceberg.silver.fbref_match_events fe
    WHERE fe.event_type IN ('goal', 'penalty', 'penalty_missed', 'own_goal',
                            'yellow_card', 'second_yellow_card', 'red_card',
                            'substitution')
),

fb_resolved AS (
    SELECT
        COALESCE(xm.canonical_id, fb.src_match_id)           AS match_id,
        CASE
            WHEN fb.minute <= 45 THEN 1
            WHEN fb.minute <= 90 THEN 2
            ELSE 3
        END                                                  AS period_num,
        fb.minute                                            AS minute,
        fb.minute_added                                      AS minute_added,
        CAST(NULL AS integer)                                AS second_in_minute,
        fb.event_type                                        AS event_type,
        xt.canonical_id                                      AS team_id,
        xp_actor.canonical_id                                AS player_id,
        xp_rel.canonical_id                                  AS related_player_id,
        fb.credited_side                                     AS credited_side,
        CAST('fbref' AS varchar)                             AS event_source,
        fb.league                                            AS league,
        fb.season                                            AS season,
        fb._ingested_at                                      AS _ingested_at
    FROM fb_raw fb
    LEFT JOIN iceberg.silver.xref_match xm
        ON xm.source    = 'fbref'
       AND xm.source_id = fb.src_match_id
       AND xm.league    = fb.league
       AND xm.season    = fb.season
    LEFT JOIN iceberg.silver.xref_team xt
        ON xt.source    = 'fbref'
       AND xt.source_id = fb.team_name_raw
       AND xt.league    = fb.league
       AND xt.season    = fb.season
    LEFT JOIN iceberg.silver.xref_player xp_actor
        ON xp_actor.source    = 'fbref'
       AND xp_actor.source_id = fb.actor_raw
       AND xp_actor.league    = fb.league
       AND xp_actor.season    = fb.season
    LEFT JOIN iceberg.silver.xref_player xp_rel
        ON xp_rel.source    = 'fbref'
       AND xp_rel.source_id = fb.related_raw
       AND xp_rel.league    = fb.league
       AND xp_rel.season    = fb.season
    WHERE fb.minute IS NOT NULL
      AND fb.event_type IS NOT NULL
),

-- ============================================================================
-- Branch B: WhoScored (per-match fallback)
-- ============================================================================
-- SubstitutionOn rows are intentionally NOT selected: the Off row carries
-- related_player_id = ON player, giving one timeline row per swap for free.
-- #736: WhoScored fallback reads silver.whoscored_events_spadl (one-hop), NOT
-- bronze. Silver already deduped re-scrapes (ROW_NUMBER over the full natural
-- key) and pre-cast the double ids to varchar, so the former ws_events_dedup
-- ROW_NUMBER pass is gone. `_action_source_note` carries the original WhoScored
-- type (these 6 types are never 'Aerial', so it equals the raw type exactly);
-- `qualifiers_raw` carries the original JSON — the classification below is the
-- byte-for-byte bronze logic, just re-sourced.
ws_events_src AS (
    SELECT
        match_id                  AS ws_game_id,    -- = CAST(game_id AS varchar)
        period,
        minute,
        second,
        _action_source_note       AS type,
        qualifiers_raw            AS qualifiers,
        player_id_raw,
        related_player_id_raw,
        team_name_raw,
        league,
        season,
        _bronze_ingested_at       AS _ingested_at
    FROM iceberg.silver.whoscored_events_spadl
    WHERE _action_source_note IN ('Goal', 'Card', 'SubstitutionOff',
                                  'MissedShots', 'SavedShot', 'ShotOnPost')
      AND period IN ('FirstHalf', 'SecondHalf',
                     'FirstPeriodOfExtraTime', 'SecondPeriodOfExtraTime')
),

ws_schedule_dedup AS (
    SELECT
        game_id,
        date,
        home_team,
        away_team,
        league,
        season,
        ROW_NUMBER() OVER (
            PARTITION BY game_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.whoscored_schedule
),

-- #459: xref_team is season-grained (PK = source, source_id, league, season;
-- season is the compact slug '2425' for ALL sources since #404 — the old
-- "ws='2425' vs fb='2024' format mismatch" justification for dropping season
-- no longer holds). Without the season key one canonical_id expands to every
-- historical FBref name variant ('Newcastle Utd' / 'Newcastle United'); the
-- variant that misses fme.home/away produced a second bridge row with
-- fbref_match_id = NULL and duplicated WhoScored events under
-- 'whoscored_raw_<game_id>' (same pattern as the former fct_card.sql,
-- removed in #448).
-- #445: season-scoping alone no longer suffices — xref_team now legally
-- carries TWO same-season fbref spellings per canonical (schedule short name
-- + match-page full name), so the bridge below aggregates to one row per
-- WS game instead of relying on (canonical, league, season) uniqueness.
xref_team_canonical AS (
    SELECT DISTINCT source, source_id, canonical_id, league, season
    FROM iceberg.silver.xref_team
    WHERE canonical_id IS NOT NULL
),

-- WhoScored → FBref match bridge, extended vs the former fct_card.sql
-- (removed in #448) with home/away
-- names + canonicals: needed for event-side resolution (running score) and
-- the own_goal credited-team flip.
-- #445 guard: the xt_*_fb reverse lookups fan out across same-season fbref
-- name variants; only the schedule spelling can match fme.home/away, so
-- MAX over the NULL twins is exact (not a tiebreak) and GROUP BY collapses
-- the bridge to ≤1 row per WS game.
ws_match_bridge AS (
    SELECT
        CAST(s.game_id AS varchar)               AS ws_game_id,
        s.league                                 AS league,
        s.season                                 AS ws_season,
        CAST(s.date AS date)                     AS match_date,     -- #459: identity-gate key
        s.home_team                              AS home_team_name,
        s.away_team                              AS away_team_name,
        xt_home_ws.canonical_id                  AS home_team_id,
        xt_away_ws.canonical_id                  AS away_team_id,
        MAX(fme.match_id)                        AS fbref_match_id  -- #445
    FROM ws_schedule_dedup s
    LEFT JOIN xref_team_canonical xt_home_ws
        ON xt_home_ws.source    = 'whoscored'
       AND xt_home_ws.source_id = s.home_team
       AND xt_home_ws.league    = s.league
       AND xt_home_ws.season    = s.season          -- #459
    LEFT JOIN xref_team_canonical xt_away_ws
        ON xt_away_ws.source    = 'whoscored'
       AND xt_away_ws.source_id = s.away_team
       AND xt_away_ws.league    = s.league
       AND xt_away_ws.season    = s.season          -- #459
    LEFT JOIN xref_team_canonical xt_home_fb
        ON xt_home_fb.source       = 'fbref'
       AND xt_home_fb.canonical_id = xt_home_ws.canonical_id
       AND xt_home_fb.league       = s.league
       AND xt_home_fb.season       = s.season       -- #459
    LEFT JOIN xref_team_canonical xt_away_fb
        ON xt_away_fb.source       = 'fbref'
       AND xt_away_fb.canonical_id = xt_away_ws.canonical_id
       AND xt_away_fb.league       = s.league
       AND xt_away_fb.season       = s.season       -- #459
    LEFT JOIN iceberg.silver.fbref_match_enriched fme
        ON fme.league = s.league
       AND fme.home   = xt_home_fb.source_id
       AND fme.away   = xt_away_fb.source_id
       AND fme.date   = CAST(s.date AS date)
    WHERE s.rn = 1
    -- #445: ordinals, NOT select aliases — Trino raises COLUMN_NOT_FOUND on
    -- same-level aliases in GROUP BY (DuckDB-based tests mask this).
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

-- Classify deduped WhoScored events onto the 8-value dictionary. The
-- quote-anchored displayName regex does NOT match 'PenaltyFaced' /
-- 'KeeperPenaltySaved' etc. — same trick the former fct_card.sql relied on.
ws_classified AS (
    SELECT
        we.ws_game_id                                        AS ws_game_id,
        we.period                                            AS period_raw,
        -- #521: Opta minute is 0-based cumulative; +1 → FBref 1-based scale
        CAST(we.minute AS integer) + 1                       AS minute_cum,
        CAST(we.second AS integer)                           AS second_in_minute,
        CASE
            WHEN we.type = 'Goal'
                 AND regexp_like(we.qualifiers, '"displayName"\s*:\s*"OwnGoal"')
                THEN 'own_goal'
            WHEN we.type = 'Goal'
                 AND regexp_like(we.qualifiers, '"displayName"\s*:\s*"Penalty"')
                THEN 'penalty_goal'
            WHEN we.type = 'Goal'
                THEN 'goal'
            WHEN we.type IN ('MissedShots', 'SavedShot', 'ShotOnPost')
                 AND regexp_like(we.qualifiers, '"displayName"\s*:\s*"Penalty"')
                THEN 'penalty_missed'
            WHEN we.type = 'Card'
                 AND regexp_like(we.qualifiers, '"displayName"\s*:\s*"SecondYellow"')
                THEN 'second_yellow'
            WHEN we.type = 'Card'
                 AND regexp_like(we.qualifiers, '"displayName"\s*:\s*"Red"')
                THEN 'red_card'
            WHEN we.type = 'Card'
                 AND regexp_like(we.qualifiers, '"displayName"\s*:\s*"Yellow"')
                THEN 'yellow_card'
            WHEN we.type = 'SubstitutionOff'
                THEN 'substitution'
            ELSE NULL                                        -- drop in WHERE below
        END                                                  AS event_type,
        we.player_id_raw                                     AS actor_raw,
        CASE
            WHEN we.type = 'SubstitutionOff'                 -- coming ON
                THEN we.related_player_id_raw
            WHEN we.type = 'Goal'                            -- assist
                 AND NOT regexp_like(we.qualifiers, '"displayName"\s*:\s*"OwnGoal"')
                THEN we.related_player_id_raw
            ELSE NULL
        END                                                  AS related_raw,
        we.team_name_raw                                     AS team_name_raw,
        we.league                                            AS league,
        we.season                                            AS season,
        we._ingested_at                                      AS _ingested_at
    FROM ws_events_src we
),

ws_resolved AS (
    SELECT
        COALESCE(
            mb.fbref_match_id,
            'whoscored_raw_' || wc.ws_game_id
        )                                                    AS match_id,
        CASE wc.period_raw
            WHEN 'FirstHalf'  THEN 1
            WHEN 'SecondHalf' THEN 2
            ELSE 3
        END                                                  AS period_num,
        -- minute_cum already +1-normalised (1-based, #521) → base + stoppage
        CASE
            WHEN wc.period_raw = 'FirstHalf'  AND wc.minute_cum > 45 THEN 45
            WHEN wc.period_raw = 'SecondHalf' AND wc.minute_cum > 90 THEN 90
            ELSE wc.minute_cum
        END                                                  AS minute,
        CASE
            WHEN wc.period_raw = 'FirstHalf'  AND wc.minute_cum > 45
                THEN wc.minute_cum - 45
            WHEN wc.period_raw = 'SecondHalf' AND wc.minute_cum > 90
                THEN wc.minute_cum - 90
            ELSE NULL
        END                                                  AS minute_added,
        wc.second_in_minute                                  AS second_in_minute,
        wc.event_type                                        AS event_type,
        -- own_goal: credit goes to the OPPOSITE team (Opta attributes the
        -- event to the striker's team)
        CASE
            WHEN wc.event_type = 'own_goal'
                 AND wc.team_name_raw = mb.home_team_name THEN mb.away_team_id
            WHEN wc.event_type = 'own_goal'
                 AND wc.team_name_raw = mb.away_team_name THEN mb.home_team_id
            WHEN wc.event_type = 'own_goal'               THEN NULL
            ELSE xt.canonical_id
        END                                                  AS team_id,
        xp_actor.canonical_id                                AS player_id,
        xp_rel.canonical_id                                  AS related_player_id,
        -- lowercase 'home'/'away' — same convention as the FBref branch
        CASE
            WHEN wc.event_type = 'own_goal'
                 AND wc.team_name_raw = mb.home_team_name THEN 'away'
            WHEN wc.event_type = 'own_goal'
                 AND wc.team_name_raw = mb.away_team_name THEN 'home'
            WHEN wc.team_name_raw = mb.home_team_name     THEN 'home'
            WHEN wc.team_name_raw = mb.away_team_name     THEN 'away'
            ELSE NULL                              -- side unresolved → inc=0
        END                                                  AS credited_side,
        CAST('whoscored' AS varchar)                         AS event_source,
        wc.league                                            AS league,
        wc.season                                            AS season,
        wc._ingested_at                                      AS _ingested_at,
        -- #459: bridge identity for the ws_only identity gate — filtered out
        -- of the final output by `unified`'s explicit projection.
        mb.match_date                                        AS bridge_match_date,
        mb.home_team_id                                      AS bridge_home_team_id,
        mb.away_team_id                                      AS bridge_away_team_id
    FROM ws_classified wc
    LEFT JOIN ws_match_bridge mb
        ON mb.ws_game_id = wc.ws_game_id
       AND mb.league     = wc.league
       AND mb.ws_season  = wc.season
    LEFT JOIN iceberg.silver.xref_team xt
        ON xt.source    = 'whoscored'
       AND xt.source_id = wc.team_name_raw
       AND xt.league    = wc.league
       AND xt.season    = wc.season
    LEFT JOIN iceberg.silver.xref_player xp_actor
        ON xp_actor.source    = 'whoscored'
       AND xp_actor.source_id = wc.actor_raw
       AND xp_actor.league    = wc.league
       AND xp_actor.season    = wc.season
    LEFT JOIN iceberg.silver.xref_player xp_rel
        ON xp_rel.source    = 'whoscored'
       AND xp_rel.source_id = wc.related_raw
       AND xp_rel.league    = wc.league
       AND xp_rel.season    = wc.season
    WHERE wc.event_type IS NOT NULL
      AND wc.minute_cum IS NOT NULL
),

-- ============================================================================
-- Per-match fallback gate + unification
-- ============================================================================
fb_match_ids AS (
    SELECT DISTINCT match_id FROM fb_resolved
),

-- #459: physical-identity keys of every FBref-covered match. When the
-- WhoScored→FBref bridge fails, the WS match enters under
-- 'whoscored_raw_<game_id>' and the match_id gate alone cannot see that the
-- same physical match already has FBref events — double counting in
-- league/season aggregates. Identity = (league, season, date, canonical
-- home/away). INNER JOINs: an FBref match without an fme row or without
-- season-scoped canonicals contributes no key (safe direction — the WS twin
-- is kept rather than wrongly dropped).
fb_match_keys AS (
    SELECT
        f.league                                 AS league,
        f.season                                 AS season,
        fme.date                                 AS match_date,
        xt_home.canonical_id                     AS home_team_id,
        xt_away.canonical_id                     AS away_team_id
    FROM (SELECT DISTINCT match_id, league, season FROM fb_resolved) f
    JOIN iceberg.silver.fbref_match_enriched fme
        ON fme.match_id = f.match_id
    JOIN xref_team_canonical xt_home
        ON xt_home.source    = 'fbref'
       AND xt_home.source_id = fme.home
       AND xt_home.league    = f.league
       AND xt_home.season    = f.season
    JOIN xref_team_canonical xt_away
        ON xt_away.source    = 'fbref'
       AND xt_away.source_id = fme.away
       AND xt_away.league    = f.league
       AND xt_away.season    = f.season
),

-- Two-leg gate: a WS row survives only with NEITHER a match_id twin NOR a
-- physical-identity twin in the FBref branch. Rows whose bridge identity is
-- unresolvable (orphan xref, no schedule row) can never satisfy the equality
-- join → they are kept under the raw id, never wrongly dropped.
ws_only AS (
    SELECT w.*
    FROM ws_resolved w
    LEFT JOIN fb_match_ids f
        ON f.match_id = w.match_id
    LEFT JOIN fb_match_keys k
        ON  k.league       = w.league
        AND k.season       = w.season
        AND k.match_date   = w.bridge_match_date
        AND k.home_team_id = w.bridge_home_team_id
        AND k.away_team_id = w.bridge_away_team_id
    WHERE f.match_id   IS NULL      -- no match_id twin
      AND k.match_date IS NULL      -- no physical-identity twin (#459)
),

unified AS (
    SELECT
        match_id, period_num, minute, minute_added, second_in_minute,
        event_type, team_id, player_id, related_player_id, credited_side,
        event_source, league, season, _ingested_at
    FROM fb_resolved
    UNION ALL
    SELECT
        match_id, period_num, minute, minute_added, second_in_minute,
        event_type, team_id, player_id, related_player_id, credited_side,
        event_source, league, season, _ingested_at
    FROM ws_only
),

enriched AS (
    SELECT
        u.*,
        CASE u.event_type
            WHEN 'goal'           THEN 1
            WHEN 'own_goal'       THEN 1
            WHEN 'penalty_goal'   THEN 1
            WHEN 'penalty_missed' THEN 2
            WHEN 'yellow_card'    THEN 3
            WHEN 'second_yellow'  THEN 4
            WHEN 'red_card'       THEN 5
            ELSE 6                                           -- substitution
        END                                                  AS event_priority,
        CASE WHEN u.event_type IN ('goal', 'own_goal', 'penalty_goal')
                  AND u.credited_side = 'home' THEN 1 ELSE 0
        END                                                  AS home_inc,
        CASE WHEN u.event_type IN ('goal', 'own_goal', 'penalty_goal')
                  AND u.credited_side = 'away' THEN 1 ELSE 0
        END                                                  AS away_inc
    FROM unified u
),

-- event_seq materialised one CTE level BELOW the score windows — Trino
-- rejects same-level SELECT aliases inside OVER() (COLUMN_NOT_FOUND).
sequenced AS (
    SELECT
        e.*,
        CAST(ROW_NUMBER() OVER (
            PARTITION BY e.match_id
            ORDER BY e.period_num,
                     e.minute,
                     COALESCE(e.minute_added, 0),
                     COALESCE(e.second_in_minute, 0),
                     e.event_priority,
                     COALESCE(e.player_id, '?'),
                     e.event_type
        ) AS integer)                                        AS event_seq
    FROM enriched e
)

SELECT
    s.match_id                                               AS match_id,
    s.event_seq                                              AS event_seq,
    CASE s.period_num
        WHEN 1 THEN '1H'
        WHEN 2 THEN '2H'
        ELSE 'ET'
    END                                                      AS period,
    s.minute                                                 AS minute,
    s.minute_added                                           AS minute_added,
    s.event_type                                             AS event_type,
    s.team_id                                                AS team_id,
    s.player_id                                              AS player_id,
    s.related_player_id                                      AS related_player_id,
    CAST(SUM(s.home_inc) OVER (
        PARTITION BY s.match_id
        ORDER BY s.event_seq
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS integer)                                            AS score_home_after,
    CAST(SUM(s.away_inc) OVER (
        PARTITION BY s.match_id
        ORDER BY s.event_seq
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS integer)                                            AS score_away_after,
    s.event_source                                           AS event_source,
    s.league                                                 AS league,
    s.season                                                 AS season,
    s._ingested_at                                           AS _ingested_at
FROM sequenced s
