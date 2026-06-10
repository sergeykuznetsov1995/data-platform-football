-- =============================================================================
-- Gold: feat_team_event_style
-- =============================================================================
-- Per-(match, team) rolling event-style features (last 5 completed matches),
-- computed PRE-match (point-in-time safe).
--
-- Captures team play-style fingerprint via SPADL action shares + outcome
-- success rate + shot-set-piece tendencies. Strong complements to xG and
-- form features for downstream 1X2 model — especially picks up tactical
-- shifts (high-press vs sit-deep, set-piece reliance, etc.).
--
-- Window: ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING per (team_id, season)
-- ORDER BY date — excludes current match to prevent leakage. Mask first
-- 5 rows per partition with CASE WHEN match_rn > 5 (same convention as
-- feat_team_form / feat_team_xg_form).
--
-- =============================================================================
-- Sources
-- =============================================================================
--   iceberg.gold.fct_event           — ~695k rows, 24 SPADL action_canonical
--                                      enum + outcome_success bool. team_id_canonical
--                                      is FBref-canonical (resolved via xref_team
--                                      in fct_event.sql). match_id_canonical is
--                                      now the FBref hex slug when the WhoScored
--                                      game is bridged (E3 Phase B SHIPPED — see
--                                      fct_event.sql ADR-1: LEFT JOIN silver.xref_match
--                                      on source='whoscored'). Unbridged games carry
--                                      an orphan id ('ws_<game_id>') or raw game_id
--                                      and simply fall out on the spine JOIN.
--   iceberg.gold.fct_shot            — ~47k rows, situation_canonical +
--                                      body_part_canonical. match_id_canonical
--                                      is FBref hex slug (Understat-bridged in
--                                      fct_shot.sql via date+canonical teams).
--                                      team_id_canonical is FBref-canonical.
--   iceberg.gold.fct_team_match      — spine: one row per (match_id, team_id)
--                                      with date+season+league. match_id is
--                                      FBref hex slug.
--
-- =============================================================================
-- ADR — match_id bridging strategy (direct join, post E3 Phase B)
-- =============================================================================
-- fct_event.match_id_canonical is already the FBref hex slug (E3 Phase B bridged
-- WhoScored game_id → FBref via silver.xref_match in fct_event.sql ADR-1). So it
-- joins DIRECTLY to fct_team_match.match_id with no extra bridging — exactly the
-- same pattern shot_agg/shot_shares use for fct_shot below.
--
-- We aggregate events to (match_id_canonical, team_id_canonical) and feed the
-- shares straight onto the spine. Unbridged WhoScored games (match_id_canonical
-- = 'ws_<game_id>' orphan, or raw game_id fallback) do not match any FBref hex
-- on the spine and drop out via the LEFT JOIN (NULL shares) — acceptable, and
-- consistent with shot_agg's behaviour for unbridged shots.
--
-- History: a prior version of this file double-bridged match_id_canonical back
-- through bronze.whoscored_schedule + dim_match by (date, league, season). After
-- Phase B made match_id_canonical = FBref hex, that bridge silently matched zero
-- rows and left every event-share column NULL (issue #206).
--
-- =============================================================================
-- ADR — team_id_canonical compatibility
-- =============================================================================
-- fct_event.team_id_canonical resolves WhoScored team_id_raw (numeric Opta) →
-- FBref-canonical team_id via xref_team(source='whoscored'). fct_team_match.team_id
-- and dim_match.home_team_id/away_team_id come from silver.xref_team(source='fbref')
-- canonical_id (post-E1.5 cutover; previously gold.entity_xref). Both pipelines
-- feed the same canonical universe (silver.xref_team) so canonical_ids ARE
-- comparable post-resolution. Orphan WhoScored teams
-- (xref_team has no row) emit team_id_canonical=NULL on fct_event side and the
-- aggregation per (game_id, team_id_canonical) drops those rows naturally.
-- This is acceptable: orphan-team coverage gap surfaces in DQ on the empty
-- fallback path or as <100% join coverage on this view.
--
-- =============================================================================
-- ADR — denominator semantics (per-match shares)
-- =============================================================================
-- total_events = COUNT(*) WHERE action_canonical != 'unknown'. R3.D5 keeps
-- 'unknown' rows in fct_event for audit (row-count parity with bronze), but
-- including them in the denominator would dilute the shares with non-football
-- markers (substitutions, half-time markers, period flags). Numerator filters
-- mirror the same WHERE clause for consistency.
--
-- success_rate uses the same filtered denominator: SUM(outcome_success
-- WHERE action != 'unknown') / total_events. NULL outcome_success counts as
-- 0 (Trino BOOL→INT cast: NULL→NULL→excluded by SUM, total stays denominator
-- → ~equivalent to "outcome=success/total_played"). Acceptable for E6 MVP.
--
-- shot_share uses fct_event.action_canonical='shot' for self-consistency.
-- The 3 shot situation/body_part shares (set_piece/open_play/header) divide
-- by total_shots PER MATCH-TEAM from fct_shot, NOT by total_events — they
-- describe the *composition of shots*, not the share of all actions that
-- were a particular shot type. NULLIF on shots_total avoids division-by-zero
-- for matches with zero shots from a side.
--
-- =============================================================================
-- Output schema (frozen — must match feat_team_event_style_empty.sql exactly)
-- =============================================================================
--   match_id                  varchar    fct_team_match.match_id (FBref hex)
--   team_id                   varchar    fct_team_match.team_id (canonical)
--   date                      date       fct_team_match.date
--   season                    varchar    partition key (slug '2425', #404 — inherited
--                                        from fct_team_match.season, now slug)
--   league                    varchar    partition key
--   pass_share_l5_avg         double
--   dribble_share_l5_avg      double
--   tackle_share_l5_avg       double
--   interception_share_l5_avg double
--   cross_share_l5_avg        double
--   shot_share_l5_avg         double
--   success_rate_l5_avg       double
--   set_piece_share_l5_avg    double
--   open_play_share_l5_avg    double
--   header_share_l5_avg       double
--
-- PK: (match_id, team_id). Partitioning: (league, season).
-- =============================================================================

WITH

-- 1) Per (match_id, team_id_canonical) action aggregates --------------------
--    match_id_canonical is already the FBref hex (E3 Phase B), so we group on
--    it directly — no WhoScored game_id bridge needed (see ADR above).
--    Filter action_canonical != 'unknown' both for the denominator and every
--    numerator (R3.D5 semantics — see ADR above).
event_agg AS (
    SELECT
        e.match_id_canonical                                  AS match_id,
        e.team_id_canonical                                   AS team_id,
        COUNT(*)                                              AS total_events,
        SUM(CASE WHEN e.action_canonical = 'pass'         THEN 1 ELSE 0 END) AS n_pass,
        SUM(CASE WHEN e.action_canonical = 'dribble'      THEN 1 ELSE 0 END) AS n_dribble,
        SUM(CASE WHEN e.action_canonical = 'tackle'       THEN 1 ELSE 0 END) AS n_tackle,
        SUM(CASE WHEN e.action_canonical = 'interception' THEN 1 ELSE 0 END) AS n_interception,
        SUM(CASE WHEN e.action_canonical = 'cross'        THEN 1 ELSE 0 END) AS n_cross,
        SUM(CASE WHEN e.action_canonical = 'shot'         THEN 1 ELSE 0 END) AS n_shot,
        SUM(CASE WHEN e.outcome_success                   THEN 1 ELSE 0 END) AS n_success
    FROM iceberg.gold.fct_event e
    WHERE e.action_canonical    != 'unknown'
      AND e.team_id_canonical   IS NOT NULL
    GROUP BY
        e.match_id_canonical,
        e.team_id_canonical
),

-- 2) Per (fbref_match_id, team_id) shot-mix aggregates ----------------------
--    fct_shot.match_id_canonical is already the FBref hex (Understat-bridged
--    via xref_match in fct_shot.sql). Direct JOIN on fct_team_match.match_id
--    works without further bridging.
shot_agg AS (
    SELECT
        s.match_id_canonical                                  AS match_id,
        s.team_id_canonical                                   AS team_id,
        COUNT(*)                                              AS total_shots,
        SUM(CASE WHEN s.situation_canonical = 'set_piece' THEN 1 ELSE 0 END) AS n_set_piece,
        SUM(CASE WHEN s.situation_canonical = 'open_play' THEN 1 ELSE 0 END) AS n_open_play,
        SUM(CASE WHEN s.body_part_canonical = 'head'      THEN 1 ELSE 0 END) AS n_header
    FROM iceberg.gold.fct_shot s
    WHERE s.team_id_canonical IS NOT NULL
    GROUP BY
        s.match_id_canonical,
        s.team_id_canonical
),

-- 3) Per (match_id, team_id) action shares + success rate -------------------
--    match_id is the FBref hex from event_agg; joins directly to the spine
--    below (mirrors shot_shares from shot_agg).
event_shares AS (
    SELECT
        match_id,
        team_id,
        1.0 * n_pass         / NULLIF(total_events, 0)        AS pass_share,
        1.0 * n_dribble      / NULLIF(total_events, 0)        AS dribble_share,
        1.0 * n_tackle       / NULLIF(total_events, 0)        AS tackle_share,
        1.0 * n_interception / NULLIF(total_events, 0)        AS interception_share,
        1.0 * n_cross        / NULLIF(total_events, 0)        AS cross_share,
        1.0 * n_shot         / NULLIF(total_events, 0)        AS shot_share,
        1.0 * n_success      / NULLIF(total_events, 0)        AS success_rate
    FROM event_agg
),

-- 4) Per (fbref_match_id, team_id) shot-mix shares --------------------------
shot_shares AS (
    SELECT
        match_id,
        team_id,
        1.0 * n_set_piece / NULLIF(total_shots, 0)            AS set_piece_share,
        1.0 * n_open_play / NULLIF(total_shots, 0)            AS open_play_share,
        1.0 * n_header    / NULLIF(total_shots, 0)            AS header_share
    FROM shot_agg
),

-- 5) Combine onto fct_team_match spine + assign per-team match_rn ------------
--    LEFT JOIN keeps every (match_id, team_id) row from the spine even if
--    we have no events / shots for it (the per-share columns will be NULL,
--    and the L5 window will see them as NULL — strict point-in-time semantics
--    keep these matches' rolling averages NULL until 5 prior matches with
--    non-NULL shares accumulate via window LAG behaviour).
spine AS (
    SELECT
        tm.match_id,
        tm.team_id,
        tm.date,
        tm.season,
        tm.league,
        es.pass_share,
        es.dribble_share,
        es.tackle_share,
        es.interception_share,
        es.cross_share,
        es.shot_share,
        es.success_rate,
        ss.set_piece_share,
        ss.open_play_share,
        ss.header_share,
        ROW_NUMBER() OVER (
            PARTITION BY tm.team_id, tm.season
            ORDER BY tm.date, tm.match_id
        ) AS match_rn
    FROM iceberg.gold.fct_team_match tm
    LEFT JOIN event_shares es
        ON  es.match_id = tm.match_id
        AND es.team_id  = tm.team_id
    LEFT JOIN shot_shares ss
        ON  ss.match_id = tm.match_id
        AND ss.team_id  = tm.team_id
),

-- 6) Rolling L5 averages (excludes current match) ---------------------------
rolled AS (
    SELECT
        *,
        AVG(pass_share)         OVER w AS pass_share_l5_avg_raw,
        AVG(dribble_share)      OVER w AS dribble_share_l5_avg_raw,
        AVG(tackle_share)       OVER w AS tackle_share_l5_avg_raw,
        AVG(interception_share) OVER w AS interception_share_l5_avg_raw,
        AVG(cross_share)        OVER w AS cross_share_l5_avg_raw,
        AVG(shot_share)         OVER w AS shot_share_l5_avg_raw,
        AVG(success_rate)       OVER w AS success_rate_l5_avg_raw,
        AVG(set_piece_share)    OVER w AS set_piece_share_l5_avg_raw,
        AVG(open_play_share)    OVER w AS open_play_share_l5_avg_raw,
        AVG(header_share)       OVER w AS header_share_l5_avg_raw
    FROM spine
    WINDOW w AS (
        PARTITION BY team_id, season
        ORDER BY date, match_id
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    )
)

SELECT
    match_id,
    team_id,
    date,
    season,
    league,

    -- Mask to NULL until at least 5 prior matches accumulated (point-in-time).
    CASE WHEN match_rn > 5 THEN pass_share_l5_avg_raw         END AS pass_share_l5_avg,
    CASE WHEN match_rn > 5 THEN dribble_share_l5_avg_raw      END AS dribble_share_l5_avg,
    CASE WHEN match_rn > 5 THEN tackle_share_l5_avg_raw       END AS tackle_share_l5_avg,
    CASE WHEN match_rn > 5 THEN interception_share_l5_avg_raw END AS interception_share_l5_avg,
    CASE WHEN match_rn > 5 THEN cross_share_l5_avg_raw        END AS cross_share_l5_avg,
    CASE WHEN match_rn > 5 THEN shot_share_l5_avg_raw         END AS shot_share_l5_avg,
    CASE WHEN match_rn > 5 THEN success_rate_l5_avg_raw       END AS success_rate_l5_avg,
    CASE WHEN match_rn > 5 THEN set_piece_share_l5_avg_raw    END AS set_piece_share_l5_avg,
    CASE WHEN match_rn > 5 THEN open_play_share_l5_avg_raw    END AS open_play_share_l5_avg,
    CASE WHEN match_rn > 5 THEN header_share_l5_avg_raw       END AS header_share_l5_avg
FROM rolled
WHERE match_id IS NOT NULL
  AND team_id  IS NOT NULL
