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
--                                      in fct_event.sql). match_id_canonical here
--                                      is the raw whoscored game_id (varchar BIGINT
--                                      — NO 'whoscored_raw_' prefix; the source
--                                      label is in match_id_source column).
--                                      v0_unbridged until E1.5 cutover.
--   iceberg.gold.fct_shot            — ~47k rows, situation_canonical +
--                                      body_part_canonical. match_id_canonical
--                                      is FBref hex slug (Understat-bridged in
--                                      fct_shot.sql via date+canonical teams).
--                                      team_id_canonical is FBref-canonical.
--   iceberg.gold.fct_team_match      — spine: one row per (match_id, team_id)
--                                      with date+season+league. match_id is
--                                      FBref hex slug.
--   iceberg.bronze.whoscored_schedule— bridge: provides game_id → match_date
--                                      (parsed from start_time ISO-8601 string)
--                                      so we can JOIN whoscored game_id to dim_match
--                                      via (date, league, season) + team identity.
--   iceberg.gold.dim_match           — dim_match.match_id is FBref hex; carries
--                                      home_team_id / away_team_id / date as the
--                                      anchor for cross-source bridging.
--
-- =============================================================================
-- ADR — match_id bridging strategy (Option A: dim_match anchor)
-- =============================================================================
-- fct_event.match_id_canonical is the raw WhoScored game_id (v0_unbridged per
-- fct_event.sql ADR-1). It does NOT join directly to fct_team_match.match_id
-- (FBref hex slug). We bridge by aggregating events to (game_id, team_id_canonical),
-- then mapping game_id → fbref match_id via:
--
--   bronze.whoscored_events (game_id, date) → dim_match.match_id
--     ON  dim_match.date = bronze.date
--     AND (game_id-side resolves to team_id_canonical that equals
--          dim_match.home_team_id OR away_team_id for the given fixture)
--
-- We keep this lightweight: a `ws_game_dates` CTE that pre-aggregates one row
-- per WhoScored game_id with the kickoff date (MIN to dedup multi-event per
-- game), then a `ws_match_bridge` CTE that JOINs bronze game_id → dim_match
-- via date + team_id_canonical match (we already have team_id_canonical on
-- fct_event side, so single-side identity check is sufficient — a date+team
-- fixture is unique).
--
-- Why not bridge via (date, home_canonical_id, away_canonical_id)?
-- That would require resolving both teams of each WhoScored fixture (two extra
-- xref_team JOINs through the bronze.team_id → name bridge), exactly mirroring
-- fct_event.sql's event_team_names CTE. fct_event already did that work and
-- baked team_id_canonical into the row. We just inherit it — cheaper and
-- consistent with the team identity already shipped in Gold.
--
-- Cutover plan: when E1.5 ships (xref_match adds whoscored→fbref bridge),
-- replace ws_match_bridge with `LEFT JOIN xref_match xm ON xm.source='whoscored'
-- AND xm.source_id = e.match_id_canonical`. Output schema unchanged.
--
-- =============================================================================
-- ADR — team_id_canonical compatibility
-- =============================================================================
-- fct_event.team_id_canonical resolves WhoScored team_id_raw (numeric Opta) →
-- FBref-canonical team_id via xref_team(source='whoscored'). fct_team_match.team_id
-- and dim_match.home_team_id/away_team_id come from gold.entity_xref(source='fbref')
-- canonical_id. Both pipelines feed the same canonical universe (silver.xref_team)
-- so canonical_ids ARE comparable post-resolution. Orphan WhoScored teams
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
--   season                    bigint     partition key (year-of-start, normalised
--                                        from compact 4-char form '2425' → 2024
--                                        to align with dim_match.season=bigint)
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

-- 0) WhoScored game_id → kickoff date (one row per game_id) ------------------
--    bronze.whoscored_schedule stores game_id as BIGINT and start_time as
--    varchar ISO-8601 ('2025-02-23T14:00:00'). Direct CAST AS date fails
--    on the 'T' separator → SUBSTR(start_time, 1, 10) gets the YYYY-MM-DD
--    prefix, which casts cleanly. Schedule has one row per game_id, so no
--    aggregation needed (event-level table fan-out avoided).
ws_game_dates AS (
    SELECT
        CAST(game_id AS varchar)                              AS ws_game_id,
        league,
        -- Normalise compact season ('2425') → bigint year-of-start (2024)
        -- to match dim_match.season=bigint. Mirrors fct_card.sql normalisation.
        CASE
            WHEN length(season) = 4
             AND TRY_CAST(season AS bigint) BETWEEN 2000 AND 2100
                THEN TRY_CAST(season AS bigint)
            ELSE 2000 + TRY_CAST(substr(season, 1, 2) AS bigint)
        END                                                   AS season,
        CAST(SUBSTR(start_time, 1, 10) AS date)               AS match_date
    FROM iceberg.bronze.whoscored_schedule
    WHERE game_id    IS NOT NULL
      AND start_time IS NOT NULL
),

-- 1) Per (game_id, team_id_canonical) action aggregates ---------------------
--    Filter action_canonical != 'unknown' both for the denominator and every
--    numerator (R3.D5 semantics — see ADR above).
event_agg AS (
    SELECT
        e.match_id_canonical                                  AS ws_game_id,
        e.team_id_canonical                                   AS team_id,
        e.league                                              AS league,
        -- fct_event.season is varchar (silver passthrough); normalise to
        -- bigint year-of-start to match dim_match.season for the bridge JOIN.
        CASE
            WHEN length(e.season) = 4
             AND TRY_CAST(e.season AS bigint) BETWEEN 2000 AND 2100
                THEN TRY_CAST(e.season AS bigint)
            ELSE 2000 + TRY_CAST(substr(e.season, 1, 2) AS bigint)
        END                                                   AS season,
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
        e.team_id_canonical,
        e.league,
        e.season
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

-- 3) WhoScored game_id → FBref match_id bridge (via dim_match) ---------------
--    Anchor on (date, league, season) + identity check that team_id appears
--    on at least one side of the dim_match fixture (home or away). For a
--    single date+league+season+team, a club plays at most one fixture, so
--    the JOIN is 1:1 (modulo orphans, which fall out via NULL).
ws_match_bridge AS (
    SELECT
        ea.ws_game_id,
        ea.team_id,
        dm.match_id                                           AS fbref_match_id
    FROM event_agg ea
    LEFT JOIN ws_game_dates gd
        ON  gd.ws_game_id = ea.ws_game_id
        AND gd.league     = ea.league
        AND gd.season     = ea.season
    LEFT JOIN iceberg.gold.dim_match dm
        ON  dm.date    = gd.match_date
        AND dm.league  = ea.league
        AND dm.season  = ea.season
        AND (dm.home_team_id = ea.team_id OR dm.away_team_id = ea.team_id)
),

-- 4) Per (fbref_match_id, team_id) action shares + success rate -------------
event_shares AS (
    SELECT
        b.fbref_match_id                                      AS match_id,
        ea.team_id                                            AS team_id,
        ea.league                                             AS league,
        ea.season                                             AS season,
        1.0 * ea.n_pass         / NULLIF(ea.total_events, 0)  AS pass_share,
        1.0 * ea.n_dribble      / NULLIF(ea.total_events, 0)  AS dribble_share,
        1.0 * ea.n_tackle       / NULLIF(ea.total_events, 0)  AS tackle_share,
        1.0 * ea.n_interception / NULLIF(ea.total_events, 0)  AS interception_share,
        1.0 * ea.n_cross        / NULLIF(ea.total_events, 0)  AS cross_share,
        1.0 * ea.n_shot         / NULLIF(ea.total_events, 0)  AS shot_share,
        1.0 * ea.n_success      / NULLIF(ea.total_events, 0)  AS success_rate
    FROM event_agg ea
    INNER JOIN ws_match_bridge b
        ON  b.ws_game_id = ea.ws_game_id
        AND b.team_id    = ea.team_id
    WHERE b.fbref_match_id IS NOT NULL
),

-- 5) Per (fbref_match_id, team_id) shot-mix shares --------------------------
shot_shares AS (
    SELECT
        match_id,
        team_id,
        1.0 * n_set_piece / NULLIF(total_shots, 0)            AS set_piece_share,
        1.0 * n_open_play / NULLIF(total_shots, 0)            AS open_play_share,
        1.0 * n_header    / NULLIF(total_shots, 0)            AS header_share
    FROM shot_agg
),

-- 6) Combine onto fct_team_match spine + assign per-team match_rn ------------
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

-- 7) Rolling L5 averages (excludes current match) ---------------------------
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
