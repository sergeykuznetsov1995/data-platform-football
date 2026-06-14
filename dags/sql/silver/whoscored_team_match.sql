-- =============================================================================
-- Silver: whoscored_team_match
-- =============================================================================
--
-- One row per (match_id, team_id, league, season) — WhoScored event counters
-- rolled up to the team grain from `silver.whoscored_events_spadl`
-- (SPADL-canonical, 24-value action enum + outcome_success boolean).
--
-- Feeds Gold `fct_team_match` v2 (#95) — see
-- docs/decisions/T6_team_facts_schema.md §6.2 (UNIQUE_WHOSCORED block).
--
-- Sources:
--   silver.whoscored_events_spadl  (action_canonical, outcome_success,
--                                   x/y coords, qualifiers_raw,
--                                   team_id_raw — varchar, already double-cast)
--
-- Notes:
--   * Bronze dedup already done inside events_spadl (ROW_NUMBER on full
--     natural key) — no need to repeat.
--   * `team_id_raw` already passed through CAST(CAST(... AS BIGINT) AS varchar)
--     in events_spadl (avoids '9.5408E4' scientific notation). Just rename
--     to `team_id` on SELECT. See feedback_bronze_double_id_cast.md.
--   * `match_id` is raw game_id varchar; xref_match resolution happens in
--     Gold (#95) via silver.xref_match. See ADR in dag_transform_e3.py.
--   * Cards (yellow/red) NOT aggregated here — SPADL collapses Card events
--     into 'unknown' (not one of the 24 canonical actions). Gold COALESCEs
--     from FBref + SofaScore.
--   * shots_total / shots_on_target_proxy count all shot variants ('shot' +
--     'shot_penalty' + 'shot_freekick'), incl. goals (Goal→shot family, #462),
--     matching whoscored_player_match_aggregate.shots. shots_on_target_proxy
--     stays a coarse proxy: WhoScored marks MissedShots outcome_type=
--     'Successful', so it over-counts off-target attempts. fct_shot is the
--     source of truth for actual goals / true on-target.
--   * Derived metrics:
--     - defensive_actions_third = defensive third (x < 33.33 on SPADL pitch
--       [0..100], attacking direction) ∩ {tackle, interception, clearance,
--       ball_recovery, foul}.
--     - set_piece_events: count of throw-ins/corners/freekicks/goal-kicks
--       detected via `qualifiers_raw` regex. SPADL canonization collapses
--       these into action_canonical='pass', so the sub-type markers
--       (ThrowIn / CornerTaken / FreekickTaken / GoalKick displayNames) only
--       survive on the preserved JSON-string. Probe on APL 25/26 confirms
--       ~80 set-pieces/match, share ≈ 5-6%. Preserved as a column so
--       team_season can recompute share without re-scanning events.
--     - set_piece_share_pct = set_piece_events / total_events × 100.
-- =============================================================================

SELECT
    match_id,
    team_id_raw                                                              AS team_id,

    -- ========= Volume =========
    COUNT(*)                                                                  AS total_events,

    -- ========= Pass-block =========
    COUNT_IF(action_canonical = 'pass')                                       AS pass_total,
    COUNT_IF(action_canonical = 'pass' AND outcome_success)                   AS pass_ok,
    ROUND(100.0 * COUNT_IF(action_canonical = 'pass' AND outcome_success)
          / NULLIF(COUNT_IF(action_canonical = 'pass'), 0), 2)                AS pass_pct,
    -- KeyPass qualifier preserved on Pass rows; SPADL keeps qualifiers_raw verbatim.
    COUNT_IF(action_canonical = 'pass'
             AND regexp_like(COALESCE(qualifiers_raw, ''),
                             '"displayName"\s*:\s*"KeyPass"'))                AS key_passes_ws,

    -- ========= Take-on =========
    COUNT_IF(action_canonical = 'take_on')                                    AS takeon_att,
    COUNT_IF(action_canonical = 'take_on' AND outcome_success)                AS takeon_won,
    ROUND(100.0 * COUNT_IF(action_canonical = 'take_on' AND outcome_success)
          / NULLIF(COUNT_IF(action_canonical = 'take_on'), 0), 2)             AS takeon_pct,

    -- ========= Defensive =========
    COUNT_IF(action_canonical = 'tackle')                                     AS tackle_att,
    COUNT_IF(action_canonical = 'tackle' AND outcome_success)                 AS tackle_won,
    COUNT_IF(action_canonical = 'interception')                               AS interceptions,
    COUNT_IF(action_canonical = 'clearance')                                  AS clearances,
    COUNT_IF(action_canonical = 'ball_recovery')                              AS ball_recoveries,

    -- ========= Shooting (proxy — see header) =========
    -- Count ALL shot variants (open-play 'shot' + 'shot_penalty' +
    -- 'shot_freekick'), incl. goals now routed into the shot family (#462).
    -- Mirrors whoscored_player_match_aggregate.shots (counted from bronze).
    COUNT_IF(action_canonical IN ('shot', 'shot_penalty', 'shot_freekick'))
        AS shots_total,
    COUNT_IF(action_canonical IN ('shot', 'shot_penalty', 'shot_freekick')
             AND outcome_success)                                             AS shots_on_target_proxy,

    -- ========= Discipline =========
    COUNT_IF(action_canonical = 'foul')                                       AS fouls_committed,

    -- ========= Spatial =========
    COUNT_IF(x >= 83 AND y BETWEEN 21 AND 79
             AND action_canonical IN ('pass','take_on','shot','dribble','bad_touch')) AS touches_in_box,
    COUNT_IF(action_canonical IN ('tackle','interception','clearance',
                                   'ball_recovery','foul')
             AND x < 33.33)                                                   AS defensive_actions_third,

    -- ========= Set-piece (qualifier-based, see header) =========
    -- SPADL canonization rolls throw-ins/corners/freekicks into 'pass',
    -- so set-piece sub-type markers only survive on qualifiers_raw.
    COUNT_IF(regexp_like(COALESCE(qualifiers_raw, ''),
                         '"displayName"\s*:\s*"(ThrowIn|CornerTaken|FreekickTaken|GoalKick)"')
             OR action_canonical IN ('shot_penalty','shot_freekick'))           AS set_piece_events,
    ROUND(100.0 * COUNT_IF(regexp_like(COALESCE(qualifiers_raw, ''),
                                       '"displayName"\s*:\s*"(ThrowIn|CornerTaken|FreekickTaken|GoalKick)"')
                           OR action_canonical IN ('shot_penalty','shot_freekick'))
          / NULLIF(COUNT(*), 0), 2)                                             AS set_piece_share_pct,

    -- ========= Partition keys =========
    league,
    season

FROM iceberg.silver.whoscored_events_spadl
WHERE team_id_raw IS NOT NULL
GROUP BY match_id, team_id_raw, league, season
