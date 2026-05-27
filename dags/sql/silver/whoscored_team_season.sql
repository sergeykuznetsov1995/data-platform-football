-- =============================================================================
-- Silver: whoscored_team_season
-- =============================================================================
--
-- One row per (team_id, league, season) — season rollup of
-- `silver.whoscored_team_match`. Pct/share metrics recomputed at season
-- grain from SUM(ok) / SUM(total), NOT averaged across matches.
--
-- Feeds Gold `fct_team_season_stats` (#94) — see
-- docs/decisions/T6_team_facts_schema.md §5.1 (UNIQUE_WHOSCORED block).
--
-- Sources:
--   silver.whoscored_team_match    (T6.3 match-grain aggregate, this file's sibling)
--
-- Notes:
--   * Rollup from team_match (not events_spadl) keeps COUNT_IF logic DRY —
--     single source of truth for action classification.
--   * Pct recomputation uses ratio of sums, not mean of ratios. A team with
--     5 passes (100%) and 500 passes (90%) has true season pct ≈ 90.1%, not 95%.
--   * `set_piece_events` is read solely for share recomputation here; not
--     exposed downstream — Gold gets the recomputed `set_piece_share_pct`.
--   * No xref JOIN here — Gold #94 resolves team_id_canonical via
--     silver.xref_team with the (league, season) predicate.
-- =============================================================================

SELECT
    team_id,

    -- ========= Volume =========
    COUNT(*)                                                                  AS matches_seen,
    SUM(total_events)                                                          AS total_events,

    -- ========= Pass-block (SUM + recompute pct) =========
    SUM(pass_total)                                                            AS pass_total,
    SUM(pass_ok)                                                               AS pass_ok,
    ROUND(100.0 * SUM(pass_ok) / NULLIF(SUM(pass_total), 0), 2)                AS pass_pct,
    SUM(key_passes_ws)                                                         AS key_passes_ws,

    -- ========= Take-on (SUM + recompute) =========
    SUM(takeon_att)                                                            AS takeon_att,
    SUM(takeon_won)                                                            AS takeon_won,
    ROUND(100.0 * SUM(takeon_won) / NULLIF(SUM(takeon_att), 0), 2)             AS takeon_pct,

    -- ========= Defensive (SUM) =========
    SUM(tackle_att)                                                            AS tackle_att,
    SUM(tackle_won)                                                            AS tackle_won,
    SUM(interceptions)                                                         AS interceptions,
    SUM(clearances)                                                            AS clearances,
    SUM(ball_recoveries)                                                       AS ball_recoveries,

    -- ========= Shooting (SUM) =========
    SUM(shots_total)                                                           AS shots_total,
    SUM(shots_on_target_proxy)                                                 AS shots_on_target_proxy,

    -- ========= Discipline (SUM) =========
    SUM(fouls_committed)                                                       AS fouls_committed,

    -- ========= Spatial (SUM) =========
    SUM(touches_in_box)                                                        AS touches_in_box,
    SUM(defensive_actions_third)                                               AS defensive_actions_third,

    -- ========= Set-piece (recompute share at season grain) =========
    ROUND(100.0 * SUM(set_piece_events) / NULLIF(SUM(total_events), 0), 2)     AS set_piece_share_pct,

    -- ========= Partition keys =========
    league,
    season

FROM iceberg.silver.whoscored_team_match
GROUP BY team_id, league, season
