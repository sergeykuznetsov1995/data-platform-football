-- =============================================================================
-- Silver: whoscored_player_season_aggregate
-- =============================================================================
--
-- One row per (canonical_id, league, season) — aggregated WhoScored event-level
-- metrics rolled up from `silver.whoscored_events_spadl` (SPADL-normalised).
--
-- Joins through `silver.xref_player` (source='whoscored') so the canonical_id
-- column matches the rest of the medallion (FBref-rooted 'fb_*' or orphan
-- 'ws_*' prefixes). Players in events without a resolved xref row are
-- excluded — they do not show up in any cross-source view anyway.
--
-- Sources:
--   silver.whoscored_events_spadl  (events, 21 cols)
--   silver.xref_player              (canonical_id ↔ raw player_id bridge)
--
-- Notes:
--   * shots_total / shots_on_target_proxy count all shot variants ('shot' +
--     'shot_penalty' + 'shot_freekick'), incl. goals (Goal→shot family, #462).
--     shots_on_target_proxy stays a coarse proxy: WhoScored marks MissedShots
--     outcome_type='Successful', so it over-counts off-target attempts —
--     match-level fct_shot is the source of truth for goals / true on-target.
--   * Spatial avg_x/avg_y is computed only on on-ball offensive actions
--     (pass / take_on / shot / dribble) so defensive recoveries don't
--     pull a winger's average back to his own half.
--   * Touches-in-box uses the SPADL coordinate convention (x ∈ [0,100],
--     attacking direction): opposition box ≈ x >= 83 AND y ∈ [21,79].
--   * (league, season) JOIN predicate against xref_player is mandatory
--     (see CLAUDE.md / feedback_xref_join_season_predicate.md) — without it
--     a player active across multiple seasons fan-outs 1.5–4×.
-- =============================================================================

WITH events AS (
    SELECT
        match_id,
        player_id_raw,
        action_canonical,
        outcome_success,
        x,
        y,
        league,
        season
    FROM iceberg.silver.whoscored_events_spadl
    WHERE player_id_raw IS NOT NULL
),

xp AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_player
    WHERE source = 'whoscored'
),

joined AS (
    SELECT
        xp.canonical_id,
        e.player_id_raw,
        e.league,
        e.season,
        e.match_id,
        e.action_canonical,
        e.outcome_success,
        e.x,
        e.y
    FROM events e
    JOIN xp
      ON xp.source_id = e.player_id_raw
     AND xp.league    = e.league
     AND xp.season    = e.season
)

SELECT
    canonical_id,
    player_id_raw,

    -- ========= Volume =========
    COUNT(DISTINCT match_id)                                                AS matches_seen,
    COUNT(*)                                                                 AS total_events,

    -- ========= Possession / on-ball =========
    COUNT_IF(action_canonical = 'pass')                                      AS pass_total,
    COUNT_IF(action_canonical = 'pass' AND outcome_success)                  AS pass_ok,
    ROUND(100.0 * COUNT_IF(action_canonical = 'pass' AND outcome_success)
          / NULLIF(COUNT_IF(action_canonical = 'pass'), 0), 2)               AS pass_pct,
    COUNT_IF(action_canonical = 'take_on')                                   AS takeon_att,
    COUNT_IF(action_canonical = 'take_on' AND outcome_success)               AS takeon_won,
    ROUND(100.0 * COUNT_IF(action_canonical = 'take_on' AND outcome_success)
          / NULLIF(COUNT_IF(action_canonical = 'take_on'), 0), 2)            AS takeon_pct,
    COUNT_IF(action_canonical = 'dribble')                                   AS dribbles,
    COUNT_IF(action_canonical = 'bad_touch')                                 AS bad_touches,

    -- ========= Shooting (proxy — see header) =========
    -- All shot variants ('shot' + 'shot_penalty' + 'shot_freekick'), incl.
    -- goals now routed into the shot family (#462).
    COUNT_IF(action_canonical IN ('shot', 'shot_penalty', 'shot_freekick'))
        AS shots_total,
    COUNT_IF(action_canonical IN ('shot', 'shot_penalty', 'shot_freekick')
             AND outcome_success)                                            AS shots_on_target_proxy,

    -- ========= Defensive =========
    COUNT_IF(action_canonical = 'tackle')                                    AS tackle_att,
    COUNT_IF(action_canonical = 'tackle' AND outcome_success)                AS tackle_won,
    ROUND(100.0 * COUNT_IF(action_canonical = 'tackle' AND outcome_success)
          / NULLIF(COUNT_IF(action_canonical = 'tackle'), 0), 2)             AS tackle_pct,
    COUNT_IF(action_canonical = 'interception')                              AS interceptions,
    COUNT_IF(action_canonical = 'ball_recovery')                             AS ball_recoveries,
    COUNT_IF(action_canonical = 'clearance')                                 AS clearances,

    -- ========= Discipline =========
    COUNT_IF(action_canonical = 'foul')                                      AS fouls_committed,

    -- ========= Goalkeeper (mostly NULL for outfield players) =========
    COUNT_IF(action_canonical = 'keeper_save')                               AS keeper_saves,
    COUNT_IF(action_canonical = 'keeper_pick_up')                            AS keeper_pickups,
    COUNT_IF(action_canonical = 'keeper_claim')                              AS keeper_claims,

    -- ========= Spatial profile (on-ball offensive actions only) =========
    ROUND(AVG(CASE WHEN action_canonical IN ('pass','take_on','shot','dribble')
                   THEN x END), 2)                                           AS avg_x,
    ROUND(AVG(CASE WHEN action_canonical IN ('pass','take_on','shot','dribble')
                   THEN y END), 2)                                           AS avg_y,
    COUNT_IF(x >= 83 AND y BETWEEN 21 AND 79
             AND action_canonical IN ('pass','take_on','shot','dribble','bad_touch')) AS touches_in_box,

    -- ========= Partition keys =========
    league,
    season

FROM joined
GROUP BY canonical_id, player_id_raw, league, season
