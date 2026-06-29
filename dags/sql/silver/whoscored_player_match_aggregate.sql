-- =============================================================================
-- Silver: whoscored_player_match_aggregate
-- =============================================================================
--
-- One row per (match_id, player_id, league, season) — aggregated WhoScored
-- event-level metrics from `bronze.whoscored_events` via `COUNT(*) FILTER`
-- per Opta `type`. Mirror of `silver.whoscored_player_season_aggregate`
-- but at match-grain, so Gold `fct_player_match` can LEFT JOIN.
--
-- Sources:
--   bronze.whoscored_events            (Opta-typed JSON events, ~700K rows /
--                                       APL season)
--
-- Notes:
--   * Bronze IDs are DOUBLE — direct CAST to varchar yields scientific
--     notation '9.5408E4'. CAST through BIGINT first
--     (feedback_bronze_double_id_cast.md). Apply to BOTH player_id AND
--     team_id (sample in silver/whoscored_events_spadl.sql does the same;
--     plan's claim that team_id/game_id are already BIGINT contradicts the
--     spadl reference — using the safer double-CAST).
--   * `game_id → match_id` rename for cross-source alignment.
--   * Card detection: qualifiers JSON-string carries
--     `"displayName":"Yellow"|"Red"|"SecondYellow"` (see match_cards.sql).
--   * Goal detection: a scored goal is a `type='Goal'` row carrying the
--     scoring player. Own-goals are NOT a separate `type='OwnGoal'` row — they
--     arrive as `type='Goal'` with qualifier `"displayName":"OwnGoal"` (#572),
--     and are EXCLUDED from goals/shots/shots_on_target via `is_own_goal` so the
--     deflecting defender is not credited. The own-goal still counts as a touch
--     (it was a real touch). Team-credit lives in Gold `fct_match_timeline`.
--   * Aerial duels: WhoScored emits paired `type='Aerial'` rows
--     (winner + loser); `outcome_type='Successful'` flags the winner.
--   * Discipline (offsides): `type='OffsideGiven'` rows attribute to player.
--   * No xG / xA / rating in WhoScored — those come from Understat /
--     SofaScore at Gold layer via COALESCE.
--   * Per-event Bronze dedup: ROW_NUMBER on full natural key (game_id,
--     period, minute, second, type, player_id, qualifiers) — same as
--     whoscored_events_spadl. Without dedup re-scrapes would double-count.
--   * (league, season) JOIN predicate against xref_player is mandatory
--     for Gold-side bridges; this Silver itself does no xref JOIN — it
--     stays on raw `player_id_raw` and the Gold layer bridges via
--     silver.xref_player.
-- =============================================================================

WITH events_dedup AS (
    SELECT *
    FROM (
        SELECT
            game_id,
            type,
            outcome_type,
            qualifiers,
            team_id,
            player_id,
            league,
            season,
            _ingested_at,
            ROW_NUMBER() OVER (
                PARTITION BY
                    game_id, period, minute, second, type,
                    team_id, player_id, qualifiers
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.whoscored_events
        WHERE player_id IS NOT NULL
    )
    WHERE rn = 1
),

normalised AS (
    SELECT
        -- Bronze IDs are DOUBLE — round through BIGINT to avoid scientific
        -- notation ('9.5408E4'); produces the digit-form ('95408') that
        -- xref_player.source_id stores.
        CAST(CAST(game_id   AS BIGINT) AS varchar)   AS match_id,
        CAST(CAST(player_id AS BIGINT) AS varchar)   AS player_id,
        CAST(CAST(team_id   AS BIGINT) AS varchar)   AS team_id,
        type,
        outcome_type,
        qualifiers,
        -- Own-goals arrive as type='Goal' + qualifier OwnGoal (NOT a distinct
        -- type='OwnGoal' row); flag them so goals/shots exclude the scorer (#572).
        regexp_like(COALESCE(qualifiers, ''),
                    '"displayName"\s*:\s*"OwnGoal"')          AS is_own_goal,
        league,
        season
    FROM events_dedup
)

SELECT
    match_id,
    player_id,
    team_id,

    -- ========= Volume =========
    COUNT(*)                                                              AS total_events,

    -- ========= Goals / shots =========
    -- `type='Goal'` row emitted once per scored goal. Own-goals (type='Goal'
    -- + qualifier OwnGoal) are EXCLUDED via `is_own_goal` so the deflecting
    -- defender is not credited a goal/shot (#572).
    COUNT_IF(type = 'Goal' AND NOT is_own_goal)                           AS goals,
    -- Shots: SavedShot + MissedShots + ShotOnPost + ChanceMissed (all the
    -- "attempt on goal" Opta types) + scored goals, own-goals excluded.
    COUNT_IF(type IN ('SavedShot', 'MissedShots', 'ShotOnPost',
                      'ChanceMissed'))
        + COUNT_IF(type = 'Goal' AND NOT is_own_goal)                     AS shots,
    -- Shots on target = saved + scored goals (own-goals, posts, misses excluded).
    COUNT_IF(type = 'SavedShot')
        + COUNT_IF(type = 'Goal' AND NOT is_own_goal)                     AS shots_on_target,

    -- ========= Passes =========
    COUNT_IF(type IN ('Pass', 'OffsidePass'))                             AS passes,
    COUNT_IF(type IN ('Pass', 'OffsidePass') AND outcome_type = 'Successful') AS passes_completed,
    -- KeyPass qualifier: emitted on Pass rows that led to a shot.
    COUNT_IF(type = 'Pass'
             AND regexp_like(COALESCE(qualifiers, ''),
                             '"displayName"\s*:\s*"KeyPass"'))            AS key_passes,
    COUNT_IF(type = 'Pass'
             AND regexp_like(COALESCE(qualifiers, ''),
                             '"displayName"\s*:\s*"Cross"'))              AS crosses,

    -- ========= Defensive =========
    COUNT_IF(type = 'Tackle')                                             AS tackles,
    COUNT_IF(type = 'Tackle' AND outcome_type = 'Successful')             AS tackles_won,
    COUNT_IF(type = 'Interception')                                       AS interceptions,
    COUNT_IF(type = 'Clearance')                                          AS clearances,
    COUNT_IF(type = 'BallRecovery')                                       AS ball_recoveries,
    COUNT_IF(type = 'BlockedPass')                                        AS blocks,

    -- ========= Aerial duels (paired rows) =========
    COUNT_IF(type = 'Aerial' AND outcome_type = 'Successful')             AS aerials_won,
    COUNT_IF(type = 'Aerial')                                             AS aerial_duels_total,

    -- ========= Take-ons / dribbles =========
    COUNT_IF(type = 'TakeOn')                                             AS dribbles_attempted,
    COUNT_IF(type = 'TakeOn' AND outcome_type = 'Successful')             AS dribbles_won,

    -- ========= Discipline =========
    COUNT_IF(type = 'Foul')                                               AS fouls_committed,
    -- Fouls drawn: Foul rows where outcome flags the OPPOSING player as fouled.
    -- WhoScored attributes the foul to the offender, "fouled" qualifier
    -- present on the same row; without paired-event we can't reliably
    -- separate. Use OffenceFouled flag in qualifiers (Opta convention).
    COUNT_IF(type = 'Foul'
             AND regexp_like(COALESCE(qualifiers, ''),
                             '"displayName"\s*:\s*"OffenceFouled"'))      AS fouls_drawn,
    COUNT_IF(type = 'OffsideGiven')                                       AS offsides,
    COUNT_IF(type = 'Card'
             AND regexp_like(COALESCE(qualifiers, ''),
                             '"displayName"\s*:\s*"Yellow"'))             AS yellow_cards,
    COUNT_IF(type = 'Card'
             AND regexp_like(COALESCE(qualifiers, ''),
                             '"displayName"\s*:\s*"Red"'))                AS red_cards,
    COUNT_IF(type = 'Card'
             AND regexp_like(COALESCE(qualifiers, ''),
                             '"displayName"\s*:\s*"SecondYellow"'))       AS second_yellow_cards,

    -- ========= Touches / possession =========
    -- Touches ≈ всё что игрок коснулся: Pass / TakeOn / Tackle / Interception /
    -- BallRecovery / Clearance / Shot variants / BallTouch / Dispossessed.
    COUNT_IF(type IN ('Pass', 'OffsidePass', 'TakeOn', 'Tackle',
                      'Interception', 'BallRecovery', 'Clearance',
                      'BallTouch', 'Dispossessed', 'BlockedPass',
                      'SavedShot', 'MissedShots', 'ShotOnPost', 'Goal'))  AS touches,
    COUNT_IF(type = 'Dispossessed')                                       AS dispossessed,

    -- ========= Partition keys =========
    league,
    season

FROM normalised
GROUP BY match_id, player_id, team_id, league, season
