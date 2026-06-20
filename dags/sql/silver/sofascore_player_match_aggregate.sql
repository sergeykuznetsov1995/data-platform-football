-- =============================================================================
-- Silver: sofascore_player_match_aggregate
-- =============================================================================
--
-- One row per (match_id, player_id, league, season) — passthrough from
-- `bronze.sofascore_event_player_stats` with cross-source-aligned column
-- aliases (snake_case rename of SofaScore-native camelCase → standardised
-- names matching FBref / WhoScored / Understat).
--
-- Bronze is already match-grain (PK = (match_id, player_id)) — no aggregation
-- needed, just dedup + rename.
--
-- Sources:
--   bronze.sofascore_event_player_stats
--
-- Notes:
--   * Column names in Bronze come from `_camel_to_snake` of the SofaScore
--     `statistics` JSON block — see scrapers/sofascore/scraper.py
--     `_flatten_event_player_stats`. Rename here keeps `match_cards`-style
--     standard names (e.g. SofaScore `key_pass` → `key_passes`).
--   * Season convention: varchar slug ('2526' for 2025/26), passthrough
--     from Bronze (matches xref_player season convention).
--   * Bronze dedup: defensive ROW_NUMBER on (match_id, player_id, league,
--     season) — Bronze SHOULD be 1:1 (replace_partitions=True), realistic
--     duplicate count = 0.
--   * `goal_assist` is the SofaScore camelCase for "primary assist" — rename
--     to `assists` for cross-source alignment.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY match_id, player_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.sofascore_event_player_stats b
        WHERE match_id  IS NOT NULL
          AND player_id IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    -- ========= Identification =========
    match_id,
    player_id,
    CAST(team_id AS varchar)               AS team_id,
    team_name,
    is_home,
    -- is_captain (#439): bronze.captain is varchar 'True'/'False' (from the
    -- /lineups overlay, #301 — confirmed live 760 'True' / 14.4K 'False').
    -- Conform to boolean via CASE, NOT TRY_CAST(boolean) which would silently
    -- NULL the capitalised literals. NULL = lineup overlay missed for this row.
    CASE
        WHEN captain = 'True'  THEN true
        WHEN captain = 'False' THEN false
        ELSE NULL
    END                                    AS is_captain,
    -- is_starter (#693): bronze.substitute is varchar 'True'/'False' from the
    -- same /lineups overlay (#301; scraper.py overlay → real bool stored as
    -- text). A starter == NOT a substitute. Conform via CASE, NOT
    -- TRY_CAST(boolean) which would silently NULL the capitalised literals.
    -- NULL = lineup overlay missed for this row.
    -- Coverage note: bronze.sofascore_event_player_stats only carries players
    -- with a statistics block (starters + subs who came on) — unused bench
    -- players are absent, so SofaScore lineup ⊂ FBref matchsheet. fct_lineup
    -- keeps FBref primary; SofaScore adds rows where FBref is missing (#693).
    CASE
        WHEN substitute = 'False' THEN true
        WHEN substitute = 'True'  THEN false
        ELSE NULL
    END                                    AS is_starter,
    position,
    position_specific,

    -- ========= HARD_FACT (counters, FBref-aligned names) =========
    minutes_played,
    goals,
    goal_assist                            AS assists,
    own_goals,
    total_shots                            AS shots,
    on_target_scoring_attempt              AS shots_on_target,
    blocked_scoring_attempt                AS shots_blocked,
    shot_off_target                        AS shots_off_target,

    -- Cards: SofaScore `event_player_stats` Bronze не отдаёт received_yellow_card /
    -- received_red_card — карточки приходят только через `match_cards` / `event_incidents`
    -- стрим (вне scope этого Silver). Оставляем NULL — downstream Gold берёт fb.yellow/red
    -- из FBref или ws.* из WhoScored.
    CAST(NULL AS DOUBLE)                   AS yellow_cards,
    CAST(NULL AS DOUBLE)                   AS red_cards,

    -- Crosses
    total_cross                            AS crosses,
    accurate_cross                         AS accurate_crosses,

    -- Discipline
    fouls                                  AS fouls_committed,
    was_fouled                             AS fouls_drawn,
    total_offside                          AS offsides,

    -- Defensive
    total_tackle                           AS tackles,
    won_tackle                             AS tackles_won,
    interception_won                       AS interceptions,
    total_clearance                        AS clearances,
    outfielder_block                       AS blocks,
    ball_recovery                          AS ball_recoveries,
    error_lead_to_a_goal                   AS errors_lead_to_goal,
    error_lead_to_a_shot                   AS errors_lead_to_shot,

    -- Passes
    total_pass                             AS passes,
    accurate_pass                          AS passes_completed,
    key_pass                               AS key_passes,
    accurate_long_balls,
    total_long_balls,

    -- Dribbles / take-ons
    total_contest                          AS dribbles_attempted,
    won_contest                            AS dribbles_won,

    -- Duels
    duel_won                               AS total_duels_won,
    duel_lost                              AS total_duels_lost,
    aerial_won                             AS aerial_duels_won,
    aerial_lost                            AS aerial_duels_lost,
    challenge_lost,

    -- Touches / possession
    touches,
    dispossessed,
    possession_lost_ctrl                   AS possession_lost,

    -- Penalties
    penalty_won                            AS penalties_won,
    penalty_conceded                       AS penalties_conceded,
    penalty_miss                           AS penalties_missed,
    -- penalty_goal не emit'ится в `event_player_stats` (приходит через `goal_assist`+penalty incident).
    -- Оставляем NULL; downstream Gold может реализовать derive если потребуется.
    CAST(NULL AS DOUBLE)                   AS penalty_goals,
    penalty_save                           AS penalty_saves,

    -- ========= Ground duels (SofaScore-derived: total - aerial) =========
    -- SofaScore не отдаёт `groundDuels*` напрямую — выводим арифметикой.
    -- NULL-safe: COALESCE(NULL - x, x) корректно даёт NULL когда хоть один
    -- член отсутствует, потому используем явный CASE.
    CASE
        WHEN duel_won IS NOT NULL AND aerial_won IS NOT NULL
            THEN duel_won - aerial_won
        ELSE NULL
    END                                    AS ground_duels_won,

    -- ========= MODELED (xG / xA / rating) =========
    expected_goals                         AS xg,
    expected_assists                       AS xa,
    rating,

    -- ========= Lineage =========
    _ingested_at                           AS _bronze_ingested_at,

    -- ========= Partition keys =========
    league,
    season

FROM bronze_dedup
