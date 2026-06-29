-- =============================================================================
-- Silver: sofascore_team_match
-- =============================================================================
--
-- One row per (match_id, team_id, league, season) — flattened SofaScore
-- team-level match statistics. Two rows per match (home side + away side).
--
-- Sources:
--   bronze.sofascore_match_stats          — long-form team stats (one row per
--                                            (match, period, stat_group, stat_name)).
--                                            We PIVOT period='ALL' to wide form.
--   bronze.sofascore_schedule             — for canonical (home_team, away_team,
--                                            home_score, away_score).
--
-- Pure single-source conform: PIVOT match_stats (period='ALL') joined to the
-- schedule outcome. Reads only bronze.sofascore_* — no silver.* reads (R2).
--
-- Why two passes:
--   * `bronze.sofascore_match_stats` is *team-level* but lacks the home/away
--      team_id columns (rows carry only `home_value`/`away_value` pairs).
--   * Goals come from the schedule; the remaining counters come from the
--      `match_stats` PIVOT.
--
-- stat_key mapping (discovered 2026-05-27 via Trino DISTINCT):
--   Match overview      | Total shots         | totalShotsOnGoal       (count)
--   Match overview      | Ball possession     | ballPossession         (% in home_value)
--   Match overview      | Expected goals      | expectedGoals          (xG double)
--   Match overview      | Yellow cards        | yellowCards            (count)
--   Match overview      | Red cards           | redCards               (count)
--   Match overview      | Fouls               | fouls                  (count)
--   Match overview      | Corner kicks        | cornerKicks            (count)
--   Match overview      | Passes              | passes                 (count)
--   Attack              | Offsides            | offsides               (count)
--   Shots               | Shots on target     | shotsOnGoal            (count)
--   Passes              | Accurate passes     | accuratePasses         (count)
--   Passes              | Long balls          | accurateLongBalls      (count; text="won/total (pct%)")
--   Passes              | Crosses             | accurateCross          (count; text="won/total (pct%)")
--   Defending           | Interceptions       | interceptionWon        (count)
--   Defending           | Tackles won         | wonTacklePercent       (count; text=pct only)
--   Defending           | Total tackles       | totalTackle            (count)
--   Duels               | Ground duels        | groundDuelsPercentage  (count won; text="won/total (pct%)")
--   Duels               | Aerial duels        | aerialDuelsPercentage  (count won; text="won/total (pct%)")
--
-- Notes:
--   * Season is varchar slug ('2526' for 2025/26), matches xref_team season convention.
--   * `bronze.sofascore_schedule.game_id` is BIGINT — CAST to varchar to match
--     `bronze.sofascore_match_stats.match_id` (varchar).
--   * Ground/aerial duels totals are parsed from `home_text`/`away_text`
--     ("X/Y (Z%)") via regexp_extract — `home_value` alone gives only count won.
--   * `accurate_passes_pct` is derived (no native stat_key) — `100.0 * accurate / total`.
--   * `minutes`/`assists` are NULL placeholders: SofaScore match_stats has no
--     team-grain minutes/assists. A prior cross-entity rollup from
--     silver.sofascore_player_match_aggregate never matched on team_id (NAME vs
--     numeric id) and was removed per Silver Charter R2 (#367); the columns are
--     kept NULL so the downstream schema/contract is unchanged.
--   * Bronze dedup: ROW_NUMBER on (match_id, period, stat_key); defensive
--     against re-ingests (replace_partitions=True should keep this 1:1).
-- =============================================================================

WITH stats_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY match_id, period, stat_key
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.sofascore_match_stats b
        WHERE match_id IS NOT NULL
          AND period = 'ALL'
    )
    WHERE rn = 1
),

stats_pivot AS (
    SELECT
        match_id,
        league,
        season,
        MAX(_ingested_at) AS _bronze_ingested_at,

        -- ===== Shots =====
        MAX(IF(stat_key = 'totalShotsOnGoal', home_value)) AS home_total_shots,
        MAX(IF(stat_key = 'totalShotsOnGoal', away_value)) AS away_total_shots,
        MAX(IF(stat_key = 'shotsOnGoal',      home_value)) AS home_shots_on_target,
        MAX(IF(stat_key = 'shotsOnGoal',      away_value)) AS away_shots_on_target,

        -- ===== Discipline =====
        MAX(IF(stat_key = 'yellowCards', home_value)) AS home_yellow_cards,
        MAX(IF(stat_key = 'yellowCards', away_value)) AS away_yellow_cards,
        MAX(IF(stat_key = 'redCards',    home_value)) AS home_red_cards,
        MAX(IF(stat_key = 'redCards',    away_value)) AS away_red_cards,
        MAX(IF(stat_key = 'fouls',       home_value)) AS home_fouls,
        MAX(IF(stat_key = 'fouls',       away_value)) AS away_fouls,
        MAX(IF(stat_key = 'offsides',    home_value)) AS home_offsides,
        MAX(IF(stat_key = 'offsides',    away_value)) AS away_offsides,
        MAX(IF(stat_key = 'cornerKicks', home_value)) AS home_corner_kicks,
        MAX(IF(stat_key = 'cornerKicks', away_value)) AS away_corner_kicks,

        -- ===== Defending =====
        MAX(IF(stat_key = 'interceptionWon',  home_value)) AS home_interceptions,
        MAX(IF(stat_key = 'interceptionWon',  away_value)) AS away_interceptions,
        MAX(IF(stat_key = 'wonTacklePercent', home_value)) AS home_tackles_won,
        MAX(IF(stat_key = 'wonTacklePercent', away_value)) AS away_tackles_won,
        MAX(IF(stat_key = 'totalTackle',      home_value)) AS home_total_tackles,
        MAX(IF(stat_key = 'totalTackle',      away_value)) AS away_total_tackles,

        -- ===== Passing =====
        MAX(IF(stat_key = 'passes',         home_value)) AS home_total_passes,
        MAX(IF(stat_key = 'passes',         away_value)) AS away_total_passes,
        MAX(IF(stat_key = 'accuratePasses', home_value)) AS home_accurate_passes,
        MAX(IF(stat_key = 'accuratePasses', away_value)) AS away_accurate_passes,

        -- ===== Possession =====
        MAX(IF(stat_key = 'ballPossession', home_value)) AS home_possession_pct,
        MAX(IF(stat_key = 'ballPossession', away_value)) AS away_possession_pct,

        -- ===== Modeled (xG) =====
        MAX(IF(stat_key = 'expectedGoals', home_value)) AS home_xg,
        MAX(IF(stat_key = 'expectedGoals', away_value)) AS away_xg,

        -- ===== Duels (home_value = wins; total parsed from text "won/total (pct%)") =====
        MAX(IF(stat_key = 'groundDuelsPercentage', home_value)) AS home_ground_duels_won,
        MAX(IF(stat_key = 'groundDuelsPercentage', away_value)) AS away_ground_duels_won,
        MAX(IF(stat_key = 'groundDuelsPercentage',
               TRY_CAST(regexp_extract(home_text, '/(\d+)', 1) AS INTEGER))) AS home_ground_duels_total,
        MAX(IF(stat_key = 'groundDuelsPercentage',
               TRY_CAST(regexp_extract(away_text, '/(\d+)', 1) AS INTEGER))) AS away_ground_duels_total,
        MAX(IF(stat_key = 'aerialDuelsPercentage', home_value)) AS home_aerial_duels_won,
        MAX(IF(stat_key = 'aerialDuelsPercentage', away_value)) AS away_aerial_duels_won,
        MAX(IF(stat_key = 'aerialDuelsPercentage',
               TRY_CAST(regexp_extract(home_text, '/(\d+)', 1) AS INTEGER))) AS home_aerial_duels_total,
        MAX(IF(stat_key = 'aerialDuelsPercentage',
               TRY_CAST(regexp_extract(away_text, '/(\d+)', 1) AS INTEGER))) AS away_aerial_duels_total,

        -- ===== Long balls / Crosses (count + total parsed from text) =====
        MAX(IF(stat_key = 'accurateLongBalls', home_value)) AS home_accurate_long_balls,
        MAX(IF(stat_key = 'accurateLongBalls', away_value)) AS away_accurate_long_balls,
        MAX(IF(stat_key = 'accurateLongBalls',
               TRY_CAST(regexp_extract(home_text, '/(\d+)', 1) AS INTEGER))) AS home_total_long_balls,
        MAX(IF(stat_key = 'accurateLongBalls',
               TRY_CAST(regexp_extract(away_text, '/(\d+)', 1) AS INTEGER))) AS away_total_long_balls,
        MAX(IF(stat_key = 'accurateCross', home_value)) AS home_accurate_crosses,
        MAX(IF(stat_key = 'accurateCross', away_value)) AS away_accurate_crosses,
        MAX(IF(stat_key = 'accurateCross',
               TRY_CAST(regexp_extract(home_text, '/(\d+)', 1) AS INTEGER))) AS home_total_crosses,
        MAX(IF(stat_key = 'accurateCross',
               TRY_CAST(regexp_extract(away_text, '/(\d+)', 1) AS INTEGER))) AS away_total_crosses

    FROM stats_dedup
    GROUP BY match_id, league, season
),

schedule_dim AS (
    -- Dedup by game_id (latest snapshot) instead of SELECT DISTINCT: two
    -- snapshots of one game_id with different scores (live 0:0 vs final 2:1)
    -- both survive DISTINCT → ×2 fan-out in home_side/away_side. Take the
    -- freshest by _ingested_at (= final score); _batch_id breaks ties (#464).
    SELECT
        CAST(game_id AS varchar)   AS match_id,
        CAST(home_team AS varchar) AS home_team_id,
        CAST(away_team AS varchar) AS away_team_id,
        CAST(home_score AS INTEGER) AS home_score,
        CAST(away_score AS INTEGER) AS away_score,
        league,
        season
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY game_id
                   ORDER BY _ingested_at DESC, _batch_id DESC
               ) AS rn
        FROM iceberg.bronze.sofascore_schedule
        WHERE home_score IS NOT NULL
          AND away_score IS NOT NULL
          AND game_id IS NOT NULL
    )
    WHERE rn = 1
),

home_side AS (
    SELECT
        sp.match_id,
        sd.home_team_id            AS team_id,
        sd.away_team_id            AS opponent_id,
        TRUE                       AS is_home,
        sd.home_score              AS goals_for,
        sd.away_score              AS goals_against,
        sp.home_total_shots        AS total_shots,
        sp.home_shots_on_target    AS shots_on_target,
        sp.home_yellow_cards       AS yellow_cards,
        sp.home_red_cards          AS red_cards,
        sp.home_fouls              AS fouls,
        sp.home_offsides           AS offsides,
        sp.home_corner_kicks       AS corner_kicks,
        sp.home_interceptions      AS interceptions,
        sp.home_tackles_won        AS tackles_won,
        sp.home_total_tackles      AS total_tackles,
        sp.home_total_passes       AS total_passes,
        sp.home_accurate_passes    AS accurate_passes,
        sp.home_possession_pct     AS possession_pct,
        sp.home_xg                 AS expected_goals,
        sp.away_xg                 AS expected_goals_against,
        sp.home_ground_duels_won   AS ground_duels_won,
        sp.home_ground_duels_total AS ground_duels_total,
        sp.home_aerial_duels_won   AS aerial_duels_won,
        sp.home_aerial_duels_total AS aerial_duels_total,
        sp.home_accurate_long_balls AS accurate_long_balls,
        sp.home_total_long_balls   AS total_long_balls,
        sp.home_accurate_crosses   AS accurate_crosses,
        sp.home_total_crosses      AS total_crosses,
        sp._bronze_ingested_at,
        sp.league,
        sp.season
    FROM stats_pivot sp
    JOIN schedule_dim sd
      ON sd.match_id = sp.match_id
     AND sd.league   = sp.league
     AND sd.season   = sp.season
),

away_side AS (
    SELECT
        sp.match_id,
        sd.away_team_id            AS team_id,
        sd.home_team_id            AS opponent_id,
        FALSE                      AS is_home,
        sd.away_score              AS goals_for,
        sd.home_score              AS goals_against,
        sp.away_total_shots        AS total_shots,
        sp.away_shots_on_target    AS shots_on_target,
        sp.away_yellow_cards       AS yellow_cards,
        sp.away_red_cards          AS red_cards,
        sp.away_fouls              AS fouls,
        sp.away_offsides           AS offsides,
        sp.away_corner_kicks       AS corner_kicks,
        sp.away_interceptions      AS interceptions,
        sp.away_tackles_won        AS tackles_won,
        sp.away_total_tackles      AS total_tackles,
        sp.away_total_passes       AS total_passes,
        sp.away_accurate_passes    AS accurate_passes,
        sp.away_possession_pct     AS possession_pct,
        sp.away_xg                 AS expected_goals,
        sp.home_xg                 AS expected_goals_against,
        sp.away_ground_duels_won   AS ground_duels_won,
        sp.away_ground_duels_total AS ground_duels_total,
        sp.away_aerial_duels_won   AS aerial_duels_won,
        sp.away_aerial_duels_total AS aerial_duels_total,
        sp.away_accurate_long_balls AS accurate_long_balls,
        sp.away_total_long_balls   AS total_long_balls,
        sp.away_accurate_crosses   AS accurate_crosses,
        sp.away_total_crosses      AS total_crosses,
        sp._bronze_ingested_at,
        sp.league,
        sp.season
    FROM stats_pivot sp
    JOIN schedule_dim sd
      ON sd.match_id = sp.match_id
     AND sd.league   = sp.league
     AND sd.season   = sp.season
),

unioned AS (
    SELECT * FROM home_side
    UNION ALL
    SELECT * FROM away_side
)

SELECT
    -- ===== Identity =====
    match_id,
    team_id,
    opponent_id,
    is_home,

    -- ===== Outcome (from schedule) =====
    goals_for,
    goals_against,

    -- ===== HARD_FACT (counters from match_stats) =====
    CAST(total_shots      AS INTEGER) AS total_shots,
    CAST(shots_on_target  AS INTEGER) AS shots_on_target,
    CAST(yellow_cards     AS INTEGER) AS yellow_cards,
    CAST(red_cards        AS INTEGER) AS red_cards,
    CAST(fouls            AS INTEGER) AS fouls,
    CAST(offsides         AS INTEGER) AS offsides,
    CAST(corner_kicks     AS INTEGER) AS corner_kicks,
    CAST(interceptions    AS INTEGER) AS interceptions,
    CAST(tackles_won      AS INTEGER) AS tackles_won,
    CAST(total_tackles    AS INTEGER) AS total_tackles,
    CAST(total_passes     AS INTEGER) AS total_passes,
    CAST(accurate_passes  AS INTEGER) AS accurate_passes,
    CASE
        WHEN total_passes > 0
            THEN ROUND(100.0 * accurate_passes / total_passes, 2)
        ELSE NULL
    END                              AS accurate_passes_pct,
    possession_pct,

    -- ===== HARD_FACT (minutes/assists — not in SofaScore match_stats; NULL) =====
    -- Kept as NULL placeholders (#367): the team-grain minutes/assists rollup from
    -- silver.sofascore_player_match_aggregate was a Silver Charter R2 violation and
    -- never matched on team_id anyway. Types match the prior schema (double/integer).
    CAST(NULL AS DOUBLE)  AS minutes,
    CAST(NULL AS INTEGER) AS assists,

    -- ===== MODELED (xG / xA — xA not provided by SofaScore match_stats) =====
    expected_goals,
    expected_goals_against,

    -- ===== UNIQUE_SOFASCORE — duels =====
    CAST(ground_duels_won   AS INTEGER) AS ground_duels_won,
    ground_duels_total,
    CASE
        WHEN ground_duels_total > 0
            THEN ROUND(100.0 * ground_duels_won / ground_duels_total, 2)
        ELSE NULL
    END                                AS ground_duels_won_pct,
    CAST(aerial_duels_won   AS INTEGER) AS aerial_duels_won,
    aerial_duels_total,
    CASE
        WHEN aerial_duels_total > 0
            THEN ROUND(100.0 * aerial_duels_won / aerial_duels_total, 2)
        ELSE NULL
    END                                AS aerial_duels_won_pct,

    -- ===== UNIQUE_SOFASCORE — long balls & crosses =====
    CAST(accurate_long_balls AS INTEGER) AS accurate_long_balls,
    total_long_balls,
    CASE
        WHEN total_long_balls > 0
            THEN ROUND(100.0 * accurate_long_balls / total_long_balls, 2)
        ELSE NULL
    END                                  AS accurate_long_balls_pct,
    CAST(accurate_crosses AS INTEGER) AS accurate_crosses,
    total_crosses,

    -- ===== Lineage =====
    _bronze_ingested_at,

    -- ===== Partition keys =====
    league,
    season

FROM unioned
WHERE team_id IS NOT NULL
