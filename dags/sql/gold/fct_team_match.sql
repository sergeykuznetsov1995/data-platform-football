-- =============================================================================
-- Gold: fct_team_match
-- =============================================================================
-- Long-form fact: one row per (match_id, team_id).
--
-- Used by: feat_team_form (rolling), feat_team_h2h, fct_match (wide join).
-- Single source of truth for team-match metrics — no duplication across home/away.
--
-- Sources: iceberg.gold.dim_match + iceberg.silver.fbref_match_enriched
-- PK: (match_id, team_id)
-- Partitioning: (league, season)
-- =============================================================================

WITH home AS (
    SELECT
        dm.match_id,
        dm.date,
        dm.season,
        dm.league,
        dm.gameweek,
        dm.home_team_id    AS team_id,
        dm.away_team_id    AS opponent_id,
        TRUE               AS is_home,
        m.home_score       AS goals_for,
        m.away_score       AS goals_against,
        m.home_shots       AS shots,
        m.home_sot         AS shots_on_target,
        m.home_possession  AS possession,
        m.home_yellow_cards AS yellow_cards,
        m.home_red_cards   AS red_cards,
        m.home_saves       AS saves,
        dm.is_completed
    FROM iceberg.gold.dim_match dm
    JOIN iceberg.silver.fbref_match_enriched m ON m.match_id = dm.match_id
),
away AS (
    SELECT
        dm.match_id,
        dm.date,
        dm.season,
        dm.league,
        dm.gameweek,
        dm.away_team_id,
        dm.home_team_id,
        FALSE,
        m.away_score,
        m.home_score,
        m.away_shots,
        m.away_sot,
        m.away_possession,
        m.away_yellow_cards,
        m.away_red_cards,
        m.away_saves,
        dm.is_completed
    FROM iceberg.gold.dim_match dm
    JOIN iceberg.silver.fbref_match_enriched m ON m.match_id = dm.match_id
),
unioned AS (
    SELECT * FROM home
    UNION ALL
    SELECT * FROM away
)
SELECT
    match_id,
    team_id,
    opponent_id,
    date,
    gameweek,
    is_home,
    goals_for,
    goals_against,
    shots,
    shots_on_target,
    possession,
    yellow_cards,
    red_cards,
    saves,
    CASE
        WHEN goals_for > goals_against THEN 3
        WHEN goals_for = goals_against THEN 1
        ELSE 0
    END AS points,
    CASE
        WHEN goals_for > goals_against THEN 'W'
        WHEN goals_for = goals_against THEN 'D'
        ELSE 'L'
    END AS result,
    is_completed,
    league,
    season
FROM unioned
WHERE match_id IS NOT NULL
  AND team_id  IS NOT NULL
