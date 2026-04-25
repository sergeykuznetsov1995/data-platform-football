-- =============================================================================
-- Gold: feat_team_h2h
-- =============================================================================
-- Head-to-head rolling features between two teams (last 5 encounters).
-- Same point-in-time safety as feat_team_form: excludes current match.
--
-- Sources: iceberg.gold.fct_team_match
-- PK: (match_id, team_id, opponent_id)
-- Partitioning: (league, season)
-- =============================================================================

WITH pairs AS (
    -- Canonical ordering of the pair so H2H is symmetric; then we project
    -- features back onto the non-canonical side via is_home flag.
    SELECT
        match_id,
        team_id,
        opponent_id,
        date,
        gameweek,
        goals_for,
        goals_against,
        points,
        result,
        league,
        season
    FROM iceberg.gold.fct_team_match
),
rolled AS (
    SELECT
        *,
        AVG(CAST(goals_for - goals_against AS DOUBLE)) OVER w AS h2h_goals_diff_avg_raw,
        AVG(CAST(goals_for     AS DOUBLE))             OVER w AS h2h_goals_for_avg_raw,
        AVG(CAST(goals_against AS DOUBLE))             OVER w AS h2h_goals_against_avg_raw,
        SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) OVER w AS h2h_wins_raw,
        SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) OVER w AS h2h_losses_raw,
        SUM(CASE WHEN result='D' THEN 1 ELSE 0 END) OVER w AS h2h_draws_raw,
        ROW_NUMBER() OVER (
            PARTITION BY team_id, opponent_id
            ORDER BY date, match_id
        ) AS h2h_rn
    FROM pairs
    WINDOW w AS (
        PARTITION BY team_id, opponent_id
        ORDER BY date, match_id
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    )
)
SELECT
    match_id,
    team_id,
    opponent_id,
    date,
    gameweek,
    CASE WHEN h2h_rn > 1 THEN h2h_goals_diff_avg_raw     END AS h2h_goals_diff_avg,
    CASE WHEN h2h_rn > 1 THEN h2h_goals_for_avg_raw      END AS h2h_goals_for_avg,
    CASE WHEN h2h_rn > 1 THEN h2h_goals_against_avg_raw  END AS h2h_goals_against_avg,
    CASE WHEN h2h_rn > 1 THEN h2h_wins_raw               END AS h2h_wins,
    CASE WHEN h2h_rn > 1 THEN h2h_losses_raw             END AS h2h_losses,
    CASE WHEN h2h_rn > 1 THEN h2h_draws_raw              END AS h2h_draws,
    h2h_rn - 1                                               AS h2h_matches_prior,
    league,
    season
FROM rolled
