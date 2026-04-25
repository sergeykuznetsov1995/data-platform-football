-- =============================================================================
-- Gold: feat_team_form
-- =============================================================================
-- Rolling team features (last 5 completed matches) computed PRE-match.
-- Window: ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING — excludes current match
-- to avoid data leakage.
--
-- Guarantee: first 5 rows per (team_id, season) have NULL in *_avg, *_std and
-- *_trend columns. This is enforced by the window definition — aggregations
-- over <5 rows in PRECEDING+CURRENT range return NULL for unbounded-frame
-- aggregates, and for bounded frames they aggregate what's available; we
-- additionally mask with a CASE (match_rn > 5) to guarantee NULL behaviour
-- across all rolling columns.
--
-- Volatility & trend (added T3.3):
--   *_std        — STDDEV_SAMP over the same L5 window (sample std, n-1).
--                  Captures dispersion / consistency of team form.
--   l5_form_trend — REGR_SLOPE(points, match_seq) over L5 window.
--                  Positive slope -> team trending up; negative -> declining.
--                  match_seq is a per-team monotonically increasing sequence
--                  (ROW_NUMBER), so slope units are "points per match".
--
-- Sources: iceberg.gold.fct_team_match
-- PK: (match_id, team_id)
-- Partitioning: (league, season)
-- =============================================================================

WITH base AS (
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
        points,
        result,
        league,
        season,
        ROW_NUMBER() OVER (
            PARTITION BY team_id, season
            ORDER BY date, match_id
        ) AS match_rn,
        -- Independent variable for REGR_SLOPE: monotonic per (team_id, season).
        -- Equivalent to match_rn but kept as a separate column for clarity —
        -- units of slope become "points per match".
        CAST(
            ROW_NUMBER() OVER (
                PARTITION BY team_id, season
                ORDER BY date, match_id
            ) AS DOUBLE
        ) AS match_seq
    FROM iceberg.gold.fct_team_match
),
rolled AS (
    SELECT
        *,
        -- Existing averages
        AVG(CAST(goals_for     AS DOUBLE)) OVER w AS l5_goals_for_avg_raw,
        AVG(CAST(goals_against AS DOUBLE)) OVER w AS l5_goals_against_avg_raw,
        AVG(CAST(shots         AS DOUBLE)) OVER w AS l5_shots_avg_raw,
        AVG(CAST(shots_on_target AS DOUBLE)) OVER w AS l5_sot_avg_raw,
        AVG(CAST(possession    AS DOUBLE)) OVER w AS l5_possession_avg_raw,
        SUM(points)    OVER w                    AS l5_form_points_raw,
        SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) OVER w AS l5_wins_raw,
        SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) OVER w AS l5_losses_raw,
        SUM(CASE WHEN result='D' THEN 1 ELSE 0 END) OVER w AS l5_draws_raw,
        -- T3.3: volatility (sample std-dev over the same L5 window)
        STDDEV_SAMP(CAST(goals_for     AS DOUBLE)) OVER w AS l5_goals_for_std_raw,
        STDDEV_SAMP(CAST(goals_against AS DOUBLE)) OVER w AS l5_goals_against_std_raw,
        STDDEV_SAMP(CAST(points        AS DOUBLE)) OVER w AS l5_points_std_raw,
        -- T3.3: form trend (linear regression slope of points over match_seq)
        REGR_SLOPE(CAST(points AS DOUBLE), match_seq) OVER w AS l5_form_trend_raw,
        LAG(date, 1) OVER (
            PARTITION BY team_id, season
            ORDER BY date, match_id
        ) AS prev_match_date
    FROM base
    WINDOW w AS (
        PARTITION BY team_id, season
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
    is_home,
    -- Mask to NULL for rows with < 5 prior matches (strict point-in-time).
    CASE WHEN match_rn > 5 THEN l5_goals_for_avg_raw     END AS l5_goals_for_avg,
    CASE WHEN match_rn > 5 THEN l5_goals_against_avg_raw END AS l5_goals_against_avg,
    CASE WHEN match_rn > 5 THEN l5_shots_avg_raw         END AS l5_shots_avg,
    CASE WHEN match_rn > 5 THEN l5_sot_avg_raw           END AS l5_sot_avg,
    CASE WHEN match_rn > 5 THEN l5_possession_avg_raw    END AS l5_possession_avg,
    CASE WHEN match_rn > 5 THEN l5_form_points_raw       END AS l5_form_points,
    CASE WHEN match_rn > 5 THEN l5_wins_raw              END AS l5_wins,
    CASE WHEN match_rn > 5 THEN l5_losses_raw            END AS l5_losses,
    CASE WHEN match_rn > 5 THEN l5_draws_raw             END AS l5_draws,
    -- T3.3: volatility (NULL for first 5 rows per partition)
    CASE WHEN match_rn > 5 THEN l5_goals_for_std_raw     END AS l5_goals_for_std,
    CASE WHEN match_rn > 5 THEN l5_goals_against_std_raw END AS l5_goals_against_std,
    CASE WHEN match_rn > 5 THEN l5_points_std_raw        END AS l5_points_std,
    -- T3.3: trend (NULL for first 5 rows per partition)
    CASE WHEN match_rn > 5 THEN l5_form_trend_raw        END AS l5_form_trend,
    match_rn                                                    AS matches_played_so_far,
    DATE_DIFF('day', prev_match_date, date)                     AS rest_days,
    league,
    season
FROM rolled
