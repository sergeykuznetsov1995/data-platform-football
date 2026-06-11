-- =============================================================================
-- Gold: mart_referee_dashboard (E7 BI mart)
-- =============================================================================
-- Per-(referee, season, league) season summary for referee profile dashboard.
-- Denormalized (carries referee_name) for Superset bar/scatter charts without
-- dim_referee JOIN.
--
-- Sources:
--   iceberg.gold.dim_match   -- spine: matches with referee + 1X2 result
--   iceberg.gold.fct_card    -- card counts per match
--   iceberg.gold.fct_goal    -- goal + penalty counts per match
--   iceberg.gold.dim_referee -- referee_name lookup
--
-- referee_id (#425): read directly from dim_match.referee_id — the canonical
-- silver.xref_referee id resolved once at the star centre; identical to the
-- dim_referee PK by construction (no inline hash anymore).
--
-- PK: (referee_id, season, league)
-- =============================================================================

WITH match_with_ref AS (
    SELECT
        m.referee_id,
        m.match_id,
        m.season,
        m.league,
        m.result_1x2,
        m.home_score,
        m.away_score
    FROM iceberg.gold.dim_match m
    WHERE m.referee_id IS NOT NULL
),

card_agg AS (
    SELECT
        match_id_canonical                                        AS match_id,
        SUM(CASE WHEN card_type = 'yellow' THEN 1 ELSE 0 END)     AS yellow_count,
        SUM(CASE WHEN card_type IN ('red', 'second_yellow')
                 THEN 1 ELSE 0 END)                               AS red_count
    FROM iceberg.gold.fct_card
    WHERE match_id_canonical IS NOT NULL
    GROUP BY match_id_canonical
),

goal_agg AS (
    SELECT
        match_id_canonical                                        AS match_id,
        COUNT(*)                                                  AS total_goals,
        SUM(CASE WHEN is_penalty THEN 1 ELSE 0 END)               AS pen_count
    FROM iceberg.gold.fct_goal
    WHERE match_id_canonical IS NOT NULL
    GROUP BY match_id_canonical
),

per_match AS (
    SELECT
        mr.referee_id,
        mr.season,
        mr.league,
        COALESCE(c.yellow_count, 0)                               AS yellow_count,
        COALESCE(c.red_count,    0)                               AS red_count,
        COALESCE(g.total_goals,  0)                               AS total_goals,
        COALESCE(g.pen_count,    0)                               AS pen_count,
        CASE WHEN mr.result_1x2 = 'H' THEN 1.0 ELSE 0.0 END       AS home_win_flag
    FROM match_with_ref mr
    LEFT JOIN card_agg c ON c.match_id = mr.match_id
    LEFT JOIN goal_agg g ON g.match_id = mr.match_id
)

SELECT
    pm.referee_id,
    dr.referee_name,
    pm.season,
    pm.league,
    COUNT(*)                                                       AS matches_officiated,
    AVG(CAST(pm.yellow_count + pm.red_count AS double))            AS cards_per_match,
    AVG(CAST(pm.yellow_count AS double))                           AS yellows_per_match,
    AVG(CAST(pm.red_count    AS double))                           AS reds_per_match,
    AVG(CAST(pm.total_goals  AS double))                           AS goals_per_match,
    AVG(CAST(pm.pen_count    AS double))                           AS penalties_per_match,
    AVG(pm.home_win_flag)                                          AS home_win_pct
FROM per_match pm
LEFT JOIN iceberg.gold.dim_referee dr
       ON dr.referee_id = pm.referee_id
GROUP BY
    pm.referee_id,
    dr.referee_name,
    pm.season,
    pm.league
