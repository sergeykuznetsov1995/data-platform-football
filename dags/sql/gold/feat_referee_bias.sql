-- =============================================================================
-- Gold: feat_referee_bias
-- =============================================================================
-- Rolling referee-bias features computed PRE-match. One row per
-- (referee_id, match_id). Window: ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
-- per (referee_id, season) — excludes current match to avoid data leakage.
-- First 5 rows per partition are masked to NULL via match_rn>5 to suppress
-- noise from cold-start referees in early matchdays.
--
-- Sources:
--   iceberg.gold.dim_match           (referee_id, match_date, season, league, result)
--   iceberg.gold.fct_card            (yellow/red counts per match)
--   iceberg.gold.fct_goal            (penalty + total goal counts per match)
--
-- referee_id (#425): read directly from dim_match.referee_id — the canonical
-- silver.xref_referee id resolved once at the star centre. The legacy inline
-- name-hash is gone; ids are consistent with dim_referee by construction.
--
-- DAG-integration note: gold_tasks.run_gold_transform() wraps this SELECT in
-- `CREATE TABLE iceberg.gold.feat_referee_bias AS ... WITH
-- (partitioning=ARRAY['season'])`. Single-key partition because referees
-- work cross-league (a Premier-League ref can be appointed to a UCL group
-- stage, etc.) — partitioning by league would skew partition counts.
--
-- PK: (referee_id, match_id)
-- Partitioning: (season)
-- =============================================================================

WITH match_with_ref AS (
    -- Spine: matches with a resolved canonical referee (#425).
    SELECT
        m.referee_id,
        m.match_id,
        m.match_date AS date,
        m.season,
        m.league,
        m.result_1x2,
        m.home_score,
        m.away_score
    FROM iceberg.gold.dim_match m
    WHERE m.referee_id IS NOT NULL
),

card_agg AS (
    -- Per-match card counts. card_type enum: 'yellow'|'red'|'second_yellow'.
    -- 'second_yellow' is a yellow that BECOMES a red — counted as red here
    -- (industry-standard tally; FBref aggregates the same way on referee
    -- profile pages).
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
    -- Per-match goal + penalty counts. is_penalty already only flips on the
    -- regular-goal branch (own_goals are always is_penalty=FALSE).
    SELECT
        match_id_canonical                                        AS match_id,
        COUNT(*)                                                  AS total_goals,
        SUM(CASE WHEN is_penalty THEN 1 ELSE 0 END)               AS pen_count
    FROM iceberg.gold.fct_goal
    WHERE match_id_canonical IS NOT NULL
    GROUP BY match_id_canonical
),

spine AS (
    -- Per (referee_id, match_id) row with all aggregates pre-collapsed.
    -- COALESCE on counts so window AVG never sees NULL gaps (a match with
    -- zero cards / zero goals semantically scores 0, not "missing data").
    -- home_win_flag is computed from dim_match.result_1x2 directly.
    SELECT
        mr.referee_id,
        mr.match_id,
        mr.date,
        mr.season,
        mr.league,
        COALESCE(c.yellow_count, 0)                                AS yellow_count,
        COALESCE(c.red_count,    0)                                AS red_count,
        COALESCE(g.total_goals,  0)                                AS total_goals,
        COALESCE(g.pen_count,    0)                                AS pen_count,
        CASE WHEN mr.result_1x2 = 'H' THEN 1.0 ELSE 0.0 END        AS home_win_flag,
        ROW_NUMBER() OVER (
            PARTITION BY mr.referee_id, mr.season
            ORDER BY mr.date, mr.match_id
        )                                                          AS match_rn
    FROM match_with_ref mr
    LEFT JOIN card_agg c ON c.match_id = mr.match_id
    LEFT JOIN goal_agg g ON g.match_id = mr.match_id
),

rolled AS (
    SELECT
        referee_id,
        match_id,
        date,
        season,
        league,
        match_rn,
        AVG(CAST(yellow_count  AS DOUBLE)) OVER w10                AS yellow_avg_raw,
        AVG(CAST(red_count     AS DOUBLE)) OVER w10                AS red_avg_raw,
        AVG(CAST(yellow_count + red_count AS DOUBLE)) OVER w10     AS cards_avg_raw,
        AVG(CAST(total_goals   AS DOUBLE)) OVER w10                AS goals_avg_raw,
        AVG(home_win_flag)                  OVER w10               AS home_win_rate_raw,
        AVG(CAST(pen_count     AS DOUBLE)) OVER w10                AS pen_avg_raw
    FROM spine
    WINDOW w10 AS (
        PARTITION BY referee_id, season
        ORDER BY date, match_id
        ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    )
)

SELECT
    referee_id,
    match_id,
    date,
    league,
    -- Mask first 5 rows per (referee_id, season) — strict point-in-time:
    -- a rolling window built on <5 prior matches is too noisy to feed ML.
    CASE WHEN match_rn > 5 THEN yellow_avg_raw    END                AS ref_yellow_per_match_l10,
    CASE WHEN match_rn > 5 THEN red_avg_raw       END                AS ref_red_per_match_l10,
    CASE WHEN match_rn > 5 THEN cards_avg_raw     END                AS ref_cards_per_match_l10,
    CASE WHEN match_rn > 5 THEN goals_avg_raw     END                AS ref_goals_per_match_l10,
    CASE WHEN match_rn > 5 THEN home_win_rate_raw END                AS ref_home_win_rate_l10,
    CASE WHEN match_rn > 5 THEN pen_avg_raw       END                AS ref_pen_per_match_l10,
    season
FROM rolled
