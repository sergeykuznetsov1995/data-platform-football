-- =============================================================================
-- Gold: mart_referee_dashboard — EMPTY FALLBACK
-- =============================================================================
-- Schema MUST mirror mart_referee_dashboard.sql exactly.
-- =============================================================================

SELECT
    CAST(NULL AS varchar) AS referee_id,
    CAST(NULL AS varchar) AS referee_name,
    CAST(NULL AS bigint)  AS season,
    CAST(NULL AS varchar) AS league,
    CAST(NULL AS bigint)  AS matches_officiated,
    CAST(NULL AS double)  AS cards_per_match,
    CAST(NULL AS double)  AS yellows_per_match,
    CAST(NULL AS double)  AS reds_per_match,
    CAST(NULL AS double)  AS goals_per_match,
    CAST(NULL AS double)  AS penalties_per_match,
    CAST(NULL AS double)  AS home_win_pct
WHERE 1 = 0
