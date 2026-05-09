-- =============================================================================
-- Gold: mart_event_heatmap — EMPTY FALLBACK
-- =============================================================================
-- Schema MUST mirror mart_event_heatmap.sql exactly.
-- =============================================================================

SELECT
    CAST(NULL AS varchar) AS team_id,
    CAST(NULL AS varchar) AS team_name,
    CAST(NULL AS varchar) AS season,
    CAST(NULL AS varchar) AS league,
    CAST(NULL AS integer) AS zone_x,
    CAST(NULL AS integer) AS zone_y,
    CAST(NULL AS varchar) AS action_canonical,
    CAST(NULL AS bigint)  AS event_count,
    CAST(NULL AS double)  AS success_rate
WHERE 1 = 0
