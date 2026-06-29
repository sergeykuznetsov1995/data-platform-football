-- =============================================================================
-- Gold: fct_match_officials  — EMPTY FALLBACK   (issue #613)
-- =============================================================================
-- Materialized when silver.fbref_match_officials is absent (Bronze
-- fbref_match_officials not populated yet — combined_match_data runs that parse
-- only after this change ships / a --no-incremental backfill).
--
-- Goal: keep the Gold contract intact so downstream LEFT JOINs keep resolving
-- (find 0 rows). Schema MUST mirror fct_match_officials.sql exactly — Trino
-- preserves column types/order from the SELECT list; the CAST(NULL AS …) calls
-- anchor types for columns not sourced from dim_match. Spine is dim_match with
-- WHERE 1=0 (same pattern as fct_player_unavailable_empty.sql).
-- =============================================================================

SELECT
    match_id,
    CAST(NULL AS VARCHAR)  AS role,
    CAST(NULL AS VARCHAR)  AS referee_id,
    CAST(NULL AS VARCHAR)  AS official_name,
    league,
    season
FROM iceberg.gold.dim_match
WHERE 1 = 0
