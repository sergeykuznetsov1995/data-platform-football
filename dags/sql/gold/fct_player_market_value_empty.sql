-- =============================================================================
-- Gold: fct_player_market_value (EMPTY FALLBACK)
-- =============================================================================
-- Identical schema to fct_player_market_value.sql, all NULL rows.
-- Routed by gold_tasks.run_gold_transform when require_silver source
-- `fotmob_player_market_value_history` is absent (MVP env без FotMob ingest).
-- =============================================================================

SELECT
    CAST(NULL AS varchar)   AS player_id_canonical,
    CAST(NULL AS date)      AS value_date,
    CAST(NULL AS bigint)    AS market_value_eur,
    CAST(NULL AS varchar)   AS currency,
    CAST(NULL AS timestamp(6) with time zone) AS _bronze_ingested_at,
    CAST(NULL AS varchar)   AS league,
    CAST(NULL AS bigint)    AS season
WHERE 1 = 0
