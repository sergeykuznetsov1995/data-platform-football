-- =============================================================================
-- Gold: fct_player_market_value (EMPTY FALLBACK)
-- =============================================================================
-- Identical schema to fct_player_market_value.sql, zero rows.
-- Routed by gold_tasks.run_gold_transform when the require_silver source
-- (`transfermarkt_market_value_history`) is absent (MVP env without
-- Transfermarkt ingest).
-- =============================================================================

SELECT
    CAST(NULL AS varchar)      AS player_id,
    CAST(NULL AS date)         AS valuation_date,
    CAST(NULL AS bigint)       AS market_value_eur,
    CAST(NULL AS varchar)      AS currency,
    CAST(NULL AS varchar)      AS source,
    CAST(NULL AS timestamp(6)) AS _bronze_ingested_at
WHERE 1 = 0
