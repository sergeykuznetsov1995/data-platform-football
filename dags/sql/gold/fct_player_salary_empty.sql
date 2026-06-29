-- =============================================================================
-- Gold: fct_player_salary (EMPTY FALLBACK)
-- =============================================================================
-- Identical schema to fct_player_salary.sql, zero rows.
-- Routed by gold_tasks.run_gold_transform when require_silver source
-- `capology_player_salaries` is absent (env without Capology ingest).
-- =============================================================================

SELECT
    CAST(NULL AS varchar)        AS player_id,
    CAST(NULL AS varchar)        AS player_name,
    CAST(NULL AS varchar)        AS club_name,
    CAST(NULL AS decimal(12,2))  AS weekly_gross_eur,
    CAST(NULL AS decimal(14,2))  AS annual_gross_eur,
    CAST(NULL AS decimal(12,2))  AS weekly_gross_gbp,
    CAST(NULL AS decimal(14,2))  AS annual_gross_gbp,
    CAST(NULL AS decimal(12,2))  AS weekly_gross_usd,
    CAST(NULL AS decimal(14,2))  AS annual_gross_usd,
    CAST(NULL AS varchar)        AS contract_status,
    CAST(NULL AS boolean)        AS is_verified,
    CAST(NULL AS timestamp(6))   AS _bronze_ingested_at,
    CAST(NULL AS varchar)        AS league,
    CAST(NULL AS varchar)        AS season
WHERE 1 = 0
