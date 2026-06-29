-- =============================================================================
-- Gold: fct_transfer (EMPTY FALLBACK)
-- =============================================================================
-- Identical schema to fct_transfer.sql, zero rows.
-- Routed by gold_tasks.run_gold_transform when require_silver source
-- `transfermarkt_transfers` is absent (env без Transfermarkt ingest).
-- =============================================================================

SELECT
    CAST(NULL AS varchar)       AS player_id,
    CAST(NULL AS date)          AS transfer_date,
    CAST(NULL AS varchar)       AS from_team_id,
    CAST(NULL AS varchar)       AS to_team_id,
    CAST(NULL AS varchar)       AS from_club_name,
    CAST(NULL AS varchar)       AS to_club_name,
    CAST(NULL AS bigint)        AS fee_eur,
    CAST(NULL AS bigint)        AS market_value_at_transfer_eur,
    CAST(NULL AS boolean)       AS is_loan,
    CAST(NULL AS boolean)       AS is_upcoming,
    CAST(NULL AS timestamp(6))  AS _bronze_ingested_at,
    CAST(NULL AS varchar)       AS league,
    CAST(NULL AS varchar)       AS season
WHERE 1 = 0
