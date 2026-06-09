-- =============================================================================
-- Gold: fct_player_unavailable  — EMPTY FALLBACK
-- =============================================================================
-- Materialized when silver.whoscored_player_unavailable is absent (Bronze not
-- ingested yet, or WhoScored DAG paused — see project_whoscored_cloudflare.md).
--
-- Goal: keep the Gold contract intact so feat_team_form's LEFT JOIN keeps
-- resolving (finds 0 rows, COALESCEs to 0 unavailable players per match).
--
-- Schema MUST mirror fct_player_unavailable.sql exactly. Spine is dim_match
-- with WHERE 1=0 — Trino preserves column types from the SELECT list; the
-- CAST(NULL AS …) calls anchor types for columns not sourced from dim_match.
-- =============================================================================

SELECT
    match_id,
    date                               AS match_date,

    CAST(NULL AS VARCHAR)              AS team_id,
    CAST(NULL AS VARCHAR)              AS team_name_raw,

    CAST(NULL AS VARCHAR)              AS player_id_canonical,
    CAST(NULL AS BIGINT)               AS ws_player_id,
    CAST(NULL AS VARCHAR)              AS player_name,

    CAST(NULL AS VARCHAR)              AS reason,

    -- Type does not need to match the non-fallback timestamp(3 with tz)
    -- variant: table is rebuilt every run (DROP + CTAS) and downstream
    -- consumers never read this column.
    CAST(NULL AS TIMESTAMP)            AS _silver_ingested_at,

    league,
    -- season — varchar slug, как в основном fct_player_unavailable.sql (#388).
    -- WHERE 1=0 => строк нет; важен только тип колонки для schema-parity.
    format('%02d%02d', mod(season, 100), mod(season + 1, 100)) AS season
FROM iceberg.gold.dim_match
WHERE 1 = 0
