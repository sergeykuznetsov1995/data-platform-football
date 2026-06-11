-- =============================================================================
-- Gold: fct_player_unavailable  — EMPTY FALLBACK
-- =============================================================================
-- Materialized when silver.whoscored_player_unavailable is absent (Bronze not
-- ingested yet, or WhoScored DAG paused — see project_whoscored_cloudflare.md).
--
-- Goal: keep the Gold contract intact so feat_team_form's LEFT JOIN keeps
-- resolving (finds 0 rows, COALESCEs to 0 unavailable players per match).
--
-- Schema MUST mirror fct_player_unavailable.sql exactly (star design §4.6,
-- issue #426). Spine is dim_match with WHERE 1=0 — Trino preserves column
-- types from the SELECT list; the CAST(NULL AS …) calls anchor types for
-- columns not sourced from dim_match.
-- =============================================================================

SELECT
    match_id,

    CAST(NULL AS VARCHAR)              AS team_id,

    CAST(NULL AS VARCHAR)              AS player_id,

    CAST(NULL AS VARCHAR)              AS reason,
    CAST(NULL AS VARCHAR)              AS detail,

    -- Type does not need to match the non-fallback timestamp(3 with tz)
    -- variant: table is rebuilt every run (DROP + CTAS) and downstream
    -- consumers never read this column.
    CAST(NULL AS TIMESTAMP)            AS _silver_ingested_at,

    league,
    -- season — varchar slug ('2425'); dim_match.season is slug now (#404).
    -- WHERE 1=0 => no rows; only the column type matters for schema parity.
    season
FROM iceberg.gold.dim_match
WHERE 1 = 0
