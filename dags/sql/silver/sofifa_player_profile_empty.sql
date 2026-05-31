-- =============================================================================
-- Silver: sofifa_player_profile  — EMPTY FALLBACK
-- =============================================================================
-- Materialized when bronze.sofifa_player_ratings is absent (SoFIFA ingest
-- frozen by Cloudflare Turnstile — issue #180). Keeps the Silver contract
-- intact so gold.dim_player_attributes' LEFT JOIN sofifa_latest keeps
-- resolving (0 rows -> NULL sofifa attributes). When Bronze returns, the DAG
-- auto-resumes the real SQL (no manual cleanup; DROP + CTAS each run).
--
-- Schema MUST mirror sofifa_player_profile.sql SELECT (lines 126-170), same
-- order, WITHOUT _silver_created_at (the CTAS wrapper appends it). Spine is
-- silver.xref_player WHERE 1=0 — always exists, supplies league/season for the
-- partitioning=ARRAY['league','season'] writer. The CAST(NULL AS …) calls
-- anchor types for the remaining columns (matching the source CASTs).
-- =============================================================================

SELECT
    CAST(NULL AS BIGINT)        AS player_id,
    CAST(NULL AS VARCHAR)       AS canonical_id,
    CAST(NULL AS VARCHAR)       AS player_name,

    CAST(NULL AS INTEGER)       AS overall,
    CAST(NULL AS INTEGER)       AS potential,
    CAST(NULL AS INTEGER)       AS pace,
    CAST(NULL AS INTEGER)       AS shooting,
    CAST(NULL AS INTEGER)       AS passing,
    CAST(NULL AS INTEGER)       AS dribbling,
    CAST(NULL AS INTEGER)       AS defending,
    CAST(NULL AS INTEGER)       AS physical,
    CAST(NULL AS INTEGER)       AS gk_diving,
    CAST(NULL AS INTEGER)       AS gk_handling,
    CAST(NULL AS INTEGER)       AS gk_kicking,
    CAST(NULL AS INTEGER)       AS gk_positioning,
    CAST(NULL AS INTEGER)       AS gk_reflexes,

    CAST(NULL AS BIGINT)        AS value_eur,
    CAST(NULL AS BIGINT)        AS wage_eur,
    CAST(NULL AS BIGINT)        AS release_clause_eur,
    CAST(NULL AS INTEGER)       AS contract_valid_until,
    CAST(NULL AS TIMESTAMP)     AS joined,

    CAST(NULL AS VARCHAR)       AS position,
    CAST(NULL AS VARCHAR)       AS dob,
    CAST(NULL AS INTEGER)       AS height_cm,
    CAST(NULL AS INTEGER)       AS weight_kg,
    CAST(NULL AS VARCHAR)       AS nationality,

    CAST(NULL AS VARCHAR)       AS team,
    CAST(NULL AS VARCHAR)       AS fifa_edition,

    CAST(NULL AS TIMESTAMP)     AS _bronze_ingested_at,

    -- Partition keys last (from spine, matching writer convention).
    league,
    season
FROM iceberg.silver.xref_player
WHERE 1 = 0
