-- =============================================================================
-- Silver: sofascore_venue  — EMPTY FALLBACK   (issue #812)
-- =============================================================================
-- bronze.sofascore_venue is OPTIONAL (#753): the SofaScore match capture writes
-- it only when an event payload actually carries a `.venue` block, so until a
-- capture pass returns venue data the Bronze table does not physically exist.
--
-- When the Bronze source is absent, `_run_silver_e3` (dag_transform_e3) runs
-- THIS fallback instead of sofascore_venue.sql, materialising an EMPTY
-- silver.sofascore_venue with the IDENTICAL schema. That keeps the contract for
-- gold.dim_venue's `sofascore_venue` enrichment CTE intact (LEFT JOIN by
-- normalised stadium name → COALESCE behind FotMob): with zero rows it simply
-- contributes nothing instead of failing the whole Gold layer on
-- TABLE_NOT_FOUND (the #812 cascade that blocked dag_transform_e3 gold_e3 AND
-- dag_transform_fbref_gold).
--
-- Schema MUST mirror sofascore_venue.sql exactly — Trino preserves column
-- types/order from the SELECT list (positional CREATE OR REPLACE). The
-- CAST(NULL AS …) calls anchor the types; `WHERE 1 = 0` yields no rows.
-- =============================================================================

SELECT
    -- ===== Identity =====
    CAST(NULL AS varchar)       AS stadium,

    -- ===== Attributes =====
    CAST(NULL AS varchar)       AS city,
    CAST(NULL AS varchar)       AS country,
    CAST(NULL AS double)        AS venue_latitude,
    CAST(NULL AS double)        AS venue_longitude,

    -- ===== Lineage =====
    CAST(NULL AS timestamp(6))  AS _bronze_ingested_at,

    -- ===== Partition keys =====
    CAST(NULL AS varchar)       AS league,
    CAST(NULL AS varchar)       AS season

FROM (VALUES (1)) AS t(x)
WHERE 1 = 0
