-- =============================================================================
-- Gold: dim_manager
-- =============================================================================
-- Canonical manager dimension, aligned to the star-schema design (issue #425).
-- One row per manager — a plain dictionary.
--
-- The previous version of this file was an SCD-2 stint table
-- (manager × team × valid_from intervals, islands-and-gaps over
-- bronze.fbref_match_managers). Per the star design, employment history is a
-- FACT, not a dimension attribute: it moves to gold.fct_manager_stint
-- (issue #429). The stint logic lives in this file's git history —
-- `git log -p dags/sql/gold/dim_manager.sql` — for #429 to resurrect.
--
-- Source: iceberg.silver.xref_manager (canonical ids merged across
-- FBref + FotMob). GROUP BY canonical_id collapses the per-(source,
-- source_id, league, season) xref rows, so no (league, season) predicate is
-- needed — the fan-out footgun does not apply here.
--
-- nationality / dob: no source carries them today (xref has names only) —
-- NULL placeholders keep the schema aligned to the design; enrichment is a
-- tracked followup.
--
-- PK:           manager_id  (canonical from silver.xref_manager)
-- Partitioning: NONE  (small global dim — star design: dims unpartitioned)
-- =============================================================================

SELECT
    canonical_id AS manager_id,
    COALESCE(
        MAX(display_name) FILTER (WHERE source = 'fbref'),
        MAX(display_name)
    )            AS manager_name,
    CAST(NULL AS varchar) AS nationality,
    CAST(NULL AS date)    AS dob
FROM iceberg.silver.xref_manager
GROUP BY canonical_id
