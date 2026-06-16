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
-- Spine: iceberg.silver.xref_manager (canonical ids merged across
-- FBref + FotMob). GROUP BY canonical_id collapses the per-(source,
-- source_id, league, season) xref rows, so no (league, season) predicate is
-- needed — the fan-out footgun does not apply to the spine.
--
-- nationality / dob (issue #434): enriched from FotMob. xref_manager carries the
-- FotMob coachId in source_id (source='fotmob'); we bridge canonical_id ↔ coachId
-- (xref_fotmob, deduped to 1 row/canonical) and pull country/dob from
-- silver.fotmob_manager_profile (freshest season via MAX_BY). FotMob covers
-- current-season APL coaches only (~18/81) — historical managers stay NULL, same
-- coverage caveat as dim_player. A Transfermarkt-coach source is a tracked
-- followup to raise coverage. Each attribute lives in an already-aggregated CTE
-- (1 row/key) before the LEFT JOIN, so the "1 row/manager" grain is preserved.
--
-- PK:           manager_id  (canonical from silver.xref_manager)
-- Partitioning: NONE  (small global dim — star design: dims unpartitioned)
-- =============================================================================

WITH managers AS (
    -- Spine: one row per canonical manager.
    SELECT
        canonical_id AS manager_id,
        COALESCE(
            MAX(display_name) FILTER (WHERE source = 'fbref'),
            MAX(display_name)
        )            AS manager_name
    FROM iceberg.silver.xref_manager
    GROUP BY canonical_id
),

-- canonical_id ↔ FotMob coachId (source_id). ROW_NUMBER keeps it 1:1 (latest
-- season wins) so the LEFT JOIN below cannot fan out the spine.
xref_fotmob AS (
    SELECT canonical_id, fotmob_coach_id
    FROM (
        SELECT
            canonical_id,
            source_id AS fotmob_coach_id,
            ROW_NUMBER() OVER (
                PARTITION BY canonical_id
                ORDER BY season DESC
            ) AS rn
        FROM iceberg.silver.xref_manager
        WHERE source = 'fotmob'
    )
    WHERE rn = 1
),

-- FotMob coach attributes, freshest value per coachId (MAX_BY ignores
-- league/season — snapshot grain, same idiom as dim_player's fotmob_latest).
fotmob_manager AS (
    SELECT
        player_id,
        MAX_BY(nationality,   season) AS nationality,
        MAX_BY(date_of_birth, season) AS date_of_birth
    FROM iceberg.silver.fotmob_manager_profile
    WHERE player_id IS NOT NULL
    GROUP BY player_id
)

SELECT
    m.manager_id,
    m.manager_name,
    fm.nationality                         AS nationality,
    -- FotMob dob is an ISO-string passthrough — TRY_CAST keeps the column
    -- DATE-typed (a non-ISO value degrades to NULL).
    TRY_CAST(fm.date_of_birth AS DATE)     AS dob
FROM managers m
LEFT JOIN xref_fotmob xf
    ON xf.canonical_id = m.manager_id
LEFT JOIN fotmob_manager fm
    ON fm.player_id = xf.fotmob_coach_id
