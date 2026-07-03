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
-- nationality / dob (issue #434): enriched from two sources, priority
-- FotMob > Transfermarkt (COALESCE).
--   * FotMob — xref_manager carries the coachId in source_id (source='fotmob');
--     we bridge canonical_id ↔ coachId (xref_fotmob, 1 row/canonical) and pull
--     country/dob from silver.fotmob_manager_profile. Covers current-season APL
--     coaches only.
--   * Transfermarkt (xref-improvements) — bridged through xref_manager
--     (source='transfermarkt', source_id=coach_id) instead of the old direct
--     name-join on transfermarkt_coaches.canonical_id: the xref cascade also
--     glues name_alias / name_initial rows the direct join could not see, so
--     dob/nationality coverage is strictly ≥ the old path. Adds historical
--     managers (the bulk of the 81).
-- Each attribute lives in an already-aggregated CTE (1 row/key) before the LEFT
-- JOIN, so the "1 row/manager" grain is preserved. A canonical_id neither source
-- covers stays NULL (same caveat as dim_player).
--
-- PK:           manager_id  (canonical from silver.xref_manager)
-- Partitioning: NONE  (small global dim — star design: dims unpartitioned)
-- =============================================================================

WITH managers AS (
    -- Spine: one row per canonical manager. TM orphans are EXCLUDED: a TM
    -- coach that failed every cascade tier is almost always the same human as
    -- an existing FBref canonical under a mis-normalised slug — admitting it
    -- would mint a duplicate manager_id row. FotMob orphans keep the historic
    -- behaviour (present since #144) so the dim row set is unchanged by the
    -- TM bridge.
    SELECT
        canonical_id AS manager_id,
        COALESCE(
            MAX(display_name) FILTER (WHERE source = 'fbref'),
            MAX(display_name)
        )            AS manager_name
    FROM iceberg.silver.xref_manager
    WHERE NOT (source = 'transfermarkt' AND confidence = 'orphan')
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
),

-- canonical_id ↔ TM coach_id via the xref bridge (mirror of xref_fotmob).
-- Orphans excluded — an un-glued TM coach must not enrich (or mint) a
-- canonical. ROW_NUMBER keeps it 1:1 (latest season wins).
xref_tm AS (
    SELECT canonical_id, tm_coach_id
    FROM (
        SELECT
            canonical_id,
            source_id AS tm_coach_id,
            ROW_NUMBER() OVER (
                PARTITION BY canonical_id
                ORDER BY season DESC
            ) AS rn
        FROM iceberg.silver.xref_manager
        WHERE source = 'transfermarkt'
          AND confidence <> 'orphan'
    )
    WHERE rn = 1
),

-- Transfermarkt head-coach attributes (issue #434), re-keyed through the xref
-- bridge (xref-improvements): coach_id → canonical_id. The old direct join on
-- transfermarkt_coaches.canonical_id (a locally-computed name slug) missed
-- every coach the cascade glues via name_alias / name_initial; that column is
-- now DEPRECATED. One row per canonical_id via MAX_BY (freshest season).
-- dob is already DATE.
tm_manager AS (
    SELECT
        x.canonical_id,
        MAX_BY(tc.nationality, tc.season) AS nationality,
        MAX_BY(tc.dob,         tc.season) AS dob
    FROM iceberg.silver.transfermarkt_coaches tc
    JOIN xref_tm x
      ON CAST(tc.coach_id AS varchar) = x.tm_coach_id
    GROUP BY x.canonical_id
)

SELECT
    m.manager_id,
    m.manager_name,
    -- Priority FotMob > Transfermarkt (FotMob exact for current-season coaches;
    -- TM adds historical managers). FotMob dob is an ISO-string passthrough —
    -- TRY_CAST keeps the column DATE-typed; TM dob is already DATE.
    COALESCE(fm.nationality, tm.nationality)               AS nationality,
    COALESCE(TRY_CAST(fm.date_of_birth AS DATE), tm.dob)   AS dob
FROM managers m
LEFT JOIN xref_fotmob xf
    ON xf.canonical_id = m.manager_id
LEFT JOIN fotmob_manager fm
    ON fm.player_id = xf.fotmob_coach_id
LEFT JOIN tm_manager tm
    ON tm.canonical_id = m.manager_id
