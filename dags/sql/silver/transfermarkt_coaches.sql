-- =============================================================================
-- Silver: transfermarkt_coaches
-- =============================================================================
--
-- Head-coach snapshot from Transfermarkt: dob + nationality per
-- (coach_id, league, season). Bronze scraper keeps only the club's "Manager"
-- (head coach) — assistants/analysts are dropped upstream
-- (scrapers/transfermarkt/scraper.py::_parse_staff_managers, issue #434).
--
-- Feeds gold.dim_manager nationality/dob enrichment as the historical-coverage
-- source on top of FotMob (which only covers current-season coaches).
--
-- canonical_id (issue #434): derived here with the SAME diacritic-stripping
-- name-normalize idiom as silver.xref_manager.sql — NOT via an xref bridge.
-- Managers canonicalise deterministically by name (no fuzzy resolver), so the
-- TM head coach lands on the same canonical_id as the FBref/FotMob spine and
-- dim_manager's LEFT JOIN enriches it. A TM name that matches no spine coach
-- simply fails the JOIN (does not pollute dim_manager).
--
-- Notes:
--   * Grain = (coach_id, league, season), matching Bronze. ROW_NUMBER dedup
--     because Bronze ingest mode = replace_partitions.
--   * dob is already a DATE (parsed in the scraper via _parse_tm_date);
--     nationality is a full country name ('Spain'), consistent with FotMob.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY coach_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.transfermarkt_coaches b
        WHERE coach_id IS NOT NULL
          AND name IS NOT NULL AND name <> ''
    )
    WHERE rn = 1
)

SELECT
    b.coach_id,
    -- Same idiom as xref_manager.sql: NORMALIZE(NFD) decomposes accents,
    -- \p{Mn}+ strips the marks, then non-alphanumerics collapse to '_'.
    LOWER(REGEXP_REPLACE(
        REGEXP_REPLACE(NORMALIZE(b.name, NFD), '\p{Mn}+', ''),
        '[^a-zA-Z0-9]+', '_'))                     AS canonical_id,
    b.name,
    b.role,
    b.dob,
    b.nationality,

    b.current_club_id,
    b.current_club_name,

    b._ingested_at                                 AS _bronze_ingested_at,

    -- Partition keys last (matching writer convention).
    b.league,
    b.season

FROM bronze_dedup b
