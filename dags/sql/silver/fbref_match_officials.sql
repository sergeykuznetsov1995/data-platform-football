-- =============================================================================
-- Silver: fbref_match_officials   (issue #613)
-- =============================================================================
-- One row per (match_id, role) — the officiating crew of a match from FBref.
--
-- Bronze (bronze.fbref_match_officials) is WIDE: one row per match with the
-- columns referee / ar1 / ar2 / fourth_official / var (parsed from the match
-- page scorebox_meta — scrapers/fbref/parsers/finders.py::parse_match_officials).
-- This Silver layer UNPIVOTS that wide row to LONG via a two-array UNNEST that
-- zips the role label with the name positionally. role ∈ {referee, ar1, ar2,
-- fourth_official, var}. A role absent on the page (e.g. no VAR on some
-- fixtures) is encoded as a MISSING ROW, never a NULL official_name — the
-- WHERE clause drops empty positions.
--
-- This is clean/conform only (a reshape), NOT aggregation — Silver charter OK.
--
-- NOTE: the MAIN referee here is the same official already surfaced by
-- fbref_schedule.referee (and thus already in silver.xref_referee /
-- gold.dim_referee). The genuinely-new coverage is ar1/ar2/fourth_official/var.
-- This table is the spine for gold.fct_match_officials.
--
-- Source:
--   bronze.fbref_match_officials (1 row/match). Bronze may carry >1 ingest
--   snapshot per match across re-scrapes → dedup to the latest by _ingested_at
--   (same idiom as fotmob_match_referee.sql / xref_referee.sql.j2).
--
-- Pure SELECT: run_silver_transform wraps CREATE OR REPLACE + partitioning
-- (league, season) + _silver_created_at. This file MUST stay a pure SELECT.
-- =============================================================================

WITH officials_dedup AS (
    SELECT
        match_id,
        league,
        season,
        source_season_id,
        referee,
        ar1,
        ar2,
        fourth_official,
        var,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, league, season
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.fbref_match_officials
)

SELECT
    o.match_id,
    o.league,
    -- season → slug ('2526'); bronze fbref_match_officials is year-start bigint (#404).
    -- #913 Phase 2
    CASE
         WHEN REGEXP_LIKE(COALESCE(o.source_season_id, ''), '^\d{4}$')
             THEN o.source_season_id
         WHEN REGEXP_LIKE(COALESCE(o.source_season_id, ''), '^\d{4}-\d{4}$')
             THEN SUBSTR(o.source_season_id, 3, 2)
                  || SUBSTR(o.source_season_id, 8, 2)
         WHEN NULLIF(TRIM(o.source_season_id), '') IS NOT NULL
             THEN TRIM(o.source_season_id)
         WHEN o.league = 'INT-World Cup'
             THEN LPAD(CAST(o.season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(o.season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(o.season + 1, 100) AS varchar), 2, '0')
    END AS season,
    r.role,
    r.official_name,
    o._ingested_at AS _bronze_ingested_at
FROM officials_dedup o
CROSS JOIN UNNEST(
    ARRAY['referee', 'ar1', 'ar2', 'fourth_official', 'var'],
    ARRAY[o.referee, o.ar1, o.ar2, o.fourth_official, o.var]
) AS r(role, official_name)
WHERE o.rn = 1
  AND r.official_name IS NOT NULL
  AND TRIM(r.official_name) <> ''
