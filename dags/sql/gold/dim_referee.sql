-- =============================================================================
-- Gold: dim_referee
-- =============================================================================
-- Canonical referee dimension, aligned to the star-schema design (issue #425).
-- One row per referee.
--
-- PK migrated from the legacy FBref-only name hash ('ref_<xxhash64-hex>') to
-- the canonical_id of silver.xref_referee — the curated 3-source identity
-- (FBref + MatchHistory + FotMob, issues #143/#270): 'ref_<slug>' for aliased
-- referees, '<src>_ref_<slug>' for orphans. Downstream referee_id consumers
-- read the id from dim_match.referee_id instead of re-deriving an inline hash.
--
-- Sources:
--   iceberg.silver.xref_referee          (spine: canonical_id + display_name)
--   iceberg.silver.fotmob_match_referee  (country — FotMob is the only source
--                                         carrying referee nationality)
--   iceberg.silver.fbref_match_enriched  (first/last_seen via match dates)
--
-- Both enrichment JOINs go through xref_referee WITH the (league, season)
-- predicate — xref rows are per-(source, source_id, league, season); without
-- it a referee active across N seasons fans out N× (memory:
-- feedback_xref_join_season_predicate).
--
-- Partitioning: NONE (small global dim — star design: dims unpartitioned;
-- referees move between leagues, so even season partitioning is wrong here).
-- =============================================================================

WITH ref_spine AS (
    SELECT
        canonical_id AS referee_id,
        -- display_name == curated canonical_name on aliased rows; orphans
        -- fall back to the FBref raw spelling, then any source's.
        COALESCE(
            MAX(display_name) FILTER (WHERE confidence = 'name_alias'),
            MAX(display_name) FILTER (WHERE source = 'fbref'),
            MAX(display_name)
        ) AS referee_name
    FROM iceberg.silver.xref_referee
    GROUP BY canonical_id
),

fotmob_country AS (
    SELECT
        xr.canonical_id,
        MAX(fm.referee_country) AS country
    FROM iceberg.silver.fotmob_match_referee fm
    INNER JOIN iceberg.silver.xref_referee xr
        ON  xr.source    = 'fotmob'
        AND xr.source_id = fm.referee_name
        AND xr.league    = fm.league
        AND xr.season    = fm.season
    WHERE fm.referee_country IS NOT NULL
    GROUP BY xr.canonical_id
),

fbref_seen AS (
    SELECT
        xr.canonical_id,
        MIN(s.date) AS first_seen_date,
        MAX(s.date) AS last_seen_date
    FROM iceberg.silver.fbref_match_enriched s
    INNER JOIN iceberg.silver.xref_referee xr
        ON  xr.source    = 'fbref'
        AND xr.source_id = TRIM(s.referee)
        AND xr.league    = s.league
        AND xr.season    = s.season
    WHERE s.referee IS NOT NULL
      AND TRIM(s.referee) <> ''
    GROUP BY xr.canonical_id
)

SELECT
    r.referee_id,
    r.referee_name,
    c.country,
    f.first_seen_date,
    f.last_seen_date
FROM ref_spine r
LEFT JOIN fotmob_country c
    ON c.canonical_id = r.referee_id
LEFT JOIN fbref_seen f
    ON f.canonical_id = r.referee_id
