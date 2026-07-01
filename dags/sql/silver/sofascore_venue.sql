-- =============================================================================
-- Silver: sofascore_venue   (from bronze.sofascore_venue)
-- =============================================================================
--
-- One row per (league, season, stadium) — conform-only projection of the
-- per-match venue block captured from SofaScore `/api/v1/event/{id}.venue`
-- (#753). SofaScore records the stadium THIS match was played at, so it stays
-- historically accurate for clubs that moved grounds (Everton → Goodison Park,
-- Spurs → White Hart Lane) — exactly where FotMob's current-ground
-- `team_profile` is wrong. Feeds the `sofascore_venue` enrichment CTE in
-- gold.dim_venue (LEFT JOIN by (league, normalised stadium name); COALESCE'd
-- BEHIND FotMob for city/coords, and supplies country FotMob cannot).
--
-- Source Bronze (см. scrapers/sofascore + run_sofascore_scraper match_capture):
--   bronze.sofascore_venue — #840 auto-passthrough: game_id (bigint),
--     stadium_name/city_name/country_name (varchar),
--     venue_coordinates_latitude/longitude (double), + bonus stadium_capacity,
--     country_alpha2, league, season (varchar slug). Pre-#840 legacy names
--     stadium/city/country/venue_latitude/venue_longitude bridged via
--     COALESCE in the `src` CTE. Full-state replace_partitions, но bronze несёт
--     одну строку на МАТЧ (game_id) → dedup ROW_NUMBER до одной на стадион здесь.
--
-- Notes:
--   * Грейн bronze = per-match; many matches share a home stadium, so dedup to
--     one row per (league, season, stadium), latest snapshot wins.
--   * Season НЕ конвертируем — bronze SofaScore уже slug '2526' (#404). Эмитим
--     as-is (ср. sofascore_league_table.sql).
--   * coords уже double в bronze (scraper coerces) — pass-through.
--   * canonical venue identity (alias-resolve) отложен в Gold (charter §5);
--     dim_venue джойнит по нормализованному имени стадиона.
-- =============================================================================

WITH
-- #840: Bronze is now auto-passthrough (source-key names). Rename/derive here.
-- COALESCE(old, new) bridges pre-#840 partitions and freshly re-scraped ones.
src AS (
    SELECT
        game_id,
        COALESCE(stadium, stadium_name)                         AS stadium,
        COALESCE(city, city_name)                               AS city,
        COALESCE(country, country_name)                         AS country,
        COALESCE(venue_latitude, venue_coordinates_latitude)    AS venue_latitude,
        COALESCE(venue_longitude, venue_coordinates_longitude)  AS venue_longitude,
        _ingested_at,
        league,
        season
    FROM iceberg.bronze.sofascore_venue
),
bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            v.*,
            ROW_NUMBER() OVER (
                PARTITION BY league, season, stadium
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM src v
        WHERE stadium IS NOT NULL
          AND trim(stadium) <> ''
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    b.stadium                                             AS stadium,

    -- ===== Attributes =====
    b.city,
    b.country,
    b.venue_latitude,
    b.venue_longitude,

    -- ===== Lineage =====
    b._ingested_at                                        AS _bronze_ingested_at,

    -- ===== Partition keys (season already slug '2526' in bronze, #404) =====
    b.league,
    b.season

FROM bronze_dedup b
