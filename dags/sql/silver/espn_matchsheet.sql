-- =============================================================================
-- Silver: espn_matchsheet   (from bronze.espn_matchsheet)
-- =============================================================================
--
-- Conform-only venue-проекция ESPN matchsheet (issue #735, Gold one-hop аудит #704).
-- gold.dim_venue раньше читал bronze.espn_matchsheet напрямую — единственное raw-data
-- чтение Bronze в этом dim. Поднимаем ESPN venue-фид в Silver, чтобы dim_venue остался
-- one-hop от Silver (FBref-сторона уже из silver.fbref_match_enriched). charter §2:
-- без агрегации, без cross-source JOIN — ср. sofascore_league_table.sql.
--
-- Источник Bronze (backfill-only, scripts/backfill_espn_e3_5.py read_matchsheet):
--   bronze.espn_matchsheet — venue (varchar), game (varchar 'YYYY-MM-DD-...'),
--     league/season (varchar slug '2526', partition keys), _ingested_at (timestamp).
--   APPEND-mode снапшоты → dedup ROW_NUMBER здесь (перенесён из gold.dim_venue).
--
-- Output schema (контракт для gold.dim_venue ESPN-стороны):
--   venue (varchar), match_date (date), _bronze_ingested_at (timestamp),
--   league, season (varchar slug, partition keys).
--
-- DAG-integration note
--   silver_tasks.run_silver_transform() оборачивает этот SELECT в
--   `CREATE OR REPLACE TABLE iceberg.silver.espn_matchsheet AS ...` с
--   partitioning by (league, season). Этот файл ДОЛЖЕН оставаться pure SELECT.
--
-- Notes:
--   * Grain = одна строка на матч (dedup по trim(venue),league,season,game → latest
--     _ingested_at — тот же ключ, что использовал dim_venue).
--   * match_date = первые 10 символов game (try_cast → DATE; NULL при плохом парсе).
--   * Season НЕ конвертируем — ESPN bronze уже slug '2526' (#404), как у FBref.
--   * capacity НЕ тянем — 100% NULL by design (feedback_bronze_expected_null_columns);
--     capacity в dim_venue приходит из venue_aliases.yaml.
-- =============================================================================

WITH espn_downstream_scope (
    scope_id, espn_id, source_season_year, platform_league,
    platform_season_slug, convention, effective_start_date, effective_end_date
) AS (VALUES
__ESPN_DOWNSTREAM_SCOPE_VALUES__
),
bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            trim(es_source.venue)                    AS venue,
            try_cast(substr(es_source.game, 1, 10) AS date) AS match_date,
            es_source._ingested_at,
            -- Native source identity remains verbatim; legacy fallback rows
            -- intentionally retain NULL native identity.
            es_source.scope_id                       AS source_scope_id,
            es_source.source_season_year             AS source_season_year,
            espn_scope.platform_league               AS league,
            espn_scope.platform_season_slug          AS season,
            ROW_NUMBER() OVER (
                PARTITION BY trim(es_source.venue),
                    espn_scope.platform_league,
                    espn_scope.platform_season_slug,
                    es_source.game
                ORDER BY es_source._ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.espn_matchsheet_current es_source
        JOIN espn_downstream_scope espn_scope ON
__ESPN_DOWNSTREAM_SCOPE_FILTER__
        WHERE es_source.venue IS NOT NULL
          AND trim(es_source.venue) <> ''
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    venue,
    match_date,
    source_scope_id,
    source_season_year,

    -- ===== Lineage =====
    _ingested_at                                     AS _bronze_ingested_at,

    -- ===== Partition keys (season already slug '2526' in bronze, #404) =====
    league,
    season

FROM bronze_dedup
