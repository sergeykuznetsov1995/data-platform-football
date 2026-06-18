-- =============================================================================
-- Silver: fotmob_team_profile
-- =============================================================================
--
-- One row per (team_id, league, season) — conform-only проекция профиля команды
-- из FotMob (endpoint /api/data/teams). Snapshot на момент ingest'а: страна,
-- домашний стадион, текущее место в таблице. Питает будущий gold.dim_team
-- (FotMob-ветка) — резолв canonical_id отложен в Gold (charter §5), здесь храним
-- сырой numeric team_id + team_name (Gold джойнит silver.xref_team по имени, как
-- fct_team_match для fotmob_team_match).
--
-- Источник Bronze (см. scrapers/fotmob/scraper.py read_team_profile):
--   bronze.fotmob_team_profile — team_id (bigint), team_name/short_name/country/
--     venue (varchar), overview_table_position (varchar — stringified int),
--     league (varchar), season (bigint year-start).
--
-- Notes:
--   * Снапшотные/низкоценные колонки НЕ переносим: next_match / last_match (сырой
--     JSON ближайшего/прошлого матча), overview_season (дублирует season),
--     history_seasons_count.
--   * overview_table_position приходит строкой → TRY_CAST AS INTEGER.
--   * Season — bigint year-start (2025) в bronze; emit slug ('2526') тем же
--     выражением, что fotmob_team_match.sql и xref_team.sql.j2 (charter S2).
--   * Bronze ingest mode = replace_partitions(['league','season']) → ROW_NUMBER
--     dedup defensive против повторных прогонов внутри одной партиции.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            p.*,
            ROW_NUMBER() OVER (
                PARTITION BY team_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.fotmob_team_profile p
        WHERE team_id IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    b.team_id,
    b.team_name,
    b.short_name,

    -- ===== Attributes =====
    b.country,
    b.venue,
    TRY_CAST(b.overview_table_position AS INTEGER) AS table_position,

    -- ===== Lineage =====
    b._ingested_at AS _bronze_ingested_at,

    -- ===== Partition keys (season → slug to match other Silver tables) =====
    b.league,
    LPAD(CAST(MOD(b.season,     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(b.season + 1, 100) AS varchar), 2, '0') AS season

FROM bronze_dedup b
