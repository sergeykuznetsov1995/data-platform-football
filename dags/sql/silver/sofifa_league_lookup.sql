-- =============================================================================
-- Silver: sofifa_league_lookup
-- =============================================================================
--
-- league → sofifa league_id reference (#601). Conform-only pass-through из
-- bronze.sofifa_leagues — маленький справочник (одна строка на configured
-- league), полезен чтобы резолвить league-имя в sofifa league_id для
-- существующего sofifa_player_* пайплайна.
--
-- Bronze sofifa_leagues пишется replace-on-`league` (scrape_leagues), поэтому
-- между прогонами возможны повторные строки на один league → ROW_NUMBER dedup.
-- Беспартиционная (как и bronze-источник).
-- =============================================================================

WITH dedup AS (
    SELECT
        league,
        league_id,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY league
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.sofifa_leagues
    WHERE league IS NOT NULL
)

SELECT
    league,
    TRY_CAST(league_id AS INTEGER)  AS sofifa_league_id,
    _ingested_at                    AS _bronze_ingested_at
FROM dedup
WHERE rn = 1
