-- =============================================================================
-- Gold: fct_player_market_value
-- =============================================================================
--
-- Timeline рыночной стоимости игроков из FotMob. Один row per
-- (player_id_canonical, value_date, league, season).
--
-- Source: silver.fotmob_player_market_value_history (UNNEST из
--         bronze.fotmob_player_details.market_values_json).
-- Bridge: FotMob player_id → canonical_id через silver.xref_player
--         WHERE source='fotmob' AND confidence <> 'orphan'.
--
-- Зерно и partitioning (issue #11):
--   * Grain per-row: (player_id_canonical, value_date, league, season).
--   * Partitioned by (league, season) — для совместимости с остальными fct.
--   * Cross-season дубликаты точек FotMob timeline (FotMob отдаёт всю history
--     в каждом ingest snapshot) — потребитель фильтрует WHERE season = (MAX)
--     для «last view». Полная одиночная картина = WHERE season = current.
--
-- ⚠️ xref JOIN MUST include (league, season) predicate (CLAUDE.md footgun):
--   silver.xref_player имеет per-(source, source_id, season) rows; без
--   season-condition будет fan-out 1.5-4×.
--
-- Season type mapping (all varchar slug 'YYNN' after #404):
--   * silver.fotmob_player_market_value_history.season = varchar slug '2526'
--   * silver.xref_player.season                        = varchar slug '2526'
--   #404 unified silver/xref season onto the slug form → JOIN is slug = slug.
--
-- TM extension followup: silver.transfermarkt_market_value_history уже
-- содержит canonical_id и параллельный timeline; UNION ALL добавление
-- источника = новая `source` колонка + расширение PK на `source`. Не входит
-- в issue #11 scope.
-- =============================================================================

WITH xref_fotmob AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fotmob_player_id,
        league,
        season  /* #404: slug passthrough (was slug→year-start) */      AS season_year
    FROM iceberg.silver.xref_player
    WHERE source = 'fotmob'
      AND confidence <> 'orphan'
)

SELECT
    xfm.canonical_id                                      AS player_id_canonical,
    mv.value_date                                         AS value_date,
    mv.market_value_eur                                   AS market_value_eur,
    mv.currency                                           AS currency,
    mv._bronze_ingested_at                                AS _bronze_ingested_at,
    mv.league                                             AS league,
    mv.season                                             AS season

FROM iceberg.silver.fotmob_player_market_value_history mv
INNER JOIN xref_fotmob xfm
    ON  xfm.fotmob_player_id = mv.player_id
    AND xfm.league           = mv.league
    AND xfm.season_year      = mv.season
WHERE mv.value_date IS NOT NULL
