-- =============================================================================
-- Silver: fotmob_player_market_value_history
-- =============================================================================
--
-- Time-series рыночной стоимости игроков из FotMob. Один row per
-- (player_id, value_date, league, season) — точка timeline = snapshot MV на
-- конкретную дату.
--
-- Bronze shape (`fotmob_player_details.market_values_json`):
--   {"values": [{"date": "ISO", "value": <int>, "currency": "EUR", ...}, ...]}
-- (тот же UNNEST-идиом, что в silver/fotmob_player_profile.sql при выборе
--  latest точки — см. mv_latest CTE).
--
-- Cross-season дубликаты: FotMob отдаёт полную history до сегодня в каждом
-- ingest-snapshot. Игрок в APL 2024/25 и 2025/26 → исторические точки timeline
-- лягут в обе партиции. PK включает (league, season) — потребитель фильтрует
-- WHERE season = (MAX) для «last view» либо специфический season-snapshot.
-- Полностью обещанный timeline = WHERE season = latest.
--
-- canonical_id НЕ резолвится в Silver (отличие от transfermarkt_market_value_history,
-- где canonical_id мерджится из silver.xref_player в момент Silver-материализации).
-- Здесь FotMob bridge применяется в Gold (fct_player_market_value), чтобы Silver
-- оставался pure-shaping слоем bronze→silver без cross-Silver dependencies.
--
-- Зерно: (player_id, value_date, league, season).
-- =============================================================================

WITH details_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fotmob_player_details
    WHERE NOT is_coach
      AND market_values_json IS NOT NULL
      AND market_values_json <> 'null'
      AND market_values_json <> '{}'
)

SELECT
    d.player_id,
    TRY_CAST(SUBSTR(json_extract_scalar(v, '$.date'), 1, 10) AS DATE) AS value_date,
    TRY_CAST(json_extract_scalar(v, '$.value')    AS BIGINT)          AS market_value_eur,
    json_extract_scalar(v, '$.currency')                              AS currency,
    d._ingested_at                                                    AS _bronze_ingested_at,
    -- season → slug ('2425'); FotMob bronze stores year-start bigint (2024).
    d.league,
    -- #913 Phase 2
    CASE WHEN d.league = 'INT-World Cup'
         THEN LPAD(CAST(d.season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(d.season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(d.season + 1, 100) AS varchar), 2, '0')
    END AS season

FROM details_dedup d
CROSS JOIN UNNEST(
    CAST(json_extract(d.market_values_json, '$.values') AS array<json>)
) AS t(v)
WHERE d.rn = 1
  AND TRY_CAST(SUBSTR(json_extract_scalar(v, '$.date'), 1, 10) AS DATE) IS NOT NULL
