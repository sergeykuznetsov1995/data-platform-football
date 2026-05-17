-- =============================================================================
-- Silver: fotmob_player_profile
-- =============================================================================
--
-- Time-invariant snapshot per (player_id, league, season). Хранит атрибуты
-- игрока, которые не меняются в течение сезона: рост, дата рождения,
-- национальность, ведущая нога, номер на футболке.
--
-- Zerno: (player_id, league, season). Симметрично с silver.fotmob_player_season_profile
-- и silver.fotmob_keeper_profile. Для одного игрока в одном сезоне — один row;
-- если игрок появится в нескольких сезонах, у каждого свой row (атрибуты
-- de-facto константны, но per-season хранение упрощает downstream JOIN-ы).
--
-- Sources (all from iceberg.bronze):
--   fotmob_team_squad     (s) — основной источник: height_cm / date_of_birth /
--                                country / country_code / shirt_number
--                                (структурированные поля, не JSON).
--   fotmob_player_details (d) — `foot` (Preferred foot) — единственное поле,
--                                которое отсутствует в team_squad и доступно
--                                ТОЛЬКО внутри player_information_json.
--                                Также `contract_end` (varchar, ISO-дата либо
--                                JSON-dumped dict) — slowly-changing, latest-per-season.
--
-- Pipeline:
--   1. details_dedup — ROW_NUMBER dedup на (player_id, league, season).
--      Фильтр NOT is_coach (тренеры из FotMob /api/playerData приходят с
--      is_coach=true в той же таблице).
--   2. squad_dedup   — то же для fotmob_team_squad. JOIN-ключ
--      CAST(s.player_id AS VARCHAR) = d.player_id (team_squad.player_id —
--      bigint, details.player_id — varchar).
--   3. Final SELECT — извлекает foot через element_at(map_from_entries(...))
--      по title='Preferred foot' (Trino не поддерживает JSONPath filter
--      [?(...)]; correlated subquery с UNNEST тоже unsupported, поэтому
--      используем map-lookup идиому).
--
-- Coverage (probed 2026-05-15, APL 2025):
--   total=572 → 552 после NOT is_coach
--   height_cm:     93%  (team_squad)
--   date_of_birth: 100% (team_squad)
--   country:       100% (team_squad)
--   country_code:  100% (team_squad)
--   foot:           97% (details player_information_json)
-- =============================================================================

WITH details_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fotmob_player_details
    WHERE NOT is_coach
),

squad_dedup AS (
    SELECT
        CAST(player_id AS VARCHAR) AS player_id,
        league,
        season,
        height_cm,
        date_of_birth,
        country,
        country_code,
        shirt_number,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, league, season
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.fotmob_team_squad
),

-- Latest market value per (player_id, league, season).
-- bronze.fotmob_player_details.market_values_json shape:
--   {"values": [{"date": "ISO", "value": int, "currency": "EUR", ...}, ...]}.
-- UNNEST values array → MAX_BY(value, date) даёт самую свежую точку timeline.
-- Полная история уйдёт в gold.fct_player_market_value (issue #11).
mv_latest AS (
    SELECT
        d.player_id,
        d.league,
        d.season,
        TRY_CAST(MAX_BY(json_extract_scalar(v, '$.value'),
                        json_extract_scalar(v, '$.date')) AS BIGINT) AS current_market_value_eur,
        MAX_BY(json_extract_scalar(v, '$.currency'),
               json_extract_scalar(v, '$.date'))                     AS market_value_currency
    FROM details_dedup d
    CROSS JOIN UNNEST(
        CAST(json_extract(d.market_values_json, '$.values') AS array<json>)
    ) AS t(v)
    WHERE d.rn = 1
      AND d.market_values_json IS NOT NULL
      AND d.market_values_json <> 'null'
      AND d.market_values_json <> '{}'
    GROUP BY d.player_id, d.league, d.season
)

SELECT
    d.player_id,
    d.name                                           AS player_name,

    -- ========= Time-invariant attributes from team_squad =========
    s.date_of_birth,
    CAST(s.height_cm AS INTEGER)                     AS height_cm,
    s.country                                        AS nationality,
    s.country_code,

    -- ========= Slowly-changing attributes (latest snapshot per-season) =========
    -- contract_end: меняется при подписании нового контракта. В Bronze хранится
    -- JSON-dumped dict `{"utcTime": "YYYY-MM-DDT00:00:00.000Z", "timezone": "UTC"}`;
    -- COALESCE fallback на сырое значение для редких scalar-форматов.
    -- Выходной тип — date (ISO-substring → CAST), NULL для безконтрактных.
    TRY_CAST(
        SUBSTR(
            COALESCE(
                json_extract_scalar(d.contract_end, '$.utcTime'),
                d.contract_end
            ),
            1, 10
        ) AS date
    )                                                AS contract_end,

    -- current_market_value_eur: latest точка из market_values_json (timeline);
    -- историю смотри в gold.fct_player_market_value (issue #11).
    mv.current_market_value_eur,
    mv.market_value_currency,

    -- Current club from FotMob (per-snapshot snapshot of primary team).
    d.primary_team_id,
    d.primary_team_name,

    -- ========= foot from player_information_json (only source) =========
    -- JSON shape: array of {value: {fallback: "Right"|"Left"|"Both", ...},
    --                       title: "Preferred foot", translationKey: ...}.
    -- Map title -> value.fallback, lookup по 'Preferred foot'.
    element_at(
        map_from_entries(
            transform(
                CAST(json_parse(d.player_information_json) AS array<json>),
                e -> ROW(
                    json_extract_scalar(e, '$.title'),
                    json_extract_scalar(e, '$.value.fallback')
                )
            )
        ),
        'Preferred foot'
    )                                                AS foot,

    CAST(s.shirt_number AS INTEGER)                  AS shirt_number,

    -- ========= Lineage =========
    GREATEST(
        d._ingested_at,
        COALESCE(s._ingested_at, d._ingested_at)
    )                                                AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    d.league,
    d.season

FROM details_dedup d
LEFT JOIN squad_dedup s
    ON  s.player_id = d.player_id
    AND s.league    = d.league
    AND s.season    = d.season
    AND s.rn = 1
LEFT JOIN mv_latest mv
    ON  mv.player_id = d.player_id
    AND mv.league    = d.league
    AND mv.season    = d.season
WHERE d.rn = 1
