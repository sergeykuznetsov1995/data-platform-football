-- =============================================================================
-- Silver: capology_contract_extensions
-- =============================================================================
--
-- Контрактные данные игроков из Capology (player-grain). Снимок один row per
-- (player_slug, league, season) — последний контракт сезона. `canonical_id`
-- через silver.xref_player (source='capology', non-orphan).
--
-- НОВОЕ vs capology_player_salaries: дата подписания (signed), истечения
-- (expiration), срок (years) и ПОЛНАЯ стоимость контракта (contract_total_*),
-- а не только годовая зарплата. Issue #603 (consume write-only bronze).
--
-- Bronze schema (контракт scripts/audit_bronze_columns.py; типы live-verified):
--   player_slug, player_name, club_slug, club_name (varchar),
--   signed, expiration (varchar ISO 'YYYY-MM-DD'), years (bigint),
--   {weekly,annual}_{gross,net}_*, {bonus,total,adjusted_total,contract_total}_
--     {gross,net}_{gbp,eur,usd} — bigint,
--   league, season (varchar slug '2526'), _ingested_at (timestamp).
--   Bronze partitioning = (league, season). Live: 819 rows / 8 сезонов (APL,
--   floor 1819 — pre-2018 URL отдают current data, см. scraper #321).
--
-- Notes:
--   * (league, season) JOIN predicate против xref_player MANDATORY (fan-out guard).
--   * xref_player source_id = player_slug (как в capology_player_salaries.sql).
--   * canonical_id NULLable: live 685/819 матчатся (~83.6%); orphans = игроки
--     с контрактом, но без FBref counterpart (structural, как salary orphans).
--   * Грейн (player_slug, league, season) имеет 9 дублей live: игроки с ДВУМЯ
--     подписаниями за сезон (один клуб, разные signed). Берём последний контракт
--     (ORDER BY signed DESC) — снимок «текущий контракт», паритет с player_salaries.
--   * signed/expiration: TRY(CAST AS DATE) — null-guard против пустых/битых строк.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_slug, league, season
                ORDER BY TRY(CAST(signed AS DATE)) DESC NULLS LAST,
                         _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.capology_contract_extensions b
        WHERE player_slug IS NOT NULL
    )
    WHERE rn = 1
),

xp AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_player
    WHERE source = 'capology'
      AND confidence <> 'orphan'
)

SELECT
    b.player_slug,
    xp.canonical_id,
    b.player_name,
    b.club_slug,
    b.club_name,

    -- Контрактные сроки.
    TRY(CAST(b.signed AS DATE))                         AS signed,
    TRY(CAST(b.expiration AS DATE))                     AS expiration,
    CAST(b.years AS INTEGER)                            AS years,

    -- Текущая годовая зарплата (gross).
    b.annual_gross_gbp,
    b.annual_gross_eur,
    b.annual_gross_usd,

    -- Полная стоимость контракта (за весь срок) — gross + net.
    b.contract_total_gross_gbp,
    b.contract_total_gross_eur,
    b.contract_total_gross_usd,
    b.contract_total_net_gbp,
    b.contract_total_net_eur,
    b.contract_total_net_usd,

    b._ingested_at                                      AS _bronze_ingested_at,

    -- Partition keys last.
    b.league,
    b.season

FROM bronze_dedup b
LEFT JOIN xp
    ON xp.source_id = b.player_slug
   AND xp.league    = b.league
   AND xp.season    = b.season
