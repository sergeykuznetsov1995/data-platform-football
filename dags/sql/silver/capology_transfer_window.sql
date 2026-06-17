-- =============================================================================
-- Silver: capology_transfer_window
-- =============================================================================
--
-- Клубный нетто-баланс трансферного окна из Capology. Один row per
-- (club_slug, league, season). `canonical_id` через silver.xref_team
-- (source='capology', non-orphan).
--
-- НОВЫЕ метрики, не выводимые из зарплат: income/expense/balance (нетто-расход)
-- + размер заявок (players), средний возраст (age), число легионеров (foreign).
-- Issue #603 (consume write-only bronze).
--
-- Bronze schema (контракт scripts/audit_bronze_columns.py; типы live-verified):
--   club_slug, club_name, club_code (varchar),
--   players (bigint), age (double), foreign (bigint),
--   {income,expense,balance,adjbalance}_{gbp,eur,usd} — bigint (balance может
--     быть отрицательным = нетто-расход),
--   league, season (varchar slug '2526'), _ingested_at (timestamp).
--   Bronze partitioning = (league, season). Live: 240 rows / 12 сезонов (APL).
--
-- Notes:
--   * (league, season) JOIN predicate против xref_team MANDATORY (fan-out guard).
--   * xref_team source_id = club_name → JOIN по club_name, НЕ slug.
--   * canonical_id NULLable: live 239/240 матчатся.
--   * `foreign` — зарезервированное слово Trino → двойные кавычки "foreign".
--   * balance отрицательный = нетто-расход — НЕ фильтруем.
--   * (club_slug, league, season) уникален live (0 дублей); ROW_NUMBER defensive.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY club_slug, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.capology_transfer_window b
        WHERE club_slug IS NOT NULL
    )
    WHERE rn = 1
),

xt AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_team
    WHERE source = 'capology'
      AND confidence <> 'orphan'
)

SELECT
    b.club_slug,
    xt.canonical_id,
    b.club_name,
    b.club_code,

    -- Активность окна.
    b.players,
    b.age,
    b."foreign",

    -- Доход от продаж.
    b.income_gbp,
    b.income_eur,
    b.income_usd,

    -- Расход на покупки.
    b.expense_gbp,
    b.expense_eur,
    b.expense_usd,

    -- Нетто-баланс (income - expense; отрицательный = нетто-расход).
    b.balance_gbp,
    b.balance_eur,
    b.balance_usd,

    -- Скорректированный баланс.
    b.adjbalance_gbp,
    b.adjbalance_eur,
    b.adjbalance_usd,

    b._ingested_at                                      AS _bronze_ingested_at,

    -- Partition keys last.
    b.league,
    b.season

FROM bronze_dedup b
LEFT JOIN xt
    ON xt.source_id = b.club_name
   AND xt.league    = b.league
   AND xt.season    = b.season
