-- =============================================================================
-- Silver: fotmob_transfers
-- =============================================================================
--
-- Event-grain: one row per (player_id, from_club_id, to_club_id, transfer_date,
-- league, season) — conform-only проекция трансферных событий из FotMob
-- (endpoint /api/data/transfers по лиге). Игрок переходит из клуба в клуб; даже
-- двойной переход за сезон = две строки. Conform-структура повторяет
-- silver.transfermarkt_transfers, но FotMob отдаёт on_loan-флаг и fee_value
-- готовыми, поэтому проще (без alias-резолва клубов — он отложен в Gold).
--
-- Источник Bronze (см. scrapers/fotmob/scraper.py read_transfers):
--   bronze.fotmob_transfers — player_id / from_club_id / to_club_id (bigint),
--     player_name / position_label / position_key (varchar),
--     transfer_date (varchar ISO), from_club_full_name / to_club_full_name
--     (varchar), fee_value (double), market_value (varchar), on_loan (boolean),
--     transfer_type_key / transfer_type_text (varchar), league (varchar),
--     season (bigint year-start).
--
-- Notes:
--   * `fee_text` НЕ переносим — 100% NULL (текущий продюсер не заполняет).
--   * transfer_date в bronze — строка; TRY_CAST(SUBSTR(...,1,10) AS DATE) робастно
--     к 'YYYY-MM-DD' и к полному ISO-таймстемпу. Dedup идёт по СЫРОЙ строке.
--   * fee_value (double, NULL ~63% для free/нераскрытых) → fee_eur (BIGINT евро).
--   * market_value оставляем сырым varchar (формат вариативен) — парсинг в Gold.
--   * on_loan (boolean) — нативный loan-флаг FotMob; transfer_type_key/text —
--     сырой тип ('transfer'/'loan'). Резолв player/club identity отложен в Gold
--     (charter §5) — храним numeric ids + имена клубов.
--   * Season → slug ('2526') тем же выражением, что fotmob_team_match.sql.
--   * replace_partitions(['league','season']) → ROW_NUMBER dedup defensive.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, from_club_id, to_club_id, transfer_date, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.fotmob_transfers t
        WHERE player_id IS NOT NULL
          AND transfer_date IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    b.player_id,
    b.player_name,
    TRY_CAST(SUBSTR(b.transfer_date, 1, 10) AS DATE) AS transfer_date,

    -- ===== Clubs (raw ids + full names; canonical resolution deferred to Gold) =====
    b.from_club_id,
    b.from_club_full_name,
    b.to_club_id,
    b.to_club_full_name,

    -- ===== Player / deal attributes =====
    b.position_label,
    b.position_key,
    CAST(b.fee_value AS BIGINT) AS fee_eur,
    b.market_value,
    b.on_loan,
    b.transfer_type_key,
    b.transfer_type_text,

    -- ===== Lineage =====
    b._ingested_at AS _bronze_ingested_at,

    -- ===== Partition keys (season → slug to match other Silver tables) =====
    b.league,
    LPAD(CAST(MOD(b.season,     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(b.season + 1, 100) AS varchar), 2, '0') AS season

FROM bronze_dedup b
