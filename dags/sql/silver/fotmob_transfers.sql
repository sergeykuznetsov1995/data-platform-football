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
-- Источник Bronze (#930 cutover, см. scrapers/fotmob/parsers.py parse_transfers):
--   bronze.fotmob_transfer_events_current — тот же парсер, что у legacy
--     bronze.fotmob_transfers → event-колонки 1:1: player_id / from_club_id /
--     to_club_id (bigint), player_name / position_label / position_key (varchar),
--     transfer_date (varchar ISO), from_club_full_name / to_club_full_name
--     (varchar), fee_value (double), market_value (число; legacy — varchar),
--     on_loan (boolean), transfer_type_key / transfer_type_text (varchar);
--     плюс competition_id (VARCHAR!) вместо league и event_year вместо season.
--   ⚠ (снят 2026-07-21) fotmob_transfer_events_current существует и наполнен —
--     transfers отработали в бэкфилле #930 (158/158).
--   bronze.fotmob_transfers (замороженная legacy-таблица) — UNION ALL как
--     история: лента трансферов у источника скользящая, legacy наблюдал окна,
--     которых native уже не видел (parity #930: overlap ~44/200 на лигу).
--     Native при совпадении PK побеждает (src_priority в dedup).
--
-- Notes:
--   * `fee_text` НЕ переносим (native его заполняет, но контракт Silver прежний).
--   * league: native несёт competition_id (varchar) → CAST AS bigint + INNER
--     JOIN league_map (одновременно скоупит выдачу до legacy-поверхности 14 лиг;
--     расширение скоупа — отдельное решение, не cutover).
--   * season: native-стрим БЕЗ сезона (partition event_year); legacy season был
--     сезоном скрейпа. Выводим из transfer_date по клубному окну июль–июнь:
--     месяц >= 7 → год даты, иначе год-1; затем ТОТ ЖЕ legacy-слаг-CASE
--     ('2526', WC '2026'). СЕМАНТИЧЕСКИЙ сдвиг vs legacy зафиксирован в карте
--     cutover (§3.8) и PR. Ряды с нераспарсиваемой датой фильтруются (иначе
--     NULL в partition key season; legacy-фильтр был лишь IS NOT NULL).
--   * грейн: native = полная пагинация (legacy читал только page 1) → строк
--     станет больше, включая старые окна; season-атрибуция по transfer_date
--     раскладывает их по своим партициям.
--   * transfer_date в bronze — строка; TRY_CAST(SUBSTR(...,1,10) AS DATE) робастно
--     к 'YYYY-MM-DD' и к полному ISO-таймстемпу. Dedup идёт по СЫРОЙ строке.
--   * fee_value (double, NULL для free/нераскрытых) → fee_eur (BIGINT евро).
--   * market_value в native — source-native число → CAST AS varchar (контракт
--     Silver «сырой varchar, парсинг в Gold» сохраняется).
--   * on_loan (boolean) — нативный loan-флаг FotMob; transfer_type_key/text —
--     сырой тип ('transfer'/'loan'). Резолв player/club identity отложен в Gold
--     (charter §5) — храним numeric ids + имена клубов.
--   * Season → slug ('2526') тем же выражением, что fotmob_team_match.sql.
--   * dedup: _current уже дедуплицирован по transfer_event_id, но ключ Silver
--     крупнее (разные event_id могут схлопнуться в один PK) → defensive
--     ROW_NUMBER по PK сохраняем; _batch_id в native нет → ORDER BY
--     _observed_at DESC, _target_batch_id DESC.
--   * replace_partitions(['league','season']).
-- =============================================================================

WITH league_map(competition_id, league) AS (
    VALUES (47, 'ENG-Premier League'), (48, 'ENG-Championship'), (87, 'ESP-La Liga'),
           (54, 'GER-Bundesliga'), (55, 'ITA-Serie A'), (53, 'FRA-Ligue 1'),
           (57, 'NED-Eredivisie'), (61, 'POR-Primeira Liga'), (42, 'UEFA-Champions League'),
           (73, 'UEFA-Europa League'), (77, 'INT-World Cup'), (50, 'INT-European Championship'),
           (289, 'INT-Africa Cup of Nations'), (44, 'INT-Copa America')
),

events AS (
    SELECT
        t.player_id,
        t.player_name,
        t.transfer_date,
        TRY_CAST(SUBSTR(t.transfer_date, 1, 10) AS DATE) AS transfer_date_parsed,
        t.from_club_id,
        t.from_club_full_name,
        t.to_club_id,
        t.to_club_full_name,
        t.position_label,
        t.position_key,
        t.fee_value,
        CAST(t.market_value AS varchar) AS market_value,
        t.on_loan,
        t.transfer_type_key,
        t.transfer_type_text,
        lm.league,
        t._observed_at,
        t._target_batch_id,
        1 AS src_priority
    FROM iceberg.bronze.fotmob_transfer_events_current t
    JOIN league_map lm ON lm.competition_id = CAST(t.competition_id AS bigint)
    WHERE t.player_id IS NOT NULL
      AND TRY_CAST(SUBSTR(t.transfer_date, 1, 10) AS DATE) IS NOT NULL

    UNION ALL

    -- Замороженная legacy-история (лента скользящая — см. шапку). Скоуп тот же
    -- league_map; legacy несёт league-слаг напрямую.
    SELECT
        t.player_id,
        t.player_name,
        t.transfer_date,
        TRY_CAST(SUBSTR(t.transfer_date, 1, 10) AS DATE) AS transfer_date_parsed,
        t.from_club_id,
        t.from_club_full_name,
        t.to_club_id,
        t.to_club_full_name,
        t.position_label,
        t.position_key,
        t.fee_value,
        t.market_value,
        t.on_loan,
        t.transfer_type_key,
        t.transfer_type_text,
        lm.league,
        t._ingested_at AS _observed_at,
        t._batch_id AS _target_batch_id,
        0 AS src_priority
    FROM iceberg.bronze.fotmob_transfers t
    JOIN league_map lm ON lm.league = t.league
    WHERE t.player_id IS NOT NULL
      AND TRY_CAST(SUBSTR(t.transfer_date, 1, 10) AS DATE) IS NOT NULL
),

seasoned AS (
    SELECT
        e.*,
        -- Клубное окно июль–июнь: сезон = год-старта окна, в которое попала дата.
        CASE WHEN MONTH(e.transfer_date_parsed) >= 7
             THEN YEAR(e.transfer_date_parsed)
             ELSE YEAR(e.transfer_date_parsed) - 1
        END AS season_year
    FROM events e
),

bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, from_club_id, to_club_id, transfer_date, league, season_year
                ORDER BY src_priority DESC, _observed_at DESC, _target_batch_id DESC
            ) AS rn
        FROM seasoned s
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    b.player_id,
    b.player_name,
    b.transfer_date_parsed AS transfer_date,

    -- ===== Clubs (raw ids + full names; canonical resolution deferred to Gold) =====
    b.from_club_id,
    b.from_club_full_name,
    b.to_club_id,
    b.to_club_full_name,

    -- ===== Player / deal attributes =====
    b.position_label,
    b.position_key,
    CAST(b.fee_value AS BIGINT) AS fee_eur,
    CAST(b.market_value AS varchar) AS market_value,
    b.on_loan,
    b.transfer_type_key,
    b.transfer_type_text,

    -- ===== Lineage =====
    b._observed_at AS _bronze_ingested_at,

    -- ===== Partition keys (season → slug to match other Silver tables) =====
    b.league,
    -- #913 Phase 2
    CASE WHEN b.league = 'INT-World Cup'
         THEN LPAD(CAST(b.season_year AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(b.season_year, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(b.season_year + 1, 100) AS varchar), 2, '0')
    END AS season

FROM bronze_dedup b
