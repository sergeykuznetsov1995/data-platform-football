-- =============================================================================
-- Silver: capology_player_salaries
-- =============================================================================
--
-- Snapshot зарплат игроков из Capology: weekly_gross_gbp, status, флаги
-- active/loan/verified. Один row per (player_slug, league, season) для ТЕКУЩЕГО
-- сезона — snapshot grain совпадает с Bronze после фильтра currency='GBP' +
-- (active OR loan) + season = MAX(season) (issue #632).
-- `canonical_id` подтягивается через silver.xref_player (source='capology',
-- non-orphan) и материализуется как сторонний ключ для Gold-витрин уровня
-- fct_player_season_stats (привязка salary→canonical player).
--
-- Bronze schema (типы корректные после парсера, см.
-- scrapers/capology/scraper.py:461-525):
--   player_slug, player_name, club_slug, club_name (varchar),
--   position, status, currency (varchar),
--   active, loan, verified (boolean),
--   age, weekly_gross_{gbp,eur,usd}, annual_gross_{gbp,eur,usd} (integer; salary
--     через accounting.formatMoney — все 3 валюты приходят inline в одной строке),
--   weekly_net_* / bonus_* / total_* / adjusted_* / country_code — не переносим,
--   league, season (varchar short-form '2526'), _ingested_at (timestamp).
--   Bronze partitioning = (league, season, currency); валюта остаётся 'GBP' —
--     EUR/USD живут в отдельных колонках (wide), а не в партициях (issue #195).
--
-- Notes:
--   * (league, season) JOIN predicate против xref_player MANDATORY
--     (CLAUDE.md / feedback_xref_join_season_predicate.md).
--   * Filter (active = true OR loan = true) — симметрия с xref_player_resolver
--     (см. _fetch_capology_players); Bronze несёт ~28% inactive строк
--     (release/academy/youth) без FBref counterpart, отбрасываем до Silver.
--   * Filter currency = 'GBP' — партиция всегда 'GBP' (одна строка/игрок);
--     EUR/USD теперь переносятся как weekly/annual_gross_{eur,usd} колонки
--     (issue #195), фильтр оставлен как guard против будущих партиций.
--   * Scope = ТОЛЬКО текущий сезон (issue #632): bronze_dedup фильтрует
--     (league, season) IN (SELECT league, MAX(season) ... GROUP BY league) —
--     максимум сезона per league, чтобы лига с отстающим бэкфиллом не
--     выпадала из Silver при мультилиговом ingest. Bronze копит
--     исторические партиции (backfill), но Silver-снепшот — current-season
--     only: xref-резолвер и DQ-пороги (canonical_coverage 0.80,
--     no_nulls weekly_gross_gbp) калиброваны под активный APL-сезон. Без
--     фильтра CTAS промоутил все ~13 сезонов → 7121 строк, coverage 67%,
--     600 undisclosed-NULL. MAX(season) трекает сезон без Python-параметра
--     (slug 'YYNN' сортируется лексикографически: '2526' > '2425').
--   * canonical_id остаётся NULLable: Capology orphan rate ≈ 9.5% live APL
--     2025/26 (50/526, post issue #84 HTML-decode + Bynoe-Gittens alias). Это
--     STRUCTURAL FLOOR: Capology = roster snapshot (контракты), FBref =
--     appearance data (минуты); ~50 игроков (suspended/injured/backup-GK/youth/
--     loan-outs) есть в Capology но без FBref counterpart → необъединяемы
--     никаким резолвер-алгоритмом. DoD #84 закрыт reinterpretation
--     (resolvable_orphan_rate = 0% post-fix).
--   * Bronze ingest mode = replace_partitions → ROW_NUMBER dedup defensive.
--   * annual_gross_gbp = weekly_gross_gbp * 52 (per issue #63 DoD); Bronze
--     annual_gross_gbp игнорируется ради формульной консистентности.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_slug, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.capology_player_salaries b
        WHERE player_slug IS NOT NULL
          AND currency = 'GBP'
          AND (active = true OR loan = true)
          -- issue #632: scope to current season only. Bronze копит исторические
          -- партиции (backfill); без этого фильтра CTAS промоутил все ~13 APL
          -- сезонов (7121 строк) → coverage 67%, 600 undisclosed-NULL.
          -- MAX(season) считается PER LEAGUE (GROUP BY): глобальный MAX молча
          -- выкидывал бы лигу, у которой ещё нет строк текущего сезона
          -- (разная глубина бэкфилла при мультилиговом ingest).
          AND (b.league, b.season) IN (
              SELECT league, MAX(season)
              FROM iceberg.bronze.capology_player_salaries
              WHERE currency = 'GBP'
              GROUP BY league
          )
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
    b.position,

    CAST(b.weekly_gross_gbp AS DECIMAL(12,2))           AS weekly_gross_gbp,
    CAST(b.weekly_gross_gbp * 52 AS DECIMAL(14,2))      AS annual_gross_gbp,
    CAST(b.weekly_gross_eur AS DECIMAL(12,2))           AS weekly_gross_eur,
    CAST(b.weekly_gross_eur * 52 AS DECIMAL(14,2))      AS annual_gross_eur,
    CAST(b.weekly_gross_usd AS DECIMAL(12,2))           AS weekly_gross_usd,
    CAST(b.weekly_gross_usd * 52 AS DECIMAL(14,2))      AS annual_gross_usd,

    CAST(b.age AS INTEGER)                              AS age,
    b.status,
    b.active,
    b.loan,
    b.verified,
    b.currency,

    b._ingested_at                                      AS _bronze_ingested_at,

    -- Partition keys last (matching writer convention).
    b.league,
    b.season

FROM bronze_dedup b
LEFT JOIN xp
    ON xp.source_id = b.player_slug
   AND xp.league    = b.league
   AND xp.season    = b.season
