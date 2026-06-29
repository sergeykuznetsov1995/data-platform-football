-- =============================================================================
-- Silver: transfermarkt_players
-- =============================================================================
--
-- Time-invariant snapshot игроков из Transfermarkt: рост, нога, дата рождения,
-- nationality, контракт, актуальный market value. Один row per
-- (player_id, league, season) — snapshot-grain совпадает с Bronze.
-- `canonical_id` подтягивается через silver.xref_player (source='transfermarkt',
-- non-orphan) и материализуется как сторонний ключ для Gold-витрин
-- (dim_player_attributes расширение, fct_player_market_value).
--
-- Bronze schema (типы уже корректные после парсера, см.
-- scrapers/transfermarkt/scraper.py:777-800):
--   player_id (varchar), player_slug (varchar),
--   name, position, foot, nationality, current_club_id, current_club_name (varchar),
--   dob, contract_until, market_value_last_update (date),
--   age, height_cm, market_value_eur (int/bigint),
--   league, season (varchar short-form '2526'), _ingested_at (timestamp).
--
-- Notes:
--   * (league, season) JOIN predicate против xref_player MANDATORY
--     (CLAUDE.md / feedback_xref_join_season_predicate.md).
--   * canonical_id остаётся NULLable: TM содержит игроков, которых FBref-spine
--     не покрывает (loan-out, новые трансферы, U21 без FBref-strата).
--     Live APL 2025/26: orphan rate ≈ 10.2% (55/528, per
--     feedback_xref_player_tm_capology.md).
--   * Bronze ingest mode = replace_partitions → ROW_NUMBER dedup defensive.
--   * Колонки переименованы под issue #60 DoD:
--       player_slug              → slug
--       market_value_eur         → current_market_value_eur
--       market_value_last_update → mv_last_update
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.transfermarkt_players b
        WHERE player_id IS NOT NULL
    )
    WHERE rn = 1
),

xp AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_player
    WHERE source = 'transfermarkt'
      AND confidence <> 'orphan'
      -- #788: canonical историзирован за ВСЕ сезоны (было current-season-only в
      -- #803). Резолвер больше не плодит fan-out на тонком историческом spine —
      -- ложные TM-совпадения демоутятся в tm_<id> orphan (исключены условием
      -- выше), поэтому дублей (canonical_id, mv_date) нет. JOIN ниже включает
      -- season-predicate, так что per-season canonical матчится точно.
)

SELECT
    b.player_id,
    xp.canonical_id,
    b.name,
    b.player_slug                              AS slug,
    b.position,

    b.dob,
    CAST(b.age AS INTEGER)                     AS age,
    CAST(b.height_cm AS INTEGER)               AS height_cm,
    b.foot,
    b.nationality,

    b.contract_until,
    CAST(b.market_value_eur AS BIGINT)         AS current_market_value_eur,
    b.market_value_last_update                 AS mv_last_update,

    b.current_club_id,
    b.current_club_name,

    b._ingested_at                             AS _bronze_ingested_at,

    -- Partition keys last (matching writer convention).
    b.league,
    b.season

FROM bronze_dedup b
LEFT JOIN xp
    ON xp.source_id = b.player_id
   AND xp.league    = b.league
   AND xp.season    = b.season
