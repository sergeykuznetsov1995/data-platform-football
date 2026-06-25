-- =============================================================================
-- Silver: transfermarkt_market_value_history
-- =============================================================================
--
-- Time-series рыночной стоимости игроков из Transfermarkt. Один row per
-- (player_id, mv_date, league, season) — каждая точка timeline = снимок MV
-- на конкретную дату с club_name/age на тот момент.
--
-- Открывает путь к Gold-витрине fct_player_market_value (issue #11).
-- canonical_id подтягивается через silver.xref_player (source='transfermarkt',
-- non-orphan); orphans остаются с canonical_id=NULL (TM orphan rate ≈ 10%).
--
-- Bronze schema (см. scrapers/transfermarkt/scraper.py:358-394):
--   player_id (varchar), mv_date (date), value_eur (int, nullable),
--   club_name (varchar), age (int), mv_raw (varchar, dropped — typed уже есть),
--   league, season ('2526'), _ingested_at (timestamp).
--
-- Notes:
--   * (league, season) JOIN predicate против xref_player MANDATORY
--     (CLAUDE.md / feedback_xref_join_season_predicate.md).
--   * canonical_id остаётся NULLable: TM orphan rate ≈ 10.2% APL 2025/26
--     (feedback_xref_player_tm_capology.md). DQ слой использует coverage(),
--     а не no_nulls(canonical_id), как в sibling transfermarkt_players.
--   * Bronze ingest mode = replace_partitions → ROW_NUMBER dedup defensive.
--   * mv_date может быть глубоко в прошлом (timeline идёт за всю карьеру),
--     при этом partition season = current ingest season ('2526').
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, mv_date, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.transfermarkt_market_value_history b
        WHERE player_id IS NOT NULL
          AND mv_date   IS NOT NULL
    )
    WHERE rn = 1
),

xp AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_player
    WHERE source = 'transfermarkt'
      AND confidence <> 'orphan'
      -- #803: canonical только за последний (текущий) сезон. На тонком
      -- историческом FBref-spine резолвер даёт ложные совпадения (один
      -- canonical → много игроков) → 24963 дубля (canonical_id, mv_date).
      -- История остаётся canonical=NULL до историзации xref (#788).
      AND season = (
          SELECT max(season)
          FROM iceberg.silver.xref_player
          WHERE source = 'transfermarkt'
      )
)

SELECT
    b.player_id,
    xp.canonical_id,

    b.mv_date,
    CAST(b.value_eur AS BIGINT)                AS value_eur,

    b.club_name,
    CAST(b.age AS INTEGER)                     AS age,

    b._ingested_at                             AS _bronze_ingested_at,

    -- Partition keys last (matching writer convention).
    b.league,
    b.season

FROM bronze_dedup b
LEFT JOIN xp
    ON xp.source_id = b.player_id
   AND xp.league    = b.league
   AND xp.season    = b.season
