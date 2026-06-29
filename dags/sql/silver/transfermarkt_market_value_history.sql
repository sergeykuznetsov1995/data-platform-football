-- =============================================================================
-- Silver: transfermarkt_market_value_history
-- =============================================================================
--
-- Time-series рыночной стоимости игроков из Transfermarkt. Один row per
-- (player_id, mv_date) — каждая точка timeline = снимок MV на конкретную дату
-- с club_name/age на тот момент.
--
-- Открывает путь к Gold-витрине fct_player_market_value (issue #11).
-- canonical_id подтягивается через silver.xref_player (source='transfermarkt',
-- non-orphan); orphans остаются с canonical_id=NULL (TM orphan rate ≈ 13%).
--
-- Bronze schema (см. scrapers/transfermarkt/scraper.py:358-394):
--   player_id (varchar), mv_date (date), value_eur (int, nullable),
--   club_name (varchar), age (int), mv_raw (varchar, dropped — typed уже есть),
--   league, season ('2526'), _ingested_at (timestamp).
--
-- #835 ИСТОРИЗАЦИЯ: Bronze кладёт ПОЛНУЮ карьерную MV-историю игрока в КАЖДЫЙ
-- сезонный snapshot, поэтому одна (player_id, mv_date) повторяется во всех
-- season-партициях (live #788: ×3.18 избыточность). Раньше canonical джойнился
-- только за max(season) (как #803) → игроки, ушедшие из АПЛ, теряли canonical
-- во всех строках и выпадали из Gold. Теперь:
--   1. bronze_dedup схлопывает кросс-сезонные дубли до ОДНОЙ строки на
--      (player_id, mv_date) — freshest snapshot wins.
--   2. season ВЫВОДИТСЯ из mv_date (футбольный сезон, Aug–Jun), НЕ из
--      bronze.season (там сезон = окно скрейпа, одинаков для всей карьеры).
--   3. canonical берётся per-season за сезон самой точки. Дублей
--      (canonical_id, mv_date) нет по построению: per-season two-pass dedup
--      из #788 гарантирует один source_id на canonical в каждом (league,season).
-- Точки за сезоны вне АПЛ-xref (до прихода / после ухода) → canonical_id=NULL.
--
-- Notes:
--   * (league, season) JOIN predicate против xref_player MANDATORY
--     (CLAUDE.md / feedback_xref_join_season_predicate.md). season здесь =
--     ВЫВЕДЕННЫЙ из mv_date (см. bronze_dedup), а не bronze.season.
--   * canonical_id остаётся NULLable: TM orphan rate ≈ 13% APL 2025/26
--     (feedback_xref_player_tm_capology.md). DQ слой использует coverage(),
--     а не no_nulls(canonical_id), как в sibling transfermarkt_players.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT
        player_id,
        mv_date,
        value_eur,
        club_name,
        age,
        league,
        -- #835: футбольный сезон точки из mv_date (Aug–Jun), short-form '2122'
        -- как в xref_player. Месяц ≥ 7 (июль) → сезон стартует в этом году.
        substr(cast(if(month(mv_date) >= 7, year(mv_date), year(mv_date) - 1) AS varchar), 3, 2)
            || substr(cast(if(month(mv_date) >= 7, year(mv_date), year(mv_date) - 1) + 1 AS varchar), 3, 2)
                                                       AS season,
        _ingested_at
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, mv_date
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
      -- #835: историзировано — сезон точки выведен из mv_date (см. bronze_dedup),
      -- canonical джойнится per-season за соответствующий сезон. Резолвер демоутит
      -- ложные TM-совпадения в tm_<id> orphan (исключены условием выше), а JOIN
      -- ниже включает season-predicate → дублей (canonical_id, mv_date) нет.
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
