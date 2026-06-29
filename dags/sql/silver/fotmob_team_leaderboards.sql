-- =============================================================================
-- Silver: fotmob_team_leaderboards
-- =============================================================================
--
-- Long-form: one row per (team_id, stat_category_group, stat_name, league,
-- season) — conform-only проекция сезонных командных лидербордов из FotMob
-- (index /api/data/leagues + полные списки data.fotmob.com/stats/.../*.json).
-- Грейн bronze сохраняем как есть (charter §1 grain rule) — это НЕ PIVOT в wide
-- и НЕ rollup: место (rank) + значение метрики (stat_value) на команду в каждой
-- статистической категории.
--
-- Источник Bronze (см. scrapers/fotmob/scraper.py read_team_leaderboards):
--   bronze.fotmob_team_leaderboards — team_id (bigint), team_name (varchar),
--     stat_category_group / stat_category_header / stat_name (varchar),
--     rank / stat_value_count / matches_played / minutes_played (bigint),
--     stat_value / sub_stat_value (double), league (varchar),
--     season (bigint year-start).
--
-- Notes:
--   * Низкоценные колонки НЕ переносим: participant_name (= team_name),
--     team_color, country_code.
--   * Dedup key включает stat_category_group: один stat_name может встречаться в
--     нескольких группах — без группы в ключе разные категории схлопнулись бы.
--   * Резолв canonical_id отложен в Gold (charter §5) — храним numeric team_id +
--     team_name.
--   * Season → slug ('2526') тем же выражением, что fotmob_team_match.sql.
--   * replace_partitions(['league','season']) → ROW_NUMBER dedup defensive.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            l.*,
            ROW_NUMBER() OVER (
                PARTITION BY team_id, stat_category_group, stat_name, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.fotmob_team_leaderboards l
        WHERE team_id IS NOT NULL
          AND stat_name IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    b.team_id,
    b.team_name,
    b.stat_category_group,
    b.stat_name,

    -- ===== Leaderboard values =====
    b.rank,
    b.stat_value,
    b.sub_stat_value,
    b.stat_value_count,
    b.matches_played,
    b.minutes_played,
    b.stat_category_header,

    -- ===== Lineage =====
    b._ingested_at AS _bronze_ingested_at,

    -- ===== Partition keys (season → slug to match other Silver tables) =====
    b.league,
    LPAD(CAST(MOD(b.season,     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(b.season + 1, 100) AS varchar), 2, '0') AS season

FROM bronze_dedup b
