-- =============================================================================
-- Silver: fotmob_team_standings   (from bronze.fotmob_team_stats)
-- =============================================================================
--
-- One row per (team_id, league, season) — conform-only проекция турнирной таблицы
-- из FotMob (endpoint /api/data/leagues, секция table.all). Место + итоговые
-- показатели сезона (игры / победы / голы / очки). Это season-grain SNAPSHOT,
-- который FotMob отдаёт напрямую, НЕ rollup из match-grain — поэтому conform, а
-- не Gold-факт (ср. charter §2: sofascore_player_season_aggregate COMPLIANT, т.к.
-- bronze уже season-grain).
--
-- Источник Bronze (см. scrapers/fotmob/scraper.py read_team_season_stats):
--   bronze.fotmob_team_stats — team_id (bigint), team_name (varchar),
--     position/played/wins/draws/losses/goals_for/goals_against/goal_diff/points
--     (bigint), league (varchar), season (bigint year-start).
--
-- Notes:
--   * `form` НЕ переносим — 100% NULL (текущий продюсер не заполняет).
--   * Snapshot-семантика: если ingest прошёл в середине сезона, это позиция на
--     тот момент (played < полного календаря). Это data-quality-оговорка, не
--     нарушение charter — мы конформим выдачу источника как есть, без фильтров.
--   * Резолв canonical_id отложен в Gold (charter §5) — храним numeric team_id +
--     team_name; Gold джойнит silver.xref_team по имени.
--   * Season → slug ('2526') тем же выражением, что fotmob_team_match.sql.
--   * replace_partitions(['league','season']) → ROW_NUMBER dedup defensive.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY team_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.fotmob_team_stats s
        WHERE team_id IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    b.team_id,
    b.team_name,

    -- ===== Standings =====
    b.position,
    b.played,
    b.wins,
    b.draws,
    b.losses,
    b.goals_for,
    b.goals_against,
    b.goal_diff,
    b.points,

    -- group_id for WC (Фаза 4 #913). NULL for regular leagues.
    -- "group" is a Trino reserved word — must stay quoted.
    b."group"                                             AS group_id,

    -- ===== Lineage =====
    b._ingested_at AS _bronze_ingested_at,

    -- ===== Partition keys (season → slug to match other Silver tables) =====
    b.league,
    -- #913 Phase 2
    CASE WHEN b.league = 'INT-World Cup'
         THEN LPAD(CAST(b.season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(b.season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(b.season + 1, 100) AS varchar), 2, '0')
    END AS season

FROM bronze_dedup b
