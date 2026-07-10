-- =============================================================================
-- Silver: sofascore_league_table   (from bronze.sofascore_league_table)
-- =============================================================================
--
-- One row per (league, season, team_name, group_id) — conform-only проекция
-- турнирной таблицы из SofaScore. Итоговые показатели сезона (игры / победы /
-- голы / очки). Для турниров SofaScore отдаёт НЕСКОЛЬКО блоков (WC: 12 групп +
-- «Third-placed teams»), и одна команда легально живёт в двух блоках — поэтому
-- group_id входит в ключ дедупа (#913 Phase 4).
-- Это season-grain SNAPSHOT, который SofaScore отдаёт напрямую, НЕ rollup из
-- match-grain — поэтому conform, а не Gold-факт (ср. charter §2 и
-- fotmob_team_standings.sql, который делает то же самое для FotMob).
--
-- Источник Bronze (см. scrapers/sofascore + run_sofascore_scraper.read_league_table):
--   bronze.sofascore_league_table — league (varchar), season (varchar slug),
--     team (varchar), mp/w/d/l/gf/ga/gd/pts (числовые), _ingested_at (timestamp).
--   APPEND-mode → dedup ROW_NUMBER здесь (перенесён из gold.fct_standings).
--
-- Notes:
--   * SofaScore НЕ отдаёт numeric team_id и position в этой выдаче — храним только
--     team_name; canonical-резолв и расчёт position отложены в Gold (charter §5).
--     Gold джойнит silver.xref_team по имени (source='sofascore').
--   * Season НЕ конвертируем — в bronze SofaScore уже slug '2526' (#404), в отличие
--     от FotMob (там year-start bigint → LPAD/MOD). Эмитим as-is.
--   * SofaScore Pts уже post-deduction; trust-check отложен (как было в Gold).
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                -- COALESCE("group",''): tournament standings come in blocks
                -- (WC groups + third-placed table) and one team appears in
                -- two blocks — dedup only WITHIN a block, or 8 group rows
                -- vanish (#913 Phase 4). "group" is reserved — keep quoted.
                PARTITION BY league, season, team, COALESCE("group", '')
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.sofascore_league_table s
        WHERE team IS NOT NULL
          AND trim(team) <> ''
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    b.team                                                AS team_name,

    -- ===== Standings =====
    CAST(b.mp  AS integer)                                AS played,
    CAST(b.w   AS integer)                                AS wins,
    CAST(b.d   AS integer)                                AS draws,
    CAST(b.l   AS integer)                                AS losses,
    CAST(b.gf  AS integer)                                AS goals_for,
    CAST(b.ga  AS integer)                                AS goals_against,
    CAST(b.gd  AS integer)                                AS goal_diff,
    CAST(b.pts AS integer)                                AS points,

    -- group_id for WC group stage (Фаза 4 #913). NULL for club leagues / knockout.
    -- "group" is a Trino reserved word — must stay quoted.
    b."group"                                             AS group_id,

    -- ===== Lineage =====
    b._ingested_at                                        AS _bronze_ingested_at,

    -- ===== Partition keys (season already slug '2526' in bronze, #404) =====
    b.league,
    b.season

FROM bronze_dedup b
