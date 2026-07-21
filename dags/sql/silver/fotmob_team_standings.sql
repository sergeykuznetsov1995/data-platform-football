-- =============================================================================
-- Silver: fotmob_team_standings   (from bronze.fotmob_standings_current)
-- =============================================================================
--
-- One row per (team_id, league, season) — conform-only проекция турнирной таблицы
-- из FotMob (endpoint /api/data/leagues, секция table.all). Место + итоговые
-- показатели сезона (игры / победы / голы / очки). Это season-grain SNAPSHOT,
-- который FotMob отдаёт напрямую, НЕ rollup из match-grain — поэтому conform, а
-- не Gold-факт (ср. charter §2: sofascore_player_season_aggregate COMPLIANT, т.к.
-- bronze уже season-grain).
--
-- Источник Bronze (#930 cutover, см. scrapers/fotmob/repository.py
-- CURRENT_VIEW_SPECS и parsers._parse_standings):
--   bronze.fotmob_standings_current — team_id (bigint), team_name (varchar),
--     position/played/wins/draws/losses/goals_for/goals_against/goal_difference/
--     points (bigint), table_name/table_type (varchar), competition_id (bigint),
--     source_season_key (varchar, '2025/2026' | '2025').
--
-- Notes:
--   * Фильтр table_type = 'all' ОБЯЗАТЕЛЕН: native хранит и home/away/form/xg
--     контексты, legacy читал только table.all — без фильтра строки размножатся.
--   * goal_diff ← native goal_difference (переименование; контракт Gold прежний).
--   * `form` по-прежнему НЕ переносим: в native колонка заполнена, но контракт
--     silver прежний — добавление колонок это не cutover. Аналогично НЕ переносим
--     новые deduction / qualification_* / ongoing.
--   * league ← competition_id через league_map (обратная карта
--     FotMobScraper.LEAGUE_IDS); INNER JOIN одновременно скоупит выдачу
--     прежними 14 лигами — расширение скоупа НЕ входит в cutover.
--   * season ← year-start = substr(source_season_key, 1, 4); слаг НЕ выводить
--     из формы ключа (AFCON single-year обязан дать '2526', как legacy).
--   * group_id (Фаза 4 #913): legacy заполнял "group" только для
--     group_knockout-лиг (configs/medallion/competitions.yaml). В native имя
--     группы составной таблицы лежит в table_name, а для обычной лиги
--     table_name = имя лиги — НЕ утекать его в group_id: CASE по списку
--     group_knockout-лиг.
--   * Snapshot-семантика: если ingest прошёл в середине сезона, это позиция на
--     тот момент (played < полного календаря). Это data-quality-оговорка, не
--     нарушение charter — мы конформим выдачу источника как есть, без фильтров.
--   * Резолв canonical_id отложен в Gold (charter §5) — храним numeric team_id +
--     team_name; Gold джойнит silver.xref_team по имени.
--   * Season → slug ('2526') тем же выражением, что fotmob_team_match.sql.
--   * replace_partitions(['league','season']) → ROW_NUMBER dedup defensive
--     (после table_type='all' view-ключ даёт одну строку на команду;
--     ORDER BY _observed_at DESC, _target_batch_id DESC — native без _batch_id).
-- =============================================================================

WITH league_map(competition_id, league) AS (
    VALUES
        (47, 'ENG-Premier League'),
        (48, 'ENG-Championship'),
        (87, 'ESP-La Liga'),
        (54, 'GER-Bundesliga'),
        (55, 'ITA-Serie A'),
        (53, 'FRA-Ligue 1'),
        (57, 'NED-Eredivisie'),
        (61, 'POR-Primeira Liga'),
        (42, 'UEFA-Champions League'),
        (73, 'UEFA-Europa League'),
        (77, 'INT-World Cup'),
        (50, 'INT-European Championship'),
        (289, 'INT-Africa Cup of Nations'),
        (44, 'INT-Copa America')
),

native_scoped AS (
    SELECT
        s.team_id,
        s.team_name,
        s.position,
        s.played,
        s.wins,
        s.draws,
        s.losses,
        s.goals_for,
        s.goals_against,
        s.goal_difference AS goal_diff,
        s.points,
        -- group_id только для group_knockout-турниров (#913 Фаза 4): там
        -- table_name = имя группы; в обычной лиге table_name = имя лиги.
        CASE WHEN lm.league IN (
                 'INT-World Cup',
                 'INT-European Championship',
                 'INT-Africa Cup of Nations',
                 'INT-Copa America'
             )
             THEN s.table_name
        END AS group_id,
        s._observed_at,
        s._target_batch_id,
        lm.league,
        TRY_CAST(substr(s.source_season_key, 1, 4) AS integer) AS season_year
    FROM iceberg.bronze.fotmob_standings_current s
    JOIN league_map lm ON lm.competition_id = s.competition_id
    WHERE s.table_type = 'all'
      AND s.team_id IS NOT NULL
),

bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY team_id, league, season_year
                ORDER BY _observed_at DESC, _target_batch_id DESC
            ) AS rn
        FROM native_scoped s
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
    b.group_id,

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
