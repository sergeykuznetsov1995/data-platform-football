-- =============================================================================
-- Silver: fotmob_keeper_profile
-- =============================================================================
--
-- One wide row per goalkeeper / league / season.
--
-- Симметрично silver.fbref_keeper_profile, но из FotMob Bronze
-- (native *_current, cutover #930).
--
-- Sources (all from iceberg.bronze):
--   fotmob_player_snapshots_current — карточка игрока. ГЛОБАЛЬНЫЙ снапшот
--       (natural key player_id, без league/season) — членство игрока в
--       (лиге, сезоне) реконструирует каркас player_scope ниже
--       (cutover-карта §3.3): per-season вселенная из
--       fotmob_leaderboards_current (история) ∪ текущий состав
--       (fotmob_squad_snapshots_current × fotmob_season_teams_current).
--   fotmob_leaderboards_current     — сезонные статы в LONG-формате
--       (замена legacy fotmob_player_stats; WHERE participant_type = 'player';
--       zerno: participant_id × stat_name).
--
-- Filter: LOWER(position_description) = 'keeper' AND NOT is_coach
--          (вратари из FotMob player-снапшотов, без тренеров).
--
-- Содержит:
--   * Identity (per-season): player_id, player_name, primary_team_*
--   * Volume:        matches_played, minutes_played, fotmob_rating
--   * Goalkeeping:   clean_sheets, goals_conceded_per_90, save_percentage,
--                    saves_per_90, goals_prevented
--   * Distribution:  accurate_passes_per_90, accurate_long_balls_per_90
--   * Discipline:    yellow_cards, red_cards, fouls_per_90
--
-- Time-invariant атрибуты (birth_date/height/foot/country) НЕ хранятся здесь —
-- уйдут в silver.fotmob_player_profile (snapshot, T4 backlog).
-- =============================================================================

WITH
-- ============================================================================
-- [CUTOVER-FRAMEWORK #930] Сезонный скоуп для глобальных native-снапшотов.
-- Синхронизируемая копия: НЕ менять имена CTE и выражение season-слага.
-- Контракт: docs/architecture/fotmob-native-silver.md
-- ============================================================================

-- 1) Обратная карта competition_id -> legacy league. INNER JOIN к ней — это
--    ОДНОВРЕМЕННО скоуп-фильтр 14 лиг (native-каталог шире; см. пределы).
league_map (competition_id, league) AS (
    VALUES
        {{ fotmob_league_map_values_sql }}
),

-- 2) Ось сезонов: (competition_id, source_season_key) -> (league, season)
--    + флаг текущего сезона. ЕДИНСТВЕННОЕ место вычисления season-слага.
--    Правило season-ключа:
--      * season_year = TRY_CAST(substr(source_season_key, 1, 4) AS integer) —
--        корректно для обеих форм ключа ('2025/2026' -> 2025, '2025' -> 2025);
--      * слаг — СУЩЕСТВУЮЩИЙ legacy-CASE по league, НИКОГДА не по форме ключа
--        (single-year ключ AFCON обязан дать '2526', а не '2025' — иначе
--        битовое расхождение с legacy silver и разрыв JOIN'ов xref);
--      * is_current_season = is_selected OR is_latest (то, что источник сейчас
--        показывает по умолчанию; live: comp 47 -> '2026/2027').
season_axis AS (
    SELECT
        CAST(cs.competition_id AS bigint)  AS competition_id,   -- в этой таблице varchar
        cs.source_season_key,
        lm.league,
        TRY_CAST(substr(cs.source_season_key, 1, 4) AS integer) AS season_year,
        CASE WHEN lm.league = 'INT-World Cup'
             THEN LPAD(CAST(TRY_CAST(substr(cs.source_season_key, 1, 4) AS integer)
                            AS varchar), 4, '0')
             ELSE LPAD(CAST(MOD(TRY_CAST(substr(cs.source_season_key, 1, 4) AS integer),
                                100) AS varchar), 2, '0')
               || LPAD(CAST(MOD(TRY_CAST(substr(cs.source_season_key, 1, 4) AS integer) + 1,
                                100) AS varchar), 2, '0')
        END                                AS season,
        (cs.is_selected OR cs.is_latest)   AS is_current_season
    FROM iceberg.bronze.fotmob_competition_seasons_current cs
    JOIN league_map lm
      ON lm.competition_id = CAST(cs.competition_id AS bigint)
),

-- 3) Вселенная команд сезона (season_teams УЖЕ per-season, скоуп не нужен —
--    только league/season-атрибуция). team_id здесь bigint.
team_scope AS (
    SELECT
        sa.league,
        sa.season,
        sa.competition_id,
        sa.source_season_key,
        sa.is_current_season,
        st.team_id,                        -- bigint
        st.team_name
    FROM iceberg.bronze.fotmob_season_teams_current st
    JOIN season_axis sa
      ON  sa.competition_id    = st.competition_id
      AND sa.source_season_key = st.source_season_key
),

-- 4) ТЕКУЩИЙ сезон: членство из живого ростера. squad_snapshots — глобальный
--    снапшот «сейчас» (field_map: observed roster, never historical), поэтому
--    клеится ТОЛЬКО к is_current_season — иначе сегодняшний состав налипнет
--    на прошлые сезоны (класс бага seasonless_source_snapshot_replicated_as_
--    _historical из замороженного legacy-контура).
squad_scope AS (
    SELECT DISTINCT
        ts.league,
        ts.season,
        ts.competition_id,
        ts.source_season_key,
        ts.team_id,                        -- bigint
        sq.member_id,                      -- varchar (= legacy player_id в varchar)
        sq.member_type                     -- 'player' | 'coach' (live: только эти два)
    FROM team_scope ts
    JOIN iceberg.bronze.fotmob_squad_snapshots_current sq
      ON CAST(sq.team_id AS bigint) = ts.team_id   -- squad.team_id -- varchar!
    WHERE ts.is_current_season
),

-- 5) ИСТОРИЯ (и текущий сезон тоже): членство игроков из per-season
--    лидербордов — ровно та вселенная, по которой legacy silver реально
--    отдавал статистику (LEFT JOIN stats). competition_id здесь bigint.
lb_player_scope AS (
    SELECT DISTINCT
        sa.league,
        sa.season,
        sa.competition_id,
        sa.source_season_key,
        lb.team_id,                            -- bigint: клуб игрока в сезоне
        CAST(lb.participant_id AS varchar)     AS member_id
    FROM iceberg.bronze.fotmob_leaderboards_current lb
    JOIN season_axis sa
      ON  sa.competition_id    = lb.competition_id
      AND sa.source_season_key = lb.source_season_key
    WHERE lb.participant_type = 'player'
),

-- 6) Итоговое членство ИГРОКА в (лиге, сезоне): текущий сезон — ростер,
--    история — лидерборды. UNION дедупит пересечение. team_id намеренно НЕ
--    выносится (между ветками может отличаться при переходе внутри сезона) —
--    клуб потребители берут из своей ветки (squad_scope / lb_player_scope).
player_scope AS (
    SELECT DISTINCT league, season, competition_id, source_season_key, player_id
    FROM (
        SELECT league, season, competition_id, source_season_key,
               member_id AS player_id
        FROM squad_scope
        WHERE member_type = 'player'
        UNION ALL
        SELECT league, season, competition_id, source_season_key, member_id
        FROM lb_player_scope
    )
),

-- 7) Членство ТРЕНЕРА: только текущий сезон через клуб (в лидербордах
--    тренеров нет; исторической глубины тренеров в native НЕТ — как и в
--    legacy, где squad тоже был снапшотом «сейчас»).
coach_scope AS (
    SELECT
        league,
        season,
        competition_id,
        source_season_key,
        team_id,                           -- bigint
        member_id AS coach_id              -- varchar (= coachId = player_id)
    FROM squad_scope
    WHERE member_type = 'coach'
),

-- ОПЦИОНАЛЬНО (НЕ включать в cutover без решения владельца): расширение
-- исторической вселенной всеми, кто попал в player-статистику матчей.
-- Живьём шире лидербордов (comp 289 сезон '2023': 653 vs 510 игроков).
-- , match_player_scope AS (
--     SELECT DISTINCT
--         sa.league, sa.season, sa.competition_id, sa.source_season_key,
--         t.pid AS member_id                 -- varchar (ключи player_stats_json)
--     FROM iceberg.bronze.fotmob_match_payloads_current p
--     JOIN season_axis sa
--       ON  sa.competition_id    = CAST(p.competition_id AS bigint)  -- payloads: varchar!
--       AND sa.source_season_key = p.source_season_key
--     CROSS JOIN UNNEST(
--         map_keys(CAST(json_parse(p.player_stats_json) AS map(varchar, json)))
--     ) AS t(pid)
--     WHERE p.player_stats_json IS NOT NULL
--       AND p.player_stats_json <> 'null'
--       AND p.player_stats_json <> '{}'
-- )
-- ============================================================================
-- [/CUTOVER-FRAMEWORK]
-- ============================================================================

-- Драйвер — player_scope каркаса; снапшот подклеивается по id. *_current уже
-- дедуплицирован по natural key (player_id) — ROW_NUMBER здесь defensive:
-- игрок легально присутствует в нескольких (league, season), партиция по
-- тройке сохраняет зерно. Фильтры keeper/NOT is_coach — в финальном SELECT.
details_dedup AS (
    -- явные алиасы league/season: колонки приходят из scope-каркаса, а не из
    -- bronze-снапшота (важно для sqlglot-walker'а bronze-alignment теста)
    SELECT sc.league AS league, sc.season AS season, ps.*,
           ROW_NUMBER() OVER (
               PARTITION BY ps.player_id, sc.league, sc.season
               ORDER BY ps._observed_at DESC, ps._target_batch_id DESC
           ) AS rn
    FROM player_scope sc
    JOIN iceberg.bronze.fotmob_player_snapshots_current ps
      ON ps.player_id = sc.player_id
),

stats_dedup AS (
    -- Dedup long-format stats by (participant_id, league, season, stat_name)
    -- BEFORE the MAX-pivot — otherwise MAX(stat_value) over multiple snapshots
    -- returns the historical high for non-monotonic metrics (rating,
    -- save_percentage), not the latest value. _target_batch_id breaks ties
    -- (#464). ROW_NUMBER здесь ОБЯЗАТЕЛЕН (не defensive): natural key
    -- `_current`-view мельче legacy-ключа дедупа — содержит team_id, rank,
    -- top_list_index; смена клуба в сезоне даёт две строки на
    -- (participant_id, stat_name) (cutover-карта §3.5).
    SELECT * FROM (
        SELECT sa.league AS league, sa.season AS season, s.*,
               ROW_NUMBER() OVER (
                   PARTITION BY s.participant_id, sa.league, sa.season, s.stat_name
                   ORDER BY s._observed_at DESC, s._target_batch_id DESC
               ) AS rn
        FROM iceberg.bronze.fotmob_leaderboards_current s
        JOIN season_axis sa
          ON  sa.competition_id    = s.competition_id
          AND sa.source_season_key = s.source_season_key
        WHERE s.participant_type = 'player'
    ) WHERE rn = 1
),

stats_pivoted AS (
    SELECT
        CAST(participant_id AS VARCHAR) AS player_id,
        league,
        season,
        MAX(_observed_at) AS _stats_observed_at,
        MAX(matches_played) AS matches_played,
        -- Volume
        MAX(CASE WHEN stat_name = 'mins_played' THEN stat_value END) AS minutes_played,
        MAX(CASE WHEN stat_name = 'rating' THEN stat_value END) AS fotmob_rating,
        -- Goalkeeping
        MAX(CASE WHEN stat_name = 'clean_sheet' THEN stat_value END) AS clean_sheets,
        MAX(CASE WHEN stat_name = 'goals_conceded' THEN stat_value END) AS goals_conceded_per_90,
        MAX(CASE WHEN stat_name = '_save_percentage' THEN stat_value END) AS save_percentage,
        MAX(CASE WHEN stat_name = 'saves' THEN stat_value END) AS saves_per_90,
        MAX(CASE WHEN stat_name = '_goals_prevented' THEN stat_value END) AS goals_prevented,
        -- Distribution (GK passing)
        MAX(CASE WHEN stat_name = 'accurate_pass' THEN stat_value END) AS accurate_passes_per_90,
        MAX(CASE WHEN stat_name = 'accurate_long_balls' THEN stat_value END) AS accurate_long_balls_per_90,
        -- Discipline
        MAX(CASE WHEN stat_name = 'yellow_card' THEN stat_value END) AS yellow_cards,
        MAX(CASE WHEN stat_name = 'red_card' THEN stat_value END) AS red_cards,
        MAX(CASE WHEN stat_name = 'fouls' THEN stat_value END) AS fouls_per_90
    FROM stats_dedup
    GROUP BY participant_id, league, season
)

SELECT
    -- ========= Identity (per-season attributes only) =========
    d.player_id,
    d.name                                                            AS player_name,
    d.primary_team_id,
    d.primary_team_name,

    -- ========= Volume =========
    s.matches_played,
    s.minutes_played,
    s.fotmob_rating,

    -- ========= Goalkeeping =========
    s.clean_sheets,
    s.goals_conceded_per_90,
    s.save_percentage,
    s.saves_per_90,
    s.goals_prevented,

    -- ========= Distribution =========
    s.accurate_passes_per_90,
    s.accurate_long_balls_per_90,

    -- ========= Discipline =========
    s.yellow_cards,
    s.red_cards,
    s.fouls_per_90,

    -- ========= Lineage =========
    GREATEST(d._observed_at, COALESCE(s._stats_observed_at, d._observed_at)) AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season — слаг ('2425'/'2026') ИЗ КАРКАСА (season_axis) — файл его
    -- не пересчитывает (cutover-framework, чек №1).
    d.league,
    d.season

FROM details_dedup d
LEFT JOIN stats_pivoted s
    ON  d.player_id = s.player_id
    AND d.league    = s.league
    AND d.season    = s.season
WHERE d.rn = 1
  AND NOT d.is_coach
  AND LOWER(d.position_description) = 'keeper'
