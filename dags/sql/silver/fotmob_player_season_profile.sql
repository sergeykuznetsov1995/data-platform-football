-- =============================================================================
-- Silver: fotmob_player_season_profile
-- =============================================================================
--
-- One wide row per player / league / season.
--
-- Содержит ТОЛЬКО per-season атрибуты (клуб/позиция/номер сезона) и
-- статистику. Time-invariant поля игрока (birth_date, height, foot, country)
-- НЕ хранятся здесь — они уйдут в snapshot-таблицу silver.fotmob_player_profile
-- (T4 в task.md). Pass-through JSON (career_history, trophies, ...) удалены —
-- raw json остаётся в bronze, а структурированные представления при
-- необходимости заслуживают отдельных silver-таблиц.
--
-- Sources (all from iceberg.bronze; native #930 cutover):
--   fotmob_player_snapshots_current (d) — карточка игрока (ГЛОБАЛЬНЫЙ снапшот,
--                                          без league/season — членство игрока
--                                          в (лиге, сезоне) реконструируется
--                                          player_scope-каркасом ниже)
--   fotmob_leaderboards_current     (s) — сезонные статы в LONG-формате
--                                          (зерно: participant_id × stat_name),
--                                          WHERE participant_type = 'player'
--   + scope-каркас: fotmob_competition_seasons_current /
--     fotmob_season_teams_current / fotmob_squad_snapshots_current
--
-- Pipeline:
--   1. [CUTOVER-FRAMEWORK] — league_map + season_axis + player_scope
--                       (единственное место вычисления league/season).
--   2. details_dedup — player_scope × snapshots; ROW_NUMBER dedup на
--                       (player_id, league, season) — defensive: _current даёт
--                       одну строку на player_id, но игрок легально состоит в
--                       нескольких (league, season).
--   3. stats_dedup   — дедуп LONG-статов по (participant_id, league, season,
--                       stat_name) — ОБЯЗАТЕЛЕН: natural key _current-view
--                       мельче (содержит team_id/rank/top_list_index — смена
--                       клуба в сезоне даёт 2 строки; #464-семантика).
--   4. stats_pivoted — MAX(CASE WHEN stat_name='X') pivot long -> wide
--                       (~32 stat-колонки, без goalkeeping).
--   5. Final SELECT  — LEFT JOIN dedup ← pivot по (player_id, league, season).
--                       JOIN-key cast: CAST(participant_id AS VARCHAR).
--                       Фильтры: NOT is_coach (тренеры из FotMob приходят в тех
--                       же снапшотах с is_coach=true)
--                       AND LOWER(primary_position) != 'keeper'
--                       (вратари в отдельной silver.fotmob_keeper_profile).
--
-- Нормализация position: LOWER(...) — FotMob отдаёт inconsistent case
-- (Keeper vs keeper, и т.п.) и generic fallback'и (midfielder, defender,
-- forward) у части записей. LOWER даёт consistent lowercase. Capitalize
-- делается на BI-слое (Trino INITCAP отсутствует).
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

details_dedup AS (
    -- Драйвер — player_scope (членство в (лиге, сезоне)), снапшот
    -- подклеивается по id. ROW_NUMBER defensive: _current даёт одну строку
    -- на player_id, но игрок легально присутствует в нескольких
    -- (league, season) — партиция по тройке сохраняет зерно.
    -- Явные AS-алиасы: league/season приходят из scope-CTE, а не из bronze
    -- (иначе column-alignment тест припишет их fotmob_player_snapshots_current).
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
    -- returns the historical high for non-monotonic metrics (rating, per-90),
    -- not the latest value. ROW_NUMBER здесь ОБЯЗАТЕЛЕН (не defensive):
    -- natural key _current-view мельче legacy-ключа дедупа — содержит
    -- team_id/rank/top_list_index, смена клуба в сезоне даёт две строки на
    -- (participant_id, stat_name) (#464-семантика, cutover-mapping §3.5).
    -- _target_batch_id breaks _observed_at ties (native: _batch_id не существует).
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
        MAX(_observed_at) AS _stats_ingested_at,
        MAX(matches_played) AS matches_played,
        -- Top Stat
        MAX(CASE WHEN stat_name = 'mins_played' THEN stat_value END) AS minutes_played,
        MAX(CASE WHEN stat_name = 'goals' THEN stat_value END) AS goals,
        MAX(CASE WHEN stat_name = 'goal_assist' THEN stat_value END) AS assists,
        MAX(CASE WHEN stat_name = '_goals_and_goal_assist' THEN stat_value END) AS goals_assists,
        MAX(CASE WHEN stat_name = 'rating' THEN stat_value END) AS fotmob_rating,
        -- Attacking
        MAX(CASE WHEN stat_name = 'goals_per_90' THEN stat_value END) AS goals_per_90,
        MAX(CASE WHEN stat_name = 'expected_goals' THEN stat_value END) AS expected_goals,
        MAX(CASE WHEN stat_name = 'expected_goals_per_90' THEN stat_value END) AS expected_goals_per_90,
        MAX(CASE WHEN stat_name = 'expected_goalsontarget' THEN stat_value END) AS expected_goals_on_target,
        MAX(CASE WHEN stat_name = 'expected_assists' THEN stat_value END) AS expected_assists,
        MAX(CASE WHEN stat_name = 'expected_assists_per_90' THEN stat_value END) AS expected_assists_per_90,
        MAX(CASE WHEN stat_name = '_expected_goals_and_expected_assists_per_90' THEN stat_value END) AS xg_xa_per_90,
        MAX(CASE WHEN stat_name = 'total_scoring_att' THEN stat_value END) AS shots_per_90,
        MAX(CASE WHEN stat_name = 'ontarget_scoring_att' THEN stat_value END) AS shots_on_target_per_90,
        MAX(CASE WHEN stat_name = 'total_att_assist' THEN stat_value END) AS chances_created,
        MAX(CASE WHEN stat_name = 'big_chance_created' THEN stat_value END) AS big_chances_created,
        MAX(CASE WHEN stat_name = 'big_chance_missed' THEN stat_value END) AS big_chances_missed,
        MAX(CASE WHEN stat_name = 'accurate_pass' THEN stat_value END) AS accurate_passes_per_90,
        MAX(CASE WHEN stat_name = 'accurate_long_balls' THEN stat_value END) AS accurate_long_balls_per_90,
        MAX(CASE WHEN stat_name = 'won_contest' THEN stat_value END) AS successful_dribbles_per_90,
        MAX(CASE WHEN stat_name = 'penalty_won' THEN stat_value END) AS penalties_won,
        -- Defending
        MAX(CASE WHEN stat_name = 'defensive_contributions' THEN stat_value END) AS defensive_actions_per_90,
        MAX(CASE WHEN stat_name = 'total_tackle' THEN stat_value END) AS tackles_per_90,
        MAX(CASE WHEN stat_name = 'interception' THEN stat_value END) AS interceptions_per_90,
        MAX(CASE WHEN stat_name = 'effective_clearance' THEN stat_value END) AS clearances_per_90,
        MAX(CASE WHEN stat_name = 'ball_recovery' THEN stat_value END) AS recoveries_per_90,
        MAX(CASE WHEN stat_name = 'outfielder_block' THEN stat_value END) AS blocks_per_90,
        MAX(CASE WHEN stat_name = 'poss_won_att_3rd' THEN stat_value END) AS poss_won_final_third_per_90,
        MAX(CASE WHEN stat_name = 'penalty_conceded' THEN stat_value END) AS penalties_conceded,
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
    LOWER(d.position_description) AS primary_position,
    d.primary_team_id,
    d.primary_team_name,

    -- ========= Stats (PIVOT long -> wide) =========
    s.matches_played,
    s.minutes_played,
    s.goals,
    s.assists,
    s.goals_assists,
    s.fotmob_rating,
    s.goals_per_90,
    s.expected_goals,
    s.expected_goals_per_90,
    s.expected_goals_on_target,
    s.expected_assists,
    s.expected_assists_per_90,
    s.xg_xa_per_90,
    s.shots_per_90,
    s.shots_on_target_per_90,
    s.chances_created,
    s.big_chances_created,
    s.big_chances_missed,
    s.accurate_passes_per_90,
    s.accurate_long_balls_per_90,
    s.successful_dribbles_per_90,
    s.penalties_won,
    s.defensive_actions_per_90,
    s.tackles_per_90,
    s.interceptions_per_90,
    s.clearances_per_90,
    s.recoveries_per_90,
    s.blocks_per_90,
    s.poss_won_final_third_per_90,
    s.penalties_conceded,
    s.yellow_cards,
    s.red_cards,
    s.fouls_per_90,

    -- ========= Lineage =========
    GREATEST(d._observed_at, COALESCE(s._stats_ingested_at, d._observed_at)) AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season — слаг ('2425') из season_axis (каркас): ЕДИНСТВЕННОЕ место
    -- вычисления season-слага (#913 Phase 2 CASE живёт там).
    d.league,
    d.season

FROM details_dedup d
LEFT JOIN stats_pivoted s
    ON  d.player_id = s.player_id
    AND d.league    = s.league
    AND d.season    = s.season
WHERE d.rn = 1
  AND NOT d.is_coach
  AND LOWER(d.position_description) <> 'keeper'
