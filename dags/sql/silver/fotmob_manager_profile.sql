-- =============================================================================
-- Silver: fotmob_manager_profile
-- =============================================================================
--
-- Time-invariant snapshot per (player_id, league, season) для ТРЕНЕРОВ (coaches).
-- Зеркало silver.fotmob_player_profile, но source-of-truth для тренера — строка
-- живого ростера `squad_snapshots` с `member_type='coach'`. Не каждый coachId
-- имеет строку в player_snapshots, поэтому player snapshot разрешён только как
-- LEFT-fallback для name / birth_date и никогда не задаёт зерно таблицы.
--
-- Назначение — обогатить gold.dim_manager атрибутами nationality / dob (issue #434).
-- xref_manager уже несёт coachId в source_id (source='fotmob'); dim_manager
-- связывает canonical_id ↔ coachId и подтягивает отсюда nationality/dob.
--
-- Zerno: (player_id, league, season). `player_id` = FotMob coachId — СОВПАДАЕТ с
-- silver.xref_manager.source_id (source='fotmob'), который берётся из той же
-- строки bronze.fotmob_squad_snapshots_current.member_id.
--
-- Cutover #930: legacy bronze.fotmob_player_details / fotmob_team_squad →
-- native fotmob_player_snapshots_current / fotmob_squad_snapshots_current.
-- Native-снапшоты ГЛОБАЛЬНЫ (без league/season) → сезонный скоуп тренера
-- реконструируется каркасом [CUTOVER-FRAMEWORK] ниже: клуб текущего сезона
-- (season_teams × squad, member_type='coach'). Исторической глубины тренеров
-- в native нет — как и в legacy (squad был снапшотом «сейчас»).
--
-- Sources (all from iceberg.bronze):
--   fotmob_squad_snapshots_current (sq) — ОБЯЗАТЕЛЬНЫЙ driver,
--                                member_type='coach': member_id (= coachId),
--                                member_name, date_of_birth и country из
--                                member_json $.cname. team_id varchar →
--                                CAST AS bigint к coach_scope.
--   fotmob_player_snapshots_current (ps) — ОПЦИОНАЛЬНЫЙ LEFT-fallback для
--                                name / birth_date, если эти поля пусты в sq.
--   fotmob_season_teams_current / fotmob_competition_seasons_current —
--                                каркас сезонного скоупа (см. блок ниже).
--
-- Pipeline (симметрично fotmob_player_profile):
--   1. [CUTOVER-FRAMEWORK] league_map → season_axis → … → coach_scope.
--   2. squad_dedup — coach_scope × squad_snapshots_current; именно эта ветка
--      задаёт одну строку на (coachId, league, season).
--   3. snapshot_fallback — глобальный defensive-dedup player snapshot.
--   4. Final SELECT  — name/dob = COALESCE(squad, optional snapshot fallback);
--      nationality = squad.country. dob/nationality остаются varchar passthrough
--      (bronze хранит ISO-строки) — gold.dim_manager делает TRY_CAST(.. AS DATE).
--      season — ТОЛЬКО из season_axis (слаг считается в одном месте каркаса).
-- =============================================================================

WITH
-- ============================================================================
-- [CUTOVER-FRAMEWORK #930] Сезонный скоуп для глобальных native-снапшотов.
-- Синхронизируемая копия: НЕ менять имена CTE и выражение season-слага.
-- League-map source of truth: configs/fotmob/competitions.json.
-- Season-scope contract: docs/architecture/fotmob-native-silver.md
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

-- Optional profile fallback. It is intentionally global and cannot create a
-- manager row: the final SELECT is driven by squad_dedup and LEFT JOINs this CTE.
snapshot_fallback AS (
    SELECT
        ps.player_id,
        ps.name,
        ps.birth_date,
        ROW_NUMBER() OVER (
            PARTITION BY ps.player_id
            ORDER BY ps._observed_at DESC, ps._target_batch_id DESC
        ) AS rn
    FROM iceberg.bronze.fotmob_player_snapshots_current ps
    WHERE ps.is_coach
),

-- squad_dedup: driver + атрибуты тренера из живого ростера клуба сезона.
-- member_type='coach' — замена legacy role='coach' (cutover-mapping §3.4);
-- country ← member_json $.cname (типизированной колонки в native нет).
squad_dedup AS (
    SELECT
        sq.member_id AS player_id,         -- varchar (= legacy CAST(player_id AS VARCHAR))
        sq.member_name AS name,
        cs.league,
        cs.season,
        json_extract_scalar(sq.member_json, '$.cname') AS country,
        sq.date_of_birth,
        sq._observed_at,
        ROW_NUMBER() OVER (
            PARTITION BY sq.member_id, cs.league, cs.season
            ORDER BY sq._observed_at DESC, sq._target_batch_id DESC
        ) AS rn
    FROM coach_scope cs
    JOIN iceberg.bronze.fotmob_squad_snapshots_current sq
      ON  sq.member_id = cs.coach_id
      AND CAST(sq.team_id AS bigint) = cs.team_id
      AND sq.member_type = 'coach'
)

SELECT
    s.player_id,
    COALESCE(
        NULLIF(TRIM(s.name), ''),
        NULLIF(TRIM(d.name), '')
    )                                                 AS name,

    -- dob: squad_snapshots — основной источник (structured), birth_date из
    -- player_snapshots — fallback. Оба varchar (ISO) → gold делает TRY_CAST.
    COALESCE(
        NULLIF(TRIM(s.date_of_birth), ''),
        NULLIF(TRIM(d.birth_date), '')
    )                                                 AS date_of_birth,
    s.country                                        AS nationality,

    -- ========= Lineage =========
    -- _batch_id/_ingested_at в native нет — lineage-замена _observed_at.
    s._observed_at                                   AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season — слаг из season_axis каркаса (ЕДИНСТВЕННОЕ место вычисления).
    s.league,
    s.season

FROM squad_dedup s
LEFT JOIN snapshot_fallback d
    ON  d.player_id = s.player_id
    AND d.rn = 1
WHERE s.rn = 1
