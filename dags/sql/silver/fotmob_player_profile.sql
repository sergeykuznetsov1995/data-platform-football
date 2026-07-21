-- =============================================================================
-- Silver: fotmob_player_profile
-- =============================================================================
--
-- Time-invariant snapshot per (player_id, league, season). Хранит атрибуты
-- игрока, которые не меняются в течение сезона: рост, дата рождения,
-- национальность, ведущая нога, номер на футболке. `is_current_season`
-- отличает живой roster scope от исторической leaderboard-реконструкции.
--
-- Zerno: (player_id, league, season). Симметрично с silver.fotmob_player_season_profile
-- и silver.fotmob_keeper_profile. Для одного игрока в одном сезоне — один row;
-- если игрок появится в нескольких сезонах, у каждого свой row (атрибуты
-- de-facto константны, но per-season хранение упрощает downstream JOIN-ы).
--
-- Sources (all from iceberg.bronze, native #930 cutover):
--   fotmob_squad_snapshots_current  (sq) — основной источник: height_cm /
--                                date_of_birth / country (member_json $.cname) /
--                                country_code / shirt_number.
--   fotmob_player_snapshots_current (ps) — `foot` (Preferred foot) —
--                                единственное поле, которое отсутствует в squad
--                                и доступно ТОЛЬКО внутри player_information_json.
--                                Также `contract_end` (в native уже скаляр utcTime;
--                                COALESCE-fallback покрывает оба формата),
--                                market_values_json, primary_team_*.
--   Native-снапшоты ГЛОБАЛЬНЫ (без league/season) — сезонный скоуп
--   реконструируется каркасом [CUTOVER-FRAMEWORK #930] ниже:
--   season_teams_current × squad_snapshots_current (текущий сезон)
--   ∪ leaderboards_current (история). См. docs/architecture/fotmob-native-silver.md.
--
-- Pipeline:
--   1. Каркас-CTE (league_map … coach_scope) — сезонный скоуп + season-слаг
--      (season_axis — ЕДИНСТВЕННОЕ место вычисления слага).
--   2. details_dedup — player_scope × fotmob_player_snapshots_current.
--      Фильтр NOT is_coach (тренеры приходят с is_coach=true в том же снапшоте).
--      ROW_NUMBER defensive: `_current` даёт одну строку на player_id, но игрок
--      легально присутствует в нескольких (league, season).
--   3. squad_dedup   — squad_scope (member_type='player') ×
--      fotmob_squad_snapshots_current. JOIN-ключ member_id (varchar, совпадает
--      с player_snapshots.player_id) + CAST(sq.team_id AS bigint).
--      Для ИСТОРИЧЕСКИХ сезонов squad-половина честно даёт NULL (ростера
--      прошлых сезонов в native нет) — ожидаемо, см. framework §4.
--   4. Final SELECT — извлекает foot через element_at(map_from_entries(...))
--      по title='Preferred foot' (Trino не поддерживает JSONPath filter
--      [?(...)]; correlated subquery с UNNEST тоже unsupported, поэтому
--      используем map-lookup идиому).
--
-- Coverage (probed 2026-05-15, APL 2025, legacy-источники — исторический бейзлайн):
--   total=572 → 552 после NOT is_coach
--   height_cm:     93%  (squad)
--   date_of_birth: 100% (squad)
--   country:       100% (squad)
--   country_code:  100% (squad)
--   foot:           97% (player_information_json)
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
        ts.is_current_season,
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
        sa.is_current_season,
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
    SELECT DISTINCT league, season, competition_id, source_season_key,
                    is_current_season, player_id
    FROM (
        SELECT league, season, competition_id, source_season_key,
               is_current_season, member_id AS player_id
        FROM squad_scope
        WHERE member_type = 'player'
        UNION ALL
        SELECT league, season, competition_id, source_season_key,
               is_current_season, member_id
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
    SELECT sc.league AS league, sc.season AS season,
           sc.is_current_season AS scope_is_current_season, ps.*,
           ROW_NUMBER() OVER (
               PARTITION BY ps.player_id, sc.league, sc.season
               ORDER BY ps._observed_at DESC, ps._target_batch_id DESC
           ) AS rn
    FROM player_scope sc
    JOIN iceberg.bronze.fotmob_player_snapshots_current ps
      ON ps.player_id = sc.player_id
    WHERE NOT ps.is_coach
),

squad_dedup AS (
    SELECT
        sq.member_id AS player_id,         -- уже varchar (= player_snapshots.player_id)
        sc.league,
        sc.season,
        sq.height_cm,
        sq.date_of_birth,
        json_extract_scalar(sq.member_json, '$.cname') AS country,
        sq.country_code,
        sq.shirt_number,
        sq._observed_at,
        ROW_NUMBER() OVER (
            PARTITION BY sq.member_id, sc.league, sc.season
            ORDER BY sq._observed_at DESC, sq._target_batch_id DESC
        ) AS rn
    FROM squad_scope sc
    JOIN iceberg.bronze.fotmob_squad_snapshots_current sq
      ON  sq.member_id = sc.member_id
      AND CAST(sq.team_id AS bigint) = sc.team_id
    WHERE sc.member_type = 'player'
),

-- Latest market value per (player_id, league, season).
-- player_snapshots.market_values_json shape (тот же путь, что legacy):
--   {"values": [{"date": "ISO", "value": int, "currency": "EUR", ...}, ...]}.
-- UNNEST values array → MAX_BY(value, date) даёт самую свежую точку timeline.
-- Полная история уйдёт в gold.fct_player_market_value (issue #11).
mv_latest AS (
    SELECT
        d.player_id,
        d.league,
        d.season,
        TRY_CAST(MAX_BY(json_extract_scalar(v, '$.value'),
                        json_extract_scalar(v, '$.date')) AS BIGINT) AS current_market_value_eur,
        MAX_BY(json_extract_scalar(v, '$.currency'),
               json_extract_scalar(v, '$.date'))                     AS market_value_currency
    FROM details_dedup d
    CROSS JOIN UNNEST(
        CAST(json_extract(d.market_values_json, '$.values') AS array<json>)
    ) AS t(v)
    WHERE d.rn = 1
      AND d.market_values_json IS NOT NULL
      AND d.market_values_json <> 'null'
      AND d.market_values_json <> '{}'
    GROUP BY d.player_id, d.league, d.season
)

SELECT
    d.player_id,
    d.name                                           AS player_name,

    -- ========= Time-invariant attributes from squad snapshot =========
    s.date_of_birth,
    CAST(s.height_cm AS INTEGER)                     AS height_cm,
    s.country                                        AS nationality,
    s.country_code,

    -- ========= Slowly-changing attributes (latest snapshot per-season) =========
    -- contract_end: меняется при подписании нового контракта. В native уже
    -- скаляр utcTime; у legacy-строк бывал JSON-dumped dict `{"utcTime": ...}` —
    -- COALESCE покрывает оба формата (на скаляре json_extract_scalar даёт NULL).
    -- Выходной тип — date (ISO-substring → CAST), NULL для безконтрактных.
    TRY_CAST(
        SUBSTR(
            COALESCE(
                json_extract_scalar(d.contract_end, '$.utcTime'),
                d.contract_end
            ),
            1, 10
        ) AS date
    )                                                AS contract_end,

    -- current_market_value_eur: latest точка из market_values_json (timeline);
    -- историю смотри в gold.fct_player_market_value (issue #11).
    mv.current_market_value_eur,
    mv.market_value_currency,

    -- Current club from FotMob (per-snapshot snapshot of primary team).
    d.primary_team_id,
    d.primary_team_name,

    -- ========= foot from player_information_json (only source) =========
    -- JSON shape: array of {value: {fallback: "Right"|"Left"|"Both", ...},
    --                       title: "Preferred foot", translationKey: ...}.
    -- Map title -> value.fallback, lookup по 'Preferred foot'.
    element_at(
        map_from_entries(
            transform(
                CAST(json_parse(d.player_information_json) AS array<json>),
                e -> ROW(
                    json_extract_scalar(e, '$.title'),
                    json_extract_scalar(e, '$.value.fallback')
                )
            )
        ),
        'Preferred foot'
    )                                                AS foot,

    CAST(s.shirt_number AS INTEGER)                  AS shirt_number,

    -- TRUE only for the selected/latest native season. Historical rows are
    -- leaderboard-scoped and cannot honestly inherit live-roster attributes
    -- such as height; DQ coverage uses this flag to measure like with like.
    d.scope_is_current_season                        AS is_current_season,

    -- ========= Lineage =========
    -- Native lineage: _observed_at (в native `_ingested_at`-семантику несёт
    -- `_observed_at`; `_batch_id` не существует — cutover-mapping §2.3).
    GREATEST(
        d._observed_at,
        COALESCE(s._observed_at, d._observed_at)
    )                                                AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season-слаг ('2425'/'2026') считается ТОЛЬКО в season_axis каркаса —
    -- здесь готовые значения из скоупа.
    d.league,
    d.season

FROM details_dedup d
LEFT JOIN squad_dedup s
    ON  s.player_id = d.player_id
    AND s.league    = d.league
    AND s.season    = d.season
    AND s.rn = 1
LEFT JOIN mv_latest mv
    ON  mv.player_id = d.player_id
    AND mv.league    = d.league
    AND mv.season    = d.season
WHERE d.rn = 1
