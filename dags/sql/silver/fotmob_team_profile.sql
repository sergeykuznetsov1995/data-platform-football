-- =============================================================================
-- Silver: fotmob_team_profile
-- =============================================================================
--
-- One row per (team_id, league, season) — conform-only проекция профиля команды
-- из FotMob (endpoint /api/data/teams). Snapshot на момент ingest'а: страна,
-- домашний стадион, текущее место в таблице. Питает будущий gold.dim_team
-- (FotMob-ветка) — резолв canonical_id отложен в Gold (charter §5), здесь храним
-- сырой numeric team_id + team_name (Gold джойнит silver.xref_team по имени, как
-- fct_team_match для fotmob_team_match).
--
-- Источники Bronze (native cutover #930, см. scrapers/fotmob/service.py
-- _team_rows и cutover-карту §3.7):
--   bronze.fotmob_team_snapshots_current — ГЛОБАЛЬНЫЙ снапшот профиля
--     (natural key team_id, БЕЗ league/season): team_id (varchar), team_name,
--     country_code (= legacy country), venue_name/venue_city/venue_capacity
--     (varchar), details_json/overview_json (verbatim секции details/overview).
--   bronze.fotmob_season_teams_current — вселенная команд сезона
--     (competition_id bigint, source_season_key, team_id bigint) — сезонный
--     скоуп (league, season) для безсезонного снапшота.
--   bronze.fotmob_standings_current — position (WHERE table_type = 'all')
--     вместо legacy overview_table_position: для активного сезона эквивалентно
--     «на момент скрейпа», для истории — финальная позиция сезона.
--
-- Notes:
--   * Снапшотные/низкоценные секции НЕ переносим: next_match / last_match,
--     overview_season, history_seasons_count (при нужде — overview_json/history_json).
--   * league — реконструкция из competition_id по статической карте
--     (FotMobScraper.LEAGUE_IDS); INNER JOIN к карте одновременно скоупит
--     выдачу прежними 14 лигами (native-каталог шире — расширение скоупа
--     не входит в cutover).
--   * season — из source_season_key ('2025/2026' клубный | '2025' single-year):
--     год-старта = substr(key, 1, 4); слаг ('2526' / '2026' для WC) — тем же
--     выражением, что fotmob_team_match.sql и xref_team.sql.j2 (charter S2,
--     #913 Phase 2). НЕ выводить слаг из формы ключа (AFCON — single-year).
--   * short_name — details_json $.shortName (типизированной колонки нет);
--     venue_surface/venue_opened — overview_json $.venue.statPairs (map-lookup,
--     тот же идиом, что foot в fotmob_player_profile); venue_latitude/longitude —
--     overview_json $.venue.widget.location[0|1] (строки → TRY_CAST DOUBLE).
--   * `*_current` view уже дедуплен (манифест-гейт + natural key + свежайший
--     батч) — legacy ROW_NUMBER по _ingested_at/_batch_id не нужен; defensive
--     ROW_NUMBER остаётся только в standings (ключ view мельче: table_id/name);
--     `ORDER BY _observed_at DESC, _target_batch_id DESC` (_batch_id в native
--     не существует).
-- =============================================================================

WITH league_map (competition_id, league) AS (
    VALUES
        (47,  'ENG-Premier League'),
        (48,  'ENG-Championship'),
        (87,  'ESP-La Liga'),
        (54,  'GER-Bundesliga'),
        (55,  'ITA-Serie A'),
        (53,  'FRA-Ligue 1'),
        (57,  'NED-Eredivisie'),
        (61,  'POR-Primeira Liga'),
        (42,  'UEFA-Champions League'),
        (73,  'UEFA-Europa League'),
        (77,  'INT-World Cup'),
        (50,  'INT-European Championship'),
        (289, 'INT-Africa Cup of Nations'),
        (44,  'INT-Copa America')
),

-- Сезонный скоуп: команда ∈ (competition, season). Native-снапшот профиля
-- глобален (без league/season) — членство в сезоне даёт season_teams.
season_scope AS (
    SELECT DISTINCT
        st.competition_id,
        st.source_season_key,
        TRY_CAST(substr(st.source_season_key, 1, 4) AS integer) AS season_year,
        st.team_id
    FROM iceberg.bronze.fotmob_season_teams_current st
),

-- Профиль команды: одна строка на team_id (view уже дедуплен). statPairs →
-- map label→value (surface/opened приходят как пары [label, value]).
team_snapshot AS (
    SELECT
        ts.team_id,
        ts.team_name,
        json_extract_scalar(ts.details_json, '$.shortName') AS short_name,
        ts.country_code,
        ts.venue_name,
        ts.venue_city,
        ts.venue_capacity,
        map_from_entries(
            transform(
                CAST(json_extract(ts.overview_json, '$.venue.statPairs') AS array<json>),
                p -> ROW(
                    LOWER(json_extract_scalar(p, '$[0]')),
                    json_extract_scalar(p, '$[1]')
                )
            )
        ) AS venue_stat_pairs,
        json_extract_scalar(ts.overview_json, '$.venue.widget.location[0]') AS venue_latitude,
        json_extract_scalar(ts.overview_json, '$.venue.widget.location[1]') AS venue_longitude,
        ts._observed_at
    FROM iceberg.bronze.fotmob_team_snapshots_current ts
    WHERE ts.team_id IS NOT NULL
),

-- Позиция в таблице сезона (замена legacy overview_table_position).
-- table_type = 'all' ОБЯЗАТЕЛЕН — native хранит и home/away/form/xg-срезы.
standings AS (
    SELECT competition_id, source_season_key, team_id, position
    FROM (
        SELECT
            s.competition_id,
            s.source_season_key,
            s.team_id,
            s.position,
            ROW_NUMBER() OVER (
                PARTITION BY s.competition_id, s.source_season_key, s.team_id
                ORDER BY s._observed_at DESC, s._target_batch_id DESC
            ) AS rn
        FROM iceberg.bronze.fotmob_standings_current s
        WHERE s.table_type = 'all'
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    CAST(ts.team_id AS bigint) AS team_id,
    ts.team_name,
    ts.short_name,

    -- ===== Attributes =====
    ts.country_code AS country,
    ts.venue_name   AS venue,
    -- Venue attributes (#750): widget.city + statPairs (surface/capacity/opened).
    -- Feeds gold.dim_venue (city fills non-curated NULLs; capacity replaces the
    -- hand-curated #434 lookup; surface/opened are new attributes). Bronze holds
    -- raw strings → cast numeric ones here (like table_position).
    ts.venue_city,
    element_at(ts.venue_stat_pairs, 'surface')                      AS venue_surface,
    -- Типизированная native-колонка venue_capacity (widget.capacity) живьём
    -- часто пустая строка — fallback на statPairs 'capacity' (источник legacy).
    TRY_CAST(
        COALESCE(NULLIF(ts.venue_capacity, ''), element_at(ts.venue_stat_pairs, 'capacity'))
        AS INTEGER
    )                                                               AS venue_capacity,
    TRY_CAST(element_at(ts.venue_stat_pairs, 'opened') AS INTEGER)  AS venue_opened,
    -- Stadium coords (#719) for gold.dim_venue flight-distance features. Bronze
    -- holds raw strings from FotMob widget.location → cast here (like table_position).
    TRY_CAST(ts.venue_latitude  AS DOUBLE) AS venue_latitude,
    TRY_CAST(ts.venue_longitude AS DOUBLE) AS venue_longitude,
    CAST(sd.position AS INTEGER)           AS table_position,

    -- ===== Lineage =====
    ts._observed_at AS _bronze_ingested_at,

    -- ===== Partition keys (season → slug to match other Silver tables) =====
    lm.league,
    -- #913 Phase 2
    CASE WHEN lm.league = 'INT-World Cup'
         THEN LPAD(CAST(sc.season_year AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(sc.season_year, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(sc.season_year + 1, 100) AS varchar), 2, '0')
    END AS season

FROM season_scope sc
JOIN league_map lm
  ON lm.competition_id = sc.competition_id
JOIN team_snapshot ts
  ON CAST(ts.team_id AS bigint) = sc.team_id
LEFT JOIN standings sd
  ON  sd.competition_id     = sc.competition_id
 AND sd.source_season_key  = sc.source_season_key
 AND sd.team_id            = sc.team_id
