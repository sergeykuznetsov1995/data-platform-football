-- =============================================================================
-- Silver: sofascore_player_ratings
-- =============================================================================
-- DESIGN: typed + dedup + xref_player canonical resolve + match_id bridge.
--   1) Bronze `sofascore_player_ratings` имеет:
--        match_id varchar  — SofaScore-internal id (отличается от FBref hex)
--        player_id varchar — SofaScore-internal id
--        team_side enum('home','away'), rating double, position varchar
--      (производится E4.1 web-scraping-expert; до тех пор Bronze 0 rows
--      → пустая Silver-таблица; см. Empty-bronze handling ниже).
--
--   2) match_id_canonical bridging — через silver.xref_match (НЕ через
--      gold.dim_match — #477). ratings.match_id это SofaScore game_id (varchar);
--      xref_match.source_id для source='sofascore' это CAST(game_id AS varchar),
--      поэтому LEFT JOIN по (source_id, league, season) сразу даёт canonical_id
--      (FBref hex когда сбриджован, 'ss_<id>' когда orphan). Канонизация имён
--      команд живёт ВЫШЕ — в xref_match (через xref_team), здесь больше НЕТ
--      инлайнового team_aliases CTE (устранён дубль #477) и нет чтения
--      sofascore_schedule. Зависимость silver→silver: xref_match строится в E1
--      (раньше E4), инверсия слоёв silver→gold устранена.
--      Fallback (game вне xref_match): 'sofascore_' || match_id —
--      mirrors silver/sofascore_shots.sql:173.
--
--   3) player_id_canonical через silver.xref_player WHERE source='sofascore'
--      AND season+league predicate (memory note feedback_xref_join_season_predicate).
--      Fallback orphan: 'ss_' || sofascore_player_id для unresolved.
--
--   4) team_side passthrough — SofaScore JSON уже даёт home/away атрибуцию
--      на уровне rating-row; не нужно пере-вычислять через sofascore_schedule.
--
-- Reference: roadmap E4.4 + R0.2B web-scraping spec.
--
-- =============================================================================
-- Empty-bronze handling
-- =============================================================================
-- Если bronze.sofascore_player_ratings = 0 rows (R0.2B_FALLBACK ещё не
-- развёрнут), этот SELECT возвращает 0 rows — `silver_tasks.run_silver_transform`
-- спокойно создаст пустую Silver-таблицу с зафиксированной схемой. Schema-stub
-- не нужен: типы выводятся из CAST() выражений в проекции.
--
-- При отсутствии Bronze-таблицы целиком — задача упадёт на planning-time с
-- TableNotFoundException. Это намеренное поведение: добавлять
-- 'sofascore_player_ratings' в `OPTIONAL_BRONZE_TABLES` (E4.6 task) — так
-- DAG скипнет таску с warning, не блокируя остальной Silver.
--
-- =============================================================================
-- Bronze schema (expected post-E4.1; verify via DESCRIBE before unblocking E4.6)
-- =============================================================================
--   match_id        varchar      — SofaScore game_id, stringified
--   player_id       varchar      — SofaScore player_id, stringified
--   team_side       varchar      — 'home' | 'away'
--   rating          double       — 0.0–10.0; 0.0 = "no rating" sentinel
--   position        varchar      — raw SofaScore position
--   league          varchar      — partition key
--   season          varchar      — partition key (SofaScore stores varchar)
--   _ingested_at    timestamp(6) — bronze lineage
--
-- =============================================================================
-- Output schema
-- =============================================================================
--   match_id_canonical    varchar           -- FBref hex if bridged, else 'sofascore_<raw>'
--   player_id_canonical   varchar           -- xref_player canonical, else 'ss_<raw>'
--   team_side             varchar           -- 'home' | 'away'
--   rating                decimal(3,1)      -- 0.1–10.0 range (0.0 dropped as NULL)
--   position              varchar           -- passthrough (no canonicalisation in MVP)
--   source                varchar           -- 'sofascore'
--   source_version        varchar           -- 'v1'
--   league                varchar           -- partition key
--   season                varchar           -- partition key, varchar slug ('2526')
--                                              per charter S2 (slug↔year-start
--                                              reconciliation deferred to Gold)
--   _ingested_at          timestamp(6)      -- bronze lineage passthrough
--
-- Logical PK: (match_id_canonical, player_id_canonical, team_side)
--
-- =============================================================================
-- DAG-integration note
-- =============================================================================
--   silver_tasks.run_silver_transform() wraps this SELECT in
--   `CREATE TABLE iceberg.silver.sofascore_player_ratings AS ...` with
--   partitioning by (league, season). This file MUST stay a pure SELECT.
-- =============================================================================

-- =============================================================================
-- ss: raw bronze, basic dedup. Bronze cardinality = (match_id, player_id).
-- Re-scrapes возможны → ROW_NUMBER OVER (PARTITION BY match_id, player_id)
-- ORDER BY _ingested_at DESC keeps latest.
-- =============================================================================
WITH ss AS (
    SELECT
        match_id,
        player_id,
        team_side,
        rating,
        position,
        league,
        season,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, player_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.sofascore_player_ratings
    WHERE match_id  IS NOT NULL
      AND player_id IS NOT NULL
),

-- =============================================================================
-- ss_typed: rating range guard. SofaScore returns 0.0 для not-rated players
-- (substitutes who didn't get minutes); treat as NULL так как 0.0 это
-- невалидный rating в [0.1, 10.0]-домене.
-- =============================================================================
ss_typed AS (
    SELECT
        match_id,
        player_id,
        team_side,
        CASE
            WHEN rating IS NULL OR rating <= 0.0 OR rating > 10.0 THEN NULL
            ELSE rating
        END                                AS rating,
        position,
        league,
        season,
        _ingested_at
    FROM ss
    WHERE rn = 1
      AND team_side IN ('home', 'away')
),

-- =============================================================================
-- ss_with_player: LEFT JOIN xref_player с season+league predicate.
-- Если resolver не покрыл этого игрока → orphan 'ss_<raw_id>'.
-- =============================================================================
ss_with_player AS (
    SELECT
        st.*,
        COALESCE(
            xp.canonical_id,
            'ss_' || st.player_id
        )                                  AS player_id_canonical
    FROM ss_typed st
    LEFT JOIN iceberg.silver.xref_player xp
        ON  xp.source    = 'sofascore'
        AND xp.source_id = st.player_id
        AND xp.league    = st.league
        AND xp.season    = st.season
),

-- =============================================================================
-- xref_match_ss: SofaScore game_id → canonical FBref match_id via the Silver
-- xref table (#477). Mirrors silver/sofascore_shots.sql:165-169. source_id is
-- CAST(game_id AS varchar); canonical_id is the FBref hex when bridged, else
-- 'ss_<game_id>' when orphan.
-- =============================================================================
xref_match_ss AS (
    SELECT source_id, league, season, canonical_id
    FROM iceberg.silver.xref_match
    WHERE source = 'sofascore'
),

-- =============================================================================
-- ss_with_match: bridge match_id_canonical via xref_match. ratings.match_id
-- (varchar) joins xref_match.source_id (= CAST(game_id AS varchar)). xref_match
-- PK = (source, source_id, season) → season+league predicate prevents fan-out.
-- Fallback (game absent from xref_match): 'sofascore_' || match_id — same
-- pattern as silver/sofascore_shots.sql:173.
-- =============================================================================
ss_with_match AS (
    SELECT
        sp.team_side,
        sp.rating,
        sp.position,
        sp.player_id_canonical,
        sp.player_id           AS sofascore_player_id,
        sp.match_id            AS sofascore_match_id,
        sp.league,
        sp.season,
        sp._ingested_at,
        COALESCE(
            xm.canonical_id,
            'sofascore_' || sp.match_id
        )                       AS match_id_canonical
    FROM ss_with_player sp
    LEFT JOIN xref_match_ss xm
        ON  xm.source_id = sp.match_id
        AND xm.league    = sp.league
        AND xm.season    = sp.season
)

SELECT
    match_id_canonical,
    player_id_canonical,
    team_side,
    CAST(rating AS decimal(3,1))           AS rating,
    position,
    CAST('sofascore' AS varchar)           AS source,
    CAST('v1'        AS varchar)           AS source_version,
    league,
    -- season как varchar-slug ('2526'), per charter S2. SofaScore Bronze хранит
    -- season как varchar; CAST держит тот же slug без смены значения.
    CAST(season AS varchar)               AS season,
    _ingested_at
FROM ss_with_match
