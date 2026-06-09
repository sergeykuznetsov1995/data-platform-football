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
--   2) match_id_canonical bridging — двухступенчатый:
--      a) sofascore_schedule.game_id → (date, home_team, away_team) →
--         через team_aliases → gold.dim_match.match_id (FBref hex).
--         sofascore_schedule.game_id это BIGINT, ratings.match_id это varchar
--         → CAST(game_id AS varchar) для JOIN.
--      b) Если bridge не сработал (новые SofaScore матчи без FBref schedule
--         или alias не покрывает имя) → fallback prefix
--         'sofascore_' || sofascore_match_id (v0_unbridged pattern,
--         совместимо с whoscored 'whoscored_raw' approach в fct_event).
--
--   3) player_id_canonical через silver.xref_player WHERE source='sofascore'
--      AND season+league predicate (memory note feedback_xref_join_season_predicate).
--      Fallback orphan: 'ss_' || sofascore_player_id для unresolved.
--
--   4) НЕ модифицирует silver.xref_match (Phase B-deferred).
--   5) team_side passthrough — SofaScore JSON уже даёт home/away атрибуцию
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

WITH team_aliases AS (
    -- Same lookup pool as silver.matchhistory_match_odds — kept inline вместо
    -- читать xref_team чтобы не создавать кросс-Silver зависимость (xref_team
    -- всё равно строится в одной DAG-итерации). При расширении alias-списка
    -- ОБА файла надо обновлять параллельно.
    --
    -- KEY INSIGHT: canonical_id RHS = ACTUAL `gold.dim_match.home_team_id`
    -- value as observed in production, NOT slug-of-YAML-canonical_name. См.
    -- полный комментарий в matchhistory_match_odds.sql. SofaScore raw names
    -- может отличаться от football-data.co.uk (e.g. SofaScore часто пишет
    -- 'Wolverhampton', 'Brighton & Hove Albion' — оба варианта покрыты).
    SELECT raw_name, canonical_id
    FROM (VALUES
        ('AFC Bournemouth', 'bournemouth'),
        ('Arsenal', 'arsenal'),
        ('Aston Villa', 'aston_villa'),
        ('Bournemouth', 'bournemouth'),
        ('Brentford', 'brentford'),
        ('Brighton', 'brighton'),
        ('Brighton & Hove Albion', 'brighton'),
        ('Brighton and Hove Albion', 'brighton'),
        ('Burnley', 'burnley'),
        ('Cardiff', 'cardiff_city'),
        ('Cardiff City', 'cardiff_city'),
        ('Chelsea', 'chelsea'),
        ('Crystal Palace', 'crystal_palace'),
        ('Everton', 'everton'),
        ('Fulham', 'fulham'),
        ('Huddersfield', 'huddersfield_town'),
        ('Hull', 'hull_city'),
        ('Hull City', 'hull_city'),
        ('Ipswich', 'ipswich_town'),
        ('Ipswich Town', 'ipswich_town'),
        ('Leeds', 'leeds_united'),
        ('Leeds United', 'leeds_united'),
        ('Leicester', 'leicester_city'),
        ('Leicester City', 'leicester_city'),
        ('Liverpool', 'liverpool'),
        ('Luton', 'luton_town'),
        ('Luton Town', 'luton_town'),
        ('Man City', 'manchester_city'),
        ('Man United', 'manchester_utd'),
        ('Man Utd', 'manchester_utd'),
        ('Manchester City', 'manchester_city'),
        ('Manchester United', 'manchester_utd'),
        ('Manchester Utd', 'manchester_utd'),
        ('Middlesbrough', 'middlesbrough'),
        ('Newcastle', 'newcastle_united'),
        ('Newcastle United', 'newcastle_united'),
        ('Newcastle Utd', 'newcastle_united'),
        ('Norwich', 'norwich_city'),
        ('Norwich City', 'norwich_city'),
        ('Nott''m Forest', 'nottingham_forest'),
        ('Nottingham', 'nottingham_forest'),
        ('Nottingham Forest', 'nottingham_forest'),
        ('Sheff Utd', 'sheffield_united'),
        ('Sheffield United', 'sheffield_united'),
        ('Sheffield Utd', 'sheffield_united'),
        ('Southampton', 'southampton'),
        ('Spurs', 'tottenham_hotspur'),
        ('Stoke', 'stoke_city'),
        ('Stoke City', 'stoke_city'),
        ('Sunderland', 'sunderland'),
        ('Swansea', 'swansea_city'),
        ('Swansea City', 'swansea_city'),
        ('Tottenham', 'tottenham_hotspur'),
        ('Tottenham Hotspur', 'tottenham_hotspur'),
        ('Watford', 'watford'),
        ('West Brom', 'west_brom'),
        ('West Bromwich', 'west_brom'),
        ('West Bromwich Albion', 'west_brom'),
        ('West Ham', 'west_ham_united'),
        ('West Ham United', 'west_ham_united'),
        ('Wolverhampton', 'wolves'),
        ('Wolverhampton Wanderers', 'wolves'),
        ('Wolves', 'wolves')
    ) AS t(raw_name, canonical_id)
),

-- =============================================================================
-- ss: raw bronze, basic dedup. Bronze cardinality = (match_id, player_id).
-- Re-scrapes возможны → ROW_NUMBER OVER (PARTITION BY match_id, player_id)
-- ORDER BY _ingested_at DESC keeps latest.
-- =============================================================================
ss AS (
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
-- sched_bridge: SofaScore game_id → FBref match_id через team_aliases.
-- LEFT JOIN, dedup ON (game_id) — на случай re-scrape schedule.
-- =============================================================================
sched_bridge AS (
    SELECT
        sched.game_id,
        sched.league,
        sched.season,
        dm.match_id           AS fbref_match_id,
        ROW_NUMBER() OVER (
            PARTITION BY sched.game_id, sched.league, sched.season
            ORDER BY sched._ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.sofascore_schedule sched
    LEFT JOIN team_aliases ha ON ha.raw_name = sched.home_team
    LEFT JOIN team_aliases aa ON aa.raw_name = sched.away_team
    LEFT JOIN iceberg.gold.dim_match dm
        ON  dm.date         = CAST(sched.date AS DATE)
        AND dm.league       = sched.league
        AND dm.season       = TRY_CAST(sched.season AS bigint)
        AND dm.home_team_id = ha.canonical_id
        AND dm.away_team_id = aa.canonical_id
    WHERE sched.game_id IS NOT NULL
),

-- =============================================================================
-- ss_with_match: bridge match_id_canonical. Fallback v0_unbridged для misses.
-- ratings.match_id (varchar) joinится с CAST(schedule.game_id AS varchar).
-- Per-(game_id, league, season): достаточно сравнения season + match_id =
-- game_id, потому что SofaScore game_id глобально-уникален и league+season
-- передаются для предохранителя.
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
            sb.fbref_match_id,
            'sofascore_' || sp.match_id
        )                       AS match_id_canonical
    FROM ss_with_player sp
    LEFT JOIN sched_bridge sb
        ON  sb.rn = 1
        AND CAST(sb.game_id AS varchar) = sp.match_id
        AND sb.league = sp.league
        AND sb.season = sp.season
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
