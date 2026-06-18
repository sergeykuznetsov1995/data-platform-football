-- =============================================================================
-- Silver: fotmob_manager_profile
-- =============================================================================
--
-- Time-invariant snapshot per (player_id, league, season) для ТРЕНЕРОВ (coaches).
-- Зеркало silver.fotmob_player_profile, но `WHERE is_coach` (вместо NOT is_coach):
-- FotMob отдаёт тренеров в тех же двух bronze-таблицах, что и игроков, помечая их
-- `is_coach=true` (player_details) / `role='coach'` (team_squad).
--
-- Назначение — обогатить gold.dim_manager атрибутами nationality / dob (issue #434).
-- xref_manager уже несёт coachId в source_id (source='fotmob'); dim_manager
-- связывает canonical_id ↔ coachId и подтягивает отсюда nationality/dob.
--
-- Zerno: (player_id, league, season). `player_id` = FotMob coachId — СОВПАДАЕТ с
-- silver.xref_manager.source_id (source='fotmob'), который берётся из того же
-- bronze.fotmob_player_details. Live APL 2025: 18 coaches, 100% country + dob.
--
-- Sources (all from iceberg.bronze):
--   fotmob_player_details (d) — driver: WHERE is_coach. Несёт coachId (player_id),
--                                name, birth_date. Тот же источник coachId, что и
--                                xref_manager — гарантирует совпадение ключей.
--   fotmob_team_squad     (s) — role='coach': country (=nationality),
--                                date_of_birth (структурированные поля, не JSON).
--                                JOIN-ключ CAST(s.player_id AS VARCHAR) = d.player_id
--                                (team_squad.player_id — bigint, details — varchar).
--
-- Pipeline (симметрично fotmob_player_profile):
--   1. details_dedup — ROW_NUMBER dedup на (player_id, league, season), is_coach.
--   2. squad_dedup   — то же для team_squad, role='coach'.
--   3. Final SELECT  — dob = COALESCE(squad.date_of_birth, details.birth_date);
--      nationality = squad.country. dob/nationality остаются varchar passthrough
--      (bronze хранит ISO-строки) — gold.dim_manager делает TRY_CAST(.. AS DATE).
-- =============================================================================

WITH details_dedup AS (
    SELECT
        CAST(player_id AS VARCHAR) AS player_id,
        name,
        birth_date,
        league,
        season,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, league, season
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.fotmob_player_details
    WHERE is_coach
),

squad_dedup AS (
    SELECT
        CAST(player_id AS VARCHAR) AS player_id,
        league,
        season,
        country,
        date_of_birth,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, league, season
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.fotmob_team_squad
    WHERE role = 'coach'
)

SELECT
    d.player_id,
    d.name,

    -- dob: team_squad — основной источник (structured), birth_date из
    -- player_details — fallback. Оба varchar (ISO) → gold делает TRY_CAST.
    COALESCE(s.date_of_birth, d.birth_date)          AS date_of_birth,
    s.country                                        AS nationality,

    -- ========= Lineage =========
    s._ingested_at                                   AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season → slug ('2425'); FotMob bronze stores year-start bigint (2024).
    d.league,
    LPAD(CAST(MOD(d.season,     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(d.season + 1, 100) AS varchar), 2, '0') AS season

FROM details_dedup d
LEFT JOIN squad_dedup s
    ON  s.player_id = d.player_id
    AND s.league    = d.league
    AND s.season    = d.season
    AND s.rn = 1
WHERE d.rn = 1
