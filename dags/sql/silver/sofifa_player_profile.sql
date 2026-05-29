-- =============================================================================
-- Silver: sofifa_player_profile
-- =============================================================================
--
-- Per-edition snapshot игрока из SoFIFA (FIFA/EA FC video-game ratings):
-- overall/potential, main-6 card-агрегаты (pace/shooting/passing/dribbling/
-- defending/physical), 5 GK-скиллов, game-side market value / wage / release
-- clause / контракт, плюс профиль (position / dob / height / weight /
-- nationality). Один row per (player_id, fifa_edition).
--
-- ВАЖНО: это game-side рейтинги (мнение EA Sports), НЕ реальные метрики матчей.
--
-- Источники Bronze:
--   * bronze.sofifa_player_ratings — атрибуты (без league/team), key
--     (player_id, fifa_edition). Имена колонок — после soccerdata
--     standardize_colnames: overallrating, gk_diving, ... ; main-6 и финансы
--     добавлены парсером issue #42 (pace/shooting/.../value_eur/...).
--   * bronze.sofifa_players — identity (league, team, player), тот же key.
--
-- FIFA-edition → football-season mapping: edition назван по году конца сезона
--   (EA FC 26 = сезон 2025/26). 'FC 26' → '2526', 'FC 25' → '2425'.
--   season = LPAD(N-1) || LPAD(N), где N = числовой суффикс fifa_edition.
--   Нужен чтобы (а) JOIN к xref_player по общему slug-формату, (б) Silver
--   партиционировался по (league, season) как остальные профили.
--
-- canonical_id подтягивается через silver.xref_player (source='sofifa',
-- non-orphan). (league, season) JOIN-предикат MANDATORY (CLAUDE.md /
-- feedback_xref_join_season_predicate.md). canonical_id остаётся NULLable:
-- SoFIFA содержит игроков вне FBref-spine (loan-out, U21) — live APL 2025/26
-- orphan ≈ 15% (feedback по issue #42 resolver dry-run).
--
-- Bronze ingest mode = replace_partitions(['fifa_edition']) → ROW_NUMBER dedup
-- defensive против повторных прогонов внутри одной edition.
-- =============================================================================

WITH ratings_dedup AS (
    SELECT *
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, fifa_edition
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.sofifa_player_ratings r
        WHERE player_id IS NOT NULL
    )
    WHERE rn = 1
),

identity_dedup AS (
    SELECT player_id, fifa_edition, league, team, player
    FROM (
        SELECT
            p.player_id, p.fifa_edition, p.league, p.team, p.player,
            ROW_NUMBER() OVER (
                PARTITION BY p.player_id, p.fifa_edition
                ORDER BY p._ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.sofifa_players p
        WHERE p.player_id IS NOT NULL
    )
    WHERE rn = 1
),

joined AS (
    SELECT
        r.player_id,
        i.league,
        i.team,
        COALESCE(r.player, i.player)                AS player_name,
        r.fifa_edition,
        -- FIFA/FC edition number → football-season slug ('FC 26' -> '2526').
        LPAD(CAST(
            CAST(regexp_extract(r.fifa_edition, '(\d+)', 1) AS INTEGER) - 1
            AS VARCHAR), 2, '0')
        || LPAD(regexp_extract(r.fifa_edition, '(\d+)', 1), 2, '0') AS season,

        -- Headline ratings
        CAST(r.overallrating AS INTEGER)            AS overall,
        CAST(r.potential AS INTEGER)                AS potential,

        -- Main-6 card aggregates
        CAST(r.pace AS INTEGER)                     AS pace,
        CAST(r.shooting AS INTEGER)                 AS shooting,
        CAST(r.passing AS INTEGER)                  AS passing,
        CAST(r.dribbling AS INTEGER)                AS dribbling,
        CAST(r.defending AS INTEGER)                AS defending,
        CAST(r.physical AS INTEGER)                 AS physical,

        -- Goalkeeper skills
        CAST(r.gk_diving AS INTEGER)                AS gk_diving,
        CAST(r.gk_handling AS INTEGER)              AS gk_handling,
        CAST(r.gk_kicking AS INTEGER)               AS gk_kicking,
        CAST(r.gk_positioning AS INTEGER)           AS gk_positioning,
        CAST(r.gk_reflexes AS INTEGER)              AS gk_reflexes,

        -- Game-side contract / financials (snapshot, as-of-edition)
        CAST(r.value_eur AS BIGINT)                 AS value_eur,
        CAST(r.wage_eur AS BIGINT)                  AS wage_eur,
        CAST(r.release_clause_eur AS BIGINT)        AS release_clause_eur,
        CAST(r.contract_valid_until AS INTEGER)     AS contract_valid_until,
        r.joined                                    AS joined,

        -- Profile attributes
        r.position                                  AS position,
        r.dob                                       AS dob,
        CAST(r.height_cm AS INTEGER)                AS height_cm,
        CAST(r.weight_kg AS INTEGER)                AS weight_kg,
        r.nationality                               AS nationality,

        r._ingested_at                              AS _bronze_ingested_at
    FROM ratings_dedup r
    LEFT JOIN identity_dedup i
        ON i.player_id    = r.player_id
       AND i.fifa_edition = r.fifa_edition
),

xp AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_player
    WHERE source = 'sofifa'
      AND confidence <> 'orphan'
)

SELECT
    j.player_id,
    xp.canonical_id,
    j.player_name,

    j.overall,
    j.potential,
    j.pace,
    j.shooting,
    j.passing,
    j.dribbling,
    j.defending,
    j.physical,
    j.gk_diving,
    j.gk_handling,
    j.gk_kicking,
    j.gk_positioning,
    j.gk_reflexes,

    j.value_eur,
    j.wage_eur,
    j.release_clause_eur,
    j.contract_valid_until,
    j.joined,

    j.position,
    j.dob,
    j.height_cm,
    j.weight_kg,
    j.nationality,

    j.team,
    j.fifa_edition,

    j._bronze_ingested_at,

    -- Partition keys last (matching writer convention).
    j.league,
    j.season

FROM joined j
LEFT JOIN xp
    ON xp.source_id = CAST(j.player_id AS VARCHAR)
   AND xp.league    = j.league
   AND xp.season    = j.season
