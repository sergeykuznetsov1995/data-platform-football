-- =============================================================================
-- Silver: sofascore_player_profile
-- =============================================================================
--
-- Time-invariant атрибуты игрока из SofaScore (height, foot, dob,
-- nationality, jersey/shirt number, retired). Один row per
-- (player_id, league, season) — snapshot-grain совпадает с Bronze.
-- `canonical_id` подтягивается через silver.xref_player (source='sofascore',
-- non-orphan) и материализуется как сторонний ключ для Gold-витрин,
-- особенно gold.dim_player_attributes.
--
-- Bronze schema — #840 auto-passthrough (source-key names):
--   player_id, name, short_name, slug, position,
--   jersey_number (varchar), shirt_number (bigint),
--   height (bigint), preferred_foot, date_of_birth_timestamp (bigint epoch),
--   nationality, country_name, country_alpha2,
--   team_id (bigint), team_name, retired (boolean), league, season, _ingested_at.
--   Pre-#840 legacy names (height_cm, date_of_birth ISO, country_code,
--   current_team_id/name) bridged via COALESCE in the `src` CTE; the epoch->date
--   derivation and nationality<-country.name fallback moved from Bronze to `src`.
--
-- Notes:
--   * (league, season) JOIN predicate against xref_player MANDATORY
--     (CLAUDE.md / feedback_xref_join_season_predicate.md).
--   * canonical_id оставляется NULLable: SofaScore-игроки могут не иметь
--     bridge'а к FBref-spine (U21, резерв, новые подписания) — orphan
--     row сохраняется в Silver, фильтрация уже на Gold-уровне.
--   * Bronze ingest mode = replace_partitions → ROW_NUMBER dedup is
--     defensive against future ingest-mode regression.
--   * date_of_birth приходит из Bronze как varchar (ISO YYYY-MM-DD);
--     TRY_CAST AS DATE на случай аномалий (1-2 строки могут быть NULL).
--   * current_team_id — bigint в Bronze, CAST AS varchar для FK
--     совместимости с другими dim_team колонками.
-- =============================================================================

-- #840: Bronze is now auto-passthrough (source-key names). Rename/derive here.
-- COALESCE(old, new) bridges pre-#840 partitions and freshly re-scraped ones.
WITH src AS (
    SELECT
        player_id,
        name,
        short_name,
        slug,
        position,
        jersey_number,
        shirt_number,
        COALESCE(height_cm, height)                          AS height_cm,
        preferred_foot,
        -- dob: Bronze kept raw epoch (date_of_birth_timestamp); old ISO varchar
        -- date_of_birth bridged. Guard non-positive epochs (parity with old fn).
        COALESCE(
            TRY_CAST(date_of_birth AS DATE),
            CASE WHEN date_of_birth_timestamp > 0
                 THEN CAST(from_unixtime(date_of_birth_timestamp) AS DATE)
            END
        )                                                    AS date_of_birth,
        -- nationality fallback to country.name moved from Bronze to here.
        COALESCE(nationality, country_name)                  AS nationality,
        COALESCE(country_code, country_alpha2)               AS country_code,
        COALESCE(current_team_id, team_id)                   AS current_team_id,
        COALESCE(current_team_name, team_name)               AS current_team_name,
        retired,
        _ingested_at,
        league,
        season
    FROM iceberg.bronze.sofascore_player_profile
),

bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM src b
        WHERE player_id IS NOT NULL
    )
    WHERE rn = 1
),

xp AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_player
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
)

SELECT
    b.player_id,
    xp.canonical_id,
    b.name                                              AS player_name,
    b.short_name,
    b.slug,
    b.position,

    -- Numeric attributes — Bronze хранит bigint, проектируем как INTEGER
    -- (никаких рост > 32k или shirt > 32k не предвидится).
    CAST(b.height_cm AS INTEGER)                        AS height_cm,
    CAST(b.shirt_number AS INTEGER)                     AS shirt_number,
    -- #840: _coerce_scalar upcasts numeric jerseyNumber -> bigint in Bronze;
    -- CAST back to varchar to keep the Silver output contract (free-form string).
    CAST(b.jersey_number AS varchar)                    AS jersey_number,

    b.preferred_foot,
    TRY_CAST(b.date_of_birth AS DATE)                   AS date_of_birth,
    b.nationality,
    b.country_code,

    -- current_team_id: bigint → varchar (FK-symmetric с другими dim_team
    -- references; Bronze numeric ID rule из CLAUDE.md).
    CAST(b.current_team_id AS varchar)                  AS current_team_id,
    b.current_team_name,

    b.retired,

    b._ingested_at                                      AS _bronze_ingested_at,

    -- Partition keys last (matching writer convention).
    b.league,
    b.season

FROM bronze_dedup b
LEFT JOIN xp
    ON xp.source_id = b.player_id
   AND xp.league    = b.league
   AND xp.season    = b.season
