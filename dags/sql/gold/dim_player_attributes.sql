-- =============================================================================
-- Gold: dim_player_attributes
-- =============================================================================
--
-- Cross-source snapshot per игрок: time-invariant атрибуты с per-source
-- колонками (no winning-value logic). Один row per canonical_id.
--
-- Принципы (см. docs/decisions/RX-implementation-plan.md):
--   - FBref-spine: WHERE source='fbref' AND confidence != 'orphan' →
--     `xref_fbref` даёт ровно один row на canonical_id.
--   - FotMob-side bridge: per canonical_id берётся latest season — атрибуты
--     time-invariant, но xref хранит row per (source, source_id, season).
--   - SofaScore-side: silver.sofascore_player_profile уже несёт canonical_id
--     (заполняется при материализации Silver), JOIN напрямую без xref hop;
--     MAX_BY(..., season) сворачивает до одного row per canonical_id.
--   - Attributes published per source с суффиксом источника. Никаких
--     "правильных" значений — потребитель решает сам. contract_end /
--     market_value — slowly-changing, snapshot "as-of-latest-ingest"
--     (issue #11 для full timeline).
--
-- НЕ заменяет gold.dim_player (per-season, FBref-only). Additive deploy.
--
-- Сross-source season type discrepancy (CLAUDE.md):
--   - silver.fbref_player_season_profile.season = bigint  (2025)
--   - silver.fotmob_player_profile.season       = bigint  (2025)
--   - silver.sofascore_player_profile.season    = varchar ('2526')
--   - silver.xref_player.season                 = varchar ('2526')
-- В этом SQL season НЕ используется в JOIN-ключе — snapshot-зерно делает
-- season-mapping излишним. `MAX_BY(... ORDER BY season DESC)` берёт значение
-- атрибута из самого свежего сезона на стороне каждого источника
-- независимо.
-- =============================================================================

WITH
fbref_latest AS (
    SELECT
        player_id,
        MAX_BY(player, season)  AS player_name,
        MAX_BY(born,   season)  AS born_year,
        MAX_BY(nation, season)  AS nationality,
        MAX_BY(squad,  season)  AS current_team
    FROM iceberg.silver.fbref_player_season_profile
    WHERE player_id IS NOT NULL
    GROUP BY player_id
),

fotmob_latest AS (
    SELECT
        player_id,
        MAX_BY(player_name,    season) AS player_name,
        MAX_BY(date_of_birth,  season) AS date_of_birth,
        MAX_BY(nationality,    season) AS nationality,
        MAX_BY(country_code,   season) AS country_code,
        MAX_BY(height_cm,      season) AS height_cm,
        MAX_BY(foot,           season) AS foot,
        MAX_BY(shirt_number,   season) AS shirt_number,
        -- contract_end + current_market_value_eur — slowly-changing,
        -- latest-per-season snapshot. Не строго time-invariant: попадают в
        -- snapshot как "as-of-latest-ingest". История market_value уйдёт в
        -- gold.fct_player_market_value (issue #11).
        MAX_BY(contract_end,             season) AS contract_end,
        MAX_BY(current_market_value_eur, season) AS current_market_value_eur,
        MAX_BY(market_value_currency,    season) AS market_value_currency
    FROM iceberg.silver.fotmob_player_profile
    WHERE player_id IS NOT NULL
    GROUP BY player_id
),

-- SofaScore-side: silver.sofascore_player_profile уже содержит canonical_id
-- (заполняется при материализации Silver через xref_player bridge). GROUP BY
-- canonical_id со MAX_BY свёртывает potentially-multi-season Bronze snapshot
-- к одному row на канонического игрока.
sofascore_latest AS (
    SELECT
        canonical_id,
        MAX_BY(player_name,       season) AS player_name,
        MAX_BY(height_cm,         season) AS height_cm,
        MAX_BY(preferred_foot,    season) AS preferred_foot,
        MAX_BY(date_of_birth,     season) AS date_of_birth,
        MAX_BY(nationality,       season) AS nationality,
        MAX_BY(country_code,      season) AS country_code,
        MAX_BY(shirt_number,      season) AS shirt_number,
        MAX_BY(retired,           season) AS retired
    FROM iceberg.silver.sofascore_player_profile
    WHERE canonical_id IS NOT NULL
    GROUP BY canonical_id
),

-- FBref-spine: один row per canonical_id. FBref не имеет 'orphan' confidence
-- (по построению resolver'а — FBref игроки всегда 'exact'), но фильтр оставлен
-- для symmetry на случай будущей логики.
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id AS fbref_player_id
    FROM iceberg.silver.xref_player
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

-- FotMob-side: один row per canonical_id, source_id из latest season.
-- ROW_NUMBER ORDER BY season DESC — берём свежий маппинг (даже если игрок
-- сменил FotMob ID между сезонами, это редкость, но защита есть).
xref_fotmob_latest AS (
    SELECT canonical_id, fotmob_player_id
    FROM (
        SELECT
            canonical_id,
            source_id AS fotmob_player_id,
            ROW_NUMBER() OVER (
                PARTITION BY canonical_id
                ORDER BY season DESC
            ) AS rn
        FROM iceberg.silver.xref_player
        WHERE source = 'fotmob'
    )
    WHERE rn = 1
)

SELECT
    xf.canonical_id                                    AS player_id_canonical,
    COALESCE(fb.player_name, fm.player_name, ss.player_name)
                                                       AS player_name_canonical,

    -- Current club — единый источник FBref (spine 100% coverage,
    -- симметрично с remaining `*_fbref` колонками).
    fb.current_team                                    AS current_team_name,

    -- Identity attributes
    fb.born_year                                       AS born_year_fbref,
    fm.date_of_birth                                   AS dob_fotmob,
    ss.date_of_birth                                   AS dob_sofascore,
    fb.nationality                                     AS nationality_fbref,
    fm.nationality                                     AS nationality_fotmob,
    ss.nationality                                     AS nationality_sofascore,
    fm.country_code                                    AS country_code_fotmob,
    ss.country_code                                    AS country_code_sofascore,

    -- Physical attributes
    fm.height_cm                                       AS height_cm_fotmob,
    ss.height_cm                                       AS height_cm_sofascore,
    fm.foot                                            AS foot_fotmob,
    ss.preferred_foot                                  AS foot_sofascore,

    -- Squad attributes
    fm.shirt_number                                    AS shirt_number_fotmob,
    ss.shirt_number                                    AS shirt_number_sofascore,
    ss.retired                                         AS retired_sofascore,

    -- Contract / market value (slowly-changing, as-of-latest-ingest)
    fm.contract_end                                    AS contract_end_fotmob,
    fm.current_market_value_eur                        AS current_market_value_eur_fotmob,
    fm.market_value_currency                           AS market_value_currency_fotmob,

    CURRENT_TIMESTAMP                                  AS _gold_created_at

FROM xref_fbref xf
LEFT JOIN fbref_latest fb
    ON fb.player_id = xf.fbref_player_id
LEFT JOIN xref_fotmob_latest xfm
    ON xfm.canonical_id = xf.canonical_id
LEFT JOIN fotmob_latest fm
    ON fm.player_id = xfm.fotmob_player_id
LEFT JOIN sofascore_latest ss
    ON ss.canonical_id = xf.canonical_id
