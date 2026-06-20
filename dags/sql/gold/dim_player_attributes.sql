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
--   - Transfermarkt-side: тот же паттерн что SofaScore — canonical_id уже в
--     silver.transfermarkt_players, JOIN напрямую без xref hop. TM — primary
--     источник для height_cm (parsed с официального club profile) и
--     current_market_value_eur (EUR, не USD-конверсия FotMob).
--   - Attributes published per source с суффиксом источника. Никаких
--     "правильных" значений — потребитель решает сам. contract_end /
--     market_value — slowly-changing, snapshot "as-of-latest-ingest"
--     (issue #11 для full timeline).
--
-- НЕ заменяет gold.dim_player (per-season, FBref-only). Additive deploy.
--
-- Сross-source season type discrepancy (CLAUDE.md):
--   - silver.fbref_player_season_profile.season = varchar slug ('2526')  -- #404
--   - silver.fotmob_player_profile.season       = varchar slug ('2526')  -- #404
--   - silver.sofascore_player_profile.season    = varchar ('2526')
--   - silver.xref_player.season                 = varchar ('2526')
-- В этом SQL season НЕ используется в JOIN-ключе — snapshot-зерно делает
-- season-mapping излишним. `MAX_BY(... ORDER BY season DESC)` берёт значение
-- атрибута из самого свежего сезона на стороне каждого источника
-- независимо.
-- =============================================================================

WITH
-- #463: silver-профиль per-(player, squad) — multi-squad сезон делает
-- MAX_BY(squad, season) AS current_team недетерминированным. Pre-dedup до
-- одной строки на (player, season): max-minutes клуб, tie → squad —
-- консистентно с team_id в fct_player_season_stats.
fbref_profile_dedup AS (
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY player_id, season
                   ORDER BY minutes DESC NULLS LAST, squad
               ) AS rn
        FROM iceberg.silver.fbref_player_season_profile
        WHERE player_id IS NOT NULL
    ) WHERE rn = 1
),

fbref_latest AS (
    SELECT
        player_id,
        MAX_BY(player, season)  AS player_name,
        MAX_BY(born,   season)  AS born_year,
        MAX_BY(nation, season)  AS nationality,
        MAX_BY(squad,  season)  AS current_team
    FROM fbref_profile_dedup
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

-- TM-side: silver.transfermarkt_players уже содержит canonical_id (LEFT JOIN
-- xref_player в Silver, source='transfermarkt' non-orphan). GROUP BY
-- canonical_id + MAX_BY(season) сворачивает snapshot до одного row на
-- канонического игрока (как для SofaScore — без xref hop в Gold).
-- contract_until / current_market_value_eur / mv_last_update — slowly-changing,
-- snapshot "as-of-latest-ingest"; full MV timeline → fct_player_market_value
-- (issue #11).
transfermarkt_latest AS (
    SELECT
        canonical_id,
        MAX_BY(name,                     season) AS player_name,
        MAX_BY(height_cm,                season) AS height_cm,
        MAX_BY(dob,                      season) AS dob,
        MAX_BY(foot,                     season) AS foot,
        MAX_BY(nationality,              season) AS nationality,
        MAX_BY(current_market_value_eur, season) AS current_market_value_eur,
        MAX_BY(mv_last_update,           season) AS mv_last_update,
        MAX_BY(contract_until,           season) AS contract_until
    FROM iceberg.silver.transfermarkt_players
    WHERE canonical_id IS NOT NULL
    GROUP BY canonical_id
),

-- SoFIFA-side: silver.sofifa_player_profile уже содержит canonical_id (LEFT JOIN
-- xref_player в Silver, source='sofifa' non-orphan). GROUP BY canonical_id +
-- MAX_BY(..., season) сворачивает per-edition snapshot к одному row на
-- канонического игрока (как SofaScore/TM — без xref hop в Gold). Game-side
-- EA-рейтинги (overall/pace/value_eur/wage_eur/…) переехали в
-- gold.fct_player_fifa_rating per-(player, fifa_edition) (#430/#609); здесь
-- остаются только identity- и контрактные атрибуты (weight_kg — единственный
-- источник веса на платформе).
sofifa_latest AS (
    SELECT
        canonical_id,
        MAX_BY(release_clause_eur,    season) AS release_clause_eur,
        MAX_BY(contract_valid_until,  season) AS contract_valid_until,
        MAX_BY(joined,                season) AS joined,
        MAX_BY(position,              season) AS position,
        MAX_BY(dob,                   season) AS dob,
        MAX_BY(height_cm,             season) AS height_cm,
        MAX_BY(weight_kg,             season) AS weight_kg,
        MAX_BY(nationality,           season) AS nationality
    FROM iceberg.silver.sofifa_player_profile
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
    xf.canonical_id                                    AS player_id,
    COALESCE(fb.player_name, fm.player_name, ss.player_name, tm.player_name)
                                                       AS player_name,

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

    -- Transfermarkt block (snapshot, as-of-latest-ingest). Primary source
    -- for height_cm (parsed from official club profile) и MV в EUR.
    tm.height_cm                                       AS height_cm_tm,
    tm.dob                                             AS dob_tm,
    tm.foot                                            AS foot_tm,
    tm.nationality                                     AS nationality_tm,
    tm.current_market_value_eur                        AS current_market_value_eur_tm,
    tm.mv_last_update                                  AS mv_last_update_tm,
    tm.contract_until                                  AS contract_until_tm,

    -- SoFIFA block — identity + контрактные атрибуты. Game-side EA-рейтинги
    -- (overall/pace/value_eur/wage_eur/…) теперь в gold.fct_player_fifa_rating
    -- per-(player, fifa_edition) (#609).
    sf.release_clause_eur                              AS release_clause_eur_sofifa,
    sf.contract_valid_until                            AS contract_valid_until_sofifa,
    sf.joined                                          AS joined_sofifa,
    sf.position                                        AS position_sofifa,
    sf.dob                                             AS dob_sofifa,
    sf.height_cm                                       AS height_cm_sofifa,
    sf.weight_kg                                       AS weight_kg_sofifa,
    sf.nationality                                     AS nationality_sofifa,

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
LEFT JOIN transfermarkt_latest tm
    ON tm.canonical_id = xf.canonical_id
LEFT JOIN sofifa_latest sf
    ON sf.canonical_id = xf.canonical_id
