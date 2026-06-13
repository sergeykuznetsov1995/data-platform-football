-- =============================================================================
-- Gold: fct_keeper_season_stats
-- =============================================================================
--
-- Per-season cross-source stats для вратарей: FBref+FotMob объединены через
-- silver.xref_player. Структурно идентично fct_player_season_stats, но
-- источники = silver.fbref_keeper_profile + silver.fotmob_keeper_profile.
--
-- Design contract: docs/design/gold-star-schema.md §5.3 (issue #428).
-- Grain: (player_id, league, season). PK: natural composite — non-NULL по
-- INNER FBref-spine. FK: player_id → dim_player, team_id → dim_team
-- (orphan-fallback 'fb_<slug>', §6.2). Partitioning: (league, season).
--
-- #428 renames vs v1: player_id_canonical → player_id; primary_team_name
-- дропнут (контекст через dim_team); save_pct_fbref → save_pct (FBref —
-- primary spine; FotMob-шкала не калибрована, остаётся отдельной колонкой);
-- goals_prevented → psxg_minus_ga (см. ниже). Audit-таблица сохраняет
-- player_id_canonical (вне scope #428).
-- Spine — FBref subset из xref_player.
--
-- Cross-source season type: см. headerный комментарий fct_player_season_stats.sql.
-- #404 unified all silver/xref season onto the slug form → JOINs are slug = slug.
--
-- ⚠️ save_pct / save_percentage: FBref `save_pct` хранится как % (e.g. 75.5),
--    FotMob `save_percentage` — также формат %, но шкала может различаться.
--    Чтобы не маскировать расхождения, обе колонки публикуются БЕЗ COALESCE
--    (#428: FBref-колонка теперь `save_pct` — дизайн-имя §5.3, FBref primary;
--    FotMob остаётся `save_percentage_fotmob`). После калибровки в R1 —
--    решить, можно ли свернуть в одну.
--
-- ⚠️ psxg_minus_ga (§5.3): FBref PSxG мёртв с Feb-2026 (keeper_adv в
--    expected-NULL allowlist) → колонка берётся из FotMob `goals_prevented`,
--    который и есть PSxG − GA (xGOT faced минус пропущенные).
--
-- HARD_FACT (5): matches, minutes, clean_sheets, yellow_cards, red_cards —
--                overlap, COALESCE FBref→FotMob.
-- UNIQUE_FBREF (~13): goals_against, goals_against_per90, shots_on_target_against,
--                     saves, save_pct_fbref, wins, draws, losses, clean_sheet_pct,
--                     pk_faced, pk_allowed, pk_saved, pk_missed, pk_save_pct.
-- UNIQUE_FOTMOB (5): save_percentage_fotmob, saves_per_90, goals_prevented,
--                    accurate_passes_per_90, accurate_long_balls_per_90,
--                    fotmob_rating.
-- UNIQUE_WHOSCORED (3): keeper_saves_whoscored, keeper_pickups_whoscored,
--                       keeper_claims_whoscored — event-aggregate из SPADL,
--                       no overlap с FBref `saves` (другая дефиниция: SPADL
--                       keeper_save включает on-target shots где вратарь
--                       реально парировал, не учитывая блоки defender'ов).
--
-- Cross-source audit-diff (FBref - FotMob per HARD_FACT) вынесены в отдельную
-- таблицу `gold.fct_keeper_season_stats_audit`.
-- =============================================================================

WITH
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_player_id,
        league,
        season                                            AS season_slug,  -- varchar '2526' (для WS JOIN)
        season  /* #404: slug passthrough (was slug→year-start) */      AS season_year
    FROM iceberg.silver.xref_player
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

xref_fotmob AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fotmob_player_id,
        league,
        season  /* #404: slug passthrough (was slug→year-start) */      AS season_year
    FROM iceberg.silver.xref_player
    WHERE source = 'fotmob'
      AND confidence <> 'orphan'
),

-- Team FK bridge (#428 §5.3): fb.squad (FBref squad NAME) → canonical team_id.
-- ⚠️ JOIN обязан включать (league, season) — feedback_xref_join_season_predicate.
xref_team_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_team_name,
        league,
        season
    FROM iceberg.silver.xref_team
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

-- #463: silver.fbref_keeper_profile grain = (player_id, squad, league, season)
-- — зимний трансфер даёт 2 строки на вратаря-сезон. Gold PK остаётся
-- (player_id, league, season): выживает ЦЕЛИКОМ строка клуба с максимумом
-- минут (§5.3), tie → squad ASC. SUM-агрегация по клубам — followup
-- (save_pct / clean_sheet_pct несуммируемы).
fb_dedup AS (
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY player_id, league, season
                   ORDER BY minutes DESC NULLS LAST, squad
               ) AS rn
        FROM iceberg.silver.fbref_keeper_profile
    ) WHERE rn = 1
)

SELECT
    -- ========= PK / FK (per-season) =========
    xf.canonical_id                                      AS player_id,
    xf.league                                            AS league,
    xf.season_year                                       AS season,
    COALESCE(
        xt.canonical_id,
        'fb_' || lower(regexp_replace(fb.squad, '[^a-zA-Z0-9]+', '_'))
    )                                                    AS team_id,

    -- ========= HARD_FACT overlap (FBref primary, COALESCE FotMob) =========
    COALESCE(fb.mp,                fm.matches_played)          AS matches,
    COALESCE(fb.minutes,           fm.minutes_played)          AS minutes,
    COALESCE(fb.clean_sheets,      fm.clean_sheets)            AS clean_sheets,
    COALESCE(fb.yellow_cards,      fm.yellow_cards)            AS yellow_cards,
    COALESCE(fb.red_cards,         fm.red_cards)               AS red_cards,

    -- ========= UNIQUE_FBREF (keeper-specific) =========
    fb.goals_against,
    fb.goals_against_per90,
    fb.shots_on_target_against,
    fb.saves,
    -- #428: дизайн-имя save_pct (FBref primary); FotMob-вариант остаётся
    -- отдельной колонкой save_percentage_fotmob (шкалы не калиброваны).
    fb.save_pct,
    fb.wins,
    fb.draws,
    fb.losses,
    fb.clean_sheet_pct,
    fb.pk_faced,
    fb.pk_allowed,
    fb.pk_saved,
    fb.pk_missed,
    fb.pk_save_pct,

    -- ========= UNIQUE_FOTMOB (keeper-specific) =========
    fm.save_percentage                                   AS save_percentage_fotmob,
    fm.saves_per_90,
    -- #428 §5.3: FotMob goals_prevented ≡ PSxG − GA (FBref PSxG мёртв Feb-2026).
    fm.goals_prevented                                   AS psxg_minus_ga,
    fm.accurate_passes_per_90,
    fm.accurate_long_balls_per_90,
    fm.fotmob_rating,

    -- ========= UNIQUE_WHOSCORED (3) =========
    ws.keeper_saves                                      AS keeper_saves_whoscored,
    ws.keeper_pickups                                    AS keeper_pickups_whoscored,
    ws.keeper_claims                                     AS keeper_claims_whoscored,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                    AS _gold_created_at

-- INNER JOIN на FBref keeper Silver: spine = FBref всех игроков, но keeper-витрина
-- должна содержать только тех, кто реально вратарь (silver.fbref_keeper_profile
-- уже отфильтрован WHERE pos LIKE '%GK%'). INNER JOIN автоматически даёт нужный
-- набор без отдельного WHERE pos-условия.
FROM xref_fbref xf
-- #463: fb_dedup (max-minutes club) вместо raw silver — keeper-профиль теперь
-- per-(player, squad); без дедупа PK развалится на мульти-squad вратарях.
INNER JOIN fb_dedup fb
    ON  fb.player_id = xf.fbref_player_id
    AND fb.league    = xf.league
    AND fb.season    = xf.season_year
LEFT JOIN xref_team_fbref xt
    ON  xt.fbref_team_name = fb.squad
    AND xt.league          = xf.league
    AND xt.season          = xf.season_year
LEFT JOIN xref_fotmob xfm
    ON  xfm.canonical_id = xf.canonical_id
    AND xfm.league       = xf.league
    AND xfm.season_year  = xf.season_year
LEFT JOIN iceberg.silver.fotmob_keeper_profile fm
    ON  fm.player_id = xfm.fotmob_player_id
    AND fm.league    = xfm.league
    AND fm.season    = xfm.season_year
-- WhoScored event-aggregate: keeper_saves/pickups/claims. JOIN на varchar slug.
LEFT JOIN iceberg.silver.whoscored_player_season_aggregate ws
    ON  ws.canonical_id = xf.canonical_id
    AND ws.league       = xf.league
    AND ws.season       = xf.season_slug
