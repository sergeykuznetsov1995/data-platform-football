-- =============================================================================
-- Gold: fct_keeper_season_stats
-- =============================================================================
--
-- Per-season cross-source stats для вратарей: FBref+FotMob объединены через
-- silver.xref_player. Структурно идентично fct_player_season_stats, но
-- источники = silver.fbref_keeper_profile + silver.fotmob_keeper_profile.
--
-- Зерно: (player_id_canonical, league, season). Один row per канонический
-- вратарь × лига × сезон. Spine — FBref subset из xref_player.
--
-- Cross-source season type: см. headerный комментарий fct_player_season_stats.sql.
-- xref slug '2526' → bigint 2025 идиомой
-- `2000 + CAST(SUBSTR(season, 1, 2) AS bigint)` (см. fct_card.sql:51).
--
-- ⚠️ save_pct / save_percentage: FBref `save_pct` хранится как % (e.g. 75.5),
--    FotMob `save_percentage` — также формат %, но шкала может различаться.
--    Чтобы не маскировать расхождения, обе колонки публикуются БЕЗ COALESCE
--    (separate `save_pct_fbref` / `save_percentage_fotmob`). После калибровки
--    в R1 — решить, можно ли свернуть в одну.
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
)

SELECT
    -- ========= Identity (per-season) =========
    xf.canonical_id                                      AS player_id_canonical,
    xf.league                                            AS league,
    xf.season_year                                       AS season,
    COALESCE(fb.squad, fm.primary_team_name)             AS primary_team_name,

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
    fb.save_pct                                          AS save_pct_fbref,
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
    fm.goals_prevented,
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
INNER JOIN iceberg.silver.fbref_keeper_profile fb
    ON  fb.player_id = xf.fbref_player_id
    AND fb.league    = xf.league
    AND fb.season    = xf.season_year
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
