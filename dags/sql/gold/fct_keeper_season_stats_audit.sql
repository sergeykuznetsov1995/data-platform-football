-- =============================================================================
-- Gold: fct_keeper_season_stats_audit
-- =============================================================================
--
-- DQ-audit таблица для cross-source согласованности FBref vs FotMob по
-- HARD_FACT метрикам в `gold.fct_keeper_season_stats`. Структурно идентично
-- fct_player_season_stats_audit, источники = silver.fbref_keeper_profile +
-- silver.fotmob_keeper_profile.
--
-- Audit columns:
--   * FotMob diff (3): matches/minutes/clean_sheets — INNER JOIN.
--   * WhoScored diff (1): saves_diff_whoscored — LEFT JOIN.
-- yellow/red skipped — у вратарей почти всегда 0/0, audit-сигнала нет.
--
-- Зерно: (player_id_canonical, league, season). INNER JOIN на оба источника.
-- =============================================================================

WITH
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_player_id,
        league,
        season                                            AS season_slug,  -- varchar '2526' (для WS JOIN)
        2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)      AS season_year
    FROM iceberg.silver.xref_player
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

xref_fotmob AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fotmob_player_id,
        league,
        2000 + CAST(SUBSTR(season, 1, 2) AS BIGINT)      AS season_year
    FROM iceberg.silver.xref_player
    WHERE source = 'fotmob'
      AND confidence <> 'orphan'
)

SELECT
    -- ========= PK =========
    xf.canonical_id                                      AS player_id_canonical,
    xf.league                                            AS league,
    xf.season_year                                       AS season,

    -- ========= Audit (3 HARD_FACT diff) =========
    (CAST(fb.mp           AS DOUBLE) - CAST(fm.matches_played AS DOUBLE)) AS matches_diff_fotmob,
    (CAST(fb.minutes      AS DOUBLE) - CAST(fm.minutes_played AS DOUBLE)) AS minutes_diff_fotmob,
    (CAST(fb.clean_sheets AS DOUBLE) - CAST(fm.clean_sheets   AS DOUBLE)) AS clean_sheets_diff_fotmob,

    -- ========= WhoScored diff (1; LEFT JOIN) =========
    (CAST(fb.saves        AS DOUBLE) - CAST(ws.keeper_saves   AS DOUBLE)) AS saves_diff_whoscored,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                    AS _gold_created_at

FROM xref_fbref xf
INNER JOIN iceberg.silver.fbref_keeper_profile fb
    ON  fb.player_id = xf.fbref_player_id
    AND fb.league    = xf.league
    AND fb.season    = xf.season_year
INNER JOIN xref_fotmob xfm
    ON  xfm.canonical_id = xf.canonical_id
    AND xfm.league       = xf.league
    AND xfm.season_year  = xf.season_year
INNER JOIN iceberg.silver.fotmob_keeper_profile fm
    ON  fm.player_id = xfm.fotmob_player_id
    AND fm.league    = xfm.league
    AND fm.season    = xfm.season_year
-- WhoScored event-aggregate: LEFT JOIN сохраняет FBref∩FotMob spine
-- (keeper_saves может быть NULL если вратарь не имеет WS coverage).
LEFT JOIN iceberg.silver.whoscored_player_season_aggregate ws
    ON  ws.canonical_id = xf.canonical_id
    AND ws.league       = xf.league
    AND ws.season       = xf.season_slug
