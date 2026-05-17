-- =============================================================================
-- Gold: fct_player_season_stats_audit
-- =============================================================================
--
-- DQ-audit таблица для cross-source согласованности FBref vs FotMob по
-- HARD_FACT метрикам в `gold.fct_player_season_stats`. НЕ business-витрина:
-- содержит ТОЛЬКО технические diff-колонки + PK.
--
-- Зерно: (player_id_canonical, league, season). Один row per канонический
-- игрок × лига × сезон, **только когда обе стороны имеют запись** (INNER JOIN
-- на FBref AND FotMob). Это даёт чистый сигнал "оба источника измерили этого
-- игрока в этом сезоне" без NULL-шума от FBref-only исторических сезонов.
--
-- Audit columns: diff = FBref - <source> для каждого HARD_FACT.
--   * FotMob diff (8): INNER JOIN — все 8 HARD_FACT.
--   * WhoScored diff (1): LEFT JOIN — только matches_diff_whoscored (WS
--                         event-aggregate не отдаёт остальные HARD_FACT).
--   * Understat diff (6): LEFT JOIN — matches/minutes/goals/assists/
--                         yellow_cards/red_cards.
-- Используются:
--   1. DQ coverage WARNING (`audit_diff[...]`) — ABS(diff) <= threshold у ≥95% rows.
--   2. Engineer-debug при «голы не сходятся в дашборде».
--   3. R1 калибровка: измерить p95/p99 → R1_cross_source_thresholds.md.
--
-- Источники (читаем заново из Silver, НЕ из gold.fct_player_season_stats —
-- это держит audit-таблицу независимой от business-витрины и сохраняет
-- one-hop правило для Gold):
--   silver.xref_player                          (canonical_id spine)
--   silver.fbref_player_season_profile          (fb.* HARD_FACT)
--   silver.fotmob_player_season_profile         (fm.* HARD_FACT)
--
-- Связь с `gold.fct_player_season_stats`:
--   audit-таблица — subset main таблицы (только rows с обоими источниками).
--   PK совпадает → JOIN ON (player_id_canonical, league, season).
-- =============================================================================

WITH
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_player_id,
        league,
        season                                            AS season_slug,  -- varchar '2526' (для WS/US JOIN)
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
    -- ========= PK (грейн совпадает с fct_player_season_stats) =========
    xf.canonical_id                                      AS player_id_canonical,
    xf.league                                            AS league,
    xf.season_year                                       AS season,

    -- ========= Audit (8 HARD_FACT diff: FBref - FotMob) =========
    -- INNER JOIN гарантирует что обе стороны не-NULL, поэтому diff
    -- всегда заполнен. Это упрощает DQ-чек coverage без `OR diff IS NULL`.
    (CAST(fb.mp                 AS DOUBLE) - CAST(fm.matches_played     AS DOUBLE)) AS matches_diff_fotmob,
    (CAST(fb.minutes            AS DOUBLE) - CAST(fm.minutes_played     AS DOUBLE)) AS minutes_diff_fotmob,
    (CAST(fb.goals              AS DOUBLE) - CAST(fm.goals              AS DOUBLE)) AS goals_diff_fotmob,
    (CAST(fb.assists            AS DOUBLE) - CAST(fm.assists            AS DOUBLE)) AS assists_diff_fotmob,
    (CAST(fb.yellow_cards       AS DOUBLE) - CAST(fm.yellow_cards       AS DOUBLE)) AS yellow_cards_diff_fotmob,
    (CAST(fb.red_cards          AS DOUBLE) - CAST(fm.red_cards          AS DOUBLE)) AS red_cards_diff_fotmob,
    (CAST(fb.penalties_won      AS DOUBLE) - CAST(fm.penalties_won      AS DOUBLE)) AS penalties_won_diff_fotmob,
    (CAST(fb.penalties_conceded AS DOUBLE) - CAST(fm.penalties_conceded AS DOUBLE)) AS penalties_conceded_diff_fotmob,

    -- ========= WhoScored diff (1; LEFT JOIN → NULL if WS отсутствует) =========
    (CAST(fb.mp                 AS DOUBLE) - CAST(ws.matches_seen       AS DOUBLE)) AS matches_diff_whoscored,

    -- ========= Understat diff (6; LEFT JOIN → NULL if US отсутствует) =========
    (CAST(fb.mp                 AS DOUBLE) - CAST(us.games_played       AS DOUBLE)) AS matches_diff_understat,
    (CAST(fb.minutes            AS DOUBLE) - CAST(us.minutes_played     AS DOUBLE)) AS minutes_diff_understat,
    (CAST(fb.goals              AS DOUBLE) - CAST(us.goals              AS DOUBLE)) AS goals_diff_understat,
    (CAST(fb.assists            AS DOUBLE) - CAST(us.assists            AS DOUBLE)) AS assists_diff_understat,
    (CAST(fb.yellow_cards       AS DOUBLE) - CAST(us.yellow_cards       AS DOUBLE)) AS yellow_cards_diff_understat,
    (CAST(fb.red_cards          AS DOUBLE) - CAST(us.red_cards          AS DOUBLE)) AS red_cards_diff_understat,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                    AS _gold_created_at

FROM xref_fbref xf
INNER JOIN iceberg.silver.fbref_player_season_profile fb
    ON  fb.player_id = xf.fbref_player_id
    AND fb.league    = xf.league
    AND fb.season    = xf.season_year
INNER JOIN xref_fotmob xfm
    ON  xfm.canonical_id = xf.canonical_id
    AND xfm.league       = xf.league
    AND xfm.season_year  = xf.season_year
INNER JOIN iceberg.silver.fotmob_player_season_profile fm
    ON  fm.player_id = xfm.fotmob_player_id
    AND fm.league    = xfm.league
    AND fm.season    = xfm.season_year
-- WhoScored / Understat LEFT JOIN (НЕ INNER): сохраняем FotMob-INNER семантику
-- main fct (audit-coverage = FBref ∩ FotMob), а WS/US диффы — добавка с
-- nullable значениями. Coverage DQ-чеки фильтруют NULL через `OR <col> IS NULL`.
LEFT JOIN iceberg.silver.whoscored_player_season_aggregate ws
    ON  ws.canonical_id = xf.canonical_id
    AND ws.league       = xf.league
    AND ws.season       = xf.season_slug
LEFT JOIN iceberg.silver.understat_player_season_aggregate us
    ON  us.canonical_id = xf.canonical_id
    AND us.league       = xf.league
    AND us.season       = xf.season_slug
-- Outfield-only: симметрично с main fct (исключаем GK).
WHERE fb.pos IS NULL OR fb.pos NOT LIKE '%GK%'
