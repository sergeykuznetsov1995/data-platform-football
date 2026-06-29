-- =============================================================================
-- Gold: fct_player_season_stats_audit
-- =============================================================================
--
-- DQ-audit таблица для cross-source согласованности по HARD_FACT метрикам в
-- `gold.fct_player_season_stats`. НЕ business-витрина: содержит ТОЛЬКО
-- технические diff-колонки + PK.
--
-- Convention: diff = FBref - <source> для каждого HARD_FACT (FBref — primary
-- spine). Если у FBref нет данной HARD_FACT (clearances, ball_recoveries,
-- blocks, dribbles_attempted, total_passes, accurate_passes, accurate_long_balls,
-- key_passes, accurate_crosses, tackles_attempted) — primary = FotMob; diff =
-- FotMob - <source>.
--
-- Зерно: (player_id, league, season). Один row per канонический
-- игрок × лига × сезон, **только когда обе стороны имеют запись** (INNER JOIN
-- FBref ∩ FotMob — symmetric to main fct INNER FBref). WhoScored/Understat/
-- SofaScore — LEFT JOIN → diff = NULL когда источник отсутствует.
--
-- Использование:
--   1. DQ coverage WARNING — ABS(diff) <= threshold у ≥95% rows.
--   2. Engineer-debug при «голы не сходятся в дашборде».
--   3. R1 калибровка thresholds → docs/research/R1_cross_source_thresholds.md.
--
-- ⚠️ Audit-таблица читает Silver заново (НЕ gold.fct_player_season_stats):
--   one-hop правило (memory: project_gold_cleanup_2026-05-12).
--
-- #556: остаётся inline .sql (НЕ мигрирован на source_priority.yaml) — per-source
-- diff-layout несовместим с single-COALESCE эмиттером. Решение:
-- docs/decisions/season-audit-inline.md
-- =============================================================================

WITH
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_player_id,
        league,
        season                                            AS season_slug,
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

-- FotMob отдаёт shots/tackles/clearances/… как per-90 (не счётчики). Restore
-- season-count ≈ per_90 × minutes / 90 (±1, per_90 округлён источником до 2 знаков).
-- raw counts для этих метрик в FotMob API недоступны (issue #174). Pass-through
-- колонки (goals/assists/cards/xG/…) уже счётчики — через SELECT *.
fotmob_counts AS (
    SELECT
        *,
        ROUND(shots_per_90               * minutes_played / 90.0) AS shots,
        ROUND(shots_on_target_per_90     * minutes_played / 90.0) AS shots_on_target,
        ROUND(interceptions_per_90       * minutes_played / 90.0) AS interceptions,
        ROUND(tackles_per_90             * minutes_played / 90.0) AS tackles,
        ROUND(fouls_per_90               * minutes_played / 90.0) AS fouls_committed,
        ROUND(clearances_per_90          * minutes_played / 90.0) AS clearances,
        ROUND(recoveries_per_90          * minutes_played / 90.0) AS ball_recoveries,
        ROUND(blocks_per_90              * minutes_played / 90.0) AS blocks,
        ROUND(successful_dribbles_per_90 * minutes_played / 90.0) AS successful_dribbles,
        ROUND(accurate_passes_per_90     * minutes_played / 90.0) AS accurate_passes,
        ROUND(accurate_long_balls_per_90 * minutes_played / 90.0) AS accurate_long_balls,
        ROUND(defensive_actions_per_90   * minutes_played / 90.0) AS defensive_actions,
        ROUND(poss_won_final_third_per_90 * minutes_played / 90.0) AS poss_won_final_third
    FROM iceberg.silver.fotmob_player_season_profile
),

-- #463/#515: silver-профиль per-(player, squad) — счётчики СУММИРУЮТСЯ по клубам
-- сезона (SUM ... OVER w), зеркально fct_player_season_stats (Вариант B). Иначе
-- FBref-spine брал бы один клуб, а main fct — сумму → diff'ы поехали бы. team_id
-- здесь не нужен; pos берётся от max-minutes клуба (rn=1) для outfield-фильтра.
fb_dedup AS (
    SELECT * FROM (
        SELECT
            player_id,
            league,
            season,
            pos,
            SUM(mp)                 OVER w AS mp,
            SUM(minutes)            OVER w AS minutes,
            SUM(goals)              OVER w AS goals,
            SUM(assists)            OVER w AS assists,
            SUM(yellow_cards)       OVER w AS yellow_cards,
            SUM(red_cards)          OVER w AS red_cards,
            SUM(shots)              OVER w AS shots,
            SUM(shots_on_target)    OVER w AS shots_on_target,
            SUM(interceptions)      OVER w AS interceptions,
            SUM(tackles_won)        OVER w AS tackles_won,
            SUM(fouls_committed)    OVER w AS fouls_committed,
            SUM(fouls_drawn)        OVER w AS fouls_drawn,
            SUM(offsides)           OVER w AS offsides,
            SUM(crosses)            OVER w AS crosses,
            SUM(penalties_won)      OVER w AS penalties_won,
            SUM(penalties_conceded) OVER w AS penalties_conceded,
            SUM(penalty_goals)      OVER w AS penalty_goals,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, league, season
                ORDER BY minutes DESC NULLS LAST, squad
            ) AS rn
        FROM iceberg.silver.fbref_player_season_profile
        WINDOW w AS (PARTITION BY player_id, league, season)
    ) WHERE rn = 1
)

SELECT
    -- ========= PK (грейн совпадает с fct_player_season_stats) =========
    xf.canonical_id                                      AS player_id,
    xf.league                                            AS league,
    xf.season_year                                       AS season,

    -- ========= FotMob diff (INNER JOIN — всегда non-NULL) =========
    -- HARD_FACT pairs с FBref-spine. NB (issue #564): FotMob отдаёт счётные
    -- события (goals/assists/cards) разреженно — у игрока без события строки в
    -- bronze нет, silver-пивот возвращает NULL. COALESCE(fm.*, 0) трактует
    -- «не было события = 0» (а не NULL), иначе diff=NULL съедает ~38% пар и
    -- искажает coverage/within-threshold. matches/minutes НЕ coalesce'им
    -- (NULL ≠ 0, и они покрыты ~100%).
    (CAST(fb.mp                 AS DOUBLE) - CAST(fm.matches_played            AS DOUBLE)) AS matches_diff_fotmob,
    (CAST(fb.minutes            AS DOUBLE) - CAST(fm.minutes_played            AS DOUBLE)) AS minutes_diff_fotmob,
    (CAST(fb.goals              AS DOUBLE) - CAST(COALESCE(fm.goals, 0)        AS DOUBLE)) AS goals_diff_fotmob,
    (CAST(fb.assists            AS DOUBLE) - CAST(COALESCE(fm.assists, 0)      AS DOUBLE)) AS assists_diff_fotmob,
    (CAST(fb.yellow_cards       AS DOUBLE) - CAST(COALESCE(fm.yellow_cards, 0) AS DOUBLE)) AS yellow_cards_diff_fotmob,
    (CAST(fb.red_cards          AS DOUBLE) - CAST(COALESCE(fm.red_cards, 0)    AS DOUBLE)) AS red_cards_diff_fotmob,
    -- penalties_won_diff_fotmob / penalties_conceded_diff_fotmob удалены (issue #564):
    -- FotMob не отдаёт сезонные penalty_won/penalty_conceded (колонки полностью NULL,
    -- 0/430 сравнимых пар) → нечего сравнивать. SofaScore penalties остаются (ниже).
    -- NB (issue #154): FotMob silver больше не отдаёт абсолюты shots/
    -- shots_on_target/interceptions/fouls/clearances/ball_recoveries/blocks/
    -- accurate_passes/accurate_long_balls/successful_dribbles → соответствующие
    -- diff-колонки удалены (нечего сравнивать). НЕ возвращай без восстановления
    -- absolute-полей в silver.fotmob_player_season_profile.

    -- ========= WhoScored diff (LEFT JOIN → NULL if absent) =========
    (CAST(fb.mp              AS DOUBLE) - CAST(ws.matches_seen      AS DOUBLE)) AS matches_diff_whoscored,
    (CAST(fb.shots           AS DOUBLE) - CAST(ws.shots_total       AS DOUBLE)) AS shots_diff_whoscored,
    (CAST(fb.shots_on_target AS DOUBLE) - CAST(ws.shots_on_target_proxy AS DOUBLE)) AS shots_on_target_diff_whoscored,
    (CAST(fb.interceptions   AS DOUBLE) - CAST(ws.interceptions     AS DOUBLE)) AS interceptions_diff_whoscored,
    (CAST(fb.tackles_won     AS DOUBLE) - CAST(ws.tackle_won        AS DOUBLE)) AS tackles_won_diff_whoscored,
    (CAST(fb.fouls_committed AS DOUBLE) - CAST(ws.fouls_committed   AS DOUBLE)) AS fouls_committed_diff_whoscored,
    -- clearances/ball_recoveries/accurate_passes/successful_dribbles_diff_whoscored
    -- удалены (issue #154): primary был FotMob, чьи absolute-поля больше нет в Silver.

    -- ========= Understat diff (LEFT JOIN → NULL if absent) =========
    (CAST(fb.mp           AS DOUBLE) - CAST(us.games_played   AS DOUBLE)) AS matches_diff_understat,
    (CAST(fb.minutes      AS DOUBLE) - CAST(us.minutes_played AS DOUBLE)) AS minutes_diff_understat,
    (CAST(fb.goals        AS DOUBLE) - CAST(us.goals          AS DOUBLE)) AS goals_diff_understat,
    (CAST(fb.assists      AS DOUBLE) - CAST(us.assists        AS DOUBLE)) AS assists_diff_understat,
    (CAST(fb.yellow_cards AS DOUBLE) - CAST(us.yellow_cards   AS DOUBLE)) AS yellow_cards_diff_understat,
    (CAST(fb.red_cards    AS DOUBLE) - CAST(us.red_cards      AS DOUBLE)) AS red_cards_diff_understat,
    (CAST(fb.shots        AS DOUBLE) - CAST(us.shots          AS DOUBLE)) AS shots_diff_understat,

    -- ========= SofaScore diff (LEFT JOIN → NULL if absent) =========
    (CAST(fb.goals              AS DOUBLE) - CAST(ss.goals_inside_box + ss.goals_outside_box AS DOUBLE)) AS goals_diff_sofascore,
    (CAST(fb.penalties_won      AS DOUBLE) - CAST(ss.penalty_won      AS DOUBLE)) AS penalties_won_diff_sofascore,
    (CAST(fb.penalties_conceded AS DOUBLE) - CAST(ss.penalty_conceded AS DOUBLE)) AS penalties_conceded_diff_sofascore,
    (CAST(fb.penalty_goals      AS DOUBLE) - CAST(ss.penalty_goals    AS DOUBLE)) AS penalty_goals_diff_sofascore,
    (CAST(fb.shots              AS DOUBLE) - CAST(ss.total_shots      AS DOUBLE)) AS shots_diff_sofascore,
    (CAST(fb.shots_on_target    AS DOUBLE) - CAST(ss.shots_on_target  AS DOUBLE)) AS shots_on_target_diff_sofascore,
    (CAST(fb.interceptions      AS DOUBLE) - CAST(ss.interceptions    AS DOUBLE)) AS interceptions_diff_sofascore,
    (CAST(fb.tackles_won        AS DOUBLE) - CAST(ss.tackles_won      AS DOUBLE)) AS tackles_won_diff_sofascore,
    (CAST(fb.fouls_committed    AS DOUBLE) - CAST(ss.fouls            AS DOUBLE)) AS fouls_committed_diff_sofascore,
    (CAST(fb.fouls_drawn        AS DOUBLE) - CAST(ss.was_fouled       AS DOUBLE)) AS fouls_drawn_diff_sofascore,
    (CAST(fb.offsides           AS DOUBLE) - CAST(ss.offsides         AS DOUBLE)) AS offsides_diff_sofascore,
    (CAST(fb.crosses            AS DOUBLE) - CAST(ss.total_crosses    AS DOUBLE)) AS crosses_diff_sofascore,
    -- clearances/ball_recoveries/blocks/accurate_passes/accurate_long_balls/
    -- successful_dribbles_diff_sofascore удалены (issue #154): primary был FotMob,
    -- чьи absolute-поля больше нет в Silver.
    (CAST(us.key_passes         AS DOUBLE) - CAST(ss.key_passes       AS DOUBLE)) AS key_passes_diff_sofascore,

    -- ========= MODELED xG diff (different models, expected to disagree) =========
    -- Эти diff'ы для калибровки разных xG-моделей. Хранятся в audit чтобы DS-
    -- команда могла строить корреляции между моделями.
    ROUND(CAST(fm.expected_goals  AS DOUBLE) - CAST(us.expected_goals AS DOUBLE), 4) AS xg_diff_fotmob_understat,
    ROUND(CAST(fm.expected_goals  AS DOUBLE) - CAST(ss.expected_goals AS DOUBLE), 4) AS xg_diff_fotmob_sofascore,
    ROUND(CAST(us.expected_goals  AS DOUBLE) - CAST(ss.expected_goals AS DOUBLE), 4) AS xg_diff_understat_sofascore,
    ROUND(CAST(fm.expected_assists AS DOUBLE) - CAST(us.expected_assists AS DOUBLE), 4) AS xa_diff_fotmob_understat,
    ROUND(CAST(fm.fotmob_rating   AS DOUBLE) - CAST(ss.rating         AS DOUBLE), 4) AS rating_diff_fotmob_sofascore,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                    AS _gold_created_at

FROM xref_fbref xf
-- #463: fb_dedup (max-minutes club) вместо raw silver.
INNER JOIN fb_dedup fb
    ON  fb.player_id = xf.fbref_player_id
    AND fb.league    = xf.league
    AND fb.season    = xf.season_year
INNER JOIN xref_fotmob xfm
    ON  xfm.canonical_id = xf.canonical_id
    AND xfm.league       = xf.league
    AND xfm.season_year  = xf.season_year
INNER JOIN fotmob_counts fm
    ON  fm.player_id = xfm.fotmob_player_id
    AND fm.league    = xfm.league
    AND fm.season    = xfm.season_year
LEFT JOIN iceberg.silver.whoscored_player_season_aggregate ws
    ON  ws.canonical_id = xf.canonical_id
    AND ws.league       = xf.league
    AND ws.season       = xf.season_slug
LEFT JOIN iceberg.silver.understat_player_season_aggregate us
    ON  us.canonical_id = xf.canonical_id
    AND us.league       = xf.league
    AND us.season       = xf.season_slug
LEFT JOIN iceberg.silver.sofascore_player_season_aggregate ss
    ON  ss.canonical_id = xf.canonical_id
    AND ss.league       = xf.league
    AND ss.season       = xf.season_slug
WHERE fb.pos IS NULL OR fb.pos NOT LIKE '%GK%'
