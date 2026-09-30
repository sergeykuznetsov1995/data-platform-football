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
-- игрок × лига × сезон FBref-spine (outfield). WhoScored/Understat/SofaScore —
-- LEFT JOIN → diff = NULL когда источник отсутствует.
--
-- #1590: FotMob-сравнение удалено вместе со старым FotMob Silver (до этого —
-- INNER JOIN FBref ∩ FotMob). Колонки `*_fotmob*` остаются в схеме как
-- типизированный NULL — схема витрины не меняется.
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

    -- ========= FotMob diff — typed NULL since #1590 =========
    CAST(NULL AS DOUBLE)                                 AS matches_diff_fotmob,
    CAST(NULL AS DOUBLE)                                 AS minutes_diff_fotmob,
    CAST(NULL AS DOUBLE)                                 AS goals_diff_fotmob,
    CAST(NULL AS DOUBLE)                                 AS assists_diff_fotmob,
    CAST(NULL AS DOUBLE)                                 AS yellow_cards_diff_fotmob,
    CAST(NULL AS DOUBLE)                                 AS red_cards_diff_fotmob,

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
    -- *_fotmob_* — typed NULL since #1590.
    CAST(NULL AS DOUBLE)                                                             AS xg_diff_fotmob_understat,
    CAST(NULL AS DOUBLE)                                                             AS xg_diff_fotmob_sofascore,
    ROUND(CAST(us.expected_goals  AS DOUBLE) - CAST(ss.expected_goals AS DOUBLE), 4) AS xg_diff_understat_sofascore,
    CAST(NULL AS DOUBLE)                                                             AS xa_diff_fotmob_understat,
    CAST(NULL AS DOUBLE)                                                             AS rating_diff_fotmob_sofascore,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                    AS _gold_created_at

FROM xref_fbref xf
-- #463: fb_dedup (max-minutes club) вместо raw silver.
INNER JOIN fb_dedup fb
    ON  fb.player_id = xf.fbref_player_id
    AND fb.league    = xf.league
    AND fb.season    = xf.season_year
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
