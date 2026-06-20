-- =============================================================================
-- Gold: fct_player_season_stats
-- =============================================================================
--
-- Per-season cross-source stats per игрок: FBref+FotMob+WhoScored+Understat+
-- SofaScore объединены через silver.xref_player. Правило выбора источников
-- зафиксировано в docs/research/RX_cross_source_player_profile.md (D3) и
-- memory/feedback_audit_in_separate_table.md:
--
--   * HARD_FACT (счётные event-метрики, identical definition) →
--     single column через COALESCE(fb→fm→ws→us→ss). FBref — primary spine.
--     Cross-source diff'ы выносятся в `fct_player_season_stats_audit`.
--   * MODELED — два решения после исследований:
--     - xG / xA: Understat выбран как primary (RX2 — coverage 99% vs
--       82-85%; r≥0.989 между источниками). Single column через
--       COALESCE(us → fm → ss).
--     - RATING: SofaScore (Opta-derived) выбран как единственный источник —
--       FotMob rating дропнут из business-fct; cross-source diff остаётся
--       в audit-таблице.
--   * UNIQUE_<source> (метрика отсутствует у других) → single column,
--     без суффикса.
--
-- Design contract: docs/design/gold-star-schema.md §5.2 (issue #428).
-- Grain: (player_id, league, season). PK: natural composite — все компоненты
-- non-NULL по конструкции INNER FBref-spine. FK: player_id → dim_player,
-- team_id → dim_team (клуб игрока в сезоне; orphan-fallback 'fb_<slug>' если
-- squad не резолвится через xref_team — строки не теряются, §6.2).
-- Partitioning: (league, season) — passed by run_gold_transform().
--
-- #428 renames vs v1: player_id_canonical → player_id (plain FK id, паттерн
-- #438); primary_team_name / position_fbref / position_fotmob дропнуты —
-- контекст через dim_team / dim_player_attributes. Audit-таблица сохраняет
-- player_id_canonical (вне scope #428).
-- Spine — FBref subset из xref_player.
--
-- Cross-source season type (all varchar slug 'YYNN' after #404):
--   * silver.xref_player.season                       = varchar slug '2526'
--   * silver.fbref_player_season_profile.season       = varchar slug '2526'
--   * silver.fotmob_player_season_profile.season      = varchar slug '2526'
--   * silver.whoscored_player_season_aggregate.season = varchar slug
--   * silver.understat_player_season_aggregate.season = varchar slug
--   * silver.sofascore_player_season_aggregate.season = varchar slug
--   #404 unified all silver/xref season onto the slug form → JOINs are slug = slug.
--
-- ⚠️ xref JOIN MUST include (league, season) predicate (CLAUDE.md):
--   silver.xref_player имеет per-(source, source_id, season) rows;
--   без season-condition будет fan-out 1.5-4×.
-- =============================================================================

WITH
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_player_id,
        league,
        season                                            AS season_slug,  -- varchar '2526' (для WS/US/SS JOIN)
        season  /* #404: slug passthrough (was slug→year-start) */      AS season_year   -- slug '2526' (для FBref/FotMob JOIN)
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

-- Team FK bridge (#428 §5.2): fb.squad (FBref squad NAME) → canonical team_id.
-- xref_team source='fbref' source_id = squad name (см. xref_team.sql.j2).
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

-- #463: silver.fbref_player_season_profile grain = (player_id, squad, league,
-- season) — зимний трансфер внутри лиги даёт 2 строки на игрока-сезон.
-- Gold PK остаётся (player_id, league, season).
-- #515 (Вариант B): счётчики СУММИРУЮТСЯ по клубам сезона (SUM ... OVER w) —
-- тоталы сходятся с Understat/SofaScore/WhoScored (дают сумму за лигу-сезон).
-- team_id и несуммируемые ratio (points_per_match, on_off_impact — нет компонент
-- для пересчёта) берутся от клуба с максимумом минут (rn=1, §5.2; tie → squad
-- ASC). Ratio с известной формулой (goals_per_shot) пересчитывается из
-- суммированных счётчиков, но ТОЛЬКО для мульти-squad строк (COUNT(*) OVER w > 1)
-- — одноклубные игроки сохраняют родной FBref-ratio (0-diff к Варианту A; лишь
-- ~111 трансферных игроко-сезонов меняются).
fb_dedup AS (
    SELECT * FROM (
        SELECT
            player_id,
            league,
            season,
            squad,
            pos,
            SUM(mp)                 OVER w AS mp,
            SUM(minutes)            OVER w AS minutes,
            SUM(goals)              OVER w AS goals,
            SUM(assists)            OVER w AS assists,
            SUM(yellow_cards)       OVER w AS yellow_cards,
            SUM(red_cards)          OVER w AS red_cards,
            SUM(penalty_goals)      OVER w AS penalty_goals,
            SUM(penalty_attempts)   OVER w AS penalty_attempts,
            SUM(penalties_won)      OVER w AS penalties_won,
            SUM(penalties_conceded) OVER w AS penalties_conceded,
            SUM(shots)              OVER w AS shots,
            SUM(shots_on_target)    OVER w AS shots_on_target,
            SUM(interceptions)      OVER w AS interceptions,
            SUM(tackles_won)        OVER w AS tackles_won,
            SUM(fouls_committed)    OVER w AS fouls_committed,
            SUM(fouls_drawn)        OVER w AS fouls_drawn,
            SUM(offsides)           OVER w AS offsides,
            SUM(crosses)            OVER w AS crosses,
            SUM(own_goals)          OVER w AS own_goals,
            SUM(second_yellow)      OVER w AS second_yellow,
            SUM(complete_matches)   OVER w AS complete_matches,
            SUM(starts)             OVER w AS starts,
            SUM(subs)               OVER w AS subs,
            SUM(unused_sub)         OVER w AS unused_sub,
            SUM(plus_minus)         OVER w AS plus_minus,
            -- ratio с известной формулой → пересчёт из сумм (#515 B2), но только
            -- для мульти-squad; одноклубные сохраняют родной FBref goals_per_shot.
            CASE WHEN COUNT(*) OVER w > 1
                 THEN CAST(SUM(goals) OVER w AS DOUBLE)
                      / NULLIF(SUM(shots) OVER w, 0)
                 ELSE goals_per_shot
            END                     AS goals_per_shot,
            -- несуммируемы (нет компонент в профиле) → от max-minutes клуба (rn=1).
            points_per_match,
            on_off_impact,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, league, season
                ORDER BY minutes DESC NULLS LAST, squad
            ) AS rn
        FROM iceberg.silver.fbref_player_season_profile
        WINDOW w AS (PARTITION BY player_id, league, season)
    ) WHERE rn = 1
),

-- FotMob отдаёт shots/tackles/clearances/… как per-90 (не счётчики). Restore
-- season-count ≈ per_90 × minutes / 90 (±1, per_90 округлён источником до 2 знаков).
-- raw counts для этих метрик в FotMob API недоступны (issue #174). Pass-through
-- колонки (goals/assists/cards/xG/big_chances/…) уже счётчики — через SELECT *.
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
)

SELECT
    -- ========= PK / FK (per-season) =========
    xf.canonical_id                                      AS player_id,
    xf.league                                            AS league,
    xf.season_year                                       AS season,
    -- team_id: клуб игрока в сезоне (FBref squad → xref_team). Live-проверка
    -- #428: 0 unresolved squad rows; fallback — страховка по §6.2.
    COALESCE(
        xt.canonical_id,
        'fb_' || lower(regexp_replace(fb.squad, '[^a-zA-Z0-9]+', '_'))
    )                                                    AS team_id,

    -- ========= HARD_FACT (single column, COALESCE fb→fm→ws→us→ss) =========
    -- Integer counters get CAST(... AS BIGINT) — COALESCE между гетерогенными
    -- source types (FBref varchar, FotMob bigint, US bigint) иначе промотит
    -- в double и BI показывает `90.0` / `3.0`. Cross-source diff'ы (FBref - Х)
    -- хранятся в fct_player_season_stats_audit.
    CAST(COALESCE(fb.mp,                  fm.matches_played, ws.matches_seen, us.games_played) AS BIGINT) AS matches,
    CAST(COALESCE(fb.minutes,             fm.minutes_played, us.minutes_played)               AS BIGINT) AS minutes,
    CAST(COALESCE(fb.goals,               fm.goals,          us.goals)                        AS BIGINT) AS goals,
    CAST(COALESCE(fb.assists,             fm.assists,        us.assists)                      AS BIGINT) AS assists,
    CAST(COALESCE(fb.yellow_cards,        fm.yellow_cards,   us.yellow_cards)                 AS BIGINT) AS yellow_cards,
    CAST(COALESCE(fb.red_cards,           fm.red_cards,      us.red_cards)                    AS BIGINT) AS red_cards,
    CAST(COALESCE(fb.penalty_goals,                                                   ss.penalty_goals)  AS BIGINT) AS penalty_goals,
    CAST(COALESCE(fb.penalty_attempts,                                                ss.penalties_taken) AS BIGINT) AS penalty_attempts,
    CAST(COALESCE(fb.penalties_won,       fm.penalties_won,                           ss.penalty_won)    AS BIGINT) AS penalties_won,
    CAST(COALESCE(fb.penalties_conceded,  fm.penalties_conceded,                      ss.penalty_conceded) AS BIGINT) AS penalties_conceded,
    -- NB (issue #154): FotMob silver больше не отдаёт абсолютные счётчики
    -- (shots/tackles/clearances/... — только `*_per_90`), поэтому `fm.<count>`
    -- удалён из COALESCE-цепочек ниже. FBref остаётся primary spine; ws/us/ss
    -- покрывают эти HARD_FACT. НЕ возвращай `fm.<count>` — колонок нет в Silver.
    CAST(COALESCE(fb.shots,                                  ws.shots_total,         us.shots,           ss.total_shots) AS BIGINT) AS shots,
    CAST(COALESCE(fb.shots_on_target,                        ws.shots_on_target_proxy, ss.shots_on_target) AS BIGINT) AS shots_on_target,
    CAST(COALESCE(fb.interceptions,                          ws.interceptions,        ss.interceptions)  AS BIGINT) AS interceptions,
    CAST(COALESCE(fb.tackles_won,         ws.tackle_won,                              ss.tackles_won)    AS BIGINT) AS tackles_won,
    CAST(COALESCE(                        ws.tackle_att,                              ss.tackles)        AS BIGINT) AS tackles_attempted,
    CAST(COALESCE(fb.fouls_committed,                        ws.fouls_committed,     ss.fouls)          AS BIGINT) AS fouls_committed,
    CAST(COALESCE(fb.fouls_drawn,                                                     ss.was_fouled)     AS BIGINT) AS fouls_drawn,
    CAST(COALESCE(fb.offsides,                                                        ss.offsides)       AS BIGINT) AS offsides,
    CAST(COALESCE(                        ws.clearances,                              ss.clearances)     AS BIGINT) AS clearances,
    CAST(COALESCE(                        ws.ball_recoveries,                         ss.ball_recoveries) AS BIGINT) AS ball_recoveries,
    CAST(ss.blocks                                                                                      AS BIGINT) AS blocks,
    CAST(COALESCE(                        ws.takeon_won,                              ss.dribbles)       AS BIGINT) AS successful_dribbles,
    CAST(ws.takeon_att                                                                                  AS BIGINT) AS dribbles_attempted,
    CAST(ws.dribbles                                                                                    AS BIGINT) AS dribbles_completed_ws,  -- WS-specific SPADL "take_on" count (semantically different from takeon_won)
    CAST(COALESCE(ws.pass_total,                                                      ss.total_passes)   AS BIGINT) AS pass_total,
    CAST(COALESCE(                        ws.pass_ok,                                 ss.accurate_passes) AS BIGINT) AS accurate_passes,
    CAST(ss.accurate_long_balls                                                                         AS BIGINT) AS accurate_long_balls,
    CAST(ss.total_long_balls                                                                            AS BIGINT) AS total_long_balls,
    CAST(COALESCE(fb.crosses,                                                         ss.total_crosses)  AS BIGINT) AS crosses,
    CAST(ss.accurate_crosses                                                                            AS BIGINT) AS accurate_crosses,
    CAST(COALESCE(us.key_passes,                                                      ss.key_passes)     AS BIGINT) AS key_passes,
    CAST(fb.own_goals                                                                                   AS BIGINT) AS own_goals,
    CAST(fb.second_yellow                                                                               AS BIGINT) AS second_yellow,
    -- #437: derive from the SAME merged goals/penalty_goals expressions as the
    -- columns above so `goals - penalty_goals == non_penalty_goals` holds within
    -- every row. Previously us.non_penalty_goals was Understat-first while
    -- goals/penalty_goals were FBref-first → cross-source arithmetic mismatch.
    -- Trino can't reference a SELECT alias at the same level, so repeat verbatim.
    CAST(COALESCE(fb.goals,         fm.goals,          us.goals)         AS BIGINT)
      - CAST(COALESCE(fb.penalty_goals,                ss.penalty_goals) AS BIGINT)                      AS non_penalty_goals,

    -- ========= Percentages (single column, COALESCE) =========
    -- Платформы вычисляют по-разному, но разница ≤2% — приемлемо для single
    -- column. FotMob → WhoScored → SofaScore приоритет (sample-size order).
    ROUND(COALESCE(CAST(ws.pass_pct    AS DOUBLE), ss.accurate_passes_pct), 2)        AS pass_pct,
    ROUND(COALESCE(CAST(ws.takeon_pct  AS DOUBLE), ss.dribbles_pct), 2)               AS take_on_pct,
    ROUND(COALESCE(CAST(ws.tackle_pct  AS DOUBLE), ss.tackles_won_pct), 2)            AS tackle_pct,
    ROUND(ss.accurate_crosses_pct, 2)                                                  AS accurate_crosses_pct,
    ROUND(ss.accurate_long_balls_pct, 2)                                               AS accurate_long_balls_pct,

    -- ========= MODELED — после RX2: xG/xA single column =========
    -- Understat выбран как primary источник (coverage 99% vs 82-85%; r≥0.989
    -- между источниками — choice almost lossless). COALESCE fallback
    -- us → fm → ss catches the ~1% Understat-gap (U21 / backup minutes).
    -- Cross-source diff'ы (FBref - <source> aren't applicable here; the
    -- relevant diff is us vs fm vs ss) хранятся в fct_player_season_stats_audit.
    -- См. docs/research/RX2_xg_source_selection.md.
    ROUND(COALESCE(us.expected_goals, fm.expected_goals, ss.expected_goals), 2)   AS expected_goals,
    ROUND(COALESCE(us.expected_assists, fm.expected_assists), 2)                  AS expected_assists,
    -- expected_goals_on_target / xG-Chain / xG-Buildup — source-unique
    -- метрики (нет аналогов у других провайдеров), single column как UNIQUE_*.
    ROUND(fm.expected_goals_on_target, 2)                AS expected_goals_on_target,
    ROUND(us.non_penalty_xg, 2)                          AS non_penalty_xg_understat,
    ROUND(us.xg_chain, 2)                                AS xg_chain_understat,
    ROUND(us.xg_buildup, 2)                              AS xg_buildup_understat,
    -- Rating: SofaScore (Opta) выбран как единственный источник для
    -- business-витрины. Cross-source diff с FotMob по-прежнему доступен
    -- в gold.fct_player_season_stats_audit (rating_diff_fotmob_sofascore).
    ROUND(ss.rating, 2)                                  AS rating_sofascore,

    -- ========= UNIQUE_FBREF =========
    -- complete_matches/starts/subs/unused_sub — playing-time breakdown (FBref-only)
    fb.complete_matches,
    fb.starts,
    fb.subs,
    fb.unused_sub,
    fb.plus_minus,
    ROUND(fb.points_per_match, 2)                        AS points_per_match,
    ROUND(fb.on_off_impact, 2)                           AS on_off_impact,
    ROUND(fb.goals_per_shot, 2)                          AS goals_per_shot,

    -- ========= UNIQUE_FOTMOB =========
    -- defensive_actions — FotMob composite (нет в других). big_chances_*/
    -- chances_created — FotMob proprietary. poss_won_final_third — FotMob
    -- pressing-метрика (SS даёт att_third, но определения отличаются).
    -- defensive_actions / poss_won_final_third: FotMob silver хранит только
    -- per-90 форму (issue #154) → выносим как `*_per_90` (count-формы нет).
    fm.defensive_actions_per_90,
    ROUND(fm.big_chances_created, 2)                     AS big_chances_created,
    ROUND(fm.big_chances_missed, 2)                      AS big_chances_missed,
    ROUND(fm.chances_created, 2)                         AS chances_created,
    fm.poss_won_final_third_per_90,

    -- ========= UNIQUE_WHOSCORED =========
    -- bad_touches/touches_in_box/avg_x/avg_y — WS-specific event-aggregates,
    -- нет аналогов у других источников.
    ws.bad_touches,
    ws.touches_in_box,
    ROUND(ws.avg_x, 2)                                   AS avg_x,
    ROUND(ws.avg_y, 2)                                   AS avg_y,

    -- ========= UNIQUE_SOFASCORE =========
    -- Aerial/ground/total duels — нет ни у одного другого источника.
    -- Errors lead to goal/shot — критическая дефенсивная метрика SofaScore.
    -- Touches/dispossessed/possession_lost — SofaScore-specific event-counts.
    -- Structure of goals (inside/outside box, headed/L/R-foot) — SofaScore-only.
    ss.ground_duels_won,
    ROUND(ss.ground_duels_won_pct, 2)                    AS ground_duels_won_pct,
    ss.aerial_duels_won,
    ROUND(ss.aerial_duels_won_pct, 2)                    AS aerial_duels_won_pct,
    ss.total_duels_won,
    ROUND(ss.total_duels_won_pct, 2)                     AS total_duels_won_pct,
    ss.errors_lead_to_goal,
    ss.errors_lead_to_shot,
    ss.touches,
    ss.dispossessed,
    ss.possession_lost,
    ss.poss_won_att_third                                AS poss_won_att_third_sofascore,
    ss.totw_appearances,
    ss.matches_started,
    ss.appearances,
    ss.dribbled_past,
    ss.secondary_assists,
    ss.final_third_passes,
    ss.shots_off_target,
    ss.shots_inside_box,
    ss.shots_outside_box,
    ss.blocked_shots,
    ss.hit_woodwork,
    ROUND(ss.goal_conversion_pct, 2)                     AS goal_conversion_pct,
    ss.goals_inside_box,
    ss.goals_outside_box,
    ss.headed_goals,
    ss.left_foot_goals,
    ss.right_foot_goals,
    ss.set_piece_shots,
    ss.free_kick_goals,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                    AS _gold_created_at

FROM xref_fbref xf
-- #463: fb_dedup (max-minutes club) вместо raw silver — профиль теперь
-- per-(player, squad); без дедупа PK развалится на мульти-squad игроках.
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
LEFT JOIN fotmob_counts fm
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
-- Outfield-only: exclude вратарей (они в fct_keeper_season_stats).
WHERE fb.pos IS NULL OR fb.pos NOT LIKE '%GK%'
