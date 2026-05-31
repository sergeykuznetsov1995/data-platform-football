-- =============================================================================
-- Gold: fct_team_season_stats
-- =============================================================================
--
-- Per-season cross-source team stats: FBref + Understat + WhoScored + SofaScore
-- + FotMob объединены через silver.xref_team. Mirror of `fct_player_season_stats`
-- на team-уровне. Design contract: docs/decisions/T6_team_facts_schema.md §5.
--
--   * HARD_FACT counters (single column через COALESCE fb→us→ws→ss). FBref —
--     primary spine. Cross-source diff'ы → `fct_team_season_stats_audit`.
--   * MODELED xG / NPxG — Understat primary (RX2: coverage 99.2% vs SS 84.6%;
--     r ≥ 0.989). COALESCE fallback us → fm → ss.
--   * MODELED xA (expected_assists) — FotMob only (#97); team-grain xA не отдают
--     ни Understat, ни SofaScore на season-grain.
--   * UNIQUE_<source> — single column без суффикса (метрика отсутствует у
--     остальных источников). FotMob (#97): xgot / big_chances / shots_*_box.
--
-- Зерно: (team_id_canonical, league, season). Spine — FBref subset из
-- xref_team. PK = natural composite (оба компонента NOT NULL по INNER FBref
-- spine; xxhash64 + ROW_NUMBER tiebreaker не нужен — design doc §7).
--
-- Cross-source season type (verified vs production data 2026-05-28):
--   * silver.xref_team.season для source IN (fbref, fotmob) = varchar year-start '2025'
--   * silver.xref_team.season для source IN (understat,whoscored,sofascore)
--                                                       = varchar slug '2526'
--   * silver.fbref_team_season_profile.season         = bigint 2025
--   * silver.understat_team_season.season             = varchar slug '2526'
--   * silver.whoscored_team_season.season             = varchar slug '2526'
--   * silver.sofascore_team_season.season             = varchar slug '2526'
--   * silver.fotmob_team_season.season                = varchar slug '2526'
--
--   FBref xref season — CAST varchar→bigint (2025 = 2025).
--   Cross-source slug '2526' получаем из year-start через
--     LPAD(year%100,2,'0') || LPAD((year+1)%100,2,'0').
--   Note: design doc T6_team_facts_schema.md §4 ошибочно описал xref FBref
--   season как slug — в реальности там year-start (как в bronze.fbref_schedule
--   после CAST AS varchar). Followup для дока — отдельный PR.
--
-- xref source_id matching (verified vs xref_team.sql.j2):
--   * fbref source_id     = bronze.fbref_schedule.home/away (squad NAME).
--     fbref_team_season_profile хранит и `team` (squad name) и `team_id`
--     (lookup ID) — JOIN через `fb.team` для совпадения с xref source_id.
--   * understat            = team_id_canonical уже resolve'нут в Silver
--     (`silver.understat_team_season` пришёл из team_match через canonical),
--     LEFT JOIN напрямую по canonical_id (без xref CTE).
--   * whoscored source_id  = bronze.whoscored_schedule.home/away_team (NAME).
--     silver.whoscored_team_season.team_id наследуется от team_id_raw из
--     events_spadl (NUMERIC после CAST(CAST(... AS BIGINT) AS varchar) — см.
--     feedback_bronze_double_id_cast.md).
--     KNOWN GAP: bronze.whoscored_schedule содержит и team_id (numeric) и
--     team (name), но для APL заполнен только сезон '2021'. Для текущих
--     сезонов (2425/2526) mapping numeric↔name отсутствует → WhoScored-блок
--     будет NULL для большинства rows. Followup: либо добавить WS schedule
--     backfill (preferred), либо resolve team_id_canonical в Silver-stage
--     (T6.3 extension), либо matched-name probe through events. См. issue
--     создаваемый в этом PR как followup.
--   * sofascore source_id  = bronze.sofascore_schedule.home/away_team.
--     silver.sofascore_team_season.team_id = CAST(schedule.home_team AS varchar).
--
-- xref JOIN MUST include (league, season) predicate (feedback_xref_join_season_predicate.md):
--   silver.xref_team имеет per-(source, source_id, season) rows;
--   без season-condition будет fan-out 1.5-4×.
-- =============================================================================

WITH
xref_fbref AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fbref_team_name,
        league,
        CAST(season AS BIGINT)                            AS season_year,
        -- Build slug '2526' from year-start '2025' for cross-source JOIN
        LPAD(CAST(MOD(CAST(season AS BIGINT),     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(CAST(season AS BIGINT)+1, 100) AS varchar), 2, '0')
                                                          AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'fbref'
      AND confidence <> 'orphan'
),

xref_ws AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS ws_team_name,
        league,
        season                                            AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'whoscored'
      AND confidence <> 'orphan'
),

xref_ss AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS ss_team_name,
        league,
        season                                            AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'sofascore'
      AND confidence <> 'orphan'
),

-- FotMob xref (#97). season is YEAR-START '2025' (bronze.fotmob_schedule is bigint),
-- like FBref — JOIN on CAST(season_year AS varchar). silver.fotmob_team_season.season
-- is slug '2526' → that fact JOINs on season_slug. team_id = team NAME (== source_id).
xref_fm AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS fm_team_name,
        league,
        season                                            AS season_year_str
    FROM iceberg.silver.xref_team
    WHERE source = 'fotmob'
      AND confidence <> 'orphan'
),

-- WhoScored numeric team_id ↔ team name mapping derived from bronze schedule.
-- schedule забэкфилен на все сезоны (incl. 2526=380, #128) → JOIN даёт match и
-- WS-колонки заполнены для текущего сезона (обоснование #161 penalties fallback).
ws_name_to_id AS (
    SELECT DISTINCT
        CAST(home_team_id AS varchar) AS ws_team_id,
        home_team                     AS ws_team_name,
        league,
        season
    FROM iceberg.bronze.whoscored_schedule
    WHERE home_team_id IS NOT NULL
    UNION
    SELECT DISTINCT
        CAST(away_team_id AS varchar) AS ws_team_id,
        away_team                     AS ws_team_name,
        league,
        season
    FROM iceberg.bronze.whoscored_schedule
    WHERE away_team_id IS NOT NULL
),

-- ===== Team-finance sources (issue #192) =====
-- TM/Capology team universe не имеет schedule → bridge через xref_team
-- (source='transfermarkt'/'capology', добавлены в xref_team.sql.j2). Покрытие —
-- только APL season slug '2526'; для прочих сезонов колонки останутся NULL.
xref_tm AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS tm_club_name,
        league,
        season                                            AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'transfermarkt'
      AND confidence <> 'orphan'
),

xref_cap AS (
    SELECT DISTINCT
        canonical_id,
        source_id                                         AS cap_club_name,
        league,
        season                                            AS season_slug
    FROM iceberg.silver.xref_team
    WHERE source = 'capology'
      AND confidence <> 'orphan'
),

-- Squad market value: SUM рыночной стоимости состава (EUR, снимок «сейчас»).
tm_finance AS (
    SELECT
        current_club_name                                 AS tm_club_name,
        league,
        season                                            AS season_slug,
        SUM(current_market_value_eur)                     AS squad_market_value_eur
    FROM iceberg.silver.transfermarkt_players
    WHERE current_market_value_eur IS NOT NULL
    GROUP BY current_club_name, league, season
),

-- Wage bill: SUM годового gross-фонда (GBP). Silver уже отфильтрован
-- (active OR loan) + currency='GBP'; annual_gross_gbp = weekly_gross_gbp * 52.
cap_finance AS (
    SELECT
        club_name                                         AS cap_club_name,
        league,
        season                                            AS season_slug,
        CAST(SUM(annual_gross_gbp) AS BIGINT)             AS total_wage_bill_gbp
    FROM iceberg.silver.capology_player_salaries
    WHERE annual_gross_gbp IS NOT NULL
    GROUP BY club_name, league, season
)

SELECT
    -- ========= Identity =========
    xf.canonical_id                                      AS team_id_canonical,
    xf.league                                            AS league,
    xf.season_year                                       AS season,
    COALESCE(fb.team, us.primary_team_name)              AS primary_team_name,

    -- ========= HARD_FACT — counters (COALESCE fb → us → ws → ss) =========
    CAST(COALESCE(fb.mp,             us.games_played, ws.matches_seen,  ss.appearances)    AS BIGINT) AS matches,
    CAST(COALESCE(fb.minutes,                                            ss.minutes_played) AS BIGINT) AS minutes,
    CAST(COALESCE(fb.goals,          us.goals,                          ss.goals)          AS BIGINT) AS goals,
    CAST(COALESCE(fb.gk_goals_against, us.goals_against,                ss.goals_conceded) AS BIGINT) AS goals_against,
    CAST(COALESCE(fb.assists,                                            ss.assists)        AS BIGINT) AS assists,
    CAST(COALESCE(fb.yellow_cards,                                       ss.yellow_cards)   AS BIGINT) AS yellow_cards,
    CAST(COALESCE(fb.red_cards,                                          ss.red_cards)      AS BIGINT) AS red_cards,
    CAST(fb.second_yellow_cards                                                              AS BIGINT) AS second_yellow_cards,
    -- Understat не агрегирует shots count в season-rollup (только goals/xG).
    CAST(COALESCE(fb.total_shots,                     ws.shots_total,    ss.total_shots)   AS BIGINT) AS total_shots,
    CAST(COALESCE(fb.shots_on_target,                 ws.shots_on_target_proxy, ss.shots_on_target) AS BIGINT) AS shots_on_target,
    CAST(COALESCE(fb.fouls_committed,                 ws.fouls_committed, ss.fouls_committed) AS BIGINT) AS fouls_committed,
    CAST(fb.fouls_drawn                                                                       AS BIGINT) AS fouls_drawn,
    CAST(COALESCE(fb.offsides,                                            ss.offsides)       AS BIGINT) AS offsides,
    CAST(COALESCE(fb.crosses,                                             ss.total_crosses)  AS BIGINT) AS crosses,
    CAST(COALESCE(fb.interceptions,                   ws.interceptions,   ss.interceptions)  AS BIGINT) AS interceptions,
    CAST(COALESCE(fb.tackles_won,                     ws.tackle_won,      ss.tackles_won)    AS BIGINT) AS tackles_won,
    -- #161: FBref убрал PKwon/PKcon с сезона 2025/26 → WhoScored fallback.
    CAST(COALESCE(fb.penalties_won,                   ws.penalties_won)      AS BIGINT) AS penalties_won,
    CAST(COALESCE(fb.penalties_conceded,              ws.penalties_conceded) AS BIGINT) AS penalties_conceded,
    CAST(fb.own_goals                                                                         AS BIGINT) AS own_goals,

    -- ========= MODELED — xG/xA (Understat primary per RX2; FotMob then SS fallback) =========
    ROUND(COALESCE(us.xg,         fm.expected_goals, ss.expected_goals),  2) AS expected_goals,
    -- FotMob has no team-grain xGA → COALESCE stays us → ss.
    ROUND(COALESCE(us.xg_against, ss.expected_goals_against), 2)         AS expected_goals_against,
    -- expected_assists: FotMob is the ONLY source with team-grain xA (#97).
    ROUND(fm.expected_assists, 2)                                        AS expected_assists,
    ROUND(us.xpts, 2)                                                     AS xpts,
    ROUND(COALESCE(us.npxg, fm.npxg), 2)                                  AS npxg,
    ROUND(us.npxg_against, 2)                                             AS npxg_against,

    -- ========= UNIQUE_FBREF — outfield team stats =========
    fb.players_used,
    ROUND(fb.avg_age, 1)                                  AS avg_age,
    ROUND(fb.possession, 2)                               AS possession_pct,
    ROUND(fb.goals_per_90, 2)                             AS goals_per_90,
    ROUND(fb.goals_assists_per_90, 2)                     AS goals_assists_per_90,
    ROUND(fb.non_penalty_goals_per_90, 2)                 AS non_penalty_goals_per_90,
    ROUND(fb.shots_per_90, 2)                             AS shots_per_90,
    ROUND(fb.shot_on_target_pct, 2)                       AS shot_on_target_pct,
    ROUND(fb.goals_per_shot, 3)                           AS goals_per_shot,
    ROUND(fb.goals_per_shot_on_target, 3)                 AS goals_per_shot_on_target,
    fb.complete_matches,
    fb.substitutions,
    fb.unused_subs,
    ROUND(fb.points_per_match, 2)                         AS points_per_match,
    fb.on_field_goals,
    fb.on_field_goals_against,
    fb.plus_minus,
    ROUND(fb.plus_minus_per_90, 2)                        AS plus_minus_per_90,

    -- ========= UNIQUE_FBREF — goalkeeping aggregates =========
    fb.gk_goals_against,
    fb.gk_saves,
    fb.gk_shots_on_target_against,
    fb.clean_sheets,
    fb.gk_minutes,
    ROUND(fb.save_pct, 2)                                 AS save_pct,
    fb.gk_pk_attempts_faced,
    fb.gk_pk_allowed,
    fb.gk_pk_saved,
    ROUND(fb.goals_against_per_90, 2)                     AS goals_against_per_90,

    -- ========= UNIQUE_UNDERSTAT — pressing / depth =========
    ROUND(us.ppda, 2)                                     AS ppda,
    ROUND(us.oppda, 2)                                    AS oppda,
    us.deep_completions,
    us.deep_completions_allowed,
    us.wins,
    us.draws,
    us.losses,
    us.points,

    -- ========= UNIQUE_WHOSCORED — event-style aggregates =========
    ws.pass_total,
    ws.pass_ok,
    ROUND(ws.pass_pct, 2)                                 AS pass_pct,
    ws.key_passes_ws,
    ws.takeon_att,
    ws.takeon_won,
    ROUND(ws.takeon_pct, 2)                               AS takeon_pct,
    ws.clearances,
    ws.ball_recoveries,
    -- touches_in_box: WS is NULL for current seasons (#120) → fall back to FotMob.
    COALESCE(ws.touches_in_box, fm.touches_in_box)        AS touches_in_box,
    ws.defensive_actions_third,
    ROUND(ws.set_piece_share_pct, 2)                      AS set_piece_share_pct,

    -- ========= UNIQUE_SOFASCORE — duels & breakdowns =========
    ss.total_passes,
    ss.accurate_passes,
    ROUND(ss.accurate_passes_pct, 2)                      AS accurate_passes_pct,
    ROUND(ss.possession_pct_avg, 2)                       AS possession_pct_avg,
    ss.corner_kicks,
    ss.ground_duels_won,
    ss.ground_duels_total,
    ROUND(ss.ground_duels_won_pct, 2)                     AS ground_duels_won_pct,
    ss.aerial_duels_won,
    ss.aerial_duels_total,
    ROUND(ss.aerial_duels_won_pct, 2)                     AS aerial_duels_won_pct,
    ROUND(ss.total_duels_won_pct, 2)                      AS total_duels_won_pct,
    ss.accurate_long_balls,
    ss.total_long_balls,
    ROUND(ss.accurate_long_balls_pct, 2)                  AS accurate_long_balls_pct,
    ss.accurate_crosses,
    ss.total_crosses,

    -- ========= UNIQUE_FOTMOB — metrics no other source provides (#97) =========
    ROUND(fm.xgot, 4)                                     AS xgot,
    fm.big_chances,
    fm.big_chances_missed,
    fm.shots_inside_box,
    fm.shots_outside_box,

    -- ========= TEAM FINANCE (issue #192) — APL 2025/26 only, else NULL =========
    tmf.squad_market_value_eur,
    capf.total_wage_bill_gbp,

    -- ========= Lineage =========
    CURRENT_TIMESTAMP                                     AS _gold_created_at

FROM xref_fbref xf
INNER JOIN iceberg.silver.fbref_team_season_profile fb
    ON  fb.team    = xf.fbref_team_name
    AND fb.league  = xf.league
    AND fb.season  = xf.season_year
LEFT JOIN iceberg.silver.understat_team_season us
    ON  us.team_id_canonical = xf.canonical_id
    AND us.league            = xf.league
    AND us.season            = xf.season_slug
-- WhoScored: xref source_id = team NAME, Silver team_id = NUMERIC; bridge
-- через bronze.whoscored_schedule (mapping name→numeric).
LEFT JOIN xref_ws xw
    ON  xw.canonical_id = xf.canonical_id
    AND xw.league       = xf.league
    AND xw.season_slug  = xf.season_slug
LEFT JOIN ws_name_to_id wn
    ON  wn.ws_team_name = xw.ws_team_name
    AND wn.league       = xw.league
    AND wn.season       = xw.season_slug
LEFT JOIN iceberg.silver.whoscored_team_season ws
    ON  ws.team_id = wn.ws_team_id
    AND ws.league  = wn.league
    AND ws.season  = wn.season
LEFT JOIN xref_ss xs
    ON  xs.canonical_id = xf.canonical_id
    AND xs.league       = xf.league
    AND xs.season_slug  = xf.season_slug
LEFT JOIN iceberg.silver.sofascore_team_season ss
    ON  ss.team_id = xs.ss_team_name
    AND ss.league  = xs.league
    AND ss.season  = xs.season_slug
-- FotMob (#97): xref season year-start; silver.fotmob_team_season season slug.
LEFT JOIN xref_fm xfm
    ON  xfm.canonical_id   = xf.canonical_id
    AND xfm.league         = xf.league
    AND xfm.season_year_str = CAST(xf.season_year AS varchar)
LEFT JOIN iceberg.silver.fotmob_team_season fm
    ON  fm.team_id = xfm.fm_team_name
    AND fm.league  = xf.league
    AND fm.season  = xf.season_slug
-- Transfermarkt squad value (#192): bridge canonical→club name via xref_tm,
-- aggregate via tm_finance. season_slug ('2526') matches xf.season_slug.
LEFT JOIN xref_tm xtm
    ON  xtm.canonical_id = xf.canonical_id
    AND xtm.league       = xf.league
    AND xtm.season_slug  = xf.season_slug
LEFT JOIN tm_finance tmf
    ON  tmf.tm_club_name = xtm.tm_club_name
    AND tmf.league       = xtm.league
    AND tmf.season_slug  = xtm.season_slug
-- Capology wage bill (#192): same bridge pattern via xref_cap + cap_finance.
LEFT JOIN xref_cap xcap
    ON  xcap.canonical_id = xf.canonical_id
    AND xcap.league       = xf.league
    AND xcap.season_slug  = xf.season_slug
LEFT JOIN cap_finance capf
    ON  capf.cap_club_name = xcap.cap_club_name
    AND capf.league        = xcap.league
    AND capf.season_slug   = xcap.season_slug
