-- =============================================================================
-- Gold: mart_scouting_radar (E7 BI mart)
-- =============================================================================
-- Per-(player, match) scouting view. Denormalized (carries player_name,
-- position, team_id) so Superset radar/scatter charts can render without
-- additional dim JOINs. Includes L5 rolling per player on (xg, assists,
-- key_passes proxy, defensive_actions) for trend charts.
--
-- Sources:
--   iceberg.gold.fct_player_match  -- per-match minutes/goals/shots/tackles
--   iceberg.gold.fct_shot          -- shot-grained xG (Understat) — aggregated
--                                     to (player, match) before JOIN to
--                                     avoid fan-out
--   iceberg.gold.dim_player        -- name, position, current team
--
-- PK: (player_id, season, league, match_id) — UNIQUE: one row per
-- (player_id, match_id), season+league are passthrough partition keys.
-- xg_l5 / xa_l5 / key_passes_l5 / defensive_l5 NULL until 5 prior appearances
-- per (player, season) accumulated (point-in-time strict).
--
-- Filter: minutes >= 10 (drops dressing-room subs / unused substitutes).
-- =============================================================================

WITH shot_per_player_match AS (
    SELECT
        player_id_canonical AS player_id,
        match_id_canonical  AS match_id,
        SUM(xg)                                       AS xg_match
    FROM iceberg.gold.fct_shot
    WHERE player_id_canonical IS NOT NULL
      AND match_id_canonical  IS NOT NULL
    GROUP BY player_id_canonical, match_id_canonical
),

assist_per_player_match AS (
    -- xA proxy: count shots where this player is the assist-giver, weighted
    -- by xG of the resulting shot. Industry standard (Understat xA).
    SELECT
        assist_player_id_canonical AS player_id,
        match_id_canonical         AS match_id,
        SUM(xg)                                       AS xa_match
    FROM iceberg.gold.fct_shot
    WHERE assist_player_id_canonical IS NOT NULL
      AND match_id_canonical         IS NOT NULL
    GROUP BY assist_player_id_canonical, match_id_canonical
),

base AS (
    SELECT
        pm.match_id,
        pm.player_id,
        pm.team_id,
        pm.team_name,
        pm.position,
        pm.minutes,
        pm.goals,
        pm.assists,
        pm.shots,
        pm.shots_on_target,
        pm.tackles_won,
        pm.interceptions,
        (pm.tackles_won + pm.interceptions)           AS defensive_actions,
        COALESCE(s.xg_match, 0.0)                     AS xg,
        COALESCE(a.xa_match, 0.0)                     AS xa,
        pm.league,
        pm.season,
        dm.date                                       AS match_date,
        ROW_NUMBER() OVER (
            PARTITION BY pm.player_id, pm.season
            ORDER BY dm.date, pm.match_id
        )                                             AS appearance_rn
    FROM iceberg.gold.fct_player_match pm
    LEFT JOIN iceberg.gold.dim_match dm
           ON dm.match_id = pm.match_id
    LEFT JOIN shot_per_player_match s
           ON s.player_id = pm.player_id
          AND s.match_id  = pm.match_id
    LEFT JOIN assist_per_player_match a
           ON a.player_id = pm.player_id
          AND a.match_id  = pm.match_id
    WHERE pm.minutes >= 10
),

rolled AS (
    SELECT
        *,
        AVG(CAST(xg                AS double)) OVER w AS xg_l5_raw,
        AVG(CAST(xa                AS double)) OVER w AS xa_l5_raw,
        AVG(CAST(shots             AS double)) OVER w AS shots_l5_raw,
        AVG(CAST(defensive_actions AS double)) OVER w AS defensive_l5_raw
    FROM base
    WINDOW w AS (
        PARTITION BY player_id, season
        ORDER BY match_date, match_id
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    )
)

SELECT
    r.player_id,
    dp.player_name,
    r.position,
    r.team_id,
    r.team_name,
    r.match_id,
    r.match_date,
    r.minutes,
    r.goals,
    r.assists,
    r.shots,
    r.shots_on_target,
    r.tackles_won,
    r.interceptions,
    r.defensive_actions,
    r.xg,
    r.xa,
    CASE WHEN appearance_rn > 5 THEN xg_l5_raw        END AS xg_l5,
    CASE WHEN appearance_rn > 5 THEN xa_l5_raw        END AS xa_l5,
    CASE WHEN appearance_rn > 5 THEN shots_l5_raw     END AS shots_l5,
    CASE WHEN appearance_rn > 5 THEN defensive_l5_raw END AS defensive_l5,
    r.league,
    r.season
FROM rolled r
LEFT JOIN iceberg.gold.dim_player dp
       ON dp.player_id = r.player_id
      AND dp.season    = r.season
