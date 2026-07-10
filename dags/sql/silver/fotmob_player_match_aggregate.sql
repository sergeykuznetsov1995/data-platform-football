-- =============================================================================
-- Silver: fotmob_player_match_aggregate
-- =============================================================================
--
-- One row per (match_id, player_id, league, season) — per-match player stats
-- parsed from FotMob `bronze.fotmob_match_details.player_stats_json`. Column
-- shape is a full parity mirror of `sofascore_player_match_aggregate` so the
-- 5-source Gold `fct_player_match` COALESCE сравнивает одинаково-именованные
-- колонки. Колонки, которых FotMob НЕ отдаёт на match-grain → CAST(NULL AS ...).
--
-- Source:
--   bronze.fotmob_match_details.player_stats_json — JSON object keyed by
--   player_id; each value = { "teamId": <int>, "stats": [ { "stats": {
--     "<Display name>": { "stat": { "value": <num>, "total": <num?>,
--     "type": "integer|double|fractionWithPercentage|..." } }, ... } }, ... ] }.
--   `stats` is an ARRAY of stat groups (Top stats / Attack / Defense / ...);
--   we flatten ALL groups, so a stat is found regardless of its group index.
--
-- Value extraction (probed live 2026-06-20, 360 matches, 62 distinct keys):
--   * `$.stat.value` is ALWAYS a clean number (numerator for fraction stats),
--     never a "N (M%)" string like team-grain stats_json — no regexp needed.
--   * fractionWithPercentage stats carry `$.stat.total` = attempts:
--       Accurate passes      → value=accurate, total=attempted  → passes_completed / passes
--       Successful dribbles  → value=won,      total=attempted  → dribbles_won / dribbles_attempted
--       Accurate long balls  → value=accurate, total=attempted  → accurate_long_balls / total_long_balls
--       Aerial duels won     → value=won,      total=total       → aerial_duels_won / (total-won)=aerial_duels_lost
--
-- Footguns:
--   * teamId is integer in player_stats_json; home_team_id/away_team_id are
--     varchar in bronze → compare as varchar.
--   * MAX(IF()) pivot collapses the same key appearing in multiple groups
--     (identical value) — same idiom as fotmob_team_match.sql.
--   * Season is bigint year-start (2025) at bronze level; we emit a slug
--     ('2526') to match xref_player / other Silver player-match tables.
--   * Cards (yellow/red), total crosses, possession_lost, tackles_won — FotMob
--     player_stats_json has no key for these → NULL (mirrors SofaScore, which
--     also NULLs yellow/red on this grain).
-- =============================================================================

WITH match_details_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fotmob_match_details
    WHERE player_stats_json IS NOT NULL
      AND player_stats_json <> 'null'
      AND player_stats_json <> '{}'
),

-- ===== Explode players × stat groups × stats into long form =====
stat_flat AS (
    SELECT
        md.match_id,
        md.league,
        md.season,
        md._ingested_at,
        md.home_team_id,
        md.away_team_id,
        md.home_team,
        md.away_team,
        pe.player_id,
        CAST(json_extract_scalar(pe.pdata, '$.teamId') AS varchar)        AS team_id_numeric,
        s.stat_name,
        TRY_CAST(json_extract_scalar(s.stat_obj, '$.stat.value') AS DOUBLE) AS val,
        TRY_CAST(json_extract_scalar(s.stat_obj, '$.stat.total') AS DOUBLE) AS tot
    FROM match_details_dedup md
    CROSS JOIN UNNEST(
        map_entries(CAST(json_parse(md.player_stats_json) AS map<varchar, json>))
    ) AS pe(player_id, pdata)
    CROSS JOIN UNNEST(
        CAST(json_extract(pe.pdata, '$.stats') AS array<json>)
    ) AS g(grp)
    CROSS JOIN UNNEST(
        map_entries(CAST(json_extract(grp, '$.stats') AS map<varchar, json>))
    ) AS s(stat_name, stat_obj)
    WHERE md.rn = 1
),

-- ===== Pivot to wide (one row per player × match) =====
-- FotMob omits a stat key when its value is 0 (sparse), so a MEASURED stat that
-- is absent = 0 for that player (e.g. no shots → xG 0). Every row here is a real
-- appearance (Minutes played present on 100% of rows), so COALESCE(...,0) is
-- correct and yields a dense table like SofaScore/Understat siblings. `rating`
-- is NOT 0-filled — absence = genuinely unrated (GK / minimal minutes), not 0.
player_pivot AS (
    SELECT
        match_id,
        player_id,
        team_id_numeric,
        league,
        season,
        MAX(_ingested_at) AS _bronze_ingested_at,
        MAX(home_team_id) AS home_team_id,
        MAX(away_team_id) AS away_team_id,
        MAX(home_team)    AS home_team,
        MAX(away_team)    AS away_team,

        -- ===== HARD_FACT counters (0-filled: absent key = 0) =====
        COALESCE(MAX(IF(stat_name = 'Minutes played',   val)), 0) AS minutes_played,
        COALESCE(MAX(IF(stat_name = 'Goals',             val)), 0) AS goals,
        COALESCE(MAX(IF(stat_name = 'Assists',           val)), 0) AS assists,
        COALESCE(MAX(IF(stat_name = 'Own goal',          val)), 0) AS own_goals,
        COALESCE(MAX(IF(stat_name = 'Total shots',       val)), 0) AS shots,
        COALESCE(MAX(IF(stat_name = 'Shots on target',   val)), 0) AS shots_on_target,
        COALESCE(MAX(IF(stat_name = 'Blocked shots',     val)), 0) AS shots_blocked,
        COALESCE(MAX(IF(stat_name = 'Shots off target',  val)), 0) AS shots_off_target,
        COALESCE(MAX(IF(stat_name = 'Accurate crosses',  val)), 0) AS accurate_crosses,
        COALESCE(MAX(IF(stat_name = 'Fouls committed',   val)), 0) AS fouls_committed,
        COALESCE(MAX(IF(stat_name = 'Was fouled',        val)), 0) AS fouls_drawn,
        COALESCE(MAX(IF(stat_name = 'Offsides',          val)), 0) AS offsides,
        COALESCE(MAX(IF(stat_name = 'Tackles',           val)), 0) AS tackles,
        COALESCE(MAX(IF(stat_name = 'Interceptions',     val)), 0) AS interceptions,
        COALESCE(MAX(IF(stat_name = 'Clearances',        val)), 0) AS clearances,
        COALESCE(MAX(IF(stat_name = 'Blocks',            val)), 0) AS blocks,
        COALESCE(MAX(IF(stat_name = 'Recoveries',        val)), 0) AS ball_recoveries,
        COALESCE(MAX(IF(stat_name = 'Error led to goal', val)), 0) AS errors_lead_to_goal,
        -- Accurate passes → value=accurate (completed), total=attempted
        COALESCE(MAX(IF(stat_name = 'Accurate passes',   tot)), 0) AS passes,
        COALESCE(MAX(IF(stat_name = 'Accurate passes',   val)), 0) AS passes_completed,
        COALESCE(MAX(IF(stat_name = 'Chances created',   val)), 0) AS key_passes,
        -- Accurate long balls → value=accurate, total=attempted
        COALESCE(MAX(IF(stat_name = 'Accurate long balls', val)), 0) AS accurate_long_balls,
        COALESCE(MAX(IF(stat_name = 'Accurate long balls', tot)), 0) AS total_long_balls,
        -- Successful dribbles → value=won, total=attempted
        COALESCE(MAX(IF(stat_name = 'Successful dribbles', tot)), 0) AS dribbles_attempted,
        COALESCE(MAX(IF(stat_name = 'Successful dribbles', val)), 0) AS dribbles_won,
        COALESCE(MAX(IF(stat_name = 'Duels won',         val)), 0) AS total_duels_won,
        COALESCE(MAX(IF(stat_name = 'Duels lost',        val)), 0) AS total_duels_lost,
        -- Aerial duels won → value=won, total=total aerials
        COALESCE(MAX(IF(stat_name = 'Aerial duels won',  val)), 0) AS aerial_duels_won,
        COALESCE(MAX(IF(stat_name = 'Aerial duels won',  tot)), 0) AS aerial_duels_total,
        COALESCE(MAX(IF(stat_name = 'Touches',           val)), 0) AS touches,
        COALESCE(MAX(IF(stat_name = 'Dispossessed',      val)), 0) AS dispossessed,
        COALESCE(MAX(IF(stat_name = 'Penalties won',     val)), 0) AS penalties_won,
        COALESCE(MAX(IF(stat_name = 'Conceded penalty',  val)), 0) AS penalties_conceded,
        COALESCE(MAX(IF(stat_name = 'Missed penalty',    val)), 0) AS penalties_missed,
        COALESCE(MAX(IF(stat_name = 'Saved penalties',   val)), 0) AS penalty_saves,
        COALESCE(MAX(IF(stat_name = 'Ground duels won',  val)), 0) AS ground_duels_won,

        -- ===== MODELED — xG/xA 0-filled (no shots = 0 xG, like Understat) =====
        COALESCE(MAX(IF(stat_name = 'Expected goals (xG)',   val)), 0) AS xg,
        COALESCE(MAX(IF(stat_name = 'Expected assists (xA)', val)), 0) AS xa,
        -- rating: NOT 0-filled — absence = unrated, keep NULL.
        MAX(IF(stat_name = 'FotMob rating',         val)) AS rating
    FROM stat_flat
    GROUP BY match_id, player_id, team_id_numeric, league, season
)

SELECT
    -- ========= Identification =========
    match_id,
    player_id,
    team_id_numeric                          AS team_id,
    CASE WHEN team_id_numeric = home_team_id THEN home_team
         WHEN team_id_numeric = away_team_id THEN away_team END AS team_name,
    (team_id_numeric = home_team_id)         AS is_home,
    -- FotMob player_stats_json has no captain/starter/position block → NULL
    -- (parity with sofascore_player_match_aggregate). #693 added is_starter to
    -- the SofaScore aggregate; real FotMob lineup data lives in a separate
    -- silver.fotmob_lineup table (Phase 2), not this stats aggregate.
    CAST(NULL AS boolean)                    AS is_captain,
    CAST(NULL AS boolean)                    AS is_starter,
    CAST(NULL AS varchar)                    AS position,
    CAST(NULL AS varchar)                    AS position_specific,

    -- ========= HARD_FACT (counters, FBref/SofaScore-aligned names) =========
    CAST(minutes_played AS BIGINT)           AS minutes_played,
    CAST(goals AS BIGINT)                    AS goals,
    CAST(assists AS BIGINT)                  AS assists,
    CAST(own_goals AS BIGINT)                AS own_goals,
    CAST(shots AS BIGINT)                    AS shots,
    CAST(shots_on_target AS BIGINT)          AS shots_on_target,
    CAST(shots_blocked AS BIGINT)            AS shots_blocked,
    CAST(shots_off_target AS BIGINT)         AS shots_off_target,

    -- Cards: FotMob player_stats_json не несёт per-player yellow/red — приходят
    -- через events. NULL (mirrors SofaScore). Gold берёт fb/ws/us.
    CAST(NULL AS DOUBLE)                     AS yellow_cards,
    CAST(NULL AS DOUBLE)                     AS red_cards,

    -- Crosses: FotMob отдаёт только accurate, не total → crosses NULL.
    CAST(NULL AS BIGINT)                     AS crosses,
    CAST(accurate_crosses AS BIGINT)         AS accurate_crosses,

    CAST(fouls_committed AS BIGINT)          AS fouls_committed,
    CAST(fouls_drawn AS BIGINT)              AS fouls_drawn,
    CAST(offsides AS BIGINT)                 AS offsides,

    CAST(tackles AS BIGINT)                  AS tackles,
    -- FotMob "Tackles" не разделяет won/total → tackles_won NULL.
    CAST(NULL AS BIGINT)                     AS tackles_won,
    CAST(interceptions AS BIGINT)            AS interceptions,
    CAST(clearances AS BIGINT)               AS clearances,
    CAST(blocks AS BIGINT)                   AS blocks,
    CAST(ball_recoveries AS BIGINT)          AS ball_recoveries,
    CAST(errors_lead_to_goal AS BIGINT)      AS errors_lead_to_goal,
    -- FotMob отдаёт только error→goal, не error→shot.
    CAST(NULL AS BIGINT)                     AS errors_lead_to_shot,

    CAST(passes AS BIGINT)                   AS passes,
    CAST(passes_completed AS BIGINT)         AS passes_completed,
    CAST(key_passes AS BIGINT)               AS key_passes,
    CAST(accurate_long_balls AS BIGINT)      AS accurate_long_balls,
    CAST(total_long_balls AS BIGINT)         AS total_long_balls,

    CAST(dribbles_attempted AS BIGINT)       AS dribbles_attempted,
    CAST(dribbles_won AS BIGINT)             AS dribbles_won,

    CAST(total_duels_won AS BIGINT)          AS total_duels_won,
    CAST(total_duels_lost AS BIGINT)         AS total_duels_lost,
    CAST(aerial_duels_won AS BIGINT)         AS aerial_duels_won,
    -- aerial_duels_lost = total aerials − won.
    CAST(aerial_duels_total - aerial_duels_won AS BIGINT) AS aerial_duels_lost,
    -- FotMob не отдаёт challenge_lost.
    CAST(NULL AS BIGINT)                     AS challenge_lost,

    CAST(touches AS BIGINT)                  AS touches,
    CAST(dispossessed AS BIGINT)             AS dispossessed,
    -- FotMob нет possession_lost (ctrl) эквивалента на player-grain.
    CAST(NULL AS BIGINT)                     AS possession_lost,

    CAST(penalties_won AS BIGINT)            AS penalties_won,
    CAST(penalties_conceded AS BIGINT)       AS penalties_conceded,
    CAST(penalties_missed AS BIGINT)         AS penalties_missed,
    -- penalty_goals: FotMob не разделяет penalty-голы (mirrors SofaScore NULL).
    CAST(NULL AS DOUBLE)                     AS penalty_goals,
    CAST(penalty_saves AS BIGINT)            AS penalty_saves,

    CAST(ground_duels_won AS BIGINT)         AS ground_duels_won,

    -- ========= MODELED (xG / xA / rating) =========
    ROUND(xg, 4)                             AS xg,
    ROUND(xa, 4)                             AS xa,
    ROUND(rating, 2)                         AS rating,

    -- ========= Lineage =========
    _bronze_ingested_at,

    -- ========= Partition keys (season → slug to match xref / other Silver) =====
    league,
    -- #913 Phase 2
    CASE WHEN league = 'INT-World Cup'
         THEN LPAD(CAST(season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0')
    END AS season

FROM player_pivot
WHERE team_id_numeric IS NOT NULL
