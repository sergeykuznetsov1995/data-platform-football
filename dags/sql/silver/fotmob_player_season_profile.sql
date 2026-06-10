-- =============================================================================
-- Silver: fotmob_player_season_profile
-- =============================================================================
--
-- One wide row per player / league / season.
--
-- Содержит ТОЛЬКО per-season атрибуты (клуб/позиция/номер сезона) и
-- статистику. Time-invariant поля игрока (birth_date, height, foot, country)
-- НЕ хранятся здесь — они уйдут в snapshot-таблицу silver.fotmob_player_profile
-- (T4 в task.md). Pass-through JSON (career_history, trophies, ...) удалены —
-- raw json остаётся в bronze, а структурированные представления при
-- необходимости заслуживают отдельных silver-таблиц.
--
-- Sources (all from iceberg.bronze):
--   fotmob_player_details (d) — карточка игрока (берём только per-season поля)
--   fotmob_player_stats   (s) — сезонные статы в LONG-формате
--                                (zerno: participant_id × stat_name)
--
-- Pipeline:
--   1. details_dedup — ROW_NUMBER dedup на (player_id, league, season).
--   2. stats_pivoted — MAX(CASE WHEN stat_name='X') pivot long -> wide
--                       (~32 stat-колонки, без goalkeeping).
--   3. Final SELECT — LEFT JOIN dedup ← pivot по (player_id, league, season).
--                       JOIN-key cast: CAST(participant_id AS VARCHAR).
--                       Фильтры: NOT is_coach (тренеры из FotMob /api/playerData
--                       приходят в той же таблице с is_coach=true)
--                       AND LOWER(primary_position) != 'keeper'
--                       (вратари в отдельной silver.fotmob_keeper_profile).
--
-- Нормализация position: LOWER(...) — FotMob отдаёт inconsistent case
-- (Keeper vs keeper, и т.п.) и generic fallback'и (midfielder, defender,
-- forward) у части записей. LOWER даёт consistent lowercase. Capitalize
-- делается на BI-слое (Trino INITCAP отсутствует).
-- =============================================================================

WITH details_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fotmob_player_details
),

stats_pivoted AS (
    SELECT
        CAST(participant_id AS VARCHAR) AS player_id,
        league,
        season,
        MAX(_ingested_at) AS _stats_ingested_at,
        MAX(matches_played) AS matches_played,
        -- Top Stat
        MAX(CASE WHEN stat_name = 'mins_played' THEN stat_value END) AS minutes_played,
        MAX(CASE WHEN stat_name = 'goals' THEN stat_value END) AS goals,
        MAX(CASE WHEN stat_name = 'goal_assist' THEN stat_value END) AS assists,
        MAX(CASE WHEN stat_name = '_goals_and_goal_assist' THEN stat_value END) AS goals_assists,
        MAX(CASE WHEN stat_name = 'rating' THEN stat_value END) AS fotmob_rating,
        -- Attacking
        MAX(CASE WHEN stat_name = 'goals_per_90' THEN stat_value END) AS goals_per_90,
        MAX(CASE WHEN stat_name = 'expected_goals' THEN stat_value END) AS expected_goals,
        MAX(CASE WHEN stat_name = 'expected_goals_per_90' THEN stat_value END) AS expected_goals_per_90,
        MAX(CASE WHEN stat_name = 'expected_goalsontarget' THEN stat_value END) AS expected_goals_on_target,
        MAX(CASE WHEN stat_name = 'expected_assists' THEN stat_value END) AS expected_assists,
        MAX(CASE WHEN stat_name = 'expected_assists_per_90' THEN stat_value END) AS expected_assists_per_90,
        MAX(CASE WHEN stat_name = '_expected_goals_and_expected_assists_per_90' THEN stat_value END) AS xg_xa_per_90,
        MAX(CASE WHEN stat_name = 'total_scoring_att' THEN stat_value END) AS shots_per_90,
        MAX(CASE WHEN stat_name = 'ontarget_scoring_att' THEN stat_value END) AS shots_on_target_per_90,
        MAX(CASE WHEN stat_name = 'total_att_assist' THEN stat_value END) AS chances_created,
        MAX(CASE WHEN stat_name = 'big_chance_created' THEN stat_value END) AS big_chances_created,
        MAX(CASE WHEN stat_name = 'big_chance_missed' THEN stat_value END) AS big_chances_missed,
        MAX(CASE WHEN stat_name = 'accurate_pass' THEN stat_value END) AS accurate_passes_per_90,
        MAX(CASE WHEN stat_name = 'accurate_long_balls' THEN stat_value END) AS accurate_long_balls_per_90,
        MAX(CASE WHEN stat_name = 'won_contest' THEN stat_value END) AS successful_dribbles_per_90,
        MAX(CASE WHEN stat_name = 'penalty_won' THEN stat_value END) AS penalties_won,
        -- Defending
        MAX(CASE WHEN stat_name = 'defensive_contributions' THEN stat_value END) AS defensive_actions_per_90,
        MAX(CASE WHEN stat_name = 'total_tackle' THEN stat_value END) AS tackles_per_90,
        MAX(CASE WHEN stat_name = 'interception' THEN stat_value END) AS interceptions_per_90,
        MAX(CASE WHEN stat_name = 'effective_clearance' THEN stat_value END) AS clearances_per_90,
        MAX(CASE WHEN stat_name = 'ball_recovery' THEN stat_value END) AS recoveries_per_90,
        MAX(CASE WHEN stat_name = 'outfielder_block' THEN stat_value END) AS blocks_per_90,
        MAX(CASE WHEN stat_name = 'poss_won_att_3rd' THEN stat_value END) AS poss_won_final_third_per_90,
        MAX(CASE WHEN stat_name = 'penalty_conceded' THEN stat_value END) AS penalties_conceded,
        -- Discipline
        MAX(CASE WHEN stat_name = 'yellow_card' THEN stat_value END) AS yellow_cards,
        MAX(CASE WHEN stat_name = 'red_card' THEN stat_value END) AS red_cards,
        MAX(CASE WHEN stat_name = 'fouls' THEN stat_value END) AS fouls_per_90
    FROM iceberg.bronze.fotmob_player_stats
    GROUP BY participant_id, league, season
)

SELECT
    -- ========= Identity (per-season attributes only) =========
    d.player_id,
    d.name                                                            AS player_name,
    LOWER(d.position_description) AS primary_position,
    d.primary_team_id,
    d.primary_team_name,

    -- ========= Stats (PIVOT long -> wide) =========
    s.matches_played,
    s.minutes_played,
    s.goals,
    s.assists,
    s.goals_assists,
    s.fotmob_rating,
    s.goals_per_90,
    s.expected_goals,
    s.expected_goals_per_90,
    s.expected_goals_on_target,
    s.expected_assists,
    s.expected_assists_per_90,
    s.xg_xa_per_90,
    s.shots_per_90,
    s.shots_on_target_per_90,
    s.chances_created,
    s.big_chances_created,
    s.big_chances_missed,
    s.accurate_passes_per_90,
    s.accurate_long_balls_per_90,
    s.successful_dribbles_per_90,
    s.penalties_won,
    s.defensive_actions_per_90,
    s.tackles_per_90,
    s.interceptions_per_90,
    s.clearances_per_90,
    s.recoveries_per_90,
    s.blocks_per_90,
    s.poss_won_final_third_per_90,
    s.penalties_conceded,
    s.yellow_cards,
    s.red_cards,
    s.fouls_per_90,

    -- ========= Lineage =========
    GREATEST(d._ingested_at, COALESCE(s._stats_ingested_at, d._ingested_at)) AS _bronze_ingested_at,

    -- ========= Partition Keys =========
    -- season → slug ('2425'); FotMob bronze stores year-start bigint (2024).
    d.league,
    LPAD(CAST(MOD(d.season,     100) AS varchar), 2, '0')
        || LPAD(CAST(MOD(d.season + 1, 100) AS varchar), 2, '0') AS season

FROM details_dedup d
LEFT JOIN stats_pivoted s
    ON  d.player_id = s.player_id
    AND d.league    = s.league
    AND d.season    = s.season
WHERE d.rn = 1
  AND NOT d.is_coach
  AND LOWER(d.position_description) <> 'keeper'
