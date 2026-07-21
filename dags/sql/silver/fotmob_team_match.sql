-- =============================================================================
-- Silver: fotmob_team_match
-- =============================================================================
--
-- One row per (match_id, team_id, league, season) — flattened FotMob team-level
-- match statistics. Two rows per match (home + away sides).
--
-- Sources (native FotMob ingest, #930 cutover from legacy bronze.fotmob_match_details):
--   bronze.fotmob_match_payloads_current
--     * top-level: home_team_id / away_team_id (bigint), competition_id (varchar),
--                  source_season_key ('2025/2026' / '2026')
--     * stats_json: nested JSON `Periods.All.stats[group].stats[stat]`. Each leaf
--                   stat = {"key": "...", "stats": [home_value, away_value], ...}
--                   where value is int / numeric string / "N (M%)" fraction string.
--     * player_stats_json: JSON object keyed by player_id; each player carries a
--                   teamId + `stats[0].stats["Expected assists (xA)"].stat.value`
--                   (and many more). We SUM(xA) per team for `expected_assists`
--                   (FotMob does NOT expose team-grain xA in `stats_json`, only
--                   per-player; this is the entire raison d'être of issue #97).
--   bronze.fotmob_matches_current
--     * team NAMES (home_team_name / away_team_name) and home_score / away_score —
--       match_payloads carries neither; JOIN on (match_id, competition_id,
--       source_season_key). Types: payloads competition_id is varchar, matches
--       is bigint → CAST.
--   league_map (inline VALUES)
--     * competition_id → legacy `league` string. INNER JOIN doubles as the
--       14-league scope filter (native ingest covers the full FotMob catalogue;
--       Silver surface must stay the legacy 14 leagues).
--
-- Why pivot via MAX(IF):
--   Same `key` appears in multiple groups (e.g. ShotsOnTarget in "Top stats" AND
--   "Shots" group) with identical values. MAX(IF) collapses duplicates safely.
--
-- Probed keys (2026-05-29, 360 matches): 41 distinct keys. Subset materialised
-- below — HARD_FACT counters + xG variants + a few FotMob-unique metrics
-- (big chances, xGOT). Full key inventory in
-- `docs/fotmob_bronze_dq_audit_2026-05-14.md` and #97 history.
--
-- Footguns:
--   * Output `team_id` = team NAME (matches_current home/away_team_name — same
--     source string `home.name` as legacy), to match silver.xref_team.source_id
--     (source='fotmob') for a direct name-based JOIN in Gold. The numeric
--     team_id is kept internally as `team_id_numeric` ONLY to JOIN team_xa
--     (player_stats_json is keyed by numeric teamId).
--   * `home_team_id` / `away_team_id` are bigint in native; player_stats_json
--     `teamId` is integer → CAST both AS varchar so the team_xa JOIN stays
--     varchar = varchar (as legacy).
--   * `match_id` is bigint in native → CAST AS varchar in the final SELECT
--     (Gold / xref_match join on the legacy string form).
--   * Several stats are strings ("3.22"), not numbers — TRY_CAST(... AS DOUBLE).
--   * Fraction strings "453 (89%)" → regexp_extract for count + pct.
--   * stats_json is NULL for cancelled / non-finished matches — we filter to
--     NOT NULL since silver fotmob_team_match exists for matches WITH a
--     finished stats payload.
--   * Season: year-start = substr(source_season_key, 1, 4) (works for both
--     '2025/2026' and '2025' forms); we emit a slug ('2526') via the unchanged
--     legacy CASE to match other Silver team-match tables and xref JOINs.
--   * No ROW_NUMBER dedup: *_current views are already deduped (manifest gate +
--     natural-key dedup; one row per match via manifest identity).
-- =============================================================================

WITH league_map (competition_id, league) AS (
    VALUES
        {{ fotmob_league_map_values_sql }}
),

match_details AS (
    SELECT
        p.match_id,
        lm.league,
        TRY_CAST(substr(p.source_season_key, 1, 4) AS integer) AS season,
        m.home_team_name AS home_team,
        m.away_team_name AS away_team,
        CAST(p.home_team_id AS varchar) AS home_team_id,
        CAST(p.away_team_id AS varchar) AS away_team_id,
        m.home_score,
        m.away_score,
        p.stats_json,
        p.player_stats_json,
        p._observed_at AS _ingested_at
    FROM iceberg.bronze.fotmob_match_payloads_current p
    JOIN iceberg.bronze.fotmob_matches_current m
        ON  m.match_id = p.match_id
        AND m.competition_id = CAST(p.competition_id AS bigint)
        AND m.source_season_key = p.source_season_key
    JOIN league_map lm
        ON lm.competition_id = m.competition_id
    WHERE p.stats_json IS NOT NULL
      AND p.stats_json <> 'null'
      AND p.stats_json <> '{}'
),

-- ===== Step 1: explode stats_json into one row per (match, group, stat) =====
stats_flat AS (
    SELECT
        md.match_id,
        md.league,
        md.season,
        md._ingested_at,
        md.home_team_id,
        md.away_team_id,
        md.home_team,
        md.away_team,
        md.home_score,
        md.away_score,
        json_extract_scalar(stat, '$.key')      AS stat_key,
        json_extract_scalar(stat, '$.stats[0]') AS home_text,
        json_extract_scalar(stat, '$.stats[1]') AS away_text
    FROM match_details md
    CROSS JOIN UNNEST(
        CAST(json_extract(md.stats_json, '$.Periods.All.stats') AS array<json>)
    ) AS gr(grp)
    CROSS JOIN UNNEST(
        CAST(json_extract(grp, '$.stats') AS array<json>)
    ) AS st(stat)
),

-- ===== Step 2: pivot to wide form (one row per match) =====
stats_pivot AS (
    SELECT
        match_id,
        league,
        season,
        MAX(home_team_id)       AS home_team_id,
        MAX(away_team_id)       AS away_team_id,
        MAX(home_team)          AS home_team_name,
        MAX(away_team)          AS away_team_name,
        MAX(home_score)         AS home_score,
        MAX(away_score)         AS away_score,
        MAX(_ingested_at)       AS _bronze_ingested_at,

        -- ===== Possession =====
        MAX(IF(stat_key = 'BallPossesion', TRY_CAST(home_text AS DOUBLE))) AS home_possession_pct,
        MAX(IF(stat_key = 'BallPossesion', TRY_CAST(away_text AS DOUBLE))) AS away_possession_pct,

        -- ===== Shots =====
        MAX(IF(stat_key = 'total_shots',       TRY_CAST(home_text AS INTEGER))) AS home_total_shots,
        MAX(IF(stat_key = 'total_shots',       TRY_CAST(away_text AS INTEGER))) AS away_total_shots,
        MAX(IF(stat_key = 'ShotsOnTarget',     TRY_CAST(home_text AS INTEGER))) AS home_shots_on_target,
        MAX(IF(stat_key = 'ShotsOnTarget',     TRY_CAST(away_text AS INTEGER))) AS away_shots_on_target,
        MAX(IF(stat_key = 'ShotsOffTarget',    TRY_CAST(home_text AS INTEGER))) AS home_shots_off_target,
        MAX(IF(stat_key = 'ShotsOffTarget',    TRY_CAST(away_text AS INTEGER))) AS away_shots_off_target,
        MAX(IF(stat_key = 'blocked_shots',     TRY_CAST(home_text AS INTEGER))) AS home_blocked_shots,
        MAX(IF(stat_key = 'blocked_shots',     TRY_CAST(away_text AS INTEGER))) AS away_blocked_shots,
        MAX(IF(stat_key = 'shots_inside_box',  TRY_CAST(home_text AS INTEGER))) AS home_shots_inside_box,
        MAX(IF(stat_key = 'shots_inside_box',  TRY_CAST(away_text AS INTEGER))) AS away_shots_inside_box,
        MAX(IF(stat_key = 'shots_outside_box', TRY_CAST(home_text AS INTEGER))) AS home_shots_outside_box,
        MAX(IF(stat_key = 'shots_outside_box', TRY_CAST(away_text AS INTEGER))) AS away_shots_outside_box,

        -- ===== Expected goals (modeled, double strings) =====
        MAX(IF(stat_key = 'expected_goals',             TRY_CAST(home_text AS DOUBLE))) AS home_xg,
        MAX(IF(stat_key = 'expected_goals',             TRY_CAST(away_text AS DOUBLE))) AS away_xg,
        MAX(IF(stat_key = 'expected_goals_non_penalty', TRY_CAST(home_text AS DOUBLE))) AS home_npxg,
        MAX(IF(stat_key = 'expected_goals_non_penalty', TRY_CAST(away_text AS DOUBLE))) AS away_npxg,
        MAX(IF(stat_key = 'expected_goals_on_target',   TRY_CAST(home_text AS DOUBLE))) AS home_xgot,
        MAX(IF(stat_key = 'expected_goals_on_target',   TRY_CAST(away_text AS DOUBLE))) AS away_xgot,

        -- ===== Passing (accurate_passes value = "N (P%)") =====
        MAX(IF(stat_key = 'passes', TRY_CAST(home_text AS INTEGER))) AS home_total_passes,
        MAX(IF(stat_key = 'passes', TRY_CAST(away_text AS INTEGER))) AS away_total_passes,
        MAX(IF(stat_key = 'accurate_passes',
               TRY_CAST(regexp_extract(home_text, '^(\d+)', 1) AS INTEGER))) AS home_accurate_passes,
        MAX(IF(stat_key = 'accurate_passes',
               TRY_CAST(regexp_extract(away_text, '^(\d+)', 1) AS INTEGER))) AS away_accurate_passes,

        -- ===== Discipline & set-pieces =====
        MAX(IF(stat_key = 'yellow_cards', TRY_CAST(home_text AS INTEGER))) AS home_yellow_cards,
        MAX(IF(stat_key = 'yellow_cards', TRY_CAST(away_text AS INTEGER))) AS away_yellow_cards,
        MAX(IF(stat_key = 'red_cards',    TRY_CAST(home_text AS INTEGER))) AS home_red_cards,
        MAX(IF(stat_key = 'red_cards',    TRY_CAST(away_text AS INTEGER))) AS away_red_cards,
        MAX(IF(stat_key = 'fouls',        TRY_CAST(home_text AS INTEGER))) AS home_fouls,
        MAX(IF(stat_key = 'fouls',        TRY_CAST(away_text AS INTEGER))) AS away_fouls,
        MAX(IF(stat_key = 'Offsides',     TRY_CAST(home_text AS INTEGER))) AS home_offsides,
        MAX(IF(stat_key = 'Offsides',     TRY_CAST(away_text AS INTEGER))) AS away_offsides,
        MAX(IF(stat_key = 'corners',      TRY_CAST(home_text AS INTEGER))) AS home_corner_kicks,
        MAX(IF(stat_key = 'corners',      TRY_CAST(away_text AS INTEGER))) AS away_corner_kicks,

        -- ===== Defence =====
        MAX(IF(stat_key = 'matchstats.headers.tackles', TRY_CAST(home_text AS INTEGER))) AS home_tackles,
        MAX(IF(stat_key = 'matchstats.headers.tackles', TRY_CAST(away_text AS INTEGER))) AS away_tackles,
        MAX(IF(stat_key = 'interceptions',              TRY_CAST(home_text AS INTEGER))) AS home_interceptions,
        MAX(IF(stat_key = 'interceptions',              TRY_CAST(away_text AS INTEGER))) AS away_interceptions,
        MAX(IF(stat_key = 'clearances',                 TRY_CAST(home_text AS INTEGER))) AS home_clearances,
        MAX(IF(stat_key = 'clearances',                 TRY_CAST(away_text AS INTEGER))) AS away_clearances,
        MAX(IF(stat_key = 'keeper_saves',               TRY_CAST(home_text AS INTEGER))) AS home_saves,
        MAX(IF(stat_key = 'keeper_saves',               TRY_CAST(away_text AS INTEGER))) AS away_saves,

        -- ===== UNIQUE_FOTMOB =====
        MAX(IF(stat_key = 'big_chance',         TRY_CAST(home_text AS INTEGER))) AS home_big_chances,
        MAX(IF(stat_key = 'big_chance',         TRY_CAST(away_text AS INTEGER))) AS away_big_chances,
        MAX(IF(stat_key = 'big_chance_missed_title', TRY_CAST(home_text AS INTEGER))) AS home_big_chances_missed,
        MAX(IF(stat_key = 'big_chance_missed_title', TRY_CAST(away_text AS INTEGER))) AS away_big_chances_missed,
        MAX(IF(stat_key = 'touches_opp_box',    TRY_CAST(home_text AS INTEGER))) AS home_touches_in_box,
        MAX(IF(stat_key = 'touches_opp_box',    TRY_CAST(away_text AS INTEGER))) AS away_touches_in_box

    FROM stats_flat
    GROUP BY match_id, league, season
),

-- ===== Step 3: derive expected_assists per team from player_stats_json =====
-- FotMob does not surface team-grain xA in stats_json; we SUM per-player xA
-- from player_stats_json (Top stats group → "Expected assists (xA)" entry).
-- player_stats_json shape: { "<player_id>": { "teamId": <int>, "stats": [{...}] } }.
team_xa AS (
    SELECT
        md.match_id,
        md.league,
        md.season,
        CAST(json_extract_scalar(pdata, '$.teamId') AS varchar) AS team_id,
        SUM(TRY_CAST(
            json_extract_scalar(
                pdata,
                '$.stats[0].stats["Expected assists (xA)"].stat.value'
            ) AS DOUBLE
        )) AS expected_assists,
        SUM(TRY_CAST(
            json_extract_scalar(
                pdata,
                '$.stats[0].stats["Expected goals (xG)"].stat.value'
            ) AS DOUBLE
        )) AS expected_goals_player_sum
    FROM match_details md
    CROSS JOIN UNNEST(
        map_entries(CAST(json_parse(md.player_stats_json) AS map<varchar, json>))
    ) AS pe(pid, pdata)
    WHERE md.player_stats_json IS NOT NULL
      AND md.player_stats_json <> 'null'
      AND md.player_stats_json <> '{}'
    GROUP BY md.match_id, md.league, md.season,
             CAST(json_extract_scalar(pdata, '$.teamId') AS varchar)
),

-- ===== Step 4: split pivot into long-form (home + away) =====
home_side AS (
    SELECT
        sp.match_id,
        -- team_id = team NAME (matches silver.xref_team source_id for fotmob);
        -- team_id_numeric kept only to JOIN team_xa (keyed by player_stats teamId).
        sp.home_team_name         AS team_id,
        sp.away_team_name         AS opponent_id,
        sp.home_team_id           AS team_id_numeric,
        TRUE                      AS is_home,
        sp.home_score             AS goals_for,
        sp.away_score             AS goals_against,
        sp.home_possession_pct    AS possession_pct,
        sp.home_total_shots       AS total_shots,
        sp.home_shots_on_target   AS shots_on_target,
        sp.home_shots_off_target  AS shots_off_target,
        sp.home_blocked_shots     AS blocked_shots,
        sp.home_shots_inside_box  AS shots_inside_box,
        sp.home_shots_outside_box AS shots_outside_box,
        sp.home_xg                AS expected_goals,
        sp.home_npxg              AS npxg,
        sp.home_xgot              AS xgot,
        sp.home_total_passes      AS total_passes,
        sp.home_accurate_passes   AS accurate_passes,
        sp.home_yellow_cards      AS yellow_cards,
        sp.home_red_cards         AS red_cards,
        sp.home_fouls             AS fouls,
        sp.home_offsides          AS offsides,
        sp.home_corner_kicks      AS corner_kicks,
        sp.home_tackles           AS tackles,
        sp.home_interceptions     AS interceptions,
        sp.home_clearances        AS clearances,
        sp.home_saves             AS saves,
        sp.home_big_chances       AS big_chances,
        sp.home_big_chances_missed AS big_chances_missed,
        sp.home_touches_in_box    AS touches_in_box,
        sp._bronze_ingested_at,
        sp.league,
        sp.season
    FROM stats_pivot sp
),

away_side AS (
    SELECT
        sp.match_id,
        sp.away_team_name         AS team_id,
        sp.home_team_name         AS opponent_id,
        sp.away_team_id           AS team_id_numeric,
        FALSE                     AS is_home,
        sp.away_score             AS goals_for,
        sp.home_score             AS goals_against,
        sp.away_possession_pct    AS possession_pct,
        sp.away_total_shots       AS total_shots,
        sp.away_shots_on_target   AS shots_on_target,
        sp.away_shots_off_target  AS shots_off_target,
        sp.away_blocked_shots     AS blocked_shots,
        sp.away_shots_inside_box  AS shots_inside_box,
        sp.away_shots_outside_box AS shots_outside_box,
        sp.away_xg                AS expected_goals,
        sp.away_npxg              AS npxg,
        sp.away_xgot              AS xgot,
        sp.away_total_passes      AS total_passes,
        sp.away_accurate_passes   AS accurate_passes,
        sp.away_yellow_cards      AS yellow_cards,
        sp.away_red_cards         AS red_cards,
        sp.away_fouls             AS fouls,
        sp.away_offsides          AS offsides,
        sp.away_corner_kicks      AS corner_kicks,
        sp.away_tackles           AS tackles,
        sp.away_interceptions     AS interceptions,
        sp.away_clearances        AS clearances,
        sp.away_saves             AS saves,
        sp.away_big_chances       AS big_chances,
        sp.away_big_chances_missed AS big_chances_missed,
        sp.away_touches_in_box    AS touches_in_box,
        sp._bronze_ingested_at,
        sp.league,
        sp.season
    FROM stats_pivot sp
),

unioned AS (
    SELECT * FROM home_side
    UNION ALL
    SELECT * FROM away_side
)

SELECT
    -- ===== Identity =====
    -- native match_id is bigint; Gold / xref_match expect the legacy varchar form
    CAST(u.match_id AS varchar) AS match_id,
    u.team_id,
    u.opponent_id,
    u.is_home,

    -- ===== Outcome (from match_details top-level) =====
    CAST(u.goals_for     AS INTEGER) AS goals_for,
    CAST(u.goals_against AS INTEGER) AS goals_against,

    -- ===== HARD_FACT — Possession =====
    u.possession_pct,

    -- ===== HARD_FACT — Shots =====
    u.total_shots,
    u.shots_on_target,
    u.shots_off_target,
    u.blocked_shots,
    u.shots_inside_box,
    u.shots_outside_box,

    -- ===== HARD_FACT — Passing =====
    u.total_passes,
    u.accurate_passes,
    CASE
        WHEN u.total_passes > 0
            THEN ROUND(100.0 * u.accurate_passes / u.total_passes, 2)
        ELSE NULL
    END AS accurate_passes_pct,

    -- ===== HARD_FACT — Discipline & set-pieces =====
    u.yellow_cards,
    u.red_cards,
    u.fouls,
    u.offsides,
    u.corner_kicks,

    -- ===== HARD_FACT — Defence =====
    u.tackles,
    u.interceptions,
    u.clearances,
    u.saves,

    -- ===== MODELED — xG family from stats_json =====
    ROUND(u.expected_goals, 4) AS expected_goals,
    ROUND(u.npxg,           4) AS npxg,
    ROUND(u.xgot,           4) AS xgot,

    -- ===== MODELED — expected_assists (SUM of per-player xA) =====
    -- The whole point of issue #97: team-grain xA from FotMob, since US/SS team-stats
    -- do not expose it. Sourced from player_stats_json, NOT stats_json.
    ROUND(xa.expected_assists, 4) AS expected_assists,
    -- Sanity column: per-player xG sum should approximate team xG (within rounding)
    ROUND(xa.expected_goals_player_sum, 4) AS expected_goals_player_sum,

    -- ===== UNIQUE_FOTMOB =====
    u.big_chances,
    u.big_chances_missed,
    u.touches_in_box,

    -- ===== Lineage =====
    u._bronze_ingested_at,

    -- ===== Partition keys (season → slug to match other Silver team-match tables) =====
    u.league,
    -- #913 Phase 2
    CASE WHEN u.league = 'INT-World Cup'
         THEN LPAD(CAST(u.season AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(u.season, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(u.season + 1, 100) AS varchar), 2, '0')
    END AS season

FROM unioned u
LEFT JOIN team_xa xa
    ON  xa.match_id = u.match_id
    AND xa.team_id  = u.team_id_numeric
    AND xa.league   = u.league
    AND xa.season   = u.season
WHERE u.team_id IS NOT NULL
