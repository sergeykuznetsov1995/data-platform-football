-- =============================================================================
-- Silver: whoscored_lineup
-- =============================================================================
-- One row per (match_id, player_id) — per-match lineup INFERRED from
-- bronze.whoscored_events. Feeds the WhoScored branch of gold.fct_lineup (#693).
--
-- Source:
--   bronze.whoscored_events      (Opta event stream)
--   bronze.whoscored_schedule    (team_id → team name bridge)
--
-- Why inference (probed live 2026-06-20):
--   WhoScored has NO lineup/formation block in Bronze — the matchCentre
--   home.players[]/away.players[] block (isFirstEleven/position/shirtNo) is
--   discarded by the events scraper. The `type='Start'` events are PERIOD meta
--   markers with player_id=NULL (1,870 rows, ~4/match), NOT per-player starter
--   flags. So is_starter is INFERRED, not read:
--     * a player who APPEARED (has >=1 event) and is NOT in SubstitutionOn
--       started; a player in SubstitutionOn came off the bench.
--     * live check: appeared ~30/match - subbed_on ~8 => ~22 starters/match. OK
--   Consequences (thinner than SofaScore/FotMob, accepted in #693):
--     * Unused subs (no events) are absent — lineup is a subset of players who played.
--     * position / is_captain / jersey_number are NOT derivable → NULL.
--
-- Footguns:
--   * Bronze IDs: player_id is DOUBLE → CAST(CAST(.. AS BIGINT) AS varchar) or
--     it serialises as '9.5408E4'. game_id/team_id are bigint but double-cast
--     too for uniformity (feedback_bronze_double_id_cast.md).
--   * WhoScored team_id is numeric; xref_team source_id is the team NAME, so we
--     resolve team_name here via whoscored_schedule (same bridge as
--     fct_team_match.ws_team_name_bridge) and emit team_name for the gold JOIN.
--   * season is already the slug '2526' in Bronze → passthrough.
-- =============================================================================

WITH events AS (
    SELECT
        CAST(CAST(game_id   AS BIGINT) AS varchar) AS match_id,
        CAST(CAST(player_id AS BIGINT) AS varchar) AS player_id,
        CAST(CAST(team_id   AS BIGINT) AS varchar) AS team_id,
        type,
        league,
        season,
        _ingested_at
    FROM iceberg.bronze.whoscored_events
    WHERE player_id IS NOT NULL
      AND game_id   IS NOT NULL
),

-- One row per (match, player) who appeared (had >=1 event).
appeared AS (
    SELECT
        match_id,
        player_id,
        ARBITRARY(team_id)      AS team_id,
        ARBITRARY(league)       AS league,
        ARBITRARY(season)       AS season,
        MAX(_ingested_at)       AS _bronze_ingested_at
    FROM events
    GROUP BY match_id, player_id
),

-- Players who came on from the bench (→ is_starter = false).
subbed_on AS (
    SELECT DISTINCT match_id, player_id
    FROM events
    WHERE type = 'SubstitutionOn'
),

-- team_id (numeric) → team NAME, per (league, season). MAX collapses any
-- same-(team,season) spelling drift to one row so the JOIN below stays 1:1.
ws_team_name_bridge AS (
    SELECT ws_team_id, league, season, MAX(team_name) AS team_name
    FROM (
        SELECT CAST(CAST(home_team_id AS BIGINT) AS varchar) AS ws_team_id,
               home_team AS team_name, league, season
        FROM iceberg.bronze.whoscored_schedule
        WHERE home_team_id IS NOT NULL
        UNION ALL
        SELECT CAST(CAST(away_team_id AS BIGINT) AS varchar),
               away_team, league, season
        FROM iceberg.bronze.whoscored_schedule
        WHERE away_team_id IS NOT NULL
    )
    GROUP BY ws_team_id, league, season
)

SELECT
    a.match_id,
    a.player_id,
    b.team_name,
    -- inferred: appeared AND not subbed on => starter.
    (so.player_id IS NULL)                  AS is_starter,
    -- not derivable from WhoScored events:
    CAST(NULL AS boolean)                   AS is_captain,
    CAST(NULL AS varchar)                   AS position,
    CAST(NULL AS integer)                   AS jersey_number,
    a._bronze_ingested_at,
    a.league,
    a.season
FROM appeared a
LEFT JOIN subbed_on so
    ON  so.match_id  = a.match_id
   AND so.player_id  = a.player_id
LEFT JOIN ws_team_name_bridge b
    ON  b.ws_team_id = a.team_id
   AND b.league      = a.league
   AND b.season      = a.season
