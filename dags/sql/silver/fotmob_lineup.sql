-- =============================================================================
-- Silver: fotmob_lineup
-- =============================================================================
-- One row per (match_id, player_id) — per-match lineup parsed from FotMob
-- bronze.fotmob_match_payloads_current.lineup_json. Feeds the FotMob branch of
-- gold.fct_lineup (#693); grain mirrors silver.espn_lineup.
--
-- Source (native cutover #930, was bronze.fotmob_match_details):
--   bronze.fotmob_match_payloads_current.lineup_json (content.lineup verbatim,
--   same JSON paths as legacy — probed live 2026-07-17)
--
-- lineup_json shape (probed live 2026-06-20, ENG-Premier League):
--   { matchId, lineupType, source, availableFilters,
--     homeTeam: { id, name, formation, starters:[...], subs:[...], coach, ... },
--     awayTeam: { ... same ... } }
--   player = { id:int, name, positionId:int (code), shirtNumber:str, age,
--              countryName, marketValue, performance, ... }
--
-- Notes / footguns:
--   * starters[] → is_starter=true, subs[] → is_starter=false. Unused bench
--     players that FotMob lists under subs ARE included (is_starter=false),
--     so FotMob lineup ⊇ SofaScore (which only carries players who played).
--   * Use the bronze `match_id` COLUMN downstream for xref_match, NOT
--     lineup_json.matchId — they differ (bronze 4813664 vs json 5186383); the
--     xref_match bridge is keyed on the bronze id. Native match_id is bigint →
--     CAST to varchar to keep the Silver contract (fct_lineup/xref join on it).
--   * NO isCaptain on lineup players (only the player endpoint has it)
--     → is_captain = NULL. gold.fct_lineup enriches captaincy via SofaScore.
--   * positionId is an int code (11=GK, ...) with no string label → store as
--     varchar (raw passthrough; dim_position mapping deferred, as for FBref/ESPN).
--   * shirtNumber is a string ('31') → TRY_CAST to integer for jersey_number.
--   * league: native carries competition_id (varchar in payloads) instead of a
--     league string → INNER JOIN league_map (also scopes output to the legacy
--     14-league surface; widening scope is a separate decision, not cutover).
--   * season: native source_season_key is the exact source string ('2025/2026'
--     club, '2026' single-year) → year = substr(key,1,4), then the SAME legacy
--     slug CASE ('2526', WC '2026') — do NOT derive the slug from the key form.
--   * dedup: *_current view is already one row per match (manifest identity
--     entity_id = match_id) → legacy ROW_NUMBER dedup dropped.
-- =============================================================================

WITH league_map(competition_id, league) AS (
    VALUES (47, 'ENG-Premier League'), (48, 'ENG-Championship'), (87, 'ESP-La Liga'),
           (54, 'GER-Bundesliga'), (55, 'ITA-Serie A'), (53, 'FRA-Ligue 1'),
           (57, 'NED-Eredivisie'), (61, 'POR-Primeira Liga'), (42, 'UEFA-Champions League'),
           (73, 'UEFA-Europa League'), (77, 'INT-World Cup'), (50, 'INT-European Championship'),
           (289, 'INT-Africa Cup of Nations'), (44, 'INT-Copa America')
),

match_scoped AS (
    SELECT
        CAST(p.match_id AS varchar) AS match_id,
        lm.league,
        TRY_CAST(substr(p.source_season_key, 1, 4) AS integer) AS season_year,
        p._observed_at AS _ingested_at,
        p.lineup_json
    FROM iceberg.bronze.fotmob_match_payloads_current p
    JOIN league_map lm ON lm.competition_id = CAST(p.competition_id AS bigint)
    WHERE p.lineup_json IS NOT NULL
      AND p.lineup_json <> 'null'
      AND p.lineup_json <> '{}'
),

base AS (
    SELECT
        match_id,
        league,
        -- #913 Phase 2
        CASE WHEN league = 'INT-World Cup'
             THEN LPAD(CAST(season_year AS varchar), 4, '0')
             ELSE LPAD(CAST(MOD(season_year, 100) AS varchar), 2, '0')
                  || LPAD(CAST(MOD(season_year + 1, 100) AS varchar), 2, '0')
        END AS season,
        _ingested_at,
        lineup_json
    FROM match_scoped
),

-- One row per (team-side × starter/sub × player). Four UNNEST branches because
-- starters and subs are separate arrays under homeTeam / awayTeam.
exploded AS (
    SELECT b.match_id, b.league, b.season, b._ingested_at,
           json_extract_scalar(b.lineup_json, '$.homeTeam.name') AS team_name,
           true  AS is_home, true  AS is_starter, p.player
    FROM base b
    CROSS JOIN UNNEST(
        CAST(json_extract(b.lineup_json, '$.homeTeam.starters') AS array<json>)
    ) AS p(player)

    UNION ALL
    SELECT b.match_id, b.league, b.season, b._ingested_at,
           json_extract_scalar(b.lineup_json, '$.homeTeam.name'),
           true,  false, p.player
    FROM base b
    CROSS JOIN UNNEST(
        CAST(json_extract(b.lineup_json, '$.homeTeam.subs') AS array<json>)
    ) AS p(player)

    UNION ALL
    SELECT b.match_id, b.league, b.season, b._ingested_at,
           json_extract_scalar(b.lineup_json, '$.awayTeam.name'),
           false, true,  p.player
    FROM base b
    CROSS JOIN UNNEST(
        CAST(json_extract(b.lineup_json, '$.awayTeam.starters') AS array<json>)
    ) AS p(player)

    UNION ALL
    SELECT b.match_id, b.league, b.season, b._ingested_at,
           json_extract_scalar(b.lineup_json, '$.awayTeam.name'),
           false, false, p.player
    FROM base b
    CROSS JOIN UNNEST(
        CAST(json_extract(b.lineup_json, '$.awayTeam.subs') AS array<json>)
    ) AS p(player)
)

SELECT
    match_id,
    CAST(json_extract_scalar(player, '$.id') AS varchar)              AS player_id,
    json_extract_scalar(player, '$.name')                            AS player_name,
    team_name,
    is_home,
    is_starter,
    -- No captaincy in lineup_json → NULL (gold enriches via SofaScore #439).
    CAST(NULL AS boolean)                                            AS is_captain,
    -- FotMob positionId is an int CODE (no label) — raw passthrough as varchar.
    json_extract_scalar(player, '$.positionId')                      AS position,
    TRY_CAST(json_extract_scalar(player, '$.shirtNumber') AS integer) AS jersey_number,
    _ingested_at                                                     AS _bronze_ingested_at,
    league,
    season
FROM exploded
