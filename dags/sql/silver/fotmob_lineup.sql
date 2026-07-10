-- =============================================================================
-- Silver: fotmob_lineup
-- =============================================================================
-- One row per (match_id, player_id) — per-match lineup parsed from FotMob
-- bronze.fotmob_match_details.lineup_json. Feeds the FotMob branch of
-- gold.fct_lineup (#693); grain mirrors silver.espn_lineup.
--
-- Source:
--   bronze.fotmob_match_details.lineup_json
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
--     xref_match bridge is keyed on the bronze id.
--   * NO isCaptain on lineup players (only the player_details endpoint has it)
--     → is_captain = NULL. gold.fct_lineup enriches captaincy via SofaScore.
--   * positionId is an int code (11=GK, ...) with no string label → store as
--     varchar (raw passthrough; dim_position mapping deferred, as for FBref/ESPN).
--   * shirtNumber is a string ('31') → TRY_CAST to integer for jersey_number.
--   * Season is bigint year-start (2025) at bronze level → emit slug '2526' to
--     match xref / other Silver (same conversion as fotmob_player_match_aggregate).
-- =============================================================================

WITH match_dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY match_id, league, season
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fotmob_match_details
    WHERE lineup_json IS NOT NULL
      AND lineup_json <> 'null'
      AND lineup_json <> '{}'
),

base AS (
    SELECT
        match_id,
        league,
        -- #913 Phase 2
        CASE WHEN league = 'INT-World Cup'
             THEN LPAD(CAST(season AS varchar), 4, '0')
             ELSE LPAD(CAST(MOD(season, 100) AS varchar), 2, '0')
                  || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0')
        END AS season,
        _ingested_at,
        lineup_json
    FROM match_dedup
    WHERE rn = 1
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
