-- =============================================================================
-- Silver: whoscored_lineup
-- =============================================================================
-- One row per (match_id, player_id) — REAL per-match lineup from
-- bronze.whoscored_lineups (matchCentreData home/away.players[], issue #708).
-- Feeds the WhoScored branch of gold.fct_lineup (#693).
--
-- Source:
--   bronze.whoscored_lineups   (isFirstEleven / position / shirtNo / rating,
--                               parsed from the same page as whoscored_events)
--
-- History: until 2026-07 the lineup was INFERRED from bronze.whoscored_events
-- (appeared ∧ ¬SubstitutionOn ⇒ starter; position/jersey NULL; unused subs
-- absent — the structural floor behind the fct_lineup FK-orphan #839). The
-- events scraper now parses the matchCentre player block directly, so this
-- model reads real flags and INCLUDES unused substitutes.
--
-- Footguns:
--   * Bronze IDs arrive as DOUBLE → CAST(CAST(.. AS BIGINT) AS varchar) or
--     they serialise as '9.5408E4' (feedback_bronze_double_id_cast.md).
--   * bronze.whoscored_lineups is APPEND-only; a race between skip-existing
--     runs can duplicate a match → ROW_NUMBER keeps the freshest ingest.
--   * team name comes from matchCentreData home/away.name — same spelling as
--     whoscored_schedule home_team/away_team (one site), so the gold
--     xref_team JOIN keeps working.
--   * season is already the slug '2526' in Bronze → passthrough.
-- =============================================================================

WITH lineups AS (
    SELECT
        CAST(CAST(game_id   AS BIGINT) AS varchar) AS match_id,
        CAST(CAST(player_id AS BIGINT) AS varchar) AS player_id,
        team                                       AS team_name,
        is_starter,
        position,
        TRY_CAST(CAST(shirt_no AS BIGINT) AS integer) AS jersey_number,
        league,
        season,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, player_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.whoscored_lineups
    WHERE game_id IS NOT NULL
      AND player_id IS NOT NULL
)

SELECT
    match_id,
    player_id,
    team_name,
    is_starter,
    -- not exposed by matchCentreData:
    CAST(NULL AS boolean)   AS is_captain,
    position,
    jersey_number,
    _ingested_at            AS _bronze_ingested_at,
    league,
    season
FROM lineups
WHERE rn = 1
