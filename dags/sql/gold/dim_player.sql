-- =============================================================================
-- Gold: dim_player
-- =============================================================================
-- Canonical player dimension. One row per (player_id, season) — a player can
-- change teams, nationality is stable, age advances per season.
--
-- Sources: iceberg.silver.fbref_player_season_profile
-- PK: (player_id, season)
-- Partitioning: (league, season)
-- =============================================================================

SELECT
    player_id,
    MAX(player)                 AS player_name,
    MAX(nation)                 AS nation,
    MAX(pos)                    AS position,
    MAX(born)                   AS born_year,
    MAX(squad)                  AS last_team,
    league,
    season
FROM iceberg.silver.fbref_player_season_profile
WHERE player_id IS NOT NULL
  AND season    IS NOT NULL
GROUP BY player_id, league, season
