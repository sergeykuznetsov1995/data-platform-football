-- =============================================================================
-- Gold: dim_player
-- =============================================================================
-- Canonical player dimension. One row per (player_id, season) — a player can
-- change teams, nationality is stable, age advances per season.
--
-- Sources: iceberg.silver.fbref_player_season_profile
-- PK: (player_id, season)
-- Partitioning: (league, season)
--
-- Migrated from gold.entity_xref to silver.xref_player canonical convention in
-- E1.5 (2026-05-09 prep). FBref-source players in silver.xref_player carry
-- canonical_id = 'fb_' || raw player_id (see xref_player_resolver.py + the
-- '^fb_' prefix guard in xref_player.sql validation). We apply that prefix
-- inline here rather than JOINing — the JOIN would be redundant since the
-- canonical id is a deterministic function of source + raw id.
-- =============================================================================

SELECT
    'fb_' || player_id          AS player_id,
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
