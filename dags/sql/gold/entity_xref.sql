-- =============================================================================
-- Gold: entity_xref
-- =============================================================================
-- Unified cross-reference of external entity IDs to canonical IDs.
--
-- Designed to be extended by new sources (Understat, SofaScore, FotMob, ...)
-- via UNION ALL. For MVP only FBref is populated.
--
-- Columns:
--   entity_type   — 'team' | 'player' | 'match'
--   source        — 'fbref' | future: 'understat', 'sofascore', ...
--   source_id     — external ID as it appears in the source system
--   canonical_id  — stable, deterministic ID used across Gold
--   display_name  — human-readable label for debugging
--
-- Partition: (league, season) from the source row; this is wide, queried
-- mostly by (source, entity_type) so we don't partition here — the table
-- stays small.
-- =============================================================================

-- Teams: canonical_id = lower(normalized name) [simple for MVP, FBref-only]
SELECT
    'team'     AS entity_type,
    'fbref'    AS source,
    team_name  AS source_id,
    -- Strip diacritics before slugging (issue #215) so a name with/without
    -- accents maps to one canonical_id. NORMALIZE(NFD) + `\p{Mn}+` strip;
    -- same idiom as xref_team.sql.j2.
    LOWER(REGEXP_REPLACE(
        REGEXP_REPLACE(NORMALIZE(team_name, NFD), '\p{Mn}+', ''),
        '[^a-zA-Z0-9]+', '_')) AS canonical_id,
    team_name  AS display_name,
    league,
    season
FROM (
    SELECT DISTINCT home AS team_name, league, season
    FROM iceberg.silver.fbref_match_enriched
    WHERE home IS NOT NULL
    UNION
    SELECT DISTINCT away AS team_name, league, season
    FROM iceberg.silver.fbref_match_enriched
    WHERE away IS NOT NULL
) teams

UNION ALL

-- Players: canonical_id = fbref player_id (stable hash assigned by FBref)
SELECT
    'player'   AS entity_type,
    'fbref'    AS source,
    player_id  AS source_id,
    player_id  AS canonical_id,
    MAX(player) AS display_name,
    league,
    season
FROM iceberg.silver.fbref_player_season_profile
WHERE player_id IS NOT NULL
GROUP BY player_id, league, season

UNION ALL

-- Matches: canonical_id = fbref match_id
SELECT
    'match'    AS entity_type,
    'fbref'    AS source,
    match_id   AS source_id,
    match_id   AS canonical_id,
    CONCAT(home, ' vs ', away) AS display_name,
    league,
    season
FROM iceberg.silver.fbref_match_enriched
WHERE match_id IS NOT NULL
