-- =============================================================================
-- Gold: dim_team
-- =============================================================================
-- Canonical team dimension. One row per (team_id, league, season) to capture
-- team presence per season (promotions/relegations).
--
-- Sources: iceberg.gold.entity_xref
-- PK: (team_id, league, season)
-- Partitioning: (league, season)
-- =============================================================================

SELECT
    canonical_id AS team_id,
    display_name AS team_name,
    league,
    season
FROM iceberg.gold.entity_xref
WHERE entity_type = 'team'
  AND source      = 'fbref'
GROUP BY canonical_id, display_name, league, season
