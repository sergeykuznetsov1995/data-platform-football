-- =============================================================================
-- Gold: dim_team
-- =============================================================================
-- Canonical team dimension. One row per (team_id, league, season) to capture
-- team presence per season (promotions/relegations).
--
-- Sources: iceberg.silver.xref_team
-- PK: (team_id, league, season)
-- Partitioning: (league, season)
--
-- Migrated from gold.entity_xref to silver.xref_team in E1.5 (2026-05-09 prep).
-- silver.xref_team pre-aggregates per (source, source_id, league, season) and
-- carries alias-canonical IDs (e.g. 'manchester_united' rather than the legacy
-- regex-canonical 'manchester_utd'). FBref-only subset retained here.
--
-- Type note: silver.xref_team.season is varchar; we CAST back to BIGINT here
-- to preserve the legacy dim_team.season=bigint contract used by downstream
-- marts (mart_event_heatmap, fct_player_unavailable join via dim_match).
-- =============================================================================

SELECT
    canonical_id            AS team_id,
    display_name            AS team_name,
    league,
    CAST(season AS bigint)  AS season
FROM iceberg.silver.xref_team
WHERE source = 'fbref'
GROUP BY canonical_id, display_name, league, CAST(season AS bigint)
