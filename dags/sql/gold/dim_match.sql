-- =============================================================================
-- Gold: dim_match
-- =============================================================================
-- Match-level attributes (date, gameweek, venue, teams). One row per match.
--
-- Sources: iceberg.silver.fbref_match_enriched + iceberg.silver.xref_team
-- PK: match_id
-- Partitioning: (league, season)
--
-- Migrated from gold.entity_xref to silver.xref_team in E1.5 (2026-05-09 prep).
-- silver.xref_team.season is varchar — we CAST m.season (bigint) per JOIN.
-- The (league, season) predicate prevents the 1.5-4x JOIN fan-out documented
-- in feedback_xref_join_season_predicate.md (silver.xref_team rows are
-- per-(source, source_id, league, season)).
-- =============================================================================

SELECT
    m.match_id,
    m.date,
    m.gameweek,
    m.time,
    m.venue,
    m.referee,
    m.attendance,
    home_x.canonical_id AS home_team_id,
    away_x.canonical_id AS away_team_id,
    m.home              AS home_team_name,
    m.away              AS away_team_name,
    m.home_score,
    m.away_score,
    CASE
        WHEN m.home_score > m.away_score THEN 'H'
        WHEN m.home_score < m.away_score THEN 'A'
        WHEN m.home_score = m.away_score THEN 'D'
    END                 AS result_1x2,
    m.home_score + m.away_score                                          AS total_goals,
    (m.home_score > 0 AND m.away_score > 0)                              AS btts,
    (m.home_score IS NOT NULL AND m.away_score IS NOT NULL)              AS is_completed,
    m.league,
    m.season
FROM iceberg.silver.fbref_match_enriched m
LEFT JOIN iceberg.silver.xref_team home_x
    ON home_x.source    = 'fbref'
   AND home_x.source_id = m.home
   AND home_x.league    = m.league
   AND home_x.season    = CAST(m.season AS varchar)
LEFT JOIN iceberg.silver.xref_team away_x
    ON away_x.source    = 'fbref'
   AND away_x.source_id = m.away
   AND away_x.league    = m.league
   AND away_x.season    = CAST(m.season AS varchar)
