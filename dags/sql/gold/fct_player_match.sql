-- =============================================================================
-- Gold: fct_player_match
-- =============================================================================
-- Player-level performance per match.
--
-- Sources: iceberg.silver.fbref_player_match_stats + iceberg.silver.xref_team
-- PK: (match_id, player_id)
-- Partitioning: (league, season)
--
-- Migrated from gold.entity_xref to silver.xref_team in E1.5 (2026-05-09 prep).
-- player_id carries the 'fb_' prefix for consistency with dim_player and
-- silver.xref_player canonical convention (FBref-source canonical_id =
-- 'fb_' || raw player_id). team JOIN includes the (league, season) predicate
-- to prevent the 1.5-4x fan-out documented in
-- feedback_xref_join_season_predicate.md.
-- =============================================================================

SELECT
    pms.match_id,
    'fb_' || pms.player_id  AS player_id,
    tx.canonical_id         AS team_id,
    pms.team                AS team_name,
    pms.team_side,
    pms.pos                 AS position,
    pms.minutes,
    pms.goals,
    pms.assists,
    pms.shots,
    pms.shots_on_target,
    pms.penalty_goals,
    pms.penalty_attempts,
    pms.yellow_cards,
    pms.red_cards,
    pms.tackles_won,
    pms.interceptions,
    pms.fouls_committed,
    pms.fouls_drawn,
    pms.offsides,
    pms.own_goals,
    pms.league,
    pms.season
FROM iceberg.silver.fbref_player_match_stats pms
LEFT JOIN iceberg.silver.xref_team tx
    ON tx.source    = 'fbref'
   AND tx.source_id = pms.team
   AND tx.league    = pms.league
   AND tx.season    = CAST(pms.season AS varchar)
WHERE pms.match_id  IS NOT NULL
  AND pms.player_id IS NOT NULL
