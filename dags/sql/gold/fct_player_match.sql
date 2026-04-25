-- =============================================================================
-- Gold: fct_player_match
-- =============================================================================
-- Player-level performance per match.
--
-- Sources: iceberg.silver.fbref_player_match_stats + entity_xref
-- PK: (match_id, player_id)
-- Partitioning: (league, season)
-- =============================================================================

SELECT
    pms.match_id,
    pms.player_id,
    tx.canonical_id       AS team_id,
    pms.team              AS team_name,
    pms.team_side,
    pms.pos               AS position,
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
LEFT JOIN iceberg.gold.entity_xref tx
    ON tx.entity_type = 'team'
   AND tx.source      = 'fbref'
   AND tx.source_id   = pms.team
   AND tx.season      = pms.season
   AND tx.league      = pms.league
WHERE pms.match_id  IS NOT NULL
  AND pms.player_id IS NOT NULL
