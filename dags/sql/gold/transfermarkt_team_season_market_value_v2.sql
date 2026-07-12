-- =============================================================================
-- Shadow Gold v2: membership-based team-season market value
-- =============================================================================
-- Multi-club players use the deterministic end-of-season assignment produced
-- by Silver.  Ambiguous assignments are excluded from SUM and surfaced as a
-- league-season warning count on every output row.
-- Grain: (team_id, league, season).

WITH edition_bounds AS (
    SELECT
        competition_id,
        edition_id,
        COALESCE(
            end_date,
            CASE
                WHEN season_format = 'single_year' THEN CAST(
                    CONCAT(canonical_season, '-12-31') AS date
                )
            END
        ) AS edition_end_date
    FROM iceberg.silver.transfermarkt_competition_editions_v2
),

latest_season_value AS (
    SELECT competition_id, edition_id, player_id, league, season, value_eur
    FROM (
        SELECT
            a.competition_id,
            a.edition_id,
            a.player_id,
            a.league,
            a.season,
            mv.value_eur,
            ROW_NUMBER() OVER (
                PARTITION BY a.competition_id, a.edition_id, a.player_id
                ORDER BY mv.mv_date DESC, mv._bronze_ingested_at DESC
            ) AS rn
        FROM iceberg.silver.transfermarkt_player_team_season_assignment_v2 a
        JOIN edition_bounds bounds
          ON bounds.competition_id = a.competition_id
         AND bounds.edition_id = a.edition_id
        JOIN iceberg.silver.transfermarkt_market_value_points_v2 mv
          ON mv.player_id = a.player_id
         AND mv.mv_date <= bounds.edition_end_date
        WHERE bounds.edition_end_date IS NOT NULL
    )
    WHERE rn = 1
),

ambiguous AS (
    SELECT
        league,
        season,
        COUNT(*)                                           AS ambiguous_players_excluded
    FROM iceberg.silver.transfermarkt_player_team_season_assignment_v2
    WHERE assignment_status = 'ambiguous'
    GROUP BY league, season
),

resolved AS (
    SELECT
        COALESCE(a.team_id, 'tm_' || a.club_id)            AS team_id,
        a.club_name,
        a.league,
        a.season,
        a.player_id,
        v.value_eur                                       AS market_value_eur
    FROM iceberg.silver.transfermarkt_player_team_season_assignment_v2 a
    LEFT JOIN latest_season_value v
      ON v.competition_id = a.competition_id
     AND v.edition_id = a.edition_id
     AND v.player_id = a.player_id
     AND v.league = a.league
     AND v.season = a.season
    WHERE a.assignment_status <> 'ambiguous'
      AND a.club_id IS NOT NULL
)

SELECT
    r.team_id,
    MAX(r.club_name)                                      AS team_name,
    CAST(SUM(r.market_value_eur) AS bigint)               AS squad_market_value_eur,
    COUNT_IF(r.market_value_eur IS NOT NULL)               AS valued_players,
    COUNT(*)                                               AS assigned_players,
    COALESCE(MAX(a.ambiguous_players_excluded), 0)         AS ambiguous_players_excluded,
    r.league,
    r.season
FROM resolved r
LEFT JOIN ambiguous a
  ON a.league = r.league
 AND a.season = r.season
GROUP BY r.team_id, r.league, r.season
