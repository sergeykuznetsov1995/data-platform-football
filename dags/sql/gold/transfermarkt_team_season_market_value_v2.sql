-- Sum current squad values for each team, league and season, as in the legacy snapshot.
-- The selected membership supplies the club and scopes the player's latest value.
WITH membership AS (
    SELECT competition_id, edition_id, club_id, club_name, player_id, league, season
    FROM (
        SELECT
            competition_id, edition_id, club_id, club_name, player_id, league, season,
            ROW_NUMBER() OVER (
                PARTITION BY player_id, league, season
                ORDER BY observed_at DESC, _bronze_ingested_at DESC, club_id ASC
            ) AS rn
        FROM iceberg.silver.transfermarkt_squad_memberships_v2
    ) ranked
    WHERE rn = 1
),
scoped_attributes AS (
    SELECT competition_id, edition_id, club_id, player_id, market_value_eur
    FROM (
        SELECT
            competition_id, edition_id, club_id, player_id, market_value_eur,
            ROW_NUMBER() OVER (
                PARTITION BY competition_id, edition_id, club_id, player_id
                ORDER BY observed_at DESC, _bronze_ingested_at DESC, _batch_id DESC, source_body_hash DESC
            ) AS rn
        FROM iceberg.silver.transfermarkt_player_attribute_observations_v2
    ) ranked
    WHERE rn = 1
),
players AS (
    SELECT
        m.club_name AS current_club_name,
        a.market_value_eur AS current_market_value_eur,
        m.league,
        m.season
    FROM membership m
    LEFT JOIN scoped_attributes a
        ON a.competition_id = m.competition_id
        AND a.edition_id = m.edition_id
        AND a.club_id = m.club_id
        AND a.player_id = m.player_id
),
team_xref AS (
    SELECT
        source_id AS club_name,
        league,
        season,
        CASE WHEN COUNT(DISTINCT canonical_id) FILTER (WHERE confidence <> 'orphan') = 1
            THEN MAX(canonical_id) FILTER (WHERE confidence <> 'orphan')
        END AS team_id
    FROM iceberg.silver.xref_team
    WHERE source = 'transfermarkt'
    GROUP BY source_id, league, season
)
SELECT
    COALESCE(x.team_id, 'tm_' || LOWER(REGEXP_REPLACE(p.current_club_name, '[^a-zA-Z0-9]+', '_'))) AS team_id,
    MAX(p.current_club_name) AS team_name,
    CAST(SUM(p.current_market_value_eur) AS bigint) AS squad_market_value_eur,
    COUNT_IF(p.current_market_value_eur IS NOT NULL) AS valued_players,
    COUNT(*) AS assigned_players,
    CAST(0 AS bigint) AS ambiguous_players_excluded,
    p.league,
    p.season
FROM players p
LEFT JOIN team_xref x
    ON x.club_name = p.current_club_name
    AND x.league = p.league
    AND x.season = p.season
WHERE p.current_club_name IS NOT NULL
GROUP BY
    COALESCE(x.team_id, 'tm_' || LOWER(REGEXP_REPLACE(p.current_club_name, '[^a-zA-Z0-9]+', '_'))),
    p.league,
    p.season
