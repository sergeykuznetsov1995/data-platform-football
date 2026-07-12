-- Shadow Silver v2: honest player-to-club membership grain.
-- Natural key: (competition_id, edition_id, club_id, player_id).

SELECT
    CAST(competition_id AS varchar)                       AS competition_id,
    CAST(edition_id AS varchar)                           AS edition_id,
    CAST(club_id AS varchar)                               AS club_id,
    club_slug,
    club_name,
    CAST(player_id AS varchar)                             AS player_id,
    player_slug,
    player_name,
    CAST(observed_at AS timestamp(6))                      AS observed_at,
    CAST(_ingested_at AS timestamp(6))                     AS _bronze_ingested_at,
    _batch_id,
    source_url,
    source_body_hash,
    CAST(fetched_at AS timestamp(6))                      AS fetched_at,
    parser_revision,
    schema_revision,
    cycle_id,
    scope_id,
    league,
    season
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY competition_id, edition_id, club_id, player_id
            ORDER BY observed_at DESC, _ingested_at DESC, _batch_id DESC,
                     source_body_hash DESC
        ) AS rn
    FROM iceberg.bronze.transfermarkt_squad_memberships b
    WHERE competition_id IS NOT NULL
      AND edition_id IS NOT NULL
      AND club_id IS NOT NULL
      AND player_id IS NOT NULL
)
WHERE rn = 1
