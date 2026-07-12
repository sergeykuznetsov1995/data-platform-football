-- Shadow Silver v2: typed, lossless squad-page attribute observations.
-- Natural key: (competition_id, edition_id, club_id, player_id, observed_at).
-- This history is retained so a NULL contract in the latest observation is
-- distinguishable from "not scraped"; downstream must not carry contracts
-- forward from an older observation.

SELECT
    CAST(competition_id AS varchar)                       AS competition_id,
    CAST(edition_id AS varchar)                           AS edition_id,
    CAST(player_id AS varchar)                             AS player_id,
    player_slug,
    name,
    position,
    TRY_CAST(dob AS date)                                  AS dob,
    TRY_CAST(age AS integer)                               AS age,
    TRY_CAST(height_cm AS integer)                         AS height_cm,
    foot,
    nationality,
    TRY_CAST(contract_until AS date)                       AS contract_until,
    TRY_CAST(market_value_eur AS bigint)                   AS market_value_eur,
    CAST(club_id AS varchar)                               AS club_id,
    club_name,
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
            PARTITION BY competition_id, edition_id, club_id, player_id, observed_at
            ORDER BY _ingested_at DESC, _batch_id DESC, source_body_hash DESC
        ) AS rn
    FROM iceberg.bronze.transfermarkt_player_attribute_observations b
    WHERE competition_id IS NOT NULL
      AND edition_id IS NOT NULL
      AND club_id IS NOT NULL
      AND player_id IS NOT NULL
      AND observed_at IS NOT NULL
)
WHERE rn = 1
