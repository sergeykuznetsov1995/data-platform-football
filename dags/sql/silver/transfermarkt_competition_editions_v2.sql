-- Transfermarkt editions with source-year semantics preserved.
-- Grain / natural key: (competition_id, edition_id).
-- canonical_season is derived only after season_format is classified; the
-- original edition_id and label remain available for round-trip validation.

SELECT
    CAST(competition_id AS varchar)                       AS competition_id,
    CAST(edition_id AS varchar)                           AS edition_id,
    edition_label,
    canonical_season,
    season_format,
    TRY_CAST(start_date AS date)                          AS start_date,
    TRY_CAST(end_date AS date)                            AS end_date,
    TRY_CAST(active AS boolean)                           AS active,
    TRY_CAST("current" AS boolean)                        AS is_current,
    TRY_CAST(participant_count AS integer)                AS participant_count,
    participant_hash,
    source_url,
    TRY_CAST(discovered_at AS timestamp(6))               AS discovered_at,
    registry_snapshot_id,
    source_body_hash,
    parser_revision,
    schema_revision,
    CAST(fetched_at AS timestamp(6))                      AS fetched_at,
    cycle_id,
    scope_id,
    CAST(_ingested_at AS timestamp(6))                    AS _bronze_ingested_at,
    _batch_id
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY competition_id, edition_id
            ORDER BY TRY_CAST(discovered_at AS timestamp(6)) DESC,
                     _ingested_at DESC,
                     _batch_id DESC,
                     source_body_hash DESC
        ) AS rn
    FROM iceberg.bronze.transfermarkt_competition_editions b
    WHERE competition_id IS NOT NULL
      AND edition_id IS NOT NULL
)
WHERE rn = 1
