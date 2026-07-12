-- Shadow Silver v2: one global profile per Transfermarkt coach_id.

SELECT
    CAST(coach_id AS varchar)                             AS coach_id,
    coach_slug,
    name,
    TRY_CAST(dob AS date)                                 AS dob,
    nationality,
    CAST(_ingested_at AS timestamp(6))                    AS _bronze_ingested_at,
    _batch_id,
    source_competition_id,
    source_edition_id,
    source_url,
    source_body_hash,
    CAST(fetched_at AS timestamp(6))                      AS fetched_at,
    parser_revision,
    schema_revision,
    cycle_id,
    scope_id
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY coach_id
            ORDER BY _ingested_at DESC, _batch_id DESC,
                     source_body_hash DESC
        ) AS rn
    FROM iceberg.bronze.transfermarkt_coach_profiles b
    WHERE coach_id IS NOT NULL
)
WHERE rn = 1
