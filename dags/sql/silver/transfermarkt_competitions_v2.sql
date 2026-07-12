-- Transfermarkt competition registry, typed and deduplicated.
-- Grain / natural key: (competition_id).
-- Unknown or conflicting classifications are retained for audit; the Airflow
-- scope planner is responsible for blocking them from paid crawling.

SELECT
    CAST(competition_id AS varchar)                       AS competition_id,
    slug,
    name,
    country,
    confederation,
    competition_type,
    gender,
    team_type,
    age_category,
    season_format,
    TRY_CAST(active AS boolean)                           AS active,
    source_url,
    TRY_CAST(discovered_at AS timestamp(6))               AS discovered_at,
    canonical_competition_id,
    classification_status,
    classification_evidence,
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
            PARTITION BY competition_id
            ORDER BY TRY_CAST(discovered_at AS timestamp(6)) DESC,
                     _ingested_at DESC,
                     _batch_id DESC,
                     source_body_hash DESC
        ) AS rn
    FROM iceberg.bronze.transfermarkt_competitions b
    WHERE competition_id IS NOT NULL
)
WHERE rn = 1
