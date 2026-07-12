-- Lossless Transfermarkt contract observations captured from squad responses.
-- Grain / natural key:
-- (competition_id, edition_id, team_id, player_id, observed_at).
-- contract_until is intentionally nullable: applicability_status distinguishes
-- a source-backed empty value from a typed not-applicable observation.

WITH deduped AS (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY competition_id, edition_id, team_id, player_id, observed_at
            ORDER BY _ingested_at DESC,
                     _batch_id DESC,
                     source_body_hash DESC
        ) AS rn
    FROM iceberg.bronze.transfermarkt_player_contract_observations b
    WHERE competition_id IS NOT NULL
      AND edition_id IS NOT NULL
      AND team_id IS NOT NULL
      AND player_id IS NOT NULL
      AND observed_at IS NOT NULL
)

SELECT
    CAST(b.competition_id AS varchar)                     AS competition_id,
    CAST(b.edition_id AS varchar)                         AS edition_id,
    CAST(b.team_id AS varchar)                            AS team_id,
    b.team_name,
    CAST(b.player_id AS varchar)                          AS player_id,
    x.canonical_id,
    TRY_CAST(b.contract_until AS date)                    AS contract_until,
    CAST(b.observed_at AS timestamp(6))                   AS observed_at,
    b.applicability_status,
    b.source_url,
    b.source_body_hash,
    b.parser_revision,
    b.schema_revision,
    CAST(b.fetched_at AS timestamp(6))                    AS fetched_at,
    b.cycle_id,
    b.scope_id,
    CAST(b._ingested_at AS timestamp(6))                  AS _bronze_ingested_at,
    b._batch_id
FROM deduped b
LEFT JOIN iceberg.silver.transfermarkt_player_xref_global_v2 x
  ON x.player_id = b.player_id
 AND x.resolution_status = 'resolved'
WHERE b.rn = 1
