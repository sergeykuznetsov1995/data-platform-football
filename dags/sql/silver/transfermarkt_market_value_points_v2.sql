-- Shadow Silver v2: global career market-value facts.
-- Natural key: (player_id, mv_date).  No scrape league/season is retained.

SELECT
    CAST(b.player_id AS varchar)                          AS player_id,
    x.canonical_id,
    TRY_CAST(b.mv_date AS date)                           AS mv_date,
    TRY_CAST(b.value_eur AS bigint)                       AS value_eur,
    b.club_name,
    TRY_CAST(b.age AS integer)                            AS age,
    b.mv_raw,
    CAST(b._ingested_at AS timestamp(6))                  AS _bronze_ingested_at,
    b._batch_id,
    b.source_competition_id,
    b.source_edition_id,
    b.source_url,
    b.source_body_hash,
    CAST(b.fetched_at AS timestamp(6))                    AS fetched_at,
    b.parser_revision,
    b.schema_revision,
    b.cycle_id,
    b.scope_id
FROM (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, mv_date
            ORDER BY _ingested_at DESC, _batch_id DESC,
                     source_body_hash DESC
        ) AS rn
    FROM iceberg.bronze.transfermarkt_market_value_points p
    WHERE player_id IS NOT NULL
      AND mv_date IS NOT NULL
) b
LEFT JOIN iceberg.silver.transfermarkt_player_xref_global_v2 x
  ON x.player_id = b.player_id
 AND x.resolution_status = 'resolved'
WHERE b.rn = 1
