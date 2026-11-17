WITH histories AS (
    SELECT 'matches' AS table_key, snapshot_id, made_current_at
    FROM iceberg.bronze."fotmob_matches$history"
    UNION ALL
    SELECT 'payloads', snapshot_id, made_current_at
    FROM iceberg.bronze."fotmob_match_payloads$history"
    UNION ALL
    SELECT 'manifest', snapshot_id, made_current_at
    FROM iceberg.bronze."fotmob_ingest_manifest$history"
    UNION ALL
    SELECT 'c1', snapshot_id, made_current_at
    FROM iceberg.bronze."fotmob_c1_inputs$history"
), ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY table_key ORDER BY made_current_at DESC, snapshot_id DESC
    ) AS position
    FROM histories
)
SELECT table_key, snapshot_id, made_current_at FROM ranked WHERE position = 1
ORDER BY table_key
