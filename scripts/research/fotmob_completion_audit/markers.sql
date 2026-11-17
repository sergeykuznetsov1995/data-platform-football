WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY target_key ORDER BY completed_at DESC, run_id DESC, batch_id DESC
    ) AS position
    FROM iceberg.bronze.fotmob_ingest_manifest FOR VERSION AS OF {manifest_snapshot}
    WHERE target_type = 'scope_completion' AND parser_version = 'fotmob-native-v2'
)
SELECT * FROM ranked WHERE position = 1 ORDER BY target_key
