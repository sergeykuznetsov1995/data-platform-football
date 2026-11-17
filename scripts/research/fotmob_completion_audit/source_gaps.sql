SELECT *
FROM iceberg.bronze.fotmob_ingest_manifest FOR VERSION AS OF {manifest_snapshot}
WHERE target_type = 'scope_source_gap' AND status = 'not_available' AND parser_version = 'fotmob-native-v2'
ORDER BY target_key, completed_at, batch_id
