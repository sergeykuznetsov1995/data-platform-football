SELECT entity_id, target_key, batch_id, parser_version, status, fetch_outcome,
       http_status, error_code, raw_uri, content_hash, fetched_at, completed_at,
       expected_counts_json, actual_counts_json, capabilities_json, run_id
FROM iceberg.bronze.fotmob_ingest_manifest FOR VERSION AS OF {manifest_snapshot}
WHERE target_type = 'match' AND {match_id_predicate}
  AND parser_version IN ('fotmob-native-v1', 'fotmob-native-v2')
ORDER BY entity_id, completed_at, fetched_at, batch_id
