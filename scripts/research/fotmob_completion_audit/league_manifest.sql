SELECT competition_id, source_season_key, entity_id, target_key, batch_id, parser_version, status, fetch_outcome,
       http_status, error_code, raw_uri, content_hash, fetched_at, completed_at,
       expected_counts_json, actual_counts_json, capabilities_json, run_id
FROM iceberg.bronze.fotmob_ingest_manifest FOR VERSION AS OF {manifest_snapshot}
WHERE target_type = 'league_season' AND {manifest_scope_predicate} AND ((parser_version = 'fotmob-native-v2' AND status IN ('success', 'not_modified', 'not_available'))
      OR (parser_version = 'fotmob-native-v1' AND status IN ('success', 'not_modified')))
ORDER BY competition_id, source_season_key, CASE WHEN parser_version='fotmob-native-v2' THEN 1 ELSE 0 END DESC,
            COALESCE(TRY(CAST(from_iso8601_timestamp(json_extract_scalar(capabilities_json,'$.source_observed_at')) AS timestamp(6))), fetched_at, completed_at) DESC,
            completed_at DESC, batch_id DESC
