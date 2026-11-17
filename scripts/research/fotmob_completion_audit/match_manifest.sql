WITH bucket_rows AS (
    SELECT *, CASE WHEN ((parser_version = 'fotmob-native-v2' AND status IN ('success', 'not_modified', 'not_available'))
      OR (parser_version = 'fotmob-native-v1' AND status IN ('success', 'not_modified'))) THEN true ELSE false END AS eligible,
           CASE WHEN capabilities_json IS NOT NULL
                     AND TRY(CAST(json_parse(capabilities_json) AS map(varchar, json))) IS NULL
                THEN true ELSE false END AS malformed_capabilities
    FROM iceberg.bronze.fotmob_ingest_manifest FOR VERSION AS OF {manifest_snapshot}
    WHERE target_type = 'match'
      AND parser_version IN ('fotmob-native-v1', 'fotmob-native-v2')
      AND TRY_CAST(entity_id AS bigint) > 0
      AND mod(crc32(to_utf8(entity_id)), {bucket_count}) = {bucket}
), annotated AS (
    SELECT *,
           first_value(status) OVER (
               PARTITION BY entity_id ORDER BY completed_at DESC NULLS LAST,
               fetched_at DESC NULLS LAST, batch_id DESC
           ) AS latest_attempt_status,
           first_value(completed_at) OVER (
               PARTITION BY entity_id ORDER BY completed_at DESC NULLS LAST,
               fetched_at DESC NULLS LAST, batch_id DESC
           ) AS latest_attempt_completed_at,
           bool_or(malformed_capabilities AND eligible) OVER (PARTITION BY entity_id) AS ranking_unknown
    FROM bucket_rows
), ranked AS (
    SELECT *, row_number() OVER (PARTITION BY entity_id ORDER BY CASE WHEN parser_version='fotmob-native-v2' THEN 1 ELSE 0 END DESC,
            COALESCE(TRY(CAST(from_iso8601_timestamp(json_extract_scalar(capabilities_json,'$.source_observed_at')) AS timestamp(6))), fetched_at, completed_at) DESC,
            completed_at DESC, batch_id DESC) AS position
    FROM annotated WHERE eligible
)
SELECT entity_id, target_key, batch_id, parser_version, status, fetch_outcome,
       http_status, error_code, raw_uri, content_hash, fetched_at, completed_at,
       expected_counts_json, actual_counts_json, capabilities_json, run_id, ranking_unknown, latest_attempt_status, latest_attempt_completed_at,
       (latest_attempt_status NOT IN ('success', 'not_modified')
        AND latest_attempt_completed_at > completed_at) AS late_failure
FROM ranked WHERE position = 1 ORDER BY entity_id
