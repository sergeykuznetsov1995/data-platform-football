SELECT competition_id, source_season_key, match_id,
       _parser_version AS parser_version, _observed_at, _ingested_at, _target_batch_id
FROM iceberg.bronze.fotmob_match_payloads FOR VERSION AS OF {payloads_snapshot}
WHERE {manifest_scope_predicate} AND {selected_batch_predicate} AND {match_id_predicate}
ORDER BY competition_id, source_season_key, match_id, _target_batch_id, _observed_at, _ingested_at
