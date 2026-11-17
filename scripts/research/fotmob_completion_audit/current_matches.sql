SELECT competition_id, source_season_key, match_id,
       finished, cancelled, postponed, awarded, utc_time,
       _parser_version AS parser_version, _observed_at, _ingested_at, _target_batch_id
FROM iceberg.bronze.fotmob_matches FOR VERSION AS OF {matches_snapshot}
WHERE {scope_predicate} AND {selected_batch_predicate} AND {match_predicate}
ORDER BY competition_id, source_season_key, match_id, _target_batch_id, _observed_at, _ingested_at
