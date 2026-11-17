SELECT DISTINCT competition_id, source_season_key, _target_batch_id
FROM iceberg.bronze.fotmob_matches FOR VERSION AS OF {matches_snapshot}
WHERE {scope_predicate} AND {candidate_batch_predicate}
ORDER BY competition_id, source_season_key, _target_batch_id
