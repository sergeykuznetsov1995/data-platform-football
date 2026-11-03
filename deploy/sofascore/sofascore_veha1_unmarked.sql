SELECT CAST(game_id AS varchar),
       max_by(status_type, _ingested_at),
       max(CASE WHEN league LIKE 'SS-%' THEN league END),
       max(CASE WHEN league NOT LIKE 'SS-%' THEN league END),
       CAST(CAST(max(tournament_unique_tournament_id) AS bigint) AS varchar)
FROM bronze.sofascore_schedule
WHERE start_timestamp >= {since} AND start_timestamp < {until}
GROUP BY game_id
HAVING max_by(status_type, _ingested_at) IN ({statuses})
UNION ALL SELECT '__sentinel__', '', '', '', ''
