SELECT CAST(game_id AS varchar),
       CAST(CAST(min(start_timestamp) AS bigint) AS varchar),
       CAST(CAST(min(changes_change_timestamp) AS bigint) AS varchar),
       max(CASE WHEN league LIKE 'SS-%' THEN league END),
       max(CASE WHEN league LIKE 'SS-%' THEN season END),
       max(CASE WHEN league NOT LIKE 'SS-%' THEN league END),
       max(CASE WHEN league NOT LIKE 'SS-%' THEN season END),
       CAST(CAST(max(tournament_unique_tournament_id) AS bigint) AS varchar)
FROM bronze.sofascore_schedule
WHERE status_type = 'finished' AND start_timestamp >= {since} AND start_timestamp < {until}
GROUP BY game_id
UNION ALL SELECT '__sentinel__', '', '', '', '', '', '', ''
