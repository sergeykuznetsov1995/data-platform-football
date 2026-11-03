SELECT match_id,
       CAST(to_unixtime(min(CASE WHEN event_status IN ({terminal}) AND lineups_status IN ({terminal})
                                  AND statistics_status IN ({terminal}) AND shotmap_status IN ({terminal})
                                  AND incidents_status IN ({terminal}) THEN _ingested_at END)) AS varchar),
       CAST(to_unixtime(min(_ingested_at)) AS varchar),
       max_by(event_status, _ingested_at), max_by(lineups_status, _ingested_at),
       max_by(statistics_status, _ingested_at), max_by(shotmap_status, _ingested_at),
       max_by(incidents_status, _ingested_at)
FROM bronze.sofascore_match_capture_status
WHERE match_id IN ({ids})
GROUP BY match_id
UNION ALL SELECT '__sentinel__', '', '', '', '', '', '', ''
