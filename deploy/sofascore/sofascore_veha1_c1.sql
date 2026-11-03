WITH s AS (
  SELECT CAST(game_id AS varchar) AS gid,
         min(start_timestamp) AS st,
         max(CASE WHEN league LIKE 'SS-%' THEN league END) AS ss_league,
         max(CASE WHEN league NOT LIKE 'SS-%' THEN league END) AS reg_league,
         max(tournament_unique_tournament_id) AS tut
  FROM bronze.sofascore_schedule
  WHERE status_type = 'finished' AND start_timestamp >= {since} AND start_timestamp < {until}
  GROUP BY 1
),
c AS (SELECT CAST(match_id AS varchar) AS gid, min(_ingested_at) AS closed_at
      FROM bronze.sofascore_match_capture_status GROUP BY 1)
SELECT s.gid, CAST(CAST(s.st AS bigint) AS varchar), s.ss_league, s.reg_league,
       CAST(CAST(s.tut AS bigint) AS varchar), CAST(to_unixtime(c.closed_at) AS varchar)
FROM s LEFT JOIN c ON c.gid = s.gid
UNION ALL SELECT '__sentinel__', '', '', '', '', ''
