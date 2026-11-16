-- Расписание-only: окно finished + identity-колонки, без join к capture-status.
-- Закрытие считает veha1_measure.closures по каноническому журналу эндпоинтов
-- (sofascore_endpoint_journal.sql, capture_complete/closed_at), а не любая строка
-- bronze-таблицы статуса захвата.
WITH s AS (
  SELECT CAST(game_id AS varchar) AS gid,
         min(start_timestamp) AS st,
         max(CASE WHEN league LIKE 'SS-%' THEN league END) AS ss_league,
         max(CASE WHEN league NOT LIKE 'SS-%' THEN league END) AS reg_league,
         max(tournament_unique_tournament_id) AS tut
  FROM bronze.sofascore_schedule
  WHERE status_type = 'finished' AND start_timestamp >= {since} AND start_timestamp < {until}
  GROUP BY 1
)
SELECT s.gid, CAST(CAST(s.st AS bigint) AS varchar), s.ss_league, s.reg_league,
       CAST(CAST(s.tut AS bigint) AS varchar)
FROM s
UNION ALL SELECT '__sentinel__', '', '', '', ''
