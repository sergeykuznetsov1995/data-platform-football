-- Read-only семидневный отчёт Bronze-паритета daily/all-men (#1370).
-- Placeholders: {tournament_ids} — source IDs enabled-реестра; {since}/{until} — epoch-секунды окна [start, start+7).
-- Ровно 5 колонок: day, match_id, status_type, any_complete, both_complete; последняя строка — __sentinel__.
WITH schedule AS (
    SELECT CAST(from_unixtime(CAST(start_timestamp AS bigint)) AS date) AS day,
           CAST(game_id AS varchar) AS match_id,
           max_by(coalesce(status_type, 'unknown'), _ingested_at) AS status_type
    FROM bronze.sofascore_schedule
    WHERE start_timestamp >= {since}
      AND start_timestamp < {until}
      AND coalesce(
              CAST(tournament_unique_tournament_id AS bigint),
              CAST(nullif(regexp_extract(league, '^SS-([0-9]+)', 1), '') AS bigint)
          ) IN ({tournament_ids})
    GROUP BY 1, 2
),
capture AS (
    SELECT match_id,
           max(complete) AS any_complete,
           max(CASE WHEN is_ss THEN complete ELSE 0 END) AS ss_complete,
           max(CASE WHEN NOT is_ss THEN complete ELSE 0 END) AS other_complete
    FROM (
        SELECT CAST(match_id AS varchar) AS match_id,
               league LIKE 'SS-%' AS is_ss,
               CASE WHEN event_status IN ('success', 'not_available')
                     AND lineups_status IN ('success', 'not_available')
                     AND statistics_status IN ('success', 'not_available')
                     AND shotmap_status IN ('success', 'not_available')
                     AND incidents_status IN ('success', 'not_available')
                    THEN 1 ELSE 0 END AS complete
        FROM bronze.sofascore_match_capture_status
    ) AS capture_rows
    GROUP BY 1
)
SELECT CAST(s.day AS varchar) AS day,
       s.match_id,
       s.status_type,
       coalesce(c.any_complete, 0) AS any_complete,
       coalesce(c.ss_complete, 0) * coalesce(c.other_complete, 0) AS both_complete
FROM schedule s
LEFT JOIN capture c ON c.match_id = s.match_id
UNION ALL SELECT '__sentinel__', NULL, NULL, NULL, NULL
