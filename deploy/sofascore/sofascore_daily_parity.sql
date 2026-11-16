-- Read-only семидейный отчёт Bronze-паритета daily/all-men (#1370).
-- any_complete (#1366): canonical match×endpoint journal semantics — последний
-- статус iceberg.ops.sofascore_capture_manifest по каждому из 5 core endpoints;
-- complete ⇔ все пять в collected/source_404; missing/retryable/quarantine —
-- incomplete. Compatibility-проекция capture-status источником any_complete
-- НЕ является: both_complete — informational physical overlap (CTE compat_both),
-- на any_complete/covered/missing не влияет.
-- Placeholders: {tournament_ids} — source IDs enabled-реестра; {since}/{until} — epoch-секунды окна [start, start+7).
-- Ровно 5 колонок: day, match_id, status_type, any_complete, both_complete; последняя строка — __sentinel__.
WITH schedule_rows AS (
    SELECT CAST(from_unixtime(CAST(start_timestamp AS bigint)) AS date) AS day,
           CAST(game_id AS varchar) AS match_id,
           coalesce(status_type, 'unknown') AS status_type,
           _ingested_at AS _ingested_at,
           league AS league
    FROM bronze.sofascore_schedule
    WHERE start_timestamp >= {since}
      AND start_timestamp < {until}
      AND coalesce(
              CAST(tournament_unique_tournament_id AS bigint),
              CAST(nullif(regexp_extract(league, '^SS-([0-9]+)', 1), '') AS bigint)
          ) IN ({tournament_ids})
),
schedule AS (
    SELECT day,
           match_id,
           max_by(status_type, _ingested_at) AS status_type
    FROM schedule_rows
    GROUP BY 1, 2
),
schedule_identity AS (
    -- Как endpoint_journal: ENG/SS объединяются, два разных SS ID fail closed.
    SELECT match_id,
           count(DISTINCT CASE WHEN league LIKE 'SS-%' THEN league END) AS ss_id_count
    FROM schedule_rows
    GROUP BY 1
),
manifest_core AS (
    -- Тот же срез manifest, что в sofascore_endpoint_journal.sql: event-targets,
    -- 5 core endpoints, только матчи окна.
    SELECT CAST(m.target_id AS varchar) AS match_id,
           m.endpoint AS endpoint,
           m.status AS status,
           m.http_status AS http_status,
           m.updated_at AS updated_at,
           m.run_id AS run_id,
           m.freshness_key AS freshness_key
    FROM iceberg.ops.sofascore_capture_manifest AS m
    WHERE m.target_type = 'event'
      AND m.endpoint IN ('event', 'lineups', 'statistics', 'shotmap', 'incidents')
      AND CAST(m.target_id AS varchar) IN (SELECT s.match_id FROM schedule s)
),
manifest_ranked AS (
    -- Последнее наблюдение на (match_id, endpoint): порядок дедупликации тот же,
    -- что в sofascore_endpoint_journal.sql.
    SELECT mc.match_id AS match_id,
           mc.endpoint AS endpoint,
           mc.status AS status,
           mc.http_status AS http_status,
           ROW_NUMBER() OVER (
               PARTITION BY mc.match_id, mc.endpoint
               ORDER BY mc.updated_at DESC, mc.run_id DESC, mc.freshness_key DESC
           ) AS rn
    FROM manifest_core AS mc
),
journal AS (
    -- Mapping rules идентичны sofascore_endpoint_journal.sql.
    SELECT match_id,
           endpoint,
           CASE
               WHEN status IN ('success', 'legitimate_empty') THEN 'collected'
               WHEN status = 'not_supported' AND http_status = 404 THEN 'source_404'
               WHEN status = 'schema_error' THEN 'quarantine'
               WHEN status IN ('not_supported', 'retryable_failure') THEN 'not_collected'
               ELSE 'invalid'
           END AS journal_status
    FROM manifest_ranked
    WHERE rn = 1
),
capture AS (
    -- Canonical completeness (= endpoint_journal.summarize_journal): complete ⇔
    -- ровно 5 core endpoints и все терминальны; partial/missing/quarantine → 0.
    SELECT match_id,
           CASE WHEN count(*) = 5
                 AND bool_and(journal_status IN ('collected', 'source_404'))
                THEN 1 ELSE 0 END AS any_complete
    FROM journal
    GROUP BY 1
),
compat_both AS (
    -- INFORMATIONAL ONLY (#1366): physical overlap обеих lane из compatibility-
    -- проекции bronze.sofascore_match_capture_status; единственный потребитель —
    -- both_complete. any_complete/covered/missing этот CTE не потребляют.
    SELECT match_id,
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
       CASE WHEN i.ss_id_count > 1 THEN 0
            ELSE coalesce(c.any_complete, 0) END AS any_complete,
       coalesce(cb.ss_complete, 0) * coalesce(cb.other_complete, 0) AS both_complete
FROM schedule s
LEFT JOIN schedule_identity i ON i.match_id = s.match_id
LEFT JOIN capture c ON c.match_id = s.match_id
LEFT JOIN compat_both cb ON cb.match_id = s.match_id
UNION ALL SELECT '__sentinel__', NULL, NULL, NULL, NULL
