-- Fail-closed, read-only acceptance gate for the exact FBref redirect canary.
--
-- Run only after the bounded 100/50/25 canary has finished, passing its exact
-- Airflow run id without SQL quoting:
--   psql "$FBREF_CONTROL_DB_URI" --set=airflow_run_id=manual__fbref_redirect_alias_canary_20260825T163000Z --file=docs/operations/sql/fbref_20260825_redirect_alias_canary_gate.sql
--
-- Exit 0 and PASS mean both repaired targets were actual ordinals 0/1 and each
-- produced exactly one successful attempt, one HTTP request, and one HTTP 200.
-- A missing/duplicate run, wrong profile/order/URL, retry, or any 3xx exits 4.

\set ON_ERROR_STOP on

\if :{?airflow_run_id}
\else
\echo 'NO-GO: set --set=airflow_run_id=<exact Airflow run id>'
DO $missing_airflow_run_id$
BEGIN
    RAISE EXCEPTION
        'FBref redirect alias canary NO-GO: airflow_run_id is required';
END
$missing_airflow_run_id$;
\endif

BEGIN TRANSACTION READ ONLY;

WITH expected(target_id, ordinal, canonical_url) AS (
    VALUES
        (
            'fbref:season:59:2026-2027',
            0::bigint,
            'https://fbref.com/en/comps/59/3'
        ),
        (
            'fbref:season:33:2026-2027',
            1::bigint,
            'https://fbref.com/en/comps/33/2'
        )
),
selected_run AS (
    SELECT run.*
    FROM fbref_control.crawl_run AS run
    WHERE run.metadata ->> 'airflow_run_id' = :'airflow_run_id'
      AND run.metadata ->> 'dag_id' = 'dag_ingest_fbref'
),
actual AS (
    -- ``fetch_attempt`` is only individually foreign-keyed to runs and
    -- frontier targets.  Count every selected run/target attempt first; the
    -- intended-refresh aggregates below separately prove the sole attempt is
    -- bound to this run_target's logical refresh identity.
    SELECT target.target_id,
           target.ordinal,
           target.status AS target_status,
           target.logical_refresh_id AS intended_logical_refresh_id,
           frontier.canonical_url,
           count(attempt.attempt_id)::bigint AS attempt_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
           )::bigint AS intended_attempt_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.status = 'succeeded'
           )::bigint AS success_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
                 AND attempt.status = 'succeeded'
           )::bigint AS intended_success_count,
           COALESCE(sum(attempt.http_request_count), 0)::bigint
               AS request_count,
           COALESCE(sum(attempt.http_request_count) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
           ), 0)::bigint AS intended_request_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.http_status = 200
           )::bigint AS http_200_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
                 AND attempt.http_status = 200
           )::bigint AS intended_http_200_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.http_status BETWEEN 300 AND 399
           )::bigint AS http_3xx_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
                 AND attempt.http_status BETWEEN 300 AND 399
           )::bigint AS intended_http_3xx_count,
           count(attempt.attempt_id) FILTER (
               WHERE cardinality(attempt.http_status_history) = 1
           )::bigint AS http_status_history_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
                 AND cardinality(attempt.http_status_history) = 1
           )::bigint AS intended_http_status_history_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.http_status_history = ARRAY[200]::integer[]
           )::bigint AS exact_200_history_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
                 AND attempt.http_status_history = ARRAY[200]::integer[]
           )::bigint AS intended_exact_200_history_count,
           count(attempt.attempt_id) FILTER (
               WHERE EXISTS (
                   SELECT 1
                   FROM unnest(attempt.http_status_history) AS status(code)
                   WHERE status.code BETWEEN 300 AND 399
               )
           )::bigint AS history_3xx_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
                 AND EXISTS (
                   SELECT 1
                   FROM unnest(attempt.http_status_history) AS status(code)
                   WHERE status.code BETWEEN 300 AND 399
               )
           )::bigint AS intended_history_3xx_count
    FROM selected_run AS run
    JOIN fbref_control.run_target AS target
      ON target.run_id = run.run_id
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.target_id = target.target_id
    LEFT JOIN fbref_control.fetch_attempt AS attempt
      ON attempt.run_id = target.run_id
     AND attempt.target_id = target.target_id
    WHERE target.target_id IN (
        SELECT expected.target_id
        FROM expected
    )
    GROUP BY target.target_id,
             target.ordinal,
             target.status,
             target.logical_refresh_id,
             frontier.canonical_url
),
run_gate AS (
    SELECT count(*) = 1
       AND COALESCE(bool_and(
           run.run_type = 'current'
           AND run.status = 'succeeded'
           AND run.request_limit = 100
           AND run.byte_limit = 52428800
           AND NOT run.budget_exceeded
           AND run.requests_used <= run.request_limit
           AND run.bytes_used <= run.byte_limit
           AND run.metadata ->> 'execution_mode' = 'canary_nonpublishing'
           AND run.metadata ->> 'publication_eligible' = 'false'
           AND run.metadata ->> 'shard_size' = '25'
       ), false) AS passed
    FROM selected_run AS run
),
target_gate AS (
    SELECT count(*) = 2
       AND COALESCE(bool_and(
           a.target_id IS NOT NULL
           AND a.ordinal IS NOT DISTINCT FROM e.ordinal
           AND a.canonical_url IS NOT DISTINCT FROM e.canonical_url
           AND a.target_status IS NOT DISTINCT FROM 'succeeded'
           AND a.attempt_count = 1
           AND a.intended_attempt_count = 1
           AND a.success_count = 1
           AND a.intended_success_count = 1
           AND a.request_count = 1
           AND a.intended_request_count = 1
           AND a.http_200_count = 1
           AND a.intended_http_200_count = 1
           AND a.http_3xx_count = 0
           AND a.intended_http_3xx_count = 0
           AND a.http_status_history_count = 1
           AND a.intended_http_status_history_count = 1
           AND a.exact_200_history_count = 1
           AND a.intended_exact_200_history_count = 1
           AND a.history_3xx_count = 0
           AND a.intended_history_3xx_count = 0
       ), false) AS passed
    FROM expected AS e
    LEFT JOIN actual AS a USING (target_id)
)
SELECT run_gate.passed AND target_gate.passed AS passed,
       jsonb_build_object(
           'run_count', (SELECT count(*) FROM selected_run),
           'runs', COALESCE((
               SELECT jsonb_agg(jsonb_build_object(
                   'run_id', run.run_id,
                   'status', run.status,
                   'request_limit', run.request_limit,
                   'byte_limit', run.byte_limit,
                   'execution_mode', run.metadata ->> 'execution_mode',
                   'publication_eligible',
                       run.metadata ->> 'publication_eligible',
                   'shard_size', run.metadata ->> 'shard_size'
               ) ORDER BY run.run_id)
               FROM selected_run AS run
           ), '[]'::jsonb),
           'targets', COALESCE((
               SELECT jsonb_agg(to_jsonb(actual) ORDER BY actual.ordinal)
               FROM actual
           ), '[]'::jsonb)
       )::text AS evidence
FROM run_gate
CROSS JOIN target_gate
\gset fbref_redirect_gate_

\echo :fbref_redirect_gate_evidence
\if :fbref_redirect_gate_passed
COMMIT;
\echo 'PASS: exact redirect alias canary'
\else
ROLLBACK;
\echo 'NO-GO: exact redirect alias canary failed'
DO $failed_redirect_alias_canary$
BEGIN
    RAISE EXCEPTION 'FBref redirect alias canary NO-GO';
END
$failed_redirect_alias_canary$;
\endif
