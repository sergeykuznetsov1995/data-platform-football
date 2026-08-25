-- Read-only, fail-closed gate for one exact FBref oversize diagnostic run.
--
-- Run only after the reviewed source run and the purpose-built immutable
-- exact-cohort diagnostic are terminal:
--   psql "$FBREF_CONTROL_DB_URI" \
--     --set=reviewed_source_run_id=<terminal-reviewed-source-control-uuid> \
--     --set=airflow_run_id=<exact-diagnostic-airflow-run-id> \
--     --file=docs/operations/sql/fbref_20260825_oversize_evidence_canary_gate.sql
--
-- PASS requires exact source-derived membership, a physically nonpublishing
-- 100/50/25 profile, and one successful HTTP-200 request bound to each intended
-- logical refresh. Any unintended target, missing target, 3xx, extra
-- attempt/request, foreign logical refresh, or publishing mode is NO-GO.
-- response_too_large is diagnostic RED even though its HTTP status is 200: the
-- gate prints the evidence and exits nonzero; it is never production success.

\set ON_ERROR_STOP on

\if :{?reviewed_source_run_id}
\else
DO $missing_source_run_id$
BEGIN
    RAISE EXCEPTION
        'FBref oversize diagnostic gate NO-GO: reviewed source run id is required';
END
$missing_source_run_id$;
\endif

\if :{?airflow_run_id}
\else
DO $missing_airflow_run_id$
BEGIN
    RAISE EXCEPTION
        'FBref oversize diagnostic gate NO-GO: airflow_run_id is required';
END
$missing_airflow_run_id$;
\endif

BEGIN TRANSACTION READ ONLY;

WITH source_run AS (
    SELECT run.*
    FROM fbref_control.crawl_run AS run
    WHERE run.run_id = :'reviewed_source_run_id'::uuid
      AND run.finished_at IS NOT NULL
      AND run.status IN ('succeeded', 'failed', 'cancelled')
),
expected AS (
    SELECT source_target.target_id
    FROM source_run
    JOIN fbref_control.run_target AS source_target
      ON source_target.run_id = source_run.run_id
    JOIN fbref_control.fetch_attempt AS source_attempt
      ON source_attempt.run_id = source_target.run_id
     AND source_attempt.target_id = source_target.target_id
     AND source_attempt.logical_refresh_id =
         source_target.logical_refresh_id
    WHERE source_target.status = 'failed'
      AND source_attempt.status = 'failed'
      AND source_attempt.error_class = 'response_too_large'
      AND source_attempt.http_status = 200
      AND source_attempt.http_request_count = 1
      AND (
          SELECT count(*)
          FROM fbref_control.fetch_attempt AS counted_source_attempt
          WHERE counted_source_attempt.run_id = source_target.run_id
            AND counted_source_attempt.target_id = source_target.target_id
      ) = 1
),
selected_run AS (
    SELECT run.*
    FROM fbref_control.crawl_run AS run
    WHERE run.metadata ->> 'airflow_run_id' = :'airflow_run_id'
      AND run.metadata ->> 'dag_id' = 'fbref_oversize_evidence_canary'
),
actual AS (
    SELECT target.target_id,
           target.ordinal,
           target.status AS target_status,
           target.logical_refresh_id AS intended_logical_refresh_id,
           count(attempt.attempt_id)::bigint AS attempt_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
           )::bigint AS intended_attempt_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.status = 'succeeded'
           )::bigint AS success_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.status = 'failed'
                 AND attempt.error_class = 'response_too_large'
           )::bigint AS oversize_count,
           COALESCE(sum(attempt.http_request_count), 0)::bigint
               AS request_count,
           COALESCE(sum(attempt.http_request_count) FILTER (
               WHERE attempt.logical_refresh_id = target.logical_refresh_id
           ), 0)::bigint AS intended_request_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.http_status = 200
           )::bigint AS http_200_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.http_status BETWEEN 300 AND 399
                  OR EXISTS (
                      SELECT 1
                      FROM unnest(attempt.http_status_history) AS status(code)
                      WHERE status.code BETWEEN 300 AND 399
                  )
           )::bigint AS http_3xx_count,
           count(attempt.attempt_id) FILTER (
               WHERE attempt.http_status_history = ARRAY[200]::integer[]
           )::bigint AS exact_200_history_count,
           COALESCE(sum(attempt.wire_bytes), 0)::bigint AS wire_bytes,
           COALESCE(sum(attempt.provider_billed_bytes), 0)::bigint
               AS provider_billed_bytes,
           jsonb_agg(jsonb_build_object(
               'attempt_id', attempt.attempt_id,
               'logical_refresh_id', attempt.logical_refresh_id,
               'status', attempt.status,
               'error_class', attempt.error_class,
               'error_message', attempt.error_message,
               'http_status', attempt.http_status,
               'http_request_count', attempt.http_request_count,
               'http_status_history', attempt.http_status_history,
               'decoded_bytes', attempt.decoded_bytes,
               'wire_bytes', attempt.wire_bytes,
               'provider_billed_bytes', attempt.provider_billed_bytes
           ) ORDER BY attempt.attempt_number) FILTER (
               WHERE attempt.attempt_id IS NOT NULL
           ) AS attempts
    FROM selected_run AS run
    JOIN fbref_control.run_target AS target
      ON target.run_id = run.run_id
    LEFT JOIN fbref_control.fetch_attempt AS attempt
      ON attempt.run_id = target.run_id
     AND attempt.target_id = target.target_id
    GROUP BY target.target_id,
             target.ordinal,
             target.status,
             target.logical_refresh_id
),
source_gate AS (
    SELECT (SELECT count(*) FROM source_run) = 1
       AND (SELECT count(*) FROM expected) > 0
       AND NOT EXISTS (
           SELECT 1
           FROM fbref_control.run_target AS source_target
           WHERE source_target.run_id = :'reviewed_source_run_id'::uuid
             AND source_target.status IN ('pending', 'leased', 'retry')
       ) AS passed
),
run_gate AS (
    SELECT count(*) = 1
       AND COALESCE(bool_and(
           run.run_type = 'current'
           AND run.finished_at IS NOT NULL
           AND run.status IN ('succeeded', 'failed')
           AND run.request_limit = 100
           AND run.byte_limit = 52428800
           AND NOT run.budget_exceeded
           AND run.requests_used <= run.request_limit
           AND run.bytes_used <= run.byte_limit
           AND run.metadata ->> 'execution_mode' = 'acceptance_nonpublishing'
           AND run.metadata ->> 'publication_eligible' = 'false'
           AND run.metadata ->> 'acceptance_profile' = 'true'
           AND run.metadata ->> 'acceptance_scope' = 'current'
           AND run.metadata ->> 'shard_size' = '25'
           AND NOT EXISTS (
               SELECT 1
               FROM fbref_control.publication_lock AS publication_lock
               WHERE publication_lock.owner_run_id = run.run_id
                 AND publication_lock.released_at IS NULL
           )
       ), false) AS passed
    FROM selected_run AS run
),
set_gate AS (
    SELECT NOT EXISTS (
               SELECT target_id FROM expected
               EXCEPT
               SELECT target_id FROM actual
           )
       AND NOT EXISTS (
               SELECT target_id FROM actual
               EXCEPT
               SELECT target_id FROM expected
           ) AS passed
),
attempt_gate AS (
    SELECT count(*) = (SELECT count(*) FROM expected)
       AND COALESCE(bool_and(
           actual.target_status = 'succeeded'
           AND actual.attempt_count = 1
           AND actual.intended_attempt_count = 1
           AND actual.success_count = 1
           AND actual.oversize_count = 0
           AND actual.request_count = 1
           AND actual.intended_request_count = 1
           AND actual.http_200_count = 1
           AND actual.http_3xx_count = 0
           AND actual.exact_200_history_count = 1
       ), false) AS passed
    FROM actual
),
decision AS (
    SELECT source_gate.passed
       AND run_gate.passed
       AND set_gate.passed
       AND attempt_gate.passed AS passed
    FROM source_gate
    CROSS JOIN run_gate
    CROSS JOIN set_gate
    CROSS JOIN attempt_gate
)
SELECT decision.passed,
       jsonb_build_object(
           'source_run_id', :'reviewed_source_run_id',
           'airflow_run_id', :'airflow_run_id',
           'expected_target_ids', (
               SELECT jsonb_agg(target_id ORDER BY target_id) FROM expected
           ),
           'run', (
               SELECT to_jsonb(run) - 'metadata'
                      || jsonb_build_object('metadata', run.metadata)
               FROM selected_run AS run
           ),
           'targets', COALESCE((
               SELECT jsonb_agg(to_jsonb(actual) ORDER BY actual.ordinal)
               FROM actual
           ), '[]'::jsonb)
       )::text AS evidence
FROM decision
\gset fbref_oversize_gate_

\echo :fbref_oversize_gate_evidence
\if :fbref_oversize_gate_passed
COMMIT;
\echo 'PASS: exact FBref oversize diagnostic completed without oversize'
\else
ROLLBACK;
\echo 'NO-GO: FBref oversize diagnostic is RED or contract-invalid'
DO $failed_oversize_diagnostic$
BEGIN
    RAISE EXCEPTION 'FBref oversize diagnostic NO-GO';
END
$failed_oversize_diagnostic$;
\endif
