-- TERMINAL AUTHORITY BAKED — CANDIDATE NO-GO UNTIL INDEPENDENT REVIEW.
-- DO NOT EXECUTE OR PRESENT AS OPERATIONALLY APPROVED FROM THIS UNREVIEWED
-- CANDIDATE COMMIT.
--
-- Read-only, fail-closed gate for one exact FBref oversize diagnostic run.
-- The state, source UUID, and digest are baked from the saved terminal TSV and
-- must match the remediation and runner authorities. Operator-supplied source
-- UUIDs or digests cannot self-authorize this file. The retained UNREVIEWED
-- guard makes any future placeholder candidate abort before reading control
-- data.
--
-- After that reviewed commit, run only after the exact-cohort diagnostic is
-- terminal:
--   psql "$FBREF_CONTROL_DB_URI" \
--     --set=airflow_run_id=<exact-diagnostic-airflow-run-id> \
--     --file=docs/operations/sql/fbref_20260825_oversize_evidence_canary_gate.sql
--
-- PASS requires the independently rehashed source snapshot, exact baked
-- provenance in diagnostic metadata, exact source-derived membership, a
-- physically nonpublishing 100/50/25 profile, and one successful HTTP-200
-- request bound to each intended logical refresh. The attempt universe starts
-- directly from every fetch_attempt for the selected run, so an orphan attempt
-- is both evidence and NO-GO. Control-run requests_used must equal that full
-- attempt universe and the expected target count exactly. Any unintended or
-- missing target, 3xx, extra request (including bootstrap traffic),
-- foreign logical refresh, or publishing mode is NO-GO.
-- response_too_large is diagnostic RED even though its HTTP status is 200.

\set ON_ERROR_STOP on
\set fbref_oversize_authority_state REVIEWED
\set fbref_oversize_baked_source_run_id 94838bac-786a-5d59-99e4-f6a2b3f7971e
\set fbref_oversize_baked_snapshot_sha256 b114e1139c50857b2985ead5ef2f72083660fc75cc9d1e9466874959a77bd543

SELECT (
    :'fbref_oversize_authority_state' = 'REVIEWED'
    AND :'fbref_oversize_baked_source_run_id'::uuid
        <> '00000000-0000-0000-0000-000000000000'::uuid
    AND :'fbref_oversize_baked_snapshot_sha256' ~ '^[0-9a-f]{64}$'
    AND :'fbref_oversize_baked_snapshot_sha256'
        <> repeat('0', 64)
) AS fbref_oversize_authority_reviewed
\gset

\if :fbref_oversize_authority_reviewed
\else
DO $unreviewed_authority$
BEGIN
    RAISE EXCEPTION
        'FBref oversize authority is unreviewed; later reviewed commit required';
END
$unreviewed_authority$;
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
    WHERE run.run_id = :'fbref_oversize_baked_source_run_id'::uuid
      AND run.finished_at IS NOT NULL
      AND run.status IN ('succeeded', 'failed', 'cancelled')
),
source_snapshot AS (
    SELECT source_target.target_id,
           frontier.canonical_url,
           source_target.status AS target_status,
           source_attempt.status AS attempt_status,
           source_attempt.error_class,
           source_attempt.http_status,
           source_attempt.http_request_count,
           source_attempt.error_message
    FROM source_run
    JOIN fbref_control.run_target AS source_target
      ON source_target.run_id = source_run.run_id
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.target_id = source_target.target_id
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
source_snapshot_digest AS (
    SELECT count(*)::bigint AS source_count,
           count(DISTINCT target_id)::bigint AS distinct_source_count,
           encode(sha256(convert_to(COALESCE(string_agg(
               concat_ws(E'\t', target_id, canonical_url, target_status,
                         attempt_status, error_class, http_status::text,
                         http_request_count::text, error_message),
               E'\n' ORDER BY target_id), '') || E'\n', 'UTF8')), 'hex')
               AS snapshot_sha256
    FROM source_snapshot
),
expected AS (
    SELECT target_id
    FROM source_snapshot
),
selected_run AS (
    SELECT run.*
    FROM fbref_control.crawl_run AS run
    WHERE run.metadata ->> 'airflow_run_id' = :'airflow_run_id'
      AND run.metadata ->> 'dag_id' = 'fbref_oversize_evidence_canary'
),
diagnostic_targets AS (
    SELECT target.target_id,
           target.ordinal,
           target.status AS target_status,
           target.logical_refresh_id AS intended_logical_refresh_id
    FROM selected_run AS run
    JOIN fbref_control.run_target AS target
      ON target.run_id = run.run_id
),
all_attempts AS (
    SELECT attempt.attempt_id,
           attempt.target_id,
           attempt.logical_refresh_id AS attempt_logical_refresh_id,
           attempt.attempt_number,
           attempt.status,
           attempt.error_class,
           attempt.error_message,
           attempt.http_status,
           attempt.http_request_count,
           attempt.http_status_history,
           attempt.decoded_bytes,
           attempt.wire_bytes,
           attempt.provider_billed_bytes,
           diagnostic_targets.target_id AS registered_target_id,
           diagnostic_targets.intended_logical_refresh_id,
           expected.target_id AS expected_target_id
    FROM selected_run AS run
    JOIN fbref_control.fetch_attempt AS attempt
      ON attempt.run_id = run.run_id
    LEFT JOIN diagnostic_targets
      ON diagnostic_targets.target_id = attempt.target_id
    LEFT JOIN expected
      ON expected.target_id = attempt.target_id
),
target_evidence AS (
    SELECT diagnostic_targets.target_id,
           diagnostic_targets.ordinal,
           diagnostic_targets.target_status,
           diagnostic_targets.intended_logical_refresh_id,
           count(all_attempts.attempt_id)::bigint AS attempt_count,
           count(all_attempts.attempt_id) FILTER (
               WHERE all_attempts.attempt_logical_refresh_id =
                     diagnostic_targets.intended_logical_refresh_id
           )::bigint AS intended_attempt_count
    FROM diagnostic_targets
    LEFT JOIN all_attempts
      ON all_attempts.registered_target_id = diagnostic_targets.target_id
    GROUP BY diagnostic_targets.target_id,
             diagnostic_targets.ordinal,
             diagnostic_targets.target_status,
             diagnostic_targets.intended_logical_refresh_id
),
attempt_totals AS (
    SELECT count(*)::bigint AS attempt_count,
           COALESCE(sum(all_attempts.http_request_count), 0)::bigint
               AS request_count
    FROM all_attempts
),
source_gate AS (
    SELECT (SELECT count(*) FROM source_run) = 1
       AND digest.source_count > 0
       AND digest.source_count = digest.distinct_source_count
       AND digest.snapshot_sha256 =
           :'fbref_oversize_baked_snapshot_sha256'
       AND NOT EXISTS (
           SELECT 1
           FROM fbref_control.run_target AS source_target
           WHERE source_target.run_id =
                 :'fbref_oversize_baked_source_run_id'::uuid
             AND source_target.status IN ('pending', 'leased', 'retry')
       ) AS passed
    FROM source_snapshot_digest AS digest
),
run_gate AS (
    SELECT count(*) = 1
       AND COALESCE(bool_and(
           run.run_type = 'current'
           AND run.finished_at IS NOT NULL
           AND run.status = 'succeeded'
           AND run.request_limit = 100
           AND run.byte_limit = 52428800
           AND NOT run.budget_exceeded
           AND run.requests_used = attempt_totals.request_count
           AND attempt_totals.request_count =
               (SELECT count(*) FROM expected)
           AND run.requests_used <= run.request_limit
           AND run.bytes_used <= run.byte_limit
           AND run.metadata ->> 'execution_mode' =
               'acceptance_nonpublishing'
           AND run.metadata ->> 'publication_eligible' = 'false'
           AND run.metadata ->> 'acceptance_profile' = 'true'
           AND run.metadata ->> 'acceptance_scope' = 'current'
           AND run.metadata ->> 'shard_size' = '25'
           AND run.metadata ->> 'reviewed_source_run_id' =
               :'fbref_oversize_baked_source_run_id'
           AND run.metadata ->> 'reviewed_terminal_snapshot_sha256' =
               :'fbref_oversize_baked_snapshot_sha256'
           AND NOT EXISTS (
               SELECT 1
               FROM fbref_control.publication_lock AS publication_lock
               WHERE publication_lock.owner_run_id = run.run_id
                 AND publication_lock.released_at IS NULL
           )
       ), false) AS passed
    FROM selected_run AS run
    CROSS JOIN attempt_totals
),
set_gate AS (
    SELECT NOT EXISTS (
               SELECT target_id FROM expected
               EXCEPT
               SELECT target_id FROM diagnostic_targets
           )
       AND NOT EXISTS (
               SELECT target_id FROM diagnostic_targets
               EXCEPT
               SELECT target_id FROM expected
           ) AS passed
),
target_gate AS (
    SELECT count(*) = (SELECT count(*) FROM expected)
       AND COALESCE(bool_and(
           target_evidence.target_status = 'succeeded'
           AND target_evidence.attempt_count = 1
           AND target_evidence.intended_attempt_count = 1
       ), false) AS passed
    FROM target_evidence
),
attempt_gate AS (
    SELECT count(*) = (SELECT count(*) FROM expected)
       AND COALESCE(sum(all_attempts.http_request_count), 0) =
           (SELECT count(*) FROM expected)
       AND COALESCE(bool_and(
           all_attempts.registered_target_id IS NOT NULL
           AND all_attempts.expected_target_id IS NOT NULL
           AND all_attempts.attempt_logical_refresh_id =
               all_attempts.intended_logical_refresh_id
           AND all_attempts.attempt_number = 1
           AND all_attempts.status = 'succeeded'
           AND all_attempts.error_class IS NULL
           AND all_attempts.error_message IS NULL
           AND all_attempts.http_status = 200
           AND all_attempts.http_request_count = 1
           AND all_attempts.http_status_history = ARRAY[200]::integer[]
       ), false) AS passed
    FROM all_attempts
),
decision AS (
    SELECT source_gate.passed AS source_passed,
           run_gate.passed AS run_passed,
           set_gate.passed AS set_passed,
           target_gate.passed AS target_passed,
           attempt_gate.passed AS attempt_passed,
           source_gate.passed
               AND run_gate.passed
               AND set_gate.passed
               AND target_gate.passed
               AND attempt_gate.passed AS passed
    FROM source_gate
    CROSS JOIN run_gate
    CROSS JOIN set_gate
    CROSS JOIN target_gate
    CROSS JOIN attempt_gate
)
SELECT decision.passed,
       jsonb_build_object(
           'source_run_id', :'fbref_oversize_baked_source_run_id',
           'reviewed_snapshot_sha256',
               :'fbref_oversize_baked_snapshot_sha256',
           'recomputed_snapshot_sha256', (
               SELECT snapshot_sha256 FROM source_snapshot_digest
           ),
           'airflow_run_id', :'airflow_run_id',
           'gates', jsonb_build_object(
               'source', decision.source_passed,
               'run', decision.run_passed,
               'set', decision.set_passed,
               'target', decision.target_passed,
               'attempt', decision.attempt_passed
           ),
           'expected_target_ids', COALESCE((
               SELECT jsonb_agg(target_id ORDER BY target_id) FROM expected
           ), '[]'::jsonb),
           'run', (
               SELECT to_jsonb(run) - 'metadata'
                      || jsonb_build_object('metadata', run.metadata)
               FROM selected_run AS run
           ),
           'request_conservation', (
               SELECT jsonb_build_object(
                   'attempt_count', attempt_totals.attempt_count,
                   'attempt_request_count', attempt_totals.request_count,
                   'run_requests_used', (
                       SELECT run.requests_used FROM selected_run AS run
                   )
               )
               FROM attempt_totals
           ),
           'targets', COALESCE((
               SELECT jsonb_agg(
                   to_jsonb(target_evidence)
                   ORDER BY target_evidence.ordinal
               )
               FROM target_evidence
           ), '[]'::jsonb),
           'all_attempts', COALESCE((
               SELECT jsonb_agg(
                   to_jsonb(all_attempts)
                   ORDER BY all_attempts.target_id,
                            all_attempts.attempt_number,
                            all_attempts.attempt_id
               )
               FROM all_attempts
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
