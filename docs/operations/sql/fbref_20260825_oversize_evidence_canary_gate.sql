-- TERMINAL AUTHORITY BAKED — CANDIDATE NO-GO UNTIL INDEPENDENT REVIEW.
-- DO NOT EXECUTE OR PRESENT AS OPERATIONALLY APPROVED FROM THIS UNREVIEWED
-- CANDIDATE COMMIT.
--
-- Read-only, fail-closed gate for one exact FBref oversize diagnostic run.
-- The terminal source authority remains the exact saved four-row snapshot.
-- After the separately accepted current-season remediation, only the two
-- genuinely-current competition-569 pages belong to the diagnostic cohort.
--
-- Run only after that exact2 diagnostic is terminal:
--   psql "$FBREF_CONTROL_DB_URI" \
--     --set=airflow_run_id=<exact-diagnostic-airflow-run-id> \
--     --file=docs/operations/sql/fbref_20260825_oversize_evidence_canary_gate.sql
--
-- PASS requires one successful target HTTP request per intended logical
-- refresh. Browser-clearance requests are valid traffic, but every request
-- must be conserved through clearance_session_page_accounting, its session,
-- the settled budget rows, and crawl_run.requests_used. Orphan attempts/pages,
-- missing or extra budgets/sessions, a foreign logical refresh, a false-current
-- scope regression, 3xx, or any publishing state is evidence and NO-GO.
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
    AND :'fbref_oversize_baked_snapshot_sha256' <> repeat('0', 64)
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

WITH source_authority_expected (
    target_id, canonical_url, target_status, attempt_status, error_class,
    http_status, http_request_count, error_message
) AS (
    VALUES
    (
        'fbref:season_stats:6:2022:playingtime',
        'https://fbref.com/en/comps/6/2022/playingtime/2022-WCQ----UEFA-M-Stats',
        'failed', 'failed', 'response_too_large', 200, 1::bigint,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/6/2022/playingtime/2022-WCQ----UEFA-M-Stats'
    ),
    (
        'fbref:season_stats:569:2025-2026:playingtime',
        'https://fbref.com/en/comps/569/playingtime/Copa-del-Rey-Stats',
        'failed', 'failed', 'response_too_large', 200, 1::bigint,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/569/playingtime/Copa-del-Rey-Stats'
    ),
    (
        'fbref:season_stats:569:2025-2026:standard',
        'https://fbref.com/en/comps/569/stats/Copa-del-Rey-Stats',
        'failed', 'failed', 'response_too_large', 200, 1::bigint,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/569/stats/Copa-del-Rey-Stats'
    ),
    (
        'fbref:season_stats:678:2021:playingtime',
        'https://fbref.com/en/comps/678/2021/playingtime/2021-UEFA-Euro-qualification-Stats',
        'failed', 'failed', 'response_too_large', 200, 1::bigint,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/678/2021/playingtime/2021-UEFA-Euro-qualification-Stats'
    )
),
fbref_20260825_oversize_diagnostic_expected (target_id) AS (
    VALUES
        ('fbref:season_stats:569:2025-2026:playingtime'::text),
        ('fbref:season_stats:569:2025-2026:standard'::text)
),
fbref_20260825_oversize_diagnostic_current_expected (
    competition_id, season_id, target_id
) AS (
    VALUES
        ('569'::text, '2025-2026'::text,
         'fbref:season_stats:569:2025-2026:playingtime'::text),
        ('569'::text, '2025-2026'::text,
         'fbref:season_stats:569:2025-2026:standard'::text)
),
demoted_expected (competition_id, season_id, target_id) AS (
    VALUES
        ('6'::text, '2022'::text,
         'fbref:season_stats:6:2022:playingtime'::text),
        ('678'::text, '2021'::text,
         'fbref:season_stats:678:2021:playingtime'::text)
),
source_run AS (
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
     AND source_attempt.logical_refresh_id = source_target.logical_refresh_id
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
selected_run AS (
    SELECT run.*
    FROM fbref_control.crawl_run AS run
    WHERE run.metadata ->> 'airflow_run_id' = :'airflow_run_id'
      AND run.metadata ->> 'dag_id' = 'fbref_oversize_evidence_canary'
),
demoted_actual AS (
    SELECT expected.competition_id,
           expected.season_id,
           expected.target_id
    FROM demoted_expected AS expected
    JOIN fbref_control.season_registry AS season
      ON season.source = 'fbref'
     AND season.competition_id = expected.competition_id
     AND season.season_id = expected.season_id
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.target_id = expected.target_id
    WHERE season.present
      AND season.lifecycle_state = 'present'
      AND NOT season.is_current
      AND frontier.source = 'fbref'
      AND frontier.page_kind = 'season_stats'
      AND frontier.refresh_policy = 'daily'
      AND frontier.state = 'quarantined'
      AND frontier.next_fetch_at IS NULL
      AND frontier.last_error_class = 'ScopeQuarantined'
      AND frontier.last_error_message = 'noncurrent_season'
),
fbref_20260825_oversize_diagnostic_current_actual AS (
    SELECT expected.competition_id,
           expected.season_id,
           expected.target_id
    FROM fbref_20260825_oversize_diagnostic_current_expected AS expected
    JOIN fbref_control.season_registry AS season
      ON season.source = 'fbref'
     AND season.competition_id = expected.competition_id
     AND season.season_id = expected.season_id
    JOIN fbref_control.page_frontier AS frontier
      ON frontier.target_id = expected.target_id
    WHERE season.present
      AND season.lifecycle_state = 'present'
      AND season.is_current
      AND frontier.source = 'fbref'
      AND frontier.page_kind = 'season_stats'
      AND frontier.refresh_policy = 'daily'
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
           attempt.reservation_id AS attempt_reservation_id,
           attempt.status,
           attempt.error_class,
           attempt.error_message,
           attempt.http_status,
           attempt.http_request_count,
           attempt.http_status_history,
           attempt.content_hash,
           attempt.raw_manifest_key,
           attempt.decoded_bytes,
           attempt.compressed_bytes,
           attempt.wire_bytes,
           attempt.provider_billed_bytes,
           attempt.transport_version,
           attempt.session_version,
           diagnostic_targets.target_id AS registered_target_id,
           diagnostic_targets.intended_logical_refresh_id,
           diagnostic_targets.ordinal AS target_ordinal,
           expected.target_id AS expected_target_id
    FROM selected_run AS run
    JOIN fbref_control.fetch_attempt AS attempt
      ON attempt.run_id = run.run_id
    LEFT JOIN diagnostic_targets
      ON diagnostic_targets.target_id = attempt.target_id
    LEFT JOIN fbref_20260825_oversize_diagnostic_expected AS expected
      ON expected.target_id = attempt.target_id
),
all_pages AS (
    SELECT page.*,
           all_attempts.attempt_id AS mapped_attempt_id,
           all_attempts.attempt_reservation_id,
           all_attempts.attempt_logical_refresh_id,
           all_attempts.session_version AS attempt_session_version,
           all_attempts.target_ordinal,
           all_attempts.expected_target_id,
           all_attempts.registered_target_id,
           all_attempts.content_hash AS attempt_content_hash,
           all_attempts.raw_manifest_key AS attempt_raw_manifest_key,
           all_attempts.decoded_bytes AS attempt_decoded_bytes,
           all_attempts.compressed_bytes AS attempt_compressed_bytes,
           all_attempts.wire_bytes AS attempt_wire_bytes,
           all_attempts.provider_billed_bytes
               AS attempt_provider_billed_bytes,
           encode(sha256(convert_to(concat(
               '{"attempt_id":"', page.attempt_id::text,
               '","browser_asset_bytes":', page.browser_asset_bytes::text,
               ',"browser_bootstrap_attempts":',
                   page.browser_bootstrap_attempts::text,
               ',"browser_bootstrap_requests":',
                   page.browser_bootstrap_requests::text,
               ',"browser_document_bytes":',
                   page.browser_document_bytes::text,
               ',"browser_unobserved_bytes":',
                   page.browser_unobserved_bytes::text,
               ',"compressed_raw_bytes":', page.compressed_raw_bytes::text,
               ',"decoded_html_bytes":', page.decoded_html_bytes::text,
               ',"http_requests":', page.http_requests::text,
               ',"http_wire_bytes":', page.http_wire_bytes::text,
               ',"provider_billed_bytes":',
                   page.provider_billed_bytes::text,
               ',"requests_used":', page.requests_used::text,
               ',"reservation_id":"', page.reservation_id::text,
               '","session_id":"', page.session_id::text, '"}'
           ), 'UTF8')), 'hex') AS recomputed_evidence_sha256
    FROM selected_run AS run
    JOIN fbref_control.clearance_session_page_accounting AS page
      ON page.run_id = run.run_id
    LEFT JOIN all_attempts
      ON page.attempt_id = all_attempts.attempt_id
),
session_page_totals AS (
    SELECT page.session_id,
           count(*)::bigint AS page_count,
           COALESCE(sum(page.requests_used), 0)::bigint AS requests_used,
           COALESCE(sum(page.browser_bootstrap_attempts), 0)::bigint
               AS browser_bootstrap_attempts,
           COALESCE(sum(page.browser_bootstrap_requests), 0)::bigint
               AS browser_bootstrap_requests,
           COALESCE(sum(page.browser_document_bytes), 0)::bigint
               AS browser_document_bytes,
           COALESCE(sum(page.browser_asset_bytes), 0)::bigint
               AS browser_asset_bytes,
           COALESCE(sum(page.browser_unobserved_bytes), 0)::bigint
               AS browser_unobserved_bytes,
           COALESCE(sum(page.http_requests), 0)::bigint AS http_requests,
           COALESCE(sum(page.http_wire_bytes), 0)::bigint
               AS http_wire_bytes,
           COALESCE(sum(page.decoded_html_bytes), 0)::bigint
               AS decoded_html_bytes,
           COALESCE(sum(page.compressed_raw_bytes), 0)::bigint
               AS compressed_raw_bytes,
           COALESCE(sum(page.provider_billed_bytes), 0)::bigint
               AS page_provider_bytes
    FROM all_pages AS page
    GROUP BY page.session_id
),
all_sessions AS (
    SELECT session.session_id,
           session.status,
           session.browser_bootstrap_attempts,
           session.browser_bootstrap_requests,
           session.browser_document_bytes,
           session.browser_asset_bytes,
           session.browser_unobserved_bytes,
           session.http_requests,
           session.http_wire_bytes,
           session.decoded_html_bytes,
           session.compressed_raw_bytes,
           session.provider_billed_bytes,
           tail.status AS tail_status,
           tail.session_id AS tail_session_id,
           tail.reservation_id AS tail_reservation_id,
           tail.baseline_provider_bytes AS tail_baseline_provider_bytes,
           tail.bytes_reserved AS tail_bytes_reserved,
           tail.page_provider_bytes AS tail_page_provider_bytes,
           tail.authoritative_provider_bytes
               AS tail_authoritative_provider_bytes,
           tail.tail_provider_bytes,
           tail.settlement_sha256 AS tail_settlement_sha256,
           CASE WHEN tail.session_id IS NULL THEN NULL ELSE
               encode(sha256(convert_to(concat(
                   '{"authoritative_provider_bytes":',
                       tail.authoritative_provider_bytes::text,
                   ',"baseline_provider_bytes":',
                       tail.baseline_provider_bytes::text,
                   ',"meter":"proxy_filter_provider_path_v2"',
                   ',"page_provider_bytes":',
                       tail.page_provider_bytes::text,
                   ',"session_id":"', tail.session_id::text,
                   '","tail_provider_bytes":',
                       tail.tail_provider_bytes::text, '}'
               ), 'UTF8')), 'hex')
           END AS recomputed_tail_settlement_sha256,
           tail_budget.status AS tail_budget_status,
           tail_budget.logical_refresh_id AS tail_budget_logical_refresh_id,
           tail_budget.requests_reserved AS tail_budget_requests_reserved,
           tail_budget.bytes_reserved AS tail_budget_bytes_reserved,
           tail_budget.requests_used AS tail_budget_requests_used,
           tail_budget.bytes_used AS tail_budget_bytes_used,
           COALESCE(page_totals.page_count, 0)::bigint AS page_count,
           COALESCE(page_totals.requests_used, 0)::bigint AS page_requests,
           COALESCE(page_totals.browser_bootstrap_attempts, 0)::bigint
               AS page_browser_bootstrap_attempts,
           COALESCE(page_totals.browser_bootstrap_requests, 0)::bigint
               AS page_browser_bootstrap_requests,
           COALESCE(page_totals.browser_document_bytes, 0)::bigint
               AS page_browser_document_bytes,
           COALESCE(page_totals.browser_asset_bytes, 0)::bigint
               AS page_browser_asset_bytes,
           COALESCE(page_totals.browser_unobserved_bytes, 0)::bigint
               AS page_browser_unobserved_bytes,
           COALESCE(page_totals.http_requests, 0)::bigint
               AS page_http_requests,
           COALESCE(page_totals.http_wire_bytes, 0)::bigint
               AS page_http_wire_bytes,
           COALESCE(page_totals.decoded_html_bytes, 0)::bigint
               AS page_decoded_html_bytes,
           COALESCE(page_totals.compressed_raw_bytes, 0)::bigint
               AS page_compressed_raw_bytes,
           COALESCE(page_totals.page_provider_bytes, 0)::bigint
               AS page_provider_bytes
    FROM selected_run AS run
    JOIN fbref_control.clearance_session AS session
      ON session.run_id = run.run_id
    LEFT JOIN session_page_totals AS page_totals
      ON page_totals.session_id = session.session_id
    LEFT JOIN fbref_control.clearance_session_tail_reservation AS tail
      ON tail.session_id = session.session_id
     AND tail.run_id = session.run_id
    LEFT JOIN fbref_control.budget_reservation AS tail_budget
      ON tail_budget.reservation_id = tail.reservation_id
     AND tail_budget.run_id = session.run_id
),
all_budgets AS (
    SELECT budget.reservation_id,
           budget.logical_refresh_id,
           budget.requests_reserved,
           budget.bytes_reserved,
           budget.status,
           budget.requests_used,
           budget.bytes_used,
           page.reservation_id AS page_reservation_id,
           page.attempt_reservation_id AS page_attempt_reservation_id,
           page.attempt_logical_refresh_id AS page_logical_refresh_id,
           page.target_ordinal AS page_target_ordinal,
           page.requests_used AS page_requests_used,
           page.provider_billed_bytes AS page_provider_billed_bytes,
           tail.reservation_id AS tail_reservation_id,
           tail.session_id AS tail_session_id,
           budget.logical_refresh_id AS tail_logical_refresh_id,
           tail.bytes_reserved AS tail_bytes_reserved,
           tail.tail_provider_bytes
    FROM selected_run AS run
    JOIN fbref_control.budget_reservation AS budget
      ON budget.run_id = run.run_id
    LEFT JOIN all_pages AS page
      ON page.reservation_id = budget.reservation_id
    LEFT JOIN fbref_control.clearance_session_tail_reservation AS tail
      ON tail.reservation_id = budget.reservation_id
     AND tail.run_id = run.run_id
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
traffic_totals AS (
    SELECT count(*)::bigint AS page_count,
           COALESCE(sum(page.requests_used), 0)::bigint
               AS page_request_count,
           COALESCE(sum(page.http_requests), 0)::bigint
               AS http_request_count,
           COALESCE(sum(page.browser_bootstrap_attempts), 0)::bigint
               AS browser_bootstrap_attempt_count,
           COALESCE(sum(page.browser_bootstrap_requests), 0)::bigint
               AS browser_request_count,
           count(*) FILTER (
               WHERE page.mapped_attempt_id IS NULL
                  OR page.registered_target_id IS NULL
                  OR page.expected_target_id IS NULL
                  OR page.attempt_reservation_id IS NULL
                  OR page.attempt_reservation_id <> page.reservation_id
                  OR NULLIF(page.attempt_session_version, '') IS NULL
                  OR page.attempt_session_version <> page.session_id::text
                  OR page.http_requests <> 1
                  OR page.requests_used < 1
                  OR page.requests_used <>
                     page.http_requests + page.browser_bootstrap_requests
                  OR page.browser_bootstrap_attempts >
                     page.browser_bootstrap_requests
                  OR NULLIF(page.attempt_content_hash, '') IS NULL
                  OR NULLIF(page.attempt_raw_manifest_key, '') IS NULL
                  OR page.attempt_decoded_bytes IS NULL
                  OR page.attempt_decoded_bytes <> page.decoded_html_bytes
                  OR page.attempt_compressed_bytes IS NULL
                  OR page.attempt_compressed_bytes <>
                     page.compressed_raw_bytes
                  OR page.attempt_wire_bytes IS NULL
                  OR page.attempt_wire_bytes <> page.http_wire_bytes
                  OR page.attempt_provider_billed_bytes IS NULL
                  OR page.attempt_provider_billed_bytes <>
                     page.provider_billed_bytes
                  OR NULLIF(page.evidence_sha256, '') IS NULL
                  OR page.evidence_sha256 !~ '^[0-9a-f]{64}$'
                  OR page.evidence_sha256 <>
                     page.recomputed_evidence_sha256
                  OR page.decoded_html_bytes <= 4194304
                  OR page.decoded_html_bytes > 8388608
                  OR page.compressed_raw_bytes <= 0
                  OR page.http_wire_bytes <= 0
           )::bigint AS invalid_page_count
    FROM all_pages AS page
),
session_totals AS (
    SELECT count(*)::bigint AS session_count,
           COALESCE(sum(session.browser_bootstrap_attempts), 0)::bigint
               AS browser_bootstrap_attempt_count,
           COALESCE(sum(session.browser_bootstrap_requests), 0)::bigint
               AS browser_request_count,
           COALESCE(sum(session.http_requests), 0)::bigint
               AS http_request_count,
           count(*) FILTER (
               WHERE session.status <> 'closed'
                  OR session.tail_status <> 'settled'
                  OR session.provider_billed_bytes IS NULL
                  OR session.tail_session_id IS NULL
                  OR session.tail_reservation_id IS NULL
                  OR session.tail_baseline_provider_bytes <> 0
                  OR session.tail_bytes_reserved <> 9437184
                  OR session.tail_page_provider_bytes IS NULL
                  OR session.tail_authoritative_provider_bytes IS NULL
                  OR session.tail_provider_bytes IS NULL
                  OR NULLIF(session.tail_settlement_sha256, '') IS NULL
                  OR session.tail_settlement_sha256 !~ '^[0-9a-f]{64}$'
                  OR session.tail_settlement_sha256 <>
                     session.recomputed_tail_settlement_sha256
                  OR session.tail_budget_status <> 'settled'
                  OR session.tail_budget_logical_refresh_id <>
                     session.tail_session_id
                  OR session.tail_budget_requests_reserved <> 0
                  OR session.tail_budget_bytes_reserved <> 9437184
                  OR session.tail_budget_requests_used <> 0
                  OR session.tail_budget_bytes_used <>
                     session.tail_provider_bytes
                  OR session.page_count = 0
                  OR session.browser_bootstrap_attempts <>
                     session.page_browser_bootstrap_attempts
                  OR session.browser_bootstrap_requests <>
                     session.page_browser_bootstrap_requests
                  OR session.browser_document_bytes <>
                     session.page_browser_document_bytes
                  OR session.browser_asset_bytes <>
                     session.page_browser_asset_bytes
                  OR session.browser_unobserved_bytes <>
                     session.page_browser_unobserved_bytes
                  OR session.browser_document_bytes < 0
                  OR session.browser_asset_bytes < 0
                  OR session.browser_unobserved_bytes < 0
                  OR session.browser_document_bytes
                     + session.browser_asset_bytes
                     + session.browser_unobserved_bytes > 4194304
                  OR session.http_requests <> session.page_http_requests
                  OR session.http_wire_bytes <>
                     session.page_http_wire_bytes
                  OR session.decoded_html_bytes <>
                     session.page_decoded_html_bytes
                  OR session.compressed_raw_bytes <>
                     session.page_compressed_raw_bytes
                  OR session.tail_page_provider_bytes <>
                     session.page_provider_bytes
                  OR session.tail_authoritative_provider_bytes <>
                     session.provider_billed_bytes
                  OR session.tail_authoritative_provider_bytes <>
                     session.tail_page_provider_bytes
                     + session.tail_provider_bytes
           )::bigint AS invalid_session_count
    FROM all_sessions AS session
),
budget_totals AS (
    SELECT count(*)::bigint AS budget_count,
           COALESCE(sum(budget.bytes_used), 0)::bigint AS bytes_used,
           count(*) FILTER (
               WHERE budget.status <> 'settled'
                  OR (budget.page_reservation_id IS NULL) =
                     (budget.tail_reservation_id IS NULL)
                  OR (
                      budget.page_reservation_id IS NOT NULL
                      AND (
                          budget.page_attempt_reservation_id <>
                              budget.reservation_id
                          OR budget.logical_refresh_id <>
                              budget.page_logical_refresh_id
                          OR budget.page_target_ordinal NOT IN (0, 1)
                          OR (
                              budget.page_target_ordinal = 0
                              AND (
                                  budget.requests_reserved <> 22
                                  OR budget.bytes_reserved <> 13631488
                              )
                          )
                          OR (
                              budget.page_target_ordinal = 1
                              AND (
                                  budget.requests_reserved <> 2
                                  OR budget.bytes_reserved <> 9437184
                              )
                          )
                          OR budget.requests_used > budget.requests_reserved
                          OR budget.bytes_used > budget.bytes_reserved
                          OR budget.requests_used <>
                              budget.page_requests_used
                          OR budget.bytes_used <>
                              budget.page_provider_billed_bytes
                      )
                  )
                  OR (
                      budget.tail_reservation_id IS NOT NULL
                      AND (
                          budget.tail_logical_refresh_id <>
                              budget.tail_session_id
                          OR budget.requests_reserved <> 0
                          OR budget.bytes_reserved <> 9437184
                          OR budget.tail_bytes_reserved <> 9437184
                          OR budget.requests_used <> 0
                          OR budget.bytes_used <>
                              budget.tail_provider_bytes
                      )
                  )
           )::bigint AS invalid_budget_count
    FROM all_budgets AS budget
),
source_gate AS (
    SELECT (SELECT count(*) FROM source_run) = 1
       AND digest.source_count = 4
       AND digest.source_count = digest.distinct_source_count
       AND digest.snapshot_sha256 =
           :'fbref_oversize_baked_snapshot_sha256'
       AND NOT EXISTS (
           SELECT * FROM source_authority_expected
           EXCEPT
           SELECT * FROM source_snapshot
       )
       AND NOT EXISTS (
           SELECT * FROM source_snapshot
           EXCEPT
           SELECT * FROM source_authority_expected
       )
       AND NOT EXISTS (
           SELECT 1
           FROM fbref_control.run_target AS source_target
           WHERE source_target.run_id =
                 :'fbref_oversize_baked_source_run_id'::uuid
             AND source_target.status IN ('pending', 'leased', 'retry')
       ) AS passed
    FROM source_snapshot_digest AS digest
),
demotion_gate AS (
    SELECT count(*) = 2
       AND NOT EXISTS (
           SELECT * FROM demoted_expected
           EXCEPT
           SELECT * FROM demoted_actual
       )
       AND NOT EXISTS (
           SELECT * FROM demoted_actual
           EXCEPT
           SELECT * FROM demoted_expected
       ) AS passed
    FROM demoted_actual
),
diagnostic_current_gate AS (
    SELECT count(*) = 2
       AND NOT EXISTS (
           SELECT *
           FROM fbref_20260825_oversize_diagnostic_current_expected
           EXCEPT
           SELECT *
           FROM fbref_20260825_oversize_diagnostic_current_actual
       )
       AND NOT EXISTS (
           SELECT *
           FROM fbref_20260825_oversize_diagnostic_current_actual
           EXCEPT
           SELECT *
           FROM fbref_20260825_oversize_diagnostic_current_expected
       ) AS passed
    FROM fbref_20260825_oversize_diagnostic_current_actual
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
           AND run.requests_reserved = 0
           AND run.bytes_reserved = 0
           AND run.requests_used = traffic_totals.page_request_count
           AND traffic_totals.page_request_count <= run.request_limit
           AND traffic_totals.page_request_count <= 22
           AND run.bytes_used = budget_totals.bytes_used
           AND run.bytes_used <= run.byte_limit
           AND run.bytes_used <= 39321600
           AND run.metadata ->> 'execution_mode' =
               'acceptance_nonpublishing'
           AND run.metadata ->> 'publication_eligible' = 'false'
           AND run.metadata ->> 'acceptance_profile' = 'true'
           AND run.metadata ->> 'acceptance_scope' = 'current'
           AND run.metadata ->> 'persistent_http_session' = 'true'
           AND run.metadata ->> 'browser_request_limit' = '20'
           AND run.metadata ->> 'browser_solve_limit' = '1'
           AND run.metadata ->> 'provider_dag_id' =
               'dag_accept_fbref_bronze'
           AND run.metadata ->> 'provider_task_id' =
               'oversize_evidence_fetch'
           AND run.metadata ->> 'provider_scope' = :'airflow_run_id'
           AND run.metadata ->> 'provider_run_id' = run.run_id::text
           AND run.metadata ->> 'provider_byte_limit' = '39321600'
           AND run.metadata ->> 'shard_size' = '25'
           AND run.metadata ->> 'request_reservation_bytes' = '9437184'
           AND run.metadata ->> 'reviewed_source_run_id' =
               :'fbref_oversize_baked_source_run_id'
           AND run.metadata ->> 'reviewed_terminal_snapshot_sha256' =
               :'fbref_oversize_baked_snapshot_sha256'
           AND run.metadata -> 'reviewed_diagnostic_target_ids' =
               jsonb_build_array(
                   'fbref:season_stats:569:2025-2026:playingtime',
                   'fbref:season_stats:569:2025-2026:standard'
               )
           AND NOT EXISTS (
               SELECT 1
               FROM fbref_control.publication_lock AS publication_lock
               WHERE publication_lock.owner_run_id = run.run_id
                 AND publication_lock.released_at IS NULL
           )
       ), false) AS passed
    FROM selected_run AS run
    CROSS JOIN traffic_totals
    CROSS JOIN budget_totals
),
set_gate AS (
    SELECT NOT EXISTS (
               SELECT target_id
               FROM fbref_20260825_oversize_diagnostic_expected
               EXCEPT
               SELECT target_id FROM diagnostic_targets
           )
       AND NOT EXISTS (
               SELECT target_id FROM diagnostic_targets
               EXCEPT
               SELECT target_id
               FROM fbref_20260825_oversize_diagnostic_expected
           ) AS passed
),
target_gate AS (
    SELECT count(*) = (
               SELECT count(*)
               FROM fbref_20260825_oversize_diagnostic_expected
           )
       AND COALESCE(bool_and(
           target_evidence.target_status = 'succeeded'
           AND target_evidence.attempt_count = 1
           AND target_evidence.intended_attempt_count = 1
       ), false) AS passed
    FROM target_evidence
),
attempt_gate AS (
    SELECT count(*) = (
               SELECT count(*)
               FROM fbref_20260825_oversize_diagnostic_expected
           )
       AND COALESCE(sum(all_attempts.http_request_count), 0) =
           (SELECT count(*)
            FROM fbref_20260825_oversize_diagnostic_expected)
       AND COALESCE(bool_and((
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
           AND all_attempts.content_hash ~ '^[0-9a-f]{64}$'
           AND all_attempts.raw_manifest_key =
               'manifests/fetches/' ||
               all_attempts.attempt_logical_refresh_id::text || '.json'
           AND all_attempts.transport_version =
               'fbref-camoufox-metered-warm-http-v10'
           AND NULLIF(all_attempts.session_version, '') IS NOT NULL
           AND all_attempts.decoded_bytes > 4194304
           AND all_attempts.decoded_bytes <= 8388608
           AND all_attempts.compressed_bytes > 0
           AND all_attempts.wire_bytes > 0
           AND all_attempts.provider_billed_bytes IS NOT NULL
       ) IS TRUE), false) AS passed
    FROM all_attempts
),
traffic_gate AS (
    SELECT traffic_totals.page_count = (
               SELECT count(*)
               FROM fbref_20260825_oversize_diagnostic_expected
           )
       AND traffic_totals.http_request_count = (
               SELECT count(*)
               FROM fbref_20260825_oversize_diagnostic_expected
           )
       AND traffic_totals.page_request_count >=
           traffic_totals.http_request_count
       AND traffic_totals.page_request_count <= 22
       AND traffic_totals.browser_request_count <= 20
       AND traffic_totals.browser_bootstrap_attempt_count <= 1
       AND traffic_totals.invalid_page_count = 0
       AND session_totals.session_count = 1
       AND session_totals.invalid_session_count = 0
       AND session_totals.http_request_count =
           traffic_totals.http_request_count
       AND session_totals.browser_request_count =
           traffic_totals.browser_request_count
       AND session_totals.browser_bootstrap_attempt_count =
           traffic_totals.browser_bootstrap_attempt_count
       AND budget_totals.budget_count =
           traffic_totals.page_count + session_totals.session_count
       AND budget_totals.invalid_budget_count = 0 AS passed
    FROM traffic_totals
    CROSS JOIN session_totals
    CROSS JOIN budget_totals
),
decision AS (
    SELECT source_gate.passed AS source_passed,
           demotion_gate.passed AS demotion_passed,
           diagnostic_current_gate.passed AS diagnostic_current_passed,
           run_gate.passed AS run_passed,
           set_gate.passed AS set_passed,
           target_gate.passed AS target_passed,
           attempt_gate.passed AS attempt_passed,
           traffic_gate.passed AS traffic_passed,
           source_gate.passed
               AND demotion_gate.passed
               AND diagnostic_current_gate.passed
               AND run_gate.passed
               AND set_gate.passed
               AND target_gate.passed
               AND attempt_gate.passed
               AND traffic_gate.passed AS passed
    FROM source_gate
    CROSS JOIN demotion_gate
    CROSS JOIN diagnostic_current_gate
    CROSS JOIN run_gate
    CROSS JOIN set_gate
    CROSS JOIN target_gate
    CROSS JOIN attempt_gate
    CROSS JOIN traffic_gate
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
               'demotion', decision.demotion_passed,
               'diagnostic_current', decision.diagnostic_current_passed,
               'run', decision.run_passed,
               'set', decision.set_passed,
               'target', decision.target_passed,
               'attempt', decision.attempt_passed,
               'traffic', decision.traffic_passed
           ),
           'expected_target_ids', COALESCE((
               SELECT jsonb_agg(target_id ORDER BY target_id)
               FROM fbref_20260825_oversize_diagnostic_expected
           ), '[]'::jsonb),
           'demoted_scope', COALESCE((
               SELECT jsonb_agg(to_jsonb(demoted_actual)
                                ORDER BY target_id)
               FROM demoted_actual
           ), '[]'::jsonb),
           'diagnostic_current_scope', COALESCE((
               SELECT jsonb_agg(to_jsonb(current_actual)
                                ORDER BY target_id)
               FROM fbref_20260825_oversize_diagnostic_current_actual
                    AS current_actual
           ), '[]'::jsonb),
           'run', (
               SELECT to_jsonb(run) - 'metadata'
                      || jsonb_build_object('metadata', run.metadata)
               FROM selected_run AS run
           ),
           'request_conservation', (
               SELECT jsonb_build_object(
                   'attempt_count', attempt_totals.attempt_count,
                   'target_http_requests', attempt_totals.request_count,
                   'page_requests', traffic_totals.page_request_count,
                   'page_http_requests', traffic_totals.http_request_count,
                   'page_browser_requests',
                       traffic_totals.browser_request_count,
                   'session_browser_requests',
                       session_totals.browser_request_count,
                   'run_requests_used', (
                       SELECT run.requests_used FROM selected_run AS run
                   )
               )
               FROM attempt_totals
               CROSS JOIN traffic_totals
               CROSS JOIN session_totals
           ),
           'targets', COALESCE((
               SELECT jsonb_agg(to_jsonb(target_evidence)
                                ORDER BY target_evidence.ordinal)
               FROM target_evidence
           ), '[]'::jsonb),
           'all_attempts', COALESCE((
               SELECT jsonb_agg(to_jsonb(all_attempts)
                                ORDER BY all_attempts.target_id,
                                         all_attempts.attempt_number,
                                         all_attempts.attempt_id)
               FROM all_attempts
           ), '[]'::jsonb),
           'all_pages', COALESCE((
               SELECT jsonb_agg(to_jsonb(all_pages)
                                ORDER BY all_pages.reservation_id)
               FROM all_pages
           ), '[]'::jsonb),
           'all_sessions', COALESCE((
               SELECT jsonb_agg(to_jsonb(all_sessions)
                                ORDER BY all_sessions.session_id)
               FROM all_sessions
           ), '[]'::jsonb),
           'all_budgets', COALESCE((
               SELECT jsonb_agg(to_jsonb(all_budgets)
                                ORDER BY all_budgets.reservation_id)
               FROM all_budgets
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
