-- TERMINAL AUTHORITY BAKED — CANDIDATE NO-GO UNTIL INDEPENDENT REVIEW.
-- DO NOT EXECUTE, DEPLOY, OR PRESENT AS OPERATIONALLY APPROVED FROM THIS
-- UNREVIEWED CANDIDATE COMMIT.
--
-- FBref exact oversize evidence remediation (2026-08-25). The four VALUES,
-- source run, and digest below are baked from the saved terminal snapshot
-- `/root/fbref-production-20260825/oversize-terminal-snapshot-94838bac.tsv`.
-- The source is terminal failed and the exact UTF-8 TSV SHA256 is
-- b114e1139c50857b2985ead5ef2f72083660fc75cc9d1e9466874959a77bd543.
-- A separate independent review must approve this candidate before use.
--
-- Terminal-review procedure (read-only until the final apply command):
--
-- 1. Export one ordered row for every run_target whose sole fetch_attempt is
--    terminal HTTP-200 response_too_large. Include target_id, canonical_url,
--    run_target.status, attempt.status/error_class/http_status,
--    http_request_count, and error_message. Verify the source crawl_run is
--    terminal with finished_at set and no pending/leased/retry run_target.
-- 2. Export the SELECT below with `psql -XAt -F $'\t'` (no header, ordered by
--    target_id, one LF per row), then run `sha256sum` on that exact UTF-8 TSV.
--    The executable hash guard uses the identical tab/LF serialization.
--    The saved snapshot and both independent SHA256 methods must equal the
--    baked values below, the matching gate authority, and the pipeline's
--    OVERSIZE_EVIDENCE_AUTHORITY. The unit test must contain the same exact target set and
--    `git diff --check` must pass. Missing, extra, duplicate, or changed rows
--    require a new candidate commit and review; never waive the executable
--    bidirectional equality below. Operator variables are not an approval
--    mechanism and cannot override the baked values.
-- 3. Deploy the combined reviewed Bronze change: fetcher v10 with the bounded
--    season_stats 8 MiB decoded-body cap, its derived 9 MiB target reservation,
--    current-season source-link remediation, and this exact2 diagnostic. Then
--    run this read-only
--    runtime attestation in the scheduler. It must print the version and four
--    hashes exactly, and prove the effective 8/9 MiB bounds; a mismatch is
--    NO-GO:
--    docker exec airflow-scheduler /opt/legacy-scraper-venv/bin/python -B -c "import hashlib,inspect,pathlib; import scrapers.fbref.settings as fbref_settings; from scrapers.fbref.fetcher import FETCHER_VERSION; from scrapers.fbref.control.store import ControlStore; from scrapers.fbref.pipeline import FBrefPipeline,run_oversize_evidence_canary; a=hashlib.sha256(inspect.getsource(ControlStore.create_explicit_run_cohort).encode()).hexdigest(); b=hashlib.sha256(inspect.getsource(FBrefPipeline.seed_acceptance_cohort).encode()).hexdigest(); c=hashlib.sha256(pathlib.Path(inspect.getsourcefile(FBrefPipeline)).read_bytes()).hexdigest(); d=hashlib.sha256(pathlib.Path(inspect.getsourcefile(fbref_settings)).read_bytes()).hexdigest(); assert callable(run_oversize_evidence_canary); assert FETCHER_VERSION == 'fbref-camoufox-metered-warm-http-v10'; assert fbref_settings.FBREF_PERSISTENT_HTTP_SESSION is True; assert fbref_settings.DEFAULT_SEASON_STATS_HTTP_BODY_LIMIT_BYTES == 8388608; assert fbref_settings.DEFAULT_REQUEST_RESERVATION_BYTES == 9437184; assert a == '15c5f5e578b1e0dac676b2069e66c8d9340ea1d0e824797aadaaeaea176edb0f'; assert b == '979865b0254f44532428678598c03ede952212a56cfce275f8eb07cc548f541b'; assert c == '3356f6880e37259368c0dc176cb1ce5e83f6e9ef00a6632d3fd81959ee52e4e4'; assert d == 'a8397e96e0c15711f33bde0e9a497e5f9c2cbd1b88536abd57d9bbd35d45f664'; print(FETCHER_VERSION,a,b,c,d)"
-- 4. Complete the current-season remediation acceptance in
--    docs/operations/fbref_current_season_reconcile.md first.
--    In particular, old (6,2022)/(678,2021) playingtime frontiers must be
--    daily but quarantined as ScopeQuarantined/noncurrent_season and not due.
--    Competition 569/2025-2026 must still be present and current before its
--    exact playingtime/standard pair can be requeued.
--    This ordering is mandatory: false-current pages must consume no proxy.
-- 5. Only after that acceptance, with FBref ingestion paused and every
--    writer/lease guard clear, apply the baked artifact without authority
--    parameters:
--    psql "$FBREF_CONTROL_DB_URI" \
--      --file=docs/operations/sql/fbref_20260825_reanimate_exact_oversize_evidence.sql
-- 6. Run the purpose-built fetch-only command. Its source UUID and snapshot
--    digest bind all four terminal provenance rows, while its exact diagnostic
--    cohort is only the two genuinely-current competition-569 targets. Both
--    sets come only from the matching baked authority in
--    the reviewed in-place pipeline module. It creates the 100 requests / 50 MiB / shard 25
--    acceptance_nonpublishing profile and calls seed_acceptance_cohort /
--    create_explicit_run_cohort, so the immutable exact cohort is installed
--    before fetch and membership does not depend on due-frontier ordering:
--    docker exec airflow-scheduler /opt/legacy-scraper-venv/bin/python -B -c "import json,sys; from scrapers.fbref.pipeline import OversizeEvidenceConfig,run_oversize_evidence_canary; config=OversizeEvidenceConfig(logical_run_label=sys.argv[1]); print(json.dumps(run_oversize_evidence_canary(config),sort_keys=True))" <unique-manual-airflow-run-id>
--    The callable records publication_eligible=false, runs exactly one live
--    wave, and has no direct-proxy/file fallback. Before it constructs the
--    pipeline or creates a control run it requires the persistent HTTP switch,
--    authenticates the dedicated meter without paid traffic, proves at least
--    50 MiB of daily allowance, and installs the existing authorized Bronze
--    acceptance provider provenance: dag_accept_fbref_bronze /
--    oversize_evidence_fetch / the exact logical run label and control run UUID.
--    The control run itself remains fbref_oversize_evidence_canary and
--    acceptance_nonpublishing. Provider leasing is capped at 37.5 MiB, retaining
--    the supported one-quarter lease-extension race headroom inside the 50 MiB
--    control circuit. The callable permits at most one 20-request browser solve
--    plus one target HTTP request per page (22 total), releases its publication
--    lock before committing success, and contains no parse,
--    Silver/Gold, or publication finalizer call.
-- 7. Run the separate read-only gate with the diagnostic identifier only; its
--    source/snapshot authority is baked and independently reverified:
--    psql "$FBREF_CONTROL_DB_URI" \
--      --set=airflow_run_id=<exact-diagnostic-airflow-run-id> \
--      --file=docs/operations/sql/fbref_20260825_oversize_evidence_canary_gate.sql
--
-- A repeated response_too_large is diagnostic RED, not production success.
-- The gate deliberately exits nonzero after emitting bounded evidence. Any
-- unintended target, 3xx, extra attempt/request, foreign logical refresh, or
-- publishing mode is also NO-GO. If Content-Length is absent or untrusted,
-- the streaming cap still aborts fail-closed. The reviewed 8 MiB decoded-body
-- cap accepts exactly 8 MiB and aborts at 8 MiB+1; the 9 MiB target reservation
-- is a control ceiling, not billed usage, and exact settlement is unchanged.
-- This transaction preserves attempt/raw/billing rows and
-- every frontier history/evidence field; it changes only state and updated_at.

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

SELECT set_config(
    'fbref.oversize_baked_source_run_id',
    :'fbref_oversize_baked_source_run_id',
    false
) AS fbref_oversize_source_binding
\gset
SELECT set_config(
    'fbref.oversize_baked_snapshot_sha256',
    :'fbref_oversize_baked_snapshot_sha256',
    false
) AS fbref_oversize_snapshot_binding
\gset

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '30s';

SELECT pg_advisory_xact_lock(
    hashtextextended('fbref:oversize-evidence:2026-08-25', 0)
);

LOCK TABLE fbref_control.crawl_run IN SHARE MODE;
LOCK TABLE fbref_control.page_frontier IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE fbref_control.run_target IN SHARE MODE;
LOCK TABLE fbref_control.fetch_attempt IN SHARE MODE;
LOCK TABLE fbref_control.season_registry IN SHARE MODE;

DO $active_run_guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM fbref_control.crawl_run
        WHERE status IN ('pending', 'running')
    ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: active crawl run exists';
    END IF;
END
$active_run_guard$;

DO $lease_zero_guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM fbref_control.page_frontier AS frontier
        WHERE frontier.state = 'leased'
           OR frontier.claim_token IS NOT NULL
           OR frontier.lease_run_id IS NOT NULL
           OR frontier.lease_refresh_id IS NOT NULL
           OR frontier.lease_expires_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: active or residual lease exists';
    END IF;
END
$lease_zero_guard$;

CREATE TEMP TABLE fbref_20260825_oversize_source_expected (
    target_id text PRIMARY KEY,
    canonical_url text NOT NULL UNIQUE,
    target_status text NOT NULL,
    attempt_status text NOT NULL,
    error_class text NOT NULL,
    http_status integer NOT NULL,
    http_request_count bigint NOT NULL,
    error_message text NOT NULL
) ON COMMIT DROP;

INSERT INTO fbref_20260825_oversize_source_expected VALUES
    (
        'fbref:season_stats:6:2022:playingtime',
        'https://fbref.com/en/comps/6/2022/playingtime/2022-WCQ----UEFA-M-Stats',
        'failed', 'failed', 'response_too_large', 200, 1,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/6/2022/playingtime/2022-WCQ----UEFA-M-Stats'
    ),
    (
        'fbref:season_stats:569:2025-2026:playingtime',
        'https://fbref.com/en/comps/569/playingtime/Copa-del-Rey-Stats',
        'failed', 'failed', 'response_too_large', 200, 1,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/569/playingtime/Copa-del-Rey-Stats'
    ),
    (
        'fbref:season_stats:569:2025-2026:standard',
        'https://fbref.com/en/comps/569/stats/Copa-del-Rey-Stats',
        'failed', 'failed', 'response_too_large', 200, 1,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/569/stats/Copa-del-Rey-Stats'
    ),
    (
        'fbref:season_stats:678:2021:playingtime',
        'https://fbref.com/en/comps/678/2021/playingtime/2021-UEFA-Euro-qualification-Stats',
        'failed', 'failed', 'response_too_large', 200, 1,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/678/2021/playingtime/2021-UEFA-Euro-qualification-Stats'
    );

-- The terminal snapshot remains the immutable four-row provenance authority.
-- After current-scope repair, only these two genuinely-current pages may be
-- reanimated or fetched.
CREATE TEMP TABLE fbref_20260825_oversize_diagnostic_expected
ON COMMIT DROP
AS
SELECT source_expected.*
FROM fbref_20260825_oversize_source_expected AS source_expected
WHERE source_expected.target_id IN (
    'fbref:season_stats:569:2025-2026:playingtime',
    'fbref:season_stats:569:2025-2026:standard'
);

CREATE UNIQUE INDEX
    fbref_20260825_oversize_diagnostic_expected_target_idx
ON fbref_20260825_oversize_diagnostic_expected (target_id);

CREATE TEMP TABLE fbref_20260825_oversize_diagnostic_current_expected (
    competition_id text NOT NULL,
    season_id text NOT NULL,
    target_id text PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO fbref_20260825_oversize_diagnostic_current_expected VALUES
    (
        '569', '2025-2026',
        'fbref:season_stats:569:2025-2026:playingtime'
    ),
    (
        '569', '2025-2026',
        'fbref:season_stats:569:2025-2026:standard'
    );

CREATE TEMP TABLE fbref_20260825_oversize_demoted_expected (
    competition_id text NOT NULL,
    season_id text NOT NULL,
    target_id text PRIMARY KEY
) ON COMMIT DROP;

INSERT INTO fbref_20260825_oversize_demoted_expected VALUES
    (
        '6', '2022',
        'fbref:season_stats:6:2022:playingtime'
    ),
    (
        '678', '2021',
        'fbref:season_stats:678:2021:playingtime'
    );

CREATE TEMP TABLE fbref_20260825_source_terminal_oversize
ON COMMIT DROP
AS
SELECT target.target_id,
       frontier.canonical_url,
       target.status AS target_status,
       attempt.status AS attempt_status,
       attempt.error_class,
       attempt.http_status,
       attempt.http_request_count,
       attempt.error_message
FROM fbref_control.crawl_run AS run
JOIN fbref_control.run_target AS target
  ON target.run_id = run.run_id
JOIN fbref_control.page_frontier AS frontier
  ON frontier.target_id = target.target_id
JOIN fbref_control.fetch_attempt AS attempt
  ON attempt.run_id = target.run_id
 AND attempt.target_id = target.target_id
 AND attempt.logical_refresh_id = target.logical_refresh_id
WHERE run.run_id = :'fbref_oversize_baked_source_run_id'::uuid
  AND run.finished_at IS NOT NULL
  AND run.status IN ('succeeded', 'failed', 'cancelled')
  AND target.status = 'failed'
  AND attempt.status = 'failed'
  AND attempt.error_class = 'response_too_large'
  AND attempt.http_status = 200
  AND attempt.http_request_count = 1
  AND (
      SELECT count(*)
      FROM fbref_control.fetch_attempt AS counted_attempt
      WHERE counted_attempt.run_id = target.run_id
        AND counted_attempt.target_id = target.target_id
  ) = 1;

DO $source_exact_set_guard$
DECLARE
    source_run_count integer;
    expected_count integer;
    source_count integer;
    computed_snapshot_sha256 text;
BEGIN
    SELECT count(*) INTO source_run_count
    FROM fbref_control.crawl_run AS run
    WHERE run.run_id = current_setting(
        'fbref.oversize_baked_source_run_id'
    )::uuid
      AND run.finished_at IS NOT NULL
      AND run.status IN ('succeeded', 'failed', 'cancelled');

    SELECT count(*) INTO expected_count
    FROM fbref_20260825_oversize_source_expected;
    SELECT count(*) INTO source_count
    FROM fbref_20260825_source_terminal_oversize;

    SELECT encode(sha256(convert_to(COALESCE(string_agg(
        concat_ws(E'\t', target_id, canonical_url, target_status,
                  attempt_status, error_class, http_status::text,
                  http_request_count::text, error_message),
        E'\n' ORDER BY target_id), '') || E'\n', 'UTF8')), 'hex')
    INTO computed_snapshot_sha256
    FROM fbref_20260825_source_terminal_oversize;

    IF source_run_count <> 1 THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: source run is absent or nonterminal';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM fbref_control.run_target AS target
        WHERE target.run_id = current_setting(
            'fbref.oversize_baked_source_run_id'
        )::uuid
          AND target.status IN ('pending', 'leased', 'retry')
    ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: source target is nonterminal';
    END IF;
    IF expected_count <> 4 OR source_count <> 4
       OR source_count <> expected_count THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: expected/source counts differ';
    END IF;
    IF (SELECT count(*) FROM fbref_20260825_source_terminal_oversize)
       <> (SELECT count(DISTINCT target_id)
           FROM fbref_20260825_source_terminal_oversize) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: duplicate source target';
    END IF;
    IF EXISTS (
        SELECT * FROM fbref_20260825_oversize_source_expected
        EXCEPT
        SELECT * FROM fbref_20260825_source_terminal_oversize
    ) OR EXISTS (
        SELECT * FROM fbref_20260825_source_terminal_oversize
        EXCEPT
        SELECT * FROM fbref_20260825_oversize_source_expected
    ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: terminal set mismatch';
    END IF;
    IF computed_snapshot_sha256 <> current_setting(
        'fbref.oversize_baked_snapshot_sha256'
    ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: terminal snapshot SHA256 mismatch';
    END IF;
END
$source_exact_set_guard$;

CREATE TEMP TABLE fbref_20260825_oversize_demoted_actual
ON COMMIT DROP
AS
SELECT expected.competition_id,
       expected.season_id,
       expected.target_id
FROM fbref_20260825_oversize_demoted_expected AS expected
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
  AND frontier.last_error_message = 'noncurrent_season';

CREATE TEMP TABLE fbref_20260825_oversize_diagnostic_current_actual
ON COMMIT DROP
AS
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
  AND frontier.refresh_policy = 'daily';

DO $scope_remediation_guard$
DECLARE
    demoted_count integer;
    actual_count integer;
    diagnostic_count integer;
    diagnostic_current_count integer;
    diagnostic_current_actual_count integer;
BEGIN
    SELECT count(*) INTO demoted_count
    FROM fbref_20260825_oversize_demoted_expected;
    SELECT count(*) INTO actual_count
    FROM fbref_20260825_oversize_demoted_actual;
    SELECT count(*) INTO diagnostic_count
    FROM fbref_20260825_oversize_diagnostic_expected;
    SELECT count(*) INTO diagnostic_current_count
    FROM fbref_20260825_oversize_diagnostic_current_expected;
    SELECT count(*) INTO diagnostic_current_actual_count
    FROM fbref_20260825_oversize_diagnostic_current_actual;
    IF demoted_count <> 2 OR actual_count <> demoted_count
       OR diagnostic_count <> 2
       OR diagnostic_current_count <> 2
       OR diagnostic_current_actual_count <> diagnostic_current_count
       OR EXISTS (
           SELECT * FROM fbref_20260825_oversize_demoted_expected
           EXCEPT
           SELECT * FROM fbref_20260825_oversize_demoted_actual
       ) OR EXISTS (
           SELECT * FROM fbref_20260825_oversize_demoted_actual
           EXCEPT
           SELECT * FROM fbref_20260825_oversize_demoted_expected
       ) OR EXISTS (
           SELECT *
           FROM fbref_20260825_oversize_diagnostic_current_expected
           EXCEPT
           SELECT *
           FROM fbref_20260825_oversize_diagnostic_current_actual
       ) OR EXISTS (
           SELECT *
           FROM fbref_20260825_oversize_diagnostic_current_actual
           EXCEPT
           SELECT *
           FROM fbref_20260825_oversize_diagnostic_current_expected
       ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: scope remediation proof mismatch';
    END IF;
END
$scope_remediation_guard$;

CREATE TEMP TABLE fbref_20260825_oversize_evidence_selected
ON COMMIT DROP
AS
SELECT expected.target_id,
       expected.canonical_url,
       expected.error_message
FROM fbref_20260825_oversize_diagnostic_expected AS expected
JOIN fbref_control.page_frontier AS frontier
  ON frontier.target_id = expected.target_id
 AND frontier.canonical_url = expected.canonical_url
 AND frontier.last_error_message = expected.error_message
WHERE frontier.source = 'fbref'
  AND frontier.page_kind = 'season_stats'
  AND frontier.refresh_policy = 'daily'
  AND frontier.state = 'dead'
  AND frontier.last_http_status = 200
  AND frontier.last_fetched_at IS NULL
  AND frontier.last_error_class = 'response_too_large'
ORDER BY expected.target_id
FOR UPDATE OF frontier;

DO $selection_guard$
DECLARE
    expected_count integer;
    selected_count integer;
BEGIN
    SELECT count(*) INTO expected_count
    FROM fbref_20260825_oversize_diagnostic_expected;
    SELECT count(*) INTO selected_count
    FROM fbref_20260825_oversize_evidence_selected;
    IF selected_count <> expected_count THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: frontier selection mismatch';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM fbref_control.run_target AS outstanding
        JOIN fbref_control.crawl_run AS outstanding_run
          ON outstanding_run.run_id = outstanding.run_id
        WHERE outstanding.target_id IN (
            SELECT target_id
            FROM fbref_20260825_oversize_diagnostic_expected
        )
          AND outstanding.status IN ('pending', 'leased', 'retry')
          AND outstanding_run.status IN ('pending', 'running')
    ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: target has active writer';
    END IF;
END
$selection_guard$;

CREATE TEMP TABLE fbref_20260825_oversize_evidence_updated
ON COMMIT DROP
AS
WITH updated_rows AS (
    UPDATE fbref_control.page_frontier AS frontier
    SET state = 'queued',
        updated_at = clock_timestamp()
    FROM fbref_20260825_oversize_evidence_selected AS selected
    WHERE frontier.target_id = selected.target_id
      AND frontier.canonical_url = selected.canonical_url
      AND frontier.last_error_message = selected.error_message
      AND frontier.source = 'fbref'
      AND frontier.page_kind = 'season_stats'
      AND frontier.refresh_policy = 'daily'
      AND frontier.state = 'dead'
      AND frontier.last_http_status = 200
      AND frontier.last_fetched_at IS NULL
      AND frontier.last_error_class = 'response_too_large'
    RETURNING frontier.target_id
)
SELECT target_id FROM updated_rows;

DO $prospective_exact_cohort_proof$
DECLARE
    expected_count integer;
    updated_count integer;
BEGIN
    SELECT count(*) INTO expected_count
    FROM fbref_20260825_oversize_diagnostic_expected;
    SELECT count(*) INTO updated_count
    FROM fbref_20260825_oversize_evidence_updated;
    IF updated_count <> expected_count THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: update count mismatch';
    END IF;
    IF EXISTS (
        SELECT target_id FROM fbref_20260825_oversize_diagnostic_expected
        EXCEPT
        SELECT target_id FROM fbref_20260825_oversize_evidence_updated
    ) OR EXISTS (
        SELECT target_id FROM fbref_20260825_oversize_evidence_updated
        EXCEPT
        SELECT target_id FROM fbref_20260825_oversize_diagnostic_expected
    ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: unexpected eligible target';
    END IF;
END
$prospective_exact_cohort_proof$;

SELECT target_id
FROM fbref_20260825_oversize_evidence_updated
ORDER BY target_id;

COMMIT;
