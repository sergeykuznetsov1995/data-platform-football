-- PROVISIONAL — PROD1 IS NOT TERMINAL — DO NOT EXECUTE.
--
-- FBref exact oversize evidence remediation (2026-08-25).  The four VALUES
-- below are only the latest observed set from source run
-- 94838bac-786a-5d59-99e4-f6a2b3f7971e.  They are not a frozen authority.
-- Replace only after the source run is terminal and a second reviewer has
-- approved the read-only terminal snapshot, its SHA256, and this diff.
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
--    Save the snapshot outside this repository, replace the provisional VALUES
--    below exactly, update the
--    expected set in the unit test, and run `git diff --check`. Missing,
--    extra, duplicate, or changed rows require a new review; never waive the
--    executable bidirectional equality below.
-- 3. Deploy fbref-camoufox-metered-warm-http-v10, then run this read-only
--    runtime attestation in the scheduler. It must print the version and three
--    hashes exactly; a mismatch is NO-GO:
--    docker exec airflow-scheduler /opt/legacy-scraper-venv/bin/python -B -c "import hashlib,inspect,pathlib; from scrapers.fbref.fetcher import FETCHER_VERSION; from scrapers.fbref.control.store import ControlStore; from scrapers.fbref.pipeline import FBrefPipeline; a=hashlib.sha256(inspect.getsource(ControlStore.create_explicit_run_cohort).encode()).hexdigest(); b=hashlib.sha256(inspect.getsource(FBrefPipeline.seed_acceptance_cohort).encode()).hexdigest(); c=hashlib.sha256(pathlib.Path('/opt/airflow/scripts/research/run_fbref_oversize_evidence_canary.py').read_bytes()).hexdigest(); assert FETCHER_VERSION == 'fbref-camoufox-metered-warm-http-v10'; assert a == '15c5f5e578b1e0dac676b2069e66c8d9340ea1d0e824797aadaaeaea176edb0f'; assert b == '979865b0254f44532428678598c03ede952212a56cfce275f8eb07cc548f541b'; assert c == '7580817f152b19ed830d54faf7e08757086e625969aa23d5e4d208294d17fc0b'; print(FETCHER_VERSION,a,b,c)"
-- 4. Only with FBref ingestion paused and all writer/lease guards clear, apply:
--    psql "$FBREF_CONTROL_DB_URI" \
--      --set=reviewed_source_run_id=<terminal-reviewed-control-run-uuid> \
--      --set=reviewed_terminal_snapshot_sha256=<sha256-from-step-1> \
--      --file=docs/operations/sql/fbref_20260825_reanimate_exact_oversize_evidence.sql
-- 5. Run the purpose-built fetch-only command below after replacing every
--    placeholder and reconciling its repeated --target-id arguments to the
--    reviewed final VALUES. It creates the 100 requests / 50 MiB / shard 25
--    acceptance_nonpublishing profile and calls seed_acceptance_cohort /
--    create_explicit_run_cohort, so the immutable exact cohort is installed
--    before fetch and membership does not depend on due-frontier ordering:
--    docker exec airflow-scheduler /opt/legacy-scraper-venv/bin/python -B /opt/airflow/scripts/research/run_fbref_oversize_evidence_canary.py --run-label <unique-manual-airflow-run-id> --proxy-file <scheduler-proxy-file> --reviewed-source-run-id <same-reviewed-control-run-uuid> --reviewed-terminal-snapshot-sha256 <same-reviewed-sha256> --target-id fbref:season_stats:6:2022:playingtime --target-id fbref:season_stats:569:2025-2026:playingtime --target-id fbref:season_stats:569:2025-2026:standard --target-id fbref:season_stats:678:2021:playingtime
--    The runner records publication_eligible=false, runs exactly one live wave,
--    and contains no parse, Silver/Gold, or publication finalizer call.
-- 6. Run the separate read-only gate with both exact identifiers:
--    psql "$FBREF_CONTROL_DB_URI" \
--      --set=reviewed_source_run_id=<same-reviewed-control-run-uuid> \
--      --set=airflow_run_id=<exact-diagnostic-airflow-run-id> \
--      --file=docs/operations/sql/fbref_20260825_oversize_evidence_canary_gate.sql
--
-- A repeated response_too_large is diagnostic RED, not production success.
-- The gate deliberately exits nonzero after emitting bounded evidence. Any
-- unintended target, 3xx, extra attempt/request, foreign logical refresh, or
-- publishing mode is also NO-GO. If Content-Length is absent or untrusted,
-- the streaming cap still aborts fail-closed. Do not infer or install a new
-- decoded-body cap. This transaction preserves attempt/raw/billing rows and
-- every frontier history/evidence field; it changes only state and updated_at.

\set ON_ERROR_STOP on

\if :{?reviewed_source_run_id}
\else
DO $missing_source_run_id$
BEGIN
    RAISE EXCEPTION
        'FBref oversize remediation refused: source run id is required after terminal review';
END
$missing_source_run_id$;
\endif

\if :{?reviewed_terminal_snapshot_sha256}
\else
DO $missing_snapshot_sha256$
BEGIN
    RAISE EXCEPTION
        'FBref oversize remediation refused: terminal snapshot SHA256 is required after terminal review';
END
$missing_snapshot_sha256$;
\endif

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

CREATE TEMP TABLE fbref_20260825_oversize_evidence_expected (
    target_id text PRIMARY KEY,
    canonical_url text NOT NULL UNIQUE,
    target_status text NOT NULL,
    attempt_status text NOT NULL,
    error_class text NOT NULL,
    http_status integer NOT NULL,
    http_request_count bigint NOT NULL,
    error_message text NOT NULL
) ON COMMIT DROP;

INSERT INTO fbref_20260825_oversize_evidence_expected VALUES
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
        'https://fbref.com/en/comps/569/standard/Copa-del-Rey-Stats',
        'failed', 'failed', 'response_too_large', 200, 1,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/569/standard/Copa-del-Rey-Stats'
    ),
    (
        'fbref:season_stats:678:2021:playingtime',
        'https://fbref.com/en/comps/678/2021/playingtime/2021-UEFA-Euro-qualification-Stats',
        'failed', 'failed', 'response_too_large', 200, 1,
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/678/2021/playingtime/2021-UEFA-Euro-qualification-Stats'
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
WHERE run.run_id = :'reviewed_source_run_id'::uuid
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
    WHERE run.run_id = :'reviewed_source_run_id'::uuid
      AND run.finished_at IS NOT NULL
      AND run.status IN ('succeeded', 'failed', 'cancelled');

    SELECT count(*) INTO expected_count
    FROM fbref_20260825_oversize_evidence_expected;
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
        WHERE target.run_id = :'reviewed_source_run_id'::uuid
          AND target.status IN ('pending', 'leased', 'retry')
    ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: source target is nonterminal';
    END IF;
    IF expected_count = 0 OR source_count <> expected_count THEN
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
        SELECT * FROM fbref_20260825_oversize_evidence_expected
        EXCEPT
        SELECT * FROM fbref_20260825_source_terminal_oversize
    ) OR EXISTS (
        SELECT * FROM fbref_20260825_source_terminal_oversize
        EXCEPT
        SELECT * FROM fbref_20260825_oversize_evidence_expected
    ) THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: terminal set mismatch';
    END IF;
    IF :'reviewed_terminal_snapshot_sha256' !~ '^[0-9a-f]{64}$'
       OR computed_snapshot_sha256
          <> :'reviewed_terminal_snapshot_sha256' THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: terminal snapshot SHA256 mismatch';
    END IF;
END
$source_exact_set_guard$;

CREATE TEMP TABLE fbref_20260825_oversize_evidence_selected
ON COMMIT DROP
AS
SELECT expected.target_id,
       expected.canonical_url,
       expected.error_message
FROM fbref_20260825_oversize_evidence_expected AS expected
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
    FROM fbref_20260825_oversize_evidence_expected;
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
            FROM fbref_20260825_oversize_evidence_expected
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
    FROM fbref_20260825_oversize_evidence_expected;
    SELECT count(*) INTO updated_count
    FROM fbref_20260825_oversize_evidence_updated;
    IF updated_count <> expected_count THEN
        RAISE EXCEPTION
            'FBref oversize remediation refused: update count mismatch';
    END IF;
    IF EXISTS (
        SELECT target_id FROM fbref_20260825_oversize_evidence_expected
        EXCEPT
        SELECT target_id FROM fbref_20260825_oversize_evidence_updated
    ) OR EXISTS (
        SELECT target_id FROM fbref_20260825_oversize_evidence_updated
        EXCEPT
        SELECT target_id FROM fbref_20260825_oversize_evidence_expected
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
