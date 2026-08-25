-- One-shot, bounded FBref large-page remediation (2026-08-25).
--
-- This file preserves raw payloads, attempts, manifests, and crawl history. It
-- only makes eligible frontier rows schedulable again after the reviewed
-- season_stats response-body limit is deployed.
--
-- READ-ONLY PREFLIGHT (safe to run by itself before the deployment window):
-- SELECT target_id, page_kind, state, last_error_class, last_error_message
-- FROM fbref_control.page_frontier
-- WHERE source = 'fbref'
--   AND page_kind = 'season_stats'
--   AND state = 'dead'
--   AND last_error_class = 'response_too_large'
-- ORDER BY target_id
-- LIMIT 26;

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '30s';

-- Serialize the two 2026-08-25 remediation files with each other.
SELECT pg_advisory_xact_lock(
    hashtextextended('fbref:remediation:2026-08-25', 0)
);

-- SHARE conflicts with the ROW EXCLUSIVE lock needed to create or start a
-- crawl run. It closes the gap between the active-run check and COMMIT.
LOCK TABLE fbref_control.crawl_run IN SHARE MODE;

DO $active_run_guard$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM fbref_control.crawl_run
        WHERE status IN ('pending', 'running')
    ) THEN
        RAISE EXCEPTION
            'FBref remediation refused: pending or running crawl_run exists';
    END IF;
END
$active_run_guard$;

-- This is also the executable preflight output. LIMIT 26 is deliberate: the
-- 26th row proves that the reviewed maximum of 25 would be exceeded.
CREATE TEMP TABLE fbref_20260825_large_pages_selected
ON COMMIT DROP
AS
SELECT frontier.target_id
FROM fbref_control.page_frontier AS frontier
WHERE frontier.source = 'fbref'
  AND frontier.page_kind = 'season_stats'
  AND frontier.state = 'dead'
  AND frontier.last_error_class = 'response_too_large'
ORDER BY frontier.target_id
LIMIT 26
FOR UPDATE;

DO $selection_guard$
DECLARE
    selected_count integer;
BEGIN
    SELECT count(*)
    INTO selected_count
    FROM fbref_20260825_large_pages_selected;

    IF selected_count < 1 OR selected_count > 25 THEN
        RAISE EXCEPTION
            'FBref large-page remediation refused: expected 1..25 rows, got %',
            selected_count;
    END IF;
END
$selection_guard$;

CREATE TEMP TABLE fbref_20260825_large_pages_updated
ON COMMIT DROP
AS
WITH updated_rows AS (
    UPDATE fbref_control.page_frontier AS frontier
    SET state = 'queued',
        next_fetch_at = clock_timestamp(),
        retry_after = NULL,
        last_error_class = NULL,
        last_error_message = NULL,
        updated_at = clock_timestamp()
    FROM fbref_20260825_large_pages_selected AS selected
    WHERE frontier.target_id = selected.target_id
      AND frontier.source = 'fbref'
      AND frontier.page_kind = 'season_stats'
      AND frontier.state = 'dead'
      AND frontier.last_error_class = 'response_too_large'
    RETURNING frontier.target_id
)
SELECT target_id
FROM updated_rows;

DO $update_guard$
DECLARE
    selected_count integer;
    updated_count integer;
BEGIN
    SELECT count(*)
    INTO selected_count
    FROM fbref_20260825_large_pages_selected;

    SELECT count(*)
    INTO updated_count
    FROM fbref_20260825_large_pages_updated;

    IF updated_count <> selected_count THEN
        RAISE EXCEPTION
            'FBref large-page remediation refused: selected %, updated %',
            selected_count,
            updated_count;
    END IF;
END
$update_guard$;

-- The sole result set after mutation is the exact, bounded affected-ID list.
SELECT target_id
FROM fbref_20260825_large_pages_updated
ORDER BY target_id;

COMMIT;
