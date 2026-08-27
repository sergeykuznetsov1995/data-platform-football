-- One-shot, exact FBref false-season-quarantine remediation (2026-08-25).
--
-- This file preserves raw payloads, attempts, manifests, and crawl history. It
-- only makes the four reviewed false quarantines schedulable after cross-run
-- season raw adoption is disabled. Their stale HTTP validators are cleared so
-- the recovery fetch must obtain a full 200 response; last_content_hash stays
-- intact as durable evidence of the rejected bytes.
--
-- READ-ONLY PREFLIGHT (safe to run by itself before the deployment window):
-- WITH expected(target_id) AS (VALUES
--     ('fbref:season:15:2025-2026'),
--     ('fbref:season:16:2025-2026'),
--     ('fbref:season:20:2025-2026'),
--     ('fbref:season:34:2025-2026')
-- )
-- SELECT expected.target_id, frontier.page_kind, frontier.state,
--        frontier.last_content_hash, frontier.last_etag,
--        frontier.last_modified, frontier.last_error_class,
--        frontier.last_error_message
-- FROM expected
-- LEFT JOIN fbref_control.page_frontier AS frontier USING (target_id)
-- ORDER BY expected.target_id;

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET LOCAL idle_in_transaction_session_timeout = '30s';

-- Serialize the two 2026-08-25 remediation files with each other.
SELECT pg_advisory_xact_lock(
    hashtextextended('fbref:remediation:2026-08-25', 0)
);

-- Prevent a crawl_run from being created or started after the guard passes.
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

-- Exact-state preflight plus row locks. Any absent or changed target makes the
-- four-row assertion below abort the transaction before the update.
CREATE TEMP TABLE fbref_20260825_false_seasons_selected
ON COMMIT DROP
AS
SELECT frontier.target_id
FROM fbref_control.page_frontier AS frontier
WHERE frontier.target_id IN (
        'fbref:season:15:2025-2026',
        'fbref:season:16:2025-2026',
        'fbref:season:20:2025-2026',
        'fbref:season:34:2025-2026'
    )
  AND frontier.source = 'fbref'
  AND frontier.page_kind = 'season'
  AND frontier.state = 'quarantined'
  AND frontier.last_error_class = 'ParseContractQuarantined'
  AND frontier.last_error_message = 'schedule_season_mismatch'
ORDER BY frontier.target_id
FOR UPDATE;

DO $selection_guard$
DECLARE
    selected_count integer;
BEGIN
    SELECT count(*)
    INTO selected_count
    FROM fbref_20260825_false_seasons_selected;

    IF selected_count <> 4 THEN
        RAISE EXCEPTION
            'FBref season remediation refused: expected exact four rows, got %',
            selected_count;
    END IF;
END
$selection_guard$;

CREATE TEMP TABLE fbref_20260825_false_seasons_updated
ON COMMIT DROP
AS
WITH updated_rows AS (
    UPDATE fbref_control.page_frontier AS frontier
    SET state = 'queued',
        next_fetch_at = clock_timestamp(),
        retry_after = NULL,
        last_error_class = NULL,
        last_error_message = NULL,
        last_etag = NULL,
        last_modified = NULL,
        updated_at = clock_timestamp()
    FROM fbref_20260825_false_seasons_selected AS selected
    WHERE frontier.target_id = selected.target_id
      AND frontier.target_id IN (
          'fbref:season:15:2025-2026',
          'fbref:season:16:2025-2026',
          'fbref:season:20:2025-2026',
          'fbref:season:34:2025-2026'
      )
      AND frontier.source = 'fbref'
      AND frontier.page_kind = 'season'
      AND frontier.state = 'quarantined'
      AND frontier.last_error_class = 'ParseContractQuarantined'
      AND frontier.last_error_message = 'schedule_season_mismatch'
    RETURNING frontier.target_id
)
SELECT target_id
FROM updated_rows;

DO $update_guard$
DECLARE
    updated_count integer;
BEGIN
    SELECT count(*)
    INTO updated_count
    FROM fbref_20260825_false_seasons_updated;

    IF updated_count <> 4 THEN
        RAISE EXCEPTION
            'FBref season remediation refused: expected four updates, got %',
            updated_count;
    END IF;
END
$update_guard$;

-- The sole result set after mutation is the exact affected-ID list.
SELECT target_id
FROM fbref_20260825_false_seasons_updated
ORDER BY target_id;

COMMIT;
