# ClubElo #1465: dependency acceptance and history window

The approved continuation separates isolated preparation, a coordinated common
release, automatic source delivery and manual history. Production, archives,
pause states and auto-delivery policy stay unchanged during preparation.
See the [bounded release](../../deploy/shared_dependencies/README.md).

## Source acceptance after the common release

Confirm both common files match the reviewed payload. Then wait for the regular
ClubElo delivery at 04:40 МСК, in its 04:30–05:25 МСК window. Do not execute the
installed script's `--check` as a read-only probe: it writes logs/state.

Require the delivery journal's accepted result, source bytes equal the recorded
master commit, no extra files, `dag.has_import_errors=false`, no own import-error
rows, and `last_parsed_time` after delivery completion +60 seconds. Daily remains
enabled under its existing permission. PR #1668 merge is not its acceptance;
verify the delivered watch branch, then track #1466's two production executions,
state and notification acceptance separately. No fixture substitution or alert
test against production is authorized by this preparation.

## Proposed history window: 10.10.2026 08:20–08:55 МСК

UTC bounds are 2026-10-10 05:20–05:55. This is a proposal requiring separate owner
approval, not a scheduled automation. It follows WhoScored's known night window,
allows the 07:30 МСК daily its 45-minute timeout, and ends before the known FBref
09:00 МСК slot. A quiet interval is not guaranteed by the clock.

Before owner approval/start, refresh the current schedule/driver/task evidence:

- The common release and nightly source delivery are accepted. The DAG is
  active/unpaused, has no own import errors, and source files match master.
- The 07:30 МСК daily has completed `scrape_daily` and `validate_data` successfully.
- There is no active competing collection, queued ingestion, delivery,
  maintenance or external driver. Known historical rows need parent/job evidence;
  do not delete them or interpret the classifier's blocker count as worker count.
- Infrastructure is healthy. Recompute the current linked queue and latest
  `done/no_page` manifest by `(slug, _ingested_at, fetched_at)`; saved Ranking
  gives a preflight estimate, the actual history run must use its fresh Ranking.

At the 09.10 15:21 МСК audit there were 498 linked clubs, 10 closed and 488 pending,
no failed latest entries. This is not a fixed population or future readiness.

After explicit permission, issue one manual run of the existing DAG with
`{"run_history": true}`. Start by 08:22 МСК to preserve the 30-minute timeout and
end-of-window checks. Keep the existing runner, 1 request/second, batch size 200,
no Airflow retries, and `max_active_runs=1`. Batch size is a commit chunk, not a
200-club run limit: this run traverses the complete pending queue. Daily/watch
branches must be skipped in this manual run. Do not pause DAGs or other workers
to force this source window; skip it when the conditions do not hold.

Acceptance requires history task/run success, `pending_after=0`, no errors,
blocks, failed pages or unexpected redirects, latest `done/no_page` coverage of
the run's complete linked queue, and points/matches fenced by the same
`slug/_batch_id` as the closing manifest. Keep raw and result JSON evidence,
requests/wire_bytes, and check archive invariants without mutation. On failure,
retain the failed run and committed manifest. Another manual attempt needs its
own window/permission; never turn an incomplete run green.

## Remaining issue acceptance

After accepted history, record three successful new-code runs by task outcomes,
source version and current-run data evidence, following the existing #1465
order. A green gate/skip alone is insufficient. Preserve passport/handoff and
update the existing operational handoff only when authorized. Do not close
#1465/#1466 on merge or one daily; seven complete rating dates with measured lag
and overall production acceptance remain the roadmap's separate criteria.
