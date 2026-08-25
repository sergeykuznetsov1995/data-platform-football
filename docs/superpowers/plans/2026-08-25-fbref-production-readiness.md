# FBref Production Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development to implement this plan task by task, with a fresh implementer and independent reviewer at each checkpoint.

**Goal:** Make the FBref current lane publish reliably, preserve lossless historical completeness, raise backfill throughput without increasing proxy bytes per durable page, and deploy the result with measured canaries.

**Architecture:** Keep current and historical work as separate DAGs, policies, run IDs, metrics, and operating windows while retaining one paid source writer at a time. Fix only target-local failures locally; keep source-wide and mass-failure circuits fail-closed. Treat immutable Raw plus generic Bronze as the lossless source contract, and run new typed parsers offline so they consume zero proxy bytes.

**Tech Stack:** Python 3, pytest, PostgreSQL control plane, Airflow 2.11, Trino/Iceberg Bronze, S3-compatible immutable Raw, Camoufox plus metered warm HTTP, Bash campaign driver, GitHub Actions.

## Global Constraints

- Build from `origin/master` in `/root/dpf-fbref-production-20260825`; do not modify the dirty `/root/data-platform-football` checkout.
- Do not reset, stash, checkout, or bulk-copy over `/root/dpf-whoscored-merge`. Production delivery must preserve the FBref directory ctime and follow the existing in-place delivery ceremony.
- Never release a publication lock manually while its owner run is active.
- Keep one paid FBref live runner at a time until cross-process Iceberg commit safety is independently proven.
- Current has priority over history. A history run may start only outside the protected current window and when no current run is queued/running.
- Provider-billed bytes are authoritative. Do not substitute decoded bytes, curl wire estimates, or control reservations for proxy cost.
- Working acceptance defaults pending owner confirmation:
  - current: competition index, schedules, and completed matches within 24 hours; season/season_stats/squad within 7 days; player/matchlog within 30 days;
  - completeness: immutable Raw plus validated lossless generic Bronze for every eligible male target; typed Bronze remains mandatory for schedule, season, season_stats, and match;
  - backfill: historical data runs in a non-publishing lane and never delays the daily current window;
  - efficiency: no regression in provider-billed bytes per durable target; throughput improvement is measured on the same page-kind mix.

## Task 1: Pin the Pre-change Evidence

**Files:**

- Preserve: `/tmp/fbref-code-review-2026-08-25.md`
- Preserve: `/tmp/fbref-runtime-audit-2026-08-25.md`
- Preserve: `/tmp/fbref-current-dag-root-cause-2026-08-25.md`
- Create: `docs/reviews/fbref/2026-08-25-production-readiness.md`

**Step 1: Record the live baseline**

Capture, with UTC timestamps and exact denominators:

- last 1/5/10 successful history runs;
- durable targets per active hour;
- requests and provider-billed bytes per durable target;
- queue by page kind and refresh policy;
- current freshness and the last three current DAG failures;
- raw/generic/typed validation coverage.

**Step 2: Record the code-review range**

Review `b6e246838dc43177888d21a989e1c0d9f4aa2a1e..origin/master` and distinguish already-fixed defects from open blockers #1186, #1206, and #1207.

**Step 3: Verify the evidence files**

Run:

```bash
test -s /tmp/fbref-code-review-2026-08-25.md
test -s /tmp/fbref-runtime-audit-2026-08-25.md
test -s /tmp/fbref-current-dag-root-cause-2026-08-25.md
```

Expected: all three commands exit 0.

## Task 2: Prevent Cross-run Season Raw Mis-adoption (#1186)

**Files:**

- Modify: `scrapers/fbref/pipeline.py`
- Modify: `tests/unit/scrapers/test_fbref_pipeline.py`

**Step 1: Write the failing regression tests**

Add tests proving:

1. a `historical_once` season target with raw from a prior logical refresh does not adopt that raw and performs a fresh network fetch;
2. an exact-logical-refresh season manifest created before a crash still completes with zero network;
3. historical match raw remains reusable across runs, preserving #1212/#1213;
4. the DAG still drains recovery before historical seeding.

Run:

```bash
/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fbref_pipeline.py tests/unit/dags/test_dag_backfill_fbref.py -q
```

Expected before implementation: only the new prior-season-raw test fails because `recovered_from_raw == 1`.

**Step 2: Implement the narrow recovery rule**

In `FBrefPipeline.fetch_wave`, separate exact crash recovery from cross-run historical reuse:

```python
exact_refresh_raw = self.raw_store.has_fetch(lease.logical_refresh_id)
cross_run_historical_raw = (
    frontier.get("refresh_policy") == "historical_once"
    and lease.page_kind != "season"
)
recoverable = exact_refresh_raw or cross_run_historical_raw
```

Keep the rule deliberately narrow. Do not weaken raw integrity checks and do not change match identity handling.

**Step 3: Run the focused suite**

Run the command from Step 1.

Expected: all focused pipeline and backfill topology tests pass.

## Task 3: Admit Legitimate Large `season_stats` Pages Safely

**Files:**

- Modify: `scrapers/fbref/settings.py`
- Modify: `scrapers/fbref/fetcher.py`
- Modify: `dags/utils/fbref_pipeline_tasks.py`
- Modify: `dags/utils/fbref_current_dag_factory.py`
- Modify: `dags/dag_backfill_fbref.py`
- Modify: `dags/utils/fbref_bronze_acceptance_tasks.py`
- Modify: `tests/unit/scrapers/test_fbref_fetcher.py`
- Modify: `tests/unit/scrapers/test_fbref_pipeline.py`
- Modify: `tests/unit/dags/test_dag_ingest_fbref.py`
- Modify: `tests/unit/dags/test_dag_backfill_fbref.py`
- Modify: `tests/unit/dags/test_fbref_pipeline_tasks.py`
- Modify: `tests/unit/dags/test_fbref_bronze_acceptance_tasks.py`

**Step 1: Write failing body-limit and reservation tests**

Add tests proving:

- ordinary kinds still abort above 2 MiB;
- `season_stats` accepts a body above 2 MiB and up to 4 MiB;
- `season_stats` aborts immediately above 4 MiB;
- the per-target reservation is at least the largest decoded-body limit plus the 1 MiB wire-overhead reservation;
- current, backfill, replay, and acceptance tasks all pass the same reservation value instead of a hard-coded 3 MiB.

Run:

```bash
/root/.venvs/dpf-test/bin/pytest \
  tests/unit/scrapers/test_fbref_fetcher.py \
  tests/unit/scrapers/test_fbref_pipeline.py \
  tests/unit/dags/test_dag_ingest_fbref.py \
  tests/unit/dags/test_dag_backfill_fbref.py \
  tests/unit/dags/test_fbref_pipeline_tasks.py \
  tests/unit/dags/test_fbref_bronze_acceptance_tasks.py -q
```

Expected before implementation: the new `season_stats` and 5 MiB reservation assertions fail.

**Step 2: Implement a page-kind-specific cap**

Keep the default 2 MiB body cap. Add a 4 MiB `season_stats` cap and derive `DEFAULT_REQUEST_RESERVATION_BYTES` from the largest supported body plus the existing 1 MiB wire overhead. The fetcher must choose the limit before constructing `_CumulativeBodyBuffer`, retaining streaming abort behavior and cumulative accounting across retries.

Replace every DAG/acceptance hard-coded `reservation_mb=3` with `DEFAULT_REQUEST_RESERVATION_BYTES // MIB`.

**Step 3: Run the focused suite**

Run the command from Step 1.

Expected: all tests pass; no body can be buffered beyond its page-kind cap.

## Task 4: Isolate a Bounded Transient Match 404

**Files:**

- Modify: `scrapers/fbref/pipeline.py`
- Modify: `tests/unit/scrapers/test_fbref_pipeline.py`
- Modify: `tests/unit/dags/test_run_fbref_live_waves_runner.py` if the result schema assertion is explicit.

**Step 1: Write failing target-local tests**

Add tests proving:

- one match 404 records a failed attempt, returns that target as `queued/skipped`, and still fetches a healthy sibling;
- a non-match 404 remains a loud `FetchWaveError`;
- a cohort-wide majority of match 404s trips a mass-failure circuit;
- more than 25 isolated match 404s over a run trips a run-level circuit;
- serialized live-wave output exposes the deferred-404 count.

Run:

```bash
/root/.venvs/dpf-test/bin/pytest \
  tests/unit/scrapers/test_fbref_pipeline.py \
  tests/unit/dags/test_run_fbref_live_waves_runner.py -q
```

Expected before implementation: the new single-match-404 test raises `FetchWaveError` and the new counter is absent.

**Step 2: Implement narrow reversible isolation**

Classify only `page_kind == "match"`, `error_class == "http_status"`, and HTTP 404 as transient. Reuse the existing durable `fail_fetch(..., requeue=True)` outcome, count it separately from permanent redirects, reset the suspect session, continue siblings, and keep per-wave/per-run mass ceilings. Do not swallow 403/429, non-match 404, 5xx exhaustion, transport-policy failures, or oversized bodies.

**Step 3: Run the focused suite**

Run the command from Step 1.

Expected: all target-local tests pass and mass failures still fail closed.

## Task 4A: Keep Future Oversized Pages Target-local

**Files:**

- Modify: `scrapers/fbref/pipeline.py`
- Test: `tests/unit/scrapers/test_fbref_pipeline.py`

**Step 1: Add the failing target-local and circuit tests**

Prove that one `response_too_large` target remains a durable terminal failure,
but does not discard healthy siblings or prevent the run from reaching its
existing validation gates. Add the same bounded mass circuit as other
target-local exceptions: fail a wave only above five and above half its cohort,
and fail a run above 25 terminal oversized pages.

**Step 2: Implement the narrow aggregate outcome**

Add an explicit `WaveResult` counter. Keep the existing permanent/dead frontier
transition and paid-attempt evidence, continue siblings, and preserve all other
failure classes. Requeue untouched leases before stopping a mass-failure wave.

**Step 3: Run focused tests and provider-meter regressions**

Expected: one oversized target no longer freezes accepted current publication;
a cohort-wide oversized response remains red and no settlement invariant moves.

## Task 4B: Isolate Current and Backfill Recovery Lanes

**Files:**

- Modify: `scrapers/fbref/control/store.py`
- Modify: `scrapers/fbref/pipeline.py`
- Modify: `dags/dag_backfill_fbref.py`
- Test: `tests/unit/scrapers/test_fbref_control_store_v8.py`
- Test: `tests/unit/scrapers/test_fbref_pipeline.py`
- Test: `tests/unit/dags/test_dag_backfill_fbref.py`

**Step 1: Add failing lane-isolation tests**

Prove that current recovery cannot select backfill raw and vice versa. Prove
that the current validation gate uses the current-lane overdue raw count while
still reporting the global count, and that backfill defaults to
`publish=false`.

**Step 2: Implement explicit lane selectors**

Filter cross-run raw recovery by the invoking `PipelineSettings.run_type`.
Expose both lane-specific and global unprocessed-raw SLA metrics. Validation
must fail on the invoking lane's overdue raw, not another lane's backlog; the
global metric remains visible for monitoring and offline repair. Keep replay as
an explicit source-run workflow. Change the backfill DAG parameter default to
non-publishing.

**Step 3: Verify fail-closed boundaries**

Unknown run types must be rejected. Same-lane failed/cancelled raw must remain
recoverable, and source-run identity/stateful provenance must stay unchanged.
The shared publication fence and paid pool remain deliberately serialized until
cross-process Trino publication is proven safe.

## Task 5: Add Exact, Bounded Remediation SQL

**Files:**

- Create: `docs/operations/sql/fbref_20260825_reanimate_large_pages.sql`
- Create: `docs/operations/sql/fbref_20260825_reopen_false_season_quarantines.sql`
- Create: `tests/unit/sql/test_fbref_20260825_remediation_sql.py`

**Step 1: Write static safety tests**

Assert that each SQL file:

- starts a transaction;
- takes an advisory lock or locks the selected frontier rows;
- refuses to operate while an FBref crawl run is active;
- constrains page kind, state, error class, and reason;
- has a strict bounded row-count assertion;
- changes only scheduling/error fields on `page_frontier`;
- for the exact four false-season rows only, clears `last_etag` and
  `last_modified` so the forced network request cannot answer 304 and reuse
  the stale `last_content_hash`; the hash itself remains as audit evidence;
- returns the exact affected target IDs;
- does not delete raw, attempts, manifests, or run history.

**Step 2: Implement the remediation SQL**

Large-page remediation may reopen at most 25 `season_stats` targets whose exact state is `dead` and `last_error_class='response_too_large'`.

Season remediation may reopen only:

```text
fbref:season:15:2025-2026
fbref:season:16:2025-2026
fbref:season:20:2025-2026
fbref:season:34:2025-2026
```

and only when all four are `quarantined` with `ParseContractQuarantined / schedule_season_mismatch`.

**Step 3: Test SQL statically, then dry-run its selection in production**

Run:

```bash
/root/.venvs/dpf-test/bin/pytest tests/unit/sql/test_fbref_20260825_remediation_sql.py -q
```

Before applying any update, execute only the files' read-only preflight queries and archive the results in the deployment report.

## Task 6: Fix Campaign Completion and Raise Persistence Batch Utilization

**Files:**

- Modify only during a stopped-driver window: `/root/fbref_history_backfill/driver.sh`
- Create: `/root/fbref_history_backfill/test-driver-20260825.sh`

**Step 1: Stop safely before editing**

Wait for the active history DAG to finish, create `state/stop`, and verify both driver and runner have exited. Do not edit a Bash file while the live shell may still read later lines from it.

**Step 2: Write a shell regression harness**

Use command stubs to prove:

- three zero-request successes do not declare completion while `backlog_history > 0`;
- three zero-request successes with `backlog_history == 0` do declare completion;
- a DB error never counts as zero backlog;
- the live history conf defaults to shard 8 and max_batches 20;
- current-active and protected-window guards still prevent a trigger.

**Step 3: Patch the driver**

- Change the history shard default from 3 to 8; keep max_batches at 20.
- Count a zero-spend completion stamp only when the post-run historical backlog is exactly zero.
- Reset the stamp when backlog is nonzero and alert on an impossible zero-spend/nonzero-backlog pattern.
- Update stale header comments to the deployed 4096-request/2048-MiB safety circuit and actual 8x20 history profile.

**Step 4: Run shell syntax and harness checks**

```bash
bash -n /root/fbref_history_backfill/driver.sh
bash /root/fbref_history_backfill/test-driver-20260825.sh
```

Expected: both exit 0 without touching Airflow, PostgreSQL, proxy, or state files.

## Task 7: Full Verification, Independent Review, and PR

**Files:** all files changed above.

**Step 1: Run formatting and static checks**

```bash
/root/.venvs/dpf-test/bin/ruff format --check scrapers/fbref dags tests/unit
/root/.venvs/dpf-test/bin/ruff check scrapers/fbref dags tests/unit
git diff --check
```

**Step 2: Run FBref unit and DAG smoke suites**

```bash
/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fbref_*.py tests/unit/dags/test_*fbref*.py tests/unit/sql/test_fbref_*.py -q
/root/.venvs/dpf-test/bin/pytest tests/integration/dag_smoke/test_dag_ingest_fbref.py -q
```

**Step 3: Run independent specification and code-quality reviews**

Require no unresolved Critical/Important finding. Review specifically:

- reservation/body-limit invariant;
- target-local classifier narrowness and mass circuits;
- exact-vs-cross-run season recovery;
- SQL remediation target bounds;
- current/history serialization and rollback.

**Step 4: Commit and open a draft PR**

Use a conventional commit such as:

```text
fix(fbref): stabilize current ingestion and history recovery
```

Push the feature branch, open a draft PR linked to #945 and #1186, wait for required CI, then mark ready only after reviewer approval.

## Task 8: Controlled Production Delivery

**Files:**

- Create: `/root/fbref-production-20260825/deliver.sh`
- Create: `/root/fbref-production-20260825/rollback.sh`
- Create: `/root/fbref-production-20260825/manifest.sha256`

**Step 1: Freeze the operating window**

Verify no FBref DAG is queued/running, no paid proxy lease is open, and the publication lock is free. Stop the history driver as described in Task 6.

**Step 2: Build an in-place delivery script**

The script must:

- verify source and destination hashes before mutation;
- compile every Python source before delivery;
- overwrite only existing production files in place, never create/rename within attested directories;
- verify directory ctime is unchanged;
- verify byte-for-byte equality after delivery;
- import `scrapers.fbref.pipeline` in the scheduler;
- verify Airflow DAG import errors remain zero;
- leave a complete before/after hash manifest.

Rollback must restore the exact pre-delivery bytes with the same in-place constraints.

**Step 3: Deliver only the merged, CI-green SHA**

Do not deliver an unmerged worktree or a dirty production file. Record the merged SHA and deployed file hashes.

## Task 9: Apply Remediation and Run Separate Canaries

**Step 1: Apply exact season remediation**

With the driver stopped and no active run, execute the four-target season SQL transaction. Verify exactly four rows returned.

**Step 2: Apply large-page remediation**

Execute the bounded response-too-large SQL transaction. Verify 1–25 rows and archive exact IDs.

**Step 3: Current acceptance**

Trigger one non-publishing `100 requests / 50 MiB` current canary, then two consecutive production current runs. Required evidence for both production runs:

- DAG/control run succeeds;
- raw audit, freshness validation, and run validation execute;
- all reanimated `season_stats` selected by the run complete without `response_too_large`;
- a match 404, if observed, is durable and isolated without masking a mass outage;
- provider meter reconciles exactly;
- current publication-critical scope is within SLA.
- the source-triggered Silver DAG and its FBref DQ tasks succeed without requiring xref or Gold to redefine the source verdict.

If no known large page is selected by the bounded canary, do not infer the 4 MiB fix is accepted; use the production current run as the required live proof.

**Step 4: History throughput canary**

Run one non-publishing history DAG with `shard_size=8, max_batches=20`. Compare with the pinned last-10 baseline:

- durable targets per active hour;
- provider-billed bytes per durable target;
- requests per durable target;
- raw/generic/typed validation counts;
- queue delta and new-seed delta;
- no current-window overlap.

Acceptance: no correctness regression, no proxy-bytes-per-target regression above 10%, and a material throughput improvement. If accepted, restart the campaign driver with the new defaults; otherwise rollback only the driver profile to 3x20 while retaining correctness fixes.

## Task 10: Production Verdict and Durable Report

**Files:**

- Complete: `docs/reviews/fbref/2026-08-25-production-readiness.md`
- Create: `/tmp/fbref-production-review-2026-08-25.html`

**Step 1: Recompute live metrics**

Report current and backfill independently. Never combine their denominators.

**Step 2: State the verdict by layer**

- Current Raw/generic Bronze and publication-critical typed Bronze.
- Historical Raw/generic Bronze.
- Historical typed/Silver coverage for squad/player/matchlog.
- Proxy cost and campaign ETA.

**Step 3: Keep offline typed work explicit**

The missing squad/player/matchlog typed parsers cover 89.47% of the open historical queue. They must be implemented and replayed from immutable Raw as a separate zero-network workstream; they are not permission to refetch already-valid Raw.

**Step 4: Final verification snapshot**

Record exact test counts, CI URLs, merged/deployed SHA, DAG run IDs, provider bytes, target counts, remaining queue, and rollback location. Claim `GO` only for layers whose live acceptance passed; label the rest `CONDITIONAL` or `NO-GO` with an owner and next gate.
