# FBref: staged Decodo rollout and rollback

This is an operator checklist, not permission to deploy. No live or paid
request was made while this document was prepared. Each stage needs saved
evidence and an explicit GO before the next stage. The persistent session is
default-off: every existing environment keeps
`FBREF_PERSISTENT_HTTP_SESSION=0` until the dedicated stage below is accepted.

## 0. Protect the account

1. Rotate every Decodo credential visible in the issue screenshot before any
   canary. Treat the screenshot credential as permanently exposed.
2. Create a dedicated Decodo sub-user used only by FBref. Give it the smallest
   provider allowance needed for the canary.
3. Keep its single proxy entry only in a deployment-owned regular file. Set
   owner/group deliberately and mode `0640`. Airflow receives only the local
   metered proxy-filter address; it never receives the provider credential.
4. Record only a credential version/fingerprint. Never put a username,
   password, complete proxy entry, or complete proxy URL in logs or evidence.

## 1. Choose the sticky duration without touching FBref

Create separate protected candidate files for 60 minute, 90 minute, and 120
minute custom-sticky sessions. Test them one at a time. The probe schedule is
`0/10/30/60/90/115/120` minutes and the only destination is Decodo's IP-check
service:

```bash
/root/.venvs/dpf-test/bin/python \
  scripts/research/bench_fbref_decodo_session.py \
  --proxy-file /deployment-owned/protected-candidate-file \
  --output /protected-evidence/fbref-decodo-sticky.json
```

The report may contain timestamps and exit hashes only. Any missed probe by
more than 60 seconds, raw IP, changed exit on or before the 115-minute local
lease boundary, request to FBref, or secret in the artifact is NO-GO. A change
at the provider's exact 120-minute expiry is recorded but does not fail the
115-minute local-safety gate. A provider counter that has not moved yet is
`pending`, not zero usage; wait for its normal reporting delay and capture the
later counter.

The recommended configuration is a **120-minute provider sticky session** and
a **115-minute local lease**. The five-minute margin closes locally before the
provider can rotate the exit. A transport failure still closes the lease and
selects another Decodo session; an active lease is never repinned midway.

## 2. Prove the real pipeline with zero network

Use frozen successful raw documents and disposable PostgreSQL/Trino state.
Never point this stage at a fetch DAG or the proxy filter.

1. Create two isolated, empty, schema-identical Trino schemas and one
   disposable control database containing the two replay runs. Pre-seed the same outside-cohort match
   sentinel in all 12 tables: three generic tables, eight typed match tables,
   and dataset availability.
2. Capture Iceberg snapshot IDs and sentinel digests before either replay:

   ```bash
   /root/.venvs/dpf-test/bin/python scripts/research/bench_fbref_persistence.py \
     --capture-baseline --sequential-schema SEQUENTIAL_SCHEMA \
     --sentinel-match-id OUTSIDE_MATCH --output SEQUENTIAL_BASELINE_JSON
   /root/.venvs/dpf-test/bin/python scripts/research/bench_fbref_persistence.py \
     --capture-baseline --sequential-schema BATCH_SCHEMA \
     --sentinel-match-id OUTSIDE_MATCH --output BATCH_BASELINE_JSON
   ```

3. Run `dag_replay_fbref_bronze` once with
   `trino_schema=SEQUENTIAL_SCHEMA` and `persistence_mode=sequential`.
4. Restore the identical control/raw baseline, then run it in the second
   schema with `trino_schema=BATCH_SCHEMA` and `persistence_mode=batch`. Keep
   the same frozen cohort and parser versions. Both replays must show zero
   proxy requests and zero proxy bytes. The actual parse task measures its own
   monotonic elapsed time and Trino statements. It force-reprocesses every
   frozen successful match even though the accepted source already has a
   completed parser fence; it does not modify that source fence or its control
   manifests. PostgreSQL atomically stores a versioned artifact plus SHA-256
   in that run's protected metadata; there is no hand-entered timing, match
   count, or statement-count input.
5. Run the strict persistence evidence tool with both control run IDs. It must
   use dynamic installed columns, excluding only `_ingested_at` and
   `persisted_at`, and execute `EXCEPT ALL` in both directions for every one of
   the 12 tables.

   ```bash
   /root/.venvs/dpf-test/bin/python scripts/research/bench_fbref_persistence.py \
     --compare-existing --sequential-schema SEQUENTIAL_SCHEMA \
     --batch-schema BATCH_SCHEMA --sequential-control-run-id SEQUENTIAL_RUN_ID \
     --batch-control-run-id BATCH_RUN_ID --sentinel-match-id OUTSIDE_MATCH \
     --sequential-baseline SEQUENTIAL_BASELINE_JSON \
     --batch-baseline BATCH_BASELINE_JSON \
     --output STRICT_EVIDENCE_JSON
   ```

The evidence tool reads only the create-once metrics anchors from PostgreSQL
and rejects them if their artifact digest, control run ID, mode, schema, match
count, or match-key digest differs from direct evidence. It also verifies that
every non-sentinel Trino row carries that replay's own
`run_id`/`_batch_id`; only then does it map the two distinct IDs to one
comparison token. The lineage columns remain in the comparison.

The saved JSON must contain both elapsed times, Trino statement counts,
Iceberg snapshot before/after evidence, unchanged sentinel digests, direct
PostgreSQL observation/manifest/latest-state hashes, per-table left/right diff
counts, zero proxy deltas, speedup, seconds per match, and one top-level boolean
`passed`. GO requires zero differences, at least **4×** speedup, and at most
**20 seconds** per match. A digest alone is not acceptance.

## 3. Prove completeness

Run both read-only statements in
`docs/operations/sql/fbref_male_completeness_control.sql` against their named
engines. Every active/present male competition is crossed with source seasons
`2025-2026` and `2026-2027`; there is no hand-written competition count.

Give PostgreSQL a JSON list containing every successful publishing
current/backfill control run needed for the complete bounded workload, plus
the exact publication run ID. A single run is fine; when the workload is more
than one bounded run (including more than 2,000 targets), list every run. The
selected run is not an isolated replay: replay correctness was proved against
its frozen cohort in Step 2, while this gate proves the whole published male
scope.

Run PostgreSQL first. From its `TOTAL` row copy the canonical accepted-run,
expected-scope, and dead-match JSON/count/fingerprint bridge values into the
named Trino parameters. Trino recomputes each bridge. It also requires the
PostgreSQL competition-season set to equal the chosen publication generation,
requires every selected run to own a separate complete immutable scope export,
and rejects a duplicate, omitted, or stale run.

Every row, including `TOTAL`, must be `PASS`. This proves deduplicated schedule
keys, that the latest page manifest for every played match belongs to the
accepted run set, all eight same-run dataset decisions, same-run physical rows
for `available`, a nonblank reason plus zero rows for explicit empty results,
and direct latest durable/dead control evidence. Save both result sets with the
replay JSON.

## 4. Bounded live canary

Use the isolated, non-publishing acceptance project first. Keep its unchanged
limit of **100 requests / 50 MiB**, one active paid lease, the 6.1-second source
interval, and raw-first writes.

1. Run the existing path with `FBREF_PERSISTENT_HTTP_SESSION=0` and save its
   evidence as the baseline.
2. In the candidate acceptance image only, keep `FBREF_BATCH_PERSIST=0`, set
   `FBREF_PERSISTENT_HTTP_SESSION=1`, and run the same bounded cohort. This is
   the persistent-session stage; it is not a global configuration change.
3. Observe one session being reused only inside its lease, a fixed exit hash,
   per-page request/byte accounting, terminal tail settlement, and zero active
   reservations after the run. Any mid-lease repin, unmetered request, missing
   raw document, unsettled reservation, or completeness failure is NO-GO.

Hitting either safety limit is also NO-GO.

Reconcile Decodo dashboard bytes after its reporting delay against the local
provider-billed ledger. Compare **bytes per durable match** with the saved
control run; do not invent an absolute tariff-sized success cap.

## 5. Daily, then drain

After the bounded canary is green, run one deliberately bounded production
daily canary with the normal **4096 requests / 2048 MiB** emergency circuit.
The circuit is a runaway stop, not a target: reaching or exhausting it is a
loud incomplete-run failure. Require at least **1,500** fully persisted matches
per day, zero missing completeness rows, zero unsettled reservations, and the
expected bytes per durable match.

Then, and only then:

1. set `FBREF_PERSISTENT_HTTP_SESSION=1` and `FBREF_BATCH_PERSIST=1` in the
   candidate image and enable daily;
2. observe one complete scheduled run, its persistent-session accounting, and
   provider reconciliation;
3. enable drain for the existing backlog;
4. keep only one FBref paid run active at a time.

## Rollback

Stop new FBref runs, let the one active lease close, and verify all reservations
are settled. Set `FBREF_PERSISTENT_HTTP_SESSION=0` and
`FBREF_BATCH_PERSIST=0`, restore the **previous proxy file** and
**previous image digest**, and keep the raw/control evidence. Do not delete raw
documents or successful manifests. Re-run the zero-network completeness gate
before resuming the old path.

Rollback is complete only when there is no active lease, no reserved byte or
request balance, and the previous image/file fingerprints are recorded. If a
credential may have leaked, rotate it again instead of restoring it.
