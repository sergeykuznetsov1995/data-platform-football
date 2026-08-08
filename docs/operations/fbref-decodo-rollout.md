# FBref: staged Decodo rollout and rollback

This is an operator checklist, not permission to deploy. No live or paid
request was made while this document was prepared. Each stage needs saved
evidence and an explicit GO before the next stage.

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
`0/10/30/60/90/120` minutes and the only destination is Decodo's IP-check
service:

```bash
/root/.venvs/dpf-test/bin/python \
  scripts/research/bench_fbref_decodo_session.py \
  --proxy-file /deployment-owned/protected-candidate-file \
  --output /protected-evidence/fbref-decodo-sticky.json
```

The report may contain timestamps and exit hashes only. Any missed probe by
more than 60 seconds, raw IP, changed exit, request to FBref, or secret in the
artifact is NO-GO. A provider counter that has not moved yet is `pending`, not
zero usage; wait for its normal reporting delay and capture the later counter.

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

3. Run the actual offline replay pipeline once with `FBREF_BATCH_PERSIST=0`.
4. Restore the identical control/raw baseline, then run it in the second
   schema with `FBREF_BATCH_PERSIST=1`. Keep the same frozen cohort and parser
   versions. Both replays must show zero proxy requests and zero proxy bytes.
   The replay launcher must write a protected metrics artifact for each run;
   hand-entered times, match counts, or statement counts are not accepted.
   Each artifact uses schema version `fbref-pipeline-run-metrics-v1` and binds
   its mode, Trino schema, control run ID, elapsed seconds, positive statement
   counts, match count, and match-key SHA-256 to that exact replay.
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
     --sequential-metrics SEQUENTIAL_METRICS_JSON \
     --batch-metrics BATCH_METRICS_JSON --output STRICT_EVIDENCE_JSON
   ```

The evidence tool rejects a metrics file if its control run ID, mode, schema,
match count, or match-key digest differs from direct PostgreSQL evidence. It
also verifies that every non-sentinel Trino row carries that replay's own
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

Every row, including `TOTAL`, must be `PASS`. This proves deduplicated schedule
keys, a successful current-run page manifest for every played match, all eight
current-run dataset decisions per played match, current-run physical rows for
`available`, a nonblank reason plus zero rows for explicit empty results, and
direct durable/dead control evidence. Save both result sets with the replay
JSON.

## 4. Bounded live canary

Use the isolated, non-publishing acceptance project first. Keep its unchanged
limit of **100 requests / 50 MiB**, one active paid lease, the 6.1-second source
interval, and raw-first writes. Confirm the selected exit hash stays fixed for
the lease and every reservation is settled. Hitting either limit is NO-GO.

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

1. enable daily;
2. observe one complete scheduled run and provider reconciliation;
3. enable drain for the existing backlog;
4. keep only one FBref paid run active at a time.

## Rollback

Stop new FBref runs, let the one active lease close, and verify all reservations
are settled. Set `FBREF_BATCH_PERSIST=0`, restore the **previous proxy file**
and **previous image digest**, and keep the raw/control evidence. Do not delete
raw documents or successful manifests. Re-run the zero-network completeness
gate before resuming the old path.

Rollback is complete only when there is no active lease, no reserved byte or
request balance, and the previous image/file fingerprints are recorded. If a
credential may have leaked, rotate it again instead of restoring it.
