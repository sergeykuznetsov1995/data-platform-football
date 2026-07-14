# WhoScored production operations

This document is the deploy and acceptance contract for the WhoScored source.
It covers source ingestion, the current SeaweedFS S3-compatible store,
Airflow orchestration, historical replay, proxy policy, DQ and recovery.

## Invariants

- Catalog discovery includes every source tournament classified as senior men.
  Women, youth, reserve and academy competitions are excluded with persisted
  classification evidence.
- Scheduled daily and historical Airflow runs are direct-only and must report
  zero paid-proxy bytes. Paid fallback is available only through the manual CLI
  with the explicit `--allow-paid-proxy` flag and the proxy-filter byte cap.
- Raw source observations are append-only. A successful receipt is published
  only after final-object readback; quarantine invalidates the exact failed
  observation and cannot hide a concurrent healthy writer.
- A backfill plan is immutable and resumable from S3 by exact
  `queue_id + plan_id`. One DagRun maps at most 100 work items and schedules an
  idempotent continuation until the plan and historical DQ are complete.
- Daily publication is successful only after dataset/manifests DQ and the
  zero-paid traffic gate. Backfill success additionally requires historical DQ
  over all 25 business datasets, exact parser/availability proofs and roster to
  profile coverage.
- Daily profile work freezes the exact missing/due-retry/version-stale player
  set after roster ingestion. Its count and SHA-256 are verified again before
  source transport is constructed. The task processes the complete set or the
  planner fails before source traffic when it exceeds the configured hard cap
  (maximum 3,000); it never publishes an inevitably partial daily repair.
- The runtime topology is LocalExecutor. `whoscored_direct_pool` has two slots,
  `whoscored_dq_pool` has two slots and `whoscored_storage_pool` has one slot.
- The complete cold path must finish within 6 hours; normal daily rolling p95
  must stay within 4 hours; a full-history backfill plan must finish within 30
  days. The DAGs enforce these deadlines in addition to task-level timeouts.
  The current two-slot source topology cannot yet prove the modeled all-catalog
  throughput, so production activation of that plan remains fail-closed until
  a sustained representative canary justifies an enforced topology/rate change.

## Before and after

| Area | Before | After |
| --- | --- | --- |
| Source scope | Static configured subset; no enforceable complete senior-men history | Runtime full-history source discovery with persisted classification/provenance; women/youth/reserve/academy excluded |
| Stage/player data | Player feed could stop after the first page; a dead legacy stage feed remained | Bounded full pagination with page/cardinality invariants; all current stage feeds parsed; dead feed removed |
| Memory and write speed | Full stage history expanded into millions of Python dictionaries and one monolithic fingerprint/write, growing past 7.6 GiB in the live canary | Stage-atomic SQLite spool, streaming fingerprints and <=20k-row Iceberg chunks; the successful EPL cold/warm/incremental canary peaked at 757.22 MiB (an interrupted World Cup traversal stayed at 0.24-0.51 GiB) |
| Missing data | Broad parser errors could become durable `not_available` | Only a typed absence plus valid source markers can become current-version `not_available`; malformed/unsupported payloads remain retryable/failed |
| Raw S3 write | Latest-object selection and quarantine could race; no end-to-end receipt/readback contract | Append-only versioned objects, content hash, readback-before-receipt, retry, exact invalidation and LocalExecutor snapshot locks |
| S3 recovery | Single `weed mini` process and no verified off-host restore path | Supervised master/volume/filer/S3 planes, fail-closed cutover, daily raw+ops and cutover full-bucket content-addressed off-host backups, marker verification and empty-target restore/readback |
| Airflow backfill | Mutable candidate selection and no durable automatic continuation | Frozen matches/profiles, integrity-checked S3 plan/receipts, bounded dynamic mapping and deterministic continuation |
| Backfill checkpoints | Recovery materialized the cumulative receipt history and latest-state lookup grew with generations | Checkpoint v3 keeps a compact frontier snapshot plus at most 63 bounded deltas, a <=64 KiB index, and a 12-level radix lookup; full receipts are materialized only for terminal DQ/recovery |
| Backfill capacity | Every match was charged for a preview and the configured requests/day value could exceed neither a throttle nor a proven source ceiling | Policy v6 charges exact match plus frozen preview identities; the SLO-only assumption is capped at the deployed 86,400 page-request units/day ceiling, reports observed wall-clock throughput, and stops before the next source batch on breach |
| Daily profiles | Limit estimated as roster/90, so a first run, outage or parser bump could scrape only a prefix and then fail full-coverage DQ | One exact shared candidate predicate; count/set hash pinned through planner, CLI report and DQ; complete repair or pre-source hard-cap failure |
| Proxy use | Manual/default behavior could select the paid endpoint implicitly | Daily/backfill structurally direct-only; manual paid use requires an explicit flag and authenticated capped lease |
| DQ | Primarily task success and partial current-run checks | Snapshot-pinned frozen scope/match/profile identities, exact stage/feed proof, owner-level parity across 25 datasets, NULL-identity duplicate sentinels, parser/availability proof, profile terminal proof and zero-paid gates; the complete historical read is 33 cardinality-invariant queries |
| Iceberg failure handling | Metadata corruption repair could drop/recreate a table; maintenance could return green with partial failures and had no safe live-file compaction | Corruption and partial maintenance fail closed; delete-safe exact-path compaction is bounded to 64 files/512 MiB per table and 4 tables/2 GiB per task; effective retention is 30d weekly, 14d daily for WhoScored and 3d for other high-churn feeds |

## Deployment prerequisites

1. Provision `configs/seaweedfs/s3.config.json` from the tracked example with
   separate platform-admin, WhoScored writer and backup-reader identities.
2. Rotate both `PROXY_FILTER_CONTROL_TOKEN` and the legacy
   `SOFASCORE_PROXY_CONTROL_TOKEN` if either value has appeared in logs or a
   terminal transcript.
3. Provision an off-host S3-compatible destination with Object Lock or an
   equivalent versioned WORM policy. Configure a distinct endpoint and site ID:
   `WHOSCORED_BACKUP_DESTINATION_URI`, endpoint, credentials,
   `WHOSCORED_BACKUP_DESTINATION_SITE_ID`, and
   `WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE`.
4. Keep restore credentials empty during normal operation. Supply them only for
   an approved drill or recovery to a new/empty destination.
5. Render the production Compose model before changing services:

   ```bash
   docker compose --env-file "$COMPOSE_ENV_FILE" config --quiet
   ```

## Idempotent Airflow migration

Do not assume a previously completed `airflow-init` container has created new
pools. Apply the pool contract explicitly during deployment:

```bash
docker compose --env-file "$COMPOSE_ENV_FILE" exec airflow-scheduler bash -euc '
  airflow pools set "$WHOSCORED_DIRECT_POOL" 2 \
    "WhoScored direct-only scopes; backfill consumes both slots"
  if [[ "$WHOSCORED_BACKFILL_POOL" != "$WHOSCORED_DIRECT_POOL" ]]; then
    airflow pools set "$WHOSCORED_BACKFILL_POOL" 2 \
      "WhoScored resumable direct-only backfill"
  fi
  airflow pools set "$WHOSCORED_DQ_POOL" 2 \
    "WhoScored bounded Iceberg data-quality queries"
  airflow pools set whoscored_storage_pool 1 \
    "WhoScored serialized backup and restore verification"
'
```

Confirm `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, then restart the scheduler so
it loads the new DAG and environment. A missing pool is a deployment failure,
not a reason to increase task-level parallelism.

New DAGs may be created paused. Keep the backup and daily ingest paused until
their first manual acceptance run passes; then explicitly enable both schedules:

```bash
docker compose --env-file "$COMPOSE_ENV_FILE" exec \
  airflow-scheduler airflow dags unpause dag_backup_whoscored_storage
docker compose --env-file "$COMPOSE_ENV_FILE" exec \
  airflow-scheduler airflow dags unpause dag_ingest_whoscored
```

`dag_backfill_whoscored` intentionally has no schedule and remains a
manual/continuation-only DAG. Verify pause state and the next-run timestamps in
the Airflow UI or `airflow dags list` after the scheduler heartbeat.

## Storage activation and recovery

First run `dag_backup_whoscored_storage` and require successful raw and ops
inventories, copies, immutable completion markers and destination readbacks. A
successful local inventory without a committed off-host marker is not a backup.

The existing live `weed mini` volume must never be opened concurrently by the
new storage planes. Follow
[the SeaweedFS cutover runbook](seaweedfs-topology-cutover.md); the cutover
script stops writers, backs up the complete warehouse bucket, starts each plane
in dependency order, verifies every cut object through the new S3 gateway and
only then restarts the previously running writers.

The off-host completion marker contains the complete inventory, so recovery
does not depend on the primary host's Airflow logs. On the recovery deployment,
list recent immutable markers and inspect their keys/timestamps first:

```bash
docker compose --env-file "$COMPOSE_ENV_FILE" run --rm --no-deps \
  --entrypoint bash airflow-scheduler -euc '
    python /opt/airflow/scripts/whoscored_raw_backup.py list-inventories \
      --store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --expected-source-uri "$WHOSCORED_RAW_STORE_URI" --limit 20
  '
```

Select the exact approved marker key from that output; do not infer or silently
fall back to another backup. For a recovery drill, fetch and authenticate that
marker, then restore only to an empty target:

```bash
export RECOVERY_TARGET_URI=s3://approved-empty-recovery-bucket/whoscored
export RECOVERY_EXPECTED_SOURCE_URI=s3://original-bucket/raw/whoscored
export RECOVERY_INVENTORY_KEY=backup-inventories/EXACT_KEY_FROM_LIST.json
docker compose --env-file "$COMPOSE_ENV_FILE" run --rm --no-deps \
  --entrypoint bash -e RECOVERY_TARGET_URI -e RECOVERY_EXPECTED_SOURCE_URI \
  -e RECOVERY_INVENTORY_KEY \
  -e WHOSCORED_RAW_LOCK_DIR=/tmp/whoscored_restore_locks \
  airflow-scheduler -euc '
    RECOVERY_INVENTORY=/tmp/whoscored-recovery-inventory.json
    python /opt/airflow/scripts/whoscored_raw_backup.py fetch-inventory \
      --store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --inventory-key "$RECOVERY_INVENTORY_KEY" \
      --expected-source-uri "$RECOVERY_EXPECTED_SOURCE_URI" \
      --output "$RECOVERY_INVENTORY"
    python /opt/airflow/scripts/whoscored_raw_backup.py restore \
      --backup-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --restore-uri "$RECOVERY_TARGET_URI" \
      --inventory "$RECOVERY_INVENTORY" --apply --create-bucket
    python /opt/airflow/scripts/whoscored_raw_backup.py verify-restore \
      --store-uri "$RECOVERY_TARGET_URI" \
      --inventory "$RECOVERY_INVENTORY"
  '
```

Omit `--create-bucket` for a pre-provisioned recovery bucket. Never restore over
a non-empty destination.

## Canary and full backfill

Run the non-publishing direct-source workflow benchmark first. It uses a
temporary local raw store and an in-memory repository:

```bash
docker compose --env-file "$COMPOSE_ENV_FILE" run --rm --no-deps \
  --entrypoint python -e WHOSCORED_RAW_LOCK_DIR=/tmp/whoscored_raw_locks \
  -e WHOSCORED_LOCK_DIR=/tmp/whoscored_commit_locks airflow-scheduler \
  /opt/airflow/scripts/research/bench_whoscored_workflow.py \
  --scope 'INT-World Cup=2026' --match-limit 1 --profile-limit 1 \
  --flaresolverr-url http://flaresolverr:8191
```

Then trigger one daily DagRun and require all DQ/traffic/SLO gates to pass.

## Backfill capacity and the 30-day activation gate

Request-unit policy v6 counts one unit for every frozen match page, one for
every frozen preview page, one for every profile page, and 70 units per frozen
source stage for schedule work. This removes the old blanket `matches * 2`
estimate: a match without a source preview no longer consumes imaginary
capacity, while every `preview_game_id` is still accounted exactly.

`WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY` is only an SLO planning
assumption. It does not increase an Airflow pool, change a source limiter, or
throttle a worker. The accepted range is 1,000..86,400; the maximum is the
deployed two source slots times the slower 30 page requests/minute times 1,440
minutes/day. The structured-feed limiter is faster, but it cannot justify a
larger scalar assumption for the match/preview/profile-heavy portion.

Before each batch is created, the controller compares the durable remaining
lower bound with the immutable deadline. Schedule receipts make match and
preview counts exact; later roster receipts make profile counts exact. If a
newly exact population breaches the ceiling, no next source batch or automatic
continuation is created. After at least six wall-clock hours and 1,000 completed
units, the SLO summary also reports observed units/day and projected remaining
days. That observation is deliberately advisory: pauses and incidents must be
visible, but do not redefine or silently extend the agreed deadline.

The current scale evidence blocks an all-catalog production run. With 1.9M
modeled matches and 15,979 observed source stages, the old blanket estimate was
at least 4,918,530 units, or 163,951/day before profiles. Applying the current
live ratios (18,143 preview keys and 5,991 profile keys per 56,392 schedule
keys) gives a sensitivity estimate of about 3.832M units, or 127,722/day. More
importantly, the estimated 2.713M page-limited match/preview/profile units alone
need about 31.4 days at the enforced 86,400/day ceiling, before schedule work,
Airflow overhead, retries, or source failures. These ratios are sizing evidence,
not a frozen all-catalog population.

Do **not** trigger the all-catalog crawl with the current topology. First run a
sustained, representative non-publishing canary that includes historical match,
preview, profile, and multi-stage work. Any approved concurrency/rate change
must update the enforced pool/limiter, the hard capacity ceiling, tests, and
this runbook together. Only after that evidence makes the exact preflight
sufficient may the production crawl be started:

```bash
docker compose --env-file "$COMPOSE_ENV_FILE" exec \
  airflow-scheduler airflow dags trigger dag_backfill_whoscored \
  --conf '{"all_catalog":true,"direct_only":true,"require_zero_paid":true}'
```

Do not trigger a second selector for the same crawl. The first run stores the
immutable plan and queues deterministic continuation runs. Completion means:

- plan status is `complete` and no schedulable work remains;
- full-history discovery provenance is bound to the plan;
- historical DQ passes for all frozen eligible scopes and 25 datasets;
- every eligible roster player has a current profile or typed terminal proof;
- aggregate paid proxy bytes are zero; and
- the plan completed no later than its 30-day deadline.

## Validation evidence

The change is accepted only when the isolated WhoScored CI contract, Compose
rendering, shell syntax, a temporary-volume four-plane SeaweedFS smoke test and
the non-publishing live-source canary pass. Record the exact test counts and
canary artifact in the change/PR description. The repository-wide suite also
contains unrelated live/browser integration checks and is not a substitute for
this deterministic source contract. Off-host backup/cutover and the 30-day
historical crawl are deployment evidence and cannot be replaced by unit tests.

### Implementation evidence recorded 2026-07-14

- The live catalog snapshot used to size the frozen checks contained 7,472
  eligible senior-men scope-seasons and 15,979 source stages. The DQ scale
  tests use the same scope cardinality. These numbers are observations, not a
  static allowlist; full-history discovery remains source-driven.
- The isolated WhoScored CI selection passed 1,098 tests with zero skips or
  failures. Ruff, Python compilation, shell syntax, `git diff --check`, and
  production Compose rendering also passed.
- An isolated S3-compatible smoke test used four separate ephemeral SeaweedFS
  4.36 planes (master, volume, filer and S3 gateway), a temporary volume and
  synthetic credentials. The production raw-store implementation wrote two
  append-only observations and two immutable receipts for one target, selected
  the latest observation, and verified its SHA-256 and size on readback. The
  operational store created a v3 radix checkpoint with a 630-byte index; raw
  and checkpoint reads passed again after restarting the S3 gateway. All
  temporary containers/network/volume were removed; production storage was not
  touched.
- Checkpoint scale tests cover a 75,000-work frontier and 1,700 immutable
  generations. The latest v3 lookup performs at most 13 non-recursive LISTs,
  each radix level has at most ten children, and normal continuation does not
  rescan the receipt population.
- All 33 final historical DQ query shapes compiled and executed read-only on
  Trino 482 in 152.957 s sequentially; the largest distributed plan had 140 of
  the enforced 150 stages. The feed query round-tripped all 7,472 scope
  identities. This was a shape/cardinality benchmark over 56,392 real schedule
  keys, 18,143 previews and 5,991 profiles with the not-yet-deployed staged
  relation emulated read-only; it is not production DQ truth or a 1.9M-match
  acceptance run. Monitor the ten-stage margin after Trino upgrades.
- The live read-only compaction selector chose three clean data files totaling
  20,857,189 bytes in 1.482 s with zero delete-file partitions skipped. A
  distributed EXPLAIN of the exact `$path` OPTIMIZE passed; no maintenance DML
  was executed against production during validation.
- The non-publishing `EPL=2526` workflow canary completed all three phases.
  Cold: 107.063 s, 87 source attempts, 12.311117 MiB direct traffic and 464,681
  parsed/accepted rows. Warm replay: 48.653 s, zero source requests, 84 raw
  cache hits and the same 464,681 idempotent rows. Incremental: 51.270 s, one
  request, 0.126776 MiB and 83 cache hits. Total paid-proxy traffic was zero;
  cgroup peak memory was 794,005,504 bytes (757.22 MiB). A deliberately
  corrupted raw match was quarantined and fetched again successfully.
- A separate non-publishing `INT-World Cup=2026` traversal failed closed on an
  external WhoScored HTTP 502 while loading the calendar. It made 807 direct
  attempts, transferred 47.897 MiB and used zero paid-proxy bytes. This is
  negative failure-handling evidence, not a successful canary.

These results accept the implementation only. Production activation still
requires provisioned off-host WORM credentials, a successful backup/empty-target
restore drill, the SeaweedFS cutover rehearsal, one green Airflow daily DagRun,
a sustained representative throughput canary and approved source-topology/rate
change that clears the 30-day capacity gate, and then completion of the frozen
all-catalog historical plan within its 30-day deadline.

### Runtime activation audit recorded 2026-07-14

The currently running Docker project is `/root/dpf-fbref-949-release`, not this
production-hardening worktree. Its mounted WhoScored DAG and raw-store hashes do
not match this branch. The live storage service is still one `weed mini`
container; the supervised master/volume/filer/S3 topology is not active. The
running Airflow image does not contain the backup DAG or frozen-DQ staging
module, has `whoscored_dq_pool=3`, and has no `whoscored_storage_pool`.

Therefore no production backfill or source canary was started in this session:
doing so would test the old release and could create data that is not covered by
the new DQ contract. The next deployment must first promote this branch through
the normal release process, render the new Compose model, perform the backup-
gated SeaweedFS cutover, and re-run the pool/hash audit before enabling any
WhoScored task.
