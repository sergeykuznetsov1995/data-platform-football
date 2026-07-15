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
- The runtime topology is LocalExecutor. `whoscored_direct_pool` is controlled
  by `WHOSCORED_SOURCE_POOL_SLOTS`, whose runtime-enforced range is 2..4;
  `whoscored_dq_pool` has two slots. The safe deployment default is two source
  slots and promotion to four requires the sustained canary below.
- Every structured-feed browser start is admitted by one process-global
  FlareSolverr governor. Starts from different sessions and batches are at
  least 546 ms apart; callers cannot override the interval or accumulate idle
  burst credit.
- All LocalExecutor workers share
  `/opt/airflow/logs/whoscored/source-circuit-v1.json`. One authoritative
  direct-browser Cloudflare block opens a 15-minute cooldown, repeated failed
  half-open probes back off to 30 and then 60 minutes, each with 0..60 seconds
  of persisted jitter. Exactly one logical half-open probe is allowed.
  Production tasks fail fast while open; only the non-publishing capacity
  harness waits. Corrupt/insecure state fails before a rate token or network
  request, while a valid raw-cache hit is always checked first.
- The complete cold path must finish within 6 hours; normal daily rolling p95
  must stay within 4 hours; a full-history backfill plan must finish within 30
  days. The DAGs enforce these deadlines in addition to task-level timeouts.
  The all-catalog plan remains fail-closed until a sustained representative
  four-worker canary proves at least 144,000 page units/day.

## Before and after

| Area | Before | After |
| --- | --- | --- |
| Source scope | Static configured subset; no enforceable complete senior-men history | Runtime full-history source discovery with persisted classification/provenance; women/youth/reserve/academy excluded |
| Stage/player data | Player feed could stop after the first page; a dead legacy stage feed remained | Bounded full pagination with page/cardinality invariants; all current stage feeds parsed; dead feed removed |
| Memory and write speed | Full stage history expanded into millions of Python dictionaries and one monolithic fingerprint/write, growing past 7.6 GiB in the live canary | Stage-atomic SQLite spool, streaming fingerprints and <=20k-row Iceberg chunks; the successful EPL cold/warm/incremental canary peaked at 757.22 MiB (an interrupted World Cup traversal stayed at 0.24-0.51 GiB) |
| Missing data | Broad parser errors could become durable `not_available` | Only a typed absence plus valid source markers can become current-version `not_available`; malformed/unsupported payloads remain retryable/failed |
| Raw S3 write | Latest-object selection and quarantine could race; no end-to-end receipt/readback contract | Append-only versioned objects, content hash, readback-before-receipt, retry, exact invalidation and LocalExecutor snapshot locks |
| S3 recovery | Single `weed mini` process and no verified off-host restore path | Unchanged for this rollout by owner decision; single-host loss risk is explicitly accepted and the backup DAG remains paused |
| Airflow backfill | Mutable candidate selection and no durable automatic continuation | Frozen matches/profiles, integrity-checked S3 plan/receipts, bounded dynamic mapping and deterministic continuation |
| Backfill checkpoints | Recovery materialized the cumulative receipt history and latest-state lookup grew with generations | Checkpoint v3 keeps a compact frontier snapshot plus at most 63 bounded deltas, a <=64 KiB index, and a 12-level radix lookup; full receipts are materialized only for terminal DQ/recovery |
| Backfill capacity | Every match was charged for a preview and the configured requests/day value could exceed neither a throttle nor a proven source ceiling | Policy v6 charges exact match plus frozen preview identities; the hard ceiling is always `source slots * 30 * 1,440` (86,400..172,800), reports observed wall-clock throughput, and stops before the next source batch on breach |
| Daily profiles | Limit estimated as roster/90, so a first run, outage or parser bump could scrape only a prefix and then fail full-coverage DQ | One exact shared candidate predicate; count/set hash pinned through planner, CLI report and DQ; complete repair or pre-source hard-cap failure |
| Proxy use | Manual/default behavior could select the paid endpoint implicitly | Daily/backfill structurally direct-only; manual paid use requires an explicit flag and authenticated capped lease |
| Source block handling | Independent workers could retry the same block and browser batches could start together | One fixed process-global 546 ms actual-start governor plus a persistent 15/30/60-minute shared cooldown and one half-open probe; expected origin masks and ordinary 502/timeouts never authorize paid traffic |
| DQ | Primarily task success and partial current-run checks | Snapshot-pinned frozen scope/match/profile identities, exact stage/feed proof, owner-level parity across 25 datasets, NULL-identity duplicate sentinels, parser/availability proof, profile terminal proof and zero-paid gates; the complete historical read is 33 cardinality-invariant queries |
| Iceberg failure handling | Metadata corruption repair could drop/recreate a table; maintenance could return green with partial failures and had no safe live-file compaction | Corruption and partial maintenance fail closed; delete-safe exact-path compaction is bounded to 64 files/512 MiB per table and 4 tables/2 GiB per task; effective retention is 30d weekly, 14d daily for WhoScored and 3d for other high-churn feeds |

## Deployment prerequisites

Set the rollout paths once before running any command in this section:

```bash
export COMPOSE_ENV_FILE=/root/data-platform-football/.env
export WHOSCORED_ENV_FILE=/root/data-platform-football/.env.whoscored-rollout
export PROXY_POOL_ENV_FILE=/root/data-platform-football/.env.proxy-pool.whoscored-v2
export RELEASE=/root/dpf-fbref-949-release
export RUNTIME=/root/dpf-fbref-949-candidate
export RUNTIME_OVERRIDE=/root/fbref-949-evidence/20260714T073514Z/runtime-compose.override.yaml
export WHOSCORED_OVERRIDE=/root/fbref-949-evidence/20260714T073514Z/whoscored-runtime.override.yaml
```

1. Keep `dag_ingest_whoscored`, `dag_backfill_whoscored`, and
   `dag_backup_whoscored_storage` paused while code and schema gates run.
2. Rotate both `PROXY_FILTER_CONTROL_TOKEN` and the legacy
   `SOFASCORE_PROXY_CONTROL_TOKEN` if either value has appeared in logs or a
   terminal transcript.
3. Generate the environment secret from the approved legacy pool with
   `scripts/migrate_proxy_pool_secret.py`, verify count and canonical SHA-256
   without printing entries, set `PROXY_FILTER_ALLOW_FILE_FALLBACK=false`, and
   pass all three environment files to every rollout Compose command. `proxy_filter` must
   have no `proxys.txt` mount; the Airflow mount is retained for other sources.
4. Render the production Compose model before changing services:

   ```bash
   docker compose -p data-platform --project-directory "$RELEASE" \
     --env-file "$COMPOSE_ENV_FILE" --env-file "$WHOSCORED_ENV_FILE" \
     --env-file "$PROXY_POOL_ENV_FILE" -f "$RELEASE/compose.yaml" \
     config --quiet
   ```

The live `weed mini` service and its volume are deliberately not recreated by
this rollout. Off-host backup, WORM retention and four-plane cutover are outside
the approved scope; loss of this one host can therefore lose raw and Iceberg
data. The owner accepted that risk, and `dag_backup_whoscored_storage` remains
paused.

### Targeted runtime deployment

The current Airflow runtime contains an independent FBref fix, so its base
worktree is retained and the reviewed WhoScored files are mounted through the
tracked runtime override. Use all three env files, `--no-deps`, `--no-build`
and an explicit service list. The SeaweedFS fingerprint binds container ID,
command and `/data` mount and must be identical after both recreations:

```bash
SEAWEED_BEFORE=$(docker inspect seaweedfs | jq -c '.[0] | {
  id: .Id, command: .Config.Cmd,
  data_mounts: [.Mounts[] | select(.Destination == "/data") |
    {name: .Name, source: .Source, destination: .Destination}]
}' | sha256sum | cut -d" " -f1)

docker compose -p data-platform --project-directory "$RELEASE" \
  --env-file "$COMPOSE_ENV_FILE" --env-file "$WHOSCORED_ENV_FILE" \
  --env-file "$PROXY_POOL_ENV_FILE" -f "$RELEASE/compose.yaml" \
  up -d --no-deps --no-build --force-recreate proxy_filter

docker compose -p data-platform --project-directory "$RUNTIME" \
  --env-file "$COMPOSE_ENV_FILE" --env-file "$WHOSCORED_ENV_FILE" \
  --env-file "$PROXY_POOL_ENV_FILE" -f "$RUNTIME/compose.yaml" \
  -f "$RUNTIME_OVERRIDE" -f "$WHOSCORED_OVERRIDE" \
  up -d --no-deps --no-build --force-recreate \
  airflow-webserver airflow-scheduler

SEAWEED_AFTER=$(docker inspect seaweedfs | jq -c '.[0] | {
  id: .Id, command: .Config.Cmd,
  data_mounts: [.Mounts[] | select(.Destination == "/data") |
    {name: .Name, source: .Source, destination: .Destination}]
}' | sha256sum | cut -d" " -f1)
test "$SEAWEED_AFTER" = "$SEAWEED_BEFORE"
```

## Idempotent Airflow migration

Do not assume a previously completed `airflow-init` container has created new
pools. Apply the pool contract explicitly during deployment:

```bash
docker exec airflow-scheduler bash -euc '
  case "$WHOSCORED_SOURCE_POOL_SLOTS" in 2|3|4) ;; *) exit 2 ;; esac
  if [[ "$WHOSCORED_BACKFILL_POOL" != "$WHOSCORED_DIRECT_POOL" ]]; then
    echo "WhoScored daily and backfill must share one source pool" >&2
    exit 2
  fi
  airflow pools set "$WHOSCORED_DIRECT_POOL" "$WHOSCORED_SOURCE_POOL_SLOTS" \
    "WhoScored bounded direct-only source concurrency"
  airflow pools set "$WHOSCORED_DQ_POOL" 2 \
    "WhoScored bounded Iceberg data-quality queries"
'
```

Confirm `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, then restart the scheduler so
it loads the new DAG and environment. A missing pool is a deployment failure,
not a reason to increase task-level parallelism.

Keep daily ingest paused until its first manual acceptance run passes. The
backup DAG stays paused under the accepted storage decision. Then enable only
the daily schedule:

```bash
docker exec airflow-scheduler airflow dags unpause dag_ingest_whoscored
```

`dag_backfill_whoscored` intentionally has no schedule and remains a
manual/continuation-only DAG. Verify pause state and the next-run timestamps in
the Airflow UI or `airflow dags list` after the scheduler heartbeat.

## Storage scope

Do not run the SeaweedFS cutover playbook and do not recreate the storage
service during this source rollout. Verify the existing `seaweedfs` container
and a Trino read before and after the Airflow/proxy deployment, but leave the
container command, volume and credentials unchanged. The backup/cutover tools
remain available for a separately approved resilience project.

For that future project only, keep the versioned recovery command contract
below. It is not a gate or an authorization to enable backup in this rollout.
An operator first lists and authenticates an exact immutable inventory key:

```bash
docker compose --env-file "$COMPOSE_ENV_FILE" \
  --env-file "$PROXY_POOL_ENV_FILE" run --rm --no-deps \
  --entrypoint bash airflow-scheduler -euc '
    python /opt/airflow/scripts/whoscored_raw_backup.py list-inventories \
      --store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --expected-source-uri "$WHOSCORED_RAW_STORE_URI" --limit 20
  '
```

Restore is allowed only to an approved empty target and only from the exact
selected key; never infer a latest marker or restore over an existing prefix:

```bash
export RECOVERY_TARGET_URI=s3://approved-empty-recovery-bucket/whoscored
export RECOVERY_EXPECTED_SOURCE_URI=s3://original-bucket/raw/whoscored
export RECOVERY_INVENTORY_KEY=backup-inventories/EXACT_APPROVED_KEY.json
docker compose --env-file "$COMPOSE_ENV_FILE" \
  --env-file "$PROXY_POOL_ENV_FILE" run --rm --no-deps \
  --entrypoint bash -e RECOVERY_TARGET_URI -e RECOVERY_EXPECTED_SOURCE_URI \
  -e RECOVERY_INVENTORY_KEY airflow-scheduler -euc '
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

Current rollout enforcement is the opposite: keep the deferred DAG paused.

```bash
docker exec airflow-scheduler airflow dags pause dag_backup_whoscored_storage
```

## Canary and full backfill

### Production write-smoke

Before live writes, require `airflow dags list-import-errors` to return an empty
table and execute `validate_whoscored_runtime`; its result must report parser
v8, report schema 3, 25 datasets and the checked-in code-tree SHA-256. Run
additive schema initialization and full-history discovery while all WhoScored
DAGs remain paused. Then run one exact active scope from the pinned parser-v8
catalog, direct-only. Do not infer success from a green process exit: retain
the JSON report and require zero paid bytes plus all scope DQ gates. On
2026-07-14 the catalog marked `ENG-Premier League=2526` historical and
`ENG-Premier League=2627` active, so the production smoke used `2627`; the
failed `2526` selector made no source request and used no paid traffic.

The reproducible manual sequence is:

```bash
DISCOVERY=/opt/airflow/logs/whoscored/manual/20260714T152100Z/discovery.json
SMOKE=/opt/airflow/logs/whoscored/manual/20260714T161016Z/daily_epl_2627.json
CATALOG_BATCH=wsc2-c1d277275b85c0064ee72819d37a58a1b86a5852a5e9aea4636e3ec82ae4cfd5

python dags/scripts/run_whoscored_scraper.py discover \
  --full-history --direct-only --output "$DISCOVERY"

python dags/scripts/run_whoscored_scraper.py daily \
  --scope 'ENG-Premier League=2627' --skip-profiles \
  --catalog-batch-id "$CATALOG_BATCH" --direct-only --output "$SMOKE"

python - <<'PY'
from dags.dag_ingest_whoscored import validate_scope_result
validate_scope_result(
    scope_spec="ENG-Premier League=2627",
    result_path=(
        "/opt/airflow/logs/whoscored/manual/20260714T161016Z/"
        "daily_epl_2627.json"
    ),
    require_zero_paid=True,
    expected_catalog_batch_id=(
        "wsc2-c1d277275b85c0064ee72819d37a58a1b86a5852a5e9aea4636e3ec82ae4cfd5"
    ),
)
PY
```

Verify the write through the same authenticated Trino endpoint used by
DataGrip. At minimum, these queries must return a parser-v8 full-history catalog
and parser-v8 current evidence for the smoke scope:

```sql
SELECT batch_id, parser_version, discovery_mode, state,
       competitions_count, seasons_count, stages_count
FROM iceberg.bronze.whoscored_catalog_manifest
ORDER BY completed_at DESC
LIMIT 5;

SELECT league, season, entity_group, parser_version, state, completed_at
FROM iceberg.bronze.whoscored_scope_ingest_latest_success
WHERE league = 'ENG-Premier League' AND season = '2627';

SELECT parser_version, state, count(*) AS matches
FROM iceberg.bronze.whoscored_match_ingest_manifest
WHERE league = 'ENG-Premier League' AND season = '2627'
GROUP BY 1, 2;

SELECT count(*) FROM iceberg.bronze.whoscored_schedule_current
WHERE league = 'ENG-Premier League' AND season = '2627';
SELECT count(*) FROM iceberg.bronze.whoscored_matches_current
WHERE league = 'ENG-Premier League' AND season = '2627';
SELECT count(*) FROM iceberg.bronze.whoscored_events_current
WHERE league = 'ENG-Premier League' AND season = '2627';
SELECT count(*) FROM iceberg.bronze.whoscored_lineups_current
WHERE league = 'ENG-Premier League' AND season = '2627';
```

Production evidence recorded on 2026-07-14:

- discovery report
  `/opt/airflow/logs/whoscored/manual/20260714T152100Z/discovery.json` is
  schema 3, status success, errors=0, paid bytes=0;
- catalog batch
  `wsc2-c1d277275b85c0064ee72819d37a58a1b86a5852a5e9aea4636e3ec82ae4cfd5`
  is parser v8, `full_history`, success; manifest and physical counts agree at
  433 competitions, 7,477 seasons and 15,979 stages, with 7,472 eligible
  scopes and zero quarantined records;
- smoke report
  `/opt/airflow/logs/whoscored/manual/20260714T161016Z/daily_epl_2627.json`
  is schema 3, status success, errors=0, paid bytes=0 and is pinned to that
  catalog batch;
- scope manifest
  `wss2-306d1e033e7a161427c5a542516664d6011ab7316943e54ace2e93f54478a249`
  is parser v8/success. Manifest and current views agree on 380 schedule rows
  and 144 bet rows; matches/events/lineups are zero for the future season;
  68/68 feed-state observations are typed `not_available` and all DQ
  duplicate, missing, coverage and parity counters are zero;
- SeaweedFS and Trino retained their exact container identities after the
  write, restart=0 and OOM=false. Proxy health stayed HTTP 200 with
  `daily_total_bytes=0`.

Any missing `discovery_mode`, non-v8 manifest, physical/current parity failure,
paid byte, Trino restart or code-hash mismatch aborts deployment before daily
unpause.

Run the non-publishing direct-source workflow benchmark first. It uses a
temporary raw store and a non-publishing repository. The sustained harness
must exercise four workers for at least six hours across historical match,
preview, profile and multi-stage work. Acceptance requires projected throughput
of at least 144,000 page units/day, zero paid bytes, combined harness
process-tree RSS plus monitored-container cgroup memory <=12 GiB, and no
container restart/OOM evidence. Use the exact invocation printed by
`--help`; keep the normal short duration only for CI and operator rehearsal.

```bash
cd /root/dpf-fbref-949-release
CANARY_OUTPUT=/root/fbref-949-runtime/whoscored-capacity-$(date -u +%Y%m%dT%H%M%SZ).json
/root/.venvs/dpf-test/bin/python \
  scripts/research/bench_whoscored_capacity.py \
  --duration-seconds 21600 \
  --scope 'ENG-Premier League=2526' \
  --scope 'INT-World Cup=2026' \
  --match-limit 3 --profile-limit 3 \
  --flaresolverr-url http://127.0.0.1:8191 \
  --container airflow-scheduler \
  --container flaresolverr \
  --container proxy_filter \
  --output "$CANARY_OUTPUT"
```

Run this supervisor on the host: it reads its own four child process trees from
`/proc`, reads required-container cgroup memory through `docker stats`, and
uses `docker inspect` for restart/recreate/OOM evidence. The three listed
containers are mandatory even when `--container` is omitted; that option can
only add extra monitored containers. The output path is create-once mode 0600.

Only one capacity supervisor may run at a time. It holds a host flock and
stores the exact owner and endpoint in a mode-0600 state file. On normal
termination it must prove worker process-group death before API cleanup,
observe 95 continuous seconds at zero, and complete two final zero scans.
After parent SIGKILL, the next invocation must use the saved endpoint for
stale-owner preflight cleanup. The state file may be removed only after
verified zero; any unverified process death or lifecycle response fails closed
and retains recovery state.

The four capacity workers also share
`$RELEASE/logs/whoscored/source-circuit-v1.json` and enable wait mode
themselves. Do not delete or shorten this file to make a blocked canary run
faster: a real source block must remain visible in wall-clock throughput. A
successful half-open probe closes it automatically. An ordinary
timeout/502/parser failure reopens the same level without escalation; another
authoritative browser Cloudflare response advances 15 -> 30 -> 60 minutes.

The harness must never publish production manifests. After it is green, set
`WHOSCORED_SOURCE_POOL_SLOTS=4` and
`WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY=144000`, recreate only the
Airflow services, set the pool to four, and trigger one manual daily DagRun.
Require every DQ/traffic/SLO gate to pass before unpausing the schedule.

## Backfill capacity and the 30-day activation gate

Request-unit policy v6 counts one unit for every frozen match page, one for
every frozen preview page, one for every profile page, and 70 units per frozen
source stage for schedule work. This removes the old blanket `matches * 2`
estimate: a match without a source preview no longer consumes imaginary
capacity, while every `preview_game_id` is still accounted exactly.

`WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY` is only an SLO planning
assumption. It does not increase an Airflow pool, change a source limiter, or
throttle a worker. The maximum is derived, never configured independently:
`WHOSCORED_SOURCE_POOL_SLOTS * 30 * 1,440`. With the validated range 2..4 this
is 86,400, 129,600 or 172,800 page units/day. The production four-slot planning
value is deliberately lower at 144,000/day. The structured-feed limiter is
faster, but it cannot justify a larger scalar assumption for the
match/preview/profile-heavy portion.

Before each batch is created, the controller compares the durable remaining
lower bound with the immutable deadline. Schedule receipts make match and
preview counts exact; later roster receipts make profile counts exact. If a
newly exact population breaches the ceiling, no next source batch or automatic
continuation is created. After at least six wall-clock hours and 1,000 completed
units, the SLO summary also reports observed units/day and projected remaining
days. That observation is deliberately advisory: pauses and incidents must be
visible, but do not redefine or silently extend the agreed deadline.

The two-slot scale evidence blocks an all-catalog production run. With 1.9M
modeled matches and 15,979 observed source stages, the old blanket estimate was
at least 4,918,530 units, or 163,951/day before profiles. Applying the current
live ratios (18,143 preview keys and 5,991 profile keys per 56,392 schedule
keys) gives a sensitivity estimate of about 3.832M units, or 127,722/day. More
importantly, the estimated 2.713M page-limited match/preview/profile units alone
need about 31.4 days at the enforced 86,400/day ceiling, before schedule work,
Airflow overhead, retries, or source failures. These ratios are sizing evidence,
not a frozen all-catalog population.

Do **not** trigger the all-catalog crawl at two slots. First pass the sustained
four-worker non-publishing canary, promote the real Airflow pool to four, and
complete one manual plus one scheduled daily run. If any throughput, memory,
restart, paid-byte or DQ gate fails, return to two slots and do not create the
plan. Only after all evidence makes the exact preflight sufficient may the
single production crawl be started:

```bash
docker exec airflow-scheduler airflow dags trigger dag_backfill_whoscored \
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

The source change is accepted only when the isolated WhoScored CI contract,
Compose rendering, shell syntax, real Airflow DAG-import check, runtime hash and
writer-interface preflight, production write-smoke/DQ and the sustained
non-publishing live-source canary pass. Record exact test counts, discovery and
smoke report paths and batch IDs, DQ/Trino count summary, sustained-canary
artifact and post-run storage/proxy invariants. The repository-wide suite
contains unrelated live/browser checks and is not a substitute for this
deterministic source contract. Storage topology and off-host backup are
explicitly excluded from this acceptance; the 30-day historical crawl remains
deployment evidence and cannot be replaced by tests.

### Implementation evidence recorded 2026-07-14

- The live catalog snapshot used to size the frozen checks contained 7,472
  eligible senior-men scope-seasons and 15,979 source stages. The DQ scale
  tests use the same scope cardinality. These numbers are observations, not a
  static allowlist; full-history discovery remains source-driven.
- The isolated WhoScored CI selection passed 1,159 tests with zero skips or
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

The parser-v8 runtime preflight, full-history production discovery and
active-scope write-smoke/DQ are now complete. Activation still requires a green
sustained four-worker throughput canary, promotion of the real pool to four,
one green manual and scheduled Airflow daily DagRun, and then completion of the
frozen all-catalog plan within its 30-day deadline. Storage migration is not
part of this activation.

### Runtime activation audit recorded 2026-07-14

The Compose project was created from candidate HEAD
`87da72f59c92ae7cbe9b3ba83d3a7947c7975ae3`; mounted WhoScored code and writer
overrides point to this release. Runtime contract code-tree SHA-256 is
`3426b6a3cb4eae2f8a25568ae80c53b177265543a89c12b15a2b17a53668e577`, DAG
import errors are empty and direct/DQ pools remain 2/2. The earlier scheduled
daily mixed-version writer incident is historical: the contract now pins
parser, report schema, dataset count and SHA-256 of the
writer/parser/repository/service/CLI files before any source request. A parser
v8 full-history catalog manifest and parser v8 smoke scope manifest now exist.

The first sustained-canary attempt is retained at
`/root/fbref-949-runtime/whoscored-capacity-20260714T161700Z.json`. It failed
closed after 5.792 seconds because the host test venv lacked the production
`curl_cffi==0.15.0` dependency; page units were zero and paid bytes/routes were
zero. The harness now preflights that exact pin, binds literal venv path,
Python prefix, dependency version and source hashes into its immutable runtime
identity, and sanitizes all child evidence fail-closed. The urgent regression
selection passed 64 tests and an independent review found no remaining
Critical/Important issue. The replacement artifact
`/root/fbref-949-runtime/whoscored-capacity-20260714T164334Z.json` failed closed
after 1,032.838 seconds: two EPL workers completed six workflows and 546 page
units, while the World Cup worker reached preview `1976987`, whose parsed
source explicitly omitted one required preview structure. Paid bytes/routes
were zero and the memory, restart, OOM and runtime-identity gates stayed green.

The production preview commit rule remains strict. The capacity-only workflow
now probes at most nine deterministic completed candidates and selects three
whose three typed preview datasets are all AVAILABLE or EMPTY. It skips only
an explicit `DatasetStatus.NOT_AVAILABLE`; transport/parser exceptions or a
status-type contract drift fail closed. The focused workflow/capacity selection
passes 69 tests, Ruff and compile, and a repeat independent review found no
Critical/Important issue.

The non-publishing World Cup rehearsal evidence is retained at
`/root/fbref-949-runtime/whoscored-workflow-20260714T204118Z.json` (SHA-256
`eae45ded16c6372ed3c38fbf9fc708c8832c1484cb95ffbb9770c5249f9edff2`). It
passed cold/warm/incremental in 1,265.659 seconds across 13 stages. All three
matches, previews and profiles completed; cold recorded 925 successful page
units, warm made zero source requests, incremental fetched exactly one target,
and paid bytes/routes stayed zero. The stable match sample was `1953853`,
`1976989`, `1953860`.

The six-hour attempt retained at
`/root/fbref-949-runtime/whoscored-capacity-20260714T210456Z.json` failed closed
after 275.255 seconds because an external rollout stopped and recreated
`airflow-scheduler`. This is container-identity failure evidence, not a source
throughput result. It completed zero page units and used zero paid bytes/routes.

Browser-session lifecycle hardening was then deployed in FlareSolverr container
`34de7c325464e6af7a2a1641107baf497efc0f78f6739464a2aad42dba61828a`, image
`sha256:7962759d99d7e125e108e0f5e7f3cdbcd36161776d058d1d9b7153b92ef1af9e`,
with extension SHA-256
`45ddcea7d36d4d91587ceb7ad04dff9aa75c182d74264c682f2b254ea501eb46`.
A live four-thread probe observed four blocked requests and active=4 while the
control request returned in 0.005 seconds. All four requests were released and
both owner and global session counts returned to zero. The live log-redaction
probe reported `leak_count=0`.

The parent-SIGKILL recovery report is
`/root/fbref-949-runtime/whoscored-capacity-abort-recovery-20260714T233742Z.json`,
SHA-256
`137c180fe7980f3e8c8c69c06c7e1349d374c3fd23ccd80af43f06fc133ce970`.
Its browser cleanup gate passed: stale preflight was required, preflight and
final zero were verified, the quiet window and two final scans completed, the
state file was removed, and paid traffic stayed zero. The report's overall
status is intentionally failed because this ten-second recovery rehearsal did
not satisfy throughput or representative-workload gates.

Final focused validation passed 403 tests. Independent FlareSolverr and
capacity reviews passed 175 and 333 tests respectively and both returned
READY. The full unit suite reported 5,723 passed, 46 skipped, and one known
unrelated SofaScore fingerprint failure.

The replacement six-hour canary retained at
`/root/fbref-949-runtime/whoscored-capacity-20260714T234235Z.json` (SHA-256
`872e60f0aca1c6dc6d0b7c4f946d74344347f38ae01f69eb55c004044ed22084`)
failed closed after 12,987.038 seconds of work (about 3 h 36 min; 13,087.471
seconds total). Its stop reason was `worker_health`, so the sustained-duration
gate also failed. The throughput gate passed at 178,906.757 page units/day:
26,892 page units and 112 completed runs, split 46/10/46/10 across the four
workers. Paid traffic stayed zero. Memory, container, runtime-identity,
non-publishing and cleanup gates passed; final browser-session count was zero
and owner state was removed.

World Cup worker iteration 10 stopped on a partial schedule caused by a typed
HTTP 502. The browser batch-validation branch incorrectly treated that
retryable typed 502 as terminal. The fix retries only the failed URL, preserves
successful cache entries, rotates the browser session, never enables a paid
route, and reacquires a source rate token for every physical retry. A persistent
502 still fails closed. An initial live diagnostic safely reproduced `HTTP 502
rendered as HTTP 200` with three source attempts, zero paid traffic and green
cleanup. Independent review returned `READY`; 86 transport tests and 483
combined focused tests passed, as did Ruff, compile and diff check. The World
Cup replay later passed; the following six-hour attempt and its source-block
result are recorded below.

Until a new full six-hour canary exits zero, direct/DQ pools remain 2/2, all
WhoScored DAGs stay paused, and no `all_catalog` plan may be created.

The live storage service remains one `weed mini` container and is intentionally
unchanged. Promote concurrency only through the sustained canary. Do not create
an `all_catalog` plan until the canary, pool promotion, manual daily and
scheduled daily gates are green.

### Source pacing and cooldown hardening recorded 2026-07-15

The HTTP-502 fix passed its World Cup replay at
`/root/fbref-949-runtime/whoscored-workflow-http-backoff-replay-20260715T103454Z.json`
(SHA-256
`50f5cd78eb8230716c71ebe2f0c4dd0a9498882c777409d6beef7d66cb74c62b`).
The next sustained attempt,
`/root/fbref-949-runtime/whoscored-capacity-20260715T110327Z.json` (SHA-256
`e57bb7b6a7b915415987da86d6d098d10ebe13ede93fe938b320501367ad21d0`),
then stopped fail-closed after 6,733.609 seconds on authoritative EPL browser
Cloudflare evidence. Its pre-failure projection was 172,527.745 page units/day;
paid traffic, memory, runtime-identity and cleanup gates passed. A bounded
direct-XHR diagnostic and a same-session navigation diagnostic proved this was
a source/IP block rather than an access-gate, JSON, Chromium or parser error.

The candidate now arbitrates the actual in-browser `fetch()` start under one
process-global lock and conservatively anchors the next start at least 546 ms
after the previous synchronous launch acknowledgement. Network downloads may
overlap; starts cannot burst across sessions or batches. All workers also use
one process-safe circuit state: authoritative browser CF opens 15 minutes,
failed authoritative half-open probes advance to 30 and 60 minutes, and only
one logical probe may run. Ordinary timeout/502/content/access-gate outcomes
are inconclusive at the same level and can never authorize paid traffic.
State persistence is 0600, flocked, dirfd-relative and fail-closed on corrupt,
symlinked, hard-linked or parent-swapped paths.

The isolated production selection passed 1,429 tests. Ruff, Python
compilation, shell syntax, Compose rendering and `git diff --check` passed.
Independent review returned `READY` with no Critical or Important findings.
The candidate FlareSolverr extension SHA-256 is
`4e49832333664af3b773888bb1fbeb2fde7b9f41662029c817ced3176f09f249`.
That exact extension is deployed in healthy FlareSolverr container
`e833962543d8e8526910e94425b34311b21cab006185dfeddf4f6b014740c733`
with restart count zero and no OOM. The first live EPL replay failed closed in
12.452 seconds on authoritative FlareSolverr CF with one browser session, one
direct-FlareSolverr attempt and zero paid traffic. Evidence is
`whoscored-workflow-epl-paced-circuit-replay-20260715T150426Z.json`, SHA-256
`b4ecec7b2ce2087fb4e72f280facd314a43fa2261a42f7811e37019ec37ab45e`.
The circuit opened its level-zero cooldown through
`2026-07-15T15:20:07Z`. The subsequent bounded level-one half-open replay also
failed closed on its single direct-FlareSolverr attempt after 16.912 seconds,
with zero paid traffic and clean browser sessions. Evidence is
`whoscored-workflow-epl-level1-half-open-replay-20260715T155136Z.json`, SHA-256
`e4c56d36484b6c1962610d3b79a064987554e66a2c2a95f697f03fa2496c3aee`.
The circuit advanced to level two through `2026-07-15T16:52:14Z`. Its bounded
level-two probe then failed closed on its single direct-FlareSolverr attempt
after 7.052 seconds, again with zero paid traffic and clean browser sessions.
Evidence is
`whoscored-workflow-epl-level2-half-open-replay-20260715T165255Z.json`, SHA-256
`ef2c171eb56cffd66d757e39096d1ce4d70358509621bacca52026a3663a2dcb`.
Circuit generation seven is now level three with the cooldown capped at 60
minutes, through `2026-07-15T17:53:52Z`. This is an external source/IP blocker:
a bounded EPL replay and full six-hour canary must exit zero before promotion.
Pools therefore remain 2/2, WhoScored DAGs remain paused and `all_catalog`
remains forbidden.
