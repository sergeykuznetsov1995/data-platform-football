# FBref production readiness report

Date: 2026-07-11 (updated 2026-07-12)

Baseline: `origin/master` at `9eedd24783c6bb917941082385d685f841e19879`

Companion backlog: [`FBREF_PRODUCTION_BACKLOG_2026-07-11.md`](FBREF_PRODUCTION_BACKLOG_2026-07-11.md)

## Executive status

**Code complete; production validation pending.** The checkout contains a
single durable raw-first production flow, bounded backfill and zero-network
replay, generic and typed Bronze persistence, strict promotion edges, and the
requested DQ/identity/keeper consumers. It is not yet valid to call the source
production validated:

- the approved bounded live canary executed within both caps but failed closed
  on a classified warm-HTTP `500`, before raw commit or parsing;
- a paid/full 400-page soak was not authorized and was not executed;
- an accepted before/after live traffic benchmark was not run;
- #901 still has 11 matches with recorded scores but no raw event rows, so the new strict
  Silver DQ gate is expected to block promotion until the raw gap is repaired;
- GitHub Project scope is unavailable because the credential lacks
  `read:project`.

This report does not claim deployment, destructive migration, a full crawl, or
an unbounded refetch. Merge state is recorded by the pull request, not frozen
into this report.

## Production architecture

```text
FBref competition index
  -> PostgreSQL competition/season registry (male active, female skipped,
     unknown quarantined)
  -> durable canonical page frontier and immutable run cohort
  -> filtered SKIP LOCKED claim + fenced lease
  -> atomic request/byte reservation + domain throttle
  -> one Camoufox clearance bootstrap per fetch shard
  -> warm curl_cffi HTTP target fetch
  -> immutable raw-v2 response/content commit
  -> offline discovery + generic PageDocument + typed compatibility parse
  -> Bronze staging/MERGE + independent dataset availability + page marker
  -> run traffic/completeness/eligibility validation
  -> Silver -> Silver DQ -> xref -> xref DQ
  -> Gold promotion
```

The configured `LEAGUE_IDS` mapping remains only a backward-compatible output
projection for existing Bronze/Silver consumers. It is never iterated to choose
FBref crawl scope.

### Airflow topology

Every `fetch` task drives Camoufox through `dags/scripts/run_fbref_fetch_wave.py`
in its own process. Playwright's sync API deadlocks inside a task process forked
from the multi-threaded scheduler — the browser starts, the navigation never
opens a socket, and no timeout fires — which is why the repository already
requires browser scrapers to run in a subprocess. The task callable only relays
the runner's bounded JSON result, so budgets, leases and gates are unchanged;
`parse`, `validate` and `seed` tasks touch no browser and stay in-process.

| DAG | Schedule and budget | Static topology | Promotion contract |
|---|---|---|---|
| `dag_ingest_fbref` | Daily `06:00` UTC; 200 total requests and 100 MiB per run; default/max shard 25 | 20 tasks: initialize, seed index, eight sequential `fetch -> offline parse` waves, validate, trigger Silver | Every edge is `all_success`; Silver trigger blocks and propagates child failure. |
| `dag_backfill_fbref` | Manual only; maximum 25 total requests and 100 MiB; automatic durable resume; default/maximum shard 2 | 54 tasks: initialize, seed the next bounded unfinished non-current season cohort, 25 sequential `fetch -> parse` waves, validate, trigger Silver | The seed cohort is clamped to worst-case bootstrap/target reservation capacity; claims only `historical_once` targets, completed historical seasons are not requeued, and no operator cursor or manual league list exists. |
| `dag_replay_fbref` | Manual only; required source control-run UUID; request and byte budgets are both zero | 11 tasks: initialize, eight sequential parse-only waves of 25, validate, trigger Silver | Contains no fetch or seed task; validation fails if source candidates remain or any proxy traffic appears. |

The master pipeline no longer triggers a second paid FBref crawl at 14:00. It
uses a fail-closed `ExternalTaskSensor` with an eight-hour execution delta to
wait for the scheduled 06:00 ingest and its blocking Silver/xref chain.

## Control-plane contract

The `fbref_control` schema shares Airflow's PostgreSQL server but has no Airflow
ORM dependency. `airflow-init` runs `python -m scrapers.fbref.control migrate`
after the Airflow metadata migration.

Seven checksum-verified, advisory-lock-protected migrations create:

- `crawl_run` and atomic budget counters;
- `budget_reservation` with idempotent settlement per attempt;
- source snapshots plus competition and season registries;
- canonical `page_frontier` and immutable `run_target` cohorts;
- fenced `fetch_attempt` rows;
- per-dataset completion manifests;
- per-logical-refresh observation processing, including generic, typed, and
  stateful parser-version fences plus exact HTTP evidence;
- clearance-session traffic metrics and domain throttle state.

Control invariants:

- Airflow run IDs and logical refresh IDs map deterministically to UUIDv5;
- canonical URL uniqueness is checked before a request;
- cohorts cannot change identity after insertion;
- claims are bounded to 25 and use PostgreSQL `FOR UPDATE ... SKIP LOCKED`;
- `page_kinds` and `refresh_policies` are SQL filters, isolating current and
  historical work;
- lease token plus epoch fence every heartbeat, completion, and failure;
- a retry reuses raw only when that exact `logical_refresh_id` already has a
  committed manifest, which is crash recovery for the same observation;
- production never adopts prior-run raw into a new logical refresh. A target
  moving to `historical_once` or `current_completed_once` performs a fresh
  final fetch before it can become one-shot complete;
- request and byte budgets are reserved before network activity and settled
  with observed usage;
- one shared domain slot enforces a default three-second interval.

## Raw, parsing, and persistence contracts

### Raw-v2

`RawPageStore` keeps backward read compatibility with raw-v1 and writes
raw-v2. The production fetch wave does not adopt raw-v1 or prior-run raw-v2 as
a new observation. It can resume only an already committed raw manifest for
the exact same `logical_refresh_id`; mismatched or corrupt evidence fails
closed. A raw-v2 record separates the exact HTTP response from the effective
parser content:

- exact response hash/blob, including the normally empty 304 body;
- effective content hash/blob, referencing prior content for a 304;
- canonical target and source-native IDs;
- logical refresh and attempt IDs;
- HTTP status, headers, ETag/Last-Modified, latency and transport/session
  versions;
- decoded, compressed, wire, and provider-billed byte fields.

The compressed blobs are written before the manifest. Recommitting the same
logical refresh is idempotent only when the evidence is identical; conflicting
evidence fails closed. Hash verification occurs on read.

### Lossless generic Bronze

`PageDocument` is transport-free and scans visible DOM plus every HTML comment.
It preserves multiple tables per comment, header paths, `data-stat`, raw cell
values, entity IDs, source order, schema/content signatures, and duplicate
relationships. Availability is explicit: `available`, `empty`, `restricted`,
`not_applicable`, `duplicate`, `layout_only`, `unknown`, or `error`.

Generic Iceberg persistence writes:

- `bronze.fbref_table_cells`;
- `bronze.fbref_table_inventory`;
- `bronze.fbref_page_manifest` as the final page commit marker.

Each attempt uses a unique staging identity and idempotent `MERGE`. Partial
persistence cannot produce a successful page marker.

### Typed compatibility Bronze

Offline typed adapters reuse the existing pure schedule, season-table, and
match parsers. They retain legacy `league`/`season` columns while adding
source-native competition and season IDs. Match data tables are persisted
before the independent `bronze.fbref_dataset_availability` commit marker,
which is always written last. The marker records typed availability per
requested dataset; a partial remediation replaces only those requested keys
and cannot erase evidence for untouched datasets. Completion no longer depends
on `fbref_match_player_stats`. Unknown source competitions remain parseable and
never expand crawl scope from the compatibility map.

## Transport budgets and observability

The only production transport is Camoufox clearance plus a warm `curl_cffi`
session. Camoufox navigates only `https://fbref.com/en/`; target pages use the
exported cookies, user agent, proxy, and warm HTTP session. Nonessential browser
assets are blocked while Cloudflare/Turnstile assets remain allowed.
Every bootstrap navigation attempt is counted separately from browser resource
requests and persisted; more than one attempt in a clearance session fails the
run instead of being hidden behind a boolean metric.

Hard defaults and caps:

- current run: 200 total browser+HTTP requests, 100 MiB;
- backfill run: at most 25 total browser+HTTP requests, 100 MiB;
- replay: 0 requests and 0 bytes;
- cumulative warm-HTTP response bodies for one target, including the bounded
  status retry: at most 2 MiB, enforced by the native libcurl write callback;
- one request reserves 7 MiB before transport: 4 MiB for browser clearance,
  2 MiB for cumulative HTTP bodies, and 1 MiB for HTTP wire headers;
- a new clearance reserves up to 20 browser requests and 4 MiB;
- shard size: current at most 25; backfill at most 2 after worst-case
  bootstrap/target reservation clamping;
- domain interval: three seconds.

The control budget is reserved before transport and fails validation on an
overspend. The transport also fails closed before an individual response can
jump the cap: warm HTTP streams through a cumulative callback bound across
retry attempts, while every browser request reserves fixed framing overhead
and then its declared `Content-Length` against all parallel in-flight work.
Missing, invalid, chunked, or oversized browser lengths abort the whole browser
session. Failed requests use Playwright's observed sizes when available; if
sizes are unknown, their full reservation remains charged. Teardown likewise
charges every still-in-flight reservation, so partial traffic cannot be reused
to cross the cap. The canary network-namespace quota remains defense in depth.

Blocking DAG handoffs are collision-safe: current, backfill, and replay render
the child run id from both parent DAG id and parent run id, use the trigger
task's exact start timestamp as child logical date, and never reset an existing
child. Silver has an eight-hour run timeout containing its four-hour xref wait;
parent waits allow twelve hours.

Raw-v2 latest-content selection is monotonic over immutable per-refresh target
history. The old mutable target file is only a compatibility mirror. A delayed
older response cannot replace a newer observation, and a 304 always bases its
effective content on the control frontier's committed content hash when one is
available, falling back to monotonic raw history only for standalone callers.
Typed writes, discovery/registry effects, and observation completion run under
the same latest-frontier fence. The completion key includes the logical
refresh and all three parser versions, so repeated content such as A -> B -> A
is still processed once per observation. Stale observations keep generic raw
evidence but cannot overwrite current typed or stateful output.

Because bootstrap activity is charged to the same request budget, a 25-request
canary does not imply 25 target pages. The 20-request bootstrap reserve leaves
five request slots, while the 7-MiB per-target reservation under a 25-MiB
budget lowers the effective maximum to three target requests.

Durable metrics distinguish:

- browser document and browser asset bytes;
- browser bootstrap request count;
- warm HTTP request and wire bytes;
- decoded HTML and compressed raw bytes;
- provider-billed bytes when available;
- P50/P95 latency, HTTP wire bytes, provider-billed bytes, and aggregate byte
  counters by page kind;
- classified/unclassified failures and retries;
- duplicate canonical fetch violations;
- table availability and competition/sentinel coverage.

Run validation fails when warm HTTP success is below 95%, unclassified failure
rate is at least 0.5%, duplicate fetch violations are nonzero, any session has
more than one bootstrap, female/unknown downstream targets are nonzero, a
dataset/page marker is incomplete, or a budget is exceeded. These gates are
implemented. Closure also requires the current generic, typed, and stateful
parser-version tuple for every logical observation in current, backfill, and
replay runs. The failed live canary exercised the traffic gates, but it did not
reach raw persistence, table inventory, registry coverage, or sentinel gates.

### Bounded live canary

Update 2026-07-12: the bounded live canary **passed** after two transport
defects were fixed. Both were found by the canary itself and could not have
been found offline: the browser byte guard rejected every real Cloudflare
clearance.

1. Reservations were keyed by `id(request)`. Firefox delivers the response
   callbacks a different `Request` wrapper than `route()` saw, so the guard
   never found the reservation, classified the main `fbref.com/en/` navigation
   response as `untracked_response`, and tore down the whole session.
   Reservations are now indexed by request URL as well, and a request the route
   callback genuinely never saw (server-redirect hops are not re-routed by
   Playwright) is adopted: it consumes one request slot plus the fixed overhead
   under the same caps.
2. The guard demanded a declared `Content-Length` on every browser response.
   Cloudflare answers over HTTP/2, and chunked HTTP/1.1 responses carry no
   length either, so this aborted the session on the very first response. An
   undeclared body now reserves a 512-KiB ceiling before the browser may read
   it and settles to the observed size on completion; the cap still fails
   closed when the reservation does not fit or the body outgrows it.

Passing canary evidence (`/en/comps/`, 25-request/25-MiB control budget,
kernel quotas 18 MiB ingress / 4 MiB egress):

- logical label: `fbref-canary-20260712T135326Z-922d14a721f941e1842faa7574e2e162`;
- control run: `562f020a-8f9b-53fe-bb5d-2b3825e5139e`, status `succeeded`;
- `20` of 25 requests charged, `1,331,685` billed bytes, budget not exceeded;
- one clearance bootstrap (`19` browser requests), warm HTTP `1` request /
  `53,101` wire bytes, decoded HTML `268,064` bytes, raw blob `51,451` bytes;
- warm HTTP success rate `1.0`, unclassified failures `0`, duplicate canonical
  fetch violations `0`, classified retries `0`;
- kernel counters: ingress `2,019,083` bytes, egress `2,186,339` bytes — both
  far below the quotas;
- raw committed, offline parse `1` page, `10` dataset validations succeeded;
- registry coverage: `117` male competitions active, `36` female skipped, `0`
  unknown-gender quarantined; all seven sentinels published and eligible;
- female/unknown downstream targets: `0`;
- provider-billing evidence remains unavailable (`null`).

The earlier 2026-07-11 canary (`f796a73e-6db0-51fb-bff3-f9a25bc38f49`) failed
closed on a classified warm HTTP `500` before raw commit; it is superseded by
the run above. The ephemeral quota namespace is removed after every run.

## Offline replay and remediation

Replay requires an existing control-run UUID and reads only immutable raw
manifests whose observation is not complete for the current generic, typed, and
stateful parser-version tuple. It does not import or construct the fetcher.
Stateful replay effects are rebuilt only from the latest observation for a
target. Eight shards of 25 cover a source run capped at 200 requests.
Validation checks that no candidates remain and that network attempts and
charged bytes are both zero.

The manager and match-player remediation scripts now use the same bounded
raw/control contract (`--max-pages <= 25`). Dry-run opens neither raw storage
nor the writer; parser failures record a failed page marker and cannot silently
complete a target.

## DQ and downstream compatibility

Implemented contracts include:

- female competitions skipped before frontier fan-out and unknown gender
  quarantined;
- sentinel coverage for Premier League, Champions League, Nations League,
  Africa Cup of Nations, World Cup, European Championship, and Copa América;
- opaque source competition/season IDs and single-year competition support;
- tables inside comments, duplicate tables, restricted/unknown/schema-drift
  availability, and multi-stage schedules;
- independent typed match availability in
  `bronze.fbref_dataset_availability`, propagated into Silver event DQ;
- legacy source score plus additive source, on-field, official, shoot-out,
  authority, reference, provenance, and result-status columns;
- regulation, extra-time, and shoot-out event phases; shoot-out attempts never
  increment the regulation/extra-time running score;
- finite awarded-result overrides whose authority/reference are validated
  before Trino execution;
- exact-only FBref player identity recovery across season, match, lineup, and
  keeper datasets, with deterministic synthetic IDs only for residuals;
- typed match goalkeeper Silver and nullable goalkeeper metrics in the existing
  player-match Gold fact;
- no-null, uniqueness, referential-integrity, range, score round-trip, score
  mismatch, missing event, awarded override, and shoot-out parse checks.

Every FBref-derived Gold match fact is inner-scoped through
`gold.dim_match`. An FBref row outside the configured competition/season scope
therefore cannot leak into officials, player, lineup, timeline, shot, team
audit, or manager-stint Gold output.

Legacy `home_score`/`away_score` remain FBref source semantics. Consumers that
need a competition result use `official_*`; Gold `result_1x2` does so.

### Current strict-DQ blocker

#901 documents 11 matches with recorded scores whose raw Bronze event dataset
is absent; seven of those scores are nonzero.
`scored_match_without_events[silver.fbref_match_enriched]` is intentionally an
ERROR with a zero-row allowance, so production Silver promotion is expected to
fail until those raw gaps are repaired. The allowed remediation order is:

1. replay the immutable raw page with the current parser;
2. if raw is absent/corrupt, request a separately bounded refetch authorization;
3. validate the repaired datasets and rerun Silver/xref/Gold.

The pipeline must not synthesize missing FBref events or weaken the gate to
claim readiness.

## Legacy production path removed

The following obsolete production paths are deleted:

- `dags/scripts/run_fbref_scraper.py`;
- `dags/scripts/run_fbref_discovery.py`;
- `dags/utils/fbref_tasks.py`;
- `dags/utils/fbref_callbacks.py`;
- `scrapers/nodriver_fbref/`;
- `scrapers/fbref/discovery_queue.py`;
- `scrapers/fbref/discovery_service.py`;
- `scrapers/fbref/{scraper,browser_manager,data_readers,data_mergers,url_builder}.py`;
- the unused shared nodriver bypass modules and their image dependencies;
- the last unreferenced nodriver-specific proxy formatting helpers;
- their dedicated legacy unit/integration tests.

Network-enabling Make targets for curl/nodriver/full fallback tests were
replaced by `make test-fbref-offline`. Existing pure parser modules under
`scrapers/fbref` remain because typed Bronze and compatibility consumers still
use them; they are not a second production transport.

### Offline replay performance

The final reproducible 10-match fixture benchmark ran five alternating baseline
and typed production passes with zero network bytes. Results:

- compatibility baseline: `0.2532 s/match`;
- typed production adapter: `0.2589 s/match`;
- regression: `2.27%` against the allowed `10%`;
- performance gate: passed;
- all ten typed match persistence contracts passed.
- measured child-process peak RSS: `140,556 KiB` for the ten-page corpus.

```bash
/root/.venvs/dpf-test/bin/python scripts/research/bench_fbref_replay.py \
  --html-dir tests/fixtures/fbref/matches \
  --iterations 5 --max-regression-percent 10 \
  --output /tmp/fbref-replay-benchmark-final.json
```

This is an offline parser-compatibility benchmark, not a claim about live
Cloudflare, proxy traffic, Trino persistence latency, or source-wide crawl
throughput. Production claim and parse queries reject limits above 25, and a
parse wave releases each page before loading the next one; the bounded shard,
not the total registry size, therefore defines the pipeline's live working set.

## Verification evidence

The suites below overlap and must not be added into one total.

| Evidence set | Recorded result | Interpretation |
|---|---:|---|
| Final full unit suite | 3,586 passed, 40 skipped | The post-fix rerun includes the updated Gold scope golden and the logical-refresh-aware latest-observation fence. |
| Post-fix focused golden/scope set | 34 passed | The changed expectation and directly affected Gold scope behavior are green. |
| Observation/control/pipeline/downstream audit | 414 passed | Per-observation fences, latest-only stateful effects, replay/closure gates, policy and registry transitions, typed availability, and downstream identity/scope contracts. |
| Exact hard-cap transport audit | 59 passed | Browser/body/header caps, failed/in-flight/unobserved charging, 304 evidence, 7-MiB reservations, and collision-safe DAG handoffs. |
| Production-image DagBag/integration | 56 passed | Airflow-image DAG imports and the broader relevant integration set. |

### PostgreSQL 16 smoke

An isolated PostgreSQL `16.14` container was tested through the production
Airflow image with the checkout mounted read-only:

- first migration applied `(1, 2, 3, 4, 5, 6, 7)`; second applied `()`;
- CLI rerun returned `{"applied_migrations": []}`;
- the advisory migration lock and concurrent claim/lease fences passed;
- live-PostgreSQL smokes passed for gender eligibility, current/backfill lane
  separation, recurring-to-one-shot policy transitions, and explicit
  cross-run cohort exclusion;
- a live same-hash and A -> B -> A replay smoke proved that only the newest
  successful logical refresh may apply stateful effects;
- versioned observation completion and run-summary closure SQL executed under
  the current generic, typed, and stateful parser fences.

The host test venv lacks `psycopg2`, producing a pre-SQL
`ControlStoreConfigError`; the production Airflow image contains the runtime
driver and passed. The temporary database container was removed.

### Production-image DagBag and integration

The final read-only run used image `data-platform-airflow-scheduler:latest`,
which contains Airflow `2.7.3`. DagBag loaded all seven relevant DAGs with
relevant import errors `{}` and no import errors globally. Observed task counts
were current `20` at 06:00, backfill `54` manual, replay `11` manual, Silver
`16`, xref `8`, Gold `31`, and master `16` at 14:00.

The dedicated current/backfill/replay production-image smoke passed `7/7`.
The final broader five-file integration set passed `56/56`. Its two stale
expectations were corrected to follow the active `xref_manager.sql.j2` template
and the registered transitive SPADL -> WhoScored lineup -> ESPN topology.

## Reproducible verification commands

Offline FBref verification:

```bash
make test-fbref-offline

/root/.venvs/dpf-test/bin/pytest -p no:rerunfailures -q \
  tests/unit/scrapers/test_fbref_control_store.py \
  tests/unit/scrapers/test_fbref_raw_store_v2.py \
  tests/unit/scrapers/test_fbref_page_document.py \
  tests/unit/scrapers/test_fbref_discovery.py \
  tests/unit/scrapers/test_fbref_fetcher.py \
  tests/unit/scrapers/test_fbref_pipeline.py \
  tests/unit/scrapers/test_fbref_generic_bronze.py \
  tests/unit/scrapers/test_fbref_typed_bronze.py \
  tests/unit/dags/test_dag_ingest_fbref.py \
  tests/unit/dags/test_dag_backfill_fbref.py \
  tests/unit/dags/test_dag_replay_fbref.py \
  tests/unit/dags/test_fbref_medallion_order.py \
  tests/unit/dags/test_fbref_pipeline_tasks.py
```

Production-image DagBag:

```bash
docker compose exec -T airflow-scheduler python -c "\
from airflow.models import DagBag; \
ids=['dag_ingest_fbref','dag_backfill_fbref','dag_replay_fbref',\
'dag_transform_fbref_silver','dag_transform_xref',\
'dag_transform_fbref_gold','dag_master_pipeline']; \
b=DagBag('/opt/airflow/dags', include_examples=False); \
print({i: len(b.dags[i].tasks) for i in ids if i in b.dags}); \
print({k:v for k,v in b.import_errors.items() if 'fbref' in k or 'master' in k})"
```

PostgreSQL 16 migration and SQL smoke skeleton:

```bash
docker pull postgres:16
docker run --rm -d --name fbref-control-smoke-pg16 \
  -e POSTGRES_USER=fbref \
  -e POSTGRES_PASSWORD=fbref_smoke \
  -e POSTGRES_DB=fbref \
  -p 127.0.0.1::5432 postgres:16

docker run --rm \
  --network container:fbref-control-smoke-pg16 \
  --volume "$PWD:/work:ro" --workdir /work \
  --entrypoint python data-platform-airflow-scheduler \
  -c 'from scrapers.fbref.control import ControlStore; s=ControlStore("postgresql://fbref:fbref_smoke@127.0.0.1:5432/fbref"); print(s.migrate()); print(s.migrate())'

docker run --rm -i \
  --network container:fbref-control-smoke-pg16 \
  --volume "$PWD:/work:ro" --workdir /work \
  --entrypoint python data-platform-airflow-scheduler - <<'PY'
from scrapers.fbref.control import (
    CohortTarget,
    ControlStore,
    FrontierTarget,
    make_logical_refresh_id,
)

s = ControlStore("postgresql://fbref:fbref_smoke@127.0.0.1:5432/fbref")
run = s.create_run("current", request_limit=10, byte_limit=1_000_000)
s.start_run(run)
targets = [
    FrontierTarget(
        "smoke:season:historical", "season",
        "https://fbref.com/en/comps/1/2020", {"season_id": "2020"},
        "historical_once", 10,
    ),
    FrontierTarget(
        "smoke:schedule:current", "schedule",
        "https://fbref.com/en/comps/1/2026/schedule",
        {"season_id": "2026"}, "six_hourly", 80,
    ),
    FrontierTarget(
        "smoke:match:current", "match",
        "https://fbref.com/en/matches/smoke", {"match_id": "smoke"},
        "daily", 60,
    ),
]
for target in targets:
    s.upsert_frontier_target(target)
s.create_run_cohort(run, [
    CohortTarget(
        target.target_id,
        make_logical_refresh_id(run, target.target_id),
        ordinal,
    )
    for ordinal, target in enumerate(targets)
])

historical = s.claim_targets(
    run, "worker-historical", limit=2,
    page_kinds=["season"], refresh_policies=["historical_once"],
)
assert [lease.target_id for lease in historical] == [
    "smoke:season:historical"
]
assert s.claim_targets(
    run, "worker-empty", limit=2,
    page_kinds=["competition"], refresh_policies=["weekly"],
) == []

def complete(lease, content_hash, billed, latency):
    reservation = s.reserve_budget(
        run, lease.logical_refresh_id,
        attempt_id=lease.attempt_id, requests=1, bytes_=1_000,
    )
    s.bind_reservation(lease, reservation.reservation_id)
    s.settle_budget(
        reservation.reservation_id, requests_used=1, bytes_used=billed,
    )
    s.complete_fetch(
        lease, http_status=200, content_hash=content_hash,
        raw_manifest_key=f"raw/smoke/{content_hash}.json",
        decoded_bytes=billed * 2, compressed_bytes=billed,
        wire_bytes=billed, provider_billed_bytes=billed,
        transport_version="smoke", latency_ms=latency,
    )
    s.record_dataset_manifest(
        target_id=lease.target_id, content_hash=content_hash,
        parser_version="smoke-v1", dataset="__page__",
        availability="available", parse_status="succeeded",
        persistence_status="succeeded", validation_status="succeeded",
        row_count=1,
    )

complete(historical[0], "sha256-historical", 321, 123)
schedules = s.claim_targets(
    run, "worker-schedule", limit=2,
    page_kinds=["schedule"], refresh_policies=["six_hourly"],
)
assert [lease.target_id for lease in schedules] == [
    "smoke:schedule:current"
]
complete(schedules[0], "sha256-schedule", 654, 456)

summary = s.get_run_summary(run)
assert summary["target_counts"] == {"pending": 1, "succeeded": 2}
assert summary["attempt_counts"] == {"succeeded": 2}
assert summary["unvalidated_target_count"] == 0
assert summary["traffic_totals"]["network_attempts"] == 2
print(summary["traffic_by_page_kind"])
print(summary["traffic_totals"])
PY

docker exec fbref-control-smoke-pg16 psql -U fbref -d fbref -Atc \
  "SHOW server_version; SELECT version,name FROM fbref_control.schema_migration ORDER BY version;"

docker stop fbref-control-smoke-pg16
```

Static checks:

```bash
/root/.venvs/dpf-test/bin/ruff check \
  scrapers/fbref dags/dag_ingest_fbref.py dags/dag_backfill_fbref.py \
  dags/dag_replay_fbref.py dags/utils/fbref_pipeline_tasks.py
/root/.venvs/dpf-test/bin/python -m compileall -q scrapers/fbref dags
git diff --check
```

## Rollout plan

1. Merge only after the final focused, full relevant, and production-image
   DagBag/integration reruns are green.
2. Deploy the Airflow image and run the idempotent control migration through
   `airflow-init`; verify migration versions 1–7.
3. Repair the 11 #901 raw event gaps and prove the strict Silver DQ suite green
   before enabling automatic Gold promotion.
4. Diagnose the classified canary HTTP `500`, then obtain a fresh bounded-live
   authorization. Rerun the direct canary under a preventive kernel byte quota;
   do not reuse the failed run or infer a green result from unit mocks.
5. Inspect the control summary for budget use, female/unknown downstream count,
   warm success, bootstraps, duplicate fetches, failures, P50/P95, page-kind
   bytes, table availability, and sentinel coverage.
6. After a successful raw commit, replay that canary source run through
   `dag_replay_fbref`; require zero network attempts/bytes and zero remaining
   parser-version candidates.
7. Promote Silver -> xref -> Gold only after all strict gates pass. Monitor at
   least one daily 06:00 run before expanding historical backfill.
8. Run historical backfill manually in repeated 25-request batches. Each new
   run selects the next unfinished registry cohort automatically; do not supply
   or maintain an external cursor. A full 400-page soak requires separate
   authorization and is not part of this plan.

The bounded no-Silver runner must be invoked through its kernel-quota wrapper:

```bash
sudo env RUN_LIVE_FBREF_CANARY=1 \
  scripts/research/run_fbref_canary_guarded.sh
```

The wrapper creates a short-lived container/network namespace and installs
18-MiB ingress plus 4-MiB egress kernel quotas before invoking the one-page
runner. The runner never imports Airflow or triggers Silver.

## Rollback plan

1. Pause `dag_ingest_fbref`, `dag_backfill_fbref`, and `dag_replay_fbref`.
2. Roll back the ingest/Silver/xref/Gold/master DAG set atomically to the prior
   tested image or commit. The master sensor must not remain waiting on a DAG
   that was removed or paused.
3. Preserve `fbref_control` and raw-v2 objects. Migrations are additive; do not
   drop the schema or delete raw evidence during rollback.
4. Existing consumers can continue using legacy source-score and compatibility
   columns because changes are additive. Rebuild affected Silver/Gold tables
   from the prior SQL if a consumer regression is found.
5. After a parser fix, create a new parser version and replay immutable raw;
   never mutate a completed dataset manifest in place.
6. Re-enable the new path only after the bounded canary and strict DQ evidence
   are green.

## Remaining blockers

| Blocker | Consequence | Exit evidence |
|---|---|---|
| ~~Bounded canary failed on warm HTTP `500`~~ **CLEARED 2026-07-12** | — | Canary `562f020a-8f9b-53fe-bb5d-2b3825e5139e` succeeded: raw committed, parse green, every traffic/eligibility/coverage gate passed. |
| Full 400-page soak not authorized | No production-scale throughput or long-session stability claim is valid. | Separate approval, hard request/MB cap, and recorded soak report. |
| #901: 11 raw event gaps | Strict Silver DQ blocks promotion; weakening it would hide missing source data. | Raw is absent for all eleven (the legacy scraper kept none), so offline replay cannot repair them. `scripts/research/run_fbref_match_refetch.py` performs the bounded, control-plane-native refetch; exit when zero scored matches lack events. |
| Live traffic benchmark pending | No measured before/after Cloudflare/proxy/provider-billing claim is valid. Offline parser regression is measured at 2.27% and passes. | Reproducible bounded live benchmark with the approved page/request/byte mix and provider evidence. |
| GitHub Project scope unavailable | Project-field classification cannot be verified or updated. | Credential with `read:project`, followed by a read-only audit or separately authorized mutation. |
