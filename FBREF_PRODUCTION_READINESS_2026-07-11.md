# FBref production readiness report

Date: 2026-07-11 (updated 2026-07-15)

Baseline: `origin/master` at `1a93b2b`

Companion backlog: [`FBREF_PRODUCTION_BACKLOG_2026-07-11.md`](FBREF_PRODUCTION_BACKLOG_2026-07-11.md)

## Executive status

**Production candidate implemented; deployment validation pending.** The checkout contains a
single durable raw-first production flow, bounded backfill and zero-network
replay, generic and typed Bronze persistence, strict promotion edges, and the
requested DQ/identity/keeper consumers. It is not yet valid to call the source
production validated:

- the last bounded live canary completed under the earlier candidate, but no
  paid source request has been made for the post-review checkout documented
  here;
- a paid/full 400-page soak was not authorized and was not executed;
- an accepted before/after live traffic benchmark was not run;
- #901 is diagnosed (2026-07-12): FBref publishes no events for those eleven matches at
  all, so the fix is a finite evidence registry, not a repair — see below;
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
  -> one bounded Camoufox clearance lifecycle per live runner
     (up to four paid solve attempts only in the 200-request profile)
  -> warm curl_cffi HTTP target fetch
  -> immutable raw-v2 response/content commit
  -> offline discovery + generic PageDocument + typed compatibility parse
  -> Bronze staging/MERGE + independent dataset availability + page marker
  -> run traffic/completeness/eligibility validation
  -> blocking Silver + Silver DQ (source-acceptance boundary)
  -> separately scheduled xref/Gold chain outside source acceptance
```

The configured `LEAGUE_IDS` mapping remains only a backward-compatible output
projection for existing Bronze/Silver consumers. It is never iterated to choose
FBref crawl scope.

### Airflow topology

Each live DAG has one `run_live_waves` Airflow task. It starts
`dags/scripts/run_fbref_live_waves.py` in a new process session and drains up to
16 raw-first `fetch -> offline parse` batches while retaining one warm HTTP
session and one in-run proxy quarantine. Camoufox is used only to export a
clearance: the browser is closed before its final traffic is measured and the
bootstrap reservation is settled. Parse and seed remain source-network-free;
readiness and raw-integrity tasks use only PostgreSQL, Trino, and the durable
raw store, never the paid FBref transport.

| DAG | Schedule and budget | Static topology | Promotion contract |
|---|---|---|---|
| `dag_ingest_fbref` | Daily `06:00` UTC; production 200 requests/100 MiB, canary 100/50 MiB; shard at most 25 | 16 tasks: readiness, initialize, lock, seed index, anchored pre-run content inventory, raw recovery, one live runner (up to 16 batches), sealed-attempt raw audit, current-only promotion gates, blocking Silver and lock finalizer | Canary runs validate and release the lock without publishing; production runs require raw integrity, freshness, blocking Silver success, and a final lock verdict. |
| `dag_backfill_fbref` | Manual only; production 200 requests/100 MiB, canary 100/50 MiB; durable resume; shard at most 25 | dry-run planning or readiness, initialize, current-freshness preflight, bounded historical seed, pre-run inventory, raw recovery, one live runner, persisted raw audit, validation and Silver | Claims only `historical_once` targets. Completed historical seasons are not requeued, and no operator cursor or manual league list exists. |
| `dag_replay_fbref` | Manual only; required source control-run UUID; request and byte budgets are both zero | 17 tasks: readiness, initialize, lock, pre-run inventory, eight sequential parse-only waves of 25, zero-delta raw audit, validate, export, blocking Silver, finalizer | Contains no fetch or seed task; validation fails if source candidates remain, raw changes, or any proxy traffic appears. |

The master pipeline no longer triggers a second paid FBref crawl at 14:00. It
uses a fail-closed `ExternalTaskSensor` with an eight-hour execution delta to
wait for the scheduled 06:00 ingest and its blocking Silver/xref chain.

## Control-plane contract

The `fbref_control` schema shares Airflow's PostgreSQL server but has no Airflow
ORM dependency. `airflow-init` runs `python -m scrapers.fbref.control migrate`
after the Airflow metadata migration.

Nine checksum-verified, advisory-lock-protected migrations create:

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
- a retry reuses raw when that exact `logical_refresh_id` already has a
  committed manifest, which is crash recovery for the same observation;
- a `historical_once` target may additionally adopt a verified prior raw-v2 or
  legacy raw-v1 observation into a new immutable raw-v2 manifest. Recurring
  current targets and `current_completed_once` transitions never do this and
  require a fresh final fetch;
- request and byte budgets are reserved before network activity and settled
  with observed usage;
- one shared domain slot enforces a default three-second interval.
- each pre-run raw inventory is create-once outside the raw prefix and its
  exact SHA-256 is conflict-checked in `crawl_run.metadata`;
- raw audit first seals and fingerprints the successful-attempt set under the
  crawl-run row lock, preventing a late worker from changing paginated audit
  evidence;
- `_health/` is the only excluded ephemeral raw namespace; every durable raw
  object remains covered by the content/metadata inventory.

## Raw, parsing, and persistence contracts

### Raw-v2

`RawPageStore` keeps backward read compatibility with raw-v1 and writes
raw-v2. Exact-logical-refresh recovery is valid for every lane. To avoid a
second paid request for immutable history, only a `historical_once` target may
adopt verified prior raw-v2 or legacy raw-v1 into its new logical refresh;
recurring/current-completion work cannot. Mismatched or corrupt evidence fails
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

The before-run inventory is bounded on heap: local storage is traversed with
`scandir`, S3 with 1,000-object `ListObjectsV2` pages, and object rows live in
an authoritative SQLite index beside a small JSON commit marker. Local
device/inode/ctime or S3 ETag version tokens prevent unsafe mtime-only reuse.
A per-destination `flock` permits only one full-store hasher, and two stability
scans fence the commit. The post-run audit artifact and SHA-256 sidecar are
content-addressed, create-once, fsynced, reopened, and verified; a retry can
complete a digest-first partial publication but cannot overwrite evidence.

After a passed raw audit is durably anchored in PostgreSQL, a fixed-size local
cache marker may point at that run's verified SQLite index. The next run still
lists all raw metadata, but it reuses a SHA-256 only when the object key, size,
and strong local device/inode/ctime or S3 ETag token are unchanged. New,
changed, or unversioned objects alone are read and hashed. The marker is
checked against the control-plane baseline anchor, passed raw-audit anchor,
content-addressed audit JSON, and its SHA-256 sidecar; missing cache is a safe
first-run full hash, while corrupt or untrusted cache fails closed before paid
source work.

Index retention is bounded per call. It removes only old SQLite indexes for
finished-successful runs that still have a verified passed raw-audit anchor.
It never removes the active/cache-source index or an index for a running,
failed, or unanchored run. The small baseline JSON and SHA-256 sidecar plus the
final raw-audit JSON and sidecar remain queryable after index cleanup, so
DataGrip/control-plane acceptance evidence is retained without one permanent
million-row SQLite copy per run.

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
requests and persisted. The 100-request canary can fund one solve; the
200-request profile may rotate across at most four failed exits. Any attempt
beyond the reserved profile fails closed instead of being hidden behind a
boolean metric.

Hard defaults and caps:

- current run: 200 total browser+HTTP requests, 100 MiB;
- backfill run: production 200 requests/100 MiB or canary 100/50 MiB;
- replay: 0 requests and 0 bytes;
- cumulative warm-HTTP response bodies for one target, including the bounded
  status retry: at most 2 MiB, enforced by the native libcurl write callback;
- each logical warm target reserves two HTTP attempts and 3 MiB before
  transport: 2 MiB for cumulative bodies plus 1 MiB for wire overhead;
- bootstrap has an independent reservation: up to 80 browser requests/16 MiB
  for a 200-request production run, or 20 requests/4 MiB for a 100-request
  canary. Settlement releases unused capacity;
- the browser is closed before final bootstrap accounting, so teardown and
  late in-flight traffic cannot continue after reservation settlement;
- shard size is at most 25 for current and backfill; the live runner drains at
  most 16 batches under the single shared run budget;
- domain interval: three seconds.

The control budget is reserved before transport and fails validation on an
overspend. The transport also fails closed before an individual response can
jump the cap: warm HTTP streams through a cumulative callback bound across
retry attempts, while every browser request reserves fixed framing overhead
and then its declared `Content-Length` against all parallel in-flight work.
An undeclared/chunked browser body reserves a bounded 512-KiB ceiling before it
may be read; an invalid declaration, an oversized declared body, or growth past
that ceiling aborts the whole browser session. Failed requests use Playwright's
observed sizes when available; if sizes are unknown, their full reservation
remains charged. Teardown likewise charges every still-in-flight reservation,
so partial traffic cannot be reused to cross the cap. The standalone one-page
diagnostic can additionally use a network-namespace quota; it is not the
issue-949 Airflow acceptance canary.

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

Because bootstrap activity is charged to the same request budget, the removed
25-request experimental profile never implied 25 target pages: a 20-request
bootstrap plus two requests per logical target could admit at most two targets.
The supported Airflow canary is 100 requests/50 MiB and can admit one 25-target
shard when the shared actual-traffic budget remains healthy.

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
rate is at least 0.5%, duplicate fetch violations are nonzero, a session exceeds
the four-attempt hard solve cap, female/unknown downstream targets are nonzero,
a dataset/page marker is incomplete, or a budget is exceeded. The smaller
canary reservation prevents a second solve before this final gate. Closure also
requires the current generic, typed, and stateful parser-version tuple for every
logical observation in current, backfill, and replay runs. The failed live
canary exercised the traffic gates, but it did not reach raw persistence, table
inventory, registry coverage, or sentinel gates.

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

### #901: the gap is in the source, not in the scrape

Update 2026-07-12: the eleven matches were refetched through the production
control plane (bounded backfill runs, `scripts/research/run_fbref_match_refetch.py`).
The raw pages prove FBref publishes **no events at all** for them: the page
ships the two `div.event` side columns and both `stats_<team>_summary` tables
as empty containers, with no `#events_wrap`, while managers, team stats and
officials parse normally. Nine are relegation play-offs whose player tables are
empty as well; Nantes–Toulouse (FRA-Ligue 1 2025-2026) publishes full lineups
and player stats but still no events.

This is the "externally unavailable" branch of the #934 criteria, so the fix is
evidence, not synthesis: `configs/medallion/fbref_event_source_gaps.yaml` is a
finite registry (match_id, league, season, reason, evidence) rendered into the
Silver SQL exactly like `match_result_overrides.yaml`.

- `scored_match_without_events[silver.fbref_match_enriched]` keeps its zero-row
  ERROR contract and now excludes **only registered** matches — an unregistered
  scored match with no events still blocks promotion;
- `stale_event_source_gap[silver.fbref_match_enriched]` (new, ERROR) fires the
  moment a registered match starts carrying events, so the registry cannot rot
  into a mute button.

The pipeline still must not synthesize missing FBref events or weaken the gate.

### Live-only defects found while parsing match pages

Two more defects surfaced only against real match pages and are fixed:

3. The match parser counted the two **empty** `div.event` side columns (present
   on every FBref match page) as unparsed event rows, and treated published-but-
   empty player tables as a contract failure. A page whose events, team stats,
   managers and officials all parse cleanly was therefore rejected wholesale
   (commit `5bacdd4`).
4. `discover_page_links` read the page slug of a season-less comps link
   (`/en/comps/8/Champions-League-Stats` — FBref's address for a competition's
   *current* season) as a `season_id`, and let a page lend its own season to
   such links. A 2016-2017 match page thus tried to register a target whose
   canonical URL already belongs to the registry-seeded current season, the
   frontier upsert raised `StateConflict`, and **the parse wave failed on every
   match page** (commit `2870a46`).

### Lease and hang defects found while running bounded batches

5. `reap_expired_leases()` existed but had no caller, and `claim_targets` reaps
   only the leases of the run doing the claiming. A worker killed mid-wave left
   its targets `leased` forever: they dropped out of the crawl and kept
   `promotion_pending_match_count` above zero, which fails the validation gate
   of every later run. The run start now reaps globally (commit `939d989`).
6. `Camoufox.__enter__()` can block on a dead proxy (observed twice live, with
   the process idle inside Playwright's event loop). All live batches now run in
   one new process session bounded at 110 minutes inside a 120-minute Airflow
   task. Timeout or external task termination sends SIGTERM to the complete
   runner/browser process group, then SIGKILL after a bounded grace period.

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
| 2026-07-15 issue-949 offline suite | 916 passed | Final-review CI selection: every FBref unit test plus the shared maintenance DAG/task, proxy-manager, and lazy-import tests, with plugin autoload disabled. |
| 2026-07-15 real PostgreSQL semantics | 4 passed | Current-match priority, concurrent cohort exclusion, publication-lock fencing, immutable raw-baseline anchoring, and successful-attempt sealing ran against isolated migrated PostgreSQL databases. |
| 2026-07-15 production Airflow 2.11.2 DagBag | 16 / 16 / 17 tasks | A byte-verified copy of the candidate supplied both `scrapers` and `dags/utils`; current, backfill, and replay loaded with no import errors. |
| 2026-07-15 real SeaweedFS raw contract | 1 passed | Raw-v2 commit/readback/idempotency plus the disk-backed before/after audit ran through the production S3 API; all 15 objects under the unique test prefix were deleted and absence rechecked. |
| 2026-07-15 DataGrip/janitor scope | 37 passed, 1 skipped | Both acceptance SQL files parsed (11 Trino and 4 PostgreSQL statements); generated checks ran against real services and exact/no-report match completeness stayed fail-closed. |
| 2026-07-15 real Trino/Iceberg janitor | 1 passed | A nonempty stale FBref staging table was semantically proven stale, publication-fenced, dropped, and its absence rechecked. |
| Earlier repository-wide unit baseline | 3,586 passed, 40 skipped | Pre-final-patch regression history including Gold scope and logical-refresh latest-observation behavior. |
| Earlier focused golden/scope set | 34 passed | Pre-final-patch downstream Gold scope evidence. |
| Earlier observation/control/downstream audit | 414 passed | Pre-final-patch evidence for observation fences, policy transitions, typed availability, and downstream identity/scope. |
| Earlier hard-cap transport audit | 59 passed | Pre-final-patch browser/body/header cap, 304, reservation, and DAG-handoff evidence. |
| Earlier broad production-image integration | 56 passed | Pre-final-patch regression history for Airflow imports and adjacent downstream integrations; not the candidate-specific issue-949 gate. |

### PostgreSQL 16 smoke

The exact candidate copy was tested through the production Airflow runtime
against an isolated database on PostgreSQL `16.14`. Migrations 1–9 were applied
before pytest. Four real-transaction tests proved current-match admission ahead
of enrichment backlog, concurrent cross-run cohort exclusion, publication-lock
fencing across a simulated Trino write, and immutable raw-baseline/attempt-set
anchors. The fixture's nested race databases and the candidate database were
terminated and dropped; a final catalog query found no `fbref_candidate_949_*`
or `fbref_race_*` database.

The host test venv intentionally remains import-light and lacks `psycopg2`;
the production Airflow runtime contains the driver and is the environment that
passed this semantic gate.

### Production-image DagBag and integration

The final read-only run used the active `airflow-scheduler` runtime with
Airflow `2.11.2` and boto3 `1.42.61`. The candidate was copied to an isolated
directory rather than imported from the active `/opt/airflow` mounts; SHA-256
comparison covered all 55 present changed or untracked candidate files with zero
mismatches. DagBag then loaded current `16`, backfill `16`, and replay `17`
tasks with import errors `{}`. Module provenance for both `scrapers` and
`dags/utils` resolved inside that isolated copy.

CI imports the same DAGs on both the repository image pin (`2.7.3`) and the
active runtime (`2.11.2`). Cross-DAG handoffs use the backward-compatible
`execution_date` argument accepted by both versions.

This candidate-specific run intentionally makes no fresh claim about unrelated
xref, Gold, or master DAGs. Their earlier broad integration evidence remains
useful regression history, but issue #949 accepts the FBref source boundary at
the blocking Silver/Silver-DQ child.

## Reproducible verification commands

Offline FBref verification:

```bash
make test-fbref-offline

mapfile -t tests < <(find tests/unit -type f -name '*fbref*.py' -print | sort)
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /root/.venvs/dpf-test/bin/python -m pytest -q \
  "${tests[@]}" \
  tests/unit/dags/test_dag_iceberg_maintenance_daily.py \
  tests/unit/dags/test_maintenance_tasks.py \
  tests/unit/scrapers/test_proxy_manager.py \
  tests/unit/scrapers/test_scrapers_lazy_import.py
```

Production-image DagBag (run after deploying the candidate mounts):

```bash
docker compose exec -T airflow-scheduler python -c "\
from airflow.models import DagBag; \
expected={'dag_ingest_fbref':16,'dag_backfill_fbref':16,'dag_replay_fbref':17}; \
b=DagBag('/opt/airflow/dags', include_examples=False); \
counts={i:len(b.dags[i].tasks) for i in expected}; \
assert counts == expected, counts; \
assert not {k:v for k,v in b.import_errors.items() if 'fbref' in k}, b.import_errors; \
print(counts)"
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

1. Merge and deploy one immutable SHA only after the final focused, complete
   offline, real-dependency, and production-image checks are green. Run the
   idempotent control migration through `airflow-init` and verify migrations
   1–9 and their checksums.
2. Repeat the issue-949 backfill dry-run on that SHA. Require the expected
   seven-season cohort, zero network requests, zero state mutations, and no
   publication lock or Silver child.
3. Run one explicitly non-publishing current canary with exactly 100 requests,
   50 MiB, and shard size 25 through `dag_ingest_fbref` DagRun conf. Reconcile
   the control counters with the dedicated provider counter; do not substitute
   the standalone one-page research runner for this Airflow acceptance run.
4. For that canary, require the anchored raw inventory and sealed-attempt audit,
   lossless generic Bronze parity, every materialized typed Bronze contract,
   every run-owned DQ gate, and zero female/unknown publication. Record the
   global pending-match backlog as a diagnostic; the non-publishing canary is
   not allowed to claim that a bounded cohort closed the global frontier. Run
   `fbref_canary_acceptance.sql` in Trino and
   `fbref_control_dataset_acceptance.sql` in PostgreSQL against the same
   control-run UUID. Bind the control check to `current`, `100`, and `50` for
   expected run type, request limit, and byte limit MiB. The canary has no
   publication-scope generation or Silver child, so the production Trino
   script is not a canary gate.
5. Run two consecutive publishing current loads on the same immutable SHA with
   the 200-request / 100-MiB / shard-25 profile. Require the blocking
   FBref Silver transform and its DQ, zero promotion-pending current matches,
   and the final publication-lock verdict to succeed for both runs. The
   separately scheduled master/xref/Gold chain is outside this source
   acceptance gate. Run `fbref_production_acceptance.sql` in Trino and the
   PostgreSQL companion for each control-run UUID; bind the companion to
   `current`, `200`, and `100`.
6. Replay one accepted source control run through `dag_replay_fbref`. Require
   request/byte budgets `0/0`, no fetch path, a zero raw-store delta, and no
   remaining parser-version candidates. Run the PostgreSQL companion against
   the replay control-run UUID with bindings `replay`, `0`, and `0`.
7. Record run IDs, control summaries, raw object/byte counts, Bronze row counts,
   DQ output, and provider-traffic deltas in issue #949. Set the source to GO
   only when the canary, both current runs, and replay are green on the same
   SHA.
8. Only after GO, start the separate bounded historical backfill. It selects
   unfinished male-season cohorts from the durable registry; no manual league
   list or external cursor is permitted. A full 400-page soak still requires
   separate traffic authorization.

The older standalone one-page runner remains an optional transport diagnostic,
not an issue-949 GO gate. If separately authorized, it must still be invoked
through its kernel-quota wrapper:

```bash
sudo env RUN_LIVE_FBREF_CANARY=1 \
  scripts/research/run_fbref_canary_guarded.sh
```

The wrapper creates a short-lived container/network namespace and installs
18-MiB ingress plus 4-MiB egress kernel quotas before invoking the one-page
runner. It never imports Airflow or triggers Silver and therefore cannot replace
the required non-publishing `dag_ingest_fbref` 100/50/25 canary.

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

## Remaining production-acceptance gates

| Gate | Current consequence | Exit evidence |
|---|---|---|
| This candidate is not deployed as one immutable SHA | Offline and dependency smokes do not constitute production acceptance. | Deploy one reviewed SHA, verify migrations 1–9 and record the runtime tree/image identity. |
| Fresh bounded canary not run on this candidate | The source remains NO-GO and no publication/backfill is authorized. | Green non-publishing 100/50/25 canary with reconciled provider traffic and complete raw/Bronze/DQ evidence. |
| Two sequential production current runs not run | Daily stability and idempotent second-run behavior remain unproven. | Two green 200/100/25 current runs on the same SHA, including the blocking publication chain. |
| Zero-network replay not run from an accepted source run | Raw preservation and parser-only recovery are not yet proven in the deployed runtime. | Green replay with budgets `0/0`, no fetch task/path, zero raw delta, and no pending parser-version candidates. |
| Full 400-page soak not authorized | No production-scale long-session throughput claim is valid. | Separate approval, hard request/byte cap, and a recorded soak report. |
