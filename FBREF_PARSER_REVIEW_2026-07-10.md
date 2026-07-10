# FBref parser review and scale-out design

Date: 2026-07-10

## Verdict

The current scraper is a useful EPL-oriented collector, but it is not yet a
safe foundation for “all competitions, all players, all available stats”.
Scaling the current nested loops would multiply proxy cost and silently retain
coverage holes because discovery, availability, persistence, and completeness
are inferred from a few hard-coded tables.

The immediate patch in this branch removes several concrete amplifiers and
false-success paths. The next architectural step must be a raw-first page
frontier with dataset-level manifests. Adding more constants or more parser
fallbacks would increase technical debt.

## Low-traffic benchmark

The old implementation was not rerun. The comparison reuses the saved
10-match APL baseline and runs only the review version on the same URLs.
Because the runs happened on different dates and the old counter did not
record compressed HTTP wire bytes, this is a directional comparison rather
than provider-billing proof.

| Metric | Historical nodriver baseline | Review version, Camoufox |
|---|---:|---:|
| Successful matches | 10/10 | 10/10 |
| Total time | 23.77 s | 28.09 s |
| First cold page | 17.51 s | 23.80 s |
| Mean warm page | 0.481 s | 0.360 s (-25%) |
| Browser proxy traffic | 2.750 MB | 1.001 MB (-64%) |
| HTTP wire traffic | not recorded | 0.462 MB |
| Total proxy traffic | more than 2.750 MB | 1.462 MB (at least -47%) |
| Total requests | 74 | 24 (-68%) |

The cold Camoufox bootstrap is slower, but warm pages are faster and much
cheaper. A 380-match mechanical projection, including three browser
bootstraps for the 150-request HTTP lease, is about 3.5 minutes and 22 MB for
fetching match HTML. Offline parsing of the ten captured pages takes about
0.23 s per match, or roughly 90 seconds for 380 matches, with zero proxy bytes.
These projections exclude schedule/season pages, Iceberg writes, rate limits,
and retries; a full DAG should therefore be budgeted separately and verified
with provider-side billing.

Raw HTML was retained under
`/root/fbref-benchmark-artifacts/2026-07-10` for future zero-network replay.
The reusable replay runner is `scripts/research/bench_fbref_replay.py`.

## What the current code collects

Season pages:

- four outfield player tables: standard, shooting, playing time, misc;
- four squad tables for the same categories;
- one basic goalkeeper table.

Match pages, in one HTML pass:

- events and lineups;
- team match statistics;
- player summary statistics;
- managers and officials;
- basic keeper statistics;
- shot events when FBref exposes the table.

Important gaps for full FBref coverage:

- competition discovery and competition history;
- season/calendar metadata (single-year, split-year, tournament editions);
- standings and competition metadata;
- squad pages, rosters, and team profiles;
- player profiles, biographical fields, career history, and match logs;
- opponent squad tables;
- every non-summary match table that is actually available;
- an explicit availability/capability matrix for restricted, empty, or
  not-applicable datasets.

The [FBref competition index](https://fbref.com/en/comps/) currently
advertises more than 100 men’s and women’s competitions. The hard-coded
eight-entry LEAGUE_IDS dictionary and the unconditional
year-to-year season formatter therefore cannot represent the source.

## Confirmed failure modes

### Correctness and completeness

1. max_matches used to be applied before the incremental skip filter. A
   chunked backfill could repeatedly select the already completed prefix and
   never advance.
2. A match page with one team summary table could be accepted as complete.
   The parser could also return one successfully parsed side when the second
   table was malformed.
3. fbref_match_player_stats acted as a global completion marker even though
   managers, officials, and keeper data were written after it. A later write
   failure permanently hid those gaps.
4. JSON recovery files were returned in the same mapping as Iceberg tables.
   The runner could report a green task even though persistence was incomplete.
5. Generic table fallbacks could attach the wrong table to a stat type after a
   layout change.
6. No-summary tombstones counted rows rather than independent logical runs.
   Airflow retries could create a permanent exclusion; there was no TTL or
   systemic layout-drift guard.
7. Partial parser work was appended directly to shared batch buffers before
   all parsers for that match finished.

### Proxy traffic and latency

1. An expired curl_cffi session remained present after its request/TTL limit,
   so every subsequent page fell back to a full browser navigation.
2. A truncated HTTP response was counted as a fast-path hit and failed page
   validation without attempting the warm browser.
3. Internal Camoufox proxy rotations did not consistently report the proxy
   actually attempted.
4. Browser lifecycle errors and page-contract errors were attributed to proxy
   health as timeouts or Cloudflare failures, burning healthy proxies.
5. HTTP traffic used len(response.text), which measures decoded Python
   characters rather than request and response bytes transferred through the
   paid proxy.
6. Fixed /tmp filenames could reuse schedules, result manifests, and traffic
   reports from an older DAG run.
7. The strongest saving is still missing: parser changes require downloading
   every page again because there is no durable compressed raw HTML layer.

### Orchestration

1. all_done traffic observers were the only downstream edge from producer
   tasks. A failed producer could be followed by a successful guard and allow
   the pipeline to continue.
2. Validation scanned every fbref_*.json in shared /tmp and used an obsolete
   count threshold. Old files could hide current failures.
3. Qualified Iceberg paths were compared with short table names, producing
   false missing-table warnings.
4. Silver depended on the passive traffic reporter rather than directly on
   successful validation.
5. DAG league and match-limit parameters were exposed but were not threaded
   into the scraper commands.

### Scale and maintenance blockers

1. Combined season collection buffers every requested league-season before
   writing. At source-wide scope this becomes an avoidable memory and
   failure-domain problem; persist one bounded target/shard at a time.
2. Completeness guards aggregate multiple partitions, so a large healthy
   league can hide a missing small competition. Validate each target
   independently.
3. Iceberg atomic writes use a predictable per-table staging name. Parallel
   shards need a run/task-unique staging table and orphan cleanup before
   concurrency can be increased safely.
4. A separate legacy nodriver FBref scraper duplicates a large portion of the
   implementation and can drift from the active parser/transport.
5. Display headers remain part of many Bronze schemas. Source changes create
   unstable columns and parser-specific cleanup instead of one data-stat
   contract.

## Changes made in this review branch

- Apply max_matches after incremental filtering.
- Thread runtime league and max-match parameters into tasks; pass league
  values through a quoted environment variable and validate DAG parameter
  types.
- Expire, close, and re-mint curl sessions after TTL/request limits.
- Validate HTTP responses before counting a fast-path success and fall back to
  the warm browser on a page-contract failure.
- Require two distinct eight-character team summary IDs at both the HTML and
  parsed-DataFrame layers.
- Stage per-match parser outputs locally and disable the redundant match HTML
  cache, making retries independent.
- Remove broad player-table fallback discovery.
- Record Camoufox results per attempted proxy while separating hard proxy
  failures, transient network errors, browser crashes, Cloudflare failures,
  and source page-contract failures.
- Count curl request headers, response headers, upload, and download bytes;
  keep decoded HTML size as a separate metric.
- Write player stats last and suppress that completion marker when an earlier
  available dataset failed to persist.
- Stamp player completion rows with a parser-contract version; legacy rows do
  not silently satisfy the new two-team/marker-last contract and therefore
  require a controlled backfill.
- Separate fallback files from Iceberg tables and return a non-zero runner
  result when any fallback is produced.
- Scope result, schedule, fallback, and traffic files by Airflow run ID.
- Make traffic guards fail closed, preserve producer-to-consumer failure
  edges, and prevent Silver from starting after failed validation.
- Validate the three exact current-run manifests and the current 14-table core
  contract.
- Fail combined tasks when any requested league-season is missing, when a
  deadline/circuit breaker leaves match IDs unresolved, or when an empty
  incremental run cannot verify eligible IDs and required Iceberg tables.
- Count no-summary confirmations by distinct logical run, expire them after
  30 days, make observations idempotent per run, and evaluate/refuse suspicious
  tombstone batches globally across the whole multi-league run.

These fixes harden the existing architecture. They do not by themselves add
competition discovery or player-profile coverage.

## Target architecture

### 1. One authoritative competition registry

Replace constants.py, leagues.yaml, competitions.yaml, and DAG defaults as
independent sources of truth with one discovered registry:

| Field | Purpose |
|---|---|
| fbref_competition_id | Stable source identifier |
| canonical_competition_key | Internal cross-source key |
| slug, name, country, gender, tier | Descriptive dimensions |
| season_id, season_label, season_url | Source-native season identity |
| calendar_type | split_year, single_year, tournament |
| start_date, end_date, current | Refresh planning |
| discovered_at, last_seen_at | Lifecycle and deletion detection |

Seed the registry from /en/comps and follow source-provided season-history
links. Never synthesize every season URL as year-(year+1).

### 2. A persistent page frontier

The unit of scheduling should be a canonical page target, not a nested Python
loop. Suggested Iceberg table:

fbref_page_frontier:

- target_id and canonical_url;
- page_kind: competition, season, schedule, squad, player, match, matchlog;
- competition_id, season_id, squad_id, player_id, match_id where applicable;
- priority and not_before;
- state: pending, leased, fetched, parsed, retryable, terminal;
- attempts, last_error_class, next_retry_at;
- content_hash, http_status, fetched_at;
- encoded_bytes, decoded_bytes, proxy_wire_bytes;
- fetcher_version and validator_version.

Lease rows in bounded shards. A failed worker returns only its leased targets
to retry; it must not restart an entire league-season.

Every shard must use a unique Iceberg staging identifier. Concurrency should
remain one until that writer-level invariant is implemented.

### 3. Raw HTML before parsing

For every validated response:

1. canonicalize the URL;
2. hash the response bytes;
3. compress once with zstd or gzip;
4. write the blob to durable object/HDFS storage under the content hash;
5. commit the fetch manifest;
6. parse from the stored blob in a proxy-free task.

Content addressing deduplicates unchanged pages and makes parser development,
backfills, and schema migrations consume zero proxy MB. Local /tmp is not a
durable raw layer and is unsafe across distributed Airflow workers.

### 4. Dataset-level availability and completion

Do not use one Bronze table as a match-wide completion marker. Maintain:

fbref_dataset_manifest:

- target_id, dataset, parser_version, content_hash;
- availability: available, structurally_empty, restricted, not_applicable,
  unknown;
- fetch_status, parse_status, persist_status;
- row_count, identity_count, home_rows, away_rows;
- schema_signature and validation_errors;
- run_id, first_seen_at, last_checked_at.

Incremental skip then becomes:

target + dataset + parser_version + content_hash is persisted or explicitly
unavailable.

This distinguishes a legitimately absent keeper table from a broken keeper
parser and permits backfilling a newly added dataset without re-fetching pages.

### 5. Lossless Bronze, typed Silver

Display headers and pandas column normalization are not stable contracts.
Extract FBref data-stat identifiers and retain:

- source table ID and row identity;
- stable entity IDs from links;
- raw stat key/value pairs;
- raw header path and value text;
- parsed numeric value and unit when conversion succeeds.

A generic MAP(VARCHAR, VARCHAR) or long-form stat table preserves new source
columns without emergency schema changes. Curated typed tables for frequently
used metrics should be derived offline in Silver. Never relabel an arbitrary
table as another stat type when an expected table is absent.

### 6. Discovery graph without redundant player crawling

Recommended discovery order:

competition index
  -> competition seasons
    -> season overview/schedule/standings
      -> squads and matches
        -> unique players
          -> player profile and only required match-log categories

Season and match tables already contain much of the player-stat surface.
Deduplicate player IDs globally and fetch a profile once per content/version
policy. Do not download every player match log merely to recreate data already
present on match pages.

### 7. Fetch policy and proxy budget

- Use Camoufox only to obtain/refresh a clearance lease.
- Export cookies, real user agent, fingerprint target, and the same exit proxy
  to curl_cffi for HTML-only requests.
- Keep a bounded lease by age, request count, and consecutive validated
  failures; close replaced sessions.
- Block images, fonts, media, stylesheets, analytics, ads, and the known
  autocomplete endpoint while always allowing challenge assets during
  clearance acquisition.
- Refresh completed historical seasons only on a long audit cadence.
- Refresh current schedules frequently, completed matches once, and mutable
  profiles at a lower cadence.
- Canonicalize and deduplicate URLs before a request. A parser retry must read
  raw storage; only a fetch validation failure may spend another proxy request.
- Enforce global per-domain rate and byte budgets, not only per Airflow task.

Suggested operational SLOs:

| Metric | Initial gate |
|---|---:|
| HTTP fast-path success after bootstrap | >= 95% |
| Browser bootstrap | <= 1 per shard/session lease |
| Duplicate canonical URL fetches in a run | 0 outside explicit fetch retry |
| Parser reprocessing proxy bytes | 0 |
| Match pages with two parsed team summaries | 100% or explicit unavailable |
| Unclassified failures | < 0.5% |
| Soak test | >= 400 sequential pages |

Track P50/P95 proxy wire bytes per successful page kind, retries per target,
clearance MB, HTTP/body overhead ratio, blocked bytes, and cost per persisted
dataset. A single average MB/run hides cold-start and retry amplification.

## Delivery sequence

### Phase A — stabilize the current collector

Land the fixes in this branch, run a 400+ page proxy soak, and establish
budgets by page kind. Backfill managers/officials/keeper data with explicit
non-incremental scope until a dataset manifest exists.

### Phase B — raw store and manifests

Add content-addressed compressed raw storage, the page frontier, and
fetch/dataset manifests. Split fetch from parse. This phase produces the
largest long-term proxy saving.

### Phase C — discovery and season model

Discover all competition and season links, introduce source-native season IDs,
and remove the hard-coded competition URL builder. Validate representative
split-year, single-year, cup, international, women’s, and historical seasons.

### Phase D — generic table extraction and capability catalog

Inventory table IDs and data-stat columns from stored pages. Populate the
availability matrix per competition/season/dataset, then materialize lossless
Bronze and typed Silver tables.

### Phase E — squads, players, and match logs

Discover unique team/player IDs, add profile/career datasets, and fetch only
match-log categories not already recoverable from season/match pages. Add
dataset-specific refresh policies.

### Phase F — controlled scale-out

Shard frontier leases by page kind and byte budget, introduce adaptive
backpressure, validate per-target coverage, and expand competition cohorts
gradually. “All competitions” becomes a registry state and coverage report,
not one unbounded DAG invocation.

## Required test corpus

- golden raw fixtures for at least: split-year league, single-year league,
  knockout cup, international tournament, women’s competition, historical
  season, current season, awarded/forfeited match, and restricted table;
- two-team/one-team/duplicate-team summary contract tests;
- data-stat schema drift fixtures;
- replay tests proving parser upgrades use zero network;
- task retry tests proving one DagRun is one tombstone observation;
- systemic layout-drift test proving no mass tombstone is committed;
- distributed-worker test using durable manifests rather than shared /tmp;
- a 400+ page clearance/session soak with proxy byte assertions.

## Non-technical gate

Before a source-wide crawl, confirm that the intended collection, retention,
redistribution, and AI/ML use comply with the current
[FBref/Sports Reference terms](https://static.fbref.com/termsofuse.html) and
any licensing requirements. Rate limiting and proxy efficiency do not replace
that review.
