# SofaScore paid-proxy lease and budget contract

`proxy_filter` exposes the compatible control API on `:8899` and a lease-only
HTTP proxy on `:8900`. A successful `POST /v1/leases` pins one upstream pool
entry for the lifetime of the lease. Browser traffic authenticates as
`lease:<token>`; provider credentials and tokens are excluded from reports,
the durable ledger and logs.
The compatibility `SharedBudgetLedger` persists only SHA-256 reservation-token
keys; its ledger and lock files are mode `0600` and atomically fsynced.

## Production and bootstrap separation

- `dag_ingest_sofascore` / source `sofascore` is authorized only when
  `configs/sofascore/proxy_budget_canary.json` is verified and valid.
- `dag_canary_sofascore_proxy` / source `sofascore_canary` is a separate,
  serial bootstrap path. It is disabled unless an operator explicitly sets
  `PROXY_FILTER_SOFASCORE_CANARY_HARD_CAP_BYTES` for the experiment.
- The bootstrap stats carry a SHA-256 experimental-policy ID with
  `production_authorized=false`. That ID cannot authorize the production DAG.
- The fixed `https://api.ipify.org` exit probe is allowed only inside the
  isolated canary lease. The proxy meters its bytes but never inspects or stores
  its response body.

Canary mode measures six immutable workload classes: tournament-scoped
25-match and 50-player batches for each enabled source tournament, plus the
canonical enabled EPL and World Cup season shapes. For example,
`match_batch_25_t17` can authorize EPL only; it cannot authorize World Cup
(`match_batch_25_t16`). Every class needs 20 complete cold runs over at least
five hashed exits. After review, those samples become a verified versioned
artifact; only then can a signed task allocation obtain a non-zero budget for
its exact source-tournament class.

The artifact also stores a deterministic runtime fingerprint over the capture,
filter, runner, DQ and endpoint-contract code, proxy blocklist, Dockerfile and
exact browser dependency pins. Both Airflow and `proxy_filter` recompute it.
Changing any metered runtime input makes the old evidence unusable; samples are
never relabelled or copied into a new fingerprint.

## Derivation

The artifact field `budget_derivation` is
`max_observed_task_bytes_per_workload_class_v2`:

- hard task bytes = maximum measured `total_provider_bytes` among eligible cold
  runs for that one workload class;
- endpoint reservation = the largest individual request observed for that
  endpoint inside the same class;
- a v2 loader requires the explicit class and never combines class maxima into
  one free-form DagRun cap;
- match and player classes are always full batches of 25 and 50 for one source
  tournament; smaller
  production batches use the same measured ceiling, never an unmeasured linear
  estimate;
- the two season classes use the exact canonical `production_season_shape`
  digests and bounded dynamic schedule/squad/referee endpoint families;
- benchmark-only no-op/replay/resume samples never affect either maximum;
- no multiplier, handwritten headroom or percentile truncation is added.

Using exact request maps keeps a one-per-task warmup request: pooling all event
requests would make it disappear from p95. The class maximum keeps every
observed complete cold task admissible. A larger future season stops at the
hard boundary and requires new evidence; it does not receive guessed headroom.

If less than a full endpoint maximum remains near the end of a task,
`SharedBudgetLedger.reserve` returns the exact positive remainder. Every socket
read is pre-reserved and bounded by that value; zero remainder is rejected
before another provider read.

Schema-v1 artifacts are migration input only. They lack task classes and are
rejected by the production loader unless an explicit offline compatibility flag
is used. Schema v2 without a selected workload class also fails closed.

## Accounting and concurrency

The exact meter counts application bytes crossing the upstream-provider socket
in both directions, including CONNECT request/response heads. Byte events are
appended to a mode-0600 JSONL ledger and fsynced; daily and DagRun totals are
restored after restart. The daily cap is shared by every paid source.

SofaScore production permits one active warmed-session lease, and bootstrap
canary traffic is globally isolated while its lease is active. Other sources
retain the configurable `PROXY_FILTER_MAX_ACTIVE_LEASES` limit and are not
forced to serialize at one lease.

`dag_ingest_sofascore` therefore sets `max_active_tasks=1`. Its signed
`season`, match-only `targets`, and post-match `players` plans share one atomic
parent envelope. This prevents parallel league fan-out from turning the
single-lease rule into retries and keeps every phase inside one logical-run
provider-byte manifest.

## Measured readiness evidence (2026-07-11)

Both official direct-discovery hosts returned HTTP 403 with a 48-byte response
from the available egress. This was reproduced without proxy environment
variables and with an explicit empty proxy setting on the local host and
GitHub-hosted Linux, macOS and Windows runners. Every run used 0 paid-proxy
bytes and 0 browser sessions, and the registry was preserved. Live discovery
therefore requires a trusted self-hosted runner with working direct SofaScore
egress; a paid-proxy or browser fallback remains forbidden.

The comparable historical PR #927 World Cup 2026 schedule-plus-standings
cohort (104 schedule and 60 standings rows) changed from 3.9648 MiB, two browser
sessions and 112 seconds to 1.9670 MiB, one session and 38 seconds. That is
50.39% fewer browser-counted bytes and 66.07% less wall time. These are
lower-bound browser counters, not the new provider-path meter, so they are
history only and cannot authorize production. The mixed 106-run traffic history
(p50 17.21 MiB, p95 23.11 MiB, maximum 45.71 MiB) is likewise ineligible
because it combines different cohorts.

Five network-disabled fixed-cohort runs over 25 matches, 50 players and 225
endpoints measured:

| mode | paid MiB / sessions / navigations / requests | wall p50 / p95 | matches/s | players/s | replay / cache / completeness |
| --- | --- | --- | ---: | ---: | --- |
| offline replay | 0 / 0 / 0 / 0 | 0.4617 / 0.5077 s | 54.15 | 108.31 | 100% / 100% / 100% |
| manifest no-op | 0 / 0 / 0 / 0 | 0.00651 / 0.00721 s | 3837.66 | 7675.32 | 0% / 100% / 100% |

Offline endpoint duration was p50 2 ms and p95 5 ms. An older diagnostic live
EPL 25-match run completed all 125 event endpoints with 2,333,024 provider bytes
(2.224945 MiB), one browser session and complete raw retention. It was
quarantined as `rejected_126nav` while the exact browser transport was audited.

Live diagnostics proved why document navigation is required: the same warmed
session gets HTTP 403 for `fetch`/APIRequestContext, while the exact JSON
document returns HTTP 200. An intermediate `9a3653b6...` sample exposed and
fixed offline replay of a retained optional 404; changing that runtime digest
automatically made its otherwise successful traffic evidence ineligible.

Earlier runtime `a3571299...` diagnostics retained all 125/125 EPL raw payloads
(event, lineups, statistics, shotmap and incidents for 25 matches), used one
browser session and 126 explicitly counted navigations, and consumed 1,256,896
provider bytes (1.199 MiB). Endpoint duration was p50 2.989 s and p95 3.106 s;
throughput was 0.07696 matches/s. Provider up/down accounting was
17,183/1,239,713 bytes.

The matching 50-player class retained 50/50 profiles; 49 season-stat payloads
were success and one was a replayable `not_supported`. It used one session,
101 navigations and 332,395 provider bytes (0.317 MiB). Endpoint duration was
p50 2.982 s and p95 3.156 s; throughput was 0.20024 players/s. No-op, offline
replay and one-endpoint resume benchmarks passed for both classes. Those
samples became ineligible when the runtime changed. The root
warm-up, exit probe and every exact JSON document were individually
rate-limited. The candidate remains `verified=false`: these are the first cold
observations, not a production budget.

The current runtime fingerprint is
`960283608cb8d5b6602e7d82236c862ade96f30591dd8497704bc32d388498f1`.
Its first accepted EPL season-shape sample captured 38/38 planned payloads in
one browser session and 39 navigations, using 463,018 provider bytes
(0.442 MiB). It recorded 13 schedule pages, standings, rounds and participants
as success; the empty future schedule as `legitimate_empty`; and cup tree plus
20 season-scoped squad routes as `not_supported`. Endpoint completeness was
100%, p50 duration 260 ms and p95 3.145 s. No-op, offline replay and one-endpoint
resume were also saved. One sample is far below the policy requirement of 20
cold samples and five distinct exits, so the shipped artifact remains
`verified=false`.

The World Cup cohorts now contain 25 production-Bronze finished event IDs and
50 source-evidenced players selected from their frozen lineups. Earlier
World Cup match/player diagnostics completed, but are ineligible for the
current fingerprint. Activation stays fail-closed; no old cap is reused.

The production migration preflight is green and a second apply was a no-op.
Current production schedule skeleton rows and source-season mismatches are
zero. Player profile coverage still fails the 95% gate (EPL 2025/26 is 496/677,
73.26%), so historical profile/EPL backfills remain blocked until the
provider-metered policy is verified and the shared capture engine can run them
safely.
