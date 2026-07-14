# SofaScore tournament registry

`tournaments.json` is the single versioned source of truth for SofaScore
tournament IDs, slugs, source season IDs, source classification evidence,
operator review, and activation.

Schema v2 separates fields by ownership:

- discovery owns source identity, slugs, category, `classification`, and season
  source metadata (`season_id`, original name/year, dates, format, canonical
  season, and evidence);
- operators own `canonical_id`, `enabled`, `review`, custom fields, explicit
  season aliases, and named-season canonical overrides. Discovery preserves
  these fields byte-for-byte. Euro 2020's
  `2021` alias is an explicit exception, not a generic single-year heuristic.

New tournaments are always `enabled: false` with a pending review. Production
capture is fail-closed: source gender must explicitly be male, source evidence
must contain no women/mixed/youth/reserve/futsal marker, and review must confirm
adult men's first-team football with evidence. A plain name without `Women` or
`U21` is not positive evidence. Schema-v1 files remain readable for rollback,
but cannot be production-capture eligible until migrated and reviewed.

Refresh every discoverable tournament and all source season records with:

```bash
make sofascore-discovery
```

The scheduled GitHub workflow performs an `active-reviewed` direct refresh on
Monday through Saturday and a complete category scan on Sunday. It opens or
updates a review PR when metadata changes. Both scopes use the public JSON API
only. The trusted discovery job requires a Linux self-hosted runner labelled
`sofascore-direct`; pull requests stay on GitHub-hosted runners and execute
only the network-free poison-proxy contracts. Hosted Ubuntu, macOS and Windows
egress currently receives the same source-side HTTP 403 and must not be used as
a proxy/browser fallback. `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, lower-case variants, and the
repository proxy file are disabled at the libcurl transport layer; the report
always records zero paid-proxy bytes, browser sessions, and navigations. A 403,
missing category fan-out, missing season response, or schema error aborts before
the atomic compare-and-swap write.

For a read-only drift check (exit 2 means the registry would change):

```bash
make sofascore-discovery-check
```

Review and activation are separate atomic operations. Approval deliberately
leaves the row disabled. The CLI grammar is command-first:

```bash
python dags/scripts/manage_sofascore_registry.py approve \
  --tournament-id 7 \
  --canonical-id "UEFA-Champions League" \
  --reviewed-by operator@example.com \
  --evidence https://www.uefa.com/uefachampionsleague/

python dags/scripts/manage_sofascore_registry.py enable --tournament-ids 7
```

`enable` re-evaluates the full source-plus-review evidence and refuses unknown,
women, mixed, youth, reserve, futsal, or seasonless records. Airflow mounts this
registry read-only; discovery and operator commands are the only writers.

## Onboarding a wave of tournaments

Machine evidence first: a targeted discovery pass must have written
`classification.gender = male` and source seasons for every candidate. Without
it, approval is impossible — the classifier is fail-closed and no CLI flag can
override it.

```bash
python dags/scripts/manage_sofascore_registry.py prepare-review \
  --tournament-ids 8,23,34,35 --output /tmp/sofascore-review.json
```

`prepare-review` only reads: it snapshots each row's source classification and
canonical seasons, drafts the cross-source evidence stub that still needs a
human reference, and lists `blocked` reasons when `canonical_id` is absent, is
unknown to `configs/medallion/competitions.yaml`, or has no canonical season in
common with it. The evidence stub is flagged `"todo": true`; `approve-batch`
actively rejects any approval whose evidence still carries that flag
(`evidence TODO must be replaced with an out-of-source reference before
approval`), so filling in only `reviewed_by` cannot activate a competition. The
operator fills `reviewed_by`, replaces every evidence `TODO` with a real
out-of-source reference, then applies the whole wave:

```bash
python dags/scripts/manage_sofascore_registry.py approve-batch \
  --input /tmp/sofascore-review.json

python dags/scripts/manage_sofascore_registry.py enable --tournament-ids 8,23,34,35
```

`approve-batch` (and its `reject-batch` twin, which reads a `rejections` list)
is all-or-nothing: every row is applied in memory and a single ineligible
tournament aborts the wave before the compare-and-swap write, so the registry
file never holds a half-applied wave. A concurrent discovery or operator write
between read and write aborts with a concurrent-update error; rerun. `enable`
takes the whole wave in one write and re-checks eligibility per row — enable a
wave only after its capture budget classes are verified.

## Production table bootstrap

Before the first raw-first deployment, render and inspect the idempotent Iceberg
DDL, run a read-only preflight, then apply it from the Airflow runtime:

```bash
python scripts/migrate_sofascore_production.py --dry-run
python scripts/migrate_sofascore_production.py --preflight
python scripts/migrate_sofascore_production.py --apply
```

The bootstrap creates the long `ops.sofascore_capture_manifest` contract and
empty normalized Bronze table contracts needed for query planning. Before the
new MERGE writers run, it also evolves legacy `sofascore_match_stats` with a
deterministic `statistic_key`, normalizes standings `group` to `__total__`, and
atomically deduplicates both natural keys when needed. Preflight requires every
natural-key component to be non-null and duplicate-free. It never inserts
source rows or synthetic success states. Therefore bootstrap removes rollout
schema failures but committed-state DQ still fails until real raw-backed
endpoint captures are complete.

## Paid-proxy canary

`dag_canary_sofascore_proxy` is paused and manual-only. Trigger it with an
explicit positive `experimental_cap_bytes` matching proxy-filter's isolated
`sofascore_canary` cap. It resumes each v2 workload class toward at least 20
accepted cold runs: a full 25-match batch and 50-player batch for each enabled
source tournament, plus the enabled EPL and World Cup season shapes. Every cold class run
gets a fresh raw store, manifest, browser and lease. If a class has fewer than
five distinct anonymized exits after 20 runs, raise `target_cold_runs` explicitly
and collect more evidence.

The DAG atomically appends only to a private candidate under
`/opt/airflow/logs/sofascore-canary/runs/<dag-run-sha256>/`. Retries resume that
same candidate; another DagRun cannot see it. An `all_done` DQ task requires a
successful producer plus a current-run manifest whose artifact and runtime
fingerprint hashes still match. The checked-in production artifact is mounted
read-only, and the DAG has no verification or promotion task. To run the same
collector locally and then perform the separate review gate:

```bash
python scripts/research/bench_sofascore_paid_canary.py bootstrap \
  --experimental-cap-bytes <configured-cap>
python scripts/research/bench_sofascore_paid_canary.py collect \
  --experimental-cap-bytes <configured-cap> --target-cold-runs 20
python scripts/research/bench_sofascore_paid_canary.py verify
```

Match and player classes are source-tournament scoped (`..._t16`, `..._t17`).
The World Cup match fixture has 25 source-evidenced finished events. Its player
fixture stays empty with an explicit blocker until 50 players can be derived
from World Cup squads or match payloads. An EPL class cannot authorize World
Cup traffic. Collection reports that blocked class and continues gathering
independent EPL, World Cup match and season evidence; verification still fails
closed until the blocker is removed by source evidence.

`verify` changes only `verified` and does so atomically after validating every
class's exact request map, raw-once evidence, class-specific full-batch/season
shape, all benchmark modes, at least 20 cold runs and five exit hashes per
class. `no_op` and offline replay must each show zero allocation, zero lease and
zero network requests. Artifacts contain no raw exit, token, proxy URL or
response payload.

Production uses three immutable signed snapshots under one Airflow run:
bounded season expansion (`<run_id>::season`), match-only batches after its raw
commit (`<run_id>::targets`), and player-only batches (`<run_id>::players`).
The player plan is created only after every match task and the weekly/manual
gate; it rereads squads plus current Bronze lineups, incidents, event-player
stats and ratings, so newly seen players cannot be silently dropped. The
filtering proxy persists one parent envelope/manifest for the base run. Its cap
is exactly the sum of unique signed allocations in all registered phases;
retries reuse the same balance, and a different plan for any phase is rejected.
A raw/manifest no-op creates no allocation and never opens a lease. The DAG is
serialized (`max_active_tasks=1`) because production permits one SofaScore
lease at a time.
