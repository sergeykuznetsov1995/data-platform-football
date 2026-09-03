# SofaScore tournament registry

`tournaments.json` is the single versioned source of truth for SofaScore
tournament IDs, slugs, source season IDs, source classification evidence,
operator review, and activation.

Schema v2 separates fields by ownership:

- discovery owns source identity, slugs, category, `classification`, and season
  source metadata (`season_id`, original name/year, dates, format, canonical
  season, evidence, and optional evidenced `team_count`);
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

When the public JSON edge returns 403, an operator may explicitly choose the
metered browser transport. It has no direct fallback, needs a positive cap, and
records provider bytes plus browser sessions/navigations. Team counts are also
opt-in and fail closed on an empty/malformed team list:

```bash
python dags/scripts/run_sofascore_discovery.py \
  --scope targeted --tournament-id 17 \
  --transport lease-browser --budget-cap-bytes 8000000 \
  --include-team-counts --dry-run
```

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

## Paid-proxy budget policy

The budget classes ship in git as `configs/sofascore/workload_policy.json`
(schema v4, #1245). There is no paid canary any more: the policy declares one
`hard_task_bytes` ceiling per measured *shape* (match batch of 25, player batch
of 50, one season per `season_format`/`team_count_band` pair) and nothing in it
depends on the runtime fingerprint of the tree, so editing scraper code no
longer invalidates the paid path. The spend stays bounded by three static
gateway ceilings — `--daily-budget-mb`, `--max-lease-mb`, `--dagrun-budget-bytes`
— and by the signed allocation of every task.

`deploy/sofascore/deploy.sh` copies the policy of the release tree to
`<runtime>/artifacts/<release-tag>/workload_policy.json` and pins its SHA-256
into `SOFASCORE_PROXY_BUDGET_ARTIFACT_ID`. Both the scheduler and the three
gateway lanes receive that immutable host file at
`/opt/airflow/runtime/sofascore/proxy_budget_canary.json` (the in-container path
kept its historical name so the WhoScored admission contract stays untouched);
the in-container `SOFASCORE_PROXY_BUDGET_ARTIFACT` contract always points to
that exact path. Do not place the host file below `configs/sofascore`, because
that directory has its own release-scoped bind and would hide or replace the
artifact during a cutover. The all-zero artifact ID in `.env.example` exists
only so CI can render Compose and must be replaced before deployment: readiness
derives the ID from the mounted file and requires exact agreement with this
independent pin.

Before creating either container, run the UID-aware host preflight against the
exact deployment paths:

```bash
python scripts/sofascore_runtime_preflight.py preflight \
  --release-root /root/dpf-release-immutable \
  --artifact /durable/path/sofascore/workload_policy.json \
  --state-dir /durable/path/sofascore/gateway-state \
  --campaign-dir /durable/path/sofascore/all-men \
  --campaign-policy /root/dpf-release-immutable/configs/sofascore/all_mens_campaign.json \
  --expected-artifact-id <64-hex-policy-sha256>
```

`--release-root` is mandatory and must name the canonical, root-owned protected
checkout. Both durable sources must be canonical and outside that root. Install
the artifact as `root:0` mode `0640` below a root-owned parent chain that is not
group/world writable. It must be a regular non-symlink file readable by UID
50000/GID 0. The state directory must be protected, durable, and writable and
traversable by that identity; its parent chain follows the same protected-host
rule. The preflight holds the artifact through an `O_NOFOLLOW` file descriptor,
rejects inode/size/mtime changes, and loads the policy through that stable
descriptor. It never prints proxy credentials or response bodies.

Gateway container health is stricter than host preflight. It runs a real
write/unlink probe in its narrow state mount, reloads the policy, and requires
`/health` to report `sofascore_paid_enabled=true`, a positive budget, and the
exact pinned artifact ID. Internal health HTTP explicitly bypasses all proxy
environment variables. A merely live HTTP endpoint is therefore not sufficient
to make the gateway healthy.

The shared scheduler healthcheck intentionally remains SchedulerJob-only so an
independently managed source gateway cannot make all Airflow workloads appear
unhealthy. During cutover, run the stronger scheduler check explicitly inside
the admitted scheduler container (it also checks the live gateway and pinned
artifact before SchedulerJob):

```bash
docker compose exec -T airflow-scheduler \
  python /opt/airflow/scripts/sofascore_runtime_preflight.py scheduler-health \
  --artifact /opt/airflow/runtime/sofascore/proxy_budget_canary.json \
  --health-url http://sofascore_proxy_filter:8899/health
```

Adding a class means editing `workload_policy.json` and shipping it: a class
name must equal `workload_class_name(scope, shape_digest)` of its own declared
shape, `required_endpoints` must equal the endpoints of that shape, and a
measurement artifact (with `samples`, `verified` or `runtime_fingerprint`) is
rejected outright so an old canary file can never be loaded as a policy.


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
