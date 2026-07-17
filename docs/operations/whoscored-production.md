# WhoScored production operations

This document is the deploy and acceptance contract for the WhoScored source.
It covers source ingestion, the current SeaweedFS S3-compatible store,
Airflow orchestration, historical replay, proxy policy, DQ and recovery.

## Invariants

- Catalog discovery includes every source tournament classified as senior men.
  Women, youth, reserve and academy competitions are excluded with persisted
  classification evidence. Each discovery also stores a content-addressed
  technical-exclusion audit bound to the exact `allRegions` SHA-256. It lists
  normalized-name-within-region, canonical-link and cross-tournament stage-ID
  candidates plus the versioned source-ID override disposition. Any unresolved
  candidate blocks catalog publication; an empty candidate list is itself
  immutable, reproducible evidence rather than an undocumented assertion.
- Scheduled daily and historical Airflow runs are structurally `direct_only`.
  Only the paused manual `dag_canary_whoscored_proxy` may select
  `direct_then_paid`, and only with an exact signed canary approval pinned by ID
  and SHA-256. A boolean, environment scalar or the removed
  `--allow-paid-proxy` flag cannot enable paid traffic. Every canary request
  remains raw-cache first and direct first; the paid route is available only
  after authoritative Cloudflare evidence and a fresh direct recheck.
- The canary's two code-owned invoice-boundary and application-gateway
  sentinels are enabled only for its exact decimal `1_000_000_000`-byte
  contract. Execution additionally requires the admitted `ready-v1` release,
  fresh active order-38950 quota receipt, exact signed approval, active
  gateway-authenticated campaign and gateway-owned alert proof. The filtering
  proxy independently caps the lifetime of the initialized provider-order
  state at `850 MiB` across every campaign, UTC rollover and restart. The
  independent `WHOSCORED_FULL_PAID_CRAWL_AVAILABLE=False` sentinel keeps normal
  paid ingest and backfill unavailable regardless of canary evidence.
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
| Proxy use | Manual/default behavior could select the paid endpoint implicitly | Direct-only by default; only the small signed measurement canary is code-enabled through the quota receipt, authenticated gateway and durable 850 MiB safety cap; every full-crawl paid lease remains code-disabled |
| Source block handling | Independent workers could retry the same block and browser batches could start together | One fixed process-global 546 ms actual-start governor plus a persistent 15/30/60-minute shared cooldown and one half-open probe; expected origin masks and ordinary 502/timeouts never authorize paid traffic |
| DQ | Primarily task success and partial current-run checks | Snapshot-pinned frozen scope/match/profile identities, exact stage/feed proof, owner-level parity across 25 datasets, NULL-identity duplicate sentinels, parser/availability proof, profile terminal proof and zero-paid gates; the complete historical read is 33 cardinality-invariant queries |
| Iceberg failure handling | Metadata corruption repair could drop/recreate a table; maintenance could return green with partial failures and had no safe live-file compaction | Corruption and partial maintenance fail closed; delete-safe exact-path compaction is bounded to 64 files/512 MiB per table and 4 tables/2 GiB per task; effective retention is 30d weekly, 14d daily for WhoScored and 3d for other high-churn feeds |

## Deployment prerequisites

> **Production NO-GO:** the production Airflow image intentionally contains a
> root-owned `blocked-v1` WhoScored build-provenance attestation.  The immutable
> launcher exits 78 before WhoScored application imports, storage writes,
> source/proxy connections or approval signing.  Absence of the attestation is
> also a failure; there is no environment, Airflow Variable, DagRun or CLI
> override.  Do not start a WhoScored production service until a reviewed image
> rebuild replaces it with `ready-v1` and the exact SHA-256 of a root-owned
> content-addressed provenance manifest.  That manifest must bind the dated
> signed APT snapshot and exact packages, every downloaded artifact, fully
> hashed Python locks for both interpreters, GitHub Action commit SHAs, all
> third-party Compose image digests, and the immutable payload-stage image
> identities before the gate layer is added.  Because an image cannot contain
> its own final digest without a circular claim, promotion additionally
> requires a deployment-owned attestation generated after the final build and
> verified against the exact `docker inspect .Image` digest before service
> creation.  Neither a payload-stage ID nor a local test target is final-image
> promotion evidence.

The blocked attestation is enforced only by the production-class
`airflow-scheduler` and dedicated `whoscored_proxy_filter` targets; both stop
with exit 78 before their command or any mounted application import.  The
generic Airflow base used by init/webserver and the shared non-WhoScored proxy
remains operable for unrelated platform workloads and is not a WhoScored
execution target.  Every WhoScored source, raw/Iceberg/ops persistence,
migration, cleanup, backup, campaign and `whoscored-only` proxy entrypoint also
requires the image-private `production-v1` verifier before its first I/O, so a
generic service cannot become a WhoScored runner merely by naming a mounted
script. Routine Compose `run`/`exec` against either production-class service is
forbidden by the reviewed wrapper, including entrypoint overrides.

All public system-Python aliases and installed shebangs in a production target
pass through the immutable gate, including `python -S`. The interpreter at
`/usr/local/libexec/whoscored-python-real` is an internal implementation detail
needed to evaluate the gate itself and is deliberately absent from commands,
environment values and application shebangs. Direct access to that path (or to
the raw legacy-venv interpreter) is part of the trusted Docker-host authority
boundary, not an application security boundary. Production is GO only on a
dedicated single-tenant host where every Docker-capable process and principal
is trusted, or behind an independently persistent firewall/loopback boundary
that untrusted principals cannot reconfigure. Do not claim that Compose or a
container wrapper can constrain a host principal with Docker authority.

FlareSolverr is part of that same production closure.  Compose builds it from
the digest-pinned 3.4.6 base into a dedicated derived image; the WhoScored
extension, launcher, digest receipt and pristine ChromeDriver are root-owned,
the application directory and container root are read-only, and only bounded
browser state lives on tmpfs.  There is no source bind mount.  The client checks
the side-effect-free runtime identity before every session/browser POST and
checks the response again.  A `ready-v1` promotion must bind the externally
observed final digest of this derived image as well as the Airflow scheduler and
dedicated WhoScored proxy images; a local mutable tag is not sufficient.
The repository-root and Dockerfile-specific ignore files are identical strict
allowlists: only those two control files, the derived Dockerfile, its entrypoint
and `scripts/flaresolverr_extended.py` enter the build context. Never widen an
allowlist to send `.env`, logs, raw data, Git metadata or unrelated untracked
worktree content to a local or remote builder.

Set the rollout paths once before running any command in this section:

```bash
export COMPOSE_ENV_FILE=/root/data-platform-football/.env
export WHOSCORED_ENV_FILE=/root/.secrets/whoscored-runtime-v2.env
export PROXY_POOL_ENV_FILE=/root/.secrets/whoscored-proxy-v2.env
export RELEASE=/absolute/path/to/the-reviewed-release

# Clear host loader controls before starting even `/usr/bin/env`; a shell that
# was itself launched under hostile injected code is already inside the trusted
# Docker-host boundary. Every host-side Python admission command uses this
# isolated, environment-empty interpreter prefix.
for variable in ${!LD_@} ${!DYLD_@}; do
  unset "$variable"
done
unset GCONV_PATH GLIBC_TUNABLES LOCPATH MALLOC_TRACE
ADMISSION_PYTHON=(
  /usr/bin/env -i
  HOME=/nonexistent PATH=/usr/bin:/bin
  LANG=C.UTF-8 LC_ALL=C.UTF-8
  /usr/bin/python3 -I -S
)
```

1. Keep `dag_ingest_whoscored`, `dag_backfill_whoscored`,
   `dag_canary_whoscored_proxy`, and `dag_backup_whoscored_storage` paused
   while code, approval and schema gates run.
2. Rotate `PROXY_FILTER_CONTROL_TOKEN`, the dedicated
   `WHOSCORED_PROXY_FILTER_CONTROL_TOKEN`, and the legacy
   `SOFASCORE_PROXY_CONTROL_TOKEN` if either value has appeared in logs or a
   terminal transcript.
3. Generate the environment secret from the approved legacy pool with
   `scripts/migrate_proxy_pool_secret.py`, verify count and canonical SHA-256
   without printing entries, set `PROXY_FILTER_ALLOW_FILE_FALLBACK=false`, and
   pass all three environment files to every rollout Compose command. `proxy_filter` must
   have no `proxys.txt` mount; the Airflow mount is retained for other sources.
   That shared legacy credential is not valid for a WhoScored canary. Use only
   the dedicated credential for PROXYS.IO order `38950`, plan `Bronze`, with
   exact active quota and remaining values of `1.00` decimal GB. Expose it only
   as `WHOSCORED_PROXY_POOL_JSON` to the opt-in `whoscored_proxy_filter`
   service. Admission requires a fresh canonical credential-free receipt bound
   to a protected screenshot before every rollout. Its provider hop must use
   verified HTTPS/mTLS or an
   authenticated private tunnel with an IP-bound one-campaign credential;
   plaintext Basic authentication over the current raw TCP opener is not an
   authorized boundary. Do not place the dedicated credential in common Airflow
   mounts or environment. The generic service runs in
   `shared-no-whoscored` mode; the dedicated service runs in
   `whoscored-only` mode. Production also leaves the port-8899 credential-less
   data plane disabled; never add `--allow-legacy-noauth` to its command.
4. Render the production Compose model before changing services:

   ```bash
   "$RELEASE/scripts/compose.sh" -p data-platform \
     --env-file "$COMPOSE_ENV_FILE" --env-file "$WHOSCORED_ENV_FILE" \
     --env-file "$PROXY_POOL_ENV_FILE" \
     config --quiet
   ```

5. Leave `WHOSCORED_PROXY_APPROVAL_PATH` empty for ordinary direct-only
   deployment. The two enabled code-owned sentinels cover only the exact
   measurement-canary contract; they do not authorize a rollout. Only after
   admission accepts the exact schema-v1 `ready-v1` deployment attestation, the
   fresh order-38950 provider receipt and every protected service may one
   reviewed canary set it to the exact in-container path
   `/opt/airflow/secure/whoscored-approvals/<approval_id>.json`. Install that
   file under `WHOSCORED_PROXY_APPROVAL_HOST_DIR` as UID `50000`, mode `0600`;
   Compose mounts the directory read-only into `airflow-scheduler` alone. Never
   use a mutable `current` symlink or expose the signing workspace to Airflow.
   The scheduler performs structural approval checks and receives no approval
   HMAC, campaign-ledger HMAC, filter control token or paid-alert authority.
   `whoscored_proxy_filter` alone mounts
   `WHOSCORED_PROXY_FILTER_STATE_HOST_DIR` read-write at
   `/opt/airflow/state/whoscored-proxy-filter`; the scheduler mounts that exact
   host source read-only for provider-event reconciliation. Campaign mutation
   and snapshots go through the authenticated L7 gateway rather than direct
   scheduler ledger access.

   Enabled canary sentinels do not relax loaded-code provenance. Paid
   enablement still requires the digest-attested image payloads, the exact
   protected release bind sources admitted read-only, validation before any
   application import, forced process recreation and the in-memory paid
   FlareSolverr extension-hash attestation. Hashing a current on-disk tree after
   older modules were loaded is not sufficient provenance. The independent
   `WHOSCORED_FULL_PAID_CRAWL_AVAILABLE=False` sentinel keeps normal paid ingest
   and backfill unavailable.

The data volume and single `weed mini` storage core remain in scope, but an old
pre-isolation container identity cannot be left running with this release. The
wrapper deliberately blocks every writer-start command until a one-time,
quiesced isolation rollout replaces that container around the same external
volume and creates the private S3 gateway plus digest-pinned HTTP proxy. This is
not the four-plane cutover: no data migration or topology-state file is created.
Off-host backup/WORM and the four-plane cutover remain outside the approved
scope, so loss of this one host can still lose raw and Iceberg data;
`dag_backup_whoscored_storage` remains paused.

If `scripts/compose.sh` reports an old `/entrypoint.sh` mini identity, obtain a
full-platform downtime approval and run this exact one-time rollout before the
targeted WhoScored deployment. Abort if the volume is not `seaweedfs_data`, any
supervised plane exists without protected state, or any unreviewed container
uses the volume. Provision the mode-0600 S3 identity config and pull both pinned
SeaweedFS/Caddy images first. Stop external S3 clients, then:

```bash
set -Eeuo pipefail
COMPOSE=(
  "$RELEASE/scripts/compose.sh" -p data-platform
  --env-file "$COMPOSE_ENV_FILE" --env-file "$WHOSCORED_ENV_FILE"
  --env-file "$PROXY_POOL_ENV_FILE"
)
PROFILED_SERVICES=(
  tor superset-worker superset-beat opensearch
  openmetadata-server openmetadata-ingestion
)
PROFILED_WAS_RUNNING=()
for service in "${PROFILED_SERVICES[@]}"; do
  if [[ "$(docker inspect --format '{{.State.Running}}' "$service" 2>/dev/null || true)" == true ]]; then
    test "$(docker inspect --format \
      '{{index .Config.Labels "com.docker.compose.project"}}' "$service")" = data-platform
    PROFILED_WAS_RUNNING+=("$service")
  fi
done
# Migration/init services must not be running during this maintenance window.
for service in airflow-init lakekeeper-migrate openmetadata-migrate; do
  test "$(docker inspect --format '{{.State.Running}}' "$service" 2>/dev/null || true)" != true
done
# The paid profile is outside this rollout. A stopped container is still an
# inactive-profile orphan, so require the service to be completely absent.
if docker inspect whoscored_proxy_filter >/dev/null 2>&1; then
  echo "whoscored_proxy_filter must be absent before storage isolation" >&2
  exit 2
fi
SEAWEEDFS_VOLUME_IDENTITY_SHA256="$(
  docker volume inspect seaweedfs_data | sha256sum | awk '{print $1}'
)"
test -n "$SEAWEEDFS_VOLUME_IDENTITY_SHA256"
"${COMPOSE[@]}" --profile heavy down --remove-orphans
WRITERS_RE='^(airflow-init|airflow-scheduler|airflow-webserver|lakekeeper-migrate|lakekeeper|trino|superset|superset-worker|superset-beat|openmetadata-migrate|openmetadata-server|openmetadata-ingestion|jupyterhub)$'
test -z "$(docker ps --format '{{.Names}}' | grep -E "$WRITERS_RE" || true)"
test -z "$(docker ps --filter label=com.docker.compose.oneoff=True \
  --format '{{.Label "com.docker.compose.service"}}' | \
  grep -E "$WRITERS_RE|^seaweedfs($|-)' || true)"
test -z "$(docker ps --all --filter volume=seaweedfs_data --format '{{.ID}}')"
test "$(docker volume inspect seaweedfs_data | sha256sum | awk '{print $1}')" = \
  "$SEAWEEDFS_VOLUME_IDENTITY_SHA256"
"${COMPOSE[@]}" up -d --no-recreate
for service in "${PROFILED_WAS_RUNNING[@]}"; do
  "${COMPOSE[@]}" up -d --no-deps "$service"
done
# A no-op start performs a second live runtime/network/mount/port attestation.
"${COMPOSE[@]}" start trino
for service in "${PROFILED_WAS_RUNNING[@]}"; do
  test "$(docker inspect --format '{{.State.Running}}' "$service")" = true
done
docker network inspect dp-seaweedfs-control --format '{{.Internal}}' | grep -qx true
docker exec trino trino --execute 'SHOW SCHEMAS FROM iceberg'
```

Do not roll back the new storage Compose/wrapper/Make/lifecycle files after this
step. Application images may be rolled back while retaining that boundary; an
old checkout's `make clean`/`down -v` can delete the live volume.

### Targeted runtime deployment

There are two deliberately different procedures. The checked-in repository can
perform only blocked acceptance; it must not create or restart a production
service. A future promotion procedure becomes valid only after the external
evidence listed below exists.

#### Current blocked-v1 acceptance

Use `airflow-scheduler-test` for dependency, DagBag and legacy-venv checks. Then
build the two final targets only to prove their immutable gate exits 78 before
the requested command. Do not tag a payload stage as a production service, and
do not run `compose up`, `create`, `start` or `restart` for either final image.

```bash
"${ADMISSION_PYTHON[@]}" \
  "$RELEASE/scripts/validate_whoscored_build_provenance.py" \
  --root "$RELEASE" --expect-blocked \
  > /tmp/whoscored-build-provenance-blocked.json

docker build --target airflow-scheduler-test \
  -t data-platform-airflow-scheduler:test-only \
  "$RELEASE/docker/images/airflow"
docker run --rm --entrypoint /bin/bash \
  data-platform-airflow-scheduler:test-only -euc '
    test "$(airflow version)" = 2.11.2
    /usr/local/bin/python -m pip check
    /opt/legacy-scraper-venv/bin/python -m pip check
  '
docker build --target airflow-scheduler \
  -t data-platform-airflow-scheduler:blocked-v1 \
  "$RELEASE/docker/images/airflow"
docker build --target airflow-whoscored-proxy \
  -t data-platform-airflow-whoscored-proxy:blocked-v1 \
  "$RELEASE/docker/images/airflow"

set +e
docker run --rm data-platform-airflow-scheduler:blocked-v1 true
SCHEDULER_STATUS=$?
docker run --rm data-platform-airflow-whoscored-proxy:blocked-v1 true
PROXY_STATUS=$?
set -e
test "$SCHEDULER_STATUS" = 78
test "$PROXY_STATUS" = 78
```

CI additionally proves `python`, `python3`, `python3.11`, `python -S` through a
bash entrypoint, and a real WhoScored runner through the internal raw
interpreter all fail before a sentinel write. Those checks are acceptance
evidence, not permission to create a production container.

#### Future ready-v1 promotion

This section is dormant until a clean, reviewed promotion revision supplies
all of the following: canonical `ready-v1` manifest and attestation; immutable
IDs for `airflow-scheduler-payload` and `airflow-whoscored-proxy-payload`; an
external deployment attestation binding each payload ID to a final
`repository@sha256:<64 hex>` digest; the final derived FlareSolverr digest; and
the exact merged base-plus-supervised Compose model. The validator must reject
any image/build/target override in that overlay. Provider quota/origin-gateway
evidence is additionally required before the paid proxy service itself may be
created.

Do not edit either provenance JSON by hand. First resolve every source-closure
issue, refresh the generated runtime evidence below, leave the checked-in
`blocked-v1` pair in place, and create one clean, reviewed payload commit.
Build the six payload images from that exact commit
(`airflow-base`, `airflow-scheduler-payload`,
`airflow-whoscored-proxy-payload`, FlareSolverr, JupyterHub and Superset), then
capture each exact `docker image inspect --format '{{.Id}}'` value. The four
Airflow base services share one ID and the three Superset services share one
ID, but every Compose service must be named explicitly:

The runtime lock and its three image trust roots are generated files too; do
not edit them by hand. Before the payload commit, refresh and then verify all
four files with the isolated deterministic generator. It publishes the
production trust root last, so an interrupted refresh remains blocked:

```bash
"${ADMISSION_PYTHON[@]}" \
  "$RELEASE/scripts/generate_whoscored_runtime_evidence.py" \
  --root "$RELEASE" --write
"${ADMISSION_PYTHON[@]}" \
  "$RELEASE/scripts/generate_whoscored_runtime_evidence.py" \
  --root "$RELEASE" --check
git -C "$RELEASE" diff -- \
  scrapers/whoscored/runtime_contract.lock \
  docker/images/airflow/whoscored-runtime-trust-root-generic \
  docker/images/airflow/whoscored-runtime-trust-root-test \
  docker/images/airflow/whoscored-runtime-trust-root-production
```

Use one protected Buildx state and evidence directory outside the checkout.
The metadata files are part of promotion evidence: keep them root-owned mode
`0600`, and never regenerate one after recording its payload image ID. These
commands intentionally invoke the reviewed Buildx plugin and Docker CLI by
absolute path under empty environments:

```bash
test "$(id -u)" = 0
cd "$RELEASE"
test "$(pwd -P)" = "$RELEASE"
BUILDX=/root/.docker/cli-plugins/docker-buildx
BUILD_HOME=/absolute/protected/whoscored-build-home
BUILDX_STATE=/absolute/protected/whoscored-buildx-state
DOCKER_AUTH=/absolute/protected/whoscored-docker-config
BUILD_EVIDENCE=/absolute/protected/whoscored-build-evidence
REGISTRY=registry.example.invalid/data-platform
test "$REGISTRY" != registry.example.invalid/data-platform
test -x "$BUILDX"
test "$(/usr/bin/stat -c '%u:%h' "$BUILDX")" = 0:1
test $((8#$(/usr/bin/stat -c %a "$BUILDX") & 8#022)) = 0
/usr/bin/install -d -o root -g root -m 0700 \
  "$BUILD_HOME" "$BUILDX_STATE" "$DOCKER_AUTH" "$BUILD_EVIDENCE"
umask 077

CLEAN_GIT=(
  /usr/bin/env -i HOME=/nonexistent PATH=/usr/bin:/bin
  LANG=C.UTF-8 LC_ALL=C.UTF-8
  GIT_ATTR_NOSYSTEM=1 GIT_EXTERNAL_DIFF=/bin/false
  GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
  GIT_NO_REPLACE_OBJECTS=1 GIT_OPTIONAL_LOCKS=0
  GIT_PAGER=/bin/false GIT_TERMINAL_PROMPT=0
  /usr/bin/git --no-pager --no-optional-locks
  -c core.attributesFile=/dev/null -c core.excludesFile=/dev/null
  -c core.fsmonitor=false -c core.hooksPath=/dev/null
  -c core.ignoreStat=false -c core.trustctime=true
  -c core.worktree="$RELEASE" -c credential.helper=
  -c diff.external=/bin/false -c filter.lfs.clean=
  -c filter.lfs.process= -c filter.lfs.required=false
  -c filter.lfs.smudge= -c submodule.recurse=false
  -C "$RELEASE"
)
BUILDX_CMD=(
  /usr/bin/env -i HOME="$BUILD_HOME" PATH=/usr/bin:/bin
  LANG=C.UTF-8 LC_ALL=C.UTF-8
  DOCKER_CONFIG="$DOCKER_AUTH" DOCKER_HOST=unix:///run/docker.sock
  BUILDX_CONFIG="$BUILDX_STATE" BUILDX_GIT_CHECK_DIRTY=1
  BUILDX_METADATA_PROVENANCE=max
  GIT_ATTR_NOSYSTEM=1 GIT_EXTERNAL_DIFF=/bin/false
  GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
  GIT_NO_REPLACE_OBJECTS=1 GIT_OPTIONAL_LOCKS=0
  GIT_CONFIG_COUNT=14
  GIT_CONFIG_KEY_0=core.fsmonitor GIT_CONFIG_VALUE_0=false
  GIT_CONFIG_KEY_1=core.hooksPath GIT_CONFIG_VALUE_1=/dev/null
  GIT_CONFIG_KEY_2=core.attributesFile GIT_CONFIG_VALUE_2=/dev/null
  GIT_CONFIG_KEY_3=core.excludesFile GIT_CONFIG_VALUE_3=/dev/null
  GIT_CONFIG_KEY_4=core.ignoreStat GIT_CONFIG_VALUE_4=false
  GIT_CONFIG_KEY_5=core.trustctime GIT_CONFIG_VALUE_5=true
  GIT_CONFIG_KEY_6=filter.lfs.process GIT_CONFIG_VALUE_6=
  GIT_CONFIG_KEY_7=filter.lfs.clean GIT_CONFIG_VALUE_7=
  GIT_CONFIG_KEY_8=filter.lfs.smudge GIT_CONFIG_VALUE_8=
  GIT_CONFIG_KEY_9=filter.lfs.required GIT_CONFIG_VALUE_9=false
  GIT_CONFIG_KEY_10=diff.external GIT_CONFIG_VALUE_10=/bin/false
  GIT_CONFIG_KEY_11=credential.helper GIT_CONFIG_VALUE_11=
  GIT_CONFIG_KEY_12=submodule.recurse GIT_CONFIG_VALUE_12=false
  GIT_CONFIG_KEY_13=core.worktree GIT_CONFIG_VALUE_13="$RELEASE"
  "$BUILDX"
)
DOCKER_CMD=(
  /usr/bin/env -i HOME="$BUILD_HOME" PATH=/usr/bin:/bin
  LANG=C.UTF-8 LC_ALL=C.UTF-8
  DOCKER_CONFIG="$DOCKER_AUTH" DOCKER_HOST=unix:///run/docker.sock
  /usr/bin/docker
)

test -z "$(/usr/bin/find "$RELEASE" -path "$RELEASE/.git" -prune -o \
  -name .gitattributes -print -quit)"
test ! -e "$("${CLEAN_GIT[@]}" rev-parse --git-path info/attributes)"
test -z "$("${CLEAN_GIT[@]}" status --porcelain=v1 --untracked-files=all)"
PAYLOAD_REVISION="$("${CLEAN_GIT[@]}" rev-parse HEAD)"
test "${#PAYLOAD_REVISION}" = 40
for metadata in airflow-base airflow-scheduler-payload \
  airflow-whoscored-proxy-payload airflow-scheduler \
  airflow-whoscored-proxy flaresolverr jupyterhub superset; do
  test ! -e "$BUILD_EVIDENCE/$metadata.json"
done
AIRFLOW_BASE_TAG="$REGISTRY/airflow-base:payload-$PAYLOAD_REVISION"
SCHEDULER_PAYLOAD_TAG="$REGISTRY/airflow-scheduler:payload-$PAYLOAD_REVISION"
PROXY_PAYLOAD_TAG="$REGISTRY/airflow-whoscored-proxy:payload-$PAYLOAD_REVISION"
FLARESOLVERR_TAG="$REGISTRY/flaresolverr-whoscored:payload-$PAYLOAD_REVISION"
JUPYTERHUB_TAG="$REGISTRY/jupyterhub:payload-$PAYLOAD_REVISION"
SUPERSET_TAG="$REGISTRY/superset:payload-$PAYLOAD_REVISION"

"${BUILDX_CMD[@]}" build --platform linux/amd64 \
  --provenance=mode=max,version=v1 --load --target airflow-base \
  --metadata-file "$BUILD_EVIDENCE/airflow-base.json" \
  --tag "$AIRFLOW_BASE_TAG" docker/images/airflow
"${BUILDX_CMD[@]}" build --platform linux/amd64 \
  --provenance=mode=max,version=v1 --load --target airflow-scheduler-payload \
  --metadata-file "$BUILD_EVIDENCE/airflow-scheduler-payload.json" \
  --tag "$SCHEDULER_PAYLOAD_TAG" docker/images/airflow
"${BUILDX_CMD[@]}" build --platform linux/amd64 \
  --provenance=mode=max,version=v1 --load --target airflow-whoscored-proxy-payload \
  --metadata-file "$BUILD_EVIDENCE/airflow-whoscored-proxy-payload.json" \
  --tag "$PROXY_PAYLOAD_TAG" docker/images/airflow
"${BUILDX_CMD[@]}" build --platform linux/amd64 \
  --provenance=mode=max,version=v1 --load \
  --file docker/images/flaresolverr-whoscored/Dockerfile \
  --metadata-file "$BUILD_EVIDENCE/flaresolverr.json" \
  --tag "$FLARESOLVERR_TAG" .
"${BUILDX_CMD[@]}" build --platform linux/amd64 \
  --provenance=mode=max,version=v1 --load \
  --metadata-file "$BUILD_EVIDENCE/jupyterhub.json" \
  --tag "$JUPYTERHUB_TAG" docker/images/jupyterhub
"${BUILDX_CMD[@]}" build --platform linux/amd64 \
  --provenance=mode=max,version=v1 --load \
  --metadata-file "$BUILD_EVIDENCE/superset.json" \
  --tag "$SUPERSET_TAG" docker/images/superset

AIRFLOW_BASE_ID="$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$AIRFLOW_BASE_TAG")"
AIRFLOW_SCHEDULER_PAYLOAD_ID="$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$SCHEDULER_PAYLOAD_TAG")"
WHOSCORED_PROXY_PAYLOAD_ID="$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$PROXY_PAYLOAD_TAG")"
FLARESOLVERR_PAYLOAD_ID="$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$FLARESOLVERR_TAG")"
JUPYTERHUB_PAYLOAD_ID="$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$JUPYTERHUB_TAG")"
SUPERSET_PAYLOAD_ID="$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$SUPERSET_TAG")"
for image_id in "$AIRFLOW_BASE_ID" "$AIRFLOW_SCHEDULER_PAYLOAD_ID" \
  "$WHOSCORED_PROXY_PAYLOAD_ID" "$FLARESOLVERR_PAYLOAD_ID" \
  "$JUPYTERHUB_PAYLOAD_ID" "$SUPERSET_PAYLOAD_ID"; do
  case "$image_id" in sha256:????????????????????????????????????????????????????????????????) ;; *) exit 1 ;; esac
done
```

```bash
GENERATED_AT="$(/usr/bin/date -u +%Y-%m-%dT%H:%M:%SZ)"
"${ADMISSION_PYTHON[@]}" \
  "$RELEASE/scripts/validate_whoscored_build_provenance.py" \
  --root "$RELEASE" --generate-ready --generated-at "$GENERATED_AT" \
  --payload-image-id "airflow-init=$AIRFLOW_BASE_ID" \
  --payload-image-id "airflow-log-init=$AIRFLOW_BASE_ID" \
  --payload-image-id "airflow-scheduler=$AIRFLOW_SCHEDULER_PAYLOAD_ID" \
  --payload-image-id "airflow-webserver=$AIRFLOW_BASE_ID" \
  --payload-image-id "fbref_proxy_filter=$AIRFLOW_SCHEDULER_PAYLOAD_ID" \
  --payload-image-id "flaresolverr=$FLARESOLVERR_PAYLOAD_ID" \
  --payload-image-id "flaresolverr_whoscored_paid=$FLARESOLVERR_PAYLOAD_ID" \
  --payload-image-id "jupyterhub=$JUPYTERHUB_PAYLOAD_ID" \
  --payload-image-id "proxy_filter=$AIRFLOW_BASE_ID" \
  --payload-image-id "superset=$SUPERSET_PAYLOAD_ID" \
  --payload-image-id "superset-beat=$SUPERSET_PAYLOAD_ID" \
  --payload-image-id "superset-worker=$SUPERSET_PAYLOAD_ID" \
  --payload-image-id "whoscored_paid_gateway=$WHOSCORED_PROXY_PAYLOAD_ID" \
  --payload-image-id "whoscored_proxy_filter=$WHOSCORED_PROXY_PAYLOAD_ID"

test "$(/usr/bin/git -C "$RELEASE" diff --name-only | /usr/bin/sort)" = \
"docker/images/airflow/whoscored-build-provenance-attestation.json
docker/images/airflow/whoscored-build-provenance-manifest.json"
```

The generator rejects missing, extra, duplicate or malformed payload IDs,
unresolved discovery issues, untracked evidence and every dirty path outside
those two generated files. It publishes the manifest first and the ready
attestation last, so interruption remains fail-closed and an identical rerun
is safe. Commit exactly those two files as the immediate child of the payload
commit. CI then uses `--expect-ready-build` to prove that child and the complete
repository closure agree. On a pull request it checks the explicit PR-head SHA
inside GitHub's clean synthetic merge checkout, with full history available;
the release SHA itself must still be the single-parent promotion child. This
CI mode is build verification only: ordinary validation and production
admission still require the separate external deployment attestation and final
registry digests.

After reviewing and committing exactly the two generated provenance files,
finish from that clean, single-parent promotion child. Do not rebuild the four
non-derived images: push the exact local objects whose IDs were recorded. Build
the scheduler and dedicated proxy final targets once from the promotion child,
and capture Buildx maximum provenance during those two builds:

```bash
test -z "$("${CLEAN_GIT[@]}" status --porcelain=v1 --untracked-files=all)"
RELEASE_REVISION="$("${CLEAN_GIT[@]}" rev-parse HEAD)"
test "${#RELEASE_REVISION}" = 40
test "$("${CLEAN_GIT[@]}" rev-parse HEAD^)" = "$PAYLOAD_REVISION"

test "$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$AIRFLOW_BASE_TAG")" = "$AIRFLOW_BASE_ID"
test "$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$FLARESOLVERR_TAG")" = "$FLARESOLVERR_PAYLOAD_ID"
test "$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$JUPYTERHUB_TAG")" = "$JUPYTERHUB_PAYLOAD_ID"
test "$("${DOCKER_CMD[@]}" image inspect --format '{{.Id}}' "$SUPERSET_TAG")" = "$SUPERSET_PAYLOAD_ID"
"${DOCKER_CMD[@]}" image push "$AIRFLOW_BASE_TAG"
"${DOCKER_CMD[@]}" image push "$FLARESOLVERR_TAG"
"${DOCKER_CMD[@]}" image push "$JUPYTERHUB_TAG"
"${DOCKER_CMD[@]}" image push "$SUPERSET_TAG"

SCHEDULER_FINAL_TAG="$REGISTRY/airflow-scheduler:ready-$RELEASE_REVISION"
PROXY_FINAL_TAG="$REGISTRY/airflow-whoscored-proxy:ready-$RELEASE_REVISION"
"${BUILDX_CMD[@]}" build --platform linux/amd64 \
  --provenance=mode=max,version=v1 --push --target airflow-scheduler \
  --metadata-file "$BUILD_EVIDENCE/airflow-scheduler.json" \
  --tag "$SCHEDULER_FINAL_TAG" docker/images/airflow
"${BUILDX_CMD[@]}" build --platform linux/amd64 \
  --provenance=mode=max,version=v1 --push --target airflow-whoscored-proxy \
  --metadata-file "$BUILD_EVIDENCE/airflow-whoscored-proxy.json" \
  --tag "$PROXY_FINAL_TAG" docker/images/airflow

AIRFLOW_BASE_FINAL_IMAGE="${AIRFLOW_BASE_TAG%:*}@$(/usr/bin/jq -er '."containerimage.digest"' "$BUILD_EVIDENCE/airflow-base.json")"
AIRFLOW_SCHEDULER_FINAL_IMAGE="${SCHEDULER_FINAL_TAG%:*}@$(/usr/bin/jq -er '."containerimage.digest"' "$BUILD_EVIDENCE/airflow-scheduler.json")"
AIRFLOW_WHOSCORED_PROXY_FINAL_IMAGE="${PROXY_FINAL_TAG%:*}@$(/usr/bin/jq -er '."containerimage.digest"' "$BUILD_EVIDENCE/airflow-whoscored-proxy.json")"
FLARESOLVERR_FINAL_IMAGE="${FLARESOLVERR_TAG%:*}@$(/usr/bin/jq -er '."containerimage.digest"' "$BUILD_EVIDENCE/flaresolverr.json")"
JUPYTERHUB_FINAL_IMAGE="${JUPYTERHUB_TAG%:*}@$(/usr/bin/jq -er '."containerimage.digest"' "$BUILD_EVIDENCE/jupyterhub.json")"
SUPERSET_FINAL_IMAGE="${SUPERSET_TAG%:*}@$(/usr/bin/jq -er '."containerimage.digest"' "$BUILD_EVIDENCE/superset.json")"

"${DOCKER_CMD[@]}" image pull "$AIRFLOW_BASE_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image pull "$AIRFLOW_SCHEDULER_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image pull "$AIRFLOW_WHOSCORED_PROXY_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image pull "$FLARESOLVERR_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image pull "$JUPYTERHUB_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image pull "$SUPERSET_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image inspect --format '{{json .Id}} {{json .RepoDigests}} {{json .Config}}' "$AIRFLOW_BASE_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image inspect --format '{{json .Id}} {{json .RepoDigests}} {{json .Config}}' "$AIRFLOW_SCHEDULER_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image inspect --format '{{json .Id}} {{json .RepoDigests}} {{json .Config}}' "$AIRFLOW_WHOSCORED_PROXY_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image inspect --format '{{json .Id}} {{json .RepoDigests}} {{json .Config}}' "$FLARESOLVERR_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image inspect --format '{{json .Id}} {{json .RepoDigests}} {{json .Config}}' "$JUPYTERHUB_FINAL_IMAGE"
"${DOCKER_CMD[@]}" image inspect --format '{{json .Id}} {{json .RepoDigests}} {{json .Config}}' "$SUPERSET_FINAL_IMAGE"
for metadata in airflow-base airflow-scheduler airflow-whoscored-proxy \
  flaresolverr jupyterhub superset; do
  test "$(/usr/bin/stat -c '%u:%g:%a:%h' "$BUILD_EVIDENCE/$metadata.json")" = \
    0:0:600:1
done
```

Create the external evidence with the fail-closed generator. Every value below
is the exact pushed
`repository@sha256:<64 hex>` reference, never a tag. The generator performs
read-only Docker image inspection, binds each digest to its protected Buildx
maximum-provenance file and exact clean Git revision, checks the final target,
Dockerfile and runtime config, then streams `docker image save` to reconstruct
and compare the exact bytes, owner and mode of all seven gate files inside each
digest-addressed final image. For the two derived Airflow targets it also
rejects every final-layer addition, removal, whiteout or extended attribute
outside the reviewed gate and Python-alias delta. It never creates or starts a
container, expands the six groups to all 14 local services, refuses an existing
output and creates a canonical root-owned mode-0600 file:

```bash
export WHOSCORED_DEPLOYMENT_ATTESTATION=/absolute/protected/deployment-attestation.json
test ! -e "$WHOSCORED_DEPLOYMENT_ATTESTATION"
"${ADMISSION_PYTHON[@]}" \
  "$RELEASE/scripts/generate_whoscored_deployment_attestation.py" \
  --root "$RELEASE" \
  --output "$WHOSCORED_DEPLOYMENT_ATTESTATION" \
  --final-image "airflow-base=$AIRFLOW_BASE_FINAL_IMAGE" \
  --final-image "airflow-scheduler=$AIRFLOW_SCHEDULER_FINAL_IMAGE" \
  --final-image "airflow-whoscored-proxy=$AIRFLOW_WHOSCORED_PROXY_FINAL_IMAGE" \
  --final-image "flaresolverr=$FLARESOLVERR_FINAL_IMAGE" \
  --final-image "jupyterhub=$JUPYTERHUB_FINAL_IMAGE" \
  --final-image "superset=$SUPERSET_FINAL_IMAGE" \
  --build-metadata "airflow-base=$BUILD_EVIDENCE/airflow-base.json" \
  --build-metadata "airflow-scheduler=$BUILD_EVIDENCE/airflow-scheduler.json" \
  --build-metadata "airflow-whoscored-proxy=$BUILD_EVIDENCE/airflow-whoscored-proxy.json" \
  --build-metadata "flaresolverr=$BUILD_EVIDENCE/flaresolverr.json" \
  --build-metadata "jupyterhub=$BUILD_EVIDENCE/jupyterhub.json" \
  --build-metadata "superset=$BUILD_EVIDENCE/superset.json"
test "$(/usr/bin/stat -c '%u:%g:%a:%h' \
  "$WHOSCORED_DEPLOYMENT_ATTESTATION")" = "0:0:600:1"
```

Do not create this JSON by hand. A missing/extra service, changed payload ID,
mutable tag, absent `RepoDigests` entry, wrong final target/config/gate input,
dirty Git revision, unsafe metadata path or Docker/repository change during the
two validation passes leaves deployment blocked.

Deploy that exact reviewed promotion SHA. A GitHub merge commit, squash or
rebased copy has a different ancestry/identity and is rejected even when its
files look identical. Integrate the promotion commit only with a SHA-preserving
fast-forward, or regenerate the blocked payload commit, payload image IDs and
two-file promotion child after integration. Never rewrite the manifest's
`source_revision` to make a merge result fit.

Run every admission command as root from the exact protected release. The
helper rejects another `--root`, a non-system interpreter, a non-isolated
Python startup, symlinked or writable release ancestors, and Docker/Compose
control variables inherited from the shell. All three environment files, the
deployment attestation and the admission directory must likewise be root-owned
and have only root-owned, non-writable ancestors.
Create a new evidence directory and validate ready mode before any service
lifecycle command:

```bash
set -Eeuo pipefail
test "$(/usr/bin/id -u)" = 0
test "$RELEASE" = "$(/usr/bin/realpath -e -- "$RELEASE")"
for file in "$COMPOSE_ENV_FILE" "$WHOSCORED_ENV_FILE" "$PROXY_POOL_ENV_FILE"; do
  test "$file" = "$(/usr/bin/realpath -e -- "$file")"
done
unset COMPOSE_DISABLE_ENV_FILE COMPOSE_ENV_FILES COMPOSE_FILE \
  COMPOSE_PATH_SEPARATOR COMPOSE_PROFILES COMPOSE_PROJECT_NAME \
  DOCKER_API_VERSION DOCKER_CERT_PATH DOCKER_CONFIG DOCKER_CONTEXT \
  DOCKER_HOST DOCKER_TLS_VERIFY PYTHONPATH LD_PRELOAD LD_AUDIT \
  LD_LIBRARY_PATH GCONV_PATH GLIBC_TUNABLES LOCPATH MALLOC_TRACE
umask 077

export WHOSCORED_DEPLOYMENT_ATTESTATION=/absolute/protected/deployment-attestation.json
export WHOSCORED_PROVIDER_QUOTA_RECEIPT=/absolute/protected/provider-quota-receipt.json
test "$WHOSCORED_DEPLOYMENT_ATTESTATION" = \
  "$(/usr/bin/realpath -e -- "$WHOSCORED_DEPLOYMENT_ATTESTATION")"
test "$WHOSCORED_PROVIDER_QUOTA_RECEIPT" = \
  "$(/usr/bin/realpath -e -- "$WHOSCORED_PROVIDER_QUOTA_RECEIPT")"
export WHOSCORED_ADMISSION_DIR="/absolute/protected/admission-$(/usr/bin/date -u +%Y%m%dT%H%M%SZ)"
test ! -e "$WHOSCORED_ADMISSION_DIR"
/usr/bin/install -d -o root -g root -m 0700 "$WHOSCORED_ADMISSION_DIR"
export WHOSCORED_DIGEST_OVERRIDE="$WHOSCORED_ADMISSION_DIR/digest-only.yaml"
export WHOSCORED_RENDERED_COMPOSE="$WHOSCORED_ADMISSION_DIR/rendered-compose.json"

"${ADMISSION_PYTHON[@]}" \
  "$RELEASE/scripts/validate_whoscored_build_provenance.py" \
  --root "$RELEASE" \
  --deployment-attestation "$WHOSCORED_DEPLOYMENT_ATTESTATION" \
  > "$WHOSCORED_ADMISSION_DIR/build-provenance-ready.json"

"${ADMISSION_PYTHON[@]}" \
  "$RELEASE/scripts/whoscored_production_admission.py" generate-override \
  --root "$RELEASE" \
  --deployment-attestation "$WHOSCORED_DEPLOYMENT_ATTESTATION" \
  --output "$WHOSCORED_DIGEST_OVERRIDE" \
  > "$WHOSCORED_ADMISSION_DIR/override-receipt.json"
```

The generated file contains exactly `airflow-scheduler`, `flaresolverr`,
`flaresolverr_whoscored_paid`, `whoscored_paid_gateway`, and
`whoscored_proxy_filter`. Every `image` is the attested
`repository@sha256:<64 hex>` value and every inherited local `build` is removed
with Compose 2.24.4+ `!reset`. The helper rejects an existing output of any
kind and publishes the mode-0600 file without following symlinks. Mutable local
tags are not admission evidence.

The admission helper, not a caller-provided render, now invokes
`/usr/bin/docker compose config --format json` and `config --hash` for each
protected service. It fixes the project, profile, environment-file order and
the exact base, supervised and digest-only file order. Do not append another
Compose file or set a Docker/Compose control variable. The protected canonical
provider receipt must bind a fresh root-owned screenshot, its SHA-256 and
mtime, PROXYS.IO order 38950, Bronze, active status, and exact decimal
quota/remaining values of 1.00 GB without containing proxy credentials. The
helper publishes a new mode-0600 rendered model only after all five protected
services and that external evidence pass:

```bash
"${ADMISSION_PYTHON[@]}" \
  "$RELEASE/scripts/whoscored_production_admission.py" verify-rendered \
  --root "$RELEASE" \
  --deployment-attestation "$WHOSCORED_DEPLOYMENT_ATTESTATION" \
  --override "$WHOSCORED_DIGEST_OVERRIDE" \
  --env-file "$COMPOSE_ENV_FILE" \
  --env-file "$WHOSCORED_ENV_FILE" \
  --env-file "$PROXY_POOL_ENV_FILE" \
  --project data-platform \
  --provider-quota-receipt "$WHOSCORED_PROVIDER_QUOTA_RECEIPT" \
  --output "$WHOSCORED_RENDERED_COMPOSE" \
  > "$WHOSCORED_ADMISSION_DIR/rendered-receipt.json"

# This is the only lifecycle command prefix permitted after admission. It is
# intentionally independent of the caller environment and repeats the exact
# helper-owned model. The whoscored-paid profile renders the protected proxy;
# it does not authorize creating or starting it.
COMPOSE=(
  /usr/bin/env -i
  HOME=/nonexistent PATH=/usr/bin:/bin
  LANG=C.UTF-8 LC_ALL=C.UTF-8
  DOCKER_HOST=unix:///run/docker.sock
  /usr/bin/docker compose --project-name data-platform
  --env-file "$COMPOSE_ENV_FILE"
  --env-file "$WHOSCORED_ENV_FILE"
  --env-file "$PROXY_POOL_ENV_FILE"
  --profile whoscored-paid
  --file "$RELEASE/compose.yaml"
  --file "$RELEASE/compose.seaweedfs-supervised.yaml"
  --file "$WHOSCORED_DIGEST_OVERRIDE"
)

# Production requires Engine 23+ support for the explicit builtin seccomp
# profile. This operator-side check is informational; post-create admission
# repeats it and proves AppArmor enforcement inside a constrained container.
DOCKER_SECURITY_OPTIONS=$(
  /usr/bin/env -i HOME=/nonexistent PATH=/usr/bin:/bin \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 DOCKER_HOST=unix:///run/docker.sock \
    /usr/bin/docker info --format '{{json .SecurityOptions}}'
)
/usr/bin/printf '%s\n' "$DOCKER_SECURITY_OPTIONS" | /usr/bin/jq -e '
  type == "array" and
  ([.[] | select(startswith("name=apparmor") or startswith("name=seccomp"))]
    | sort) == ["name=apparmor", "name=seccomp,profile=builtin"]
' >/dev/null
```

Do not require host reads of
`/sys/kernel/security/apparmor/profiles`: hardened production hosts can deny
that securityfs read even to root. Post-create admission instead launches one
ephemeral probe from the already attested scheduler image digest with
`--pull=never`, no network, a read-only root filesystem, UID/GID `50000:0`, no
capabilities, no-new-privileges, builtin seccomp and `docker-default`. The exact
`/proc/self/attr/current` result must be `docker-default (enforce)`; any probe
failure or different output blocks admission.

Only after those commands succeed may an approved operator capture the storage
identity and create the scheduler without starting it. The fingerprint binds
container ID, command and `/data` mount and must remain identical. `--no-build`
is mandatory even though the verified model has no remaining protected build;
`--pull always` resolves the attested registry digest, never a mutable tag.
Post-create admission is mandatory before the first start and names the exact
one-service rollout wave. It repeats the daemon security-options proof, runs
the constrained digest-attested AppArmor probe, and requires both the explicit
container `AppArmorProfile=docker-default` and the exact three Compose
`security_opt` values. All three protected services also drop every Linux
capability and add none; CI requires the scheduler and FlareSolverr processes
to report an all-zero `CapEff` before their functional smoke checks:

```bash
SEAWEED_BEFORE=$(/usr/bin/docker inspect seaweedfs | /usr/bin/jq -c '.[0] | {
  id: .Id, command: .Config.Cmd,
  data_mounts: [.Mounts[] | select(.Destination == "/data") |
    {name: .Name, source: .Source, destination: .Destination}]
}' | /usr/bin/sha256sum | /usr/bin/cut -d" " -f1)

"${COMPOSE[@]}" create --no-deps --no-build --pull always \
  airflow-scheduler

"${ADMISSION_PYTHON[@]}" \
  "$RELEASE/scripts/whoscored_production_admission.py" post-create \
  --root "$RELEASE" \
  --deployment-attestation "$WHOSCORED_DEPLOYMENT_ATTESTATION" \
  --override "$WHOSCORED_DIGEST_OVERRIDE" \
  --env-file "$COMPOSE_ENV_FILE" \
  --env-file "$WHOSCORED_ENV_FILE" \
  --env-file "$PROXY_POOL_ENV_FILE" \
  --project data-platform \
  --provider-quota-receipt "$WHOSCORED_PROVIDER_QUOTA_RECEIPT" \
  --service airflow-scheduler \
  > "$WHOSCORED_ADMISSION_DIR/post-create-scheduler-receipt.json"

"${COMPOSE[@]}" start airflow-scheduler

SEAWEED_AFTER=$(/usr/bin/docker inspect seaweedfs | /usr/bin/jq -c '.[0] | {
  id: .Id, command: .Config.Cmd,
  data_mounts: [.Mounts[] | select(.Destination == "/data") |
    {name: .Name, source: .Source, destination: .Destination}]
}' | /usr/bin/sha256sum | /usr/bin/cut -d" " -f1)
test "$SEAWEED_AFTER" = "$SEAWEED_BEFORE"
```

FlareSolverr is a separate rollout wave. Re-run the same storage fingerprint,
then create only `flaresolverr`, run the same post-create command with
`--service flaresolverr` and a fresh receipt path, and only then run
`"${COMPOSE[@]}" start flaresolverr`. Do not combine its receipt with the
scheduler receipt.

##### Initialize the paid-filter state exactly once

The paid filter never auto-creates authoritative state. Before
`verify-rendered`, provision the canonical host directory referenced by
`WHOSCORED_PROXY_FILTER_STATE_HOST_DIR` as an empty root-owned `root:root`
directory with mode `0770`; the gateway and alert-authority host directories
must be separate. After the ready deployment, rendered model and fresh provider
receipt pass admission—but before the first `whoscored_proxy_filter` container
is created or started—run this one-shot initializer. Export the same canonical
state path and the three filter secrets from the approved secret manager; their
values must not appear in command arguments:

```bash
set -Eeuo pipefail
test "$(/usr/bin/id -u)" = 0
: "${WHOSCORED_PROXY_FILTER_STATE_HOST_DIR:?export the admitted filter-state path}"
: "${WHOSCORED_PROXY_FILTER_CONTROL_TOKEN:?inject the filter control token}"
: "${WHOSCORED_PROXY_APPROVAL_HMAC_SECRET:?inject the approval HMAC}"
: "${WHOSCORED_PROXY_LEDGER_HMAC_SECRET:?inject the ledger HMAC}"
PROXY_FILTER_CONTROL_TOKEN="$WHOSCORED_PROXY_FILTER_CONTROL_TOKEN"
export PROXY_FILTER_CONTROL_TOKEN \
  WHOSCORED_PROXY_APPROVAL_HMAC_SECRET \
  WHOSCORED_PROXY_LEDGER_HMAC_SECRET

STATE="$WHOSCORED_PROXY_FILTER_STATE_HOST_DIR"
test "$STATE" = "$(/usr/bin/realpath -e -- "$STATE")"
test "$(/usr/bin/stat -c '%u:%g:%a' -- "$STATE")" = '0:0:770'
test -z "$(/usr/bin/find "$STATE" -mindepth 1 -maxdepth 1 -print -quit)"

FILTER_IMAGE="$(/usr/bin/jq -er \
  '.images[] | select(.service == "whoscored_proxy_filter") | .final_image' \
  "$WHOSCORED_DEPLOYMENT_ATTESTATION")"
test "$FILTER_IMAGE" = "$(/usr/bin/jq -er \
  '.services.whoscored_proxy_filter.image' "$WHOSCORED_RENDERED_COMPOSE")"
case "$FILTER_IMAGE" in
  *@sha256:????????????????????????????????????????????????????????????????) ;;
  *) echo 'filter image is not digest-qualified' >&2; exit 2 ;;
esac
/usr/bin/docker pull "$FILTER_IMAGE"
/usr/bin/docker image inspect "$FILTER_IMAGE" >/dev/null

/usr/bin/docker run --rm --pull=never --network=none --read-only \
  --user=50000:0 --cap-drop=ALL --pids-limit=64 --memory=256m \
  --security-opt=no-new-privileges=true \
  --security-opt=apparmor=docker-default \
  --security-opt=seccomp=builtin \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=32m,uid=50000,gid=0,mode=0700 \
  --env CONNECTION_CHECK_MAX_COUNT=0 \
  --env PROXY_FILTER_CONTROL_TOKEN \
  --env WHOSCORED_PROXY_APPROVAL_HMAC_SECRET \
  --env WHOSCORED_PROXY_LEDGER_HMAC_SECRET \
  --env TM_PROXY_CONTROL_TOKEN= \
  --env PROXY_FILTER_ALLOW_FILE_FALLBACK=false \
  --mount "type=bind,src=$RELEASE/dags,dst=/opt/airflow/dags,readonly" \
  --mount "type=bind,src=$RELEASE/scripts,dst=/opt/airflow/scripts,readonly" \
  --mount "type=bind,src=$RELEASE/scrapers,dst=/opt/airflow/scrapers,readonly" \
  --mount "type=bind,src=$RELEASE/configs/medallion,dst=/opt/airflow/configs/medallion,readonly" \
  --mount "type=bind,src=$RELEASE/configs/proxy_filter,dst=/opt/airflow/configs/proxy_filter,readonly" \
  --mount "type=bind,src=$RELEASE/configs/sofascore,dst=/opt/airflow/configs/sofascore,readonly" \
  --mount "type=bind,src=$STATE,dst=/opt/airflow/state/whoscored-proxy-filter" \
  "$FILTER_IMAGE" \
  python /opt/airflow/scripts/proxy_filter/filter_proxy.py \
  --source-mode whoscored-only \
  --listen 0.0.0.0:8899 \
  --lease-listen 0.0.0.0:8900 \
  --lease-proxy-url http://whoscored_proxy_filter:8900 \
  --blocklist /opt/airflow/configs/proxy_filter/blocklist.txt \
  --out /opt/airflow/state/whoscored-proxy-filter/bytes.json \
  --daily-budget-mb 850 \
  --max-lease-mb 2 \
  --max-lease-ttl-seconds 3600 \
  --dagrun-budget-bytes 1000000000 \
  --transfermarkt-dagrun-budget-bytes 0 \
  --url-budget-bytes 2000000 \
  --max-active-leases 2 \
  --sofascore-budget-artifact /opt/airflow/configs/sofascore/proxy_budget_canary.json \
  --sofascore-canary-hard-cap-bytes 0 \
  --sofascore-discovery-dagrun-budget-bytes 0 \
  --ledger /opt/airflow/state/whoscored-proxy-filter/paid_requests.jsonl \
  --whoscored-campaign-ledger /opt/airflow/state/whoscored-proxy-filter/whoscored_campaigns.json \
  --whoscored-state-marker /opt/airflow/state/whoscored-proxy-filter/.whoscored_state_initialized.json \
  --initialize-whoscored-state

for file in \
  bytes.json \
  paid_requests.jsonl \
  paid_requests.jsonl.checkpoint.json \
  whoscored_campaigns.json \
  .whoscored_state_initialized.json; do
  test -f "$STATE/$file"
  test "$(/usr/bin/stat -c '%u:%g:%a' -- "$STATE/$file")" = '50000:0:600'
done
test ! -s "$STATE/paid_requests.jsonl"
```

The container has no network, ports, provider credential or proxy-pool input;
the production entrypoint and Python runtime attestors still execute from the
exact admitted digest. `--mount` refuses a missing host source, so Docker cannot
silently create a replacement state directory. The initializer refuses any
pre-existing state file and the service command contains no initialization
flag. Never add that flag to Compose, use `compose up` for initialization, or
delete partial/authenticated files to force a rerun. Quarantine a failed state
directory and repeat the full render/admission review with a new empty canonical
directory.

The three paid-boundary services remain forbidden to create or start until the
dedicated provider quota/usage receipt and the reviewed exact-origin release
have both been approved. After that separate approval, create and attest
`whoscored_proxy_filter` and `flaresolverr_whoscored_paid` first, start both,
then create, attest and start `whoscored_paid_gateway`. Use `--no-deps
--no-build --pull always` and a separate `post-create --service ...` receipt
for each service. The paid browser has no host port/direct egress, the filter
has only provider egress, and the gateway alone spans the API, browser and
direct-egress networks. Rendering the `whoscored-paid` profile is never
paid-route authority. The Docker daemon and every principal able to access its
socket remain part of the trusted single-tenant host boundary; no receipt can
constrain a Docker-authorized attacker.

## Idempotent Airflow migration

This and every later scheduler command in the runbook belong to the future
`ready-v1` path above. Under the checked-in `blocked-v1` image there must be no
running production scheduler to exec into.

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

`dag_backfill_whoscored` and `dag_canary_whoscored_proxy` intentionally have no
schedule and remain paused manual/continuation-only DAGs. Verify all WhoScored
pause states and the next-run timestamps in the Airflow UI or
`airflow dags list` after the scheduler heartbeat.

## Storage scope

Do not run the four-plane SeaweedFS cutover playbook during this source rollout.
The one-time quiesced isolation rollout above is a deployment prerequisite for
an old mini identity; after it succeeds, do not recreate the storage boundary
during the targeted Airflow/proxy update. Verify the exact live runtime through
the wrapper and a Trino read before and after. Backup/cutover tools remain
code-owned disabled for a separately approved resilience project.

For that future project only, keep the versioned recovery command contract
below. It is not a gate or an authorization to enable backup in this rollout.
An operator first lists and authenticates an exact immutable inventory key:

```bash
"$RELEASE/scripts/compose.sh" --env-file "$COMPOSE_ENV_FILE" \
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
"$RELEASE/scripts/compose.sh" --env-file "$COMPOSE_ENV_FILE" \
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

python dags/scripts/run_whoscored_scraper.py discover \
  --full-history --as-of-date 2026-07-14 \
  --direct-only --output "$DISCOVERY"

export CATALOG_BATCH="$(python - "$DISCOVERY" <<'PY'
import json
import re
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    report = json.load(handle)
batch_id = str(report.get("catalog_batch_id") or "")
if report.get("status") != "success" or not re.fullmatch(
    r"wsc2-[0-9a-f]{64}", batch_id
):
    raise SystemExit("discovery report has no successful exact catalog identity")
print(batch_id)
PY
)"

python dags/scripts/run_whoscored_scraper.py daily \
  --scope 'ENG-Premier League=2627' --skip-profiles \
  --catalog-batch-id "$CATALOG_BATCH" --direct-only --output "$SMOKE"

python - <<'PY'
import os

from dags.dag_ingest_whoscored import validate_scope_result
validate_scope_result(
    scope_spec="ENG-Premier League=2627",
    result_path=(
        "/opt/airflow/logs/whoscored/manual/20260714T161016Z/"
        "daily_epl_2627.json"
    ),
    require_zero_paid=True,
    expected_catalog_batch_id=os.environ["CATALOG_BATCH"],
)
PY
```

`--as-of-date` is mandatory and immutable. Scheduled runs use the Airflow
logical date. A catalog manifest created before the `as_of_date`, raw
provenance and parent-lineage fields existed is not a valid incremental
parent: keep the DAGs paused and publish one reviewed `--full-history`
generation first.

Verify the write through the same authenticated Trino endpoint used by
DataGrip. At minimum, these queries must return a parser-v8 full-history catalog
and parser-v8 current evidence for the smoke scope:

```sql
SELECT batch_id, parser_version, discovery_mode, state,
       competitions_count, seasons_count, stages_count,
       raw_provenance_sha256, as_of_date,
       parent_catalog_batch_id, parent_catalog_payload_sha256,
       parent_catalog_raw_provenance_sha256, raw_inputs_json
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

The v3 supervisor validates the exact external `ready-v1` deployment
attestation and its protected digest-only Compose override before it starts a
worker. It resolves the local ID behind the attested
`repository@sha256:<64 hex>` FlareSolverr reference on every sample. The running
container must keep that ID and the current checkout's three-file Compose
hash/labels,
use the image-baked root-owned launcher and extension with no bind/volume
mounts and only the four exact reviewed tmpfs paths, keep a read-only root
filesystem, numeric UID/GID, dropped capabilities and the exact AppArmor/seccomp
options. Retagging the reference, using the old upstream image with an extension
bind, shadowing the payload with tmpfs, or starting the service from another
checkout stops the canary fail-closed.
It renders that hash through `/usr/bin/docker compose`, the base, supervised
and exact digest-only files in admission order, and all three protected
environment files. The selected SeaweedFS topology state and
`WHOSCORED_PROXY_APPROVAL_HOST_DIR` prerequisite must already pass even though
this canary itself is direct-only. Replacement or mutation of any attestation,
Compose or environment input stops the supervisor fail-closed.

Use the exact checkout whose ready revision and payloads are named by the
deployment attestation. The evidence directory is outside the checkout and
create-once output remains mode `0600`:

```bash
RELEASE=/absolute/path/to/the-ready-release
cd "$RELEASE"
test "$(pwd -P)" = "$RELEASE"
: "${WHOSCORED_DEPLOYMENT_ATTESTATION:?set the protected ready-v1 deployment attestation}"
: "${WHOSCORED_DIGEST_OVERRIDE:?set the protected digest-only Compose file}"
test -f "$WHOSCORED_DEPLOYMENT_ATTESTATION"
test -f "$WHOSCORED_DIGEST_OVERRIDE"
: "${WHOSCORED_PROXY_APPROVAL_HOST_DIR:?set the protected approval directory used by the admitted Compose model}"
test -d "$WHOSCORED_PROXY_APPROVAL_HOST_DIR"
export WHOSCORED_PROXY_APPROVAL_HOST_DIR
CANARY_EVIDENCE_DIR=/root/whoscored-954-runtime
install -d -o root -g root -m 0700 "$CANARY_EVIDENCE_DIR"
CANARY_OUTPUT="$CANARY_EVIDENCE_DIR/whoscored-capacity-$(date -u +%Y%m%dT%H%M%SZ).json"
/root/.venvs/dpf-test/bin/python \
  scripts/research/bench_whoscored_capacity.py \
  --duration-seconds 21600 \
  --scope 'ENG-Premier League=2526' \
  --scope 'INT-World Cup=2026' \
  --match-limit 3 --profile-limit 3 \
  --flaresolverr-url http://127.0.0.1:8191 \
  --deployment-attestation "$WHOSCORED_DEPLOYMENT_ATTESTATION" \
  --digest-override "$WHOSCORED_DIGEST_OVERRIDE" \
  --container airflow-scheduler \
  --container flaresolverr \
  --output "$CANARY_OUTPUT"
```

Run this supervisor on the host: it reads its own four child process trees from
`/proc`, reads required-container cgroup memory through `docker stats`, and
uses `docker inspect` for restart/recreate/OOM evidence. Scheduler and the
WhoScored FlareSolverr are mandatory even when `--container` is omitted and
are linked to the exact running production-admission receipt. That option can
only add extra monitoring-only containers (for example the shared
`proxy_filter`) and never expands WhoScored deployment authority. The output
path is create-once mode 0600.

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
four-worker non-publishing capacity canary, promote the real Airflow pool to
four, and complete one manual plus one scheduled daily run. The paid-proxy
measurement is a separate gate described in
`docs/operations/whoscored-proxy-campaign.md`: run only its exact 1 GB signed
canary, whose executable provider-order ceiling is the durable `850 MiB`
safety cap, persist the immutable p95 evidence, and review the proposed full
cap. That canary does not authorize the full crawl.

The separately reviewed production crawl must remain `direct_only` in this
release. If paid fallback is still required, stop: the full-crawl sentinel is
false, and neither a new approval nor DagRun configuration can enable it. A
future paid-backfill proposal requires a different reviewed release; never
reuse the measurement approval.

Direct-only trigger:

```bash
docker exec airflow-scheduler airflow dags trigger dag_backfill_whoscored \
  --conf '{"all_catalog":true,"transport_policy":"direct_only","require_zero_paid":true}'
```

Do not trigger a second selector for the same crawl. The first run stores the
immutable plan and queues deterministic continuation runs. Completion means:

- plan status is `complete` and no schedulable work remains;
- full-history discovery provenance is bound to the plan;
- historical DQ passes for all frozen eligible scopes and 25 datasets;
- every eligible roster player has a current profile or typed terminal proof;
- aggregate paid proxy bytes are exactly zero; and
- the plan completed no later than its 30-day deadline.

Any `awaiting_approval` state in a production backfill is fail-closed evidence,
not a prompt to mint paid authority. Pause and investigate it; DagRun
configuration cannot increase a budget or bypass the full-crawl sentinel.

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
READY. That historical full unit run reported 5,723 passed, 46 skipped, and a
SofaScore fingerprint failure that was fixed later; it is not a release
exception.

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

The live storage core remains one `weed mini`, isolated behind the private S3
gateway and HTTP proxy after the required one-time rollout. Promote concurrency
only through the sustained canary. Do not create
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
