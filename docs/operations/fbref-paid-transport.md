# FBref paid transport

FBref live Airflow tasks have one paid path:

`Airflow runner -> fbref_proxy_filter lease -> dedicated residential pool -> FBref`

The FBref runner never opens or parses an upstream proxy login or password. It
receives a short-lived lease token. `fbref_proxy_filter` counts both directions
at the provider boundary and refuses the next byte before a lease, URL, DagRun,
or daily hard cap can be crossed. The exact delta is stored as
`provider_billed_bytes` for successful and failed fetches. Other legacy DAGs
still use the shared `/opt/airflow/proxys.txt` mount; this guarantee applies to
the FBref code path and its separately mounted production pool.

The default hard limits are 100 MiB per DagRun/lease and 300 MiB per UTC day.
Only one FBref lease may be active. The data plane allows FBref, Cloudflare
Turnstile, and one bounded Camoufox IP-location endpoint; every other host gets
403 before an upstream connection is opened.

## Production mount

The dedicated pool file is intentionally ignored by Git. The production
Compose override must replace the proxy-file and logs mounts for the new
service:

```yaml
services:
  fbref_proxy_filter:
    volumes:
      - ${FBREF_PROXY_POOL_FILE}:/opt/airflow/proxys.txt:ro
      - /root/data-platform-football/logs:/opt/airflow/logs
```

Set `FBREF_PROXY_POOL_FILE=/root/fbref-949-runtime/proxys.txt` (or another
absolute, Git-ignored path) and generate a dedicated 32+ character
`FBREF_PROXY_CONTROL_TOKEN`. Airflow and `fbref_proxy_filter` must receive that
same FBref token. The FBref client deliberately does not fall back to
`SOFASCORE_PROXY_CONTROL_TOKEN` or `PROXY_FILTER_CONTROL_TOKEN`. Never print the
pool file, lease token, or control secret.

## Isolated acceptance project

Issue #949 acceptance uses
`deploy/fbref/acceptance.compose.yaml` as a standalone Compose project. It does
not merge with `compose.yaml`, publish ports, set global container names, or
reuse production volumes. Its PostgreSQL data is tmpfs-backed, and its proxy is
attached only to a project-scoped bridge. The one-off runner is the only
container also attached to the existing `dp-backend` and `dp-storage` networks.
Therefore starting or stopping acceptance cannot recreate the shared Airflow
scheduler or any production proxy.

Build the final acceptance overlay only from a committed revision. The helper
uses `git archive`, not the working tree, and bakes all runtime imports
(`dags`, `scrapers`, `scripts`, and `configs`) over a digest-resolved reviewed
scheduler base. It then prints the exact image ID used by Compose and evidence:

```bash
export FBREF_RUNTIME_BASE=<reviewed-scheduler-image>
MERGE_SHA=<full-commit-sha>
BUILD_ENV=$(mktemp)
bash scripts/build_fbref_acceptance_image.sh "$MERGE_SHA" >"$BUILD_ENV"
set -a
. "$BUILD_ENV"
set +a
rm -f "$BUILD_ENV"
test "$FBREF_ACCEPTANCE_GIT_SHA" = "$MERGE_SHA"
test "$(docker image inspect -f '{{.Id}}' "$FBREF_ACCEPTANCE_AIRFLOW_IMAGE")" \
  = "$FBREF_ACCEPTANCE_AIRFLOW_IMAGE"
```

`Dockerfile.fbref-acceptance` seals a per-file manifest inside the image. Both
the proxy and runner execute `fbref_acceptance_entrypoint.sh` before the
inherited Airflow entrypoint; it rejects a changed source tree, a declared Git
SHA that differs from the baked SHA, or anything other than the exact
`sha256:<image-id>` selected by Compose. No application-source bind mounts are
allowed in this project, so proxy and runner use the same baked revision.

Before starting, verify the remaining paths without printing their contents
and export the required values:

```bash
test -f "$FBREF_PROXY_POOL_FILE"
test -d "$FBREF_ACCEPTANCE_EVIDENCE_DIR"
test -f "$FBREF_ACCEPTANCE_ENV_FILE"
test -n "$FBREF_ACCEPTANCE_AIRFLOW_IMAGE"
test "${#FBREF_ACCEPTANCE_GIT_SHA}" -eq 40
test -n "$FBREF_ACCEPTANCE_DB_PASSWORD"
test "${#FBREF_PROXY_CONTROL_TOKEN}" -ge 32
docker run --rm --network none --read-only \
  --user "${AIRFLOW_UID:-50000}:0" \
  --volume "$FBREF_ACCEPTANCE_EVIDENCE_DIR:/evidence" \
  --entrypoint /bin/bash "$FBREF_ACCEPTANCE_AIRFLOW_IMAGE" -euc \
  'probe=$(mktemp /evidence/.fbref-write-probe.XXXXXXXX); rm -f -- "$probe"'
```

`FBREF_ACCEPTANCE_AIRFLOW_IMAGE` must be the full image ID printed by the build
helper, not a mutable tag. `FBREF_ACCEPTANCE_DB_PASSWORD` should be a temporary
hex value. The acceptance env file must contain an explicit
`FBREF_CONTROL_DB_URI` for the production FBref control database; the runner
refuses the temporary Airflow metadata database as a fallback. It supplies
production S3/Trino credentials but must not contain SofaScore's proxy token as
an FBref substitute. The isolated
write probe confirms that the image's `airflow` UID/group can create and remove
evidence; a directory that merely exists but is root-only is not acceptable.

Before each live acceptance run, claim one of the three durable campaign
slots. Keep this evidence directory between attempts; the script refuses a
fourth paid attempt:

```bash
ATTEMPT=$(bash scripts/claim_fbref_acceptance_attempt.sh \
  "$FBREF_ACCEPTANCE_EVIDENCE_DIR")
echo "claimed acceptance attempt $ATTEMPT"
```

Start only the isolated dependencies, then execute the acceptance DAG in a
one-off runner:

```bash
docker compose -p fbref-acceptance-949 \
  -f deploy/fbref/acceptance.compose.yaml --project-directory . \
  up -d fbref_acceptance_postgres fbref_acceptance_proxy_filter
docker compose -p fbref-acceptance-949 \
  -f deploy/fbref/acceptance.compose.yaml --project-directory . \
  run --rm fbref_acceptance_runner bash -lc \
  'airflow db migrate && RUN_AT="${RUN_AT:?set an explicit UTC run time}" && airflow dags test dag_accept_fbref_bronze "$RUN_AT"'
```

Run the current cohort and the historical cohort as separate manual runs. Then
run the zero-network replay DAG against the recorded control run id. The final
decision is produced from the immutable evidence, for example:

```bash
python scripts/report_fbref_bronze_acceptance.py \
  --control-run-id <control-run-uuid> --scope current \
  --git-sha "$FBREF_ACCEPTANCE_GIT_SHA" \
  --image-digest "$FBREF_ACCEPTANCE_AIRFLOW_IMAGE" \
  --output-root "$FBREF_ACCEPTANCE_EVIDENCE_DIR/reports"
```

After the report and proxy ledger are present in the evidence directory, remove
only the acceptance project:

```bash
docker compose -p fbref-acceptance-949 \
  -f deploy/fbref/acceptance.compose.yaml --project-directory . \
  down --volumes --remove-orphans
```

## Isolated browser runtime

FBref uses the checksum-pinned browser in `/opt/fbref-camoufox`. SofaScore uses
its own reviewed browser in `/home/airflow/.cache/camoufox`; never replace that
directory during an FBref deploy. The durable scheduler build is
`docker/images/airflow/Dockerfile.scheduler-runtime` and `compose.yaml` points
the scheduler at it.

For a release, build from an explicitly tagged, already verified webserver
base and label the result with the merge SHA:

```bash
BASE_IMAGE_ID=<sha256-of-verified-running-webserver-image>
BASE_IMAGE=data-platform-airflow-webserver:verified-fbref-base
MERGE_SHA=<full-merge-sha>
docker tag "$BASE_IMAGE_ID" "$BASE_IMAGE"
test "$(docker image inspect -f '{{.Id}}' "$BASE_IMAGE")" = "$BASE_IMAGE_ID"
docker build --pull=false \
  -f docker/images/airflow/Dockerfile.scheduler-runtime \
  --build-arg AIRFLOW_RUNTIME_BASE="$BASE_IMAGE" \
  --label org.opencontainers.image.revision="$MERGE_SHA" \
  -t "data-platform-airflow-scheduler:fbref-$MERGE_SHA" \
  docker/images/airflow
```

The release override must set that exact scheduler image and use
`build: !reset null`; deploy only the scheduler with
`up -d --no-deps --no-build --pull never`. Do not rebuild the webserver from
the legacy full Dockerfile. Before any live run, execute
`validate_camoufox_runtime()` inside the scheduler. It checks the browser,
Camoufox, Playwright, and curl_cffi pins without opening a paid lease.

## Deploy and verify

```bash
RUNTIME_OVERRIDE=/path/to/runtime-compose.override.yaml
docker compose -f compose.yaml -f "$RUNTIME_OVERRIDE" up -d --no-deps fbref_proxy_filter
docker compose -f compose.yaml -f "$RUNTIME_OVERRIDE" exec -T fbref_proxy_filter \
  curl -fsS http://localhost:8899/health
```

Then recreate the Airflow scheduler/webserver so they receive
`FBREF_PROXY_CONTROL_URL`. A live runner refuses to start when the control URL,
secret, run provenance, or byte cap is missing. `dag_replay_fbref` remains
network-free and cannot request an FBref lease.

The live runner is a new process-group leader. Linux `PDEATHSIG` kills it if
its Airflow parent dies, and a pipe watchdog kills the whole group if the
runner is SIGKILLed/OOM-killed, so Firefox descendants cannot keep spending.

The durable provider ledger is
`logs/fbref/proxy_filter/paid_requests.jsonl`; the aggregate report is
`logs/fbref/proxy_filter/bytes.json`. These files contain counters and hashed
upstream identifiers, not proxy credentials.

## Manual non-publishing bootstrap

Use this mode only to advance the durable current-scope queue while production
freshness is still being established. It performs the normal raw-first
recovery, paid fetch, parse, and raw-integrity audit, but it cannot run the
freshness gate, export a publication scope, or trigger Silver.

```bash
airflow dags unpause dag_bootstrap_fbref
airflow dags trigger dag_bootstrap_fbref
```

`dag_bootstrap_fbref` has `schedule=None`, so it is safe to leave unpaused: it
can create only an explicitly triggered manual DagRun. Its tasks contain
literal `200 requests / 100 MiB / shard 25` limits and literal
`bootstrap_only=true`; DagRun conf cannot change them. The scheduled
`dag_ingest_fbref` keeps its original daily schedule, parameters, and
publishing default. The existing `100/50` canary remains a separate path.

A successful bootstrap has these three pieces of evidence:

1. Airflow task `validate_bootstrap_run` returns the deterministic control run
   ID, `execution_mode=bootstrap_only`, `publication_eligible=false`, and the
   validation summary.
2. `fbref_control.crawl_run.metadata` stores the same mode and publication
   flag, and the control run status is `succeeded`.
3. `release_bootstrap_publication_lock` succeeds, followed by the common
   `release_publication_lock` finalizer. No `export_publication_scope` or
   `trigger_silver_transform` task may run.

The finalizer always attempts an exact idempotent lock release, then requires
all ten earlier bootstrap tasks (including the direct release task) to be
`success`. If live fetch, audit, validation, or release failed, the lock is
cleaned but the finalizer raises, so Airflow keeps the DagRun red. The
control-run mode also blocks publication-scope export, replay-source
selection, and the first Silver preflight. Start a normal production DagRun
after freshness is complete.
