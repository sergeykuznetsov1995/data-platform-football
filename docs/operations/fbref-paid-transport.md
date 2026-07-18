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
      - /root/fbref-949-runtime/proxys.txt:/opt/airflow/proxys.txt:ro
      - /root/data-platform-football/logs:/opt/airflow/logs
```

Use the same 32+ character control secret in Airflow and the service through
`SOFASCORE_PROXY_CONTROL_TOKEN`. Never print the pool file, lease token, or
control secret.

## Isolated browser runtime

FBref uses the checksum-pinned browser in `/opt/fbref-camoufox`. SofaScore uses
its own reviewed browser in `/home/airflow/.cache/camoufox`; never replace that
directory during an FBref deploy. The durable scheduler build is the
`airflow-scheduler` target in `docker/images/airflow/Dockerfile`, and
`compose.yaml` points the scheduler at that target.

For a release, build the pinned unified target and label it with the merge SHA:

```bash
MERGE_SHA=<full-merge-sha>
docker build --pull=false \
  -f docker/images/airflow/Dockerfile \
  --target airflow-scheduler \
  --label org.opencontainers.image.revision="$MERGE_SHA" \
  -t "data-platform-airflow-scheduler:fbref-$MERGE_SHA" \
  docker/images/airflow
```

The release override must set that exact scheduler image and use
`build: !reset null`; deploy only the scheduler with
`up -d --no-deps --no-build --pull never`. Before any live run, execute
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
