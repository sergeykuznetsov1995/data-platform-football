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
