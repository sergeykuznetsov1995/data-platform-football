# SeaweedFS topology cutover

The existing `seaweedfs` container runs `weed mini` and owns the same external
Docker volume that the new master, volume and filer services reuse. Before the
cutover, `scripts/compose.sh` selects only `compose.yaml`. The cutover atomically
persists the exact supervised mode, volume name, image ID, effective
`master.volumeSizeLimitMB` and recovery-inventory SHA-256 in
`/var/lib/data-platform-football/seaweedfs-topology.mode`; from that point the
same wrapper always adds `compose.seaweedfs-supervised.yaml`.

The irreversible cutover is currently code-owned disabled. The script performs
only non-mutating preflight and aborts before stopping any writer. Do not change
`SEAWEEDFS_SUPERVISED_CUTOVER_AVAILABLE` until an independent off-host WORM
receipt is verifiable and a resumable application/catalog attestation keeps the
server-side Read/List fence plus protected pending state in place through
`SHOW SCHEMAS FROM iceberg` and the frozen 25-dataset DQ gate. An environment
variable cannot enable this boundary.

Release rollout is permitted only when the existing production data plane is
the legacy `weed mini` topology and its volume has no supervised marker. Older
checkouts that already started `seaweedfs-master`, `seaweedfs-volume` or
`seaweedfs-filer` have no protected state and are **not adoptable by this
release**. Leave that release and its volumes intact; do not run `make clean`,
`up`, or manufacture a state file. `scripts/compose.sh` detects surviving
planes, an S3 gateway, the rendered external volume and old Compose-labeled
volumes with any different name, then fails before a mutating Compose call. A separately
reviewed adoption/recovery workflow is required for those installations.

Every routine lifecycle command must use `scripts/compose.sh` or a Make target.
Never run raw `docker compose`, never supply `-f/--file`, and never delete the
external named volume. The wrapper rejects file overrides, image/volume/marker
drift and destructive volume flags. A verification-pending state locks all
routine commands after an interrupted cutover until the reviewed recovery
transition completes.

Before a protected supervised state exists, the legacy volume name is fixed to
`seaweedfs_data`. Changing `SEAWEEDFS_DATA_VOLUME_NAME` is rejected even when a
different external volume already exists; otherwise a container-less `down`
followed by env drift could silently present a different store. Alternate
volume names are available only through the reviewed supervised recovery state.

The legacy `weed mini` master/filer/volume listeners live only on the internal
`dp-seaweedfs-control` network. WebDAV, Admin UI, embedded S3/IAM and the
unauthenticated embedded Iceberg endpoint are disabled in that process. The
volume-less `seaweedfs-s3` process also lives only on that control network. It
enforces the static S3 identity file, disables embedded IAM and Iceberg, and is
never directly reachable by an application container. A digest-pinned,
credential-free `seaweedfs-s3-proxy` is the only dual-homed process: it exposes
only HTTP 8333 to `dp-storage` and loopback while the gateway's unauthenticated
gRPC 18333 stays private. Cutover stops proxy, private gateway and mini in that
order, then removes the legacy gateway before the supervised private gateway
can claim its internal alias. A protected state refuses to start writers while
that orphan exists.

Docker `internal: true` does **not** block the host from connecting directly to
a container IP. The raw SeaweedFS master, filer, volume and S3/gRPC listeners
are therefore isolated from untrusted containers and remote networks, but not
from processes in the host network namespace. This deployment has a strict
single-tenant-host prerequisite: every operating-system account and host
process is a trusted storage principal, and interactive SSH access is limited
to platform administrators. Keycloak/Jupyter application users are not host
accounts. If an untrusted local shell, batch account or host service exists,
this topology is a deployment **NO-GO** until a separately reviewed persistent
host OUTPUT firewall or loopback-only shared network namespace is installed
and audited. This repository does not claim that `internal` supplies that host
boundary.

Treat this as an externally enforced production preflight, not an automated
Compose guarantee. Before deployment, inventory interactive accounts with
`getent passwd`, inventory host services/process owners, and attach an operator
attestation that every result is a trusted platform principal. If that
attestation is absent or any entry is untrusted, abort deployment; an
environment variable cannot waive the NO-GO.

The wrapper also treats Compose subcommands as part of this boundary. It
rejects `attach`/`commit`/`cp`/`exec`/`export`/`run` access to every
`seaweedfs*` service, detached `run`/`exec`, `run` mounts of either the logical
or resolved storage volume, destructive volume flags, and implicit or explicit
storage targets for `kill`, `pause`, `start`, `restart`, `scale`, `unpause`,
and `watch`, including `up --scale`. `down --rmi` cannot remove the pinned
offline-restart image. Unknown global options fail closed. `config` may write
only to stdout; `--output` and image lock files are forbidden.
`jupyterhub` is also forbidden for `attach`/`commit`/`cp`/`exec`/`export`/`run`
because its Docker socket is host-equivalent control. The long-lived JupyterHub
service, Docker administrators, and all host OS accounts/processes remain
trusted control-plane principals; the wrapper is a lifecycle/data-integrity
boundary, not a defence against a compromised Docker daemon, socket holder, or
hostile process in the host network namespace. The known old
`/entrypoint.sh` legacy container is deliberately not reconciled by routine
`up`: it requires the reviewed quiesced isolation rollout, which stops writers
and the old container before creating the isolated mini/private-gateway/proxy
set. Routine unscoped `up` must use `--no-recreate`; targeted updates must
combine `--no-deps` with explicit
non-storage services. Explicit storage targets, dependency recreation and an
unscoped force-recreate are rejected. `make restart` restarts only named
application services and never the storage boundary. The
legacy service has a fixed 120-second stop grace period; routine `up`, `stop`
and `down` reject every CLI timeout override, including bundled short flags.

Provision the persistent state and lifecycle lock before the first deployment
and create the external legacy volume explicitly. The deployment account must
be able to atomically replace the state file. The state and lock directories
must be owned by root or the deployment account, must not be group/world
writable, and must not be symlinks. The lock itself must grant no group/world
permissions. These controls contain operator mistakes and misconfigured local
services; they do not relax the dedicated-host prerequisite or claim defence
against a hostile host account:

```bash
sudo install -d -o "$USER" -g "$USER" -m 0755 /var/lib/data-platform-football
sudo install -o "$USER" -g "$USER" -m 0600 /dev/null \
  /var/lib/data-platform-football/seaweedfs-topology.lock
docker volume inspect seaweedfs_data >/dev/null 2>&1 || \
  docker volume create seaweedfs_data
# A pre-created volume is audited before Compose may mutate anything. Pull the
# exact reviewed image first; the wrapper never substitutes or auto-pulls a tag.
docker pull \
  chrislusf/seaweedfs:4.36@sha256:800b2115c63236e8bd0e5d572dc25dd493dc2feed08b54a2a269dc0101c9d94a
docker pull \
  caddy:2.10-alpine@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d
export SEAWEEDFS_DATA_VOLUME_NAME=seaweedfs_data
```

Before any start/resume command, provision the S3 identity JSON at an absolute
host-protected path, owned by root or the deployment account and mode `0600`,
then set `SEAWEEDFS_S3_CONFIG_FILE` in both the shell and the Compose env file.
The wrapper renders the final Compose model (including `--env-file` values),
requires the active platform pair to map to an Admin identity, and validates any
dedicated WhoScored raw/backup pair against exact bucket-scoped actions. It also
freezes those rendered values before the final Compose invocation.

Never roll storage lifecycle files back by checking out a release older than
this wrapper. In particular, the pre-isolation `Makefile` implemented `clean`
with `docker compose down -v`, which can delete the live named volume. An
application rollback must retain the current `compose.yaml`, supervised overlay,
`Makefile`, `scripts/compose.sh`, lifecycle lock and SeaweedFS entrypoints. Never
run an old `make clean`, `down -v`, or raw mutating Compose command.

After those missing boundaries are implemented and separately reviewed, use
the fail-closed cutover script during an approved downtime window:

```bash
export COMPOSE_ENV_FILE=/absolute/path/to/production.env
export SEAWEEDFS_CUTOVER_CONFIRM=backup-and-downtime-approved
export SEAWEEDFS_CUTOVER_IMAGE_ID='sha256:<approved 64-hex image ID>'
./scripts/cutover_seaweedfs_topology.sh
```

The destination must be a different physical site/endpoint and its bucket
must enforce Object Lock or an equivalent versioned WORM retention policy;
set `WHOSCORED_BACKUP_DESTINATION_SITE_ID` and
`WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE` only after verifying that policy.
Install `configs/seaweedfs/s3.config.json` as a non-symlink secret owned by the
deployment account with no group/other permissions (for example,
`install -m 600 /secure/source/s3.config.json configs/seaweedfs/s3.config.json`).
The cutover rejects a group/world-readable credential file before contacting
Docker.

Before approving downtime, perform a recent full-warehouse rehearsal with the
same off-host destination and `WHOSCORED_BACKUP_WORKERS` value. The following
commands create an Airflow-readable evidence directory and measure all transfer
phases without mutating the primary warehouse:

```bash
export COMPOSE_ENV_FILE=/absolute/path/to/production.env
export REHEARSAL_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export REHEARSAL_DIR="/opt/airflow/logs/seaweedfs_rehearsal/$REHEARSAL_ID"
./scripts/compose.sh --env-file "$COMPOSE_ENV_FILE" \
  run --rm --no-deps \
  --user 0:0 --entrypoint /bin/sh -e REHEARSAL_DIR airflow-scheduler -euc \
  'install -d -o 50000 -g 0 -m 0770 "$REHEARSAL_DIR"'
./scripts/compose.sh --env-file "$COMPOSE_ENV_FILE" \
  run --rm --no-deps \
  --entrypoint bash -e REHEARSAL_DIR airflow-scheduler -euc '
    source_uri="s3://${ICEBERG_WAREHOUSE}"
    inventory="$REHEARSAL_DIR/inventory.json"
    python /opt/airflow/scripts/whoscored_raw_backup.py inventory \
      --store-uri "$source_uri" --output "$inventory" \
      --workers "$WHOSCORED_BACKUP_WORKERS" \
      | tee "$REHEARSAL_DIR/inventory-metrics.json"
    python /opt/airflow/scripts/whoscored_raw_backup.py backup \
      --source-uri "$source_uri" \
      --destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --inventory "$inventory" --apply \
      --workers "$WHOSCORED_BACKUP_WORKERS" \
      | tee "$REHEARSAL_DIR/backup-metrics.json"
    python /opt/airflow/scripts/whoscored_raw_backup.py verify-backup \
      --store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --inventory "$inventory" --workers "$WHOSCORED_BACKUP_WORKERS" \
      | tee "$REHEARSAL_DIR/verify-backup-metrics.json"
    export WHOSCORED_BACKUP_RESTORE_S3_ENDPOINT="$WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT"
    export WHOSCORED_BACKUP_RESTORE_S3_SCHEME="$WHOSCORED_BACKUP_SOURCE_S3_SCHEME"
    export WHOSCORED_BACKUP_RESTORE_S3_REGION="$WHOSCORED_BACKUP_SOURCE_S3_REGION"
    export WHOSCORED_BACKUP_RESTORE_S3_ACCESS_KEY="${WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY:-${WHOSCORED_RAW_S3_ACCESS_KEY:-$S3_ACCESS_KEY}}"
    export WHOSCORED_BACKUP_RESTORE_S3_SECRET_KEY="${WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY:-${WHOSCORED_RAW_S3_SECRET_KEY:-$S3_SECRET_KEY}}"
    python /opt/airflow/scripts/whoscored_raw_backup.py verify-restore \
      --store-uri "$source_uri" --inventory "$inventory" \
      --workers "$WHOSCORED_BACKUP_WORKERS" \
      | tee "$REHEARSAL_DIR/verify-source-metrics.json"
  '
```

Copy the `mib_per_second` field from `inventory-metrics.json` to
`SEAWEEDFS_CUTOVER_INVENTORY_MIBPS`, the same field from
`backup-metrics.json` to `SEAWEEDFS_CUTOVER_BACKUP_MIBPS`, and the lower of
the two verify values to `SEAWEEDFS_CUTOVER_VERIFY_MIBPS`. Set
`SEAWEEDFS_CUTOVER_REHEARSAL_INVENTORY` to
`$REHEARSAL_DIR/inventory.json` (the container path), and retain all five JSON
files with the change record.

Measure fixed overhead on an isolated clone of the production volumes and
service set, never on the primary host. Seed the clone's three measured rates,
use a deliberately non-gating `SEAWEEDFS_CUTOVER_MAX_DOWNTIME_SECONDS=86400`
and `SEAWEEDFS_CUTOVER_FIXED_OVERHEAD_SECONDS=0`, then time the complete
cutover script:

```bash
/usr/bin/time -f '%e' -o full-cutover-wall-seconds.txt \
  ./scripts/cutover_seaweedfs_topology.sh
```

Use the largest observed full wall time as
`SEAWEEDFS_CUTOVER_FIXED_OVERHEAD_SECONDS`. This deliberately double-counts
the transfer phases in the final projection and is therefore conservative.
The clone must begin from the legacy-mini snapshot for every repetition. The
script validates inventory source/age, current warehouse object/byte drift and
projected downtime before it stops any writer; missing, stale, wrong-source,
or over-budget evidence aborts.

Before it opens any new storage plane, the script:

1. validates the S3 credential file without printing secrets;
2. validates a recent measured full-warehouse capacity rehearsal;
3. stops every credential-bearing Airflow, Trino, Lakekeeper, Superset,
   OpenMetadata and Jupyter entry point;
4. recreates the legacy gateway with only the dedicated bucket Read/List
   backup identity and proves both write denial and admin-credential rejection;
5. inventories the complete S3 bucket, copies it to the configured off-host
   content-addressed backup, and verifies its completion marker;
6. stops the legacy `weed mini` container;
7. starts master, volume, filer and S3 in order while S3 remains Read/List-only;
8. reads and hashes every recovery-cut object through the new S3 gateway;
9. promotes protected state from verification-pending to supervised, restores
   the full S3 identity set and restarts only previously running writers.

Any failure leaves writers stopped. Do not bypass that state. The following is
the reviewed recovery design draft, not an executable production procedure:
`transition_seaweedfs_recovery_state.py` is code-owned disabled. It may be
enabled only after the transition retains protected verification-pending state
and a server-side Read/List fence through `SHOW SCHEMAS FROM iceberg`, the
frozen 25-dataset DQ gate and one direct-only application canary. It assumes the
PostgreSQL volume containing Lakekeeper catalog state is intact. A lost catalog
database requires its separate platform database recovery and writers must
remain stopped.

Acquire the lifecycle lock before the first stop command and hold it through
the protected-state transition. Raw Compose is permitted only inside this
locked recovery shell. The recovery S3 gateway uses a newly generated,
restoration-only identity; the platform credentials are deliberately absent.
Preserve the failed volume forever:

```bash
set -Eeuo pipefail
: "${COMPOSE_ENV_FILE:?absolute production env file is required}"
: "${ICEBERG_WAREHOUSE:?warehouse bucket name is required}"
export SEAWEEDFS_TOPOLOGY_STATE_FILE=/var/lib/data-platform-football/seaweedfs-topology.mode
export SEAWEEDFS_CUTOVER_LOCK_FILE=/var/lib/data-platform-football/seaweedfs-topology.lock
readonly SEAWEEDFS_COMPOSE_PROJECT_NAME=data-platform
readonly SEAWEEDFS_S3_PROXY_IMAGE='caddy:2.10-alpine@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d'
readonly SEAWEEDFS_S3_PROXY_CONFIG_SHA256=1f9cef7299e52272ee92ecaa4b58a413387291fd0d41626baf60da9768604a14
assert_host_protected_directory_chain() {
  local current="$1" mode owner
  while true; do
    test -d "$current" && test ! -L "$current" || {
      echo "SeaweedFS S3 proxy config directory chain is invalid" >&2
      return 2
    }
    mode="$(stat -c '%a' "$current")"
    owner="$(stat -c '%u' "$current")"
    [[ "$mode" =~ ^[0-7]{3,4}$ ]] &&
      [[ "$owner" == 0 || "$owner" == "$(id -u)" ]] &&
      { (( (8#$mode & 022) == 0 )) ||
        { [[ "$owner" == 0 ]] && (( (8#$mode & 01000) != 0 )); }; } || {
        echo "SeaweedFS S3 proxy config directory chain is not host-protected" >&2
        return 2
      }
    [[ "$current" == / ]] && break
    current="$(dirname "$current")"
  done
}
assert_s3_proxy_config_boundary() {
  local path mode owner digest
  path="$PWD/configs/seaweedfs/S3ProxyCaddyfile"
  test -f "$path" && test ! -L "$path" || {
    echo "SeaweedFS S3 proxy config must be a regular non-symlink file" >&2
    return 2
  }
  assert_host_protected_directory_chain "$(dirname "$path")"
  mode="$(stat -c '%a' "$path")"
  owner="$(stat -c '%u' "$path")"
  [[ "$mode" =~ ^[0-7]{3,4}$ ]] &&
    (( (8#$mode & 022) == 0 )) &&
    [[ "$owner" == 0 || "$owner" == "$(id -u)" ]] || {
      echo "SeaweedFS S3 proxy config is not host-protected" >&2
      return 2
    }
  digest="$(sha256sum "$path")"
  test "${digest%% *}" = "$SEAWEEDFS_S3_PROXY_CONFIG_SHA256" || {
    echo "SeaweedFS S3 proxy config differs from the reviewed boundary" >&2
    return 2
  }
}
validate_s3_proxy_rendered_boundary() {
  local compose_name="$1" rendered
  local -n compose_ref="$compose_name"
  assert_s3_proxy_config_boundary
  rendered="$("${compose_ref[@]}" config --format json)"
  if ! python3 -c '
import json, sys
model = json.load(sys.stdin)
service = model.get("services", {}).get("seaweedfs-s3-proxy", {})
mounts = service.get("volumes", [])
expected_command = [
    "caddy", "run", "--config", "/etc/caddy/S3ProxyCaddyfile",
    "--adapter", "caddyfile",
]
valid_mount = (
    len(mounts) == 1
    and mounts[0].get("type") == "bind"
    and mounts[0].get("source") == sys.argv[3]
    and mounts[0].get("target") == "/etc/caddy/S3ProxyCaddyfile"
    and mounts[0].get("read_only") is True
)
if not (
    model.get("name") == sys.argv[1]
    and service.get("image") == sys.argv[2]
    and service.get("command") == expected_command
    and service.get("user") == "65534:65534"
    and service.get("read_only") is True
    and set(service.get("networks", {})) == {"seaweedfs-control", "storage"}
    and valid_mount
):
    raise SystemExit("rendered SeaweedFS S3 proxy differs from the pinned boundary")
' "$SEAWEEDFS_COMPOSE_PROJECT_NAME" "$SEAWEEDFS_S3_PROXY_IMAGE" \
    "$PWD/configs/seaweedfs/S3ProxyCaddyfile" <<<"$rendered"; then
    return 2
  fi
}
assert_seaweedfs_control_network() {
  local inspection
  inspection="$(docker network inspect dp-seaweedfs-control)" || {
    echo "Required SeaweedFS control network is missing" >&2
    return 2
  }
  python3 scripts/audit_seaweedfs_control_network.py \
    dp-seaweedfs-control seaweedfs-master seaweedfs-volume seaweedfs-filer \
    seaweedfs seaweedfs-s3-proxy <<<"$inspection"
}
assert_s3_proxy_runtime() {
  local compose_name="$1" expected_volume="$2" rendered live
  local -n compose_ref="$compose_name"
  validate_s3_proxy_rendered_boundary "$compose_name"
  rendered="$("${compose_ref[@]}" config --format json)"
  live="$(docker inspect seaweedfs-s3-proxy)" || {
    echo "Required SeaweedFS S3 proxy is missing" >&2
    return 2
  }
  {
    printf '%s\n' "$rendered" | base64 --wrap=0
    printf '\n'
    printf '%s\n' "$live" | base64 --wrap=0
    printf '\n'
  } | python3 scripts/audit_seaweedfs_runtime_container.py \
    seaweedfs-s3-proxy "$SEAWEEDFS_S3_PROXY_IMAGE_ID" "$expected_volume"
}
assert_s3_proxy_config_boundary
SEAWEEDFS_S3_PROXY_IMAGE_ID="$(docker image inspect --format '{{.Id}}' \
  "$SEAWEEDFS_S3_PROXY_IMAGE")"
readonly SEAWEEDFS_S3_PROXY_IMAGE_ID
[[ "$SEAWEEDFS_S3_PROXY_IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]] || {
  echo "Pinned SeaweedFS S3 proxy image is unavailable" >&2
  exit 2
}
source scripts/seaweedfs_lifecycle_lock.sh
acquire_seaweedfs_lifecycle_lock
recovery_complete=0
recovery_quiesce_targets=(
  seaweedfs-s3-proxy seaweedfs-s3 seaweedfs
  airflow-init airflow-scheduler airflow-webserver
  lakekeeper-migrate lakekeeper trino
  superset superset-worker superset-beat
  openmetadata-migrate openmetadata-server openmetadata-ingestion jupyterhub
  seaweedfs-filer seaweedfs-volume seaweedfs-master
)
recovery_fail_closed() {
  local status=$? container running_names quiesce_failed=0
  if ((recovery_complete == 0)); then
    ((status != 0)) || status=125
    for container in "${recovery_quiesce_targets[@]}"; do
      docker stop --time 120 "$container" >/dev/null ||
        echo "Recovery emergency stop attempt failed for $container" >&2
    done
    if ! running_names="$(docker ps --format '{{.Names}}')"; then
      echo "Cannot enumerate containers after recovery emergency stop" >&2
      quiesce_failed=1
    else
      for container in "${recovery_quiesce_targets[@]}"; do
        if grep -Fxq "$container" <<<"$running_names"; then
          echo "Recovery target remains running: $container" >&2
          quiesce_failed=1
        fi
      done
    fi
    if ((quiesce_failed)); then
      echo "EMERGENCY: recovery quiescence could not be confirmed" >&2
      status=125
    fi
  fi
  rm -f "${RECOVERY_COMPOSE_OVERLAY:-}" "${RECOVERY_S3_CONFIG:-}"
  trap - EXIT
  exit "$status"
}
trap recovery_fail_closed EXIT
export FAILED_SEAWEEDFS_DATA_VOLUME_NAME="$(python3 -c \
  'import json,os; print(json.load(open(os.environ["SEAWEEDFS_TOPOLOGY_STATE_FILE"]))["volume_name"])')"
export SEAWEEDFS_CUTOVER_IMAGE_ID="$(python3 -c \
  'import json,os; print(json.load(open(os.environ["SEAWEEDFS_TOPOLOGY_STATE_FILE"]))["image_id"])')"
export SEAWEEDFS_VOLUME_SIZE_LIMIT_MB="$(python3 -c \
  'import json,os; print(json.load(open(os.environ["SEAWEEDFS_TOPOLOGY_STATE_FILE"]))["volume_size_limit_mb"])')"
export SEAWEEDFS_EXPECTED_INVENTORY_SHA256="$(python3 -c \
  'import json,os; print(json.load(open(os.environ["SEAWEEDFS_TOPOLOGY_STATE_FILE"]))["inventory_sha256"])')"
export SEAWEEDFS_DATA_VOLUME_NAME="$FAILED_SEAWEEDFS_DATA_VOLUME_NAME"
for container in airflow-init airflow-scheduler airflow-webserver \
  lakekeeper-migrate lakekeeper trino superset superset-worker superset-beat \
  openmetadata-migrate openmetadata-server openmetadata-ingestion jupyterhub \
  seaweedfs-s3-proxy seaweedfs seaweedfs-filer seaweedfs-volume seaweedfs-master; do
  if docker inspect "$container" >/dev/null 2>&1; then
    test "$(docker inspect --format \
      '{{index .Config.Labels "com.docker.compose.project"}}' "$container")" = \
      "$SEAWEEDFS_COMPOSE_PROJECT_NAME"
  fi
done
current_compose=(docker compose -p "$SEAWEEDFS_COMPOSE_PROJECT_NAME" \
  --env-file "$COMPOSE_ENV_FILE" \
  -f compose.yaml -f compose.seaweedfs-supervised.yaml)
assert_seaweedfs_control_network
validate_s3_proxy_rendered_boundary current_compose
if docker inspect seaweedfs-s3-proxy >/dev/null 2>&1; then
  assert_s3_proxy_runtime current_compose "$FAILED_SEAWEEDFS_DATA_VOLUME_NAME"
fi
"${current_compose[@]}" stop --timeout 120 \
  airflow-init airflow-scheduler airflow-webserver \
  lakekeeper-migrate lakekeeper trino \
  superset superset-worker superset-beat \
  openmetadata-migrate openmetadata-server openmetadata-ingestion jupyterhub \
  seaweedfs-s3-proxy seaweedfs seaweedfs-filer seaweedfs-volume seaweedfs-master
for container in airflow-init airflow-scheduler airflow-webserver \
  lakekeeper-migrate lakekeeper trino superset superset-worker superset-beat \
  openmetadata-migrate openmetadata-server openmetadata-ingestion jupyterhub \
  seaweedfs-s3-proxy seaweedfs seaweedfs-filer seaweedfs-volume seaweedfs-master; do
  test "$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)" != true
done
export RECOVERY_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export RECOVERY_DIR="$PWD/logs/seaweedfs_recovery/$RECOVERY_ID"
sudo install -d -o "$USER" -g 0 -m 0770 "$RECOVERY_DIR"
test -w "$RECOVERY_DIR"
docker volume inspect "$FAILED_SEAWEEDFS_DATA_VOLUME_NAME" \
  > "$RECOVERY_DIR/failed-volume.json"
export SEAWEEDFS_DATA_VOLUME_NAME="seaweedfs_recovery_${RECOVERY_ID}"
export SEAWEEDFS_ALLOW_FRESH_TRANSITION=restore-empty-volume-v1
if docker volume inspect "$SEAWEEDFS_DATA_VOLUME_NAME" >/dev/null 2>&1; then
  echo "Refusing to reuse a pre-existing recovery volume" >&2
  exit 2
fi
test "$(docker volume create "$SEAWEEDFS_DATA_VOLUME_NAME")" = \
  "$SEAWEEDFS_DATA_VOLUME_NAME"
docker run --rm --pull never --read-only --network none \
  --entrypoint /bin/sh -v "$SEAWEEDFS_DATA_VOLUME_NAME:/data:ro" \
  "$SEAWEEDFS_CUTOVER_IMAGE_ID" -euc \
  'test -z "$(find /data -mindepth 1 -maxdepth 1 -print -quit)"'
export RECOVERY_S3_ACCESS_KEY="recovery-$(openssl rand -hex 16)"
export RECOVERY_S3_SECRET_KEY="$(openssl rand -hex 32)"
export RECOVERY_S3_CONFIG="$RECOVERY_DIR/s3.recovery.json"
python3 - <<'PY'
import json, os
payload = {"identities": [{
    "name": "exclusive-volume-restore",
    "credentials": [{
        "accessKey": os.environ["RECOVERY_S3_ACCESS_KEY"],
        "secretKey": os.environ["RECOVERY_S3_SECRET_KEY"],
    }],
    "actions": ["Admin"],
}]}
with open(os.environ["RECOVERY_S3_CONFIG"], "x", encoding="utf-8") as handle:
    json.dump(payload, handle, separators=(",", ":"))
PY
chmod 0600 "$RECOVERY_S3_CONFIG"
export RECOVERY_COMPOSE_OVERLAY="$RECOVERY_DIR/compose.recovery.yaml"
cat >"$RECOVERY_COMPOSE_OVERLAY" <<EOF
name: $SEAWEEDFS_COMPOSE_PROJECT_NAME
services:
  seaweedfs:
    volumes: !override
      - $PWD/scripts/seaweedfs_legacy_entrypoint.sh:/usr/local/bin/seaweedfs-legacy-entrypoint:ro
      - $RECOVERY_S3_CONFIG:/etc/seaweedfs/s3.config.json:ro
EOF
chmod 0600 "$RECOVERY_COMPOSE_OVERLAY"
export S3_ACCESS_KEY="$RECOVERY_S3_ACCESS_KEY"
export S3_SECRET_KEY="$RECOVERY_S3_SECRET_KEY"
```

Start only the supervised planes with the explicit new external volume. Keep
all writers stopped and select one full-warehouse inventory by its exact source
root. Store the fetched inventory under the host-mounted `logs` directory, then
restore and run both a full byte/hash verification and an exact second inventory
comparison, which rejects unexpected objects:

```bash
recovery_compose=(docker compose -p "$SEAWEEDFS_COMPOSE_PROJECT_NAME" \
  --env-file "$COMPOSE_ENV_FILE" \
  -f compose.yaml -f compose.seaweedfs-supervised.yaml \
  -f "$RECOVERY_COMPOSE_OVERLAY")
assert_seaweedfs_control_network
export RECOVERY_CONTAINER_DIR="/opt/airflow/logs/seaweedfs_recovery/$RECOVERY_ID"
"${recovery_compose[@]}" run --rm --no-deps --user 50000:0 \
  -e RECOVERY_CONTAINER_DIR --entrypoint /bin/sh airflow-scheduler \
  -euc 'test -w "$RECOVERY_CONTAINER_DIR"'
recovery_wait_healthy() {
  local container="$1" status attempts=60
  while ((attempts--)); do
    status="$(docker inspect --format \
      '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
      "$container" 2>/dev/null || true)"
    [[ "$status" == healthy || "$status" == running ]] && return 0
    sleep 2
  done
  return 1
}
for service in seaweedfs-master seaweedfs-volume seaweedfs-filer; do
  "${recovery_compose[@]}" up -d --no-deps "$service"
  recovery_wait_healthy "$service"
done
"${recovery_compose[@]}" up -d --no-deps seaweedfs
recovery_wait_healthy seaweedfs
assert_s3_proxy_config_boundary
"${recovery_compose[@]}" up -d --no-deps seaweedfs-s3-proxy
recovery_wait_healthy seaweedfs-s3-proxy
assert_s3_proxy_runtime recovery_compose "$SEAWEEDFS_DATA_VOLUME_NAME"
assert_seaweedfs_control_network
"${recovery_compose[@]}" run --rm --no-deps --entrypoint bash \
  airflow-scheduler -euc '
    python /opt/airflow/scripts/whoscored_raw_backup.py list-inventories \
      --store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --expected-source-uri "s3://${ICEBERG_WAREHOUSE}" --limit 20
  '
export RECOVERY_INVENTORY_KEY=backup-inventories/EXACT_FULL_WAREHOUSE_KEY.json
export RECOVERY_INVENTORY="/opt/airflow/logs/seaweedfs_recovery/${RECOVERY_ID}/inventory.json"
"${recovery_compose[@]}" run --rm --no-deps \
  -e RECOVERY_INVENTORY_KEY -e RECOVERY_INVENTORY \
  --entrypoint bash airflow-scheduler -euc '
    source_uri="s3://${ICEBERG_WAREHOUSE}"
    export WHOSCORED_BACKUP_RESTORE_S3_ENDPOINT="$S3_ENDPOINT"
    export WHOSCORED_BACKUP_RESTORE_S3_SCHEME="${S3_SCHEME:-http}"
    export WHOSCORED_BACKUP_RESTORE_S3_REGION="${S3_REGION:-us-east-1}"
    export WHOSCORED_BACKUP_RESTORE_S3_ACCESS_KEY="${S3_ACCESS_KEY:?exclusive recovery access key is required}"
    export WHOSCORED_BACKUP_RESTORE_S3_SECRET_KEY="${S3_SECRET_KEY:?exclusive recovery secret key is required}"
    python /opt/airflow/scripts/whoscored_raw_backup.py fetch-inventory \
      --store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --inventory-key "$RECOVERY_INVENTORY_KEY" \
      --expected-source-uri "$source_uri" --output "$RECOVERY_INVENTORY"
    python /opt/airflow/scripts/whoscored_raw_backup.py restore \
      --backup-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --restore-uri "$source_uri" --inventory "$RECOVERY_INVENTORY" \
      --apply --create-bucket --workers "$WHOSCORED_BACKUP_WORKERS"
    python /opt/airflow/scripts/whoscored_raw_backup.py verify-restore \
      --store-uri "$source_uri" --inventory "$RECOVERY_INVENTORY" \
      --workers "$WHOSCORED_BACKUP_WORKERS"
    actual="${RECOVERY_INVENTORY}.actual"
    python /opt/airflow/scripts/whoscored_raw_backup.py inventory \
      --store-uri "$source_uri" --output "$actual" \
      --workers "$WHOSCORED_BACKUP_WORKERS"
    python -c "import json,sys; expected,actual=(json.load(open(path,encoding=\"utf-8\")) for path in sys.argv[1:]); fields=(\"object_count\",\"total_bytes\",\"objects_sha256\"); all(expected[field] == actual[field] for field in fields) or sys.exit(\"restored volume contains missing, changed or unexpected objects\")" \
      "$RECOVERY_INVENTORY" "$actual"
  '
export RECOVERY_INVENTORY_HOST="logs/seaweedfs_recovery/${RECOVERY_ID}/inventory.json"
export RECOVERY_INVENTORY_SHA256="$(python3 -c \
  'import json,os; print(json.load(open(os.environ["RECOVERY_INVENTORY_HOST"]))["inventory_sha256"])')"
export SEAWEEDFS_EXPECTED_INVENTORY_SHA256="$RECOVERY_INVENTORY_SHA256"
"${recovery_compose[@]}" run --rm --no-deps --entrypoint /bin/sh \
  seaweedfs-master -euc \
  'printf "%s\n" "$1" > /data/.supervised-topology-cutover-approved; sync' \
  _ "full-bucket-inventory-v2:${RECOVERY_INVENTORY_SHA256}"
"${recovery_compose[@]}" stop --timeout 120 \
  seaweedfs-s3-proxy seaweedfs seaweedfs-filer seaweedfs-volume seaweedfs-master
python3 scripts/transition_seaweedfs_recovery_state.py \
  --state-file "$SEAWEEDFS_TOPOLOGY_STATE_FILE" \
  --lock-file "$SEAWEEDFS_CUTOVER_LOCK_FILE" --lock-fd 9 \
  --volume-name "$SEAWEEDFS_DATA_VOLUME_NAME" \
  --image-id "$SEAWEEDFS_CUTOVER_IMAGE_ID" \
  --inventory-file "$RECOVERY_INVENTORY_HOST" \
  --inventory-sha256 "$RECOVERY_INVENTORY_SHA256" \
  --expected-source-uri "s3://${ICEBERG_WAREHOUSE}"
rm -f "$RECOVERY_COMPOSE_OVERLAY" "$RECOVERY_S3_CONFIG"
unset SEAWEEDFS_ALLOW_FRESH_TRANSITION
unset S3_ACCESS_KEY S3_SECRET_KEY RECOVERY_S3_ACCESS_KEY RECOVERY_S3_SECRET_KEY
final_compose=(docker compose -p "$SEAWEEDFS_COMPOSE_PROJECT_NAME" \
  --env-file "$COMPOSE_ENV_FILE" \
  -f compose.yaml -f compose.seaweedfs-supervised.yaml)
assert_seaweedfs_control_network
for service in seaweedfs-master seaweedfs-volume seaweedfs-filer; do
  "${final_compose[@]}" up -d --no-deps "$service"
  recovery_wait_healthy "$service"
done
"${final_compose[@]}" up -d --no-deps seaweedfs
recovery_wait_healthy seaweedfs
assert_s3_proxy_config_boundary
"${final_compose[@]}" up -d --no-deps seaweedfs-s3-proxy
recovery_wait_healthy seaweedfs-s3-proxy
assert_s3_proxy_runtime final_compose "$SEAWEEDFS_DATA_VOLUME_NAME"
assert_seaweedfs_control_network
test "$(docker inspect --format '{{.State.Running}}' seaweedfs-s3-proxy)" = true
test "$(docker inspect --format '{{.State.Health.Status}}' seaweedfs-s3-proxy)" = healthy
recovery_complete=1
trap - EXIT
exec 9<&-
```

In the current release the transition command always aborts before Docker or
state mutation, and `set -e` prevents every following unfence/start command in
the draft block. Do not run those following commands manually. A future
implementation must reject running writers/storage and wrong
image/volume/marker/inventory evidence, retain pending state plus Read/List
fencing while only catalog/query services are validated, and promote/unfence
only after `SHOW SCHEMAS FROM iceberg`, the frozen 25-dataset DQ gate and one
direct-only canary pass. Before either supervised gate can be enabled,
independently review every raw mutating Compose command that remains inside the
lifecycle-locked cutover and recovery drafts; ordinary operations and active
runbooks must use `scripts/compose.sh` or a safe Make target. The private
control-plane topology is already rendered and runtime-attested: master,
volume, filer and the private S3 gateway attach only to the internal bridge,
while the sandboxed HTTP proxy is the sole bridge to `dp-storage`. Remaining
pre-enable blockers are the external WORM receipt, resumable
application/catalog DQ, exact cutover/recovery runtime-identity adoption and an
independent negative-path rehearsal—not a missing network split.
