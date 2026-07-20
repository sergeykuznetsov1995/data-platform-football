#!/usr/bin/env bash
# Quiesced, backup-gated migration from `weed mini` to supervised planes.
set -Eeuo pipefail

# The irreversible phase stays code-owned disabled until every external
# boundary exists: independently verified off-host WORM retention; a resumable
# application/catalog DQ attestation that keeps the server-side Read/List fence
# active until promotion; an independently reviewed exact runtime adoption;
# and repository lifecycle/rollback audit. Environment
# variables cannot enable this switch.
readonly SEAWEEDFS_SUPERVISED_CUTOVER_AVAILABLE=false
readonly SEAWEEDFS_IMAGE="chrislusf/seaweedfs:4.36@sha256:800b2115c63236e8bd0e5d572dc25dd493dc2feed08b54a2a269dc0101c9d94a"
readonly SEAWEEDFS_S3_PROXY_IMAGE="caddy:2.10-alpine@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d"
readonly SEAWEEDFS_S3_PROXY_CONFIG_SHA256="1f9cef7299e52272ee92ecaa4b58a413387291fd0d41626baf60da9768604a14"
readonly SEAWEEDFS_COMPOSE_PROJECT_NAME="data-platform"

assert_host_protected_directory_chain() {
  local current path_mode path_owner
  current="${1}"
  while true; do
    if [[ ! -d "${current}" ]] || [[ -L "${current}" ]] ||
       ! path_mode="$(stat -c '%a' "${current}")" ||
       ! path_owner="$(stat -c '%u' "${current}")"; then
      echo "SeaweedFS S3 proxy config directory chain is invalid" >&2
      exit 2
    fi
    if [[ ! "${path_mode}" =~ ^[0-7]{3,4}$ ]] ||
       [[ "${path_owner}" != "0" && "${path_owner}" != "$(id -u)" ]] ||
       { (( (8#${path_mode} & 022) != 0 )) &&
         { [[ "${path_owner}" != "0" ]] ||
           (( (8#${path_mode} & 01000) == 0 )); }; }; then
      echo "SeaweedFS S3 proxy config directory chain is not host-protected" >&2
      exit 2
    fi
    [[ "${current}" == / ]] && break
    current="$(dirname "${current}")"
  done
}

assert_s3_proxy_config_boundary() {
  local config_path config_mode config_owner config_sha256
  config_path="${PWD}/configs/seaweedfs/S3ProxyCaddyfile"
  if [[ ! -f "${config_path}" ]] || [[ -L "${config_path}" ]]; then
    echo "SeaweedFS S3 proxy config must be a regular non-symlink file" >&2
    exit 2
  fi
  assert_host_protected_directory_chain "$(dirname "${config_path}")"
  if ! config_mode="$(stat -c '%a' "${config_path}")" ||
     ! config_owner="$(stat -c '%u' "${config_path}")"; then
    echo "Cannot inspect the SeaweedFS S3 proxy config boundary" >&2
    exit 2
  fi
  if [[ ! "${config_mode}" =~ ^[0-7]{3,4}$ ]] ||
     (( (8#${config_mode} & 022) != 0 )) ||
     [[ "${config_owner}" != "0" && "${config_owner}" != "$(id -u)" ]]; then
    echo "SeaweedFS S3 proxy config is not host-protected" >&2
    exit 2
  fi
  if ! config_sha256="$(sha256sum "${config_path}")"; then
    echo "Cannot hash the SeaweedFS S3 proxy config boundary" >&2
    exit 2
  fi
  config_sha256="${config_sha256%% *}"
  if [[ "${config_sha256}" != "${SEAWEEDFS_S3_PROXY_CONFIG_SHA256}" ]]; then
    echo "SeaweedFS S3 proxy config differs from the reviewed boundary" >&2
    exit 2
  fi
}

assert_compose_project_identity() {
  local container expected_service required labels
  container="${1}"
  expected_service="${2}"
  required="${3}"
  if ! docker inspect "${container}" >/dev/null 2>&1; then
    if [[ "${required}" == required ]]; then
      echo "Required Compose container ${container} is missing" >&2
      exit 2
    fi
    return 0
  fi
  if ! labels="$(
    docker inspect --format \
      '{{index .Config.Labels "com.docker.compose.project"}}\t{{index .Config.Labels "com.docker.compose.service"}}' \
      "${container}"
  )" || [[ "${labels}" != "${SEAWEEDFS_COMPOSE_PROJECT_NAME}"$'\t'"${expected_service}" ]]; then
    echo "Container ${container} is not the reviewed ${SEAWEEDFS_COMPOSE_PROJECT_NAME}/${expected_service} Compose identity" >&2
    exit 2
  fi
}

assert_no_storage_writer_oneoffs() {
  local oneoffs container_id service
  if ! oneoffs="$(
    docker ps --filter label=com.docker.compose.oneoff=True \
      --format '{{.ID}}\t{{.Label "com.docker.compose.service"}}'
  )"; then
    echo "Cannot enumerate running Compose one-off containers" >&2
    exit 2
  fi
  while IFS=$'\t' read -r container_id service; do
    [[ -n "${container_id}" ]] || continue
    if [[ ! "${container_id}" =~ ^[0-9a-f]{12,64}$ ]] ||
       [[ ! "${service}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
      echo "Cannot validate a running Compose one-off container" >&2
      exit 2
    fi
    case "${service}" in
      airflow-init|airflow-scheduler|airflow-webserver|lakekeeper-migrate|lakekeeper|trino|superset|superset-worker|superset-beat|openmetadata-migrate|openmetadata-server|openmetadata-ingestion|jupyterhub|seaweedfs|seaweedfs-*)
        echo "Running one-off writer ${container_id} (${service}) blocks the SeaweedFS cutover" >&2
        exit 2
        ;;
    esac
  done <<<"${oneoffs}"
}

assert_exact_legacy_volume_consumers() {
  local volume consumers container_id container_name
  volume="${1}"
  if ! consumers="$(
    docker ps --all --filter "volume=${volume}" --format '{{.ID}}\t{{.Names}}'
  )"; then
    echo "Cannot enumerate legacy SeaweedFS volume consumers" >&2
    exit 2
  fi
  while IFS=$'\t' read -r container_id container_name; do
    [[ -n "${container_id}" ]] || continue
    if [[ ! "${container_id}" =~ ^[0-9a-f]{12,64}$ ]] ||
       [[ "${container_name}" != seaweedfs ]]; then
      echo "Unreviewed container ${container_id} (${container_name}) uses the legacy SeaweedFS volume" >&2
      exit 2
    fi
  done <<<"${consumers}"
}

assert_no_running_volume_consumers() {
  local volume consumers
  volume="${1}"
  if ! consumers="$(
    docker ps --filter "volume=${volume}" --format '{{.ID}}\t{{.Names}}'
  )"; then
    echo "Cannot enumerate running SeaweedFS volume consumers" >&2
    exit 2
  fi
  if [[ -n "${consumers}" ]]; then
    echo "A container still has the protected SeaweedFS volume open: ${consumers}" >&2
    exit 2
  fi
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=seaweedfs_lifecycle_lock.sh
source "${script_dir}/seaweedfs_lifecycle_lock.sh"
acquire_seaweedfs_lifecycle_lock

if [[ "${SEAWEEDFS_CUTOVER_CONFIRM:-}" != "backup-and-downtime-approved" ]]; then
  echo "Set SEAWEEDFS_CUTOVER_CONFIRM=backup-and-downtime-approved" >&2
  exit 2
fi
preflight_s3_config="${SEAWEEDFS_S3_CONFIG_FILE:-${PWD}/configs/seaweedfs/s3.config.json}"
if [[ "${preflight_s3_config}" != /* ]] ||
   [[ ! -f "${preflight_s3_config}" ]] ||
   [[ -L "${preflight_s3_config}" ]]; then
  echo "SEAWEEDFS_S3_CONFIG_FILE must be an absolute regular secret file" >&2
  exit 2
fi
config_mode="$(stat -c '%a' "${preflight_s3_config}")"
if (( (8#${config_mode} & 077) != 0 )); then
  echo "SEAWEEDFS_S3_CONFIG_FILE must not grant group/other permissions" >&2
  exit 2
fi
assert_s3_proxy_config_boundary

supervised_overlay="compose.seaweedfs-supervised.yaml"
topology_state_file="${SEAWEEDFS_TOPOLOGY_STATE_FILE:-/var/lib/data-platform-football/seaweedfs-topology.mode}"
if [[ ! -f "${supervised_overlay}" ]]; then
  echo "${supervised_overlay} is required for the approved topology cutover" >&2
  exit 2
fi
if [[ "${topology_state_file}" != /* ]]; then
  echo "SEAWEEDFS_TOPOLOGY_STATE_FILE must be an absolute host path" >&2
  exit 2
fi
if [[ -e "${topology_state_file}" || -L "${topology_state_file}" ]]; then
  echo "SeaweedFS topology state already exists: ${topology_state_file}" >&2
  exit 2
fi
topology_state_dir="$(dirname "${topology_state_file}")"
if [[ ! -d "${topology_state_dir}" ]] || [[ -L "${topology_state_dir}" ]] ||
   [[ ! -w "${topology_state_dir}" ]]; then
  echo "Pre-provision a writable non-symlink topology state directory: ${topology_state_dir}" >&2
  exit 2
fi
state_dir_mode="$(stat -c '%a' "${topology_state_dir}")"
state_dir_owner="$(stat -c '%u' "${topology_state_dir}")"
if (( (8#${state_dir_mode} & 022) != 0 )) ||
   [[ "${state_dir_owner}" != "0" && "${state_dir_owner}" != "$(id -u)" ]]; then
  echo "Topology state directory is not host-protected" >&2
  exit 2
fi
if [[ "${SEAWEEDFS_SUPERVISED_CUTOVER_AVAILABLE}" != true ]]; then
  echo "Supervised SeaweedFS cutover is code-owned disabled pending WORM, application-DQ, runtime-adoption and recovery audits" >&2
  exit 2
fi
assert_no_storage_writer_oneoffs
topology_state_tmp="${topology_state_file}.prepared-$$"
umask 022
# Allocate and fsync the final state file before the downtime barrier. After
# the final inventory only an in-place rewrite plus atomic rename remains.
python3 - "${topology_state_tmp}" <<'PY'
import os
import sys

with open(sys.argv[1], "xb") as handle:
    handle.write(b" " * 512)
    handle.flush()
    os.fsync(handle.fileno())
directory = os.open(os.path.dirname(sys.argv[1]), os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY
chmod 0644 "${topology_state_tmp}"
trap 'rm -f "${topology_state_tmp}"' EXIT

# The default compose.yaml intentionally keeps the live `weed mini` service.
# Render it first: the supervised overlay requires a value read from the live
# mini.options file and must never invent that on-disk allocation policy.
base_compose=(
  docker compose -p "${SEAWEEDFS_COMPOSE_PROJECT_NAME}" -f compose.yaml
)
if [[ -n "${COMPOSE_ENV_FILE:-}" ]]; then
  base_compose+=(--env-file "${COMPOSE_ENV_FILE}")
fi
compose_version="$("${base_compose[@]}" version --short)"
minimum_compose_version="2.24.4"
if [[ "$(printf '%s\n%s\n' "${minimum_compose_version}" "${compose_version}" | sort -V | head -n1)" != "${minimum_compose_version}" ]]; then
  echo "Docker Compose ${minimum_compose_version}+ is required for !override" >&2
  exit 2
fi
rendered_base_model="$("${base_compose[@]}" config --format json)"
if ! validated_s3_values="$(
  python3 "${script_dir}/validate_seaweedfs_s3_identity_config.py" \
    <<<"${rendered_base_model}"
)"; then
  echo "Rendered SeaweedFS S3 identity boundary failed semantic validation" >&2
  exit 2
fi
IFS='|' read -r encoded_config encoded_admin_access encoded_admin_secret \
  encoded_bucket encoded_raw_access encoded_raw_secret encoded_backup_access \
  encoded_backup_secret validation_version <<<"${validated_s3_values}"
if [[ "${validation_version}" != v1 ]] ||
   [[ "${validated_s3_values}" != *'|'v1 ]]; then
  echo "Cannot parse validated SeaweedFS S3 identity values" >&2
  exit 2
fi
for encoded_value in \
  "${encoded_config}" "${encoded_admin_access}" "${encoded_admin_secret}" \
  "${encoded_bucket}" "${encoded_raw_access}" "${encoded_raw_secret}" \
  "${encoded_backup_access}" "${encoded_backup_secret}"; do
  if [[ ! "${encoded_value}" =~ ^[A-Za-z0-9+/]*={0,2}$ ]]; then
    echo "Cannot parse validated SeaweedFS S3 identity values" >&2
    exit 2
  fi
done
SEAWEEDFS_S3_CONFIG_FILE="$(printf '%s' "${encoded_config}" | base64 --decode)"
S3_ACCESS_KEY="$(printf '%s' "${encoded_admin_access}" | base64 --decode)"
S3_SECRET_KEY="$(printf '%s' "${encoded_admin_secret}" | base64 --decode)"
ICEBERG_WAREHOUSE="$(printf '%s' "${encoded_bucket}" | base64 --decode)"
WHOSCORED_RAW_S3_ACCESS_KEY="$(printf '%s' "${encoded_raw_access}" | base64 --decode)"
WHOSCORED_RAW_S3_SECRET_KEY="$(printf '%s' "${encoded_raw_secret}" | base64 --decode)"
WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY="$(printf '%s' "${encoded_backup_access}" | base64 --decode)"
WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY="$(printf '%s' "${encoded_backup_secret}" | base64 --decode)"
export SEAWEEDFS_S3_CONFIG_FILE S3_ACCESS_KEY S3_SECRET_KEY ICEBERG_WAREHOUSE
export WHOSCORED_RAW_S3_ACCESS_KEY WHOSCORED_RAW_S3_SECRET_KEY
export WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY

writers=(
  airflow-init airflow-scheduler airflow-webserver
  lakekeeper-migrate lakekeeper trino
  superset superset-worker superset-beat
  openmetadata-migrate openmetadata-server openmetadata-ingestion
  jupyterhub
)
for service in seaweedfs seaweedfs-s3 seaweedfs-s3-proxy; do
  assert_compose_project_identity "${service}" "${service}" required
done
for service in "${writers[@]}"; do
  assert_compose_project_identity "${service}" "${service}" optional
done
restart=()
for service in "${writers[@]}"; do
  writer_running="$(
    docker inspect --format '{{.State.Running}}' "${service}" 2>/dev/null || true
  )"
  case "${writer_running}" in
    true)
      case "${service}" in
        airflow-init|lakekeeper-migrate|openmetadata-migrate)
          echo "Transient service ${service} is still running; wait for completion before cutover" >&2
          exit 2
          ;;
      esac
      restart+=("${service}")
      ;;
    false|"") ;;
    *)
      echo "Cannot capture the exact pre-cutover state of ${service}" >&2
      exit 2
      ;;
  esac
done

for service in seaweedfs-master seaweedfs-volume seaweedfs-filer; do
  if docker inspect "${service}" >/dev/null 2>&1; then
    assert_compose_project_identity "${service}" "${service}" optional
    echo "${service} already exists; refusing an overlapping or stale cutover" >&2
    exit 2
  fi
done

old_command="$(docker inspect --format '{{json .Config.Cmd}}' seaweedfs 2>/dev/null || true)"
if [[ "${old_command}" != *mini* ]]; then
  echo "The running seaweedfs container is not the expected legacy weed mini" >&2
  exit 2
fi
rendered_volume="$("${base_compose[@]}" config --format json | python3 -c '
import json, sys
value = json.load(sys.stdin)["volumes"]["seaweedfs_data"].get("name")
if not isinstance(value, str) or not value:
    raise SystemExit("rendered seaweedfs_data volume has no concrete name")
print(value)
')"
live_volume="$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}' seaweedfs 2>/dev/null || true)"
if [[ -z "${live_volume}" ]] || [[ "${live_volume}" != "${rendered_volume}" ]]; then
  echo "Running legacy /data volume differs from rendered seaweedfs_data" >&2
  exit 2
fi
# Shell interpolation has higher precedence than a subsequently replaced
# COMPOSE_ENV_FILE. Freeze the exact audited volume for every long-running
# cutover command and for the persisted topology state.
export SEAWEEDFS_DATA_VOLUME_NAME="${rendered_volume}"
assert_exact_legacy_volume_consumers "${live_volume}"
expected_image_id="${SEAWEEDFS_CUTOVER_IMAGE_ID:-}"
if [[ ! "${expected_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "SEAWEEDFS_CUTOVER_IMAGE_ID=sha256:<64 hex> is required" >&2
  exit 2
fi
live_image_id="$(docker inspect --format '{{.Image}}' seaweedfs 2>/dev/null || true)"
candidate_image_id="$(docker image inspect --format '{{.Id}}' "${SEAWEEDFS_IMAGE}" 2>/dev/null || true)"
if [[ "${live_image_id}" != "${expected_image_id}" ]] ||
   [[ "${candidate_image_id}" != "${expected_image_id}" ]]; then
  echo "Legacy and candidate SeaweedFS images must equal the approved image ID" >&2
  exit 2
fi
live_legacy_model="$(docker inspect seaweedfs)"
if ! {
  printf '%s' "${rendered_base_model}" | base64 --wrap=0
  printf '\n'
  printf '%s' "${live_legacy_model}" | base64 --wrap=0
  printf '\n'
} | python3 "${script_dir}/audit_seaweedfs_runtime_container.py" \
  seaweedfs "${expected_image_id}" "${live_volume}"; then
  echo "The live legacy mini differs from the exact rendered isolated boundary" >&2
  exit 2
fi
control_network_model="$(docker network inspect dp-seaweedfs-control)"
if ! python3 "${script_dir}/audit_seaweedfs_control_network.py" \
  dp-seaweedfs-control seaweedfs seaweedfs-s3 seaweedfs-s3-proxy \
  <<<"${control_network_model}"; then
  echo "The live SeaweedFS control network is not the reviewed internal boundary" >&2
  exit 2
fi
for service in seaweedfs-s3; do
  live_service_model="$(docker inspect "${service}")"
  if ! {
    printf '%s' "${rendered_base_model}" | base64 --wrap=0
    printf '\n'
    printf '%s' "${live_service_model}" | base64 --wrap=0
    printf '\n'
  } | python3 "${script_dir}/audit_seaweedfs_runtime_container.py" \
    "${service}" "${expected_image_id}" "${live_volume}"; then
    echo "The live ${service} container differs from its rendered private boundary" >&2
    exit 2
  fi
done
proxy_image_id="$(
  docker image inspect --format '{{.Id}}' "${SEAWEEDFS_S3_PROXY_IMAGE}" 2>/dev/null || true
)"
if [[ ! "${proxy_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "The pinned SeaweedFS S3 HTTP proxy image is unavailable" >&2
  exit 2
fi
live_proxy_model="$(docker inspect seaweedfs-s3-proxy)"
if ! {
  printf '%s' "${rendered_base_model}" | base64 --wrap=0
  printf '\n'
  printf '%s' "${live_proxy_model}" | base64 --wrap=0
  printf '\n'
} | python3 "${script_dir}/audit_seaweedfs_runtime_container.py" \
  seaweedfs-s3-proxy "${proxy_image_id}" "${live_volume}"; then
  echo "The live SeaweedFS S3 HTTP proxy differs from its rendered boundary" >&2
  exit 2
fi

volume_size_limit_mb="$(docker run --rm --read-only --network none \
  --entrypoint /bin/sh -v "${live_volume}:/data:ro" "${expected_image_id}" \
  -euc '
    test ! -L /data/mini.options
    test -f /data/mini.options
    test "$(grep -c "master[.]volumeSizeLimitMB" /data/mini.options || true)" = 1
    awk '\''
      /^master[.]volumeSizeLimitMB=/ {
        count += 1
        value = substr($0, index($0, "=") + 1)
        if (value !~ /^[1-9][0-9]*$/) exit 42
      }
      END {
        if (count != 1) exit 43
        print value
      }
    '\'' /data/mini.options
  ')"
if [[ ! "${volume_size_limit_mb}" =~ ^[1-9][0-9]*$ ]] ||
   ((volume_size_limit_mb > 1048576)); then
  echo "Legacy mini.options has an invalid volume size limit" >&2
  exit 2
fi
export SEAWEEDFS_VOLUME_SIZE_LIMIT_MB="${volume_size_limit_mb}"

# Only after the exact live allocation policy is pinned may the supervised
# model be rendered or any of its planes be started.
compose=(
  docker compose -p "${SEAWEEDFS_COMPOSE_PROJECT_NAME}"
  -f compose.yaml -f "${supervised_overlay}"
)
if [[ -n "${COMPOSE_ENV_FILE:-}" ]]; then
  compose+=(--env-file "${COMPOSE_ENV_FILE}")
fi
"${compose[@]}" config --quiet

"${compose[@]}" run --rm --no-deps --user 0:0 \
  -v "${SEAWEEDFS_S3_CONFIG_FILE}:/tmp/s3.config.json:ro" \
  --entrypoint python airflow-scheduler - <<'PY'
import json
import os
from pathlib import Path

payload = json.loads(Path("/tmp/s3.config.json").read_text())
pairs = {
    (item.get("accessKey"), item.get("secretKey"))
    for identity in payload.get("identities", [])
    for item in identity.get("credentials", [])
}
required = {
    (os.environ.get("S3_ACCESS_KEY"), os.environ.get("S3_SECRET_KEY")),
}
if any(not all(pair) for pair in required):
    raise SystemExit("S3_ACCESS_KEY and S3_SECRET_KEY must be set together")
dedicated = (
    os.environ.get("WHOSCORED_RAW_S3_ACCESS_KEY"),
    os.environ.get("WHOSCORED_RAW_S3_SECRET_KEY"),
)
if any(dedicated) and not all(dedicated):
    raise SystemExit("WhoScored raw S3 credentials must be set together")
if all(dedicated):
    required.add(dedicated)
backup_reader = (
    os.environ.get("WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY"),
    os.environ.get("WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY"),
)
if not all(backup_reader):
    raise SystemExit("dedicated WhoScored backup-reader credentials are required")
required.add(backup_reader)
if any(not all(pair) or pair not in pairs for pair in required):
    raise SystemExit("SeaweedFS S3 config does not contain every active credential")
bucket = os.environ.get("ICEBERG_WAREHOUSE")
expected_actions = {f"Read:{bucket}", f"List:{bucket}"}
matches = [
    identity
    for identity in payload.get("identities", [])
    if backup_reader in {
        (item.get("accessKey"), item.get("secretKey"))
        for item in identity.get("credentials", [])
    }
]
if len(matches) != 1 or set(matches[0].get("actions", [])) != expected_actions:
    raise SystemExit("backup-reader identity must have only bucket Read/List actions")
PY

echo "Checking measured full-warehouse cutover capacity before downtime"
"${compose[@]}" run --rm --no-deps --entrypoint bash airflow-scheduler -euc '
  required=(
    WHOSCORED_BACKUP_DESTINATION_URI
    WHOSCORED_BACKUP_DESTINATION_SITE_ID
    WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE
    SEAWEEDFS_CUTOVER_REHEARSAL_INVENTORY
    SEAWEEDFS_CUTOVER_REHEARSAL_MAX_AGE_HOURS
    SEAWEEDFS_CUTOVER_INVENTORY_MIBPS
    SEAWEEDFS_CUTOVER_BACKUP_MIBPS
    SEAWEEDFS_CUTOVER_VERIFY_MIBPS
    SEAWEEDFS_CUTOVER_FIXED_OVERHEAD_SECONDS
    SEAWEEDFS_CUTOVER_MAX_DOWNTIME_SECONDS
  )
  for name in "${required[@]}"; do
    if [[ -z "${!name:-}" ]]; then
      echo "${name} is required from a recent full-warehouse rehearsal" >&2
      exit 2
    fi
  done
  case "${SEAWEEDFS_CUTOVER_REHEARSAL_INVENTORY}" in
    (/opt/airflow/logs/*/inventory.json) ;;
    (*) echo "rehearsal inventory must be an /opt/airflow/logs/.../inventory.json path" >&2; exit 2;;
  esac
  case "${WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE}" in
    (object-lock|versioned-worm) ;;
    (*) echo "off-host destination must enforce object-lock or versioned-worm" >&2; exit 2;;
  esac
  if [[ "${WHOSCORED_BACKUP_SOURCE_SITE_ID}" == "${WHOSCORED_BACKUP_DESTINATION_SITE_ID}" ]]; then
    echo "backup source and destination site IDs must differ" >&2
    exit 2
  fi
  if [[ -n "${WHOSCORED_BACKUP_DESTINATION_S3_ENDPOINT}" ]] &&
     [[ "${WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT,,}" == "${WHOSCORED_BACKUP_DESTINATION_S3_ENDPOINT,,}" ]]; then
    echo "backup source and destination endpoints must differ" >&2
    exit 2
  fi
  python /opt/airflow/scripts/whoscored_raw_backup.py preflight \
    --source-uri "s3://${ICEBERG_WAREHOUSE}" \
    --destination-uri "${WHOSCORED_BACKUP_DESTINATION_URI}" \
    --workers "${WHOSCORED_BACKUP_WORKERS}"
  echo "Verifying rehearsal backup access before downtime"
  python /opt/airflow/scripts/whoscored_raw_backup.py verify-backup \
    --store-uri "${WHOSCORED_BACKUP_DESTINATION_URI}" \
    --inventory "${SEAWEEDFS_CUTOVER_REHEARSAL_INVENTORY}" \
    --workers "${WHOSCORED_BACKUP_WORKERS}"
  python /opt/airflow/scripts/whoscored_raw_backup.py capacity-check \
    --inventory "${SEAWEEDFS_CUTOVER_REHEARSAL_INVENTORY}" \
    --expected-source-uri "s3://${ICEBERG_WAREHOUSE}" \
    --current-store-uri "s3://${ICEBERG_WAREHOUSE}" \
    --max-inventory-age-hours "${SEAWEEDFS_CUTOVER_REHEARSAL_MAX_AGE_HOURS}" \
    --inventory-mib-per-second "${SEAWEEDFS_CUTOVER_INVENTORY_MIBPS}" \
    --backup-mib-per-second "${SEAWEEDFS_CUTOVER_BACKUP_MIBPS}" \
    --verify-mib-per-second "${SEAWEEDFS_CUTOVER_VERIFY_MIBPS}" \
    --fixed-cutover-overhead-seconds \
      "${SEAWEEDFS_CUTOVER_FIXED_OVERHEAD_SECONDS}" \
    --max-downtime-seconds "${SEAWEEDFS_CUTOVER_MAX_DOWNTIME_SECONDS}"
'

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
container_inventory="/opt/airflow/logs/seaweedfs_cutover/${timestamp}/inventory.json"
"${compose[@]}" run --rm --no-deps --user 0:0 --entrypoint /bin/sh \
  airflow-scheduler -euc \
  'install -d -o 50000 -g 0 -m 0770 "$(dirname "$1")"' _ \
  "${container_inventory}"
"${compose[@]}" run --rm --no-deps --entrypoint /bin/sh \
  airflow-scheduler -euc 'test -w "$(dirname "$1")"' _ \
  "${container_inventory}"

failed=1
on_exit() {
  local status=$?
  if ((failed)); then
    set +e
    local service running_name running_names
    local inventory_valid=1
    local -a unsafe=()
    local -a cutover_quiesce_targets=(
      seaweedfs-s3-proxy seaweedfs-s3 seaweedfs
      "${writers[@]}"
      seaweedfs-filer seaweedfs-volume seaweedfs-master
    )

    # The failing phase is deliberately irrelevant here. A partial cutover can
    # have the legacy gateway, the supervised planes, or both present, so every
    # exact writer/storage target gets a best-effort stop request.
    for service in "${cutover_quiesce_targets[@]}"; do
      if docker stop --time 120 "${service}" >/dev/null 2>&1; then
        echo "Cutover emergency stop completed for exact target: ${service}" >&2
      else
        echo "Cutover emergency stop failed or target is absent: ${service}; authoritative inventory check required" >&2
      fi
    done

    # Individual inspect/stop results are not proof of quiescence. Only a
    # successful engine-wide running-container inventory can establish the
    # exact-name postcondition.
    if ! running_names="$(docker ps --format '{{.Names}}')"; then
      inventory_valid=0
      echo "EMERGENCY: cutover quiescence could not be confirmed because docker ps enumeration failed" >&2
    else
      while IFS= read -r running_name; do
        [[ -n "${running_name}" ]] || continue
        if [[ ! "${running_name}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
          inventory_valid=0
          echo "EMERGENCY: invalid container name in cutover running inventory: ${running_name}" >&2
          continue
        fi
        for service in "${cutover_quiesce_targets[@]}"; do
          if [[ "${running_name}" == "${service}" ]]; then
            unsafe+=("${service}")
            echo "EMERGENCY: cutover target remains running after stop: ${service}" >&2
            break
          fi
        done
      done <<<"${running_names}"
    fi

    if ((!inventory_valid)) || ((${#unsafe[@]})); then
      echo "EMERGENCY: cutover quiescence is unproven; refusing the ordinary exit status" >&2
      status=125
    else
      echo "Cutover stopped fail-closed; authoritative docker ps confirms every writer and storage target is stopped or absent" >&2
      if ((status == 0)); then
        status=1
      fi
    fi
  fi
  rm -rf "${fence_dir:-}"
  rm -f "${topology_state_tmp}"
  trap - EXIT
  exit "${status}"
}
trap on_exit EXIT

wait_healthy() {
  local container="$1"
  local attempts=60
  while ((attempts--)); do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container}" 2>/dev/null || true)"
    if [[ "${status}" == "healthy" || "${status}" == "running" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "${container} did not become healthy" >&2
  return 1
}

# Build a root-owned, single-identity Read/List config before quiescing. It is
# mounted into a recreated legacy S3 gateway after writers stop, making the
# recovery cut a server-side write barrier even for forgotten host clients.
fence_dir="$(mktemp -d /tmp/seaweedfs-cutover-fence.XXXXXX)"
chmod 0700 "${fence_dir}"
fence_config="${fence_dir}/s3-read-only.json"
"${compose[@]}" run -T --rm --no-deps --user 0:0 \
  -v "${SEAWEEDFS_S3_CONFIG_FILE}:/tmp/s3.config.json:ro" \
  --entrypoint python airflow-scheduler - >"${fence_config}" <<'PY'
import json
import os
from pathlib import Path

payload = json.loads(Path("/tmp/s3.config.json").read_text())
credential = (
    os.environ["WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY"],
    os.environ["WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY"],
)
bucket = os.environ["ICEBERG_WAREHOUSE"]
matches = [
    identity
    for identity in payload["identities"]
    if credential in {
        (item.get("accessKey"), item.get("secretKey"))
        for item in identity.get("credentials", [])
    }
]
expected = {f"Read:{bucket}", f"List:{bucket}"}
if len(matches) != 1 or set(matches[0].get("actions", [])) != expected:
    raise SystemExit("cannot derive the exact cutover Read/List identity")
json.dump({"identities": [matches[0]]}, os.sys.stdout, separators=(",", ":"))
os.sys.stdout.write("\n")
PY
chmod 0400 "${fence_config}"
python3 -m json.tool "${fence_config}" >/dev/null
fence_overlay="${fence_dir}/compose.read-only-fence.yaml"
cat >"${fence_overlay}" <<EOF
name: ${SEAWEEDFS_COMPOSE_PROJECT_NAME}
services:
  seaweedfs-s3:
    volumes: !override
      - ${PWD}/scripts/seaweedfs_legacy_entrypoint.sh:/usr/local/bin/seaweedfs-legacy-entrypoint:ro
      - ${fence_config}:/etc/seaweedfs/s3.config.json:ro
EOF
supervised_fence_overlay="${fence_dir}/compose.supervised-read-only-fence.yaml"
cat >"${supervised_fence_overlay}" <<EOF
name: ${SEAWEEDFS_COMPOSE_PROJECT_NAME}
services:
  seaweedfs:
    volumes: !override
      - ${PWD}/scripts/seaweedfs_legacy_entrypoint.sh:/usr/local/bin/seaweedfs-legacy-entrypoint:ro
      - ${fence_config}:/etc/seaweedfs/s3.config.json:ro
EOF
legacy_fence_compose=(
  docker compose -p "${SEAWEEDFS_COMPOSE_PROJECT_NAME}"
  -f compose.yaml -f "${fence_overlay}"
)
fenced_supervised_compose=(
  docker compose -p "${SEAWEEDFS_COMPOSE_PROJECT_NAME}"
  -f compose.yaml -f "${supervised_overlay}"
  -f "${supervised_fence_overlay}"
)
if [[ -n "${COMPOSE_ENV_FILE:-}" ]]; then
  legacy_fence_compose+=(--env-file "${COMPOSE_ENV_FILE}")
  fenced_supervised_compose+=(--env-file "${COMPOSE_ENV_FILE}")
fi
"${legacy_fence_compose[@]}" config --quiet
"${fenced_supervised_compose[@]}" config --quiet

echo "Stopping every storage writer before the recovery cut"
for service in "${writers[@]}"; do
  if docker inspect "${service}" >/dev/null 2>&1; then
    assert_compose_project_identity "${service}" "${service}" optional
    if [[ "$(docker inspect --format '{{.State.Running}}' "${service}")" == true ]]; then
      docker stop --time 120 "${service}" >/dev/null
    fi
  fi
done
for service in "${writers[@]}"; do
  if [[ "$(docker inspect --format '{{.State.Running}}' "${service}" 2>/dev/null || true)" == true ]]; then
    echo "${service} is still running after the quiesce barrier" >&2
    exit 2
  fi
done

echo "Recreating the isolated legacy S3 gateway with a Read/List-only identity"
"${legacy_fence_compose[@]}" up -d --no-deps --force-recreate seaweedfs-s3
wait_healthy seaweedfs-s3
"${compose[@]}" run -T --rm --no-deps --entrypoint python \
  airflow-scheduler - <<'PY'
import os
import re
import uuid

from pyarrow import fs

bucket = os.environ["ICEBERG_WAREHOUSE"]

def client(access_key, secret_key):
    return fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=os.environ["WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT"],
        scheme=os.environ["WHOSCORED_BACKUP_SOURCE_S3_SCHEME"],
        region=os.environ["WHOSCORED_BACKUP_SOURCE_S3_REGION"],
        background_writes=False,
    )

def require_access_denied(operation):
    try:
        operation()
    except OSError as exc:
        if re.search(
            r"AccessDenied|ACCESS_DENIED|InvalidAccessKeyId|"
            r"INVALID_ACCESS_KEY_ID|Permission denied|HTTP 403",
            str(exc),
        ):
            return
        raise
    raise SystemExit("cutover fence unexpectedly authorized a forbidden operation")

reader = client(
    os.environ["WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY"],
    os.environ["WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY"],
)
reader.get_file_info(fs.FileSelector(bucket, recursive=False))
probe = f"ops/seaweedfs-cutover-write-probe/{uuid.uuid4().hex}"
def write_probe():
    with reader.open_output_stream(f"{bucket}/{probe}") as stream:
        stream.write(b"must-be-denied")
require_access_denied(write_probe)

admin = client(os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"])
require_access_denied(
    lambda: admin.get_file_info(fs.FileSelector(bucket, recursive=False))
)
PY

echo "Creating and verifying an off-host full-bucket recovery inventory"
"${compose[@]}" run --rm --no-deps --entrypoint bash airflow-scheduler -euc '
  test -n "${WHOSCORED_BACKUP_DESTINATION_URI:-}" || {
    echo "WHOSCORED_BACKUP_DESTINATION_URI is required" >&2
    exit 2
  }
  source_uri="s3://${ICEBERG_WAREHOUSE}"
  python /opt/airflow/scripts/whoscored_raw_backup.py inventory \
    --store-uri "${source_uri}" --output "$1" \
    --workers "${WHOSCORED_BACKUP_WORKERS}"
  python /opt/airflow/scripts/whoscored_raw_backup.py backup \
    --source-uri "${source_uri}" \
    --destination-uri "${WHOSCORED_BACKUP_DESTINATION_URI}" \
    --inventory "$1" --apply --workers "${WHOSCORED_BACKUP_WORKERS}"
  python /opt/airflow/scripts/whoscored_raw_backup.py verify-backup \
    --store-uri "${WHOSCORED_BACKUP_DESTINATION_URI}" --inventory "$1" \
    --workers "${WHOSCORED_BACKUP_WORKERS}"
' _ "${container_inventory}"

echo "Stopping the public S3 HTTP proxy before its private legacy gateway"
"${base_compose[@]}" stop --timeout 120 seaweedfs-s3-proxy
if ! proxy_running="$(docker inspect --format '{{.State.Running}}' seaweedfs-s3-proxy 2>/dev/null)"; then
  echo "Cannot confirm that the public S3 HTTP proxy is stopped" >&2
  exit 2
fi
if [[ "${proxy_running}" != "false" ]]; then
  echo "S3 HTTP proxy is still running; refusing to replace its private upstream" >&2
  exit 2
fi
echo "Stopping the private legacy S3 gateway before its mini storage core"
"${base_compose[@]}" stop --timeout 120 seaweedfs-s3
if ! gateway_running="$(docker inspect --format '{{.State.Running}}' seaweedfs-s3 2>/dev/null)"; then
  echo "Cannot confirm that the legacy S3 gateway is stopped" >&2
  exit 2
fi
if [[ "${gateway_running}" != "false" ]]; then
  echo "Legacy S3 gateway is still running; refusing to open a new gateway" >&2
  exit 2
fi
echo "Stopping legacy weed mini before any new plane opens its files"
"${base_compose[@]}" stop --timeout 120 seaweedfs
if ! legacy_running="$(docker inspect --format '{{.State.Running}}' seaweedfs 2>/dev/null)"; then
  echo "Cannot confirm that legacy weed mini is stopped" >&2
  exit 2
fi
if [[ "${legacy_running}" != "false" ]]; then
  echo "Legacy weed mini is still running; refusing to open the new plane" >&2
  exit 2
fi
"${base_compose[@]}" rm -f seaweedfs-s3
if docker inspect seaweedfs-s3 >/dev/null 2>&1; then
  echo "Legacy S3 gateway still exists after removal; refusing alias/port overlap" >&2
  exit 2
fi
assert_no_storage_writer_oneoffs
assert_no_running_volume_consumers "${live_volume}"

inventory_sha256="$("${compose[@]}" run --rm --no-deps --entrypoint python \
  airflow-scheduler - "${container_inventory}" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text())["inventory_sha256"])
PY
)"
export SEAWEEDFS_EXPECTED_INVENTORY_SHA256="${inventory_sha256}"

# Persist recovery intent before writing the irreversible volume marker. A
# power loss can therefore never leave legacy mini disabled without a durable
# state record and locked recovery route.
python3 - "${topology_state_tmp}" "${rendered_volume}" \
  "${expected_image_id}" "${inventory_sha256}" \
  "${volume_size_limit_mb}" <<'PY'
import json
import os
import sys

path, volume_name, image_id, inventory_sha256, volume_size_limit_mb = sys.argv[1:]
with open(path, "r+", encoding="utf-8") as handle:
    handle.seek(0)
    json.dump(
        {
            "schema_version": 2,
            "mode": "supervised-verification-pending-v1",
            "volume_name": volume_name,
            "image_id": image_id,
            "inventory_sha256": inventory_sha256,
            "volume_size_limit_mb": int(volume_size_limit_mb),
        },
        handle,
        sort_keys=True,
        separators=(",", ":"),
    )
    handle.write("\n")
    handle.truncate()
    handle.flush()
    os.fsync(handle.fileno())
PY
chmod 0644 "${topology_state_tmp}"
mv -T "${topology_state_tmp}" "${topology_state_file}"
python3 - "${topology_state_dir}" <<'PY'
import os
import sys

directory = os.open(sys.argv[1], os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(directory)
finally:
    os.close(directory)
PY

"${compose[@]}" run --rm --no-deps --entrypoint /bin/sh seaweedfs-master \
  -euc 'umask 077; printf "%s\n" "$1" > /data/.supervised-topology-cutover-approved; sync' \
  _ "full-bucket-inventory-v2:${inventory_sha256}"

# The marker helper is a volume-bearing Compose one-off. It must be fully
# removed before any long-lived supervised process opens the same files.
assert_no_storage_writer_oneoffs
assert_no_running_volume_consumers "${live_volume}"

"${fenced_supervised_compose[@]}" up -d --no-deps seaweedfs-master
wait_healthy seaweedfs-master
"${fenced_supervised_compose[@]}" up -d --no-deps seaweedfs-volume
wait_healthy seaweedfs-volume
"${fenced_supervised_compose[@]}" up -d --no-deps seaweedfs-filer
wait_healthy seaweedfs-filer
"${fenced_supervised_compose[@]}" up -d --no-deps seaweedfs
wait_healthy seaweedfs
assert_s3_proxy_config_boundary
"${fenced_supervised_compose[@]}" up -d --no-deps seaweedfs-s3-proxy
wait_healthy seaweedfs-s3-proxy

echo "Verifying every recovery-cut object through the new S3 gateway"
"${fenced_supervised_compose[@]}" run --rm --no-deps --entrypoint bash airflow-scheduler -euc '
  export WHOSCORED_BACKUP_RESTORE_S3_ENDPOINT="${WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT}"
  export WHOSCORED_BACKUP_RESTORE_S3_SCHEME="${WHOSCORED_BACKUP_SOURCE_S3_SCHEME}"
  export WHOSCORED_BACKUP_RESTORE_S3_REGION="${WHOSCORED_BACKUP_SOURCE_S3_REGION}"
  export WHOSCORED_BACKUP_RESTORE_S3_ACCESS_KEY="${WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY:-${WHOSCORED_RAW_S3_ACCESS_KEY:-${S3_ACCESS_KEY}}}"
  export WHOSCORED_BACKUP_RESTORE_S3_SECRET_KEY="${WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY:-${WHOSCORED_RAW_S3_SECRET_KEY:-${S3_SECRET_KEY}}}"
  python /opt/airflow/scripts/whoscored_raw_backup.py verify-restore \
    --store-uri "s3://${ICEBERG_WAREHOUSE}" --inventory "$1" \
    --workers "${WHOSCORED_BACKUP_WORKERS}"
' _ "${container_inventory}"

# Exact read-back succeeded while the S3 gateway still exposed only the
# Read/List identity. Promote pending state durably before reopening writes.
python3 - "${topology_state_file}" <<'PY'
import json
import os
import sys
import tempfile
from pathlib import Path

path = Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
if payload.get("mode") != "supervised-verification-pending-v1":
    raise SystemExit("topology state is not in verification-pending mode")
payload["mode"] = "supervised-v1"
descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
try:
    os.fchmod(descriptor, 0o644)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
except Exception:
    try:
        os.unlink(temporary)
    except FileNotFoundError:
        pass
    raise
PY

echo "Restoring the full S3 identity set only after exact recovery verification"
"${compose[@]}" up -d --no-deps --force-recreate seaweedfs
wait_healthy seaweedfs

was_running() {
  local wanted="$1"
  local item
  for item in "${restart[@]}"; do
    if [[ "${item}" == "${wanted}" ]]; then
      return 0
    fi
  done
  return 1
}

for service in \
  lakekeeper trino airflow-webserver airflow-scheduler \
  superset superset-worker superset-beat \
  openmetadata-server openmetadata-ingestion jupyterhub; do
  if was_running "${service}"; then
    "${compose[@]}" start "${service}"
    wait_healthy "${service}"
  fi
done
failed=0
echo "SeaweedFS topology cutover completed and recovery inventory verified"
