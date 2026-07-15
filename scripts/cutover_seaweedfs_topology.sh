#!/usr/bin/env bash
# Quiesced, backup-gated migration from `weed mini` to supervised planes.
set -Eeuo pipefail

cutover_lock_file="${SEAWEEDFS_CUTOVER_LOCK_FILE:-/tmp/data-platform-football-seaweedfs-cutover.lock}"
mkdir -p "$(dirname "${cutover_lock_file}")"
exec 9>"${cutover_lock_file}"
if ! flock -n 9; then
  echo "Another SeaweedFS topology cutover is already running" >&2
  exit 73
fi

if [[ "${SEAWEEDFS_CUTOVER_CONFIRM:-}" != "backup-and-downtime-approved" ]]; then
  echo "Set SEAWEEDFS_CUTOVER_CONFIRM=backup-and-downtime-approved" >&2
  exit 2
fi
if [[ ! -f configs/seaweedfs/s3.config.json ]] ||
   [[ -L configs/seaweedfs/s3.config.json ]]; then
  echo "configs/seaweedfs/s3.config.json must be a regular secret file" >&2
  exit 2
fi
config_mode="$(stat -c '%a' configs/seaweedfs/s3.config.json)"
if (( (8#${config_mode} & 077) != 0 )); then
  echo "configs/seaweedfs/s3.config.json must not grant group/other permissions" >&2
  exit 2
fi

compose=(docker compose)
if [[ -n "${COMPOSE_ENV_FILE:-}" ]]; then
  compose+=(--env-file "${COMPOSE_ENV_FILE}")
fi

running_services="$("${compose[@]}" ps --status running --services)"
for service in seaweedfs-master seaweedfs-volume seaweedfs-filer; do
  if grep -qx "${service}" <<<"${running_services}"; then
    echo "${service} is already running; refusing an overlapping cutover" >&2
    exit 2
  fi
done

old_command="$(docker inspect --format '{{json .Config.Cmd}}' seaweedfs 2>/dev/null || true)"
if [[ "${old_command}" != *mini* ]]; then
  echo "The running seaweedfs container is not the expected legacy weed mini" >&2
  exit 2
fi

"${compose[@]}" run --rm --no-deps --user 0:0 \
  -v "${PWD}/configs/seaweedfs/s3.config.json:/tmp/s3.config.json:ro" \
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
if any(backup_reader) and not all(backup_reader):
    raise SystemExit("WhoScored backup-reader credentials must be set together")
if all(backup_reader):
    required.add(backup_reader)
if any(not all(pair) or pair not in pairs for pair in required):
    raise SystemExit("SeaweedFS S3 config does not contain every active credential")
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

writers=(airflow-scheduler trino lakekeeper jupyterhub)
restart=()
for service in "${writers[@]}"; do
  if grep -qx "${service}" <<<"${running_services}"; then
    restart+=("${service}")
  fi
done

failed=1
on_exit() {
  local status=$?
  if ((failed)); then
    set +e
    stop_ok=1
    if ! "${compose[@]}" stop --timeout 120 "${writers[@]}" >/dev/null 2>&1; then
      stop_ok=0
    fi
    ps_ok=1
    if ! still_running="$("${compose[@]}" ps --status running --services 2>/dev/null)"; then
      ps_ok=0
    fi
    unsafe=()
    if ((ps_ok)); then
      for service in "${writers[@]}"; do
        if grep -qx "${service}" <<<"${still_running}"; then
          unsafe+=("${service}")
        fi
      done
    fi
    if ((!stop_ok || !ps_ok)); then
      echo "Cutover failed and writer shutdown could not be confirmed" >&2
    elif ((${#unsafe[@]})); then
      echo "Cutover failed and writer shutdown is incomplete: ${unsafe[*]}" >&2
    else
      echo "Cutover stopped fail-closed; all storage writers are stopped" >&2
    fi
    set -e
  fi
  return "${status}"
}
trap on_exit EXIT

echo "Stopping every storage writer before the recovery cut"
"${compose[@]}" stop --timeout 120 "${writers[@]}"
post_stop_services="$("${compose[@]}" ps --status running --services)"
for service in "${writers[@]}"; do
  if grep -qx "${service}" <<<"${post_stop_services}"; then
    echo "${service} is still running after the quiesce barrier" >&2
    exit 2
  fi
done

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

echo "Stopping legacy weed mini before any new plane opens its files"
"${compose[@]}" stop --timeout 120 seaweedfs
if ! legacy_running="$(docker inspect --format '{{.State.Running}}' seaweedfs 2>/dev/null)"; then
  echo "Cannot confirm that legacy weed mini is stopped" >&2
  exit 2
fi
if [[ "${legacy_running}" != "false" ]]; then
  echo "Legacy weed mini is still running; refusing to open the new plane" >&2
  exit 2
fi

inventory_sha256="$("${compose[@]}" run --rm --no-deps --entrypoint python \
  airflow-scheduler - "${container_inventory}" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text())["inventory_sha256"])
PY
)"
"${compose[@]}" run --rm --no-deps --entrypoint /bin/sh seaweedfs-master \
  -euc 'umask 077; printf "%s\n" "$1" > /data/.supervised-topology-cutover-approved' \
  _ "full-bucket-inventory-v2:${inventory_sha256}"

wait_healthy() {
  local container="$1"
  local attempts=60
  while ((attempts--)); do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container}" 2>/dev/null || true)"
    if [[ "${status}" == "healthy" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "${container} did not become healthy" >&2
  return 1
}

"${compose[@]}" up -d --no-deps seaweedfs-master
wait_healthy seaweedfs-master
"${compose[@]}" up -d --no-deps seaweedfs-volume
wait_healthy seaweedfs-volume
"${compose[@]}" up -d --no-deps seaweedfs-filer
wait_healthy seaweedfs-filer
"${compose[@]}" up -d --no-deps seaweedfs
wait_healthy seaweedfs

echo "Verifying every recovery-cut object through the new S3 gateway"
"${compose[@]}" run --rm --no-deps --entrypoint bash airflow-scheduler -euc '
  export WHOSCORED_BACKUP_RESTORE_S3_ENDPOINT="${WHOSCORED_BACKUP_SOURCE_S3_ENDPOINT}"
  export WHOSCORED_BACKUP_RESTORE_S3_SCHEME="${WHOSCORED_BACKUP_SOURCE_S3_SCHEME}"
  export WHOSCORED_BACKUP_RESTORE_S3_REGION="${WHOSCORED_BACKUP_SOURCE_S3_REGION}"
  export WHOSCORED_BACKUP_RESTORE_S3_ACCESS_KEY="${WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY:-${WHOSCORED_RAW_S3_ACCESS_KEY:-${S3_ACCESS_KEY}}}"
  export WHOSCORED_BACKUP_RESTORE_S3_SECRET_KEY="${WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY:-${WHOSCORED_RAW_S3_SECRET_KEY:-${S3_SECRET_KEY}}}"
  python /opt/airflow/scripts/whoscored_raw_backup.py verify-restore \
    --store-uri "s3://${ICEBERG_WAREHOUSE}" --inventory "$1" \
    --workers "${WHOSCORED_BACKUP_WORKERS}"
' _ "${container_inventory}"

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

for service in lakekeeper trino airflow-scheduler jupyterhub; do
  if was_running "${service}"; then
    "${compose[@]}" start "${service}"
    wait_healthy "${service}"
  fi
done
failed=0
echo "SeaweedFS topology cutover completed and recovery inventory verified"
