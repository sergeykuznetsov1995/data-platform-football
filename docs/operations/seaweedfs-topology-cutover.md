# SeaweedFS topology cutover

The existing `seaweedfs` container runs `weed mini` and owns the same Docker
volume that the new master, volume and filer services reuse. Never run a plain
`docker compose up -d` for this migration: it can start both process sets over
the same Raft, LevelDB and volume files.

Use the fail-closed cutover script during an approved downtime window:

```bash
export COMPOSE_ENV_FILE=/absolute/path/to/production.env
export SEAWEEDFS_CUTOVER_CONFIRM=backup-and-downtime-approved
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
docker compose --env-file "$COMPOSE_ENV_FILE" run --rm --no-deps \
  --user 0:0 --entrypoint /bin/sh -e REHEARSAL_DIR airflow-scheduler -euc \
  'install -d -o 50000 -g 0 -m 0770 "$REHEARSAL_DIR"'
docker compose --env-file "$COMPOSE_ENV_FILE" run --rm --no-deps \
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
3. stops Airflow, Trino, Lakekeeper and Jupyter writer entry points;
4. inventories the complete S3 bucket, copies it to the configured off-host
   content-addressed backup, and verifies its completion marker;
5. stops the legacy `weed mini` container;
6. starts master, volume, filer and S3 in order;
7. reads and hashes every recovery-cut object through the new S3 gateway;
8. restarts only the writer services that were running before the cutover.

Any failure leaves writers stopped. Do not bypass that state. The following is
the executable storage-volume recovery path; it assumes the PostgreSQL volume
containing Lakekeeper catalog state is intact. A lost catalog database requires
its separate platform database recovery and writers must remain stopped.

First stop writers and all storage planes, preserve the failed named volume,
and persist a new unique volume name in the deployment environment (not only
in an interactive shell):

```bash
docker compose --env-file "$COMPOSE_ENV_FILE" stop --timeout 120 \
  airflow-scheduler trino lakekeeper jupyterhub \
  seaweedfs seaweedfs-filer seaweedfs-volume seaweedfs-master
mkdir -p logs/seaweedfs_recovery
export FAILED_SEAWEEDFS_DATA_VOLUME_NAME="$(
  docker compose --env-file "$COMPOSE_ENV_FILE" config --format json \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["volumes"]["seaweedfs_data"]["name"])'
)"
test -n "$FAILED_SEAWEEDFS_DATA_VOLUME_NAME"
docker volume inspect "$FAILED_SEAWEEDFS_DATA_VOLUME_NAME" \
  > "logs/seaweedfs_recovery/failed-volume-$(date -u +%Y%m%dT%H%M%SZ).json"
export SEAWEEDFS_DATA_VOLUME_NAME="seaweedfs_recovery_$(date -u +%Y%m%dT%H%M%SZ)"
docker volume create "$SEAWEEDFS_DATA_VOLUME_NAME"
```

Set the exact same `SEAWEEDFS_DATA_VOLUME_NAME` in the production env/secret
manager before continuing. Never remove or reuse
`$FAILED_SEAWEEDFS_DATA_VOLUME_NAME`. Start only the four storage planes on the
empty volume and require each health gate:

```bash
wait_healthy() {
  container="$1"
  for _ in $(seq 1 60); do
    [ "$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null)" = healthy ] && return 0
    sleep 2
  done
  return 1
}
docker compose --env-file "$COMPOSE_ENV_FILE" up -d --no-deps seaweedfs-master
wait_healthy seaweedfs-master
docker compose --env-file "$COMPOSE_ENV_FILE" up -d --no-deps seaweedfs-volume
wait_healthy seaweedfs-volume
docker compose --env-file "$COMPOSE_ENV_FILE" up -d --no-deps seaweedfs-filer
wait_healthy seaweedfs-filer
docker compose --env-file "$COMPOSE_ENV_FILE" up -d --no-deps seaweedfs
wait_healthy seaweedfs
```

List only full-warehouse markers by their exact original source root, choose an
approved key, then fetch, restore and verify it through the new gateway:

```bash
docker compose --env-file "$COMPOSE_ENV_FILE" run --rm --no-deps \
  --entrypoint bash airflow-scheduler -euc '
    python /opt/airflow/scripts/whoscored_raw_backup.py list-inventories \
      --store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --expected-source-uri "s3://${ICEBERG_WAREHOUSE}" --limit 20
  '
export RECOVERY_INVENTORY_KEY=backup-inventories/EXACT_FULL_WAREHOUSE_KEY.json
docker compose --env-file "$COMPOSE_ENV_FILE" run --rm --no-deps \
  --entrypoint bash -e RECOVERY_INVENTORY_KEY airflow-scheduler -euc '
    source_uri="s3://${ICEBERG_WAREHOUSE}"
    inventory=/tmp/full-warehouse-recovery-inventory.json
    # The source backup-reader is Read/List only. A fresh bucket needs the
    # explicitly approved local platform Admin/Write identity.
    : "${S3_ENDPOINT:?local recovery S3 endpoint is required}"
    : "${S3_ACCESS_KEY:?local recovery Admin/Write access key is required}"
    : "${S3_SECRET_KEY:?local recovery Admin/Write secret key is required}"
    export WHOSCORED_BACKUP_RESTORE_S3_ENDPOINT="$S3_ENDPOINT"
    export WHOSCORED_BACKUP_RESTORE_S3_SCHEME="${S3_SCHEME:-http}"
    export WHOSCORED_BACKUP_RESTORE_S3_REGION="${S3_REGION:-us-east-1}"
    export WHOSCORED_BACKUP_RESTORE_S3_ACCESS_KEY="$S3_ACCESS_KEY"
    export WHOSCORED_BACKUP_RESTORE_S3_SECRET_KEY="$S3_SECRET_KEY"
    python /opt/airflow/scripts/whoscored_raw_backup.py fetch-inventory \
      --store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --inventory-key "$RECOVERY_INVENTORY_KEY" \
      --expected-source-uri "$source_uri" --output "$inventory"
    python /opt/airflow/scripts/whoscored_raw_backup.py restore \
      --backup-uri "$WHOSCORED_BACKUP_DESTINATION_URI" \
      --restore-uri "$source_uri" --inventory "$inventory" \
      --apply --create-bucket --workers "$WHOSCORED_BACKUP_WORKERS"
    python /opt/airflow/scripts/whoscored_raw_backup.py verify-restore \
      --store-uri "$source_uri" --inventory "$inventory" \
      --workers "$WHOSCORED_BACKUP_WORKERS"
  '
```

Only after `verify-restore` passes may Lakekeeper, Trino and the previously
recorded writer set be started. Recheck `SHOW SCHEMAS FROM iceberg`, the 25
WhoScored dataset DQ gate and one direct-only canary before unpausing schedules.
