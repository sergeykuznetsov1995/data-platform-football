#!/usr/bin/env bash
# Выкат замороженного дерева на контур SofaScore (проекты sofascore-airflow / sofascore-gw).
# Использование: bash deploy/sofascore/deploy.sh <release-root> [old-release-root]
# Предпосылки: VERIFIED в <runtime>/canary-<digest8>; окно вне 14:00–15:30 UTC;
#   кампания ставится на паузу и ждёт idle здесь же.
# Переменные — из $SOFASCORE_ENV_FILE (по умолчанию /etc/data-platform/sofascore.env);
#   скрипт сам переписывает в нём SOFASCORE_RELEASE_ROOT / _PROXY_BUDGET_ARTIFACT_HOST / _ID —
#   этот файл и есть единственный источник «какое дерево в бою» (compose, сторож, приёмка).
set -euo pipefail

RELEASE="${1:?путь к замороженному дереву}"
OLD_RELEASE="${2:-}"
ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a
: "${SOFASCORE_RUNTIME_DIR:?}" "${SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR:?}" "${SOFASCORE_GATEWAY_STATE_HOST_DIR:?}" \
  "${SOFASCORE_PLATFORM_ENV_FILE:?}" "${SOFASCORE_HOST_PYTHON:?}"

TAG=$(basename "$RELEASE" | sed 's/^release-//')
WORKSPACE="$SOFASCORE_RUNTIME_DIR/canary-$TAG"
ARTIFACT="$WORKSPACE/candidate.json"
LOG="$SOFASCORE_RUNTIME_DIR/all-men/deploy.log"
SCHED_COMPOSE="$RELEASE/deploy/sofascore/airflow.compose.yaml"
GW_COMPOSE="$RELEASE/deploy/sofascore/gateway.compose.yaml"
CAMPAIGN="$SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR"
STATE="$SOFASCORE_GATEWAY_STATE_HOST_DIR"
HIST=dag_backfill_sofascore_all_mens
REFRESH=dag_refresh_sofascore_all_mens
PSQL="docker exec sofascore-airflow-metadb psql -U airflow -d airflow -At -c"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }
set_env_var() {  # set_env_var KEY VALUE — переписать строку KEY= в env-файле контура
  local key="$1" value="$2"
  grep -qE "^${key}=" "$ENV_FILE" || { echo "в $ENV_FILE нет строки ${key}=" >&2; exit 2; }
  sed -i "s#^${key}=.*#${key}=${value}#" "$ENV_FILE"
}

[ -f "$WORKSPACE/VERIFIED" ] || { echo "нет VERIFIED в $WORKSPACE" >&2; exit 2; }
[ -f "$SCHED_COMPOSE" ] && [ -f "$GW_COMPOSE" ] || { echo "в $RELEASE нет deploy/sofascore/*.compose.yaml" >&2; exit 2; }
hour=$(date -u +%H%M)
if [ "$hour" -ge 1355 ] && [ "$hour" -le 1535 ]; then echo "окно дейли 14:00–15:30 UTC — позже" >&2; exit 3; fi

log "pause $HIST and wait for idle"
docker exec sofascore-airflow-scheduler airflow dags pause "$HIST" >> "$LOG" 2>&1
paused=$($PSQL "SELECT is_paused FROM dag WHERE dag_id='$HIST';")
[ "$paused" = "t" ] || { log "$HIST did not pause"; exit 7; }
while true; do
  busy=$($PSQL "SELECT count(*) FROM task_instance WHERE dag_id IN ('dag_ingest_sofascore','$HIST') AND state IN ('queued','running');")
  active_daily=$($PSQL "SELECT count(*) FROM dag_run WHERE dag_id='dag_ingest_sofascore' AND state IN ('queued','running');")
  [ "$busy" = "0" ] && [ "$active_daily" = "0" ] && break
  sleep 60
done
active_history=$($PSQL "SELECT count(*) FROM dag_run WHERE dag_id='$HIST' AND state IN ('queued','running');")
[ "$active_history" = "0" ] || { log "$HIST still has $active_history active DagRun(s)"; exit 7; }
log "idle"

DIGEST=$(PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$RELEASE" "$SOFASCORE_HOST_PYTHON" -B -c \
  'from scrapers.sofascore.runtime_fingerprint import runtime_fingerprint; print(runtime_fingerprint()["digest"])')
CANDIDATE_DIGEST=$(python3 -c "import json; print(json.load(open('$ARTIFACT'))['runtime_fingerprint']['digest'])")
[ "$DIGEST" = "$CANDIDATE_DIGEST" ] || { echo "digest дерева $DIGEST != кандидата $CANDIDATE_DIGEST" >&2; exit 4; }
[ "${DIGEST:0:8}" = "$TAG" ] || { echo "имя дерева не совпадает с digest" >&2; exit 4; }

ARTIFACT_DEST="$SOFASCORE_RUNTIME_DIR/artifacts/$DIGEST/proxy_budget_canary.json"
mkdir -p "$(dirname "$ARTIFACT_DEST")"
cp "$ARTIFACT" "$ARTIFACT_DEST"
chmod 0644 "$ARTIFACT_DEST"
ARTIFACT_ID=$(sha256sum "$ARTIFACT_DEST" | awk '{print $1}')
log "artifact $ARTIFACT_DEST id=$ARTIFACT_ID (digest дерева $DIGEST — это разные сущности)"

# Перенос состояния кампании из старого дерева в runtime (переживает ротации).
OLD_STATE_DIR="${OLD_RELEASE:+$OLD_RELEASE/logs/sofascore-all-men}"
if [ -n "$OLD_STATE_DIR" ] && [ -d "$OLD_STATE_DIR" ] && [ ! -e "$CAMPAIGN/state.json" ]; then
  cp -a "$OLD_STATE_DIR/state.json" "$CAMPAIGN/state.json"
  [ -e "$OLD_STATE_DIR/failures.json" ] && cp -a "$OLD_STATE_DIR/failures.json" "$CAMPAIGN/failures.json"
  mkdir -p "$CAMPAIGN/results" "$CAMPAIGN/refresh-results"
  cp -a "$OLD_STATE_DIR/results/." "$CAMPAIGN/results/"
  log "campaign state migrated from $OLD_STATE_DIR ($(python3 -c "import json;print(len(json.load(open('$CAMPAIGN/state.json'))['completed']))") completed)"
fi
mkdir -p "$CAMPAIGN/results" "$CAMPAIGN/refresh-results"
chown -R 50000:0 "$CAMPAIGN"
chmod 0750 "$CAMPAIGN"
chmod 0644 "$CAMPAIGN/snapshot.json"

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$RELEASE" "$SOFASCORE_HOST_PYTHON" -B \
  "$RELEASE/scripts/sofascore_runtime_preflight.py" preflight \
    --release-root "$RELEASE" --artifact "$ARTIFACT_DEST" --state-dir "$STATE" \
    --campaign-dir "$CAMPAIGN" --campaign-policy "$RELEASE/configs/sofascore/all_mens_campaign.json" \
    --expected-artifact-id "$ARTIFACT_ID" >> "$LOG" 2>&1
log "preflight ok"

# Единственный источник истины о бое — env-файл контура; compose и сторож читают его.
set_env_var SOFASCORE_RELEASE_ROOT "$RELEASE"
set_env_var SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST "$ARTIFACT_DEST"
set_env_var SOFASCORE_PROXY_BUDGET_ARTIFACT_ID "$ARTIFACT_ID"
log "env file $ENV_FILE repinned to $RELEASE"

docker compose -p sofascore-airflow -f "$SCHED_COMPOSE" \
  --env-file "$SOFASCORE_PLATFORM_ENV_FILE" --env-file "$ENV_FILE" \
  up -d --no-deps --force-recreate airflow-scheduler >> "$LOG" 2>&1
log "scheduler up"

docker compose -p sofascore-gw -f "$GW_COMPOSE" \
  --project-directory "$RELEASE" \
  --env-file "$SOFASCORE_PLATFORM_ENV_FILE" --env-file "$ENV_FILE" \
  up -d --no-deps --force-recreate sofascore_proxy_filter >> "$LOG" 2>&1
log "gateway up"

# 10 минут: 5 минут не хватило 25.08, healthcheck успел стать healthy на 30 с позже выхода.
for _ in $(seq 1 60); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' sofascore_gw_951 2>/dev/null)" = "healthy" ] && break
  sleep 10
done
[ "$(docker inspect -f '{{.State.Health.Status}}' sofascore_gw_951)" = "healthy" ] || { log "gateway unhealthy"; exit 5; }
mem=$(docker inspect -f '{{.HostConfig.Memory}}' sofascore_gw_951)
log "gateway healthy; HostConfig.Memory=$mem (ожидание 1073741824)"
docker logs sofascore_gw_951 --since 10m 2>&1 | grep -E "residential pool|paid_enabled|compacted|listening|SofaScore paid leases disabled" | tail -8 | tee -a "$LOG"

docker exec sofascore-airflow-scheduler python /opt/airflow/scripts/sofascore_runtime_preflight.py scheduler-health \
  --artifact /opt/airflow/runtime/sofascore/proxy_budget_canary.json \
  --health-url http://sofascore_proxy_filter:8899/health \
  --campaign-dir /opt/airflow/runtime/sofascore/all-men \
  --campaign-policy /opt/airflow/configs/sofascore/all_mens_campaign.json >> "$LOG" 2>&1
for _ in $(seq 1 30); do
  errs=$($PSQL "SELECT count(*) FROM import_error;")
  present=$($PSQL "SELECT count(*) FROM dag WHERE dag_id IN ('$HIST','$REFRESH','dag_ingest_sofascore') AND is_active=true;")
  [ "$present" = "3" ] && break
  sleep 10
done
log "dags active=$present import_errors=$errs"
[ "$errs" = "0" ] || { log "import errors present — см. import_error"; exit 6; }

docker exec sofascore-airflow-scheduler airflow dags pause "$HIST" >> "$LOG" 2>&1
paused=$($PSQL "SELECT is_paused FROM dag WHERE dag_id='$HIST';")
[ "$paused" = "t" ] || { log "$HIST did not remain paused"; exit 7; }
log "history kept paused; $REFRESH остаётся paused до принятого ручного fresh-прогона"

# Сторож аренд читает тот же env-файл (EnvironmentFile= в unit) — достаточно рестарта.
systemctl restart sofascore-gw-lease-watchdog.service
log "watchdog restarted on $RELEASE: $(systemctl is-active sofascore-gw-lease-watchdog.service)"
log "DONE artifact_id=$ARTIFACT_ID runtime=$DIGEST"
