#!/usr/bin/env bash
# Выкат замороженного дерева на контур SofaScore (проекты sofascore-airflow / sofascore-gw).
# Использование: bash deploy/sofascore/deploy.sh <release-root> [old-release-root]
# Предпосылки: VERIFIED в <runtime>/canary-<digest8>; окно вне 13:55–15:35 UTC;
#   обе кампании (история и актуалка) ставятся на паузу и ждут idle здесь же,
#   актуалка после выката возвращается в прежнее состояние.
# Переменные — из $SOFASCORE_ENV_FILE (по умолчанию /etc/data-platform/sofascore.env);
#   скрипт сам переписывает в нём SOFASCORE_RELEASE_ROOT / _PROXY_BUDGET_ARTIFACT_HOST / _ID —
#   этот файл и есть единственный источник «какое дерево в бою» (compose, сторож, приёмка).
set -euo pipefail

RELEASE="${1:?путь к замороженному дереву}"
OLD_RELEASE="${2:-}"
ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
# shellcheck source=deploy/sofascore/env.sh
. "$(dirname "$0")/env.sh"
sofascore_load_env "$ENV_FILE" || exit 2
: "${SOFASCORE_RUNTIME_DIR:?}" "${SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR:?}" "${SOFASCORE_GATEWAY_STATE_HOST_DIR:?}" \
  "${SOFASCORE_HISTORY_GW_STATE_HOST_DIR:?}" "${SOFASCORE_PLAYERS_GW_STATE_HOST_DIR:?}" \
  "${SOFASCORE_PLATFORM_ENV_FILE:?}" "${SOFASCORE_HOST_PYTHON:?}"

# Имя дерева: release-<digest8>[-<gitsha8>]; digest — идентичность runtime-контракта
# (канарейка и артефакт), sha различает деревья с одинаковым контрактом (правка только
# рецепта/мини-DAG). Рабочий каталог канарейки — по digest: тот же контракт = та же VERIFIED.
TAG=$(basename "$RELEASE" | sed -e 's/^release-//' -e 's/-.*$//')
WORKSPACE="$SOFASCORE_RUNTIME_DIR/canary-$TAG"
ARTIFACT="$WORKSPACE/candidate.json"
LOG="$SOFASCORE_RUNTIME_DIR/all-men/deploy.log"
SCHED_COMPOSE="$RELEASE/deploy/sofascore/airflow.compose.yaml"
GW_COMPOSE="$RELEASE/deploy/sofascore/gateway.compose.yaml"
CAMPAIGN="$SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR"
STATE="$SOFASCORE_GATEWAY_STATE_HOST_DIR"
HIST=dag_backfill_sofascore_all_mens
REFRESH=dag_refresh_sofascore_all_mens
DAILY=dag_ingest_sofascore
# Три полосы источника (#1244): свой шлюз, свой пул, свой сторож аренд у каждой.
GATEWAYS="sofascore_proxy_filter sofascore_gw_history sofascore_gw_players"
GATEWAY_CONTAINERS="sofascore_gw_951 sofascore_gw_history sofascore_gw_players"
WATCHDOG_UNITS="sofascore-gw-lease-watchdog.service
sofascore-gw-lease-watchdog-history.service
sofascore-gw-lease-watchdog-players.service"
PSQL="docker exec sofascore-airflow-metadb psql -U airflow -d airflow -At -c"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }
set_env_var() {  # set_env_var KEY VALUE — переписать строку KEY= в env-файле контура
  local key="$1" value="$2"
  grep -qE "^${key}=" "$ENV_FILE" || { echo "в $ENV_FILE нет строки ${key}=" >&2; exit 2; }
  sed -i "s#^${key}=.*#${key}=${value}#" "$ENV_FILE"
}
is_paused() { $PSQL "SELECT is_paused FROM dag WHERE dag_id='$1';"; }
pause_dag() {
  docker exec sofascore-airflow-scheduler airflow dags pause "$1" >> "$LOG" 2>&1
  [ "$(is_paused "$1")" = "t" ] || { log "$1 did not pause"; exit 7; }
}
# Любой аварийный выход после паузы: вернуть актуалку в прежнее состояние и сказать,
# на каком шаге встали (история остаётся на паузе, как и при штатном выкате).
REFRESH_WAS_PAUSED=""
STEP="start"
on_exit() {
  local rc=$? state
  [ "$rc" -eq 0 ] && return 0
  # Внутри trap ничего не должно оборвать откат: set -e снимаем, каждый шаг — best effort.
  set +e
  log "FAILED at step '$STEP' (rc=$rc); env file: $ENV_FILE — проверь, какое дерево там записано"
  if [ "$REFRESH_WAS_PAUSED" = "f" ]; then
    # Сначала штатно через scheduler; если он сам лежит (упал recreate/health) —
    # напрямую в метабазе контура одной строкой (то же, что делает `airflow dags unpause`).
    docker exec sofascore-airflow-scheduler airflow dags unpause "$REFRESH" >> "$LOG" 2>&1
    state=$(is_paused "$REFRESH" 2>/dev/null)
    if [ "$state" != "f" ]; then
      $PSQL "UPDATE dag SET is_paused=false WHERE dag_id='$REFRESH';" >> "$LOG" 2>&1
      state=$(is_paused "$REFRESH" 2>/dev/null)
    fi
    if [ "$state" = "f" ]; then
      log "$REFRESH unpaused back after failure"
    else
      log "MANUAL ACTION REQUIRED: $REFRESH is still paused (paused='${state:-?}') — unpause it by hand"
    fi
  fi
  exit "$rc"
}
trap on_exit EXIT

[ -f "$WORKSPACE/VERIFIED" ] || { echo "нет VERIFIED в $WORKSPACE" >&2; exit 2; }
[ -f "$SCHED_COMPOSE" ] && [ -f "$GW_COMPOSE" ] || { echo "в $RELEASE нет deploy/sofascore/*.compose.yaml" >&2; exit 2; }
hour=$(date -u +%H%M)
if [ "$hour" -ge 1355 ] && [ "$hour" -le 1535 ]; then echo "окно дейли 14:00–15:30 UTC — позже" >&2; exit 3; fi

# Пересоздание scheduler'а обрывает любой идущий таск, поэтому на паузу — обе кампании.
# Актуалка после выката возвращается в то состояние, в каком была; история остаётся
# на паузе до ручного решения (как и раньше).
STEP="pause"
REFRESH_WAS_PAUSED=$(is_paused "$REFRESH")
log "pause $HIST and $REFRESH (refresh was paused=$REFRESH_WAS_PAUSED), wait for idle"
pause_dag "$HIST"
pause_dag "$REFRESH"
while true; do
  busy=$($PSQL "SELECT count(*) FROM task_instance WHERE dag_id IN ('$DAILY','$HIST','$REFRESH') AND state IN ('queued','running');")
  active=$($PSQL "SELECT count(*) FROM dag_run WHERE dag_id IN ('$DAILY','$HIST','$REFRESH') AND state IN ('queued','running');")
  [ "$busy" = "0" ] && [ "$active" = "0" ] && break
  sleep 60
done
log "idle"

STEP="digest"
DIGEST=$(PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$RELEASE" "$SOFASCORE_HOST_PYTHON" -B -c \
  'from scrapers.sofascore.runtime_fingerprint import runtime_fingerprint; print(runtime_fingerprint()["digest"])')
CANDIDATE_DIGEST=$(python3 -c "import json; print(json.load(open('$ARTIFACT'))['runtime_fingerprint']['digest'])")
[ "$DIGEST" = "$CANDIDATE_DIGEST" ] || { echo "digest дерева $DIGEST != кандидата $CANDIDATE_DIGEST" >&2; exit 4; }
[ "${DIGEST:0:8}" = "$TAG" ] || { echo "имя дерева не совпадает с digest" >&2; exit 4; }

STEP="artifact"
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

STEP="preflight"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$RELEASE" "$SOFASCORE_HOST_PYTHON" -B \
  "$RELEASE/scripts/sofascore_runtime_preflight.py" preflight \
    --release-root "$RELEASE" --artifact "$ARTIFACT_DEST" --state-dir "$STATE" \
    --campaign-dir "$CAMPAIGN" --campaign-policy "$RELEASE/configs/sofascore/all_mens_campaign.json" \
    --expected-artifact-id "$ARTIFACT_ID" >> "$LOG" 2>&1
log "preflight ok"

# Единственный источник истины о бое — env-файл контура; compose и сторож читают его.
# Значения этого выката передаются compose и явно (окружение процесса сильнее
# --env-file — так старое значение никогда не перекроет новое).
STEP="repin-env"
set_env_var SOFASCORE_RELEASE_ROOT "$RELEASE"
set_env_var SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST "$ARTIFACT_DEST"
set_env_var SOFASCORE_PROXY_BUDGET_ARTIFACT_ID "$ARTIFACT_ID"
sofascore_load_env "$ENV_FILE"
[ "$SOFASCORE_RELEASE_ROOT" = "$RELEASE" ] || { log "env file did not take the new release root"; exit 2; }
log "env file $ENV_FILE repinned to $RELEASE"

STEP="scheduler-up"
SOFASCORE_RELEASE_ROOT="$RELEASE" \
SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST="$ARTIFACT_DEST" \
SOFASCORE_PROXY_BUDGET_ARTIFACT_ID="$ARTIFACT_ID" \
docker compose -p sofascore-airflow -f "$SCHED_COMPOSE" \
  --env-file "$SOFASCORE_PLATFORM_ENV_FILE" --env-file "$ENV_FILE" \
  up -d --no-deps --force-recreate airflow-scheduler >> "$LOG" 2>&1
log "scheduler up"

STEP="gateway-up"
SOFASCORE_RELEASE_ROOT="$RELEASE" \
SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST="$ARTIFACT_DEST" \
SOFASCORE_PROXY_BUDGET_ARTIFACT_ID="$ARTIFACT_ID" \
docker compose -p sofascore-gw -f "$GW_COMPOSE" \
  --project-directory "$RELEASE" \
  --env-file "$SOFASCORE_PLATFORM_ENV_FILE" --env-file "$ENV_FILE" \
  up -d --no-deps --force-recreate $GATEWAYS >> "$LOG" 2>&1
log "gateways up: $GATEWAYS"

STEP="gateway-health"
# 10 минут на шлюз: 5 минут не хватило 25.08, healthcheck успел стать healthy на 30 с
# позже выхода. Лимит памяти проверяется, а не только логируется: `docker update
# --memory 1g` уже терялся при пересоздании (23.08), а на 1 GiB рассчитан порог WAL.
for gw in $GATEWAY_CONTAINERS; do
  for _ in $(seq 1 60); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "$gw" 2>/dev/null)" = "healthy" ] && break
    sleep 10
  done
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$gw")" = "healthy" ] || { log "$gw unhealthy"; exit 5; }
  mem=$(docker inspect -f '{{.HostConfig.Memory}}' "$gw")
  [ "$mem" = "1073741824" ] || { log "$gw HostConfig.Memory=$mem (ожидание 1073741824)"; exit 5; }
  log "$gw healthy; HostConfig.Memory=$mem"
  # `|| true`: пустой лог за 10 минут — не повод обрывать выкат (pipefail + grep=1).
  docker logs "$gw" --since 10m 2>&1 | grep -E "residential pool|paid_enabled|compacted|listening|SofaScore paid leases disabled" | tail -8 | tee -a "$LOG" || true
done

STEP="scheduler-health"
docker exec sofascore-airflow-scheduler python /opt/airflow/scripts/sofascore_runtime_preflight.py scheduler-health \
  --artifact /opt/airflow/runtime/sofascore/proxy_budget_canary.json \
  --health-url http://sofascore_proxy_filter:8899/health \
  --campaign-dir /opt/airflow/runtime/sofascore/all-men \
  --campaign-policy /opt/airflow/configs/sofascore/all_mens_campaign.json >> "$LOG" 2>&1
for _ in $(seq 1 30); do
  errs=$($PSQL "SELECT count(*) FROM import_error;")
  present=$($PSQL "SELECT count(*) FROM dag WHERE dag_id IN ('$HIST','$REFRESH','$DAILY') AND is_active=true;")
  [ "$present" = "3" ] && break
  sleep 10
done
log "dags active=$present import_errors=$errs"
[ "$errs" = "0" ] || { log "import errors present — см. import_error"; exit 6; }
[ "$present" = "3" ] || { log "expected 3 active core DAGs, got $present"; exit 6; }

STEP="pools"
# Пулы полос заводит airflow-init, но deploy.sh пересоздаёт только scheduler и шлюзы —
# на ротации init не запускается. Без этого шага задачи полосы повисли бы в
# несуществующем пуле. `airflow pools set` идемпотентен: создаёт или переставляет слоты.
set_pool() {  # set_pool <name> <slots> <description>
  docker exec sofascore-airflow-scheduler airflow pools set "$1" "$2" "$3" >> "$LOG" 2>&1
}
HISTORY_SLOTS="${SOFASCORE_HISTORY_POOL_SLOTS:-1}"
PLAYERS_SLOTS="${SOFASCORE_PLAYERS_POOL_SLOTS:-1}"
set_pool ingest_scraper_pool 1 'Serialize heavy ingest scrapers (isolated sofascore stack #951)'
set_pool sofascore_history_pool "$HISTORY_SLOTS" 'SofaScore history lane'
set_pool sofascore_players_pool "$PLAYERS_SLOTS" 'SofaScore players lane'
log "pools set: ingest_scraper_pool=1 sofascore_history_pool=$HISTORY_SLOTS sofascore_players_pool=$PLAYERS_SLOTS"

STEP="restore-pause"
pause_dag "$HIST"
if [ "$REFRESH_WAS_PAUSED" = "f" ]; then
  docker exec sofascore-airflow-scheduler airflow dags unpause "$REFRESH" >> "$LOG" 2>&1
  [ "$(is_paused "$REFRESH")" = "f" ] || { log "$REFRESH did not unpause"; exit 7; }
  log "history kept paused; $REFRESH unpaused (restored)"
else
  log "history kept paused; $REFRESH kept paused (as before)"
fi

STEP="watchdog"
# Сторож аренд читает тот же env-файл (EnvironmentFile= в unit) — достаточно рестарта.
# Свой unit на каждый шлюз: сторож смотрит один контейнер и один каталог состояния.
while IFS= read -r unit; do
  systemctl restart "$unit"
  log "watchdog restarted on $RELEASE: $unit $(systemctl is-active "$unit")"
done <<< "$WATCHDOG_UNITS"
log "DONE artifact_id=$ARTIFACT_ID runtime=$DIGEST"
