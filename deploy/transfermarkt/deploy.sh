#!/usr/bin/env bash
# Выкат замороженного дерева на контур Transfermarkt (проекты transfermarkt-airflow /
# transfermarkt-gw, #1387). Образец — deploy/sofascore/deploy.sh, без drain: доставка идёт
# только когда в своей метабазе нет running-прогона TM-DAG (иначе код 4, бой не тронут);
# перед остановкой шлюза все четыре DAG встают на паузу и простой подтверждается ещё раз.
# Использование: bash deploy/transfermarkt/deploy.sh <release-root> [old-release-root]
#   Тот же скрипт из СТАРОГО дерева — это и есть откат (auto_deliver.sh).
# Переменные — из $TRANSFERMARKT_ENV_FILE (по умолчанию /etc/data-platform/transfermarkt.env);
#   скрипт сам переписывает в нём TRANSFERMARKT_RELEASE_ROOT — этот файл и есть
#   единственный источник «какое дерево в бою» (compose, автомат, приёмка).
# Коды возврата: 2 — предпосылки; 4 — контур занят или замок выката занят, выкат не
#   начат; 5 — шлюз; 6 — импорт DAG / приёмка; 7 — паузы.
set -euo pipefail

RELEASE="${1:?путь к замороженному дереву}"
OLD_RELEASE="${2:-}"
ENV_FILE="${TRANSFERMARKT_ENV_FILE:-/etc/data-platform/transfermarkt.env}"
# shellcheck source=deploy/transfermarkt/env.sh
. "$(dirname "$0")/env.sh"
transfermarkt_load_env "$ENV_FILE" || exit 2
: "${TRANSFERMARKT_RUNTIME_DIR:?}" "${TRANSFERMARKT_RELEASES_DIR:?}" \
  "${TRANSFERMARKT_PLATFORM_ENV_FILE:?}" "${TRANSFERMARKT_PROXY_POOL_FILE:?}"

LOG="$TRANSFERMARKT_RUNTIME_DIR/deploy.log"
SCHED_COMPOSE="$RELEASE/deploy/transfermarkt/airflow.compose.yaml"
GW_COMPOSE="$RELEASE/deploy/transfermarkt/gateway.compose.yaml"
SCHED=transfermarkt-airflow-scheduler
METADB=transfermarkt-airflow-metadb
GW=transfermarkt_gw
GW_STATE="$TRANSFERMARKT_RUNTIME_DIR/gateway-state"
INGEST=dag_ingest_transfermarkt
DISCOVER=dag_discover_transfermarkt_registry
BACKFILL=dag_backfill_transfermarkt
SILVER=dag_transform_transfermarkt_silver
TM_DAGS_SQL="'$INGEST','$DISCOVER','$BACKFILL','$SILVER'"
# Таймаут обязателен: зависший `docker exec` держал бы выкат (и автомат из cron) вечно.
PSQL="timeout -k 5 ${TRANSFERMARKT_DEPLOY_METADB_TIMEOUT:-30} docker exec $METADB psql -U airflow -d airflow -At -c"
# Леджер шлюза старше стольких дней уходит в gzip-архив на доставке (решение 9 #1387).
LEDGER_MAX_AGE_DAYS="${TRANSFERMARKT_LEDGER_MAX_AGE_DAYS:-30}"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }
# t / f; «-» — DAG ещё не зарегистрирован (первый подъём: scheduler'а в этой метабазе не
# было); пусто — метабаза не ответила.
is_paused() { $PSQL "SELECT coalesce((SELECT CASE WHEN is_paused THEN 't' ELSE 'f' END FROM dag WHERE dag_id='$1'),'-');"; }
set_pool() {  # set_pool <name> <slots> <description>
  timeout -k 5 60 docker exec "$SCHED" airflow pools set "$1" "$2" "$3" >> "$LOG" 2>&1 8>&-
}
set_pause() {  # set_pause <dag_id> <t|f>
  local verb=pause
  [ "$2" = f ] && verb=unpause
  timeout -k 5 60 docker exec "$SCHED" airflow dags "$verb" "$1" >> "$LOG" 2>&1 8>&- || true
  # CLI-приёмке не верим — только SELECT по метабазе.
  [ "$(is_paused "$1")" = "$2" ] || { log "$1 не встал в paused=$2"; exit 7; }
}

STEP="start"
# Паузы до выката: при коде 4 (бой не тронут) возвращаются как были.
declare -A WAS_PAUSED=()
on_exit() {
  local rc=$? d
  [ "$rc" -eq 0 ] && return 0
  set +e
  log "FAILED at step '$STEP' (rc=$rc); env file: $ENV_FILE — проверь, какое дерево там записано"
  if [ "$rc" -eq 4 ]; then
    for d in "${!WAS_PAUSED[@]}"; do
      [ "${WAS_PAUSED[$d]}" = f ] || continue
      timeout -k 5 60 docker exec "$SCHED" airflow dags unpause "$d" >> "$LOG" 2>&1 8>&-
      [ "$(is_paused "$d")" = f ] && log "$d unpaused back (nothing deployed)" \
        || log "MANUAL ACTION REQUIRED: $d is still paused — unpause it by hand"
    done
  fi
  exit "$rc"
}
trap on_exit EXIT

STEP="preflight"
[ -d "$TRANSFERMARKT_RUNTIME_DIR" ] || { echo "нет runtime-каталога $TRANSFERMARKT_RUNTIME_DIR" >&2; exit 2; }
case "$RELEASE" in
  "$TRANSFERMARKT_RELEASES_DIR"/release-*) ;;
  *) echo "дерево $RELEASE вне каталога релизов $TRANSFERMARKT_RELEASES_DIR" >&2; exit 2 ;;
esac
[ "$(readlink -f "$RELEASE")" = "$RELEASE" ] || { echo "путь дерева не канонический: $RELEASE" >&2; exit 2; }
[ -f "$SCHED_COMPOSE" ] && [ -f "$GW_COMPOSE" ] && [ -f "$RELEASE/deploy/transfermarkt/.airflowignore" ] \
  || { echo "в $RELEASE нет deploy/transfermarkt/{airflow,gateway}.compose.yaml или .airflowignore" >&2; exit 2; }
for d in "$TRANSFERMARKT_RUNTIME_DIR/logs" "$GW_STATE"; do
  [ -d "$d" ] && [ ! -L "$d" ] || { echo "каталог состояния не на месте: $d" >&2; exit 2; }
done
[ -r "$TRANSFERMARKT_PLATFORM_ENV_FILE" ] || { echo "нет общего .env платформы: $TRANSFERMARKT_PLATFORM_ENV_FILE" >&2; exit 2; }
[ -f "$TRANSFERMARKT_PROXY_POOL_FILE" ] && [ -s "$TRANSFERMARKT_PROXY_POOL_FILE" ] \
  || { echo "файл пула шлюза пуст или отсутствует: $TRANSFERMARKT_PROXY_POOL_FILE" >&2; exit 2; }

STEP="lock"
transfermarkt_deploy_lock_init || exit 2
if [ -n "${TRANSFERMARKT_DEPLOY_LOCK_FD:-}" ]; then
  [ "/proc/self/fd/$TRANSFERMARKT_DEPLOY_LOCK_FD" -ef "$TRANSFERMARKT_DEPLOY_LOCK" ] \
    || { echo "TRANSFERMARKT_DEPLOY_LOCK_FD=$TRANSFERMARKT_DEPLOY_LOCK_FD ведёт не на замок выката $TRANSFERMARKT_DEPLOY_LOCK" >&2; exit 2; }
  flock -n "$TRANSFERMARKT_DEPLOY_LOCK_FD" || { echo "унаследованный замок выката не берётся" >&2; exit 2; }
else
  lock_rc=0
  transfermarkt_take_deploy_lock 8 || lock_rc=$?
  [ "$lock_rc" = 2 ] && exit 2
  if [ "$lock_rc" = 1 ]; then
    log "замок выката занят ($TRANSFERMARKT_DEPLOY_LOCK) — выкат не начат, бой не тронут"
    exit 4
  fi
fi

STEP="idle"
# Драйна нет (решение 9 #1387): TM-DAG идут раз в сутки в 04:00 UTC, окно доставки
# 01:00–03:00. Идущий прогон не обрываем — выходим с 4. Пустой ответ = «не знаю».
running=$($PSQL "SELECT count(*) FROM dag_run WHERE dag_id IN ($TM_DAGS_SQL) AND state='running';" || true)
[ -n "$running" ] || { log "метабаза $METADB не ответила про идущие прогоны — nothing deployed"; exit 4; }
[ "$running" = 0 ] || { log "идёт прогон TM ($running running) — nothing deployed"; exit 4; }
# Закрыть новые запуски: все четыре DAG на паузу (прогон паузного DAG планировщик не
# двигает), затем подтвердить простой ещё раз — ран мог стартовать между проверкой и паузой.
for d in "$INGEST" "$DISCOVER" "$BACKFILL" "$SILVER"; do
  WAS_PAUSED[$d]=$(is_paused "$d" || true)
  case "${WAS_PAUSED[$d]}" in
    t|f) ;;
    -) unset 'WAS_PAUSED[$d]'; log "$d ещё не зарегистрирован (первый подъём) — паузить нечего" ;;
    *) log "метабаза не ответила про паузу $d — nothing deployed"; unset 'WAS_PAUSED[$d]'; exit 4 ;;
  esac
done
for d in "${!WAS_PAUSED[@]}"; do
  timeout -k 5 60 docker exec "$SCHED" airflow dags pause "$d" >> "$LOG" 2>&1 8>&- || true
  [ "$(is_paused "$d")" = t ] || { log "$d не встал на паузу — nothing deployed"; exit 4; }
done
busy=$($PSQL "SELECT (SELECT count(*) FROM dag_run WHERE dag_id IN ($TM_DAGS_SQL) AND state='running') + (SELECT count(*) FROM task_instance WHERE dag_id IN ($TM_DAGS_SQL) AND state IN ('queued','running','restarting'));" || true)
[ "$busy" = 0 ] || { log "после паузы контур не пуст ('${busy:-нет ответа}') — nothing deployed"; exit 4; }
log "idle: running-прогонов TM нет, четыре DAG на паузе; deploy $RELEASE (old: ${OLD_RELEASE:--})"

STEP="repin-env"
transfermarkt_set_env_var "$ENV_FILE" TRANSFERMARKT_RELEASE_ROOT "$RELEASE"
transfermarkt_load_env "$ENV_FILE"
[ "$TRANSFERMARKT_RELEASE_ROOT" = "$RELEASE" ] || { log "env file did not take the new release root"; exit 2; }
log "env file $ENV_FILE repinned to $RELEASE"

STEP="gateway-stop"
TRANSFERMARKT_RELEASE_ROOT="$RELEASE" TRANSFERMARKT_PROXY_POOL_JSON=unused \
docker compose -p transfermarkt-gw -f "$GW_COMPOSE" --project-directory "$RELEASE" \
  --env-file "$TRANSFERMARKT_PLATFORM_ENV_FILE" --env-file "$ENV_FILE" \
  stop "$GW" >> "$LOG" 2>&1 8>&- || { log "gateway stop failed"; exit 5; }

STEP="ledger-rotate"
# Шлюз остановлен — единственный писатель леджера молчит. В режиме transfermarkt-only
# у леджера нет HMAC-цепочки (она только у whoscored-only), новый файл шлюз создаст сам.
LEDGER="$GW_STATE/paid_requests.jsonl"
if [ -f "$LEDGER" ] && [ -s "$LEDGER" ]; then
  first_at=$(head -n 1 "$LEDGER" | sed -n 's/.*"occurred_at":"\([^"]*\)".*/\1/p')
  first_s=$(date -u -d "$first_at" +%s 2>/dev/null || echo "")
  if [ -n "$first_s" ] && [ $(( $(date -u +%s) - first_s )) -gt $(( LEDGER_MAX_AGE_DAYS * 86400 )) ]; then
    archive="$GW_STATE/paid_requests.$(date -u +%Y%m%dT%H%M%SZ).jsonl.gz"
    # Цепочка `&&` под `set -e` не роняет скрипт: сбой gzip молча оставил бы .tmp и
    # выкат шёл бы дальше. Сбой — код 5 (шлюз уже остановлен), леджер не тронут.
    if ! ( gzip -c "$LEDGER" > "$archive.tmp" && chmod 0600 "$archive.tmp" \
           && mv -f "$archive.tmp" "$archive" && rm -f "$LEDGER" ); then
      rm -f "$archive.tmp"
      log "ledger archive failed: $LEDGER не тронут, выкат остановлен"
      exit 5
    fi
    log "ledger archived: $archive (первая запись $first_at)"
  fi
fi

STEP="gateway-up"
# Пул Decodo — только в окружение процесса compose: не в лог и не в env-файл.
TRANSFERMARKT_RELEASE_ROOT="$RELEASE" \
TRANSFERMARKT_PROXY_POOL_JSON="$(cat "$TRANSFERMARKT_PROXY_POOL_FILE")" \
docker compose -p transfermarkt-gw -f "$GW_COMPOSE" --project-directory "$RELEASE" \
  --env-file "$TRANSFERMARKT_PLATFORM_ENV_FILE" --env-file "$ENV_FILE" \
  up -d --no-deps --force-recreate "$GW" >> "$LOG" 2>&1 8>&- || { log "gateway up failed"; exit 5; }
log "gateway up: $GW"

STEP="gateway-health"
for _ in $(seq 1 60); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$GW" 2>/dev/null)" = "healthy" ] && break
  sleep 5
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$GW")" = "healthy" ] || { log "$GW unhealthy"; exit 5; }
mem=$(docker inspect -f '{{.HostConfig.Memory}}' "$GW")
[ "$mem" = "1073741824" ] || { log "$GW HostConfig.Memory=$mem (ожидание 1073741824)"; exit 5; }
log "$GW healthy; HostConfig.Memory=$mem"

STEP="scheduler-up"
TRANSFERMARKT_RELEASE_ROOT="$RELEASE" \
docker compose -p transfermarkt-airflow -f "$SCHED_COMPOSE" --project-directory "$RELEASE" \
  --env-file "$TRANSFERMARKT_PLATFORM_ENV_FILE" --env-file "$ENV_FILE" \
  up -d --no-deps --force-recreate airflow-scheduler >> "$LOG" 2>&1 8>&- || { log "scheduler up failed"; exit 6; }
STARTED=$(docker inspect -f '{{.State.StartedAt}}' "$SCHED")
log "scheduler up (StartedAt $STARTED)"

STEP="pools"
# airflow-init при ротации не идёт — пулы ставим каждый выкат (размеры как в init).
for _ in $(seq 1 30); do
  timeout -k 5 60 docker exec "$SCHED" airflow pools list >/dev/null 2>&1 8>&- && break
  sleep 5
done
set_pool ingest_scraper_pool 1 'Serialize heavy ingest scrapers to avoid VM swap (#671)'
set_pool transfermarkt_proxy 1 'Transfermarkt production and registry proxy work'
set_pool transfermarkt_backfill_proxy 1 'Transfermarkt historical backfill only; bounded dedicated proxy slot'
set_pool transfermarkt_backfill_control 1 'Transfermarkt historical planning and DQ only; isolated from daily ingest'
log "pools set: ingest_scraper_pool=1 transfermarkt_proxy=1 transfermarkt_backfill_proxy=1 transfermarkt_backfill_control=1"

STEP="acceptance"
present=""; errs=""
for _ in $(seq 1 60); do
  errs=$($PSQL "SELECT count(*) FROM import_error;" || true)
  present=$($PSQL "SELECT count(*) FROM dag WHERE dag_id IN ($TM_DAGS_SQL) AND is_active AND NOT has_import_errors AND last_parsed_time > TIMESTAMPTZ '$STARTED';" || true)
  [ "$present" = "4" ] && break
  sleep 10
done
log "dags parsed after start=$present import_errors=$errs"
[ "$errs" = "0" ] || { log "import errors present — см. import_error"; exit 6; }
[ "$present" = "4" ] || { log "expected 4 parsed TM DAGs, got '$present'"; exit 6; }
# Живой SchedulerJob: healthcheck контейнера — `airflow jobs check` по heartbeat.
for _ in $(seq 1 30); do
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$SCHED" 2>/dev/null)" = "healthy" ] && break
  sleep 10
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$SCHED")" = "healthy" ] || { log "$SCHED unhealthy"; exit 6; }
# /health шлюза изнутри планировщика: режим transfermarkt-only, суточного бюджета нет.
health_rc=0
health=$(transfermarkt_gateway_health_ok "$SCHED" 8>&-) || health_rc=$?
log "gateway /health from scheduler: $health"
[ "$health_rc" = 0 ] || { log "gateway /health не прошёл приёмку"; exit 6; }
# Все монты из каталога релизов ведут в НОВОЕ дерево (scheduler ≥ 5, шлюз ≥ 1).
for pair in "$SCHED:5" "$GW:1"; do
  c=${pair%%:*}; want=${pair##*:}
  got=$(transfermarkt_mounts_in "$c" "$TRANSFERMARKT_RELEASES_DIR" "$RELEASE" "$want" 8>&-)
  [ "$got" = 1 ] || { log "монты $c ведут не в $RELEASE"; exit 6; }
done
log "mounts of $SCHED and $GW lead into $RELEASE"

STEP="pauses"
# Паузы — как были до выката: ручная пауза оператора выкат не снимает. DAG, которого до
# выката в метабазе не было (первый подъём), получает дефолт решения 10 #1387:
# ingest/discover работают, backfill/silver — на паузе. Откат возвращает паузы из
# снимка — это делает автомат после этого скрипта.
declare -A DEFAULT_PAUSED=(["$INGEST"]=f ["$DISCOVER"]=f ["$BACKFILL"]=t ["$SILVER"]=t)
summary=""
for d in "$INGEST" "$DISCOVER" "$BACKFILL" "$SILVER"; do
  want="${WAS_PAUSED[$d]:-${DEFAULT_PAUSED[$d]}}"
  set_pause "$d" "$want"
  summary="$summary $d=$want"
done
log "pauses:$summary"
log "DONE release=$RELEASE"
