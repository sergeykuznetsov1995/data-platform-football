#!/usr/bin/env bash
# Выкат замороженного дерева на контур SofaScore (проекты sofascore-airflow / sofascore-gw).
# Использование: bash deploy/sofascore/deploy.sh <release-root> [old-release-root]
# Предпосылки: окно вне 13:55–15:35 UTC;
#   обе кампании (история и актуалка) ставятся на паузу и ждут idle здесь же,
#   актуалка после выката возвращается в прежнее состояние.
# Переменные — из $SOFASCORE_ENV_FILE (по умолчанию /etc/data-platform/sofascore.env);
#   скрипт сам переписывает в нём SOFASCORE_RELEASE_ROOT / _PROXY_BUDGET_ARTIFACT_HOST / _ID —
#   этот файл и есть единственный источник «какое дерево в бою» (compose, сторож, приёмка).
# Коды возврата: 2 — предпосылки; 3 — окно дейли; 4 — контур занят, выкат не начат; 5 — шлюзы;
#   6 — импорт DAG; 7 — паузы.
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

# Каждый шлюз — ЕДИНСТВЕННЫЙ писатель своего WAL/ledger. Два пути к ОДНОМУ каталогу
# (в том числе через симлинк) свели бы полосы вместе, и приёмка бы это пропустила:
# ожидаемые пути она берёт из того же env-файла. Дешёвая проверка — здесь, до паузы
# кампаний и любого пересоздания; полную (каноничность, владелец, доступ UID 50000,
# защищённая цепочка родителей, вне дерева релиза) делает preflight ниже — по каждому
# из трёх каталогов, а не только по каталогу актуалки.
LANE_STATE_DIRS="$SOFASCORE_GATEWAY_STATE_HOST_DIR
$SOFASCORE_HISTORY_GW_STATE_HOST_DIR
$SOFASCORE_PLAYERS_GW_STATE_HOST_DIR"
lane_canonical=""
while IFS= read -r lane_dir; do
  [ -d "$lane_dir" ] || { echo "каталог состояния полосы не существует: $lane_dir" >&2; exit 2; }
  lane_canonical="$lane_canonical$(readlink -f "$lane_dir")
"
done <<< "$LANE_STATE_DIRS"
[ "$(printf '%s' "$lane_canonical" | sort -u | wc -l)" = "3" ] \
  || { echo "каталоги состояния полос указывают на один каталог: $(echo $lane_canonical)" >&2; exit 2; }

# Имя дерева: release-<sha8> (исторические деревья — release-<digest8>-<gitsha8>).
# TAG — первый сегмент после release-; он именует каталог артефакта этого выката.
# Политика бюджета едет в самом дереве: платного замера больше нет (#1245).
TAG=$(basename "$RELEASE" | sed -e 's/^release-//' -e 's/-.*$//')
ARTIFACT="$RELEASE/configs/sofascore/workload_policy.json"
LOG="$SOFASCORE_RUNTIME_DIR/all-men/deploy.log"
SCHED_COMPOSE="$RELEASE/deploy/sofascore/airflow.compose.yaml"
GW_COMPOSE="$RELEASE/deploy/sofascore/gateway.compose.yaml"
CAMPAIGN="$SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR"
HIST=dag_backfill_sofascore_all_mens
REFRESH=dag_refresh_sofascore_all_mens
DAILY=dag_ingest_sofascore
# Три полосы источника (#1244): свой шлюз, свой пул, свой сторож аренд у каждой.
GATEWAYS="sofascore_proxy_filter sofascore_gw_history sofascore_gw_players"
GATEWAY_CONTAINERS="sofascore_gw_951 sofascore_gw_history sofascore_gw_players"
WATCHDOG_UNITS="sofascore-gw-lease-watchdog.service
sofascore-gw-lease-watchdog-history.service
sofascore-gw-lease-watchdog-players.service"
# Таймаут обязателен: зависший `docker exec` в шаге ожидания держал бы выкат вечно,
# а автомат ночной доставки (#1245) зовёт этот скрипт из cron.
PSQL="timeout -k 5 ${SOFASCORE_DEPLOY_METADB_TIMEOUT:-30} docker exec sofascore-airflow-metadb psql -U airflow -d airflow -At -c"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" | tee -a "$LOG"; }
is_paused() { $PSQL "SELECT is_paused FROM dag WHERE dag_id='$1';"; }
set_pool() {  # set_pool <name> <slots> <description>
  docker exec sofascore-airflow-scheduler airflow pools set "$1" "$2" "$3" >> "$LOG" 2>&1
}
# Источник слотов — env-файл, а не таблица slot_pool: чтение живого значения дало бы
# гонку с ручным выкатом (второй выкат прочитал бы осушённый 0 и «восстановил» пул в 0).
HISTORY_SLOTS="${SOFASCORE_HISTORY_POOL_SLOTS:-1}"
PLAYERS_SLOTS="${SOFASCORE_PLAYERS_POOL_SLOTS:-1}"
wait_idle() {  # wait_idle <секунд>; 0 — контур свободен, 1 — потолок исчерпан
  # Потолок держится по ЧАСАМ, а не по сумме sleep: каждая итерация делает два запроса к
  # метабазе с таймаутом до SOFASCORE_DEPLOY_METADB_TIMEOUT секунд каждый, и на недоступной
  # метабазе счётчик «минус 30 за виток» растянул бы 5400 с почти на 4,5 часа. Витки тоже
  # ограничены: часы могут стоять (заглушка стенда, съехавший NTP), и тогда без этого
  # предела цикл стал бы вечным.
  local deadline tries busy active
  deadline=$(( $(date -u +%s) + $1 ))
  tries=$(( $1 / 30 + 1 ))
  while :; do
    # Пустой ответ (метабаза недоступна / timeout) — это «не знаю», а не «свободно».
    busy=$($PSQL "SELECT count(*) FROM task_instance WHERE dag_id IN ('$DAILY','$HIST','$REFRESH') AND state IN ('queued','running');" || true)
    active=$($PSQL "SELECT count(*) FROM dag_run WHERE dag_id IN ('$DAILY','$HIST','$REFRESH') AND state IN ('queued','running');" || true)
    [ "${busy:-x}" = "0" ] && [ "${active:-x}" = "0" ] && return 0
    tries=$(( tries - 1 ))
    [ "$tries" -le 0 ] && return 1
    [ "$(date -u +%s)" -ge "$deadline" ] && return 1
    sleep 30
    # Ещё раз ПОСЛЕ сна и ДО нового витка: иначе, перешагнув потолок во сне, цикл успел бы
    # начать новую пару запросов с таймаутами по 30 с каждый и выйти за него на целый виток.
    [ "$(date -u +%s)" -ge "$deadline" ] && return 1
  done
}
# Прогон истории, оставшийся без задач, планировщик не закроет: паузные прогоны не попадают
# в next_dagruns_to_examine (DagModel.is_paused == false), а dagrun_timeout проверяется только
# в _schedule_dag_run. ORM внутри планировщика, а не UPDATE в метабазе: set_state сам проставит
# end_date и сохранит матрицу переходов. Строго одна физическая строка и ни одной одинарной
# кавычки внутри: перевод строки порвал бы журнал вызовов в тестах, отступ дал бы IndentationError.
close_stale_runs() {  # close_stale_runs <dag_id>
  timeout -k 5 60 docker exec sofascore-airflow-scheduler python -c 'import sys; from airflow.models import DagRun; from airflow.utils.state import DagRunState; from airflow import settings; s = settings.Session(); print("closed stale dag_run", [(r.dag_id, r.run_id, r.set_state(DagRunState.FAILED))[:2] for r in s.query(DagRun).filter(DagRun.dag_id == sys.argv[1], DagRun.state.in_(["queued", "running"])).all()]); s.commit(); s.close()' "$1" >> "$LOG" 2>&1
}
pause_dag() {
  docker exec sofascore-airflow-scheduler airflow dags pause "$1" >> "$LOG" 2>&1
  [ "$(is_paused "$1")" = "t" ] || { log "$1 did not pause"; exit 7; }
}
# Любой аварийный выход после паузы: вернуть актуалку в прежнее состояние и сказать,
# на каком шаге встали (история остаётся на паузе, как и при штатном выкате).
REFRESH_WAS_PAUSED=""; HIST_WAS_PAUSED=""; POOL_DRAINED=""
STEP="start"
on_exit() {
  local rc=$? state
  [ "$rc" -eq 0 ] && return 0
  # Внутри trap ничего не должно оборвать откат: set -e снимаем, каждый шаг — best effort.
  set +e
  log "FAILED at step '$STEP' (rc=$rc); env file: $ENV_FILE — проверь, какое дерево там записано"
  # Осушённый пул возвращаем при ЛЮБОМ обрыве: штатный шаг pools стоит после gateway-health
  # и scheduler-health, до него выкат может не дойти — и полоса истории осталась бы с нулём
  # слотов без единого сообщения.
  if [ -n "$POOL_DRAINED" ]; then
    if set_pool sofascore_history_pool "$HISTORY_SLOTS" 'SofaScore history lane'; then
      log "sofascore_history_pool restored to $HISTORY_SLOTS slots"
    else
      log "MANUAL ACTION REQUIRED: sofascore_history_pool left drained — airflow pools set sofascore_history_pool $HISTORY_SLOTS 'SofaScore history lane'"
    fi
  fi
  # rc=4 — «контур занят, выкат не начат»: паузу истории тоже возвращаем как было.
  # На прочих кодах история остаётся на паузе, как и при штатном выкате.
  if [ "$rc" -eq 4 ] && [ "$HIST_WAS_PAUSED" = "f" ]; then
    docker exec sofascore-airflow-scheduler airflow dags unpause "$HIST" >> "$LOG" 2>&1
    [ "$(is_paused "$HIST")" = "f" ] \
      && log "$HIST unpaused back (contour busy, nothing deployed)" \
      || log "MANUAL ACTION REQUIRED: $HIST is still paused — unpause it by hand"
  fi
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

[ -s "$ARTIFACT" ] || { echo "в $RELEASE нет configs/sofascore/workload_policy.json" >&2; exit 2; }
[ -f "$SCHED_COMPOSE" ] && [ -f "$GW_COMPOSE" ] || { echo "в $RELEASE нет deploy/sofascore/*.compose.yaml" >&2; exit 2; }
hour=$(date -u +%H%M)
if [ "$hour" -ge 1355 ] && [ "$hour" -le 1535 ]; then echo "окно дейли 14:00–15:30 UTC — позже" >&2; exit 3; fi

# Пересоздание scheduler'а обрывает любой идущий таск, поэтому контур сначала осушается.
# Актуалка после выката возвращается в то состояние, в каком была; история остаётся
# на паузе до ручного решения (как и раньше).
IDLE_WAIT="${SOFASCORE_DEPLOY_IDLE_WAIT:-5400}"

STEP="drain"
# История идёт @continuous: в окне выката у неё ВСЕГДА есть прогон. Дверь новым скоупам
# закрывает ПУЛ, а не пауза: в sofascore_history_pool сидит ровно одна задача —
# run_historical_scope; задача без слота остаётся `scheduled` и не попадает ни в 'queued',
# ни в 'running'. Паузу истории ставим ПОСЛЕ ожидания: под паузой не выполнится
# validate_historical_scope, а он единственный засчитывает скоуп в state.json — новый прогон
# получил бы новый run_id и купил те же 8–81 минуты платного трафика заново.
REFRESH_WAS_PAUSED=$(is_paused "$REFRESH")
HIST_WAS_PAUSED=$(is_paused "$HIST")
log "drain: sofascore_history_pool -> 0 slots, pause $REFRESH (was paused=$REFRESH_WAS_PAUSED), wait up to ${IDLE_WAIT}s"
set_pool sofascore_history_pool 0 'SofaScore history lane (drained for deploy)'
POOL_DRAINED=1
pause_dag "$REFRESH"
wait_idle "$IDLE_WAIT" || { log "contour still busy after ${IDLE_WAIT}s — nothing deployed"; exit 4; }

STEP="pause"
pause_dag "$HIST"
close_stale_runs "$HIST"
wait_idle 120 || { log "history dag_run did not close"; exit 4; }
log "idle"

STEP="artifact"
# Артефакт бюджета = статическая политика из дерева (#1245). Копия неизменяема и
# переживает ротацию дерева: её sha256 — тот самый artifact_id, которым шлюз и
# клиент склеивают подписанные планы, WAL и ledger.
ARTIFACT_DEST="$SOFASCORE_RUNTIME_DIR/artifacts/$TAG/workload_policy.json"
mkdir -p "$(dirname "$ARTIFACT_DEST")"
cp "$ARTIFACT" "$ARTIFACT_DEST"
chmod 0644 "$ARTIFACT_DEST"
ARTIFACT_ID=$(sha256sum "$ARTIFACT_DEST" | awk '{print $1}')
log "artifact $ARTIFACT_DEST id=$ARTIFACT_ID"

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
# Каталог кампании растёт вместе с ней (тысячи файлов): без потолка `chown` мог бы
# оказаться самым долгим шагом выката, который никто не ограничивает.
timeout -k 5 120 chown -R 50000:0 "$CAMPAIGN"
chmod 0750 "$CAMPAIGN"
chmod 0644 "$CAMPAIGN/snapshot.json"

STEP="preflight"
# По каталогу на полосу: preflight — единственная проверка, которая знает про UID 50000,
# каноничность пути и запрет жить внутри дерева релиза. Прогон от root с `test -w`
# её не заменяет: шлюз пишет не под root.
while IFS= read -r lane_dir; do
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$RELEASE" "$SOFASCORE_HOST_PYTHON" -B \
    "$RELEASE/scripts/sofascore_runtime_preflight.py" preflight \
      --release-root "$RELEASE" --artifact "$ARTIFACT_DEST" --state-dir "$lane_dir" \
      --campaign-dir "$CAMPAIGN" --campaign-policy "$RELEASE/configs/sofascore/all_mens_campaign.json" \
      --expected-artifact-id "$ARTIFACT_ID" >> "$LOG" 2>&1
  log "preflight ok: $lane_dir"
done <<< "$LANE_STATE_DIRS"

# Единственный источник истины о бое — env-файл контура; compose и сторож читают его.
# Значения этого выката передаются compose и явно (окружение процесса сильнее
# --env-file — так старое значение никогда не перекроет новое).
STEP="repin-env"
sofascore_set_env_var "$ENV_FILE" SOFASCORE_RELEASE_ROOT "$RELEASE"
sofascore_set_env_var "$ENV_FILE" SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST "$ARTIFACT_DEST"
sofascore_set_env_var "$ENV_FILE" SOFASCORE_PROXY_BUDGET_ARTIFACT_ID "$ARTIFACT_ID"
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
set_pool ingest_scraper_pool 1 'Serialize heavy ingest scrapers (isolated sofascore stack #951)'
set_pool sofascore_history_pool "$HISTORY_SLOTS" 'SofaScore history lane'
set_pool sofascore_players_pool "$PLAYERS_SLOTS" 'SofaScore players lane'
log "pools set: ingest_scraper_pool=1 sofascore_history_pool=$HISTORY_SLOTS sofascore_players_pool=$PLAYERS_SLOTS"
POOL_DRAINED=""   # слоты вернулись штатно — позднему обрыву возвращать нечего

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
log "DONE artifact_id=$ARTIFACT_ID release=$RELEASE"
