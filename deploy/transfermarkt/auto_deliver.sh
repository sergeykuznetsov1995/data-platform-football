#!/bin/bash
# Автомат доставки Transfermarkt (#1387, #2039): в любой час, когда полоса сбора свободна и до
# ближайшего прогона по расписанию хватает запаса, сам ставит master в бой своего контура
# (transfermarkt-airflow / transfermarkt-gw), принимает результат, при любой поломке ОТКАТЫВАЕТ
# и докладывает в Telegram. Часового окна нет (гриль TM 30.10, решение 3: «в любой час с паузой
# полосы»); ночное окно — лишь частный случай свободной полосы.
# Логика — копия deploy/sofascore/auto_deliver.sh (замок, очередь алертов, защёлка на sha,
# выключатель .off, маркеры INFLIGHT/ACCEPTED, записи попыток, самообновление копии)
# с заменами #1387/#2039:
#   * дренаж полосы (ingest + backfill): на паузу — только на границе порций, когда в контуре
#     нет running-прогона и задач в работе (пауза посреди порции заморозила бы её прогон —
#     Airflow 2 не двигает прогоны паузного DAG). Паузы ДО дренажа — в $DRAIN; дренаж
#     многотиковый (заморозка, самоустановка), его прерывание возвращает паузы из файла;
#   * откат — deploy.sh СТАРОГО дерева (он принимает любое дерево контура) плюс возврат
#     пауз четырёх DAG и слотов пулов из снимка;
#   * приёмка — 4 DAG + 1 шлюз, привязана к факту пересоздания scheduler'а (.Created);
#   * режим --drill-rollback: копия принятого дерева с намеренно сломанным
#     dags/dag_ingest_transfermarkt.py (только в копии, не в git) проходит штатную
#     доставку, приёмка падает, откат обязан подтвердиться — «откат проверен».
#
# Cron: */5 * * * * /usr/local/libexec/transfermarkt/auto_deliver.sh   (запас и одноразовость — внутри)
# Все пути машины — из env-файла контура (deploy/transfermarkt/transfermarkt.env.example).
set -u
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

DRILL=0
case "${1:-}" in
  --drill-rollback) DRILL=1 ;;
  '') ;;
  *) echo "использование: $0 [--drill-rollback]" >&2; exit 2 ;;
esac

HERE=$(dirname "$(readlink -f "$0")")
SELF=$(readlink -f "${BASH_SOURCE[0]}")
md5_of(){ md5sum "$1" 2>/dev/null | cut -c1-32; }
# md5 ИСПОЛНЯЕМОГО экземпляра (bash читает скрипт по дескриптору 255): после установки
# новой копии на диск работающий процесс всё ещё старый.
case "$(readlink "/proc/$$/fd/255" 2>/dev/null)" in
  "$SELF"|"$SELF (deleted)") RUN_MD5=$(md5_of "/proc/$$/fd/255") ;;
  *) RUN_MD5=$(md5_of "$SELF") ;;
esac
ENV_FILE="${TRANSFERMARKT_ENV_FILE:-/etc/data-platform/transfermarkt.env}"
RUN_ENV_MD5=""
if { exec {ENV_SH_FD}<"$HERE/env.sh"; } 2>/dev/null; then
  RUN_ENV_MD5=$(md5_of "/proc/$$/fd/$ENV_SH_FD")
  . "/proc/$$/fd/$ENV_SH_FD"
  exec {ENV_SH_FD}<&-
else
  . "$HERE/env.sh"
fi
transfermarkt_load_env "$ENV_FILE" || exit 2
export TRANSFERMARKT_ENV_FILE="$ENV_FILE"   # тот же файл читают freeze_release.sh и deploy.sh
STATE=${TRANSFERMARKT_AUTO_STATE_DIR:?TRANSFERMARKT_AUTO_STATE_DIR не задан в env-файле}
LOG=${TRANSFERMARKT_AUTO_LOG:?TRANSFERMARKT_AUTO_LOG не задан в env-файле}
TG_ENV=${TRANSFERMARKT_TG_ENV:?TRANSFERMARKT_TG_ENV не задан в env-файле}
LIVE_ROOT=${TRANSFERMARKT_RELEASE_ROOT:?TRANSFERMARKT_RELEASE_ROOT не задан в env-файле}
SOURCE_REPO=${TRANSFERMARKT_SOURCE_REPO:?TRANSFERMARKT_SOURCE_REPO не задан в env-файле}
RELEASES_DIR=${TRANSFERMARKT_RELEASES_DIR:?TRANSFERMARKT_RELEASES_DIR не задан в env-файле}
PLATFORM_ENV=${TRANSFERMARKT_PLATFORM_ENV_FILE:?TRANSFERMARKT_PLATFORM_ENV_FILE не задан в env-файле}
METADB=transfermarkt-airflow-metadb
SCHED=transfermarkt-airflow-scheduler
GW=transfermarkt_gw
LOCK=$STATE/transfermarkt-auto-deliver.lock
PENDING=$STATE/transfermarkt-pending-alert     # недоставленные алерты, дожимаются каждым тиком
OFF=$STATE/transfermarkt-auto-deliver.off      # выключатель: ставит человек или сам автомат
INFLIGHT=$STATE/transfermarkt-inflight         # доставка начата и ещё не закрыта
ACCEPTED=$STATE/transfermarkt-accepted         # sha кода, приёмку которого подтвердили
SNAPSHOT=$STATE/transfermarkt-rollback.env     # состояние боя до доставки: цель отката
TODAY=$(date -u +%F)
# Защёлка — на целевой sha (#2039, как SofaScore #1980): одна попытка на коммит master; путь
# известен после чтения master (шаг 4).
ATTEMPTED=""
BEHIND=$STATE/transfermarkt-behind-since       # с какого момента бой не на master (epoch)
DRAIN=$STATE/transfermarkt-drain.env           # дренаж полосы идёт: паузы полосы ДО него
# Запись попытки: transfermarkt-window-<UTC-дата>T<ЧЧММ>.env (старые — <дата>); WIN — открытая.
WIN=""
POOLS="transfermarkt_control transfermarkt_proxy transfermarkt_backfill_proxy transfermarkt_backfill_control"
CORE_DAGS="dag_ingest_transfermarkt dag_discover_transfermarkt_registry dag_backfill_transfermarkt dag_transform_transfermarkt_silver"
# Полоса — непрерывный сбор порциями (TransfermarktCurrentTimetable): у неё next_dagrun ≈ now,
# дедлайну она не годится — её закрывает пауза дренажа.
LANE_DAGS="dag_ingest_transfermarkt dag_backfill_transfermarkt"
# Дедлайн — ближайший запуск внешних DAG по расписанию (registry и DAG полосы не на непрерывном
# расписании — их добавляет lane_refresh; silver — trigger-only, расписания нет).
NEXT_RUN_DAGS="'dag_discover_transfermarkt_registry'"
NEXT_RUN_COUNT=1
LANE=""
OUTER_DAGS_SQL=""
DEADLINE_MAX=${DEADLINE_MAX:-10800}  # не дальше 3 ч вперёд; не меньше DRAIN_MAX + NEED_BUDGET
LEGACY_WINDOW_TO=0300                # дедлайн старых записей окна (до #2039): <дата> 03:00 UTC
DEPLOY_CEILING=${DEPLOY_CEILING:-1800}  # потолок deploy.sh: шлюз 300 + пулы 150 + DAG 600 + scheduler 300 + запас
ACCEPT_WAIT=${ACCEPT_WAIT:-480}
ACCEPT_POLL=${ACCEPT_POLL:-20}
METADB_TIMEOUT=${METADB_TIMEOUT:-30}
FAIL_NIGHTS_MAX=${FAIL_NIGHTS_MAX:-3}   # провальных попыток подряд до выключателя (имя — прежнее)
DRAIN_MAX=${DRAIN_MAX:-3600}       # дренаж дольше часа (порция 45 мин + запас) — прерывается
DRAIN_POLL=${DRAIN_POLL:-240}      # внутри тика ждём конца порции до 4 мин (тик — раз в 5 мин)
DRAIN_POLL_STEP=${DRAIN_POLL_STEP:-15}
ROLLBACK_RESERVE=${ROLLBACK_RESERVE:-600}  # зазор до прогона registry сверх NEED_BUDGET (в нём уже откат)
BUSY_RETRY=${BUSY_RETRY:-21600}    # полоса не освободилась / rc=4: тот же sha — не раньше чем через 6 ч
CORE_DAGS_SQL=$(for d in $CORE_DAGS; do printf "'%s'," "$d"; done); CORE_DAGS_SQL=${CORE_DAGS_SQL%,}
# Запас до дедлайна: доставка и откат — каждый с потолком deploy.sh, его `timeout -k 30`
# и приёмкой (плюс один опрос сверх её дедлайна), и общий резерв на снимок, возврат
# пауз/пулов, сбор логов и запросы к метабазе с их таймаутами.
BUDGET_RESERVE=${BUDGET_RESERVE:-600}
NEED_BUDGET=$(( 2 * (DEPLOY_CEILING + 30 + ACCEPT_WAIT + ACCEPT_POLL) + BUDGET_RESERVE ))

log(){
  if [ -L "$LOG" ] || { [ -e "$LOG" ] && [ ! -f "$LOG" ]; }; then
    echo "$(date -u +%FT%TZ) [лог подменён] $*" >&2
    return 0
  fi
  echo "$(date -u +%FT%TZ) $*" >> "$LOG"
}
is_plain(){ [ -f "$1" ] && [ ! -L "$1" ]; }
odd_path(){ { [ -e "$1" ] || [ -L "$1" ]; } && ! is_plain "$1"; }
mk_marker(){
  if odd_path "$1"; then log "на месте $1 не обычный файл — не пишу туда ничего"; return 1; fi
  { : > "$1"; } 2>/dev/null && is_plain "$1"
}
said_today(){ is_plain "$1"; }
mark_said(){ mk_marker "$1" || log "не смог поставить отметку $1 — сообщение повторится следующим заходом"; }
set_off(){ mk_marker "$OFF" || log "ВЫКЛЮЧАТЕЛЬ $OFF НЕ ЗАПИСАН — следующий тик не остановится"; }
tg(){
  local text="$*" resp
  if [ ! -f "$TG_ENV" ]; then log "АЛЕРТ НЕ ДОСТАВЛЕН (нет $TG_ENV): $text"; return 1; fi
  . "$TG_ENV"
  if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
    log "АЛЕРТ НЕ ДОСТАВЛЕН (нет токена/чата): $text"; return 1
  fi
  resp=$(curl -s --max-time 15 "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d chat_id="${TELEGRAM_CHAT_ID}" --data-urlencode text="[$(hostname)] ${text}" 2>/dev/null)
  case "$resp" in *'"ok":true'*) return 0 ;; esac
  log "АЛЕРТ НЕ ДОСТАВЛЕН (ответ: $(printf '%s' "$resp" | head -c 200)): $text"
  return 1
}
# Ни один исход не теряется: не ушло в Telegram — в очередь; нет и очереди — глушим себя.
tg_durable(){
  tg "$@" && return 0
  if ! odd_path "$PENDING" \
     && { printf '%s\n' "$*" >> "$PENDING"; } 2>/dev/null \
     && is_plain "$PENDING" && grep -qxF -e "$*" "$PENDING" 2>/dev/null; then
    log "алерт отложен в очередь ($PENDING) — дожму следующим заходом"
    return 0
  fi
  log "АЛЕРТ ПОТЕРЯН (ни Telegram, ни очередь $PENDING недоступны): $*"
  set_off
  exit 1
}
flush_pending(){
  if odd_path "$PENDING"; then
    log "ОЧЕРЕДЬ АЛЕРТОВ ПОДМЕНЕНА ($PENDING — не обычный файл); глушу автомат"
    tg "Transfermarkt: на месте очереди алертов ($PENDING) не обычный файл — отложенные сообщения потеряны. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
    exit 1
  fi
  { is_plain "$PENDING" && [ -s "$PENDING" ]; } || return 0
  local line kept="$PENDING.kept" failed=0
  if ! mk_marker "$kept"; then
    log "не могу создать $kept — очередь оставляю как есть, разберу позже"
    local m="$STATE/transfermarkt-kept-broken-$TODAY"
    if ! said_today "$m"; then
      tg "Transfermarkt: не могу разобрать очередь отложенных алертов ($kept не создаётся). НУЖНЫ РУКИ. Лог: $LOG" && mark_said "$m"
    fi
    return 0
  fi
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    tg "$line" && continue
    if ! { { printf '%s\n' "$line" >> "$kept"; } 2>/dev/null \
           && grep -qxF -e "$line" "$kept" 2>/dev/null; }; then
      failed=1
    fi
  done < "$PENDING"
  if [ "$failed" = 1 ]; then
    log "АЛЕРТ ПОТЕРЯН при разборе очереди: не могу переписать $PENDING — оставляю очередь как есть"
    rm -f "$kept" 2>/dev/null
    set_off
    exit 1
  fi
  if [ -s "$kept" ]; then
    mv -f "$kept" "$PENDING" 2>/dev/null || { log "не могу заменить $PENDING — очередь оставляю как есть"; rm -f "$kept" 2>/dev/null; }
  else
    rm -f "$kept" "$PENDING" 2>/dev/null
    log "очередь отложенных алертов разобрана"
  fi
}
lock_alert(){
  local mark="$STATE/transfermarkt-lock-broken-$TODAY"
  log "НЕ МОГУ ВЗЯТЬ ЗАМОК: $1 — автомат не работает"
  said_today "$mark" && return 0
  if tg "Transfermarkt: автомат не может взять свой замок ($1) — доставки НЕ БУДЕТ. НУЖНЫ РУКИ. Лог: $LOG"; then
    mark_said "$mark"
  fi
}
# X — метабаза не ответила («не знаю», а не «нет»).
metadb(){
  local out
  if ! out=$(timeout -k 5 "$METADB_TIMEOUT" docker exec "$METADB" psql -U airflow -d airflow -tA -c "$1" 2>/dev/null); then
    echo X; return
  fi
  printf '%s' "$out" | tr -d ' \r'
}
sched(){ timeout -k 5 60 docker exec "$SCHED" "$@" >> "$LOG" 2>&1 8>&- 9>&-; }
inspect(){ timeout -k 5 30 docker inspect "$@" 2>/dev/null; }
snap_get(){ sed -n "s/^$1=//p" "$SNAPSHOT" 2>/dev/null | head -1; }
rollback_pools(){
  local pools p value
  pools=$(snap_get OLD_POOLS)
  if [ -n "$pools" ]; then
    for p in $pools; do
      case "$p" in ingest_scraper_pool|transfermarkt_control|transfermarkt_proxy|transfermarkt_backfill_proxy|transfermarkt_backfill_control) ;;
        *) return 1 ;; esac
    done
    printf '%s' "$pools"
  else
    # Older snapshots already contain the old release's actual pool rows.
    # An absent new pool is not part of that release's rollback contract.
    for p in ingest_scraper_pool $POOLS; do
      value=$(snap_get "POOL_$p")
      case "$value" in ''|*[!0-9]*) ;; *) printf '%s ' "$p" ;; esac
    done
  fi
}
pool_want(){  # rollback uses only persisted values, never a helper in old code
  local w
  w=$(snap_get "POOL_$1")
  case "$w" in
    ''|*[!0-9]*) return 1 ;;
    *) printf '%s' "$w" ;;
  esac
}
pool_desc(){
  case "$1" in
    transfermarkt_control) printf '%s' 'Transfermarkt current planning and DQ only' ;;
    transfermarkt_proxy) printf '%s' 'Transfermarkt production and registry proxy work' ;;
    transfermarkt_backfill_proxy) printf '%s' 'Transfermarkt historical backfill only; bounded dedicated proxy slot' ;;
    *) printf '%s' 'Transfermarkt historical planning and DQ only; isolated from daily ingest' ;;
  esac
}
paused_key(){ printf 'PAUSED_%s' "$1"; }

# Паузы четырёх DAG возвращаются к снимку. Слоты OLD восстанавливаются точно;
# после успешного deploy NEW сохраняет свои TM-пулы по контракту streams.
RESTORE_PENDING=""
RESTORE_NOTE=""
ROLLBACK_NOTE=""
restore_pause(){  # restore_pause <dag_id> <t|f>
  local dag="$1" want="$2" now
  case "$want" in t) sched airflow dags pause "$dag" ;; f) sched airflow dags unpause "$dag" ;; *) return 0 ;; esac
  now=$(metadb "SELECT is_paused FROM dag WHERE dag_id='$dag';")
  [ "$now" = "$want" ] && { log "$dag: пауза вернулась к '$want'"; return 0; }
  RESTORE_NOTE="$RESTORE_NOTE $dag остался в состоянии paused='$now' вместо '$want';"
  log "MANUAL ACTION REQUIRED: $dag paused='$now', ожидалось '$want'"
  return 1
}
restore_state(){
  local release="${1:-old}"
  case "$release" in old|new) ;; *) return 1 ;; esac
  [ -n "$RESTORE_PENDING" ] || return 0
  RESTORE_PENDING=""
  RESTORE_NOTE=""
  local pool want dag
  for pool in $(rollback_pools); do
    if [ "$release" = new ]; then
      # deploy.sh уже применил canonical TM slots; старый снимок не должен
      # включать history admission в новом H0. Общий ingest-пул сохраняем.
      case " $POOLS " in *" $pool "*) continue ;; esac
    fi
    want=$(snap_get "POOL_$pool")
    [ -n "$want" ] || continue
    if ! sched airflow pools set "$pool" "$want" "$(pool_desc "$pool")"; then
      RESTORE_NOTE="$RESTORE_NOTE пул $pool не вернулся к $want слотам;"
      log "MANUAL ACTION REQUIRED: пул $pool не вернулся к $want слотам"
    fi
  done
  for dag in $CORE_DAGS; do
    restore_pause "$dag" "$(snap_get "$(paused_key "$dag")")" || true
  done
  [ -z "$RESTORE_NOTE" ]
}
trap restore_state EXIT

mounts_all_in(){  # mounts_all_in <дерево>: scheduler ≥ 5 монтов из дерева, шлюз ≥ 1
  local a b
  a=$(transfermarkt_mounts_in "$SCHED" "$RELEASES_DIR" "$1" 5)
  b=$(transfermarkt_mounts_in "$GW" "$RELEASES_DIR" "$1" 1)
  case "$a$b" in *X*) echo X ;; 11) echo 1 ;; *) echo 0 ;; esac
}
gateway_health_ok(){  # 1 / 0 / X по пробе transfermarkt_gateway_health_ok из env.sh
  local rc=0
  transfermarkt_gateway_health_ok "$SCHED" >/dev/null 2>&1 8>&- 9>&- || rc=$?
  case "$rc" in 0) echo 1 ;; 1) echo 0 ;; *) echo X ;; esac
}
# Приёмка — шесть признаков: 4 DAG перечитаны ПОСЛЕ старта этого scheduler'а и без ошибок
# импорта; scheduler healthy (heartbeat SchedulerJob); шлюз healthy на 1 GiB в проекте transfermarkt-gw; монты scheduler'а и шлюза в
# этом дереве; /health шлюза в режиме transfermarkt-only; NEW-пулы по streams,
# OLD-пулы точно как в снимке. Ответ остаётся 1/0/X; причина отказа идёт в лог.
acceptance_result(){
  log "ACCEPTANCE result=$1 predicate=$2 ${3:-}"
  printf '%s\n' "$1"
}
acceptance_seen(){  # acceptance_seen <дерево> <StartedAt scheduler'а>
  local new="$1" started="$2" dags errs gw got c want
  dags=$(metadb "SELECT count(*) FROM dag WHERE dag_id IN ($CORE_DAGS_SQL) AND is_active AND NOT has_import_errors AND last_parsed_time > TIMESTAMPTZ '$started';")
  errs=$(metadb "SELECT count(*) FROM import_error;")
  case "$dags:$errs" in *X*|*[!0-9:]*) acceptance_result X dag_parse read_failed; return ;; esac
  [ -n "$dags" ] && [ -n "$errs" ] || { acceptance_result X dag_parse missing_counts; return; }
  [ "$dags" = 4 ] && [ "$errs" = 0 ] || { acceptance_result 0 dag_parse "parsed=$dags import_errors=$errs"; return; }
  # Живой SchedulerJob: healthcheck контейнера — `airflow jobs check` по heartbeat.
  got=$(inspect -f '{{.State.Health.Status}}' "$SCHED") || { acceptance_result X scheduler_health inspect_failed; return; }
  [ "$got" = healthy ] || { acceptance_result 0 scheduler_health "observed=$got"; return; }
  gw=$(inspect -f '{{.State.Health.Status}} {{.HostConfig.Memory}} {{index .Config.Labels "com.docker.compose.project"}}' "$GW") || { acceptance_result X gateway_container inspect_failed; return; }
  [ "$gw" = "healthy 1073741824 transfermarkt-gw" ] || { acceptance_result 0 gateway_container "observed=$gw"; return; }
  got=$(mounts_all_in "$new")
  [ "$got" = X ] && { acceptance_result X mounts inspect_failed; return; }
  [ "$got" = 1 ] || { acceptance_result 0 mounts "expected_tree=$new"; return; }
  got=$(gateway_health_ok)
  [ "$got" = X ] && { acceptance_result X gateway_health probe_failed; return; }
  [ "$got" = 1 ] || { acceptance_result 0 gateway_health probe_rejected; return; }
  local acceptance_pools="$POOLS"
  if [ "$new" = "$(snap_get OLD_RELEASE_ROOT)" ]; then
    acceptance_pools=$(rollback_pools) || { acceptance_result X pool_slots invalid_snapshot; return; }
    [ -n "$acceptance_pools" ] || { acceptance_result X pool_slots missing_snapshot; return; }
  fi
  for c in $acceptance_pools; do
    got=$(metadb "SELECT slots FROM slot_pool WHERE pool='$c';")
    case "$got" in X|*[!0-9]*) acceptance_result X pool_slots "pool=$c read_failed"; return ;; esac
    [ -n "$got" ] || { acceptance_result 0 pool_slots "pool=$c missing"; return; }
    if [ "$new" = "$(snap_get OLD_RELEASE_ROOT)" ]; then
      want=$(pool_want "$c") || { acceptance_result X pool_slots "pool=$c invalid_snapshot"; return; }
    else
      want=$(timeout -k 5 30 docker exec "$SCHED" python -m scrapers.transfermarkt.airflow_pools --get "$c") || { acceptance_result X pool_slots "pool=$c helper_failed"; return; }
    fi
    case "$want" in ''|*[!0-9]*) acceptance_result X pool_slots "pool=$c invalid_helper_output"; return ;; esac
    [ "$got" = "$want" ] || { acceptance_result 0 pool_slots "pool=$c observed=$got expected=$want"; return; }
  done
  echo 1
}
env_file_value(){ sed -n "s/^$1=//p" "$ENV_FILE" 2>/dev/null | head -1; }
snapshot_ok(){
  local p d
  for p in OLD_RELEASE_ROOT NEW_RELEASE_ROOT; do
    [ -n "$(snap_get "$p")" ] || { log "снимок отката неполон: нет $p"; return 1; }
  done
  for d in $CORE_DAGS; do
    case "$(snap_get "$(paused_key "$d")")" in
      t|f) ;;
      *) log "снимок отката неполон: пауза $d не прочитана"; return 1 ;;
    esac
  done
  local old_pools
  old_pools=$(rollback_pools) || return 1
  [ -n "$old_pools" ] || { log "снимок отката неполон: нет старого контракта пулов"; return 1; }
  for p in $old_pools; do
    case "$(snap_get "POOL_$p")" in
      ''|*[!0-9]*) log "снимок отката неполон: слоты пула $p не число"; return 1 ;;
    esac
  done
  return 0
}
# Контур пуст — тот же предикат, что idle-проверка deploy.sh: running-прогонов TM нет и задач
# в queued/running/restarting нет. Число; X — метабаза не ответила.
contour_busy(){
  metadb "SELECT (SELECT count(*) FROM dag_run WHERE dag_id IN ($CORE_DAGS_SQL) AND state='running') + (SELECT count(*) FROM task_instance WHERE dag_id IN ($CORE_DAGS_SQL) AND state IN ('queued','running','restarting'));"
}
# Прогоны внешних DAG (registry, silver, полоса не на непрерывном расписании): дренаж их не
# закрывает, выкат оборвал бы их.
outer_busy(){  # OUTER_DAGS_SQL — из lane_refresh
  metadb "SELECT count(*) FROM dag_run WHERE dag_id IN ($OUTER_DAGS_SQL) AND state IN ('queued','running');"
}

# --- дренаж полосы (#2039) ---------------------------------------------------------------
# Флаг для расписания TM (TransfermarktCurrentTimetable.DRAIN_FLAG, в контейнерах —
# /opt/airflow/gateway-state/delivery-drain-until): epoch «до когда» новые порции не стартуют;
# идущая дорабатывает свой допуск. Просрочка — страховка на случай смерти автомата.
drain_flag(){ printf '%s/gateway-state/delivery-drain-until' "${TRANSFERMARKT_RUNTIME_DIR:-}"; }
flag_until(){ head -c 32 "$(drain_flag)" 2>/dev/null | tr -cd '0-9'; }
flag_set(){  # атомарно: tmp + mv, 0644 — планировщик читает его под uid 50000
  local f
  f=$(drain_flag)
  if odd_path "$f" || odd_path "$f.tmp"; then log "на месте флага дренажа $f не обычный файл"; return 1; fi
  if { printf '%s\n' "$(( $(date -u +%s) + DRAIN_MAX ))" > "$f.tmp" && chmod 0644 "$f.tmp" && mv -f "$f.tmp" "$f"; } 2>/dev/null; then
    return 0
  fi
  rm -f "$f.tmp" 2>/dev/null
  log "флаг дренажа $f не записан"
  return 1
}
flag_clear(){ is_plain "$(drain_flag)" && { rm -f "$(drain_flag)" 2>/dev/null || log "флаг дренажа $(drain_flag) не убран — истечёт сам"; }; return 0; }
# Дренируемая часть полосы — DAG на непрерывном расписании TM (флаг выше останавливает только
# его). DAG полосы на другом расписании (ingest без квалификации — cron 04:00, прогоны 4–12 ч)
# считается внешним: его прогон не дренируем, а ждём, его ближайший запуск входит в дедлайн.
# Выставляет LANE, OUTER_DAGS_SQL, NEXT_RUN_DAGS, NEXT_RUN_COUNT; 1 — метабаза не ответила.
lane_refresh(){
  local got d sql=""
  for d in $LANE_DAGS; do sql="$sql,'$d'"; done
  got=$(metadb "SELECT 'L' || coalesce(string_agg(dag_id, ',' ORDER BY dag_id), '') FROM dag WHERE dag_id IN (${sql#,}) AND timetable_description LIKE 'Continuous Transfermarkt current%';")
  case "$got" in L*) ;; *) return 1 ;; esac
  LANE=${got#L}; LANE=${LANE//,/ }
  OUTER_DAGS_SQL="'dag_discover_transfermarkt_registry','dag_transform_transfermarkt_silver'"
  NEXT_RUN_DAGS="'dag_discover_transfermarkt_registry'"; NEXT_RUN_COUNT=1
  for d in $LANE_DAGS; do
    case " $LANE " in *" $d "*) continue ;; esac
    OUTER_DAGS_SQL="$OUTER_DAGS_SQL,'$d'"; NEXT_RUN_DAGS="$NEXT_RUN_DAGS,'$d'"
    NEXT_RUN_COUNT=$(( NEXT_RUN_COUNT + 1 ))
  done
}
# Файл $DRAIN: TARGET, STARTED_AT (epoch), LANE (дренируемые DAG); после паузы — PAUSED=1 и
# PAUSED_<dag>=t|f — паузы ДО неё. Пишется вместе с флагом, убирается после возврата пауз или
# когда снимок отката уже несёт эти значения.
drain_get(){ sed -n "s/^$1=//p" "$DRAIN" 2>/dev/null | head -1; }
drain_write(){  # drain_write KEY=VAL… — файл целиком, атомарно
  if { printf '%s\n' "$@" > "$DRAIN.tmp" && mv -f "$DRAIN.tmp" "$DRAIN"; } 2>/dev/null; then return 0; fi
  rm -f "$DRAIN.tmp" 2>/dev/null
  log "файл дренажа $DRAIN не записан"
  return 1
}
drain_release(){  # флаг убрать, паузы полосы вернуть из файла, файл убрать; 1 — не вернулись
  local d want
  flag_clear
  RESTORE_NOTE=""
  if [ "$(drain_get PAUSED)" = 1 ]; then
    for d in $(drain_get LANE); do
      want=$(drain_get "$(paused_key "$d")")
      case "$want" in
        t|f) restore_pause "$d" "$want" || true ;;
        *) RESTORE_NOTE="$RESTORE_NOTE пауза $d до дренажа не прочитана из $DRAIN;" ;;
      esac
    done
  fi
  [ -z "$RESTORE_NOTE" ] || return 1
  rm -f "$DRAIN"
}
# Прервать дренаж (если он идёт): флаг убрать, паузы полосы — как до него. Не вернулись, а срок
# дренажа (STARTED_AT + DRAIN_MAX) не истёк — файл остаётся, следующий тик повторит (моргнувшая
# метабаза не повод для рук). Срок истёк — сбор стоял бы молча: выключатель и руки (запись и
# алерт — раз в сутки: под выключателем каждый тик повторяет попытку).
drain_abort(){  # drain_abort <причина>
  local m="$STATE/transfermarkt-drain-stuck-$TODAY" started
  is_plain "$DRAIN" || { flag_clear; return 0; }
  if drain_release; then
    log "дренаж прерван ($1): флаг убран, паузы полосы возвращены"
    return 0
  fi
  started=$(drain_get STARTED_AT)
  case "$started" in ''|*[!0-9]*) started=0 ;; esac
  if [ $(( $(date -u +%s) - started )) -lt "$DRAIN_MAX" ]; then
    log "дренаж прерван ($1), но паузы полосы не вернулись:$RESTORE_NOTE — повторю следующим тиком (срок дренажа до $(( started + DRAIN_MAX )))"
    window_note "паузы полосы не вернулись — повтор следующим тиком"
    exit 0
  fi
  log "ДРЕНАЖ ПРЕРВАН ($1), НО ПАУЗЫ ПОЛОСЫ НЕ ВЕРНУЛИСЬ:$RESTORE_NOTE — глушу автомат"
  set_off
  said_today "$m" && exit 1
  hands_close "" "дренаж прерван ($1), паузы полосы не вернулись:$RESTORE_NOTE" f
  tg_durable "Transfermarkt: дренаж полосы прерван ($1), но паузы ingest/backfill не вернулись к прежним —$RESTORE_NOTE Сбор стоит. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  mark_said "$m"
  exit 1
}
# Начать дренаж: файл (цель, старт, дренируемые DAG), затем флаг для расписания.
drain_begin(){
  drain_write "TARGET=$WANT" "STARTED_AT=$(date -u +%s)" "LANE=$LANE" || { window_note "файл дренажа не записан"; return 1; }
  if ! flag_set; then
    rm -f "$DRAIN"
    window_note "флаг дренажа не записан"
    return 1
  fi
  log "ДРЕНАЖ: флаг до $(flag_until) — новые порции ($LANE) не стартуют, цель ${WANT:0:8}"
}
# Пауза дренируемой полосы на границе порций: паузы ДО неё — в файл, затем пауза с проверкой по
# метабазе. 1 — не встала (дренаж прерван, всё возвращено).
drain_pause(){
  local d v kv=()
  for d in $(drain_get LANE); do
    v=$(metadb "SELECT is_paused FROM dag WHERE dag_id='$d';")
    case "$v" in
      t|f) kv+=("$(paused_key "$d")=$v") ;;
      *) drain_abort "пауза $d не прочитана ('$v')"
         window_note "пауза $d не прочитана — дренаж прерван"
         return 1 ;;
    esac
  done
  drain_write "TARGET=$(drain_get TARGET)" "STARTED_AT=$(drain_get STARTED_AT)" "LANE=$(drain_get LANE)" \
    "PAUSED=1" "${kv[@]}" || { drain_abort "файл дренажа не записан"; return 1; }
  log "ДРЕНАЖ: ставлю на паузу $(drain_get LANE) (до дренажа: ${kv[*]})"
  for d in $(drain_get LANE); do
    sched airflow dags pause "$d"
    [ "$(metadb "SELECT is_paused FROM dag WHERE dag_id='$d';")" = t ] && continue
    drain_abort "$d не встал на паузу"
    window_note "$d не встал на паузу — дренаж отменён"
    return 1
  done
}
# Защёлка sha с пометкой busy: повтор того же sha — только после BUSY_RETRY. Атомарно: пустая
# защёлка после обрыва записи заперла бы этот sha навсегда.
latch_busy(){
  if ! { printf 'busy %s\n' "$(date -u +%s)" > "$ATTEMPTED.tmp" && mv -f "$ATTEMPTED.tmp" "$ATTEMPTED"; } 2>/dev/null; then
    log "не смог пометить защёлку $ATTEMPTED как busy — снимаю её, повтор со следующего свободного тика"
    rm -f "$ATTEMPTED" "$ATTEMPTED.tmp"
  fi
}
# Откат: deploy.sh СТАРОГО дерева (перепин env, шлюз, scheduler, пулы, приёмка внутри),
# затем паузы и пулы из снимка и собственная приёмка автомата по старому дереву.
rollback_to_old(){  # rollback_to_old <старое дерево> <StartedAt scheduler'а до отката>
  local old="$1" started_before="$2" c seen tries deadline rrc started
  set +e
  for c in "$SCHED" "$GW"; do
    echo "--- docker logs $c --tail 200 ---" >> "$LOG"
    timeout -k 5 60 docker logs "$c" --tail 200 >> "$LOG" 2>&1
  done
  ROLLBACK_NOTE=""
  if [ ! -x "$old/deploy/transfermarkt/deploy.sh" ]; then
    log "в старом дереве $old нет deploy/transfermarkt/deploy.sh — откатывать нечем"
    return 1
  fi
  setsid env TRANSFERMARKT_DEPLOY_LOCK="$TRANSFERMARKT_DEPLOY_LOCK" TRANSFERMARKT_DEPLOY_LOCK_FD=8 \
    timeout -k 30 "$DEPLOY_CEILING" "$old/deploy/transfermarkt/deploy.sh" "$old" >> "$LOG" 2>&1 9>&- &
  wait $!; rrc=$?
  log "откат: deploy.sh $old вернул $rrc"
  [ "$rrc" = 0 ] || ROLLBACK_NOTE="$ROLLBACK_NOTE deploy.sh старого дерева вернул $rrc."
  transfermarkt_load_env "$ENV_FILE"
  [ "$(env_file_value TRANSFERMARKT_RELEASE_ROOT)" = "$old" ] || { log "ENV-ФАЙЛ НЕ ВЕРНУЛСЯ К $old"; return 1; }
  RESTORE_PENDING=1
  restore_state
  deadline=$(( $(date -u +%s) + ACCEPT_WAIT ))
  tries=$(( ACCEPT_WAIT / ACCEPT_POLL + 1 ))
  while :; do
    seen=0
    started=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
    if [ -n "$started" ] && [ "$started" != "$started_before" ]; then
      seen=$(acceptance_seen "$old" "$started")
    fi
    [ "$seen" = 1 ] && return 0
    tries=$(( tries - 1 ))
    [ "$tries" -le 0 ] && break
    [ "$(date -u +%s)" -ge "$deadline" ] && break
    sleep "$ACCEPT_POLL"
  done
  return 1
}
# Выставляет now и deadline = min(ближайший запуск DAG вне полосы − ROLLBACK_RESERVE,
# now + DEADLINE_MAX). 1 — метабаза не ответила или DAG пропал/неактивен, или у DAG не на
# паузе следующий запуск не вычислен (fail-closed): тик пропускается, это не провал попытки.
set_deadline(){
  local nxt
  now=$(date -u +%s)
  nxt=$(metadb "SELECT CASE WHEN count(*) FILTER (WHERE is_active) < $NEXT_RUN_COUNT OR count(*) FILTER (WHERE NOT is_paused AND next_dagrun_create_after IS NULL) > 0 THEN -1 ELSE coalesce(min(extract(epoch FROM next_dagrun_create_after)) FILTER (WHERE NOT is_paused), 0)::bigint END FROM dag WHERE dag_id IN ($NEXT_RUN_DAGS);")
  case "$nxt" in ''|*[!0-9]*) return 1 ;; esac
  deadline=$(( now + DEADLINE_MAX ))
  if [ "$nxt" != 0 ] && [ $(( nxt - ROLLBACK_RESERVE )) -lt "$deadline" ]; then
    deadline=$(( nxt - ROLLBACK_RESERVE ))
  fi
  return 0
}
# Бой не на master дольше суток — одна новость в сутки на все причины (master недоступен,
# полоса занята, попытка провалилась). Отсчёт — с первого тика, увидевшего расхождение.
announce_behind(){  # announce_behind <текст>
  local since
  if ! is_plain "$BEHIND"; then
    { printf '%s\n' "$(date -u +%s)" > "$BEHIND"; } 2>/dev/null || log "не смог записать $BEHIND"
    return 0
  fi
  since=$(head -1 "$BEHIND" 2>/dev/null)
  case "$since" in ''|*[!0-9]*) return 0 ;; esac
  [ "$(( $(date -u +%s) - since ))" -ge 86400 ] || return 0
  said_today "$STATE/transfermarkt-window-missed-$TODAY" && return 0
  tg_durable "$1"
  mark_said "$STATE/transfermarkt-window-missed-$TODAY"
}

# --- Записи попыток: одна на попытку (<дата>T<ЧЧММ>, DEADLINE); по ним считается серия провалов ---
window_file(){ printf '%s/transfermarkt-window-%s.env' "$STATE" "$1"; }
win_get(){ sed -n "s/^$2=//p" "$(window_file "$1")" 2>/dev/null | head -1; }
win_closed(){ [ -n "$(win_get "$1" OUTCOME)" ]; }
# Дедлайн записи: поле DEADLINE; у старой записи (id = дата, до #2039) — 03:00 UTC той даты.
window_deadline(){
  local d
  d=$(win_get "$1" DEADLINE)
  case "$d" in ''|*[!0-9]*) ;; *) printf '%s' "$d"; return 0 ;; esac
  case "$1" in
    [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]) date -u -d "$1 ${LEGACY_WINDOW_TO:0:2}:${LEGACY_WINDOW_TO:2:2}" +%s ;;
    *) return 1 ;;
  esac
}
# Самая свежая открытая запись — продолжение прерванной попытки (дренаж, самоустановка, заморозка).
latest_open_window(){
  local -a recs=("$STATE"/transfermarkt-window-*.env)
  local i id
  for (( i=${#recs[@]}-1; i>=0; i-- )); do
    is_plain "${recs[$i]}" || continue
    id=${recs[$i]##*/transfermarkt-window-}; id=${id%.env}
    win_closed "$id" || { printf '%s' "$id"; return 0; }
  done
}
window_open(){  # window_open <TARGET> — новая запись попытки, выставляет WIN
  local id
  id=$(date -u +%FT%H%M)
  { is_plain "$(window_file "$id")" || odd_path "$(window_file "$id")"; } && id=$(date -u +%FT%H%M%S)
  win_write "$id" "WINDOW_ID=$id" "LIVE=${LIVE:--}" "TARGET=$1" "SCRIPT_SHA=$(script_sha)" \
    "OPENED_AT=$(date -u +%FT%TZ)" "DEADLINE=$deadline" "LAST_REASON=" || return 1
  WIN=$id
}
win_write(){
  local id="$1" f tmp kv keys=""
  shift
  f=$(window_file "$id"); tmp="$f.tmp"
  if odd_path "$f" || odd_path "$tmp"; then
    log "на месте записи попытки $f не обычный файл — не пишу туда ничего"
    return 1
  fi
  for kv in "$@"; do keys="$keys|${kv%%=*}"; done
  if {
       if is_plain "$f"; then grep -vE "^(${keys#|})=" "$f" || true; fi
       for kv in "$@"; do printf '%s\n' "${kv//$'\n'/ }"; done
     } > "$tmp" 2>/dev/null && mv -f "$tmp" "$f" 2>/dev/null; then
    return 0
  fi
  rm -f "$tmp" 2>/dev/null
  log "ЗАПИСЬ ПОПЫТКИ $f НЕ ЗАПИСАНА"
  return 1
}
script_sha(){ sha256sum "$SELF" 2>/dev/null | cut -c1-64; }
# Пока попытка открытой записи не начата (нет ATTEMPT_AT), обновить её цель: `-` — бой = master,
# `?` — master недоступен или бой не проверен, sha — цель. Записи нет — нечего.
window_touch(){  # window_touch <TARGET> <LAST_REASON>
  [ -n "$WIN" ] || return 0
  win_closed "$WIN" && return 0
  [ -n "$(win_get "$WIN" ATTEMPT_AT)" ] && return 0
  if [ -z "$2" ] && [ "$(win_get "$WIN" TARGET)" = "$1" ]; then
    win_write "$WIN" "LIVE=${LIVE:--}"
    return
  fi
  win_write "$WIN" "LIVE=${LIVE:--}" "TARGET=$1" "LAST_REASON=$2"
}
window_note(){  # window_note <причина>
  [ -n "$WIN" ] || return 0
  is_plain "$(window_file "$WIN")" || return 0
  win_closed "$WIN" && return 0
  win_write "$WIN" "LAST_REASON=$1"
}
# Сбой до защёлки повторяется каждым тиком: в Telegram — только первый раз подряд.
note_and_tell(){  # note_and_tell <причина> <сообщение>
  [ -n "$WIN" ] && [ "$(win_get "$WIN" LAST_REASON)" = "$1" ] || tg_durable "$2"
  window_note "$1"
}
fail_streak(){
  local -a recs=("$STATE"/transfermarkt-window-*.env)
  local i o n=0
  for (( i=${#recs[@]}-1; i>=0; i-- )); do
    is_plain "${recs[$i]}" || continue
    o=$(sed -n 's/^OUTCOME=//p' "${recs[$i]}" 2>/dev/null | head -1)
    case "$o" in
      failed) n=$(( n + 1 )) ;;
      delivered) break ;;
    esac
  done
  echo "$n"
}
window_close(){  # window_close <id> <OUTCOME> <REASON> <RESTORED t|f> [force]
  local id="$1" outcome="$2" reason="$3" restored="$4" force="${5:-}" f n
  case "$id" in drill-*) log "учения $id: исход $outcome ($reason)"; return 0 ;; esac
  f=$(window_file "$id")
  if win_closed "$id" && [ -z "$force" ]; then
    log "попытка $id уже закрыта ($(win_get "$id" OUTCOME)) — повторное закрытие ($outcome: $reason) пропущено"
    return 2
  fi
  local -a kv=("OUTCOME=$outcome" "REASON=$reason" "RESTORED=$restored")
  is_plain "$f" || kv=("WINDOW_ID=$id" "LIVE=${LIVE:--}" "TARGET=${WANT:-?}" "SCRIPT_SHA=$(script_sha)" \
    "OPENED_AT=$(date -u +%FT%TZ)" "DEADLINE=${deadline:-}" "LAST_REASON=" "${kv[@]}")
  kv+=("CLOSED_AT=$(date -u +%FT%TZ)")
  win_write "$id" "${kv[@]}" || return 1
  n=$(fail_streak)
  log "ИТОГ ПОПЫТКИ $id: цель $(win_get "$id" TARGET | head -c 8), исход $outcome ($reason), контур $restored, провальных попыток подряд $n из $FAIL_NIGHTS_MAX"
  return 0
}
# needs-hands — в запись разбираемой доставки, в открытую запись тика или в новую.
hands_close(){  # hands_close <id|""> <причина> <RESTORED>
  local id="${1:-$WIN}"
  [ -n "$id" ] || id=$(date -u +%FT%H%M)
  window_close "$id" needs-hands "$2" "$3" "${4:-}"
}
streak_tail(){
  local n
  n=$(fail_streak)
  if [ "$n" -ge "$FAIL_NIGHTS_MAX" ]; then
    set_off
    printf ' Провал %s из %s подряд. Больше не пробую — автомат заглушен, снять %s после разбора.' "$n" "$FAIL_NIGHTS_MAX" "$OFF"
  else
    printf ' Провал %s из %s подряд.' "$n" "$FAIL_NIGHTS_MAX"
  fi
}
# Просроченное закрытие: открытая запись с прошедшим дедлайном (DEADLINE; у старых — 03:00 их
# даты) и без INFLIGHT своей попытки. Отсутствие записи — не новость (#2039): попытка
# открывается только при цели и свободной полосе; отставание боя сторожит announce_behind.
close_overdue_windows(){
  local f id now_s infl="" last tgt why
  now_s=$(date -u +%s)
  is_plain "$INFLIGHT" && infl=$(snap_get WINDOW_ID)
  for f in "$STATE"/transfermarkt-window-*.env; do
    is_plain "$f" || continue
    id=${f##*/transfermarkt-window-}; id=${id%.env}
    win_closed "$id" && continue
    [ "$now_s" -gt "$(window_deadline "$id" 2>/dev/null || echo 9999999999)" ] || continue
    [ "$id" = "$infl" ] && continue
    last=$(win_get "$id" LAST_REASON)
    if [ "$(win_get "$id" DELIVERY_PHASE)" = finishing ]; then
      window_close "$id" unknown "обрыв между снятием маркера доставки и записью исхода" t
      continue
    fi
    tgt=$(win_get "$id" TARGET)
    case "$tgt" in
      -) window_close "$id" no-target "бой = master, доставлять нечего" t ;;
      '?'|'') window_close "$id" unknown "master недоступен или бой не проверен${last:+: $last}" t ;;
      *)
        # Запись старой копии (<дата>) без начатой попытки — окно закрылось без попытки: не провал.
        if [[ "$id" != *T* ]] && [ -z "$(win_get "$id" ATTEMPT_AT)" ]; then
          window_close "$id" busy "окно закрылось без попытки: ${last:-причина не записана}" t
          continue
        fi
        if window_close "$id" failed "дедлайн попытки истёк: ${last:-причина не записана}" t; then
          why=$(streak_tail)
          is_plain "$OFF" && tg_durable "Transfermarkt: попытка $id закрылась без доставки ${tgt:0:8} (${last:-причина не записана}).$why Лог: $LOG"
        fi ;;
    esac
  done
}
install_copy(){  # install_copy <источник> <цель>
  local src="$1" dst="$2" tmp
  tmp="$(dirname "$dst")/.$(basename "$dst").install-tmp"
  rm -f "$tmp" 2>/dev/null
  { [ -e "$tmp" ] || [ -L "$tmp" ]; } && return 1
  if cp "$src" "$tmp" 2>/dev/null && chmod 0755 "$tmp" 2>/dev/null && mv -f "$tmp" "$dst" 2>/dev/null \
     && [ "$(md5_of "$dst")" = "$(md5_of "$src")" ]; then
    return 0
  fi
  rm -f "$tmp" 2>/dev/null
  return 1
}
# Работающая копия автомата (/usr/local/libexec/transfermarkt/) обязана совпадать по md5 с
# auto_deliver.sh и env.sh доставляемого релиза: иначе ставит одна версия, а принимает
# и откатывает другая. Не совпадает — ставим копию из релиза (один раз на релиз) и
# выходим: доставку делает следующий тик уже новой копией.
ensure_automat_matches_release(){  # ensure_automat_matches_release <дерево релиза>
  local new="$1" sha8=${WANT:0:8} m_run m_env d_run d_env r_run r_env marker ok=1 why
  m_run=$RUN_MD5; m_env=$RUN_ENV_MD5
  d_run=$(md5_of "$SELF"); d_env=$(md5_of "$HERE/env.sh")
  r_run=$(md5_of "$new/deploy/transfermarkt/auto_deliver.sh"); r_env=$(md5_of "$new/deploy/transfermarkt/env.sh")
  if [ -n "$r_run" ] && [ -n "$r_env" ] && [ "$m_run" = "$r_run" ] && [ "$m_env" = "$r_env" ]; then
    log "автомат сверен с релизом $sha8: auto_deliver.sh $m_run, env.sh $m_env"
    win_write "$WIN" "AUTOMAT=ok" || true
    return 0
  fi
  if [ -n "$r_run" ] && [ -n "$r_env" ] && [ "$d_run" = "$r_run" ] && [ "$d_env" = "$r_env" ]; then
    log "работающий экземпляр автомата старше копии на диске — тик без доставки, следующий пойдёт новой копией"
    win_write "$WIN" "LAST_REASON=экземпляр автомата старше копии на диске — доставка следующим тиком" || true
    exit 0
  fi
  marker=$STATE/transfermarkt-automat-installed-$sha8
  if [ -n "$r_run" ] && [ -n "$r_env" ] && ! is_plain "$marker" && ! odd_path "$marker"; then
    [ "$d_env" = "$r_env" ] || install_copy "$new/deploy/transfermarkt/env.sh" "$HERE/env.sh" || ok=0
    [ "$ok" = 1 ] && { [ "$d_run" = "$r_run" ] || install_copy "$new/deploy/transfermarkt/auto_deliver.sh" "$SELF" || ok=0; }
    if [ "$ok" = 1 ]; then
      mk_marker "$marker" || log "не смог поставить отметку $marker"
      log "автомат обновлён из релиза $sha8: auto_deliver.sh ${m_run:-?}→$r_run, env.sh ${m_env:-?}→$r_env"
      win_write "$WIN" "AUTOMAT=installed" "LAST_REASON=автомат обновлён из релиза $sha8 — доставка следующим тиком" || true
      exit 0
    fi
    why="установка копии из релиза не удалась ($(dirname "$SELF") недоступен на запись или md5 после замены не тот)"
  elif [ -z "$r_run" ] || [ -z "$r_env" ]; then
    why="в релизе нет deploy/transfermarkt/auto_deliver.sh или env.sh"
  else
    why="копия уже ставилась из этого релиза (отметка $marker), а md5 снова не тот"
  fi
  log "АВТОМАТ ≠ РЕЛИЗ $sha8 — $why; доставки $sha8 не будет"
  mk_marker "$ATTEMPTED" || log "не смог поставить защёлку $ATTEMPTED"
  drain_abort "автомат ≠ релиз"
  win_write "$WIN" "AUTOMAT=mismatch" || true
  window_close "$WIN" failed "автомат ≠ релиз: $why" t
  why="$why.$(streak_tail)"
  tg_durable "Transfermarkt: работающая копия автомата ($SELF) не совпадает с релизом $sha8 по md5 — $why Доставки $sha8 не будет (защёлка на этот sha). Лог: $LOG"
  exit 0
}

# ============================ Шаг 0: состояние на месте ============================
if [ -L "$STATE" ] || [ ! -d "$STATE" ]; then
  log "КАТАЛОГ СОСТОЯНИЯ $STATE НЕ НА МЕСТЕ (ссылка или не каталог) — не трогаю ничего"
  said="$(dirname "$LOG")/transfermarkt-state-broken-$TODAY"
  if ! is_plain "$said"; then
    tg "Transfermarkt: каталог состояния автомата ($STATE) — ссылка или его нет. Автомат не делает НИЧЕГО. НУЖНЫ РУКИ. Лог: $LOG" \
      && { mk_marker "$said" || true; }
  fi
  exit 1
fi
if odd_path "$LOG"; then
  echo "$(date -u +%FT%TZ) НА МЕСТЕ ЛОГА $LOG НЕ ОБЫЧНЫЙ ФАЙЛ — глушу автомат" >&2
  tg "Transfermarkt: на месте лога автомата ($LOG) не обычный файл. Ничего не делаю. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора."
  set_off
  exit 1
fi
for p in "$LOCK" "$PENDING" "$OFF" "$INFLIGHT" "$ACCEPTED" "$SNAPSHOT" "$BEHIND" "$DRAIN" "$DRAIN.tmp" \
         "$STATE"/transfermarkt-auto-deliver-attempted-* "$STATE"/transfermarkt-window-*.env; do
  if odd_path "$p"; then
    log "НА МЕСТЕ $p НЕ ОБЫЧНЫЙ ФАЙЛ — состояние автомата недостоверно, глушу"
    tg "Transfermarkt: на месте $p не обычный файл — состояние автомата недостоверно. Ничего не делаю. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
    exit 1
  fi
done

# ============================ Шаг 1: замки ============================
# Свой замок (fd 9) — один тик за раз; замок выката (fd 8) — общий с ручным deploy.sh,
# держится весь тик и передаётся внутрь deploy.sh.
if ! { exec 9>"$LOCK"; } 2>/dev/null; then
  lock_alert "не могу открыть замок $LOCK"
  exit 1
fi
flock -n 9; frc=$?
if [ "$frc" != 0 ]; then
  [ "$frc" = 1 ] && exit 0
  log "flock отказал кодом $frc — это не конкуренция"
  lock_alert "flock отказал кодом $frc (не конкуренция)"
  exit 1
fi
transfermarkt_take_deploy_lock 8; drc=$?
if [ "$drc" != 0 ]; then
  if [ "$drc" = 1 ]; then
    log "идёт другой выкат (замок ${TRANSFERMARKT_DEPLOY_LOCK:-?}) — пропускаем тик"
    exit 0
  fi
  log "ЗАМОК ВЫКАТА НЕ БЕРЁТСЯ (${TRANSFERMARKT_DEPLOY_LOCK:-?}) — это не конкуренция"
  lock_alert "замок выката ${TRANSFERMARKT_DEPLOY_LOCK:-?} не берётся (код $drc)"
  exit 1
fi
# Под замком перечитываем env: ручной выкат мог перепинить дерево, пока тик ждал.
LOCK_PATH_BEFORE=${TRANSFERMARKT_DEPLOY_LOCK:-}
transfermarkt_load_env "$ENV_FILE" || exit 2
LIVE_ROOT=${TRANSFERMARKT_RELEASE_ROOT:?TRANSFERMARKT_RELEASE_ROOT не задан в env-файле}
transfermarkt_deploy_lock_init
[ "$TRANSFERMARKT_DEPLOY_LOCK" = "$LOCK_PATH_BEFORE" ] || {
  log "ПУТЬ ЗАМКА ВЫКАТА ИЗМЕНИЛСЯ ПОСЛЕ ПЕРЕЧИТЫВАНИЯ env ($LOCK_PATH_BEFORE -> $TRANSFERMARKT_DEPLOY_LOCK)"
  lock_alert "путь замка выката изменился под замком"
  exit 1
}

# ============================ Шаг 2: очередь, записи, выключатель ============================
flush_pending
close_overdue_windows
# Открытая запись, пережившая просроченное закрытие, — продолжение попытки.
WIN=$(latest_open_window)
if is_plain "$OFF"; then
  # Паузы полосы — ДО выхода: иначе выключатель оставил бы сбор на паузе навсегда.
  drain_abort "стоит выключатель $OFF"
  [ "$DRILL" = 1 ] && echo "автомат заглушен ($OFF) — учения не начаты" >&2
  if is_plain "$INFLIGHT" && ! said_today "$STATE/transfermarkt-off-reminded-$TODAY"; then
    tg_durable "Transfermarkt: автомат заглушен ($OFF), а незакрытая доставка ещё висит ($INFLIGHT). Бой: $LIVE_ROOT. НУЖНЫ РУКИ."
    mark_said "$STATE/transfermarkt-off-reminded-$TODAY"
  fi
  exit 0
fi
if [ ! -r "$PLATFORM_ENV" ]; then
  log "НЕТ ОБЩЕГО .env ПЛАТФОРМЫ ($PLATFORM_ENV)"
  set_off
  hands_close "" "нет общего .env платформы ($PLATFORM_ENV)" t
  tg_durable "Transfermarkt: общий .env платформы ($PLATFORM_ENV) недоступен — compose упал бы на середине. Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  exit 1
fi

# ============================ Шаг 3: незакрытая доставка ============================
if is_plain "$INFLIGHT"; then
  flag_clear   # дренаж кончился вместе с оборванной доставкой
  OLD=$(snap_get OLD_RELEASE_ROOT)
  WIN_INFL=$(snap_get WINDOW_ID)
  infl_close(){ [ -n "$WIN_INFL" ] || return 0; window_close "$WIN_INFL" "$1" "$2" "$3" force; }
  if ! snapshot_ok; then
    log "НЕЗАКРЫТАЯ ДОСТАВКА, а снимок отката ($SNAPSHOT) неполон — не трогаю ничего"
    infl_close needs-hands "незакрытая доставка, снимок отката негоден" f
    set_off
    tg_durable "Transfermarkt: прошлая доставка оборвалась, но снимок отката ($SNAPSHOT) неполон — куда возвращать бой, неизвестно. Ничего не трогаю. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    exit 1
  fi
  RESTORE_PENDING=1
  on_old=$(mounts_all_in "$OLD")
  if [ "$on_old" = X ]; then
    log "НЕЗАКРЫТАЯ ДОСТАВКА, но docker не отвечает — понять, где стоит бой, нечем"
    tg_durable "Transfermarkt: прошлая доставка оборвалась, а docker не отвечает. Ничего не трогаю, повторю следующим заходом. Лог: $LOG"
    RESTORE_PENDING=""
    exit 1
  fi
  gw_health=$(inspect -f '{{.State.Health.Status}}' "$GW")
  if [ "$on_old" = 1 ] && [ "$gw_health" != healthy ]; then
    # Обрыв между остановкой шлюза и его пересозданием: монты ещё на OLD, но шлюз стоит.
    log "НЕЗАКРЫТАЯ ДОСТАВКА: монты на $OLD, а шлюз '$gw_health' — поднимаю бой откатом"
    on_old=0
  fi
  if [ "$on_old" = 1 ]; then
    # Контейнеры не пересоздавались: вернуть строку env-файла, паузы и пулы, затем
    # подтвердить тот же контракт приёмки по OLD, и только потом снять маркер.
    if ! transfermarkt_set_env_var "$ENV_FILE" TRANSFERMARKT_RELEASE_ROOT "$OLD" \
       || [ "$(env_file_value TRANSFERMARKT_RELEASE_ROOT)" != "$OLD" ]; then
      infl_close needs-hands "обрыв до пересоздания контейнеров, env не вернулся к снимку" f
      set_off
      tg_durable "Transfermarkt: прошлая доставка оборвалась до пересоздания контейнеров (бой на $OLD), но вернуть env-файл к снимку не удалось. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
      exit 1
    fi
    restored_old=0
    restore_state && restored_old=1
    seen=X
    started=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
    [ -n "$started" ] && seen=$(acceptance_seen "$OLD" "$started")
    if [ "$restored_old" = 1 ] && [ "$seen" != 1 ]; then
      log "НЕЗАКРЫТАЯ ДОСТАВКА: бой на $OLD, но приёмка по нему '$seen' — поднимаю бой откатом"
      RESTORE_PENDING=1
      restored_old=rollback
    fi
  fi
  if [ "$on_old" = 1 ] && [ "$restored_old" != rollback ]; then
    if [ "$restored_old" = 1 ]; then
      infl_close failed "доставка оборвалась до пересоздания контейнеров, бой остался на $OLD" t
      why=$(streak_tail)
      tg_durable "Transfermarkt: прошлая доставка оборвалась на середине, но бой целиком остался на прежнем дереве ($OLD) — env-файл, паузы и пулы вернул к снимку.$why Лог: $LOG"
    else
      infl_close needs-hands "обрыв до пересоздания контейнеров, контур не вернулся:$RESTORE_NOTE" f
      tg_durable "Transfermarkt: прошлая доставка оборвалась (бой остался на $OLD), но контур не вернулся в рабочее состояние —$RESTORE_NOTE НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
      set_off
    fi
    rm -f "$INFLIGHT"
    exit 1
  fi
  log "НЕЗАКРЫТАЯ ДОСТАВКА и бой не на $OLD — откатываю"
  started_before=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
  if rollback_to_old "$OLD" "$started_before"; then
    rm -f "$INFLIGHT"
    if [ -n "$RESTORE_NOTE" ]; then
      # Приёмка паузы не сверяет: несошедшийся возврат пауз/пулов запрещает «успех».
      infl_close needs-hands "откат после обрыва подтверждён, контур не вернулся:$RESTORE_NOTE" f
      tg_durable "Transfermarkt: прошлая доставка оборвалась, откат на $OLD подтверждён, НО паузы/пулы не вернулись к снимку —$RESTORE_NOTE${ROLLBACK_NOTE} НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
      set_off
      exit 1
    fi
    infl_close failed "откат после обрыва доставки подтверждён, бой на $OLD" t
    why=$(streak_tail)
    tg_durable "Transfermarkt: прошлая доставка оборвалась на середине. Откат на $OLD подтверждён.${ROLLBACK_NOTE}$why Лог: $LOG"
  else
    infl_close needs-hands "обрыв доставки, откат на $OLD не подтверждён" f
    tg_durable "Transfermarkt: прошлая доставка оборвалась И откат на $OLD не подтверждён.${ROLLBACK_NOTE}${RESTORE_NOTE:+ Не вернулось:$RESTORE_NOTE} НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
  fi
  exit 1
fi

# ============================ Шаг 3а: просрочка дренажа ============================
# До любых ранних выходов (master недоступен, метабаза молчит): полоса не стоит дольше DRAIN_MAX.
if is_plain "$DRAIN"; then
  started=$(drain_get STARTED_AT)
  case "$started" in ''|*[!0-9]*) started=0 ;; esac
  if [ $(( $(date -u +%s) - started )) -ge "$DRAIN_MAX" ]; then
    why="полоса не освободилась за ${DRAIN_MAX}s"
    tgt=$(drain_get TARGET)
    log "$why — прерываю дренаж, sha ${tgt:0:8} откладываю на ${BUSY_RETRY}s"
    drain_abort "$why"
    # Не провал кода, а «не сейчас»: исход busy в серию провалов не идёт.
    case "$tgt" in *[!0-9a-f]*|'') ;; *) ATTEMPTED=$STATE/transfermarkt-auto-deliver-attempted-${tgt:0:8}; odd_path "$ATTEMPTED" || latch_busy ;; esac
    [ -n "$WIN" ] && window_close "$WIN" busy "$why" t
    tg_durable "Transfermarkt: доставка ${tgt:0:8} не состоялась — $why; флаг убран, паузы полосы возвращены, бой не тронут. Повторю не раньше чем через $(( BUSY_RETRY / 3600 )) ч. Лог: $LOG"
    exit 0
  fi
else
  flag_clear   # флаг без файла дренажа — след оборванного тика
fi

# ============================ Шаг 4: что в бою и что в master ============================
LIVE=$(git -C "$LIVE_ROOT" --no-optional-locks rev-parse HEAD 2>/dev/null)
is_sha40(){ case "$1" in *[!0-9a-f]*) return 1 ;; esac; [ "${#1}" = 40 ]; }
if [ "$DRILL" = 1 ]; then
  # Учения: цель — копия ПРИНЯТОГО боевого дерева со сломанным DAG; master не нужен.
  WANT=$LIVE
  if ! is_sha40 "$LIVE" || [ "$(head -c 64 "$ACCEPTED" 2>/dev/null | tr -d ' \n\r')" != "$LIVE" ]; then
    echo "учения только на принятом бою: HEAD боевого дерева '$LIVE' не равен $ACCEPTED" >&2
    exit 2
  fi
else
  WANT=$(timeout -k 5 60 git ls-remote "$SOURCE_REPO" refs/heads/master 2>/dev/null | cut -f1)
  if ! is_sha40 "$WANT"; then
    log "master недоступен (git ls-remote $SOURCE_REPO вернул '$WANT') — вслепую не переключаемся"
    window_touch '?' "master недоступен (git ls-remote вернул '$WANT')"
    drain_abort "master недоступен"
    announce_behind "Transfermarkt: бой не на master больше суток, сейчас master недоступен — бой остаётся на ${LIVE:0:8}. Лог: $LOG"
    exit 0
  fi
  if [ "$LIVE" = "$WANT" ]; then
    drain_abort "бой уже на master"
    live_mounts=$(mounts_all_in "$LIVE_ROOT")
    if [ "$live_mounts" = X ]; then
      window_touch '?' "бой на master, монты проверить нечем (docker не отвечает)"
      exit 0
    fi
    if [ "$live_mounts" != 1 ]; then
      log "КОНТУР СМЕШАННЫЙ: env-файл говорит $LIVE_ROOT (это master), а контейнеры смонтированы с другого дерева"
      window_touch '?' "контур смешанный: env на master, контейнеры на другом дереве"
      if ! said_today "$STATE/transfermarkt-mixed-contour-$TODAY"; then
        tg_durable "Transfermarkt: env-файл контура говорит $LIVE_ROOT (это master), но контейнеры смонтированы с ДРУГОГО дерева — контур смешанный. НУЖНЫ РУКИ. Лог: $LOG"
        mark_said "$STATE/transfermarkt-mixed-contour-$TODAY"
      fi
      exit 1
    fi
    if [ "$(head -c 64 "$ACCEPTED" 2>/dev/null | tr -d ' \n\r')" != "$WANT" ]; then
      started=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
      seen=X
      [ -n "$started" ] && seen=$(acceptance_seen "$LIVE_ROOT" "$started")
      if [ "$seen" = X ]; then
        window_touch '?' "бой на master, контракт приёмки проверить нечем"
        exit 0
      fi
      if [ "$seen" != 1 ]; then
        log "БОЙ НА MASTER, НО КОНТРАКТ ПРИЁМКИ НЕ СОШЁЛСЯ — маркер приёмки не выдаю"
        window_touch '?' "бой на master, контракт приёмки не сошёлся"
        if ! said_today "$STATE/transfermarkt-contract-failed-$TODAY"; then
          tg_durable "Transfermarkt: бой стоит на master ($LIVE_ROOT), но контур не проходит приёмку (4 DAG перечитаны без ошибок импорта, шлюз healthy на 1 GiB, монты в этом дереве, /health transfermarkt-only, пулы). Маркер приёмки не выдаю. Лог: $LOG"
          mark_said "$STATE/transfermarkt-contract-failed-$TODAY"
        fi
        exit 0
      fi
      printf '%s\n' "$WANT" > "$ACCEPTED" 2>/dev/null || log "маркер приёмки $ACCEPTED не записан"
    fi
    rm -f "$BEHIND" 2>/dev/null
    window_touch - "бой = master, доставлять нечего"
    exit 0
  fi
  # Запись с начатой попыткой другого sha — чужой след: исходы новой цели в неё не пишутся.
  if [ -n "$WIN" ] && [ -n "$(win_get "$WIN" ATTEMPT_AT)" ] && [ "$(win_get "$WIN" TARGET)" != "$WANT" ]; then
    WIN=""
  fi
  window_touch "$WANT" ""
  ATTEMPTED=$STATE/transfermarkt-auto-deliver-attempted-${WANT:0:8}
  announce_behind "Transfermarkt: бой стоит на ${LIVE:0:8}, а master ${WANT:0:8} — бой не на master больше суток. Причина — в $LOG (полоса занята, мало запаса до прогона registry или попытка этого sha уже была)."
fi

# ============================ Шаг 5: боевое дерево законно ============================
case "$LIVE_ROOT" in
  "$RELEASES_DIR"/*) ;;
  *) log "БОЕВОЕ ДЕРЕВО $LIVE_ROOT ВНЕ КАТАЛОГА РЕЛИЗОВ $RELEASES_DIR — не доставляю"
     set_off
     hands_close "" "боевое дерево вне каталога релизов" t
     tg_durable "Transfermarkt: боевое дерево ($LIVE_ROOT) лежит вне каталога релизов ($RELEASES_DIR) — откатывать было бы некуда. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
     exit 1 ;;
esac
is_sha40 "$LIVE" || LIVE=""
DIRTY=$(git -C "$LIVE_ROOT" --no-optional-locks status --porcelain 2>/dev/null)
if [ -z "$LIVE" ] || [ -n "$DIRTY" ]; then
  log "БОЕВОЕ ДЕРЕВО НЕ В ЗАКОННОМ СОСТОЯНИИ (HEAD='$LIVE', правок: $(printf '%s' "$DIRTY" | grep -c . || true)) — не доставляю"
  set_off
  hands_close "" "боевое дерево не в законном состоянии (HEAD или правки)" t
  tg_durable "Transfermarkt: боевое дерево ($LIVE_ROOT) не в законном состоянии — HEAD='$LIVE' или в дереве правки. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  exit 1
fi

# ============================ Шаг 6: защёлка, запас, дренаж полосы ============================
if [ "$DRILL" = 1 ]; then
  WIN="drill-$(date -u +%Y%m%dT%H%M%SZ)"
  now=$(date -u +%s)
  busy=$(contour_busy)
  if [ "$busy" != 0 ]; then
    log "контур занят (running-прогонов и задач TM в работе: '$busy'; X = метабаза недоступна) — учения не начаты"
    echo "учения не начаты: контур занят ('$busy')" >&2
    exit 0
  fi
else
  if odd_path "$ATTEMPTED"; then
    log "НА МЕСТЕ $ATTEMPTED НЕ ОБЫЧНЫЙ ФАЙЛ — состояние автомата недостоверно, глушу"
    set_off
    hands_close "" "на месте защёлки не обычный файл" t
    tg_durable "Transfermarkt: на месте защёлки $ATTEMPTED не обычный файл — понять, была ли попытка, нельзя. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    exit 1
  fi
  if is_plain "$ATTEMPTED"; then
    # «busy <epoch>» — полоса не освободилась или rc=4: повтор того же sha после BUSY_RETRY.
    read -r mark since < "$ATTEMPTED" || true
    if [ "${mark:-}" = busy ] && [ "${since:-x}" -eq "${since:-x}" ] 2>/dev/null \
       && [ $(( $(date -u +%s) - since )) -ge "$BUSY_RETRY" ]; then
      rm -f "$ATTEMPTED"
    else
      drain_abort "попытка ${WANT:0:8} уже была"
      exit 0
    fi
  fi
  # Метабаза молчит — дренаж не прерываем: вернуть паузы было бы нечем, срок ограничен шагом 3а
  # и флагом. Внешний прогон проверяется до дедлайна: у идущего cron-DAG следующего запуска нет.
  if ! lane_refresh; then
    log "метабаза не ответила про расписания полосы — тик пропущен"
    window_note "метабаза не ответила про расписания полосы"
    exit 0
  fi
  outer=$(outer_busy)
  if [ "$outer" = X ] || [ -z "$outer" ]; then
    log "метабаза не ответила про внешние прогоны — тик пропущен"
    window_note "метабаза не ответила про внешние прогоны"
    exit 0
  fi
  if [ "$outer" != 0 ]; then
    # registry/silver или полоса не на непрерывном расписании (cron-ingest): его не дренируем.
    log "идёт внешний прогон ($outer: registry/silver или DAG полосы не на непрерывном расписании) — дренаж не начинаю"
    drain_abort "идёт внешний прогон ($outer)"
    window_note "идёт внешний прогон (registry/silver или cron-полоса)"
    exit 0
  fi
  if ! set_deadline; then
    log "метабаза не ответила про ближайший внешний прогон (или он не вычислен: DAG пропал) — тик пропущен"
    window_note "метабаза не ответила про ближайший внешний прогон"
    exit 0
  fi
  if is_plain "$DRAIN"; then
    if [ $(( deadline - now )) -lt "$NEED_BUDGET" ]; then
      why="до ближайшего внешнего прогона осталось $(( deadline - now ))s (< $NEED_BUDGET), полоса не освободилась"
      log "$why — прерываю дренаж, sha ${WANT:0:8} откладываю на ${BUSY_RETRY}s"
      drain_abort "$why"
      latch_busy
      [ -n "$WIN" ] && window_close "$WIN" busy "$why" t
      tg_durable "Transfermarkt: доставка ${WANT:0:8} не состоялась — $why; флаг убран, паузы полосы возвращены, бой не тронут. Повторю не раньше чем через $(( BUSY_RETRY / 3600 )) ч. Лог: $LOG"
      exit 0
    fi
  elif [ $(( deadline - now )) -lt $(( DRAIN_MAX + NEED_BUDGET )) ]; then
    log "мало запаса: до ближайшего внешнего прогона $(( (deadline - now) / 60 )) мин (нужно $(( (DRAIN_MAX + NEED_BUDGET) / 60 ))) — доставка после него"
    window_note "мало запаса до ближайшего внешнего прогона: $(( deadline - now )) с"
    exit 0
  fi
  # Попытка начинается: открытая запись (продолжение) или новая.
  if [ -z "$WIN" ] && ! window_open "$WANT"; then
    log "запись попытки не создаётся — доставку не начинаю"
    set_off
    tg_durable "Transfermarkt: не могу создать запись попытки в $STATE — исход доставки ${WANT:0:8} потерялся бы. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    exit 1
  fi
  # Флаг дренажа не даёт стартовать новой порции; конец идущей ловим опросом внутри тика, не
  # пойман — продолжит следующий тик. Пауза — только на границе порций: Airflow 2 не двигает
  # прогон паузного DAG, пауза посреди порции заморозила бы её до распаузы.
  if ! is_plain "$DRAIN"; then
    drain_begin || exit 0
  fi
  busy=$(contour_busy)
  poll_end=$(( $(date -u +%s) + DRAIN_POLL ))
  while [ "$busy" != 0 ] && [ "$(date -u +%s)" -lt "$poll_end" ]; do
    sleep "$DRAIN_POLL_STEP"
    busy=$(contour_busy)
  done
  if [ "$busy" != 0 ]; then
    log "контур занят (running-прогонов и задач TM в работе: '$busy'; X = метабаза недоступна) — жду конца порции"
    window_note "контур занят (running-прогонов и задач TM в работе: '$busy')"
    exit 0
  fi
  if [ "$(drain_get PAUSED)" != 1 ]; then
    drain_pause || exit 0
    # Порция могла стартовать между проверкой и паузой: её не морозим — паузы назад.
    busy=$(contour_busy)
    if [ "$busy" != 0 ]; then
      drain_abort "между проверкой и паузой стартовала порция ('$busy')"
      window_note "между проверкой и паузой стартовала порция"
      exit 0
    fi
  fi
  log "полоса на паузе и пуста, до ближайшего внешнего прогона $(( (deadline - now) / 60 )) мин — начинаю попытку ${WANT:0:8}"
fi

# ============================ Шаг 7: дерево цели ============================
if [ "$DRILL" = 1 ]; then
  NEW="$RELEASES_DIR/release-${LIVE:0:8}-drill"
  rm -rf "$NEW"
  if ! cp -a "$LIVE_ROOT" "$NEW" \
     || ! printf '\n# drill-rollback #1387: intentional SyntaxError\ndef (:\n' >> "$NEW/dags/dag_ingest_transfermarkt.py"; then
    rm -rf "$NEW"
    echo "учения не начаты: копия дерева $NEW не создалась" >&2
    exit 1
  fi
  log "УЧЕНИЯ ОТКАТА: цель $NEW (копия $LIVE_ROOT со сломанным dags/dag_ingest_transfermarkt.py)"
else
  NEW="$RELEASES_DIR/release-${WANT:0:8}"
  if [ -d "$NEW" ]; then
    ok=1
    [ "$(stat -c %a "$NEW" 2>/dev/null)" = 755 ] || ok=0
    [ "$(git -C "$NEW" --no-optional-locks rev-parse HEAD 2>/dev/null)" = "$WANT" ] || ok=0
    [ -z "$(git -C "$NEW" --no-optional-locks status --porcelain 2>/dev/null)" ] || ok=0
    if [ "$ok" != 1 ]; then
      log "КАТАЛОГ $NEW УЖЕ ЕСТЬ И БИТЫЙ (права 755, HEAD=$WANT, чистота)"
      drain_abort "каталог релиза битый"
      note_and_tell "каталог релиза $NEW уже есть и битый" \
        "Transfermarkt: каталог релиза $NEW уже существует и не годится (права, HEAD или правки). Убрать руками. Доставки ${WANT:0:8} не будет, пока каталог на месте. Лог: $LOG"
      exit 1
    fi
    log "переиспользую целый каталог $NEW от прошлой попытки"
  else
    log "замораживаю $WANT"
    if ! out=$(timeout -k 30 900 "$LIVE_ROOT/deploy/transfermarkt/freeze_release.sh" "$WANT" 2>&1 8>&- 9>&-); then
      printf '%s\n' "$out" >> "$LOG"
      drain_abort "заморозка не удалась"
      note_and_tell "заморозка дерева ${WANT:0:8} не удалась" \
        "Transfermarkt: заморозка дерева $WANT не удалась — бой не тронут, повторю следующим тиком. Лог: $LOG"
      exit 1
    fi
    printf '%s\n' "$out" >> "$LOG"
    NEW=${out##*дерево заморожено: }
    NEW=${NEW%% (sha *}
    if [ ! -d "$NEW" ]; then
      drain_abort "заморозка отчиталась несуществующим деревом"
      note_and_tell "заморозка отчиталась несуществующим деревом" \
        "Transfermarkt: заморозка отчиталась деревом '$NEW', которого нет. Бой не тронут, повторю следующим тиком. Лог: $LOG"
      exit 1
    fi
  fi
  ensure_automat_matches_release "$NEW"
fi

# Заморозка могла съесть до 900 с: запас пересчитывается непосредственно перед доставкой.
# Бой не тронут, защёлки нет: это не провал кода — исход busy в серию провалов не идёт.
if [ "$DRILL" != 1 ] && [ $(( deadline - $(date -u +%s) )) -lt "$NEED_BUDGET" ]; then
  left=$(( deadline - $(date -u +%s) ))
  log "после заморозки запаса нет ($left с до дедлайна, нужно $NEED_BUDGET) — доставка позже"
  drain_abort "после заморозки запаса нет"
  window_close "$WIN" busy "после заморозки запаса нет: $left с" t
  exit 0
fi

# ============================ Шаг 8: снимок отката, защёлки ============================
OLD="$LIVE_ROOT"
SCHED_CREATED_BEFORE=$(inspect -f '{{.Created}}' "$SCHED")
if [ -z "$SCHED_CREATED_BEFORE" ]; then
  log "не читается .Created контейнера $SCHED — приёмке не на что опереться"
  drain_abort "не читается .Created scheduler'а"
  window_note "не читается .Created контейнера $SCHED"
  tg_durable "Transfermarkt: не читается .Created контейнера $SCHED — факт пересоздания недоказуем. Бой не тронут. Лог: $LOG"
  exit 1
fi
if ! mk_marker "$SNAPSHOT"; then
  set_off
  hands_close "" "снимок отката не записывается" t
  tg_durable "Transfermarkt: не могу записать снимок отката ($SNAPSHOT). Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  exit 1
fi
{
  printf 'OLD_RELEASE_ROOT=%s\n' "$OLD"
  printf 'NEW_RELEASE_ROOT=%s\n' "$NEW"
  printf 'SNAPSHOT_VERSION=2\n'
  printf 'WINDOW_ID=%s\n' "$WIN"
  # Паузы полосы — из файла дренажа (значения ДО него), не текущие `t`: иначе выкат и откат
  # вернули бы полосу на паузу навсегда.
  for d in $CORE_DAGS; do
    if [ "$(drain_get PAUSED)" = 1 ] && case " $(drain_get LANE) " in *" $d "*) true ;; *) false ;; esac; then
      printf '%s=%s\n' "$(paused_key "$d")" "$(drain_get "$(paused_key "$d")")"
    else
      printf '%s=%s\n' "$(paused_key "$d")" "$(metadb "SELECT is_paused FROM dag WHERE dag_id='$d';")"
    fi
  done
  old_pools=""
  for p in ingest_scraper_pool $POOLS; do
    slots=$(metadb "SELECT slots FROM slot_pool WHERE pool='$p';")
    case "$slots" in
      '') ;; # Pool absent in the old release.
      *[!0-9]*) printf 'POOL_%s=%s\n' "$p" "$slots"; old_pools="$old_pools $p" ;;
      *) printf 'POOL_%s=%s\n' "$p" "$slots"; old_pools="$old_pools $p" ;;
    esac
  done
  printf 'OLD_POOLS=%s\n' "${old_pools# }"
  printf 'SCHED_CREATED=%s\n' "$SCHED_CREATED_BEFORE"
} >> "$SNAPSHOT" 2>/dev/null
snap_ok=1
snapshot_ok || snap_ok=0
[ "$(snap_get OLD_RELEASE_ROOT)" = "$OLD" ] || snap_ok=0
[ "$(snap_get NEW_RELEASE_ROOT)" = "$NEW" ] || snap_ok=0
if [ "$snap_ok" != 1 ]; then
  log "СНИМОК ОТКАТА НЕ ЧИТАЕТСЯ ОБРАТНО ИЛИ МЕТАБАЗА НЕ ОТВЕТИЛА — доставки не будет"
  set_off
  hands_close "" "снимок отката не читается обратно или метабаза не ответила" t
  tg_durable "Transfermarkt: снимок отката ($SNAPSHOT) не читается обратно или метабаза не ответила про паузы и пулы. Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  exit 1
fi
RESTORE_PENDING=1
if [ "$DRILL" != 1 ] && ! mk_marker "$ATTEMPTED"; then
  set_off
  hands_close "" "защёлка попытки не создаётся" t
  tg_durable "Transfermarkt: не могу создать защёлку попытки ($ATTEMPTED). Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  exit 1
fi
if ! mk_marker "$INFLIGHT"; then
  set_off
  hands_close "" "маркер доставки не создаётся" t
  tg_durable "Transfermarkt: не могу создать маркер доставки ($INFLIGHT). Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  exit 1
fi
# Паузы полосы до дренажа теперь в снимке (возврат — restore_state), файл больше не нужен.
rm -f "$DRAIN"

# ============================ Шаг 9: доставка ============================
[ "$DRILL" = 1 ] || win_write "$WIN" "TARGET=$WANT" "ATTEMPT_AT=$(date -u +%FT%TZ)" "DEADLINE=$deadline" || true
log "ДОСТАВЛЯЮ ${WANT:0:8} ($NEW), бой сейчас $OLD, потолок ${DEPLOY_CEILING}s"
[ "$DRILL" = 1 ] || tg "Transfermarkt: начинаю доставку ${WANT:0:8} (автомат)"
setsid env TRANSFERMARKT_DEPLOY_LOCK="$TRANSFERMARKT_DEPLOY_LOCK" TRANSFERMARKT_DEPLOY_LOCK_FD=8 \
  timeout -k 30 "$DEPLOY_CEILING" "$NEW/deploy/transfermarkt/deploy.sh" "$NEW" "$OLD" >> "$LOG" 2>&1 9>&- &
dpid=$!
wait "$dpid"; rc=$?
if [ "$rc" = 124 ]; then
  log "ТАЙМАУТ доставки (${DEPLOY_CEILING}s) — добиваю группу процессов"
  kill -TERM -"$dpid" 2>/dev/null
  sleep 5
  kill -KILL -"$dpid" 2>/dev/null
fi
log "deploy.sh вернул $rc"
flag_clear   # выкат кончился (успех, провал или rc=4): новые порции снова допускаются

# ============================ Шаг 10: исход ============================
# rc=4 — «контур занят или замок занят, выкат не начат»: не провал кода, а «не сейчас»
# (как SofaScore #2000): запись busy (в серию провалов не идёт), защёлка sha — busy до
# BUSY_RETRY. Контур не вернулся — авария: failed, выключатель.
if [ "$rc" = 4 ]; then
  log "контур занят или замок занят — выкат не начинался, откатывать нечего"
  restored=1
  restore_state || restored=0
  rm -f "$INFLIGHT"
  [ "$DRILL" = 1 ] && rm -rf "$NEW"
  if [ "$restored" = 1 ]; then
    window_close "$WIN" busy "deploy.sh rc=4, бой не тронут" t
    [ "$DRILL" = 1 ] || latch_busy
  else
    window_close "$WIN" failed "deploy.sh rc=4, бой не тронут, контур не вернулся" f
  fi
  if [ "$restored" != 1 ]; then
    set_off
    tg_durable "Transfermarkt: доставка ${WANT:0:8} не состоялась (deploy.sh rc=4, бой не тронут), И контур не вернулся в рабочее состояние —$RESTORE_NOTE НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    exit 1
  fi
  [ "$DRILL" = 1 ] && { tg_durable "Transfermarkt: учения отката не состоялись — контур занят (deploy.sh rc=4), бой не тронут. Лог: $LOG"; exit 1; }
  tg_durable "Transfermarkt: доставка ${WANT:0:8} не состоялась — контур занят (deploy.sh rc=4, бой не тронут). Повторю не раньше чем через $(( BUSY_RETRY / 3600 )) ч. Лог: $LOG"
  exit 0
fi
# NEW сохраняет TM-пулы deploy.sh по streams; ДО приёмки возвращаем исходные паузы
# и общий ingest-пул. Откат и EXIT-trap по умолчанию возвращают все OLD-пулы.
restored=1
if [ "$rc" = 0 ]; then
  restore_state new || restored=0
fi
accept_deadline=$(( $(date -u +%s) + ACCEPT_WAIT ))
accept_tries=$(( ACCEPT_WAIT / ACCEPT_POLL + 1 ))
seen=0
while [ "$rc" = 0 ]; do
  created=$(inspect -f '{{.Created}}' "$SCHED")
  if [ -n "$created" ] && [ "$created" != "$SCHED_CREATED_BEFORE" ]; then
    started=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
    [ -n "$started" ] && seen=$(acceptance_seen "$NEW" "$started")
  fi
  [ "$seen" = 1 ] && break
  accept_tries=$(( accept_tries - 1 ))
  [ "$accept_tries" -le 0 ] && break
  [ "$(date -u +%s)" -ge "$accept_deadline" ] && break
  log "приёмки пока нет (ответ '$seen'), жду ещё ${ACCEPT_POLL}s"
  sleep "$ACCEPT_POLL"
done
if [ "$rc" = 0 ] && [ "$seen" = 1 ]; then
  if [ "$DRILL" = 1 ]; then
    # Сломанное дерево прошло приёмку — приёмка слепа. Возвращаем бой и кричим.
    log "УЧЕНИЯ ПРОВАЛЕНЫ: сломанное дерево $NEW прошло приёмку — откатываю"
    started_before=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
    rollback_to_old "$OLD" "$started_before" && rm -f "$INFLIGHT"
    set_off
    tg_durable "Transfermarkt: УЧЕНИЯ ОТКАТА ПРОВАЛЕНЫ — дерево с намеренной ошибкой импорта прошло приёмку. Бой возвращён на $OLD (если откат подтвердился — маркер доставки снят). НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    exit 1
  fi
  printf '%s\n' "$WANT" > "$ACCEPTED" 2>/dev/null || log "маркер приёмки $ACCEPTED не записан"
  if [ "$restored" != 1 ]; then
    tg_durable "Transfermarkt: код ${WANT:0:8} доставлен и приёмка сошлась, НО паузы/пулы не вернулись к снимку —$RESTORE_NOTE НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    rm -f "$INFLIGHT"
    set_off
    window_close "$WIN" failed "доставлено и принято, но контур не вернулся:$RESTORE_NOTE" f
    exit 1
  fi
  extra=""
  n=$( { ls -d "$RELEASES_DIR"/release-* 2>/dev/null || true; } | grep -c . )
  orphans=$( { ls -d "$RELEASES_DIR"/freeze.* 2>/dev/null || true; } | grep -c . )
  [ "$orphans" = 0 ] || extra=" Осиротевших freeze.* — $orphans (убрать руками)."
  log "ДОСТАВЛЕНО: $NEW (sha ${WANT:0:8})"
  tg_durable "Transfermarkt: доставлено ${WANT:0:8} → $NEW, приёмка подтверждена (4 DAG перечитаны после старта нового scheduler'а, ошибок импорта нет, шлюз healthy в transfermarkt-gw, монты в новом дереве, /health transfermarkt-only, TM-пулы по streams, исходные паузы). Деревьев release-*: $n.${extra}"
  win_write "$WIN" "DELIVERY_PHASE=finishing" || true
  rm -f "$INFLIGHT"
  window_close "$WIN" delivered "приёмка подтверждена" t
  exit 0
fi
log "ПРОВАЛ доставки (rc=$rc, приёмка '$seen') — откатываю бой на $OLD"
started_before=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
fails=0
if [ "$DRILL" != 1 ]; then
  window_close "$WIN" failed "deploy.sh вернул $rc, приёмка '$seen' — откат на $OLD" f; wrc=$?
  fails=$FAIL_NIGHTS_MAX
  [ "$wrc" = 1 ] || fails=$(fail_streak)
fi
if rollback_to_old "$OLD" "$started_before"; then
  log "ОТКАТ ПОДТВЕРЖДЁН: бой на $OLD"
  rm -f "$INFLIGHT"
  if [ "$DRILL" = 1 ]; then
    rm -rf "$NEW"
    if [ -n "$RESTORE_NOTE" ]; then
      set_off
      tg_durable "Transfermarkt: учения отката: откат на $OLD подтверждён, НО паузы/пулы не вернулись —$RESTORE_NOTE НУЖНЫ РУКИ. Лог: $LOG"
      exit 1
    fi
    tg_durable "Transfermarkt: откат проверен — дерево с намеренной ошибкой импорта не прошло приёмку (deploy.sh rc=$rc), бой возвращён на $OLD и принят заново.${ROLLBACK_NOTE} Лог: $LOG"
    exit 0
  fi
  [ -n "$RESTORE_NOTE" ] || win_write "$WIN" "RESTORED=t" || true
  if [ -n "$RESTORE_NOTE" ]; then
    tg_durable "Transfermarkt: доставка ${WANT:0:8} НЕ состоялась (deploy.sh вернул $rc, приёмка '$seen'), откат на $OLD подтверждён, НО паузы/пулы не вернулись —$RESTORE_NOTE${ROLLBACK_NOTE} НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
  else
    msg="Transfermarkt: доставка ${WANT:0:8} НЕ состоялась (deploy.sh вернул $rc, приёмка '$seen'). Откат на $OLD подтверждён.${ROLLBACK_NOTE} Провал $fails из $FAIL_NIGHTS_MAX подряд. Лог: $LOG"
    if [ "$fails" -ge "$FAIL_NIGHTS_MAX" ]; then
      msg="$msg Больше не пробую — автомат заглушен, снять $OFF после разбора."
      set_off
    fi
    tg_durable "$msg"
  fi
else
  log "ОТКАТ НЕ ПОДТВЕРЖДЁН — глушу автомат, маркер доставки оставляю"
  tg_durable "Transfermarkt: доставка ${WANT:0:8} провалилась (rc=$rc) И откат на $OLD не подтверждён.${ROLLBACK_NOTE}${RESTORE_NOTE:+ Не вернулось:$RESTORE_NOTE} НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  set_off
fi
exit 1
