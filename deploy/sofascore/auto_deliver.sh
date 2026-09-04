#!/bin/bash
# Автомат ночной доставки SofaScore: в окно между прогонами сам ставит master в бой,
# принимает результат, при любой поломке ОТКАТЫВАЕТ и докладывает в Telegram.
#
# Заведён решением владельца 03.09 (гриль, решение №4: канарейку убрать, «любая правка
# едет автоматом ночью»). До него доставка была привязана к живой сессии: код лежал в
# master, а в бою стоял старый — иногда сутками.
#
# Cron: */5 * * * * <каталог установки>/auto_deliver.sh   (окно и одноразовость — внутри)
# Все пути машины — из env-файла контура (deploy/sofascore/sofascore.env.example);
# скрипт не правится ни под машину, ни под релиз.
#
# Чем это НЕ похоже на автомат FotMob, с которого скопирована машинерия (замок, очередь
# алертов, суточная защёлка, выключатель, маркеры INFLIGHT/ACCEPTED):
#   * у FotMob одно неподвижное дерево, доставка = `git checkout -f` внутри него, цель и
#     откат — короткие SHA в env-файле. У SofaScore — РОТАЦИЯ КАТАЛОГОВ: freeze_release.sh
#     клонирует репозиторий в release-<gitsha8>, deploy.sh перепинивает три строки
#     env-файла и ПЕРЕСОЗДАЁТ scheduler и три шлюза. Поэтому цель — origin/master (пин не
#     нужен), откат — ПУТЬ к прежнему дереву (снимок, снимаемый перед доставкой), а
#     доказательство доставки — монты живых контейнеров, а не HEAD дерева.
#   * откат повторным вызовом deploy.sh невозможен: он требует
#     configs/sofascore/workload_policy.json, которого в старых деревьях нет (exit 2).
#     Откат — своя процедура, комплектом и best-effort (см. «Провал» ниже).
#   * postdeploy_checks.sh машинной приёмкой служить не может: раздел 6 даёт шесть ложных
#     ✗ на нераскрытых ${VAR} из `systemctl show -p ExecStart` и всегда возвращает 1.
#     Приёмка здесь своя — шесть признаков, привязанных к ФАКТУ пересоздания контейнера,
#     а не к часам автомата (deploy.sh из-за шага drain работает десятки минут, и всё это
#     время старый scheduler перепарсивает дерево: условие «перечитано после T0 моих
#     часов» выполнилось бы и при упавшем выкате).
#
# Чего автомат НЕ делает: не удаляет старые деревья релизов (только предупреждает), не
# запрещает ручной выкат по слову владельца (идёт deploy.sh — пропускаем тик), не судит
# о том, собирает ли кампания данные (это утренний сторож — «зелёно, но мертво» ловит он).
#
# И ни один исход не теряется: неудачная отправка в Telegram кладёт текст в очередь,
# которую дожимает первым делом каждый следующий тик cron; если недоступна и очередь —
# автомат глушит сам себя, чтобы не действовать молча.
set -u
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

HERE=$(dirname "$(readlink -f "$0")")
ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
# shellcheck source=deploy/sofascore/env.sh
. "$HERE/env.sh"
sofascore_load_env "$ENV_FILE" || exit 2
export SOFASCORE_ENV_FILE="$ENV_FILE"   # тот же файл читают freeze_release.sh и deploy.sh

# --- то, без чего автомат не делает ничего ------------------------------------------
STATE=${SOFASCORE_AUTO_STATE_DIR:?SOFASCORE_AUTO_STATE_DIR не задан в env-файле}
LOG=${SOFASCORE_AUTO_LOG:?SOFASCORE_AUTO_LOG не задан в env-файле}
TG_ENV=${SOFASCORE_TG_ENV:?SOFASCORE_TG_ENV не задан в env-файле}
METADB=${SOFASCORE_METADB_CONTAINER:?SOFASCORE_METADB_CONTAINER не задан в env-файле}
SCHED=${SOFASCORE_SCHEDULER_CONTAINER:?SOFASCORE_SCHEDULER_CONTAINER не задан в env-файле}
LIVE_ROOT=${SOFASCORE_RELEASE_ROOT:?SOFASCORE_RELEASE_ROOT не задан в env-файле}
SOURCE_REPO=${SOFASCORE_SOURCE_REPO:?SOFASCORE_SOURCE_REPO не задан в env-файле}
RELEASES_DIR=${SOFASCORE_RELEASES_DIR:?SOFASCORE_RELEASES_DIR не задан в env-файле}
PLATFORM_ENV=${SOFASCORE_PLATFORM_ENV_FILE:?SOFASCORE_PLATFORM_ENV_FILE не задан в env-файле}

LOCK=$STATE/sofascore-auto-deliver.lock
PENDING=$STATE/sofascore-pending-alert     # недоставленные алерты, дожимаются каждым тиком
OFF=$STATE/sofascore-auto-deliver.off      # выключатель: ставит человек или сам автомат
INFLIGHT=$STATE/sofascore-inflight         # доставка начата и ещё не закрыта
ACCEPTED=$STATE/sofascore-accepted         # sha кода, приёмку которого подтвердили
SNAPSHOT=$STATE/sofascore-rollback.env     # состояние боя до доставки: цель отката
FAILNIGHTS=$STATE/sofascore-fail-nights    # подряд провальных ночей
TODAY=$(date -u +%F)
ATTEMPTED=$STATE/sofascore-auto-deliver-attempted-$TODAY

# Имена контейнеров, сервисов compose и unit'ов сторожей — те же значения, что в
# deploy.sh:53-57. Литералами: это состав контура, а не настройка машины.
GATEWAYS="sofascore_proxy_filter sofascore_gw_history sofascore_gw_players"
GATEWAY_CONTAINERS="sofascore_gw_951 sofascore_gw_history sofascore_gw_players"
WATCHDOG_UNITS="sofascore-gw-lease-watchdog.service
sofascore-gw-lease-watchdog-history.service
sofascore-gw-lease-watchdog-players.service"
POOLS="ingest_scraper_pool sofascore_history_pool sofascore_players_pool"
CORE_DAGS="dag_ingest_sofascore dag_backfill_sofascore_all_mens dag_refresh_sofascore_all_mens dag_trigger_sofascore_daily dag_sofascore_manifest_maintenance"
HIST=dag_backfill_sofascore_all_mens
REFRESH=dag_refresh_sofascore_all_mens
DAILY=dag_ingest_sofascore
MAINT=dag_sofascore_manifest_maintenance

# Окно и потолки. Переопределяются из окружения только для стенда.
WINDOW_FROM=${WINDOW_FROM:-0330}   # 03:30 UTC: скоупы истории идут 8–81 мин (p90 остатка
WINDOW_TO=${WINDOW_TO:-0600}       #   ≈ 75 мин), от 04:00 запаса не хватало бы в ~35 % ночей
SUNDAY_TO=${SUNDAY_TO:-0445}       # вс. 05:00 UTC — dag_sofascore_manifest_maintenance
DEPLOY_CEILING=${DEPLOY_CEILING:-3000}  # жёсткие потолки deploy.sh после drain + 30 % запаса
ACCEPT_WAIT=${ACCEPT_WAIT:-480}    # dag_dir_list_interval = 300 с, 90 с не хватало (25.08)
ACCEPT_POLL=${ACCEPT_POLL:-20}
MIN_DRAIN=${MIN_DRAIN:-900}        # меньше 15 мин на осушение — не начинаем вовсе
METADB_TIMEOUT=${METADB_TIMEOUT:-30}
FAIL_NIGHTS_MAX=${FAIL_NIGHTS_MAX:-3}
HISTORY_SLOTS=${SOFASCORE_HISTORY_POOL_SLOTS:-1}
PLAYERS_SLOTS=${SOFASCORE_PLAYERS_POOL_SLOTS:-1}

# --- сообщения и маркеры -------------------------------------------------------------
log(){
  # В подменённый лог не пишем: перенаправление пошло бы ПО ссылке, а целью может
  # оказаться файл боевого дерева. Немыми не остаёмся — строка уходит в cron-лог.
  if [ -L "$LOG" ] || { [ -e "$LOG" ] && [ ! -f "$LOG" ]; }; then
    echo "$(date -u +%FT%TZ) [лог подменён] $*" >&2
    return 0
  fi
  echo "$(date -u +%FT%TZ) $*" >> "$LOG"
}
# «Обычный файл» проверяется вместе с `! -L`: `-f` идёт ПО ссылке, поэтому symlink на
# любой существующий файл иначе сходил бы за настоящий маркер.
is_plain(){ [ -f "$1" ] && [ ! -L "$1" ]; }
# «Не обычный файл» и «ничего нет» — разные новости: там, где отсутствие маркера что-то
# РАЗРЕШАЕТ, подмена молча выдавала бы себя за чистое место. Битую ссылку `-e` не видит.
odd_path(){ { [ -e "$1" ] || [ -L "$1" ]; } && ! is_plain "$1"; }
mk_marker(){
  # Проверяем ДО записи: перенаправление идёт ПО ссылке и обнуляет её цель.
  if odd_path "$1"; then log "на месте $1 не обычный файл — не пишу туда ничего"; return 1; fi
  { : > "$1"; } 2>/dev/null && is_plain "$1"
}
said_today(){ is_plain "$1"; }
mark_said(){ mk_marker "$1" || log "не смог поставить отметку $1 — сообщение повторится следующим заходом"; }
set_off(){ mk_marker "$OFF" || log "ВЫКЛЮЧАТЕЛЬ $OFF НЕ ЗАПИСАН — следующий тик не остановится"; }
# Отправляем сами и проверяем ответ API: штатный хук tg-send выходит нулём и при
# отсутствии настроек, и при упавшем curl, поэтому подтверждением доставки быть не может.
tg(){
  local text="$*" resp
  if [ ! -f "$TG_ENV" ]; then log "АЛЕРТ НЕ ДОСТАВЛЕН (нет $TG_ENV): $text"; return 1; fi
  # shellcheck disable=SC1090
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
# Возвращает 0, если сообщение ДОСТАВЛЕНО ИЛИ ГАРАНТИРОВАННО БУДЕТ доставлено — только на
# этом основании вызывающий вправе ставить суточные отметки и снимать INFLIGHT. Ни канала,
# ни очереди — функция не возвращает неудачу, а завершает работу: действовать молча нельзя.
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
    log "ОЧЕРЕДЬ АЛЕРТОВ ПОДМЕНЕНА ($PENDING — не обычный файл): отложенное не рассылается, новое складывать некуда; глушу автомат"
    tg "🆘 SofaScore: на месте очереди алертов ($PENDING) не обычный файл — отложенные сообщения потеряны, новые складывать некуда. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
    exit 1
  fi
  { is_plain "$PENDING" && [ -s "$PENDING" ]; } || return 0
  local line kept="$PENDING.kept" failed=0
  if ! mk_marker "$kept"; then
    log "не могу создать $kept — очередь оставляю как есть, разберу позже"
    local m="$STATE/sofascore-kept-broken-$TODAY"
    if ! said_today "$m"; then
      tg "🆘 SofaScore: не могу разобрать очередь отложенных алертов ($kept не создаётся). Сообщения целы, но не рассылаются. НУЖНЫ РУКИ. Лог: $LOG" && mark_said "$m"
    fi
    return 0
  fi
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    tg "$line" && continue
    # Перенос считается удавшимся, только если строка ЧИТАЕТСЯ обратно: пустой $kept
    # выглядел бы как «всё разослано», и очередь стёрлась бы вместе с алертом.
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
  local mark="$STATE/sofascore-lock-broken-$TODAY"
  log "НЕ МОГУ ВЗЯТЬ ЗАМОК: $1 — автомат не работает"
  said_today "$mark" && return 0
  if tg "🆘 SofaScore: автомат не может взять свой замок ($1) — доставки НЕ БУДЕТ. НУЖНЫ РУКИ. Лог: $LOG"; then
    mark_said "$mark"
  fi
}

# --- разговор с контуром --------------------------------------------------------------
# Код возврата берём у `docker exec`, а не у хвоста конвейера: с `| tr` статус всегда
# нулевой, и мёртвая метабаза возвращала бы пустую строку вместо X.
metadb(){
  local out
  if ! out=$(timeout -k 5 "$METADB_TIMEOUT" docker exec "$METADB" psql -U airflow -d airflow -tA -c "$1" 2>/dev/null); then
    echo X; return
  fi
  printf '%s' "$out" | tr -d ' \r'
}
sched(){ timeout -k 5 60 docker exec "$SCHED" "$@" >> "$LOG" 2>&1; }
inspect(){ timeout -k 5 30 docker inspect "$@" 2>/dev/null; }

# --- снимок боя (цель отката) ---------------------------------------------------------
snap_get(){ sed -n "s/^$1=//p" "$SNAPSHOT" 2>/dev/null | head -1; }
# Ожидаемые слоты пула: снимок, если он есть (сверка с литералом 1/1/1 дала бы ложный откат
# после первого же расширения полосы), иначе — значения env-файла, те же, что ставит deploy.sh.
# У ingest_scraper_pool ключа нет нигде: он всегда 1, и поднятый руками пул будет читаться как
# «контракт не сошёлся» — это видно в алерте и уходит после первой же доставки со снимком.
pool_want(){  # pool_want <pool>
  local w
  w=$(snap_get "POOL_$1")
  case "$w" in ''|*[!0-9]*) ;; *) printf '%s' "$w"; return ;; esac
  case "$1" in
    sofascore_history_pool) printf '%s' "$HISTORY_SLOTS" ;;
    sofascore_players_pool) printf '%s' "$PLAYERS_SLOTS" ;;
    *) printf '1' ;;
  esac
}
RESTORE_PENDING=""
RESTORE_NOTE=""
ROLLBACK_NOTE=""
# С момента снимка и до выхода: вернуть паузы и слоты пулов к тому, что было. Это
# закрывает и штатный след deploy.sh (после выката история остаётся на паузе), и любой
# аварийный обрыв автомата.
#
# Возвращает 1, если хоть что-то не вернулось, и складывает причины в RESTORE_NOTE.
# Игнорировать это нельзя: доставка, после которой история осталась на паузе, — это
# «зелёно, но пусто». Код в бою, приёмка сошлась, а кампания стоит до утра, ровно то,
# ради чего автомат и заведён.
restore_state(){
  [ -n "$RESTORE_PENDING" ] || return 0
  RESTORE_PENDING=""
  RESTORE_NOTE=""
  local pool want desc
  for pool in $POOLS; do
    want=$(snap_get "POOL_$pool")
    [ -n "$want" ] || continue
    case "$pool" in
      ingest_scraper_pool) desc='Serialize heavy ingest scrapers (isolated sofascore stack #951)' ;;
      sofascore_history_pool) desc='SofaScore history lane' ;;
      *) desc='SofaScore players lane' ;;
    esac
    if ! sched airflow pools set "$pool" "$want" "$desc"; then
      RESTORE_NOTE="$RESTORE_NOTE пул $pool не вернулся к $want слотам;"
      log "MANUAL ACTION REQUIRED: пул $pool не вернулся к $want слотам"
    fi
  done
  restore_pause "$HIST" "$(snap_get HIST_PAUSED)"
  restore_pause "$REFRESH" "$(snap_get REFRESH_PAUSED)"
  [ -z "$RESTORE_NOTE" ]
}
restore_pause(){  # restore_pause <dag_id> <t|f>
  local dag="$1" want="$2" now
  case "$want" in t) sched airflow dags pause "$dag" ;; f) sched airflow dags unpause "$dag" ;; *) return 0 ;; esac
  now=$(metadb "SELECT is_paused FROM dag WHERE dag_id='$dag';")
  [ "$now" = "$want" ] && { log "$dag: пауза вернулась к '$want'"; return 0; }
  RESTORE_NOTE="$RESTORE_NOTE $dag остался в состоянии paused='$now' вместо '$want';"
  log "MANUAL ACTION REQUIRED: $dag paused='$now', ожидалось '$want'"
  return 1
}
trap restore_state EXIT

# --- приёмка ---------------------------------------------------------------------------
# Все монты контейнера, ведущие в каталог релизов, обязаны вести в ОДНО дерево — новое.
# `t>=min`, а не `t>0`: пустой список (контейнер лежит) иначе выглядел бы «всё в новом».
mounts_in(){  # mounts_in <контейнер> <дерево> <сколько монтов минимум>
  local out
  out=$(inspect -f '{{range .Mounts}}{{if eq .Type "bind"}}{{println .Source}}{{end}}{{end}}' "$1") || { echo X; return; }
  printf '%s\n' "$out" | awk -v root="$RELEASES_DIR/" -v new="$2" -v min="$3" '
    index($0,root)==1 { t++; if ($0!=new && index($0,new"/")!=1) b++ }
    END { print (t>=min && b==0) ? 1 : 0 }'
}
# 1 — все четыре контейнера контура смонтированы с этого дерева, 0 — нет, X — не знаю.
mounts_all_in(){  # mounts_all_in <дерево>
  local tree="$1" c want got out=1
  for c in "$SCHED" $GATEWAY_CONTAINERS; do
    case "$c" in "$SCHED") want=10 ;; *) want=1 ;; esac
    got=$(mounts_in "$c" "$tree" "$want")
    [ "$got" = X ] && { echo X; return; }
    [ "$got" = 1 ] || out=0
  done
  echo "$out"
}
# 1 — принято, 0 — не принято, X — проверить нечем (вслепую не решаем).
acceptance_seen(){  # acceptance_seen <новое дерево> <StartedAt scheduler'а>
  local new="$1" started="$2" dags errs gw n c line want got
  dags=$(metadb "SELECT count(*) FROM dag WHERE dag_id IN ($CORE_DAGS_SQL) AND is_active AND NOT has_import_errors AND last_parsed_time > TIMESTAMPTZ '$started';")
  errs=$(metadb "SELECT count(*) FROM import_error;")
  case "$dags$errs" in *X*) echo X; return ;; esac
  [ "$dags" = 5 ] || { echo 0; return; }
  [ "$errs" = 0 ] || { echo 0; return; }
  # Три шлюза ОДНИМ вызовом: `docker inspect a b c` при отсутствии имени печатает только
  # найденные и отдаёт rc=1 — «нет контейнера» иначе было бы неотличимо от «упал docker».
  # Метка проекта обязательна: рядом живёт ЧУЖОЙ контейнер, буквально названный
  # sofascore_proxy_filter (проект dpf-whoscored-merge), и он тоже healthy.
  # shellcheck disable=SC2086
  gw=$(inspect -f '{{.State.Health.Status}} {{.HostConfig.Memory}} {{index .Config.Labels "com.docker.compose.project"}}' $GATEWAY_CONTAINERS) || { echo X; return; }
  n=0
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    n=$(( n + 1 ))
    [ "$line" = "healthy 1073741824 sofascore-gw" ] || { echo 0; return; }
  done <<< "$gw"
  [ "$n" = 3 ] || { echo 0; return; }
  # Монты И scheduler'а, И трёх шлюзов: половинчатый выкат (scheduler на новом дереве,
  # шлюзы на старом) иначе прошёл бы приёмку.
  got=$(mounts_all_in "$new")
  [ "$got" = X ] && { echo X; return; }
  [ "$got" = 1 ] || { echo 0; return; }
  # Слоты сверяем со СНИМКОМ, а не с литералом 1/1/1: первое же расширение полосы дало бы
  # ложный откат.
  for c in $POOLS; do
    got=$(metadb "SELECT slots FROM slot_pool WHERE pool='$c';")
    [ "$got" = X ] && { echo X; return; }
    [ "$got" = "$(pool_want "$c")" ] || { echo 0; return; }
  done
  # Сторожа: Type=simple + Restart=always означают, что `is-active` отвечает active сразу
  # после exec, а MainPID меняется — /proc читаем каждый раз заново, не по кэшу.
  while IFS= read -r line; do
    [ "$(timeout -k 5 20 systemctl show -p ActiveState --value "$line" 2>/dev/null)" = active ] || { echo 0; return; }
    c=$(timeout -k 5 20 systemctl show -p MainPID --value "$line" 2>/dev/null)
    case "$c" in ''|0|*[!0-9]*) echo 0; return ;; esac
    tr '\0' ' ' < "/proc/$c/cmdline" 2>/dev/null | grep -qF -- "--expected-mount $new" || { echo 0; return; }
  done <<< "$WATCHDOG_UNITS"
  echo 1
}
CORE_DAGS_SQL=$(for d in $CORE_DAGS; do printf "'%s'," "$d"; done); CORE_DAGS_SQL=${CORE_DAGS_SQL%,}

# Единственный источник истины о бое — env-файл контура; deploy.sh перепинывает его ДО
# пересоздания контейнеров, поэтому обрыв между этими шагами оставляет env на новом дереве
# при контейнерах на старом. Возврат трёх строк — первый шаг любого отката, и он всегда
# сверяется перечитыванием: без сверки корня «откат» пересоздал бы контейнеры обратно на
# НОВОЕ дерево, то есть доставил бы; без сверки артефакта старые контейнеры поднялись бы с
# чужим или пустым бюджетом, а приёмка env-файл не читает и такого не заметила бы.
env_file_value(){ sed -n "s/^$1=//p" "$ENV_FILE" 2>/dev/null | head -1; }
# Снимок — единственная цель отката, и неполный снимок опаснее отсутствующего: пустые
# OLD_ARTIFACT_HOST/ID выглядели бы «успешно восстановленными», а старые контейнеры
# поднялись бы с пустым пином бюджета (для compose это fail-closed переменная).
snapshot_ok(){
  local p v
  for v in OLD_RELEASE_ROOT NEW_RELEASE_ROOT OLD_ARTIFACT_HOST OLD_ARTIFACT_ID; do
    [ -n "$(snap_get "$v")" ] || { log "снимок отката неполон: нет $v"; return 1; }
  done
  case "$(snap_get HIST_PAUSED)$(snap_get REFRESH_PAUSED)" in
    tt|tf|ft|ff) ;;
    *) log "снимок отката неполон: паузы кампаний не прочитаны"; return 1 ;;
  esac
  for p in $POOLS; do
    case "$(snap_get "POOL_$p")" in
      ''|*[!0-9]*) log "снимок отката неполон: слоты пула $p не число"; return 1 ;;
    esac
  done
  return 0
}
repin_env_to_old(){  # repin_env_to_old <старое дерево>
  local old="$1" root host id ok=1
  snapshot_ok || return 1
  # Коды возврата каждой правки важны: отсутствующая строка KEY= даёт rc=2, и без проверки
  # «откат» шёл бы дальше с непереписанным пином.
  sofascore_set_env_var "$ENV_FILE" SOFASCORE_RELEASE_ROOT "$old" || ok=0
  sofascore_set_env_var "$ENV_FILE" SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST "$(snap_get OLD_ARTIFACT_HOST)" || ok=0
  sofascore_set_env_var "$ENV_FILE" SOFASCORE_PROXY_BUDGET_ARTIFACT_ID "$(snap_get OLD_ARTIFACT_ID)" || ok=0
  sofascore_load_env "$ENV_FILE"
  # Сверяемся с ФАЙЛОМ, а не с переменными оболочки: sofascore_load_env снимает только те
  # ключи, что есть в файле, поэтому ИСЧЕЗНУВШАЯ строка оставила бы в памяти прежнее
  # значение и проверка прошла бы на устаревших данных, ничего не заметив.
  root=$(env_file_value SOFASCORE_RELEASE_ROOT)
  host=$(env_file_value SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST)
  id=$(env_file_value SOFASCORE_PROXY_BUDGET_ARTIFACT_ID)
  if [ "$ok" = 1 ] && [ "$root" = "$old" ] \
     && [ "$host" = "$(snap_get OLD_ARTIFACT_HOST)" ] \
     && [ "$id" = "$(snap_get OLD_ARTIFACT_ID)" ]; then
    return 0
  fi
  log "ENV-ФАЙЛ НЕ ВЕРНУЛСЯ К СНИМКУ (корень='$root', артефакт='$host', id='$id') — контейнеры не трогаю"
  return 1
}

# --- откат комплектом -------------------------------------------------------------------
# Best-effort: шаги выполняются, даже если предыдущий не удался. Исход решается ТОЛЬКО
# подтверждением теми же шестью признаками против старого дерева, а не кодами возврата.
rollback_to_old(){  # rollback_to_old <старое дерево> <StartedAt scheduler'а до отката>
  local old="$1" started_before="$2" c left seen tries deadline busy_now
  set +e
  # Улики до отката: --force-recreate сносит контейнеры вместе с логами.
  for c in "$SCHED" $GATEWAY_CONTAINERS; do
    echo "--- docker logs $c --tail 200 ---" >> "$LOG"
    timeout -k 5 60 docker logs "$c" --tail 200 >> "$LOG" 2>&1
  done
  repin_env_to_old "$old" || return 1
  # Возврат пауз и слотов (ниже, restore_state) — часть исхода отката, а не побочный шаг:
  # бой на старом дереве при остановленной кампании — это «зелёно, но пусто». Ответ несёт
  # RESTORE_NOTE, его читают обе ветки «откат подтверждён».
  # Пересоздание scheduler'а обрывает любой идущий таск: второй раз за ночь этого делать
  # нельзя не глядя. Ждём освобождения контура, но НЕ бесконечно: бой, оставшийся на
  # непринятом дереве, хуже оборванного прогона. Если ждать не дождались — откатываем всё
  # равно и говорим об этом в алерте, а не молчим.
  ROLLBACK_NOTE=""
  left=$(( ${ROLLBACK_IDLE_WAIT:-300} ))
  while [ "$left" -gt 0 ] && [ "$(contour_busy)" != 0 ]; do sleep 30; left=$(( left - 30 )); done
  busy_now=$(contour_busy)
  if [ "$busy_now" != 0 ]; then
    ROLLBACK_NOTE=" ВНИМАНИЕ: откат пересоздавал контейнеры при непустом контуре (dag_run в работе: '$busy_now') — идущий прогон дейли/актуалки/обслуживания оборван, его трафик оплачен впустую."
    log "ОТКАТ ПРИ ЗАНЯТОМ КОНТУРЕ (dag_run: '$busy_now') — идущий прогон будет оборван"
  fi
  SOFASCORE_RELEASE_ROOT="$old" \
  SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST="$(snap_get OLD_ARTIFACT_HOST)" \
  SOFASCORE_PROXY_BUDGET_ARTIFACT_ID="$(snap_get OLD_ARTIFACT_ID)" \
  timeout -k 30 600 docker compose -p sofascore-airflow -f "$old/deploy/sofascore/airflow.compose.yaml" \
    --env-file "$PLATFORM_ENV" --env-file "$ENV_FILE" \
    up -d --no-deps --force-recreate airflow-scheduler >> "$LOG" 2>&1
  # shellcheck disable=SC2086
  SOFASCORE_RELEASE_ROOT="$old" \
  SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST="$(snap_get OLD_ARTIFACT_HOST)" \
  SOFASCORE_PROXY_BUDGET_ARTIFACT_ID="$(snap_get OLD_ARTIFACT_ID)" \
  timeout -k 30 600 docker compose -p sofascore-gw -f "$old/deploy/sofascore/gateway.compose.yaml" \
    --project-directory "$old" \
    --env-file "$PLATFORM_ENV" --env-file "$ENV_FILE" \
    up -d --no-deps --force-recreate $GATEWAYS >> "$LOG" 2>&1
  while IFS= read -r c; do
    timeout -k 5 60 systemctl restart "$c" >> "$LOG" 2>&1
  done <<< "$WATCHDOG_UNITS"
  restore_state
  deadline=$(( $(date -u +%s) + ACCEPT_WAIT ))
  tries=$(( ACCEPT_WAIT / ACCEPT_POLL + 1 ))
  while :; do
    seen=$(acceptance_rollback "$old" "$started_before")
    [ "$seen" = 1 ] && return 0
    tries=$(( tries - 1 ))
    [ "$tries" -le 0 ] && break
    [ "$(date -u +%s)" -ge "$deadline" ] && break
    sleep "$ACCEPT_POLL"
    # Ещё раз ПОСЛЕ сна: перешагнув потолок во сне, цикл успел бы начать новый заход
    # приёмки — десяток внешних вызовов с таймаутами 20–30 с каждый.
    [ "$(date -u +%s)" -ge "$deadline" ] && break
  done
  return 1
}
# У отката тот же шестипризнаковый критерий, только дерево — старое, а «контейнер
# пересоздан» считается от StartedAt, который был ДО отката.
acceptance_rollback(){
  local started
  started=$(inspect -f '{{.State.StartedAt}}' "$SCHED") || { echo X; return; }
  [ -n "$started" ] && [ "$started" != "$2" ] || { echo 0; return; }
  acceptance_seen "$1" "$started"
}
contour_busy(){
  # Историю сюда НЕ включаем: ею занимается шаг drain внутри deploy.sh.
  metadb "SELECT count(*) FROM dag_run WHERE dag_id IN ('$DAILY','$REFRESH','$MAINT') AND state IN ('queued','running');"
}

# --- окно доставки ----------------------------------------------------------------------
# Считается в одном месте, потому что спрашивают о нём двое: шаг «окно и запас» и ветка
# «master недоступен» — она выходила из тика раньше всякого разговора об окне, и обещанный
# ежесуточный сигнал не звучал ни разу за ночь, когда до master не дозвониться.
set_window(){   # выставляет hm, end, now, deadline, in_window
  now=$(date -u +%s)
  hm=$((10#$(date -u +%H%M)))
  end=$WINDOW_TO
  [ "$(date -u +%u)" = 7 ] && end=$SUNDAY_TO   # вс. 05:00 UTC — обслуживание манифеста
  deadline=$(date -u -d "$TODAY ${end:0:2}:${end:2:2}" +%s)
  in_window=0
  { [ "$hm" -ge "$((10#$WINDOW_FROM))" ] && [ "$hm" -le "$((10#$end))" ]; } && in_window=1
}
# Один суточный маркер на все причины: ночь без доставки — одна новость, а не три.
announce_missed_window(){  # announce_missed_window <текст>
  [ "$in_window" = 1 ] || return 0
  [ "$(( deadline - $(date -u +%s) ))" -le 300 ] || return 0
  said_today "$STATE/sofascore-window-missed-$TODAY" && return 0
  tg_durable "$1"
  mark_said "$STATE/sofascore-window-missed-$TODAY"
}

# ---- 0. Каталог состояния ---------------------------------------------------------------
# НЕ создаём: `mkdir -p` означал бы, что потеря каталога незаметна — вместе с ним исчезают
# маркер незакрытой доставки, снимок отката и выключатель.
if [ -L "$STATE" ] || [ ! -d "$STATE" ]; then
  log "КАТАЛОГ СОСТОЯНИЯ $STATE НЕ НА МЕСТЕ (ссылка или не каталог) — не трогаю ничего"
  said="$(dirname "$LOG")/sofascore-state-broken-$TODAY"
  if ! is_plain "$said"; then
    tg "🆘 SofaScore: каталог состояния автомата ($STATE) — ссылка или его нет. Замок, маркеры, снимок отката и очередь ушли бы в чужое место. Автомат не делает НИЧЕГО и заглушить себя не может — выключатель лежит там же. НУЖНЫ РУКИ. Лог: $LOG" \
      && { mk_marker "$said" || true; }
  fi
  exit 1
fi

# ---- 0а. Собственный лог -----------------------------------------------------------------
# В лог перенаправляется вывод freeze_release.sh и deploy.sh: подменённый путь означает,
# что чужой файл (вплоть до файла боевого дерева) будет исписан выводом выката.
if odd_path "$LOG"; then
  echo "$(date -u +%FT%TZ) НА МЕСТЕ ЛОГА $LOG НЕ ОБЫЧНЫЙ ФАЙЛ — глушу автомат" >&2
  tg "🆘 SofaScore: на месте лога автомата ($LOG) не обычный файл — вывод выката ушёл бы по ссылке в чужой файл. Ничего не делаю. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора."
  set_off
  exit 1
fi

# ---- 0б. Обобщённый гейт подмены ----------------------------------------------------------
# У каждого своего пути ровно одно законное состояние: «обычный файл» или «ничего нет».
# Symlink принимает запись с нулевым кодом и читается пустым; каталог рвёт перенаправление;
# FIFO вешает чтение навсегда вместе с замком. Любой из них — руки, а не «маркера нет».
for p in "$LOCK" "$PENDING" "$OFF" "$INFLIGHT" "$ACCEPTED" "$SNAPSHOT" "$ATTEMPTED" "$FAILNIGHTS"; do
  if odd_path "$p"; then
    log "НА МЕСТЕ $p НЕ ОБЫЧНЫЙ ФАЙЛ — состояние автомата недостоверно, глушу"
    tg "🆘 SofaScore: на месте $p не обычный файл — понять состояние автомата нельзя, писать туда опасно. Ничего не делаю. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
    exit 1
  fi
done

# ---- 0в. Замок ---------------------------------------------------------------------------
# Один экземпляр: доставка идёт десятки минут, cron будит каждые 5.
if ! { exec 9>"$LOCK"; } 2>/dev/null; then
  lock_alert "не могу открыть замок $LOCK"
  exit 1
fi
# Не всякий ненулевой код flock — конкуренция. Ровно 1 означает «замок занят», и только он
# оправдывает молчаливый выход; любой другой код — поломка.
flock -n 9; frc=$?
if [ "$frc" != 0 ]; then
  [ "$frc" = 1 ] && exit 0
  log "flock отказал кодом $frc — это не конкуренция"
  lock_alert "flock отказал кодом $frc (не конкуренция)"
  exit 1
fi

# ---- 0г. Недоставленные алерты ------------------------------------------------------------
# ДО выключателя: заглушенный автомат тем более обязан договорить то, что не смог сказать.
flush_pending

# ---- 0д. Выключатель ----------------------------------------------------------------------
if is_plain "$OFF"; then
  if is_plain "$INFLIGHT" && ! said_today "$STATE/sofascore-off-reminded-$TODAY"; then
    tg_durable "🆘 SofaScore: автомат заглушен ($OFF), а незакрытая доставка ещё висит ($INFLIGHT). Бой: $LIVE_ROOT. НУЖНЫ РУКИ."
    mark_said "$STATE/sofascore-off-reminded-$TODAY"
  fi
  exit 0
fi

# ---- 0е. Предпосылки, без которых доставка сломала бы бой ---------------------------------
# Без общего .env платформы `docker compose` упал бы уже ПОСЛЕ перепина env-файла контура.
if [ ! -r "$PLATFORM_ENV" ]; then
  log "НЕТ ОБЩЕГО .env ПЛАТФОРМЫ ($PLATFORM_ENV) — доставка сломала бы контур на середине"
  tg_durable "🆘 SofaScore: общий .env платформы ($PLATFORM_ENV) недоступен — compose упал бы уже после перепина env-файла контура. Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  set_off
  exit 1
fi
# deploy.sh — инструмент владельца по слову «выкатывай»; запрещать его автомат не вправе,
# но и лезть под него не должен.
if pgrep -f 'deploy/sofascore/deploy[.]sh' >/dev/null 2>&1; then
  log "идёт ручной выкат (deploy.sh) — пропускаем тик"
  exit 0
fi

# ---- 1. Разбор прерванной доставки --------------------------------------------------------
# Маркер остаётся, только если прошлый запуск умер между началом доставки и её закрытием
# (в том числе перезагрузкой машины). Сначала ФАКТ, потом действие.
if is_plain "$INFLIGHT"; then
  OLD=$(snap_get OLD_RELEASE_ROOT)
  NEW=$(snap_get NEW_RELEASE_ROOT)
  # Неполный снимок опаснее отсутствующего: пустые пины артефакта прошли бы сверку как
  # «восстановленные», и откат поднял бы старые контейнеры с пустым бюджетом.
  if ! snapshot_ok; then
    log "НЕЗАКРЫТАЯ ДОСТАВКА, а снимок отката ($SNAPSHOT) пуст, неполон или нечитаем — не трогаю ничего"
    tg_durable "🆘 SofaScore: прошлая доставка оборвалась, но снимок отката ($SNAPSHOT) пуст, неполон или нечитаем — куда и с каким пином бюджета возвращать бой, неизвестно. Ничего не трогаю. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
    exit 1
  fi
  RESTORE_PENDING=1
  on_old=$(mounts_all_in "$OLD")
  if [ "$on_old" = X ]; then
    log "НЕЗАКРЫТАЯ ДОСТАВКА, но docker не отвечает — понять, где стоит бой, нечем"
    tg_durable "⚠️ SofaScore: прошлая доставка оборвалась, а docker не отвечает — понять, с какого дерева смонтированы контейнеры, нечем. Ничего не трогаю, повторю следующим заходом. Лог: $LOG"
    exit 1
  fi
  if [ "$on_old" = 1 ]; then
    # Контейнеры не пересоздавали — но deploy.sh перепинивает env ДО них, и обрыв ровно в
    # этой щели оставляет env на новом дереве при контейнерах на старом. Не вернуть три
    # строки — значит оставить контур смешанным навсегда: следующий тик увидел бы
    # HEAD($SOFASCORE_RELEASE_ROOT) == master, записал бы маркер приёмки без единой
    # проверки и молча выходил бы нулём каждые пять минут.
    if ! repin_env_to_old "$OLD"; then
      tg_durable "🆘 SofaScore: прошлая доставка оборвалась до пересоздания контейнеров (бой на $OLD), но вернуть env-файл к снимку не удалось — контур остался бы смешанным: файл говорит одно, контейнеры смонтированы с другого дерева. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
      set_off
      exit 1
    fi
    # Паузы и слоты возвращаем ЗДЕСЬ, а не EXIT-trap'ом: иначе сообщение об исходе уходило
    # бы раньше восстановления, и оставшийся осушённым пул (или запаузенная история) не
    # попал бы ни в один алерт — автомат отчитался бы «⚠️ ничего страшного» о ночи, в
    # которой кампания не работает.
    if restore_state; then
      log "НЕЗАКРЫТАЯ ДОСТАВКА, но бой целиком на $OLD — контейнеры не трогаю, env возвращён к снимку"
      tg_durable "⚠️ SofaScore: прошлая доставка оборвалась на середине, но бой целиком остался на прежнем дереве ($OLD) — контейнеры не трогал, env-файл, паузы и пулы вернул к снимку. Следующая попытка — в ближайшее окно. Лог: $LOG"
      rm -f "$INFLIGHT"
    else
      log "НЕЗАКРЫТАЯ ДОСТАВКА: бой на $OLD, но контур не вернулся в работу:$RESTORE_NOTE"
      tg_durable "🆘 SofaScore: прошлая доставка оборвалась (бой остался на $OLD, env вернул к снимку), но контур не вернулся в рабочее состояние —$RESTORE_NOTE Кампания будет стоять, пока это не поправят. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
      rm -f "$INFLIGHT"
      set_off
    fi
    exit 1
  fi
  log "НЕЗАКРЫТАЯ ДОСТАВКА и бой не на $OLD — откатываю комплектом"
  started_before=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
  if rollback_to_old "$OLD" "$started_before"; then
    # Маркер снимаем в любом случае: откат подтверждён, бой на старом дереве, разбирать
    # «прерванную доставку» следующему тику незачем.
    rm -f "$INFLIGHT"
    if [ -n "$RESTORE_NOTE" ]; then
      log "ОТКАТ ПОДТВЕРЖДЁН, НО КОНТУР НЕ ВЕРНУЛСЯ В РАБОТУ:$RESTORE_NOTE — глушу автомат"
      tg_durable "🆘 SofaScore: прошлая доставка оборвалась, откат на $OLD подтверждён, НО контур не вернулся в рабочее состояние —$RESTORE_NOTE${ROLLBACK_NOTE} Кампания будет стоять, пока это не поправят. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
      set_off
    else
      tg_durable "⛔ SofaScore: прошлая доставка оборвалась на середине. Откат на $OLD подтверждён (пять DAG перечитаны, ошибок импорта нет, три шлюза healthy на старом дереве, сторожа на нём же).${ROLLBACK_NOTE} Следующая попытка — в ближайшее окно. Лог: $LOG"
    fi
  else
    tg_durable "🆘 SofaScore: прошлая доставка оборвалась И откат на $OLD не подтверждён. НУЖНЫ РУКИ. Если шлюз не поднялся и после отката — смотреть формат $(dirname "$(snap_get OLD_ARTIFACT_HOST)")/../gateway-state*/sofascore_allocations.json: новый код мог переписать ledger так, что старый бинарь его не читает; процедурой это не лечится. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
  fi
  exit 1
fi

# ---- 2. Нужна ли доставка ------------------------------------------------------------------
# Идёт ПЕРЕД проверкой законности боя: если доставлять нечего, к бою не обращаемся вовсе.
# --no-optional-locks обязателен: обычный rev-parse пишет .git/index, а дерево read-only.
LIVE=$(git -C "$LIVE_ROOT" --no-optional-locks rev-parse HEAD 2>/dev/null)
WANT=$(timeout -k 5 60 git ls-remote "$SOURCE_REPO" refs/heads/master 2>/dev/null | cut -f1)
is_sha40(){ case "$1" in *[!0-9a-f]*) return 1 ;; esac; [ "${#1}" = 40 ]; }
if ! is_sha40 "$WANT"; then
  log "master недоступен (git ls-remote $SOURCE_REPO вернул '$WANT') — вслепую не переключаемся"
  # Молчаливый выход съедал бы обещанный ежесуточный сигнал: недоступный master держит бой на
  # старом коде ровно так же, как неудачная доставка, и узнать об этом надо той же ночью.
  set_window
  announce_missed_window "⚠️ SofaScore: окно доставки закрывается, а master недоступен (git ls-remote $SOURCE_REPO вернул '$WANT') — бой остаётся на ${LIVE:0:8}. Причина — в $LOG; следующая попытка завтра."
  exit 0
fi
if [ "$LIVE" = "$WANT" ]; then
  # Совпадение HEAD — ЕЩЁ НЕ «бой в порядке». deploy.sh (в том числе ручной, по слову
  # «выкатывай») перепинивает env-файл ДО пересоздания контейнеров, и обрыв ровно в этой
  # щели даёт env на новом дереве при контейнерах на старом: HEAD сходится, контур
  # смешанный. Раньше автомат в этом состоянии молча писал маркер приёмки и выходил нулём
  # каждые пять минут — навсегда.
  live_mounts=$(mounts_all_in "$LIVE_ROOT")
  if [ "$live_mounts" = X ]; then
    log "бой на master, но проверить монты нечем (docker не отвечает) — маркер приёмки не трогаю"
    exit 0
  fi
  if [ "$live_mounts" != 1 ]; then
    log "КОНТУР СМЕШАННЫЙ: env-файл говорит $LIVE_ROOT (это master), а контейнеры смонтированы с другого дерева"
    if ! said_today "$STATE/sofascore-mixed-contour-$TODAY"; then
      tg_durable "🆘 SofaScore: env-файл контура говорит $LIVE_ROOT (это master), но контейнеры смонтированы с ДРУГОГО дерева — контур смешанный. Похоже, выкат оборвался между перепином env и пересозданием контейнеров. Доставлять нечего, а чинить это автомат не берётся: пересоздание контейнеров вне окна оборвало бы идущие прогоны. НУЖНЫ РУКИ. Лог: $LOG"
      mark_said "$STATE/sofascore-mixed-contour-$TODAY"
    fi
    exit 1
  fi
  # Бой на master и целиком на одном дереве. Маркер приёмки — утверждение «принято», и по
  # одним монтам его выдавать нельзя: контракт из шести признаков ловит и сторожа на старом
  # дереве, и осушённый пул, и ошибку импорта. Полную проверку делаем ровно там, где маркер
  # ВЫДАЁТСЯ: в установившемся режиме (маркер уже равен master) она стоила бы полутора
  # десятков внешних вызовов каждые пять минут круглосуточно, а автомат не сторож кампании.
  if [ "$(head -c 64 "$ACCEPTED" 2>/dev/null | tr -d ' \n\r')" != "$WANT" ]; then
    started=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
    seen=X
    [ -n "$started" ] && seen=$(acceptance_seen "$LIVE_ROOT" "$started")
    if [ "$seen" = X ]; then
      log "бой на master, но контракт приёмки проверить нечем — маркер приёмки не трогаю"
      exit 0
    fi
    if [ "$seen" != 1 ]; then
      log "БОЙ НА MASTER, НО КОНТРАКТ ПРИЁМКИ НЕ СОШЁЛСЯ — маркер приёмки не выдаю"
      if ! said_today "$STATE/sofascore-contract-failed-$TODAY"; then
        tg_durable "⚠️ SofaScore: бой стоит на master ($LIVE_ROOT), но контур не проходит контракт приёмки из шести признаков (пять DAG перечитаны и без ошибок импорта, три шлюза healthy на 1 GiB в проекте sofascore-gw, монты scheduler'а и шлюзов в этом дереве, слоты пулов как ожидается, три сторожа на нём же). Маркер приёмки не выдаю, доставлять нечего. Разбор: $LOG"
        mark_said "$STATE/sofascore-contract-failed-$TODAY"
      fi
      exit 0
    fi
    printf '%s\n' "$WANT" > "$ACCEPTED" 2>/dev/null || log "маркер приёмки $ACCEPTED не записан"
  fi
  rm -f "$FAILNIGHTS"
  exit 0
fi

# ---- 3. Законность боя ----------------------------------------------------------------------
# Спрашивается только когда доставка нужна: тогда мы собираемся ЗАПОМНИТЬ это дерево как
# цель отката, и оно обязано быть настоящим замороженным деревом, а не чем угодно.
case "$LIVE_ROOT" in
  "$RELEASES_DIR"/*) ;;
  *) log "БОЕВОЕ ДЕРЕВО $LIVE_ROOT ВНЕ КАТАЛОГА РЕЛИЗОВ $RELEASES_DIR — не доставляю"
     tg_durable "🆘 SofaScore: боевое дерево ($LIVE_ROOT) лежит вне каталога релизов ($RELEASES_DIR) — откатывать было бы некуда. Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
     set_off; exit 1 ;;
esac
is_sha40 "$LIVE" || LIVE=""
DIRTY=$(git -C "$LIVE_ROOT" --no-optional-locks status --porcelain 2>/dev/null)
if [ -z "$LIVE" ] || [ -n "$DIRTY" ]; then
  log "БОЕВОЕ ДЕРЕВО НЕ В ЗАКОННОМ СОСТОЯНИИ (HEAD='$LIVE', правок: $(printf '%s' "$DIRTY" | grep -c . || true)) — не доставляю"
  tg_durable "🆘 SofaScore: боевое дерево ($LIVE_ROOT) не в законном состоянии — HEAD='$LIVE', в дереве есть правки. Оно должно быть чистым замороженным клоном: иначе откат уничтожил бы чужую работу. Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  set_off
  exit 1
fi

# ---- 4. Окно и запас времени ------------------------------------------------------------------
set_window
[ "$in_window" = 1 ] || exit 0
announce_missed_window "⚠️ SofaScore: окно доставки закрывается, а бой всё ещё на ${LIVE:0:8} (master ${WANT:0:8}). Причина — в $LOG; следующая попытка завтра."

IDLE_WAIT=$(( deadline - now - DEPLOY_CEILING - ACCEPT_WAIT ))
if [ "$IDLE_WAIT" -lt "$MIN_DRAIN" ]; then
  log "запаса нет ($IDLE_WAIT с до дедлайна за вычетом потолков) — сегодня не доставляем"
  exit 0
fi
DELIVER_TIMEOUT=$(( IDLE_WAIT + DEPLOY_CEILING ))
busy=$(contour_busy)
if [ "$busy" != 0 ]; then
  log "контур занят (dag_run в работе: '$busy'; X = метабаза недоступна) — тик пропущен"
  exit 0
fi
if is_plain "$ATTEMPTED"; then
  exit 0
fi

# ---- 5. Заморозка нового дерева ------------------------------------------------------------------
# Бой она не меняет: при любой неудаче защёлка не тратится и INFLIGHT не создаётся.
# Путь берётся из ВЫВОДА заморозки, а не вычисляется из sha: freeze_release.sh:41 берёт
# `git rev-parse --short=8` — «минимум 8, больше при коллизии».
NEW="$RELEASES_DIR/release-${WANT:0:8}"
if [ -d "$NEW" ]; then
  # Каталог от прошлой ночи может быть БИТЫМ: mv идёт до chmod 755 и до mkdir logs, а
  # airflow.compose.yaml монтирует ${ROOT}/logs с create_host_path: false. Пересоздать
  # его нельзя — freeze_release.sh:43 откажется, поэтому переиспользуем только целый.
  ok=1
  [ -d "$NEW/logs" ] || ok=0
  [ "$(stat -c %a "$NEW" 2>/dev/null)" = 755 ] || ok=0
  [ "$(git -C "$NEW" --no-optional-locks rev-parse HEAD 2>/dev/null)" = "$WANT" ] || ok=0
  [ -z "$(git -C "$NEW" --no-optional-locks status --porcelain 2>/dev/null)" ] || ok=0
  if [ "$ok" != 1 ]; then
    log "КАТАЛОГ $NEW УЖЕ ЕСТЬ И БИТЫЙ (logs/, права 755, HEAD=$WANT, чистота) — пересоздать его freeze_release.sh не даст"
    tg_durable "⚠️ SofaScore: каталог релиза $NEW уже существует и не годится (нет logs/, права не 755, HEAD не тот или дерево грязное). Пересоздать его freeze_release.sh откажется — убрать руками. Доставки сегодня не будет. Лог: $LOG"
    exit 1
  fi
  log "переиспользую целый каталог $NEW от прошлой попытки"
else
  log "замораживаю $WANT"
  if ! out=$(timeout -k 30 900 "$LIVE_ROOT/deploy/sofascore/freeze_release.sh" "$WANT" 2>&1); then
    printf '%s\n' "$out" >> "$LOG"
    tg_durable "⚠️ SofaScore: заморозка дерева $WANT не удалась — бой не тронут, доставки сегодня не будет. Лог: $LOG"
    exit 1
  fi
  printf '%s\n' "$out" >> "$LOG"
  NEW=${out##*дерево заморожено: }
  NEW=${NEW%% (sha *}
  if [ ! -d "$NEW" ]; then
    tg_durable "⚠️ SofaScore: заморозка отчиталась деревом '$NEW', которого нет. Бой не тронут, доставки сегодня не будет. Лог: $LOG"
    exit 1
  fi
fi

# Дедлайн держится по ЧАСАМ, а не по началу тика: заморозка дерева занимает до 900 с, и
# воскресный старт в 03:30 уезжал бы к 05:00 против объявленного дедлайна 04:45 — прямо на
# обслуживание манифеста. Пересчитываем ЗДЕСЬ, до снимка: дальше стоит RESTORE_PENDING=1, и
# выход прошёл бы через trap restore_state, чья неудача осталась бы немой.
IDLE_WAIT=$(( deadline - $(date -u +%s) - DEPLOY_CEILING - ACCEPT_WAIT ))
if [ "$IDLE_WAIT" -lt "$MIN_DRAIN" ]; then
  log "после заморозки запаса нет ($IDLE_WAIT с до дедлайна за вычетом потолков) — сегодня не доставляем"
  announce_missed_window "⚠️ SofaScore: окно доставки закрывается, а бой всё ещё на ${LIVE:0:8} (master ${WANT:0:8}): заморозка дерева съела запас. Причина — в $LOG; следующая попытка завтра."
  exit 0
fi
DELIVER_TIMEOUT=$(( IDLE_WAIT + DEPLOY_CEILING ))

# ---- 6. Снимок боя ---------------------------------------------------------------------------
OLD="$LIVE_ROOT"
SCHED_CREATED_BEFORE=$(inspect -f '{{.Created}}' "$SCHED")
if [ -z "$SCHED_CREATED_BEFORE" ]; then
  log "не читается .Created контейнера $SCHED — приёмке не на что опереться, доставки не будет"
  tg_durable "⚠️ SofaScore: не читается .Created контейнера $SCHED — приёмке не на что опереться (факт пересоздания недоказуем). Бой не тронут. Лог: $LOG"
  exit 1
fi
if ! mk_marker "$SNAPSHOT"; then
  tg_durable "🆘 SofaScore: не могу записать снимок отката ($SNAPSHOT) — без него откатывать некуда. Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  set_off
  exit 1
fi
{
  printf 'OLD_RELEASE_ROOT=%s\n' "$OLD"
  printf 'NEW_RELEASE_ROOT=%s\n' "$NEW"
  printf 'OLD_ARTIFACT_HOST=%s\n' "${SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST:-}"
  printf 'OLD_ARTIFACT_ID=%s\n' "${SOFASCORE_PROXY_BUDGET_ARTIFACT_ID:-}"
  printf 'HIST_PAUSED=%s\n' "$(metadb "SELECT is_paused FROM dag WHERE dag_id='$HIST';")"
  printf 'REFRESH_PAUSED=%s\n' "$(metadb "SELECT is_paused FROM dag WHERE dag_id='$REFRESH';")"
  for p in $POOLS; do printf 'POOL_%s=%s\n' "$p" "$(metadb "SELECT slots FROM slot_pool WHERE pool='$p';")"; done
  printf 'SCHED_CREATED=%s\n' "$SCHED_CREATED_BEFORE"
} >> "$SNAPSHOT" 2>/dev/null
snap_ok=1
snapshot_ok || snap_ok=0
[ "$(snap_get OLD_RELEASE_ROOT)" = "$OLD" ] || snap_ok=0
[ "$(snap_get NEW_RELEASE_ROOT)" = "$NEW" ] || snap_ok=0
if [ "$snap_ok" != 1 ]; then
  log "СНИМОК ОТКАТА НЕ ЧИТАЕТСЯ ОБРАТНО ИЛИ МЕТАБАЗА НЕ ОТВЕТИЛА — доставки не будет"
  tg_durable "🆘 SofaScore: снимок отката ($SNAPSHOT) не читается обратно или метабаза не ответила про паузы и пулы. Доставки не будет: откатывать было бы вслепую. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  set_off
  exit 1
fi
RESTORE_PENDING=1

# ---- 7. Доставка -----------------------------------------------------------------------------
if ! mk_marker "$ATTEMPTED"; then
  log "НЕ СМОГ создать суточную защёлку $ATTEMPTED — доставку не начинаю"
  tg_durable "🆘 SofaScore: не могу создать суточную защёлку ($ATTEMPTED) — без неё доставка повторялась бы каждые 5 минут. Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  set_off
  exit 1
fi
if ! mk_marker "$INFLIGHT"; then
  log "НЕ СМОГ создать маркер доставки $INFLIGHT — доставку не начинаю"
  tg_durable "🆘 SofaScore: не могу создать маркер доставки ($INFLIGHT) — смерть между выкатом и приёмкой стала бы неотличима от успеха. Доставки не будет. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  set_off
  exit 1
fi
log "ОКНО ОТКРЫТО: доставляю ${WANT:0:8} ($NEW), запас на осушение ${IDLE_WAIT}s, потолок доставки ${DELIVER_TIMEOUT}s"
tg "🚚 SofaScore: окно открыто, начинаю доставку ${WANT:0:8} (автомат)"

# Группой процессов: timeout обязан доставать и потомков. `docker exec` шага
# scheduler-health своего таймаута не имеет и пережил бы родителя, пересоздавая
# контейнеры параллельно с откатом. Рецепт едет вместе с кодом — deploy.sh берётся из
# НОВОГО дерева, тот же принцип, что SCHED_COMPOSE="$RELEASE/…" внутри него.
# 9>&- : без этого потомок унаследовал бы дескриптор замка и держал его.
setsid env SOFASCORE_DEPLOY_IDLE_WAIT="$IDLE_WAIT" \
  timeout -k 30 "$DELIVER_TIMEOUT" "$NEW/deploy/sofascore/deploy.sh" "$NEW" "$OLD" >> "$LOG" 2>&1 9>&- &
dpid=$!
wait "$dpid"; rc=$?
if [ "$rc" = 124 ]; then
  log "ТАЙМАУТ доставки (${DELIVER_TIMEOUT}s) — добиваю группу процессов"
  kill -TERM -"$dpid" 2>/dev/null
  sleep 5
  kill -KILL -"$dpid" 2>/dev/null
fi
log "deploy.sh вернул $rc"

# rc=4 — «контур занят, выкат не начат»: бой не тронут, откатывать нечего. Защёлку снимаем,
# но за ночь такая попытка физически возможна одна: IDLE_WAIT — это запас до дедлайна, и
# следующий тик не пройдёт порог MIN_DRAIN. Цена — до 90 минут ночи без прогресса кампании.
if [ "$rc" = 4 ]; then
  log "контур не освободился за ${IDLE_WAIT}s — выкат не начинался, откатывать нечего"
  # Возврат контура проверяем ЗДЕСЬ, а не оставляем EXIT-trap'у: deploy.sh на этом коде уже
  # вернул пул и паузы сам, но если не вернул — при `exit 0` неудача restore_state ушла бы в
  # один лог, а наружу пошло бы ⚠️ «ничего страшного» о ночи, в которую кампания стоит.
  restored=1
  restore_state || restored=0
  rm -f "$ATTEMPTED" "$INFLIGHT"
  if [ "$restored" != 1 ]; then
    log "ВЫКАТ НЕ НАЧАЛСЯ, И КОНТУР НЕ ВЕРНУЛСЯ В РАБОТУ:$RESTORE_NOTE — глушу автомат"
    tg_durable "🆘 SofaScore: доставка ${WANT:0:8} не состоялась (контур не освободился за ${IDLE_WAIT}s, бой не тронут), И контур не вернулся в рабочее состояние —$RESTORE_NOTE Кампания будет стоять, пока это не поправят. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
    exit 1
  fi
  tg_durable "⚠️ SofaScore: доставка ${WANT:0:8} не состоялась — контур не освободился за ${IDLE_WAIT}s (выкат не начинался, бой не тронут). Следующая попытка завтра. Лог: $LOG"
  exit 0
fi

# ---- 8. Приёмка ------------------------------------------------------------------------------
# Привязка — к ФАКТУ пересоздания контейнера, а не к часам автомата.
# Потолок ожидания держится по ЧАСАМ, а не по сумме sleep: один заход приёмки — это
# десяток внешних вызовов с таймаутами 20–30 с каждый, и «минус ACCEPT_POLL за виток»
# на деградировавшем docker/systemd вынес бы автомат далеко за конец окна, а в
# воскресенье — за 05:00, прямо на обслуживание манифеста. Витки тоже ограничены:
# остановившиеся часы иначе сделали бы цикл вечным.
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
  # Ещё раз ПОСЛЕ сна: перешагнув потолок во сне, цикл начал бы новый заход приёмки.
  [ "$(date -u +%s)" -ge "$accept_deadline" ] && break
done

if [ "$rc" = 0 ] && [ "$seen" = 1 ]; then
  # ---- 9. Успех --------------------------------------------------------------------------
  restored=1
  restore_state || restored=0
  printf '%s\n' "$WANT" > "$ACCEPTED" 2>/dev/null || log "маркер приёмки $ACCEPTED не записан"
  rm -f "$FAILNIGHTS"
  # Код в бою и принят — но если контур не вернулся в рабочее состояние (история осталась
  # на паузе, слот пула не восстановлен), это «зелёно, но пусто»: кампания простоит до
  # утра, ровно то, ради чего автомат и заведён. Такой исход обязан звучать как авария.
  if [ "$restored" != 1 ]; then
    log "ДОСТАВЛЕНО, НО КОНТУР НЕ ВЕРНУЛСЯ В РАБОТУ:$RESTORE_NOTE — глушу автомат"
    # Выключатель обязателен: контур стоит, и следующий коммит начал бы новый выкат прямо
    # на остановленной кампании. Разбирают руки, они же снимают .off.
    tg_durable "🆘 SofaScore: код ${WANT:0:8} доставлен и приёмка сошлась, НО контур не вернулся в рабочее состояние —$RESTORE_NOTE Кампания будет стоять, пока это не поправят. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    rm -f "$INFLIGHT"
    set_off
    exit 1
  fi
  extra=""
  n=$( { ls -d "$RELEASES_DIR"/release-* 2>/dev/null || true; } | grep -c . )
  orphans=$( { ls -d "$RELEASES_DIR"/freeze.* 2>/dev/null || true; } | grep -c . )
  if [ "$n" -gt 5 ] || [ "$orphans" != 0 ]; then
    extra=" Пора убрать: деревьев release-* — $n, осиротевших freeze.* — $orphans."
  fi
  log "ДОСТАВЛЕНО: $NEW (sha ${WANT:0:8}), артефакт ${SOFASCORE_PROXY_BUDGET_ARTIFACT_ID:0:12}"
  # Маркер снимаем ПОСЛЕ гарантированной отправки: смерть между снятием и сообщением
  # оставила бы исход немым, а суточная защёлка — следующие тики молчаливыми.
  tg_durable "✅ SofaScore: доставлено ${WANT:0:8} → $NEW, приёмка подтверждена (пять DAG перечитаны после старта нового scheduler'а, ошибок импорта нет, три шлюза healthy на 1 GiB в проекте sofascore-gw, монты scheduler'а и шлюзов ведут в новое дерево, пулы как были, три сторожа на новом дереве). artifact_id=${SOFASCORE_PROXY_BUDGET_ARTIFACT_ID:0:12}, деплой занял $(( $(date -u +%s) - now ))s.${extra}"
  rm -f "$INFLIGHT"
  exit 0
fi

# ---- 10. Провал: откат комплектом --------------------------------------------------------
log "ПРОВАЛ доставки (rc=$rc, приёмка '$seen') — откатываю бой на $OLD"
started_before=$(inspect -f '{{.State.StartedAt}}' "$SCHED")
fails=$( { cat "$FAILNIGHTS" 2>/dev/null || true; } | tr -cd '0-9' | head -c 2)
fails=$(( ${fails:-0} + 1 ))
{ printf '%s\n' "$fails" > "$FAILNIGHTS"; } 2>/dev/null || fails=$FAIL_NIGHTS_MAX
if rollback_to_old "$OLD" "$started_before"; then
  log "ОТКАТ ПОДТВЕРЖДЁН: бой на $OLD"
  rm -f "$INFLIGHT"
  if [ -n "$RESTORE_NOTE" ]; then
    # Откат сошёлся, но кампания стоит — это не ⛔ «попробуем завтра», а авария: следующий
    # коммит начал бы новую доставку на остановленном контуре.
    log "ОТКАТ ПОДТВЕРЖДЁН, НО КОНТУР НЕ ВЕРНУЛСЯ В РАБОТУ:$RESTORE_NOTE — глушу автомат"
    tg_durable "🆘 SofaScore: доставка ${WANT:0:8} НЕ состоялась (deploy.sh вернул $rc, приёмка '$seen'), откат на $OLD подтверждён, НО контур не вернулся в рабочее состояние —$RESTORE_NOTE${ROLLBACK_NOTE} Кампания будет стоять, пока это не поправят. НУЖНЫ РУКИ. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
    set_off
  else
    msg="⛔ SofaScore: доставка ${WANT:0:8} НЕ состоялась (deploy.sh вернул $rc, приёмка '$seen'). Откат на $OLD подтверждён теми же шестью признаками.${ROLLBACK_NOTE} Ночь $fails из $FAIL_NIGHTS_MAX подряд. Разбор: $LOG"
    if [ "$fails" -ge "$FAIL_NIGHTS_MAX" ]; then
      msg="$msg Больше не пробую — автомат заглушен, снять $OFF после разбора."
      set_off
    fi
    tg_durable "$msg"
  fi
else
  log "ОТКАТ НЕ ПОДТВЕРЖДЁН — глушу автомат, маркер доставки оставляю"
  tg_durable "🆘 SofaScore: доставка ${WANT:0:8} провалилась (rc=$rc) И откат на $OLD не подтверждён.${ROLLBACK_NOTE}${RESTORE_NOTE:+ Не вернулось:$RESTORE_NOTE} НУЖНЫ РУКИ. Если шлюз не поднялся и после отката — смотреть формат sofascore_allocations.json в каталогах состояния полос: новый код мог переписать ledger так, что старый бинарь его не читает, и процедурой отката это не лечится. Автомат глушу: снять $OFF после разбора. Лог: $LOG"
  set_off
fi
exit 1
