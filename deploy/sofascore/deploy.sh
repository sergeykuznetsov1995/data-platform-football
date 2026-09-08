#!/usr/bin/env bash
# Выкат замороженного дерева на контур SofaScore (проекты sofascore-airflow / sofascore-gw).
# Использование: bash deploy/sofascore/deploy.sh <release-root> [old-release-root]
# Предпосылки: окно вне 13:55–15:35 UTC; свободный замок выката (SOFASCORE_DEPLOY_LOCK);
#   шаг drain осушает пул истории, паузит актуалку и обслуживание манифеста и ждёт завершения
#   того прогона истории, который НАЧАЛ платную работу (история при этом остаётся
#   распаущенной — иначе не отработает validate_historical_scope и оплаченный скоуп не будет
#   засчитан); припарковавшийся в осушённом пуле повтор скоупа шаг переводит в failed сам
#   (drain_breaker.py) — иначе прогон висит вечно и выкат ждёт сам себя;
#   актуалка после выката возвращается в прежнее состояние, обслуживание манифеста —
#   при ручном запуске (из автомата ночной доставки паузу снимает сам автомат).
# Переменные — из $SOFASCORE_ENV_FILE (по умолчанию /etc/data-platform/sofascore.env);
#   скрипт сам переписывает в нём SOFASCORE_RELEASE_ROOT / _PROXY_BUDGET_ARTIFACT_HOST / _ID —
#   этот файл и есть единственный источник «какое дерево в бою» (compose, сторож, приёмка).
# Коды возврата: 2 — предпосылки; 3 — окно дейли; 4 — контур занят или замок выката занят,
#   выкат не начат; 5 — шлюзы; 6 — импорт DAG; 7 — паузы.
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
MAINT=dag_sofascore_manifest_maintenance
# Один источник имени пула на set_pool, предикат ожидания и ломатель: разъехавшись, они
# осушали бы один пул, а ждали задачи в другом.
HIST_POOL=sofascore_history_pool
# Доказательство учёта оплаченного скоупа за этот drain (#1245): автомат ночной доставки
# переносит его в запись окна, утренняя приёмка сверяет DRAIN_RUN_ID с метабазой.
# Каталог — тот же ключ, что читает автомат (SOFASCORE_AUTO_STATE_DIR): два независимо
# настраиваемых пути разъехались бы молча, и автомат читал бы вечно чужой файл (Sol круг 2).
LAST_DRAIN="${SOFASCORE_AUTO_STATE_DIR:-$SOFASCORE_RUNTIME_DIR/auto-deliver}/last-drain.env"
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
  # Прогонов АКТУАЛКИ здесь нет намеренно: к этому моменту она на паузе, а прогон паузного
  # DAG планировщик не двигает — терминального состояния он не получит, и ждать его значит
  # ждать до потолка. Её задачи в счёте остаются: именно их обрывает пересоздание.
  local deadline tries busy active
  deadline=$(( $(date -u +%s) + $1 ))
  tries=$(( $1 / 30 + 1 ))
  while :; do
    # Пустой ответ (метабаза недоступна / timeout) — это «не знаю», а не «свободно».
    busy=$($PSQL "SELECT count(*) FROM task_instance WHERE dag_id IN ('$DAILY','$HIST','$REFRESH') AND state IN ('queued','running');" || true)
    active=$($PSQL "SELECT count(*) FROM dag_run WHERE dag_id IN ('$DAILY','$HIST') AND state IN ('queued','running');" || true)
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
# Ожидание шага drain. Ждать «в контуре нет прогонов истории» нельзя: история идёт
# @continuous, и как только отслеживаемый прогон кончается, планировщик почти мгновенно
# (замер 04.09: медиана 26 с) создаёт следующий, а с осушённым пулом тот остаётся running
# навсегда — его run_historical_scope вечно scheduled.
#
# Ждём прогон, который НАЧАЛ платную работу: у него есть run_historical_scope с
# map_index >= 0 в queued/running/restarting/up_for_retry/success/failed/upstream_failed
# либо scheduled с try_number > 1 (припарковавшийся повтор — ночь 05.09). Прогон без
# такого скоупа платного трафика не купил: его не ждём и не трогаем, после паузы его
# закроет close_stale_runs. Выбранный run_id удерживается до записи доказательства учёта:
# запрос по running-прогонам терминальный прогон уже не вернёт.
#
# Пятое число строки опроса — «припарковано»: скоуп выбранного прогона в осушённом пуле
# в scheduled/up_for_retry. Он не сдвинется до конца выката, а прогон из-за него висит
# running вечно (deadlock-детектор Airflow для этого DAG отключён: max_active_tis_per_dag=1
# у run_historical_scope). Такой скоуп переводит в failed ломатель (drain_breaker.py), и
# дальше планировщик доигрывает хвост сам: validate -> upstream_failed, finalize пишет
# отказ в failures.json, cooldown, propagate, прогон закрыт.
#
# Актуалка и обслуживание манифеста к этому моменту на паузе, а прогон запаущенного DAG
# планировщик не двигает (DagModel.is_paused == false в next_dagruns_to_examine),
# терминального состояния он уже не получит — поэтому по ним ждём отсутствия ЗАДАЧ, а не
# закрытия прогона. Задачи доработают, сам прогон продолжится после снятия паузы.
DRAIN_ROW=""
HIST_RUN=""
HIST_DIAG=""
BREAKER_CALLS=0
BREAKER_RESULT="-"
# Один вызов на шаг, а не пять (Sol круг 3): внешний `timeout` убивает КЛИЕНТА docker exec,
# а процесс в контейнере переживает его (moby#9098). Пять зависших ломателей могли бы
# тронуть задачу уже после того, как drain вернул код 4 и вернул слоты пула. Один вызов
# ограничивает и это окно, и цену ошибки; зависшего добиваем явно (см. break_deadlock).
BREAKER_MAX="${SOFASCORE_DEPLOY_BREAKER_MAX:-1}"
# hist_scope_row [run_id] -> "<run_id>|<pool>|<state>|<try_number>"; "-" — такого прогона
# нет; пустая строка — метабаза не ответила («не знаю», а не «нет»).
# Без аргумента выбирает прогон с начатой платной работой, с аргументом — рассказывает про
# уже выбранный (в том числе терминальный: его состояние и есть диагностика).
hist_scope_row() {
  local where
  if [ -n "${1:-}" ]; then
    where="dr.run_id='$1'"
  else
    where="dr.state IN ('queued','running') AND (ti.state IN ('queued','running','restarting','up_for_retry','success','failed','upstream_failed') OR (ti.state='scheduled' AND ti.try_number>1))"
  fi
  $PSQL "SELECT coalesce((SELECT dr.run_id || '|' || coalesce(ti.pool,'-') || '|' || coalesce(ti.state,'none') || '|' || coalesce(ti.try_number,0) FROM dag_run dr JOIN task_instance ti ON ti.dag_id=dr.dag_id AND ti.run_id=dr.run_id WHERE dr.dag_id='$HIST' AND ti.task_id='run_historical_scope' AND ti.map_index>=0 AND $where ORDER BY dr.start_date NULLS LAST LIMIT 1),'-');" || true
}
# Диагностика ожидания: почему ждём, если ломателю звать некого. Пишется один раз на
# ИЗМЕНЕНИЕ, иначе каждые 30 с в лог шла бы одна и та же строка.
hist_diag() {
  local row pool state try note
  [ -n "$HIST_RUN" ] || return 0
  row=$(hist_scope_row "$HIST_RUN")
  [ -n "$row" ] && [ "$row" != "-" ] || return 0
  pool=$(printf '%s' "$row" | cut -d'|' -f2)
  state=$(printf '%s' "$row" | cut -d'|' -f3)
  try=$(printf '%s' "$row" | cut -d'|' -f4)
  note=""
  if [ "$pool" != "$HIST_POOL" ]; then
    case "$state" in
      scheduled|up_for_retry) note="скоуп в пуле '$pool', drain осушил '$HIST_POOL' — ломателя не зову, жду до потолка" ;;
    esac
  else
    case "$state" in
      queued|running|restarting|up_for_retry|success|failed|upstream_failed|skipped|removed) ;;
      scheduled) [ "${try:-0}" -gt 1 ] || note="скоуп scheduled с try_number=$try — состояние вне протокола, жду до потолка" ;;
      *) note="скоуп в состоянии '$state' — состояние вне протокола, жду до потолка" ;;
    esac
  fi
  [ -n "$note" ] || return 0
  [ "$note" = "$HIST_DIAG" ] && return 0
  HIST_DIAG="$note"
  log "drain: $note"
}
# 0 — ломатель вызван (следующий виток сразу), 1 — звать некого или потолок вызовов исчерпан.
# Best-effort: код возврата на rc выката не влияет. Иначе сбой `docker exec` дал бы код,
# отличный от 4, и автомат ночной доставки откатил бы НЕТРОНУТЫЙ бой.
break_deadlock() {
  [ -n "$HIST_RUN" ] || return 1
  if [ "$BREAKER_CALLS" -ge "$BREAKER_MAX" ]; then
    if [ "$BREAKER_RESULT" != "не-ломается" ]; then
      BREAKER_RESULT="не-ломается"
      log "drain: тупик не ломается ($BREAKER_CALLS вызовов ломателя) — жду до потолка"
    fi
    return 1
  fi
  BREAKER_CALLS=$(( BREAKER_CALLS + 1 ))
  log "drain: припаркованный скоуп прогона '$HIST_RUN' — ломаю тупик (вызов $BREAKER_CALLS из $BREAKER_MAX)"
  # Текст ломателя идёт по stdin: однострочником без кавычек он уже не выражается.
  # 8>&- : дескриптор замка выката потомкам не наследуется.
  # Метка в argv — единственный способ найти ломателя внутри контейнера: код пришёл по
  # stdin, имени файла у него нет.
  local marker="drain-breaker-$$-$BREAKER_CALLS" brc=0
  timeout -k 5 60 docker exec -i sofascore-airflow-scheduler python - \
       "$HIST" "$HIST_RUN" "$HIST_POOL" "$marker" < "$RELEASE/deploy/sofascore/drain_breaker.py" >> "$LOG" 2>&1 8>&- || brc=$?
  if [ "$brc" = 0 ]; then
    BREAKER_RESULT=ok
  else
    BREAKER_RESULT=сбой
    log "drain: ломатель не отработал (код $brc)"
    # 124/137 — сработал timeout: клиент убит, а python в контейнере жив и держит открытую
    # транзакцию на строке dag_run. Добиваем по метке, иначе он проснётся после выката и
    # переведёт задачу уже в чужом мире (Sol круг 3).
    if [ "$brc" = 124 ] || [ "$brc" = 137 ]; then
      if timeout -k 5 30 docker exec sofascore-airflow-scheduler pkill -f "$marker" >> "$LOG" 2>&1 8>&-; then
        log "drain: зависший ломатель $marker добит в контейнере"
      else
        log "MANUAL ACTION REQUIRED: ломатель $marker мог остаться жить в планировщике — проверить руками"
      fi
    fi
  fi
  return 0
}
wait_drained() {  # wait_drained <секунд>; 0 — контур осушён, 1 — потолок исчерпан
  local deadline tries parked
  deadline=$(( $(date -u +%s) + $1 ))
  tries=$(( $1 / 30 + 1 ))
  while :; do
    # Выбор повторяется, пока прогон не выбран: платная работа могла начаться до осушения
    # пула, но ещё не быть видимой первым запросом.
    [ -n "$HIST_RUN" ] || pick_hist_run
    # Одним запросом, пять чисел: прогоны дейли; задачи дейли, актуалки и обслуживания;
    # отслеживаемый прогон истории; задачи истории — они закрывают окно, когда у нового
    # прогона успел стартовать plan_historical_batch; припаркованный скоуп отслеживаемого
    # прогона. Одним, а не пятью: рваное чтение показало бы контур свободным по числам из
    # разных моментов. Пустой ответ (метабаза недоступна / timeout) — это «не знаю», а не
    # «свободно».
    DRAIN_ROW=$($PSQL "SELECT (SELECT count(*) FROM dag_run WHERE dag_id='$DAILY' AND state IN ('queued','running')), (SELECT count(*) FROM task_instance WHERE dag_id IN ('$DAILY','$REFRESH','$MAINT') AND state IN ('queued','running')), (SELECT count(*) FROM dag_run WHERE dag_id='$HIST' AND run_id='$HIST_RUN' AND state IN ('queued','running')), (SELECT count(*) FROM task_instance WHERE dag_id='$HIST' AND state IN ('queued','running')), (SELECT count(*) FROM task_instance WHERE dag_id='$HIST' AND run_id='$HIST_RUN' AND task_id='run_historical_scope' AND map_index>=0 AND pool='$HIST_POOL' AND state IN ('scheduled','up_for_retry'));" || true)
    [ "${DRAIN_ROW:-x}" = "0|0|0|0|0" ] && return 0
    parked=${DRAIN_ROW##*|}
    case "$parked" in ''|*[!0-9]*) parked=0 ;; esac
    if [ "$parked" -gt 0 ] && break_deadlock; then
      # Ломатель отработал — следующий виток без сна: хвост прогона пойдёт сразу.
      tries=$(( tries - 1 ))
      [ "$tries" -le 0 ] && return 1
      [ "$(date -u +%s)" -ge "$deadline" ] && return 1
      continue
    fi
    hist_diag
    tries=$(( tries - 1 ))
    [ "$tries" -le 0 ] && return 1
    [ "$(date -u +%s)" -ge "$deadline" ] && return 1
    sleep 30
    # Ещё раз ПОСЛЕ сна и ДО нового витка — по той же причине, что в wait_idle.
    [ "$(date -u +%s)" -ge "$deadline" ] && return 1
  done
}
# Выбор отслеживаемого прогона. Пустой ответ метабазы на ПЕРВОМ обращении — rc=4:
# выкатывать, не зная, идёт ли оплаченный скоуп, значит оборвать его.
pick_hist_run() {
  local row
  row=$(hist_scope_row)
  [ -n "$row" ] || return 0
  [ "$row" = "-" ] && return 0
  HIST_RUN=${row%%|*}
  log "drain: отслеживаю прогон истории '$HIST_RUN' (скоуп: пул $(printf '%s' "$row" | cut -d'|' -f2), состояние $(printf '%s' "$row" | cut -d'|' -f3), try $(printf '%s' "$row" | cut -d'|' -f4))"
}
# Поле JSON-объекта плана: значений с кавычками внутри у него не бывает (пути, id, числа).
json_field() {  # json_field <json> <ключ>
  printf '%s' "$1" | sed -n "s/.*\"$2\": *\"\([^\"]*\)\".*/\1/p" | head -1
}
# Доказательство учёта (#1245). Терминальный прогон — ещё не учтённый скоуп: finalize мог
# упасть, а dagrun_timeout 6 ч закрывает прогон вовсе без финализации. Пишем то, что видно
# в метабазе, атомарно (tmp + mv): автомат ночной доставки переносит это в запись окна, а
# утренняя приёмка сверяет DRAIN_RUN_ID со строками task_instance — они долговечны, в
# отличие от failures.json, где следующий отказ перезаписывает last_run_id.
write_drain_proof() {
  local row scope validate0 validate_ph validate finalize propagate dagrun xcom kind key accounted tmp
  kind="-"; key="-"; accounted="n/a"
  scope="-"; validate="-"; finalize="-"; propagate="-"; dagrun="-"
  if [ -n "$HIST_RUN" ]; then
    row=$($PSQL "SELECT coalesce((SELECT coalesce(state,'none') FROM task_instance WHERE dag_id='$HIST' AND run_id='$HIST_RUN' AND task_id='run_historical_scope' AND map_index=0),'-'), coalesce((SELECT coalesce(state,'none') FROM task_instance WHERE dag_id='$HIST' AND run_id='$HIST_RUN' AND task_id='validate_historical_scope' AND map_index=0),'-'), coalesce((SELECT coalesce(state,'none') FROM task_instance WHERE dag_id='$HIST' AND run_id='$HIST_RUN' AND task_id='validate_historical_scope' AND map_index=-1),'-'), coalesce((SELECT coalesce(state,'none') FROM task_instance WHERE dag_id='$HIST' AND run_id='$HIST_RUN' AND task_id='finalize_historical_run'),'-'), coalesce((SELECT coalesce(state,'none') FROM task_instance WHERE dag_id='$HIST' AND run_id='$HIST_RUN' AND task_id='propagate_historical_status'),'-'), coalesce((SELECT state FROM dag_run WHERE dag_id='$HIST' AND run_id='$HIST_RUN'),'-'), coalesce((SELECT convert_from(value,'UTF8') FROM xcom WHERE dag_id='$HIST' AND run_id='$HIST_RUN' AND task_id='plan_historical_batch' AND key='return_value' LIMIT 1),'-');" || true)
    if [ -z "$row" ]; then
      # Метабаза не ответила — это «не знаю», а не доказанный незачёт: пустой ответ в роли
      # `f` заставил бы утреннюю приёмку искать потерянные деньги там, где их не теряли
      # (Sol круг 3).
      scope="?"; validate="?"; finalize="?"; propagate="?"; dagrun="?"; accounted=unknown
    else
      scope=$(printf '%s' "$row" | cut -d'|' -f1)
      validate0=$(printf '%s' "$row" | cut -d'|' -f2)
      validate_ph=$(printf '%s' "$row" | cut -d'|' -f3)
      finalize=$(printf '%s' "$row" | cut -d'|' -f4)
      propagate=$(printf '%s' "$row" | cut -d'|' -f5)
      dagrun=$(printf '%s' "$row" | cut -d'|' -f6)
      xcom=$(printf '%s' "$row" | cut -d'|' -f7-)
      # Скоуп упал до раскрытия validate: у карты нет map 0, состояние учёта несёт
      # NULL-плейсхолдер map -1 (при провале апстрима планировщик ставит ему upstream_failed).
      validate="$validate0"
      [ "$validate" = "-" ] && validate="$validate_ph"
      kind=$(json_field "$xcom" SOFASCORE_CAMPAIGN_ACTION)
      case "$kind" in
        capture)
          key=$(json_field "$xcom" SOFASCORE_SCOPE_KEY)
          accounted=f
          if [ "$finalize" = success ]; then
            case "$validate" in success|failed|upstream_failed) accounted=t ;; esac
          fi ;;
        metadata)
          # У метаданных нет SOFASCORE_SCOPE_KEY: finalize их пропускает, результат несёт
          # состояние самой задачи (sofascore_all_mens_state.py). Учтён — только успех.
          # Падение — `unknown`, и это не осторожность, а факт: чекпойнт пишется в середине
          # задачи (scripts/enrich_sofascore_all_mens_snapshot.py), поэтому `failed` бывает и
          # до записи (волну купят заново), и после неё (волна учтена, упало закрытие
          # клиента). По цвету задачи эти два случая неразличимы — врать в любую сторону
          # хуже, чем сказать «не знаю» (Sol круги 1 и 2; в плане §3.1 стояло `success|failed`).
          key="$(json_field "$xcom" SOFASCORE_EXPECTED_CAMPAIGN_ID):metadata:$(json_field "$xcom" SOFASCORE_METADATA_WAVE)"
          accounted=f
          case "$scope" in success) accounted=t ;; failed) accounted=unknown ;; esac ;;
        *) kind="-"; accounted=f ;;
      esac
    fi
  fi
  # Каталог НЕ создаём: в нём же живут выключатель, маркер незакрытой доставки и снимок
  # отката, и молча созданный пустой каталог снял бы fail-closed проверки автомата
  # (Sol круг 2; тот же запрет — в sofascore.env.example).
  if [ ! -d "$(dirname "$LAST_DRAIN")" ]; then
    log "каталога состояния автомата нет ($(dirname "$LAST_DRAIN")) — доказательство учёта не пишу"
    return 0
  fi
  tmp="$LAST_DRAIN.$$.tmp"
  {
    printf 'DRAIN_WINDOW_ID=%s\n' "${SOFASCORE_DEPLOY_WINDOW_ID:-manual-$(date -u +%Y%m%dT%H%M%SZ)}"
    printf 'DRAIN_RUN_ID=%s\n' "${HIST_RUN:--}"
    printf 'DRAIN_SCOPE_KIND=%s\n' "${kind:--}"
    printf 'DRAIN_SCOPE_KEY=%s\n' "${key:--}"
    printf 'DRAIN_SCOPE_STATE=%s\n' "$scope"
    printf 'DRAIN_VALIDATE_STATE=%s\n' "$validate"
    printf 'DRAIN_FINALIZE_STATE=%s\n' "$finalize"
    printf 'DRAIN_PROPAGATE_STATE=%s\n' "$propagate"
    printf 'DRAIN_DAGRUN_STATE=%s\n' "$dagrun"
    printf 'DRAIN_BREAKER_CALLS=%s\n' "$BREAKER_CALLS"
    printf 'DRAIN_BREAKER_RESULT=%s\n' "$BREAKER_RESULT"
    printf 'DRAIN_AT=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'DRAIN_ACCOUNTED=%s\n' "$accounted"
  } > "$tmp" && mv -f "$tmp" "$LAST_DRAIN" || log "доказательство учёта $LAST_DRAIN не записано"
  local accounted_ru
  case "$accounted" in
    t) accounted_ru="подтверждён" ;;
    f) accounted_ru="НЕ подтверждён" ;;
    unknown) accounted_ru="неизвестен" ;;
    *) accounted_ru="$accounted" ;;
  esac
  if [ -z "$HIST_RUN" ]; then
    log "drain: прогона истории с начатой платной работой не было — учитывать нечего (ACCOUNTED=$accounted)"
  else
    log "drain: прогон $HIST_RUN закрыт: kind=$kind, run=$scope, validate=$validate, finalize=$finalize, propagate=$propagate, dag_run=$dagrun; учёт: $accounted_ru"
  fi
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
REFRESH_WAS_PAUSED=""; HIST_WAS_PAUSED=""; MAINT_WAS_PAUSED=""; POOL_DRAINED=""
# Паузу обслуживания манифеста возвращает тот, кто снял снимок: ручной запуск — здесь,
# запуск из автомата (задан SOFASCORE_DEPLOY_LOCK_FD) — сам автомат, после приёмки или
# отката. Иначе на разрушительном пути (откат вторым тиком) обслуживание оказалось бы
# распаущенным и его прогон встретил бы пересоздание контейнеров.
restore_maint_pause() {
  [ -n "$MAINT_WAS_PAUSED" ] || return 0
  if [ -n "${SOFASCORE_DEPLOY_LOCK_FD:-}" ]; then
    log "$MAINT остаётся на паузе — её вернёт автомат ночной доставки (was paused=$MAINT_WAS_PAUSED)"
    return 0
  fi
  [ "$MAINT_WAS_PAUSED" = "f" ] || { log "$MAINT kept paused (as before)"; return 0; }
  docker exec sofascore-airflow-scheduler airflow dags unpause "$MAINT" >> "$LOG" 2>&1
  if [ "$(is_paused "$MAINT")" = "f" ]; then
    log "$MAINT unpaused back"
    return 0
  fi
  # Возврат не состоялся — это отказ, а не примечание: раньше функция возвращала ноль от
  # log, выкат заканчивался DONE, и обслуживание манифеста молча оставалось под паузой
  # навсегда (Sol круг 2). В EXIT-trap отказ остаётся best effort — там уже падают.
  log "MANUAL ACTION REQUIRED: $MAINT is still paused — unpause it by hand"
  return 1
}
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
    if set_pool "$HIST_POOL" "$HISTORY_SLOTS" 'SofaScore history lane'; then
      log "$HIST_POOL restored to $HISTORY_SLOTS slots"
    else
      log "MANUAL ACTION REQUIRED: $HIST_POOL left drained — airflow pools set $HIST_POOL $HISTORY_SLOTS 'SofaScore history lane'"
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
  restore_maint_pause
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

STEP="lock"
# Замок выката (#1245): один протокол на автомат ночной доставки и ручной запуск. Раньше
# автомат отличал ручной выкат по `pgrep deploy.sh`, а deploy.sh не проверял ничего — между
# «процесса нет» и первым изменением контура помещался целый чужой выкат. Берём ПОСЛЕ
# дешёвых предпосылок и ДО снимка пауз: занятый замок обязан кончаться нетронутым боем.
sofascore_deploy_lock_init || exit 2
if [ -n "${SOFASCORE_DEPLOY_LOCK_FD:-}" ]; then
  # Запуск из автомата: замок взят родителем, дескриптор унаследован. Проверяем, что он
  # ведёт именно на файл замка, иначе «выкат под замком» был бы словом, а не фактом.
  # На том же open file description flock отдаёт замок сразу — конкуренции здесь быть не может.
  [ "/proc/self/fd/$SOFASCORE_DEPLOY_LOCK_FD" -ef "$SOFASCORE_DEPLOY_LOCK" ] \
    || { echo "SOFASCORE_DEPLOY_LOCK_FD=$SOFASCORE_DEPLOY_LOCK_FD ведёт не на замок выката $SOFASCORE_DEPLOY_LOCK" >&2; exit 2; }
  flock -n "$SOFASCORE_DEPLOY_LOCK_FD" || { echo "унаследованный замок выката не берётся" >&2; exit 2; }
else
  # `|| lock_rc=$?`, а не `; lock_rc=$?`: под `set -e` занятый замок оборвал бы скрипт
  # кодом 1, и автомат ночной доставки принял бы это за поломку выката, а не за «не начат».
  lock_rc=0
  sofascore_take_deploy_lock 8 || lock_rc=$?
  [ "$lock_rc" = 2 ] && exit 2
  if [ "$lock_rc" = 1 ]; then
    log "замок выката занят ($SOFASCORE_DEPLOY_LOCK) — выкат не начат, бой не тронут"
    exit 4
  fi
fi

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
# Старое доказательство учёта убираем ДО первого действия: файл прошлой ночи, доживший до
# утра, автомат принял бы за свидетельство сегодняшней (окно сверяется, но пустая строка
# честнее чужой).
rm -f "$LAST_DRAIN"
REFRESH_WAS_PAUSED=$(is_paused "$REFRESH")
HIST_WAS_PAUSED=$(is_paused "$HIST")
# Обслуживание манифеста паузится вместе с актуалкой: его прогон в 05:00 UTC воскресенья
# раньше резал окно доставки (SUNDAY_TO), теперь окно одно на все дни, а пересечения не
# случается, потому что DAG на паузе. Паузу снимает тот, кто снял снимок: при ручном
# запуске — этот скрипт, из автомата ночной доставки — сам автомат после приёмки или отката.
MAINT_WAS_PAUSED=$(is_paused "$MAINT")
log "drain: $HIST_POOL -> 0 slots, pause $REFRESH (was paused=$REFRESH_WAS_PAUSED) и $MAINT (was paused=$MAINT_WAS_PAUSED), wait up to ${IDLE_WAIT}s"
set_pool "$HIST_POOL" 0 'SofaScore history lane (drained for deploy)'
POOL_DRAINED=1
pause_dag "$REFRESH"
pause_dag "$MAINT"
# `|| true` внутри hist_scope_row обязателен: без него отказ метабазы под `set -e` вышел бы
# кодом timeout (124), а для автомата 124 — это «таймаут доставки», то есть полный откат
# боя, которого не было.
HIST_ROW=$(hist_scope_row)
[ -n "$HIST_ROW" ] || { log "метабаза не ответила про идущий прогон истории — nothing deployed"; exit 4; }
if [ "$HIST_ROW" = "-" ]; then
  log "drain: прогона истории с начатой платной работой нет — жду только контур"
else
  HIST_RUN=${HIST_ROW%%|*}
  log "drain: отслеживаю прогон истории '$HIST_RUN' (скоуп: пул $(printf '%s' "$HIST_ROW" | cut -d'|' -f2), состояние $(printf '%s' "$HIST_ROW" | cut -d'|' -f3), try $(printf '%s' "$HIST_ROW" | cut -d'|' -f4))"
fi
wait_drained "$IDLE_WAIT" \
  || { log "contour still busy after ${IDLE_WAIT}s (последний ответ '${DRAIN_ROW:-пусто}') — nothing deployed"; exit 4; }
write_drain_proof

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
  up -d --no-deps --force-recreate airflow-scheduler >> "$LOG" 2>&1 8>&-
log "scheduler up"

STEP="gateway-up"
SOFASCORE_RELEASE_ROOT="$RELEASE" \
SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST="$ARTIFACT_DEST" \
SOFASCORE_PROXY_BUDGET_ARTIFACT_ID="$ARTIFACT_ID" \
docker compose -p sofascore-gw -f "$GW_COMPOSE" \
  --project-directory "$RELEASE" \
  --env-file "$SOFASCORE_PLATFORM_ENV_FILE" --env-file "$ENV_FILE" \
  up -d --no-deps --force-recreate $GATEWAYS >> "$LOG" 2>&1 8>&-
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
set_pool "$HIST_POOL" "$HISTORY_SLOTS" 'SofaScore history lane'
set_pool sofascore_players_pool "$PLAYERS_SLOTS" 'SofaScore players lane'
log "pools set: ingest_scraper_pool=1 $HIST_POOL=$HISTORY_SLOTS sofascore_players_pool=$PLAYERS_SLOTS"
POOL_DRAINED=""   # слоты вернулись штатно — позднему обрыву возвращать нечего

STEP="restore-pause"
pause_dag "$HIST"
restore_maint_pause || exit 7
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
