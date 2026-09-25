#!/usr/bin/env bash
# Автомат доставки контура WhoScored (#1473): ставит origin/master в дерево изолированного
# контура ($RUNTIME/src, git worktree, detached), копирует два DAG и .airflowignore в
# $RUNTIME/dags, проверяет импорт и перечитывание, а приёмку делает первым завершённым
# прогоном ежедневника после доставки. Провал — откат на прежний SHA и Telegram.
#
# Ставится КОПИЕЙ (README): /root/whoscored-auto-deliver.sh. Из $RUNTIME/src НЕ запускается —
# это worktree, checkout подменил бы скрипт посреди работы.
# Режимы: без аргумента — cron-тик; --check — все проверки без единой записи;
# --rollback [sha] — на указанный SHA (по умолчанию — прежний принятый) тем же порядком.
# Cron: 15 * * * * (каждый час — приёмка висящей доставки; доставка — только в окне).
# shellcheck disable=SC2086  # $WS_PATHS намеренно режется на слова (список путей git)
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
set -u -o pipefail
export GIT_OPTIONAL_LOCKS=0   # git status не освежает индекс: --check не пишет даже в .git

RUNTIME="${WHOSCORED_RUNTIME_DIR:-/root/whoscored-1017-runtime}"
SRC="$RUNTIME/src"
DAGS_DIR="$RUNTIME/dags"
STATE="${STATE:-/root/watchdog/state}"
OUT="${OUT:-/root/whoscored-deliveries}"
METADB="${METADB:-whoscored-airflow-metadb}"
SCHED="${SCHED:-whoscored-airflow-scheduler}"
COMPOSE_PROJECT=whoscored-airflow
COMPOSE_ENV_FILE="${WHOSCORED_COMPOSE_ENV_FILE:-/root/data-platform-football/.env}"
TRINO_RO="${TRINO_RO:-/root/.claude/bin/trino-ro.sh}"
TG_ENV="${TG_ENV:-$HOME/.claude/telegram.env}"
HISTORY_PAT="${HISTORY_PAT:-whoscored_history_backfil[l]/driver}"
WIN_FROM=0200 WIN_TO=0500            # окно доставки, UTC (прогоны — 10:00 и 22:00 UTC)
REREAD_TRIES=30                      # × 20 с = 10 мин на перечитывание DAG
MODE="${1:-night}"
SELF=$(readlink -f "${BASH_SOURCE[0]}")

DAG_FILES="dag_ingest_whoscored.py dag_backfill_whoscored.py"
DAG_IDS="'dag_ingest_whoscored','dag_backfill_whoscored'"
# Пути, изменение которых — повод доставлять (DAG импортируют dags/utils; раннер — scrapers/*).
WS_PATHS="scrapers/whoscored scrapers/base scrapers/utils scrapers/__init__.py dags/utils dags/dag_ingest_whoscored.py dags/dag_backfill_whoscored.py dags/scripts configs/medallion deploy/whoscored"
AF_COMPOSE=deploy/whoscored/airflow.compose.yaml
GW_COMPOSE=deploy/whoscored/gw.compose.yaml

ACCEPTED="$STATE/whoscored-accepted"            # SHA, который сейчас в бою и принят
ACCEPTED_PREV="$STATE/whoscored-accepted-prev"  # прежний принятый — цель --rollback без аргумента
INFLIGHT="$STATE/whoscored-inflight"            # доставка идёт или ждёт приёмки прогоном
REJECTED="$STATE/whoscored-rejected"            # SHA master, откаченный — повторно не ставим
OFF="$STATE/whoscored-auto-deliver.off"         # выключатель: ставит человек или сам автомат
LOCK="$STATE/whoscored-deliver.lock"
DAY=$(date -u +%Y%m%d)
TS=$(date -u +%Y%m%dT%H%M%SZ)
LOG="$OUT/auto_deliver.log"
SHA=""
CHECK=0; [ "$MODE" = --check ] && CHECK=1

# --- вывод: в --check ничего не пишется на диск (ни лог, ни журнал, ни замок)
log() { echo "$*"; [ "$CHECK" = 1 ] || echo "$(date -u +%FT%TZ) [$MODE] $*" >> "$LOG"; }
journal() { [ "$CHECK" = 1 ] || echo "$(date -u +%FT%TZ) mode=$MODE sha=${SHA:0:12} $*" >> "$OUT/journal.log"; }
tg() {
  local text="$1" resp
  if [ "$CHECK" = 1 ]; then log "(--check: в Telegram не шлём) $text"; return 0; fi
  if [ ! -f "$TG_ENV" ]; then log "АЛЕРТ НЕ ДОСТАВЛЕН (нет $TG_ENV): $text"; return 1; fi
  # shellcheck disable=SC1090
  . "$TG_ENV"
  resp=$(curl -s --max-time 15 "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN:-}/sendMessage" \
    -d chat_id="${TELEGRAM_CHAT_ID:-}" --data-urlencode text="$text" 2>/dev/null)
  case "$resp" in *'"ok":true'*) return 0 ;; esac
  log "АЛЕРТ НЕ ДОСТАВЛЕН (ответ: $(printf '%s' "$resp" | head -c 200)): $text"
  return 1
}
stop() { log "СТОП: $1"; tg "⛔ WhoScored: $1"; journal "стоп: $1"; exit 1; }
q() { docker exec "$METADB" psql -U airflow -d airflow -At -c "$1" 2>/dev/null; }
g() { git -C "$SRC" "$@"; }
is_sha() { [[ "${1:-}" =~ ^[0-9a-f]{40}$ ]] && g cat-file -e "$1^{commit}" 2>/dev/null; }
put() { echo "$2" > "$1.tmp" && mv -f "$1.tmp" "$1"; }   # атомарная запись маркера
iget() { sed -n "s/^$1=//p" "$INFLIGHT" 2>/dev/null | head -1; }

# Строки bronze, записанные после момента $1 (время метабазы, UTC; _ingested_at — UTC без зоны):
# «матчи события манифест» или пусто, если Trino не ответил. Приёмка — по матчам/событиям,
# манифест только в отчёт: его рост без новых матчей и событий доставку не принимает.
measure() {
  local t="${1:0:19}" r
  r=$("$TRINO_RO" "select (select count(*) from whoscored_matches where _ingested_at > timestamp '$t'), (select count(*) from whoscored_events where _ingested_at > timestamp '$t'), (select count(*) from whoscored_match_ingest_manifest where _ingested_at > timestamp '$t')" 2>/dev/null | tr -d '"' | tail -1)
  [[ "$r" =~ ^[0-9]+,[0-9]+,[0-9]+$ ]] && echo "${r//,/ }"
}

# Причина не трогать бой сейчас (пусто — можно): идёт прогон или жива история.
busy_reason() {
  local n
  n=$(q "select count(*) from dag_run where state in ('running','queued')")
  if [ "$n" != "0" ]; then echo "контур занят: dag_run running/queued = '${n:-метабаза не ответила}'"
  elif pgrep -f "$HISTORY_PAT" >/dev/null; then echo "контур занят: жив драйвер истории"
  fi
}

# Законное состояние боя: HEAD == $1, дерево чистое, копии DAG = файлам дерева.
# С $2 (прерванная доставка/откат): HEAD == $1 или $2, копии не сверяются — их перепишет откат.
illegal_reason() {
  local head f
  head=$(g rev-parse HEAD 2>/dev/null)
  [ "$head" = "$1" ] || { [ -n "${2:-}" ] && [ "$head" = "$2" ]; } \
    || { echo "HEAD дерева ${head:0:12} ≠ ожидаемому ${1:0:12}${2:+ / ${2:0:12}}"; return; }
  [ -z "$(g status --porcelain 2>/dev/null)" ] || { echo "в дереве незакоммиченные правки"; return; }
  [ -n "${2:-}" ] && return
  for f in $DAG_FILES; do
    cmp -s "$SRC/dags/$f" "$DAGS_DIR/$f" || { echo "копия $DAGS_DIR/$f ≠ дереву"; return; }
  done
}
# Спасённый патч: правки отслеживаемых файлов, список неотслеживаемых, расхождения копий DAG.
rescue() {
  local p="$OUT/rescued-$TS.patch" f
  { g diff HEAD; echo "# untracked:"; g status --porcelain | sed -n 's/^?? /#   /p'
    for f in $DAG_FILES; do diff -u "$SRC/dags/$f" "$DAGS_DIR/$f"; done; } > "$p" 2>&1
  echo "$p"
}

# Ставит $1 (цель) поверх $2 (что стоит сейчас): checkout → копии DAG → import-check →
# рестарт scheduler при смене dags/utils или compose → ждём перечитывания. 0 — успех, иначе REASON.
deploy_to() {
  local to="$1" from="$2" f cut i r ok restart="" up=""
  REASON=""
  if [ -n "$(g status --porcelain)" ]; then
    log "  дерево грязное перед checkout — патч $(rescue)"
  fi
  g checkout -q -f --detach "$to" && [ "$(g rev-parse HEAD)" = "$to" ] || { REASON="checkout ${to:0:12}"; return 1; }
  for f in $DAG_FILES; do
    cp "$SRC/dags/$f" "$DAGS_DIR/.$f.tmp" && mv -f "$DAGS_DIR/.$f.tmp" "$DAGS_DIR/$f" \
      && cmp -s "$SRC/dags/$f" "$DAGS_DIR/$f" || { REASON="копия $f"; return 1; }
  done
  if [ -f "$SRC/deploy/whoscored/.airflowignore" ]; then
    cp "$SRC/deploy/whoscored/.airflowignore" "$DAGS_DIR/.airflowignore.tmp" \
      && mv -f "$DAGS_DIR/.airflowignore.tmp" "$DAGS_DIR/.airflowignore" || { REASON="копия .airflowignore"; return 1; }
  fi
  log "  дерево и копии DAG = ${to:0:12}"
  g diff --quiet "$from" "$to" -- dags/utils || restart=1
  g diff --quiet "$from" "$to" -- "$AF_COMPOSE" || up=1
  # Контейнер приводится к конфигурации цели ДО import-check: при откате с compose, на котором
  # scheduler не поднялся, импорт в неисправном контейнере невозможен — сначала пересоздание.
  if [ -n "$up" ] && [ -f "$SRC/$AF_COMPOSE" ]; then
    log "  изменён $AF_COMPOSE — пересоздаю только airflow-scheduler"
    docker compose -p "$COMPOSE_PROJECT" -f "$SRC/$AF_COMPOSE" --env-file "$COMPOSE_ENV_FILE" \
      up -d --no-deps --force-recreate airflow-scheduler >> "$LOG" 2>&1 || { REASON="compose up airflow-scheduler"; return 1; }
    restart=""
  elif [ -n "$up" ]; then
    log "  в ${to:0:12} нет $AF_COMPOSE — только рестарт (compose — руками)"; restart=1
  fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$SCHED" 2>/dev/null)" != true ]; then
    log "  $SCHED не запущен — рестарт до import-check"
    docker restart "$SCHED" >> "$LOG" 2>&1 || { REASON="рестарт $SCHED"; return 1; }
    restart=""
  fi
  timeout 300 docker exec -w /opt/airflow/dags -e PYTHONDONTWRITEBYTECODE=1 "$SCHED" \
    python -c 'import dag_ingest_whoscored, dag_backfill_whoscored' >> "$LOG" 2>&1 \
    || { REASON="import-check в $SCHED"; return 1; }
  log "  import-check пройден"
  if [ -n "$restart" ]; then
    log "  изменены dags/utils — рестарт $SCHED"
    docker restart "$SCHED" >> "$LOG" 2>&1 || { REASON="рестарт $SCHED"; return 1; }
  fi
  g diff --quiet "$from" "$to" -- "$GW_COMPOSE" \
    || tg "ℹ️ WhoScored: в ${to:0:7} изменён $GW_COMPOSE — whoscored_flaresolverr пересоздаётся руками (README)"
  # Метка — время метабазы ПОСЛЕ записи; +60 с больше таймаута разбора файла: разбор,
  # начатый до записи, не засчитывается. airflow CLI не используем — он строит свой DagBag с диска.
  cut=$(q "select now()"); [ -n "$cut" ] || { REASON="метабаза не отвечает (now())"; return 1; }
  REASON="DAG не перечитаны за $((REREAD_TRIES * 20 / 60)) мин"
  for i in $(seq 1 "$REREAD_TRIES"); do
    sleep 20
    ok=1
    r=$(q "select count(*) from dag where dag_id in ($DAG_IDS) and has_import_errors = false and last_parsed_time > timestamptz '$cut' + interval '60 seconds'")
    [ "$r" = "2" ] || { ok=0; REASON="перечитаны без ошибок $r из 2 DAG (после $cut)"; }
    r=$(q "select count(*) from import_error")
    [ "$r" = "0" ] || { ok=0; REASON="import_error = '${r:-нет ответа}'"; }
    [ "$ok" = 1 ] && { log "  DAG перечитаны без ошибок (после $cut, попытка $i)"; DELIVERED_AT="$cut"; return 0; }
  done
  return 1
}

# Откат на $1 с причиной $2 (текущий HEAD — непринятый $3). Успех — 🔴 и REJECTED; провал — 🆘 и .off.
auto_rollback() {
  local to="$1" why="$2" bad="$3"
  log "откат на ${to:0:12}: $why"
  if deploy_to "$to" "$bad"; then
    put "$REJECTED" "$bad"; put "$ACCEPTED" "$to"; rm -f "$INFLIGHT"
    journal "отклонено ${bad:0:12} ($why), откачено на ${to:0:12}"
    tg "🔴 WhoScored: доставка ${bad:0:7} отклонена ($why), откачено на ${to:0:7}, DAG перечитаны"
    exit 1
  fi
  touch "$OFF"; journal "отклонено ($why), откат НЕ подтверждён ($REASON), выключатель поставлен"
  tg "🆘 WhoScored: доставка ${bad:0:7} отклонена ($why), откат на ${to:0:7} НЕ подтверждён ($REASON) — НУЖНЫ РУКИ; автомат выключен ($OFF)"
  exit 2
}

# --- запуск из дерева контура запрещён
case "$SELF" in "$(readlink -f "$SRC")"/*) echo "запуск из $SRC запрещён: поставь копию (README)"; exit 2 ;; esac
case "$MODE" in night|--check|--rollback) ;; *) echo "режимы: (без аргумента) | --check | --rollback [sha]"; exit 2 ;; esac
if [ "$CHECK" = 0 ]; then
  mkdir -p "$STATE" "$OUT"
  exec 9>"$LOCK"
  flock -n 9 || { log "другой запуск держит замок — выход"; exit 0; }
fi
ACC=$(cat "$ACCEPTED" 2>/dev/null)

# --- ручной откат
if [ "$MODE" = --rollback ]; then
  CUR=$(g rev-parse HEAD)
  ALT=""
  if [ -f "$INFLIGHT" ]; then EXPECT=$(iget sha); DEF=$(iget prev); [ "$(iget phase)" = delivered ] || ALT=$DEF
  else EXPECT=$ACC; DEF=$(cat "$ACCEPTED_PREV" 2>/dev/null); fi
  SHA=$(g rev-parse --verify -q "${2:-$DEF}^{commit}") || stop "--rollback: нет коммита '${2:-$DEF}'"
  WHY=$(illegal_reason "$EXPECT" "$ALT"); [ -z "$WHY" ] || stop "--rollback: бой в незаконном состоянии ($WHY) — руками"
  BUSY=$(busy_reason); [ -z "$BUSY" ] || stop "--rollback не выполнен: $BUSY"
  put "$INFLIGHT" "phase=rollback
sha=$SHA
prev=$CUR" || stop "--rollback: не записан $INFLIGHT — бой не тронут"
  if deploy_to "$SHA" "$CUR"; then
    put "$ACCEPTED_PREV" "$CUR"; put "$ACCEPTED" "$SHA"; rm -f "$INFLIGHT"
    MASTER=$(g rev-parse origin/master)
    if [ "$SHA" = "$MASTER" ]; then rm -f "$REJECTED"; else put "$REJECTED" "$MASTER"; fi
    journal "ручной откат ${CUR:0:12} → ${SHA:0:12} принят"
    tg "↩️ WhoScored: ручной откат ${CUR:0:7} → ${SHA:0:7}, DAG перечитаны без ошибок"
    exit 0
  fi
  touch "$OFF"; journal "ручной откат на ${SHA:0:12} НЕ подтверждён ($REASON), выключатель поставлен"
  tg "🆘 WhoScored: ручной откат на ${SHA:0:7} НЕ подтверждён ($REASON) — НУЖНЫ РУКИ; автомат выключен ($OFF)"
  exit 2
fi

# --- выключатель
if [ -f "$OFF" ]; then
  [ "$CHECK" = 1 ] && log "заметка: стоит выключатель $OFF" || { log "выключатель $OFF — выход"; exit 0; }
fi

# --- два законных состояния дерева: принятый SHA или SHA висящей доставки, оба чистые
ALT=""
if [ -f "$INFLIGHT" ]; then EXPECT=$(iget sha); [ "$(iget phase)" = delivered ] || ALT=$(iget prev); else
  # выключатель — иначе cron-тик слал бы алерт каждый час
  is_sha "$ACC" || { [ "$CHECK" = 1 ] || touch "$OFF"
    stop "автомат не знает базы: $ACCEPTED пуст/не SHA/нет такого коммита — посей руками (README); автомат выключен"; }
  EXPECT=$ACC
fi
WHY=$(illegal_reason "$EXPECT" "$ALT")
if [ -n "$WHY" ]; then
  [ "$CHECK" = 1 ] && stop "бой в незаконном состоянии: $WHY (--check: патч и выключатель не ставлю)"
  P=$(rescue); touch "$OFF"; journal "незаконное состояние: $WHY; патч $P; выключатель"
  tg "🆘 WhoScored: бой в незаконном состоянии ($WHY) — НУЖНЫ РУКИ; патч $P, автомат выключен ($OFF)"
  exit 1
fi

# --- висящая доставка: прерванная — откат; доставленная — приёмка первым завершённым прогоном
if [ -f "$INFLIGHT" ]; then
  SHA=$(iget sha); PREV=$(iget prev); PHASE=$(iget phase); AT=$(iget at)
  AFTER=$(iget after); AFTER=${AFTER:-$AT}   # прогоны до AFTER уже разобраны (нулевой прирост)
  if [ "$PHASE" = rollback ]; then
    [ "$CHECK" = 1 ] && stop "ручной откат на ${SHA:0:12} прерван — руками (--rollback повторно)"
    touch "$OFF"; journal "ручной откат прерван, выключатель"
    tg "🆘 WhoScored: ручной откат на ${SHA:0:7} прерван — НУЖНЫ РУКИ (повтори --rollback); автомат выключен ($OFF)"
    exit 2
  fi
  if [ "$PHASE" != delivered ]; then
    [ "$CHECK" = 1 ] && { log "заметка: доставка ${SHA:0:12} прервана на фазе '$PHASE' — cron-тик откатит на ${PREV:0:12}"; exit 0; }
    BUSY=$(busy_reason); [ -z "$BUSY" ] || { log "прерванная доставка, откат ждёт: $BUSY"; exit 0; }
    auto_rollback "$PREV" "автомат прерван на фазе '$PHASE'" "$SHA"
  fi
  RUN=$(q "select run_id || '|' || state from dag_run where dag_id='dag_ingest_whoscored' and start_date > timestamptz '$AFTER' order by start_date limit 1")
  if [ -z "$RUN" ] || [[ "$RUN" != *"|success" && "$RUN" != *"|failed" ]]; then
    log "доставка ${SHA:0:12} ждёт приёмки: прогон после $AFTER ${RUN:-ещё не стартовал}"
    AGE=$(q "select extract(epoch from now() - timestamptz '$AT')::int / 3600")
    R="$STATE/whoscored-inflight-reminded-$DAY"
    if [ "$CHECK" = 0 ] && [ "${AGE:-0}" -ge 30 ] && [ ! -f "$R" ]; then
      tg "⏳ WhoScored: доставка ${SHA:0:7} ${AGE} ч ждёт приёмки — прогона ежедневника после неё нет" && touch "$R"
    fi
    exit 0
  fi
  RID=${RUN%|*}
  TASKS=$(q "select string_agg(task_id || '=' || coalesce(state,'none'), ' ' order by task_id) from task_instance where dag_id='dag_ingest_whoscored' and run_id='$RID' and task_id in ('discover_catalog','ingest_daily')")
  IE=$(q "select count(*) from import_error")
  HIE=$(q "select count(*) from dag where dag_id in ($DAG_IDS) and has_import_errors")
  log "приёмка ${SHA:0:12} прогоном $RID: $TASKS; import_error=$IE; DAG с ошибкой импорта=$HIE"
  # validate_data в приёмку не входит до #1476 (сейчас красный всегда).
  if [ "$TASKS" != "discover_catalog=success ingest_daily=success" ] || [ "$IE" != "0" ] || [ "$HIE" != "0" ]; then
    WHY="прогон $RID: ${TASKS:-задач нет}; import_error=${IE:-?}"
    [ "$CHECK" = 1 ] && { log "заметка: приёмка провалена ($WHY) — cron-тик откатит на ${PREV:0:12}"; exit 0; }
    BUSY=$(busy_reason); [ -z "$BUSY" ] || { log "приёмка провалена, откат ждёт: $BUSY"; exit 0; }
    auto_rollback "$PREV" "$WHY" "$SHA"
  fi
  NOW=$(measure "$AT") || { log "Trino не ответил — приёмка ${SHA:0:12} повторится следующим тиком"; exit 0; }
  read -r NM NE NF <<< "$NOW"
  if [ "$NM" -le 0 ] && [ "$NE" -le 0 ]; then
    # Задачи зелёные, а строк нет: источник мог быть пуст — без отката, но и НЕ принято.
    # Доставка остаётся висящей; следующий завершённый прогон разбирается заново.
    [ "$CHECK" = 1 ] && { log "заметка: прогон $RID зелёный, но строк после доставки нет (манифест +$NF) — не принято, без отката"; exit 0; }
    NEXT=$(q "select start_date from dag_run where dag_id='dag_ingest_whoscored' and run_id='$RID'")
    [ -n "$NEXT" ] || { log "метабаза не ответила — повтор следующим тиком"; exit 0; }
    put "$INFLIGHT" "$(grep -v '^after=' "$INFLIGHT")
after=$NEXT" || { touch "$OFF"; tg "🆘 WhoScored: не записан $INFLIGHT — НУЖНЫ РУКИ; автомат выключен"; exit 2; }
    journal "прогон $RID зелёный, строк после доставки 0 (манифест +$NF) — не принято, ждёт следующий прогон"
    tg "⚠️ WhoScored: доставка ${SHA:0:7}: прогон $RID зелёный, но новых строк матчей/событий после доставки нет (манифест +$NF) — НУЖНЫ РУКИ; без отката, не принято, жду следующий прогон"
    exit 0
  fi
  [ "$CHECK" = 1 ] && { log "заметка: приёмка сейчас прошла бы; после доставки матчей +$NM, событий +$NE, манифест +$NF"; exit 0; }
  put "$ACCEPTED_PREV" "$PREV"; put "$ACCEPTED" "$SHA" \
    || { touch "$OFF"; tg "🆘 WhoScored: приёмка ${SHA:0:7} пройдена, но $ACCEPTED не записан — НУЖНЫ РУКИ; автомат выключен"; exit 2; }
  rm -f "$INFLIGHT"
  journal "принято прогоном $RID: матчи +$NM, события +$NE, манифест +$NF"
  tg "✅ WhoScored: доставка ${SHA:0:7} принята прогоном $RID: после доставки матчей +$NM, событий +$NE, манифест +$NF"
  exit 0
fi

# --- доставка: окно, раз в сутки, контур свободен
HHMM=$(date -u +%H%M)
LATCH="$STATE/whoscored-auto-deliver-attempted-$DAY"
if (( 10#$HHMM < 10#$WIN_FROM || 10#$HHMM >= 10#$WIN_TO )); then
  [ "$CHECK" = 1 ] && log "заметка: вне окна ($HHMM UTC; окно $WIN_FROM–$WIN_TO)" || exit 0
fi
if [ "$CHECK" = 0 ] && [ -f "$LATCH" ]; then exit 0; fi
BUSY=$(busy_reason)
if [ -n "$BUSY" ]; then
  [ "$CHECK" = 1 ] && log "заметка: $BUSY" || { log "$BUSY — выход"; exit 0; }
fi
[ "$CHECK" = 0 ] && touch "$LATCH"

# --- пин = origin/master (в --check без fetch: fetch пишет refs общего .git)
if [ "$CHECK" = 0 ]; then g fetch -q origin || stop "git fetch origin не удался"; fi
SHA=$(g rev-parse origin/master) || stop "нет origin/master"
[ "$CHECK" = 1 ] && log "заметка: --check без fetch — пин по локальному origin/master"
if [ "$SHA" = "$(cat "$REJECTED" 2>/dev/null)" ]; then log "пин ${SHA:0:12} уже откачен — жду нового коммита master"; exit 0; fi
if [ "$SHA" = "$ACC" ] || g diff --quiet "$ACC" "$SHA" -- $WS_PATHS; then
  log "без изменений: пути WhoScored ${ACC:0:12} = ${SHA:0:12}"; journal "без изменений"; exit 0
fi
[ "$(md5sum < "$SELF" | cut -c1-32)" = "$(g show "$SHA:deploy/whoscored/auto_deliver.sh" 2>/dev/null | md5sum | cut -c1-32)" ] \
  || stop "копия автомата $SELF отстала от master ${SHA:0:7} — переустанови копию (README)"
# Образ по тегу (pull_policy: never): тег мог быть пересобран — тогда compose тихо сменил бы образ.
TAG=$(g show "$SHA:$AF_COMPOSE" | sed -n 's/^ *image: *\(data-platform-airflow[^ ]*\).*/\1/p' | head -1)
[ -n "$TAG" ] && [ "$(docker image inspect -f '{{.Id}}' "$TAG" 2>/dev/null)" = "$(docker inspect -f '{{.Image}}' "$SCHED" 2>/dev/null)" ] \
  || stop "образ '$TAG' из $AF_COMPOSE ≠ образу работающего $SCHED — руками"
for f in $DAG_FILES; do
  g show "$SHA:dags/$f" | python3 -I -S -c 'import sys; compile(sys.stdin.read(), sys.argv[1], "exec")' "$f" \
    || stop "dags/$f из ${SHA:0:7} не компилируется"
done
log "к доставке: ${ACC:0:12} → ${SHA:0:12} ($(g diff --name-only "$ACC" "$SHA" -- $WS_PATHS | wc -l) файлов в путях WhoScored)"
if [ "$CHECK" = 1 ]; then log "проверки пройдены (--check, ничего не записано)"; exit 0; fi

# --- доставка. Состояние для отката пишется ДО checkout; не записалось — бой не трогаем.
put "$INFLIGHT" "phase=deploying
sha=$SHA
prev=$ACC" || stop "не записан $INFLIGHT — бой не тронут"
if deploy_to "$SHA" "$ACC"; then
  put "$INFLIGHT" "phase=delivered
sha=$SHA
prev=$ACC
at=$DELIVERED_AT" || { touch "$OFF"; tg "🆘 WhoScored: ${SHA:0:7} доставлен, но $INFLIGHT не записан — НУЖНЫ РУКИ; автомат выключен"; exit 2; }
  journal "доставлено ${ACC:0:12} → ${SHA:0:12}, ждёт приёмки прогоном"
  tg "🚚 WhoScored: доставлено ${SHA:0:7} (было ${ACC:0:7}), DAG перечитаны; приёмка — ближайшим прогоном ежедневника"
  exit 0
fi
auto_rollback "$ACC" "доставка: $REASON" "$SHA"
