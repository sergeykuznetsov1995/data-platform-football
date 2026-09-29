#!/usr/bin/env bash
# Ночной автомат доставки ClubElo (#1465) в общее боевое дерево планировщика ($TREE).
# Копия deploy/understat/auto_deliver.sh: доставляет из origin/master ТОЛЬКО файлы ClubElo
# (включая тесты ClubElo в tests/), перезаписью на месте (`cat >`, инод сохраняется, ctime
# каталогов dags/ и scrapers/ не меняется — сторож WhoScored молчит). Отличие от Understat:
# удаление (D) разрешено внутри scrapers/clubelo/ и тестов ClubElo (так уезжает scraper.py),
# откат возвращает и удалённые файлы; в dags/ и верхнем уровне scrapers/ создание/удаление — руками.
# Режимы: без аргумента — ночной запуск; --check — все проверки без записи;
# --rollback <YYYYMMDD> — возврат .prev-копий той доставки + приёмка. Документ — README.md рядом.
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
set -u -o pipefail

TREE="${TREE:-/root/dpf-whoscored-merge}"
REPO="${REPO:-/root/data-platform-football}"
STATE="${STATE:-/root/watchdog/state}"
OUT="${OUT:-/root/clubelo-deliveries}"
METADB="${METADB:-postgres}"
TG_ENV="${TG_ENV:-$HOME/.claude/telegram.env}"
MODE="${1:-night}"
SELF="${BASH_SOURCE[0]}"

# Набор ClubElo (git pathspec). Glob ловит новые *clubelo* в dags/ и верхнем уровне scrapers/
# (их создание/удаление тронуло бы сторожевые каталоги) и тесты ClubElo в tests/.
CLUBELO_PATHS=(scrapers/clubelo dags/dag_ingest_clubelo.py dags/utils/clubelo_tasks.py dags/scripts/run_clubelo_scraper.py
  ':(glob)dags/**/*clubelo*.py' ':(glob)scrapers/*clubelo*' ':(glob)tests/**/*clubelo*' ':(glob)tests/**/*clubelo*/**')
# Общие модули, которые импортируют файлы ClubElo (с транзитивными: default_args → alerts,
# config → medallion_config, scrapers.base.iceberg_writer → scrapers/utils).
SHARED_PATHS="scrapers/base scrapers/utils scrapers/__init__.py dags/utils/__init__.py dags/utils/config.py dags/utils/default_args.py dags/utils/alerts.py dags/utils/medallion_config.py"
# Порядок записи файлов вне scrapers/clubelo/ и tests/ — импортируемые раньше импортирующих.
DAG_ORDER="dags/utils/clubelo_tasks.py dags/scripts/run_clubelo_scraper.py dags/dag_ingest_clubelo.py"
DAGS="dag_ingest_clubelo"
ACCEPTED_F="$STATE/clubelo-accepted"
INFLIGHT="$STATE/clubelo-inflight"
OFF="$STATE/clubelo-auto-deliver.off"

mkdir -p "$STATE" "$OUT"
LOG="$OUT/auto_deliver.log"
DAY=$(date -u +%Y%m%d)
SHA="" FILES_N=0

log() { echo "$(date -u +%FT%TZ) [$MODE] $*" >> "$LOG"; echo "$*"; }
journal() { echo "$(date -u +%FT%TZ) mode=$MODE sha=${SHA:0:12} files=$FILES_N $*" >> "$OUT/journal.log"; }
tg() {
  local text="$1" resp
  if [ "$MODE" = "--check" ]; then log "(--check: в Telegram не шлём) $text"; return 0; fi
  if [ ! -f "$TG_ENV" ]; then log "АЛЕРТ НЕ ДОСТАВЛЕН (нет $TG_ENV): $text"; return 1; fi
  # shellcheck disable=SC1090
  . "$TG_ENV"
  resp=$(curl -s --max-time 15 "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN:-}/sendMessage" \
    -d chat_id="${TELEGRAM_CHAT_ID:-}" --data-urlencode text="$text" 2>/dev/null)
  case "$resp" in *'"ok":true'*) return 0 ;; esac
  log "АЛЕРТ НЕ ДОСТАВЛЕН (ответ: $(printf '%s' "$resp" | head -c 200)): $text"
  return 1
}
# Стоп с алертом: ничего не записано (или откат уже решён вызывающим).
stop() { log "СТОП: $1"; tg "⛔ ClubElo: $1"; journal "стоп: $1"; exit 1; }
q() { docker exec "$METADB" psql -U airflow -d airflow -At -c "$1" 2>/dev/null; }
g() { git -C "$REPO" "$@"; }

# Файл в scrapers/clubelo/ или тест ClubElo в tests/ — там создание и удаление разрешены.
own_dir() { [[ "$1" == scrapers/clubelo/* || ( "$1" == tests/* && "$1" == *clubelo* ) ]]; }

# Файлы ClubElo в коммите $1 — тот же набор, что CLUBELO_PATHS (ls-tree не знает glob-магии).
# Пустой список — ошибка git: без него сверка боя молча пропускалась бы.
own_files() {
  local list
  list=$(g ls-tree -r --name-only "$1" \
    | grep -E '^scrapers/clubelo/|^scrapers/[^/]*clubelo[^/]*$|^dags/(.*/)?[^/]*clubelo[^/]*\.py$|^tests/(.*/)?[^/]*clubelo' \
    | LC_ALL=C sort -u)
  [ -n "$list" ] || return 1
  echo "$list"
}

# Лишние исходные файлы ClubElo в бою против коммита $1 (без __pycache__/.pyc).
extra_files() {
  comm -13 <(own_files "$1") \
    <( { cd "$TREE" && find scrapers/clubelo tests -type f -path '*clubelo*' ! -path '*/__pycache__/*' ! -name '*.pyc' 2>/dev/null
         find scrapers -maxdepth 1 -type f -name '*clubelo*' ! -name '*.pyc'
         find dags -name '*clubelo*.py' ! -path '*/__pycache__/*'; } | LC_ALL=C sort -u)
}

# Приёмка: DAG перечитан после CUT без ошибок, своих import_error нет, файлы ClubElo = BASE.
# CUT берётся ПОСЛЕ всех записей; +60 с — больше таймаута разбора файла (dag_file_processor_timeout
# 50 с по умолчанию): разбор, начатый до окончания записи, приёмку не засчитает.
accept() {
  local cut="$1" base="$2" i d r ok f
  REASON="DAG не перечитаны за 7 мин"
  for i in $(seq 1 21); do
    ok=1
    for d in $DAGS; do
      r=$(q "select has_import_errors, last_parsed_time > timestamptz '$cut' + interval '60 seconds' from dag where dag_id='$d'")
      [ "$r" = "f|t" ] || { ok=0; REASON="$d has_import_errors|перечитан = '${r:-нет ответа}'"; }
    done
    r=$(q "select count(*) from import_error where filename like '%clubelo%'")
    [ "$r" = "0" ] || { ok=0; REASON="import_error ClubElo = '${r:-нет ответа}'"; }
    [ "$ok" = 1 ] && break
    sleep 20
  done
  [ "$ok" = 1 ] || return 1
  OWN=$(own_files "$base") || { REASON="нет списка файлов ClubElo в ${base:0:7}"; return 1; }
  for f in $OWN; do
    g show "$base:$f" | cmp -s - "$TREE/$f" || { REASON="после записи $f в бою ≠ ${base:0:7}"; return 1; }
  done
  f=$(extra_files "$base"); [ -z "$f" ] || { REASON="лишние файлы ClubElo в бою: $(echo $f)"; return 1; }
  log "приёмка пройдена (DAG перечитаны после $cut, бой = ${base:0:7})"
}

# Возврат доставки из каталога $1 (файл files: «статус путь» в порядке записи; M и D попадают
# туда только после проверенной полной .prev-копии, A — до создания файла).
restore() {
  local bk="$1" day="$2" st f rc=0
  while read -r st f; do
    if [ "$st" = A ]; then
      if own_dir "$f"; then rm -f "${TREE:?}/${f:?}" || rc=1; else rc=1; fi
    elif [ -f "$bk/$f.prev-$day" ]; then
      cat "$bk/$f.prev-$day" > "$TREE/$f" && cmp -s "$bk/$f.prev-$day" "$TREE/$f" || rc=1
    fi
    log "  возвращён $st $f"
  done < "$bk/files"
  return $rc
}

# Причина не трогать бой сейчас (пусто — можно): окно 01:30–02:25 UTC (между запусками DAG
# в :30 каждые 4 ч, execution_timeout 45 мин) и занятость ClubElo.
busy_reason() {
  local hhmm n
  hhmm=$(date -u +%H%M)
  if (( 10#$hhmm < 130 || 10#$hhmm >= 225 )); then echo "вне окна ($hhmm UTC; окно 01:30–02:25)"; return; fi
  n=$(q "select count(*) from task_instance where dag_id='dag_ingest_clubelo' and state in ('running','queued')")
  if [ "$n" != "0" ]; then echo "ClubElo занят: task_instance running/queued = '${n:-метабаза не ответила}'"
  elif pgrep -f 'run_clubelo_scrape[r]' >/dev/null; then echo "ClubElo занят: живой процесс run_clubelo_scraper"
  fi
}
set_accepted() { echo "$1" > "$ACCEPTED_F.tmp" && mv "$ACCEPTED_F.tmp" "$ACCEPTED_F"; }

exec 9>"$STATE/clubelo-auto-deliver.lock"
flock -n 9 || { log "другой запуск держит замок — выход"; exit 0; }

if [ "$MODE" = "--rollback" ]; then
  RB="${2:-}"; BK="$OUT/$RB"
  [ -n "$RB" ] && [ -f "$BK/files" ] && [ -f "$BK/accepted-before" ] || stop "--rollback: нет $BK/files или accepted-before"
  BASE=$(cat "$BK/accepted-before"); SHA=$BASE; FILES_N=$(wc -l < "$BK/files")
  BUSY=$(busy_reason); [ -z "$BUSY" ] || stop "--rollback $RB не выполнен: $BUSY"
  echo "rollback $RB" > "$INFLIGHT" || stop "--rollback $RB: не записан $INFLIGHT — бой не тронут"
  REASON="возврат файлов не удался"
  if restore "$BK" "$RB" && { REASON="метабаза не отвечает"; CUT=$(q "select now()"); [ -n "$CUT" ]; } \
     && accept "$CUT" "$BASE" && { REASON="не записан $ACCEPTED_F"; set_accepted "$BASE"; }; then
    rm -f "$INFLIGHT"
    journal "ручной откат $RB принят"; tg "↩️ ClubElo: ручной откат доставки $RB принят, бой = ${BASE:0:7}"; exit 0
  fi
  touch "$OFF"; journal "ручной откат $RB НЕ подтверждён ($REASON), выключатель поставлен"
  tg "🆘 ClubElo: ручной откат $RB НЕ подтверждён ($REASON) — НУЖНЫ РУКИ; автомат выключен ($OFF)"
  exit 2
fi
[ "$MODE" = night ] || [ "$MODE" = --check ] || { echo "режимы: (без аргумента) | --check | --rollback <YYYYMMDD>"; exit 2; }

# --- защёлки ночного режима (в --check только отмечаются)
if [ -f "$OFF" ]; then
  [ "$MODE" = --check ] && log "заметка: стоит выключатель $OFF" || { log "выключатель $OFF — выход"; exit 0; }
fi
if [ -f "$INFLIGHT" ]; then
  if [ "$MODE" = --check ]; then log "заметка: висит $INFLIGHT"
  else
    log "висит $INFLIGHT ($(cat "$INFLIGHT")) — ничего не делаю"
    R="$STATE/clubelo-inflight-reminded-$DAY"
    [ -f "$R" ] || { tg "🆘 ClubElo: незавершённая доставка ($(cat "$INFLIGHT")) — НУЖНЫ РУКИ, см. $LOG" && touch "$R"; }
    exit 0
  fi
fi
LATCH="$STATE/clubelo-auto-deliver-attempted-$DAY"
if [ "$MODE" = night ] && [ -f "$LATCH" ]; then log "сегодня ($DAY) попытка уже была — выход"; exit 0; fi

# --- окно и занятость ClubElo
BUSY=$(busy_reason)
if [ -n "$BUSY" ]; then
  [ "$MODE" = --check ] && log "заметка: $BUSY" || { log "$BUSY — выход"; exit 0; }
fi
[ "$MODE" = night ] && touch "$LATCH"

# --- база, самопроверка, общие модули
g fetch -q origin || stop "git fetch origin не удался"
SHA=$(g rev-parse origin/master) || stop "нет origin/master"
ACC=$(cat "$ACCEPTED_F" 2>/dev/null)
[[ "$ACC" =~ ^[0-9a-f]{40}$ ]] && g cat-file -e "$ACC^{commit}" 2>/dev/null \
  || stop "автомат не знает базы: $ACCEPTED_F пуст/не SHA/нет такого коммита — посей руками (README)"
[ "$(md5sum < "$SELF" | cut -c1-32)" = "$(g show "$SHA:deploy/clubelo/auto_deliver.sh" 2>/dev/null | md5sum | cut -c1-32)" ] \
  || stop "копия автомата отстала от master ${SHA:0:7}, переустанови: cp из master (README)"
for f in $(g ls-tree -r --name-only "$SHA" -- $SHARED_PATHS); do
  g show "$SHA:$f" | cmp -s - "$TREE/$f" || stop "ОТМЕНА: общий модуль $f в бою ≠ master ${SHA:0:7}"
done
# Состав тоже: удалённый в master или лишний в бою общий модуль (без __pycache__/.pyc) — та же отмена.
EXTRA=$(comm -13 <(g ls-tree -r --name-only "$SHA" -- $SHARED_PATHS | LC_ALL=C sort -u) \
  <(cd "$TREE" && for p in $SHARED_PATHS; do [ -e "$p" ] && find "$p" -type f ! -path '*/__pycache__/*' ! -name '*.pyc'; done | LC_ALL=C sort -u))
[ -z "$EXTRA" ] || stop "ОТМЕНА: общий модуль $(echo $EXTRA) в бою ≠ master ${SHA:0:7} (в master его нет)"
log "база ${ACC:0:7} → master ${SHA:0:7}; общие модули в бою = master"

# --- бой = принятая база по всем файлам ClubElo (иначе — чужая живая правка)
OWN=$(own_files "$ACC") || stop "нет списка файлов ClubElo в базе ${ACC:0:7}"
for f in $OWN; do
  g show "$ACC:$f" | cmp -s - "$TREE/$f" || stop "чужая живая правка: $f в бою ≠ принятой базе ${ACC:0:7}"
done
EXTRA=$(extra_files "$ACC"); [ -z "$EXTRA" ] || stop "чужая живая правка: лишние файлы ClubElo в бою: $(echo $EXTRA)"

# --- список изменений: M — доставить; A/D в scrapers/clubelo/ и тестах ClubElo — создать/удалить;
# прочее — руками. --no-renames: переименование = D + A и проходит те же правила.
declare -A ST=()
while IFS=$'\t' read -r st p1 p2; do
  [ -n "$st" ] || continue
  case "$st" in
    M) if own_dir "$p1" || [[ " $DAG_ORDER " == *" $p1 "* ]]; then ST[$p1]=M
       else stop "нужны руки: $p1 вне известного порядка записи"; fi ;;
    A|D) if own_dir "$p1"; then ST[$p1]=$st
         else stop "нужны руки: сторож каталогов — $st $p1 вне scrapers/clubelo/ и тестов ClubElo"; fi ;;
    *) stop "нужны руки: сторож каталогов — $st $p1 ${p2:-}" ;;
  esac
done < <(g diff --no-renames --name-status "$ACC" "$SHA" -- "${CLUBELO_PATHS[@]}")
if [ "${#ST[@]}" = 0 ]; then
  log "нечего доставлять: файлы ClubElo ${ACC:0:7} = ${SHA:0:7}"
  [ "$MODE" = night ] && { set_accepted "$SHA" || stop "не записан $ACCEPTED_F"; journal "нечего доставлять, база сдвинута"; }
  exit 0
fi
# Порядок: модули ClubElo (__init__ последним), DAG-файлы, тесты, удаления — в самом конце.
live() { for f in "${!ST[@]}"; do [ "${ST[$f]}" != D ] && echo "$f"; done; }
ORDERED=$(
  live | grep '^scrapers/clubelo/' | grep -v '/__init__\.py$' | LC_ALL=C sort
  live | grep '^scrapers/clubelo/.*__init__\.py$' | LC_ALL=C sort -r
  for f in $DAG_ORDER; do [ -n "${ST[$f]:-}" ] && echo "$f"; done
  live | grep '^tests/' | LC_ALL=C sort
  for f in "${!ST[@]}"; do [ "${ST[$f]}" = D ] && echo "$f"; done | LC_ALL=C sort
)
FILES_N=$(echo "$ORDERED" | wc -l)

# --- предзапись: новые байты во временную папку, *.py компилируются (строкой, без .pyc)
BK="$OUT/$DAY"; WORK="$BK"
if [ "$MODE" = --check ]; then WORK=$(mktemp -d); trap 'rm -rf "${WORK:?}"' EXIT; fi
for f in $ORDERED; do
  [ "${ST[$f]}" = A ] && [ -e "$TREE/$f" ] && stop "новый файл $f уже есть в бою — чужая правка"
  [ "${ST[$f]}" = D ] && continue
  mkdir -p "$WORK/$(dirname "$f")"
  g show "$SHA:$f" > "$WORK/$f.new" || stop "git show $f"
  case "$f" in *.py)
    python3 -I -S -c 'import sys; compile(open(sys.argv[1], encoding="utf-8").read(), sys.argv[2], "exec")' \
      "$WORK/$f.new" "$f" || stop "$f из ${SHA:0:7} не компилируется" ;;
  esac
done
log "к доставке ($FILES_N): $(for f in $ORDERED; do printf '%s %s; ' "${ST[$f]}" "$f"; done)"
if [ "$MODE" = --check ]; then log "проверки пройдены (--check, ничего не записано)"; journal "check: проверки пройдены"; exit 0; fi

# --- запись
# Состояние для отката пишется ДО первой записи в бой; не записалось — в бой не идём.
{ echo "$SHA $DAY" > "$INFLIGHT" && echo "$ACC" > "$BK/accepted-before" && : > "$BK/files"; } \
  || { rm -f "$INFLIGHT"; stop "не записано состояние доставки (inflight/accepted-before/files) — бой не тронут"; }
WRITE_OK=1
for f in $ORDERED; do
  mkdir -p "$BK/$(dirname "$f")" || { WRITE_OK=0; REASON="каталог копии $f"; break; }
  if [ "${ST[$f]}" = A ]; then
    { : > "$BK/$f.absent-$DAY" && echo "A $f" >> "$BK/files" && mkdir -p "$TREE/$(dirname "$f")"; } \
      || { WRITE_OK=0; REASON="подготовка нового $f"; break; }
  else
    { cp -p "$TREE/$f" "$BK/$f.prev-$DAY" && cmp -s "$TREE/$f" "$BK/$f.prev-$DAY" && echo "${ST[$f]} $f" >> "$BK/files"; } \
      || { WRITE_OK=0; REASON="копия .prev $f"; break; }
  fi
  if [ "${ST[$f]}" = D ]; then
    rm -f "${TREE:?}/${f:?}" && [ ! -e "$TREE/$f" ] || { WRITE_OK=0; REASON="удаление $f"; break; }
    log "  удалён D $f"
  else
    cat "$BK/$f.new" > "$TREE/$f" && cmp -s "$BK/$f.new" "$TREE/$f" || { WRITE_OK=0; REASON="запись $f"; break; }
    log "  записан ${ST[$f]} $f"
  fi
done

if [ "$WRITE_OK" = 1 ]; then CUT=$(q "select now()"); [ -n "$CUT" ] || { WRITE_OK=0; REASON="метабаза не отвечает (now())"; }; fi
if [ "$WRITE_OK" = 1 ] && accept "$CUT" "$SHA"; then
  if ! set_accepted "$SHA"; then
    touch "$OFF"; journal "доставлено, но $ACCEPTED_F не записан"
    tg "🆘 ClubElo: доставка ${SHA:0:7} принята, но база $ACCEPTED_F не записана — НУЖНЫ РУКИ; автомат выключен"
    exit 2
  fi
  rm -f "$INFLIGHT"
  journal "доставлено: $(echo $ORDERED)"
  tg "✅ ClubElo: доставлено $FILES_N файлов из ${SHA:0:7}, DAG перечитаны без ошибок"
  exit 0
fi

# --- откат
log "приёмка/запись провалена: $REASON — откат"
WHY="$REASON"
ROK=0
if restore "$BK" "$DAY"; then
  CUT=$(q "select now()")
  if [ -z "$CUT" ]; then REASON="метабаза не отвечает"; elif accept "$CUT" "$ACC"; then ROK=1; fi
else REASON="возврат файлов не удался"; fi
if [ "$ROK" = 1 ]; then
  rm -f "$INFLIGHT"; journal "отклонено ($WHY), откачено"
  tg "🔴 ClubElo: доставка ${SHA:0:7} отклонена ($WHY), откачено, DAG перечитаны"
  exit 1
fi
touch "$OFF"; journal "отклонено ($WHY), откат НЕ подтверждён ($REASON), выключатель поставлен"
tg "🆘 ClubElo: доставка ${SHA:0:7} отклонена ($WHY), откат НЕ подтверждён ($REASON) — НУЖНЫ РУКИ; автомат выключен ($OFF)"
exit 2
