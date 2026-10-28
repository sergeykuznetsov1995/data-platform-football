#!/bin/bash
# B6: перевод боевого дерева FotMob (FOTMOB_RELEASE_ROOT) на цель FOTMOB_TARGET.
# База механики — ревью 24.08 (/root/FOTMOB-REVIEW-2026-08-24.md, вопрос Q1).
# Пути машины — из env-файла контура (deploy/fotmob/fotmob.env.example).
# Пины и runtime fingerprints — из неизменяемого пакета; следующая доставка
# меняет одну строку FOTMOB_DELIVERY_PACKAGE в env-файле, скрипты не правятся.
#
#   b6_deliver.sh check   — только проверки (окно, дерево, цель)
#   b6_deliver.sh apply   — проверки + переключение + приёмка метабазой
#   TREE=/путь/копии b6_deliver.sh apply   — сухой прогон на копии
#
# Откат при любой поломке:  git -C <FOTMOB_RELEASE_ROOT> checkout <FOTMOB_ROLLBACK_REF>
# Ветки deploy/* НЕ удалять — прод исторически стоит на их коммитах.
set -euo pipefail

PACKAGE_FILE=${FOTMOB_PINNED_PACKAGE:-}
PACKAGE_DIGEST=${FOTMOB_PINNED_DIGEST:-}
ENV_FILE="${FOTMOB_ENV_FILE:-/etc/data-platform/fotmob.env}"
# Эстафета автомата (одноразовый пропуск) приходит окружением, а не env-файлом;
# загрузчик ниже сбрасывает ВСЕ FOTMOB_*, поэтому её значения снимаются до него.
DELIVER_NONCE=${FOTMOB_DELIVER_NONCE:-}
DELIVER_NONCE_FILE=${FOTMOB_DELIVER_NONCE_FILE:-}
# shellcheck source=deploy/fotmob/env.sh
. "$(dirname "$(readlink -f "$0")")/env.sh"
fotmob_load_env "$ENV_FILE" || exit 2
fotmob_load_package "${PACKAGE_FILE:-${FOTMOB_DELIVERY_PACKAGE:?FOTMOB_DELIVERY_PACKAGE не задан}}" "$PACKAGE_DIGEST" || exit 2

TARGET=${FOTMOB_TARGET:?FOTMOB_TARGET не задан в env-файле}                   # цель выката
ROLLBACK_REF=${FOTMOB_ROLLBACK_REF:?FOTMOB_ROLLBACK_REF не задан в env-файле} # текущий боевой HEAD; голый SHA —
ROLLBACK_SHA=${FOTMOB_ROLLBACK_SHA:?FOTMOB_ROLLBACK_SHA не задан в env-файле} # ветка deploy/fotmob-b6-master при apply переезжает на цель
PROD_TREE=${FOTMOB_RELEASE_ROOT:?FOTMOB_RELEASE_ROOT не задан в env-файле}
# Пины — только короткие SHA (7–8 hex, как `git rev-parse --short`): автомат сравнивает
# их с `--short HEAD` по префиксу, полный SHA не совпал бы никогда (ревью Sol, #1155).
for pin in TARGET ROLLBACK_SHA; do
  case "${!pin}" in
    ???????|????????) case "${!pin}" in *[!0-9a-f]*) ;; *) continue ;; esac ;;
  esac
  echo "FOTMOB_$pin='${!pin}': ожидается короткий SHA (7–8 hex, как git rev-parse --short)" >&2
  exit 2
done
METADB=${FOTMOB_METADB_CONTAINER:?FOTMOB_METADB_CONTAINER не задан в env-файле}
SCHED=${FOTMOB_SCHEDULER_CONTAINER:?FOTMOB_SCHEDULER_CONTAINER не задан в env-файле}
CAMPAIGN=${FOTMOB_CAMPAIGN_DIR:?FOTMOB_CAMPAIGN_DIR не задан в env-файле}
PYTEST=${FOTMOB_HOST_PYTEST:?FOTMOB_HOST_PYTEST не задан в env-файле}
# Доказательство доставки: целевой модуль в контейнере и его md5. Переопределяется
# лишь для репетиции (обе ветки: доказательство есть / его нет).
NEW_CODE_FILE=${NEW_CODE_FILE:-${FOTMOB_NEW_CODE_FILE:?FOTMOB_NEW_CODE_FILE не задан в env-файле}}
NEW_CODE_MD5=${NEW_CODE_MD5:-${FOTMOB_NEW_CODE_MD5:?FOTMOB_NEW_CODE_MD5 не задан в env-файле}}
# Канонизация обязательна: без неё «<дерево>/» или симлинк на него считались бы копией,
# и сухой режим пропустил бы окно, приёмку и сьют, сделав при этом НАСТОЯЩИЙ checkout
# боевого дерева (ревью Sol 25.08).
TREE=$(realpath -m "${TREE:-$PROD_TREE}")
DRY=$([ "$TREE" = "$(realpath -m "$PROD_TREE")" ] && echo 0 || echo 1)
MODE=${1:-check}

say() { printf '\n== %s ==\n' "$1"; }
die() { printf 'ОТКАЗ: %s\n' "$1" >&2; exit 1; }

# Ручной запуск обязан брать тот же замок, что и автомат: иначе оператор и cron
# могут одновременно пройти предполётные проверки и переключать/откатывать дерево
# друг под другом (ревью Sol, раунд 2). FOTMOB_DELIVERY_LOCK=held ставит автомат —
# он уже держит замок, и повторный захват был бы самоблокировкой.
if [ "$MODE" = apply ] && [ "$DRY" = 0 ]; then
  # Боевой apply допустим ТОЛЬКО из обёртки, и признак этого — УНАСЛЕДОВАННЫЙ
  # дескриптор её flock-замка (fd 9). Переменную окружения (раунд 3) и файл-маркер
  # (раунд 4) можно выставить со стороны; открытый дескриптор именно этого замка
  # достаётся только потомку процесса, который замок держит. Заодно это и есть
  # взаимное исключение: пока обёртка работает, замок занят.
  # Сам скрипт откатываться не умеет, поэтому запуск мимо обёртки способен
  # оставить бой на непринятом коде — без маркера, отката и алерта.
  # Двух условий мало по отдельности: путь fd подделывается пробой `exec 9< lock`
  # (ревью Sol, раунд 6), а занятость замка сама по себе ничего не говорит о том,
  # кто нас позвал. Вместе они означают ровно «я потомок процесса, который держит
  # этот замок»: независимый flock на том же файле обязан НЕ пройти.
  # Двух условий всё ещё мало: fd 9 можно открыть самому (незаблокированным) в
  # момент, когда замок держит ЧУЖОЙ процесс, — и обе проверки пройдут, а боевой
  # apply уйдёт мимо маркера, отката и алертов (ревью Sol, раунд 21). Поэтому
  # третье условие: замок обязан держать кто-то из НАШИХ предков. Держателей
  # спрашиваем у ядра (/proc/locks), сопоставляя по устройству и inode.
  # Третье условие: замок обязан держать ИМЕННО наш дескриптор. Иначе fd 9 можно
  # открыть самому (незаблокированным) в момент, когда замок держит чужой
  # процесс, — и первые два условия пройдут, а боевой apply уйдёт мимо маркера,
  # отката и алертов (ревью Sol, раунд 21). `flock -n 9` на унаследованном от
  # автомата описании — пустая операция и даёт успех; на своём свежем fd при
  # занятом кем-то замке — отказ. Спрашивать /proc/locks бесполезно: там
  # владельцем записан pid уже завершившейся команды flock, а не автомата.
  # Замок — тот же файл, что открывает автомат: $STATE/fotmob-auto-deliver.lock,
  # оба выводят путь из одного FOTMOB_STATE_DIR. Сравниваем с РАЗРЕШЁННЫМ путём:
  # /proc/self/fd/9 показывает канонический путь, а в env-файле мог оказаться
  # хвостовой `/` или symlink в родителе — сырой текст не совпал бы, и законный
  # вызов автомата отбивался бы уже после израсходованного пропуска и суточной
  # защёлки (ревью Sol, #1155 этап 3).
  LOCKFILE=${FOTMOB_STATE_DIR:?FOTMOB_STATE_DIR не задан в env-файле}/fotmob-auto-deliver.lock
  LOCK_REAL=$(realpath -e "$LOCKFILE" 2>/dev/null) || LOCK_REAL=__нет-замка__
  # Четвёртое условие — одноразовый пропуск. Три предыдущих обходятся тем, кто сам
  # откроет дескриптор и сам возьмёт замок (ревью Sol, раунд 22). Пропуск автомат
  # выписывает перед вызовом: значение приходит переменной, а файл мы сверяем и
  # тут же удаляем — повторно тот же пропуск не сработает. Против root-а,
  # сознательно повторяющего весь протокол, это не защита и быть ею не может;
  # цель — чтобы ручной запуск не прошёл мимо маркера, отката и алертов.
  nonce_ok=0
  if [ -n "$DELIVER_NONCE" ] && [ -n "$DELIVER_NONCE_FILE" ] \
     && [ -f "$DELIVER_NONCE_FILE" ] && [ ! -L "$DELIVER_NONCE_FILE" ] \
     && [ "$(cat "$DELIVER_NONCE_FILE" 2>/dev/null)" = "$DELIVER_NONCE" ]; then
    nonce_ok=1
    rm -f "$DELIVER_NONCE_FILE"
  fi
  if [ "$(readlink /proc/self/fd/9 2>/dev/null)" != "$LOCK_REAL" ] \
     || flock -n "$LOCKFILE" true 2>/dev/null \
     || ! flock -n 9 2>/dev/null \
     || [ "$nonce_ok" != 1 ]; then
    die "ручная доставка на бой запрещена: запускай автомат auto_deliver.sh (он держит замок, маркер, откат и алерты). Для проверок: $0 check"
  fi
fi

say "1. Окно доставки (сейчас UTC $(date -u +%F' '%T))"
if [ "$DRY" = 0 ]; then
  # `|| true` обязателен: pgrep без совпадений возвращает 1, и под `set -euo pipefail`
  # скрипт умирал бы молча ровно в открытом окне (раннеров 0) — до checkout дело не
  # доходило вовсе. Найдено ревью Sol 25.08, воспроизведено изолированным прогоном.
  n=$( { pgrep -f 'dags/scripts/run_fotmob_scrape[r].py' || true; } | wc -l )
  [ "$n" = 0 ] || die "живой раннер FotMob: $n процесс(ов)"
  act=$(docker exec "$METADB" psql -U airflow -d airflow -tA \
    -c "SELECT count(*) FROM dag_run WHERE dag_id='dag_ingest_fotmob' AND state IN ('running','queued');")
  [ "$act" = 0 ] || die "в метабазе изолята $act активных dag_ingest_fotmob"
  hm=$(date -u +%H%M)
  { [ "$hm" -ge 1945 ] && [ "$hm" -le 2320 ]; } || die "вне безопасного интервала 19:45–23:20 UTC (сейчас $hm); волна рождается 23:55"
  echo "раннеров 0, активных ранов 0, время в интервале — окно открыто"
else
  echo "сухой прогон на копии ($TREE) — проверка окна пропущена"
fi

say "2. Предпосылки дерева"
cd "$TREE"
[ -z "$(git status --porcelain)" ] || die "рабочее дерево грязное"
case "$(git rev-parse HEAD)" in "$ROLLBACK_SHA"*) : ;; *) die "HEAD не $ROLLBACK_SHA — дерево кто-то трогал, разобраться прежде";; esac
git cat-file -e "${TARGET}^{commit}" 2>/dev/null || git fetch origin
git cat-file -e "${TARGET}^{commit}" || die "объекта $TARGET нет даже после fetch"
if git rev-parse -q --verify origin/master >/dev/null 2>&1; then
  om=$(git rev-parse --short origin/master)
  [ "$om" = "$TARGET" ] || echo "ВНИМАНИЕ: origin/master ушёл вперёд ($om) — ставим пиненный $TARGET, свежие коммиты отдельным решением"
fi
echo "дерево чистое на $ROLLBACK_SHA, цель $TARGET доступна"

if [ "$MODE" != apply ]; then say "check ПРОЙДЕН (apply не запрошен)"; exit 0; fi

DELIVERY_STARTED_AT=$(date -u +%FT%TZ)
say "3. Переключение (git checkout -B deploy/fotmob-b6-master $TARGET)"
git checkout -B deploy/fotmob-b6-master "$TARGET"
git log --oneline -1

if [ "$DRY" = 1 ]; then say "сухой прогон: приёмка контейнером пропущена"; exit 0; fi

say "4. Приёмка метабазой изолята (CLI не верить)"
echo "жду 90 с — DagFileProcessor перечитывает dags/ ..."
sleep 90
err=$(docker exec "$METADB" psql -U airflow -d airflow -tA -c "SELECT count(*) FROM import_error;")
if [ "$err" != 0 ]; then
  docker exec "$METADB" psql -U airflow -d airflow \
    -c "SELECT filename, left(stacktrace,400) FROM import_error;"
  die "import_error=$err (база была 0). ОТКАТ: git -C $TREE checkout $ROLLBACK_REF"
fi
echo "import_error = 0"
echo "-- даги и паузы:"
docker exec "$METADB" psql -U airflow -d airflow -tA -F' | ' \
  -c "SELECT dag_id, is_paused FROM dag ORDER BY dag_id;"
echo "-- целевой модуль в контейнере ($NEW_CODE_FILE):"
cmd5=$({ docker exec "$SCHED" md5sum "$NEW_CODE_FILE" | cut -d' ' -f1; } || true)
[ "$cmd5" = "$NEW_CODE_MD5" ] \
  || die "md5 $NEW_CODE_FILE в контейнере '$cmd5' не целевой. ОТКАТ: git -C $TREE checkout $ROLLBACK_REF"
echo "md5 $NEW_CODE_FILE в контейнере целевой ($cmd5)"

# Все runtime fingerprints принадлежат тому же пакету, что TARGET и ROLLBACK.
cmd5=$(docker exec "$SCHED" md5sum /opt/airflow/scrapers/fotmob/repository.py | cut -d' ' -f1)
[ "$cmd5" = "$FOTMOB_REPOSITORY_MD5" ] || die "release fingerprint mismatch: repository.py md5"
IFS='|' read -r -a fingerprints <<< "$FOTMOB_RUNTIME_HASHES"
for fingerprint in "${fingerprints[@]}"; do
  expected=${fingerprint%% *}
  module=${fingerprint#* }
  observed=$(docker exec "$SCHED" sha256sum "$module" | cut -d' ' -f1)
  [ "$observed" = "$expected" ] || die "release fingerprint mismatch: $module"
done

parse_left=${PARSE_WAIT:-300}
while :; do
  parsed=$(docker exec "$METADB" psql -X -U airflow -d airflow -tA -c "SELECT count(*) FROM dag WHERE dag_id IN ('dag_ingest_fotmob','dag_orchestrate_fotmob','dag_collect_fotmob_players','dag_refresh_fotmob','dag_backfill_fotmob','dag_trigger_fotmob_daily') AND is_active AND last_parsed_time > '$DELIVERY_STARTED_AT'::timestamptz;")
  [ "$parsed" = 6 ] && break
  [ "$parse_left" -gt 0 ] || die "six DAGs not parsed after checkout: $parsed"
  sleep 20
  parse_left=$((parse_left - 20))
done

say "5. Проверки exact-target в изолированной рабочей копии"
TEST_TREE=${FOTMOB_VALIDATION_ROOT:?FOTMOB_VALIDATION_ROOT не задан}
test "$(realpath -e "$TEST_TREE")" != "$(realpath -e "$PROD_TREE")" || die "проверки не могут выполняться в production-дереве"
validation_sha=$(git -C "$TEST_TREE" rev-parse HEAD)
target_sha=$(git -C "$TREE" rev-parse "${TARGET}^{commit}")
[ "$validation_sha" = "$target_sha" ] || die "validation tree не соответствует цели"
[ -z "$(git -C "$TEST_TREE" status --porcelain)" ] || die "validation tree грязное"
(
  cd "$TEST_TREE"
  env -u PYTEST_ADDOPTS -u PYTEST_PLUGINS PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 "$PYTEST" -p no:cacheprovider tests/unit/scrapers/test_fotmob_calendar_contract.py \
    tests/unit/scrapers/test_fotmob_calendar_availability.py tests/unit/scrapers/test_fotmob_field_map.py \
    tests/unit/scrapers/test_fotmob_writer_lock.py tests/unit/scrapers/test_fotmob_bronze_interruption.py \
    tests/unit/scrapers/test_fotmob_trino_instrumentation.py \
    tests/unit/scrapers/test_fotmob_row_identity.py tests/unit/scrapers/test_fotmob_transfers.py \
    tests/unit/dags/test_fotmob_transfer_contract.py tests/unit/dags/test_fotmob_orchestration.py \
    tests/unit/dags/test_fotmob_publication.py \
    tests/unit/scripts/test_fotmob_acceptance.py tests/unit/scrapers/test_fotmob_service.py tests/unit/scrapers/test_fotmob_transfer_page_identity.py \
    tests/unit/scrapers/test_fotmob_planner.py tests/unit/scrapers/test_fotmob_repository.py \
    tests/unit/scrapers/test_fotmob_infrastructure_retry.py \
    tests/unit/scripts/test_fotmob_backfill.py tests/unit/scrapers/test_run_fotmob_scraper.py -q
) || die "сьют isolated exact-target красный. ОТКАТ: git -C $TREE checkout $ROLLBACK_REF"

say "B6 ДОСТАВЛЕН. Дальше по порядку:"
echo "1) наблюдать рождение волны 23:55–00:05 UTC и первые ~30 мин;"
echo "2) историю удержать: hold/STOP сохраняются; current-first driver и запуск требуют отдельного разрешения;"
echo "3) утром — замер первой закрытой волны нового кода (make: закрыла ли план до дедлайна, матчи/сутки)."
