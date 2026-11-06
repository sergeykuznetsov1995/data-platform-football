# shellcheck shell=bash
# Загрузчик env-файла контура SofaScore для скриптов deploy/sofascore/*.sh.
# Читает KEY=VALUE как docker compose (--env-file): без shell-подстановок, внешние
# одинарные кавычки — литерал, в двойных декодируются только \" и \\, CRLF-хвост
# снимается. Значения становятся переменными оболочки и НЕ экспортируются (даже если
# одноимённая переменная пришла экспортированной из окружения оператора — она
# сбрасывается): compose получает значения только через --env-file, поэтому устаревшее
# значение из окружения процесса не может перекрыть перепинованный файл.
# Принимаются только ключи контура (SOFASCORE_*, PROXY_FILTER_SOFASCORE_*) — файл не
# может подменить PATH, HOME и прочее окружение скрипта.
#
#   ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
#   . "$(dirname "$0")/env.sh"; sofascore_load_env "$ENV_FILE" || exit 2

sofascore_load_env() {
  local file="$1" line key value evidence_configured=0
  # #2182: унаследованный из окружения процесса SOFASCORE_EVIDENCE_CODE_REVISION не
  # должен переживать загрузку — цикл ниже снимает только ключи, ЕСТЬ в файле, а
  # compose отдал бы приоритет процессному env над --env-file, и отчётность
  # evidence приписала бы код не того релиза (после выката и отката alike).
  # Сброс — ДО проверки читаемости и парсинга: отказ по любой причине (нет файла,
  # битая строка, невыводимая ревизия) обязан оставить ревизию неустановленной,
  # а не унаследованной.
  unset -v SOFASCORE_EVIDENCE_CODE_REVISION
  [ -r "$file" ] || { echo "нет env-файла контура: $file" >&2; return 2; }
  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) echo "строка без '=' в $file: ${line%%=*}" >&2; return 2 ;; esac
    key=${line%%=*}
    value=${line#*=}
    case "$key" in
      SOFASCORE_[A-Z0-9_]*|PROXY_FILTER_SOFASCORE_[A-Z0-9_]*) ;;
      *) echo "недопустимый ключ в $file (ожидаются SOFASCORE_*/PROXY_FILTER_SOFASCORE_*): $key" >&2; return 2 ;;
    esac
    case "$key" in *[!A-Z0-9_]*) echo "недопустимое имя переменной в $file: $key" >&2; return 2 ;; esac
    case "$value" in
      \'*\') value=${value#\'}; value=${value%\'} ;;
      \"*\") value=${value#\"}; value=${value%\"}; value=${value//\\\"/\"}; value=${value//\\\\/\\} ;;
    esac
    # Решение «recorder настроен» принимается ТОЛЬКО по файлу: непустые
    # SOFASCORE_REFRESH_EVIDENCE_DIR / SOFASCORE_EVIDENCE_CODE_REVISION из файла —
    # конфигурация, унаследованные из окружения процесса — нет (#2182).
    case "$key" in
      SOFASCORE_REFRESH_EVIDENCE_DIR)
        [ -n "$value" ] && evidence_configured=1 ;;
      SOFASCORE_EVIDENCE_CODE_REVISION)
        # #2182: пин из файла в процесс НЕ устанавливается — ревизию выводит только
        # sofascore_derive_evidence_revision из дерева релиза. Пин лишь отмечает,
        # что recorder сконфигурирован; иначе при сбое вывода (или отсутствии
        # вывода) compose увидел бы устаревший файловый пин как боевую ревизию.
        [ -n "$value" ] && evidence_configured=1
        continue ;;
    esac
    # Снять переменную целиком (в том числе атрибут export, унаследованный от окружения),
    # затем задать заново как обычную переменную оболочки.
    unset -v "$key"
    printf -v "$key" '%s' "$value"
  done < "$file"
  # Выключенный recorder (ни каталога, ни пина в файле) не обязан иметь git-дерево:
  # вывод ревизии — только когда recorder/пин сконфигурирован.
  [ "$evidence_configured" = 1 ] || return 0
  sofascore_derive_evidence_revision
}

# #2182: полный sha кода, который реально смонтирован в контур, выводится из САМОГО
# дерева SOFASCORE_RELEASE_ROOT, а не доверяется пину из env-файла/окружения: перепин
# корня релиза при выкате и откате иначе оставлял бы отчётность evidence на чужом коде.
# Вывод — строгий (fail closed): корень обязан быть вершиной git-дерева (не подкаталогом
# чужого репозитория), HEAD — полным 40-hex, отслеживаемый код — чистым. Подменяется
# ТОЛЬКО SOFASCORE_EVIDENCE_CODE_REVISION и только в процессе (export для compose,
# чьё процессное окружение сильнее --env-file); env-файл не правится. Дерево
# переиспользуется из $SOFASCORE_RELEASE_ROOT, уже загруженного выше, — без новых
# подпроцессов на каждый матч и без новых зависимостей (git и так нужен выкату).
sofascore_derive_evidence_revision() {
  local root top sha dirty
  [ -n "${SOFASCORE_RELEASE_ROOT:-}" ] || { echo "recorder evidence настроен, а SOFASCORE_RELEASE_ROOT не задан" >&2; return 2; }
  root=$SOFASCORE_RELEASE_ROOT
  [ -d "$root" ] || { echo "SOFASCORE_RELEASE_ROOT не каталог: $root" >&2; return 2; }
  root=$(cd "$root" && pwd -P) || { echo "не канонизируется SOFASCORE_RELEASE_ROOT: $SOFASCORE_RELEASE_ROOT" >&2; return 2; }
  top=$(git -C "$root" --no-optional-locks rev-parse --show-toplevel 2>/dev/null)     || { echo "recorder evidence настроен, а $root не git-дерево — ревизию кода вывести нечем" >&2; return 2; }
  top=$(cd "$top" && pwd -P) || { echo "не канонизируется вершина git-дерева: $top" >&2; return 2; }
  [ "$top" = "$root" ] || { echo "SOFASCORE_RELEASE_ROOT $root не вершина git-дерева ($top) — ревизия была бы чужой" >&2; return 2; }
  sha=$(git -C "$root" --no-optional-locks rev-parse HEAD 2>/dev/null)     || { echo "в дереве $root не читается HEAD — ревизию evidence вывести нечем" >&2; return 2; }
  case "$sha" in
    *[!0-9a-f]*) echo "HEAD дерева $root не 40-hex: $sha" >&2; return 2 ;;
  esac
  [ "${#sha}" = 40 ] || { echo "HEAD дерева $root не 40-hex: $sha" >&2; return 2; }
  dirty=$(git -C "$root" --no-optional-locks status --porcelain --untracked-files=no 2>/dev/null)     || { echo "git status в дереве $root не отработал — чистоту кода не подтвердить, fail closed" >&2; return 2; }
  [ -z "$dirty" ] || { echo "отслеживаемый код дерева $root грязный — ревизия evidence непредставительна" >&2; return 2; }
  SOFASCORE_EVIDENCE_CODE_REVISION=$sha
  export SOFASCORE_EVIDENCE_CODE_REVISION
}

# Замок выката (#1245): один протокол на автомат ночной доставки и на ручной deploy.sh.
# Раньше автомат отличал ручной выкат по `pgrep deploy.sh` — между «процесса нет» и первым
# изменением контура помещался целый чужой выкат, а обратной проверки у deploy.sh не было
# вовсе. Файл лежит в $SOFASCORE_RUNTIME_DIR, а не в all-men/: там `chown -R` и preflight
# кампании, а корень runtime не меняется от релиза к релизу.
# Путь считается ПОСЛЕ загрузки env-файла (SOFASCORE_RUNTIME_DIR берётся оттуда), поэтому
# он не константа файла, а отдельный шаг: sofascore_deploy_lock_init задаёт
# SOFASCORE_DEPLOY_LOCK, если его не переопределили ключом env-файла или окружением стенда.
sofascore_deploy_lock_init() {
  [ -n "${SOFASCORE_DEPLOY_LOCK:-}" ] && return 0
  [ -n "${SOFASCORE_RUNTIME_DIR:-}" ] || { echo "SOFASCORE_RUNTIME_DIR не задан — замок выката класть некуда" >&2; return 2; }
  SOFASCORE_DEPLOY_LOCK="$SOFASCORE_RUNTIME_DIR/deploy.lock"
}

# sofascore_take_deploy_lock <fd> — 0 взят, 1 занят (чужой выкат идёт), 2 поломка.
# «Занято» и «поломка» — разные новости: подменённый путь или нечитаемый каталог не должны
# выглядеть как мирная конкуренция, из-за которой тик просто пропускают.
sofascore_take_deploy_lock() {  # sofascore_take_deploy_lock <fd>
  local fd="$1" path dir perm rc
  sofascore_deploy_lock_init || return 2
  path="$SOFASCORE_DEPLOY_LOCK"
  dir=$(dirname "$path")
  if [ -L "$dir" ] || [ ! -d "$dir" ]; then
    echo "каталог замка выката не на месте: $dir" >&2; return 2
  fi
  perm=$(stat -c %a "$dir" 2>/dev/null) || { echo "каталог замка выката не читается: $dir" >&2; return 2; }
  case "${perm: -1}" in
    2|3|6|7) echo "каталог замка выката доступен на запись всем: $dir ($perm)" >&2; return 2 ;;
  esac
  if [ -L "$path" ] || { [ -e "$path" ] && [ ! -f "$path" ]; }; then
    echo "на месте замка выката не обычный файл: $path" >&2; return 2
  fi
  # `>>`, а не `>`: путь настраиваемый, и опечатка в SOFASCORE_DEPLOY_LOCK обнулила бы
  # обычный файл, на который она указала, ещё до flock (Sol круг 1). Дописывать в замок
  # никто не собирается — нужен только дескриптор.
  eval "exec $fd>>\"\$path\"" 2>/dev/null || { echo "не открывается замок выката: $path" >&2; return 2; }
  flock -n "$fd"; rc=$?
  [ "$rc" = 0 ] && return 0
  [ "$rc" = 1 ] && return 1
  echo "flock отказал кодом $rc — это не конкуренция" >&2
  return 2
}

# Переписать строку KEY= в env-файле контура. Живёт здесь, а не в deploy.sh: ту же
# правку делает автомат ночной доставки при откате (#1245), и две копии одного `sed`
# рано или поздно разъехались бы — откат перепинывал бы не то, что перепинул выкат.
sofascore_set_env_var() {  # sofascore_set_env_var <file> <key> <value>
  # Значение подставляется в правую часть sed: `&` в нём развернулось бы в найденное.
  # Путям релиза и hex-идентификаторам это не грозит; шире функция не применяется.
  grep -q "^$2=" "$1" || { echo "в $1 нет строки $2=" >&2; return 2; }
  sed -i "s#^$2=.*#$2=$3#" "$1"
}
