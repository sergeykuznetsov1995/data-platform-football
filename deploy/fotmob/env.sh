# shellcheck shell=bash
# Загрузчик env-файла контура FotMob для скриптов deploy/fotmob/*.sh (#1155, этап 3).
# Читает KEY=VALUE как docker compose (--env-file): без shell-подстановок, внешние
# одинарные кавычки — литерал, в двойных декодируются только \" и \\, CRLF-хвост
# снимается. Файл — ЕДИНСТВЕННЫЙ источник: перед чтением сбрасываются ВСЕ переменные
# FOTMOB_* окружения (в том числе экспортированные), а не только те, что в файле есть, —
# иначе унаследованный FOTMOB_TARGET пережил бы файл без этого ключа, и `${VAR:?}` в
# скрипте не сработал бы (ревью Sol, #1155 этап 3). Значения становятся переменными
# оболочки и НЕ экспортируются: устаревшее значение из окружения процесса не может
# перекрыть файл. Принимаются только ключи контура (FOTMOB_*) — файл не может подменить
# PATH, HOME и прочее окружение скрипта. Всё, что скрипту нужно из окружения под именем
# FOTMOB_* (сам путь к env-файлу, эстафета автомата в b6_deliver.sh), он снимает в свои
# переменные ДО вызова загрузчика.
#
#   ENV_FILE="${FOTMOB_ENV_FILE:-/etc/data-platform/fotmob.env}"
#   . "$(dirname "$(readlink -f "$0")")/env.sh"; fotmob_load_env "$ENV_FILE" || exit 2

fotmob_load_env() {
  local file="$1" mode="${2:-host}" line key value stale
  [ -r "$file" ] || { echo "нет env-файла контура FotMob: $file" >&2; return 2; }
  for stale in $(compgen -v FOTMOB_); do
    if [ "$mode" = host ] || fotmob_package_key "$stale"; then unset -v "$stale"; fi
  done
  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) echo "строка без '=' в $file: ${line%%=*}" >&2; return 2 ;; esac
    key=${line%%=*}
    value=${line#*=}
    case "$key" in
      FOTMOB_[A-Z0-9_]*) ;;
      *) echo "недопустимый ключ в $file (ожидаются FOTMOB_*): $key" >&2; return 2 ;;
    esac
    case "$key" in *[!A-Z0-9_]*) echo "недопустимое имя переменной в $file: $key" >&2; return 2 ;; esac
    # Compose подставляет ${VAR} в env-файле и режет inline-комментарии ` #` у значений без
    # кавычек; этот загрузчик ни того, ни другого не делает. Чтобы compose и скрипты не
    # прочли один файл по-разному, такие значения отвергаются, а не трактуются по-своему.
    if [ "$mode" = package ] && ! fotmob_package_key "$key"; then
      echo "недопустимый ключ пакета: $key" >&2; return 2
    fi
    case "$value" in *'$'*) echo "значение $key в $file содержит '\$' — подстановки не поддерживаются, compose прочёл бы файл иначе" >&2; return 2 ;; esac
    case "$value" in
      \'*\') value=${value#\'}; value=${value%\'} ;;
      \"*\") value=${value#\"}; value=${value%\"}; value=${value//\\\"/\"}; value=${value//\\\\/\\} ;;
      *' #'*) echo "значение $key в $file содержит inline-комментарий ' #' — compose обрезал бы его" >&2; return 2 ;;
    esac
    # Переменная снята выше вместе со всеми FOTMOB_*; задаём заново как обычную
    # переменную оболочки (без export).
    printf -v "$key" '%s' "$value"
  done < "$file"
}

# Пакет содержит только пины; пути хоста он менять не может.
fotmob_package_key() {
  case "$1" in
    FOTMOB_TARGET|FOTMOB_ROLLBACK_REF|FOTMOB_ROLLBACK_SHA|FOTMOB_NEW_CODE_FILE|FOTMOB_NEW_CODE_MD5|FOTMOB_REPOSITORY_MD5|FOTMOB_RUNTIME_HASHES|FOTMOB_REQUIRED_MODULES) return 0 ;;
  esac
  return 1
}

fotmob_load_package() {
  local content digest key fingerprint expected module
  PACKAGE_FILE=$(realpath -e "$1") || return 2
  # Снимок читается один раз; проверяется и разбирается именно этот набор байтов.
  content=$(cat -- "$PACKAGE_FILE" && printf '.') || return 2
  content=${content%.}
  digest=$(printf '%s' "$content" | sha256sum) || return 2
  digest=${digest%% *}
  if [ -n "${2:-}" ] && [ "$digest" != "$2" ]; then
    echo "пакет доставки изменён после запуска автомата" >&2; return 2
  fi
  PACKAGE_DIGEST=$digest
  fotmob_load_env <(printf '%s' "$content") package || return 2
  for key in FOTMOB_TARGET FOTMOB_ROLLBACK_REF FOTMOB_ROLLBACK_SHA FOTMOB_NEW_CODE_FILE FOTMOB_NEW_CODE_MD5 FOTMOB_REPOSITORY_MD5 FOTMOB_RUNTIME_HASHES FOTMOB_REQUIRED_MODULES; do
    [ -n "${!key:-}" ] || { echo "$key не задан в пакете" >&2; return 2; }
  done
  for key in FOTMOB_TARGET FOTMOB_ROLLBACK_SHA; do
    [[ "${!key}" =~ ^[a-f0-9]{7,8}$ ]] || { echo "$key: ожидается короткий SHA (7–8 hex)" >&2; return 2; }
  done
  [[ "$FOTMOB_NEW_CODE_FILE" = /opt/airflow/* ]] || { echo "недопустимый FOTMOB_NEW_CODE_FILE" >&2; return 2; }
  [[ "$FOTMOB_NEW_CODE_MD5" =~ ^[a-f0-9]{32}$ && "$FOTMOB_REPOSITORY_MD5" =~ ^[a-f0-9]{32}$ ]] || { echo "недопустимый MD5 в пакете" >&2; return 2; }
  local -a fingerprints
  local -A modules=()
  IFS='|' read -r -a fingerprints <<< "$FOTMOB_RUNTIME_HASHES"
  for fingerprint in "${fingerprints[@]}"; do
    expected=${fingerprint%% *}; module=${fingerprint#* }
    [[ "$expected" =~ ^[a-f0-9]{64}$ && "$module" = /opt/airflow/* && "$module" != *' '* ]] || { echo "недопустимый SHA256/путь runtime fingerprint: $module" >&2; return 2; }
    [ -z "${modules[$module]:-}" ] || { echo "дубль runtime fingerprint: $module" >&2; return 2; }
    modules[$module]=1
  done
  # Состав релиза задаётся пакетом; HASHES обязан покрывать его целиком.
  local -a required
  local -A required_seen=()
  IFS='|' read -r -a required <<< "$FOTMOB_REQUIRED_MODULES"
  for module in "${required[@]}"; do
    [[ "$module" = /opt/airflow/* && "$module" != *' '* ]] || { echo "недопустимый required module: $module" >&2; return 2; }
    [ -z "${required_seen[$module]:-}" ] || { echo "дубль required module: $module" >&2; return 2; }
    required_seen[$module]=1
    [ -n "${modules[$module]:-}" ] || { echo "нет runtime fingerprint: $module" >&2; return 2; }
  done
  [ "${#modules[@]}" = "${#required_seen[@]}" ] || { echo "runtime manifest не соответствует required modules" >&2; return 2; }
}
