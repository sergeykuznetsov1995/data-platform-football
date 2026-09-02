# shellcheck shell=bash
# Загрузчик env-файла контура SofaScore для скриптов deploy/sofascore/*.sh.
# Читает KEY=VALUE как docker compose (--env-file): без shell-подстановок, внешние
# одинарные/двойные кавычки снимаются. Значения становятся переменными оболочки,
# но НЕ экспортируются: compose получает их только через --env-file, поэтому
# устаревшее значение из окружения процесса не может перекрыть перепинованный файл.
#
#   ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
#   . "$(dirname "$0")/env.sh"; sofascore_load_env "$ENV_FILE"

sofascore_load_env() {
  local file="$1" line key value
  [ -r "$file" ] || { echo "нет env-файла контура: $file" >&2; return 2; }
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) echo "строка без '=' в $file: ${line%%=*}" >&2; return 2 ;; esac
    key=${line%%=*}
    value=${line#*=}
    case "$key" in
      *[!A-Za-z0-9_]*|'') echo "недопустимое имя переменной в $file: $key" >&2; return 2 ;;
    esac
    case "$value" in
      \'*\') value=${value#\'}; value=${value%\'} ;;
      \"*\") value=${value#\"}; value=${value%\"} ;;
    esac
    printf -v "$key" '%s' "$value"
  done < "$file"
}
