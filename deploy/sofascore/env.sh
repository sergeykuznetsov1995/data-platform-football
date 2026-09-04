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
  local file="$1" line key value
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
    # Снять переменную целиком (в том числе атрибут export, унаследованный от окружения),
    # затем задать заново как обычную переменную оболочки.
    unset -v "$key"
    printf -v "$key" '%s' "$value"
  done < "$file"
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
