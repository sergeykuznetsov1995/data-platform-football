# shellcheck shell=bash
# Загрузчик env-файла контура FotMob для скриптов deploy/fotmob/*.sh (#1155, этап 3).
# Читает KEY=VALUE как docker compose (--env-file): без shell-подстановок, внешние
# одинарные кавычки — литерал, в двойных декодируются только \" и \\, CRLF-хвост
# снимается. Значения становятся переменными оболочки и НЕ экспортируются (даже если
# одноимённая переменная пришла экспортированной из окружения оператора — она
# сбрасывается): устаревшее значение из окружения процесса не может перекрыть файл.
# Принимаются только ключи контура (FOTMOB_*) — файл не может подменить PATH, HOME
# и прочее окружение скрипта.
#
#   ENV_FILE="${FOTMOB_ENV_FILE:-/etc/data-platform/fotmob.env}"
#   . "$(dirname "$(readlink -f "$0")")/env.sh"; fotmob_load_env "$ENV_FILE" || exit 2

fotmob_load_env() {
  local file="$1" line key value
  [ -r "$file" ] || { echo "нет env-файла контура FotMob: $file" >&2; return 2; }
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
    case "$value" in *'$'*) echo "значение $key в $file содержит '\$' — подстановки не поддерживаются, compose прочёл бы файл иначе" >&2; return 2 ;; esac
    case "$value" in
      \'*\') value=${value#\'}; value=${value%\'} ;;
      \"*\") value=${value#\"}; value=${value%\"}; value=${value//\\\"/\"}; value=${value//\\\\/\\} ;;
      *' #'*) echo "значение $key в $file содержит inline-комментарий ' #' — compose обрезал бы его" >&2; return 2 ;;
    esac
    # Снять переменную целиком (в том числе атрибут export, унаследованный от окружения),
    # затем задать заново как обычную переменную оболочки.
    unset -v "$key"
    printf -v "$key" '%s' "$value"
  done < "$file"
}
