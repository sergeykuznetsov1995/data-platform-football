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
  local file="$1" line key value stale
  [ -r "$file" ] || { echo "нет env-файла контура FotMob: $file" >&2; return 2; }
  for stale in $(compgen -v FOTMOB_); do unset -v "$stale"; done
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
    # Переменная снята выше вместе со всеми FOTMOB_*; задаём заново как обычную
    # переменную оболочки (без export).
    printf -v "$key" '%s' "$value"
  done < "$file"
}
