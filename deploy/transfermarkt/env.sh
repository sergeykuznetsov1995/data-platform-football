# shellcheck shell=bash
# Загрузчик env-файла контура Transfermarkt (#1387) для скриптов deploy/transfermarkt/*.sh.
# Копия deploy/sofascore/env.sh: меняются только белый список ключей и имена функций.
# Читает KEY=VALUE как docker compose (--env-file): без shell-подстановок, внешние
# одинарные кавычки — литерал, в двойных декодируются только \" и \\, CRLF-хвост
# снимается. Значения становятся переменными оболочки и НЕ экспортируются (даже если
# одноимённая переменная пришла экспортированной из окружения оператора — она
# сбрасывается): compose получает значения только через --env-file, поэтому устаревшее
# значение из окружения процесса не может перекрыть перепинованный файл.
# Принимаются только ключи контура (TRANSFERMARKT_*, TM_*) — файл не
# может подменить PATH, HOME и прочее окружение скрипта.
#
#   ENV_FILE="${TRANSFERMARKT_ENV_FILE:-/etc/data-platform/transfermarkt.env}"
#   . "$(dirname "$0")/env.sh"; transfermarkt_load_env "$ENV_FILE" || exit 2

transfermarkt_load_env() {
  local file="$1" line key value
  [ -r "$file" ] || { echo "нет env-файла контура: $file" >&2; return 2; }
  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) echo "строка без '=' в $file: ${line%%=*}" >&2; return 2 ;; esac
    key=${line%%=*}
    value=${line#*=}
    case "$key" in
      TRANSFERMARKT_[A-Z0-9_]*|TM_[A-Z0-9_]*) ;;
      *) echo "недопустимый ключ в $file (ожидаются TRANSFERMARKT_*/TM_*): $key" >&2; return 2 ;;
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

# Замок выката: один протокол на автомат ночной доставки и на ручной deploy.sh.
# Раньше автомат отличал ручной выкат по `pgrep deploy.sh` — между «процесса нет» и первым
# изменением контура помещался целый чужой выкат, а обратной проверки у deploy.sh не было
# вовсе. Файл лежит в корне $TRANSFERMARKT_RUNTIME_DIR: он не меняется от релиза к релизу.
# Путь считается ПОСЛЕ загрузки env-файла (TRANSFERMARKT_RUNTIME_DIR берётся оттуда), поэтому
# он не константа файла, а отдельный шаг: transfermarkt_deploy_lock_init задаёт
# TRANSFERMARKT_DEPLOY_LOCK, если его не переопределили ключом env-файла или окружением стенда.
transfermarkt_deploy_lock_init() {
  [ -n "${TRANSFERMARKT_DEPLOY_LOCK:-}" ] && return 0
  [ -n "${TRANSFERMARKT_RUNTIME_DIR:-}" ] || { echo "TRANSFERMARKT_RUNTIME_DIR не задан — замок выката класть некуда" >&2; return 2; }
  TRANSFERMARKT_DEPLOY_LOCK="$TRANSFERMARKT_RUNTIME_DIR/deploy.lock"
}

# transfermarkt_take_deploy_lock <fd> — 0 взят, 1 занят (чужой выкат идёт), 2 поломка.
# «Занято» и «поломка» — разные новости: подменённый путь или нечитаемый каталог не должны
# выглядеть как мирная конкуренция, из-за которой тик просто пропускают.
transfermarkt_take_deploy_lock() {  # transfermarkt_take_deploy_lock <fd>
  local fd="$1" path dir perm rc
  transfermarkt_deploy_lock_init || return 2
  path="$TRANSFERMARKT_DEPLOY_LOCK"
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
  # `>>`, а не `>`: путь настраиваемый, и опечатка в TRANSFERMARKT_DEPLOY_LOCK обнулила бы
  # обычный файл, на который она указала, ещё до flock. Дописывать в замок
  # никто не собирается — нужен только дескриптор.
  eval "exec $fd>>\"\$path\"" 2>/dev/null || { echo "не открывается замок выката: $path" >&2; return 2; }
  flock -n "$fd"; rc=$?
  [ "$rc" = 0 ] && return 0
  [ "$rc" = 1 ] && return 1
  echo "flock отказал кодом $rc — это не конкуренция" >&2
  return 2
}

# Переписать строку KEY= в env-файле контура. Живёт здесь, а не в deploy.sh: ту же
# правку делает автомат ночной доставки при откате, и две копии одного `sed`
# рано или поздно разъехались бы — откат перепинывал бы не то, что перепинул выкат.
transfermarkt_set_env_var() {  # transfermarkt_set_env_var <file> <key> <value>
  # Значение подставляется в правую часть sed: `&` в нём развернулось бы в найденное.
  # Путям релиза и hex-идентификаторам это не грозит; шире функция не применяется.
  grep -q "^$2=" "$1" || { echo "в $1 нет строки $2=" >&2; return 2; }
  sed -i "s#^$2=.*#$2=$3#" "$1"
}

# /health шлюза изнутри планировщика: режим transfermarkt-only и ни одного ключа daily_*
# (суточного бюджета нет, #1387). Одна проба на выкат, автомат и приёмку — три копии
# рано или поздно разошлись бы в том, что считать «здоровым».
# Печатает одну строку «ok|bad source_mode=… daily_keys=… paid_enabled=… live_exit_ratio=…»;
# код 0 — ok, 1 — шлюз ответил не так, 2 — проба не выполнилась (строка — её вывод).
transfermarkt_gateway_health_ok() {  # transfermarkt_gateway_health_ok <контейнер планировщика>
  local out
  out=$(timeout -k 5 60 docker exec "$1" python -c '
import json, urllib.request
h = json.load(urllib.request.urlopen("http://transfermarkt_gw:8899/health", timeout=10))
daily = sum(k.startswith("daily_") for k in h)
ok = h.get("source_mode") == "transfermarkt-only" and daily == 0
print(("ok" if ok else "bad") + " source_mode=%s daily_keys=%d paid_enabled=%s live_exit_ratio=%s" % (h.get("source_mode"), daily, h.get("transfermarkt_paid_enabled"), h.get("live_exit_ratio")))
' 2>&1) || { echo "probe failed: $out"; return 2; }
  echo "$out"
  case "$out" in ok|ok\ *) return 0 ;; *) return 1 ;; esac
}

# Все bind-монты контейнера из каталога релизов ведут в дерево <дерево>, и их не меньше
# <минимум>. Печатает 1 / 0; X — docker inspect не ответил.
transfermarkt_mounts_in() {  # transfermarkt_mounts_in <контейнер> <каталог релизов> <дерево> <минимум>
  local out
  out=$(timeout -k 5 30 docker inspect -f '{{range .Mounts}}{{if eq .Type "bind"}}{{println .Source}}{{end}}{{end}}' "$1" 2>/dev/null) \
    || { echo X; return 0; }
  printf '%s\n' "$out" | awk -v root="$2/" -v new="$3" -v min="$4" '
    index($0,root)==1 { t++; if ($0!=new && index($0,new"/")!=1) b++ }
    END { print (t>=min && b==0) ? 1 : 0 }'
}
