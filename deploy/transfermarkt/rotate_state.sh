#!/usr/bin/env bash
# Ротация состояния контура Transfermarkt (решение 9 #1387). Cron: 20 10 * * *
#   <каталог установки>/rotate_state.sh [--dry-run]
# Удаляет:
#   * logs/transfermarkt-native-v2/cycles/<id> — если внутри ничего не менялось > 35 суток;
#   * logs/transfermarkt-native-v2/cache/*     — файлы старше 7 суток;
#   * деревья release-* сверх 5 самых новых — только если дерево не в env-файле и не
#     смонтировано ни в один контейнер.
# --dry-run печатает, что удалил бы, и ничего не трогает. Леджер шлюза здесь не
# трогается: его архивирует deploy.sh при остановленном шлюзе.
set -euo pipefail

DRY=0
case "${1:-}" in --dry-run) DRY=1 ;; '') ;; *) echo "использование: $0 [--dry-run]" >&2; exit 2 ;; esac
ENV_FILE="${TRANSFERMARKT_ENV_FILE:-/etc/data-platform/transfermarkt.env}"
# shellcheck source=deploy/transfermarkt/env.sh
. "$(dirname "$(readlink -f "$0")")/env.sh"
transfermarkt_load_env "$ENV_FILE" || exit 2
: "${TRANSFERMARKT_RUNTIME_DIR:?}" "${TRANSFERMARKT_RELEASES_DIR:?}" "${TRANSFERMARKT_RELEASE_ROOT:?}"
CYCLES_DAYS="${TRANSFERMARKT_ROTATE_CYCLES_DAYS:-35}"
CACHE_DAYS="${TRANSFERMARKT_ROTATE_CACHE_DAYS:-7}"
KEEP_TREES="${TRANSFERMARKT_ROTATE_KEEP_TREES:-5}"
V2="$TRANSFERMARKT_RUNTIME_DIR/logs/transfermarkt-native-v2"
LOG="$TRANSFERMARKT_RUNTIME_DIR/rotate_state.log"

say() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"; [ "$DRY" = 1 ] || echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" >> "$LOG"; }
drop() {  # drop <путь> <почему>
  if [ "$DRY" = 1 ]; then say "dry-run: удалил бы $1 ($2)"; else rm -rf -- "$1"; say "удалено $1 ($2)"; fi
}

# Замок выката: дерево не должно исчезнуть посреди доставки или отката.
if [ "$DRY" != 1 ]; then
  lock_rc=0
  transfermarkt_take_deploy_lock 8 || lock_rc=$?
  [ "$lock_rc" = 1 ] && { say "идёт выкат (замок занят) — ротация пропущена"; exit 0; }
  [ "$lock_rc" = 0 ] || exit 2
fi

n_cycles=0; n_cache=0; n_trees=0
if [ -d "$V2/cycles" ] && [ ! -L "$V2/cycles" ]; then
  while IFS= read -r -d '' d; do
    # Свежий файл где угодно внутри — цикл жив.
    [ -z "$(find "$d" -newermt "-$CYCLES_DAYS days" -print -quit 2>/dev/null)" ] || continue
    drop "$d" "cycles: без изменений > $CYCLES_DAYS сут"; n_cycles=$((n_cycles + 1))
  done < <(find "$V2/cycles" -mindepth 1 -maxdepth 1 -print0)
fi
if [ -d "$V2/cache" ] && [ ! -L "$V2/cache" ]; then
  while IFS= read -r -d '' f; do
    drop "$f" "cache: старше $CACHE_DAYS сут"; n_cache=$((n_cache + 1))
  done < <(find "$V2/cache" -mindepth 1 -maxdepth 1 -type f -mtime "+$CACHE_DAYS" -print0)
fi

# Источники bind-монтов всех контейнеров хоста: смонтированное дерево не трогаем.
mounted=$(docker ps -aq | xargs -r docker inspect -f '{{range .Mounts}}{{println .Source}}{{end}}' 2>/dev/null) \
  || { say "docker не ответил про монты — деревья не трогаю"; mounted="__docker_unavailable__"; }
i=0
while IFS= read -r tree; do
  [ -n "$tree" ] || continue
  i=$((i + 1))
  [ "$i" -le "$KEEP_TREES" ] && continue
  [ "$mounted" = "__docker_unavailable__" ] && break
  [ "$tree" = "$TRANSFERMARKT_RELEASE_ROOT" ] && continue
  if printf '%s\n' "$mounted" | grep -qE "^$(printf '%s' "$tree" | sed 's/[][\.*^$]/\\&/g')(/|$)"; then
    say "дерево $tree смонтировано — оставляю"
    continue
  fi
  drop "$tree" "дерево сверх $KEEP_TREES последних"; n_trees=$((n_trees + 1))
done < <(find "$TRANSFERMARKT_RELEASES_DIR" -mindepth 1 -maxdepth 1 -type d -name 'release-*' -printf '%T@ %p\n' 2>/dev/null \
          | sort -rn | cut -d' ' -f2-)

say "итог$([ "$DRY" = 1 ] && echo ' (dry-run)'): cycles=$n_cycles cache=$n_cache trees=$n_trees"
