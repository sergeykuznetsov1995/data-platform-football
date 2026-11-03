#!/usr/bin/env bash
# Запуск python-скрипта deploy/sofascore/ из ТЕКУЩЕГО релиза контура (#1376): сторож полос
# в cron, измеритель вехи 1 в утренней сводке. Дерево берётся из SOFASCORE_RELEASE_ROOT
# env-файла при каждом запуске — новая доставка подхватывается без правки cron.
# Ставится рядом с env.sh в /usr/local/libexec/sofascore/; в md5-сверку автомата не входит —
# при изменении переустановить вручную (рунбук). Логики здесь нет и быть не должно.
# Использование: run_from_release.sh <скрипт.py> [аргументы…]; нет скрипта/env-файла → код 2.
set -uo pipefail
ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
# shellcheck source=deploy/sofascore/env.sh
. "$(dirname "$0")/env.sh"
sofascore_load_env "$ENV_FILE" || exit 2
ROOT="${SOFASCORE_RELEASE_ROOT:-}"
[ -n "$ROOT" ] || { echo "в $ENV_FILE нет SOFASCORE_RELEASE_ROOT" >&2; exit 2; }
case "${1:-}" in
  ''|*/*) echo "ожидается имя скрипта из deploy/sofascore/: '${1:-}'" >&2; exit 2 ;;
esac
SCRIPT="$ROOT/deploy/sofascore/$1"
[ -f "$SCRIPT" ] || { echo "нет скрипта в релизе: $SCRIPT" >&2; exit 2; }
exec env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$ROOT" /usr/bin/python3 "$SCRIPT" "${@:2}"
