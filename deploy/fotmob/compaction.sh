#!/usr/bin/env bash
# Ночная компакция мелких файлов bronze FotMob (#1284) — хостовая обёртка.
#
# Репо-копия. Устанавливает её владелец-сессия (автомат доставки хостовые
# скрипты не возит), с .prev-копией прежней версии:
#
#   cp deploy/fotmob/compaction.sh /root/watchdog/fotmob_compaction.sh
#   chmod 755 /root/watchdog/fotmob_compaction.sh && bash -n /root/watchdog/fotmob_compaction.sh
#   crontab -l > /root/watchdog/crontab.prev-<дата>
#   # 23:30 UTC летом (хост в UTC+3):
#   30 1 * * * /root/watchdog/fotmob_compaction.sh
#   # после перевода часов 25.10 строку сменить на `30 0 * * *`
#
# Окно, барьер волны, замок писателя и потолок порции держит сам модуль:
# обёртка только запускает его в контейнере изолята и хранит итоговый JSON.
set -uo pipefail

CONTAINER="${FOTMOB_COMPACTION_CONTAINER:-fotmob-airflow-scheduler}"
STATE_DIR="${FOTMOB_COMPACTION_STATE:-/root/watchdog/state}"
LOG="${FOTMOB_COMPACTION_LOG:-/root/watchdog/fotmob_compaction.log}"

mkdir -p "$STATE_DIR" "$(dirname "$LOG")"
out=$(timeout -k 30 1500 docker exec "$CONTAINER" python -m scrapers.fotmob.maintenance 2>>"$LOG")
rc=$?

printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$out" >> "$LOG"
last=$(printf '%s\n' "$out" | tail -n 1)
case "$last" in
  '{'*) printf '%s\n' "$last" > "$STATE_DIR/fotmob_compaction_last.json" ;;
esac

exit "$rc"
