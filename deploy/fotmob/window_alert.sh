#!/bin/bash
# Сторож окна доставки FotMob: TG-алерт при открытии окна (раз в сутки).
# Cron: */5 * * * * (внутренний фильтр по UTC-часам, DST не страшен).
# Окно = нет процесса раннера И нет running/queued dag_ingest_fotmob в метабазе изолята;
# по замерам 22–24.08 открывается ≈20:10–20:30 UTC, закрывается рождением волны 23:55.
# Заведён по ревью 24.08 (/root/FOTMOB-REVIEW-2026-08-24.md, Q2): четыре окна подряд
# были упущены, потому что дежурство держалось на живой сессии.
# Пути машины — из env-файла контура (deploy/fotmob/fotmob.env.example, #1155 этап 3).
set -u
ENV_FILE="${FOTMOB_ENV_FILE:-/etc/data-platform/fotmob.env}"
# shellcheck source=deploy/fotmob/env.sh
. "$(dirname "$(readlink -f "$0")")/env.sh"
fotmob_load_env "$ENV_FILE" || exit 2
STATE=${FOTMOB_STATE_DIR:?FOTMOB_STATE_DIR не задан в env-файле}
METADB=${FOTMOB_METADB_CONTAINER:?FOTMOB_METADB_CONTAINER не задан в env-файле}
TG_ENV=${FOTMOB_TG_ENV:?FOTMOB_TG_ENV не задан в env-файле}
LOG=${FOTMOB_LOG:?FOTMOB_LOG не задан в env-файле}
# Алерт сторож шлёт сам — тем же файлом токена, что автомат (tg() в auto_deliver.sh), и
# судит об отправке по ответу Telegram. Внешний хук для этого не годится: живой
# tg-send.sh возвращал 0 и без конфигурации, и при упавшем curl, так что суточная
# защёлка вставала при недоставленном сообщении, и окно терялось молча (ревью Sol,
# #1155). Нет файла с токеном — сторож не имеет смысла: громко в cron-лог и без защёлки.
[ -f "$TG_ENV" ] || { echo "нет файла с токеном Telegram $TG_ENV — сторож окна не работает" >&2; exit 2; }
# Каталог состояния сторож НЕ создаёт: его создаёт установка, а пропажу автомат
# считает аварией (без каталога нет выключателя, маркеров и суточной защёлки).
# Пустой каталог, созданный здесь перед тиком автомата, снял бы эту защиту и
# разрешил бы повторную доставку (ревью Sol, #1155).
[ -d "$STATE" ] && [ ! -L "$STATE" ] || { echo "нет каталога состояния $STATE — сторож окна не работает" >&2; exit 2; }

# 0 — только если Telegram ответил "ok":true.
tg(){
  local resp
  # shellcheck disable=SC1090
  . "$TG_ENV"
  if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
    echo "в $TG_ENV нет TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID" >&2; return 1
  fi
  resp=$(curl -s --max-time 15 "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
    -d chat_id="${TELEGRAM_CHAT_ID}" --data-urlencode text="[$(hostname)] $*" 2>/dev/null)
  case "$resp" in *'"ok":true'*) return 0 ;; esac
  echo "Telegram не принял алерт (ответ: $(printf '%s' "$resp" | head -c 200))" >&2
  return 1
}

hm=$(date -u +%H%M)
# strip возможного ведущего нуля для арифметики
hm=$((10#$hm))
[ "$hm" -ge 1945 ] && [ "$hm" -le 2355 ] || exit 0

FLAG=$STATE/fotmob-window-alert-$(date -u +%F)
[ -e "$FLAG" ] && exit 0

n=$(pgrep -f 'dags/scripts/run_fotmob_scrape[r].py' | wc -l)
act=$(docker exec "$METADB" psql -U airflow -d airflow -tA \
  -c "SELECT count(*) FROM dag_run WHERE dag_id='dag_ingest_fotmob' AND state IN ('running','queued');" \
  2>/dev/null || echo X)
# X (метабаза недоступна) — не окно: молчим, попробуем через 5 минут
[ "$n" = 0 ] && [ "$act" = 0 ] || exit 0

# Ручной путь ведёт через автомат, а не мимо него: автомат берёт flock, поэтому
# ручной и автоматический запуски не могут пройти приёмку одновременно и откатить
# дерево друг у друга (ревью Sol 25.08).
# Суточная защёлка — только после ПОДТВЕРЖДЁННОЙ отправки: иначе недоставленный алерт
# глушил бы сторожа до завтра, а окно терялось бы молча (ревью Sol, #1155).
if tg "🟢 FotMob: окно доставки ОТКРЫТО ($(date -u +%H:%M) UTC, закроется к 23:55 UTC). Доставка идёт сама по cron; вмешаться: автомат auto_deliver.sh (лог $LOG)"; then
  touch "$FLAG"
else
  echo "алерт не доставлен — защёлку не ставлю, повторю следующим тиком" >&2
  exit 3
fi
