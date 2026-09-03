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
TG_HOOK=${FOTMOB_TG_HOOK:?FOTMOB_TG_HOOK не задан в env-файле}
LOG=${FOTMOB_LOG:?FOTMOB_LOG не задан в env-файле}

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

mkdir -p "$STATE"
# Ручной путь ведёт через автомат, а не мимо него: автомат берёт flock, поэтому
# ручной и автоматический запуски не могут пройти приёмку одновременно и откатить
# дерево друг у друга (ревью Sol 25.08).
"$TG_HOOK" "🟢 FotMob: окно доставки ОТКРЫТО ($(date -u +%H:%M) UTC, закроется к 23:55 UTC). Доставка идёт сама по cron; вмешаться: автомат auto_deliver.sh (лог $LOG)"
touch "$FLAG"
