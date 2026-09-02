#!/usr/bin/env bash
# Приёмка выката на контур SofaScore. Только чтение.
# Использование: bash deploy/sofascore/postdeploy_checks.sh [release-root]
#   (по умолчанию — SOFASCORE_RELEASE_ROOT из $SOFASCORE_ENV_FILE)
set -uo pipefail
ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
# shellcheck disable=SC1090
set -a; . "$ENV_FILE"; set +a
RELEASE="${1:-${SOFASCORE_RELEASE_ROOT:?}}"
CAMPAIGN="${SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR:?}"
PSQL="docker exec sofascore-airflow-metadb psql -U airflow -d airflow -At -c"

echo "== 0. Ожидаемое дерево: $RELEASE =="

echo "== 1. Шлюз: память, дерево, старт =="
docker inspect -f 'Memory={{.HostConfig.Memory}} Started={{.State.StartedAt}} Health={{.State.Health.Status}}' sofascore_gw_951
docker inspect -f '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}' sofascore_gw_951
echo "-- аргумент бюджета discovery (в Cmd) --"
docker inspect -f '{{range .Config.Cmd}}{{println .}}{{end}}' sofascore_gw_951 \
  | grep -A1 -E "discovery-dagrun-budget-bytes" || echo "(нет аргумента бюджета discovery!)"
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' sofascore_gw_951 | grep -E "ARTIFACT|PAID" || true

echo
echo "== 2. Лог шлюза: пул, платный путь, компакция WAL =="
docker logs sofascore_gw_951 --since 30m 2>&1 \
  | grep -E "residential pool|paid_enabled|compacted|listening|paid leases disabled" | tail -10

echo
echo "== 3. Планировщик: дерево и env кампании =="
docker inspect -f 'Started={{.State.StartedAt}} Health={{.State.Health.Status}}' sofascore-airflow-scheduler
docker inspect -f '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}' sofascore-airflow-scheduler
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' sofascore-airflow-scheduler | grep -E "ALL_MENS_(STATE|RESULT)|REFRESH" || echo "(нет env кампании!)"
echo "-- монты не на ожидаемом дереве --"
docker inspect -f '{{range .Mounts}}{{.Source}}{{"\n"}}{{end}}' sofascore-airflow-scheduler sofascore_gw_951 \
  | grep -E "/release-|dpf-release-" | grep -v "^$RELEASE" || echo "(все монты релиза на $RELEASE)"

echo
echo "== 4. Метабаза: import_error, состояние DAG-ов =="
$PSQL "SELECT count(*) FROM import_error;" | sed 's/^/import_error=/'
$PSQL "SELECT dag_id, is_paused, is_active FROM dag WHERE dag_id LIKE '%sofascore%' ORDER BY 1;"

echo
echo "== 5. Состояние кампании =="
python3 - "$CAMPAIGN/state.json" <<'PY'
import json, sys, os
p = sys.argv[1]
if not os.path.exists(p):
    print("НЕТ", p); raise SystemExit
s = json.load(open(p))
print("completed =", len(s.get("completed", [])), "| файл:", p)
PY
ls -1 "$CAMPAIGN/results" 2>/dev/null | wc -l | sed 's/^/results файлов: /'
[ -f "$CAMPAIGN/failures.json" ] && python3 -c "import json;d=json.load(open('$CAMPAIGN/failures.json'));print('failures ключей:',len(d) if isinstance(d,dict) else len(d))"

echo
echo "== 6. Вотчдог аренд =="
systemctl show -p ExecStart sofascore-gw-lease-watchdog.service | grep -o -- "--expected-mount [^ ]*"
systemctl is-active sofascore-gw-lease-watchdog.service

echo
echo "== 7. Health шлюза =="
docker exec sofascore-airflow-scheduler python -c "
import json,urllib.request
print(json.dumps(json.load(urllib.request.urlopen('http://sofascore_proxy_filter:8899/health', timeout=15)), ensure_ascii=False)[:600])
" 2>&1 | tail -3
