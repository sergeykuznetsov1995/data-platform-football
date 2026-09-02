#!/usr/bin/env bash
# Приёмка выката на контур SofaScore. Только чтение; код выхода 1, если хоть одна
# проверка не сошлась (монты не на ожидаемом дереве, import_error, неактивные DAG,
# сторож не на дереве или не active, /health не отвечает).
# Использование: bash deploy/sofascore/postdeploy_checks.sh [release-root]
#   (по умолчанию — SOFASCORE_RELEASE_ROOT из $SOFASCORE_ENV_FILE)
set -uo pipefail
ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
# shellcheck source=deploy/sofascore/env.sh
. "$(dirname "$0")/env.sh"
sofascore_load_env "$ENV_FILE" || exit 2
RELEASE="${1:-${SOFASCORE_RELEASE_ROOT:?}}"
CAMPAIGN="${SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR:?}"
PSQL="docker exec sofascore-airflow-metadb psql -U airflow -d airflow -At -c"
FAILS=0
fail() { echo "  ✗ $*"; FAILS=$((FAILS + 1)); }
ok() { echo "  ✓ $*"; }

echo "== 0. Ожидаемое дерево: $RELEASE =="

echo "== 1. Шлюз: память, дерево, старт =="
docker inspect -f 'Memory={{.HostConfig.Memory}} Started={{.State.StartedAt}} Health={{.State.Health.Status}}' sofascore_gw_951
docker inspect -f '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}' sofascore_gw_951
[ "$(docker inspect -f '{{.State.Health.Status}}' sofascore_gw_951 2>/dev/null)" = "healthy" ] && ok "шлюз healthy" || fail "шлюз не healthy"
[ "$(docker inspect -f '{{.HostConfig.Memory}}' sofascore_gw_951 2>/dev/null)" = "1073741824" ] && ok "лимит памяти 1 GiB" || fail "лимит памяти шлюза ≠ 1 GiB"
echo "-- аргумент бюджета discovery (в Cmd) --"
docker inspect -f '{{range .Config.Cmd}}{{println .}}{{end}}' sofascore_gw_951 \
  | grep -A1 -E "discovery-dagrun-budget-bytes" || fail "нет аргумента бюджета discovery"
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' sofascore_gw_951 | grep -E "ARTIFACT|PAID" || true

echo
echo "== 2. Лог шлюза: пул, платный путь, компакция WAL =="
docker logs sofascore_gw_951 --since 30m 2>&1 \
  | grep -E "residential pool|paid_enabled|compacted|listening|paid leases disabled" | tail -10 || true

echo
echo "== 3. Планировщик: дерево и env кампании =="
docker inspect -f 'Started={{.State.StartedAt}} Health={{.State.Health.Status}}' sofascore-airflow-scheduler
docker inspect -f '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}' sofascore-airflow-scheduler
[ "$(docker inspect -f '{{.State.Health.Status}}' sofascore-airflow-scheduler 2>/dev/null)" = "healthy" ] && ok "scheduler healthy" || fail "scheduler не healthy"
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' sofascore-airflow-scheduler | grep -qE "ALL_MENS_STATE=" && ok "env кампании на месте" || fail "нет env кампании"
echo "-- точные пары монтов (destination → ожидаемый source) --"
check_mounts() {  # check_mounts <container> <dest>=<expected-source> ...
  local container="$1"; shift
  local actual dest expected got extra
  actual=$(docker inspect -f '{{range .Mounts}}{{.Destination}}={{.Source}}{{"\n"}}{{end}}' "$container")
  for pair in "$@"; do
    dest=${pair%%=*}; expected=${pair#*=}
    got=$(printf '%s\n' "$actual" | grep -F -- "$dest=" | grep -E "^$(printf '%s' "$dest" | sed 's/[][\.*^$]/\\&/g')=" | head -1 | cut -d= -f2-)
    if [ "$got" = "$expected" ]; then ok "$container $dest ← $expected"; else fail "$container $dest ← '${got:-<нет монта>}' (ожидалось $expected)"; fi
  done
  extra=$(printf '%s\n' "$actual" | grep -E '^/opt/airflow/dags/' | grep -vF '/opt/airflow/dags/.airflowignore=' || true)
  [ -z "$extra" ] || fail "$container: лишние монты поверх dags/: $extra"
}
check_mounts sofascore-airflow-scheduler \
  "/opt/airflow/dags=$RELEASE/dags" \
  "/opt/airflow/dags/.airflowignore=$RELEASE/deploy/sofascore/.airflowignore" \
  "/opt/airflow/logs=$RELEASE/logs" \
  "/opt/airflow/scrapers=$RELEASE/scrapers" \
  "/opt/airflow/scripts=$RELEASE/scripts" \
  "/opt/airflow/configs/medallion=$RELEASE/configs/medallion" \
  "/opt/airflow/configs/soccerdata=$RELEASE/configs/soccerdata" \
  "/opt/airflow/configs/sofascore=$RELEASE/configs/sofascore" \
  "/opt/airflow/configs/proxy_filter=$RELEASE/configs/proxy_filter" \
  "/opt/airflow/docker=$RELEASE/docker" \
  "/opt/airflow/runtime/sofascore/proxy_budget_canary.json=${SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST:?}" \
  "/opt/airflow/runtime/sofascore/all-men=$CAMPAIGN" \
  "/opt/airflow/proxys.txt=${SOFASCORE_PROXY_POOL_FILE:?}" \
  "/opt/legacy-scraper-venv=${SOFASCORE_LEGACY_SCRAPER_VENV_HOST_DIR:?}"
check_mounts sofascore_gw_951 \
  "/opt/sofascore-repo=$RELEASE" \
  "/opt/airflow/proxys.txt=${SOFASCORE_GATEWAY_FALLBACK_PROXY_FILE:?}" \
  "/opt/airflow/runtime/sofascore/proxy_budget_canary.json=${SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST:?}" \
  "/opt/airflow/logs/sofascore_proxy_filter=${SOFASCORE_GATEWAY_STATE_HOST_DIR:?}"

echo
echo "== 4. Метабаза: import_error, состояние DAG-ов =="
errs=$($PSQL "SELECT count(*) FROM import_error;")
echo "import_error=$errs"
[ "$errs" = "0" ] && ok "import_error=0" || fail "import_error=$errs"
$PSQL "SELECT dag_id, is_paused, is_active FROM dag WHERE dag_id LIKE '%sofascore%' ORDER BY 1;"
active=$($PSQL "SELECT count(*) FROM dag WHERE dag_id IN ('dag_backfill_sofascore_all_mens','dag_refresh_sofascore_all_mens','dag_ingest_sofascore','dag_trigger_sofascore_daily','dag_sofascore_manifest_maintenance') AND is_active=true;")
[ "$active" = "5" ] && ok "5 DAG контура активны" || fail "активных DAG контура: $active из 5"

echo
echo "== 5. Состояние кампании =="
python3 - "$CAMPAIGN/state.json" <<'PY' || fail "state.json кампании не читается"
import json, sys, os
p = sys.argv[1]
if not os.path.exists(p):
    print("НЕТ", p); raise SystemExit(1)
s = json.load(open(p))
print("completed =", len(s.get("completed", [])), "| файл:", p)
PY
ls -1 "$CAMPAIGN/results" 2>/dev/null | wc -l | sed 's/^/results файлов: /'
[ -f "$CAMPAIGN/failures.json" ] && python3 -c "import json;d=json.load(open('$CAMPAIGN/failures.json'));print('failures ключей:',len(d) if isinstance(d,dict) else len(d))"

echo
echo "== 6. Вотчдог аренд =="
pin=$(systemctl show -p ExecStart sofascore-gw-lease-watchdog.service | grep -o -- "--expected-mount [^ ;]*" || true)
echo "${pin:-(нет --expected-mount)}"
[ "$pin" = "--expected-mount $RELEASE" ] && ok "сторож на $RELEASE" || fail "сторож не на $RELEASE"
[ "$(systemctl is-active sofascore-gw-lease-watchdog.service)" = "active" ] && ok "сторож active" || fail "сторож не active"

echo
echo "== 7. Health шлюза =="
docker exec sofascore-airflow-scheduler python -c "
import json,urllib.request
print(json.dumps(json.load(urllib.request.urlopen('http://sofascore_proxy_filter:8899/health', timeout=15)), ensure_ascii=False)[:600])
" 2>&1 | tail -3 && ok "/health отвечает" || fail "/health не отвечает"

echo
if [ "$FAILS" -eq 0 ]; then echo "ПРИЁМКА: ок"; else echo "ПРИЁМКА: $FAILS проблем(ы)"; exit 1; fi
