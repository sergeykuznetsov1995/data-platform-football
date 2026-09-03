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
# Три полосы источника (#1244). Пара «контейнер : переменная каталога состояния»:
# у каждого шлюза свой каталог — WAL/ledger рассчитаны на единственного писателя.
GATEWAY_CONTAINERS=(sofascore_gw_951 sofascore_gw_history sofascore_gw_players)
GATEWAY_STATE_DIRS=("${SOFASCORE_GATEWAY_STATE_HOST_DIR:?}" "${SOFASCORE_HISTORY_GW_STATE_HOST_DIR:?}" "${SOFASCORE_PLAYERS_GW_STATE_HOST_DIR:?}")
GATEWAY_SERVICES=(sofascore_proxy_filter sofascore_gw_history sofascore_gw_players)
WATCHDOG_UNITS=(sofascore-gw-lease-watchdog.service sofascore-gw-lease-watchdog-history.service sofascore-gw-lease-watchdog-players.service)
FAILS=0
fail() { echo "  ✗ $*"; FAILS=$((FAILS + 1)); }
ok() { echo "  ✓ $*"; }

echo "== 0. Ожидаемое дерево: $RELEASE =="

echo "== 1. Шлюзы полос: память, дерево, старт =="
for gw in "${GATEWAY_CONTAINERS[@]}"; do
  echo "-- $gw --"
  docker inspect -f 'Memory={{.HostConfig.Memory}} Started={{.State.StartedAt}} Health={{.State.Health.Status}}' "$gw"
  docker inspect -f '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}' "$gw"
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$gw" 2>/dev/null)" = "healthy" ] && ok "$gw healthy" || fail "$gw не healthy"
  [ "$(docker inspect -f '{{.HostConfig.Memory}}' "$gw" 2>/dev/null)" = "1073741824" ] && ok "$gw лимит памяти 1 GiB" || fail "$gw лимит памяти ≠ 1 GiB"
  docker inspect -f '{{range .Config.Cmd}}{{println .}}{{end}}' "$gw" \
    | grep -A1 -E "discovery-dagrun-budget-bytes" || fail "$gw: нет аргумента бюджета discovery"
  docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$gw" | grep -E "ARTIFACT|PAID" || true
done

echo
echo "== 2. Логи шлюзов: пул, платный путь, компакция WAL =="
for gw in "${GATEWAY_CONTAINERS[@]}"; do
  echo "-- $gw --"
  docker logs "$gw" --since 30m 2>&1 \
    | grep -E "residential pool|paid_enabled|compacted|listening|paid leases disabled" | tail -10 || true
done

echo
echo "== 3. Планировщик: дерево и env кампании =="
docker inspect -f 'Started={{.State.StartedAt}} Health={{.State.Health.Status}}' sofascore-airflow-scheduler
docker inspect -f '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}' sofascore-airflow-scheduler
[ "$(docker inspect -f '{{.State.Health.Status}}' sofascore-airflow-scheduler 2>/dev/null)" = "healthy" ] && ok "scheduler healthy" || fail "scheduler не healthy"
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' sofascore-airflow-scheduler | grep -qE "ALL_MENS_STATE=" && ok "env кампании на месте" || fail "нет env кампании"
echo "-- точные пары монтов (destination → ожидаемый source) --"
check_mounts() {  # check_mounts <container> <dest>=<expected-source> ...
  # Точное множество bind-монтов: каждая ожидаемая пара должна совпасть, а любой bind,
  # которого нет в списке (второе дерево, старый file-bind мини-DAG, чужой файл), — ошибка.
  # Именованные тома (type=volume) не сверяются.
  local container="$1"; shift
  local actual dest expected got line
  actual=$(docker inspect -f '{{range .Mounts}}{{.Type}}:{{.Destination}}={{.Source}}{{"\n"}}{{end}}' "$container" | grep '^bind:' | sed 's/^bind://')
  for pair in "$@"; do
    dest=${pair%%=*}; expected=${pair#*=}
    got=$(printf '%s\n' "$actual" | grep -E "^$(printf '%s' "$dest" | sed 's/[][\.*^$]/\\&/g')=" | head -1 | cut -d= -f2-)
    if [ "$got" = "$expected" ]; then ok "$container $dest ← $expected"; else fail "$container $dest ← '${got:-<нет монта>}' (ожидалось $expected)"; fi
  done
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    dest=${line%%=*}
    case " $* " in *" $dest="*) ;; *) fail "$container: лишний bind-монт $line" ;; esac
  done <<< "$actual"
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
# Дерево, fallback-файл и артефакт у трёх шлюзов общие; каталог состояния — свой.
for i in "${!GATEWAY_CONTAINERS[@]}"; do
  check_mounts "${GATEWAY_CONTAINERS[$i]}" \
    "/opt/sofascore-repo=$RELEASE" \
    "/opt/airflow/proxys.txt=${SOFASCORE_GATEWAY_FALLBACK_PROXY_FILE:?}" \
    "/opt/airflow/runtime/sofascore/proxy_budget_canary.json=${SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST:?}" \
    "/opt/airflow/logs/sofascore_proxy_filter=${GATEWAY_STATE_DIRS[$i]}"
done

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
echo "== 6. Вотчдоги аренд (по одному на шлюз) =="
# Сверяем не только дерево, но и пару «контейнер / каталог состояния»: сторож,
# перепутавший полосу, был бы active и рестартовал бы чужой шлюз по чужому WAL.
for i in "${!WATCHDOG_UNITS[@]}"; do
  unit="${WATCHDOG_UNITS[$i]}"
  exec_start=$(systemctl show -p ExecStart "$unit" 2>/dev/null || true)
  echo "$unit: $(printf '%s' "$exec_start" | grep -o -- "--container [^ ;]* --state-dir [^ ;]* --expected-mount [^ ;]*" || echo "(ExecStart не разобран)")"
  for expected in "--container ${GATEWAY_CONTAINERS[$i]}" \
                  "--state-dir ${GATEWAY_STATE_DIRS[$i]}" \
                  "--expected-mount $RELEASE"; do
    flag=${expected%% *}
    got=$(printf '%s' "$exec_start" | grep -o -- "$flag [^ ;]*" | head -1 || true)
    [ "$got" = "$expected" ] && ok "$unit $expected" || fail "$unit: '${got:-<нет $flag>}' (ожидалось $expected)"
  done
  [ "$(systemctl is-active "$unit")" = "active" ] && ok "$unit active" || fail "$unit не active"
done

echo
echo "== 7. Health шлюзов (изнутри scheduler'а, по алиасам сети контура) =="
for svc in "${GATEWAY_SERVICES[@]}"; do
  docker exec sofascore-airflow-scheduler python -c "
import json,urllib.request
print(json.dumps(json.load(urllib.request.urlopen('http://$svc:8899/health', timeout=15)), ensure_ascii=False)[:600])
" 2>&1 | tail -3 && ok "$svc /health отвечает" || fail "$svc /health не отвечает"
done

echo
echo "== 8. Пулы полос =="
$PSQL "SELECT pool, slots FROM slot_pool WHERE pool IN ('ingest_scraper_pool','sofascore_history_pool','sofascore_players_pool') ORDER BY 1;"
for spec in "ingest_scraper_pool=1" \
            "sofascore_history_pool=${SOFASCORE_HISTORY_POOL_SLOTS:-1}" \
            "sofascore_players_pool=${SOFASCORE_PLAYERS_POOL_SLOTS:-1}"; do
  pool=${spec%%=*}; want=${spec#*=}
  got=$($PSQL "SELECT slots FROM slot_pool WHERE pool='$pool';")
  [ "$got" = "$want" ] && ok "пул $pool slots=$got" || fail "пул $pool slots='${got:-<нет пула>}' (ожидалось $want)"
done

echo
if [ "$FAILS" -eq 0 ]; then echo "ПРИЁМКА: ок"; else echo "ПРИЁМКА: $FAILS проблем(ы)"; exit 1; fi
