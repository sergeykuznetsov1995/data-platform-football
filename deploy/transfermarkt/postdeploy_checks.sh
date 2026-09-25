#!/usr/bin/env bash
# Приёмка выката на контур Transfermarkt (#1387; образец deploy/sofascore). Только чтение;
# код выхода 1, если хоть одна проверка не сошлась.
# Использование: bash deploy/transfermarkt/postdeploy_checks.sh [release-root]
#   (по умолчанию — TRANSFERMARKT_RELEASE_ROOT из $TRANSFERMARKT_ENV_FILE)
set -uo pipefail
ENV_FILE="${TRANSFERMARKT_ENV_FILE:-/etc/data-platform/transfermarkt.env}"
# shellcheck source=deploy/transfermarkt/env.sh
. "$(dirname "$0")/env.sh"
transfermarkt_load_env "$ENV_FILE" || exit 2
RELEASE="${1:-${TRANSFERMARKT_RELEASE_ROOT:?}}"
RUNTIME="${TRANSFERMARKT_RUNTIME_DIR:?}"
SCHED=transfermarkt-airflow-scheduler
GW=transfermarkt_gw
PSQL="timeout -k 5 30 docker exec transfermarkt-airflow-metadb psql -U airflow -d airflow -At -c"
TM_DAGS="dag_ingest_transfermarkt dag_discover_transfermarkt_registry dag_backfill_transfermarkt dag_transform_transfermarkt_silver"
TM_DAGS_SQL="'dag_ingest_transfermarkt','dag_discover_transfermarkt_registry','dag_backfill_transfermarkt','dag_transform_transfermarkt_silver'"
FAILS=0
fail() { echo "  ✗ $*"; FAILS=$((FAILS + 1)); }
ok() { echo "  ✓ $*"; }
check_mounts() {  # check_mounts <container> <dest>=<expected-source> ...
  # Точное множество bind-монтов: лишний bind (второе дерево, чужой файл) — ошибка.
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

echo "== 0. Дерево: $RELEASE =="
[ -d "$RELEASE" ] && ok "каталог есть" || fail "нет каталога $RELEASE"
[ "$(stat -c %a "$RELEASE" 2>/dev/null)" = 755 ] && ok "права 755" || fail "права дерева не 755"
head=$(git -C "$RELEASE" --no-optional-locks rev-parse HEAD 2>/dev/null)
[ -n "$head" ] && ok "HEAD ${head:0:8}" || fail "HEAD не читается"
[ -z "$(git -C "$RELEASE" --no-optional-locks status --porcelain 2>/dev/null)" ] && ok "дерево чистое" || fail "в дереве правки"
[ "${TRANSFERMARKT_RELEASE_ROOT:-}" = "$RELEASE" ] && ok "env-файл указывает на это дерево" || fail "env-файл указывает на '${TRANSFERMARKT_RELEASE_ROOT:-}'"

echo "== 1. Шлюз: память, монты, /health =="
docker inspect -f 'Memory={{.HostConfig.Memory}} Started={{.State.StartedAt}} Health={{.State.Health.Status}} Project={{index .Config.Labels "com.docker.compose.project"}}' "$GW"
[ "$(docker inspect -f '{{.State.Health.Status}}' "$GW" 2>/dev/null)" = "healthy" ] && ok "$GW healthy" || fail "$GW не healthy"
[ "$(docker inspect -f '{{.HostConfig.Memory}}' "$GW" 2>/dev/null)" = "1073741824" ] && ok "$GW лимит памяти 1 GiB" || fail "$GW лимит памяти ≠ 1 GiB"
cmd=$(docker inspect -f '{{range .Config.Cmd}}{{println .}}{{end}}' "$GW")
printf '%s\n' "$cmd" | grep -qx transfermarkt-only && ok "--source-mode transfermarkt-only" || fail "шлюз не в режиме transfermarkt-only"
printf '%s\n' "$cmd" | grep -qE -- "--(daily-budget-mb|dagrun-budget-bytes|transfermarkt-dagrun-budget-bytes|url-budget-bytes)" \
  && fail "в команде шлюза бюджетный флаг" || ok "бюджетных флагов в команде нет"
check_mounts "$GW" \
  "/opt/transfermarkt-repo=$RELEASE" \
  "/opt/airflow/logs/proxy_filter=$RUNTIME/gateway-state"
docker logs "$GW" --since 30m 2>&1 | grep -E "residential pool|paid_enabled|listening|source_mode" | tail -5 || true

echo "== 2. Планировщик: монты, env без секретов, /health шлюза изнутри =="
docker inspect -f 'Started={{.State.StartedAt}} Health={{.State.Health.Status}}' "$SCHED"
[ "$(docker inspect -f '{{.State.Health.Status}}' "$SCHED" 2>/dev/null)" = "healthy" ] && ok "scheduler healthy" || fail "scheduler не healthy"
check_mounts "$SCHED" \
  "/opt/airflow/dags=$RELEASE/dags" \
  "/opt/airflow/dags/.airflowignore=$RELEASE/deploy/transfermarkt/.airflowignore" \
  "/opt/airflow/scrapers=$RELEASE/scrapers" \
  "/opt/airflow/scripts=$RELEASE/scripts" \
  "/opt/airflow/configs=$RELEASE/configs" \
  "/opt/airflow/logs=$RUNTIME/logs"
# Только имена и несекретные значения: токены, пароли и ключи не печатаются.
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$SCHED" \
  | grep -E '^(TM_PROXY_CONTROL_URL|TM_BACKFILL_PROXY_CONTROL_URL|TRANSFERMARKT_RAW_STORE_URI|TRANSFERMARKT_REQUIRE_RAW_STORE|TM_NATIVE_V2_ENABLED|TM_STANDING_POLICY_ENABLED|TM_REQUIRE_METERED_PROXY|ALERT_ENV)=' || true
env_line() { docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$SCHED" | grep -E "^$1=" | head -1; }
[ "$(env_line TM_PROXY_CONTROL_URL)" = "TM_PROXY_CONTROL_URL=http://transfermarkt_gw:8899" ] && ok "TM_PROXY_CONTROL_URL → свой шлюз" || fail "TM_PROXY_CONTROL_URL не на transfermarkt_gw"
[ "$(env_line TRANSFERMARKT_REQUIRE_RAW_STORE)" = "TRANSFERMARKT_REQUIRE_RAW_STORE=true" ] && ok "raw-store обязателен" || fail "TRANSFERMARKT_REQUIRE_RAW_STORE не true"
if health=$(transfermarkt_gateway_health_ok "$SCHED"); then
  echo "  /health: $health"; ok "/health: transfermarkt-only без daily_*"
else
  echo "  /health: $health"; fail "/health не в ожидаемом виде"
fi

echo "== 3. Метабаза: import_error, 4 DAG, паузы =="
errs=$($PSQL "SELECT count(*) FROM import_error;")
[ "$errs" = 0 ] && ok "import_error = 0" || fail "import_error = '$errs'"
active=$($PSQL "SELECT count(*) FROM dag WHERE is_active;")
[ "$active" = 4 ] && ok "активных DAG ровно 4" || fail "активных DAG '$active' (ожидалось 4)"
$PSQL "SELECT dag_id, is_active, is_paused, has_import_errors, last_parsed_time FROM dag WHERE dag_id IN ($TM_DAGS_SQL) ORDER BY dag_id;"
for d in $TM_DAGS; do
  p=$($PSQL "SELECT is_paused FROM dag WHERE dag_id='$d';")
  case "$d" in
    dag_ingest_transfermarkt|dag_discover_transfermarkt_registry) want=f ;;
    *) want=t ;;
  esac
  [ "$p" = "$want" ] && ok "$d paused=$p" || fail "$d paused='$p' (ожидалось $want)"
done
running=$($PSQL "SELECT count(*) FROM dag_run WHERE state='running';")
echo "  running-прогонов: $running"

echo "== 4. Пулы =="
for p in ingest_scraper_pool transfermarkt_proxy transfermarkt_backfill_proxy transfermarkt_backfill_control; do
  s=$($PSQL "SELECT slots FROM slot_pool WHERE pool='$p';")
  [ "$s" = 1 ] && ok "$p = 1" || fail "$p = '$s' (ожидалось 1)"
done

echo "== 5. Состояние вне дерева =="
for d in "$RUNTIME/logs" "$RUNTIME/gateway-state"; do
  [ -d "$d" ] && [ ! -L "$d" ] && ok "$d есть" || fail "нет каталога $d"
  [ "$(stat -c %u "$d" 2>/dev/null)" = 50000 ] && ok "$d владелец uid 50000" || fail "$d владелец не uid 50000"
done
for sub in transfermarkt-approvals transfermarkt-registry transfermarkt-native-v2; do
  [ -d "$RUNTIME/logs/$sub" ] && ok "logs/$sub есть" || fail "нет logs/$sub"
done
timeout -k 5 30 docker exec "$SCHED" sh -c 't=/opt/airflow/logs/.postdeploy-write-probe; : > "$t" && rm -f "$t"' \
  && ok "logs пишутся uid 50000 изнутри планировщика" || fail "logs не пишутся изнутри планировщика"

echo "== 6. Автомат доставки =="
[ -x /usr/local/libexec/transfermarkt/auto_deliver.sh ] && ok "копия автомата установлена" || fail "нет /usr/local/libexec/transfermarkt/auto_deliver.sh"
crontab -l 2>/dev/null | grep -q "/usr/local/libexec/transfermarkt/auto_deliver.sh" && ok "cron автомата на месте" || fail "нет строки cron автомата"
crontab -l 2>/dev/null | grep -q "rotate_state.sh" && ok "cron ротации на месте" || fail "нет строки cron ротации"

echo
[ "$FAILS" = 0 ] && { echo "ИТОГ: всё сошлось"; exit 0; }
echo "ИТОГ: не сошлось $FAILS"
exit 1
