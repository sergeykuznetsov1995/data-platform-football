#!/usr/bin/env bash
# Канарейка ротации: отдельный шлюз из НОВОГО дерева (без боевого DNS-алиаса),
# сбор холодных сэмплов по всем классам манифеста, verify → VERIFIED.
# Бой (sofascore_gw_951) не останавливается.
# Использование: bash deploy/sofascore/run_canary.sh <release-root>
# Переменные — из $SOFASCORE_ENV_FILE: SOFASCORE_RUNTIME_DIR, SOFASCORE_GATEWAY_IMAGE,
#   SOFASCORE_CANARY_COLLECTOR_IMAGE, SOFASCORE_GATEWAY_FALLBACK_PROXY_FILE,
#   SOFASCORE_PROXY_POOL_JSON (тот же пул, что у боевого шлюза), SOFASCORE_HOST_PYTHON.
set -uo pipefail

RELEASE="${1:?путь к замороженному дереву}"
ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
# shellcheck source=deploy/sofascore/env.sh
. "$(dirname "$0")/env.sh"
sofascore_load_env "$ENV_FILE"
: "${SOFASCORE_RUNTIME_DIR:?}" "${SOFASCORE_GATEWAY_IMAGE:?}" "${SOFASCORE_CANARY_COLLECTOR_IMAGE:?}" \
  "${SOFASCORE_GATEWAY_FALLBACK_PROXY_FILE:?}" "${SOFASCORE_PROXY_POOL_JSON:?}" "${SOFASCORE_HOST_PYTHON:?}"

TAG=$(basename "$RELEASE" | sed 's/^release-//')
WORKSPACE="$SOFASCORE_RUNTIME_DIR/canary-$TAG"
STATE="$WORKSPACE/gateway-state"
ARTIFACT="$WORKSPACE/candidate.json"
LOG="$WORKSPACE/runner.log"
STATUS="$WORKSPACE/status.json"
TOKEN_FILE="$WORKSPACE/control.token"
GATEWAY="sofascore_canary_gw_$TAG"
COLLECTOR="sofascore_canary_collector_$TAG"
CAP=47930410
TEMPLATE="$RELEASE/configs/sofascore/proxy_budget_canary.json"

mkdir -p "$WORKSPACE" "$STATE"
chmod 0700 "$WORKSPACE"
if [ ! -s "$TOKEN_FILE" ]; then
  umask 0077
  openssl rand -hex 24 > "$TOKEN_FILE"
fi
TOKEN=$(<"$TOKEN_FILE")
# Канарейка проверяет ровно тот пул, который затем уйдёт в бой (gateway.compose.yaml
# читает ту же SOFASCORE_PROXY_POOL_JSON); значение уходит контейнеру только через
# окружение, в лог и argv не попадает.
export PROXY_POOL_JSON="$SOFASCORE_PROXY_POOL_JSON"
ARTIFACT_ID=$(sha256sum "$TEMPLATE" | awk '{print $1}')

cleanup() {
  docker rm -f "$COLLECTOR" >/dev/null 2>&1 || true
  docker rm -f "$GATEWAY" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker run -d --name "$GATEWAY" \
  --network sofascore-net \
  --memory 512m \
  -u 0:0 \
  -e PROXY_POOL_JSON \
  -e PYTHONPATH=/opt/sofascore-repo \
  -e PROXY_FILTER_CONTROL_TOKEN="$TOKEN" \
  -e SOFASCORE_PROXY_BUDGET_ARTIFACT_ID="$ARTIFACT_ID" \
  -e PROXY_FILTER_ALLOW_FILE_FALLBACK=true \
  -v "$RELEASE:/opt/sofascore-repo:ro" \
  -v "$SOFASCORE_GATEWAY_FALLBACK_PROXY_FILE:/opt/airflow/proxys.txt:ro" \
  -v "$TEMPLATE:/opt/airflow/runtime/sofascore/proxy_budget_canary.json:ro" \
  -v "$STATE:/opt/airflow/logs/sofascore_proxy_filter" \
  "$SOFASCORE_GATEWAY_IMAGE" \
  python /opt/sofascore-repo/scripts/proxy_filter/filter_proxy.py \
    --listen 0.0.0.0:8899 \
    --lease-listen 0.0.0.0:8900 \
    --lease-proxy-url "http://$GATEWAY:8900" \
    --proxy-file /opt/airflow/proxys.txt \
    --allow-proxy-file-fallback \
    --blocklist /opt/sofascore-repo/configs/proxy_filter/blocklist.txt \
    --out /opt/airflow/logs/sofascore_proxy_filter/bytes.json \
    --daily-budget-mb 300 \
    --max-lease-mb 46 \
    --max-lease-ttl-seconds 3600 \
    --dagrun-budget-bytes 8000000 \
    --url-budget-bytes 2000000 \
    --max-active-leases 1 \
    --sofascore-budget-artifact /opt/airflow/runtime/sofascore/proxy_budget_canary.json \
    --sofascore-canary-hard-cap-bytes "$CAP" \
    --ledger /opt/airflow/logs/sofascore_proxy_filter/paid_requests.jsonl \
    --sofascore-allocation-ledger /opt/airflow/logs/sofascore_proxy_filter/sofascore_allocations.json \
    --sofascore-allocation-wal /opt/airflow/logs/sofascore_proxy_filter/sofascore_allocation_claims.jsonl \
    --sofascore-parent-envelope /opt/airflow/logs/sofascore_proxy_filter/sofascore_parent_envelopes.json \
  >> "$LOG" 2>&1

for _ in $(seq 1 24); do
  if docker exec "$GATEWAY" python -c \
    "import socket; [socket.create_connection(('127.0.0.1', p), 2).close() for p in (8899, 8900)]" \
    >/dev/null 2>&1; then
    break
  fi
  sleep 5
done
docker exec "$GATEWAY" python -c \
  "import socket; [socket.create_connection(('127.0.0.1', p), 2).close() for p in (8899, 8900)]" || exit 6

docker create --name "$COLLECTOR" \
  --network sofascore-net \
  -u 0:0 \
  --ulimit nofile=65536:65536 \
  -e PYTHONPATH=/opt/airflow/repo \
  -e HOME=/root \
  -e SOFASCORE_PROXY_CONTROL_URL="http://$GATEWAY:8899" \
  -e SOFASCORE_PROXY_CONTROL_TOKEN="$TOKEN" \
  -v "$RELEASE:/opt/airflow/repo:ro" \
  -v "$WORKSPACE:/workspace" \
  -w /opt/airflow/repo \
  "$SOFASCORE_CANARY_COLLECTOR_IMAGE" \
  python scripts/research/bench_sofascore_paid_canary.py collect \
    --experimental-cap-bytes "$CAP" \
    --target-cold-runs 20 \
    --workspace /workspace/collect \
    --artifact /workspace/candidate.json \
  >> "$LOG" 2>&1

attempt=0
while [ "$attempt" -lt 400 ]; do
  attempt=$((attempt + 1))
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] attempt=$attempt" >> "$LOG"
  docker start -a "$COLLECTOR" >> "$LOG" 2>&1
  rc=$?
  python3 - "$ARTIFACT" "$STATUS" "$attempt" "$rc" <<'PY'
import json, os, sys
artifact, status, attempt, rc = sys.argv[1:]
payload = json.load(open(artifact)) if os.path.exists(artifact) else {}
classes = payload.get("workload_classes") or {}
rows = {}
for name, value in classes.items():
    cold = value.get("samples") or []
    rows[name] = {
        "cold": len(cold),
        "exits": len({row.get("proxy_exit_hash") for row in cold}),
    }
out = {"attempt": int(attempt), "last_rc": int(rc), "classes": rows}
with open(status, "w") as handle:
    json.dump(out, handle, indent=2, sort_keys=True)
PY
  if [ "$rc" -eq 0 ]; then
    if PYTHONPATH="$RELEASE" "$SOFASCORE_HOST_PYTHON" \
      "$RELEASE/scripts/research/bench_sofascore_paid_canary.py" verify \
      --artifact "$ARTIFACT" >> "$LOG" 2>&1; then
      touch "$WORKSPACE/VERIFIED"
      echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] verified" >> "$LOG"
      exit 0
    fi
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] verify failed" >> "$LOG"
    exit 5
  fi
  if tail -n 120 "$LOG" | grep -qiE "daily paid-proxy budget|budget exhausted|exceeded the cap|budget_exceeded"; then
    echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] stopped: budget" >> "$LOG"
    exit 2
  fi
  docker restart "$GATEWAY" >/dev/null 2>&1 || exit 3
  sleep 20
done
exit 1
