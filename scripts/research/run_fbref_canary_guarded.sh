#!/usr/bin/env bash
set -euo pipefail

# One-page live FBref canary with a kernel-enforced total traffic ceiling.
# This wrapper intentionally needs host root: it installs quota rules inside
# only the short-lived canary container's network namespace.

if [[ "${RUN_LIVE_FBREF_CANARY:-}" != "1" ]]; then
  echo "Set RUN_LIVE_FBREF_CANARY=1 to authorize the bounded live canary." >&2
  exit 2
fi

readonly REQUEST_LIMIT="${FBREF_CANARY_REQUEST_LIMIT:-22}"
readonly BYTE_LIMIT_MB="${FBREF_CANARY_BYTE_LIMIT_MB:-25}"
readonly INGRESS_LIMIT_BYTES=$((18 * 1024 * 1024))
readonly EGRESS_LIMIT_BYTES=$((4 * 1024 * 1024))
readonly ENV_FILE="${FBREF_CANARY_ENV_FILE:-/root/data-platform-football/.env}"
readonly PROXY_FILE="${FBREF_CANARY_PROXY_FILE:-/root/data-platform-football/proxys.txt}"
readonly CONTAINER="fbref-canary-guard-${$}"
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
START_LOG=""
COMPOSE_PID=""

if (( REQUEST_LIMIT < 22 || REQUEST_LIMIT > 25 )); then
  echo "Request limit must stay between 22 and 25." >&2
  exit 2
fi
if (( BYTE_LIMIT_MB < 6 || BYTE_LIMIT_MB > 25 )); then
  echo "Byte limit must stay between 6 and 25 MiB." >&2
  exit 2
fi
if [[ ! -r "$ENV_FILE" || ! -f "$PROXY_FILE" || -L "$PROXY_FILE" ||
      ! -r "$PROXY_FILE" ]]; then
  echo "Canary env/proxy input is missing or unreadable." >&2
  exit 2
fi
START_LOG="$(mktemp)"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  if [[ -n "$COMPOSE_PID" ]] && kill -0 "$COMPOSE_PID" 2>/dev/null; then
    kill "$COMPOSE_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$COMPOSE_PID" ]]; then
    wait "$COMPOSE_PID" >/dev/null 2>&1 || true
  fi
  [[ -z "$START_LOG" ]] || rm -f "$START_LOG"
}
trap cleanup EXIT INT TERM

export PUBLIC_IP=127.0.0.1
export TS_IP=127.0.0.1
export TS_HOSTNAME=localhost

"$REPO_ROOT/scripts/compose.sh" --env-file "$ENV_FILE" run --rm --no-deps \
  --user 0:0 \
  --name "$CONTAINER" \
  --entrypoint sleep \
  airflow-scheduler 1200 >"$START_LOG" 2>&1 &
COMPOSE_PID=$!

# The wrapper stays in the foreground of its own background job, so it keeps
# the shared lifecycle lock until this short-lived container exits. Wait only
# for Docker to publish the inspected identity; never continue after an early
# wrapper failure.
container_ready=0
for ((_attempt = 1; _attempt <= 150; _attempt++)); do
  if docker inspect "$CONTAINER" >/dev/null 2>&1; then
    container_ready=1
    break
  fi
  if ! kill -0 "$COMPOSE_PID" 2>/dev/null; then
    set +e
    wait "$COMPOSE_PID"
    readonly START_STATUS=$?
    set -e
    COMPOSE_PID=""
    echo "Protected Compose failed to start the canary container (status ${START_STATUS})." >&2
    exit 1
  fi
  sleep 0.2
done
if ((container_ready == 0)); then
  echo "Timed out waiting for the protected canary container." >&2
  exit 1
fi

docker cp "$PROXY_FILE" "$CONTAINER:/tmp/fbref-canary-proxys.txt"
docker exec --user 0:0 "$CONTAINER" \
  chown 50000:0 /tmp/fbref-canary-proxys.txt
docker exec --user 0:0 "$CONTAINER" \
  chmod 0400 /tmp/fbref-canary-proxys.txt

readonly PID="$(docker inspect --format '{{.State.Pid}}' "$CONTAINER")"
if [[ ! "$PID" =~ ^[1-9][0-9]*$ ]]; then
  echo "Could not resolve the canary network namespace." >&2
  exit 1
fi

nsenter -t "$PID" -n iptables -N FBREF_CANARY_INPUT
nsenter -t "$PID" -n iptables -A FBREF_CANARY_INPUT \
  -m quota --quota "$INGRESS_LIMIT_BYTES" -j ACCEPT
nsenter -t "$PID" -n iptables -A FBREF_CANARY_INPUT -j REJECT
nsenter -t "$PID" -n iptables -I INPUT 1 -i lo -j ACCEPT
nsenter -t "$PID" -n iptables -I INPUT 2 -j FBREF_CANARY_INPUT

nsenter -t "$PID" -n iptables -N FBREF_CANARY_OUTPUT
nsenter -t "$PID" -n iptables -A FBREF_CANARY_OUTPUT \
  -m quota --quota "$EGRESS_LIMIT_BYTES" -j ACCEPT
nsenter -t "$PID" -n iptables -A FBREF_CANARY_OUTPUT -j REJECT
nsenter -t "$PID" -n iptables -I OUTPUT 1 -o lo -j ACCEPT
nsenter -t "$PID" -n iptables -I OUTPUT 2 -j FBREF_CANARY_OUTPUT

set +e
docker exec --user 50000:0 \
  -e PYTHONPATH=/opt/airflow \
  "$CONTAINER" \
  python /opt/airflow/scripts/research/run_fbref_canary.py \
    --proxy-file /tmp/fbref-canary-proxys.txt \
    --request-limit "$REQUEST_LIMIT" \
    --byte-limit-mb "$BYTE_LIMIT_MB"
readonly STATUS=$?
set -e

echo "Kernel quota counters (ingress <=18 MiB, egress <=4 MiB):" >&2
nsenter -t "$PID" -n iptables -L FBREF_CANARY_INPUT -n -v -x >&2
nsenter -t "$PID" -n iptables -L FBREF_CANARY_OUTPUT -n -v -x >&2
exit "$STATUS"
