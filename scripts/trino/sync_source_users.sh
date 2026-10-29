#!/usr/bin/env bash
# #1937: render per-source Trino accounts (src_<source>) into password.db from
# TRINO_SRC_<SOURCE>_PASSWORD in .env. Idempotent: htpasswd replaces an existing
# user line, other users are untouched. password.db is gitignored, so this is
# the only reproducible way to (re)create the accounts on a fresh checkout.
# Trino's file authenticator re-reads password.db (file.refresh-period, 5s
# default) — no restart. Resource-group membership does need a restart.
#
# Strict: a missing or placeholder password is an error (exit 1) and nothing is
# written — all eight accounts exist or none change.
#
# Usage: scripts/trino/sync_source_users.sh <path/to/password.db> [path/to/.env]
set -euo pipefail

db="${1:?password.db path}"
env_file="${2:-$(dirname "$0")/../../.env}"
sources=(fbref fotmob sofascore whoscored transfermarkt espn understat clubelo)

# Read only our own variables: the shared .env holds values bash cannot source
# (unquoted brackets/URLs in OM_AUTH_PUBLIC_KEYS etc.), so never `. .env`.
while IFS= read -r line; do
  key="${line%%=*}"; val="${line#*=}"
  val="${val%\"}"; val="${val#\"}"
  export "$key=$val"
done < <(grep -E '^TRINO_SRC_[A-Z]+_PASSWORD=' "$env_file" || true)
command -v htpasswd >/dev/null || { echo "htpasswd (apache2-utils) is required" >&2; exit 1; }

missing=()
for src in "${sources[@]}"; do
  var="TRINO_SRC_$(echo "$src" | tr '[:lower:]' '[:upper:]')_PASSWORD"
  pw="${!var:-}"
  if [ -z "$pw" ] || [ "$pw" = "change-me" ]; then missing+=("$var"); fi
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "refusing to render: not set in $env_file: ${missing[*]}" >&2
  exit 1
fi

[ -f "$db" ] || touch "$db"
for src in "${sources[@]}"; do
  var="TRINO_SRC_$(echo "$src" | tr '[:lower:]' '[:upper:]')_PASSWORD"
  # -i: password from stdin (not argv), -B: bcrypt (Trino needs cost >= 8).
  printf '%s' "${!var}" | htpasswd -i -B -C 10 "$db" "src_$src" 2>/dev/null
  echo "ok src_$src"
done
