#!/usr/bin/env bash
# #1937: render per-source Trino accounts (src_<source>) into password.db from
# TRINO_SRC_<SOURCE>_PASSWORD in .env. Idempotent: htpasswd replaces an existing
# user line, other users are untouched. password.db is gitignored, so this is
# the only reproducible way to (re)create the accounts on a fresh checkout.
# Trino's file authenticator re-reads password.db (file.refresh-period, 5s
# default) — no restart. Resource-group membership does need a restart.
#
# Usage: scripts/trino/sync_source_users.sh <path/to/password.db> [path/to/.env]
set -euo pipefail

db="${1:?password.db path}"
env_file="${2:-$(dirname "$0")/../../.env}"
sources=(fbref fotmob sofascore whoscored transfermarkt espn understat clubelo)

# shellcheck disable=SC1090
set -a; . "$env_file"; set +a
command -v htpasswd >/dev/null || { echo "htpasswd (apache2-utils) is required" >&2; exit 1; }
[ -f "$db" ] || touch "$db"

for src in "${sources[@]}"; do
  var="TRINO_SRC_$(echo "$src" | tr '[:lower:]' '[:upper:]')_PASSWORD"
  pw="${!var:-}"
  if [ -z "$pw" ] || [ "$pw" = "change-me" ]; then
    echo "skip src_$src: $var not set" >&2
    continue
  fi
  # -i: password from stdin (not argv), -B: bcrypt (Trino needs cost >= 8).
  printf '%s' "$pw" | htpasswd -i -B -C 10 "$db" "src_$src" 2>/dev/null
  echo "ok src_$src"
done
