#!/usr/bin/env bash
set -euo pipefail

# Durable, atomic campaign guard for issue #949.  The evidence directory must
# survive between attempts; deleting it intentionally resets the campaign and
# is therefore an operator action that must be recorded separately.
evidence_root="${1:?usage: claim_fbref_acceptance_attempt.sh EVIDENCE_DIR}"
mkdir -p -- "$evidence_root/attempts"
for attempt in 1 2 3; do
  marker="$evidence_root/attempts/attempt-$attempt"
  if mkdir -- "$marker" 2>/dev/null; then
    printf '%s\n' "$attempt"
    exit 0
  fi
done
echo "FBref acceptance campaign already used three paid attempts" >&2
exit 3
