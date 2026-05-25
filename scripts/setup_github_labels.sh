#!/usr/bin/env bash
# Sync labels from .github/labels.yml to the GitHub repo.
#
# Idempotent: re-run after editing labels.yml to update colors/descriptions.
# Existing labels with the same name are overwritten (--force).
# Labels in the repo but NOT in labels.yml are LEFT ALONE (no deletion).
#
# Usage:
#   bash scripts/setup_github_labels.sh                 # use repo from origin
#   bash scripts/setup_github_labels.sh owner/repo      # explicit
#   DRY_RUN=1 bash scripts/setup_github_labels.sh       # print, don't apply

set -euo pipefail

LABELS_FILE=".github/labels.yml"

if [[ ! -f "${LABELS_FILE}" ]]; then
  echo "ERROR: ${LABELS_FILE} not found. Run from repo root." >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: gh CLI not installed." >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 not installed." >&2
  exit 1
fi

REPO="${1:-}"
if [[ -z "${REPO}" ]]; then
  REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)
  if [[ -z "${REPO}" ]]; then
    echo "ERROR: cannot determine repo. Pass owner/repo as 1st arg or run 'gh auth login'." >&2
    exit 1
  fi
fi

echo "Target repo: ${REPO}"
echo "Source:      ${LABELS_FILE}"
echo

# Parse YAML → TSV (name<TAB>color<TAB>description), one label per line.
# Uses stdlib only — no PyYAML dependency (we don't require it on host).
parse_labels() {
  python3 - "${LABELS_FILE}" <<'PY'
import re, sys

path = sys.argv[1]
with open(path) as f:
    text = f.read()

# Find each "- name: ..." block and extract name/color/description.
blocks = re.split(r'^\s*-\s+name:\s*', text, flags=re.MULTILINE)[1:]
for block in blocks:
    lines = block.splitlines()
    name = lines[0].strip().strip('"').strip("'")
    color, desc = "", ""
    for line in lines[1:]:
        s = line.strip()
        if s.startswith("color:"):
            color = s.split(":", 1)[1].strip().strip('"').strip("'")
        elif s.startswith("description:"):
            desc = s.split(":", 1)[1].strip().strip('"').strip("'")
        elif s.startswith("- name:") or (s and not s.startswith("#") and ":" not in s):
            break
    if name and color:
        # TAB-separated; description may contain spaces but not tabs
        print(f"{name}\t{color}\t{desc}")
PY
}

count=0
errors=0
while IFS=$'\t' read -r name color desc; do
  [[ -z "${name}" ]] && continue
  count=$((count + 1))
  if [[ -n "${DRY_RUN:-}" ]]; then
    printf "  [dry-run] %-25s #%s  %s\n" "${name}" "${color}" "${desc}"
    continue
  fi
  if gh label create "${name}" \
        --repo "${REPO}" \
        --color "${color}" \
        --description "${desc}" \
        --force >/dev/null 2>&1; then
    printf "  ✓ %-25s #%s\n" "${name}" "${color}"
  else
    printf "  ✗ %-25s FAILED\n" "${name}" >&2
    errors=$((errors + 1))
  fi
done < <(parse_labels)

echo
echo "Processed: ${count} labels, errors: ${errors}"
[[ "${errors}" -eq 0 ]]
