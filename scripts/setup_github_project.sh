#!/usr/bin/env bash
# Bootstrap GitHub Project v2 "Data Platform" with custom fields.
#
# Idempotent: re-running detects an existing project by title and only adds
# missing fields. Existing field options are left as-is (gh project field-edit
# is fragile, so we don't auto-reconcile options — edit manually if needed).
#
# Usage:
#   bash scripts/setup_github_project.sh                # owner = current gh user
#   bash scripts/setup_github_project.sh some-owner     # explicit owner (user or org)
#
# Requires gh auth scopes: project, repo
#   gh auth login --web --scopes "project,repo"

set -euo pipefail

TITLE="Data Platform"

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: gh CLI not installed." >&2
  exit 1
fi

OWNER="${1:-}"
if [[ -z "${OWNER}" ]]; then
  OWNER=$(gh api user -q .login 2>/dev/null || true)
  if [[ -z "${OWNER}" ]]; then
    echo "ERROR: cannot resolve owner. Pass as 1st arg or run 'gh auth login'." >&2
    exit 1
  fi
fi

echo "Owner: ${OWNER}"
echo "Title: ${TITLE}"
echo

# 1) Find or create project ─────────────────────────────────────────────────
EXISTING_NUM=$(gh project list --owner "${OWNER}" --format json \
                 --jq ".projects[] | select(.title==\"${TITLE}\") | .number" 2>/dev/null \
                 | head -1)

if [[ -n "${EXISTING_NUM}" ]]; then
  PROJECT_NUM="${EXISTING_NUM}"
  echo "Found existing project #${PROJECT_NUM}"
else
  echo "Creating project '${TITLE}'..."
  PROJECT_NUM=$(gh project create --owner "${OWNER}" --title "${TITLE}" --format json \
                  --jq .number)
  echo "Created project #${PROJECT_NUM}"
fi

PROJECT_URL=$(gh project view "${PROJECT_NUM}" --owner "${OWNER}" --format json \
                --jq .url 2>/dev/null || echo "")

# 2) Helper: create field only if missing ───────────────────────────────────
field_exists() {
  local fname="$1"
  gh project field-list "${PROJECT_NUM}" --owner "${OWNER}" --format json \
    --jq ".fields[] | select(.name==\"${fname}\") | .id" 2>/dev/null | head -1
}

create_field_if_missing() {
  local fname="$1"
  local options="$2"
  if [[ -n "$(field_exists "${fname}")" ]]; then
    printf "  = %-10s exists\n" "${fname}"
    return
  fi
  if gh project field-create "${PROJECT_NUM}" \
       --owner "${OWNER}" \
       --name "${fname}" \
       --data-type SINGLE_SELECT \
       --single-select-options "${options}" >/dev/null 2>&1; then
    printf "  + %-10s created (%s)\n" "${fname}" "${options}"
  else
    printf "  ✗ %-10s FAILED\n" "${fname}" >&2
  fi
}

echo
echo "Fields:"
create_field_if_missing "Wave"     "E0,E1,E2,E3,E4,E5,E7"
create_field_if_missing "Priority" "P0,P1,P2,P3"
# Note: 'Status' is a built-in field on every Project v2 (Todo/In Progress/Done by default).
# Extend via UI: Project → ⋯ → Settings → Fields → Status → add 'Backlog', 'In Review', 'Blocked'.

echo
echo "Done."
echo "Project URL: ${PROJECT_URL:-<re-run after auth>}"
echo
echo "Next: create views in UI (CLI cannot create Project v2 views yet)."
echo "See .github/PROJECT_README.md for the recommended view set."
