#!/usr/bin/env bash
set -euo pipefail

# This runs before the inherited Airflow entrypoint and before proxy imports.
# It validates both operator-declared provenance and every baked runtime file.
if [[ -z "${FBREF_CONTROL_DB_URI:-}" ]]; then
  echo "FBREF_CONTROL_DB_URI must point at the production FBref control DB" >&2
  exit 2
fi
if [[ "${FBREF_CONTROL_DB_URI}" == *"fbref_acceptance_postgres"* ]]; then
  echo "FBREF_CONTROL_DB_URI must not use the isolated Airflow metadata DB" >&2
  exit 2
fi
python /opt/airflow/scripts/verify_fbref_acceptance_image.py

exec /entrypoint "$@"
