#!/bin/bash
# =============================================================================
# Superset bootstrap: DB upgrade + admin user + init + datasources + dashboards
# =============================================================================
# Mounted at /app/pythonpath/bootstrap.sh; intended to be invoked manually
# (e.g. via `docker compose exec superset bash /app/pythonpath/bootstrap.sh`)
# or from a Makefile target. Все шаги ИДЕМПОТЕНТНЫ.
#
# Шаги:
#   1) superset db upgrade
#   2) Создать admin (или пропустить, если уже есть)
#   3) superset init (роли + permissions)
#   4) Импорт datasources (через python implant; читает datasources.yaml)
#   5) (опционально) Импорт dashboards из ZIP-файлов в /app/pythonpath/dashboards/
# =============================================================================
set -euo pipefail

PYTHONPATH_DIR="/app/pythonpath"
DATASOURCES_YAML="${PYTHONPATH_DIR}/datasources.yaml"
IMPORT_SCRIPT="${PYTHONPATH_DIR}/import_datasources.py"
DASHBOARDS_DIR="${PYTHONPATH_DIR}/dashboards"

echo "[superset-bootstrap] (1/5) running superset db upgrade..."
superset db upgrade

# -----------------------------------------------------------------------------
# (2) admin user
# -----------------------------------------------------------------------------
if [ -n "${SUPERSET_ADMIN_PASSWORD:-}" ]; then
    echo "[superset-bootstrap] (2/5) ensuring admin user..."
    if superset fab list-users 2>/dev/null | grep -qE '^\s*admin\s'; then
        echo "[superset-bootstrap] admin user already exists; skipping create"
    else
        superset fab create-admin \
            --username admin \
            --firstname Admin \
            --lastname User \
            --email admin@example.com \
            --password "${SUPERSET_ADMIN_PASSWORD}" \
            || echo "[superset-bootstrap] create-admin failed (likely race / already exists) — continuing"
    fi
else
    echo "[superset-bootstrap] (2/5) SUPERSET_ADMIN_PASSWORD not set; skipping admin creation"
fi

# -----------------------------------------------------------------------------
# (3) init (roles + permissions)
# -----------------------------------------------------------------------------
echo "[superset-bootstrap] (3/5) running superset init..."
superset init

# -----------------------------------------------------------------------------
# (4) datasources (database connection + 10 datasets)
# -----------------------------------------------------------------------------
if [ -f "${DATASOURCES_YAML}" ] && [ -f "${IMPORT_SCRIPT}" ]; then
    echo "[superset-bootstrap] (4/5) importing datasources via python implant..."
    if [ -z "${TRINO_SUPERSET_PASSWORD:-}" ]; then
        echo "[superset-bootstrap] WARNING: TRINO_SUPERSET_PASSWORD is empty — Trino connection will fail until set"
    fi
    python "${IMPORT_SCRIPT}" "${DATASOURCES_YAML}"
    # (4b) роль analyst_data для SSO-аналитиков (фаза 7): database_access на
    # trino_iceberg. Идемпотентно; требует уже импортированную БД.
    echo "[superset-bootstrap] (4b) ensuring analyst_data role..."
    python "${PYTHONPATH_DIR}/create_analyst_role.py" \
        || echo "[superset-bootstrap] create_analyst_role failed — continuing"
else
    echo "[superset-bootstrap] (4/5) datasources.yaml or import_datasources.py not found; skipping"
fi

# -----------------------------------------------------------------------------
# (5) dashboards
#     a) Python declarative imports (configs/superset/dashboards/*.py)
#     b) Опциональные ZIP-экспорты из UI (configs/superset/dashboards/*.zip)
# -----------------------------------------------------------------------------
if [ -d "${DASHBOARDS_DIR}" ]; then
    # (5a) declarative Python dashboards via Superset SDK
    if [ -f "${DASHBOARDS_DIR}/import_dashboards.py" ]; then
        echo "[superset-bootstrap] (5/5) importing declarative dashboards..."
        ( cd "${DASHBOARDS_DIR}" && python import_dashboards.py ) \
            || echo "[superset-bootstrap] declarative dashboards import returned non-zero — continuing"
    else
        echo "[superset-bootstrap] (5/5) no import_dashboards.py in ${DASHBOARDS_DIR}; skipping declarative step"
    fi

    # (5b) optional ZIPs (e.g. UI-exported бэкап)
    shopt -s nullglob
    zips=("${DASHBOARDS_DIR}"/*.zip)
    shopt -u nullglob
    if [ "${#zips[@]}" -gt 0 ]; then
        echo "[superset-bootstrap] (5/5) importing ${#zips[@]} dashboard ZIP(s)..."
        for z in "${zips[@]}"; do
            echo "  - ${z}"
            superset import-dashboards -p "${z}" -u admin \
                || echo "    (import-dashboards reported non-zero; continuing)"
        done
    fi
else
    echo "[superset-bootstrap] (5/5) ${DASHBOARDS_DIR} does not exist; skipping dashboards"
fi

echo "[superset-bootstrap] done."
exit 0
