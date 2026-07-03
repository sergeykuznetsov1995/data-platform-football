#!/bin/bash
# Создание базы keycloak в работающем postgres (идемпотентно, паттерн
# docker/images/postgres/init-databases.sh — тот скрипт выполняется только
# на свежем томе, поэтому для живого прода нужен этот).
# ВАЖНО: env-var KEYCLOAK_DB_PASSWORD сервису postgres в compose.yaml
# сознательно НЕ добавлена — это пересоздало бы контейнер postgres на проде.
set -euo pipefail
cd "$(dirname "$0")/.."

KEYCLOAK_DB_PASSWORD=$(grep '^KEYCLOAK_DB_PASSWORD=' .env | cut -d= -f2-)
if [ -z "${KEYCLOAK_DB_PASSWORD}" ] || [[ "${KEYCLOAK_DB_PASSWORD}" == \<* ]]; then
    echo "ERROR: KEYCLOAK_DB_PASSWORD не задан в .env" >&2
    exit 1
fi

docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'keycloak') THEN
            CREATE USER keycloak WITH PASSWORD '${KEYCLOAK_DB_PASSWORD}';
        END IF;
    END
    \$\$;
    SELECT 'CREATE DATABASE keycloak OWNER keycloak'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'keycloak')\gexec
    GRANT ALL PRIVILEGES ON DATABASE keycloak TO keycloak;
EOSQL

docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d keycloak' <<'EOSQL'
    GRANT ALL ON SCHEMA public TO keycloak;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO keycloak;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO keycloak;
EOSQL

echo "OK: база keycloak готова"
