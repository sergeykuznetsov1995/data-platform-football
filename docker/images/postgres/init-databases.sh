#!/bin/bash
# =============================================================================
# Data Platform - PostgreSQL Initialization Script
# Creates databases and users for Airflow, Hive Metastore, Superset, OpenMetadata
# Idempotent: safe to re-run; checks existence before creating users/databases.
# =============================================================================

set -e

# Validate required environment variables
if [ -z "$AIRFLOW_DB_PASSWORD" ]; then
    echo "ERROR: AIRFLOW_DB_PASSWORD environment variable is required"
    exit 1
fi

if [ -z "$HIVE_METASTORE_DB_PASSWORD" ]; then
    echo "ERROR: HIVE_METASTORE_DB_PASSWORD environment variable is required"
    exit 1
fi

if [ -z "$SUPERSET_DB_PASSWORD" ]; then
    echo "ERROR: SUPERSET_DB_PASSWORD environment variable is required"
    exit 1
fi

if [ -z "$OPENMETADATA_DB_PASSWORD" ]; then
    echo "ERROR: OPENMETADATA_DB_PASSWORD environment variable is required"
    exit 1
fi

echo "Creating Airflow database and user (idempotent)..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
            CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
        END IF;
    END
    \$\$;
    SELECT 'CREATE DATABASE airflow OWNER airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

echo "Creating Hive Metastore database and user (idempotent)..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'hive') THEN
            CREATE USER hive WITH PASSWORD '${HIVE_METASTORE_DB_PASSWORD}';
        END IF;
    END
    \$\$;
    SELECT 'CREATE DATABASE hive_metastore OWNER hive'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'hive_metastore')\gexec
    GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO hive;
EOSQL

echo "Creating Superset database and user (idempotent)..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'superset') THEN
            CREATE USER superset WITH PASSWORD '${SUPERSET_DB_PASSWORD}';
        END IF;
    END
    \$\$;
    SELECT 'CREATE DATABASE superset OWNER superset'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec
    GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
EOSQL

echo "Creating OpenMetadata database and user (idempotent)..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openmetadata') THEN
            CREATE USER openmetadata WITH PASSWORD '${OPENMETADATA_DB_PASSWORD}';
        END IF;
    END
    \$\$;
    SELECT 'CREATE DATABASE openmetadata OWNER openmetadata'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata')\gexec
    GRANT ALL PRIVILEGES ON DATABASE openmetadata TO openmetadata;
EOSQL

echo "Granting schema permissions for Airflow..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "airflow" <<-EOSQL
    GRANT ALL ON SCHEMA public TO airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;
EOSQL

echo "Granting schema permissions for Hive..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "hive_metastore" <<-EOSQL
    GRANT ALL ON SCHEMA public TO hive;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO hive;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO hive;
EOSQL

echo "Granting schema permissions for Superset..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "superset" <<-EOSQL
    GRANT ALL ON SCHEMA public TO superset;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO superset;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO superset;
EOSQL

echo "Granting schema permissions for OpenMetadata..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "openmetadata" <<-EOSQL
    GRANT ALL ON SCHEMA public TO openmetadata;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO openmetadata;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO openmetadata;
EOSQL

echo "Database initialization complete!"
echo "Created databases: airflow, hive_metastore, superset, openmetadata"
echo "Created users: airflow, hive, superset, openmetadata"
