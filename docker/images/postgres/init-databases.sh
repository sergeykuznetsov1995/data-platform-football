#!/bin/bash
# =============================================================================
# Data Platform - PostgreSQL Initialization Script
# Creates databases and users for Airflow and Hive Metastore
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

echo "Creating Airflow database and user..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

echo "Creating Hive Metastore database and user..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER hive WITH PASSWORD '${HIVE_METASTORE_DB_PASSWORD}';
    CREATE DATABASE hive_metastore OWNER hive;
    GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO hive;
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

echo "Database initialization complete!"
echo "Created databases: airflow, hive_metastore"
echo "Created users: airflow, hive"
