-- Create metastore database for Hive (PostgreSQL syntax)
-- Note: This runs only on first PostgreSQL initialization
CREATE DATABASE metastore;
GRANT ALL PRIVILEGES ON DATABASE metastore TO airflow;
