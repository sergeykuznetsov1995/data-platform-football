#!/usr/bin/env python3
"""
Add player_id column to existing FBref Iceberg tables via Trino.

This migration script adds the player_id column to tables that were created
before the player_id extraction feature was implemented.

Note: For existing data, player_id will be NULL. New data scraped after
updating the scrapers will contain player_id values.

Usage:
    python scripts/migrate_add_player_id.py

    # Or with custom Trino connection:
    TRINO_HOST=localhost TRINO_PORT=8080 python scripts/migrate_add_player_id.py
"""

import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Tables that need player_id column added
PLAYER_TABLES = [
    'fbref_player_standard',
    'fbref_player_shooting',
    'fbref_player_passing',
    'fbref_player_passing_types',
    'fbref_player_gca',
    'fbref_player_defense',
    'fbref_player_possession',
    'fbref_player_playingtime',
    'fbref_player_misc',
]

KEEPER_TABLES = [
    'fbref_keeper_stats',
    'fbref_keeper_adv',
]

ALL_TABLES = PLAYER_TABLES + KEEPER_TABLES


def get_trino_connection():
    """Create Trino database connection."""
    try:
        import trino
    except ImportError:
        logger.error(
            "trino package not installed. Install with: pip install trino"
        )
        sys.exit(1)

    host = os.environ.get('TRINO_HOST', 'trino')
    port = int(os.environ.get('TRINO_PORT', '8080'))
    user = os.environ.get('TRINO_USER', 'admin')
    catalog = os.environ.get('TRINO_CATALOG', 'iceberg')
    schema = os.environ.get('TRINO_SCHEMA', 'bronze')

    logger.info(
        f"Connecting to Trino at {host}:{port} "
        f"(catalog={catalog}, schema={schema})"
    )

    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
    )
    return conn


def table_exists(cursor, table_name: str) -> bool:
    """Check if table exists in the schema."""
    try:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchall()
        return len(result) > 0
    except Exception as e:
        logger.warning(f"Error checking table existence for {table_name}: {e}")
        return False


def column_exists(cursor, table_name: str, column_name: str) -> bool:
    """Check if column exists in the table."""
    try:
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        for col in columns:
            if col[0].lower() == column_name.lower():
                return True
        return False
    except Exception as e:
        logger.warning(f"Error checking column existence for {table_name}.{column_name}: {e}")
        return False


def add_player_id_column(cursor, table_name: str) -> bool:
    """Add player_id column to table if it doesn't exist."""
    if not table_exists(cursor, table_name):
        logger.info(f"Table {table_name} does not exist, skipping")
        return False

    if column_exists(cursor, table_name, 'player_id'):
        logger.info(f"Column player_id already exists in {table_name}, skipping")
        return False

    try:
        sql = f"ALTER TABLE {table_name} ADD COLUMN player_id VARCHAR"
        logger.info(f"Executing: {sql}")
        cursor.execute(sql)
        logger.info(f"Successfully added player_id to {table_name}")
        return True
    except Exception as e:
        logger.error(f"Error adding player_id to {table_name}: {e}")
        return False


def main():
    """Main migration function."""
    logger.info("Starting player_id migration for FBref Iceberg tables")

    conn = get_trino_connection()
    cursor = conn.cursor()

    success_count = 0
    skip_count = 0
    error_count = 0

    for table in ALL_TABLES:
        try:
            result = add_player_id_column(cursor, table)
            if result:
                success_count += 1
            else:
                skip_count += 1
        except Exception as e:
            logger.error(f"Unexpected error processing {table}: {e}")
            error_count += 1

    cursor.close()
    conn.close()

    logger.info(
        f"Migration complete: "
        f"{success_count} tables updated, "
        f"{skip_count} tables skipped, "
        f"{error_count} errors"
    )

    if error_count > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
