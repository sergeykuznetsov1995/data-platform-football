#!/usr/bin/env python3
"""
Storage Initialization Script
==============================

Creates the Medallion schemas (bronze/silver/gold) in the Iceberg REST catalog
(Lakekeeper). Object-storage layout is managed automatically by Iceberg +
Lakekeeper under the warehouse bucket on SeaweedFS — no directory pre-creation
is needed (unlike the previous HDFS setup).

Prerequisite: the warehouse must already exist (see `make bootstrap-catalog`).

Usage:
    python init_storage.py

Or via Makefile:
    make init-storage
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SCHEMAS = ['bronze', 'silver', 'gold']


def init_schemas():
    """Create Medallion schemas (namespaces) in the Iceberg catalog via Trino."""
    from scrapers.base.trino_manager import TrinoTableManager, TrinoError

    logger.info("Initializing Iceberg schemas...")

    # Port/auth are resolved by TrinoTableManager from TRINO_PASSWORD/TRINO_PORT.
    manager = TrinoTableManager(host=os.environ.get('TRINO_HOST', 'trino'))

    for schema in SCHEMAS:
        try:
            manager.create_schema(schema)
            logger.info(f"Created schema: iceberg.{schema}")
        except TrinoError as e:
            logger.warning(f"Could not create schema {schema}: {e}")

    manager.close()
    logger.info("Iceberg schemas initialized.")


def verify_storage():
    """Verify schemas exist in the Iceberg catalog."""
    logger.info("Verifying storage setup...")

    try:
        from scrapers.base.trino_manager import TrinoTableManager

        manager = TrinoTableManager(host=os.environ.get('TRINO_HOST', 'trino'))
        for schema in SCHEMAS:
            exists = manager.schema_exists(schema)
            status = "OK" if exists else "MISSING"
            logger.info(f"Schema iceberg.{schema}: {status}")
        manager.close()
    except Exception as e:
        logger.error(f"Trino verification failed: {e}")


def main():
    """Main initialization routine."""
    logger.info("=" * 60)
    logger.info("Storage Initialization")
    logger.info("=" * 60)

    try:
        init_schemas()
    except Exception as e:
        logger.error(f"Schema initialization failed: {e}")

    try:
        verify_storage()
    except Exception as e:
        logger.error(f"Verification failed: {e}")

    logger.info("=" * 60)
    logger.info("Storage initialization complete.")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
