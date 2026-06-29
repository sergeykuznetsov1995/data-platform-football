#!/usr/bin/env python3
"""
Storage Initialization Script
==============================

Initializes HDFS directories and Hive schemas for the Medallion architecture.

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


def init_hdfs_directories():
    """Create Medallion architecture directories in HDFS."""
    from scrapers.base.hdfs_client import HDFSClient, HDFSError

    logger.info("Initializing HDFS directories...")

    client = HDFSClient(
        namenode_host=os.environ.get('HDFS_NAMENODE_HOST', 'namenode'),
        port=int(os.environ.get('HDFS_WEBHDFS_PORT', 9870)),
    )

    # Data sources for Bronze layer
    sources = [
        'fbref',
        'understat',
        'whoscored',
        'fotmob',
        'sofascore',
        'sofifa',
        'clubelo',
        'espn',
        'matchhistory',
    ]

    # Bronze layer directories
    for source in sources:
        path = f"/data/bronze/{source}"
        try:
            if not client.exists(path):
                client.mkdir(path)
                logger.info(f"Created: {path}")
            else:
                logger.debug(f"Exists: {path}")
        except HDFSError as e:
            logger.error(f"Failed to create {path}: {e}")

    # Silver layer directories
    silver_domains = ['matches', 'players', 'teams', 'standings', 'stats']
    for domain in silver_domains:
        path = f"/data/silver/{domain}"
        try:
            if not client.exists(path):
                client.mkdir(path)
                logger.info(f"Created: {path}")
            else:
                logger.debug(f"Exists: {path}")
        except HDFSError as e:
            logger.error(f"Failed to create {path}: {e}")

    # Gold layer directories
    gold_use_cases = ['analytics', 'ml', 'reports']
    for use_case in gold_use_cases:
        path = f"/data/gold/{use_case}"
        try:
            if not client.exists(path):
                client.mkdir(path)
                logger.info(f"Created: {path}")
            else:
                logger.debug(f"Exists: {path}")
        except HDFSError as e:
            logger.error(f"Failed to create {path}: {e}")

    logger.info("HDFS directories initialized.")
    client.close()


def init_hive_schemas():
    """Create Hive schemas via Trino."""
    from scrapers.base.trino_manager import TrinoTableManager, TrinoError

    logger.info("Initializing Hive schemas...")

    manager = TrinoTableManager(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8080)),
    )

    schemas = ['bronze', 'silver', 'gold']

    for schema in schemas:
        try:
            manager.create_schema(schema)
            logger.info(f"Created schema: hive.{schema}")
        except TrinoError as e:
            logger.warning(f"Could not create schema {schema}: {e}")

    manager.close()
    logger.info("Hive schemas initialized.")


def verify_storage():
    """Verify storage setup."""
    logger.info("Verifying storage setup...")

    # Verify HDFS
    try:
        from scrapers.base.hdfs_client import HDFSClient

        client = HDFSClient()
        dirs = client.list_dir('/data')
        logger.info(f"HDFS /data contents: {[d['name'] for d in dirs]}")
        client.close()
    except Exception as e:
        logger.error(f"HDFS verification failed: {e}")

    # Verify Trino schemas
    try:
        from scrapers.base.trino_manager import TrinoTableManager

        manager = TrinoTableManager()
        for schema in ['bronze', 'silver', 'gold']:
            exists = manager.schema_exists(schema)
            status = "OK" if exists else "MISSING"
            logger.info(f"Schema hive.{schema}: {status}")
        manager.close()
    except Exception as e:
        logger.error(f"Trino verification failed: {e}")


def main():
    """Main initialization routine."""
    logger.info("=" * 60)
    logger.info("Storage Initialization")
    logger.info("=" * 60)

    # Initialize HDFS directories
    try:
        init_hdfs_directories()
    except Exception as e:
        logger.error(f"HDFS initialization failed: {e}")
        logger.info("Continuing with Hive schema initialization...")

    # Initialize Hive schemas
    try:
        init_hive_schemas()
    except Exception as e:
        logger.error(f"Hive schema initialization failed: {e}")

    # Verify setup
    try:
        verify_storage()
    except Exception as e:
        logger.error(f"Verification failed: {e}")

    logger.info("=" * 60)
    logger.info("Storage initialization complete.")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
