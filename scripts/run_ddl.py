#!/usr/bin/env python3
"""
Execute Trino DDL to create all_leagues schema and tables
"""

from trino.dbapi import connect
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Trino connection
conn = connect(
    host='localhost',
    port=8085,
    user='trino',
    catalog='hive',
    schema='default'
)

# Read DDL file
ddl_file = '/root/data_platform/sql/create_tables.sql'
logger.info(f"📖 Reading DDL from {ddl_file}\n")

with open(ddl_file, 'r') as f:
    sql_content = f.read()

# Remove SQL comments (-- style)
sql_content = re.sub(r'--[^\n]*\n', '\n', sql_content)

# Split by semicolon but keep multi-line statements together
raw_statements = sql_content.split(';')
statements = []

for stmt in raw_statements:
    stmt = stmt.strip()
    if stmt and not stmt.startswith('--'):
        statements.append(stmt)

cursor = conn.cursor()
executed = 0
skipped = 0

for i, statement in enumerate(statements, 1):
    # Skip empty or comment-only statements
    if not statement or len(statement) < 10:
        continue

    # Extract statement type for logging
    stmt_type = statement.split()[0].upper() if statement else "UNKNOWN"

    logger.info(f"{'='*80}")
    logger.info(f"📝 Statement {i}/{len(statements)}: {stmt_type}")

    # Show first 150 chars
    preview = statement[:150].replace('\n', ' ')
    logger.info(f"   {preview}...")

    try:
        cursor.execute(statement)
        executed += 1
        logger.info(f"   ✅ Success\n")
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            skipped += 1
            logger.info(f"   ⚠️  Already exists - skipped\n")
        elif "not found" in error_msg.lower() and "CALL" in statement:
            skipped += 1
            logger.info(f"   ⚠️  Table not found yet - skipping partition sync (run after data upload)\n")
        else:
            logger.error(f"   ❌ Error: {error_msg}\n")
            raise

logger.info(f"{'='*80}")
logger.info(f"✅ DDL execution completed!")
logger.info(f"   Executed: {executed}")
logger.info(f"   Skipped: {skipped}")
logger.info(f"{'='*80}\n")

# Verify tables created
logger.info("🔍 Verifying schema and tables...")
cursor.execute("SHOW SCHEMAS IN hive")
schemas = cursor.fetchall()
logger.info(f"   Schemas: {', '.join([s[0] for s in schemas])}")

if ('all_leagues',) in schemas:
    cursor.execute("SHOW TABLES IN hive.all_leagues")
    tables = cursor.fetchall()
    logger.info(f"   Tables in all_leagues: {', '.join([t[0] for t in tables]) if tables else 'None yet'}")
else:
    logger.warning("   ⚠️  Schema 'all_leagues' not found!")

logger.info(f"\n{'='*80}")
logger.info("✨ Ready! You can now run the DAG to upload data:")
logger.info("   airflow dags trigger hdfs_data_pipeline")
logger.info(f"{'='*80}")

cursor.close()
conn.close()
