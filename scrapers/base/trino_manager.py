"""
Trino Table Manager
===================

Manages Iceberg tables via Trino SQL.
Creates schemas, tables, and inserts data directly into Iceberg.

Storage Pipeline:
    DataFrame → Trino INSERT INTO iceberg.{schema}.{table}
"""

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)

# Trino client import with graceful fallback
try:
    import trino
    TRINO_AVAILABLE = True
except ImportError:
    TRINO_AVAILABLE = False
    logger.warning("trino package not installed. TrinoTableManager will not work.")


class TrinoTableManager:
    """
    Manages Iceberg tables via Trino SQL.

    Usage:
        manager = TrinoTableManager()

        # Create schema
        manager.create_schema('bronze')

        # Create Iceberg table
        manager.create_iceberg_table(
            schema='bronze',
            table='fbref_schedule',
            columns={'date': 'DATE', 'home': 'VARCHAR', 'away': 'VARCHAR'},
            partition_columns=['league', 'season']
        )

        # Insert DataFrame
        manager.insert_dataframe('bronze', 'fbref_schedule', df)

        # Check if table exists
        if manager.table_exists('bronze', 'fbref_schedule'):
            ...
    """

    def __init__(
        self,
        host: str = 'trino',
        port: int = 8080,
        user: str = 'airflow',
        catalog: str = 'iceberg',
    ):
        """
        Initialize Trino Table Manager.

        Args:
            host: Trino coordinator hostname
            port: Trino coordinator port
            user: Trino user
            catalog: Default catalog (iceberg)
        """
        if not TRINO_AVAILABLE:
            raise RuntimeError("trino package not installed. Run: pip install trino")

        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self._conn = None

    @property
    def connection(self):
        """Get or create Trino connection."""
        if self._conn is None:
            self._conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
            )
        return self._conn

    def _execute(self, sql: str, fetch: bool = False) -> Optional[List[Any]]:
        """Execute SQL statement."""
        cursor = self.connection.cursor()
        try:
            logger.debug(f"Executing SQL: {sql}")
            cursor.execute(sql)

            if fetch:
                return cursor.fetchall()
            return None

        except Exception as e:
            logger.error(f"SQL execution error: {e}\nSQL: {sql}")
            raise TrinoError(f"SQL execution failed: {e}") from e

        finally:
            cursor.close()

    def create_schema(self, schema: str) -> None:
        """
        Create schema if it doesn't exist.

        Args:
            schema: Schema name (e.g., 'bronze', 'silver', 'gold')
        """
        sql = f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{schema}"

        try:
            self._execute(sql)
            logger.info(f"Created schema: {self.catalog}.{schema}")
        except TrinoError:
            # Schema might already exist
            logger.debug(f"Schema {schema} may already exist")

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists."""
        sql = f"SHOW SCHEMAS FROM {self.catalog} LIKE '{schema}'"
        result = self._execute(sql, fetch=True)
        return len(result) > 0

    def table_exists(self, schema: str, table: str) -> bool:
        """
        Check if table exists.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            True if table exists
        """
        sql = f"SHOW TABLES FROM {self.catalog}.{schema} LIKE '{table}'"

        try:
            result = self._execute(sql, fetch=True)
            return len(result) > 0
        except TrinoError:
            return False

    def create_external_table(
        self,
        schema: str,
        table: str,
        columns: Dict[str, str],
        location: str,
        partition_columns: Optional[Dict[str, str]] = None,
        file_format: str = 'PARQUET',
        if_not_exists: bool = True,
    ) -> None:
        """
        Create external Hive table.

        Args:
            schema: Schema name
            table: Table name
            columns: Dict of column_name -> trino_type
            location: HDFS path (e.g., 'hdfs://namenode:9000/data/bronze/fbref/schedule')
            partition_columns: Optional dict of partition columns
            file_format: File format (PARQUET, ORC, etc.)
            if_not_exists: Add IF NOT EXISTS clause
        """
        # Ensure schema exists
        self.create_schema(schema)

        # Build column definitions (quote names to handle reserved keywords like 'from', 'to')
        col_defs = [f'    "{name}" {dtype}' for name, dtype in columns.items()]

        # Add partition columns to column list (but not to PARTITIONED BY)
        if partition_columns:
            for name, dtype in partition_columns.items():
                if name not in columns:
                    col_defs.append(f'    "{name}" {dtype}')

        columns_sql = ",\n".join(col_defs)

        # Build CREATE TABLE statement
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        sql = f"""CREATE TABLE {exists_clause}{self.catalog}.{schema}.{table} (
{columns_sql}
)
WITH (
    format = '{file_format}',
    external_location = '{location}'
)"""

        self._execute(sql)
        logger.info(f"Created external table: {self.catalog}.{schema}.{table} at {location}")

    def create_iceberg_table(
        self,
        schema: str,
        table: str,
        columns: Dict[str, str],
        partition_columns: Optional[List[str]] = None,
        if_not_exists: bool = True,
    ) -> None:
        """
        Create native Iceberg table.

        Args:
            schema: Schema name (e.g., 'bronze', 'silver', 'gold')
            table: Table name
            columns: Dict of column_name -> trino_type
            partition_columns: Optional list of columns for partitioning
            if_not_exists: Add IF NOT EXISTS clause
        """
        # Ensure schema exists
        self.create_schema(schema)

        # Build column definitions
        col_defs = [f'    "{name}" {dtype}' for name, dtype in columns.items()]
        columns_sql = ",\n".join(col_defs)

        exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        # Iceberg partitioning syntax
        partition_clause = ""
        if partition_columns:
            cols = ", ".join(f"'{c}'" for c in partition_columns)
            partition_clause = f"\nWITH (partitioning = ARRAY[{cols}])"

        sql = f"""CREATE TABLE {exists_clause}{self.catalog}.{schema}.{table} (
{columns_sql}
){partition_clause}"""

        self._execute(sql)
        logger.info(f"Created Iceberg table: {self.catalog}.{schema}.{table}")

    def insert_dataframe(
        self,
        schema: str,
        table: str,
        df: pd.DataFrame,
        batch_size: int = 1000,
    ) -> int:
        """
        Insert DataFrame rows into Iceberg table via VALUES clause.

        Args:
            schema: Schema name
            table: Table name
            df: Pandas DataFrame to insert
            batch_size: Number of rows per INSERT statement (default 1000)

        Returns:
            Number of rows inserted
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping insert to {schema}.{table}")
            return 0

        total_inserted = 0
        columns = ', '.join(f'"{c}"' for c in df.columns)

        # Process in batches to avoid SQL size limits
        for batch_start in range(0, len(df), batch_size):
            batch_df = df.iloc[batch_start:batch_start + batch_size]
            rows = []

            for _, row in batch_df.iterrows():
                values = []
                for col in df.columns:
                    val = row[col]
                    if pd.isna(val):
                        values.append("NULL")
                    elif isinstance(val, str):
                        # Escape single quotes
                        escaped = val.replace("'", "''")
                        values.append(f"'{escaped}'")
                    elif isinstance(val, date) and not isinstance(val, datetime):
                        # Date without time - use DATE literal
                        values.append(f"DATE '{val}'")
                    elif isinstance(val, (datetime, pd.Timestamp)):
                        # Handle timezone-aware timestamps
                        if hasattr(val, 'tzinfo') and val.tzinfo is not None:
                            # Convert to UTC and format
                            ts_str = val.strftime('%Y-%m-%d %H:%M:%S.%f')
                            values.append(f"TIMESTAMP '{ts_str}'")
                        else:
                            ts_str = val.strftime('%Y-%m-%d %H:%M:%S.%f')
                            values.append(f"TIMESTAMP '{ts_str}'")
                    elif isinstance(val, bool):
                        # Must check bool before int (bool is subclass of int)
                        values.append("TRUE" if val else "FALSE")
                    elif isinstance(val, (int, np.integer)):
                        # Cast to BIGINT for consistency with table schema
                        values.append(f"CAST({val} AS BIGINT)")
                    elif isinstance(val, (float, np.floating)):
                        if np.isnan(val) or np.isinf(val):
                            values.append("NULL")
                        else:
                            values.append(f"CAST({val} AS DOUBLE)")
                    else:
                        # Fallback: convert to string
                        escaped = str(val).replace("'", "''")
                        values.append(f"'{escaped}'")
                rows.append(f"({', '.join(values)})")

            values_sql = ",\n".join(rows)

            sql = f"""INSERT INTO {self.catalog}.{schema}.{table} ({columns})
VALUES
{values_sql}"""

            self._execute(sql)
            total_inserted += len(batch_df)
            logger.debug(
                f"Inserted batch {batch_start // batch_size + 1}: "
                f"{len(batch_df)} rows into {self.catalog}.{schema}.{table}"
            )

        logger.info(f"Inserted {total_inserted} rows into {self.catalog}.{schema}.{table}")
        return total_inserted

    def drop_table(self, schema: str, table: str, if_exists: bool = True) -> None:
        """
        Drop table.

        Args:
            schema: Schema name
            table: Table name
            if_exists: Add IF EXISTS clause
        """
        exists_clause = "IF EXISTS " if if_exists else ""
        sql = f"DROP TABLE {exists_clause}{self.catalog}.{schema}.{table}"

        self._execute(sql)
        logger.info(f"Dropped table: {self.catalog}.{schema}.{table}")

    def sync_partitions(self, schema: str, table: str) -> None:
        """
        Sync partitions from storage (calls Hive MSCK REPAIR).

        Note: Trino doesn't have direct MSCK REPAIR, but we can use
        CALL system.sync_partition_metadata for Hive tables.

        Args:
            schema: Schema name
            table: Table name
        """
        # For Hive connector, we use sync_partition_metadata procedure
        sql = f"""CALL {self.catalog}.system.sync_partition_metadata(
    schema_name => '{schema}',
    table_name => '{table}',
    mode => 'ADD'
)"""

        try:
            self._execute(sql)
            logger.info(f"Synced partitions for {self.catalog}.{schema}.{table}")
        except TrinoError as e:
            # Procedure might not exist or table not partitioned
            logger.warning(f"Could not sync partitions: {e}")

    def get_table_columns(self, schema: str, table: str) -> Dict[str, str]:
        """
        Get table columns and types.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Dict of column_name -> type
        """
        sql = f"DESCRIBE {self.catalog}.{schema}.{table}"
        result = self._execute(sql, fetch=True)

        columns = {}
        for row in result:
            # DESCRIBE returns (column_name, data_type, extra, comment)
            col_name = row[0]
            col_type = row[1]
            columns[col_name] = col_type

        return columns

    def execute_query(self, sql: str) -> List[Any]:
        """
        Execute arbitrary SQL query.

        Args:
            sql: SQL query

        Returns:
            Query results
        """
        return self._execute(sql, fetch=True)

    def arrow_schema_to_trino(self, arrow_schema: pa.Schema) -> Dict[str, str]:
        """
        Convert PyArrow schema to Trino column types.

        Args:
            arrow_schema: PyArrow Schema

        Returns:
            Dict of column_name -> trino_type
        """
        type_map = {
            pa.int8(): 'TINYINT',
            pa.int16(): 'SMALLINT',
            pa.int32(): 'INTEGER',
            pa.int64(): 'BIGINT',
            pa.uint8(): 'SMALLINT',
            pa.uint16(): 'INTEGER',
            pa.uint32(): 'BIGINT',
            pa.uint64(): 'BIGINT',  # Trino doesn't have unsigned
            pa.float16(): 'REAL',
            pa.float32(): 'REAL',
            pa.float64(): 'DOUBLE',
            pa.bool_(): 'BOOLEAN',
            pa.string(): 'VARCHAR',
            pa.large_string(): 'VARCHAR',
            pa.utf8(): 'VARCHAR',
            pa.large_utf8(): 'VARCHAR',
            pa.binary(): 'VARBINARY',
            pa.large_binary(): 'VARBINARY',
            pa.date32(): 'DATE',
            pa.date64(): 'DATE',
        }

        columns = {}

        for field in arrow_schema:
            name = field.name
            arrow_type = field.type

            # Direct type mapping
            if arrow_type in type_map:
                columns[name] = type_map[arrow_type]

            # Timestamp types
            elif pa.types.is_timestamp(arrow_type):
                if arrow_type.tz:
                    columns[name] = 'TIMESTAMP WITH TIME ZONE'
                else:
                    columns[name] = 'TIMESTAMP'

            # Time types
            elif pa.types.is_time(arrow_type):
                columns[name] = 'TIME'

            # Duration
            elif pa.types.is_duration(arrow_type):
                columns[name] = 'BIGINT'  # Store as microseconds

            # Decimal
            elif pa.types.is_decimal(arrow_type):
                columns[name] = f'DECIMAL({arrow_type.precision}, {arrow_type.scale})'

            # List/Array types
            elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
                inner_type = self._arrow_type_to_trino(arrow_type.value_type)
                columns[name] = f'ARRAY({inner_type})'

            # Map types
            elif pa.types.is_map(arrow_type):
                key_type = self._arrow_type_to_trino(arrow_type.key_type)
                value_type = self._arrow_type_to_trino(arrow_type.item_type)
                columns[name] = f'MAP({key_type}, {value_type})'

            # Struct types
            elif pa.types.is_struct(arrow_type):
                struct_fields = []
                for i in range(arrow_type.num_fields):
                    sf = arrow_type.field(i)
                    sf_type = self._arrow_type_to_trino(sf.type)
                    struct_fields.append(f'"{sf.name}" {sf_type}')
                columns[name] = f'ROW({", ".join(struct_fields)})'

            # Dictionary (categorical)
            elif pa.types.is_dictionary(arrow_type):
                # Use the value type
                columns[name] = self._arrow_type_to_trino(arrow_type.value_type)

            # Null type
            elif pa.types.is_null(arrow_type):
                columns[name] = 'VARCHAR'  # Default to VARCHAR for null

            # Default fallback
            else:
                logger.warning(f"Unknown Arrow type {arrow_type} for column {name}, using VARCHAR")
                columns[name] = 'VARCHAR'

        return columns

    def _arrow_type_to_trino(self, arrow_type: pa.DataType) -> str:
        """Convert single Arrow type to Trino type string."""
        type_map = {
            pa.int8(): 'TINYINT',
            pa.int16(): 'SMALLINT',
            pa.int32(): 'INTEGER',
            pa.int64(): 'BIGINT',
            pa.uint8(): 'SMALLINT',
            pa.uint16(): 'INTEGER',
            pa.uint32(): 'BIGINT',
            pa.uint64(): 'BIGINT',
            pa.float16(): 'REAL',
            pa.float32(): 'REAL',
            pa.float64(): 'DOUBLE',
            pa.bool_(): 'BOOLEAN',
            pa.string(): 'VARCHAR',
            pa.utf8(): 'VARCHAR',
            pa.binary(): 'VARBINARY',
            pa.date32(): 'DATE',
            pa.date64(): 'DATE',
        }

        if arrow_type in type_map:
            return type_map[arrow_type]

        if pa.types.is_timestamp(arrow_type):
            return 'TIMESTAMP WITH TIME ZONE' if arrow_type.tz else 'TIMESTAMP'

        if pa.types.is_decimal(arrow_type):
            return f'DECIMAL({arrow_type.precision}, {arrow_type.scale})'

        return 'VARCHAR'

    def close(self):
        """Close connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class TrinoError(Exception):
    """Trino operation error."""
    pass
