"""
Iceberg Writer
==============

Writes DataFrames to Apache Iceberg tables through Hive Metastore.
Supports partitioning, schema evolution, and ACID transactions.
"""

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Try to import pyiceberg - may not be available in all environments
try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.table import Table
    from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False
    logger.warning("PyIceberg not available, using fallback Spark SQL writer")


class IcebergWriter:
    """
    Writes DataFrames to Iceberg tables via Hive Metastore.

    Usage:
        writer = IcebergWriter()

        # Write DataFrame
        table_path = writer.write_dataframe(
            df=my_dataframe,
            database='bronze',
            table='fbref_schedule',
            partition_spec=[('league', 'identity'), ('season', 'identity')]
        )

        # Check if table exists
        if writer.table_exists('bronze', 'fbref_schedule'):
            ...
    """

    def __init__(
        self,
        catalog_name: str = 'iceberg',
        metastore_uri: str = 'thrift://hive-metastore:9083',
        warehouse_path: str = 'hdfs://namenode:9000/user/hive/warehouse/iceberg',
    ):
        """
        Initialize Iceberg writer.

        Args:
            catalog_name: Name of the Iceberg catalog
            metastore_uri: Hive Metastore Thrift URI
            warehouse_path: HDFS path for warehouse
        """
        self.catalog_name = catalog_name
        self.metastore_uri = metastore_uri
        self.warehouse_path = warehouse_path

        self._catalog = None
        self._spark = None

    @property
    def catalog(self):
        """Get or create the Iceberg catalog."""
        if self._catalog is None:
            if PYICEBERG_AVAILABLE:
                self._catalog = load_catalog(
                    self.catalog_name,
                    **{
                        'type': 'hive',
                        'uri': self.metastore_uri,
                        'warehouse': self.warehouse_path,
                    }
                )
            else:
                raise RuntimeError("PyIceberg not available")
        return self._catalog

    def _get_spark(self):
        """Get or create SparkSession for fallback operations."""
        if self._spark is None:
            from pyspark.sql import SparkSession

            self._spark = SparkSession.builder \
                .appName("IcebergWriter") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.iceberg.type", "hive") \
                .config("spark.sql.catalog.iceberg.uri", self.metastore_uri) \
                .getOrCreate()

        return self._spark

    def namespace_exists(self, database: str) -> bool:
        """Check if namespace (database) exists."""
        try:
            if PYICEBERG_AVAILABLE:
                namespaces = self.catalog.list_namespaces()
                return (database,) in namespaces
            else:
                spark = self._get_spark()
                result = spark.sql(f"SHOW DATABASES LIKE '{database}'").collect()
                return len(result) > 0
        except Exception as e:
            logger.error(f"Error checking namespace: {e}")
            return False

    def create_namespace(self, database: str) -> None:
        """Create namespace if it doesn't exist."""
        if self.namespace_exists(database):
            return

        try:
            if PYICEBERG_AVAILABLE:
                self.catalog.create_namespace(database)
            else:
                spark = self._get_spark()
                spark.sql(f"CREATE DATABASE IF NOT EXISTS iceberg.{database}")
            logger.info(f"Created namespace: {database}")
        except Exception as e:
            logger.error(f"Error creating namespace: {e}")
            raise

    def table_exists(self, database: str, table: str) -> bool:
        """Check if table exists."""
        try:
            if PYICEBERG_AVAILABLE:
                self.catalog.load_table(f"{database}.{table}")
                return True
            else:
                spark = self._get_spark()
                result = spark.sql(
                    f"SHOW TABLES IN iceberg.{database} LIKE '{table}'"
                ).collect()
                return len(result) > 0
        except (NoSuchTableError, Exception):
            return False

    def _pandas_to_arrow(self, df: pd.DataFrame) -> pa.Table:
        """Convert pandas DataFrame to PyArrow Table."""
        return pa.Table.from_pandas(df, preserve_index=False)

    def _add_metadata_columns(
        self,
        df: pd.DataFrame,
        source: str,
        batch_id: Optional[str] = None
    ) -> pd.DataFrame:
        """Add standard metadata columns to DataFrame."""
        df = df.copy()
        df['_source'] = source
        df['_ingested_at'] = datetime.utcnow()
        df['_batch_id'] = batch_id or str(uuid.uuid4())
        return df

    def write_dataframe(
        self,
        df: pd.DataFrame,
        database: str,
        table: str,
        partition_spec: Optional[List[Tuple[str, str]]] = None,
        mode: str = 'append',
        add_metadata: bool = True,
        source: Optional[str] = None,
    ) -> str:
        """
        Write DataFrame to Iceberg table.

        Args:
            df: Pandas DataFrame to write
            database: Target database (e.g., 'bronze')
            table: Target table name
            partition_spec: List of (column, transform) tuples for partitioning
            mode: Write mode ('append', 'overwrite')
            add_metadata: Whether to add _source, _ingested_at, _batch_id columns
            source: Source name for metadata

        Returns:
            Full table identifier (e.g., 'iceberg.bronze.fbref_schedule')
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping write to {database}.{table}")
            return f"iceberg.{database}.{table}"

        # Add metadata columns if requested
        if add_metadata:
            df = self._add_metadata_columns(df, source or table)

        # Ensure namespace exists
        self.create_namespace(database)

        full_table_name = f"iceberg.{database}.{table}"

        try:
            if PYICEBERG_AVAILABLE:
                return self._write_with_pyiceberg(
                    df, database, table, partition_spec, mode
                )
            else:
                return self._write_with_spark(
                    df, database, table, partition_spec, mode
                )
        except Exception as e:
            logger.error(f"Error writing to {full_table_name}: {e}")
            raise

    def _write_with_pyiceberg(
        self,
        df: pd.DataFrame,
        database: str,
        table: str,
        partition_spec: Optional[List[Tuple[str, str]]],
        mode: str,
    ) -> str:
        """Write using PyIceberg API."""
        table_id = f"{database}.{table}"
        arrow_table = self._pandas_to_arrow(df)

        if not self.table_exists(database, table):
            # Create table with schema
            iceberg_table = self.catalog.create_table(
                identifier=table_id,
                schema=arrow_table.schema,
            )
            logger.info(f"Created Iceberg table: {table_id}")
        else:
            iceberg_table = self.catalog.load_table(table_id)

        # Append data
        iceberg_table.append(arrow_table)

        logger.info(f"Wrote {len(df)} rows to {table_id}")
        return f"iceberg.{table_id}"

    def _write_with_spark(
        self,
        df: pd.DataFrame,
        database: str,
        table: str,
        partition_spec: Optional[List[Tuple[str, str]]],
        mode: str,
    ) -> str:
        """Write using Spark SQL as fallback."""
        spark = self._get_spark()

        # Convert pandas to Spark DataFrame
        spark_df = spark.createDataFrame(df)

        full_table_name = f"iceberg.{database}.{table}"

        # Build write query
        writer = spark_df.writeTo(full_table_name)

        if partition_spec:
            partition_cols = [col for col, _ in partition_spec]
            writer = writer.partitionedBy(*partition_cols)

        if mode == 'overwrite':
            writer.createOrReplace()
        else:
            if self.table_exists(database, table):
                writer.append()
            else:
                writer.create()

        logger.info(f"Wrote {len(df)} rows to {full_table_name}")
        return full_table_name

    def create_table_if_not_exists(
        self,
        database: str,
        table: str,
        schema: pa.Schema,
        partition_spec: Optional[List[Tuple[str, str]]] = None,
        comment: Optional[str] = None,
    ) -> None:
        """
        Create Iceberg table if it doesn't exist.

        Args:
            database: Target database
            table: Table name
            schema: PyArrow schema for the table
            partition_spec: Partition specification
            comment: Table comment/description
        """
        if self.table_exists(database, table):
            logger.debug(f"Table {database}.{table} already exists")
            return

        self.create_namespace(database)

        table_id = f"{database}.{table}"

        if PYICEBERG_AVAILABLE:
            self.catalog.create_table(
                identifier=table_id,
                schema=schema,
            )
        else:
            # Use Spark SQL to create table
            spark = self._get_spark()

            # Build CREATE TABLE statement
            columns = []
            for field in schema:
                spark_type = self._arrow_to_spark_type(field.type)
                columns.append(f"`{field.name}` {spark_type}")

            columns_str = ", ".join(columns)

            partition_clause = ""
            if partition_spec:
                partition_cols = ", ".join(col for col, _ in partition_spec)
                partition_clause = f"PARTITIONED BY ({partition_cols})"

            sql = f"""
                CREATE TABLE IF NOT EXISTS iceberg.{database}.{table} (
                    {columns_str}
                )
                USING iceberg
                {partition_clause}
            """
            spark.sql(sql)

        logger.info(f"Created Iceberg table: {table_id}")

    def _arrow_to_spark_type(self, arrow_type: pa.DataType) -> str:
        """Convert PyArrow type to Spark SQL type string."""
        type_map = {
            pa.int8(): 'TINYINT',
            pa.int16(): 'SMALLINT',
            pa.int32(): 'INT',
            pa.int64(): 'BIGINT',
            pa.float32(): 'FLOAT',
            pa.float64(): 'DOUBLE',
            pa.string(): 'STRING',
            pa.bool_(): 'BOOLEAN',
            pa.date32(): 'DATE',
            pa.date64(): 'DATE',
        }

        if arrow_type in type_map:
            return type_map[arrow_type]

        if pa.types.is_timestamp(arrow_type):
            return 'TIMESTAMP'
        if pa.types.is_decimal(arrow_type):
            return f'DECIMAL({arrow_type.precision}, {arrow_type.scale})'
        if pa.types.is_list(arrow_type):
            inner = self._arrow_to_spark_type(arrow_type.value_type)
            return f'ARRAY<{inner}>'
        if pa.types.is_struct(arrow_type):
            fields = [
                f"`{f.name}`: {self._arrow_to_spark_type(f.type)}"
                for f in arrow_type
            ]
            return f"STRUCT<{', '.join(fields)}>"

        return 'STRING'  # Default fallback

    def read_table(
        self,
        database: str,
        table: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Read data from Iceberg table.

        Args:
            database: Database name
            table: Table name
            columns: Columns to select (None = all)
            filter_expr: SQL filter expression
            limit: Maximum rows to return

        Returns:
            Pandas DataFrame
        """
        spark = self._get_spark()

        full_table_name = f"iceberg.{database}.{table}"

        if columns:
            cols = ", ".join(f"`{c}`" for c in columns)
        else:
            cols = "*"

        sql = f"SELECT {cols} FROM {full_table_name}"

        if filter_expr:
            sql += f" WHERE {filter_expr}"
        if limit:
            sql += f" LIMIT {limit}"

        return spark.sql(sql).toPandas()

    def get_table_history(
        self,
        database: str,
        table: str
    ) -> pd.DataFrame:
        """Get table snapshot history for time travel."""
        spark = self._get_spark()
        full_table_name = f"iceberg.{database}.{table}"

        return spark.sql(
            f"SELECT * FROM {full_table_name}.history"
        ).toPandas()

    def read_snapshot(
        self,
        database: str,
        table: str,
        snapshot_id: int,
    ) -> pd.DataFrame:
        """Read table at specific snapshot (time travel)."""
        spark = self._get_spark()
        full_table_name = f"iceberg.{database}.{table}"

        return spark.sql(
            f"SELECT * FROM {full_table_name} VERSION AS OF {snapshot_id}"
        ).toPandas()

    def compact_table(self, database: str, table: str) -> None:
        """Run compaction on table to merge small files."""
        spark = self._get_spark()
        full_table_name = f"iceberg.{database}.{table}"

        spark.sql(f"CALL iceberg.system.rewrite_data_files('{full_table_name}')")
        logger.info(f"Compacted table: {full_table_name}")

    def expire_snapshots(
        self,
        database: str,
        table: str,
        older_than: str = "7 days"
    ) -> None:
        """Expire old snapshots to reclaim storage."""
        spark = self._get_spark()
        full_table_name = f"iceberg.{database}.{table}"

        spark.sql(
            f"CALL iceberg.system.expire_snapshots("
            f"table => '{full_table_name}', "
            f"older_than => TIMESTAMP '{older_than}')"
        )
        logger.info(f"Expired snapshots older than {older_than} for {full_table_name}")
