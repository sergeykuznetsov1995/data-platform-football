"""
Iceberg Writer
==============

Writes DataFrames directly to Apache Iceberg tables via Trino INSERT.
Supports partitioning, schema evolution, and ACID transactions.

Storage Pipeline:
    DataFrame → Trino INSERT INTO iceberg.{schema}.{table}

Benefits of Iceberg:
    - Time Travel: query historical snapshots
    - Schema Evolution: add/rename columns without rewrite
    - ACID: DELETE, UPDATE, MERGE support
    - Automatic file management
"""

import logging
import os
import uuid
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
import pyarrow as pa

from scrapers.base.sql_validator import (
    validate_catalog_qualified_name,
    validate_identifier,
    sanitize_filter_expr,
    validate_snapshot_id,
)

logger = logging.getLogger(__name__)


class IcebergWriter:
    """
    Writes DataFrames directly to Iceberg tables via Trino INSERT.

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
        trino_host: str = None,
        trino_port: int = None,
        catalog: str = 'iceberg',
    ):
        """
        Initialize Iceberg writer.

        Args:
            trino_host: Trino coordinator hostname (default from env or 'trino')
            trino_port: Trino coordinator port (default from env or 8443)
            catalog: Iceberg catalog name
        """
        self.trino_host = trino_host or os.environ.get('TRINO_HOST', 'trino')
        self.trino_port = trino_port  # None = let TrinoTableManager decide based on auth mode
        self.catalog = catalog
        self._trino_manager = None

    def _get_trino_manager(self):
        """Get or create TrinoTableManager."""
        if self._trino_manager is None:
            from scrapers.base.trino_manager import TrinoTableManager

            self._trino_manager = TrinoTableManager(
                host=self.trino_host,
                port=self.trino_port,
                catalog=self.catalog,
            )
        return self._trino_manager

    def namespace_exists(self, database: str) -> bool:
        """Check if namespace (database/schema) exists."""
        try:
            trino = self._get_trino_manager()
            return trino.schema_exists(database)
        except Exception as e:
            logger.warning(f"Could not check namespace existence: {e}")
            return False

    def table_exists(self, database: str, table: str) -> bool:
        """Check if Iceberg table exists.

        Raises connection errors instead of swallowing them.
        """
        try:
            trino = self._get_trino_manager()
            return trino.table_exists(database, table)
        except Exception as e:
            err_msg = str(e).lower()
            if 'connection' in err_msg or 'unreachable' in err_msg or 'refused' in err_msg:
                raise
            logger.warning(f"Could not check table existence: {e}")
            return False

    def _pandas_to_arrow(self, df: pd.DataFrame) -> pa.Table:
        """Convert pandas DataFrame to PyArrow Table.

        Handles timestamp[ns] conversion for PyIceberg compatibility.
        """
        df = df.copy()

        # Convert timestamp columns to microseconds for Iceberg compatibility
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Handle timezone-aware datetimes by converting to UTC and removing timezone
                if hasattr(df[col], 'dt') and df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None)
                # Convert to datetime64[us] which Iceberg supports
                df[col] = df[col].astype('datetime64[us]')

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

    def _evolve_schema(self, trino, database: str, table: str, arrow_schema) -> None:
        """
        Add missing columns to existing Iceberg table (schema evolution).

        Compares DataFrame columns with existing table columns and issues
        ALTER TABLE ADD COLUMN for any new columns. Existing columns that
        are absent from the DataFrame are left as-is (INSERT fills them with NULL).

        Args:
            trino: TrinoTableManager instance
            database: Database/schema name
            table: Table name
            arrow_schema: PyArrow schema of the incoming DataFrame
        """
        existing_cols = trino.get_table_columns(database, table)
        existing_col_names = {c.lower() for c in existing_cols}

        new_columns = trino.arrow_schema_to_trino(arrow_schema)

        for col_name, col_type in new_columns.items():
            if col_name.lower() not in existing_col_names:
                trino.add_column(database, table, col_name, col_type)
                logger.info(f'Schema evolution: added column "{col_name}" {col_type}')

    def _recover_corrupted_table(
        self, trino, database: str, table: str,
        columns: dict, partition_cols: list = None,
    ) -> None:
        """
        Recover from ICEBERG_INVALID_METADATA by dropping and recreating the table.

        Bronze tables are idempotent — data is reloaded on every scrape,
        so dropping the table is safe.

        After DROP TABLE with corrupt metadata, Iceberg may leave orphaned
        files on HDFS that prevent CREATE TABLE (ICEBERG_COMMIT_ERROR:
        "Failed to write manifest list file"). We clean the HDFS directory
        between DROP and CREATE to ensure a clean state.

        Args:
            trino: TrinoTableManager instance
            database: Database/schema name
            table: Table name
            columns: Dict of column_name -> trino_type for CREATE TABLE
            partition_cols: Optional list of partition column names
        """
        full_table = f"{self.catalog}.{database}.{table}"
        logger.warning(
            f"ICEBERG_INVALID_METADATA detected for {full_table}. "
            f"Dropping and recreating table..."
        )
        try:
            trino.drop_table(database, table, if_exists=True)
        except Exception as drop_err:
            logger.error(
                f"Failed to DROP corrupted table {full_table}: {drop_err}. "
                f"Manual intervention required: connect to Trino and run "
                f"DROP TABLE IF EXISTS {full_table}"
            )
            raise

        # Clean orphaned HDFS files left after DROP TABLE with corrupt metadata
        self._clean_hdfs_table_dir(database, table)

        trino.create_iceberg_table(
            schema=database,
            table=table,
            columns=columns,
            partition_columns=partition_cols,
        )
        logger.info(f"Recreated table {full_table} after metadata recovery")

    def _clean_hdfs_table_dir(self, database: str, table: str) -> None:
        """
        Remove orphaned HDFS directories for a dropped Iceberg table.

        After DROP TABLE with corrupt metadata, Iceberg may leave behind
        orphaned files on HDFS that prevent CREATE TABLE from succeeding
        with ICEBERG_COMMIT_ERROR: "Failed to write manifest list file".

        Iceberg creates directories with UUID suffix:
          /user/hive/warehouse/{schema}.db/{table}-{uuid}
        We match all directories starting with the table name.
        """
        schema_dir = f"/user/hive/warehouse/{database}.db"
        try:
            from scrapers.base.hdfs_client import HDFSClient
            hdfs = HDFSClient()
            if not hdfs.exists(schema_dir):
                logger.debug(f"HDFS schema directory does not exist: {schema_dir}")
                return

            entries = hdfs.list_dir(schema_dir)
            # Match exact name or name-{uuid} pattern
            prefix = f"{table}-"
            orphaned = [
                e['name'] for e in entries
                if e['name'] == table or e['name'].startswith(prefix)
            ]

            for dirname in orphaned:
                hdfs_path = f"{schema_dir}/{dirname}"
                hdfs.delete(hdfs_path, recursive=True)
                logger.info(f"Cleaned orphaned HDFS directory: {hdfs_path}")

            if not orphaned:
                logger.debug(f"No orphaned HDFS directories for {table}")
        except Exception as e:
            logger.warning(
                f"Could not clean HDFS directories for {table}: {e}. "
                f"CREATE TABLE may fail if orphaned files remain."
            )

    def write_dataframe(
        self,
        df: pd.DataFrame,
        database: str,
        table: str,
        partition_spec: Optional[List[Tuple[str, str]]] = None,
        mode: str = 'append',
        add_metadata: bool = True,
        source: Optional[str] = None,
        delete_filter: Optional[str] = None,
    ) -> str:
        """
        Write DataFrame to Iceberg table via Trino INSERT.

        Args:
            df: Pandas DataFrame to write
            database: Target database (e.g., 'bronze')
            table: Target table name
            partition_spec: List of (column, transform) tuples for partitioning
            mode: Write mode ('append', 'overwrite')
            add_metadata: Whether to add _source, _ingested_at, _batch_id columns
            source: Source name for metadata
            delete_filter: Optional SQL WHERE clause executed as
                ``DELETE FROM ... WHERE <delete_filter>`` BEFORE INSERT.
                Use for partition-replace semantics, e.g.
                ``"league='ENG-Premier League' AND season=2025"``.

        Returns:
            Full table identifier (e.g., 'iceberg.bronze.fbref_schedule')
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping write to {database}.{table}")
            return f"{self.catalog}.{database}.{table}"

        # Add metadata columns if requested
        if add_metadata:
            df = self._add_metadata_columns(df, source or table)

        try:
            return self._write_to_iceberg(
                df, database, table, partition_spec, mode=mode,
                delete_filter=delete_filter,
            )
        except Exception as e:
            logger.error(f"Error writing to {database}.{table}: {e}")
            raise

    def _write_to_iceberg(
        self,
        df: pd.DataFrame,
        database: str,
        table: str,
        partition_spec: Optional[List[Tuple[str, str]]],
        mode: str = 'append',
        delete_filter: Optional[str] = None,
    ) -> str:
        """
        Write DataFrame directly to Iceberg via Trino INSERT.

        Data flow:
            1. Create Iceberg table if not exists
            2. INSERT data via Trino VALUES clause
            3. Iceberg manages files automatically in HDFS

        Args:
            df: DataFrame to write
            database: Target database (e.g., 'bronze')
            table: Target table name
            partition_spec: Optional partition specification [(col, transform), ...]
            mode: Write mode ('append', 'overwrite')

        Returns:
            Full table identifier (e.g., 'iceberg.bronze.clubelo_ratings')
        """
        from scrapers.base.trino_manager import TrinoTableManager, TrinoError, _is_iceberg_invalid_metadata

        trino = self._get_trino_manager()
        full_table = f"{self.catalog}.{database}.{table}"

        # Convert DataFrame for Arrow schema extraction
        arrow_table = self._pandas_to_arrow(df)

        # Pre-compute columns and partition_cols (needed for both create and recovery)
        columns = trino.arrow_schema_to_trino(arrow_table.schema)
        partition_cols = [col for col, _ in partition_spec] if partition_spec else None

        # Create table if not exists
        if not trino.table_exists(database, table):
            trino.create_iceberg_table(
                schema=database,
                table=table,
                columns=columns,
                partition_columns=partition_cols,
            )
            logger.info(f"Created Iceberg table: {full_table}")

        # Schema evolution — add missing columns to existing table
        if trino.table_exists(database, table):
            try:
                self._evolve_schema(trino, database, table, arrow_table.schema)
            except TrinoError as e:
                if _is_iceberg_invalid_metadata(e):
                    self._recover_corrupted_table(
                        trino, database, table, columns, partition_cols,
                    )
                else:
                    raise

        # Handle overwrite mode by deleting existing data
        if mode == 'overwrite':
            try:
                trino._execute(f"DELETE FROM {full_table}")
                logger.info(f"Deleted existing data from {full_table}")
            except TrinoError as e:
                # Table might be empty or not support DELETE
                logger.warning(f"Could not delete existing data: {e}")

        # Insert data — atomic stage+merge so the target gets ONE snapshot
        # regardless of how many byte-budget VALUES batches the rows need (#269).
        #
        # Partition-replace semantics: the caller-supplied filter narrows the
        # delete to the partitions being rewritten (so each run replaces
        # (league, season) instead of appending dupes). The DELETE is handed to
        # insert_dataframe_atomic so it runs AFTER the data is safely staged and
        # back-to-back with the merge INSERT — a failed INSERT can no longer
        # leave the table empty behind a committed DELETE (#314 / #283).
        rows_inserted = trino.insert_dataframe_atomic(
            database, table, df, delete_filter=delete_filter,
        )
        logger.info(f"Inserted {rows_inserted} rows into {full_table}")

        return full_table

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
            database: Database name
            table: Table name
            schema: PyArrow schema for table columns
            partition_spec: Optional partition specification
            comment: Optional table comment (not used currently)
        """
        trino = self._get_trino_manager()

        if not trino.table_exists(database, table):
            columns = trino.arrow_schema_to_trino(schema)
            partition_cols = [col for col, _ in partition_spec] if partition_spec else None

            trino.create_iceberg_table(
                schema=database,
                table=table,
                columns=columns,
                partition_columns=partition_cols,
            )
            logger.info(f"Created Iceberg table: {self.catalog}.{database}.{table}")

    def read_table(
        self,
        database: str,
        table: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Read data from Iceberg table via Trino.

        Args:
            database: Database name
            table: Table name
            columns: Columns to select (None = all)
            filter_expr: SQL filter expression
            limit: Maximum rows to return

        Returns:
            Pandas DataFrame
        """
        trino = self._get_trino_manager()
        full_table = validate_catalog_qualified_name(self.catalog, database, table)

        if columns:
            cols = ", ".join(f'"{c}"' for c in columns)
        else:
            cols = "*"

        sql = f"SELECT {cols} FROM {full_table}"

        if filter_expr:
            filter_expr = sanitize_filter_expr(filter_expr)
            sql += f" WHERE {filter_expr}"
        if limit:
            limit = int(limit)
            sql += f" LIMIT {limit}"

        result = trino.execute_query(sql)

        # Convert to DataFrame
        if result:
            # Get column names from cursor description
            cursor = trino.connection.cursor()
            cursor.execute(sql)
            col_names = [desc[0] for desc in cursor.description]
            cursor.close()
            return pd.DataFrame(result, columns=col_names)

        return pd.DataFrame()

    def get_table_history(
        self,
        database: str,
        table: str
    ) -> pd.DataFrame:
        """Get table snapshot history for time travel via Trino."""
        trino = self._get_trino_manager()
        validate_catalog_qualified_name(self.catalog, database, table)

        # Iceberg snapshots metadata table
        sql = f'SELECT * FROM {self.catalog}.{database}."{table}$snapshots"'
        result = trino.execute_query(sql)

        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()

    def read_snapshot(
        self,
        database: str,
        table: str,
        snapshot_id: int,
    ) -> pd.DataFrame:
        """Read table at specific snapshot (time travel) via Trino."""
        trino = self._get_trino_manager()
        full_table = validate_catalog_qualified_name(self.catalog, database, table)
        snapshot_id = validate_snapshot_id(snapshot_id)

        sql = f"SELECT * FROM {full_table} FOR VERSION AS OF {snapshot_id}"
        result = trino.execute_query(sql)

        if result:
            return pd.DataFrame(result)
        return pd.DataFrame()

    def compact_table(self, database: str, table: str) -> None:
        """Run compaction on table to merge small files via Trino."""
        trino = self._get_trino_manager()
        full_table = validate_catalog_qualified_name(self.catalog, database, table)

        # Trino Iceberg optimize procedure
        sql = f"ALTER TABLE {full_table} EXECUTE optimize"
        trino._execute(sql)
        logger.info(f"Compacted table: {full_table}")

    def expire_snapshots(
        self,
        database: str,
        table: str,
        retention_days: int = 7
    ) -> None:
        """Expire old snapshots to reclaim storage via Trino."""
        trino = self._get_trino_manager()
        full_table = validate_catalog_qualified_name(self.catalog, database, table)
        retention_days = int(retention_days)

        # Trino Iceberg expire_snapshots procedure
        sql = f"ALTER TABLE {full_table} EXECUTE expire_snapshots(retention_threshold => '{retention_days}d')"
        trino._execute(sql)
        logger.info(f"Expired snapshots older than {retention_days} days for {full_table}")
