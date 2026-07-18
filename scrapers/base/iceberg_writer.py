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
import math
import os
import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa

from scrapers.base.sql_validator import (
    validate_catalog_qualified_name,
    sanitize_filter_expr,
    validate_snapshot_id,
)

logger = logging.getLogger(__name__)


def _pyiceberg_s3_properties(values: Mapping[str, str]) -> dict[str, str]:
    """Build explicit HTTP(S) FileIO properties from the platform S3 env."""

    missing = [
        name for name in ("S3_ACCESS_KEY", "S3_SECRET_KEY") if not values.get(name)
    ]
    if missing:
        raise RuntimeError(
            "bulk Iceberg append requires environment variables: " + ", ".join(missing)
        )
    endpoint = values.get("S3_ENDPOINT", "seaweedfs:8333").strip()
    scheme = values.get("S3_SCHEME", "http").strip().lower()
    if scheme not in {"http", "https"}:
        raise RuntimeError("S3_SCHEME must be http or https for bulk Iceberg append")
    if "://" not in endpoint:
        endpoint = f"{scheme}://{endpoint}"
    parsed = urlparse(endpoint)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeError(
            "S3_ENDPOINT must be an HTTP(S) authority without credentials"
        )
    return {
        "s3.endpoint": endpoint.rstrip("/"),
        "s3.region": values.get("S3_REGION", "us-east-1"),
        "s3.access-key-id": values["S3_ACCESS_KEY"],
        "s3.secret-access-key": values["S3_SECRET_KEY"],
    }


class IcebergMetadataCorruptionError(RuntimeError):
    """Fail-closed signal for an Iceberg table that needs operator recovery.

    A scraper must never turn a metadata-read failure into ``DROP TABLE``.
    Append-only Bronze tables can contain source history that is impossible or
    expensive to recapture, so recovery is deliberately kept outside the
    writer and must start from a verified catalog/object-store backup.
    """


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
        catalog: str = "iceberg",
    ):
        """
        Initialize Iceberg writer.

        Args:
            trino_host: Trino coordinator hostname (default from env or 'trino')
            trino_port: Trino coordinator port (default from env or 8443)
            catalog: Iceberg catalog name
        """
        self.trino_host = trino_host or os.environ.get("TRINO_HOST", "trino")
        self.trino_port = (
            trino_port  # None = let TrinoTableManager decide based on auth mode
        )
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
            if (
                "connection" in err_msg
                or "unreachable" in err_msg
                or "refused" in err_msg
            ):
                raise
            logger.warning(f"Could not check table existence: {e}")
            return False

    @staticmethod
    def _is_null_scalar(value: Any) -> bool:
        if value is None or value is pd.NaT:
            return True
        return isinstance(value, float) and math.isnan(value)

    def _pandas_to_arrow(self, df: pd.DataFrame) -> pa.Table:
        """Convert pandas DataFrame to PyArrow Table.

        Handles timestamp[ns] conversion for PyIceberg compatibility.
        """
        df = df.copy()

        # Convert timestamp columns to microseconds for Iceberg compatibility
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Handle timezone-aware datetimes by converting to UTC and removing timezone
                if hasattr(df[col], "dt") and df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
                # Convert to datetime64[us] which Iceberg supports
                df[col] = df[col].astype("datetime64[us]")

        try:
            return pa.Table.from_pandas(df, preserve_index=False)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # A source that answers with a number for one row and a word for the
            # next (FotMob roundName: 12 vs "Round of 16") produces a mixed
            # object column that Arrow cannot type — and this schema inference
            # runs on EVERY write, so it fails even when the live column is
            # already varchar. Such a column is textual by nature: the number is
            # a label, not a measure. Stringify exactly the columns Arrow
            # rejects and leave every other dtype untouched.
            for col in df.columns:
                if df[col].dtype != object:
                    continue
                try:
                    pa.array(df[col])
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    logger.warning(
                        f"Column '{col}' mixes incompatible types; writing it as "
                        "text so the heterogeneous source values survive"
                    )
                    df[col] = df[col].map(
                        lambda v: None if self._is_null_scalar(v) else str(v)
                    )
            return pa.Table.from_pandas(df, preserve_index=False)

    def _add_metadata_columns(
        self, df: pd.DataFrame, source: str, batch_id: Optional[str] = None
    ) -> pd.DataFrame:
        """Add standard metadata columns to DataFrame."""
        df = df.copy()
        df["_source"] = source
        df["_ingested_at"] = datetime.utcnow()
        df["_batch_id"] = batch_id or str(uuid.uuid4())
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
        self,
        trino,
        database: str,
        table: str,
        columns: dict,
        partition_cols: list = None,
    ) -> None:
        """Refuse destructive automatic recovery from corrupt metadata.

        The unused arguments remain in the private method signature so older
        callers receive the safer behaviour without a compatibility break.
        """
        del trino, columns, partition_cols
        full_table = f"{self.catalog}.{database}.{table}"
        message = (
            f"ICEBERG metadata is invalid for {full_table}; automatic DROP/CREATE "
            "is disabled. Stop writers and restore the table/catalog from a "
            "verified backup before retrying."
        )
        logger.critical(message)
        raise IcebergMetadataCorruptionError(message)

    def write_dataframe(
        self,
        df: pd.DataFrame,
        database: str,
        table: str,
        partition_spec: Optional[List[Tuple[str, str]]] = None,
        mode: str = "append",
        add_metadata: bool = True,
        source: Optional[str] = None,
        delete_filter: Optional[str] = None,
        merge_keys: Optional[Sequence[str]] = None,
        bulk_arrow: bool = False,
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
            merge_keys: Optional natural key for an incremental Iceberg MERGE.

        Returns:
            Full table identifier (e.g., 'iceberg.bronze.fbref_schedule')
        """
        if mode not in {"append", "overwrite"}:
            raise ValueError("mode must be 'append' or 'overwrite'")
        if df.empty:
            if mode == "overwrite":
                raise ValueError(
                    "empty full-table overwrite is ambiguous; use an explicit "
                    "delete_filter replacement"
                )
            logger.warning(f"Empty DataFrame, skipping write to {database}.{table}")
            return f"{self.catalog}.{database}.{table}"

        # Add metadata columns if requested
        if add_metadata:
            df = self._add_metadata_columns(df, source or table)

        try:
            return self._write_to_iceberg(
                df,
                database,
                table,
                partition_spec,
                mode=mode,
                delete_filter=delete_filter,
                merge_keys=merge_keys,
                bulk_arrow=bulk_arrow,
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
        mode: str = "append",
        delete_filter: Optional[str] = None,
        merge_keys: Optional[Sequence[str]] = None,
        bulk_arrow: bool = False,
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
        from scrapers.base.trino_manager import TrinoError, _is_iceberg_invalid_metadata

        if mode not in {"append", "overwrite"}:
            raise ValueError("mode must be 'append' or 'overwrite'")
        if mode == "overwrite":
            if delete_filter or merge_keys:
                raise ValueError(
                    "full-table overwrite cannot be combined with delete_filter "
                    "or merge_keys"
                )
            # Stage the complete replacement before deleting live rows. The
            # Trino manager retains the verified stage if the final swap fails,
            # so overwrite can never silently degrade into an append and the
            # replacement remains recoverable.
            delete_filter = "TRUE"

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
                        trino,
                        database,
                        table,
                        columns,
                        partition_cols,
                    )
                else:
                    raise

        # Insert data — atomic stage+merge so the target gets ONE snapshot
        # regardless of how many byte-budget VALUES batches the rows need (#269).
        #
        # Partition-replace semantics: the caller-supplied filter narrows the
        # delete to the partitions being rewritten (so each run replaces
        # (league, season) instead of appending dupes). The DELETE is handed to
        # insert_dataframe_atomic so it runs AFTER the data is safely staged and
        # back-to-back with the merge INSERT — a failed INSERT can no longer
        # leave the table empty behind a committed DELETE (#314 / #283).
        if bulk_arrow:
            if delete_filter or merge_keys:
                raise ValueError("bulk Arrow append does not support replace or merge")
            rows_inserted = self._append_dataframe_pyiceberg(
                arrow_table, database=database, table=table
            )
        else:
            rows_inserted = trino.insert_dataframe_atomic(
                database,
                table,
                df,
                delete_filter=delete_filter,
                merge_keys=merge_keys,
            )
        logger.info(f"Inserted {rows_inserted} rows into {full_table}")

        return full_table

    def _append_dataframe_pyiceberg(
        self, arrow_table: pa.Table, *, database: str, table: str
    ) -> int:
        """Append a bounded Arrow frame as Parquet in one Iceberg snapshot.

        Long-form WhoScored player statistics cannot be sent as thousands of
        Trino ``VALUES`` statements within the daily SLO.  The repository
        bounds each frame before this method; PyIceberg writes that frame as
        Parquet and commits it without changing the source manifest protocol.
        """

        target = self._load_pyiceberg_table(database=database, table=table)
        aligned = self._align_pyiceberg_arrow_table(arrow_table, target=target)
        target.append(aligned)
        return len(aligned)

    @staticmethod
    def _align_pyiceberg_arrow_table(arrow_table: pa.Table, *, target) -> pa.Table:
        """Align a bounded Arrow relation to an existing Iceberg schema."""

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        target_schema = schema_to_pyarrow(target.schema())
        source = {name: arrow_table[name] for name in arrow_table.column_names}
        arrays = []
        for field in target_schema:
            column = source.get(field.name)
            if column is None:
                column = pa.nulls(len(arrow_table), type=field.type)
            elif column.type != field.type:
                column = column.cast(field.type, safe=False)
            arrays.append(column)
        return pa.Table.from_arrays(arrays, schema=target_schema)

    @staticmethod
    def _load_pyiceberg_table(*, database: str, table: str):
        """Load one REST-catalog table with the platform's explicit S3 FileIO."""

        from pyiceberg.catalog import load_catalog
        from pyiceberg.io.pyarrow import PyArrowFileIO

        properties = _pyiceberg_s3_properties(os.environ)
        rest_warehouse = os.environ.get("ICEBERG_REST_WAREHOUSE", "football").strip()
        if not rest_warehouse:
            raise RuntimeError("ICEBERG_REST_WAREHOUSE is required")
        catalog = load_catalog(
            "football_bulk_writer",
            type="rest",
            uri=os.environ.get("ICEBERG_REST_URI", "http://lakekeeper:8181/catalog"),
            warehouse=rest_warehouse,
        )
        target = catalog.load_table(f"{database}.{table}")
        target.io = PyArrowFileIO(properties)
        return target

    def replace_identity_partition_arrow_batches(
        self,
        arrow_tables,
        *,
        database: str,
        table: str,
        partition_column: str,
        partition_value: str,
    ) -> int:
        """Atomically replace one identity partition from bounded Arrow batches.

        Data files are produced batch by batch, while every delete/append is
        committed through one Iceberg metadata transaction. Readers therefore
        observe either the old complete partition or the new complete one.
        """

        import pyarrow.compute as pc
        from pyiceberg.expressions import EqualTo

        target = self._load_pyiceberg_table(database=database, table=table)
        self._require_exact_identity_partition(
            target,
            database=database,
            table=table,
            partition_column=partition_column,
        )
        rows = 0
        batches = 0
        with target.transaction() as transaction:
            transaction.delete(EqualTo(partition_column, partition_value))
            for arrow_table in arrow_tables:
                if not isinstance(arrow_table, pa.Table) or not arrow_table.num_rows:
                    raise ValueError(
                        "identity partition replacement batches must be non-empty "
                        "Arrow tables"
                    )
                if partition_column not in arrow_table.column_names:
                    raise ValueError(
                        "partition column is absent from Arrow relation: "
                        + partition_column
                    )
                partition_values = arrow_table[partition_column]
                if partition_values.null_count or bool(
                    pc.any(pc.not_equal(partition_values, partition_value)).as_py()
                ):
                    raise ValueError(
                        "Arrow relation contains rows outside the replacement partition"
                    )
                aligned = self._align_pyiceberg_arrow_table(arrow_table, target=target)
                transaction.append(
                    aligned,
                    snapshot_properties={
                        "operation": "replace-identity-partition-batch",
                        "partition-column": partition_column,
                        "partition-value": partition_value,
                    },
                )
                batches += 1
                rows += len(aligned)
            if not batches:
                raise ValueError("identity partition replacement has no batches")
        return rows

    @staticmethod
    def _require_exact_identity_partition(
        target,
        *,
        database: str,
        table: str,
        partition_column: str,
    ) -> None:
        fields = tuple(target.spec().fields)
        if (
            len(fields) != 1
            or fields[0].name != partition_column
            or str(fields[0].transform) != "identity"
        ):
            raise RuntimeError(
                f"{database}.{table} must be partitioned only by identity("
                f"{partition_column})"
            )

    def require_exact_identity_partition(
        self,
        *,
        database: str,
        table: str,
        partition_column: str,
    ) -> None:
        """Fail closed if an existing operational table has unsafe layout."""

        target = self._load_pyiceberg_table(database=database, table=table)
        self._require_exact_identity_partition(
            target,
            database=database,
            table=table,
            partition_column=partition_column,
        )

    def current_snapshot_id(self, *, database: str, table: str) -> int:
        """Return the catalog's current main snapshot, never a timestamp guess."""

        target = self._load_pyiceberg_table(database=database, table=table)
        snapshot = target.current_snapshot()
        snapshot_id = int(snapshot.snapshot_id) if snapshot is not None else 0
        if snapshot_id <= 0:
            raise RuntimeError(f"{database}.{table} has no current Iceberg snapshot")
        return snapshot_id

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
            partition_cols = (
                [col for col, _ in partition_spec] if partition_spec else None
            )

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

    def get_table_history(self, database: str, table: str) -> pd.DataFrame:
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

    def expire_snapshots(
        self, database: str, table: str, retention_days: int = 7
    ) -> None:
        """Expire old snapshots to reclaim storage via Trino."""
        trino = self._get_trino_manager()
        full_table = validate_catalog_qualified_name(self.catalog, database, table)
        retention_days = int(retention_days)

        # Trino Iceberg expire_snapshots procedure
        sql = f"ALTER TABLE {full_table} EXECUTE expire_snapshots(retention_threshold => '{retention_days}d')"
        trino._execute(sql)
        logger.info(
            f"Expired snapshots older than {retention_days} days for {full_table}"
        )
