"""
Trino Table Manager
===================

Manages Iceberg tables via Trino SQL.
Creates schemas, tables, and inserts data directly into Iceberg.

Storage Pipeline:
    DataFrame → Trino INSERT INTO iceberg.{schema}.{table}
"""

import logging
import os
import time
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa

from scrapers.base.sql_validator import validate_identifier, validate_catalog_qualified_name

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
        port: int = None,
        user: str = 'airflow',
        catalog: str = 'iceberg',
    ):
        if not TRINO_AVAILABLE:
            raise RuntimeError("trino package not installed. Run: pip install trino")

        self.host = host
        self.user = user
        self.catalog = catalog
        self._password = os.environ.get('TRINO_PASSWORD')
        if self._password:
            self.port = port or int(os.environ.get('TRINO_PORT', 8443))
        else:
            self.port = port or int(os.environ.get('TRINO_PORT', 8080))
            logger.info("TRINO_PASSWORD not set, connecting via HTTP (no auth)")
        self._conn = None

    # Retry settings for connection. A Trino container restart takes ~30-60s
    # (SERVER STARTED ~13s + authenticator warm-up), so the cumulative window
    # must outlast it: 3+5+10+20+30+45 = 113s (#847). The old 3-attempt/18s
    # window gave up mid-restart and the bronze write was lost (#842 APL 16/17
    # — ~20 min of residential proxy spent on a half-saved season).
    _CONNECT_RETRIES = 7
    _CONNECT_BACKOFF = (3, 5, 10, 20, 30, 45)  # seconds between attempts

    # Class-level fail-fast cache: if Trino was unreachable in this process,
    # skip retries to avoid wasting 18+ seconds per call
    _trino_unreachable = False

    @property
    def connection(self):
        """Get or create Trino connection with retry logic."""
        if self._conn is None:
            if TrinoTableManager._trino_unreachable:
                raise TrinoError(
                    f"Trino fast-fail: previously detected as unreachable "
                    f"at {self.host}:{self.port}. "
                    f"Skipping retry to avoid delays."
                )
            self._connect_with_retry()
        return self._conn

    def _create_connection(self):
        """Create a new Trino connection."""
        if self._password:
            return trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                http_scheme='https',
                auth=trino.auth.BasicAuthentication(self.user, self._password),
                verify=False,  # self-signed certificate
            )
        else:
            return trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
            )

    def _connect_with_retry(self):
        """Connect to Trino with retry and exponential backoff."""
        last_error = None
        for attempt in range(self._CONNECT_RETRIES):
            try:
                conn = self._create_connection()
                # Test connection with simple query
                cursor = conn.cursor()
                cursor.execute('SELECT 1')
                cursor.fetchall()
                cursor.close()
                self._conn = conn
                TrinoTableManager._trino_unreachable = False
                if attempt > 0:
                    logger.info(f"Trino connection established on attempt {attempt + 1}")
                return
            except Exception as e:
                last_error = e
                if attempt < self._CONNECT_RETRIES - 1:
                    backoff = self._CONNECT_BACKOFF[min(attempt, len(self._CONNECT_BACKOFF) - 1)]
                    logger.warning(
                        f"Trino connection attempt {attempt + 1}/{self._CONNECT_RETRIES} failed: {e}. "
                        f"Retrying in {backoff}s..."
                    )
                    time.sleep(backoff)
                else:
                    logger.warning(
                        f"Trino connection attempt {attempt + 1}/{self._CONNECT_RETRIES} failed: {e}. "
                        f"No more retries."
                    )

        TrinoTableManager._trino_unreachable = True
        raise TrinoError(
            f"Failed to connect to Trino after {self._CONNECT_RETRIES} attempts: {last_error}"
        )

    def _reset_connection(self):
        """Reset the connection (close existing, will reconnect on next use).

        Also resets the fast-fail cache so that the next access to
        ``self.connection`` will attempt a fresh ``_connect_with_retry``.
        """
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        TrinoTableManager._trino_unreachable = False

    # 'authenticators were not loaded' is the HTTP 500 Trino serves during its
    # post-restart warm-up, before PASSWORD auth is wired (#847) — functionally
    # "not up yet", so treat it exactly like a refused/reset connection.
    _CONNECTION_ERRORS = ('Connection refused', 'Connection reset',
                          'Connection aborted', 'authenticators were not loaded')

    def _execute(self, sql: str, fetch: bool = False,
                 params: Optional[tuple] = None) -> Optional[List[Any]]:
        """Execute SQL statement.

        Always consumes query results to prevent USER_CANCELED errors.
        Trino Python client cancels queries on cursor.close() if results
        are not fully consumed.

        Runtime failures (ICEBERG_COMMIT_ERROR, OOM, dead worker) surface
        while polling results — i.e. inside fetchall() — so they propagate
        as TrinoError even for DDL/DML, never a silent success (#456).

        On connection errors, resets connection and retries once.

        ``params`` binds ``?`` placeholders in ``sql`` (Trino prepared
        statement) — prefer it over interpolating values into the string.
        """
        for attempt in range(2):
            cursor = self.connection.cursor()
            try:
                logger.debug(f"Executing SQL: {sql[:200]}...")
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                if fetch:
                    return cursor.fetchall()

                # Consume results even for DDL/DML to ensure query completes
                cursor.fetchall()
                return None

            except Exception as e:
                error_str = str(e)
                is_conn_error = any(msg in error_str for msg in self._CONNECTION_ERRORS)

                if is_conn_error and attempt == 0:
                    logger.warning(
                        f"Connection error during SQL execution: {e}. "
                        f"Resetting connection and retrying..."
                    )
                    self._reset_connection()
                    continue

                logger.error(f"SQL execution error: {e}\nSQL (truncated): {sql[:500]}")
                raise TrinoError(f"SQL execution failed: {e}") from e

            finally:
                cursor.close()

    def create_schema(self, schema: str) -> None:
        """
        Create schema if it doesn't exist.

        Args:
            schema: Schema name (e.g., 'bronze', 'silver', 'gold')
        """
        qualified = validate_catalog_qualified_name(self.catalog, schema)
        sql = f"CREATE SCHEMA IF NOT EXISTS {qualified}"

        try:
            self._execute(sql)
            logger.info(f"Created schema: {self.catalog}.{schema}")
        except TrinoError:
            # Schema might already exist
            logger.debug(f"Schema {schema} may already exist")

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists."""
        validate_identifier(schema, "schema")
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
        qualified = validate_catalog_qualified_name(self.catalog, schema)
        validate_identifier(table, "table")
        sql = f"SHOW TABLES FROM {qualified} LIKE '{table}'"

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

        qualified = validate_catalog_qualified_name(self.catalog, schema, table)

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

        sql = f"""CREATE TABLE {exists_clause}{qualified} (
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

        qualified = validate_catalog_qualified_name(self.catalog, schema, table)
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        # Iceberg partitioning syntax
        partition_clause = ""
        if partition_columns:
            for pc in partition_columns:
                validate_identifier(pc, "partition column")
            cols = ", ".join(f"'{c}'" for c in partition_columns)
            partition_clause = f"\nWITH (partitioning = ARRAY[{cols}])"

        sql = f"""CREATE TABLE {exists_clause}{qualified} (
{columns_sql}
){partition_clause}"""

        self._execute(sql)
        logger.info(f"Created Iceberg table: {self.catalog}.{schema}.{table}")

    def _format_sql_value(self, val, target_type: str = '') -> str:
        """
        Format a Python value as a SQL literal, casting to match the target column type.

        When target_type is known (from DESCRIBE TABLE), the value is cast to match
        the table schema. This prevents TYPE_MISMATCH errors when pandas changes
        column dtypes between runs (e.g., int→float due to NaN).

        Args:
            val: Python value from DataFrame row
            target_type: Trino column type (e.g., 'bigint', 'varchar', 'double')

        Returns:
            SQL literal string
        """
        if pd.isna(val):
            return "NULL"

        tt = target_type.upper()

        # When target type is known, cast value to match table schema
        if tt:
            if tt.startswith('VARCHAR') or tt.startswith('CHAR'):
                escaped = str(val).replace("'", "''")
                return f"'{escaped}'"

            if tt in ('BIGINT', 'INTEGER', 'SMALLINT', 'TINYINT'):
                try:
                    return f"CAST({int(val)} AS {tt})"
                except (ValueError, TypeError, OverflowError):
                    return "NULL"

            if tt in ('DOUBLE', 'REAL'):
                try:
                    float_val = float(val)
                    if np.isnan(float_val) or np.isinf(float_val):
                        return "NULL"
                    return f"CAST({float_val} AS {tt})"
                except (ValueError, TypeError):
                    return "NULL"

            if tt.startswith('DECIMAL'):
                try:
                    return f"CAST({val} AS {tt})"
                except (ValueError, TypeError):
                    return "NULL"

            if tt == 'BOOLEAN':
                # A string like "False" is truthy in Python, so plain
                # `if val` would silently render it as TRUE (#470). Parse the
                # common string forms explicitly; refuse to guess otherwise.
                if isinstance(val, str):
                    s = val.strip().lower()
                    if s in ('true', 't', '1'):
                        return "TRUE"
                    if s in ('false', 'f', '0', ''):
                        return "FALSE"
                    return "NULL"
                return "TRUE" if val else "FALSE"

            if tt == 'DATE':
                if isinstance(val, (date, datetime, pd.Timestamp)):
                    if isinstance(val, datetime):
                        return f"DATE '{val.strftime('%Y-%m-%d')}'"
                    return f"DATE '{val}'"
                safe = str(val).replace("'", "''")
                return f"DATE '{safe}'"

            if 'TIMESTAMP' in tt:
                timestamp = None
                if isinstance(val, (datetime, pd.Timestamp)):
                    timestamp = pd.Timestamp(val)
                elif isinstance(val, str):
                    try:
                        timestamp = pd.Timestamp(val)
                    except (TypeError, ValueError):
                        pass
                if timestamp is not None and not pd.isna(timestamp):
                    if 'WITH TIME ZONE' in tt:
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.tz_localize('UTC')
                        else:
                            timestamp = timestamp.tz_convert('UTC')
                        ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
                        return f"TIMESTAMP '{ts_str} UTC'"
                    if timestamp.tzinfo is not None:
                        timestamp = timestamp.tz_convert('UTC').tz_localize(None)
                    ts_str = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
                    return f"TIMESTAMP '{ts_str}'"
                safe = str(val).replace("'", "''")
                return f"TIMESTAMP '{safe}'"

        # Fallback: infer type from Python value (used when table types unknown)
        if isinstance(val, str):
            escaped = val.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(val, date) and not isinstance(val, datetime):
            return f"DATE '{val}'"
        elif isinstance(val, (datetime, pd.Timestamp)):
            ts_str = val.strftime('%Y-%m-%d %H:%M:%S.%f')
            return f"TIMESTAMP '{ts_str}'"
        elif isinstance(val, bool):
            # Must check bool before int (bool is subclass of int)
            return "TRUE" if val else "FALSE"
        elif isinstance(val, (int, np.integer)):
            return f"CAST({val} AS BIGINT)"
        elif isinstance(val, (float, np.floating)):
            if np.isnan(val) or np.isinf(val):
                return "NULL"
            return f"CAST({val} AS DOUBLE)"
        else:
            escaped = str(val).replace("'", "''")
            return f"'{escaped}'"

    def insert_dataframe(
        self,
        schema: str,
        table: str,
        df: pd.DataFrame,
        batch_size: int = 1000,
    ) -> int:
        """
        Insert DataFrame rows into Iceberg table via VALUES clause.

        Fetches actual table column types to cast values correctly,
        preventing TYPE_MISMATCH errors when pandas changes dtypes
        between runs (e.g., int column becomes float due to NaN).

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

        # Fetch actual table column types for type-safe casting
        table_col_types: Dict[str, str] = {}
        try:
            raw_types = self.get_table_columns(schema, table)
            table_col_types = {k.lower(): v for k, v in raw_types.items()}
        except Exception as e:
            logger.debug(f"Could not fetch table column types: {e}")

        qualified = validate_catalog_qualified_name(self.catalog, schema, table)

        total_inserted = 0
        columns = ', '.join(f'"{c}"' for c in df.columns)
        header = f"INSERT INTO {qualified} ({columns})\nVALUES\n"

        # Trino's `query.max-length` defaults to 1,000,000 bytes; leave headroom
        # for wide schemas (e.g. sofascore_player_season_stats ~150 cols).
        sql_byte_budget = 900_000

        pending_rows: List[str] = []
        pending_bytes = len(header)
        batch_num = 0

        def _flush() -> None:
            nonlocal pending_rows, pending_bytes, total_inserted, batch_num
            if not pending_rows:
                return
            batch_num += 1
            self._execute(header + ",\n".join(pending_rows))
            total_inserted += len(pending_rows)
            logger.debug(
                f"Inserted batch {batch_num}: {len(pending_rows)} rows "
                f"({pending_bytes} bytes SQL) into "
                f"{self.catalog}.{schema}.{table}"
            )
            pending_rows = []
            pending_bytes = len(header)

        # ``iterrows`` boxes every scalar into a Series and can silently
        # coerce integer identifiers to float.  Tuple iteration preserves the
        # DataFrame's column order/types and is substantially faster for the
        # wide, long-form WhoScored stat batches. SQL is still flushed in
        # bounded multi-row VALUES chunks; there is never one query per row.
        for source_values in df.itertuples(index=False, name=None):
            values = []
            for col, val in zip(df.columns, source_values):
                target_type = table_col_types.get(col.lower(), '')
                values.append(self._format_sql_value(val, target_type))
            row_sql = f"({', '.join(values)})"
            # +2 accounts for the ",\n" separator before the next row.
            if pending_rows and (
                pending_bytes + len(row_sql) + 2 > sql_byte_budget
                or len(pending_rows) >= batch_size
            ):
                _flush()
            pending_rows.append(row_sql)
            pending_bytes += len(row_sql) + 2

        _flush()

        logger.info(f"Inserted {total_inserted} rows into {self.catalog}.{schema}.{table}")
        return total_inserted

    def insert_dataframe_atomic(
        self,
        schema: str,
        table: str,
        df: pd.DataFrame,
        batch_size: int = 1000,
        delete_filter: Optional[str] = None,
        staging_id: Optional[str] = None,
        merge_keys: Optional[Sequence[str]] = None,
    ) -> int:
        """
        Insert a DataFrame so the target table receives exactly ONE snapshot.

        ``insert_dataframe`` flushes a separate ``INSERT INTO ... VALUES`` for
        every ~900 KB byte-budget batch, and each INSERT into an Iceberg table
        creates a new snapshot + metadata.json + manifest. Wide rows (e.g.
        ``fotmob_match_details`` with 8 large JSON blobs per row) therefore
        produce hundreds of snapshots per run (#269).

        This method stages the byte-budget batches into a transient table and
        merges them with a single ``INSERT INTO target SELECT ... FROM stage``,
        so the target gains exactly one snapshot regardless of batch count.
        The throwaway stage is dropped, discarding its snapshot churn.

        ``replace_partitions`` semantics (#314): when ``delete_filter`` is set the
        partition DELETE is deferred until AFTER the stage is fully populated and
        verified, then runs back-to-back with the merge INSERT. This is the
        "stage-first → atomic swap" fix: all the slow, flaky network I/O happens
        on the throwaway stage while the live table is untouched, so a transient
        error (the kind that wiped ``clubelo_team_history`` 105600→0 in #283)
        fails before any DELETE is issued. If the swap itself fails after the
        DELETE commits, the stage is *retained* (not dropped) so the rows can be
        recovered with ``INSERT INTO target SELECT * FROM stage``.

        Args:
            schema: Schema name
            table: Target table name (must already exist)
            df: Pandas DataFrame to insert
            batch_size: Number of rows per staged INSERT statement
            delete_filter: Optional SQL WHERE clause. When set, rows matching it
                are deleted from the target as part of the swap (partition
                replace) instead of a plain append. A failure here raises — it is
                never downgraded to a silent append.
            merge_keys: Optional natural key for an incremental Iceberg MERGE.
                Mutually exclusive with ``delete_filter``.
            staging_id: Optional caller-owned unique staging suffix.

        Returns:
            Number of rows inserted into the target
        """
        if df.empty:
            if not delete_filter:
                logger.warning(
                    f"Empty DataFrame, skipping insert to {schema}.{table}"
                )
                return 0

            # An explicitly empty replacement is still a state change: the
            # selected live rows must disappear. There is nothing to stage or
            # merge, so this DELETE is the complete atomic replacement.
            qualified_target = validate_catalog_qualified_name(
                self.catalog, schema, table
            )
            self._execute(
                f"DELETE FROM {qualified_target} WHERE {delete_filter}"
            )
            logger.info(
                f"Replaced rows matching '{delete_filter}' with an empty "
                f"dataset in {qualified_target}"
            )
            return 0

        if delete_filter and merge_keys:
            raise ValueError("delete_filter and merge_keys are mutually exclusive")

        keys = tuple(merge_keys or ())
        for key in keys:
            validate_identifier(key, "merge key")
            if key not in df.columns:
                raise ValueError(f"merge key {key!r} is absent from DataFrame")
            if df[key].isna().any():
                raise ValueError(f"merge key {key!r} contains null values")
        if keys and df.duplicated(subset=list(keys)).any():
            raise ValueError(f"duplicate rows for merge keys {list(keys)!r}")

        # Parallel ingestion shards must never share a predictable staging
        # table. Distributed callers pass a run/task/map/try/UUID-qualified
        # token; everyone else gets a random suffix — a deterministic
        # ``{table}__stg`` races as soon as two mapped Airflow tasks write the
        # same target: one task can drop or populate the other's stage.
        if staging_id:
            validate_identifier(staging_id, "staging id")
            stage = f"{table}__stg_{staging_id}"
        else:
            stage = f"{table}__stg_{uuid.uuid4().hex[:12]}"
        qualified_target = validate_catalog_qualified_name(self.catalog, schema, table)
        qualified_stage = validate_catalog_qualified_name(self.catalog, schema, stage)

        # Empty copy of the target schema (column names/types incl. metadata).
        self._execute(f"CREATE TABLE {qualified_stage} AS SELECT * FROM {qualified_target} WHERE false")

        # --- Phase 1: stage every row (all the slow, flaky network I/O). The
        # target is NOT touched yet, so any failure here leaves it intact (#314).
        try:
            inserted = self.insert_dataframe(schema, stage, df, batch_size)
            staged = self._execute(
                f"SELECT count(*) FROM {qualified_stage}", fetch=True
            )[0][0]
            if staged != len(df):
                raise TrinoError(
                    f"Stage row count mismatch for {qualified_stage}: staged "
                    f"{staged}, expected {len(df)}. Aborting before touching target."
                )
        except Exception:
            # Target untouched — safe to discard the half-built stage.
            self.drop_table(schema, stage, if_exists=True)
            raise

        # --- Phase 2: swap (narrow window, two metadata-only operations). The
        # stage already holds the data, so a partition DELETE followed by a single
        # INSERT...SELECT is the whole replace.
        cols = ', '.join(f'"{c}"' for c in df.columns)
        try:
            if keys:
                on_clause = " AND ".join(
                    f't."{key}" = s."{key}"' for key in keys
                )
                update_columns = [column for column in df.columns if column not in keys]
                update_clause = ""
                if update_columns:
                    assignments = ", ".join(
                        f't."{column}" = s."{column}"'
                        for column in update_columns
                    )
                    update_clause = f"WHEN MATCHED THEN UPDATE SET {assignments} "
                values = ", ".join(f's."{column}"' for column in df.columns)
                self._execute(
                    f"MERGE INTO {qualified_target} t USING {qualified_stage} s "
                    f"ON {on_clause} {update_clause}"
                    f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({values})"
                )
            else:
                if delete_filter:
                    self._execute(f"DELETE FROM {qualified_target} WHERE {delete_filter}")
                    logger.info(
                        f"Deleted rows matching '{delete_filter}' from {qualified_target}"
                    )
                self._execute(
                    f"INSERT INTO {qualified_target} ({cols}) "
                    f"SELECT {cols} FROM {qualified_stage}"
                )
        except Exception:
            # The DELETE may already be committed while the INSERT failed, so the
            # stage is now the only copy of these rows. Do NOT drop it — keep it
            # for recovery (`INSERT INTO target SELECT * FROM stage`) and fail loud.
            logger.error(
                f"Atomic swap failed for {qualified_target}; stage {qualified_stage} "
                f"retained for recovery (holds {inserted} rows)"
            )
            raise

        # Swap succeeded — discard the throwaway stage and its snapshot churn.
        self.drop_table(schema, stage, if_exists=True)

        operation = "Merged" if keys else "Inserted"
        logger.info(
            f"{operation} {inserted} rows into {qualified_target} "
            f"(via unique stage, 1 snapshot)"
        )
        return inserted

    def drop_table(self, schema: str, table: str, if_exists: bool = True) -> None:
        """
        Drop table.

        Args:
            schema: Schema name
            table: Table name
            if_exists: Add IF EXISTS clause
        """
        qualified = validate_catalog_qualified_name(self.catalog, schema, table)
        exists_clause = "IF EXISTS " if if_exists else ""
        sql = f"DROP TABLE {exists_clause}{qualified}"

        self._execute(sql)
        logger.info(f"Dropped table: {self.catalog}.{schema}.{table}")

    def add_column(self, schema: str, table: str, column: str, column_type: str) -> None:
        """
        Add a column to an existing Iceberg table.

        Args:
            schema: Schema name
            table: Table name
            column: Column name
            column_type: Trino type (e.g., 'VARCHAR', 'BIGINT', 'DOUBLE')
        """
        qualified = validate_catalog_qualified_name(self.catalog, schema, table)
        sql = f'ALTER TABLE {qualified} ADD COLUMN "{column}" {column_type}'
        self._execute(sql)
        logger.info(f'Added column "{column}" {column_type} to {self.catalog}.{schema}.{table}')

    def get_table_columns(self, schema: str, table: str) -> Dict[str, str]:
        """
        Get table columns and types.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Dict of column_name -> type
        """
        qualified = validate_catalog_qualified_name(self.catalog, schema, table)
        sql = f"DESCRIBE {qualified}"
        result = self._execute(sql, fetch=True)

        columns = {}
        for row in result:
            # DESCRIBE returns (column_name, data_type, extra, comment)
            col_name = row[0]
            col_type = row[1]
            columns[col_name] = col_type

        return columns

    def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Any]:
        """
        Execute arbitrary SQL query.

        Args:
            sql: SQL query (may contain ``?`` placeholders)
            params: optional bind values for the ``?`` placeholders

        Returns:
            Query results
        """
        return self._execute(sql, fetch=True, params=params)

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


_ICEBERG_METADATA_ERRORS = frozenset({
    'ICEBERG_INVALID_METADATA',
    'ICEBERG_MISSING_METADATA',
})


def _is_iceberg_invalid_metadata(error: Exception) -> bool:
    """
    Check if error is caused by corrupted/missing Iceberg metadata.

    Matches both ICEBERG_INVALID_METADATA and ICEBERG_MISSING_METADATA
    error names. Walks the __cause__ chain because _execute() wraps the
    original TrinoExternalError in TrinoError via 'raise ... from e'.

    Args:
        error: Exception to check

    Returns:
        True if Iceberg metadata error found in cause chain
    """
    cause = error
    while cause is not None:
        if getattr(cause, 'error_name', None) in _ICEBERG_METADATA_ERRORS:
            return True
        cause = getattr(cause, '__cause__', None)
    return False
