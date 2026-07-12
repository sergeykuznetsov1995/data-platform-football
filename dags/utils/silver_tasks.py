"""
Silver Transformation Tasks
============================

Utility functions for executing Silver-layer CTAS transformations via Trino.

Uses the `trino` Python library directly (NOT scrapers.base.trino_manager)
to avoid importing the entire scrapers package with its heavy dependencies
(browser engines, soccerdata, curl_cffi).

Usage:
    from utils.silver_tasks import run_silver_transform

    run_silver_transform(
        sql_file='dags/sql/silver/fbref_player_season_profile.sql',
        table_name='fbref_player_season_profile',
    )
"""

import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import trino as trino_lib

# Inline SQL identifier validation (same as scrapers.base.sql_validator)
# Imported inline to avoid triggering scrapers/__init__.py heavy imports
_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
_DANGEROUS_KEYWORDS = frozenset({
    'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE',
    'EXEC', 'EXECUTE', 'GRANT', 'REVOKE', 'UNION', 'INTO',
})


def _validate_identifier(name: str, context: str = "identifier") -> str:
    """Validate a SQL identifier to prevent injection."""
    if not isinstance(name, str) or not name:
        raise ValueError(f"SQL {context} must be a non-empty string, got {name!r}")
    if len(name) > 128:
        raise ValueError(f"SQL {context} too long: {len(name)} chars (max 128)")
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL {context}: '{name}'. Must match [a-zA-Z_][a-zA-Z0-9_]*")
    if name.upper() in _DANGEROUS_KEYWORDS:
        raise ValueError(f"SQL {context} '{name}' is a reserved keyword")
    return name

logger = logging.getLogger(__name__)


def _get_trino_connection(
    host: str = None,
    port: int = None,
    catalog: str = 'iceberg',
    session_properties: Optional[Dict[str, str]] = None,
) -> trino_lib.dbapi.Connection:
    """Create a Trino DBAPI connection.

    Supports two modes based on environment:
    - TRINO_PASSWORD set: HTTPS with basic auth (production)
    - TRINO_PASSWORD not set: HTTP without auth (development/no certs)
    """
    host = host or os.environ.get('TRINO_HOST', 'trino')
    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')

    # Disable Trino dynamic filtering for all pipeline transforms. Iceberg's
    # manifest Evaluator throws NullPointerException in CharSequenceSet.contains
    # when a join produces a dynamic filter ``<string_col> IN (..., NULL)`` that
    # is pushed into an Iceberg string-column scan (reproduced deterministically
    # on gold.fct_team_season_stats_audit reading silver.xref_team). DF is a pure
    # optimization, so disabling it preserves correctness — the only cost is
    # un-pruned probe-side scans on these batch-sized transforms.
    effective_session_properties = {'enable_dynamic_filtering': 'false'}
    if session_properties:
        effective_session_properties.update(session_properties)

    if password:
        port = port or int(os.environ.get('TRINO_PORT', 8443))
        return trino_lib.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            http_scheme='https',
            auth=trino_lib.auth.BasicAuthentication(user, password),
            verify=False,
            session_properties=effective_session_properties,
        )

    port = port or int(os.environ.get('TRINO_PORT', 8080))
    logger.info("TRINO_PASSWORD not set, connecting via HTTP (no auth)")
    return trino_lib.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        session_properties=effective_session_properties,
    )


def _execute(conn: trino_lib.dbapi.Connection, sql: str, fetch: bool = False):
    """Execute SQL and consume results to prevent USER_CANCELED."""
    cursor = conn.cursor()
    try:
        logger.debug(f"Executing SQL: {sql[:200]}")
        cursor.execute(sql)
        if fetch:
            return cursor.fetchall()
        # Consume results for DDL/DML to ensure query completes. Do NOT swallow
        # errors here (#265): a failing CTAS must surface its real error, not be
        # masked into a downstream TABLE_NOT_FOUND.
        cursor.fetchall()
        return None
    finally:
        cursor.close()


def _is_positional_schema_drift(exc: Exception) -> bool:
    """True for the Trino ``ICEBERG_COMMIT_ERROR`` raised when ``CREATE OR
    REPLACE`` matches a CHANGED column set POSITIONALLY against an existing
    partitioned table (#741).

    Signature: ``ICEBERG_COMMIT_ERROR: The following columns have types
    incompatible with the existing columns in their respective positions: ...``.

    Deliberately narrow — it must NOT match concurrent/transient commit
    conflicts (also ``ICEBERG_COMMIT_ERROR``), because the heal path DROPs the
    live table and a DROP+retry on a transient conflict could destroy a table
    another writer legitimately needs.
    """
    msg = str(exc)
    return "ICEBERG_COMMIT_ERROR" in msg and "respective positions" in msg


def run_silver_transform(
    sql_file: str,
    table_name: str,
    schema: str = 'silver',
    catalog: str = 'iceberg',
    partition_columns: Optional[List[str]] = None,
    trino_host: str = None,
    trino_port: int = None,
    add_timestamp: bool = True,
) -> Dict[str, Any]:
    """
    Execute a Silver-layer transformation via an atomic ``CREATE OR REPLACE`` (#265).

    Rebuilds the target table with a single ``CREATE OR REPLACE TABLE ... AS
    SELECT`` (Trino 479 Iceberg). The new schema + data are committed in one
    metastore transaction, so readers see either the old or the new table —
    never a missing one, and a failing SELECT leaves the EXISTING table intact.

    This replaces the #191 staging swap (``CREATE {table}_new`` → DROP live →
    RENAME), which had two flaws: (a) the RENAME could hit a transient
    ``TABLE_NOT_FOUND`` on ``{table}_new`` from metastore visibility lag (#265),
    and (b) once the live table was dropped, a RENAME failure left no table at
    all. ``CREATE OR REPLACE`` removes both by construction. (The old flow
    itself replaced DROP-then-CREATE, which lost ``gold.dim_player_attributes``
    in #180.)

    Steps:
        1. Read the SELECT query from the SQL file
        2. CREATE SCHEMA IF NOT EXISTS iceberg.{schema}
        3. CREATE OR REPLACE TABLE iceberg.{schema}.{table_name}
           WITH (partitioning = ARRAY['league', 'season']) AS {sql}
        4. Log the resulting row count

    On failure the live table is left untouched (CREATE OR REPLACE is atomic at
    the Iceberg commit level) and the error is surfaced as a RuntimeError. The
    one exception is positional schema drift (#741): when the SQL added/removed/
    reordered columns vs the existing partitioned table, Trino's positional
    commit fails with ``ICEBERG_COMMIT_ERROR`` — that single case is auto-healed
    by a DROP + rebuild (brief, schema-change-only non-atomic window).

    Args:
        sql_file: Path to SQL file containing the SELECT query.
                  Can be absolute or relative to /opt/airflow/.
        table_name: Target table name (e.g. 'fbref_player_season_profile')
        schema: Target schema (default: 'silver')
        catalog: Iceberg catalog name (default: 'iceberg')
        partition_columns: Partition columns (default: ['league', 'season'])
        trino_host: Trino coordinator host (default from env or 'trino')
        trino_port: Trino coordinator port (default from env or 8080)

    Returns:
        Dictionary with execution results:
            - table: full table name
            - rows: number of rows created
            - status: 'success' or 'failed'
            - error: error message if failed

    Raises:
        FileNotFoundError: If SQL file does not exist
        RuntimeError: If Trino execution fails
    """
    if partition_columns is None:
        partition_columns = ['league', 'season']

    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")
    _validate_identifier(table_name, "table")
    for pc in partition_columns:
        _validate_identifier(pc, "partition column")

    full_table = f"{catalog}.{schema}.{table_name}"
    result = {
        'table': full_table,
        'rows': 0,
        'status': 'pending',
        'error': None,
    }

    # --- 1. Read SQL file ---
    sql_path = _resolve_sql_path(sql_file)
    logger.info(f"Reading SQL from {sql_path}")
    select_sql = sql_path.read_text(encoding='utf-8').strip()

    if not select_sql:
        raise ValueError(f"SQL file is empty: {sql_path}")

    # Remove trailing semicolon if present (Trino CTAS doesn't need it)
    if select_sql.endswith(';'):
        select_sql = select_sql[:-1].rstrip()

    # --- 2. Connect to Trino ---
    conn = _get_trino_connection(host=trino_host, port=trino_port, catalog=catalog)

    try:
        # --- 3. Ensure schema exists ---
        _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        logger.info(f"Schema ensured: {catalog}.{schema}")

        # --- 4. Atomic rebuild via CREATE OR REPLACE TABLE (#265) ---
        # A single statement commits the new schema + data in one metastore
        # transaction. There is no staging table and no DROP-then-RENAME window,
        # so the TABLE_NOT_FOUND visibility flake and the post-DROP data-loss
        # window of the #191 swap are both gone.
        #
        # Caveat (#741): for an EXISTING partitioned Iceberg table, Trino matches
        # the new SELECT against the old schema POSITIONALLY — it does NOT write a
        # fresh schema wholesale. If the SQL added/removed/reordered columns vs the
        # physical table, the commit fails with ICEBERG_COMMIT_ERROR ("... in their
        # respective positions"). That case is auto-healed below (DROP + rebuild on
        # empty ground); every other failure leaves the live table intact.
        partition_clause = ''
        if partition_columns:
            cols = ", ".join(f"'{c}'" for c in partition_columns)
            partition_clause = f"WITH (partitioning = ARRAY[{cols}])\n"

        # Transforms re-selecting a table that already carries _silver_created_at
        # (via m.*) would raise DUPLICATE_COLUMN_NAME on re-add. Such callers
        # pass add_timestamp=False.
        if add_timestamp:
            ctas_sql = (
                f"CREATE OR REPLACE TABLE {full_table}\n"
                f"{partition_clause}"
                f"AS\n"
                f"SELECT *, CURRENT_TIMESTAMP AS _silver_created_at\n"
                f"FROM (\n"
                f"{select_sql}\n"
                f")"
            )
        else:
            ctas_sql = (
                f"CREATE OR REPLACE TABLE {full_table}\n"
                f"{partition_clause}"
                f"AS\n"
                f"{select_sql}"
            )

        logger.info(f"Executing CREATE OR REPLACE TABLE {full_table} ...")
        try:
            _execute(conn, ctas_sql)
        except Exception as e:
            # #741: positional schema drift — the SQL added/reordered columns vs
            # the stale physical table. Auto-heal: DROP the table and re-run the
            # SAME CTAS on empty ground, where there is no old schema to match
            # positionally. The original CTAS already passed planning/execution
            # (the error is raised at commit time), so the retry is near-certain
            # to succeed. This briefly drops the table (small, rare, schema-change
            # -only window); all NON-positional failures re-raise untouched so the
            # live table stays intact (#265 atomicity preserved for those).
            if not _is_positional_schema_drift(e):
                raise
            logger.warning(
                "Schema drift on %s (positional ICEBERG_COMMIT_ERROR) — auto-healing: "
                "DROP TABLE + rebuild from scratch (#741). Original error: %s",
                full_table, e,
            )
            _execute(conn, f"DROP TABLE IF EXISTS {full_table}")
            _execute(conn, ctas_sql)

        # --- 5. Count rows ---
        count_result = _execute(
            conn,
            f"SELECT COUNT(*) FROM {full_table}",
            fetch=True,
        )
        row_count = count_result[0][0] if count_result else 0

        result['rows'] = row_count
        result['status'] = 'success'
        logger.info(f"Silver transform complete: {full_table} => {row_count} rows")

    except Exception as e:
        # CREATE OR REPLACE is atomic at the Iceberg commit level — a failing
        # SELECT leaves the existing table intact, so there is nothing to clean
        # up. Surface the real error (#265).
        result['status'] = 'failed'
        result['error'] = str(e)
        logger.error(f"Silver transform FAILED for {full_table}: {e}")
        raise RuntimeError(f"Silver transform failed for {full_table}: {e}") from e

    finally:
        conn.close()

    return result


_PARTITION_STAGED_SESSION_PROPERTIES = {
    # Bound full-corpus Iceberg writer fan-out. These Trino 479 session
    # properties were verified through SHOW SESSION in the runtime image.
    'task_concurrency': '4',
    'task_max_writer_count': '4',
    'scale_writers': 'false',
    'min_hash_partition_count_for_write': '4',
    'redistribute_writes': 'false',
}


def run_silver_transform_partition_staged(
    sql_file: str,
    table_name: str,
    source_table: str,
    source_version_sql: str,
    schema: str = 'silver',
    catalog: str = 'iceberg',
    partition_columns: Optional[List[str]] = None,
    trino_host: str = None,
    trino_port: int = None,
    add_timestamp: bool = True,
) -> Dict[str, Any]:
    """Atomically rebuild a large transform through bounded partition inserts.

    Some SELECTs legitimately contain a window whose semantic partition is
    ``(league, season, game_id)``.  Running such a window over the complete
    28M-row corpus can exceed the coordinator heap even though each match is
    small.  This runner keeps the live table untouched while it:

      1. clears any failed-run staging residue;
      2. captures the source's complete, ordered logical commit set, then
         discovers and counts concrete ``(league, season)`` scopes;
      3. creates an empty stage and evaluates the SELECT once per scope;
      4. captures the commit set again, then proves exact source-version and
         staging/source row-count parity;
      5. atomically replaces the live table from the already materialised stage.

    The original SELECT must include every ``partition_columns`` value in its
    window ``PARTITION BY`` keys.  That makes the outer scope predicate safe to
    push below the window; callers protect this requirement with SQL-shape
    tests.  The final replace is a streaming Iceberg scan/write with no window.

    ``source_version_sql`` is mandatory and must be a read-only SELECT with a
    deterministic top-level ORDER BY. Its full result set is compared before
    and after the partition inserts. A count or hash is deliberately
    insufficient: replacing one manifest batch with another can preserve the
    source row count while producing a mixed-snapshot stage.

    The runner is intentionally single-writer: its deterministic stage name
    lets a retry clean residue even when a JVM OOM prevented in-process cleanup.
    ``dag_transform_e3`` enforces this with ``max_active_runs=1``. A failure
    before the final ``CREATE OR REPLACE`` drops only the staging table; the
    existing live table is never dropped. Unlike
    :func:`run_silver_transform`, this safety-oriented path deliberately does
    not auto-heal positional schema drift by dropping the live table.
    """
    if partition_columns is None:
        partition_columns = ['league', 'season']
    if not partition_columns:
        raise ValueError("partition_columns must be non-empty for staged rebuild")

    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")
    _validate_identifier(table_name, "table")
    for column in partition_columns:
        _validate_identifier(column, "partition column")

    source_parts = source_table.split('.')
    if len(source_parts) != 3:
        raise ValueError(
            "source_table must be a three-part catalog.schema.table name, "
            f"got {source_table!r}"
        )
    source_qualified = '.'.join(
        _validate_identifier(part, "source table component")
        for part in source_parts
    )

    if not isinstance(source_version_sql, str) or not source_version_sql.strip():
        raise ValueError("source_version_sql must be a non-empty ordered SELECT")
    source_version_sql = source_version_sql.strip()
    if source_version_sql.endswith(';'):
        source_version_sql = source_version_sql[:-1].rstrip()
    if ';' in source_version_sql:
        raise ValueError("source_version_sql must contain exactly one statement")
    if not re.match(r'^(?:SELECT|WITH)\b', source_version_sql, re.IGNORECASE):
        raise ValueError("source_version_sql must be a read-only SELECT")
    if not re.search(r'\bORDER\s+BY\b', source_version_sql, re.IGNORECASE):
        raise ValueError("source_version_sql must have a deterministic ORDER BY")

    sql_path = _resolve_sql_path(sql_file)
    select_sql = sql_path.read_text(encoding='utf-8').strip()
    if not select_sql:
        raise ValueError(f"SQL file is empty: {sql_path}")
    if select_sql.endswith(';'):
        select_sql = select_sql[:-1].rstrip()

    full_table = f"{catalog}.{schema}.{table_name}"
    staging_name = f"{table_name}_stage"
    _validate_identifier(staging_name, "staging table")
    staging_table = f"{catalog}.{schema}.{staging_name}"
    partition_clause = ''
    if partition_columns:
        cols = ", ".join(f"'{column}'" for column in partition_columns)
        partition_clause = f"WITH (partitioning = ARRAY[{cols}])\n"

    result: Dict[str, Any] = {
        'table': full_table,
        'rows': 0,
        'source_rows': 0,
        'partitions': 0,
        'status': 'pending',
        'error': None,
    }
    conn = _get_trino_connection(
        host=trino_host,
        port=trino_port,
        catalog=catalog,
        session_properties=_PARTITION_STAGED_SESSION_PROPERTIES,
    )
    staging_created = False
    try:
        _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        # A coordinator JVM OOM can outlive both the work connection and the
        # immediate reconnect attempt in finally. Because this runner is
        # single-writer, every retry can safely reclaim that deterministic
        # residue before doing any expensive source work.
        _execute(conn, f"DROP TABLE IF EXISTS {staging_table}")

        source_version_before = tuple(
            _execute(
                conn,
                source_version_sql,
                fetch=True,
            ) or []
        )
        if not source_version_before:
            raise RuntimeError(
                f"source version query returned no rows for {source_qualified}"
            )

        partition_select = ', '.join(partition_columns)
        grouped_scopes = _execute(
            conn,
            f"SELECT {partition_select}, COUNT(*) AS __row_count "
            f"FROM {source_qualified} "
            f"GROUP BY {partition_select} "
            + f" ORDER BY {', '.join(str(i) for i in range(1, len(partition_columns) + 1))}",
            fetch=True,
        )
        if not grouped_scopes:
            raise RuntimeError(f"partition source is empty: {source_qualified}")
        null_scopes = [
            row for row in grouped_scopes
            if any(value is None for value in row[:len(partition_columns)])
        ]
        if null_scopes:
            raise RuntimeError(
                f"partition source contains NULL scope values: {source_qualified}"
            )
        scopes = [row[:len(partition_columns)] for row in grouped_scopes]
        source_rows = sum(int(row[len(partition_columns)]) for row in grouped_scopes)
        if source_rows <= 0:
            raise RuntimeError(f"partition source is empty: {source_qualified}")
        result['source_rows'] = source_rows
        result['partitions'] = len(scopes)

        empty_select = f"SELECT * FROM (\n{select_sql}\n) AS __src WHERE FALSE"
        # Mark before execution: if Trino commits CREATE but the client loses
        # the response, DROP IF EXISTS in finally still cleans up this stage on
        # reconnect-capable failures.
        staging_created = True
        _execute(
            conn,
            f"CREATE TABLE {staging_table}\n{partition_clause}AS\n{empty_select}",
        )

        keys = list(partition_columns)
        for scope in scopes:
            values = [str(value) for value in scope]
            partition_filter = _build_silver_partition_filter(keys, values)
            insert_select = (
                f"SELECT * FROM (\n{select_sql}\n) AS __src "
                f"WHERE {partition_filter}"
            )
            logger.info(
                "Staged partition for %s: %s",
                full_table,
                partition_filter,
            )
            _execute(conn, f"INSERT INTO {staging_table}\n{insert_select}")

        staging_count_rows = _execute(
            conn,
            f"SELECT COUNT(*) FROM {staging_table}",
            fetch=True,
        )
        staging_rows = int(staging_count_rows[0][0]) if staging_count_rows else 0

        # Each scope is a separate statement and therefore can see a newer
        # Iceberg snapshot. Fail closed if ingestion changed the current view
        # during the rebuild instead of publishing a mixed-snapshot Silver.
        final_source_count_rows = _execute(
            conn,
            f"SELECT COUNT(*) FROM {source_qualified}",
            fetch=True,
        )
        final_source_rows = (
            int(final_source_count_rows[0][0])
            if final_source_count_rows else 0
        )
        # Read the logical version last, immediately before the fail-closed
        # checks and live replacement, to minimise the unguarded publish window.
        source_version_after = tuple(
            _execute(
                conn,
                source_version_sql,
                fetch=True,
            ) or []
        )
        if final_source_rows != source_rows:
            raise RuntimeError(
                f"source changed during staged rebuild for {full_table}: "
                f"before={source_rows}, after={final_source_rows}"
            )
        if staging_rows != final_source_rows:
            raise RuntimeError(
                f"staged row parity failed for {full_table}: "
                f"source={final_source_rows}, staged={staging_rows}"
            )
        if source_version_after != source_version_before:
            raise RuntimeError(
                f"source version changed during staged rebuild for {full_table}: "
                f"before_rows={len(source_version_before)}, "
                f"after_rows={len(source_version_after)}"
            )

        # Add the lineage timestamp once, during the final streaming copy. This
        # preserves the single-CTAS contract (one timestamp for the rebuild)
        # rather than assigning a different value to every staged scope.
        if add_timestamp:
            final_select = (
                "SELECT *, CURRENT_TIMESTAMP AS _silver_created_at "
                f"FROM {staging_table}"
            )
        else:
            final_select = f"SELECT * FROM {staging_table}"
        replace_sql = (
            f"CREATE OR REPLACE TABLE {full_table}\n"
            f"{partition_clause}AS\n{final_select}"
        )
        _execute(conn, replace_sql)

        live_count_rows = _execute(
            conn,
            f"SELECT COUNT(*) FROM {full_table}",
            fetch=True,
        )
        live_rows = int(live_count_rows[0][0]) if live_count_rows else 0
        if live_rows != source_rows:
            raise RuntimeError(
                f"live row parity failed for {full_table}: "
                f"source={source_rows}, live={live_rows}"
            )

        result['rows'] = live_rows
        result['status'] = 'success'
        logger.info(
            "Partition-staged transform complete: %s => %d rows across %d scopes",
            full_table,
            live_rows,
            len(scopes),
        )
    except Exception as exc:
        result['status'] = 'failed'
        result['error'] = str(exc)
        logger.error("Partition-staged transform FAILED for %s: %s", full_table, exc)
        raise RuntimeError(
            f"Partition-staged transform failed for {full_table}: {exc}"
        ) from exc
    finally:
        if staging_created:
            try:
                _execute(conn, f"DROP TABLE IF EXISTS {staging_table}")
            except Exception as cleanup_exc:
                # The query that failed may also have invalidated its DBAPI
                # connection. Retry cleanup once through a fresh connection so
                # an ordinary transport failure does not leave stage residue.
                logger.warning(
                    "Staging cleanup failed on the work connection for %s; "
                    "retrying through a fresh connection: %s",
                    staging_table,
                    cleanup_exc,
                )
                cleanup_conn = None
                try:
                    cleanup_conn = _get_trino_connection(
                        host=trino_host,
                        port=trino_port,
                        catalog=catalog,
                        session_properties=_PARTITION_STAGED_SESSION_PROPERTIES,
                    )
                    _execute(
                        cleanup_conn,
                        f"DROP TABLE IF EXISTS {staging_table}",
                    )
                except Exception:
                    logger.exception(
                        "Could not drop staging table %s through a fresh connection",
                        staging_table,
                    )
                finally:
                    if cleanup_conn is not None:
                        cleanup_conn.close()
        conn.close()

    return result


# ---------------------------------------------------------------------------
# E3.5: per-partition INSERT runner for Silver (wrapper-style)
# ---------------------------------------------------------------------------
#
# DESIGN
# ------
# Mirrors ``gold_tasks.run_gold_partition_insert_wrapped`` but for Silver tables.
# Used by ``dag_e3_backfill`` to materialise a single (season, league) slice
# of ``silver.whoscored_events_spadl`` and ``silver.espn_lineup`` without
# touching other partitions. Full rebuild strategy is configured separately
# by the production E3 DAG; SPADL uses the bounded staged runner above.
#
# Two engines are offered:
#   * ``run_silver_partition_insert``           (this function) — *wrapper*
#     style. Reads the original SELECT verbatim, then wraps it as
#     ``SELECT * FROM (<orig>) WHERE league=... AND season=...``. ZERO
#     modification of the SQL files needed — works on every existing
#     Silver SELECT. Slightly less efficient than sentinel-based pushdown
#     (the inner SELECT scans all bronze rows then filters at the outer
#     layer; Trino's optimiser usually pushes the predicate down anyway).
#   * Sentinel-style would inject the WHERE inside the SELECT body via the
#     ``-- WHERE_PARTITION_FILTER_HERE`` marker (see ``gold_tasks``).
#     Skipped here because Silver SQL files don't have the sentinel yet —
#     wrapper approach is unblocked and equally correct.
#
# IDEMPOTENCY
# -----------
# The runner ALWAYS performs DELETE-FROM ... WHERE <partition_filter> before
# the INSERT, so re-running the task for the same (season, league) tuple
# produces the same final state. The DDL is run once with IF NOT EXISTS;
# *if* the target table doesn't exist yet, we fall back to creating it via
# ``run_silver_transform`` first (a one-time bootstrap path) so the runner
# always has a target to INSERT into.


# Hard cap on partition value length — same shield as Gold.
_MAX_PARTITION_VALUE_LEN_SILVER = 128


def _safe_silver_value(value: str, key: str) -> str:
    """Escape a partition VALUE for inline use in a Trino predicate.

    Kept inline (rather than shared with gold_tasks) to avoid a circular
    ``silver_tasks → gold_tasks`` import.
    """
    if not isinstance(value, str):
        raise ValueError(
            f"partition value for {key!r} must be str, got {type(value).__name__}"
        )
    if not value:
        raise ValueError(f"partition value for {key!r} must be non-empty")
    if len(value) > _MAX_PARTITION_VALUE_LEN_SILVER:
        raise ValueError(
            f"partition value for {key!r} too long: "
            f"{len(value)} chars (max {_MAX_PARTITION_VALUE_LEN_SILVER})"
        )
    if any(ch in value for ch in ('\x00', '\n', '\r', '\t', ';')):
        raise ValueError(
            f"partition value for {key!r} contains forbidden chars: {value!r}"
        )
    if '--' in value or '/*' in value or '*/' in value:
        raise ValueError(
            f"partition value for {key!r} contains SQL comment marker: {value!r}"
        )
    return value.replace("'", "''")


def _build_silver_partition_filter(
    partition_keys: List[str],
    partition_values: List[str],
) -> str:
    """Build ``league = 'X' AND season = 'Y'`` predicate (no leading WHERE)."""
    if len(partition_keys) != len(partition_values):
        raise ValueError(
            f"partition_keys / values length mismatch: "
            f"{len(partition_keys)} vs {len(partition_values)}"
        )
    parts: List[str] = []
    for k, v in zip(partition_keys, partition_values):
        _validate_identifier(k, "partition column")
        parts.append(f"{k} = '{_safe_silver_value(v, k)}'")
    return " AND ".join(parts)


def run_silver_partition_insert(
    sql_file: str,
    table_name: str,
    partition_values: Dict[str, str],
    schema: str = 'silver',
    catalog: str = 'iceberg',
    partition_columns: Optional[List[str]] = None,
    trino_host: str = None,
    trino_port: int = None,
    add_timestamp: bool = True,
) -> Dict[str, Any]:
    """Idempotent per-partition INSERT for a Silver table (wrapper-style).

    Used by ``dag_e3_backfill`` to materialise a single (league, season) slice
    of Silver E3 tables without touching other partitions.

    Flow:
      1. CREATE SCHEMA IF NOT EXISTS — idempotent.
      2. If target table doesn't exist yet, fall back to
         :func:`run_silver_transform` to bootstrap it (atomic CTAS for the
         partition only — produces a single-partition table that subsequent
         partition-runs append to).
      3. DELETE FROM <table> WHERE <partition_filter>  — idempotency.
      4. INSERT INTO <table>
           SELECT *, CURRENT_TIMESTAMP AS _silver_created_at
             FROM (<orig SELECT>) AS __src
            WHERE <partition_filter>
      5. Return row count for the partition.

    Parameters
    ----------
    sql_file : str
        Path to the SELECT-only SQL file (no sentinel needed).
    table_name : str
        Target Silver table (e.g. 'whoscored_events_spadl').
    partition_values : Dict[str, str]
        Concrete values for partition columns, e.g. ``{'league': 'ENG-Premier League',
        'season': '2324'}``.
    schema, catalog : str
        Iceberg target (default 'silver' / 'iceberg').
    partition_columns : Optional[List[str]]
        Partition column names (default ``['league', 'season']``).
    add_timestamp : bool
        Whether to append ``CURRENT_TIMESTAMP AS _silver_created_at`` to the
        wrapped SELECT — must match what the original CTAS would do, otherwise
        downstream ``_silver_created_at IS NOT NULL`` DQ checks regress.

    Returns
    -------
    Dict[str, Any]
        ``{table, rows_inserted, partition, status, bootstrap}``.
        ``bootstrap=True`` means the table was created in this call (no prior
        rows for the other partitions existed).
    """
    if partition_columns is None:
        partition_columns = ['league', 'season']

    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")
    _validate_identifier(table_name, "table")
    for pc in partition_columns:
        _validate_identifier(pc, "partition column")

    # Validate every partition value is provided
    if set(partition_values.keys()) != set(partition_columns):
        raise ValueError(
            f"partition_values keys must equal partition_columns. "
            f"Got keys={sorted(partition_values.keys())}, "
            f"expected={sorted(partition_columns)}"
        )

    full_table = f"{catalog}.{schema}.{table_name}"
    keys_ordered = list(partition_columns)
    values_ordered = [partition_values[k] for k in keys_ordered]
    partition_filter = _build_silver_partition_filter(keys_ordered, values_ordered)
    partition_label = ", ".join(f"{k}={v!r}" for k, v in zip(keys_ordered, values_ordered))

    # Read SELECT body
    sql_path = _resolve_sql_path(sql_file)
    select_sql = sql_path.read_text(encoding='utf-8').strip()
    if not select_sql:
        raise ValueError(f"SQL file is empty: {sql_path}")
    if select_sql.endswith(';'):
        select_sql = select_sql[:-1].rstrip()

    result: Dict[str, Any] = {
        'table': full_table,
        'partition': dict(partition_values),
        'rows_inserted': 0,
        'status': 'pending',
        'bootstrap': False,
    }

    conn = _get_trino_connection(host=trino_host, port=trino_port, catalog=catalog)
    try:
        # 1. Ensure schema exists.
        _execute(conn, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        # 2. Check if target table exists. If not, bootstrap via run_silver_transform
        #    (CTAS the slice for THIS partition only). The wrapped INSERT path needs
        #    a target table to write to.
        exists_rows = _execute(
            conn,
            f"SHOW TABLES FROM {catalog}.{schema} LIKE '{table_name}'",
            fetch=True,
        )
        target_exists = bool(exists_rows and len(exists_rows) > 0)

        if not target_exists:
            logger.warning(
                "%s does not exist — bootstrapping via run_silver_transform "
                "scoped to the requested partition (%s).",
                full_table, partition_label,
            )
            # Bootstrap: a fresh CTAS scoped to (this partition). We synthesise
            # a wrapped SELECT so that the freshly-created table only contains
            # the requested partition. Subsequent partition runs will INSERT.
            wrapped = (
                f"SELECT * FROM (\n{select_sql}\n) AS __src\n"
                f"WHERE {partition_filter}"
            )
            partition_clause = ''
            if partition_columns:
                cols = ", ".join(f"'{c}'" for c in partition_columns)
                partition_clause = f"WITH (partitioning = ARRAY[{cols}])\n"

            if add_timestamp:
                ctas_sql = (
                    f"CREATE TABLE {full_table}\n"
                    f"{partition_clause}"
                    f"AS\n"
                    f"SELECT *, CURRENT_TIMESTAMP AS _silver_created_at\n"
                    f"FROM (\n{wrapped}\n)"
                )
            else:
                ctas_sql = (
                    f"CREATE TABLE {full_table}\n"
                    f"{partition_clause}"
                    f"AS\n"
                    f"{wrapped}"
                )
            logger.info("Bootstrap CTAS for %s [%s]", full_table, partition_label)
            _execute(conn, ctas_sql)
            result['bootstrap'] = True
        else:
            # 3. Idempotency DELETE — wipe any prior rows for this partition.
            delete_sql = f"DELETE FROM {full_table} WHERE {partition_filter}"
            logger.info("DELETE (idempotency): %s", delete_sql)
            _execute(conn, delete_sql)

            # 4. INSERT INTO ... SELECT, wrapped + filtered.
            wrapped = (
                f"SELECT * FROM (\n{select_sql}\n) AS __src\n"
                f"WHERE {partition_filter}"
            )
            if add_timestamp:
                insert_select = (
                    f"SELECT *, CURRENT_TIMESTAMP AS _silver_created_at "
                    f"FROM (\n{wrapped}\n) AS __src_ts"
                )
            else:
                insert_select = wrapped
            insert_sql = f"INSERT INTO {full_table}\n{insert_select}"
            logger.info("INSERT into %s for %s", full_table, partition_label)
            _execute(conn, insert_sql)

        # 5. Row count for THIS partition (observability).
        cnt_rows = _execute(
            conn,
            f"SELECT COUNT(*) FROM {full_table} WHERE {partition_filter}",
            fetch=True,
        )
        rows_inserted = cnt_rows[0][0] if cnt_rows else 0
        result['rows_inserted'] = int(rows_inserted)
        result['status'] = 'success'
        logger.info(
            "Silver partition INSERT done: %s [%s] => %d rows",
            full_table, partition_label, rows_inserted,
        )

    except Exception as e:
        result['status'] = 'failed'
        result['error'] = str(e)
        logger.error(
            "Silver partition INSERT FAILED for %s [%s]: %s",
            full_table, partition_label, e,
        )
        raise RuntimeError(
            f"Silver partition INSERT failed for {full_table} [{partition_label}]: {e}"
        ) from e
    finally:
        conn.close()

    return result


def check_bronze_table_exists(
    table_name: str,
    schema: str = 'bronze',
    catalog: str = 'iceberg',
    trino_host: str = None,
    trino_port: int = None,
) -> bool:
    """Check if a Bronze table exists before attempting a Silver transform.

    Used for optional Bronze tables (e.g. fbref_shot_events) that may not
    have been populated yet by the ingestion pipeline.

    Returns:
        True if the table exists, False otherwise.
    """
    _validate_identifier(catalog, "catalog")
    _validate_identifier(schema, "schema")
    _validate_identifier(table_name, "table")

    conn = _get_trino_connection(host=trino_host, port=trino_port, catalog=catalog)
    try:
        result = _execute(
            conn,
            f"SHOW TABLES FROM {catalog}.{schema} LIKE '{table_name}'",
            fetch=True,
        )
        exists = bool(result and len(result) > 0)
        logger.info(
            f"Bronze table check: {catalog}.{schema}.{table_name} "
            f"{'exists' if exists else 'NOT FOUND'}"
        )
        return exists
    except Exception as e:
        logger.warning(f"Bronze table check failed for {table_name}: {e}")
        return False
    finally:
        conn.close()


def _resolve_sql_path(sql_file: str) -> Path:
    """
    Resolve SQL file path.

    Supports:
        - Absolute paths: /opt/airflow/dags/sql/silver/...
        - Relative paths: dags/sql/silver/...
          (resolved against /opt/airflow/ in container or project root)

    Returns:
        Resolved Path object

    Raises:
        FileNotFoundError: If the file does not exist at any candidate path
    """
    path = Path(sql_file)

    if path.is_absolute() and path.exists():
        return path

    # Try common base directories
    candidates = [
        path,
        Path('/opt/airflow') / path,
        Path('/opt/airflow') / 'dags' / path.name,
        Path('/opt/airflow/dags/sql/silver') / path.name,
    ]

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        f"SQL file not found: {sql_file}. "
        f"Tried: {[str(c) for c in candidates]}"
    )


def validate_silver_tables(
    tables: Dict[str, int],
    min_rows: int = 1,
) -> Dict[str, Any]:
    """
    Validate Silver tables after transformation.

    Args:
        tables: Dictionary mapping table_name to expected minimum row count.
                Use 0 for tables that may legitimately be empty.
        min_rows: Default minimum row count for tables not in the dict.

    Returns:
        Validation result dictionary with status, warnings, and details.
    """
    conn = _get_trino_connection()

    validation = {
        'status': 'success',
        'warnings': [],
        'details': {},
    }

    try:
        for table_name, threshold in tables.items():
            _validate_identifier(table_name, "table")
            full_table = f"iceberg.silver.{table_name}"
            try:
                count_result = _execute(
                    conn,
                    f"SELECT COUNT(*) FROM {full_table}",
                    fetch=True,
                )
                row_count = count_result[0][0] if count_result else 0
                validation['details'][table_name] = row_count

                effective_threshold = threshold if threshold > 0 else min_rows
                if row_count < effective_threshold:
                    warning = (
                        f"{table_name}: {row_count} rows "
                        f"(expected >= {effective_threshold})"
                    )
                    validation['warnings'].append(warning)
                    logger.warning(f"Silver validation: {warning}")
                else:
                    logger.info(f"Silver validation OK: {table_name} => {row_count} rows")

            except Exception as e:
                validation['warnings'].append(f"{table_name}: query failed ({e})")
                validation['details'][table_name] = -1
                logger.error(f"Silver validation error for {table_name}: {e}")

    finally:
        conn.close()

    if validation['warnings']:
        validation['status'] = 'partial_success'

    total = sum(v for v in validation['details'].values() if v > 0)
    validation['total_rows'] = total
    logger.info(
        f"Silver validation complete: {validation['status']} "
        f"({total} total rows across {len(tables)} tables)"
    )

    return validation
