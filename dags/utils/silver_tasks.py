"""
Silver Transformation Tasks
============================

Utility functions for executing Silver-layer CTAS transformations via Trino.

Uses the `trino` Python library directly (NOT scrapers.base.trino_manager)
to avoid importing the entire scrapers package with its heavy dependencies
(nodriver, selenium, soccerdata, curl_cffi ~1.5GB RAM).

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
) -> trino_lib.dbapi.Connection:
    """Create a Trino DBAPI connection.

    Supports two modes based on environment:
    - TRINO_PASSWORD set: HTTPS with basic auth (production)
    - TRINO_PASSWORD not set: HTTP without auth (development/no certs)
    """
    host = host or os.environ.get('TRINO_HOST', 'trino')
    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')

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
        )

    port = port or int(os.environ.get('TRINO_PORT', 8080))
    logger.info("TRINO_PASSWORD not set, connecting via HTTP (no auth)")
    return trino_lib.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
    )


def _execute(conn: trino_lib.dbapi.Connection, sql: str, fetch: bool = False):
    """Execute SQL and consume results to prevent USER_CANCELED."""
    cursor = conn.cursor()
    try:
        logger.debug(f"Executing SQL: {sql[:200]}")
        cursor.execute(sql)
        if fetch:
            return cursor.fetchall()
        # Consume results for DDL/DML to ensure query completes
        try:
            cursor.fetchall()
        except Exception:
            pass
        return None
    finally:
        cursor.close()


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
    Execute a Silver-layer transformation: DROP existing table + CTAS.

    Steps:
        1. Read the SELECT query from the SQL file
        2. CREATE SCHEMA IF NOT EXISTS iceberg.{schema}
        3. DROP TABLE IF EXISTS iceberg.{schema}.{table_name}
        4. CREATE TABLE iceberg.{schema}.{table_name}
           WITH (partitioning = ARRAY['league', 'season']) AS {sql}
        5. Log the resulting row count

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

        # --- 4. DROP TABLE IF EXISTS ---
        logger.info(f"Dropping table if exists: {full_table}")
        _execute(conn, f"DROP TABLE IF EXISTS {full_table}")

        # --- 5. CREATE TABLE AS SELECT ---
        partition_clause = ''
        if partition_columns:
            cols = ", ".join(f"'{c}'" for c in partition_columns)
            partition_clause = f"WITH (partitioning = ARRAY[{cols}])\n"

        # Gold-on-Gold transforms (e.g. fct_match_train SELECT m.* FROM gold.fct_match)
        # already carry _silver_created_at via m.* — re-adding it would raise
        # DUPLICATE_COLUMN_NAME. Such callers pass add_timestamp=False.
        if add_timestamp:
            ctas_sql = (
                f"CREATE TABLE {full_table}\n"
                f"{partition_clause}"
                f"AS\n"
                f"SELECT *, CURRENT_TIMESTAMP AS _silver_created_at\n"
                f"FROM (\n"
                f"{select_sql}\n"
                f")"
            )
        else:
            ctas_sql = (
                f"CREATE TABLE {full_table}\n"
                f"{partition_clause}"
                f"AS\n"
                f"{select_sql}"
            )

        logger.info(f"Executing CTAS for {full_table} ...")
        _execute(conn, ctas_sql)

        # --- 6. Count rows ---
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
        result['status'] = 'failed'
        result['error'] = str(e)
        logger.error(f"Silver transform FAILED for {full_table}: {e}")
        raise RuntimeError(f"Silver transform failed for {full_table}: {e}") from e

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


# ---------------------------------------------------------------------------
# Data Quality Checks
# ---------------------------------------------------------------------------
#
# The Silver DAG (`dag_transform_fbref_silver._validate_silver_quality`) is the
# canonical entry point and uses the typed `CHECK` API from `data_quality.py`
# with ERROR severity for PK / ref_integrity violations so dirty data never
# reaches Gold.
#
# `QUALITY_CHECKS` and `validate_silver_quality()` below remain for ad-hoc
# operational use (e.g. shell, REPL, manual reruns) and now mirror the DAG:
# critical checks raise, freshness / ranges are WARNING.

# Freshness threshold: ingestion runs weekly (Monday). 48h covers the
# post-ingest grace window; mid-week staleness is normal and stays WARNING.
_FRESH_HOURS = 48


def _build_silver_checks(schema: str = 'silver'):
    """Construct the canonical Silver DQ check list.

    Imported lazily so `silver_tasks` keeps working in callers that don't
    have `data_quality` on the path (e.g. unit tests that mock the module).
    """
    from utils.data_quality import CHECK

    return [
        # ---- ERROR: PK NULLs (joins / dedup logic break otherwise) ----
        CHECK.no_nulls(f'{schema}.fbref_match_enriched',        cols=['match_id', 'date']),
        CHECK.no_nulls(f'{schema}.fbref_player_season_profile', cols=['player_id', 'league', 'season']),
        CHECK.no_nulls(f'{schema}.fbref_keeper_profile',        cols=['player_id', 'league', 'season']),
        CHECK.no_nulls(f'{schema}.fbref_player_match_stats',    cols=['match_id']),
        CHECK.no_nulls(f'{schema}.fbref_match_events',          cols=['match_id']),
        CHECK.no_nulls(f'{schema}.fbref_match_lineups',         cols=['match_id', 'player_id']),
        CHECK.no_nulls(f'{schema}.fbref_team_season_profile',   cols=['team', 'league', 'season']),

        # ---- ERROR: PK uniqueness (duplicates explode downstream facts) ----
        CHECK.no_duplicates(f'{schema}.fbref_match_enriched',        pk=['match_id']),
        CHECK.no_duplicates(f'{schema}.fbref_player_season_profile', pk=['player_id', 'league', 'season']),
        CHECK.no_duplicates(f'{schema}.fbref_keeper_profile',        pk=['player_id', 'league', 'season']),
        CHECK.no_duplicates(f'{schema}.fbref_match_lineups',         pk=['match_id', 'player_id']),
        CHECK.no_duplicates(f'{schema}.fbref_team_season_profile',   pk=['team', 'league', 'season']),
        CHECK.no_duplicates(
            f'{schema}.fbref_player_match_stats',
            pk=['match_id', 'player_id', 'team'],
            where='player_id IS NOT NULL',
        ),
        CHECK.no_duplicates(
            f'{schema}.fbref_match_events',
            pk=['match_id', 'minute', 'player_id', 'event_type'],
            where='player_id IS NOT NULL',
        ),

        # ---- ERROR: Referential integrity (orphans drop silently in joins) ----
        CHECK.ref_integrity(f'{schema}.fbref_player_match_stats', f'{schema}.fbref_match_enriched', 'match_id'),
        CHECK.ref_integrity(f'{schema}.fbref_match_events',       f'{schema}.fbref_match_enriched', 'match_id'),
        CHECK.ref_integrity(f'{schema}.fbref_match_lineups',      f'{schema}.fbref_match_enriched', 'match_id'),

        # ---- WARNING: Freshness (weekly ingest; >48h is normal mid-week) ----
        CHECK.freshness(f'{schema}.fbref_match_enriched',        ts_col='_bronze_ingested_at',
                        max_age_hours=_FRESH_HOURS, severity='WARNING'),
        CHECK.freshness(f'{schema}.fbref_player_season_profile', ts_col='_bronze_ingested_at',
                        max_age_hours=_FRESH_HOURS, severity='WARNING'),
        CHECK.freshness(f'{schema}.fbref_keeper_profile',        ts_col='_bronze_ingested_at',
                        max_age_hours=_FRESH_HOURS, severity='WARNING'),
        CHECK.freshness(f'{schema}.fbref_player_match_stats',    ts_col='_bronze_ingested_at',
                        max_age_hours=_FRESH_HOURS, severity='WARNING'),
        CHECK.freshness(f'{schema}.fbref_match_events',          ts_col='_bronze_ingested_at',
                        max_age_hours=_FRESH_HOURS, severity='WARNING'),
        CHECK.freshness(f'{schema}.fbref_match_lineups',         ts_col='_bronze_ingested_at',
                        max_age_hours=_FRESH_HOURS, severity='WARNING'),
        CHECK.freshness(f'{schema}.fbref_team_season_profile',   ts_col='_bronze_ingested_at',
                        max_age_hours=_FRESH_HOURS, severity='WARNING'),

        # ---- WARNING: Value ranges (legitimate outliers possible) ----
        CHECK.value_range(f'{schema}.fbref_player_season_profile', 'goals',
                          min_val=0, severity='WARNING'),
        CHECK.value_range(f'{schema}.fbref_player_season_profile', 'minutes',
                          min_val=0, max_val=5000, severity='WARNING'),
        CHECK.value_range(f'{schema}.fbref_keeper_profile', 'save_pct',
                          min_val=0, max_val=100, severity='WARNING'),
        CHECK.value_range(f'{schema}.fbref_team_season_profile', 'possession',
                          min_val=0, max_val=100, severity='WARNING'),
        CHECK.value_range(f'{schema}.fbref_team_season_profile', 'goals',
                          min_val=0, severity='WARNING'),
    ]


def validate_silver_quality(
    checks: Optional[List[Any]] = None,
    schema: str = 'silver',
    raise_on_error: bool = True,
) -> Dict[str, Any]:
    """Run Silver DQ checks via the universal `data_quality` framework.

    PK NULLs / uniqueness / referential integrity are ERROR severity and
    raise ``AirflowException`` (or ``RuntimeError`` outside Airflow) on
    failure. Freshness and value-range violations are WARNING and only logged.

    Args:
        checks: Override the default check list (must be `Check` instances).
        schema: Iceberg schema name (default 'silver').
        raise_on_error: Re-raise on ERROR-severity failures (default True).

    Returns:
        Dict with `passed`, `total`, `errors`, `warnings`.
    """
    from utils.data_quality import run_checks

    if checks is None:
        checks = _build_silver_checks(schema=schema)

    report = run_checks(checks, raise_on_error=raise_on_error)
    return {
        'passed': len(report.passed),
        'total': len(report.results),
        'errors': [r.name for r in report.errors],
        'warnings': [r.name for r in report.warnings],
    }


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
