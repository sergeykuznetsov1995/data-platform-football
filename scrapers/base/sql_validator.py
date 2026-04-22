"""
SQL Validator
=============

Validates SQL identifiers and filter expressions to prevent SQL injection.
All SQL queries built via f-strings MUST pass through these validators.

Usage:
    from scrapers.base.sql_validator import validate_identifier, sanitize_filter_expr

    validate_identifier(schema)   # raises ValueError on invalid
    validate_identifier(table)
    sanitize_filter_expr(expr)    # raises ValueError on dangerous patterns
"""

import re
from typing import Optional

# Valid SQL identifier: starts with letter/underscore, contains only alnum/underscore
_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

# Maximum identifier length (Trino/Iceberg limit)
_MAX_IDENTIFIER_LENGTH = 128

# SQL keywords that should never appear as bare identifiers in our context
_DANGEROUS_KEYWORDS = frozenset({
    'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE',
    'EXEC', 'EXECUTE', 'GRANT', 'REVOKE', 'UNION', 'INTO',
})

# Patterns that indicate SQL injection in filter expressions
_INJECTION_PATTERNS = [
    r';',            # Statement terminator
    r'--',           # Line comment
    r'/\*',          # Block comment start
    r'\*/',          # Block comment end
    r'\bDROP\b',
    r'\bDELETE\b',
    r'\bINSERT\b',
    r'\bALTER\b',
    r'\bCREATE\b',
    r'\bTRUNCATE\b',
    r'\bEXEC\b',
    r'\bEXECUTE\b',
    r'\bGRANT\b',
    r'\bREVOKE\b',
    r'\bUNION\b',
    r'\bINTO\s+OUTFILE\b',
    r'\bINTO\s+DUMPFILE\b',
    r'\bLOAD_FILE\b',
]

_INJECTION_RE = re.compile('|'.join(_INJECTION_PATTERNS), re.IGNORECASE)


def validate_identifier(name: str, context: str = "identifier") -> str:
    """
    Validate a SQL identifier (schema name, table name, column name).

    Args:
        name: The identifier to validate
        context: Description for error messages (e.g., "schema", "table")

    Returns:
        The validated identifier (unchanged)

    Raises:
        ValueError: If the identifier is invalid
    """
    if not isinstance(name, str):
        raise ValueError(f"SQL {context} must be a string, got {type(name).__name__}")

    if not name:
        raise ValueError(f"SQL {context} cannot be empty")

    if len(name) > _MAX_IDENTIFIER_LENGTH:
        raise ValueError(
            f"SQL {context} too long: {len(name)} chars (max {_MAX_IDENTIFIER_LENGTH})"
        )

    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid SQL {context}: '{name}'. "
            f"Must match [a-zA-Z_][a-zA-Z0-9_]*"
        )

    if name.upper() in _DANGEROUS_KEYWORDS:
        raise ValueError(
            f"SQL {context} '{name}' is a reserved/dangerous keyword"
        )

    return name


def validate_catalog_qualified_name(
    catalog: str, schema: str, table: Optional[str] = None
) -> str:
    """
    Validate and build a catalog-qualified SQL name.

    Args:
        catalog: Catalog name (e.g., 'iceberg')
        schema: Schema name (e.g., 'bronze')
        table: Optional table name

    Returns:
        Qualified name like 'iceberg.bronze' or 'iceberg.bronze.my_table'

    Raises:
        ValueError: If any component is invalid
    """
    validate_identifier(catalog, "catalog")
    validate_identifier(schema, "schema")

    if table is not None:
        validate_identifier(table, "table")
        return f"{catalog}.{schema}.{table}"

    return f"{catalog}.{schema}"


def sanitize_filter_expr(expr: str) -> str:
    """
    Sanitize a SQL filter expression (WHERE clause content).

    Rejects expressions containing dangerous SQL patterns that could
    indicate injection attempts.

    Args:
        expr: SQL filter expression

    Returns:
        The validated expression (unchanged)

    Raises:
        ValueError: If the expression contains dangerous patterns
    """
    if not isinstance(expr, str):
        raise ValueError(f"Filter expression must be a string, got {type(expr).__name__}")

    if not expr:
        raise ValueError("Filter expression cannot be empty")

    match = _INJECTION_RE.search(expr)
    if match:
        raise ValueError(
            f"Potentially dangerous SQL pattern in filter expression: '{match.group()}'"
        )

    return expr


def validate_snapshot_id(snapshot_id) -> int:
    """
    Validate a snapshot ID (must be a positive integer).

    Args:
        snapshot_id: The snapshot ID to validate

    Returns:
        The validated snapshot ID as int

    Raises:
        ValueError: If the snapshot ID is invalid
    """
    try:
        sid = int(snapshot_id)
    except (TypeError, ValueError):
        raise ValueError(f"Snapshot ID must be an integer, got: {snapshot_id!r}")

    if sid < 0:
        raise ValueError(f"Snapshot ID must be non-negative, got: {sid}")

    return sid
