"""
Data Quality Checks
===================

Universal DQ-check module for Bronze / Silver / Gold layers.

Uses `trino` Python library directly (NOT scrapers.base.trino_manager)
to avoid heavy imports. Each check returns a structured result with
severity (ERROR or WARNING). ERRORs cause AirflowException when
``run_checks(raise_on_error=True)`` is used.

Check types
-----------
- row_count      — table row count within [min, max]
- no_duplicates  — PK columns are unique
- freshness      — MAX(timestamp_col) >= NOW() - max_age
- no_nulls       — listed columns have zero NULLs
- ref_integrity  — child.key exists in parent.key
- value_range    — column values within [min, max]
- coverage       — two-tier: COUNT_IF(condition)/COUNT(*) vs warn/error thresholds
- canonical_completeness — <base>_canonical IS NOT NULL implies <base>_source/_version NOT NULL
- point_in_time  — rolling feature is NULL for first N rows per partition
- scd2_no_overlap— SCD-2 validity intervals do not overlap within a key

Typical usage
-------------
    from utils.data_quality import CHECK, run_checks

    checks = [
        CHECK.row_count('bronze.fbref_schedule', min_rows=3800,
                        where="league='ENG-Premier League' AND season<=2024"),
        CHECK.no_duplicates('bronze.fbref_schedule', pk=['season', 'match_url'],
                            where="match_url IS NOT NULL"),
        CHECK.freshness('bronze.fbref_schedule', ts_col='_ingested_at',
                        max_age_hours=36, severity='WARNING'),
        CHECK.no_nulls('silver.fbref_match_enriched', cols=['match_id', 'date']),
    ]
    run_checks(checks, raise_on_error=True)
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import trino as trino_lib

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL identifier validation (inline copy from silver_tasks; avoids scrapers/)
# ---------------------------------------------------------------------------
_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_.]*$')
_DANGEROUS_KEYWORDS = frozenset({
    'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE',
    'EXEC', 'EXECUTE', 'GRANT', 'REVOKE',
})


def _safe_ident(name: str, kind: str = "identifier") -> str:
    if not isinstance(name, str) or not name:
        raise ValueError(f"SQL {kind} must be a non-empty string, got {name!r}")
    if len(name) > 128 or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL {kind}: '{name}'")
    if name.upper() in _DANGEROUS_KEYWORDS:
        raise ValueError(f"SQL {kind} '{name}' is a reserved keyword")
    return name


# ---------------------------------------------------------------------------
# Trino connection helper (same pattern as silver_tasks._get_trino_connection)
# ---------------------------------------------------------------------------

def _get_conn(catalog: str = 'iceberg') -> trino_lib.dbapi.Connection:
    host = os.environ.get('TRINO_HOST', 'trino')
    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')
    if password:
        port = int(os.environ.get('TRINO_PORT', 8443))
        return trino_lib.dbapi.connect(
            host=host, port=port, user=user, catalog=catalog,
            http_scheme='https',
            auth=trino_lib.auth.BasicAuthentication(user, password),
            verify=False,
        )
    port = int(os.environ.get('TRINO_PORT', 8080))
    return trino_lib.dbapi.connect(host=host, port=port, user=user, catalog=catalog)


def _fetchone(conn, sql: str):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchone()
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Check dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Check:
    """A single DQ check definition."""
    name: str
    kind: str
    params: Dict[str, Any]
    severity: str = 'ERROR'  # ERROR or WARNING


class CHECK:
    """Factory methods for building Check instances.

    Using a class as a namespace so call sites read like
    ``CHECK.row_count(...)``.
    """

    @staticmethod
    def row_count(
        table: str,
        min_rows: int,
        max_rows: Optional[int] = None,
        where: Optional[str] = None,
        severity: str = 'ERROR',
        name: Optional[str] = None,
    ) -> Check:
        return Check(
            name=name or f"row_count[{table}]",
            kind='row_count',
            params={'table': table, 'min_rows': min_rows, 'max_rows': max_rows, 'where': where},
            severity=severity,
        )

    @staticmethod
    def no_duplicates(
        table: str,
        pk: List[str],
        where: Optional[str] = None,
        severity: str = 'ERROR',
        name: Optional[str] = None,
    ) -> Check:
        return Check(
            name=name or f"no_duplicates[{table}({','.join(pk)})]",
            kind='no_duplicates',
            params={'table': table, 'pk': pk, 'where': where},
            severity=severity,
        )

    @staticmethod
    def freshness(
        table: str,
        ts_col: str,
        max_age_hours: float,
        where: Optional[str] = None,
        severity: str = 'ERROR',
        name: Optional[str] = None,
    ) -> Check:
        return Check(
            name=name or f"freshness[{table}.{ts_col}<{max_age_hours}h]",
            kind='freshness',
            params={'table': table, 'ts_col': ts_col, 'max_age_hours': max_age_hours, 'where': where},
            severity=severity,
        )

    @staticmethod
    def no_nulls(
        table: str,
        cols: List[str],
        where: Optional[str] = None,
        severity: str = 'ERROR',
        name: Optional[str] = None,
    ) -> Check:
        return Check(
            name=name or f"no_nulls[{table}({','.join(cols)})]",
            kind='no_nulls',
            params={'table': table, 'cols': cols, 'where': where},
            severity=severity,
        )

    @staticmethod
    def ref_integrity(
        child: str,
        parent: str,
        key: str,
        parent_key: Optional[str] = None,
        where: Optional[str] = None,
        warn_rate: Optional[float] = None,
        error_rate: Optional[float] = None,
        severity: str = 'ERROR',
        name: Optional[str] = None,
    ) -> Check:
        """Child.key must exist in parent.key (or parent.parent_key when names differ).

        ``where`` restricts the check to a subset of child rows (e.g.
        ``lineup_source = 'fbref'`` to exclude other-source pseudo-ids that
        legitimately don't appear in the parent).

        Rate mode (issue #432): when ``warn_rate`` is set, the runner measures
        the orphan ROW share (orphan rows / rows with non-NULL key) instead of
        failing on the first orphan. Two-tier severity, mirroring ``coverage``:

            rate <= warn_rate                  -> passed=True
            warn_rate < rate <= error_rate     -> failed, WARNING
            rate > error_rate                  -> failed, ERROR (runtime override)

        ``error_rate=None`` never escalates past the static ``severity``.
        NULL child keys are excluded from both numerator and denominator —
        "no claim, no violation" (NULL-orphan shares are measured by
        ``CHECK.coverage`` instead).
        """
        for label, rate in (('warn_rate', warn_rate), ('error_rate', error_rate)):
            if rate is not None and not (0.0 <= rate <= 1.0):
                raise ValueError(f"{label} must be in [0, 1], got {rate}")
        if error_rate is not None and warn_rate is None:
            raise ValueError("error_rate requires warn_rate")
        if warn_rate is not None and error_rate is not None and warn_rate > error_rate:
            raise ValueError(
                f"warn_rate must be <= error_rate, got warn={warn_rate}, error={error_rate}"
            )
        pk = parent_key or key
        suffix = key if pk == key else f"{key}->{pk}"
        return Check(
            name=name or f"ref_integrity[{child}.{suffix}->{parent}]",
            kind='ref_integrity',
            params={'child': child, 'parent': parent, 'key': key,
                    'parent_key': pk, 'where': where,
                    'warn_rate': warn_rate, 'error_rate': error_rate},
            severity=severity,
        )

    @staticmethod
    def value_range(
        table: str,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        where: Optional[str] = None,
        severity: str = 'WARNING',
        name: Optional[str] = None,
    ) -> Check:
        return Check(
            name=name or f"value_range[{table}.{column}]",
            kind='value_range',
            params={
                'table': table, 'column': column,
                'min': min_val, 'max': max_val, 'where': where,
            },
            severity=severity,
        )

    @staticmethod
    def canonical_completeness(
        table: str,
        canonical_col: str,
        severity: str = 'ERROR',
        name: Optional[str] = None,
    ) -> Check:
        """Schema-versioning completeness check (R0.4).

        Asserts that every row with a non-NULL ``<base>_canonical`` value
        also carries a non-NULL ``<base>_source`` and ``<base>_version``.
        See ``docs/research/R0.4_schema_versioning.md`` for the contract.

        ``canonical_col`` must end with ``_canonical``; the base name is
        derived by stripping that suffix (e.g. ``venue_canonical → venue``).
        Offender count > 0 fails the check (severity=ERROR by default).
        """
        if not canonical_col.endswith('_canonical'):
            raise ValueError(
                f"canonical_col must end with '_canonical', got: {canonical_col!r}"
            )
        return Check(
            name=name or f"canonical_completeness[{table}.{canonical_col}]",
            kind='canonical_completeness',
            params={'table': table, 'canonical_col': canonical_col},
            severity=severity,
        )

    @staticmethod
    def coverage(
        table: str,
        column: Optional[str] = None,
        condition: Optional[str] = None,
        where: Optional[str] = None,
        warn_threshold: float = 0.80,
        error_threshold: float = 0.50,
        severity: str = 'WARNING',
        name: Optional[str] = None,
    ) -> Check:
        """Two-tier coverage check.

        Measures ``COUNT_IF(condition) / COUNT(*)`` over ``table`` (optionally
        filtered by ``where``). Severity is determined by the runner at
        runtime — overriding the static ``severity`` param:

            ratio >= warn_threshold (default 0.80)  -> passed=True
            error_threshold <= ratio < warn         -> WARNING
            ratio < error_threshold (default 0.50)  -> ERROR

        Either ``column`` (shortcut for ``<column> IS NOT NULL``) or
        ``condition`` (explicit predicate) must be provided.
        """
        if column is None and condition is None:
            raise ValueError("coverage() requires either 'column' or 'condition'")
        if condition is None:
            col = _safe_ident(column, "column")
            condition = f"{col} IS NOT NULL"
        # Inline-SQL safety guard, identical to _where_clause shape
        if ';' in condition or '--' in condition or '/*' in condition:
            raise ValueError(f"Unsafe condition: {condition!r}")
        if not (0.0 <= error_threshold <= warn_threshold <= 1.0):
            raise ValueError(
                f"thresholds must satisfy 0 <= error <= warn <= 1, "
                f"got error={error_threshold}, warn={warn_threshold}"
            )
        return Check(
            name=name or f"coverage[{table}: {condition}]",
            kind='coverage',
            params={
                'table': table,
                'condition': condition,
                'where': where,
                'warn_threshold': warn_threshold,
                'error_threshold': error_threshold,
            },
            severity=severity,
        )

    @staticmethod
    def point_in_time(
        table: str,
        feature_col: str,
        partition_by: List[str],
        order_by: str,
        skip_first_n: int = 5,
        severity: str = 'ERROR',
        name: Optional[str] = None,
    ) -> Check:
        """Rolling feature must be NULL for first N rows per partition.

        Guards against data leakage in Gold rolling features.
        """
        return Check(
            name=name or f"point_in_time[{table}.{feature_col}]",
            kind='point_in_time',
            params={
                'table': table,
                'feature_col': feature_col,
                'partition_by': partition_by,
                'order_by': order_by,
                'skip_first_n': skip_first_n,
            },
            severity=severity,
        )

    @staticmethod
    def scd2_no_overlap(
        table: str,
        pk_cols: List[str],
        valid_from_col: str = 'valid_from',
        valid_to_col: str = 'valid_to',
        severity: str = 'ERROR',
        name: Optional[str] = None,
    ) -> Check:
        """SCD-2 timeline integrity check.

        Asserts that for every business key (``pk_cols``) the validity
        intervals ``[valid_from, valid_to)`` do not overlap. NULL
        ``valid_to`` is interpreted as the open-ended (current) row.

        Used by SCD-2 dimensions such as ``dim_manager`` (Phase C.4),
        ``dim_player_contract``, ``dim_team_kit``, etc. Adjacent stints
        sharing an endpoint (``t1.valid_to == t2.valid_from``) are OK
        because intervals are closed-open.

        ``pk_cols`` MUST be non-empty — it defines the partition (timeline)
        within which overlaps are forbidden. Different partitions with
        date-overlapping rows are independent timelines and are NOT a
        violation.
        """
        if not pk_cols:
            raise ValueError("scd2_no_overlap requires at least one pk_col")
        return Check(
            name=name or f"scd2_no_overlap[{table}({','.join(pk_cols)})]",
            kind='scd2_no_overlap',
            params={
                'table': table,
                'pk_cols': pk_cols,
                'valid_from_col': valid_from_col,
                'valid_to_col': valid_to_col,
            },
            severity=severity,
        )


# ---------------------------------------------------------------------------
# Check runners (one per kind)
# ---------------------------------------------------------------------------

def _qualify(table: str) -> str:
    """Ensure table is fully qualified with iceberg catalog."""
    _safe_ident(table, "table")
    if table.count('.') == 2:
        return table
    if table.count('.') == 1:
        return f"iceberg.{table}"
    raise ValueError(f"Table must be 'schema.name' or 'catalog.schema.name', got: {table}")


def _where_clause(where: Optional[str]) -> str:
    if not where:
        return ""
    # Minimal guard — no semicolons, no comment markers
    if ';' in where or '--' in where or '/*' in where:
        raise ValueError(f"Unsafe WHERE clause: {where!r}")
    return f" WHERE {where}"


def _run_row_count(conn, check: Check) -> Dict[str, Any]:
    p = check.params
    table = _qualify(p['table'])
    sql = f"SELECT COUNT(*) FROM {table}{_where_clause(p.get('where'))}"
    row = _fetchone(conn, sql)
    count = row[0] if row else 0
    min_r = p['min_rows']
    max_r = p.get('max_rows')
    passed = count >= min_r and (max_r is None or count <= max_r)
    return {
        'passed': passed,
        'details': f"count={count}, expected>={min_r}" + (f", <={max_r}" if max_r is not None else ""),
        'value': count,
    }


def _run_no_duplicates(conn, check: Check) -> Dict[str, Any]:
    p = check.params
    table = _qualify(p['table'])
    pk_cols = [_safe_ident(c, "column") for c in p['pk']]
    pk_list = ", ".join(pk_cols)
    # SUM(cnt-1) over GROUP BY, not COUNT(DISTINCT (row(...))): the row
    # constructor materialises per-row objects and heap-OOM'd Trino at 28M
    # rows (gold.fct_event after the top-5 spadl backfill, #913). A plain
    # GROUP BY aggregation is columnar and spill-to-disk-capable; NULL keys
    # group together exactly like DISTINCT row(), so the count is identical.
    sql = (
        f"SELECT COALESCE(SUM(cnt - 1), 0) FROM ("
        f"SELECT COUNT(*) AS cnt FROM {table}"
        f"{_where_clause(p.get('where'))} GROUP BY {pk_list}"
        f") g"
    )
    row = _fetchone(conn, sql)
    dup_count = row[0] if row else 0
    return {
        'passed': dup_count == 0,
        'details': f"{dup_count} duplicate row(s) by ({pk_list})",
        'value': dup_count,
    }


def _run_freshness(conn, check: Check) -> Dict[str, Any]:
    p = check.params
    table = _qualify(p['table'])
    ts_col = _safe_ident(p['ts_col'], "column")
    max_age = float(p['max_age_hours'])
    sql = (
        f"SELECT DATE_DIFF('hour', MAX({ts_col}), CURRENT_TIMESTAMP) AS age_h, "
        f"MAX({ts_col}) AS max_ts "
        f"FROM {table}{_where_clause(p.get('where'))}"
    )
    row = _fetchone(conn, sql)
    age_h = row[0] if row and row[0] is not None else 9999
    max_ts = row[1] if row else None
    return {
        'passed': age_h <= max_age,
        'details': f"max({ts_col})={max_ts}, age={age_h}h, threshold={max_age}h",
        'value': age_h,
    }


def _run_no_nulls(conn, check: Check) -> Dict[str, Any]:
    p = check.params
    table = _qualify(p['table'])
    cols = [_safe_ident(c, "column") for c in p['cols']]
    null_counts = []
    offenders = []
    for c in cols:
        sql = (
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE {c} IS NULL{(' AND ' + p['where']) if p.get('where') else ''}"
        )
        if p.get('where') and ('--' in p['where'] or ';' in p['where']):
            raise ValueError(f"Unsafe WHERE: {p['where']!r}")
        row = _fetchone(conn, sql)
        n = row[0] if row else 0
        null_counts.append((c, n))
        if n > 0:
            offenders.append(f"{c}={n}")
    total_nulls = sum(n for _, n in null_counts)
    return {
        'passed': total_nulls == 0,
        'details': (
            f"nulls by column: {null_counts}" if offenders
            else f"all {len(cols)} columns NULL-free"
        ),
        'value': total_nulls,
    }


def _columns_of(conn, fully_qualified_table: str) -> Optional[set]:
    """Set of column names for ``catalog.schema.table``. Returns:
      * ``None`` if ``information_schema.columns`` is unreachable (e.g. tests
        using DuckDB as a Trino stub) — caller falls back to running the
        underlying SQL and reporting the engine error directly.
      * empty ``set()`` if the table exists in catalog terms but has no rows.
      * a populated ``set`` of column names otherwise.
    """
    try:
        parts = fully_qualified_table.split('.')
        schema_table = f"{parts[1]}.{parts[2]}" if len(parts) == 3 else fully_qualified_table
        return {c for c, _ in _fetch_schema(conn, schema_table)}
    except Exception:
        return None


def _run_ref_integrity(conn, check: Check) -> Dict[str, Any]:
    p = check.params
    child = _qualify(p['child'])
    parent = _qualify(p['parent'])
    key = _safe_ident(p['key'], "column")
    parent_key = _safe_ident(p.get('parent_key') or p['key'], "column")

    # Pre-flight: catch missing column / table BEFORE the orphan SQL so the
    # operator sees a readable CheckResult instead of an opaque
    # ``TrinoUserError COLUMN_NOT_FOUND``. E4 postmortem (2026-05-09) surfaced
    # 6× false-WARN of this exact shape: call sites passed key='match_id_canonical'
    # without parent_key, and the default parent_key=key didn't match
    # dim_match.match_id. The hint below names the typical fix.
    #
    # When information_schema is unreachable (None), we skip the pre-flight
    # entirely and let the orphan SQL fail naturally — preserves backward
    # compatibility with the DuckDB-stub tests.
    child_cols = _columns_of(conn, child)
    parent_cols = _columns_of(conn, parent)
    if child_cols is not None and parent_cols is not None:
        if not child_cols:
            return {'passed': False, 'value': None,
                    'details': f"child table not found in catalog: {child}"}
        if not parent_cols:
            return {'passed': False, 'value': None,
                    'details': f"parent table not found in catalog: {parent}"}
        missing = []
        if key not in child_cols:
            missing.append(f"{child}.{key}")
        if parent_key not in parent_cols:
            missing.append(f"{parent}.{parent_key}")
        if missing:
            return {
                'passed': False,
                'value': None,
                'details': (
                    f"column(s) not found in catalog: {', '.join(missing)}. "
                    f"Hint: pass explicit parent_key= when parent uses a different "
                    f"column name (e.g. parent_key='match_id' when key='match_id_canonical')."
                ),
            }

    # Optional child-row filter (e.g. scope to a single source). Applied as a
    # subquery so the predicate can't collide with parent column names in the
    # JOIN. Same injection guard as _run_no_nulls / _run_value_range.
    where = p.get('where')
    if where:
        if ';' in where or '--' in where:
            raise ValueError(f"Unsafe WHERE: {where!r}")
        child_src = f"(SELECT * FROM {child} WHERE {where})"
    else:
        child_src = child

    warn_rate = p.get('warn_rate')
    if warn_rate is None:
        sql = (
            f"SELECT COUNT(DISTINCT c.{key}) FROM {child_src} c "
            f"LEFT JOIN {parent} p ON c.{key} = p.{parent_key} "
            f"WHERE p.{parent_key} IS NULL AND c.{key} IS NOT NULL"
        )
        row = _fetchone(conn, sql)
        orphan = row[0] if row else 0
        return {
            'passed': orphan == 0,
            'details': f"{orphan} orphan key(s) in {child}.{key} not in {parent}.{parent_key}",
            'value': orphan,
        }

    # Rate mode (#432): orphan ROW share with two-tier severity. The parent
    # is de-duplicated so a non-unique parent_key can't inflate the row
    # counts via JOIN fan-out (the legacy branch counts DISTINCT child keys,
    # where fan-out is harmless).
    error_rate = p.get('error_rate')
    sql = (
        f"SELECT COUNT(*), "
        f"COUNT_IF(p.{parent_key} IS NULL), "
        f"COUNT(DISTINCT CASE WHEN p.{parent_key} IS NULL THEN c.{key} END) "
        f"FROM {child_src} c "
        f"LEFT JOIN (SELECT DISTINCT {parent_key} FROM {parent}) p "
        f"ON c.{key} = p.{parent_key} "
        f"WHERE c.{key} IS NOT NULL"
    )
    row = _fetchone(conn, sql)
    total, orphan_rows, orphan_keys = (
        (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)) if row else (0, 0, 0)
    )
    if total == 0:
        return {
            'passed': True,
            'value': 0.0,
            'details': f"0 rows with non-NULL {child}.{key} — vacuous pass",
        }
    rate = orphan_rows / total
    details = (
        f"orphan rate {rate:.1%} ({orphan_rows}/{total} rows, "
        f"{orphan_keys} distinct keys) in {child}.{key} not in "
        f"{parent}.{parent_key}, warn<={warn_rate:.0%}"
        + (f", error<={error_rate:.0%}" if error_rate is not None else "")
    )
    if rate <= warn_rate:
        return {'passed': True, 'value': rate, 'details': details}
    if error_rate is not None and rate > error_rate:
        return {'passed': False, 'severity': 'ERROR', 'value': rate,
                'details': details}
    return {'passed': False, 'severity': 'WARNING', 'value': rate,
            'details': details}


def _run_value_range(conn, check: Check) -> Dict[str, Any]:
    p = check.params
    table = _qualify(p['table'])
    col = _safe_ident(p['column'], "column")
    min_v, max_v = p.get('min'), p.get('max')
    conds = []
    if min_v is not None:
        conds.append(f"{col} < {float(min_v)}")
    if max_v is not None:
        conds.append(f"{col} > {float(max_v)}")
    if not conds:
        return {'passed': True, 'details': "no bounds specified", 'value': 0}
    where_parts = [f"{col} IS NOT NULL", "(" + " OR ".join(conds) + ")"]
    if p.get('where'):
        if ';' in p['where'] or '--' in p['where']:
            raise ValueError(f"Unsafe WHERE: {p['where']!r}")
        where_parts.append(p['where'])
    where_sql = " AND ".join(where_parts)
    sql = f"SELECT COUNT(*) FROM {table} WHERE {where_sql}"
    row = _fetchone(conn, sql)
    violations = row[0] if row else 0
    rng = f"[{min_v}, {max_v}]" if max_v is not None else f">= {min_v}"
    return {
        'passed': violations == 0,
        'details': f"{violations} row(s) outside {rng}",
        'value': violations,
    }


def _run_coverage(conn, check: Check) -> Dict[str, Any]:
    """Two-tier coverage check (see ``CHECK.coverage``).

    SQL: ``SELECT COUNT(*), COUNT_IF(<condition>) FROM <table> [WHERE <where>]``.

    Returns ``severity`` in the result dict so ``run_checks`` can override
    the static check.severity based on the observed ratio:
      * ratio >= warn_threshold  -> passed=True (severity unchanged)
      * error_threshold <= ratio -> passed=False, severity='WARNING'
      * ratio <  error_threshold -> passed=False, severity='ERROR'

    Empty table (total=0) is treated as 0% coverage and fails with WARNING —
    the absence of data is a quality issue but not severe enough to halt
    Gold runs (use a separate ``row_count`` check for hard floor).
    """
    p = check.params
    table = _qualify(p['table'])
    cond = p['condition']
    if ';' in cond or '--' in cond or '/*' in cond:
        # Defensive — factory already validated, but the runner is callable
        # via the registry so we re-check at the boundary.
        raise ValueError(f"Unsafe condition: {cond!r}")
    where_sql = _where_clause(p.get('where'))
    sql = f"SELECT COUNT(*), COUNT_IF({cond}) FROM {table}{where_sql}"
    row = _fetchone(conn, sql)
    total, covered = (int(row[0] or 0), int(row[1] or 0)) if row else (0, 0)
    warn = float(p['warn_threshold'])
    err = float(p['error_threshold'])

    if total == 0:
        return {
            'passed': False,
            'severity': 'WARNING',
            'value': 0.0,
            'details': f"empty table — coverage = 0% (0/0); condition: {cond}",
        }
    ratio = covered / total
    pct = f"{ratio * 100:.1f}%"
    if ratio >= warn:
        return {
            'passed': True,
            'value': ratio,
            'details': f"coverage = {pct} ({covered}/{total}, ≥ {warn * 100:.0f}% target)",
        }
    if ratio >= err:
        return {
            'passed': False,
            'severity': 'WARNING',
            'value': ratio,
            'details': (
                f"coverage = {pct} ({covered}/{total}) below {warn * 100:.0f}% "
                f"target (≥ {err * 100:.0f}% floor)"
            ),
        }
    return {
        'passed': False,
        'severity': 'ERROR',
        'value': ratio,
        'details': (
            f"coverage = {pct} ({covered}/{total}) below {err * 100:.0f}% floor"
        ),
    }


def _run_canonical_completeness(conn, check: Check) -> Dict[str, Any]:
    """R0.4: rows with non-NULL ``<base>_canonical`` MUST have non-NULL
    ``<base>_source`` and ``<base>_version``.

    Mirrors the shape of ``_run_no_nulls``: derive the three column names,
    run a single COUNT(*) over the offending predicate, build a CheckResult
    payload. Offender count > 0 fails the check.
    """
    p = check.params
    table = _qualify(p['table'])
    canonical_col = _safe_ident(p['canonical_col'], "column")
    if not canonical_col.endswith('_canonical'):
        # Defensive: factory already validates this, but the runner is
        # callable directly via the registry so re-check at the boundary.
        raise ValueError(
            f"canonical_col must end with '_canonical', got: {canonical_col!r}"
        )
    base = canonical_col[: -len('_canonical')]
    source_col = _safe_ident(f"{base}_source", "column")
    version_col = _safe_ident(f"{base}_version", "column")

    sql = (
        f"SELECT COUNT(*) AS offenders FROM {table} "
        f"WHERE {canonical_col} IS NOT NULL "
        f"AND ({source_col} IS NULL OR {version_col} IS NULL)"
    )
    row = _fetchone(conn, sql)
    offenders = row[0] if row else 0
    return {
        'passed': offenders == 0,
        'details': (
            f"all rows with {canonical_col} carry {source_col} + {version_col}"
            if offenders == 0
            else (
                f"{offenders} row(s) with non-NULL {canonical_col} are missing "
                f"{source_col} and/or {version_col}"
            )
        ),
        'value': offenders,
    }


def _run_point_in_time(conn, check: Check) -> Dict[str, Any]:
    p = check.params
    table = _qualify(p['table'])
    feat = _safe_ident(p['feature_col'], "column")
    order_by = _safe_ident(p['order_by'], "column")
    pb = [_safe_ident(c, "column") for c in p['partition_by']]
    skip_n = int(p['skip_first_n'])
    pb_list = ", ".join(pb)

    # Rows where the feature is NOT NULL but rn <= skip_n (leakage suspects)
    sql = (
        f"WITH ranked AS ("
        f"  SELECT {feat} AS fv, "
        f"         ROW_NUMBER() OVER (PARTITION BY {pb_list} ORDER BY {order_by}) AS rn "
        f"  FROM {table}"
        f") "
        f"SELECT COUNT(*) FROM ranked WHERE rn <= {skip_n} AND fv IS NOT NULL"
    )
    row = _fetchone(conn, sql)
    leaks = row[0] if row else 0
    return {
        'passed': leaks == 0,
        'details': (
            f"{leaks} row(s) with non-null {feat} in first {skip_n} "
            f"rows per ({pb_list}) — suspected leakage"
        ),
        'value': leaks,
    }


def _run_scd2_no_overlap(conn, check: Check) -> Dict[str, Any]:
    """SCD-2 overlap check: count timeline overlaps within each partition.

    For every business key (``pk_cols``) we self-join the table and count
    pairs of rows whose validity intervals overlap. We use the standard
    closed-open ``[valid_from, valid_to)`` interpretation:

        overlap iff t1.valid_from < COALESCE(t2.valid_to, '9999-12-31')
                  AND COALESCE(t1.valid_to, '9999-12-31') > t2.valid_from

    To avoid double-counting symmetric pairs and self-matches we add a
    strict tiebreaker on ``valid_from`` (``t1.valid_from < t2.valid_from``).
    Rows that share the same ``valid_from`` for the same partition are also
    considered overlapping (equal intervals) — caught via an OR-branch on
    ``t1.valid_from = t2.valid_from`` plus a tiebreaker on ``valid_to`` to
    keep the count deterministic.
    """
    p = check.params
    table = _qualify(p['table'])
    pk_cols = [_safe_ident(c, "column") for c in p['pk_cols']]
    if not pk_cols:
        raise ValueError("scd2_no_overlap requires at least one pk_col")
    vf = _safe_ident(p['valid_from_col'], "column")
    vt = _safe_ident(p['valid_to_col'], "column")

    pk_match = " AND ".join(f"t1.{c} = t2.{c}" for c in pk_cols)
    # Closed-open overlap predicate. Open-ended valid_to => +infinity.
    open_end = "DATE '9999-12-31'"
    overlap_pred = (
        f"t1.{vf} < COALESCE(t2.{vt}, {open_end}) "
        f"AND COALESCE(t1.{vt}, {open_end}) > t2.{vf}"
    )
    # Tiebreaker — count each unordered overlapping pair once.
    # If valid_from differs: take the strictly-earlier row as t1.
    # If valid_from is equal (duplicate-start overlap): break ties by
    # valid_to (NULL last) so each pair contributes exactly once.
    tiebreaker = (
        f"(t1.{vf} < t2.{vf} "
        f"OR (t1.{vf} = t2.{vf} "
        f"    AND COALESCE(t1.{vt}, {open_end}) < COALESCE(t2.{vt}, {open_end})))"
    )

    sql = (
        f"SELECT COUNT(*) AS overlaps "
        f"FROM {table} t1 "
        f"JOIN {table} t2 "
        f"  ON {pk_match} "
        f" AND {tiebreaker} "
        f"WHERE {overlap_pred}"
    )
    row = _fetchone(conn, sql)
    overlaps = row[0] if row else 0
    pk_list = ", ".join(pk_cols)
    return {
        'passed': overlaps == 0,
        'details': (
            f"no SCD-2 overlaps for ({pk_list})"
            if overlaps == 0
            else f"{overlaps} overlapping interval pair(s) for ({pk_list})"
        ),
        'value': overlaps,
    }


def _fetch_schema(conn, table: str) -> List[tuple]:
    """Return ``[(column_name, data_type), ...]`` for ``schema.table``.

    Reads from ``iceberg.information_schema.columns`` ordered by
    ``ordinal_position`` so the returned list preserves table layout.
    Raises ``ValueError`` if ``table`` does not match the strict
    ``'<schema>.<table>'`` shape (defensive boundary against SQL injection).

    Returns ``[]`` if the table does not exist in the catalog, so callers can
    surface a missing table as a clear error rather than a silent pass.
    """
    if not isinstance(table, str) or '.' not in table:
        raise ValueError(f"table must be 'schema.table', got: {table!r}")
    schema_name, _, table_name = table.partition('.')
    schema_ident = _safe_ident(schema_name, "schema")
    table_ident = _safe_ident(table_name, "table")
    sql = (
        "SELECT column_name, data_type "
        "FROM iceberg.information_schema.columns "
        f"WHERE table_schema = '{schema_ident}' "
        f"AND table_name = '{table_ident}' "
        "ORDER BY ordinal_position"
    )
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall() or []
    finally:
        cur.close()
    # Each row is (column_name, data_type) — coerce to plain str tuples.
    return [(str(r[0]), str(r[1])) for r in rows]


_RUNNERS = {
    'row_count': _run_row_count,
    'no_duplicates': _run_no_duplicates,
    'freshness': _run_freshness,
    'no_nulls': _run_no_nulls,
    'ref_integrity': _run_ref_integrity,
    'value_range': _run_value_range,
    'coverage': _run_coverage,
    'canonical_completeness': _run_canonical_completeness,
    'point_in_time': _run_point_in_time,
    'scd2_no_overlap': _run_scd2_no_overlap,
}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    kind: str
    severity: str
    passed: bool
    details: str = ''
    value: Any = None
    error: Optional[str] = None


@dataclass
class RunReport:
    results: List[CheckResult] = field(default_factory=list)

    @property
    def errors(self) -> List[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == 'ERROR']

    @property
    def warnings(self) -> List[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == 'WARNING']

    @property
    def passed(self) -> List[CheckResult]:
        return [r for r in self.results if r.passed]

    def summary(self) -> str:
        return (
            f"{len(self.passed)}/{len(self.results)} passed, "
            f"{len(self.errors)} ERRORs, {len(self.warnings)} WARNINGs"
        )


def run_checks(
    checks: List[Check],
    raise_on_error: bool = True,
) -> RunReport:
    """Execute DQ checks and return a report.

    If any ERROR-severity check fails and ``raise_on_error=True``,
    raises ``AirflowException`` (or ``RuntimeError`` outside Airflow).
    """
    report = RunReport()
    conn = _get_conn()
    try:
        for chk in checks:
            runner = _RUNNERS.get(chk.kind)
            if runner is None:
                report.results.append(CheckResult(
                    name=chk.name, kind=chk.kind, severity=chk.severity,
                    passed=False, error=f"unknown kind: {chk.kind}",
                ))
                continue
            try:
                out = runner(conn, chk)
                # Allow runners to override severity at runtime (used by
                # two-tier coverage check). Fall back to the check's static
                # severity for runners that don't return one.
                effective_severity = out.get('severity', chk.severity)
                report.results.append(CheckResult(
                    name=chk.name, kind=chk.kind, severity=effective_severity,
                    passed=out['passed'], details=out['details'], value=out.get('value'),
                ))
            except Exception as e:
                report.results.append(CheckResult(
                    name=chk.name, kind=chk.kind, severity=chk.severity,
                    passed=False, error=str(e),
                ))
                logger.exception(f"DQ check raised: {chk.name}")
    finally:
        conn.close()

    # Structured logging
    logger.info(f"DQ report: {report.summary()}")
    for r in report.passed:
        logger.info(f"  OK   {r.name} — {r.details}")
    for r in report.warnings:
        logger.warning(f"  WARN {r.name} — {r.details or r.error}")
    for r in report.errors:
        logger.error(f"  FAIL {r.name} — {r.details or r.error}")

    if raise_on_error and report.errors:
        try:
            from airflow.exceptions import AirflowException  # noqa
            exc = AirflowException
        except ImportError:
            exc = RuntimeError
        raise exc(
            f"DQ failed: {len(report.errors)} error(s): "
            + "; ".join(f"{r.name}: {r.details or r.error}" for r in report.errors)
        )

    return report
