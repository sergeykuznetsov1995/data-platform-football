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
- point_in_time  — rolling feature is NULL for first N rows per partition

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
        severity: str = 'ERROR',
        name: Optional[str] = None,
    ) -> Check:
        return Check(
            name=name or f"ref_integrity[{child}.{key}->{parent}]",
            kind='ref_integrity',
            params={'child': child, 'parent': parent, 'key': key},
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
        'details': f"count={count}, expected>={min_r}" + (f", <={max_r}" if max_r else ""),
        'value': count,
    }


def _run_no_duplicates(conn, check: Check) -> Dict[str, Any]:
    p = check.params
    table = _qualify(p['table'])
    pk_cols = [_safe_ident(c, "column") for c in p['pk']]
    pk_list = ", ".join(pk_cols)
    sql = (
        f"SELECT COUNT(*) - COUNT(DISTINCT ({pk_list})) FROM {table}"
        f"{_where_clause(p.get('where'))}"
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


def _run_ref_integrity(conn, check: Check) -> Dict[str, Any]:
    p = check.params
    child = _qualify(p['child'])
    parent = _qualify(p['parent'])
    key = _safe_ident(p['key'], "column")
    sql = (
        f"SELECT COUNT(DISTINCT c.{key}) FROM {child} c "
        f"LEFT JOIN {parent} p ON c.{key} = p.{key} "
        f"WHERE p.{key} IS NULL AND c.{key} IS NOT NULL"
    )
    row = _fetchone(conn, sql)
    orphan = row[0] if row else 0
    return {
        'passed': orphan == 0,
        'details': f"{orphan} orphan key(s) in {child}.{key} not in {parent}",
        'value': orphan,
    }


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


_RUNNERS = {
    'row_count': _run_row_count,
    'no_duplicates': _run_no_duplicates,
    'freshness': _run_freshness,
    'no_nulls': _run_no_nulls,
    'ref_integrity': _run_ref_integrity,
    'value_range': _run_value_range,
    'point_in_time': _run_point_in_time,
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
                report.results.append(CheckResult(
                    name=chk.name, kind=chk.kind, severity=chk.severity,
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
