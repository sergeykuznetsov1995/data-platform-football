"""Read-only comparison of two consumer queries over materialized relations."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any


_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_TOKEN = re.compile(
    r"""--[^\r\n]*|/\*.*?\*/|'(?:''|[^'])*'|"(?:""|[^"])*"|"""
    r"""[A-Za-z_][A-Za-z0-9_]*|\s+|.""",
    re.DOTALL,
)
_FORBIDDEN = frozenset(
    "alter analyze attach begin call checkpoint comment commit copy create deallocate "
    "delete describe detach drop execute export grant import insert install into load "
    "merge prepare pragma refresh rename replace_database reset revoke rollback set "
    "show start truncate update use vacuum".split()
)
# These functions can execute nested SQL or change state inside a SELECT.
_UNSAFE_FUNCTIONS = frozenset(
    "query nextval setval set_config postgres_query sqlite_query mysql_query".split()
)
_MAX_SAMPLE = 100


def _query(sql: str) -> str:
    if not isinstance(sql, str):
        raise ValueError("expected SQL text")
    # Use standard SQL quoting only. Reject dialect-dependent delimiters so that
    # validation and the database agree about where literals and statements end.
    if "\x00" in sql:
        raise ValueError("NUL in SQL text")
    depth = 0
    ended = False
    started = False
    has_query_body = False
    previous = ""
    parts = []
    for match in _TOKEN.finditer(sql):
        token = match.group()
        if token.startswith("--") or token.startswith("/*"):
            if token.startswith("/*") and "/*" in token[2:]:
                raise ValueError("nested block comment")
            parts.append(" ")
            continue
        if token.isspace():
            parts.append(token)
            continue
        if token in {"$", "`"} or "\\" in token:
            raise ValueError("unsupported SQL delimiter")
        if ended:
            raise ValueError("multiple SQL statements")
        word = token.lower()
        if not started and word not in {"select", "with", "values"}:
            raise ValueError("expected SELECT, WITH or VALUES")
        started = True
        if word in _FORBIDDEN:
            raise ValueError("unsupported SQL operation")
        if token in {"'", '"'} or sql.startswith("/*", match.start()):
            raise ValueError("unterminated SQL quote or comment")
        if token == ";":
            if depth:
                raise ValueError("statement separator inside query")
            ended = True
            continue
        if depth == 0 and word in {"select", "values"}:
            has_query_body = True
        if token == "(":
            if previous in _UNSAFE_FUNCTIONS:
                raise ValueError("unsupported SQL function")
            depth += 1
        elif token == ")":
            depth -= 1
            if depth < 0:
                raise ValueError("unbalanced SQL parentheses")
        parts.append(token)
        previous = word.strip('"')
    if depth or not has_query_body:
        raise ValueError("incomplete SQL query")
    return "".join(parts).strip()


def _columns(values: Iterable[str]) -> tuple[str, ...]:
    if not isinstance(values, (tuple, list)) or not values:
        raise ValueError("expected a nonempty column list")
    if any(not isinstance(v, str) or not _IDENTIFIER.fullmatch(v) for v in values):
        raise ValueError("unsafe column name")
    if len({v.lower() for v in values}) != len(values):
        raise ValueError("duplicate column name")
    return tuple(values)


@dataclass(frozen=True)
class QueryPair:
    name: str
    legacy_sql: str
    native_sql: str
    key_columns: tuple[str, ...]
    compare_columns: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("expected a nonempty query name")
        object.__setattr__(self, "name", self.name.strip())
        object.__setattr__(self, "legacy_sql", _query(self.legacy_sql))
        object.__setattr__(self, "native_sql", _query(self.native_sql))
        object.__setattr__(self, "key_columns", _columns(self.key_columns))
        if self.compare_columns is not None:
            cols = _columns(self.compare_columns)
            if not set(self.key_columns).issubset(cols):
                raise ValueError("compare_columns must include all key_columns")
            object.__setattr__(self, "compare_columns", cols)


def _schema(cur: Any, sql: str) -> list[tuple[str, str]]:
    cur.execute(f"SELECT * FROM ({sql}) AS schema_probe LIMIT 0")
    return [(d[0], _normalize_type(d[1])) for d in cur.description]


def _normalize_type(type_code: Any) -> str:
    # Preserve quoted field names and literal values inside complex types.
    return "".join(
        token if token.startswith(('"', "'")) else " " if token.isspace() else token.lower()
        for token in _TOKEN.findall(str(type_code).strip())
    )


def _difference(pair: QueryPair, columns: tuple[str, ...]) -> str:
    # Internal aliases keep consumer column names independent of counters.
    aliases = [f"c{i}" for i in range(len(columns))]
    names = ", ".join(f'"{col}"' for col in columns)
    fields = ", ".join(f'"{col}" AS {alias}' for col, alias in zip(columns, aliases))

    def grouped(sql: str) -> str:
        return f"SELECT {fields}, COUNT(*) AS copies FROM ({sql}) AS src GROUP BY {names}"

    on = " AND ".join(f"l.{a} IS NOT DISTINCT FROM n.{a}" for a in aliases)
    keys = ", ".join(f"COALESCE(l.{a}, n.{a}) AS {a}" for a in aliases)
    return f"""
        SELECT {keys}, COALESCE(l.copies, 0) AS legacy_copies,
               COALESCE(n.copies, 0) AS native_copies
        FROM ({grouped(pair.legacy_sql)}) AS l
        FULL OUTER JOIN ({grouped(pair.native_sql)}) AS n ON {on}
    """


def compare_query_pair(cur: Any, pair: QueryPair, sample_limit: int = 20) -> dict:
    """Count unmatched copies and affected keys on the side that supplied them."""
    if type(sample_limit) is not int or not 0 <= sample_limit <= _MAX_SAMPLE:
        raise ValueError(f"sample_limit must be an integer between 0 and {_MAX_SAMPLE}")
    legacy = _schema(cur, pair.legacy_sql)
    native = _schema(cur, pair.native_sql)
    result = dict(
        name=pair.name,
        passed=False,
        legacy_schema=legacy,
        native_schema=native,
        legacy_sql_sha256=hashlib.sha256(pair.legacy_sql.encode()).hexdigest(),
        native_sql_sha256=hashlib.sha256(pair.native_sql.encode()).hexdigest(),
    )
    if legacy != native:
        return {**result, "reason": "ordered output names or types differ"}
    names = [name for name, _ in legacy]
    columns = pair.compare_columns if pair.compare_columns is not None else tuple(names)
    if not set(pair.key_columns + columns).issubset(names):
        return {**result, "reason": "missing key or comparison column"}
    try:
        columns = _columns(columns)
    except ValueError as exc:
        return {**result, "reason": str(exc)}
    folded_names = [name.lower() for name in names]
    if any(folded_names.count(col.lower()) != 1 for col in columns):
        return {**result, "reason": "ambiguous output column"}

    by_key = _difference(pair, pair.key_columns)
    cur.execute(f"""
        SELECT COALESCE(SUM(legacy_copies), 0), COALESCE(SUM(native_copies), 0),
               COUNT(CASE WHEN legacy_copies <> native_copies THEN 1 END)
        FROM ({by_key}) AS key_counts
    """)
    legacy_count, native_count, keyed_diff = cur.fetchone()

    by_value = _difference(pair, columns)
    key_fields = ", ".join(f"c{columns.index(k)}" for k in pair.key_columns)
    sample_fields = f", {key_fields}" if sample_limit else ""
    # Window totals include every differing key before the sample limit applies.
    # Only the key fields and aggregate counters cross the cursor boundary.
    cur.execute(f"""
        SELECT SUM(unequal_groups) OVER (), SUM(legacy_delta) OVER (),
               SUM(native_delta) OVER (),
               COUNT(CASE WHEN legacy_delta > 0 THEN 1 END) OVER (),
               COUNT(CASE WHEN native_delta > 0 THEN 1 END) OVER (){sample_fields}
        FROM (
            SELECT {key_fields}, COUNT(*) AS unequal_groups,
                   SUM(GREATEST(legacy_copies - native_copies, 0)) AS legacy_delta,
                   SUM(GREATEST(native_copies - legacy_copies, 0)) AS native_delta
            FROM ({by_value}) AS value_counts
            WHERE legacy_copies <> native_copies
            GROUP BY {key_fields}
        ) AS per_key
        ORDER BY {key_fields} LIMIT {max(1, sample_limit)}
    """)
    rows = cur.fetchall()
    multiset_diff, legacy_delta, native_delta, legacy_keys, native_keys = (
        rows[0][:5] if rows else (0, 0, 0, 0, 0)
    )
    return {
        **result,
        "passed": legacy_count == native_count and keyed_diff == 0 and multiset_diff == 0,
        "legacy_row_count": int(legacy_count),
        "native_row_count": int(native_count),
        "keyed_diff": int(keyed_diff),
        "multiset_diff": int(multiset_diff),
        "legacy_missing_or_value_mismatch_keys": int(legacy_keys),
        "native_missing_or_value_mismatch_keys": int(native_keys),
        "legacy_excess_copies": int(legacy_delta),
        "native_excess_copies": int(native_delta),
        "sample_keys": [dict(zip(pair.key_columns, row[5:])) for row in rows[:sample_limit]],
    }


def compare_many(cur: Any, pairs: Iterable[QueryPair]) -> dict:
    pairs = tuple(pairs)
    if not pairs or len({p.name for p in pairs}) != len(pairs):
        raise ValueError("expected a nonempty collection of uniquely named query pairs")
    reports = []
    for pair in pairs:
        try:
            report = compare_query_pair(cur, pair)
        except Exception:
            # A driver failure belongs to this query. Its text can contain SQL.
            report = {"name": pair.name, "passed": False, "reason": "query comparison failed"}
        reports.append(report)
    return {"passed": all(report["passed"] for report in reports), "queries": reports}
