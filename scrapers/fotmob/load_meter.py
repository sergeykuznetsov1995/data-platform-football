"""Read-only FotMob load meter and deterministic, offline daily accounting.

Only ``poll`` and ``qualify`` load the optional Trino client. Importing this
module, parsing SQL and evaluating saved evidence require only the stdlib.
"""

from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence


N1_MAX_COMMITS_PER_MIN = 1
N2_MAX_POLLS_PER_MIN = 1
N3_MAX_COMMITS_PER_DAY = 1440
N4_MAX_SCANS_PER_DAY = 1440
CONTROL_TABLES = frozenset({"iceberg.bronze.fotmob_ingest_manifest"})
# The wave MERGEs these journal/reference tables once per scope batch, so their
# commit cadence tracks batch rhythm (measured 16-19.11: peaks 2-8/min, daily
# totals <=167) instead of the per-data-table <=1/min norm; match-payload tables
# stay under the regular limit. Scan/poll control limits above stay manifest-only.
WAVE_JOURNAL_TABLES = frozenset(CONTROL_TABLES | {
    "iceberg.bronze.fotmob_competition_scope_observations",
    "iceberg.bronze.fotmob_competition_seasons",
    "iceberg.bronze.fotmob_competition_season_history",
    "iceberg.bronze.fotmob_season_stages",
    "iceberg.bronze.fotmob_squad_snapshots",
    "iceberg.bronze.fotmob_team_snapshots",
    "iceberg.bronze.fotmob_transfer_events",
})
CONTROL_N1_MAX_COMMITS_PER_MIN = 8
CONTROL_N2_MAX_POLLS_PER_MIN = 130
CONTROL_N4_MAX_SCANS_PER_DAY = 5760
POLL_ELIGIBLE_TABLES = frozenset({
    "iceberg.bronze.fotmob_ingest_manifest",
    "iceberg.bronze.fotmob_squad_snapshots",
})
PLATFORM_USERS = frozenset({"airflow"})
PRODUCER_VARIANT = "query_history_poll"
SOURCE_USER = "src_fotmob"
PASSWORD_ENV = "TRINO_SRC_FOTMOB_PASSWORD"
DEFAULT_ROOT = Path("/root/watchdog/fotmob_meter")
UTC = timezone.utc
MINUTE = timedelta(minutes=1)
ATTRIBUTIONS = (SOURCE_USER, "platform", "unknown")
WRITE_TYPES = frozenset({"insert", "merge", "update", "delete", "ctas", "alter_write", "drop", "table_ddl"})
TABLE_NAME_RE = re.compile(r"fotmob_[a-z0-9_]+\Z")
EPHEMERAL_STAGE_RE = re.compile(r"fotmob_[a-z0-9_]+__stg_[0-9a-f]{6,}\Z")
QUERY_ID_RE = re.compile(r"[A-Za-z0-9_]+\Z")
_SQL_IDENTIFIER = r'(?:"(?:[^"]|"")*"|[\w$]+)'
MAINTENANCE_PROCEDURE_RE = re.compile(
    rf"^\s*ALTER\s+TABLE\s+{_SQL_IDENTIFIER}(?:\s*\.\s*{_SQL_IDENTIFIER}){{0,2}}"
    r"\s+EXECUTE\s+(?a:optimize|expire_snapshots|remove_orphan_files)\s*\(",
    re.IGNORECASE | re.DOTALL,
)
MAINTENANCE_JOBS = ("compaction", "janitor", "morning_report")
OPERATION_FIELDS = {"commit": "commits", "scan_read": "scan_reads", "scan_write": "scan_writes"}

S1_SQL = '''SELECT query_id, "user", source, query, state, created, started,
       "end" AS ended, error_code
FROM system.runtime.queries
WHERE "end" >= TIMESTAMP '{t_from}' AND "end" < TIMESTAMP '{t_to}' '''
S2_SQL = r'''SELECT table_name FROM iceberg.information_schema.tables
WHERE table_schema = 'bronze' AND table_name LIKE 'fotmob\_%' ESCAPE '\' '''
S3_SQL = r'''CASE
 WHEN regexp_like(query, '(?i)^\s*INSERT\s+INTO\b') THEN 'insert'
 WHEN regexp_like(query, '(?i)^\s*MERGE\s+INTO\b') THEN 'merge'
 WHEN regexp_like(query, '(?i)^\s*UPDATE\b') THEN 'update'
 WHEN regexp_like(query, '(?i)^\s*DELETE\s+FROM\b') THEN 'delete'
 WHEN regexp_like(query, '(?i)^\s*DROP\s+TABLE\b') THEN 'drop'
 WHEN regexp_like(query, '(?i)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\b') THEN 'view_write'
 WHEN regexp_like(query, '(?i)^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\b')
  AND regexp_like(query, '(?i)\bAS\s*\(?\s*(SELECT|WITH|TABLE|VALUES)\b') THEN 'other'
 WHEN regexp_like(query, '(?i)^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\b') THEN 'ensure_table'
 WHEN regexp_like(query, '(?i)^\s*CREATE\s+TABLE\b')
  AND regexp_like(query, '(?i)\bAS\s*\(?\s*(SELECT|WITH|TABLE|VALUES)\b') THEN 'ctas'
 WHEN regexp_like(query, '(?i)^\s*CREATE\s+TABLE\b') THEN 'table_ddl'
 WHEN regexp_like(query, '(?i)^\s*ALTER\s+TABLE\b') THEN 'alter_write'
 WHEN regexp_like(query, '(?i)^\s*EXPLAIN\s+ANALYZE\b') THEN 'read_exec'
 WHEN regexp_like(query, '(?i)^\s*(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN)\b')
  THEN 'read' ELSE 'other' END'''
S4_SQL = "SELECT query_id, state FROM system.runtime.queries WHERE query_id = '{query_id}'"
S5_SQL = "SELECT min(created) AS oldest, max(created) AS newest, count(*) AS n FROM system.runtime.queries"
S6_SQL = 'SELECT "user", count(*) AS n FROM system.runtime.queries GROUP BY "user"'
S7_SQL = '''SELECT count(*) AS snapshots FROM iceberg.bronze."{table}$snapshots"
WHERE committed_at >= TIMESTAMP '{t_from}' AND committed_at < TIMESTAMP '{t_to}' '''
P1_PATTERN = r'(?i)\b(?:FROM|JOIN|USING)\s+([\w$"]+(?:\s*\.\s*[\w$"]+){0,2})'
P2_PATTERN = r'(?i)^\s*(?:INSERT\s+INTO|MERGE\s+INTO|UPDATE|DELETE\s+FROM|CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE(?:\s+IF\s+EXISTS)?)\s+([\w$"]+(?:\s*\.\s*[\w$"]+){0,2})'
P3_PATTERN = r'(?i)(?:\bWITH|,)\s*([\w$"]+)\s+AS\s*\('


def utc_datetime(value: str | datetime) -> datetime:
    """Accept an explicit UTC instant, never the host's implicit timezone."""
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValueError("an explicit UTC timestamp is required")
    return result.astimezone(UTC)


def utc_text(value: datetime) -> str:
    return utc_datetime(value).isoformat().replace("+00:00", "Z")


def minute_utc(value: str | datetime) -> str:
    return utc_text(utc_datetime(value).replace(second=0, microsecond=0))


def day_bounds(day: str) -> tuple[datetime, datetime]:
    parsed = date.fromisoformat(day)
    if parsed.isoformat() != day:
        raise ValueError("day must be YYYY-MM-DD")
    start = datetime(parsed.year, parsed.month, parsed.day, tzinfo=UTC)
    return start, start + timedelta(days=1)


def attribution(user: Any) -> str:
    if user == SOURCE_USER:
        return SOURCE_USER
    return "platform" if isinstance(user, str) and user in PLATFORM_USERS else "unknown"


def is_fotmob_io_table(name: str) -> bool:
    parts = name.lower().split(".")
    return len(parts) == 3 and parts[:2] == ["iceberg", "bronze"] and bool(TABLE_NAME_RE.fullmatch(parts[2]))


def is_ephemeral_stage(name: str) -> bool:
    parts = name.lower().split(".")
    return (len(parts) == 1 or parts[:-1] == ["iceberg", "bronze"]) and bool(EPHEMERAL_STAGE_RE.fullmatch(parts[-1]))


def discovered_tables(names: Iterable[str]) -> list[str]:
    result = set()
    for name in names:
        name = name.lower()
        if TABLE_NAME_RE.fullmatch(name):
            result.add("iceberg.bronze." + name)
        elif name.startswith("iceberg.bronze.") and TABLE_NAME_RE.fullmatch(name.split(".")[-1]) and len(name.split(".")) == 3:
            result.add(name)
    return sorted(result)


@dataclass(frozen=True)
class Token:
    value: str
    kind: str


@dataclass(frozen=True)
class ParsedSQL:
    stmt_type: str
    operations: tuple[tuple[str, str], ...]
    unparsed: bool = False
    unparsed_fotmob: bool = False


def _skip_sql_trivia(sql: str, index: int) -> int:
    while index < len(sql):
        if sql[index].isspace():
            index += 1
        elif sql.startswith("--", index):
            end = sql.find("\n", index + 2)
            index = len(sql) if end == -1 else end + 1
        elif sql.startswith("/*", index):
            # Nested block comments are legal; their contents are never SQL.
            depth = 1
            index += 2
            while index < len(sql) and depth:
                if sql.startswith("/*", index):
                    depth += 1
                    index += 2
                elif sql.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise ValueError("unterminated comment")
        else:
            break
    return index


def _tokens(sql: str) -> list[Token]:
    """P0: discard comments; keep literals opaque and quoted identifiers distinct."""
    result: list[Token] = []
    index = _skip_sql_trivia(sql, 0)
    while index < len(sql):
        char = sql[index]
        if char in "'\"":
            quote = char
            index += 1
            value = []
            while index < len(sql):
                if sql[index] == quote:
                    if index + 1 < len(sql) and sql[index + 1] == quote:
                        value.append(quote)
                        index += 2
                        continue
                    index += 1
                    break
                value.append(sql[index])
                index += 1
            else:
                raise ValueError("unterminated quoted token")
            result.append(Token("" if quote == "'" else "".join(value).lower(), "literal" if quote == "'" else "identifier"))
        elif char.isalpha() or char == "_":
            end = index + 1
            while end < len(sql) and (sql[end].isalnum() or sql[end] in "_$"):
                end += 1
            result.append(Token(sql[index:end].lower(), "word"))
            index = end
        elif char.isdigit():
            end = index + 1
            while end < len(sql) and (sql[end].isdigit() or sql[end] == "."):
                end += 1
            result.append(Token(sql[index:end], "number"))
            index = end
        elif char in "().,;*+-/%=<>!|[]:?":
            result.append(Token(char, "punct"))
            index += 1
        else:
            raise ValueError("unsupported token")
        index = _skip_sql_trivia(sql, index)
    if result and result[-1].value == ";":
        result.pop()
    if not result or any(t.value == ";" and t.kind == "punct" for t in result):
        raise ValueError("empty or multiple statements")
    return result


def _word(tokens: Sequence[Token], index: int, word: str) -> bool:
    return index < len(tokens) and tokens[index].kind == "word" and tokens[index].value == word


def _name(tokens: Sequence[Token], index: int) -> tuple[tuple[str, ...], int]:
    parts = []
    while index < len(tokens):
        if tokens[index].kind not in {"word", "identifier"}:
            raise ValueError("expected table identifier")
        if tokens[index].kind == "word" and tokens[index].value in {
            "select", "from", "where", "join", "values", "only", "if", "exists", "not",
            "table", "lateral", "unnest", "using", "as", "into", "set", "with", "when",
            "on", "delete", "insert", "update", "merge", "create", "alter",
        }:
            raise ValueError("unsupported relation or target syntax")
        parts.append(tokens[index].value)
        index += 1
        if index >= len(tokens) or tokens[index].value != ".":
            break
        index += 1
    if not parts or len(parts) > 3 or (index and tokens[index - 1].value == "."):
        raise ValueError("invalid qualified identifier")
    return tuple(parts), index


def _resolve(parts: tuple[str, ...], discovered: set[str]) -> str | None:
    if len(parts) == 1:
        candidates = [table for table in discovered if table.split(".")[-1] == parts[0]]
        if len(candidates) > 1:
            raise ValueError("ambiguous identifier")
        return candidates[0] if candidates else None
    full = parts if len(parts) == 3 else ("iceberg",) + parts
    if full[:2] == ("iceberg", "bronze") and TABLE_NAME_RE.fullmatch(full[2]):
        return ".".join(full)
    return None


def _parentheses(tokens: Sequence[Token]) -> tuple[dict[int, int], list[int]]:
    stack: list[int] = []
    matches: dict[int, int] = {}
    depths = []
    for index, token in enumerate(tokens):
        depths.append(len(stack))
        if token.kind == "punct" and token.value == "(":
            stack.append(index)
        elif token.kind == "punct" and token.value == ")":
            if not stack:
                raise ValueError("unbalanced parentheses")
            matches[stack.pop()] = index
    if stack:
        raise ValueError("unbalanced parentheses")
    return matches, depths


def _cte_scopes(tokens: Sequence[Token], matches: Mapping[int, int], depths: Sequence[int]) -> list[tuple[str, int, int]]:
    scopes = []
    for index in range(len(tokens)):
        if not _word(tokens, index, "with"):
            continue
        if index + 1 < len(tokens) and tokens[index + 1].value == "(":
            continue  # CTAS table properties; no CTE alias.
        end = next((j for j in range(index + 1, len(tokens)) if tokens[j].value == ")" and depths[j] == depths[index]), len(tokens))
        cursor = index + 1
        while True:
            if cursor >= len(tokens) or tokens[cursor].kind not in {"word", "identifier"} or _word(tokens, cursor, "recursive"):
                raise ValueError("unsupported CTE")
            alias = tokens[cursor].value
            if not _word(tokens, cursor + 1, "as") or cursor + 2 not in matches:
                raise ValueError("unsupported CTE definition")
            body = cursor + 2
            if not (_word(tokens, body + 1, "select") or _word(tokens, body + 1, "with")):
                raise ValueError("unsupported CTE body")
            cursor = matches[body] + 1
            # A nonrecursive alias is not in scope inside its own definition.
            scopes.append((alias, cursor, end))
            if cursor < len(tokens) and tokens[cursor].value == ",":
                cursor += 1
                continue
            if not (_word(tokens, cursor, "select") or _word(tokens, cursor, "with")):
                raise ValueError("unsupported statement after WITH")
            break
    return scopes


def _classify(tokens: Sequence[Token]) -> tuple[str, int | None]:
    for words, kind in ((('insert', 'into'), 'insert'), (('merge', 'into'), 'merge'),
                        (('update',), 'update'), (('delete', 'from'), 'delete'),
                        (('create', 'table'), 'ctas'), (('alter', 'table'), 'alter_write'),
                        (('drop', 'table'), 'drop'), (('create', 'view'), 'view_write'),
                        (('create', 'or', 'replace', 'view'), 'view_write')):
        if all(_word(tokens, i, word) for i, word in enumerate(words)):
            if kind == "ctas" and all(_word(tokens, 2 + i, word) for i, word in enumerate(("if", "not", "exists"))):
                return "ensure_table", 5
            return kind, len(words)
    if _word(tokens, 0, "explain") and _word(tokens, 1, "analyze"):
        if not (_word(tokens, 2, "select") or _word(tokens, 2, "with")):
            raise ValueError("EXPLAIN ANALYZE of writes is outside the grammar")
        return "read_exec", None
    if any(_word(tokens, 0, word) for word in ("select", "with", "show", "describe", "desc", "explain")):
        return "read", None
    raise ValueError("statement outside the grammar")


def _opens_column_list(tokens, after, matches):
    """A real '(' token that opens a group; a quoted identifier named '(' is not one."""
    return after < len(tokens) and tokens[after].kind == "punct" and tokens[after].value == "(" and after in matches


def _opens_column_list(tokens, after, matches):
    """A real '(' token that opens a group; a quoted identifier named '(' is not one."""
    return after < len(tokens) and tokens[after].kind == "punct" and tokens[after].value == "(" and after in matches


def _ctas_query(tokens, after, depths):
    """A top-level AS before SELECT or WITH: the query that makes CREATE TABLE a write."""
    words = {t.value for i, t in enumerate(tokens) if i >= after and depths[i] == 0 and t.kind == "word"}
    return {"as"} <= words and bool({"select", "with"} & words)


def _ddl_operations(kind, tokens, after, target, matches):
    """Operations of the DDL the contour emits; other shapes stay unparsed.

    A view definition and an existing-table no-op are analyzed only, so they
    neither scan nor commit. Creating a table commits its first snapshot.
    """
    if target is None:
        raise ValueError("DDL must name one known table or view")
    if kind == "view_write":
        if not (_word(tokens, after, "as") and (_word(tokens, after + 1, "select") or _word(tokens, after + 1, "with"))):
            raise ValueError("CREATE VIEW without AS SELECT")
        return ()
    if not _opens_column_list(tokens, after, matches):
        raise ValueError("CREATE TABLE without a column list")
    options = matches[after] + 1
    if options < len(tokens):
        if not (_word(tokens, options, "with") and matches.get(options + 1) == len(tokens) - 1):
            raise ValueError("unsupported CREATE TABLE options")
    return () if kind == "ensure_table" else ((target, "commit"),)


def parse_sql(sql: str, tables: Iterable[str]) -> ParsedSQL:
    """P0–P4, one scan per distinct source table in a statement; fail closed."""
    known = set(discovered_tables(tables))
    try:
        tokens = _tokens(sql)
        matches, depths = _parentheses(tokens)
        kind, target_index = _classify(tokens)
        if kind in {"view_write", "ensure_table", "ctas"}:
            parts, after = _name(tokens, target_index)
            # DDL defines schema only: its column list and view body are never
            # relations. A column list alone does not make DDL: CREATE TABLE
            # stays on the query path whenever a top-level AS introduces a query.
            if kind != "ctas" or (not _ctas_query(tokens, after, depths)
                                  and _opens_column_list(tokens, after, matches)):
                return ParsedSQL("table_ddl" if kind == "ctas" else kind,
                                 _ddl_operations(kind, tokens, after, _resolve(parts, known), matches))
        scopes = _cte_scopes(tokens, matches, depths)
        target = None
        if target_index is not None:
            if kind == "drop" and _word(tokens, target_index, "if"):
                # Only DROP TABLE [IF EXISTS] name; modifiers stay outside the grammar.
                if not _word(tokens, target_index + 1, "exists"):
                    raise ValueError("incomplete DROP")
                target_index += 2
            parts, after_target = _name(tokens, target_index)
            target = _resolve(parts, known)
            top_words = {t.value for i, t in enumerate(tokens) if i >= after_target and depths[i] == 0 and t.kind == "word"}
            if kind == "ctas" and not ({"as"} <= top_words and ({"select", "with"} & top_words)):
                raise ValueError("CREATE without AS SELECT")
            if kind == "insert" and not ({"values", "select", "with"} & top_words):
                raise ValueError("INSERT without VALUES or SELECT")
            if kind == "update" and "set" not in top_words:
                raise ValueError("UPDATE without SET")
            if kind == "merge" and not {"using", "on", "when"} <= top_words:
                raise ValueError("incomplete MERGE")
            if kind == "alter_write" and after_target >= len(tokens):
                raise ValueError("incomplete ALTER")
            if kind == "drop" and (target is None or after_target != len(tokens)):
                raise ValueError("DROP must name exactly one known table")
        sources = set()
        from_depths: set[int] = set()
        for index, token in enumerate(tokens):
            depth = depths[index]
            if token.kind == "word" and token.value in {"where", "group", "order", "having", "limit", "union", "except", "intersect", "window", "returning", "when", "set"}:
                from_depths.discard(depth)
            if token.value == ")" and token.kind == "punct":
                from_depths = {d for d in from_depths if d < depth}
            if token.value == "," and token.kind == "punct" and depth in from_depths:
                raise ValueError("comma joins are outside P1; use explicit JOIN")
            if token.kind != "word" or token.value not in {"from", "join", "using"}:
                continue
            if token.value == "from":
                from_depths.add(depth)
            cursor = index + 1
            if cursor >= len(tokens):
                raise ValueError("missing relation")
            if tokens[cursor].value == "(":
                if token.value != "using" and not (_word(tokens, cursor + 1, "select") or _word(tokens, cursor + 1, "with")):
                    raise ValueError("unsupported derived relation")
                continue
            parts, after = _name(tokens, cursor)
            if after < len(tokens) and tokens[after].value == "(":
                raise ValueError("table functions are outside P1")
            if after < len(tokens) and tokens[after].value in {"tablesample", "match_recognize"}:
                raise ValueError("unsupported relation modifier")
            if len(parts) == 1 and any(alias == parts[0] and begin <= cursor < end for alias, begin, end in scopes):
                continue
            resolved = _resolve(parts, known)
            if resolved:
                sources.add(resolved)
        operations = []
        if kind in WRITE_TYPES:
            if target:
                operations.append((target, "commit"))
                if kind in {"update", "delete"}:
                    sources.add(target)
            operations.extend((table, "scan_write") for table in sorted(sources))
        else:
            operations.extend((table, "scan_read") for table in sorted(sources))
        return ParsedSQL(kind, tuple(sorted(operations)))
    except (ValueError, IndexError):
        mentions = any(re.search(r"(?<![\w$])" + re.escape(table.split(".")[-1]) + r"(?![\w$])", sql, re.IGNORECASE) for table in known)
        return ParsedSQL("other", (), True, mentions)


def read_raw(paths: Iterable[Path]) -> list[dict[str, Any]]:
    records = []
    for path in sorted(paths, key=lambda p: (p.name, str(p))):
        with path.open(encoding="utf-8") as source:
            for line in source:
                if line.strip():
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise ValueError("raw JSONL must contain objects")
                    records.append(record)
    return records


def _query_time(record: Mapping[str, Any]) -> datetime:
    for field in ("ended", "started", "created"):
        if record.get(field) is not None:
            return utc_datetime(record[field])
    raise ValueError("snapshot has no timestamp")


def _query_work(records: Sequence[Mapping[str, Any]], parsed: ParsedSQL, tables: set[str]) -> tuple[dict[tuple[str, str], set[str]], str, bool]:
    """Combine physical evidence with the first snapshot's SQL for one query ID."""
    first = records[0]

    def poll_ids(record: Mapping[str, Any]) -> set[str]:
        return {str(record["poll_id"])} if record.get("poll_id") else set()

    operations = {operation: poll_ids(first) for operation in parsed.operations}
    physical: dict[tuple[str, str], set[str]] = {}
    outputs = set()
    for record in records:
        for field in ("io_input_tables", "io_output_tables", "io_tables"):
            for name in record.get(field, []):
                table = name.lower()
                if field == "io_output_tables":
                    outputs.add(table)
                if is_fotmob_io_table(table):
                    physical.setdefault((table, field), set()).update(poll_ids(record))

    user = first.get("user")
    principal_conflict = any(record.get("user") != user for record in records)
    user_attr = "unknown" if principal_conflict else attribution(user)
    uncertain = parsed.unparsed_fotmob or (principal_conflict and bool(operations or physical))
    write = parsed.stmt_type in WRITE_TYPES
    if physical or principal_conflict:
        for query in {record["query"] for record in records} - {first["query"]}:
            other = parse_sql(query, tables)
            if principal_conflict and (other.operations or other.unparsed_fotmob):
                uncertain = True
            if physical and other != parsed:
                uncertain = True
                if other.unparsed or (other.stmt_type in WRITE_TYPES) != write:
                    return operations, user_attr, True
    if parsed.unparsed:
        return operations, user_attr, uncertain or bool(physical)

    flat_tables = set()
    for (table, field), polls in physical.items():
        if field == "io_output_tables":
            if not write:
                continue
            operation = "commit"
        elif field == "io_tables" and write:
            flat_tables.add(table)
            continue
        elif field == "io_tables" and table in outputs:
            continue
        else:
            operation = "scan_write" if write else "scan_read"
        operations.setdefault((table, operation), set()).update(polls)
    # Old flat snapshots can corroborate a write, but cannot assign a role.
    if flat_tables - {table for table, _ in operations}:
        uncertain = True
    sql_targets = {table for table, operation in parsed.operations if operation == "commit"}
    if outputs and (physical or operations) and (not write or len(outputs) > 1 or sql_targets - outputs):
        uncertain = True
    return operations, user_attr, uncertain


def _gap_intervals(reasons: Sequence[str | None], start: datetime) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for index, reason in enumerate(reasons):
        if reason is None:
            continue
        begin = utc_text(start + index * MINUTE)
        end = utc_text(start + (index + 1) * MINUTE)
        if result and result[-1]["reason"] == reason and result[-1]["end"] == begin:
            result[-1]["end"] = end
        else:
            result.append({"start": begin, "end": end, "reason": reason})
    return result


def _coverage(heartbeats: Sequence[Mapping[str, Any]], start: datetime, end: datetime) -> tuple[list[str | None], list[Mapping[str, Any]]]:
    segments: list[list[tuple[datetime, datetime]]] = [[] for _ in range(1440)]
    present = [False] * 1440
    lost = [False] * 1440
    down = [False] * 1440
    relevant = []
    for beat in heartbeats:
        left, right = utc_datetime(beat["window_start"]), utc_datetime(beat["window_end"])
        if left >= right:
            raise ValueError("heartbeat window must be nonempty")
        observed = utc_datetime(beat["polled_at"])
        if right <= start or left >= end:
            if not start <= observed - timedelta(microseconds=1) < end:
                continue
        relevant.append(beat)
        marker = int(((observed - timedelta(microseconds=1)) - start).total_seconds() // 60)
        if 0 <= marker < 1440:
            present[marker] = True
        first = max(0, int((left - start).total_seconds() // 60))
        last = min(1439, int(((right - timedelta(microseconds=1)) - start).total_seconds() // 60))
        for index in range(first, last + 1):
            present[index] = True
            if beat.get("status", "ok") != "ok":
                down[index] = True
            elif beat.get("watermark_ok") is True:
                segments[index].append((max(left, start + index * MINUTE), min(right, start + (index + 1) * MINUTE)))
            elif beat.get("gap_reason") != "no_sample":
                lost[index] = True
    reasons: list[str | None] = []
    for index in range(1440):
        cursor = start + index * MINUTE
        minute_end = cursor + MINUTE
        for left, right in sorted(segments[index]):
            if left > cursor:
                break
            cursor = max(cursor, right)
        if cursor >= minute_end:
            reasons.append(None)
        elif lost[index]:
            reasons.append("history_lost")
        elif down[index] or not present[index]:
            reasons.append("producer_down")
        else:
            reasons.append("no_sample")
    return reasons, relevant


def _valid_operation(table: Any, op: Any) -> bool:
    return (isinstance(table, str) and table == table.lower() and is_fotmob_io_table(table)
            and isinstance(op, str) and op in OPERATION_FIELDS)


def _operation_pairs(value: Any) -> set[tuple[str, str]]:
    if not isinstance(value, list) or not value:
        raise ValueError("invalid_operations")
    pairs = set()
    for item in value:
        if not isinstance(item, list) or len(item) != 2:
            raise ValueError("invalid_operations")
        table, op = item
        if not _valid_operation(table, op) or (table, op) in pairs:
            raise ValueError("invalid_operations")
        pairs.add((table, op))
    return pairs


def _receipt_index(registry: Any, day: str) -> dict[str, dict[str, Any]]:
    if (not isinstance(registry, dict) or set(registry) != {"day_utc", "receipts"}
            or registry["day_utc"] != day or not isinstance(registry["receipts"], list)):
        raise ValueError("invalid_registry")
    receipts = {}
    fields = {"day_utc", "job", "evidence_ref", "query_id", "user", "sql_sha256", "operations"}
    for receipt in registry["receipts"]:
        if not isinstance(receipt, dict) or set(receipt) != fields:
            raise ValueError("invalid_receipt")
        query_id = receipt["query_id"]
        if (receipt["day_utc"] != day or receipt["job"] not in MAINTENANCE_JOBS
                or not isinstance(receipt["evidence_ref"], str) or not receipt["evidence_ref"].strip()
                or not isinstance(query_id, str) or not QUERY_ID_RE.fullmatch(query_id)
                or not isinstance(receipt["user"], str) or receipt["user"] not in PLATFORM_USERS
                or not isinstance(receipt["sql_sha256"], str) or not re.fullmatch("[0-9a-f]{64}", receipt["sql_sha256"])):
            raise ValueError("invalid_receipt")
        if query_id in receipts:
            raise ValueError("duplicate_receipt")
        _operation_pairs(receipt["operations"])
        receipts[query_id] = receipt
    return receipts


def _norm_limit(norm: str, table: str | None) -> int:
    control = table in CONTROL_TABLES
    if norm == "N1":
        return CONTROL_N1_MAX_COMMITS_PER_MIN if table in WAVE_JOURNAL_TABLES else N1_MAX_COMMITS_PER_MIN
    if norm == "N2":
        return CONTROL_N2_MAX_POLLS_PER_MIN if control else N2_MAX_POLLS_PER_MIN
    if norm == "N3":
        return N3_MAX_COMMITS_PER_DAY
    if norm == "N4":
        return CONTROL_N4_MAX_SCANS_PER_DAY if control else N4_MAX_SCANS_PER_DAY
    raise ValueError("unknown source norm")


def _norm_order(norm: str, table: str, value: int) -> tuple[int, int, str]:
    """Largest excess first, otherwise largest measurement. Break ties by table."""
    limit = _norm_limit(norm, table)
    if value > limit:
        return (0, limit - value, table)
    return (1, -value, table)


def aggregate_day(records: Iterable[Mapping[str, Any]], day: str, *, snapshot_counts: Mapping[str, int] | None = None, generated_at: str | None = None, maintenance_registry: Any = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Pure snapshot → minute → day pipeline. Missing S7 evidence blocks PASS."""
    start, end = day_bounds(day)
    seen: dict[str, Mapping[str, Any]] = {}
    repeat_records: dict[str, list[Mapping[str, Any]]] = {}
    duplicates = 0
    conflicts = set()
    heartbeats = []
    core_fields = ("user", "source", "query", "state", "created", "started", "ended", "stmt_type", "error_code")
    for record in records:
        if record.get("grain") == "heartbeat":
            heartbeats.append(record)
            continue
        if record.get("grain", "snapshot") != "snapshot" or not isinstance(record.get("query_id"), str) or not record["query_id"]:
            raise ValueError("invalid raw record")
        query_id = record["query_id"]
        if query_id in seen:
            first = seen[query_id]
            if start <= _query_time(first) < end:
                duplicates += 1
                if any(first.get(key) != record.get(key) for key in core_fields):
                    conflicts.add(query_id)
                repeat_records.setdefault(query_id, []).append(record)
        else:
            seen[query_id] = record
    reasons, relevant = _coverage(heartbeats, start, end)
    tables = set(discovered_tables(name for beat in relevant for name in beat.get("discovered_tables", [])))
    counts: Counter[tuple[str, str, str, str]] = Counter()
    procedure_ops: Counter[tuple[str, str]] = Counter()
    evidence: dict[tuple[str, str, str, str], set[str]] = {}
    unparsed_queries = 0
    unparsed_fotmob = False
    observed = {}
    for query_id, record in seen.items():
        instant = _query_time(record)
        if not start <= instant < end:
            continue
        sql = record["query"]
        parsed = parse_sql(sql, tables)
        unparsed_queries += int(parsed.unparsed)
        snapshots = [record, *repeat_records.get(query_id, [])]
        operations, user_attr, uncertain = _query_work(snapshots, parsed, tables)
        unparsed_fotmob |= uncertain
        maintenance_procedure = (
            user_attr == "platform" and parsed.stmt_type == "alter_write"
            and not parsed.unparsed and not uncertain
            and MAINTENANCE_PROCEDURE_RE.match(sql[_skip_sql_trivia(sql, 0):]) is not None
            and all(snapshot["query"] == sql for snapshot in snapshots)
        )
        if maintenance_procedure:
            procedure_ops.update(operations.keys())
        if maintenance_registry is not None and operations and user_attr != SOURCE_USER:
            observed[query_id] = {
                "user": record.get("user"), "source": record.get("source"), "user_attr": user_attr,
                "ended": record.get("ended"), "state": record.get("state"), "error_code": record.get("error_code"),
                "stmt_type": None if uncertain or parsed.unparsed else parsed.stmt_type,
                "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                "operations": [list(pair) for pair in sorted(operations)],
            }
            if maintenance_procedure:
                # A receipt for this query can corroborate the same operations.
                observed[query_id]["maintenance_procedure"] = True
        for (table, operation), polls in operations.items():
            key = (minute_utc(instant), table, user_attr, operation)
            counts[key] += 1
            evidence.setdefault(key, set()).update(polls)
    tables.update(key[1] for key in counts)
    minute_rows = [{"grain": "minute_table", "minute_utc": key[0], "table": key[1], "user_attr": key[2], "operation": key[3], "count": count, "coverage": "measured", "gap_reason": None, "source_polls": sorted(evidence.get(key, set()))} for key, count in sorted(counts.items())]
    for index, reason in enumerate(reasons):
        if reason:
            minute_rows.append({"grain": "minute_table", "minute_utc": utc_text(start + index * MINUTE), "table": None, "user_attr": "unknown", "operation": "scan_read", "count": None, "coverage": "gap", "gap_reason": reason, "source_polls": []})
    minute_rows.sort(key=lambda row: (row["minute_utc"], row["table"] or "", row["user_attr"], row["operation"]))
    coverage = reasons.count(None)
    gaps = _gap_intervals(reasons, start)
    day_rows = []
    for table in sorted(tables):
        for attr in ATTRIBUTIONS:
            items = [(key, count) for key, count in counts.items() if key[1:3] == (table, attr)]
            if attr != SOURCE_USER and not items:
                continue
            totals = Counter()
            for key, count in items:
                totals[key[3]] += count
            day_rows.append({
                "grain": "day_table", "day_utc": day, "table": table, "user_attr": attr,
                "commits": totals["commit"], "scan_reads": totals["scan_read"], "scan_writes": totals["scan_write"],
                "scans_total": totals["scan_read"] + totals["scan_write"],
                "peak_commits_per_min": max((count for key, count in items if key[3] == "commit"), default=0),
                "peak_polls_per_min": max((count for key, count in items if key[3] == "scan_read"), default=0) if table in POLL_ELIGIBLE_TABLES else 0,
                "coverage_minutes": coverage, "gap_intervals": copy.deepcopy(gaps), "completeness": "full" if coverage == 1440 and not gaps else "gap",
            })
    peak_evidence = {}
    for norm, operation in (("N1", "commit"), ("N2", "scan_read")):
        candidates = ((table, minute, count) for (minute, table, attr, op), count in counts.items()
                      if attr == SOURCE_USER and op == operation and not is_ephemeral_stage(table)
                      and (norm != "N2" or table in POLL_ELIGIBLE_TABLES))
        worst = min(candidates, key=lambda item: (_norm_order(norm, item[0], item[2]), item[1]), default=None)
        peak_evidence[norm] = f"{worst[0]}@{worst[1]}" if worst else None
    checks = []
    for table in sorted({key[1] for key in counts if key[3] == "commit" and not is_ephemeral_stage(key[1])}):
        sql_commits = sum(value for key, value in counts.items() if key[1] == table and key[3] == "commit")
        snapshots = None if snapshot_counts is None else snapshot_counts.get(table)
        if snapshots is not None and (type(snapshots) is not int or snapshots < 0):
            raise ValueError("snapshot counts must be nonnegative integers")
        difference = None if snapshots is None else snapshots - sql_commits
        checks.append({"table": table, "sql_commits": sql_commits, "snapshot_commits": snapshots, "difference": difference, "match": None if difference is None else difference == 0})
    platform_rows = [row for row in day_rows if row["user_attr"] == "platform"]
    unknown_rows = [row for row in day_rows if row["user_attr"] == "unknown"]
    intervals = sorted({beat.get("poll_interval_s", 60) for beat in relevant})
    variants = sorted({beat.get("producer_variant", PRODUCER_VARIANT) for beat in relevant})
    report = {
        "grain": "day_report", "day_utc": day,
        # The default is a logical report slot, not the host's current clock.
        "generated_at": utc_text(utc_datetime(generated_at)) if generated_at else utc_text(end + timedelta(minutes=5)),
        "producer_variant": variants[0] if len(variants) == 1 else ("mixed" if variants else PRODUCER_VARIANT),
        "poll_interval_s": max(intervals, default=60), "poll_intervals_s": intervals,
        "coverage_minutes_total": coverage, "gap_intervals_total": gaps,
        "completeness": "full" if coverage == 1440 and not gaps else "gap",
        "discovered_tables": sorted(tables), "unparsed_queries": unparsed_queries, "unparsed_fotmob": unparsed_fotmob,
        "duplicate_queries": duplicates, "conflicting_query_ids": sorted(conflicts),
        "n5_measurable": bool(relevant) and all(beat.get("n5_measurable") is True for beat in relevant),
        "platform_ops_total": sum(row["commits"] + row["scans_total"] for row in platform_rows),
        "unknown_ops_total": sum(row["commits"] + row["scans_total"] for row in unknown_rows),
        "tables": day_rows, "platform_activity": platform_rows, "unknown_activity": unknown_rows,
        "maintenance_procedure_ops": {"platform": [[table, op, count] for (table, op), count in sorted(procedure_ops.items())]},
        "peak_evidence": peak_evidence, "commit_checks": checks,
        "commit_check_complete": all(row["match"] is not None for row in checks),
        "commit_mismatch": any(row["match"] is False for row in checks),
    }
    if maintenance_registry is not None:
        report["maintenance_evidence"] = {"observed": observed, "registry": maintenance_registry}
        try:
            _receipt_index(maintenance_registry, day)
        except ValueError as exc:
            # Do not copy unrecognised fields (possibly SQL) into the day artifact.
            report["maintenance_evidence"] = {"observed": observed, "registry_error": str(exc)}
    return minute_rows, evaluate_day(report)


def _maintenance_deductions(report: Mapping[str, Any], raw: Counter) -> dict[str, list[list[str]]]:
    evidence = report["maintenance_evidence"]
    if not isinstance(evidence, dict):
        raise ValueError("invalid_maintenance_evidence")
    receipts = _receipt_index(evidence.get("registry"), report["day_utc"])
    observed = evidence.get("observed")
    conflicts = report.get("conflicting_query_ids")
    if not isinstance(observed, dict) or not isinstance(conflicts, list):
        raise ValueError("invalid_observations")
    census: Counter = Counter()
    deductions = {}
    for query_id, query in observed.items():
        if not isinstance(query, dict) or query.get("user_attr") not in ("platform", "unknown"):
            raise ValueError("invalid_observation")
        pairs = _operation_pairs(query.get("operations"))
        census.update((query["user_attr"], table, op) for table, op in pairs)
        receipt = receipts.get(query_id)
        if receipt is None:
            continue
        ended = query.get("ended")
        if (query["user_attr"] != "platform" or query.get("user") != receipt["user"]
                or query_id in conflicts or query.get("state") != "FINISHED"
                or query.get("error_code") is not None or not isinstance(ended, str)
                or utc_datetime(ended).date().isoformat() != receipt["day_utc"]
                or query.get("stmt_type") not in (*WRITE_TYPES, "read", "read_exec")
                or query.get("sql_sha256") != receipt["sql_sha256"]
                or pairs != _operation_pairs(receipt["operations"])):
            raise ValueError("receipt_query_mismatch")
        deductions[query_id] = query["operations"]
    if receipts.keys() != deductions.keys():
        raise ValueError("unobserved_receipt")
    if census != raw:
        raise ValueError("observation_counts_mismatch")
    return deductions


def _maintenance_procedure_counts(report: Mapping[str, Any], raw: Counter) -> Counter:
    stored = report.get("maintenance_procedure_ops", {"platform": []})
    if not isinstance(stored, dict) or set(stored) != {"platform"} or not isinstance(stored["platform"], list):
        raise ValueError("invalid_maintenance_procedure_ops")
    counts: Counter = Counter()
    for item in stored["platform"]:
        if not isinstance(item, list) or len(item) != 3:
            raise ValueError("invalid_maintenance_procedure_ops")
        table, op, count = item
        key = ("platform", table, op)
        if (not _valid_operation(table, op) or op == "scan_read" or type(count) is not int
                or not 0 < count <= raw[key] or key in counts):
            raise ValueError("invalid_maintenance_procedure_ops")
        counts[key] = count
    return counts


def _observed_procedure_ids(report: Mapping[str, Any], counts: Counter) -> set[str]:
    """Cross-check the optional receipt census before combining the two deductions."""
    procedure_ids = set()
    census: Counter = Counter()
    for query_id, query in report["maintenance_evidence"]["observed"].items():
        flag = query.get("maintenance_procedure", False)
        if flag is False:
            continue
        if flag is not True or query["user_attr"] != "platform" or query.get("stmt_type") != "alter_write":
            raise ValueError("invalid_maintenance_procedure_observation")
        procedure_ids.add(query_id)
        census.update(("platform", table, op) for table, op in query["operations"])
    if census != counts:
        raise ValueError("maintenance_procedure_counts_mismatch")
    return procedure_ids


def evaluate_day(report: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(report))
    # Re-derive totals and reconciliation flags on every verdict invocation.
    # A stale summary flag must never hide a foreign row or an S7 discrepancy.
    raw: Counter = Counter()
    for attr in ("platform", "unknown"):
        rows = [row for row in result["tables"] if row["user_attr"] == attr]
        for row in rows:
            row["scans_total"] = row["scan_reads"] + row["scan_writes"]
            for op, field in OPERATION_FIELDS.items():
                if row[field]:
                    raw[attr, row["table"], op] += row[field]
        result[f"{attr}_activity"] = rows
        result[f"{attr}_ops_total"] = sum(row["commits"] + row["scans_total"] for row in rows)
    sql_commits: Counter[str] = Counter()
    for row in result["tables"]:
        if row["commits"] and not is_ephemeral_stage(row["table"]):
            sql_commits[row["table"]] += row["commits"]
    saved_checks = [check for check in result.get("commit_checks", []) if not is_ephemeral_stage(check["table"])]
    checks_by_table = {check["table"]: check for check in saved_checks}
    if len(checks_by_table) != len(saved_checks):
        raise ValueError("duplicate table in commit reconciliation")
    checks = []
    for table in sorted(set(sql_commits) | set(checks_by_table)):
        snapshots = checks_by_table.get(table, {}).get("snapshot_commits")
        if snapshots is not None and (type(snapshots) is not int or snapshots < 0):
            raise ValueError("snapshot counts must be nonnegative integers")
        difference = None if snapshots is None else snapshots - sql_commits[table]
        checks.append({"table": table, "sql_commits": sql_commits[table], "snapshot_commits": snapshots,
                       "difference": difference, "match": None if difference is None else difference == 0})
    result["commit_checks"] = checks
    result["commit_check_complete"] = all(check["match"] is not None for check in checks)
    result["commit_mismatch"] = any(check["match"] is False for check in checks)
    source_rows = [row for row in result["tables"] if row["user_attr"] == SOURCE_USER and not is_ephemeral_stage(row["table"])]
    verdicts = {}
    for norm, field, unit in (
        ("N1", "peak_commits_per_min", "min"),
        ("N2", "peak_polls_per_min", "min"),
        ("N3", "commits", "day"),
        ("N4", "scans_total", "day"),
    ):
        candidates = (row for row in source_rows if norm != "N2" or row["table"] in POLL_ELIGIBLE_TABLES)
        worst = min(candidates, key=lambda row: _norm_order(norm, row["table"], row[field]), default=None)
        table = worst["table"] if worst else None
        value = worst[field] if worst else 0
        threshold = _norm_limit(norm, table)
        label = f"<={threshold}/{unit}"
        if table in (WAVE_JOURNAL_TABLES if norm == "N1" else CONTROL_TABLES) and norm != "N3":
            label += " (control)"
        proof = table if value else None
        if norm in {"N1", "N2"}:
            peak = result.get("peak_evidence", {}).get(norm)
            if peak is not None:
                if proof and peak.startswith(proof + "@"):
                    proof = peak
                else:
                    # Older days may only have the minute for a different table.
                    result["peak_evidence"][norm] = None
        verdicts[norm] = {"pass": value <= threshold, "value": value, "threshold": label, "evidence": proof}
    foreign = result["platform_ops_total"] + result["unknown_ops_total"]
    procedure_counts = _maintenance_procedure_counts(result, raw)
    blocking = raw.copy()
    blocking.subtract(procedure_counts)
    result["maintenance_procedure_ops_total"] = sum(procedure_counts.values())
    result.pop("maintenance_error", None)
    deductions = {}
    procedure_ids: set[str] = set()
    if "maintenance_evidence" in result:
        try:
            deductions = _maintenance_deductions(result, raw)
        except ValueError as exc:
            result["maintenance_error"] = str(exc)
        else:
            if "maintenance_procedure_ops" in result:
                procedure_ids = _observed_procedure_ids(result, procedure_counts)
    for query_id, operations in deductions.items():
        if query_id not in procedure_ids:
            blocking.subtract(("platform", table, op) for table, op in operations)
    result["maintenance_deductions"] = deductions
    result["blocking_ops_total"] = sum(blocking.values())
    result["allowed_platform_ops_total"] = foreign - result["blocking_ops_total"]
    verdicts["N5"] = {
        "pass": result["blocking_ops_total"] == 0, "value": result["blocking_ops_total"],
        "threshold": "blocking_ops==0" if procedure_counts or "maintenance_evidence" in result else "foreign_ops==0",
        "evidence": min((table for (_, table, _), count in blocking.items() if count), default=None),
    }
    result["foreign_load"] = foreign > 0
    if foreign and (procedure_counts or "maintenance_evidence" in result):
        result["foreign_load_note"] = "Счётчики platform и unknown показывают все операции, включая подтверждённое обслуживание."
    elif foreign:
        result["foreign_load_note"] = "Все учтённые операции platform и unknown за день блокируют N5."
    else:
        result.pop("foreign_load_note", None)
    reasons = [norm for norm, verdict in verdicts.items() if not verdict["pass"]]
    if "maintenance_error" in result:
        reasons.append("maintenance_evidence_invalid")
    full = result["coverage_minutes_total"] == 1440 and not result["gap_intervals_total"] and result["completeness"] == "full"
    full = full and all(row["coverage_minutes"] == 1440 and not row["gap_intervals"] and row["completeness"] == "full" for row in result["tables"])
    if not full:
        reasons.append("gap")
    if result["unparsed_fotmob"]:
        reasons.append("unparsed_fotmob")
    if result.get("n5_measurable") is not True:
        reasons.append("n5_unmeasurable")
    if result.get("commit_check_complete") is not True:
        reasons.append("commit_check_missing")
    if result.get("commit_mismatch") is True:
        reasons.append("commit_mismatch")
    result.update(verdicts=verdicts, pass_reasons=reasons)
    result["pass"] = not reasons
    return result


def summary_line(report: Mapping[str, Any]) -> str:
    report = evaluate_day(report)
    reasons = report["pass_reasons"]
    verdict = "PASS" if report["pass"] else ("FAIL" if any(reason in {"N1", "N2", "N3", "N4", "N5"} for reason in reasons) else "NOT_PASS") + "(" + ",".join(reasons) + ")"
    gaps = report["gap_intervals_total"]
    gap_text = str(len(gaps))
    if gaps:
        descriptions = []
        for gap in gaps:
            ending = "24:00" if gap["end"][:10] != report["day_utc"] else gap["end"][11:16]
            descriptions.append(f"{gap['reason']}:{gap['start'][11:16]}-{ending}")
        gap_text += "(" + ",".join(descriptions) + ")"
    norms = report["verdicts"]
    split = (f"allowed_platform_ops={report['allowed_platform_ops_total']} blocking_ops={report['blocking_ops_total']} "
             if report["foreign_load"] or "maintenance_evidence" in report else "")
    return (f"Trino fotmob: day={report['day_utc']} verdict={verdict} "
            f"coverage={report['coverage_minutes_total']}/1440 tables={len(report['discovered_tables'])} user=src_fotmob "
            f"commits_day_max={norms['N3']['value']} scans_day_max={norms['N4']['value']} "
            f"peak_commit_min={norms['N1']['value']} peak_poll_min={norms['N2']['value']} "
            f"platform_ops={report['platform_ops_total']} unknown_ops={report['unknown_ops_total']} "
            f"{split}unparsed={report['unparsed_queries']} gaps={gap_text}")


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=".meter-", delete=False) as target:
            temporary = Path(target.name)
            target.write(content)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


@contextmanager
def _poll_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".poll.lock").open("a", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _password() -> str:
    value = os.environ.get(PASSWORD_ENV)
    if not value:
        raise ValueError(f"required environment variable: {PASSWORD_ENV}")
    return value


def _connect(trino: Any, password: str) -> Any:
    return trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "127.0.0.1"), port=int(os.environ.get("TRINO_PORT", "8082")),
                              user=SOURCE_USER, auth=trino.auth.BasicAuthentication(SOURCE_USER, password),
                              http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"), catalog="iceberg", schema="bronze", timezone="UTC",
                              source="fotmob_load_meter", request_timeout=10, max_attempts=1,
                              verify=False)  # self-signed certificate


def _fetch(connection: Any, sql: str) -> tuple[list[dict[str, Any]], str | None]:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        values = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in values], getattr(cursor, "query_id", None)
    finally:
        cursor.close()


def _server_time(connection: Any) -> datetime:
    rows, _ = _fetch(connection, "SELECT current_timestamp AS now_utc")
    value = rows[0]["now_utc"]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return utc_datetime(value)


def _stamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def snapshot_sql(table: str, day: str) -> str:
    if table not in discovered_tables([table]):
        raise ValueError("S7 requires a canonical FotMob table")
    start, end = day_bounds(day)
    return S7_SQL.format(table=table.split(".")[-1], t_from=_stamp(start), t_to=_stamp(end))


def _load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        result = json.load(source)
    if not isinstance(result, dict):
        raise ValueError("expected a JSON object")
    return result


def _load_maintenance_registry(path: Path) -> Any:
    def unique_keys(items: list) -> dict:
        obj = dict(items)
        if len(obj) != len(items):
            raise ValueError("duplicate registry key")
        return obj

    try:
        with path.open(encoding="utf-8") as source:
            registry = json.load(source, object_pairs_hook=unique_keys)
        return registry if isinstance(registry, dict) else {}
    except FileNotFoundError:
        return None
    except (OSError, ValueError, RecursionError):
        return {}  # Present but unusable. Aggregate publishes the diagnostic day.


def _rotate(root: Path, now: datetime) -> None:
    cutoff = (now - timedelta(days=35)).date()
    for folder in ("raw", "minute", "day", "verdict"):
        directory = root / folder
        if not directory.exists():
            continue
        for path in directory.iterdir():
            if path.is_symlink() or not path.is_file():
                continue
            raw_match = re.fullmatch(r"(\d{8})-\d{6}-[A-Za-z0-9_]+\.jsonl", path.name) if folder == "raw" else None
            daily_match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})\.jsonl?", path.name) if folder != "raw" else None
            try:
                if raw_match:
                    file_day = datetime.strptime(raw_match[1], "%Y%m%d").date()
                elif daily_match:
                    file_day = date.fromisoformat(daily_match[1])
                else:
                    continue
            except ValueError:
                continue
            if file_day < cutoff:
                path.unlink()


def poll(root: Path = DEFAULT_ROOT, *, poll_interval_s: int = 60, qualification: Mapping[str, Any] | None = None, discover_only: bool = False, snapshot_day: str | None = None, snapshot_tables: Sequence[str] = ()) -> dict[str, Any]:
    """Read-only network entry point; also serves discovery and daily S7 reads."""
    password = _password()
    import trino
    import trino.auth
    import trino.dbapi

    if not 15 <= poll_interval_s <= 120:
        raise ValueError("poll interval must be between 15 and 120 seconds")
    connection = _connect(trino, password)
    try:
        if discover_only:
            rows, _ = _fetch(connection, S2_SQL)
            return {"discovered_tables": discovered_tables(row["table_name"] for row in rows)}
        if snapshot_day is not None:
            now = _server_time(connection)
            _, end = day_bounds(snapshot_day)
            if end > now:
                raise ValueError("cannot reconcile an unfinished UTC day")
            counts = {}
            errors = {}
            for table in sorted(set(snapshot_tables)):
                try:
                    if is_ephemeral_stage(table):
                        continue
                    rows, _ = _fetch(connection, snapshot_sql(table, snapshot_day))
                    value = rows[0]["snapshots"]
                    if type(value) is not int or value < 0:
                        raise ValueError("invalid S7 count")
                    counts[table] = value
                except Exception as exc:
                    errors[table] = type(exc).__name__
            return {"day_utc": snapshot_day, "checked_at": utc_text(now), "counts": counts, "errors": errors}
        with _poll_lock(root):
            state_path = root / "raw" / "producer-state.json"
            previous = _load_json(state_path) if state_path.exists() else {}
            q = qualification or {}
            checks = q.get("checks", {})
            interval_matches = q.get("poll_interval_s") == poll_interval_s
            retention_ok = all(checks.get(key, {}).get("pass") is True for key in ("K0", "K1", "K2", "K3")) and interval_matches
            retention = q.get("retention_s", 0)
            now = _server_time(connection).replace(microsecond=0)
            left = now - timedelta(seconds=2 * poll_interval_s)
            poll_id = now.strftime("%Y%m%d_%H%M%S")
            beat = {"grain": "heartbeat", "poll_id": poll_id, "polled_at": utc_text(now),
                    "window_start": utc_text(left), "window_end": utc_text(now), "poll_interval_s": poll_interval_s,
                    "producer_variant": PRODUCER_VARIANT, "watermark_ok": False,
                    "n5_measurable": checks.get("K1", {}).get("pass") is True and interval_matches,
                    "discovered_tables": [], "status": "ok", "gap_reason": "no_sample"}
            records: list[dict[str, Any]] = []
            query_id = None
            try:
                discovery, _ = _fetch(connection, S2_SQL)
                beat["discovered_tables"] = discovered_tables(row["table_name"] for row in discovery)
                previous_id = previous.get("query_id")
                if previous_id and QUERY_ID_RE.fullmatch(previous_id):
                    marker, _ = _fetch(connection, S4_SQL.format(query_id=previous_id))
                    age = (now - utc_datetime(previous["polled_at"])).total_seconds()
                    beat["watermark_ok"] = bool(marker) and retention_ok and 0 <= age < retention
                    beat["gap_reason"] = None if beat["watermark_ok"] else "history_lost"
                beat["prev_poll_query_id"] = previous_id
                rows, query_id = _fetch(connection, S1_SQL.format(t_from=_stamp(left), t_to=_stamp(now)))
                for row in rows:
                    saved = dict(row)
                    for key in ("created", "started", "ended"):
                        if isinstance(saved.get(key), datetime):
                            value = saved[key]
                            saved[key] = utc_text((value.replace(tzinfo=UTC) if value.tzinfo is None else value).astimezone(UTC))
                    saved.update(grain="snapshot", poll_id=poll_id, observed_at=utc_text(now), stmt_type=parse_sql(row["query"], beat["discovered_tables"]).stmt_type)
                    records.append(saved)
                if not query_id or not QUERY_ID_RE.fullmatch(query_id):
                    raise ValueError("S1 did not expose its query id")
                beat["query_id"] = query_id
            except Exception as exc:
                # Never persist exception messages, connection strings or credentials.
                beat.update(status="error", watermark_ok=False, gap_reason="producer_down", error_type=type(exc).__name__)
            records.append(beat)
            suffix = query_id if query_id and QUERY_ID_RE.fullmatch(query_id) else "error"
            raw_path = root / "raw" / f"{now.strftime('%Y%m%d-%H%M%S')}-{suffix}.jsonl"
            if raw_path.exists():
                raise ValueError("refusing to overwrite a poll artifact")
            _atomic_write(raw_path, "".join(json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n" for record in records))
            if beat["status"] == "ok":
                _atomic_write(state_path, json_text({"query_id": query_id, "polled_at": utc_text(now)}))
            _rotate(root, now)
            return {"poll_id": poll_id, "raw_file": str(raw_path), "status": beat["status"], "watermark_ok": beat["watermark_ok"], "n5_measurable": beat["n5_measurable"]}
    finally:
        connection.close()


def qualify(*, poll_interval_s: int = 60, previous: Mapping[str, Any] | None = None, history_limit: int | None = None, peak: bool = False, slow_log_verified: bool = False, slow_log_threshold_s: float | None = None) -> dict[str, Any]:
    """One read-only K0–K3 sample; repeat three times across at least an hour."""
    result: dict[str, Any] = {"producer_variant": PRODUCER_VARIANT, "poll_interval_s": poll_interval_s, "checks": {}}
    checks = result["checks"]
    connection = None
    try:
        password = _password()
        import trino
        import trino.auth
        import trino.dbapi

        if not 15 <= poll_interval_s <= 120:
            raise ValueError("poll interval must be between 15 and 120 seconds")
        connection = _connect(trino, password)
        now = _server_time(connection)
        result["checked_at"] = utc_text(now)
        layout, _ = _fetch(connection, "DESCRIBE system.runtime.queries")
        columns = {str(row.get("Column", row.get("column", ""))).lower() for row in layout}
        required = {"query_id", "user", "source", "query", "state", "created", "started", "end", "error_code"}
        checks["K0"] = {"pass": required <= columns, "columns": sorted(columns), "missing_columns": sorted(required - columns), "timezone": "UTC", "client_importable": True}
        if not checks["K0"]["pass"]:
            raise ValueError("query history layout mismatch")
        users, _ = _fetch(connection, S6_SQL)
        # Retain categories and counts, never arbitrary user identities.
        visible = Counter()
        for row in users:
            visible[attribution(row["user"])] += int(row["n"])
        checks["K1"] = {"pass": visible["platform"] + visible["unknown"] > 0, "visible_categories": dict(sorted(visible.items()))}
        retention_rows, _ = _fetch(connection, S5_SQL)
        oldest = retention_rows[0]["oldest"]
        if isinstance(oldest, datetime):
            oldest = utc_text((oldest.replace(tzinfo=UTC) if oldest.tzinfo is None else oldest).astimezone(UTC))
        age = max(0, (now - utc_datetime(oldest)).total_seconds()) if oldest else 0
        compatible = previous and previous.get("poll_interval_s") == poll_interval_s and previous.get("history_limit") == history_limit
        samples = list(previous.get("retention_samples", [])) if compatible else []
        samples = [sample for sample in samples if utc_datetime(sample["at"]) < now]
        samples.append({"at": utc_text(now), "oldest_age_s": age, "n": int(retention_rows[0]["n"])})
        samples = samples[-3:]
        minimum = min(sample["oldest_age_s"] for sample in samples)
        span = (now - utc_datetime(samples[0]["at"])).total_seconds()
        below_cap = history_limit is not None and history_limit > 0 and all(sample["n"] < history_limit for sample in samples)
        checks["K2"] = {"pass": len(samples) == 3 and span >= 3600 and minimum >= max(300, 3 * poll_interval_s) and below_cap,
                        "samples": len(samples), "span_s": span, "retention_s": minimum, "below_confirmed_count_limit": below_cap,
                        "suggested_poll_interval_s": max(15, min(120, int(minimum // 3)))}
        result.update(retention_samples=samples, history_limit=history_limit, retention_s=minimum)
        started = time.monotonic()
        _fetch(connection, S1_SQL.format(t_from=_stamp(now - timedelta(seconds=2 * poll_interval_s)), t_to=_stamp(now)))
        duration = time.monotonic() - started
        checks["K3"] = {"pass": peak and slow_log_verified and slow_log_threshold_s is not None and duration < min(5, slow_log_threshold_s),
                        "snapshot_duration_s": round(duration, 6), "sample_at_peak": peak,
                        "slow_log_verified": slow_log_verified, "slow_log_threshold_s": slow_log_threshold_s}
    except Exception as exc:
        result["error_type"] = type(exc).__name__
    finally:
        if connection is not None:
            connection.close()
    for check in ("K0", "K1", "K2", "K3"):
        checks.setdefault(check, {"pass": False, "reason": "not_measured"})
    result["n5_measurable"] = checks["K1"]["pass"]
    result["pass"] = all(checks[key]["pass"] for key in ("K0", "K1", "K2", "K3"))
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("poll", "discover", "aggregate", "verdict", "line", "qualify"):
        command = commands.add_parser(name)
        command.add_argument("--root", type=Path, default=DEFAULT_ROOT)
        if name in {"aggregate", "verdict", "line"}:
            command.add_argument("--day", required=True)
        if name in {"poll", "qualify"}:
            command.add_argument("--poll-interval-s", type=int, choices=range(15, 121), default=60)
        if name == "poll":
            command.add_argument("--qualification", type=Path)
        if name == "aggregate":
            command.add_argument("--raw-dir", type=Path)
            command.add_argument("--offline", action="store_true", help="never connect; missing S7 evidence blocks PASS")
            command.add_argument("--snapshot-counts", type=Path, help="offline S7 evidence: day_utc, checked_at, counts")
            command.add_argument("--generated-at", help="explicit UTC report generation time")
        if name == "qualify":
            command.add_argument("--previous-report", type=Path)
            command.add_argument("--output", type=Path)
            command.add_argument("--history-limit", type=int)
            command.add_argument("--peak", action="store_true")
            command.add_argument("--slow-log-verified", action="store_true")
            command.add_argument("--slow-log-threshold-s", type=float)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command in {"poll", "discover"}:
            # Env contract first: no filesystem access before it. Path.exists()
            # raises PermissionError on unreadable parents (EACCES) on Python
            # <=3.11, which would mask the missing-password diagnostics.
            _password()
            qualification = None
            if args.command == "poll":
                path = args.qualification or args.root / "qualification.json"
                try:
                    qualification = _load_json(path) if path.exists() else None
                except OSError:
                    qualification = None
            result = poll(args.root, discover_only=args.command == "discover", qualification=qualification, poll_interval_s=getattr(args, "poll_interval_s", 60))
            print(json_text(result), end="")
            return 1 if result.get("status") == "error" else 0
        if args.command == "qualify":
            result = qualify(poll_interval_s=args.poll_interval_s, previous=_load_json(args.previous_report) if args.previous_report else None,
                             history_limit=args.history_limit, peak=args.peak, slow_log_verified=args.slow_log_verified, slow_log_threshold_s=args.slow_log_threshold_s)
            if args.output:
                _atomic_write(args.output, json_text(result))
            print(json_text(result), end="")
            return 0 if result["pass"] else 2
        _, end = day_bounds(args.day)
        day_path = args.root / "day" / f"{args.day}.json"
        verdict_path = args.root / "verdict" / f"{args.day}.json"
        if args.command == "aggregate":
            if args.snapshot_counts and not args.offline:
                raise ValueError("saved S7 evidence requires --offline")
            raw_dir = args.raw_dir or args.root / "raw"
            # A poll just after midnight can contain the previous day's last
            # minute. Older retained days cannot add evidence to this window.
            prefixes = {day_bounds(args.day)[0].strftime("%Y%m%d"), end.strftime("%Y%m%d")}
            paths = [path for path in raw_dir.glob("*.jsonl") if not re.match(r"\d{8}-", path.name) or path.name[:8] in prefixes]
            records = read_raw(paths)
            minute_rows, preliminary = aggregate_day(records, args.day, generated_at=args.generated_at)
            write_tables = [check["table"] for check in preliminary["commit_checks"]]
            reconciliation: dict[str, Any] = {"counts": {}}
            if args.offline:
                if args.snapshot_counts:
                    reconciliation = _load_json(args.snapshot_counts)
                    if reconciliation.get("day_utc") != args.day or utc_datetime(reconciliation["checked_at"]) < end:
                        raise ValueError("S7 evidence must describe the completed requested day")
            elif write_tables:
                try:
                    reconciliation = poll(args.root, snapshot_day=args.day, snapshot_tables=write_tables)
                except Exception as exc:
                    reconciliation = {"counts": {}, "error_type": type(exc).__name__}
            registry = _load_maintenance_registry(args.root / "maintenance-receipts" / f"{args.day}.json")
            minute_rows, report = aggregate_day(records, args.day, snapshot_counts=reconciliation["counts"],
                                               generated_at=args.generated_at, maintenance_registry=registry)
            report["commit_check_evidence"] = {key: value for key, value in reconciliation.items() if key != "counts"}
            _atomic_write(args.root / "minute" / f"{args.day}.jsonl", "".join(json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n" for row in minute_rows))
            _atomic_write(day_path, json_text(report))
            print(json_text(report), end="")
            # A measured failure is a successfully produced artifact. This lets
            # cron's aggregate && verdict && line also publish NOT_PASS days.
            return 0
        report = _load_json(day_path)
        if report["day_utc"] != args.day:
            raise ValueError("day artifact does not match requested day")
        if args.command == "verdict":
            report = evaluate_day(report)
            _atomic_write(verdict_path, json_text(report))
            print(json_text(report), end="")
        else:
            print(summary_line(report))
        return 0
    except Exception as exc:
        # Raw exception text can contain HTTP headers, credentials or SQL.
        reason = f"required environment variable: {PASSWORD_ENV}" if isinstance(exc, ValueError) and str(exc) == f"required environment variable: {PASSWORD_ENV}" else type(exc).__name__
        print(f"fotmob_load_meter: {reason}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
