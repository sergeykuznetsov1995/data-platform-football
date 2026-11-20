"""Classify collector SQL and hold durable ESPN table slots through its last page."""

from __future__ import annotations

import logging
import re
import sys
from itertools import islice

from scrapers.base import trino_manager as base
from scrapers.espn.gate import default_state_path
from scrapers.whoscored.trino_budget import TrinoBudget

logger = logging.getLogger(__name__)
DEFAULT_SCHEMA = "bronze"


def default_budget():
    return TrinoBudget(default_state_path().parent / "trino-budget")


def _definite_rejection(driver_error):
    # The shared manager classifies TrinoError with the DBAPI error as its
    # cause. Give it that same chain here, while propagating the raw error
    # unchanged to the caller of cursor.execute.
    error = base.TrinoError(str(driver_error))
    error.__cause__ = driver_error
    return base._is_definite_iceberg_commit_conflict(error)


# Keep quoted identifiers distinct from keywords. A string literal, including
# escaped quotes, is one opaque token even when it contains an entire query.
_LEX = re.compile(
    r"""(?P<skip>\s+|--[^\r\n]*)|(?P<comment>/\*)"""
    r"""|(?P<literal>'[^']*(?:''[^']*)*')|"[^"]*(?:""[^"]*)*"|[a-zA-Z_][\w$]*|\d+|.""",
    re.DOTALL,
)
_IDENTIFIER = re.compile(r'[a-z_][\w$]*|"(?:[^"]|"")+"')
_COMMENT_EDGE = re.compile(r"/\*|\*/")


def _tokens(sql):
    pos = 0
    while pos < len(sql):
        match = _LEX.match(sql, pos)
        pos = match.end()
        if match.lastgroup == "comment":
            depth = 1
            for edge in _COMMENT_EDGE.finditer(sql, pos):
                depth += 1 if edge[0] == "/*" else -1
                if not depth:
                    pos = edge.end()
                    break
            else:
                raise ValueError("Unterminated ESPN Trino SQL comment")
        elif match.lastgroup != "skip":
            yield "?" if match.lastgroup == "literal" else match[0].lower()


def may_change_data(sql):
    # Only inspect the prefix on the manager path. The cursor classifies the
    # complete statement before submission and permits only queries after WITH.
    return next(_tokens(sql), "") not in {
        "select", "with", "table", "values", "show", "describe", "desc", "explain"
    }


def _identifier(token):
    if not _IDENTIFIER.fullmatch(token):
        raise ValueError("Expected a Trino relation identifier")
    return token[1:-1].replace('""', '"') if token.startswith('"') else token


class _Statement:
    def __init__(self, sql, catalog, schema):
        self.words = list(_tokens(sql))
        if self.words[-1:] == [";"]:
            self.words.pop()
        if not self.words or ";" in self.words:
            raise ValueError("Expected one ESPN Trino statement")
        self.catalog = catalog.lower()
        self.schema = schema.lower()
        self.use_namespace = None
        self.ends = {}
        stack = []
        for i, word in enumerate(self.words):
            if word == "(":
                stack.append(i)
            elif word == ")":
                if not stack:
                    raise ValueError("Unbalanced ESPN Trino SQL")
                self.ends[stack.pop()] = i
        if stack:
            raise ValueError("Unbalanced ESPN Trino SQL")
        self.keys = set()

    def name(self, pos):
        words = self.words
        if pos >= len(words):
            raise ValueError("Missing ESPN Trino relation")
        parts = [_identifier(words[pos])]
        pos += 1
        while words[pos:pos + 1] == ["."]:
            if pos + 1 >= len(words):
                raise ValueError("Incomplete ESPN Trino relation")
            parts.append(_identifier(words[pos + 1]))
            pos += 2
        if len(parts) > 3:
            raise ValueError("Invalid ESPN Trino relation")
        return parts, pos

    def key(self, parts):
        if len(parts) == 1:
            parts = [self.schema, *parts]
        if len(parts) == 2:
            parts = [self.catalog, *parts]
        # Iceberg's $files/$snapshots relations belong to the same table.
        table = parts[-1].partition("$")[0]
        # Escape separator characters inside quoted identifiers.
        return ".".join(p.replace("%", "%25").replace(".", "%2e")
                        for p in [*parts[:-1], table])

    def relation(self, pos, end, aliases):
        words = self.words
        if words[pos:pos + 1] == ["lateral"]:
            pos += 1
        if pos >= end:
            raise ValueError("Missing ESPN Trino scan relation")
        if words[pos] == "(":
            close = self.ends[pos]
            if words[pos + 1] not in {"select", "with", "values", "table"}:
                # Parenthesized join or another parenthesized query.
                self.relation(pos + 1, close, aliases)
            self.scans(pos + 1, close, aliases)
            return close + 1
        if words[pos:pos + 2] == ["unnest", "("]:
            close = self.ends[pos + 1]
            self.scans(pos + 2, close, aliases, expressions=True)
            return close + 1
        parts, after = self.name(pos)
        if words[after:after + 1] == ["("]:
            raise ValueError("Unclassified ESPN Trino table function")
        metadata = (len(parts) >= 2 and parts[-2] == "information_schema")
        if not metadata and (len(parts) != 1 or parts[0] not in aliases):
            self.keys.add("scan:" + self.key(parts))
        return after

    def ctes(self, pos, end, aliases):
        words = self.words
        recursive = words[pos:pos + 1] == ["recursive"]
        if recursive:
            pos += 1
        while pos < end:
            alias = _identifier(words[pos])
            pos += 1
            if words[pos:pos + 1] == ["("]:  # optional CTE column list
                pos = self.ends[pos] + 1
            if words[pos:pos + 2] != ["as", "("]:
                raise ValueError("Unclassified ESPN Trino WITH clause")
            close = self.ends[pos + 1]
            self.scans(pos + 2, close, aliases | {alias} if recursive else aliases)
            aliases = aliases | {alias}
            pos = close + 1
            if words[pos:pos + 1] != [","]:
                return pos, aliases
            pos += 1
        raise ValueError("Incomplete ESPN Trino WITH clause")

    def scans(self, pos, end, aliases, *, expressions=False):
        words = self.words
        if pos < end and words[pos] == "with":
            pos, aliases = self.ctes(pos + 1, end, aliases)
            if words[pos:pos + 1] not in (["select"], ["values"], ["table"], ["("]):
                raise ValueError("Unclassified ESPN Trino query after WITH")
        if pos < end and words[pos] == "table":
            pos = self.relation(pos + 1, end, aliases)
        in_from = False
        while pos < end:
            word = words[pos]
            if word == "table" and not expressions:
                pos = self.relation(pos + 1, end, aliases)
            elif word == "(":
                close = self.ends[pos]
                is_query = words[pos + 1] in {"with", "select", "values", "table"}
                self.scans(pos + 1, close, aliases, expressions=not is_query)
                pos = close + 1
            elif not expressions and (
                word == "join" or (word == "from" and words[pos - 1] != "distinct")
                or (word == "," and in_from)
            ):
                pos = self.relation(pos + 1, end, aliases)
                in_from = True
            else:
                if word in {"where", "group", "order", "having", "union", "intersect",
                            "except", "limit", "offset", "window", "select", "when",
                            "set", "fetch"}:
                    in_from = False
                pos += 1

    def create(self):
        words = self.words
        if words[1:2] == ["schema"]:
            return
        if words[1:2] != ["table"]:
            raise ValueError("Unclassified ESPN Trino CREATE statement")
        pos = 5 if words[2:5] == ["if", "not", "exists"] else 2
        target, pos = self.name(pos)
        # Trino's Iceberg connector publishes an initial snapshot even when
        # a newly created table has no data files.
        self.keys.add("commit:" + self.key(target))
        while pos < len(words) and words[pos] != "as":
            pos = self.ends[pos] + 1 if words[pos] == "(" else pos + 1
        if pos < len(words):
            self.scans(pos + 1, len(words), set())

    def classify(self):
        words = self.words
        head = words[0]
        end = len(words)
        if head in {"insert", "merge", "update", "delete"}:
            prefix = {"insert": ["into"], "merge": ["into"], "update": [], "delete": ["from"]}[head]
            if words[1:1 + len(prefix)] != prefix:
                raise ValueError("Unclassified ESPN Trino mutation")
            parts, pos = self.name(1 + len(prefix))
            key = self.key(parts)
            self.keys.add("commit:" + key)
            if head != "insert":
                self.keys.add("scan:" + key)
            if head == "merge":
                using = words.index("using", pos)
                pos = self.relation(using + 1, end, set())
            # INSERT may prefix a WITH query with a target column list.
            if head == "insert" and words[pos:pos + 1] == ["("]:
                if words[pos + 1] not in {"with", "select", "values", "table"}:
                    pos = self.ends[pos] + 1
            self.scans(pos, end, set())
        elif head in {"select", "values", "table", "with"}:
            self.scans(0, end, set())
        elif head == "create":
            self.create()
        elif head == "use":
            parts, after = self.name(1)
            if after != end or len(parts) > 2:
                raise ValueError("Invalid ESPN Trino USE namespace")
            self.use_namespace = (parts[0] if len(parts) == 2 else self.catalog, parts[-1])
        elif head in {"show", "describe", "desc"}:
            pass
        elif words[:3] == ["set", "time", "zone"] or words[:2] == ["set", "path"]:
            pass
        elif head in {"set", "reset"} and words[1:2] == ["session"]:
            if (words[2:3] == ["authorization"] or
                    any(w.strip('"') in {"query_max_run_time", "enable_dynamic_filtering"}
                        for w in words[2:])):
                raise ValueError("Cannot override the ESPN Trino connection settings")
        elif head == "drop" and words[1:2] in (["table"], ["schema"]):
            pass
        elif words[:2] == ["alter", "table"] and "execute" not in words:
            pass
        else:
            raise ValueError("Unclassified ESPN Trino statement")
        return self.keys


def statement_keys(sql, catalog="iceberg", schema=DEFAULT_SCHEMA):
    """Return pacing slots. The physical scan count depends on the Trino plan."""
    return _Statement(sql, catalog, schema).classify()


class BudgetCursor:
    def __init__(self, cursor, connection):
        self._cursor = cursor
        self._connection = connection
        self._rows = None
        self._offset = 0
        self._execution_failed = False

    @property
    def arraysize(self):
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, size):
        self._cursor.arraysize = size

    def execute(self, operation, *args, **kwargs):
        # Discard the previous result before any operation that can fail.
        self._rows = []
        self._offset = 0
        self._execution_failed = True
        owner = self._connection
        statement = _Statement(operation, owner.catalog, owner.schema)
        keys = statement.classify()
        if keys:
            with owner.budget.statement(keys, rejected=_definite_rejection):
                self._cursor.execute(operation, *args, **kwargs)
                rows = self._cursor.fetchall()
            self._rows = rows
        else:
            self._cursor.execute(operation, *args, **kwargs)
            if statement.use_namespace:
                # Apply USE only after the coordinator has accepted it, before
                # another cursor resolves an unqualified name on this connection.
                self._rows = self._cursor.fetchall()
                owner.catalog, owner.schema = statement.use_namespace
            else:
                self._rows = None
        self._execution_failed = False
        return self

    def fetchall(self):
        if self._rows is None:
            return self._cursor.fetchall()
        rows = self._rows
        if self._offset:
            if isinstance(rows, list):
                del rows[:self._offset]
            else:
                rows = rows[self._offset:]
        self._rows = []
        self._offset = 0
        return rows

    def fetchone(self):
        if self._rows is None:
            return self._cursor.fetchone()
        if self._offset == len(self._rows):
            return None
        row = self._rows[self._offset]
        self._offset += 1
        return row

    def fetchmany(self, size=None):
        if self._rows is None:
            return self._cursor.fetchmany() if size is None else self._cursor.fetchmany(size)
        return list(islice(self, self.arraysize if size is None else size))

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def executemany(self, operation, seq_of_params):
        for params in seq_of_params:
            self.execute(operation, params)
        return self

    def close(self):
        self._rows = None
        if self._execution_failed and sys.exc_info()[0] is not None:
            # Raw journal/history callers close in finally. Preserve the actual
            # query failure when closing the same broken connection also fails.
            try:
                return self._cursor.close()
            except Exception:
                logger.warning("Failed to close ESPN cursor after query failure", exc_info=True)
                return
        return self._cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class BudgetConnection:
    def __init__(self, connection, budget, catalog):
        self._connection = connection
        self.budget = budget
        self.catalog = catalog
        self.schema = DEFAULT_SCHEMA

    def cursor(self, *args, **kwargs):
        return BudgetCursor(self._connection.cursor(*args, **kwargs), self)

    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, *exc):
        return self._connection.__exit__(*exc)

    def __getattr__(self, name):
        return getattr(self._connection, name)
