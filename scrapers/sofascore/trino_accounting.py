"""Process-wide count of SofaScore queries sent to Trino (#1357).

The refresh plan used to spend one SELECT per endpoint (216 858 HTTPS
round-trips to Trino in one 15:30 run).  The phase report now carries these
counters as ``traffic.trino_queries`` so a regression is visible per scope.
Only SofaScore call sites increment them; the shared ``TrinoTableManager``
is deliberately left untouched.
"""

from __future__ import annotations

import threading

KINDS = ("select", "merge", "other")

_lock = threading.Lock()
_counts = dict.fromkeys(KINDS, 0)


def record(kind: str) -> None:
    if kind not in _counts:
        raise ValueError(f"unknown Trino query kind: {kind!r}")
    with _lock:
        _counts[kind] += 1


def classify(sql: str) -> str:
    head = str(sql).lstrip().split(None, 1)
    word = head[0].lower() if head else ""
    if word in ("select", "with"):
        return "select"
    if word == "merge":
        return "merge"
    return "other"


def record_sql(sql: str) -> None:
    record(classify(sql))


def snapshot() -> dict[str, int]:
    with _lock:
        return dict(_counts)


def reset() -> None:
    with _lock:
        for kind in KINDS:
            _counts[kind] = 0


class _CountingCursor:
    def __init__(self, cursor) -> None:
        self._cursor = cursor

    def execute(self, sql, *args, **kwargs):
        record_sql(sql)
        return self._cursor.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _CountingConnection:
    def __init__(self, connection) -> None:
        self._connection = connection

    def cursor(self, *args, **kwargs):
        return _CountingCursor(self._connection.cursor(*args, **kwargs))

    def __getattr__(self, name):
        return getattr(self._connection, name)


def counted_connection(connection):
    """Wrap a dbapi connection so every ``cursor().execute`` is counted."""

    if connection is None:
        return None
    return _CountingConnection(connection)
