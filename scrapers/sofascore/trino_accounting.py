"""Process-wide count of SofaScore statements sent to Trino (#1357).

The refresh plan used to spend one SELECT per endpoint (216 858 HTTPS
round-trips to Trino in one 15:30 run).  The phase report now carries these
counters as ``traffic.trino_queries`` so a regression is visible per scope.
Counting happens at the dbapi cursor: every statement a counted connection
executes (a batch MERGE's staging CREATE/INSERT/count/MERGE/DROP and the
connect probe included) is one query; ``classify`` names its kind.  The shared ``TrinoTableManager``
class is left untouched; ``instrument_manager`` wraps one instance.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager

KINDS = ("select", "merge", "other")

_lock = threading.Lock()
_counts = dict.fromkeys(KINDS, 0)
_write = threading.local()


def record(kind: str) -> None:
    if kind not in _counts:
        raise ValueError(f"unknown Trino query kind: {kind!r}")
    with _lock:
        _counts[kind] += 1


def classify(sql: str) -> str:
    """``select`` is a read; statements serving a batch write never are.

    Inside ``write_batch`` the staged MERGE's own verification ``SELECT
    count(*)`` is part of the write and counts as ``other``, so ``select``
    measures reads (the thing #1357 bounds) and ``merge`` the batch MERGEs.
    """

    head = str(sql).lstrip().split(None, 1)
    word = head[0].lower() if head else ""
    if word == "merge":
        return "merge"
    if word in ("select", "with") and not getattr(_write, "depth", 0):
        return "select"
    return "other"


@contextmanager
def write_batch():
    """Mark the statements of one batch write on this thread."""

    _write.depth = getattr(_write, "depth", 0) + 1
    try:
        yield
    finally:
        _write.depth -= 1


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


def instrument_manager(manager) -> None:
    """Count every statement one ``TrinoTableManager`` instance sends.

    Wraps the instance's connection factory (the base class stays as is), so
    connections made later — the lazy first one, a reset after a connection
    error — are counted too.  Idempotent.  A manager without a connection
    factory (test doubles) gets its ``_execute`` counted instead.
    """

    if getattr(manager, "_sofascore_trino_counted", False):
        return
    factory = getattr(manager, "_create_connection", None)
    if callable(factory):
        manager._create_connection = lambda: counted_connection(factory())
        existing = getattr(manager, "_conn", None)
        if existing is not None:
            manager._conn = counted_connection(existing)
    else:
        execute = manager._execute

        def _counted_execute(sql, *args, **kwargs):
            record_sql(sql)
            return execute(sql, *args, **kwargs)

        manager._execute = _counted_execute
    manager._sofascore_trino_counted = True
