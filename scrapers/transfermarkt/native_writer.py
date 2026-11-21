"""Mono-native Transfermarkt writer: lock/commit-retry primitives (#2404).

Layer 5 of the rewrite (#2400). The cross-process lock and the Iceberg
commit-conflict retry ladder moved here verbatim from ``writer.py``, which
keeps re-exporting them for the donor stack. HTTP and parsing never hold the
lock; all TM processes on the scheduler must share its path (the shared
Airflow logs volume).
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import random
import re
import threading
import time

from scrapers.base.trino_manager import _is_iceberg_commit_conflict


_MUTEX = threading.RLock()


_LOCAL = threading.local()


_MUTATION = re.compile(r'^\s*(?:/\*.*?\*/\s*)*(MERGE|INSERT|DELETE|UPDATE|CREATE|ALTER|DROP|TRUNCATE)\b', re.I | re.S)


def shared_writer_lock_ready():
    """Prove a configured lock aliases the approved shared host file.

    Separate scheduler logs volumes are not a common lock. Maintenance must
    skip TM until delivery supplies both path and host device/inode proof.
    Ingest retains its existing common TM-volume default before that rollout.
    """
    path = os.environ.get('TM_WRITER_LOCK_PATH')
    expected = os.environ.get('TM_WRITER_LOCK_SHARED_FILE_ID')
    if not path or not expected:
        return False
    stat = Path(path).stat()
    actual = f'{stat.st_dev}:{stat.st_ino}'
    if actual != expected:
        raise RuntimeError('Transfermarkt writer lock is not the approved shared host file')
    return True


@contextmanager
def writer_lock():
    """Reentrant within one thread, exclusive across processes and threads."""
    with _MUTEX:
        if getattr(_LOCAL, 'depth', 0):
            _LOCAL.depth += 1
            try:
                yield
            finally:
                _LOCAL.depth -= 1
            return
        if os.environ.get('TM_WRITER_LOCK_SHARED_FILE_ID'):
            shared_writer_lock_ready()
        path = Path(os.environ.get('TM_WRITER_LOCK_PATH', '/opt/airflow/logs/transfermarkt-writer.lock'))
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a+') as handle:
            # Deadline-aware polling keeps a waiting writer inside its portion.
            deadline = getattr(_LOCAL, 'deadline', None)
            while True:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError('Transfermarkt writer lock deadline exceeded')
                    time.sleep(0.05)
            _LOCAL.depth = 1
            try:
                yield
            finally:
                _LOCAL.depth = 0
                fcntl.flock(handle, fcntl.LOCK_UN)


@contextmanager
def writer_deadline(deadline):
    previous = getattr(_LOCAL, 'deadline', None)
    _LOCAL.deadline = deadline
    try:
        yield
    finally:
        _LOCAL.deadline = previous


def _execute_committing(operation, *, attempts=4):
    """Retry only explicit Iceberg commit conflicts; drain inside operation."""
    for attempt in range(attempts):
        try:
            with writer_lock():
                return operation()
        except Exception as exc:
            if attempt + 1 == attempts or not (_is_iceberg_commit_conflict(exc) or 'ICEBERG_COMMIT_ERROR' in str(exc)):
                raise
            delay = min(2.0, 0.1 * 2 ** attempt) + random.uniform(0, 0.1)
            deadline = getattr(_LOCAL, 'deadline', None)
            if deadline is not None and time.monotonic() + delay >= deadline:
                raise TimeoutError('Transfermarkt commit retry deadline exceeded') from exc
            time.sleep(delay)


def execute_statement(cursor, sql, params=None):
    """Execute reads normally; mutation results must be consumed before return."""
    if not _MUTATION.match(sql):
        return cursor.execute(sql, tuple(params)) if params is not None else cursor.execute(sql)
    def commit():
        cursor.execute(sql, tuple(params)) if params is not None else cursor.execute(sql)
        return list(cursor.fetchall())
    return _execute_committing(commit)


class CommittingCursor:
    def __init__(self, cursor):
        self._cursor = cursor
        self._committed_rows = None
    def __getattr__(self, name):
        return getattr(self._cursor, name)
    def execute(self, sql, params=None):
        self._committed_rows = None
        result = execute_statement(self._cursor, sql, params)
        if is_mutation(sql):
            self._committed_rows = result
            return self
        return result
    def fetchall(self):
        if self._committed_rows is not None:
            rows, self._committed_rows = self._committed_rows, None
            return rows
        return self._cursor.fetchall()
    def fetchone(self):
        if self._committed_rows is not None:
            return self._committed_rows.pop(0) if self._committed_rows else None
        return self._cursor.fetchone()


class CommittingConnection:
    def __init__(self, connection):
        self._connection = connection
    def __getattr__(self, name):
        return getattr(self._connection, name)
    def cursor(self, *args, **kwargs):
        return CommittingCursor(self._connection.cursor(*args, **kwargs))


class StaleTransfermarktWrite(RuntimeError):
    """An older captured bundle cannot supersede an already committed capture."""


def is_mutation(sql):
    return bool(_MUTATION.match(sql))


def locked_write(function):
    from functools import wraps
    @wraps(function)
    def call(*args, **kwargs):
        with writer_lock():
            return function(*args, **kwargs)
    return call
