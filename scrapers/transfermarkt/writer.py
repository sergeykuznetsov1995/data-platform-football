"""Transfermarkt-local committing SQL and cross-process write boundary.

The lock protects warehouse work only. HTTP and parsing never hold it. All TM
processes on the scheduler must share its path (the shared Airflow logs volume).
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

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import TrinoTableManager, _is_iceberg_commit_conflict

_MUTEX = threading.RLock()
_LOCAL = threading.local()
_MUTATION = re.compile(r'^\s*(?:/\*.*?\*/\s*)*(MERGE|INSERT|DELETE|UPDATE|CREATE|ALTER|DROP|TRUNCATE)\b', re.I | re.S)
CAREER_TABLES = frozenset({
    'transfermarkt_market_value_points', 'transfermarkt_transfer_events',
    'transfermarkt_market_value_history', 'transfermarkt_transfers',
})


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


class TransfermarktTrinoManager(TrinoTableManager):
    def _execute(self, sql, fetch=False, params=None):
        parent = super()._execute
        if fetch or not _MUTATION.match(sql):
            return parent(sql, fetch=fetch, params=params)
        return _execute_committing(lambda: parent(sql, fetch=fetch, params=params))
    def _execute_committing(self, sql):
        # Avoid the common manager's independent retry loop and nested jitter.
        return self._execute(sql)
    def insert_dataframe_atomic(self, schema, table, df, **kwargs):
        with writer_lock():
            if kwargs.get('delete_filter'):
                kwargs['single_statement_replace'] = True
            return super().insert_dataframe_atomic(schema, table, df, **kwargs)


class TransfermarktIcebergWriter(IcebergWriter):
    def _get_trino_manager(self):
        if self._trino_manager is None:
            self._trino_manager = TransfermarktTrinoManager(host=self.trino_host, port=self.trino_port, catalog=self.catalog)
        return self._trino_manager
    def write_dataframe(self, df=None, **kwargs):
        if df is not None:
            kwargs['df'] = df
        with writer_lock():
            # Preserve original parser lineage and batch across Bronze and ops.
            kwargs['add_metadata'] = False
            return super().write_dataframe(**kwargs)


class StaleTransfermarktWrite(RuntimeError):
    """An older captured bundle cannot supersede an already committed capture."""


def guard_frames(scraper, outputs, frames):
    """Compare original capture clocks before the first bundle mutation.

    One grouped query per output keeps the lock short even for 500 careers.
    Global native careers deliberately compare by player, across source scopes.
    """
    import pandas as pd
    connection = scraper._bronze_connection()
    try:
        for output in outputs:
            frame = frames.get(output.key)
            if frame is None or frame.empty or not output.replace_keys:
                continue
            if output.is_legacy and any(not item.is_legacy for item in outputs):
                continue
            clock_column = 'fetched_at' if 'fetched_at' in frame else '_ingested_at'
            if clock_column not in frame:
                if output.table_name in CAREER_TABLES:
                    raise StaleTransfermarktWrite('replacement lacks original capture time')
                continue
            keys = list(output.replace_keys)
            incoming = {}
            for identity, part in frame.groupby(keys, dropna=False):
                identity = identity if isinstance(identity, tuple) else (identity,)
                stamps = pd.to_datetime(part[clock_column], utc=True)
                if stamps.isna().any():
                    raise StaleTransfermarktWrite('replacement capture time is incomplete')
                incoming[tuple(str(value) for value in identity)] = max(stamps.max(), pd.Timestamp(frame.attrs.get('tm_bundle_fetched_at', stamps.max())))
            predicate = scraper._build_partition_delete_filter(frame, keys)
            cursor = connection.cursor()
            try:
                execute_statement(cursor, 'SELECT ' + ', '.join(keys) + ', MAX(' + clock_column + ') FROM iceberg.bronze.'
                    + output.table_name + ' WHERE ' + predicate + ' GROUP BY ' + ', '.join(keys))
                rows = cursor.fetchall()
            except Exception as exc:
                if any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'does not exist')):
                    continue
                raise
            finally:
                cursor.close()
            for row in rows:
                identity = tuple(str(value) for value in row[:-1])
                latest = row[-1]
                if latest is None:
                    continue
                stamp = pd.Timestamp(latest)
                stamp = stamp.tz_localize('UTC') if stamp.tzinfo is None else stamp.tz_convert('UTC')
                if identity not in incoming or stamp > incoming[identity]:
                    raise StaleTransfermarktWrite('newer capture already committed to ' + output.table_name)
    finally:
        connection.close()


def is_mutation(sql):
    return bool(_MUTATION.match(sql))


def guard_cached_frames(scraper, outputs, frames):
    """Cached physical references must still exist before legacy hydration."""
    import pandas as pd
    connection = scraper._bronze_connection()
    try:
        for output in outputs:
            frame = frames.get(output.key)
            refs = frame.attrs.get('tm_original_capture_refs', []) if frame is not None else []
            if output.is_legacy or not refs:
                continue
            original = {item['player_id']: item['batch_id'] for item in refs}
            expected = frame[frame.player_id.astype(str).isin(original)].copy()
            expected['_batch_id'] = expected.player_id.astype(str).map(original)
            predicate = ' OR '.join('(player_id = ? AND _batch_id = ?)' for _ in refs)
            params = tuple(value for item in refs for value in (item['player_id'], item['batch_id']))
            cursor = connection.cursor()
            try:
                execute_statement(cursor, 'SELECT ' + ', '.join(frame.columns) + ' FROM iceberg.bronze.'
                    + output.table_name + ' WHERE ' + predicate, params)
                actual = pd.DataFrame(cursor.fetchall(), columns=frame.columns)
            finally:
                cursor.close()
            from dags.utils.transfermarkt_current_write import _rows
            if _rows(actual) != _rows(expected):
                raise StaleTransfermarktWrite('cached career physical captures changed: ' + output.table_name)
    finally:
        connection.close()


def locked_write(function):
    from functools import wraps
    @wraps(function)
    def call(*args, **kwargs):
        with writer_lock():
            return function(*args, **kwargs)
    return call
