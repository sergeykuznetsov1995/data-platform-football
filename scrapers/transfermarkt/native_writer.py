"""Mono-native Transfermarkt writer: lock/commit-retry primitives (#2404).

Layer 5 of the rewrite (#2400). The cross-process lock and the Iceberg
commit-conflict retry ladder moved here verbatim from ``writer.py``, which
keeps re-exporting them for the donor stack. HTTP and parsing never hold the
lock; all TM processes on the scheduler must share its path (the shared
Airflow logs volume).
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
import fcntl
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from re import fullmatch
from typing import Any, Mapping, Sequence

from . import write_journal
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


CORE_RECEIPTS_TABLE = 'iceberg.ops.transfermarkt_core_receipts_v1'
RECEIPT_COLUMNS = (
    'receipt_id', 'target_table', 'entity', 'scope_id', 'cycle_id', 'run_id',
    'universe_version', 'source_capture_id', 'source_body_hash',
    'parser_revision', 'schema_revision', 'natural_key_columns_json',
    'replace_key_columns_json', 'expected_rows', 'row_count',
    'status_counts_json', 'plan_sha256', 'physical_hash', 'committed_batch',
    'committed_at', 'captured_at',
)
_RECEIPT_BIGINT_COLUMNS = frozenset({'expected_rows', 'row_count'})
_RECEIPT_TIMESTAMPS = frozenset({'committed_at', 'captured_at'})


def core_receipts_ddl() -> str:
    """Create the warehouse evidence table for verified core writes."""
    types = dict.fromkeys(RECEIPT_COLUMNS, 'varchar')
    types.update(dict.fromkeys(_RECEIPT_BIGINT_COLUMNS, 'bigint'))
    types.update(dict.fromkeys(_RECEIPT_TIMESTAMPS, 'timestamp(6)'))
    columns = ', '.join(f'{name} {types[name]}' for name in RECEIPT_COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {CORE_RECEIPTS_TABLE} ({columns}) WITH (format = 'PARQUET')"


def _timestamp(value) -> datetime:
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _digest(value) -> str:
    return sha256(write_journal._canonical(value)).hexdigest()


def _check_columns(columns) -> None:
    for name in columns:
        if not isinstance(name, str) or not fullmatch(r'[a-z_][a-z0-9_]*', name):
            raise ValueError(f'invalid core column name: {name!r}')


def _sorted_rows(rows, natural_keys) -> list[dict]:
    # Keep numeric key ordering even for a bundle containing both ints and strings.
    def key(row):
        return tuple((isinstance(row[col], str), row[col]) for col in natural_keys)

    return sorted(rows, key=key)


@dataclass(frozen=True)
class NativeWritePlan:
    """One captured entity and its authoritative warehouse scope.

    ``write_mode`` is the lane's replacement semantics, not a shape guess:
    roster lanes REPLACE their club-season scope (departed players leave with
    the old rows); accumulating observation and career lanes MERGE by natural
    key (a later capture adds or updates rows and never deletes the rest).
    """

    entity: str
    table: str
    ddl: str
    natural_keys: tuple[str, ...]
    scope_columns: tuple[str, ...]
    scope_values: tuple[str, ...]
    rows: tuple[dict, ...] = ()
    captured_at: str = ''
    write_mode: str = 'replace'

    def __post_init__(self) -> None:
        if not isinstance(self.table, str) or not fullmatch(r'transfermarkt_[a-z0-9_]+', self.table):
            raise ValueError('invalid core table name')
        target = 'CREATE TABLE IF NOT EXISTS ' + self.full_table
        if not isinstance(self.ddl, str) or not self.ddl.startswith(target):
            raise ValueError('core DDL target differs from plan table')
        suffix = self.ddl[len(target):]
        if suffix and not (suffix[0].isspace() or suffix.startswith('(')):
            raise ValueError('core DDL target differs from plan table')
        if not self.natural_keys:
            raise ValueError('core natural_keys must be non-empty')
        if not self.scope_columns or len(self.scope_columns) != len(self.scope_values):
            raise ValueError('core scope_columns and scope_values must have equal non-zero length')
        if not self.captured_at:
            raise ValueError('core captured_at must be non-empty')
        if self.write_mode not in ('replace', 'merge'):
            raise ValueError('core write_mode must be replace or merge')
        _check_columns(self.natural_keys)
        _check_columns(self.scope_columns)
        self._check_rows()
        clock = _timestamp(self.captured_at)
        if self.rows and clock != max(_timestamp(row['captured_at']) for row in self.rows):
            raise ValueError('core captured_at must equal the newest row capture')

    def readback_sql(self) -> tuple[str, tuple]:
        """Parameterised physical read-back predicate for this plan.

        A replace plan owns its whole scope, so the scope predicate is the
        read-back. A merge plan only owns its natural-key rows: the scope
        still holds rows committed by earlier captures.
        """
        if self.write_mode == 'replace' or not self.rows:
            return self.scope_sql(), tuple(self.scope_values)
        keys = ', '.join(self.natural_keys)
        tuples = ', '.join(
            '(' + ', '.join('?' for _ in self.natural_keys) + ')' for _ in self.rows
        )
        return f'({keys}) IN ({tuples})', tuple(
            value for row in self.rows for value in (row[column] for column in self.natural_keys)
        )

    def _check_rows(self) -> None:
        keys = set(self.natural_keys) | set(self.scope_columns)
        required = keys | {'captured_at', 'source_capture_id'}
        seen = set()
        for row in self.rows:
            if not isinstance(row, dict):
                raise ValueError('core plan row must be a dict')
            _check_columns(row)
            if missing := required.difference(row):
                raise ValueError(f'core plan row is missing columns: {sorted(missing)}')
            if any(value is not None and not isinstance(value, (int, str)) for value in row.values()):
                raise ValueError('core plan row values must be strings, integers or null')
            if any(row[col] in ('', None) for col in required):
                raise ValueError('core plan row has an empty key, captured_at or source_capture_id')
            if any(str(row[col]) != str(value) for col, value in zip(self.scope_columns, self.scope_values)):
                raise ValueError('core plan row is outside its replacement scope')
            if row['source_capture_id'] != self.rows[0]['source_capture_id']:
                raise ValueError('core plan rows must share one source_capture_id')
            key = tuple(row[col] for col in self.natural_keys)
            if key in seen:
                raise ValueError('duplicate core natural key')
            seen.add(key)

    @property
    def full_table(self) -> str:
        """Return the qualified Bronze table name."""
        return 'iceberg.bronze.' + self.table

    def scope_sql(self) -> str:
        """Return the predicate bound by scope_values in order."""
        return ' AND '.join(f'{col} = ?' for col in self.scope_columns)

    def plan_sha256(self) -> str:
        """Hash the logical plan independently of row order."""
        return _digest({
            'entity': self.entity, 'table': self.table, 'natural_keys': self.natural_keys,
            'scope': list(zip(self.scope_columns, self.scope_values)),
            'rows': _sorted_rows(self.rows, self.natural_keys),
        })


def _query_rows(cursor, sql, params=None):
    result = execute_statement(cursor, sql, params)
    if result is None or result is cursor:
        return cursor.fetchall()
    return result


def guard_native_bundle(connection, plan: NativeWritePlan) -> None:
    """An older bundle must not overwrite a fresher committed capture."""
    cursor = connection.cursor()
    sql = f'SELECT MAX(captured_at) FROM {plan.full_table} WHERE {plan.scope_sql()}'
    try:
        rows = _query_rows(cursor, sql, plan.scope_values)
    except Exception as exc:
        if write_journal._table_not_found(exc):
            return
        raise
    finally:
        cursor.close()
    if rows and rows[0][0] is not None and _timestamp(rows[0][0]) > _timestamp(plan.captured_at):
        raise StaleTransfermarktWrite('newer capture already committed to ' + plan.table)


def _canonical_cell(value) -> Any:
    if isinstance(value, bool):
        # Engines return booleans differently (0/1, True/False); the
        # canonical comparison form is an integer.
        return int(value)
    if isinstance(value, datetime):
        return _timestamp(value).isoformat(timespec='microseconds')
    if value is None or isinstance(value, int):
        return value
    return str(value)


def _read_plan_rows(connection, plan) -> list[dict]:
    cursor = connection.cursor()
    predicate, params = plan.readback_sql()
    sql = f'SELECT * FROM {plan.full_table} WHERE {predicate}'
    try:
        rows = _query_rows(cursor, sql, params)
        if not rows:
            return []
        columns = [desc[0] for desc in cursor.description]
    finally:
        cursor.close()
    return [
        dict(zip(columns, map(_canonical_cell, row), strict=True))
        for row in rows
    ]


def _comparison_row(row: Mapping, columns) -> bytes:
    values = {}
    for col in columns:
        value = row.get(col)
        if col.endswith('_at') and isinstance(value, str):
            try:
                value = _timestamp(value)
            except ValueError:
                pass
        values[col] = _canonical_cell(value)
    return write_journal._canonical(values)


def _rows_equal(left: Sequence[Mapping], right: Sequence[Mapping]) -> bool:
    if len(left) != len(right):
        return False
    columns = {col for row in left for col in row}
    if columns != {col for row in right for col in row}:
        return False
    return sorted(_comparison_row(row, columns) for row in left) == sorted(
        _comparison_row(row, columns) for row in right
    )


def _already_written(connection, plan: NativeWritePlan) -> bool:
    existing = _read_plan_rows(connection, plan)
    if not plan.rows:
        return False
    capture = plan.rows[0]['source_capture_id']
    matching = [row for row in existing if row.get('source_capture_id') == capture]
    if matching and not _rows_equal(matching, plan.rows):
        raise RuntimeError('core replay differs from committed capture: ' + plan.table)
    return bool(matching)


def _uses_merge(plan: NativeWritePlan) -> bool:
    return plan.write_mode == 'merge'


def _mutate_plan(connection, writer, plan: NativeWritePlan) -> None:
    if _uses_merge(plan):
        if not plan.rows:
            # An empty accumulating bundle writes nothing: earlier captures
            # of the same scope keep their rows by design.
            return
        import pandas as pd

        columns = list(dict.fromkeys(col for row in plan.rows for col in row))
        frame = pd.DataFrame(list(plan.rows), columns=columns, dtype=object)
        writer.write_dataframe(
            df=frame, database='bronze', table=plan.table, add_metadata=False,
            merge_keys=list(plan.natural_keys),
        )
        return
    cursor = connection.cursor()
    sql = f'DELETE FROM {plan.full_table} WHERE {plan.scope_sql()}'
    try:
        execute_statement(cursor, sql, plan.scope_values)
    finally:
        cursor.close()
    if plan.rows:
        import pandas as pd

        columns = list(dict.fromkeys(col for row in plan.rows for col in row))
        frame = pd.DataFrame(list(plan.rows), columns=columns, dtype=object)
        writer.write_dataframe(
            df=frame, database='bronze', table=plan.table, add_metadata=False
        )


def _physical_proof(connection, plan: NativeWritePlan) -> dict:
    if _uses_merge(plan) and not plan.rows:
        return {
            'entity': plan.entity, 'table': plan.table, 'rows': 0,
            'physical_hash': _digest([]), 'expected_rows': 0, 'status_counts': {},
        }
    persisted = _read_plan_rows(connection, plan)
    if not _rows_equal(persisted, plan.rows):
        raise RuntimeError('core physical readback differs: ' + plan.table)
    if not _uses_merge(plan):
        cursor = connection.cursor()
        sql = f'SELECT COUNT(*) FROM {plan.full_table} WHERE {plan.scope_sql()}'
        try:
            counts = _query_rows(cursor, sql, plan.scope_values)
        finally:
            cursor.close()
        if len(counts) != 1 or counts[0][0] != len(plan.rows):
            raise RuntimeError('core physical readback differs: ' + plan.table)
    status_counts = Counter()
    for row in persisted:
        if 'observation_status' in row:
            status = row['observation_status']
            status_counts['null' if status is None else str(status)] += 1
    return {
        'entity': plan.entity, 'table': plan.table, 'rows': len(persisted),
        'physical_hash': _digest(_sorted_rows(persisted, plan.natural_keys)),
        'expected_rows': len(plan.rows), 'status_counts': dict(status_counts),
    }


def _plan_receipt(plan, proof, plan_hash, run_id, universe_version, batch, committed_at) -> dict:
    first = plan.rows[0] if plan.rows else {}
    evidence = {
        col: first.get(col, '') for col in (
            'scope_id', 'cycle_id', 'source_capture_id', 'source_body_hash',
            'parser_revision', 'schema_revision',
        )
    }
    receipt_hash = sha256((plan_hash + run_id).encode('utf-8')).hexdigest()
    return {
        **evidence, 'receipt_id': 'tm-core-receipt-' + receipt_hash[:32],
        'target_table': plan.full_table, 'entity': plan.entity,
        'run_id': run_id, 'universe_version': universe_version,
        'natural_key_columns_json': write_journal._canonical(plan.natural_keys).decode(),
        'replace_key_columns_json': write_journal._canonical(plan.scope_columns).decode(),
        'expected_rows': proof['expected_rows'], 'row_count': proof['rows'],
        'status_counts_json': write_journal._canonical(proof['status_counts']).decode(),
        'plan_sha256': plan_hash, 'physical_hash': proof['physical_hash'],
        'committed_batch': batch, 'committed_at': committed_at, 'captured_at': plan.captured_at,
    }


def _receipt_cell(col, value):
    if value is None:
        return None
    if col in _RECEIPT_BIGINT_COLUMNS:
        return int(value)
    if col in _RECEIPT_TIMESTAMPS:
        return _timestamp(value).isoformat(timespec='microseconds')
    return str(value)


def _merge_receipts(connection, receipts) -> None:
    names = ', '.join(RECEIPT_COLUMNS)
    params = ', '.join('?' for _ in RECEIPT_COLUMNS)
    assignments = ', '.join(f'{col} = s.{col}' for col in RECEIPT_COLUMNS if col != 'receipt_id')
    values = ', '.join(f's.{col}' for col in RECEIPT_COLUMNS)
    sql = (
        f'MERGE INTO {CORE_RECEIPTS_TABLE} t USING (VALUES ({params})) s({names}) '
        f'ON t.receipt_id = s.receipt_id WHEN MATCHED THEN UPDATE SET {assignments} '
        f'WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})'
    )
    for receipt in receipts:
        bound = []
        for col in RECEIPT_COLUMNS:
            value = _receipt_cell(col, receipt[col])
            if col in _RECEIPT_TIMESTAMPS:
                # Trino timestamp(6) stores UTC wall time without an offset.
                value = _timestamp(value).replace(tzinfo=None)
            bound.append(value)
        execute_statement(connection.cursor(), sql, bound)


def _verify_receipts(connection, receipts) -> None:
    names = ', '.join(RECEIPT_COLUMNS)
    ids = [receipt['receipt_id'] for receipt in receipts]
    placeholders = ', '.join('?' for _ in ids)
    sql = f'SELECT {names} FROM {CORE_RECEIPTS_TABLE} WHERE receipt_id IN ({placeholders})'
    rows = _query_rows(connection.cursor(), sql, ids)
    expected = {
        receipt['receipt_id']: tuple(_receipt_cell(col, receipt[col]) for col in RECEIPT_COLUMNS)
        for receipt in receipts
    }
    try:
        for row in rows:
            actual = tuple(_receipt_cell(col, value) for col, value in zip(RECEIPT_COLUMNS, row, strict=True))
            if not actual or actual != expected.pop(actual[0], None):
                raise RuntimeError('core receipt readback differs')
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError('core receipt readback differs') from exc
    if expected:
        raise RuntimeError('core receipt readback differs')


def _validate_bundle(plans, run_id, universe_version, committed_at) -> None:
    if not plans:
        raise ValueError('core bundle requires at least one plan')
    # One entity may appear in several plans (one club per memberships plan);
    # natural-key uniqueness across plans is the real collision contract.
    if not all((run_id, universe_version, committed_at)):
        raise ValueError('core run_id, universe_version and committed_at must be non-empty')
    _timestamp(committed_at)
    seen = set()
    for plan in plans:
        for row in plan.rows:
            key = (plan.table, tuple(row[col] for col in plan.natural_keys))
            if key in seen:
                raise ValueError('duplicate core natural key across plans')
            seen.add(key)


def _commit_plans(connection, writer, plans) -> list[dict]:
    execute_statement(connection.cursor(), core_receipts_ddl())
    for plan in plans:
        execute_statement(connection.cursor(), plan.ddl)
    pending = [plan for plan in plans if not _already_written(connection, plan)]
    for plan in pending:
        guard_native_bundle(connection, plan)
    for plan in pending:
        _mutate_plan(connection, writer, plan)
    return [_physical_proof(connection, plan) for plan in plans]


def write_core_bundle(
    connection_factory, writer, plans: Sequence[NativeWritePlan], *,
    run_id: str, universe_version: str, committed_at: str, deadline: float | None = None,
) -> dict:
    """Commit one journaled bundle and acknowledge its verified warehouse receipts."""
    with writer_deadline(deadline), writer_lock():
        _validate_bundle(plans, run_id, universe_version, committed_at)
        plan_hashes = [plan.plan_sha256() for plan in plans]
        digest = _digest({'plans': plan_hashes, 'run_id': run_id})
        receipt = write_journal.read_receipt(digest)
        if receipt is not None:
            if receipt.get('plan_sha256') != digest:
                raise RuntimeError('core journal receipt plan_sha256 differs')
            # An acknowledgement re-verifies the physical rows: committed
            # rows that drifted since the receipt fail closed, deleted rows
            # replay through the idempotent commit path.
            connection = connection_factory()
            try:
                write_journal.snapshot_anchors(digest, connection, {plan.table for plan in plans})
                _commit_plans(connection, writer, plans)
            finally:
                connection.close()
            return receipt

        descriptors = [asdict(plan) for plan in plans]
        for descriptor, plan in zip(descriptors, plans):
            descriptor['rows'] = _sorted_rows(plan.rows, plan.natural_keys)
        intent_path = write_journal.save_intent(
            {'run_id': run_id, 'digest': digest}, {'plans': descriptors}, run_id=run_id
        )
        connection = connection_factory()
        try:
            write_journal.snapshot_anchors(digest, connection, {plan.table for plan in plans})
            proofs = _commit_plans(connection, writer, plans)
            batch = 'tm-core-batch-' + digest[:32]
            plan_receipts = [
                _plan_receipt(plan, proof, plan_hash, run_id, universe_version, batch, committed_at)
                for plan, proof, plan_hash in zip(plans, proofs, plan_hashes, strict=True)
            ]
            _merge_receipts(connection, plan_receipts)
            _verify_receipts(connection, plan_receipts)
        finally:
            connection.close()
        receipt = {
            'verified': True, 'receipt_readback': True, 'run_id': run_id,
            'universe_version': universe_version, 'committed_at': committed_at,
            'plan_sha256': digest, 'committed_batch': batch, 'plans': proofs, 'manifests': [],
        }
        write_journal.record_receipt(digest, receipt)
        write_journal.finish_intent(intent_path)
        return receipt
