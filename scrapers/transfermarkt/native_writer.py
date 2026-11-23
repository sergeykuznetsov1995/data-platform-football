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
_RECEIPTS_ANCHOR_NAME = 'ops.transfermarkt_core_receipts_v1'
RECEIPT_COLUMNS = (
    'receipt_id', 'target_table', 'entity', 'scope_id', 'cycle_id', 'run_id',
    'universe_version', 'source_capture_id', 'source_body_hash',
    'parser_revision', 'schema_revision', 'natural_key_columns_json',
    'replace_key_columns_json', 'scope_values_json', 'expected_rows',
    'row_count', 'status_counts_json', 'observation_status', 'status_reason',
    'status_proof', 'plan_sha256', 'physical_hash', 'committed_batch',
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


# The accepted page evidence of every committed multi-page replacement
# snapshot: one row per source page a scope ever accepted, holding the
# newest evidence of that page. A page that contributes no row leaves
# nothing in the data table, and a page that later leaves the walk would
# leave nothing either: without this manifest its capture id and clock are
# known only while the write runs, and it could come back with an older
# capture. Rows are never deleted; each page is kept by one statement.
CORE_PAGES_TABLE = 'iceberg.ops.transfermarkt_core_pages_v1'
_PAGES_ANCHOR_NAME = 'ops.transfermarkt_core_pages_v1'
PAGES_COLUMNS = (
    'target_table', 'scope_values_json', 'source_url', 'source_capture_id',
    'captured_at', 'source_body_hash',
)


def core_pages_ddl() -> str:
    """Create the warehouse page manifest of replacement snapshots."""
    columns = (
        'target_table varchar, scope_values_json varchar, source_url varchar, '
        'source_capture_id varchar, captured_at timestamp(6), source_body_hash varchar')
    return f"CREATE TABLE IF NOT EXISTS {CORE_PAGES_TABLE} ({columns}) WITH (format = 'PARQUET')"


def _timestamp(value) -> datetime:
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _digest(value) -> str:
    return sha256(write_journal._canonical(value)).hexdigest()


# Every written row proves its source; a plan that cannot state this
# lineage is rejected before any mutation, not audited after the fact.
_EVIDENCE_COLUMNS = frozenset({
    'source_url', 'source_endpoint', 'raw_capture_id', 'source_capture_id',
    'source_body_hash', 'scope_id', 'cycle_id', 'parser_revision', 'schema_revision',
})
_REQUIRED_ROW_COLUMNS = frozenset({
    'captured_at', 'source_capture_id', 'source_url', 'source_endpoint',
    'raw_capture_id', 'source_body_hash', 'run_id', 'universe_version',
    'parser_revision', 'schema_revision', '_batch_id', '_ingested_at',
})


def _nonempty(value) -> bool:
    return isinstance(value, str) and bool(value)


def _check_columns(columns) -> None:
    for name in columns:
        if not isinstance(name, str) or not fullmatch(r'[a-z_][a-z0-9_]*', name):
            raise ValueError(f'invalid core column name: {name!r}')


class _Row(dict):
    """A validated plan row: readable as a mapping, never written again.

    Hashing, journalling, writing and read-back must all see the content the
    plan validated. Staying a dict keeps every consumer (JSON, dataclasses,
    frames) working unchanged; another plan is the way to other rows.
    """

    __slots__ = ()

    @staticmethod
    def _read_only(*args, **kwargs):
        raise TypeError('core plan rows are read-only: build another plan')

    def __reduce__(self):
        # copy and pickle rebuild a row through its constructor, never by
        # assigning into it.
        return _Row, (dict(self),)

    __setitem__ = __delitem__ = __ior__ = _read_only
    clear = update = pop = popitem = setdefault = _read_only


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
    ``pages`` is the evidence of a multi-page replacement snapshot: every
    walked page as (source_url, source_capture_id, captured_at,
    source_body_hash, next_url), rowless ones included, because a page that
    contributes no row still has a clock and a body. The manifest is the
    walk: each page names the page its own link leads to and the last one
    names none, so a prefix of a walk is never mistaken for the whole.
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
    status: str = 'observed_present'
    status_reason: str = ''
    proof: str = ''
    capture_id: str = ''
    evidence: tuple[tuple[str, str], ...] = ()
    pages: tuple[tuple[str, str, str, str, str], ...] = ()

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
        if any(not _nonempty(value) for value in self.scope_values):
            # The writer's staged replacement receives the scope as SQL text.
            raise ValueError('core scope values must be non-empty strings')
        if not self.captured_at:
            raise ValueError('core captured_at must be non-empty')
        if self.write_mode not in ('replace', 'merge'):
            raise ValueError('core write_mode must be replace or merge')
        if not _nonempty(self.status) or not _nonempty(self.status_reason):
            raise ValueError('core plan needs its observation status and reason')
        if self.status in ('observed_empty', 'not_applicable') and not _nonempty(self.proof):
            raise ValueError('core plan proof is required for empty observations')
        # An empty plan has no row to carry its capture lineage; the evidence
        # mapping stands in for it so the receipt stays fully attributable.
        if not self.rows:
            missing = _EVIDENCE_COLUMNS.difference(column for column, _ in self.evidence)
            if missing:
                raise ValueError(f'core empty plan lacks capture evidence: {sorted(missing)}')
            if any(not _nonempty(value) for _, value in self.evidence):
                raise ValueError('core empty plan evidence values must be non-empty')
            if not _nonempty(self.capture_id):
                raise ValueError('core empty plan lacks its capture id')
        object.__setattr__(self, 'pages', tuple(tuple(page) for page in self.pages))
        _check_columns(self.natural_keys)
        _check_columns(self.scope_columns)
        self._check_rows()
        # The plan keeps the rows it validated, and keeps them as validated:
        # the caller's dictionaries stay mutable, hashing and writing must see
        # this content and nothing else.
        object.__setattr__(self, 'rows', tuple(_Row(row) for row in self.rows))
        self._check_pages()
        self._check_row_evidence()
        clock = _timestamp(self.captured_at)
        if self.pages:
            # The snapshot is as fresh as its newest walked page, rowless or not.
            if clock != max(_timestamp(page[2]) for page in self.pages):
                raise ValueError('core captured_at must equal the newest walked page')
        if self.rows:
            newest_at, capture_id = max(
                (_timestamp(row['captured_at']), row['source_capture_id']) for row in self.rows
            )
            if clock < newest_at or (clock != newest_at and not self.pages):
                raise ValueError('core captured_at must equal the newest row capture')
            if self.capture_id and self.capture_id != capture_id:
                raise ValueError('core capture_id must equal the newest row capture id')
            object.__setattr__(self, 'capture_id', capture_id)
        if not self.rows and self.pages:
            # A snapshot without rows carries the clock of its newest page
            # and names that page: its receipt pairs a capture with its own
            # clock.
            if self.capture_id != max(self.pages, key=lambda page: _timestamp(page[2]))[1]:
                raise ValueError('core rowless snapshot must name its newest page')
        evidence_map = dict(self.evidence)
        for column in ('raw_capture_id', 'source_capture_id'):
            if evidence_map.get(column, self.capture_id) != self.capture_id:
                raise ValueError('core plan evidence names a different capture')

    def _multi_page(self) -> bool:
        return self.write_mode == 'replace' and self.entity in _MULTI_PAGE_REPLACE_ENTITIES

    def _check_pages(self) -> None:
        if not self.pages:
            # Without its page evidence a snapshot proves none of the pages
            # it did not walk: a terminal page would replace the whole scope.
            if self._multi_page():
                raise ValueError('core multi-page replacement requires its page manifest')
            return
        if not self._multi_page():
            raise ValueError('core page evidence belongs to a multi-page replacement snapshot')
        seen = set()
        captures = set()
        for url, capture_id, clock, body_hash, next_url in self.pages:
            # Each predicate on its own: inside all(...) a truthy map object
            # would stand in for the checks after it.
            if not all(map(_nonempty, (url, capture_id, clock, body_hash))):
                raise ValueError('core page evidence needs distinct pages with url, capture and clock')
            if url in seen or capture_id in captures:
                raise ValueError('core page evidence needs distinct pages with url, capture and clock')
            if not fullmatch(r'[0-9a-f]{64}', body_hash) or not isinstance(next_url, str):
                raise ValueError('core page evidence needs distinct pages with url, capture and clock')
            _timestamp(clock)
            seen.add(url)
            captures.add(capture_id)
        # The manifest is the walk. A page that leads on to a page the
        # manifest does not hold, or a last page that still leads somewhere,
        # is a part of a walk offered as the whole edition.
        if any(page[4] != (self.pages[index + 1][0] if index + 1 < len(self.pages) else '')
               for index, page in enumerate(self.pages)):
            raise ValueError('core page evidence is not one walk from its first to its last page')
        # A row carries the whole lineage of one page of the snapshot: URL,
        # capture, clock and body hash.
        lineage = {page[:4] for page in self.pages}
        if any((row['source_url'], row['source_capture_id'], row['captured_at'],
                row['source_body_hash']) not in lineage
               or row['raw_capture_id'] != row['source_capture_id'] for row in self.rows):
            raise ValueError(
                'core row lineage differs: rows must come from a page of the snapshot evidence')

    def _check_row_evidence(self) -> None:
        """Without a page manifest the plan's evidence stands for every row."""
        if self.pages or not self.rows:
            return
        evidence = dict(self.evidence)
        for column in ('source_url', 'source_body_hash'):
            if evidence.get(column) is not None and any(
                    row[column] != evidence[column] for row in self.rows):
                raise ValueError('core row lineage differs from the plan evidence')

    def readback_sql(self) -> tuple[str, tuple]:
        """Parameterised physical read-back predicate for this plan.

        A replace plan owns its whole scope, so the scope predicate is the
        read-back. A merge plan owns exactly its capture's rows inside the
        scope: earlier captures keep their rows there. Scope plus
        ``source_capture_id`` keeps every bound parameter varchar, so date
        and timestamp key columns never receive string casts.
        """
        predicate = self.scope_sql()
        params = list(self.scope_values)
        if self.write_mode == 'merge':
            predicate += ' AND source_capture_id = ?'
            params.append(self.capture_id)
        return predicate, tuple(params)

    def _check_rows(self) -> None:
        keys = set(self.natural_keys) | set(self.scope_columns)
        required = keys | _REQUIRED_ROW_COLUMNS
        seen = set()
        pages = {}
        column_types = {}
        timestamps = [column for column, kind in _ddl_columns(self.ddl).items()
                      if kind.startswith('timestamp')]
        for row in self.rows:
            if not isinstance(row, dict):
                raise ValueError('core plan row must be a dict')
            _check_columns(row)
            if missing := required.difference(row):
                raise ValueError(f'core plan row is missing columns: {sorted(missing)}')
            if any(value is not None and not isinstance(value, (int, str)) for value in row.values()):
                raise ValueError('core plan row values must be strings, integers or null')
            if any(type(value) is int and not -2 ** 63 <= value < 2 ** 63 for value in row.values()):
                # The bigint column cannot hold it; refuse it before anything
                # is staged.
                raise ValueError('core plan row integer exceeds the bigint range')
            for column in timestamps:
                value = row.get(column)
                # The engine reads several spellings of one moment. A key
                # cell is compared as text, so a plan states it one way only.
                if value is not None and (not isinstance(value, str)
                                          or value != _canonical_clock(value)):
                    raise ValueError(f'core plan row timestamp is not canonical: {column}')
            if ('observed_at' in self.natural_keys and row.get('observed_at') is not None
                    and _timestamp(row['observed_at']) != _timestamp(row['captured_at'])):
                raise ValueError('core observation clock must equal its capture clock')
            for column, value in row.items():
                kind = (None if value is None else 'bool' if isinstance(value, bool)
                        else 'int' if isinstance(value, int) else 'str')
                if kind and column_types.setdefault(column, kind) != kind:
                    raise ValueError(f'core plan column mixes value types: {column}')
            if any(row[col] in ('', None) for col in required):
                raise ValueError('core plan row has empty lineage, key or capture columns')
            if any(str(row[col]) != str(value) for col, value in zip(self.scope_columns, self.scope_values)):
                raise ValueError('core plan row is outside its replacement scope')
            if not _nonempty(row['source_capture_id']):
                raise ValueError('core plan row has invalid source_capture_id')
            if not self._multi_page() and row['source_capture_id'] != self.rows[0]['source_capture_id']:
                raise ValueError('core plan rows must share one source_capture_id')
            if self._multi_page():
                page = (row['source_capture_id'], row['captured_at'])
                if pages.setdefault(row['source_url'], page) != page:
                    raise ValueError('core snapshot allows one capture per source page')
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

    def scope_filter_sql(self) -> str:
        """Return the scope predicate as inline SQL for the writer's filter.

        The base writer takes a delete filter as SQL text. Column names are
        validated identifiers and scope values non-empty strings, quoted the
        way the donor renders its partition filters.
        """
        if any(not _nonempty(value) for value in self.scope_values):
            raise ValueError('core scope values must be non-empty strings')
        return ' AND '.join(
            f"{col} = '{value.replace(chr(39), chr(39) * 2)}'"
            for col, value in zip(self.scope_columns, self.scope_values))

    def plan_sha256(self) -> str:
        """Hash the logical plan independently of row order.

        The capture clock, write mode and observation status belong to the
        identity: two empty captures of one scope at different times are
        different plans, not one reusable acknowledgement. So is the capture
        of a rowless plan, which has no row to name it.
        """
        plan = {
            'entity': self.entity, 'table': self.table, 'natural_keys': self.natural_keys,
            'scope': list(zip(self.scope_columns, self.scope_values)),
            'write_mode': self.write_mode, 'captured_at': self.captured_at,
            'status': self.status, 'status_reason': self.status_reason, 'proof': self.proof,
            'rows': _sorted_rows(self.rows, self.natural_keys),
        }
        if self.pages:
            plan['pages'] = self.pages
        if not self.rows:
            plan['capture'] = {'capture_id': self.capture_id, 'evidence': self.evidence}
        return _digest(plan)


def _query_rows(cursor, sql, params=None):
    result = execute_statement(cursor, sql, params)
    if isinstance(result, list):
        return result
    # Read paths hand back the (possibly wrapped) cursor, not rows.
    return cursor.fetchall()


def _committed_clock(connection, table_sql: str, params: tuple) -> object:
    cursor = connection.cursor()
    try:
        rows = _query_rows(cursor, table_sql, params)
    except Exception as exc:
        if write_journal._table_not_found(exc):
            return None
        raise
    finally:
        cursor.close()
    return rows[0][0] if rows else None


# Section lanes that observe ONE scope through several page captures per run
# (a feed, a paged directory). Their natural key names the capture clock, so
# an older capture owns other rows than a newer sibling page: freshness is
# decided per capture, never against the sibling's clock. What keeps such a
# plan honest is that no other capture may already own one of its keys.
_MULTI_CAPTURE_MERGE_ENTITIES = frozenset({
    'rumour_observations', 'referee_aggregate_observations', 'club_value_observations',
})
# Pages of the referee directory re-own rows between walks: the donor keeps
# the first page that lists a referee, and a later walk may list it earlier.
# Every other capture is exactly its committed rows, rumours included.
_PAGE_OWNED_ROWS_ENTITIES = frozenset({'referee_aggregate_observations'})
# Varchar key columns that narrow the ownership lookup of a large scope; a
# lane without an entry is read scope-wide (date and timestamp key columns
# are never bound as parameters).
_KEY_PREFILTER_COLUMNS = {
    'rumour_observations': ('rumour_identity', 'subject_id', 'correction_identity'),
    'referee_aggregate_observations': (
        'referee_id', 'competition_id', 'edition_id', 'metric'),
}
_CAPTURE_KEY_COLUMNS = frozenset({'observed_at', 'source_capture_id'})
# The attendance edition is the one replacement snapshot that spans several
# walked pages; every other replacement is one captured page.
_MULTI_PAGE_REPLACE_ENTITIES = frozenset({'attendance_observations'})


def _capture_pinned(plan: NativeWritePlan) -> bool:
    """A merge lane whose natural key names its own capture."""
    return plan.write_mode == 'merge' and bool(_CAPTURE_KEY_COLUMNS.intersection(plan.natural_keys))


def guard_native_bundle(connection, plan: NativeWritePlan) -> None:
    """An older bundle must not overwrite a fresher committed capture.

    The data table proves freshness while rows exist; a proven-empty
    replacement leaves none, so the per-scope warehouse receipts carry the
    clock of the capture they acknowledged for empty scopes too. A multi-page
    snapshot keeps more: its page manifest records the accepted capture and
    clock of every walked page, rowless ones included, and that is where its
    freshness and its per-page check are read. The snapshot's newest clock
    says nothing about a stale page riding along on a fresh sibling.
    """
    if plan.write_mode == 'merge' and _capture_pinned(plan):
        _guard_key_owners(connection, plan)
        if plan.entity in _MULTI_CAPTURE_MERGE_ENTITIES:
            return
    committed = _committed_clock(
        connection,
        f'SELECT MAX(captured_at) FROM {plan.full_table} WHERE {plan.scope_sql()}',
        tuple(plan.scope_values),
    )
    scope_json = _scope_json(plan)
    acknowledged = _committed_clock(
        connection,
        f'SELECT MAX(captured_at) FROM {CORE_RECEIPTS_TABLE} '
        'WHERE target_table = ? AND scope_values_json = ?',
        (plan.full_table, scope_json),
    )
    # A rowless page leaves no row: the receipt and the page manifest are the
    # only proof of how fresh the committed scope really is.
    manifest = _committed_pages(connection, plan)
    clocks = [_timestamp(value) for value in (committed, acknowledged) if value is not None]
    clocks.extend(_timestamp(clock) for _url, _capture_id, clock, _body_hash in manifest)
    if not clocks:
        return
    if max(clocks) > _timestamp(plan.captured_at):
        raise StaleTransfermarktWrite('newer capture already committed to ' + plan.table)
    if plan.write_mode == 'replace' and (plan.rows or plan.pages):
        _guard_source_pages(connection, plan, manifest)


def _scope_json(plan: NativeWritePlan) -> str:
    return write_journal._canonical(
        list(zip(plan.scope_columns, plan.scope_values))).decode()


def _read_manifest(connection, plan: NativeWritePlan) -> dict[str, tuple]:
    """The accepted evidence of every page of the scope.

    ``url -> (capture id, clock, body hash)``. Two rows naming one page are
    not a manifest. Only a multi-page replacement snapshot keeps one: the
    pages of every other lane are exactly the captures of its committed rows.
    """
    if not plan._multi_page():
        return {}
    cursor = connection.cursor()
    try:
        rows = _query_rows(
            cursor,
            f'SELECT source_url, source_capture_id, captured_at, source_body_hash '
            f'FROM {CORE_PAGES_TABLE} WHERE target_table = ? AND scope_values_json = ?',
            (plan.full_table, _scope_json(plan)),
        )
    except Exception as exc:
        if write_journal._table_not_found(exc):
            return {}
        raise
    finally:
        cursor.close()
    accepted = {}
    for url, capture_id, clock, body_hash in rows:
        if url in accepted:
            raise RuntimeError('core page manifest differs: two rows name one page of ' + plan.table)
        accepted[url] = (capture_id, _timestamp(clock), body_hash)
    return accepted


def _committed_pages(connection, plan: NativeWritePlan) -> list[tuple]:
    """The accepted page evidence of the scope, (url, capture_id, clock, hash)."""
    return [(url, *evidence) for url, evidence in _read_manifest(connection, plan).items()]


def _accepted_without_rows(connection, plan: NativeWritePlan) -> bool:
    """Whether the scope acknowledged this plan's capture as a page without rows.

    A single captured page leaves no manifest: its zero-row receipt is the
    only evidence that its capture was accepted as empty.
    """
    cursor = connection.cursor()
    try:
        rows = _query_rows(
            cursor,
            f'SELECT row_count FROM {CORE_RECEIPTS_TABLE} WHERE target_table = ? '
            'AND scope_values_json = ? AND source_capture_id = ?',
            (plan.full_table, _scope_json(plan), plan.capture_id))
    except Exception as exc:
        if write_journal._table_not_found(exc):
            return False
        raise
    finally:
        cursor.close()
    return any(row[0] == 0 for row in rows)


def _verify_pages(connection, plan: NativeWritePlan) -> None:
    """The recorded manifest reads back as the snapshot's pages, one row each."""
    accepted = _read_manifest(connection, plan)
    for url, capture_id, clock, body_hash, _next in plan.pages:
        if accepted.get(url) != (capture_id, _timestamp(clock), body_hash):
            raise RuntimeError('core page manifest differs: ' + plan.table)


def _record_pages(cursor, plan: NativeWritePlan) -> None:
    """Keep the newest accepted evidence of every page the snapshot walked.

    One statement per page, upserting that page's row: nothing is deleted,
    so an interrupted update leaves the previous evidence of the page it
    was updating, and every other page, exactly as accepted.
    """
    columns = ', '.join(PAGES_COLUMNS)
    marks = ', '.join('?' for _ in PAGES_COLUMNS)
    inserted = ', '.join('s.' + column for column in PAGES_COLUMNS)
    sql = (
        f'MERGE INTO {CORE_PAGES_TABLE} t USING (VALUES ({marks})) s({columns}) '
        'ON t.target_table = s.target_table AND t.scope_values_json = s.scope_values_json '
        'AND t.source_url = s.source_url '
        'WHEN MATCHED AND s.captured_at >= t.captured_at THEN UPDATE SET '
        'source_capture_id = s.source_capture_id, captured_at = s.captured_at, '
        'source_body_hash = s.source_body_hash '
        f'WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({inserted})'
    )
    scope = (plan.full_table, _scope_json(plan))
    for url, capture_id, clock, body_hash, _next in plan.pages:
        # Trino timestamp(6) stores UTC wall time without an offset.
        execute_statement(
            cursor, sql,
            (*scope, url, capture_id, _timestamp(clock).replace(tzinfo=None), body_hash))


def _same_pages(manifest, pages) -> bool:
    """Whether every page the snapshot walks is accepted exactly as planned.

    The evidence of other pages the scope once accepted stays beside it; it
    says nothing about this snapshot.
    """
    accepted = {(url, capture_id, _timestamp(clock), body_hash)
                for url, capture_id, clock, body_hash in manifest}
    return all((page[0], page[1], _timestamp(page[2]), page[3]) in accepted for page in pages)


def _guard_source_pages(connection, plan: NativeWritePlan, manifest=()) -> None:
    """Each source page the snapshot shares with the scope must not regress.

    The data rows prove the pages that still contribute one; the manifest
    proves the pages a newer walk left without rows.
    """
    planned = {url: _timestamp(clock) for url, _capture_id, clock, _body_hash, _next in plan.pages}
    for row in plan.rows:
        clock, url = _timestamp(row['captured_at']), row['source_url']
        if url not in planned or planned[url] < clock:
            planned[url] = clock
    cursor = connection.cursor()
    try:
        committed = _query_rows(
            cursor,
            f'SELECT source_url, MAX(captured_at) FROM {plan.full_table} '
            f'WHERE {plan.scope_sql()} GROUP BY source_url',
            tuple(plan.scope_values),
        )
    finally:
        cursor.close()
    accepted = [(url, clock) for url, _capture_id, clock, _body_hash in manifest]
    for url, clock in [*committed, *accepted]:
        if url in planned and _timestamp(clock) > planned[url]:
            raise StaleTransfermarktWrite('newer capture already committed to ' + plan.table)


def _canonical_clock(value) -> str:
    """The one spelling of a moment: UTC ISO text with microseconds.

    Anything the text does not parse as (a non-string, a broken timestamp)
    is returned unchanged, so the caller's own comparison refuses it.
    """
    try:
        return _timestamp(value).isoformat(timespec='microseconds')
    except (TypeError, ValueError):
        return value


def _key_cells(values, columns=()) -> tuple:
    cells = []
    for index, value in enumerate(values):
        # An engine returns a clock as a datetime, a plan holds it as text;
        # both are compared as the moment they name, never as spelling.
        if index < len(columns) and columns[index].endswith('_at'):
            value = _canonical_clock(value) if isinstance(value, str) else value
        cells.append(_canonical_cell(value))
    return tuple(cells)


def _guard_key_owners(connection, plan: NativeWritePlan) -> None:
    """A natural key that names its capture belongs to exactly one capture.

    Two captures can share a clock (a replayed page, a re-stored body): the
    merge would replace the other capture's observation row by row. The
    lookup binds only varchar key columns; dates, clocks and the rest of the
    key are compared here against the incoming row.
    """
    if not plan.rows or 'source_capture_id' in plan.natural_keys:
        return
    keys = [column for column in plan.natural_keys if column != 'source_capture_id']
    prefilter = _KEY_PREFILTER_COLUMNS.get(plan.entity, ())
    planned = {_key_cells((row[column] for column in keys), keys) for row in plan.rows}
    matches = ' OR '.join(
        '(' + ' AND '.join(f'{column} = ?' for column in prefilter) + ')' for _ in plan.rows
    ) if prefilter else '1 = 1'
    params = [*plan.scope_values, plan.capture_id]
    for row in plan.rows:
        params.extend(row[column] for column in prefilter)
    cursor = connection.cursor()
    try:
        rows = _query_rows(
            cursor,
            f"SELECT {', '.join(keys)}, source_capture_id FROM {plan.full_table} "
            f"WHERE {plan.scope_sql()} AND source_capture_id <> ? AND ({matches})",
            tuple(params),
        )
    finally:
        cursor.close()
    if any(_key_cells(row[:len(keys)], keys) in planned for row in rows):
        raise RuntimeError('core natural key is owned by another capture: ' + plan.table)


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


# A re-observation of the same capture regenerates run identity only.
_LINEAGE_ONLY_COLUMNS = frozenset({'run_id', '_batch_id', '_ingested_at', 'extracted_at'})


def _strip_run_lineage(rows) -> list[dict]:
    return [
        {column: value for column, value in row.items()
         if column not in _LINEAGE_ONLY_COLUMNS} for row in rows]


def _rows_equal_ignoring_lineage(left, right) -> bool:
    return _rows_equal(_strip_run_lineage(left), _strip_run_lineage(right))


def _plan_key(plan: NativeWritePlan, row: Mapping) -> tuple:
    return tuple(str(row[column]) for column in plan.natural_keys)


def _by_capture(rows) -> dict:
    grouped = {}
    for row in rows:
        grouped.setdefault(row['source_capture_id'], []).append(row)
    return grouped


def _replay_state(connection, plan: NativeWritePlan) -> str:
    """Classify a plan against what the scope already holds.

    ``written`` needs nothing, ``refresh`` is the same capture observed under
    a new run (only lineage differs), ``pending`` still has to be written. A
    capture that is committed with other business rows is never rewritten.
    """
    if plan.write_mode == 'replace':
        # A replacement is classified capture by capture against everything
        # the scope accepted: its rows, and for a snapshot its manifest. A
        # page without rows is a capture with an empty row set: committed
        # with rows it is never replayed as empty, accepted without rows it
        # never acquires rows, and it never changes its evidence.
        existing = _read_plan_rows(connection, plan)
        committed, planned = _by_capture(existing), _by_capture(plan.rows)
        accepted = _read_manifest(connection, plan)
        accepted_by_capture = {evidence[0]: (url, *evidence[1:]) for url, evidence in accepted.items()}
        for url, capture_id, clock, body_hash, _next in plan.pages:
            planned.setdefault(capture_id, [])
            was_accepted = accepted_by_capture.get(capture_id)
            if was_accepted is not None:
                if was_accepted != (url, _timestamp(clock), body_hash):
                    raise RuntimeError('core replay differs from committed capture: ' + plan.table)
                committed.setdefault(capture_id, [])
        if not plan.pages:
            planned.setdefault(plan.capture_id, [])
            if _accepted_without_rows(connection, plan):
                committed.setdefault(plan.capture_id, [])
        for capture_id, rows in planned.items():
            shared = committed.get(capture_id)
            if shared is not None and (len(shared) != len(rows)
                                       or not _rows_equal_ignoring_lineage(shared, rows)):
                raise RuntimeError('core replay differs from committed capture: ' + plan.table)
        if not plan.rows and not plan.pages:
            # One empty page proves itself only through its receipt.
            return 'pending'
        # A refreshed snapshot reuses the pages it still shares with the
        # committed one. Its rowless pages hold no row to compare: the
        # committed manifest is what makes an identical snapshot "written".
        # Only captures that carry rows live in the data table; rowless ones
        # are proven by the manifest above.
        carrying = [capture for capture, rows in planned.items() if rows]
        if {capture for capture, rows in committed.items() if rows} != set(carrying) \
                or not _same_pages(_committed_pages(connection, plan), plan.pages):
            return 'pending'
        refreshed = any(not _rows_equal(committed[capture_id], planned[capture_id])
                        for capture_id in carrying)
        return 'refresh' if refreshed else 'written'
    if not plan.rows:
        return 'pending'
    existing = _read_plan_rows(connection, plan)
    if not existing:
        return 'pending'
    partner, complete = plan.rows, True
    if plan.entity in _PAGE_OWNED_ROWS_ENTITIES:
        # Only the keys this plan still holds are compared; the other
        # committed rows of the capture stay the observations they were.
        held = {_plan_key(plan, row) for row in plan.rows}
        existing = [row for row in existing if _plan_key(plan, row) in held]
        committed_keys = {_plan_key(plan, row) for row in existing}
        partner = [row for row in plan.rows if _plan_key(plan, row) in committed_keys]
        complete = len(existing) == len(plan.rows)
    if existing and not _rows_equal(existing, partner):
        if not _rows_equal_ignoring_lineage(existing, partner):
            raise RuntimeError('core replay differs from committed capture: ' + plan.table)
        # A lineage-only refresh may skip the guards only when every incoming
        # key is already committed under this very capture.
        return 'refresh' if complete else 'pending'
    return 'written' if existing and len(existing) == len(plan.rows) else 'pending'


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
    if not plan.rows:
        # A proven-empty replacement has nothing to stage: emptying the
        # scope is the one statement of its own.
        cursor = connection.cursor()
        try:
            execute_statement(
                cursor, f'DELETE FROM {plan.full_table} WHERE {plan.scope_sql()}',
                plan.scope_values)
        finally:
            cursor.close()
        return
    import pandas as pd

    columns = list(dict.fromkeys(col for row in plan.rows for col in row))
    frame = pd.DataFrame(list(plan.rows), columns=columns, dtype=object)
    # The scope is handed to the writer as its delete filter: the
    # Transfermarkt writer stages the rows and replaces the scope with one
    # statement, so a failed insertion leaves the committed rows in place.
    writer.write_dataframe(
        df=frame, database='bronze', table=plan.table, add_metadata=False,
        delete_filter=plan.scope_filter_sql(),
    )


def _physical_proof(connection, plan: NativeWritePlan) -> dict:
    if _uses_merge(plan) and not plan.rows:
        cursor = connection.cursor()
        try:
            counts = _query_rows(
                cursor,
                f'SELECT COUNT(*) FROM {plan.full_table} WHERE {plan.scope_sql()} '
                'AND source_capture_id = ?',
                (*plan.scope_values, plan.capture_id),
            )
        finally:
            cursor.close()
        if len(counts) != 1 or counts[0][0]:
            raise RuntimeError('core physical readback differs: ' + plan.table)
        return {
            'entity': plan.entity, 'table': plan.table, 'rows': 0,
            'physical_hash': _digest([]), 'expected_rows': 0, 'status_counts': {},
        }
    persisted = _read_plan_rows(connection, plan)
    if _uses_merge(plan) and plan.entity in _PAGE_OWNED_ROWS_ENTITIES:
        # An earlier walk may have left rows of this capture that another
        # page owns now: this plan verifies the rows it still holds.
        held = {_plan_key(plan, row) for row in plan.rows}
        persisted = [row for row in persisted if _plan_key(plan, row) in held]
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


def _plan_receipt(plan, proof, plan_hash, digest, run_id, universe_version,
                  batch, committed_at) -> dict:
    first = {**dict(plan.evidence)}
    # The snapshot clock is its newest walked page; the receipt names one
    # capture and carries that capture's own clock (the same clock whenever
    # a single captured page is the whole plan).
    captured_at = plan.captured_at
    if plan.rows:
        row = next(row for row in plan.rows if row['source_capture_id'] == plan.capture_id)
        captured_at = row['captured_at']
        # The validated evidence is what the receipt names; a row only
        # supplies what the evidence does not carry.
        first.update({column: value for column, value in row.items() if column not in first})
    evidence = {
        col: first.get(col, '') for col in (
            'scope_id', 'cycle_id', 'source_capture_id', 'source_body_hash',
            'parser_revision', 'schema_revision',
        )
    }
    # Bind the receipt to this bundle: the same plan committed inside a
    # different bundle gets its own append-only receipt, never an UPDATE
    # that would contradict the earlier immutable acknowledgement.
    receipt_hash = sha256((plan_hash + run_id + digest).encode('utf-8')).hexdigest()
    return {
        **evidence, 'receipt_id': 'tm-core-receipt-' + receipt_hash[:32],
        'target_table': plan.full_table, 'entity': plan.entity,
        'run_id': run_id, 'universe_version': universe_version,
        'natural_key_columns_json': write_journal._canonical(plan.natural_keys).decode(),
        'replace_key_columns_json': write_journal._canonical(plan.scope_columns).decode(),
        'scope_values_json': write_journal._canonical(
            list(zip(plan.scope_columns, plan.scope_values))).decode(),
        'expected_rows': proof['expected_rows'], 'row_count': proof['rows'],
        'status_counts_json': write_journal._canonical(proof['status_counts']).decode(),
        'observation_status': plan.status, 'status_reason': plan.status_reason,
        'status_proof': plan.proof,
        'plan_sha256': plan_hash, 'physical_hash': proof['physical_hash'],
        'committed_batch': batch, 'committed_at': committed_at, 'captured_at': captured_at,
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


def validate_run_plans(plans) -> None:
    """The cross-plan contract of a set of plans.

    One entity may appear in several plans (one club per memberships plan);
    a natural key and a replacement scope are each owned by one plan. It is
    checked for a bundle before it is journalled, and for every bundle of a
    run before the first of them mutates anything: a collision between two
    targets must not surface after the first one has committed.
    """
    seen = set()
    scopes = set()
    for plan in plans:
        for row in plan.rows:
            key = (plan.table, tuple(row[col] for col in plan.natural_keys))
            if key in seen:
                raise ValueError('duplicate core natural key across plans')
            seen.add(key)
        if plan.write_mode == 'replace':
            scope = (plan.table, plan.scope_values)
            if scope in scopes:
                raise ValueError('duplicate core replacement scope across plans')
            scopes.add(scope)


def _validate_bundle(plans, run_id, universe_version, committed_at) -> None:
    if not plans:
        raise ValueError('core bundle requires at least one plan')
    if not all((run_id, universe_version, committed_at)):
        raise ValueError('core run_id, universe_version and committed_at must be non-empty')
    _timestamp(committed_at)
    validate_run_plans(plans)


def _ddl_columns(ddl: str) -> dict[str, str]:
    """Column names and types declared by a plan's CREATE TABLE statement."""
    head = ddl.split('WITH', 1)[0]
    body = head[head.index('(') + 1:head.rindex(')')]
    parts, current, depth = [], '', 0
    for char in body:
        if char == '(':
            depth += 1
        elif char == ')':
            depth -= 1
        if char == ',' and depth == 0:
            parts.append(current)
            current = ''
        else:
            current += char
    parts.append(current)
    columns = {}
    for part in parts:
        declared = fullmatch(r'\s*([A-Za-z_][A-Za-z0-9_]*)\s+(.+?)\s*', part, re.S)
        if declared:
            columns[declared[1]] = declared[2].replace(' ', '').lower()
    return columns


def _guard_table_schema(connection, plan: NativeWritePlan) -> None:
    """A write never lands on a table of another schema.

    Section lanes reuse the names of tables the retired donor stack left
    behind, and CREATE TABLE IF NOT EXISTS does not recreate those. Refuse
    before any mutation: a column the plan's DDL declares is missing or has
    another type, or the table carries a column that belongs neither to that
    DDL nor to the plan. A rowless replacement deletes the scope, so it is
    checked as well.
    """
    cursor = connection.cursor()
    try:
        try:
            _query_rows(cursor, f'SELECT * FROM {plan.full_table} WHERE 1 = 0')
        except Exception as exc:
            if write_journal._table_not_found(exc):
                return
            raise
        existing = {column[0]: str(column[1] or '').replace(' ', '').lower()
                    for column in cursor.description or []}
    finally:
        cursor.close()
    if not existing:
        return
    declared = _ddl_columns(plan.ddl)
    carried = {column for row in plan.rows for column in row}
    # An engine that reports no type for a column leaves it to the names.
    mistyped = [name for name, kind in declared.items()
                if name in existing and existing[name] not in ('', 'none', 'null')
                and existing[name] != kind]
    # A rowless replacement deletes live rows of the scope: the table must be
    # exactly the plan's, with no donor column beside the declared ones.
    foreign = set(existing) - set(declared) - carried
    if set(declared) - set(existing) or foreign or mistyped:
        raise RuntimeError('core table schema differs from the plan: ' + plan.table)


def _commit_plans(connection, writer, plans) -> list[dict]:
    execute_statement(connection.cursor(), core_receipts_ddl())
    if any(plan.pages for plan in plans):
        execute_statement(connection.cursor(), core_pages_ddl())
    for plan in plans:
        execute_statement(connection.cursor(), plan.ddl)
        _guard_table_schema(connection, plan)
    states = [(plan, _replay_state(connection, plan)) for plan in plans]
    for plan, state in states:
        # A lineage-only refresh rewrites the run identity of rows that are
        # already committed under this very capture: nothing can regress,
        # whatever clock a sibling capture of the scope carries.
        if state == 'pending' or (state == 'refresh' and not _capture_pinned(plan)):
            guard_native_bundle(connection, plan)
    for plan, state in states:
        if state != 'written':
            _mutate_plan(connection, writer, plan)
    for plan, state in states:
        # The manifest follows the snapshot it belongs to; a snapshot that
        # was already written has its manifest already.
        if plan.pages and state != 'written':
            cursor = connection.cursor()
            try:
                _record_pages(cursor, plan)
            finally:
                cursor.close()
    # Like the receipts, the manifest is acknowledged only once it reads
    # back: a snapshot whose pages did not reach the warehouse is not written.
    for plan in plans:
        if plan.pages:
            _verify_pages(connection, plan)
    return [_physical_proof(connection, plan) for plan in plans]


def _batch_id_for_run(run_id: str) -> str:
    """The batch printed on the rows; the receipt must name the same one."""
    return 'tm-core-batch-' + sha256(run_id.encode('utf-8')).hexdigest()[:32]


def _anchored_tables(plans) -> set[str]:
    """Every table the bundle mutates, for its pre-write snapshot anchors."""
    tables = {plan.table for plan in plans} | {_RECEIPTS_ANCHOR_NAME}
    if any(plan.pages for plan in plans):
        tables.add(_PAGES_ANCHOR_NAME)
    return tables


def _finish_leftover_intent(digest: str, run_id: str) -> None:
    """A crash between record_receipt and finish_intent must not strand it.

    Journal files are named by their own body checksum, not the bundle
    digest, so the leftover unit is found through the identity index.
    """
    for path, _body in write_journal.pending_intents({'run_id': run_id, 'digest': digest}):
        write_journal.finish_intent(path)


def _bundle_identity(plans, run_id: str) -> tuple[list[str], str]:
    """The plan hashes and the bundle digest a commit is acknowledged under."""
    plan_hashes = [plan.plan_sha256() for plan in plans]
    return plan_hashes, _digest({'plans': plan_hashes, 'run_id': run_id})


def _stage_b_plan_sha256(plan: NativeWritePlan) -> str:
    """The plan digest of the stage-B writer (#2404).

    Stage C added page evidence to a plan and capture evidence to a rowless
    plan, which changed the digest of the latter. An intent the stage-B
    writer left pending names its unit by this digest and by the warehouse
    receipts derived from it, so its recovery is bound to the same identity.
    """
    return _digest({
        'entity': plan.entity, 'table': plan.table, 'natural_keys': plan.natural_keys,
        'scope': list(zip(plan.scope_columns, plan.scope_values)),
        'write_mode': plan.write_mode, 'captured_at': plan.captured_at,
        'status': plan.status, 'status_reason': plan.status_reason, 'proof': plan.proof,
        'rows': _sorted_rows(plan.rows, plan.natural_keys),
    })


def write_core_bundle(
    connection_factory, writer, plans: Sequence[NativeWritePlan], *,
    run_id: str, universe_version: str, committed_at: str, deadline: float | None = None,
) -> dict:
    """Commit one journaled bundle and acknowledge its verified warehouse receipts."""
    plan_hashes, digest = _bundle_identity(plans, run_id)
    return _commit_bundle(
        connection_factory, writer, plans, plan_hashes=plan_hashes, digest=digest,
        run_id=run_id, universe_version=universe_version, committed_at=committed_at,
        deadline=deadline)


def _commit_bundle(
    connection_factory, writer, plans, *, plan_hashes, digest, run_id: str,
    universe_version: str, committed_at: str, deadline: float | None = None,
) -> dict:
    """Commit a bundle under a given identity: its own, or the one it was
    journalled under by an earlier writer."""
    with writer_deadline(deadline), writer_lock():
        _validate_bundle(plans, run_id, universe_version, committed_at)
        if len(set(plan_hashes)) != len(plan_hashes):
            # One acknowledgement never stands for two plans of a bundle.
            raise ValueError('duplicate core receipt identity across plans')
        batch = _batch_id_for_run(run_id)
        stored = write_journal.read_receipt(digest)
        if stored is not None:
            if stored.get('plan_sha256') != digest:
                raise RuntimeError('core journal receipt plan_sha256 differs')
            # An acknowledgement re-verifies the whole physical tail: drifted
            # rows fail closed, a deleted receipt row is rewritten and
            # re-read, and a leftover intent is finally finished.
            connection = connection_factory()
            try:
                write_journal.snapshot_anchors(digest, connection, _anchored_tables(plans))
                proofs = _commit_plans(connection, writer, plans)
                plan_receipts = [
                    _plan_receipt(plan, proof, plan_hash, digest, run_id, universe_version,
                                  batch, stored['committed_at'])
                    for plan, proof, plan_hash in zip(plans, proofs, plan_hashes, strict=True)
                ]
                _merge_receipts(connection, plan_receipts)
                _verify_receipts(connection, plan_receipts)
            finally:
                connection.close()
            _finish_leftover_intent(digest, run_id)
            return stored

        descriptors = [asdict(plan) for plan in plans]
        for descriptor, plan in zip(descriptors, plans):
            descriptor['rows'] = _sorted_rows(plan.rows, plan.natural_keys)
        intent_path = write_journal.save_intent(
            {'run_id': run_id, 'digest': digest}, {'plans': descriptors},
            run_id=run_id, universe_version=universe_version, committed_at=committed_at,
        )
        connection = connection_factory()
        try:
            write_journal.snapshot_anchors(digest, connection, _anchored_tables(plans))
            proofs = _commit_plans(connection, writer, plans)
            plan_receipts = [
                _plan_receipt(plan, proof, plan_hash, digest, run_id, universe_version,
                              batch, committed_at)
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


def replay_pending_intents(connection_factory, writer, *, run_id: str | None = None) -> list[dict]:
    """Replay journaled units after a crash without another paid request.

    Each pending intent carries its exact plans. Its identity is settled
    before anything is mutated: this writer's digest, or the stage-B digest
    of an intent that writer left pending. Such an intent is recovered under
    the identity it was written with, so a warehouse receipt it had already
    produced is verified or completed, never duplicated. A digest that is
    neither says nothing about what the intent committed and is refused.
    """
    receipts = []
    for path, body in write_journal.pending_intents({'run_id': run_id} if run_id else {}):
        plans = []
        for descriptor in body['payload']['plans']:
            descriptor = dict(descriptor)
            descriptor['rows'] = [dict(row) for row in descriptor['rows']]
            descriptor['natural_keys'] = tuple(descriptor['natural_keys'])
            descriptor['scope_columns'] = tuple(descriptor['scope_columns'])
            descriptor['scope_values'] = tuple(descriptor['scope_values'])
            plans.append(NativeWritePlan(**descriptor))
        run = body['identity']['run_id']
        plan_hashes, digest = _bundle_identity(plans, run)
        saved = body['identity'].get('digest')
        if saved != digest:
            plan_hashes = [_stage_b_plan_sha256(plan) for plan in plans]
            digest = _digest({'plans': plan_hashes, 'run_id': run})
            if saved != digest:
                raise RuntimeError('core journal intent digest is not recognised: ' + str(saved))
        receipt = _commit_bundle(
            connection_factory, writer, plans, plan_hashes=plan_hashes, digest=digest,
            run_id=run,
            universe_version=body['evidence'].get('universe_version', 'core-universe-v1'),
            committed_at=body['evidence'].get('committed_at', ''))
        if path.exists():
            # The verified recovery finishes the very file that was replayed,
            # whatever format this writer would save the unit in.
            write_journal.finish_intent(path)
        receipts.append(receipt)
    return receipts
