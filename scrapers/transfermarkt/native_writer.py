"""Mono-native Transfermarkt writer: lock/commit-retry primitives (#2404).

Layer 5 of the rewrite (#2400). The cross-process lock and the Iceberg
commit-conflict retry ladder moved here verbatim from ``writer.py``, which
keeps re-exporting them for the donor stack. HTTP and parsing never hold the
lock; all TM processes on the scheduler must share its path (the shared
Airflow logs volume).
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from contextlib import contextmanager
import fcntl
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
from re import fullmatch
from typing import Any, NamedTuple, Sequence
from uuid import uuid4

from . import write_journal
import random
import re
import threading
import time
from types import MappingProxyType, SimpleNamespace

try:
    from trino.dbapi import Connection as TrinoConnection
    from trino.exceptions import TrinoQueryError
except ImportError:  # the donor modules also support imports without the DBAPI
    TrinoConnection = TrinoQueryError = ()

from scrapers.base.trino_manager import (
    TrinoAmbiguousWriteError, _is_definite_iceberg_commit_conflict, _is_iceberg_commit_conflict,
)


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


@contextmanager
def journalled_write():
    """Mark the statements of this thread as belonging to a journalled unit.

    Staging uses a single-submit transport. The target cursor fences its
    own dispatch across calls under the shared writer lock.
    """
    previous = getattr(_LOCAL, 'journalled', False)
    _LOCAL.journalled = True
    try:
        yield
    finally:
        _LOCAL.journalled = previous


def write_is_journalled() -> bool:
    return bool(getattr(_LOCAL, 'journalled', False))


def _journal_connection(factory):
    """Configure the bundle-owned DBAPI connection before it constructs any requests."""
    connection = factory()
    raw = connection._connection if isinstance(connection, CommittingConnection) else connection
    if isinstance(raw, TrinoConnection):
        raw.max_attempts = 1
        raw.legacy_prepared_statements = False
    return connection


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
    def execute_once(self, sql, params=None):
        """Run a statement exactly once: the caller resolves an ambiguous commit."""
        result = self._cursor.execute(sql, tuple(params)) if params is not None else self._cursor.execute(sql)
        return list(self._cursor.fetchall()) if is_mutation(sql) else result

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
# What a receipt states about the physical result of its write; every other
# cell is known before anything is written.
_RECEIPT_PROOF_COLUMNS = frozenset({'row_count', 'expected_rows', 'status_counts_json', 'physical_hash'})
_UNWRITTEN_PROOF = {'entity': '', 'table': '', 'rows': None, 'physical_hash': None,
                   'expected_rows': None, 'status_counts': None}


def core_receipts_ddl() -> str:
    """Create the warehouse evidence table for verified core writes."""
    types = dict.fromkeys(RECEIPT_COLUMNS, 'varchar')
    types.update(dict.fromkeys(_RECEIPT_BIGINT_COLUMNS, 'bigint'))
    types.update(dict.fromkeys(_RECEIPT_TIMESTAMPS, 'timestamp(6)'))
    columns = ', '.join(f'{name} {types[name]}' for name in RECEIPT_COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {CORE_RECEIPTS_TABLE} ({columns}) WITH (format = 'PARQUET')"


# The page evidence of every acknowledged multi-page replacement snapshot.
# A page that contributes no row leaves nothing in the data table, and a page
# that later leaves the walk would leave nothing either: without this log its
# capture id and clock are known only while the write runs, and it could come
# back with an older capture. Every acknowledgement logs the walk it stands
# for under its receipt id, one immutable row per page: rows are never
# updated or deleted, and a receipt left without its rows has lost evidence.
CORE_PAGES_TABLE = 'iceberg.ops.transfermarkt_core_pages_v1'
_PAGES_ANCHOR_NAME = 'ops.transfermarkt_core_pages_v1'
PAGES_COLUMNS = (
    'receipt_id', 'target_table', 'scope_values_json', 'page_number', 'pages_count',
    'source_url', 'source_capture_id', 'captured_at', 'source_body_hash', 'row_count',
    'rows_sha256', 'acknowledged_at',
)
_PAGES_BIGINT_COLUMNS = frozenset({'page_number', 'pages_count', 'row_count'})
_PAGES_TIMESTAMPS = frozenset({'captured_at', 'acknowledged_at'})


def core_pages_ddl() -> str:
    """Create the warehouse log of the pages every acknowledgement stands for."""
    types = dict.fromkeys(PAGES_COLUMNS, 'varchar')
    types.update(dict.fromkeys(_PAGES_BIGINT_COLUMNS, 'bigint'))
    types.update(dict.fromkeys(_PAGES_TIMESTAMPS, 'timestamp(6)'))
    columns = ', '.join(f'{name} {types[name]}' for name in PAGES_COLUMNS)
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
# Only what a page conclusively established is written. An unknown,
# unavailable or failed observation replaces nothing, whatever calls the
# writer with it. Rows exist under the two statuses that carry rows; an
# absence exists only as a proof.
_CONCLUSIVE_STATUSES = frozenset({
    'observed_present', 'observed_empty', 'explicit_none', 'not_applicable',
})
_STATUSES_WITH_ROWS = frozenset({'observed_present', 'explicit_none'})
_PROOF_STATUSES = frozenset({'observed_empty', 'not_applicable'})


def _nonempty(value) -> bool:
    return isinstance(value, str) and bool(value)


def _check_columns(columns) -> None:
    for name in columns:
        if not isinstance(name, str) or not fullmatch(r'[a-z_][a-z0-9_]*', name):
            raise ValueError(f'invalid core column name: {name!r}')


class _Row(Mapping):
    """A validated plan row: an immutable mapping of its values.

    Hashing, journalling, writing and read-back must all see the content the
    plan validated. The values live in a mapping proxy no other code holds a
    reference to, and the row has neither a writable initialiser nor any
    mutating method: another plan is the way to other rows.
    """

    __slots__ = ('_values',)

    def __init__(self, values):
        # A row is filled once. The initialiser a copy or a pickle starts is
        # that of a new instance; calling it again on a built row refills
        # nothing.
        if hasattr(self, '_values'):
            raise TypeError('core plan rows are read-only: build another plan')
        object.__setattr__(self, '_values', MappingProxyType(dict(values)))

    def __setattr__(self, name, value):
        raise TypeError('core plan rows are read-only: build another plan')

    def __delattr__(self, name):
        raise TypeError('core plan rows are read-only: build another plan')

    def __getitem__(self, column):
        return self._values[column]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __repr__(self):
        return f'_Row({dict(self._values)!r})'

    def __reduce__(self):
        # copy and pickle rebuild a row through its constructor, never by
        # assigning into it.
        return _Row, (dict(self._values),)


def _plain_rows(rows) -> list[dict]:
    """Plan rows as plain dictionaries: for hashing, journalling and frames."""
    return [dict(row) for row in rows]


def _sorted_rows(rows, natural_keys) -> list[dict]:
    # Keep numeric key ordering even for a bundle containing both ints and strings.
    def key(row):
        return tuple((isinstance(row[col], str), row[col]) for col in natural_keys)

    return sorted(_plain_rows(rows), key=key)


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
        object.__setattr__(self, 'scope_values', tuple(self.scope_values))
        object.__setattr__(self, 'evidence', tuple(tuple(item) for item in self.evidence))
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
        if self.captured_at != _canonical_clock(self.captured_at):
            raise ValueError('core captured_at must be a timestamp in its canonical spelling')
        if self.write_mode not in ('replace', 'merge'):
            raise ValueError('core write_mode must be replace or merge')
        if self.status not in _CONCLUSIVE_STATUSES or not _nonempty(self.status_reason):
            raise ValueError('core plan status must be conclusive, with its reason')
        if bool(self.rows) != (self.status in _STATUSES_WITH_ROWS):
            raise ValueError(
                'core plan status and rows disagree: rows need a status that carries '
                'rows, and an absence has none')
        if self.status in _PROOF_STATUSES and not _nonempty(self.proof):
            raise ValueError('core plan proof is required for empty observations')
        self._check_entity_shape()
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
        self._check_entity_rows()
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
            # The same tie-break as the adapter: the newest clock, then the
            # highest capture id.
            if self.capture_id != max(
                    self.pages, key=lambda page: (_timestamp(page[2]), page[1]))[1]:
                raise ValueError('core rowless snapshot must name its newest page')
        evidence_map = dict(self.evidence)
        for column in ('raw_capture_id', 'source_capture_id'):
            if evidence_map.get(column, self.capture_id) != self.capture_id:
                raise ValueError('core plan evidence names a different capture')

    def _multi_page(self) -> bool:
        return self.write_mode == 'replace' and self.entity in _MULTI_PAGE_REPLACE_ENTITIES

    def response_evidence(self) -> dict[str, tuple]:
        """Every capture the plan rests on and the one response it names.

        ``capture id -> (source_url, source_body_hash, captured_at)``.
        """
        if self.pages:
            return {page[1]: (page[0], page[3], _timestamp(page[2])) for page in self.pages}
        if self.rows:
            row = self.rows[0]
            return {row['source_capture_id']: (
                row['source_url'], row['source_body_hash'], _timestamp(row['captured_at']))}
        evidence = dict(self.evidence)
        return {self.capture_id: (
            evidence.get('source_url'), evidence.get('source_body_hash'),
            _timestamp(self.captured_at))}

    def _check_entity_shape(self) -> None:
        """The plan is the registered entity's, in its registered shape.

        A plan reaches the writer from ``plan_for_outcome`` or from a journal
        descriptor. Neither may decide the table, the mode, the keys, the
        scope or the DDL of an entity on its own: that is what lets a
        reconstructed descriptor turn a merge lane into a replacement.
        """
        contract = _entity_registry(self.entity)
        if (self.table != contract['table'] or self.write_mode != contract['write_mode']
                or self.natural_keys != contract['natural_keys']
                or self.scope_columns != contract['scope_columns'] or self.ddl != contract['ddl']):
            raise ValueError('core plan departs from the entity contract: ' + self.entity)

    def _check_entity_rows(self) -> None:
        """Rows, verdicts and page evidence follow the entity's contract.

        The same policy that certifies an outcome certifies a plan: rows are
        exactly the entity's columns, the verdict agrees with the rows, a
        proof belongs to an absence only, and a snapshot is a walk from the
        scope's first page.
        """
        from scrapers.transfermarkt import core

        columns = set(core.CORE_COLUMNS[self.entity])
        if any(row.keys() != columns for row in self.rows):
            raise ValueError("core plan rows are not the entity's column set: " + self.entity)
        if not core._status_matches_rows(self.entity, self.status, self.rows):
            raise ValueError('core plan status contradicts its rows: ' + self.entity)
        if self.proof and self.status not in _PROOF_STATUSES:
            raise ValueError('core plan proof is unexpected beside rows')
        scope = dict(zip(self.scope_columns, self.scope_values, strict=True))
        core._check_pages_against_scope(self.entity, scope, self.pages)
        # A journal descriptor has no SourceCapture body. Its evidence (or
        # the row lineage of early populated descriptors) still names the route.
        evidence = dict(self.evidence)
        first = self.rows[0] if self.rows else evidence
        endpoint = evidence.get('source_endpoint', first['source_endpoint'])
        urls = {page[0] for page in self.pages} | {row['source_url'] for row in self.rows}
        urls.add(evidence.get('source_url', first['source_url']))
        for url in urls:
            core._check_scope_against_capture(self.entity, scope, SimpleNamespace(url=url, endpoint=endpoint))
        if self.entity == 'club_value_observations' and any(
                row['competition_id'] != core._club_value_competition(row['source_url'])
                for row in self.rows):
            raise core.CoreParseError('row_scope_mismatch')

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
            # A page clock is stated one way only, like the clock of a row.
            if (not fullmatch(r'[0-9a-f]{64}', body_hash) or not isinstance(next_url, str)
                    or clock != _canonical_clock(clock)):
                raise ValueError('core page evidence needs distinct pages with url, capture and clock')
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
        """A row carries the evidence its plan names.

        Without a page manifest the plan's evidence is the one response
        behind every row; a snapshot's evidence describes the run its rows
        share, each page being compared with the manifest instead.
        """
        if not self.rows:
            return
        evidence = dict(self.evidence)
        columns = ['source_endpoint', 'scope_id', 'cycle_id']
        if not self.pages:
            columns += ['source_url', 'source_body_hash']
        else:
            # A snapshot's evidence describes the capture the plan names:
            # the receipt may not state another page's URL or body.
            named = next((page for page in self.pages if page[1] == self.capture_id), None)
            for column, index in (('source_url', 0), ('raw_capture_id', 1),
                                  ('source_capture_id', 1), ('source_body_hash', 3)):
                if (named is not None and evidence.get(column) is not None
                        and evidence[column] != named[index]):
                    raise ValueError('core plan evidence differs from its page evidence')
        for column in columns:
            if evidence.get(column) is not None and any(
                    column in row and row[column] != evidence[column] for row in self.rows):
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
        first = None
        column_types = {}
        shared = _RUN_COLUMNS if self._multi_page() else _RESPONSE_COLUMNS
        declared = _ddl_columns(self.ddl)
        timestamps = [column for column, kind in declared.items() if kind.startswith('timestamp')]
        for row in self.rows:
            if not isinstance(row, Mapping):
                raise ValueError('core plan row must be a mapping')
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
                # A clock is stated one way only: the engine reads several
                # spellings of one moment, and a key cell is compared as text.
                if value is not None and value != _canonical_clock(value):
                    raise ValueError(f'core plan row timestamp is not canonical: {column}')
            if ('observed_at' in self.natural_keys and row.get('observed_at') is not None
                    and _timestamp(row['observed_at']) != _timestamp(row['captured_at'])):
                raise ValueError('core observation clock must equal its capture clock')
            for column, value in row.items():
                kind = (None if value is None else 'bool' if isinstance(value, bool)
                        else 'int' if isinstance(value, int) else 'str')
                if kind and column_types.setdefault(column, kind) != kind:
                    raise ValueError(f'core plan column mixes value types: {column}')
            for column, value in row.items():
                if (value is not None and column in declared
                        and not _value_has_type(declared[column], value)):
                    raise ValueError(f'core plan value does not match its column type: {column}')
            if any(row[col] in ('', None) for col in required):
                raise ValueError('core plan row has empty lineage, key or capture columns')
            if any(str(row[col]) != str(value) for col, value in zip(self.scope_columns, self.scope_values)):
                raise ValueError('core plan row is outside its replacement scope')
            if not _nonempty(row['source_capture_id']):
                raise ValueError('core plan row has invalid source_capture_id')
            # The batch is derived from the run and the ingestion clock is
            # the extraction clock: a row cannot carry another run's.
            if (row['_batch_id'] != _batch_id_for_run(row['run_id'])
                    or row['_ingested_at'] != row['extracted_at']
                    or row['extracted_at'] != _canonical_clock(row['extracted_at'])):
                raise ValueError('core plan row disagrees with its run')
            if row['raw_capture_id'] != row['source_capture_id']:
                # One response has one identity: a row never names two captures.
                raise ValueError('core row names two captures: raw_capture_id differs from source_capture_id')
            if first is None:
                first = row
            # Every row carries the lineage of the response (the run, for a
            # snapshot) alike: the receipt names one capture for them all.
            elif any(column in first and row.get(column) != first[column] for column in shared):
                raise ValueError('core plan rows must share one source response')
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
_OBSERVATION_CLOCK_ENTITIES = _PAGE_OWNED_ROWS_ENTITIES | {
    'player_attribute_observations', 'player_contract_observations',
}
# Varchar key columns that narrow the ownership lookup of a large scope; a
# lane without an entry is read scope-wide (date and timestamp key columns
# are never bound as parameters).
_KEY_PREFILTER_COLUMNS = {
    'rumour_observations': ('rumour_identity', 'subject_id', 'correction_identity'),
    'referee_aggregate_observations': (
        'referee_id', 'competition_id', 'edition_id', 'metric'),
}
_CAPTURE_KEY_COLUMNS = frozenset({'observed_at', 'source_capture_id'})
# The lineage every row of one plan carries alike: the run it belongs to.
# A plan of one captured page is one response, and its rows carry that
# response's whole lineage; a snapshot holds one response per page.
_RUN_COLUMNS = (
    'source_endpoint', 'scope_id', 'cycle_id', 'run_id', 'universe_version', 'extracted_at',
)
_RESPONSE_COLUMNS = ('captured_at', 'source_url', 'source_body_hash', *_RUN_COLUMNS)
# The attendance edition is the one replacement snapshot that spans several
# walked pages; every other replacement is one captured page.
_MULTI_PAGE_REPLACE_ENTITIES = frozenset({'attendance_observations'})


def _capture_pinned(plan: NativeWritePlan) -> bool:
    """A merge lane whose natural key names its own capture."""
    return plan.write_mode == 'merge' and bool(_CAPTURE_KEY_COLUMNS.intersection(plan.natural_keys))


def _warehouse_members(connection, table, run, digest):
    """Original receipt IDs, scopes and counts bound to this table and bundle."""
    cursor = connection.cursor()
    try:
        rows = _query_rows(
            cursor, f'SELECT receipt_id, plan_sha256, scope_values_json, captured_at, row_count '
            f'FROM {CORE_RECEIPTS_TABLE} WHERE target_table = ? AND run_id = ?',
            (table, run))
    finally:
        cursor.close()
    members = [(key, scope, count) for key, plan_hash, scope, _, count in rows
               if key == _receipt_id(plan_hash, run, digest)]
    if len({key for key, _, _ in members}) != len(members):
        raise RuntimeError('core replacement receipt differs: ' + table)
    return members


def _stage_b_replacement_ids(connection, plan, receipt, proofs, own):
    """Resolve pre-identity proofs to the warehouse receipts of this unit."""
    digest, run = receipt['plan_sha256'], receipt['run_id']
    members = {key: count for key, _, count in _warehouse_members(connection, plan.full_table, run, digest)}
    key = _receipt_id(_stage_b_plan_sha256(plan), run, digest)
    if key in own:
        members.setdefault(key, len(plan.rows))
    if Counter(members.values()) != Counter(proof['rows'] for proof in proofs):
        raise RuntimeError('core replacement receipt lost: ' + plan.table)
    return members


def _replacement_history(connection, plan, own):
    """Yield acknowledged response and order evidence for this single-page scope.

    Current journal proofs survive loss of the receipt and all target rows.
    Older proofs need the warehouse to identify their scope and response.
    """
    scope = _scope_json(plan)
    ids = _scope_receipt_ids(connection, plan)
    required = {}
    for receipt in write_journal.acknowledgements():
        proofs = [(index, proof) for index, proof in enumerate(receipt.get('plans', ()))
                  if proof['table'] == plan.table]
        if not proofs:
            continue
        identity = receipt.get('identity')
        if not identity:
            keys = _stage_b_replacement_ids(connection, plan, receipt, [proof for _, proof in proofs], own)
            required.update(dict.fromkeys(keys, receipt['committed_at']))
            continue
        for index, proof in proofs:
            if 'scope_values_json' in proof and proof['scope_values_json'] != scope:
                continue
            if all(key in proof for key in ('scope_values_json', 'captured_at', 'committed_at',
                                             'source_capture_id', 'source_body_hash')):
                if _timestamp(receipt['committed_at']) != _timestamp(proof['committed_at']):
                    raise RuntimeError('core replacement acknowledgement clock differs')
                yield proof
            else:
                key = _receipt_id(identity['plan_hashes'][index], receipt['run_id'], identity['digest'])
                required[key] = receipt['committed_at']
    keys = sorted(ids | required.keys())
    for index in range(0, len(keys), 100):
        batch = keys[index:index + 100]
        held = _stored_receipts(connection, batch)
        if set(batch) - held.keys() - set(own):
            raise RuntimeError('core replacement receipt lost: ' + plan.table)
        for key, row in held.items():
            cells = dict(zip(RECEIPT_COLUMNS, row, strict=True))
            if (cells['target_table'] != plan.full_table
                    or not _nonempty(cells['scope_values_json'])
                    or key in required and _timestamp(required[key]) != _timestamp(cells['committed_at'])):
                raise RuntimeError('core replacement receipt differs: ' + plan.table)
            if cells['scope_values_json'] == scope:
                yield {**cells, 'rows': cells['row_count']}


def _check_replacement_order(plan, clock, acknowledged_at, previous_clock, previous_ack, *, same):
    if not all((clock, previous_clock, previous_ack)):
        raise RuntimeError('core replacement order lacks capture or acknowledgement clock')
    previous, incoming = _timestamp(previous_clock), _timestamp(clock)
    if previous > incoming or (previous == incoming and not same and acknowledged_at is not None
                               and _timestamp(previous_ack) >= _timestamp(acknowledged_at)):
        raise StaleTransfermarktWrite('newer capture already committed or conflicting tie in ' + plan.table)


def guard_native_bundle(connection, plan: NativeWritePlan, *, own=(), acknowledged_at=None, accepted=None) -> None:
    """An older bundle must not overwrite a fresher committed capture.

    The data table proves freshness while rows exist; a proven-empty
    replacement leaves none, so the per-scope warehouse receipts carry the
    clock of the capture they acknowledged for empty scopes too. A multi-page
    snapshot keeps more: its logged walks hold the accepted capture and clock
    of every walked page, rowless ones included, and that is where its
    freshness and its per-page check are read. The snapshot's newest clock
    says nothing about a stale page riding along on a fresh sibling.

    ``own`` names receipts of the unit being retried. The commit boundary
    supplies the checked walks, including evidence recovered from the journal.
    ``acknowledged_at`` is the commit clock of the unit being written: two
    different captures of one page at one clock are ordered by it, and the
    later acknowledgement is the one in force.
    """
    if plan.write_mode == 'merge' and _capture_pinned(plan):
        _guard_key_owners(connection, plan)
        if plan.entity in _MULTI_CAPTURE_MERGE_ENTITIES or plan.entity in _OBSERVATION_CLOCK_ENTITIES:
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
    # A rowless page leaves no row: the receipt and the logged walks are the
    # only proof of how fresh the committed scope really is.
    if accepted is None:
        accepted = _accepted_pages(connection, plan, own=own)
    clocks = [_timestamp(value) for value in (committed, acknowledged) if value is not None]
    if plan.write_mode == 'replace' and not plan.pages:
        url, body, clock = plan.response_evidence()[plan.capture_id]
        for proof in _replacement_history(connection, plan, own):
            if not proof['source_capture_id'] or not proof['source_body_hash']:
                raise RuntimeError('core replacement receipt lacks capture evidence')
            same = proof['source_capture_id'] == plan.capture_id
            if same and (proof['source_body_hash'] != body
                         or proof.get('source_url', url) != url or proof['rows'] != len(plan.rows)):
                raise RuntimeError('core replay differs from committed capture: ' + plan.table)
            _check_replacement_order(plan, clock, acknowledged_at, proof['captured_at'], proof['committed_at'],
                                     same=same)
    if clocks and max(clocks) > _timestamp(plan.captured_at):
        raise StaleTransfermarktWrite('newer capture already committed to ' + plan.table)
    if not clocks and not accepted.walks:
        return
    for walk in accepted.walks.values():
        previous = [(row[5], row[6], row[7], row[8]) for row in walk]
        _check_replacement_order(plan, plan.captured_at, acknowledged_at, max(row[7] for row in walk),
                                 walk[0][-1], same=previous == [page[:4] for page in plan.pages])
    if plan.write_mode == 'replace' and (plan.rows or plan.pages):
        _guard_source_pages(connection, plan, accepted.pages, acknowledged_at)


def _scope_json(plan: NativeWritePlan) -> str:
    return write_journal._canonical(
        list(zip(plan.scope_columns, plan.scope_values))).decode()


class _AcceptedPages(NamedTuple):
    """Newest response per numeric page, capture identities and original walks/rules by receipt."""

    pages: dict
    captures: dict
    walks: dict
    versions: dict


_NO_PAGES = _AcceptedPages({}, {}, {}, {})


def _page_cell(column: str, value):
    if column in _PAGES_BIGINT_COLUMNS:
        return int(value)
    if column in _PAGES_TIMESTAMPS:
        return _timestamp(value).isoformat(timespec='microseconds')
    return str(value)


def _walk_cells(row) -> tuple:
    return tuple(
        _page_cell(column, value) for column, value in zip(PAGES_COLUMNS, row, strict=True))


def _scope_receipt_ids(connection, plan: NativeWritePlan) -> set[str]:
    cursor = connection.cursor()
    try:
        receipts = _query_rows(
            cursor,
            f'SELECT receipt_id FROM {CORE_RECEIPTS_TABLE} '
            'WHERE target_table = ? AND scope_values_json = ?',
            (plan.full_table, _scope_json(plan)))
    except Exception as exc:
        if not write_journal._table_not_found(exc):
            raise
        return set()
    finally:
        cursor.close()
    return {row[0] for row in receipts}


def _accepted_pages(connection, plan: NativeWritePlan, *, own=(), restore=None) -> _AcceptedPages:
    """Read the scope's complete walks. Only a commit may supply a repair callback."""
    if not plan._multi_page():
        return _NO_PAGES
    cursor = connection.cursor()
    try:
        rows = _query_rows(
            cursor,
            f'SELECT {", ".join(PAGES_COLUMNS)} FROM {CORE_PAGES_TABLE} '
            'WHERE target_table = ? AND scope_values_json = ?',
            (plan.full_table, _scope_json(plan)))
    except Exception as exc:
        if not write_journal._table_not_found(exc):
            raise
        rows = []
    finally:
        cursor.close()
    walks = {}
    try:
        for row in rows:
            cells = _walk_cells(row)
            walks.setdefault(cells[0], []).append(cells)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError('core page manifest differs: ' + plan.table) from exc
    pages, captures, current_hashes, rules = {}, {}, {}, {}
    journal, missing = write_journal.scope_walks(plan.full_table, _scope_json(plan))
    cursor = connection.cursor()
    try:
        clocks, named = {}, {}
        for key, clock, capture, body, captured in _query_rows(
                cursor, f'SELECT receipt_id, committed_at, source_capture_id, source_body_hash, captured_at '
                f'FROM {CORE_RECEIPTS_TABLE} '
                'WHERE target_table = ? AND scope_values_json = ?', (plan.full_table, _scope_json(plan))):
            if key in clocks:
                raise RuntimeError('core page manifest receipt differs: ' + plan.table)
            clocks[key] = _page_cell('acknowledged_at', clock)
            named[key] = (capture, _page_cell('captured_at', captured), body)
    except Exception as exc:
        if not write_journal._table_not_found(exc):
            raise
    finally:
        cursor.close()
    ids = walks.keys() | clocks.keys() | journal.keys()
    for key, (scope, _) in missing.items():
        if key in ids:
            continue
        if scope is None:
            held = _stored_receipts(connection, [key]).get(key)
            if held is not None and held[RECEIPT_COLUMNS.index('target_table')] == plan.full_table:
                scope = held[RECEIPT_COLUMNS.index('scope_values_json')]
            else:
                held_walk = _stored_walk(connection, key)
                if (not held_walk or held_walk[0][1] != plan.full_table
                        or any(row[1:3] != held_walk[0][1:3] for row in held_walk)):
                    raise RuntimeError('core page manifest lost: ' + plan.table)
                scope = held_walk[0][2]
        if not _nonempty(scope):
            raise RuntimeError('core journal walk lacks scope evidence')
        if scope == _scope_json(plan):
            ids.add(key)
    for receipt_id in sorted(ids):
        walk = walks.get(receipt_id, ())
        saved = journal.get(receipt_id)
        original = _journal_walk_rows(plan, receipt_id, saved) if saved else None
        expected_clocks = set()
        if saved or receipt_id in missing:
            local_clock = saved[1] if saved else missing[receipt_id][1]
            expected_clocks.add(_page_cell('acknowledged_at', local_clock))
        if receipt_id in clocks:
            expected_clocks.add(clocks[receipt_id])
        if (len(expected_clocks) > 1
                or (walk and not expected_clocks and receipt_id not in own)
                or (expected_clocks and any(row[-1] not in expected_clocks for row in walk))):
            raise RuntimeError('core page manifest acknowledgement clock differs: ' + plan.table)
        if original:
            _held_page_numbers(walk, tuple(map(_walk_cells, original)), plan.table)
        numbers = sorted(cells[3] for cells in walk)
        complete = (bool(walk) and numbers == list(range(1, len(walk) + 1))
                    and all(cells[4] == len(walk) and cells[-1] == walk[0][-1] for cells in walk))
        if not complete:
            if receipt_id in missing and not original:
                raise RuntimeError('core page manifest lacks original cells: ' + plan.table)
            if restore is None:
                if receipt_id in own:
                    walks.pop(receipt_id, None)
                    continue  # the retried member may still supply this walk
                reason = 'differs' if walk else 'lost'
                raise RuntimeError(f'core page manifest {reason}: ' + plan.table)
            walk = restore(plan, receipt_id, walk, original)
            if not walk:
                raise RuntimeError('core page manifest lost: ' + plan.table)
        if receipt_id in named:
            if sum(row[6:9] == named[receipt_id] for row in walk) != 1:
                raise RuntimeError('core page manifest named capture differs: ' + plan.table)
        elif not original and receipt_id not in own:
            raise RuntimeError('core page manifest lacks original capture evidence: ' + plan.table)
        walks[receipt_id] = tuple(sorted(walk, key=lambda cells: cells[3]))
        if not original and len(walk) > 1:
            # The receipt names just one response. The other clocks must
            # match the independently bound descriptor or immutable walk.
            member = own.get(receipt_id) if isinstance(own, dict) else None
            if member is None:
                raise StaleTransfermarktWrite('core page manifest lacks independent page clocks: ' + plan.table)
            ordered = walks[receipt_id]
            _expected_walk(member, receipt_id, ordered[0][-1], held=ordered)
        if complete and receipt_id not in own and original:
            _settle_statement(connection, (CORE_PAGES_TABLE, receipt_id), _digest(walks[receipt_id]),
                              lambda: True, release=restore is not None, independent_postimage=True)
        versions = rules[receipt_id] = _walk_versions(saved[0]) if saved else (8, 7, 6)
        for cells in walk:
            url, capture_id, clock, body_hash, count, digest, acknowledged_at = cells[5:]
            page = _source_page_key(plan, url)
            if page[-1] != cells[3]:
                raise RuntimeError('core page manifest differs: ' + plan.table)
            response = (url, capture_id, clock, body_hash, count)
            if captures.setdefault(capture_id, response) != response:
                raise RuntimeError('core page manifest differs: ' + plan.table)
            if versions == (8,) and current_hashes.setdefault(capture_id, digest) != digest:
                raise RuntimeError('core page manifest differs: ' + plan.table)
            order = (clock, acknowledged_at)
            newest = pages.get(page)
            if newest and order == newest[0] and response != newest[1]:
                raise StaleTransfermarktWrite('conflicting page acknowledgement in ' + plan.table)
            if newest is None or order > newest[0]:
                pages[page] = (order, response)
    return _AcceptedPages(pages, captures, walks, rules)


def _refuse_committed_rows(connection, plan: NativeWritePlan) -> None:
    """Refuse an empty plan whose capture the scope holds with rows."""
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
        raise RuntimeError('core replay differs from committed capture: ' + plan.table)


def _acknowledged_capture(connection, plan: NativeWritePlan) -> list[tuple]:
    """How the scope acknowledged this plan's capture: (rows, body hash, clock) per receipt."""
    cursor = connection.cursor()
    try:
        rows = _query_rows(
            cursor,
            f'SELECT row_count, source_body_hash, captured_at, observation_status, status_reason, '
            f'status_proof FROM {CORE_RECEIPTS_TABLE} '
            'WHERE target_table = ? AND scope_values_json = ? AND source_capture_id = ?',
            (plan.full_table, _scope_json(plan), plan.capture_id))
    except Exception as exc:
        if write_journal._table_not_found(exc):
            return []
        raise
    finally:
        cursor.close()
    if not plan.rows and plan.entity in _OBSERVATION_CLOCK_ENTITIES and any(
            count == 0 and (status, reason, proof) != (plan.status, plan.status_reason, plan.proof)
            for count, _, _, status, reason, proof in rows):
        raise RuntimeError('core replay differs from committed capture: ' + plan.table)
    return [(row[0], row[1], _timestamp(row[2])) for row in rows]


def _refuse_capture_contradiction(connection, plan: NativeWritePlan, *, empty: bool) -> bool:
    """Refuse a plan that contradicts how its capture was acknowledged.

    A capture id names one response body. Its observation clock can change on
    replay; a different body is a contradiction. A capture acknowledged with
    rows never comes back without them, and one acknowledged without rows
    never acquires them. Returns whether it was acknowledged without rows.
    """
    acknowledged = _acknowledged_capture(connection, plan)
    if plan.rows:
        own = plan.rows[0]['source_body_hash']
    else:
        own = dict(plan.evidence).get('source_body_hash')
    if len({body for _rows, body, _clock in acknowledged} | {own}) > 1:
        raise RuntimeError('core replay differs from committed capture: ' + plan.table)
    counts = {rows for rows, _body, _clock in acknowledged}
    if (empty and any(count > 0 for count in counts)) \
            or (not empty and any(count == 0 for count in counts)):
        raise RuntimeError('core replay differs from committed capture: ' + plan.table)
    return counts == {0}


def _capture_bindings(connection, plans):
    """Bind accumulating captures across scopes, including pre-upgrade in-flight targets."""
    bindings = {}
    for plan in plans:
        if plan.entity not in _OBSERVATION_CLOCK_ENTITIES:
            continue
        key = (plan.full_table, plan.capture_id)
        body = plan.response_evidence()[plan.capture_id][1]
        if key in bindings:
            continue  # validate_run_plans already checked the body across these plans
        bindings[key] = body
        path = write_journal._record_path('capture-bodies', _digest(key))
        record = {'table': key[0], 'capture': key[1], 'body': body}
        if path.exists() and write_journal._read_record(path) != record:
            raise RuntimeError('core replay differs from committed capture: ' + plan.table)
        cursor = connection.cursor()
        try:
            rows = _query_rows(cursor, f'SELECT DISTINCT source_body_hash FROM {CORE_RECEIPTS_TABLE} '
                               'WHERE target_table = ? AND source_capture_id = ?', key)
        finally:
            cursor.close()
        if any(held != body for held, in rows):
            raise RuntimeError('core replay differs from committed capture: ' + plan.table)
    if bindings:
        for _, pending in write_journal.pending_intents({}):
            for index, descriptor in enumerate(pending['payload']['plans']):
                key = ('iceberg.bronze.' + descriptor['table'], descriptor['capture_id'])
                if key not in bindings:
                    continue
                source = next(iter(descriptor['rows']), dict(descriptor['evidence']))
                if bindings[key] == source['source_body_hash']:
                    continue
                scope = (key[0], write_journal._canonical(list(zip(
                    descriptor['scope_columns'], descriptor['scope_values'], strict=True))).decode())
                identity = pending['payload'].get('warehouse_identity') or pending['identity']
                _, hashes, _ = _intent_unit(pending)
                ids = [_receipt_id(h, pending['identity']['run_id'], identity['digest'])
                       for h in identity.get('plan_hashes', hashes)]
                statement = write_journal.pending_statement(scope)
                if ((statement and _statement_expected(statement) == ids[index])
                        or write_journal.scope_owner(scope) == ids[0]
                        or (not pending['identity'].get('scope_protocol')
                            and write_journal.read_snapshot_anchors(identity['digest']))):
                    raise RuntimeError('core replay differs from committed capture: ' + key[0])
    return bindings


def _receipt_id(plan_hash: str, run_id: str, digest: str) -> str:
    return 'tm-core-receipt-' + sha256(
        (plan_hash + run_id + digest).encode('utf-8')).hexdigest()[:32]


# A re-observation of the same capture regenerates run identity only: the run
# that observed it, when it was extracted, and the cycle it belonged to.
_LINEAGE_ONLY_COLUMNS = frozenset({'run_id', '_batch_id', '_ingested_at', 'extracted_at', 'cycle_id'})
_REPLAY_LINEAGE_COLUMNS = _LINEAGE_ONLY_COLUMNS | {'captured_at', 'observed_at'}


def _strip_run_lineage(rows, ignored=_REPLAY_LINEAGE_COLUMNS) -> list[dict]:
    return [
        {column: value for column, value in row.items()
         if column not in ignored} for row in rows]


def _page_rows(plan: NativeWritePlan, capture_id: str) -> list:
    """The business rows one page of the snapshot contributed."""
    return [row for row in plan.rows if row['source_capture_id'] == capture_id]


# r6 included cycle identity. r7 excluded it. r8 excludes all observation
# lineage and binds the exact response even for a page with no business rows.
_PAGE_LINEAGE = {
    6: _LINEAGE_ONLY_COLUMNS - {'cycle_id'},
    7: _LINEAGE_ONLY_COLUMNS,
    8: _LINEAGE_ONLY_COLUMNS | {'scope_id', 'universe_version'},
}


def _page_digest(plan, page, rows, version=8, lineage=None):
    ordered = _sorted_rows(rows, plan.natural_keys)
    if version == 8:
        from scrapers.transfermarkt import core

        columns = (*core._BUSINESS_COLUMNS[plan.entity], 'observation_status', 'status_reason')
        facts = [{column: row[column] for column in columns} for row in ordered]
        return _digest({'response': page, 'rows': facts})
    ignored = _PAGE_LINEAGE[version]
    lineage = lineage or {}
    facts = [{col: lineage.get(col, value) for col, value in row.items() if col not in ignored}
             for row in ordered]
    return _digest(facts)


def _matches_page_digest(connection, plan, page, rows, digest, version, receipt_id, persisted):
    if digest == _page_digest(plan, page, rows, version):
        return True
    if version == 8:
        return False
    # A cached page may be observed under a different scope, cycle and
    # universe. Surviving physical rows or the original receipt can supply
    # lineage. Only a match to the prior digest authenticates the result.
    def matches_with_lineage(source):
        lineage = {key: source[key] for key in ('scope_id', 'cycle_id', 'universe_version')}
        return digest == _page_digest(plan, page, rows, version, lineage)

    if persisted and matches_with_lineage(persisted[0]):
        return True
    held = _stored_receipts(connection, [receipt_id]).get(receipt_id)
    if held is None:
        return False
    cells = dict(zip(RECEIPT_COLUMNS, held, strict=True))
    if cells['target_table'] != plan.full_table or cells['scope_values_json'] != _scope_json(plan):
        return False
    return matches_with_lineage(cells)


def _walk_rows(plan: NativeWritePlan, receipt_id: str, committed_at: str, *, held=None) -> tuple:
    """The pages of the plan as its acknowledgement logs them, in walk order.

    Every row states how many pages the walk has, and the page is logged
    with the business rows it contributed, by count and digest: a page that
    returns under its acknowledged capture is held to exactly these, whether
    or not a shorter walk has since replaced them.
    """
    acknowledged_at = _timestamp(committed_at).replace(tzinfo=None)
    saved = write_journal.acknowledged_walk(receipt_id)
    original = _journal_walk_rows(plan, receipt_id, saved) if saved else None
    versions = _walk_versions(saved[0]) if saved else (8,)
    if not saved and held:
        _, missing = write_journal.scope_walks(plan.full_table, _scope_json(plan))
        if receipt_id in missing:
            versions = (8, 7, 6)
    if saved and tuple(tuple(p[:5]) for p in saved[0]['pages']) != plan.pages:
        raise RuntimeError('core page manifest differs: ' + plan.table)
    # One rule must reproduce the entire walk, including every original cell.
    for version in versions:
        rows = []
        for number, page in enumerate(plan.pages, start=1):
            url, capture_id, clock, body_hash, _next = page
            page_rows = _page_rows(plan, capture_id)
            rows.append((
                receipt_id, plan.full_table, _scope_json(plan), number, len(plan.pages), url, capture_id,
                _timestamp(clock).replace(tzinfo=None), body_hash,
                len(page_rows), _page_digest(plan, page, page_rows, version), acknowledged_at))
        if original is not None and tuple(rows) != original:
            continue
        if not held or tuple(map(_walk_cells, rows)) == held:
            return tuple(rows)
    raise RuntimeError('core page manifest differs: ' + plan.table)


def _expected_walk(plan: NativeWritePlan, receipt_id: str, committed_at: str, *, held=None) -> tuple:
    """The walk as it reads back from the warehouse, cell by cell."""
    return tuple(_walk_cells(row) for row in _walk_rows(plan, receipt_id, committed_at, held=held))


def _stored_walk(connection, receipt_id: str):
    """The page rows the warehouse holds under one receipt, in walk order.

    ``None`` when it holds none. Anything but the whole walk reads back as it
    was logged is refused by the caller: a walk lands with one statement.
    """
    cursor = connection.cursor()
    try:
        rows = _query_rows(
            cursor,
            f'SELECT {", ".join(PAGES_COLUMNS)} FROM {CORE_PAGES_TABLE} WHERE receipt_id = ?',
            (receipt_id,))
    except Exception as exc:
        if not write_journal._table_not_found(exc):
            raise
        return None
    finally:
        cursor.close()
    if not rows:
        return None
    try:
        walk = [_walk_cells(row) for row in rows]
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError('core page manifest differs') from exc
    return tuple(sorted(walk, key=lambda cells: cells[3]))


def _record_pages(connection, plan: NativeWritePlan, receipt_id: str, committed_at: str, repair=False) -> None:
    """Log the walk the acknowledgement stands for, once and as a whole."""
    stored = _stored_walk(connection, receipt_id)
    complete = stored if stored and len(stored) == len(plan.pages) else None
    rows = _walk_rows(plan, receipt_id, committed_at, held=complete)
    _log_walk(connection, plan.table, receipt_id, rows, stored=stored, repair=repair)


def _held_page_numbers(stored, expected, table):
    by_number = {cells[3]: cells for cells in expected}
    kept = {cells[3] for cells in stored}
    if len(kept) != len(stored) or any(by_number.get(cells[3]) != cells for cells in stored):
        raise RuntimeError('core page manifest differs: ' + table)
    return kept


_UNREAD_WALK = object()


def _log_walk(connection, table: str, receipt_id: str, rows, *, stored=_UNREAD_WALK, repair=False) -> None:
    """Log the pages of a walk its receipt still lacks.

    One statement carries every page the receipt lacks: they land whole or
    not at all, and nothing is ever updated. An interrupted acknowledgement
    leaves the evidence of every page exactly as the earlier ones accepted
    it. Pages the receipt already holds must read back exactly as intended,
    and are left as they are.
    """
    expected = tuple(_walk_cells(row) for row in rows)
    key = (CORE_PAGES_TABLE, receipt_id)
    if stored is _UNREAD_WALK:
        stored = _stored_walk(connection, receipt_id)
    if stored is not None:
        kept = _held_page_numbers(stored, expected, table)
        if len(kept) == len(expected):
            _settle_statement(connection, key, _digest(expected), lambda: stored == expected,
                              independent_postimage=repair)
            return
        rows = tuple(row for row, cells in zip(rows, expected, strict=True) if cells[3] not in kept)
    names = ', '.join(PAGES_COLUMNS)
    marks = '(' + ', '.join('?' for _ in PAGES_COLUMNS) + ')'
    sql = f'INSERT INTO {CORE_PAGES_TABLE} ({names}) VALUES ' + ', '.join([marks] * len(rows))
    _insert_once(
        connection, key, sql, tuple(value for row in rows for value in row),
        lambda: _stored_walk(connection, receipt_id), expected, repair=repair)


def _walk_versions(walk):
    version = walk.get('digest_version')
    if version is None:
        return (6, 7)
    if version != 8:
        raise RuntimeError('core page manifest digest version differs')
    return (8,)


def _journal_walk_rows(plan: NativeWritePlan, receipt_id: str, kept):
    """Recover the original bound values of a lost walk from its immutable acknowledgement."""
    walk, acknowledged_at = kept
    if walk.get('table') != plan.full_table or walk.get('scope_values_json') != _scope_json(plan) \
            or not isinstance(walk.get('pages'), list) or not walk['pages']:
        raise RuntimeError('core page manifest differs: ' + plan.table)
    pages = walk['pages']
    rows, captures = [], set()
    for number, page in enumerate(pages, start=1):
        url, capture_id, clock, body_hash, following, count, digest = page
        if (not capture_id or capture_id in captures or type(count) is not int or count < 0
                or not fullmatch('[0-9a-f]{64}', body_hash) or not fullmatch('[0-9a-f]{64}', digest)
                or following != (pages[number][0] if number < len(pages) else '')):
            raise RuntimeError('core page manifest differs: ' + plan.table)
        captures.add(capture_id)
        rows.append((
            receipt_id, plan.full_table, _scope_json(plan), number, len(walk['pages']), url,
            capture_id, _timestamp(clock).replace(tzinfo=None), body_hash, count, digest,
            _timestamp(acknowledged_at).replace(tzinfo=None)))
    return tuple(rows)


def _verify_pages(connection, plan: NativeWritePlan, receipt_id: str, committed_at: str, *, settle=True) -> None:
    """The logged walk reads back exactly as the acknowledgement states it."""
    held = _stored_walk(connection, receipt_id)
    expected = _expected_walk(plan, receipt_id, committed_at, held=held)
    if held != expected:
        raise RuntimeError('core page manifest differs: ' + plan.table)
    if settle:
        _settle_statement(connection, (CORE_PAGES_TABLE, receipt_id), _digest(expected), lambda: True)


def _source_page_key(plan, url):
    if not plan._multi_page():
        return url
    from scrapers.transfermarkt import core, sections

    scope = dict(zip(plan.scope_columns, plan.scope_values, strict=True))
    core._check_scope_against_capture(plan.entity, scope, SimpleNamespace(url=url, endpoint='attendance'))
    return scope['competition_id'], scope['edition_id'], sections._attendance_page_number(url)


def _guard_source_pages(connection, plan: NativeWritePlan, manifest, acknowledged_at=None) -> None:
    """Compare per-page clocks by edition and numeric page; retain the exact response URL."""
    planned = {_source_page_key(plan, url): (_timestamp(clock), capture_id, body_hash)
               for url, capture_id, clock, body_hash, _next in plan.pages}
    for row in plan.rows:
        clock, key = _timestamp(row['captured_at']), _source_page_key(plan, row['source_url'])
        if key not in planned or planned[key][0] < clock:
            planned[key] = (clock, row['source_capture_id'], row['source_body_hash'])
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
    for url, clock in committed:
        key = _source_page_key(plan, url)
        if key in planned and _timestamp(clock) > planned[key][0]:
            raise StaleTransfermarktWrite('newer capture already committed to ' + plan.table)
    for key, ((clock, logged_at), response) in manifest.items():
        if key in planned:
            _check_replacement_order(plan, planned[key][0], acknowledged_at, clock, logged_at,
                                     same=planned[key][1:] == (response[1], response[3]))


def _entity_registry(entity: str) -> dict:
    """The registered contract of one core entity.

    ``core`` loads this module lazily when it plans an outcome, and a plan
    loads ``core`` lazily when it is validated: neither imports the other at
    module load.
    """
    from scrapers.transfermarkt import core

    if entity not in core.CORE_ENTITIES:
        raise ValueError('invalid core entity: ' + str(entity))
    return {'table': core.CORE_TABLES[entity], 'write_mode': core.CORE_WRITE_MODES[entity],
            'natural_keys': core.CORE_NATURAL_KEYS[entity],
            'scope_columns': core.CORE_SCOPE_COLUMNS[entity], 'ddl': core.core_ddl(entity)}


def _value_has_type(kind: str, value) -> bool:
    """Whether a row value has the Python form of its declared SQL column.

    A plan carries text, integers, booleans or null. The engine would store
    something else than the plan states for any other combination: text in
    a numeric column is cast on the way in and reads back as a number.
    """
    if kind.startswith(('bigint', 'integer', 'smallint', 'tinyint')):
        return type(value) is int
    if kind.startswith('boolean'):
        return type(value) is bool
    if kind.startswith(('double', 'real', 'decimal', 'float')):
        # Exact decimal text is kept in a varchar column instead.
        return False
    if kind.startswith('date'):
        # The engine casts the text to a calendar date: it has to be one.
        if not isinstance(value, str):
            return False
        try:
            return date.fromisoformat(value).isoformat() == value
        except ValueError:
            return False
    return isinstance(value, str)


def _canonical_clock(value) -> str | None:
    """The one spelling of a moment: UTC ISO text with microseconds.

    None for anything a plan may not carry as a clock: not text, or text
    that is not a timestamp. The caller's own comparison refuses it.
    """
    if not isinstance(value, str):
        return None
    try:
        return _timestamp(value).isoformat(timespec='microseconds')
    except ValueError:
        return None


def _key_cells(values, columns=()) -> tuple:
    cells = []
    for index, value in enumerate(values):
        # An engine returns a clock as a datetime, a plan holds it as text;
        # both are compared as the moment they name, never as spelling.
        if index < len(columns) and columns[index].endswith('_at') and isinstance(value, str):
            value = _canonical_clock(value) or value
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


def _read_plan_rows(connection, plan, *, capture_only=False) -> list[dict]:
    cursor = connection.cursor()
    predicate, params = ('source_capture_id = ?', (plan.capture_id,)) if capture_only else plan.readback_sql()
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


def _rows_equal_ignoring_lineage(left, right, ignored=_REPLAY_LINEAGE_COLUMNS) -> bool:
    return _rows_equal(_strip_run_lineage(left, ignored), _strip_run_lineage(right, ignored))


def _plan_key(plan: NativeWritePlan, row: Mapping) -> tuple:
    return tuple(str(row[column]) for column in plan.natural_keys)


def _owned_rows(connection, plan):
    if plan.write_mode == 'replace':
        return _read_plan_rows(connection, plan)
    # Every surviving row identifies the capture body before ownership filters.
    # Merge readback then compares the incoming natural-key observations, not
    # earlier observations of the same response at other clocks or pages.
    rows = _read_plan_rows(connection, plan, capture_only=True)
    _, body, _clock = plan.response_evidence()[plan.capture_id]
    if any(row['source_body_hash'] != body for row in rows):
        raise RuntimeError('core replay differs from committed capture: ' + plan.table)
    scoped = [row for row in rows if tuple(row[col] for col in plan.scope_columns) == plan.scope_values]
    if plan.entity not in _OBSERVATION_CLOCK_ENTITIES:
        return scoped
    if plan.entity not in _PAGE_OWNED_ROWS_ENTITIES:
        if any(_timestamp(row['observed_at']) != _timestamp(row['captured_at']) for row in rows):
            raise RuntimeError('core replay differs from committed capture: observation clock ' + plan.table)
        if not plan.rows:
            return scoped
        return [row for row in scoped if _timestamp(row['captured_at']) == _timestamp(plan.captured_at)]
    if not plan.rows:
        return scoped
    keys = {_plan_key(plan, row) for row in plan.rows}
    return [row for row in scoped if _plan_key(plan, row) in keys]


def _strictly_additive(connection, plan, before):
    if len(before) >= len(plan.rows):
        return False
    columns = tuple(plan.rows[0])
    existing = Counter(_comparison_row(row, columns) for row in before)
    planned = Counter(_comparison_row(row, columns) for row in plan.rows)
    if not existing <= planned:
        return False
    keys = plan.natural_keys
    if plan.write_mode == 'replace' or {*plan.scope_columns, 'source_capture_id'} <= set(keys):
        return True
    # A keyed MERGE can match a row outside the capture readback. Check the
    # affected keys globally before treating an apparent insert as additive.
    # Bind a varchar prefix and compare the full typed key in Python.
    types = _ddl_columns(plan.ddl)
    prefix = next((col for col in keys if types[col] == 'varchar'), None)
    if prefix is None:
        return False
    def key(row):
        return _key_cells((row[col] for col in keys), keys)

    incoming = {key(row) for row in plan.rows}
    found = Counter()
    values = sorted({row[prefix] for row in plan.rows})
    for i in range(0, len(values), 100):
        batch = values[i:i + 100]
        marks = ', '.join('?' for _ in batch)
        cursor = connection.cursor()
        try:
            rows = _query_rows(cursor, f'SELECT {", ".join(keys)} FROM {plan.full_table} '
                               f'WHERE {prefix} IN ({marks})', tuple(batch))
        finally:
            cursor.close()
        found.update(k for row in rows if (k := _key_cells(row, keys)) in incoming)
    return found == Counter(key(row) for row in before)


def _by_capture(rows) -> dict:
    grouped = {}
    for row in rows:
        grouped.setdefault(row['source_capture_id'], []).append(row)
    return grouped


def _check_walk_digests(connection, plan, accepted, planned, committed, restoring, remember):
    incoming = {page[1]: page for page in plan.pages}
    for key, walk in accepted.walks.items():
        candidates = accepted.versions[key]
        historical = len(candidates) > 1
        witness = write_journal._record_path('verified-walk-rules', key)
        certified = historical and witness.exists()
        if certified:
            proof = write_journal._read_record(witness)
            if proof['walk'] != _digest(walk) or proof['version'] not in candidates:
                raise RuntimeError('core page manifest differs: ' + plan.table)
            candidates = (proof['version'],)
        needs_rule = key in restoring or any(cell[6] in incoming for cell in walk)
        complete = True
        for index, cell in enumerate(walk):
            url, capture, clock, body, count, digest = cell[5:11]
            persisted = committed.get(capture, ())
            if capture in incoming:
                page, rows = incoming[capture], planned.get(capture, ())
                if accepted.captures[capture] != (page[0], capture, _page_cell('captured_at', page[2]),
                                                  page[3], len(rows)):
                    raise RuntimeError('core replay differs from committed capture: ' + plan.table)
            elif historical and not certified and len(persisted) == count:
                following = walk[index + 1][5] if index + 1 < len(walk) else ''
                page = (url, capture, clock, body, following)
                # The physical proof canonicalizes booleans to integers. The
                # historical attendance digest used the original JSON boolean.
                rows = [{**r, 'final_season': bool(r['final_season'])} if r['final_season'] is not None
                        else r for r in persisted]
            else:
                if needs_rule and historical and not certified:
                    raise RuntimeError('core page manifest lacks original row evidence: ' + plan.table)
                complete = False
                continue
            candidates = tuple(v for v in candidates if _matches_page_digest(
                connection, plan, page, rows, digest, v, key, persisted))
            if not candidates:
                raise RuntimeError('core replay differs from committed capture: page manifest ' + plan.table)
        if historical and complete and remember and not certified:
            write_journal._immutable_record(witness, {'walk': _digest(walk), 'version': candidates[0]})


def _replay_state(connection, plan: NativeWritePlan, receipt_id: str = '',
                  committed_at: str = '', own=None, accepted=None, restoring=(), remember_rules=False) -> str:
    """Classify a plan against what the scope already holds.

    ``written`` needs nothing, ``refresh`` is the same capture observed under
    a new run (only lineage differs), ``pending`` still has to be written. A
    capture that is committed with other business rows is never rewritten.

    A snapshot is ``written`` only under the acknowledgement that names it:
    its walk has to be logged under ``receipt_id``, with ``committed_at``.
    """
    if plan.write_mode == 'replace':
        # A replacement is classified capture by capture against everything
        # the scope accepted: its rows, and for a snapshot its logged walks.
        # A page without rows is a capture with an empty row set: committed
        # with rows it is never replayed as empty, accepted without rows it
        # never acquires rows, and it never changes its evidence.
        existing = _read_plan_rows(connection, plan)
        committed, planned = _by_capture(existing), _by_capture(plan.rows)
        if accepted is None:
            accepted = _accepted_pages(connection, plan, own={receipt_id: plan} if own is None else own)
        _check_walk_digests(connection, plan, accepted, planned, committed, restoring, remember_rules)
        for page in plan.pages:
            rows = planned.setdefault(page[1], [])
            if page[1] in accepted.captures:
                committed.setdefault(page[1], rows)
        if not plan.pages:
            planned.setdefault(plan.capture_id, [])
            # Its receipt says whether the scope accepted this capture without
            # rows, and whether the capture is still the response it was.
            if _refuse_capture_contradiction(connection, plan, empty=not plan.rows):
                committed.setdefault(plan.capture_id, [])
        ignored = _PAGE_LINEAGE[8] if plan.pages else _REPLAY_LINEAGE_COLUMNS
        for capture_id, rows in planned.items():
            shared = committed.get(capture_id)
            if shared is not None and (len(shared) != len(rows)
                                       or not _rows_equal_ignoring_lineage(shared, rows, ignored)):
                raise RuntimeError('core replay differs from committed capture: ' + plan.table)
        if not plan.rows and not plan.pages:
            # One empty page proves itself only through its receipt.
            return 'pending'
        # Only captures that carry rows live in the data table; rowless ones
        # are proven by the logged walks. A snapshot is written while its
        # pages are still the scope's newest evidence and the walk it stands
        # for is logged under its own acknowledgement: a walk that a later
        # one superseded is never "written", rowless or not.
        carrying = [capture for capture, rows in planned.items() if rows]
        logged = True
        if plan.pages:
            own_walk = accepted.walks.get(receipt_id)
            if own_walk is not None:
                _expected_walk(plan, receipt_id, committed_at, held=own_walk)
            logged = own_walk is not None and all(
                accepted.pages[_source_page_key(plan, page[0])][1][:4] == page[:4]
                for page in plan.pages)
        if {capture for capture, rows in committed.items() if rows} != set(carrying) or not logged:
            return 'pending'
        refreshed = any(not _rows_equal(committed[capture_id], planned[capture_id])
                        for capture_id in carrying)
        return 'refresh' if refreshed else 'written'
    if not plan.rows:
        # An empty merge writes nothing. A capture the scope holds with rows,
        # or acknowledged with rows, is never replayed as empty: the
        # contradiction is found while the bundle is classified, before any
        # plan of it mutates.
        if plan.entity in _OBSERVATION_CLOCK_ENTITIES:
            if _owned_rows(connection, plan):
                raise RuntimeError('core replay differs from committed capture: ' + plan.table)
        else:
            _refuse_committed_rows(connection, plan)
        _refuse_capture_contradiction(connection, plan, empty=True)
        return 'pending'
    # One response per capture id, whoever's rows are already there: its
    # receipts carry the plan's own body and clock, and a capture that was
    # acknowledged without rows never acquires them.
    _refuse_capture_contradiction(connection, plan, empty=False)
    existing = _owned_rows(connection, plan)
    if not existing:
        return 'pending'
    partner, complete = plan.rows, True
    if plan.entity in _PAGE_OWNED_ROWS_ENTITIES:
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
        frame = pd.DataFrame(_plain_rows(plan.rows), columns=columns, dtype=object)
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
            with _target_dispatch() as statement:
                _execute_once(cursor, f'DELETE FROM {plan.full_table} WHERE {plan.scope_sql()}',
                              plan.scope_values, statement)
        finally:
            cursor.close()
        return
    import pandas as pd

    columns = list(dict.fromkeys(col for row in plan.rows for col in row))
    frame = pd.DataFrame(_plain_rows(plan.rows), columns=columns, dtype=object)
    # The scope is handed to the writer as its delete filter: the
    # Transfermarkt writer stages the rows and replaces the scope with one
    # statement, so a failed insertion leaves the committed rows in place.
    writer.write_dataframe(
        df=frame, database='bronze', table=plan.table, add_metadata=False,
        delete_filter=plan.scope_filter_sql(),
    )


def _physical_proof(connection, plan: NativeWritePlan, committed_at=None) -> dict:
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
    persisted = _owned_rows(connection, plan)
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
    proof = {
        'entity': plan.entity, 'table': plan.table, 'rows': len(persisted),
        'physical_hash': _digest(_sorted_rows(persisted, plan.natural_keys)),
        'expected_rows': len(plan.rows), 'status_counts': dict(status_counts),
    }
    if plan.write_mode == 'replace':
        proof.update(scope_values_json=_scope_json(plan), captured_at=plan.captured_at)
        if committed_at is not None:
            url, body, _ = plan.response_evidence()[plan.capture_id]
            proof.update(committed_at=committed_at, source_capture_id=plan.capture_id,
                         source_url=url, source_body_hash=body)
    return proof


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
    return {
        **evidence, 'receipt_id': _receipt_id(plan_hash, run_id, digest),
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


def _receipt_cells(receipt) -> tuple:
    return tuple(_receipt_cell(col, receipt[col]) for col in RECEIPT_COLUMNS)


def _stored_receipts(connection, ids) -> dict[str, tuple]:
    """The receipts the warehouse holds under these ids, cell by cell."""
    if not ids:
        return {}
    names = ', '.join(RECEIPT_COLUMNS)
    placeholders = ', '.join('?' for _ in ids)
    cursor = connection.cursor()
    try:
        rows = _query_rows(
            cursor,
            f'SELECT {names} FROM {CORE_RECEIPTS_TABLE} '
            f'WHERE receipt_id IN ({placeholders})', ids)
    except Exception as exc:
        if write_journal._table_not_found(exc):
            return {}  # nothing was ever acknowledged
        raise
    finally:
        cursor.close()
    stored = {}
    try:
        for row in rows:
            cells = tuple(_receipt_cell(col, value)
                          for col, value in zip(RECEIPT_COLUMNS, row, strict=True))
            if not cells or cells[0] in stored:
                raise RuntimeError('core receipt readback differs')
            stored[cells[0]] = cells
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError('core receipt readback differs') from exc
    return stored


def _refuse_receipt_contradictions(connection, plans, plan_hashes, digest, run_id,
                                 universe_version, batch, committed_at, *, require_rowless=False) -> None:
    """A receipt the warehouse holds under one of this bundle's ids must agree with it.

    Everything a receipt states but its physical proof is known before the
    bundle writes anything. A contradiction with an earlier acknowledgement is
    therefore found before the first mutation, not by the receipts step after
    the data has been replaced.
    """
    expected = [
        _plan_receipt(plan, _UNWRITTEN_PROOF, plan_hash, digest, run_id, universe_version,
                      batch, committed_at)
        for plan, plan_hash in zip(plans, plan_hashes, strict=True)
    ]
    stored = _stored_receipts(connection, [receipt['receipt_id'] for receipt in expected])
    for plan, receipt in zip(plans, expected, strict=True):
        earlier = stored.get(receipt['receipt_id'])
        if earlier is None:
            if require_rowless and not plan.rows:
                raise RuntimeError('core legacy receipt lacks independent capture evidence')
            continue
        cells = _receipt_cells(receipt)
        if any(earlier[index] != cells[index]
               for index, column in enumerate(RECEIPT_COLUMNS)
               if column not in _RECEIPT_PROOF_COLUMNS):
            raise RuntimeError('core receipt contradicts its earlier acknowledgement')


_CTAS_ANALYSIS_ERRORS = {'TYPE_MISMATCH', 'COLUMN_NOT_FOUND', 'TABLE_NOT_FOUND', 'COLUMN_TYPE_UNKNOWN',
                         'COLUMN_NAME_NOT_SPECIFIED', 'DUPLICATE_COLUMN_NAME', 'SYNTAX_ERROR'}


def _query_outcome(connection, statement):
    query_id = write_journal.statement_query(statement)
    if not query_id:
        return None
    cursor = connection.cursor()
    try:
        rows = _query_rows(cursor, 'SELECT state, error_code FROM system.runtime.queries '
                           'WHERE query_id = ?', (query_id,))
    finally:
        cursor.close()
    # FAILED alone can mean a lost commit response. These two coordinator
    # errors occur during admission or parsing, before any write can start.
    if len(rows) == 1:
        if rows[0][0] == 'FINISHED':
            return 'finished'
        if tuple(rows[0]) in {('FAILED', 'QUERY_QUEUE_FULL'), ('FAILED', 'SYNTAX_ERROR')}:
            return 'rejected'
        if (statement['key'][0] == 'stage-create' and rows[0][0] == 'FAILED'
                and rows[0][1] in _CTAS_ANALYSIS_ERRORS):
            return 'rejected'
    return None


def _definite_rejection(exc):
    return not isinstance(exc, TrinoAmbiguousWriteError) and _is_definite_iceberg_commit_conflict(exc)


@contextmanager
def _target_dispatch(*, direct=True, stage=None):
    """One attempt, after acquiring the target cursor and immediately before execute."""
    target = getattr(_LOCAL, 'target', None)
    if target is None:
        yield None
        return
    key, expected, additive, owner = target
    statement = write_journal.begin_statement(key, {'single_submit': expected}, observable=additive,
                                              target_preimage=True, stage=stage)
    try:
        yield statement
    except Exception as exc:
        if direct and _definite_rejection(exc):
            write_journal.finish_statement(statement)
        raise
    write_journal.hold_scope(key, owner)
    write_journal.finish_statement(statement)


def _remember_dml_query(statement, cursor, error=None):
    if statement is None:
        return
    # Each attempt has a fresh cursor, one POST and no prepare/probe/cleanup.
    # A first-response rejection is raised by process() before TrinoQuery
    # sets cursor.query_id. Only the direct coordinator error identifies it.
    query_id = getattr(cursor, 'query_id', None)
    if not query_id and isinstance(error, TrinoQueryError):
        query_id = error.query_id
    write_journal.remember_query(statement, query_id)


def _statement_expected(statement):
    # An earlier transport could submit several POSTs per attempt or save a
    # cleanup ID. Neither one result nor one rejection settles that record.
    binding = statement['expected']
    if not isinstance(binding, dict) or set(binding) != {'single_submit'}:
        raise RuntimeError('core pre-upgrade statement outcome is still pending: ' + statement['key'][0])
    return binding['single_submit']


def _settle_statement(connection, key, expected, landed, *, release=True, target=False, require_finished=False,
                      on_finished=None, independent_postimage=False):
    statement = write_journal.pending_statement(key)
    if statement is None:
        return
    binding = _statement_expected(statement)
    observable = statement['observable'] and (not target or statement.get('target_preimage') is True)
    may_observe = observable and not (require_finished or independent_postimage)
    matches = landed() if may_observe and binding == expected else None
    applied = matches is True
    if not applied:
        outcome = _query_outcome(connection, statement)
        applied = outcome == 'finished' and binding == expected and (landed() if matches is None else matches)
        if applied and on_finished is not None:
            on_finished(statement)
        if not applied and (require_finished or outcome != 'rejected'):
            raise RuntimeError('core statement outcome is still pending: ' + key[0])
    if release:
        write_journal.finish_statement(statement)
    return applied


def _execute_once(cursor, sql, bound, statement):
    if isinstance(cursor, CommittingCursor):
        cursor = cursor._cursor
    try:
        if bound is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, tuple(bound))
        _remember_dml_query(statement, cursor)
        cursor.fetchall()
    except Exception as exc:
        _remember_dml_query(statement, cursor, exc)
        raise


def _insert_once(connection, key, sql, bound, read, expected, *, repair=False) -> None:
    """Fence one physical INSERT until its previously missing cells land or Trino rejects it."""
    fingerprint = _digest(expected)
    _settle_statement(connection, key, fingerprint, lambda: read() == expected, independent_postimage=repair)
    if read() == expected:
        return
    cursor = connection.cursor()
    statement = None
    try:
        statement = write_journal.begin_statement(key, {'single_submit': fingerprint}, observable=not repair)
        _execute_once(cursor, sql, bound, statement)
    except Exception as exc:
        if statement is None:
            raise
        if _definite_rejection(exc):
            write_journal.finish_statement(statement)
            raise
        if repair or read() != expected:
            raise
    finally:
        cursor.close()
    write_journal.finish_statement(statement)


def _merge_receipts(connection, receipts, *, repair=False) -> None:
    """Acknowledge the receipts the warehouse does not hold yet.

    A receipt is immutable: one already stored under the same id must equal
    the receipt about to be acknowledged. Anything else contradicts the
    earlier acknowledgement and is refused before anything is written.
    """
    stored = _stored_receipts(connection, [receipt['receipt_id'] for receipt in receipts])
    names = ', '.join(RECEIPT_COLUMNS)
    marks = ', '.join('?' for _ in RECEIPT_COLUMNS)
    for receipt in receipts:
        expected = _receipt_cells(receipt)
        key = (CORE_RECEIPTS_TABLE, expected[0])
        earlier = stored.get(expected[0])
        if earlier is not None:
            if earlier != expected:
                raise RuntimeError('core receipt contradicts its earlier acknowledgement')
            _settle_statement(connection, key, _digest(expected), lambda: True, independent_postimage=repair)
            continue
        bound = []
        for col in RECEIPT_COLUMNS:
            value = _receipt_cell(col, receipt[col])
            if col in _RECEIPT_TIMESTAMPS:
                # Trino timestamp(6) stores UTC wall time without an offset.
                value = _timestamp(value).replace(tzinfo=None)
            bound.append(value)
        _insert_once(
            connection, key, f'INSERT INTO {CORE_RECEIPTS_TABLE} ({names}) VALUES ({marks})',
            bound, lambda: _stored_receipts(connection, [expected[0]]).get(expected[0]), expected, repair=repair)


def _verify_receipts(connection, receipts) -> None:
    """Read back every receipt of the bundle exactly as acknowledged."""
    expected = {receipt['receipt_id']: _receipt_cells(receipt) for receipt in receipts}
    stored = _stored_receipts(connection, list(expected))
    for receipt_id, cells in expected.items():
        if stored.get(receipt_id) != cells:
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
    identities = set()
    owners = {}
    captures = {}
    for plan in plans:
        # The same plan twice is one acknowledgement asked for twice, a
        # rowless one included: it has no key and no scope to compare.
        identity = plan.plan_sha256()
        if identity in identities:
            raise ValueError('duplicate core receipt identity across plans')
        identities.add(identity)
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
        # One capture of one scope is owned by one plan, and a capture id
        # names one response across the whole run: plans that are valid on
        # their own may still contradict each other about a capture.
        owner = (plan.table, plan.scope_values, plan.capture_id)
        evidence = None if plan.rows else (plan.status, plan.status_reason, plan.proof)
        clocks, previous = owners.setdefault(owner, (set(), evidence))
        if clocks and (plan.entity not in _OBSERVATION_CLOCK_ENTITIES
                       or previous != evidence or plan.captured_at in clocks):
            raise ValueError('duplicate core capture across plans')
        clocks.add(plan.captured_at)
        for capture_id, (_url, body, _clock) in plan.response_evidence().items():
            if captures.setdefault(capture_id, body) != body:
                raise ValueError('duplicate core capture evidence across plans')


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


def _descriptors(plans) -> list[dict]:
    """The plans as the journal stores them: rows sorted by natural key."""
    descriptors = [asdict(plan) for plan in plans]
    for descriptor, plan in zip(descriptors, plans):
        descriptor['rows'] = _sorted_rows(plan.rows, plan.natural_keys)
    return descriptors


def _target_matches(connection, plan):
    return _rows_equal(_owned_rows(connection, plan), plan.rows)


def _target_scope(plan):
    return (plan.full_table, _scope_json(plan))


class _ScopePending(StaleTransfermarktWrite):
    def __init__(self, message, owner):
        super().__init__(message)
        self.owner = owner


def _check_scope_owners(plans, bundle_owner):
    scopes = {_target_scope(plan) for plan in plans}
    requested = {(plan.table, plan.scope_values) for plan in plans}
    reservations = {r['owner']: {tuple(s) for s in r['scopes']} for r in write_journal.bundle_reservations()}
    for owner, reserved in reservations.items():
        if owner != bundle_owner and not scopes.isdisjoint(reserved):
            raise _ScopePending('core bundle scope acknowledgement is still pending', owner)
    # Older writers materialized the members separately. One bound member
    # proves the entire pending descriptor crossed the reservation boundary.
    for path, body in write_journal.pending_intents({}):
        descriptors = body['payload']['plans']
        if len(descriptors) < 2 or body['payload'].get('receipt_only') or write_journal.completed_intent(path.stem):
            continue
        if not any((d['table'], tuple(d['scope_values'])) in requested for d in descriptors):
            continue
        members, hashes, digest = _intent_unit(body)
        warehouse = body['payload'].get('warehouse_identity') or {}
        keys = [_receipt_id(h, body['identity']['run_id'], warehouse.get('digest', digest))
                for h in warehouse.get('plan_hashes', hashes)]
        member_scopes = [_target_scope(member) for member in members]
        if len(set(member_scopes)) < 2:
            continue
        if _released_after_ack(body, members, keys):
            if not _intent_terminal(path, body['identity'], members, keys):
                # Old DML uncertainty survives the release of packet ownership.
                by_table = {}
                for member in members:
                    if member.write_mode == 'replace':
                        by_table.setdefault(member.table, set()).add(_scope_json(member))
                for table, guarded in by_table.items():
                    write_journal._immutable_record(write_journal._record_path(
                        'legacy-scopes/' + table, path.stem), {
                            'table': table, 'digest': digest, 'warehouse': digest, 'scopes': sorted(guarded)})
            continue  # the remaining individual owners still exclude competitors
        if any(write_journal.scope_owner(scope) == keys[0]
               or (target := write_journal.pending_statement(scope))
               and _statement_expected(target) == key
               for scope, key in zip(member_scopes, keys, strict=True)):
            if (any(owner != keys[0] and not reserved.isdisjoint(member_scopes)
                    for owner, reserved in reservations.items())
                    or any(write_journal.scope_owner(scope) not in (None, keys[0]) for scope in member_scopes)):
                raise _ScopePending('competing packet scope acknowledgement is still pending', keys[0])
            write_journal.reserve_scopes(member_scopes, keys[0])
            if keys[0] != bundle_owner:
                raise _ScopePending('core packet scope acknowledgement is still pending', keys[0])
    for plan in plans:
        owner = write_journal.scope_owner(_target_scope(plan))
        if owner is not None and owner != bundle_owner:
            raise _ScopePending('core scope acknowledgement is still pending: ' + plan.table, owner)


def _released_after_ack(body, plans, receipt_ids):
    """An old releaser could stop after any unlink, including the last one."""
    identity = body['identity']
    if (not identity.get('single_submit') or identity.get('packet_attempt')
            or body['payload'].get('warehouse_identity') or len(plans) < 2):
        return False
    owner = receipt_ids[0]
    scopes = {_target_scope(p) for p in plans}
    held = {scope for scope in scopes if write_journal.scope_owner(scope) == owner}
    if held == scopes or write_journal._record_path('reserving-bundles', owner).exists():
        return False
    hashes, descriptor = _bundle_identity(plans, identity['run_id'])
    ack = write_journal.read_receipt(identity['digest'])
    if (not ack or ack.get('receipt_readback') is not True or ack.get('descriptor_digest', descriptor) != descriptor
            or ack.get('identity') != {'digest': descriptor, 'plan_hashes': hashes}
            or ack.get('run_id') != identity['run_id'] or ack['plan_sha256'] != identity['digest']
            or ack.get('committed_at') != body['evidence'].get('committed_at')
            or ack.get('universe_version') != body['evidence'].get('universe_version')
            or ack.get('committed_batch') != _batch_id_for_run(identity['run_id'])
            or len(ack.get('plans', ())) != len(plans)
            or not write_journal.acknowledgement_matches_inventory(identity['digest'], ack)
            or write_journal.read_snapshot_anchors(descriptor).keys() != _anchored_tables(plans)):
        return False
    # Unlinking an owner is post-ack evidence only after every bound
    # submission has settled. A foreign statement on a released member is OK.
    for plan, key, proof in zip(plans, receipt_ids, ack['plans'], strict=True):
        if (proof.get('table') != plan.table or proof.get('entity') != plan.entity
                or proof.get('rows') != len(plan.rows) or proof.get('expected_rows') != len(plan.rows)
                or not fullmatch('[0-9a-f]{64}', proof.get('physical_hash') or '')
                or proof.get('scope_values_json', _scope_json(plan)) != _scope_json(plan)):
            return False
        scope = _target_scope(plan)
        target = write_journal.pending_statement(scope)
        if ((target and (scope in held or _statement_expected(target) in receipt_ids))
                or write_journal.pending_statement((CORE_RECEIPTS_TABLE, key))
                or write_journal.pending_statement((CORE_PAGES_TABLE, key))):
            return False
    return True


def _settle_plan_statements(connection, plan, receipt_id, committed_at, unit, bundle_owner, contested,
                            historical_intent, *, repair):
    scope = _target_scope(plan)
    target = write_journal.pending_statement(scope)
    if target is not None:
        # Several pages in one bundle can share a scope. Only the fenced
        # member's exact postimage can establish that its submission landed.
        expected = _statement_expected(target)
        owner = unit.get(expected)

        def record_finished(statement):
            if historical_intent:
                write_journal.remember_terminal_target(historical_intent.stem, expected, statement)

        if owner is None:
            if _query_outcome(connection, target) != 'rejected':
                raise _ScopePending('core statement outcome is still pending: ' + plan.table, expected)
        elif historical_intent and write_journal.terminal_target(historical_intent.stem, expected, target):
            if not _target_matches(connection, owner):
                raise RuntimeError('core statement outcome is still pending: ' + plan.table)
            write_journal.hold_scope(scope, bundle_owner)
        elif _settle_statement(connection, scope, expected, lambda: _target_matches(connection, owner),
                               release=False, target=True, require_finished=scope in contested,
                               on_finished=record_finished, independent_postimage=repair):
            write_journal.hold_scope(scope, bundle_owner)
        write_journal.finish_statement(target)
    elif scope in contested and not _target_matches(connection, plan):
        raise RuntimeError('contested core target outcome is still pending: ' + plan.table)
    page_key = (CORE_PAGES_TABLE, receipt_id)
    if plan.pages and write_journal.pending_statement(page_key) is not None:
        held = _stored_walk(connection, receipt_id)
        complete = held if held and len(held) == len(plan.pages) else None
        expected = _expected_walk(plan, receipt_id, committed_at, held=complete)
        _settle_statement(connection, page_key, _digest(expected), lambda: held == expected,
                          independent_postimage=repair)
    key = (CORE_RECEIPTS_TABLE, receipt_id)
    statement = write_journal.pending_statement(key)
    if statement is not None:
        expected = _statement_expected(statement)
        _settle_statement(connection, key, expected, lambda: _digest(
            _stored_receipts(connection, [receipt_id]).get(receipt_id)) == expected, independent_postimage=repair)


def _write_plan(connection, writer, plan, receipt_id, bundle_owner, repair=False):
    if _uses_merge(plan) and not plan.rows:
        return
    before = _owned_rows(connection, plan)
    if not before and not plan.rows:
        return
    scope = _target_scope(plan)
    if write_journal.scope_owner(scope) == bundle_owner and _rows_equal(before, plan.rows):
        return  # this unit already dispatched its target and only lacks ops evidence
    previous = getattr(_LOCAL, 'target', None)
    observable = not repair and _strictly_additive(connection, plan, before)
    _LOCAL.target = (scope, receipt_id, observable, bundle_owner)
    try:
        if not plan.rows or getattr(writer, 'dispatches_journalled_target', False) is True:
            _mutate_plan(connection, writer, plan)
        else:
            # A custom writer without a cursor handoff exposes only the call
            # itself as its submission boundary. Retain uncertainty on error.
            with _target_dispatch(direct=False):
                _mutate_plan(connection, writer, plan)
    finally:
        _LOCAL.target = previous


def _commit_plans(connection, writer, plans, repair=None, receipt_ids=(), committed_at='',
                  verify_only=False, contested=(), historical_intent=None, run_id=None) -> list[dict]:
    """Guard, mutate and prove the plans against the warehouse.

    ``repair`` is called when the plans are about to mutate although their
    bundle is already acknowledged: an interrupted repair must leave an
    intent behind to replay. ``receipt_ids`` names, per plan, the warehouse
    receipt about to acknowledge it: a snapshot's walk is logged under it.
    """
    receipt_ids = tuple(receipt_ids) or ('',) * len(plans)
    unit = dict(zip(receipt_ids, plans, strict=True))
    for plan, receipt_id in zip(plans, receipt_ids, strict=True):
        scope = _target_scope(plan)
        target = write_journal.pending_statement(scope)
        if target and target.get('stage') and _statement_expected(target) == receipt_id:
            _clean_stages(connection, writer, [plan], [receipt_id], run_id, retained=target['stage'])
        if verify_only and target and _statement_expected(target) not in unit:
            raise _ScopePending('core statement outcome is still pending: ' + plan.table,
                                _statement_expected(target))
        required = {scope} if verify_only and target else contested
        _settle_plan_statements(connection, plan, receipt_id, committed_at, unit, receipt_ids[0], required,
                                historical_intent, repair=repair is not None)
    if not verify_only:
        execute_statement(connection.cursor(), core_receipts_ddl())
        if any(plan.pages for plan in plans):
            execute_statement(connection.cursor(), core_pages_ddl())
    for plan in plans:
        if not verify_only:
            execute_statement(connection.cursor(), plan.ddl)
        _guard_table_schema(connection, plan)
    restorations = {}

    def recover_walk(plan, receipt_id, surviving, rows):
        member = unit.get(receipt_id)
        if member is None and repair is not None:
            return None  # an acknowledged retry may repair only its unit
        if member is not None:
            if member.full_table != plan.full_table or _scope_json(member) != _scope_json(plan):
                raise RuntimeError('core page manifest differs: ' + plan.table)
            expected = _walk_rows(member, receipt_id, committed_at)
            if rows is not None and rows != expected:
                raise RuntimeError('core page manifest differs: ' + plan.table)
            rows = expected
        if not rows:
            return None
        expected = tuple(_walk_cells(row) for row in rows)
        held = _stored_walk(connection, receipt_id)
        _held_page_numbers(held or (), expected, plan.table)
        # Preflight the fence of every historical INSERT before the first
        # repair. In particular an absent walk cannot settle a queued INSERT.
        _settle_statement(connection, (CORE_PAGES_TABLE, receipt_id), _digest(expected),
                          lambda: held == expected, release=False,
                          independent_postimage=repair is not None or member is None)
        restorations[receipt_id] = (plan.table, rows, 'differs' if surviving else 'lost')
        return expected

    recover = None if verify_only else recover_walk
    bindings = _capture_bindings(connection, plans)
    evidence = [_accepted_pages(connection, plan, own=unit, restore=recover) for plan in plans]
    states = [(plan, _replay_state(connection, plan, receipt_id, committed_at, accepted=accepted,
                                   restoring=restorations, remember_rules=not verify_only))
              for plan, receipt_id, accepted in zip(plans, receipt_ids, evidence, strict=True)]
    for (plan, state), accepted, receipt_id in zip(states, evidence, receipt_ids, strict=True):
        # A lineage-only refresh rewrites the run identity of rows that are
        # already committed under this very capture: nothing can regress,
        # whatever clock a sibling capture of the scope carries.
        if (plan.write_mode == 'replace' or plan.entity in _OBSERVATION_CLOCK_ENTITIES or state == 'pending'
                or (state == 'refresh' and not _capture_pinned(plan))):
            try:
                guard_native_bundle(connection, plan, accepted=accepted, own=(receipt_id,),
                                    acknowledged_at=_timestamp(committed_at))
            except StaleTransfermarktWrite as exc:
                for table, _, reason in restorations.values():
                    if table == plan.table:
                        raise StaleTransfermarktWrite(f'{exc} (core page manifest {reason})') from exc
                raise
    if verify_only:
        for plan, receipt_id in zip(plans, receipt_ids, strict=True):
            if plan.pages:
                _verify_pages(connection, plan, receipt_id, committed_at, settle=False)
        return [_physical_proof(connection, plan, committed_at) for plan in plans]
    needs_write = restorations or any(state != 'written' for _plan, state in states)
    if repair is not None and needs_write:
        repair()
    if repair is None or needs_write:
        scopes = {_target_scope(plan) for plan in plans}
        for (table, capture), body in bindings.items():
            path = write_journal._record_path('capture-bodies', _digest((table, capture)))
            write_journal._immutable_record(path, {'table': table, 'capture': capture, 'body': body})
        if len(scopes) > 1:
            write_journal.reserve_scopes(scopes, receipt_ids[0])
    for key, (table, rows, _reason) in restorations.items():
        write_journal.hold_scope((rows[0][1], rows[0][2]), receipt_ids[0])
        if key in unit:
            _record_pages(connection, unit[key], key, committed_at, repair is not None)
        else:
            _log_walk(connection, table, key, rows, repair=True)
        if _stored_walk(connection, key) != tuple(_walk_cells(row) for row in rows):
            raise RuntimeError('core page manifest differs: ' + table)
    for (plan, state), receipt_id in zip(states, receipt_ids, strict=True):
        if state == 'written' or _target_scope(plan) in contested:
            continue
        _write_plan(connection, writer, plan, receipt_id, receipt_ids[0], repair is not None)
    if repair is None or needs_write:
        for scope in scopes:
            write_journal.hold_scope(scope, receipt_ids[0])
    for plan, receipt_id, (_plan, state) in zip(plans, receipt_ids, states, strict=True):
        # The logged walk follows the snapshot it belongs to; a snapshot that
        # is written is logged under its own acknowledgement.
        if plan.pages and state != 'written':
            _record_pages(connection, plan, receipt_id, committed_at, repair is not None)
    # Like the receipts, the walk is acknowledged only once it reads back: a
    # snapshot whose pages did not reach the warehouse is not written.
    for plan, receipt_id in zip(plans, receipt_ids, strict=True):
        if plan.pages:
            _verify_pages(connection, plan, receipt_id, committed_at)
    return [_physical_proof(connection, plan, committed_at) for plan in plans]


def _batch_id_for_run(run_id: str) -> str:
    """The batch printed on the rows; the receipt must name the same one."""
    return 'tm-core-batch-' + sha256(run_id.encode('utf-8')).hexdigest()[:32]


def _anchored_tables(plans) -> set[str]:
    """Every table the bundle mutates, for its pre-write snapshot anchors."""
    tables = {plan.table for plan in plans} | {_RECEIPTS_ANCHOR_NAME}
    if any(plan.pages for plan in plans):
        tables.add(_PAGES_ANCHOR_NAME)
    return tables


def _bundle_identity(plans, run_id: str) -> tuple[list[str], str]:
    """The plan hashes and the bundle digest a commit is acknowledged under."""
    plan_hashes = [plan.plan_sha256() for plan in plans]
    return plan_hashes, _digest({'plans': plan_hashes, 'run_id': run_id})


# What the stage-B writer (#2404) could journal: the core entities of that
# stage, each described in the format of its plan, which had no page evidence.
_STAGE_B_ENTITIES = frozenset({
    'squad_memberships', 'player_attribute_observations', 'player_contract_observations',
    'market_value_points', 'transfer_events', 'coach_profiles', 'coach_stints',
})
_STAGE_B_PLAN_FIELDS = frozenset({
    'entity', 'table', 'ddl', 'natural_keys', 'scope_columns', 'scope_values', 'rows',
    'captured_at', 'write_mode', 'status', 'status_reason', 'proof', 'capture_id', 'evidence',
})


def _journalled_by_stage_b(descriptors) -> bool:
    """Whether a unit is exactly what the stage-B writer journalled.

    Read from the descriptors as saved, before a rebuilt plan applies the
    defaults of today: a snapshot with page evidence never was a stage-B
    unit, whatever digest it is filed under.
    """
    return bool(descriptors) and all(
        set(descriptor) == _STAGE_B_PLAN_FIELDS and descriptor['entity'] in _STAGE_B_ENTITIES
        for descriptor in descriptors)


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


def _earlier_acknowledgement(plans, run_id: str) -> tuple[list[str], str] | None:
    """Find the earlier writer's pending or acknowledged unit."""
    if not all(plan.entity in _STAGE_B_ENTITIES and not plan.pages for plan in plans):
        return None
    plan_hashes = [_stage_b_plan_sha256(plan) for plan in plans]
    digest = _digest({'plans': plan_hashes, 'run_id': run_id})
    if write_journal.read_receipt(digest) is not None or write_journal.pending_intents(
            {'run_id': run_id, 'digest': digest}):
        return plan_hashes, digest
    return None


def _publish_alias(current: str, receipt: dict, plan_hashes, digest: str) -> None:
    """Journal under today's identity an acknowledgement written under an earlier writer's.

    Bound to the warehouse receipts already written and to the anchors
    recorded before that write: an ordinary retry of the unit reuses both.
    """
    if current == digest:
        return
    # The anchors first: an alias is never visible without the pre-write
    # anchors of the acknowledgement it stands for, and an interrupted
    # publication leaves nothing for a retry to complete.
    write_journal.copy_snapshot_anchors(digest, current)
    original = write_journal.read_snapshot_anchors(digest)
    if not original or write_journal.read_snapshot_anchors(current) != original:
        raise RuntimeError('core alias snapshot anchors differ')
    if write_journal.read_receipt(current) is not None:
        return
    write_journal.record_receipt(current, {
        **receipt, 'plan_sha256': current, 'descriptor_digest': current,
        'identity': {'plan_hashes': list(plan_hashes), 'digest': digest}})


def _journal_walks(plans, receipt_ids) -> list[dict]:
    """The walks the acknowledgement logged in the warehouse, kept with it.

    A warehouse that lost them is restored from here: by the acknowledgement
    itself while its plan is at hand, and from this record afterwards.
    """
    walks = []
    for plan, receipt_id in zip(plans, receipt_ids, strict=True):
        if not plan.pages:
            continue
        pages = []
        for page in plan.pages:
            page_rows = _page_rows(plan, page[1])
            pages.append([*page, len(page_rows), _page_digest(plan, page, page_rows)])
        walks.append({'table': plan.full_table, 'scope_values_json': _scope_json(plan),
                      'receipt_id': receipt_id, 'pages': pages, 'digest_version': 8})
    return walks


def _legacy_transport_intents():
    # The old statement protocol also covered keyed MERGEs without fencing
    # them. Its adoption marker could mean either pre-dispatch adoption or
    # post-dispatch readback. Neither marker certifies a single POST.
    legacy = {entry['digest'] for entry in write_journal._load_index()
              if entry['status'] == 'pending' and entry['identity'].get('scope_protocol') != 1}
    if legacy:
        for path, body in write_journal.pending_intents({}):
            if path.stem in legacy:
                yield path, body


def _legacy_proof_scopes(connection, table, receipt, proofs, warehouse):
    full_table = 'iceberg.bronze.' + table
    if all(p.get('scope_values_json') for p in proofs):
        return {p['scope_values_json'] for p in proofs}
    run, digest = receipt['run_id'], warehouse['digest']
    if 'plan_hashes' in warehouse:
        expected = {_receipt_id(h, run, digest): p for h, p in
                    zip(warehouse['plan_hashes'], receipt['plans'], strict=True) if p['table'] == table}
        scopes = {key: p['scope_values_json'] for key, p in expected.items() if p.get('scope_values_json')}
        for walk in receipt.get('walks') or ():
            key, scope = walk['receipt_id'], walk['scope_values_json']
            if key in expected and (walk['table'] != full_table or scopes.setdefault(key, scope) != scope):
                raise RuntimeError('finished pre-upgrade scope evidence differs: ' + table)
        if scopes.keys() != expected.keys():
            for key, scope, count in _warehouse_members(connection, full_table, run, digest):
                if (key not in expected or expected[key]['rows'] != count
                        or scopes.setdefault(key, scope) != scope):
                    raise RuntimeError('finished pre-upgrade scope evidence differs: ' + table)
        if scopes.keys() == expected.keys() and all(scopes.values()):
            return set(scopes.values())
    else:
        members = _warehouse_members(connection, full_table, run, digest)
        scopes = {scope for _, scope, _ in members}
        known = {p['scope_values_json'] for p in proofs if p.get('scope_values_json')}
        if (Counter(count for _, _, count in members) == Counter(p['rows'] for p in proofs)
                and all(scopes) and known <= scopes):
            return scopes
    raise RuntimeError('finished pre-upgrade scope evidence is still pending: ' + table)


def _missing_ack_overlaps(connection, identity, plans):
    """Exclude a table using anchors, or an edition using a complete, bound warehouse member."""
    digest = identity['digest']
    targets = set(write_journal.read_snapshot_anchors(digest)) - {_RECEIPTS_ANCHOR_NAME, _PAGES_ANCHOR_NAME}
    requested = {plan.table for plan in plans}
    # Separate finished attempts can share one warehouse identity. A
    # surviving full scope inventory for a table applies to all of them.
    for inventory in write_journal.guard_inventories():
        if inventory['digest'] == digest:
            for plan in plans:
                guard = inventory['guards'].get(plan.table)
                if guard:
                    if guard['digest'] != digest or guard['table'] != plan.table or not guard['scopes']:
                        raise RuntimeError('finished pre-upgrade scope evidence differs')
                    if _scope_json(plan) in guard['scopes']:
                        return True
                    requested.discard(plan.table)
    if not requested:
        return False
    if targets and requested.isdisjoint(targets):
        return False
    if len(targets) > 1:
        return True
    run = identity.get('run_id')
    if run:
        predicate, bound = 'run_id = ?', (run,)
    elif targets:
        predicate, bound = 'target_table = ?', ('iceberg.bronze.' + next(iter(targets)),)
    else:
        return True
    cursor = connection.cursor()
    try:
        rows = _query_rows(cursor, f'SELECT receipt_id, plan_sha256, run_id, target_table, entity, scope_values_json, '
                           f'committed_at, source_capture_id, captured_at, source_body_hash '
                           f'FROM {CORE_RECEIPTS_TABLE} WHERE {predicate}', bound)
    except Exception as exc:
        if write_journal._table_not_found(exc):
            return True
        raise
    finally:
        cursor.close()
    members = [row for row in rows if row[0] == _receipt_id(row[1], row[2], digest)]
    if len(members) != 1:
        return True
    key, plan_hash, run, full_table, entity, scope, acknowledged_at, capture, captured_at, body = members[0]
    # Only a complete bundle hash rules out an unobserved sibling scope.
    if _digest({'plans': [plan_hash], 'run_id': run}) != digest:
        return True
    contract = _entity_registry(entity)
    table = contract['table']
    if full_table != 'iceberg.bronze.' + table or targets and targets != {table}:
        return True
    if table not in requested:
        return False
    if entity not in _MULTI_PAGE_REPLACE_ENTITIES:
        return True
    # A receipt's scope string alone can drift. Bind every page to the
    # receipt ID, table, canonical scope and original edition route.
    from scrapers.transfermarkt import core, sections

    walk = _stored_walk(connection, key)
    if not walk:
        return True
    try:
        pairs = json.loads(scope)
        values = dict(pairs)
        if (list(values) != list(contract['scope_columns']) or not all(map(_nonempty, values.values()))
                or write_journal._canonical(list(values.items())).decode() != scope):
            return True
        for number, row in enumerate(walk, 1):
            if (row[:5] != (key, full_table, scope, number, len(walk))
                    or sections._attendance_page_number(row[5]) != number):
                return True
            core._check_scope_against_capture(entity, values, SimpleNamespace(url=row[5], endpoint='attendance'))
    except (ValueError, TypeError, KeyError):
        return True
    overlaps = any(p.table == table and _scope_json(p) == scope for p in plans)
    if overlaps:
        if any(row[-1] != _page_cell('acknowledged_at', acknowledged_at) for row in walk):
            raise RuntimeError('core page manifest acknowledgement clock differs: ' + table)
        named = (capture, _page_cell('captured_at', captured_at), body)
        if sum(row[6:9] == named for row in walk) != 1:
            raise RuntimeError('core page manifest named capture differs: ' + table)
    return overlaps


def _publish_scope_guards(migration, guards):
    intent = migration['intent']
    for table, guard in guards.items():
        if guard['table'] != table or guard['digest'] != migration['digest']:
            raise RuntimeError('finished pre-upgrade scope evidence differs')
        write_journal._immutable_record(write_journal._record_path('legacy-scopes/' + table, intent), guard)
    write_journal._immutable_record(write_journal._record_path('migrated-scopes', intent), migration)


def _finished_legacy_scopes(connection, plans, identities, *, run_id, acknowledged=False, historical_intent=None):
    """Recover scope ownership even after an old writer unlinked its intent.

    Old index entries retain the bundle identity, not the descriptors. If a
    proof predates scope metadata, resolve its original warehouse receipt by
    its bound ID, then persist the mapping before another writer proceeds.
    """
    verify_only = False
    requested = {plan.table for plan in plans}
    for entry in write_journal.guard_history():
        identity = entry.get('identity', {})
        if entry['status'] != 'finished':
            continue
        completion = write_journal.completed_intent(entry['digest'])
        if (completion and completion['terminal'] and completion['digest'] == identity.get('digest')
                and write_journal._record_path('receipts', completion['digest']).exists()):
            continue
        digest = identity.get('digest')
        if not digest:
            raise RuntimeError('finished pre-upgrade intent identity is still pending')
        migrated = write_journal._record_path('migrated-scopes', entry['digest'])
        migration = {'digest': digest, 'intent': entry['digest']}
        if migrated.exists() and write_journal._read_record(migrated) != migration:
            raise RuntimeError('finished pre-upgrade migration differs')
        inventory_path = write_journal._record_path('guard-inventories', entry['digest'])
        inventory = write_journal._read_record(inventory_path) if inventory_path.exists() else None
        if inventory is not None:
            guards = inventory['guards']
            _publish_scope_guards(migration, guards)
            if any(p.table in guards and _scope_json(p) in guards[p.table]['scopes'] for p in plans):
                receipt = write_journal.read_receipt(digest)
                if receipt is None:
                    raise RuntimeError('finished pre-upgrade acknowledgement is still pending')
                if inventory['acknowledgement'] != _digest(receipt):
                    raise RuntimeError('finished pre-upgrade acknowledgement differs')
            continue
        receipt = write_journal.read_receipt(digest)
        if receipt is not None and not write_journal.acknowledgement_matches_inventory(digest, receipt):
            if _missing_ack_overlaps(connection, identity, plans):
                raise RuntimeError('finished pre-upgrade acknowledgement differs')
            continue
        if receipt is None:
            if not _missing_ack_overlaps(connection, identity, plans):
                continue
            raise RuntimeError('finished pre-upgrade acknowledgement is still pending')
        if receipt['plan_sha256'] != digest or receipt['run_id'] != identity.get('run_id', receipt['run_id']):
            raise RuntimeError('finished pre-upgrade acknowledgement differs')
        if not receipt['plans']:
            anchored = set(write_journal.read_snapshot_anchors(digest))
            if requested.isdisjoint(anchored) and anchored:
                continue
            if acknowledged and digest in identities:
                verify_only = True  # only its bound warehouse proofs can corroborate this retry
                continue
            raise RuntimeError('finished pre-upgrade acknowledgement lacks target proofs')
        warehouse = receipt.get('identity') or {'digest': digest}
        by_table = {}
        for proof in receipt['plans']:
            contract = _entity_registry(proof['entity'])
            table = proof['table']
            if contract['table'] != table:
                raise RuntimeError('finished pre-upgrade proof target differs')
            if contract['write_mode'] == 'replace' or identity.get('single_submit') is not True:
                by_table.setdefault(table, []).append(proof)
        if requested.isdisjoint(by_table):
            continue
        guards = {}
        for table, proofs in by_table.items():
            previous = write_journal._record_path('legacy-scopes/' + table, entry['digest'])
            if (acknowledged and digest in identities and receipt['run_id'] == run_id
                    and len(receipt['plans']) == len(plans)
                    and all(p.table == proof['table'] and p.entity == proof['entity']
                            for p, proof in zip(plans, receipt['plans']))):
                scopes = {_scope_json(p) for p in plans if p.table == table}
                if any(proof.get('scope_values_json', _scope_json(p)) != _scope_json(p)
                       for p, proof in zip(plans, receipt['plans']) if p.table == table):
                    raise RuntimeError('finished pre-upgrade scope evidence differs: ' + table)
            elif previous.exists() and migrated.exists():
                # The old marker recorded completion of migration. It can
                # corroborate a surviving guard, never stand in for one.
                scopes = set(write_journal._read_record(previous)['scopes'])
                if not scopes or not all(map(_nonempty, scopes)):
                    raise RuntimeError('finished pre-upgrade scope evidence differs: ' + table)
            else:
                scopes = _legacy_proof_scopes(connection, table, receipt, proofs, warehouse)
            guards[table] = {
                'table': table, 'digest': digest, 'warehouse': warehouse['digest'],
                'scopes': sorted(scopes)}
        write_journal._immutable_record(inventory_path, {
            **migration, 'acknowledgement': _digest(receipt), 'guards': guards})
        _publish_scope_guards(migration, guards)
    bound_hashes, bound_digest = _bundle_identity(plans, run_id)
    keys = [_receipt_id(plan_hash, run_id, bound_digest) for plan_hash in bound_hashes]
    bound_targets = set()
    for plan, key in zip(plans, keys, strict=True):
        target = write_journal.pending_statement(_target_scope(plan))
        if ((target and _statement_expected(target) == key)
                or historical_intent and write_journal.terminal_target(historical_intent.stem, key)):
            bound_targets.add(key)
    for plan, key in zip(plans, keys, strict=True):
        rowless_member = historical_intent is not None and bool(bound_targets) and not plan.rows and _uses_merge(plan)
        for path in (write_journal._root() / 'legacy-scopes' / plan.table).glob('*.json'):
            guard = write_journal._read_record(path)
            if guard['table'] != plan.table:
                raise RuntimeError('finished pre-upgrade scope evidence differs')
            if _scope_json(plan) in guard['scopes']:
                if not acknowledged and key not in bound_targets and not rowless_member and guard['digest'] not in identities \
                        and guard['warehouse'] not in identities:
                    raise StaleTransfermarktWrite('finished pre-upgrade core statement outcome is still pending')
                # Exact read-only verification is allowed; later DML remains
                # fenced regardless of which historical guard is newest.
                verify_only = True
    return verify_only


def _pre_upgrade_pending(plans, paths, descriptor_digest, *, acknowledged, receipt_ids, run_id):
    """An old anchored intent may have dispatched DML without a statement fence.

    Its stage-B recovery may verify an already complete warehouse unit. A
    missing result cannot be resubmitted, and other captures of the same
    scope must wait. Without pre-write anchors the old writer had not yet
    reached DML, so it is safe to adopt that exact intent before submission.
    """
    scopes = {(plan.table, plan.scope_values) for plan in plans}
    verify_only = False
    contested = set()
    # Completing a legacy unit can retire its replay file. The warehouse
    # cannot identify how many submissions produced the observed rows.
    def legacy_target_key(plan):
        return ('pre-fence-target', plan.full_table, _scope_json(plan))

    for plan in plans:
        guard = write_journal.pending_statement(legacy_target_key(plan))
        if guard is not None:
            if guard['expected'] != descriptor_digest and not acknowledged:
                raise RuntimeError('pre-upgrade core statement outcome is still pending')
            verify_only = True
    bound_hashes, bound_digest = _bundle_identity(plans, run_id)
    bound_receipt_ids = [_receipt_id(plan_hash, run_id, bound_digest) for plan_hash in bound_hashes]

    def bound_owner_first(item):
        path, body = item
        if path not in paths:
            return False
        for member, receipt_id in zip(plans, bound_receipt_ids, strict=True):
            scope = _target_scope(member)
            target = write_journal.pending_statement(scope)
            if ((target and _statement_expected(target) == receipt_id)
                    or (not target and write_journal.scope_owner(scope) == bound_receipt_ids[0])):
                return True
        return False

    for path, body in sorted(_legacy_transport_intents(), key=bound_owner_first, reverse=True):
        descriptors = body['payload']['plans']
        if not any((item['table'], tuple(item['scope_values'])) in scopes for item in descriptors):
            continue
        if len(descriptors) > 1 and body['identity'].get('single_submit'):
            members, hashes, digest = _intent_unit(body)
            keys = [_receipt_id(h, body['identity']['run_id'], digest) for h in hashes]
            if _released_after_ack(body, members, keys):
                continue
        warehouse = body['payload'].get('warehouse_identity') or body['identity']
        adoption = write_journal._record_path('single-submit-adoptions', path.stem)
        adopted = adoption.exists()
        if adopted:
            if write_journal._read_record(adoption) != {'intent': path.stem}:
                raise RuntimeError('core adopted intent differs')
        if adopted or body['identity'].get('single_submit') is True:
            # r8 removed the target statement before logging the walk. Its
            # anchored unit must finish before a competitor enters that gap.
            if path not in paths and write_journal.read_snapshot_anchors(warehouse['digest']):
                shared = {(item['table'], tuple(item['scope_values'])) for item in descriptors} & scopes
                for member, receipt_id in zip(plans, bound_receipt_ids, strict=True):
                    if (member.table, member.scope_values) not in shared:
                        continue
                    scope = _target_scope(member)
                    target = write_journal.pending_statement(scope)
                    if ((target and _statement_expected(target) == receipt_id)
                            or (not target and write_journal.scope_owner(scope) == bound_receipt_ids[0])):
                        contested.add(scope)
                        continue
                    if contested:
                        # The bound owner settles first; competing intents
                        # remain pending instead of masking its coordinator read.
                        continue
                    raise _ScopePending('pre-upgrade scope acknowledgement is still pending', warehouse['digest'])
            continue
        if not write_journal.read_snapshot_anchors(warehouse['digest']):
            if write_journal.read_receipt(warehouse['digest']) is not None:
                raise RuntimeError('pre-upgrade acknowledged snapshot anchors are missing')
            if path in paths:
                write_journal._immutable_record(adoption, {'intent': path.stem})
                write_journal.adopt_intent(path)
            continue
        if path not in paths or (not acknowledged and (
                body['identity'].get('statement_protocol') == 1 or not _journalled_by_stage_b(descriptors))):
            raise RuntimeError('pre-upgrade core statement outcome is still pending')
        for plan in plans:
            if plan.write_mode == 'replace' or plan.rows:
                key = legacy_target_key(plan)
                if write_journal.pending_statement(key) is None:
                    write_journal.begin_statement(key, descriptor_digest, observable=False)
        verify_only = True
    return verify_only, contested


def _stage_identity(receipt_id, table, stage, record):
    identity = {'receipt_id': receipt_id, 'table': table, 'stage': stage}
    if (not fullmatch(r'iceberg\.bronze\.transfermarkt_[a-z0-9_]+', table)
            or not fullmatch(re.escape(table.split('.')[-1]) + '__stg_tm_[0-9a-f]{32}', stage)
            or record not in (identity, {**identity, 'fenced_create': True})):
        raise RuntimeError('core retained stage identity differs')


def _retire_abandoned_stage(connection, writer, path, record):
    stage = path.stem
    qualified = 'iceberg.bronze.' + stage
    _stage_identity(path.parent.name, record['table'], stage, record)
    if write_journal.stage_in_use(record):
        return
    create = write_journal.pending_statement(('stage-create', qualified))
    if create and _statement_expected(create) != qualified:
        raise RuntimeError('core retained stage identity differs')
    try:
        if not record.get('fenced_create') or (
                create and _query_outcome(connection, create) not in ('finished', 'rejected')):
            cursor = connection.cursor()
            try:
                _query_rows(cursor, 'SELECT * FROM ' + qualified + ' WHERE 1 = 0')
            except Exception as exc:
                if write_journal._table_not_found(exc):
                    return
                raise
            finally:
                cursor.close()
        if create:
            write_journal.finish_statement(create)
        drop = getattr(writer, 'drop_journalled_stage', None)
        if drop is not None:
            drop(stage)
        else:
            cursor = connection.cursor()
            try:
                execute_statement(cursor, 'DROP TABLE IF EXISTS ' + qualified)
            finally:
                cursor.close()
    except Exception as exc:
        logging.getLogger(__name__).warning('Deferred Transfermarkt stage cleanup for %s (%s)',
                                            stage, type(exc).__name__)
        return
    # Earlier transports could submit more than one CREATE. Keep watching
    # their unique names even after dropping a visible orphan.
    if record.get('fenced_create'):
        write_journal.forget_stage(path)


def _clean_stages(connection, writer, plans, receipt_ids, run_id, *, retained=None):
    for plan, receipt_id in zip(plans, receipt_ids, strict=True):
        if retained is not None:
            protected = write_journal.stage_record(receipt_id, retained)
            if not protected.exists():
                continue
            _stage_identity(receipt_id, plan.full_table, retained, write_journal._read_record(protected))
        for path, record in write_journal.retained_stages(receipt_id):
            _stage_identity(receipt_id, plan.full_table, path.stem, record)
            if path.stem == retained:
                continue
            cleanup = write_journal._record_path('abandoned-stages/' + receipt_id, path.stem)
            write_journal._immutable_record(cleanup, {'run_id': run_id, 'stage': record})
            write_journal.forget_stage(path)
            _retire_abandoned_stage(connection, writer, cleanup, record)

def _release_packet(plans, owner, paths):
    scopes = dict.fromkeys(_target_scope(plan) for plan in plans)
    write_journal.finish_reservation(owner, scopes)
    for scope in scopes:
        write_journal.release_scope(scope, owner)
    for path in paths:
        write_journal.finish_intent(path)


def _intent_terminal(path, identity, plans, receipt_ids):
    witness = write_journal._record_path('fenced-from-start', path.stem)
    if identity.get('packet_attempt') or (
            witness.exists() and write_journal._read_record(witness) == {'intent': path.stem}):
        return True
    replacements = [key for key, plan in zip(receipt_ids, plans, strict=True) if plan.write_mode == 'replace']
    return identity.get('single_submit') is True and all(
        write_journal.terminal_target(path.stem, key) for key in replacements)


def _complete_packet(plans, receipt_ids, paths):
    owner = receipt_ids[0]
    for path in paths:
        identity = write_journal._intent_body(path, 'core write intent checksum differs')['identity']
        terminal = _intent_terminal(path, identity, plans, receipt_ids)
        write_journal.complete_intent(path, identity['digest'], owner, terminal)
    _release_packet(plans, owner, paths)


def _terminal_packet(path, plans, receipt_ids):
    return path is not None and all(write_journal.terminal_target(path.stem, key)
                                    for plan, key in zip(plans, receipt_ids, strict=True)
                                    if plan.rows or plan.write_mode == 'replace')


def _commit_bundle(
    connection_factory, writer, plans, *, plan_hashes, digest, run_id: str,
    universe_version: str, committed_at: str, deadline: float | None = None,
    journaled: bool = False, repair_identity=None,
) -> dict:
    """Commit a bundle under a given identity: its own, or the one it was
    journalled under by an earlier writer.

    ``journaled`` says the caller already holds the pending intent of this
    unit and finishes it: no second intent is published.
    """
    with writer_deadline(deadline), journalled_write(), writer_lock():
        _validate_bundle(plans, run_id, universe_version, committed_at)
        write_journal.check_dispatch_bindings()
        if len(set(plan_hashes)) != len(plan_hashes):
            # One acknowledgement never stands for two plans of a bundle.
            raise ValueError('duplicate core receipt identity across plans')
        batch = _batch_id_for_run(run_id)
        current = digest
        descriptor_digest = _bundle_identity(plans, run_id)[1]
        if write_journal.read_receipt(digest) is None:
            earlier = _earlier_acknowledgement(plans, run_id)
            if earlier is not None:
                plan_hashes, digest = earlier
        stored = write_journal.read_receipt(digest)
        identity = (stored or {}).get('identity') or {'plan_hashes': plan_hashes, 'digest': digest}
        keys = {descriptor_digest, digest, identity['digest']}
        pending = [entry for key in sorted(keys) for entry in
                   write_journal.pending_intents({'run_id': run_id, 'digest': key})]
        if pending:
            bound_hashes, bound_digest = _bundle_identity(plans, run_id)
            for index, plan in enumerate(plans):
                target = write_journal.pending_statement(_target_scope(plan))
                if target and _statement_expected(target) == _receipt_id(
                        bound_hashes[index], run_id, bound_digest):
                    identity = {'plan_hashes': bound_hashes, 'digest': bound_digest}
                    break
        paths = [path for path, _ in pending]
        receipt_repairs = {path for path, body in pending if body['payload'].get('receipt_only') is True}
        if receipt_repairs and stored is None:
            raise RuntimeError('core receipt repair lacks its original acknowledgement')
        historical_intent = None
        if len(pending) == 1:
            path, body = pending[0]
            if body['identity'].get('single_submit') is True and path not in receipt_repairs:
                historical_intent = path
        if journaled and not paths and stored is None:
            raise RuntimeError('core replay lost its pending intent')
        for _path, body in pending:
            if body['evidence'].get('universe_version', universe_version) != universe_version:
                raise RuntimeError('core pending intent belongs to another universe')
            saved_plans, _, _ = _intent_unit(body)
            if _bundle_identity(saved_plans, run_id)[1] != descriptor_digest:
                raise RuntimeError('core pending intent capture evidence differs')
            warehouse = body['payload'].get('warehouse_identity')
            if warehouse is not None and (stored is None or warehouse != identity):
                raise RuntimeError('core repair warehouse identity differs')
        if repair_identity is not None and stored is None:
            raise RuntimeError('core repair lacks its original acknowledgement')
        receipt_ids = [_receipt_id(h, run_id, identity['digest']) for h in identity['plan_hashes']]
        completed = [write_journal.completed_intent(path.stem) for path in paths]
        if any(completed) and (stored is None or any(
                c and (c['digest'] not in keys or c['owner'] != receipt_ids[0]) for c in completed)):
            raise RuntimeError('core completed intent acknowledgement differs')
        if stored is not None:
            if not write_journal.acknowledgement_matches_inventory(digest, stored):
                raise RuntimeError('finished pre-upgrade acknowledgement differs')
            if stored.get('plan_sha256') != digest:
                raise RuntimeError('core journal receipt plan_sha256 differs')
            if stored['run_id'] != run_id or stored['committed_batch'] != batch:
                raise RuntimeError('core journal receipt run identity differs')
            if stored.get('universe_version', universe_version) != universe_version:
                # The acknowledgement names its universe; a retry under
                # another one is another unit.
                raise RuntimeError('core journal receipt belongs to another universe')
            # The acknowledgement names the identity its warehouse receipts
            # were written under. A unit migrated from an earlier writer keeps
            # that one, so a retry under today's identity reuses them.
            if repair_identity is not None and repair_identity != identity:
                raise RuntimeError('core repair warehouse identity differs')
            binding = stored.get('descriptor_digest')
            if binding is not None and binding != descriptor_digest:
                raise RuntimeError('core legacy acknowledgement capture evidence differs')
            receipt_hashes, receipt_digest = list(identity['plan_hashes']), identity['digest']
            alias = digest == descriptor_digest and receipt_digest != digest
            if alias:
                legacy = [_stage_b_plan_sha256(plan) for plan in plans]
                original_anchors = write_journal.read_snapshot_anchors(receipt_digest)
                if (not all(plan.entity in _STAGE_B_ENTITIES for plan in plans)
                        or receipt_hashes != legacy or receipt_digest != _digest({'plans': legacy, 'run_id': run_id})
                        or not original_anchors or write_journal.read_snapshot_anchors(digest) != original_anchors):
                    raise RuntimeError('core alias warehouse identity or snapshot anchors differ')
            elif receipt_hashes != list(plan_hashes) or receipt_digest != digest:
                raise RuntimeError('core journal receipt warehouse identity differs')
            if write_journal.read_snapshot_anchors(receipt_digest).keys() != _anchored_tables(plans):
                raise RuntimeError('core acknowledged snapshot anchors differ')
            if len(pending) == 1 and not any(completed) and _released_after_ack(pending[0][1], plans, receipt_ids):
                _complete_packet(plans, receipt_ids, paths)
                if journaled:
                    return stored
                paths.clear()
            if completed and all(completed):
                if set(paths) == receipt_repairs:
                    for path in paths:
                        write_journal.finish_intent(path)
                else:
                    _release_packet(plans, receipt_ids[0], paths)
                if journaled:
                    return stored
                paths.clear()  # an ordinary retry still verifies live data
                receipt_repairs.clear()
            verify_only, contested = _pre_upgrade_pending(
                plans, paths, descriptor_digest, acknowledged=True, receipt_ids=receipt_ids, run_id=run_id)
            verify_only |= bool(receipt_repairs)
            has_descriptor_evidence = alias or binding is not None or any(
                body['identity']['digest'] == receipt_digest
                or body['payload'].get('warehouse_identity') == identity for _, body in pending)

            def repair(*, receipt_only=False):
                # The acknowledged bundle has to change again (drifted rows, a
                # lost manifest, a deleted receipt): journal it first.
                if not paths:
                    paths.append(write_journal.save_intent(
                        {'run_id': run_id, 'digest': descriptor_digest, 'statement_protocol': 1,
                         'single_submit': True, 'scope_protocol': 1, 'packet_attempt': uuid4().hex},
                        {'plans': _descriptors(plans), 'warehouse_identity': identity,
                         **({'receipt_only': True} if receipt_only else {})},
                        run_id=run_id, universe_version=universe_version,
                        committed_at=stored['committed_at']))
                    if receipt_only:
                        receipt_repairs.add(paths[-1])

            _check_scope_owners(plans, receipt_ids[0])
            connection = _journal_connection(connection_factory)
            try:
                verify_only |= _finished_legacy_scopes(connection, plans, keys, run_id=run_id, acknowledged=True)
                _refuse_receipt_contradictions(
                    connection, plans, receipt_hashes, receipt_digest, run_id,
                    universe_version, batch, stored['committed_at'],
                    require_rowless=(receipt_digest != descriptor_digest and not has_descriptor_evidence))
                proofs = _commit_plans(
                    connection, writer, plans, repair=repair, receipt_ids=receipt_ids,
                    committed_at=stored['committed_at'], verify_only=verify_only, contested=contested,
                    historical_intent=historical_intent, run_id=run_id)
                if verify_only:
                    original = stored['plans']
                    if stored.get('receipt_readback') is not True:
                        raise RuntimeError('core journal physical proof differs')
                    if original and (len(original) != len(proofs) or any(
                            not _UNWRITTEN_PROOF.keys() <= old.keys()
                            or any(key not in proof or proof[key] != value for key, value in old.items())
                            for old, proof in zip(original, proofs))):
                        raise RuntimeError('core journal physical proof differs')
                plan_receipts = [
                    _plan_receipt(plan, proof, plan_hash, receipt_digest, run_id,
                                  universe_version, batch, stored['committed_at'])
                    for plan, proof, plan_hash in zip(plans, proofs, receipt_hashes, strict=True)
                ]
                held = _stored_receipts(connection, receipt_ids)
                if verify_only and not stored['plans']:
                    _verify_receipts(connection, plan_receipts)
                if len(held) < len(plan_receipts):
                    repair(receipt_only=verify_only)
                    if not verify_only:
                        write_journal.reserve_scopes((_target_scope(plan) for plan in plans), receipt_ids[0])
                _merge_receipts(connection, plan_receipts, repair=True)
                _verify_receipts(connection, plan_receipts)
                settled = not verify_only or _terminal_packet(historical_intent, plans, receipt_ids)
                if settled:
                    _clean_stages(connection, writer, plans, receipt_ids, run_id)
            finally:
                connection.close()
            # An alias copies an existing acknowledgement and its anchors
            # only after its bound warehouse identity has verified in full.
            if settled or not paths:
                _publish_alias(current, stored, receipt_hashes, receipt_digest)
            for path in receipt_repairs:
                write_journal.complete_intent(path, descriptor_digest, receipt_ids[0], terminal=True)
                write_journal.finish_intent(path)
                paths.remove(path)
            if settled:
                _complete_packet(plans, receipt_ids, paths)
            return stored

        # An interrupted write of this very unit left its intent behind: the
        # unit is committed with the metadata that intent names, and no second
        # intent is published for an identity that is already in flight.
        verify_only, contested = _pre_upgrade_pending(
            plans, paths, descriptor_digest, acknowledged=False, receipt_ids=receipt_ids, run_id=run_id)
        if pending:
            evidence = pending[0][1]['evidence']
            if any(body['evidence'] != evidence for _, body in pending):
                raise RuntimeError('core pending intent metadata differs')
            committed_at = evidence.get('committed_at', committed_at)
        elif not journaled:
            if write_journal.read_snapshot_anchors(digest):
                raise RuntimeError('core anchored acknowledgement is still pending')
            paths.append(write_journal.save_intent(
                {'run_id': run_id, 'digest': digest, 'statement_protocol': 1, 'single_submit': True,
                 'scope_protocol': 1, 'packet_attempt': uuid4().hex},
                {'plans': _descriptors(plans)},
                run_id=run_id, universe_version=universe_version, committed_at=committed_at,
            ))
        _check_scope_owners(plans, receipt_ids[0])
        connection = _journal_connection(connection_factory)
        try:
            verify_only |= _finished_legacy_scopes(connection, plans, keys, run_id=run_id,
                                                   historical_intent=historical_intent)
            _refuse_receipt_contradictions(
                connection, plans, plan_hashes, digest, run_id, universe_version, batch,
                committed_at)
            if pending and not write_journal.read_snapshot_anchors(digest):
                for path in paths:
                    write_journal._immutable_record(write_journal._record_path('fenced-from-start', path.stem),
                                                    {'intent': path.stem})
            write_journal.snapshot_anchors(digest, connection, _anchored_tables(plans))
            proofs = _commit_plans(
                connection, writer, plans, receipt_ids=receipt_ids, committed_at=committed_at,
                verify_only=verify_only, contested=contested, historical_intent=historical_intent, run_id=run_id)
            plan_receipts = [
                _plan_receipt(plan, proof, plan_hash, digest, run_id, universe_version,
                              batch, committed_at)
                for plan, proof, plan_hash in zip(plans, proofs, plan_hashes, strict=True)
            ]
            may_acknowledge = not verify_only or _terminal_packet(historical_intent, plans, receipt_ids)
            if may_acknowledge:
                _merge_receipts(connection, plan_receipts)
            _verify_receipts(connection, plan_receipts)
            if may_acknowledge:
                _clean_stages(connection, writer, plans, receipt_ids, run_id)
        finally:
            connection.close()
        receipt = {
            'verified': True, 'receipt_readback': True, 'run_id': run_id,
            'universe_version': universe_version, 'committed_at': committed_at,
            'plan_sha256': digest, 'committed_batch': batch, 'plans': proofs, 'manifests': [],
            # The plan hashes and digest the warehouse receipts were written under.
            'identity': {'plan_hashes': list(plan_hashes), 'digest': digest},
            'descriptor_digest': descriptor_digest,
            'walks': _journal_walks(plans, receipt_ids),
        }
        if not may_acknowledge:
            return receipt
        write_journal.record_receipt(digest, receipt)
        _publish_alias(current, receipt, plan_hashes, digest)
        # A pre-fence intent still guards the scope after read-only verification.
        _complete_packet(plans, receipt_ids, paths)
        return receipt


def _intent_unit(body):
    descriptors = body['payload']['plans']
    plans = [NativeWritePlan(
        **{**descriptor, 'natural_keys': tuple(descriptor['natural_keys']),
           'scope_columns': tuple(descriptor['scope_columns']),
           'scope_values': tuple(descriptor['scope_values'])})
        for descriptor in descriptors]
    run = body['identity']['run_id']
    hashes, digest = _bundle_identity(plans, run)
    saved = body['identity'].get('digest')
    if saved != digest and _journalled_by_stage_b(descriptors):
        hashes = [_stage_b_plan_sha256(plan) for plan in plans]
        digest = _digest({'plans': hashes, 'run_id': run})
    if saved != digest:
        raise RuntimeError('core journal intent digest is not recognised: ' + str(saved))
    return plans, hashes, digest


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
    with writer_lock():
        write_journal.check_dispatch_bindings()
        pending = write_journal.pending_intents({'run_id': run_id} if run_id else {})
    failures = {}
    while pending:
        deferred = []
        completed = set()
        for path, body in pending:
            with writer_lock():
                if not path.exists():
                    failures.pop(path, None)
                    continue  # another recovery finished this exact intent
                try:
                    plans, plan_hashes, digest = _intent_unit(body)
                    receipt = _commit_bundle(
                        connection_factory, writer, plans, plan_hashes=plan_hashes, digest=digest,
                        run_id=body['identity']['run_id'], journaled=True,
                        repair_identity=body['payload'].get('warehouse_identity'),
                        universe_version=body['evidence'].get('universe_version', 'core-universe-v1'),
                        committed_at=body['evidence'].get('committed_at', ''))
                except Exception as exc:
                    failures[path] = exc
                    if isinstance(exc, _ScopePending):
                        deferred.append((path, body))
                else:
                    receipts.append(receipt)
                    failures.pop(path, None)
                    warehouse = receipt.get('identity') or {'digest': digest, 'plan_hashes': plan_hashes}
                    completed.update((digest, warehouse['digest']))
                    completed.update(_receipt_id(h, receipt['run_id'], warehouse['digest'])
                                     for h in warehouse['plan_hashes'])
        # Only a foreign scope owner can have been released by another
        # member of this pass. Never repeat an ambiguous dispatch on error.
        pending = [(path, body) for path, body in deferred if failures[path].owner in completed]
    with writer_lock(), journalled_write():
        write_journal.check_dispatch_bindings()
        connection = None
        try:
            for path in (write_journal._root() / 'abandoned-stages').glob('*/*.json'):
                cleanup = write_journal._read_record(path)
                if run_id is None or cleanup['run_id'] == run_id:
                    if connection is None:
                        try:
                            connection = _journal_connection(connection_factory)
                        except Exception as exc:
                            logging.getLogger(__name__).warning('Deferred Transfermarkt stage cleanup (%s)',
                                                                type(exc).__name__)
                            break
                    _retire_abandoned_stage(connection, writer, path, cleanup['stage'])
        finally:
            if connection is not None:
                connection.close()
    if failures:
        error = next(iter(failures.values()))
        if len(failures) > 1:
            error = RuntimeError('unresolved core intents: ' + ', '.join(
                f'{path.stem} ({exc})' for path, exc in failures.items()))
        error.receipts = receipts
        error.unresolved = tuple(path.stem for path in failures)
        raise error
    return receipts
