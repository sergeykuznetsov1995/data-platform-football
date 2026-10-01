"""Durable attempt outbox. SQLite commits precede HTTP and Trino publication.

One shared file beside gate.json, WAL + FULL synchronous, survives worker death.
Unfinished starts remain visible forever: never infer a complete window from
successful requests alone. Flush retries MERGE by attempt_id under a process-
shared publication lock; rows are retained locally for coverage/recovery.
"""
from __future__ import annotations

import json
import fcntl
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .journal import _execute, _literal
from .pace import utc

ATTEMPT_TABLE = 'iceberg.ops.espn_http_attempt_v1'
ATTEMPT_COLUMNS = (
    ('attempt_id', 'varchar'), ('run_id', 'varchar'), ('task_id', 'varchar'),
    ('requested_at', 'timestamp(6)'), ('origin', 'varchar'),
    ('endpoint', 'varchar'), ('lane', 'varchar'), ('step', 'integer'),
    ('status', 'integer'), ('timeout', 'boolean'), ('http_ms', 'double'),
    ('direct_bytes', 'bigint'), ('complete', 'boolean'),
    ('measurement_id', 'varchar'),
)


class AttemptJournal:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY, requested_at TEXT NOT NULL, payload TEXT NOT NULL, dirty INTEGER NOT NULL)')
            db.execute('CREATE INDEX IF NOT EXISTS attempts_requested_at ON attempts(requested_at)')
            db.execute('CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.execute('INSERT OR IGNORE INTO metadata VALUES (?, ?)', ('created_at', datetime.now(timezone.utc).isoformat()))

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=120)
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    def begin(self, *, run_id, task_id, requested_at, origin, endpoint, lane, step, measurement_id=None):
        row = dict(attempt_id=uuid.uuid4().hex, run_id=run_id, task_id=task_id,
                   requested_at=utc(requested_at).isoformat(), origin=origin,
                   endpoint=endpoint, lane=lane, step=step, status=None,
                   timeout=False, http_ms=None, direct_bytes=0, complete=False,
                   measurement_id=measurement_id)
        # Store only approved origin and endpoint labels, never URL/query/error.
        from .transport_contracts import normalize_transport_origin, EndpointType
        row['origin'] = normalize_transport_origin(origin)
        row['endpoint'] = EndpointType.parse(endpoint).value
        with self._db() as db:
            db.execute('INSERT INTO attempts VALUES (?, ?, ?, 1)',
                       (row['attempt_id'], row['requested_at'], json.dumps(row)))
        return row['attempt_id']

    def finish(self, attempt_id, *, status, timeout, http_ms, direct_bytes):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            found = db.execute('SELECT payload FROM attempts WHERE id=?', (attempt_id,)).fetchone()
            if found is None:
                raise RuntimeError('attempt start missing; coverage lost')
            row = json.loads(found[0])
            row.update(status=status, timeout=bool(timeout), http_ms=http_ms,
                       direct_bytes=direct_bytes, complete=True)
            db.execute('UPDATE attempts SET payload=?, dirty=1 WHERE id=?', (json.dumps(row), attempt_id))

    def rows(self):
        with self._db() as db:
            return [json.loads(r[0]) for r in db.execute('SELECT payload FROM attempts ORDER BY requested_at, rowid')]

    def coverage(self, start, end):
        """Local completeness only; PR B must additionally prove load/freshness."""
        start, end = utc(start), utc(end)
        with self._db() as db:
            created = datetime.fromisoformat(db.execute("SELECT value FROM metadata WHERE key='created_at'").fetchone()[0])
            rows = [json.loads(r[0]) for r in db.execute('SELECT payload FROM attempts WHERE requested_at>=? AND requested_at<?', (start.isoformat(), end.isoformat()))]
        return created <= start < end and bool(rows) and all(
            r['complete'] and r['http_ms'] is not None and (r['status'] is not None or r['timeout']) for r in rows
        )

    def flush(self, conn):
        """Recover all workers' dirty starts/results, idempotently, in batches.

        A separate flock serializes publishers without blocking HTTP recording. If Trino commits and
        the caller dies before SQLite acknowledgement, replay updates that id.
        Incomplete starts are published too and subsequently updated on finish.
        """
        _execute(conn, 'CREATE SCHEMA IF NOT EXISTS iceberg.ops')
        columns = ', '.join(f'{name} {kind}' for name, kind in ATTEMPT_COLUMNS)
        _execute(conn, f'CREATE TABLE IF NOT EXISTS {ATTEMPT_TABLE} ({columns})')
        descriptor = os.open(str(self.path) + '.publish.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            return self._flush_locked(conn)
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _flush_locked(self, conn):
        count = 0
        while True:
            with self._db() as db:
                pending = db.execute('SELECT id, payload FROM attempts WHERE dirty=1 LIMIT 200').fetchall()
            if not pending:
                return count
            rows = [json.loads(payload) for _, payload in pending]
            values = []
            for row in rows:
                row['requested_at'] = datetime.fromisoformat(row['requested_at'])
                values.append('(' + ', '.join(
                    ('TRUE' if row[name] else 'FALSE') if kind == 'boolean' else _literal(row[name], kind)
                    for name, kind in ATTEMPT_COLUMNS
                ) + ')')
            names = ', '.join(name for name, _ in ATTEMPT_COLUMNS)
            updates = ', '.join(f'{name}=s.{name}' for name, _ in ATTEMPT_COLUMNS if name != 'attempt_id')
            inserts = ', '.join(f's.{name}' for name, _ in ATTEMPT_COLUMNS)
            sql = (f'MERGE INTO {ATTEMPT_TABLE} t USING (VALUES ' + ', '.join(values)
                   + f') s ({names}) ON t.attempt_id=s.attempt_id '
                   + f'WHEN MATCHED THEN UPDATE SET {updates} '
                   + f'WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({inserts})')
            _execute(conn, sql)
            with self._db() as db:
                # An in-flight request may have finished during publication.
                # Acknowledge only the exact version sent, then replay updates.
                db.executemany('UPDATE attempts SET dirty=0 WHERE id=? AND payload=?', pending)
            count += len(pending)
