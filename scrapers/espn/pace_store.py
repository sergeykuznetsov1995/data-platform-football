"""Persistent controller observations and serial history write timings (#1510)."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import sqlite3


class ControllerBusy(RuntimeError):
    pass


class PaceStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.db() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS evidence (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, at REAL NOT NULL, payload TEXT NOT NULL)')
            db.execute('CREATE INDEX IF NOT EXISTS evidence_window ON evidence(kind, at)')

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextmanager
    def lock(self):
        fd = os.open(str(self.path) + '.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ControllerBusy('another ESPN pace controller owns the runtime lock') from None
            yield
        finally:
            os.close(fd)

    def get(self):
        with self.db() as db:
            row = db.execute("SELECT value FROM state WHERE key='controller'").fetchone()
        return json.loads(row[0]) if row else None

    def save(self, state):
        payload = json.dumps(state, allow_nan=False, sort_keys=True)
        with self.db() as db:
            db.execute("INSERT OR REPLACE INTO state VALUES ('controller', ?)", (payload,))

    def save_with_evidence(self, state, kind, at, **evidence):
        """Commit a window decision and its audit record as one durable change."""
        payload = json.dumps(state, allow_nan=False, sort_keys=True)
        audit = json.dumps(evidence, allow_nan=False, sort_keys=True)
        with self.db() as db:
            db.execute('INSERT INTO evidence(kind, at, payload) VALUES (?, ?, ?)',
                       (kind, float(at), audit))
            db.execute("INSERT OR REPLACE INTO state VALUES ('controller', ?)", (payload,))

    def record(self, kind, at, **payload):
        with self.db() as db:
            db.execute('INSERT INTO evidence(kind, at, payload) VALUES (?, ?, ?)',
                       (kind, float(at), json.dumps(payload, allow_nan=False, sort_keys=True)))

    def rows(self, kind, start, end):
        with self.db() as db:
            return [dict(at=at, **json.loads(payload)) for at, payload in db.execute(
                'SELECT at, payload FROM evidence WHERE kind=? AND at>=? AND at<=? ORDER BY at, id',
                (kind, float(start), float(end)))]

    def latest(self, kind):
        with self.db() as db:
            row = db.execute('SELECT at, payload FROM evidence WHERE kind=? ORDER BY at DESC, id DESC LIMIT 1',
                             (kind,)).fetchone()
        return dict(at=row[0], **json.loads(row[1])) if row else None
