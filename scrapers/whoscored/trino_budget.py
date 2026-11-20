"""Per-table pacing and shared history inventory on the local Airflow log volume."""

from __future__ import annotations

import fcntl
import hashlib
import json
import sqlite3
import time
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path

INTERVAL = 60
# Keep a crashed writer fenced for the source query's maximum lifetime.
WRITE_TIMEOUT = 45 * 60
_BOOT = Path("/proc/sys/kernel/random/boot_id")
# Stored in the existing slots.active column. Values 0/1 also occur in older files.
_COOLING, _IN_FLIGHT, _UNCERTAIN = 0, 1, 2


class TrinoBudget:
    def __init__(self, root, *, clock=time.monotonic, sleep=time.sleep):
        self.root = Path(root)
        self.clock = clock
        self.sleep = sleep

    @contextmanager
    def _locked(self, keys):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with ExitStack() as stack:
            names = set()
            for key in keys:
                digest = hashlib.sha256(key.encode()).hexdigest()
                # Stage tables have unique per-write suffixes. Bound their lock files.
                names.add("stage-" + digest[:2] if "__stg_" in key else digest)
            for name in sorted(names):
                handle = stack.enter_context((self.root / (name + ".lock")).open("ab"))
                fcntl.flock(handle, fcntl.LOCK_EX)
            yield

    @contextmanager
    def _db(self):
        with closing(sqlite3.connect(self.root / "budget.sqlite3", timeout=30)) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS slots (key TEXT PRIMARY KEY, boot TEXT NOT NULL, "
                "ready REAL NOT NULL, active INTEGER NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS snapshots (key TEXT PRIMARY KEY, boot TEXT NOT NULL, "
                "sampled REAL NOT NULL, rows TEXT NOT NULL)"
            )
            with db:
                yield db

    @contextmanager
    def statement(self, keys, *, rejected=None):
        """Hold each relation through server completion, including all result pages."""
        keys = sorted(set(keys))
        if not keys:
            yield
            return
        boot = _BOOT.read_text().strip()
        while True:
            with self._locked(keys):
                now = self.clock()
                ready = now
                with self._db() as db:
                    for key in keys:
                        row = db.execute(
                            "SELECT boot,ready,active FROM slots WHERE key=?", (key,)
                        ).fetchone()
                        if row is None:
                            continue
                        old_boot, due, active = row
                        if old_boot != boot or active == _IN_FLIGHT:
                            # We own the flock. A former holder could have submitted
                            # just before it died, long after acquiring the slot.
                            uncertain = bool(active) and key.startswith("commit:")
                            recovered = (
                                now + INTERVAL + (WRITE_TIMEOUT if uncertain else 0)
                            )
                            due = recovered if old_boot != boot else max(due, recovered)
                            state = _UNCERTAIN if uncertain else _COOLING
                            db.execute(
                                "UPDATE slots SET boot=?,ready=?,active=? WHERE key=?",
                                (boot, due, state, key),
                            )
                        ready = max(ready, due)
                    if ready <= now:
                        db.execute(
                            "DELETE FROM slots WHERE boot=? AND active!=? AND ready<=?",
                            (boot, _IN_FLIGHT, now),
                        )
                        db.executemany(
                            "INSERT OR REPLACE INTO slots VALUES (?,?,?,1)",
                            [
                                (
                                    key,
                                    boot,
                                    now
                                    + INTERVAL
                                    + (
                                        WRITE_TIMEOUT
                                        if key.startswith("commit:")
                                        else 0
                                    ),
                                )
                                for key in keys
                            ],
                        )
                if ready <= now:
                    uncertain = set()
                    try:
                        yield
                    except BaseException as exc:
                        if rejected is None or not rejected(exc):
                            uncertain = {
                                key for key in keys if key.startswith("commit:")
                            }
                        raise
                    finally:
                        # The last POST may have happened just before the error.
                        # Keep its full server lifetime, including across reboots.
                        done = self.clock() + INTERVAL
                        updates = [
                            (done + WRITE_TIMEOUT, _UNCERTAIN, key)
                            if key in uncertain
                            else (done, _COOLING, key)
                            for key in keys
                        ]
                        with self._db() as db:
                            db.executemany(
                                "UPDATE slots SET ready=?,active=? WHERE key=?",
                                updates,
                            )
                    return
            # No relation lock or DB transaction is held during the cooldown.
            self.sleep(max(0, ready - self.clock()))

    def snapshot(self, key, load):
        """Coalesce one JSON-safe aggregate query, including concurrent first readers."""
        boot = _BOOT.read_text().strip()
        with self._locked(["snapshot:" + key]):
            with self._db() as db:
                row = db.execute(
                    "SELECT boot,sampled,rows FROM snapshots WHERE key=?", (key,)
                ).fetchone()
            if row and row[0] == boot and 0 <= self.clock() - row[1] < INTERVAL:
                return json.loads(row[2])
            rows = load()
            payload = json.dumps(rows, separators=(",", ":"), allow_nan=False)
            with self._db() as db:
                db.execute(
                    "INSERT OR REPLACE INTO snapshots VALUES (?,?,?,?)",
                    (key, boot, self.clock(), payload),
                )
            return rows
