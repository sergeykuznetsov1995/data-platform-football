"""Bounded, session-owned PostgreSQL authority for FotMob bronze writes."""
from __future__ import annotations

from contextlib import contextmanager
import logging
import math
import os
import select
import threading
import time
from typing import Callable, Iterator, Mapping

logger = logging.getLogger(__name__)
# Both connection establishment and each ownership probe have a hard IO deadline.
PROBE_SECONDS = 5.0
HEARTBEAT_SECONDS = 1.0


class WriterLockBusy(RuntimeError):
    """The shared FotMob writer lock could not be acquired."""


class WriterLockLost(WriterLockBusy):
    """This lease permanently lost authority; retrying it is unsafe."""


def _poll(connection, timeout: float) -> None:
    import psycopg2.extensions

    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("FotMob writer lock IO deadline exceeded")
        state = connection.poll()
        if state == psycopg2.extensions.POLL_OK:
            return
        if state == psycopg2.extensions.POLL_READ:
            ready = select.select([connection.fileno()], [], [], remaining)[0]
        elif state == psycopg2.extensions.POLL_WRITE:
            ready = select.select([], [connection.fileno()], [], remaining)[1]
        else:
            raise RuntimeError("unexpected PostgreSQL polling state")
        if not ready:
            raise TimeoutError("FotMob writer lock IO deadline exceeded")


def _connect(environ: Mapping[str, str], timeout: float):
    import psycopg2
    from scrapers.fbref.control.store import resolve_control_db_uri

    connection = psycopg2.connect(
        resolve_control_db_uri(environ), async_=True,
        connect_timeout=max(1, math.ceil(timeout)),
    )
    try:
        _poll(connection, timeout)
        return connection
    except BaseException:
        connection.close()
        raise


def _query(connection, sql: str, params: tuple, timeout: float):
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params)
        _poll(connection, timeout)
        return cursor.fetchone()
    finally:
        cursor.close()


class WriterLease:
    """An irrevocable right to write while this exact DB session owns the key."""

    def __init__(self, connection, key: int, heartbeat_seconds: float = HEARTBEAT_SECONDS):
        self._connection = connection
        self._key = key
        self._heartbeat_seconds = heartbeat_seconds
        self._probe_lock = threading.Lock()
        self._lost = threading.Event()
        self._reason = "FotMob writer lease ended"

    def _lose(self, reason: str) -> None:
        self._reason = reason
        self._lost.set()

    def check(self) -> None:
        if self._lost.is_set():
            raise WriterLockLost(self._reason)
        if not self._probe_lock.acquire(timeout=PROBE_SECONDS):
            self._lose("FotMob writer ownership probe timed out")
            raise WriterLockLost(self._reason)
        try:
            if self._lost.is_set():
                raise WriterLockLost(self._reason)
            # Advisory bigint keys occupy the high/low 32-bit fields. objsubid=1
            # distinguishes them from the independent two-int advisory namespace.
            row = _query(
                self._connection,
                "SELECT EXISTS (SELECT 1 FROM pg_locks "
                "WHERE locktype = 'advisory' AND pid = pg_backend_pid() "
                "AND database = (SELECT oid FROM pg_database "
                "WHERE datname = current_database()) "
                "AND classid = %s::oid AND objid = %s::oid "
                "AND objsubid = 1 AND mode = 'ExclusiveLock' AND granted)",
                ((self._key >> 32) & 0xffffffff, self._key & 0xffffffff),
                PROBE_SECONDS,
            )
            if not row or not row[0]:
                raise WriterLockLost("FotMob session no longer owns the writer lock")
        except Exception as exc:
            self._lose("FotMob writer lock ownership lost or unverifiable")
            raise WriterLockLost(self._reason) from exc
        finally:
            self._probe_lock.release()
        if self._lost.is_set():
            raise WriterLockLost(self._reason)

    @contextmanager
    def watch(self, cancel: Callable[[], object]) -> Iterator[None]:
        """Cancel active Trino SQL on loss, including while fetching results."""
        self.check()
        stopped = threading.Event()

        def heartbeat():
            while not stopped.wait(self._heartbeat_seconds):
                try:
                    self.check()
                except WriterLockLost:
                    try:
                        cancel()
                    except Exception:
                        logger.warning("FotMob SQL cancellation failed", exc_info=True)
                    return

        worker = threading.Thread(target=heartbeat, name="fotmob-writer-lease", daemon=True)
        worker.start()
        try:
            yield
        finally:
            stopped.set()
            # A worker can be in a bounded DB probe. Joining it avoids a late
            # cancellation being delivered to a cursor already used by new SQL.
            worker.join(PROBE_SECONDS * 2 + .1)
            if worker.is_alive():
                self._lose("FotMob writer heartbeat did not finish in time")
            self.check()


@contextmanager
def writer_lock(
    environ: Mapping[str, str] | None = None, *, key: int,
    wait_seconds: float, poll_seconds: float,
) -> Iterator[WriterLease | bool]:
    """Acquire the existing session lock; explicit offline aliases disable it."""
    env = os.environ if environ is None else environ
    if str(env.get("FOTMOB_WRITER_LOCK", "1")).strip().casefold() in {"0", "false", "no"}:
        yield False
        return
    if (not math.isfinite(wait_seconds) or not math.isfinite(poll_seconds)
            or wait_seconds < 0 or poll_seconds <= 0):
        raise ValueError("writer lock wait must be nonnegative and poll positive")
    connection = _connect(env, PROBE_SECONDS)
    lease = None
    try:
        deadline = time.monotonic() + wait_seconds
        while True:
            try:
                # Zero wait forbids waiting behind an owner, not waiting for
                # the one acquisition query to return from PostgreSQL.
                timeout = (PROBE_SECONDS if wait_seconds == 0 else
                           min(PROBE_SECONDS, max(.001, deadline - time.monotonic())))
                row = _query(connection, "SELECT pg_try_advisory_lock(%s)", (key,), timeout)
            except Exception as exc:
                raise WriterLockBusy("FotMob writer lock acquisition failed") from exc
            if row and row[0]:
                lease = WriterLease(connection, key, min(HEARTBEAT_SECONDS, poll_seconds))
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise WriterLockBusy(
                    "another FotMob writer holds the bronze writer lock "
                    f"(key {key}) after waiting {wait_seconds:.0f}s; "
                    "refusing to write in parallel"
                )
            time.sleep(min(poll_seconds, remaining))
        yield lease
    finally:
        if lease is not None:
            lease._lose("FotMob writer lease released")
        connection.close()
