"""Cross-process writer lock for SofaScore Bronze MERGEs (D5, issue #1218).

Every Bronze write already goes through Iceberg MERGE with a commit-conflict
retry (``TrinoTableManager._execute_committing``), but that retry is only
serialized inside one process (``_COMMIT_LOCK`` is a ``threading.Lock``).
Lesson 55 observed a silent loss with two concurrent writers (``pool=2``) and
the mechanism was never established, so a second writer lane (package #3:
second history gateway, player lane) needs a conservative premise: at most
one process commits to SofaScore Bronze at a time.  The lock is taken only
around the contiguous block of ``save_to_iceberg`` calls at the end of a
phase -- network capture stays parallel, commits are serialized -- so the
cost is seconds of waiting per commit block.

Unlike the FotMob lock (``run_fotmob_scraper._writer_lock``) this one waits:
a busy lock is polled with ``pg_try_advisory_lock`` until
``SOFASCORE_WRITER_LOCK_TIMEOUT_SECONDS`` elapse, because the second lane is
legitimate work that should queue, not fail.  Inside an active refresh scope
(``SOFASCORE_SCOPE_DEADLINE_EPOCH``, #2133) the wait is additionally capped
to that absolute deadline minus a 30-second completion reserve, so repeated
reacquisition cannot starve the scope past the task timeout Airflow would
kill it at.  The session-level advisory lock is released when the connection
closes, so a killed process leaves no stale lock behind.  The DSN is the
Airflow metadata DB the scraper process already carries in
``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``.
"""

from __future__ import annotations

import faulthandler
import hashlib
import json
import logging
import os
import sys
import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator, Mapping, Optional

logger = logging.getLogger(__name__)

WRITER_LOCK_ENV = "SOFASCORE_WRITER_LOCK"
WRITER_LOCK_TIMEOUT_ENV = "SOFASCORE_WRITER_LOCK_TIMEOUT_SECONDS"
WRITER_LOCK_DSN_ENV = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
DEFAULT_WRITER_LOCK_TIMEOUT_SECONDS = 1800
SCOPE_DEADLINE_ENV = "SOFASCORE_SCOPE_DEADLINE_EPOCH"
SCOPE_DEADLINE_RESERVE_SECONDS = 30.0
# #2062: the longest a process may hold the global Bronze key. Below the
# waiters' own timeout, so a stuck holder is dumped and killed before it fails
# every other SofaScore writer (measured legit holds: p99 306s, max 1316s).
WRITER_LOCK_MAX_HOLD_ENV = "SOFASCORE_WRITER_LOCK_MAX_HOLD_SECONDS"
DEFAULT_WRITER_LOCK_MAX_HOLD_SECONDS = 1500
WRITER_LOCK_HOLD_EXIT_CODE = 75
WRITER_LOCK_POLL_SECONDS = 5.0
WRITER_LOCK_KEY = int.from_bytes(
    hashlib.blake2b(b"sofascore-bronze-writer", digest_size=8).digest(),
    "big",
    signed=True,
)
_SQLALCHEMY_PREFIX = "postgresql+psycopg2://"


class WriterLockTimeout(RuntimeError):
    """Another SofaScore writer held the Bronze lock for the whole timeout."""


def _abort_stuck_holder(message: str) -> None:
    """Dump every thread's stack and exit, which closes the PG session."""

    try:
        logger.critical(message)
        for handler in logging.getLogger().handlers:
            try:
                handler.flush()
            except Exception:
                pass
        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        sys.stderr.flush()
    finally:
        # Diagnostics must never keep a stuck holder (and its lock) alive.
        os._exit(WRITER_LOCK_HOLD_EXIT_CODE)


def _writer_lock_dsn(env: Mapping[str, str]) -> str:
    dsn = str(env.get(WRITER_LOCK_DSN_ENV, "")).strip()
    if not dsn:
        raise RuntimeError(
            f"{WRITER_LOCK_DSN_ENV} is not set; the SofaScore bronze writer lock "
            f"cannot be taken (set {WRITER_LOCK_ENV}=0 only for offline replay)"
        )
    if dsn.startswith(_SQLALCHEMY_PREFIX):
        dsn = "postgresql://" + dsn[len(_SQLALCHEMY_PREFIX):]
    return dsn


@contextmanager
def bronze_writer_lock(
    environ: Optional[Mapping[str, str]] = None,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], float] = time.time,
    _lock_key: int = WRITER_LOCK_KEY,
    _abort: Callable[[str], None] = _abort_stuck_holder,
    _timer: Optional[Callable[..., threading.Timer]] = None,
) -> Iterator[bool]:
    """Hold the SofaScore Bronze writer lock for the duration of the block.

    Yields ``True`` once the advisory lock is held, or ``False`` when the lock
    is explicitly disabled (``SOFASCORE_WRITER_LOCK=0|false|no``).  There is
    no silent bypass: a missing DSN with the lock enabled fails closed.
    """

    env = os.environ if environ is None else environ
    if str(env.get(WRITER_LOCK_ENV, "1")).strip().casefold() in {"0", "false", "no"}:
        logger.warning(
            "SofaScore writer lock disabled via %s — parallel Bronze writes "
            "are not serialized",
            WRITER_LOCK_ENV,
        )
        yield False
        return

    dsn = _writer_lock_dsn(env)
    timeout = int(
        env.get(WRITER_LOCK_TIMEOUT_ENV, DEFAULT_WRITER_LOCK_TIMEOUT_SECONDS)
    )
    scope_deadline = str(env.get(SCOPE_DEADLINE_ENV, "")).strip()
    if scope_deadline:
        remaining = (
            float(scope_deadline) - wall_clock() - SCOPE_DEADLINE_RESERVE_SECONDS
        )
        timeout = min(timeout, max(0, int(remaining)))

    # Only the global key is bounded: a season key legitimately spans capture.
    max_hold = (
        float(env.get(WRITER_LOCK_MAX_HOLD_ENV, DEFAULT_WRITER_LOCK_MAX_HOLD_SECONDS))
        if _lock_key == WRITER_LOCK_KEY else None
    )

    import psycopg2

    connection = psycopg2.connect(dsn)
    watchdog = None
    try:
        connection.autocommit = True
        started = clock()
        warned = False
        while True:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(%s)", (_lock_key,))
                acquired = bool(cursor.fetchone()[0])
            waited = clock() - started
            if acquired:
                if waited:
                    logger.info(
                        "SofaScore bronze writer lock (key %s) acquired after %.0fs",
                        _lock_key, waited,
                    )
                break
            if waited >= timeout:
                raise WriterLockTimeout(
                    "another SofaScore writer held the bronze writer lock "
                    f"(key {_lock_key}) for {timeout}s; giving up"
                )
            if not warned:
                warned = True
                logger.warning(
                    "SofaScore bronze writer lock (key %s) is busy; waiting up to %ss",
                    _lock_key, timeout,
                )
            sleep(WRITER_LOCK_POLL_SECONDS)
        if max_hold is not None and max_hold > 0:
            watchdog = (_timer or threading.Timer)(max_hold, _abort, args=(
                f"SofaScore bronze writer lock hold limit exceeded: key {_lock_key} "
                f"held for {max_hold:.0f}s ({WRITER_LOCK_MAX_HOLD_ENV}); dumping "
                "stacks and exiting to release it",
            ))
            watchdog.daemon = True
            watchdog.start()
        yield True
    finally:
        if watchdog is not None:
            watchdog.cancel()
        # Closing the session releases the advisory lock; an explicit UNLOCK
        # would not run when the process dies.
        connection.close()


@contextmanager
def season_capture_lock(
    tournament_id: str | int,
    season_id: str | int,
    freshness_key: str,
    environ: Optional[Mapping[str, str]] = None,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Iterator[bool]:
    """Serialize check→capture→raw/manifest flush for one season freshness.

    Unlike the Bronze writer key, this namespace preserves parallel capture
    of other seasons. PostgreSQL releases the session lock on process death;
    no new shared filesystem mount or live lockfile migration is needed.
    """
    identity = ["sofascore-season-capture-v1", str(tournament_id), str(season_id), freshness_key]
    if not all(isinstance(value, str) and value.strip() for value in identity):
        raise ValueError("season capture lock identity must not be empty")
    key = int.from_bytes(hashlib.blake2b(
        json.dumps(identity, separators=(",", ":")).encode(), digest_size=8,
    ).digest(), "big", signed=True)
    with bronze_writer_lock(environ, sleep=sleep, clock=clock, _lock_key=key) as acquired:
        yield acquired
