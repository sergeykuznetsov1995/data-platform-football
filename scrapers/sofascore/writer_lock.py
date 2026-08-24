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
legitimate work that should queue, not fail.  The session-level advisory lock
is released when the connection closes, so a killed process leaves no stale
lock behind.  The DSN is the Airflow metadata DB the scraper process already
carries in ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from contextlib import contextmanager
from typing import Callable, Iterator, Mapping, Optional

logger = logging.getLogger(__name__)

WRITER_LOCK_ENV = "SOFASCORE_WRITER_LOCK"
WRITER_LOCK_TIMEOUT_ENV = "SOFASCORE_WRITER_LOCK_TIMEOUT_SECONDS"
WRITER_LOCK_DSN_ENV = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
DEFAULT_WRITER_LOCK_TIMEOUT_SECONDS = 1800
WRITER_LOCK_POLL_SECONDS = 5.0
WRITER_LOCK_KEY = int.from_bytes(
    hashlib.blake2b(b"sofascore-bronze-writer", digest_size=8).digest(),
    "big",
    signed=True,
)
_SQLALCHEMY_PREFIX = "postgresql+psycopg2://"


class WriterLockTimeout(RuntimeError):
    """Another SofaScore writer held the Bronze lock for the whole timeout."""


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

    import psycopg2

    connection = psycopg2.connect(dsn)
    try:
        connection.autocommit = True
        started = clock()
        while True:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(%s)", (WRITER_LOCK_KEY,))
                acquired = bool(cursor.fetchone()[0])
            waited = clock() - started
            if acquired:
                if waited:
                    logger.info(
                        "SofaScore bronze writer lock acquired after %.0fs", waited
                    )
                break
            if waited >= timeout:
                raise WriterLockTimeout(
                    "another SofaScore writer held the bronze writer lock "
                    f"(key {WRITER_LOCK_KEY}) for {timeout}s; giving up"
                )
            if not waited:
                logger.warning(
                    "SofaScore bronze writer lock is busy; waiting up to %ss", timeout
                )
            sleep(WRITER_LOCK_POLL_SECONDS)
        yield True
    finally:
        # Closing the session releases the advisory lock; an explicit UNLOCK
        # would not run when the process dies.
        connection.close()
