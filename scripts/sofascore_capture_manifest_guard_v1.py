#!/usr/bin/env python3
"""Temporary v1 lifecycle guard for a large SofaScore player run.

The process holds one non-blocking file lock for its whole lifetime. Every
cycle opens a fresh lightweight Trino connection, logs manifest metrics,
fully compacts the static ops table, expires old snapshots with a zero-second
session floor, logs the resulting metrics, and closes the connection.
Authenticated Trino HTTPS verifies certificates by default and honors the
repository-standard ``TRINO_TLS_VERIFY``, ``REQUESTS_CA_BUNDLE``, and
``SSL_CERT_FILE`` environment contract.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import math
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence


GUARD_VERSION = "sofascore-capture-manifest-guard-v1"
DEFAULT_INTERVAL_SECONDS = 300
DEFAULT_MAX_CONSECUTIVE_FAILURES = 2
HARD_MAX_SNAPSHOTS = 2_000
DEFAULT_MAX_SNAPSHOTS = HARD_MAX_SNAPSHOTS
DEFAULT_LOCK_FILE = Path("/tmp/sofascore-capture-manifest-guard-v1.lock")

MANIFEST_SQL = 'iceberg."ops"."sofascore_capture_manifest"'
SNAPSHOTS_SQL = 'iceberg."ops"."sofascore_capture_manifest$snapshots"'
FILES_SQL = 'iceberg."ops"."sofascore_capture_manifest$files"'

logger = logging.getLogger("sofascore_capture_manifest_guard_v1")


class GuardAlreadyRunning(RuntimeError):
    """Another process owns the lifecycle guard lock."""


class SnapshotLimitExceeded(RuntimeError):
    """Manifest history exceeded the hard operational safety bound."""


def _emit(event: str, *, level: int = logging.INFO, **fields: Any) -> None:
    payload = {
        "event": event,
        "guard_version": GUARD_VERSION,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    logger.log(level, "%s", json.dumps(payload, sort_keys=True, default=str))


def _get_trino_connection():
    """Connect without importing Airflow or any scraper package."""

    import trino

    host = os.environ.get("TRINO_HOST", "trino")
    default_port = "8443" if os.environ.get("TRINO_PASSWORD") else "8080"
    port = int(os.environ.get("TRINO_PORT", default_port))
    user = os.environ.get("TRINO_USER", "airflow")
    password = os.environ.get("TRINO_PASSWORD")
    kwargs: dict[str, Any] = {
        "host": host,
        "port": port,
        "user": user,
        "catalog": "iceberg",
        "session_properties": {"enable_dynamic_filtering": "false"},
    }
    if password:
        kwargs.update(
            {
                "http_scheme": "https",
                "auth": trino.auth.BasicAuthentication(user, password),
                "verify": _trino_tls_verify(),
            }
        )
    return trino.dbapi.connect(**kwargs)


def _trino_tls_verify() -> bool | str:
    """Resolve the repository's fail-closed Trino TLS verification contract."""

    raw = os.environ.get("TRINO_TLS_VERIFY", "true").strip().casefold()
    if raw in {"0", "false", "no"}:
        _emit(
            "tls_verification_disabled",
            level=logging.WARNING,
            reason="explicit_dev_only_opt_out",
        )
        return False
    if raw not in {"1", "true", "yes"}:
        raise RuntimeError(
            "TRINO_TLS_VERIFY must be one of true/false, yes/no, or 1/0"
        )
    for variable in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        bundle = os.environ.get(variable, "").strip()
        if bundle:
            return bundle
    return True


def _fetch_one(conn, sql: str) -> tuple[Any, ...]:
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
    finally:
        cursor.close()
    if len(rows) != 1:
        raise RuntimeError(f"expected one Trino row, received {len(rows)}")
    return tuple(rows[0])


def _execute(conn, sql: str) -> list[tuple[Any, ...]]:
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def query_manifest_stats(conn) -> dict[str, int]:
    """Query current row, snapshot, and live data-file metrics."""

    row_count_row = _fetch_one(conn, f"SELECT count(*) FROM {MANIFEST_SQL}")
    snapshot_count_row = _fetch_one(
        conn,
        f"SELECT count(*) FROM {SNAPSHOTS_SQL}",
    )
    files_row = _fetch_one(
        conn,
        "SELECT count_if(content = 0), "
        "coalesce(sum(IF(content = 0, file_size_in_bytes, 0)), 0) "
        f"FROM {FILES_SQL}",
    )
    if len(row_count_row) != 1 or len(snapshot_count_row) != 1 or len(files_row) != 2:
        raise RuntimeError("manifest stats row has an unexpected shape")
    stats = {
        "row_count": int(row_count_row[0]),
        "snapshot_count": int(snapshot_count_row[0]),
        "live_data_file_count": int(files_row[0] or 0),
        "live_data_file_bytes": int(files_row[1] or 0),
    }
    if any(value < 0 for value in stats.values()):
        raise RuntimeError("manifest stats cannot be negative")
    return stats


def run_cycle(
    *,
    connection_factory: Callable[[], Any] | None = None,
    max_snapshots: int = DEFAULT_MAX_SNAPSHOTS,
) -> dict[str, Mapping[str, int]]:
    """Run one fail-closed maintenance cycle and always close its connection."""

    _validate_snapshot_limit(max_snapshots)
    conn = (connection_factory or _get_trino_connection)()
    try:
        _execute(
            conn,
            "SET SESSION iceberg.expire_snapshots_min_retention = '0s'",
        )
        before = query_manifest_stats(conn)
        _emit("cycle_before", metrics=before)
        _enforce_snapshot_limit(before, stage="before", limit=max_snapshots)

        _execute(conn, f"ALTER TABLE {MANIFEST_SQL} EXECUTE optimize")
        _execute(
            conn,
            f"ALTER TABLE {MANIFEST_SQL} EXECUTE expire_snapshots("
            "retention_threshold => '0s')",
        )
        after = query_manifest_stats(conn)
        _emit("cycle_after", metrics=after)
        _enforce_snapshot_limit(after, stage="after", limit=max_snapshots)
        return {"before": before, "after": after}
    finally:
        conn.close()


def _validate_snapshot_limit(value: object) -> int:
    if type(value) is not int or not 1 <= value <= HARD_MAX_SNAPSHOTS:
        raise ValueError(
            f"max_snapshots must be an integer from 1 to {HARD_MAX_SNAPSHOTS}"
        )
    return value


def _snapshot_limit_argument(value: str) -> int:
    try:
        parsed = int(value)
        return _validate_snapshot_limit(parsed)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _enforce_snapshot_limit(
    metrics: Mapping[str, int],
    *,
    stage: str,
    limit: int,
) -> None:
    snapshot_count = int(metrics["snapshot_count"])
    if snapshot_count > limit:
        raise SnapshotLimitExceeded(
            "SofaScore capture manifest snapshot count "
            f"{snapshot_count} exceeds {limit} at {stage} maintenance stats"
        )


@contextmanager
def single_instance_lock(path: Path) -> Iterator[None]:
    """Hold an exclusive non-blocking advisory lock until the guard exits."""

    lock_path = Path(path)
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    handle = os.fdopen(descriptor, "a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise GuardAlreadyRunning(
                f"SofaScore capture manifest guard already owns {lock_path}"
            ) from exc
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def supervise(
    *,
    once: bool = False,
    interval: int = DEFAULT_INTERVAL_SECONDS,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    stop_file: Path | None = None,
    cycle: Callable[[], Mapping[str, Any]] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> int:
    """Run cycles on fixed monotonic start deadlines until a terminal event."""

    if type(interval) is not int or interval <= 0:
        raise ValueError("interval must be a positive integer")
    if type(max_consecutive_failures) is not int or max_consecutive_failures <= 0:
        raise ValueError("max_consecutive_failures must be a positive integer")

    cycle_callable = cycle or run_cycle
    consecutive_failures = 0
    next_start_deadline = monotonic()
    while True:
        if stop_file is not None and Path(stop_file).exists():
            _emit("stop_file_observed", stop_file=str(stop_file))
            return 0

        remaining = next_start_deadline - monotonic()
        if remaining > 0:
            sleep(remaining)
        if stop_file is not None and Path(stop_file).exists():
            _emit("stop_file_observed", stop_file=str(stop_file))
            return 0

        try:
            cycle_callable()
        except SnapshotLimitExceeded as exc:
            _emit("snapshot_limit_exceeded", level=logging.ERROR, error=str(exc))
            return 1
        except Exception as exc:
            consecutive_failures += 1
            _emit(
                "cycle_failed",
                level=logging.ERROR,
                consecutive_failures=consecutive_failures,
                error=f"{type(exc).__name__}: {exc}",
            )
            if once:
                _emit(
                    "once_cycle_failed",
                    level=logging.ERROR,
                    consecutive_failures=consecutive_failures,
                )
                return 1
            if consecutive_failures >= max_consecutive_failures:
                _emit(
                    "failure_threshold_reached",
                    level=logging.ERROR,
                    consecutive_failures=consecutive_failures,
                )
                return 1
        else:
            consecutive_failures = 0
            if once:
                return 0

        next_start_deadline += interval
        observed = monotonic()
        if next_start_deadline < observed:
            skipped_intervals = math.ceil(
                (observed - next_start_deadline) / interval
            )
            next_start_deadline += skipped_intervals * interval
            _emit(
                "cadence_deadline_missed",
                level=logging.WARNING,
                skipped_intervals=skipped_intervals,
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Seconds between cycle starts (default: 300).",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=DEFAULT_MAX_CONSECUTIVE_FAILURES,
        help="Exit non-zero after this many consecutive cycle failures (default: 2).",
    )
    parser.add_argument(
        "--max-snapshots",
        type=_snapshot_limit_argument,
        default=DEFAULT_MAX_SNAPSHOTS,
        help=(
            "Exit non-zero immediately if the pre-cycle snapshot count "
            f"exceeds this bound (allowed range: 1..{HARD_MAX_SNAPSHOTS})."
        ),
    )
    parser.add_argument(
        "--stop-file",
        type=Path,
        help="Exit cleanly before the next cycle once this path exists.",
    )
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=DEFAULT_LOCK_FILE,
        help=f"Single-instance lock path (default: {DEFAULT_LOCK_FILE}).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _validate_snapshot_limit(args.max_snapshots)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        with single_instance_lock(args.lock_file):
            _emit("guard_started", lock_file=str(args.lock_file))
            return supervise(
                once=args.once,
                interval=args.interval,
                max_consecutive_failures=args.max_consecutive_failures,
                stop_file=args.stop_file,
                cycle=lambda: run_cycle(max_snapshots=args.max_snapshots),
            )
    except GuardAlreadyRunning as exc:
        _emit("lock_unavailable", level=logging.ERROR, error=str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
