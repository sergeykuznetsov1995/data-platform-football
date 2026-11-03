"""Nightly lifecycle for the ``iceberg.ops`` journals (#1892).

Source journals in ``ops`` are MERGEd every 30–60 s around the clock, so they
accumulate small data files, delete files and snapshots. Bronze compaction
skips them (no partitions + live delete files), therefore each table gets a
FULL ``optimize`` (applies delete files; the tables are small) followed by
``expire_snapshots``. ``remove_orphan_files`` is intentionally never run:
active writers may own files not yet referenced by committed metadata (same
contract as ``maintain_sofascore_capture_manifest``).

A commit conflict with a concurrent writer is retried; a table that stays
busy is recorded as ``busy`` and the pass continues with the next table.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
import random
import re
import time

from utils import maintenance_tasks as maintenance

OPS_SCHEMA = "ops"
OPS_RETENTION = "3d"
COMMIT_ATTEMPTS = 4
_IDENTIFIER_RE = re.compile(r"^[a-z0-9_]+$")


def _now():
    return datetime.now(timezone.utc)


def _is_commit_conflict(exc: BaseException) -> bool:
    cause = exc
    while cause is not None:
        message = str(cause)
        if (getattr(cause, "error_name", None) == "ICEBERG_COMMIT_ERROR"
                or "ICEBERG_COMMIT_ERROR" in message
                or "conflicting files" in message
                or "Failed to commit the transaction" in message):
            return True
        cause = cause.__cause__
    return False


def _scalar(conn, sql: str) -> int:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
    return int(rows[0][0])


def _metrics(conn, schema: str, table: str) -> dict[str, int]:
    return {
        "files": _scalar(conn, f'SELECT count(*) FROM iceberg."{schema}"."{table}$files"'),
        "snapshots": _scalar(conn, f'SELECT count(*) FROM iceberg."{schema}"."{table}$snapshots"'),
    }


def _execute(conn, sql: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cur.fetchall()
    finally:
        cur.close()


def _alter_with_retry(conn, sql: str, deadline: datetime, sleep) -> bool:
    """Run one ALTER bounded by the deadline; False when commits stay busy."""
    for attempt in range(COMMIT_ATTEMPTS):
        remaining = math.floor((deadline - _now()).total_seconds())
        if remaining < 1:
            raise TimeoutError("ops maintenance deadline reached")
        # Trino cancels the statement itself instead of overrunning the window.
        _execute(conn, f"SET SESSION query_max_run_time = '{remaining}s'")
        try:
            _execute(conn, sql)
            return True
        except Exception as exc:
            if _now() >= deadline:
                raise TimeoutError("ops maintenance deadline reached") from exc
            if not _is_commit_conflict(exc):
                raise
            if attempt + 1 == COMMIT_ATTEMPTS:
                return False
            delay = min(8.0, 2.0 ** attempt) + random.uniform(0, 1)
            if (deadline - _now()).total_seconds() <= delay:
                raise TimeoutError("ops maintenance deadline reached") from exc
            sleep(delay)
    return False


def maintain_ops_tables(
    connection_factory,
    *,
    schema: str = OPS_SCHEMA,
    retention: str = OPS_RETENTION,
    exclude=(),
    only=None,
    deadline: datetime,
    stop_file=None,
    journal=None,
    dry_run: bool = True,
    sleep=time.sleep,
) -> dict:
    """Optimize + expire every BASE TABLE of ``schema``, fattest first.

    Returns a numeric summary (no exception texts): per-table status
    ``ok``/``busy``/``failed``/``planned``, ``stop_reason`` and ``exit_code``
    (0 unless a table failed or every attempted table stayed busy).
    """
    if not _IDENTIFIER_RE.fullmatch(schema):
        raise ValueError("schema must be a plain identifier")
    if not maintenance._RETENTION_THRESHOLD_RE.fullmatch(retention):
        raise ValueError("retention must be a positive Trino duration")
    if deadline.tzinfo is None:
        raise ValueError("deadline must be timezone-aware")
    stop = Path(stop_file) if stop_file else None
    summary = dict(event="summary", schema=schema, retention=retention,
                   dry_run=dry_run, tables={}, stop_reason="error", exit_code=1)

    def record(event):
        if journal is not None:
            with Path(journal).open("a") as out:
                out.write(json.dumps(event) + "\n")

    conn = None
    try:
        conn = connection_factory()
        _execute(conn, "SET SESSION iceberg.expire_snapshots_min_retention = "
                       f"'{maintenance.SESSION_MIN_RETENTION}'")
        tables = [t for t in maintenance._list_tables(conn, schema) if t not in set(exclude)]
        if only is not None:
            missing = set(only) - set(tables)
            if missing:
                raise ValueError("Unknown or excluded ops table requested")
            tables = [t for t in tables if t in set(only)]
        for table in tables:
            if not _IDENTIFIER_RE.fullmatch(table):
                raise ValueError("Unexpected table identifier")
        before = {t: _metrics(conn, schema, t) for t in tables}
        tables.sort(key=lambda t: (-before[t]["files"], t))
        summary["stop_reason"] = "exhausted"
        for table in tables:
            if stop is not None and stop.exists():
                summary["stop_reason"] = "stop"
                break
            if _now() >= deadline:
                summary["stop_reason"] = "deadline"
                break
            event = dict(event="table", table=table, before=before[table])
            if dry_run:
                event["status"] = "planned"
            else:
                fq = f'iceberg."{schema}"."{table}"'
                try:
                    optimized = _alter_with_retry(
                        conn, f"ALTER TABLE {fq} EXECUTE optimize", deadline, sleep)
                    expired = _alter_with_retry(
                        conn, f"ALTER TABLE {fq} EXECUTE expire_snapshots("
                              f"retention_threshold => '{retention}')", deadline, sleep)
                    event["status"] = "ok" if optimized and expired else "busy"
                    event["optimized"], event["expired"] = optimized, expired
                    event["after"] = _metrics(conn, schema, table)
                except TimeoutError:
                    event["status"] = "deadline"
                    summary["stop_reason"] = "deadline"
                except Exception as exc:
                    if _now() >= deadline:
                        event["status"] = summary["stop_reason"] = "deadline"
                    else:
                        event.update(status="failed", error_type=type(exc).__name__)
            summary["tables"][table] = event["status"]
            record(event)
            if summary["stop_reason"] == "deadline":
                break
        statuses = list(summary["tables"].values())
        failed = statuses.count("failed")
        busy = statuses.count("busy")
        maintained = statuses.count("ok")
        summary.update(maintained=maintained, busy=busy, failed=failed)
        summary["exit_code"] = 1 if failed or (busy and not maintained) else 0
    except Exception as exc:
        summary.update(stop_reason="error", error_type=type(exc).__name__, exit_code=1)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        record(summary)
    return summary
