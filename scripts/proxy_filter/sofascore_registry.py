"""SofaScore-only journal compaction; legacy byte replay stays exact.

The compacted journal is its own checkpoint: ordinary ``bytes`` events carry
sums with the same recovery keys, so old releases can replay it too. The
original inode remains readable during scanning. Appends are excluded only
while the bounded tail is copied and the durable replacement is installed.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock

PAID_TRIGGER_BYTES = 32_000_000
WAL_TRIGGER_BYTES = 4_000_000
MAX_LINE_BYTES = 4 * 1024 * 1024
FORENSIC_DAYS = 7


def _events(stream, end):
    while stream.tell() < end:
        raw = stream.readline(MAX_LINE_BYTES + 1)
        if not raw.endswith(b"\n") or len(raw) > MAX_LINE_BYTES or stream.tell() > end:
            raise RuntimeError("corrupt SofaScore registry: incomplete or oversized event")
        event = json.loads(raw)
        if not isinstance(event, dict):
            raise RuntimeError("corrupt SofaScore registry: event is not an object")
        yield event, raw


def _line(event):
    return (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode()


def compact(path: str, lock: RLock, *, kind: str, now=None, trigger=None, before_replace=None):
    """Atomically compact a prefix, preserving all appends and recovery sums.

    No event is pruned on a size-only decision. If exact retained state itself
    exceeds the cap, retain the original and report an error to the operator.
    """
    limit = trigger if trigger is not None else (PAID_TRIGGER_BYTES if kind == "paid" else WAL_TRIGGER_BYTES)
    with lock:
        try:
            stream = open(path, "rb")
        except FileNotFoundError:
            return None
        prefix = os.fstat(stream.fileno()).st_size
    if prefix < limit:
        stream.close()
        return None
    current = now or datetime.now(timezone.utc)
    cutoff = (current - timedelta(days=FORENSIC_DAYS)).isoformat()
    groups = {}
    finished = set()
    before = kept = 0
    directory = Path(path).parent
    descriptor, temporary = tempfile.mkstemp(prefix=".registry-", suffix=".tmp", dir=directory)
    try:
        with stream, os.fdopen(descriptor, "wb") as out:
            if kind == "wal":
                for event, _ in _events(stream, prefix):
                    if event.get("event_type") == "allocation_finished":
                        finished.add(str(event.get("lease_id", "")))
                stream.seek(0)
            for event, raw in _events(stream, prefix):
                before += 1
                if kind == "wal":
                    if str(event.get("lease_id", "")) not in finished:
                        out.write(raw)
                        kept += 1
                    continue
                if kind != "paid":
                    raise ValueError("unknown registry kind")
                if event.get("source") not in {"sofascore", "sofascore_discovery"}:
                    raise RuntimeError("SofaScore compaction refuses another source's journal")
                if event.get("event_type") == "bytes":
                    count = event.get("bytes")
                    if isinstance(count, bool) or not isinstance(count, int) or count <= 0 or event.get("direction") not in {"up", "down"}:
                        raise RuntimeError("corrupt SofaScore registry: invalid bytes")
                    fields = ("source", "dag_id", "run_id", "canonical_url", "lease_id", "allocation_id", "base_run_id", "workload_phase", "direction")
                    day = str(event.get("occurred_at", ""))[:10]
                    if len(day) != 10:
                        raise RuntimeError("corrupt SofaScore registry: missing day")
                    key = tuple(str(event.get(k, "")) for k in fields) + (day,)
                    if key not in groups:
                        groups[key] = {k: event[k] for k in fields if event.get(k)}
                        groups[key].update(event_type="bytes", occurred_at=day + "T00:00:00+00:00", bytes=0)
                    groups[key]["bytes"] += count
                elif str(event.get("occurred_at", "")) >= cutoff:
                    forensic = {key: event[key] for key in (
                        "event_type", "lease_id", "source", "occurred_at", "dag_id", "run_id", "task_id", "map_index", "try_number",
                        "endpoint", "endpoint_path", "request_id", "reason", "error", "error_type", "total_bytes", "max_bytes", "classification", "upstream_status", "endpoint_request_provider_bytes"
                    ) if key in event}
                    out.write(_line(forensic))
                    kept += 1
            for event in groups.values():
                out.write(_line(event))
                kept += 1
            out.flush()
            # The initial scan never holds the event-loop writer lock.
            with lock:
                if before_replace is not None:
                    before_replace()
                actual = os.stat(path)
                original = os.fstat(stream.fileno())
                if (actual.st_dev, actual.st_ino) != (original.st_dev, original.st_ino):
                    raise RuntimeError("SofaScore registry changed during compaction")
                stream.seek(prefix)
                for _event, raw in _events(stream, actual.st_size):
                    out.write(raw)
                    kept += 1
                out.flush()
                after = out.tell()
                if after >= limit or after >= actual.st_size:
                    raise RuntimeError(f"SofaScore registry cannot compact safely: retained={after}, limit={limit}")
                os.fsync(out.fileno())
                os.replace(temporary, path)
                dirfd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(dirfd)
                finally:
                    os.close(dirfd)
        # Legacy WAL backup is no longer needed: the installed WAL is durable
        # and uses the original schema. Never remove it before directory fsync.
        if kind == "wal":
            Path(path + ".compacted.bak").unlink(missing_ok=True)
        return {"kind": kind, "before_bytes": prefix, "after_bytes": after, "events_before": before, "events_after": kept}
    finally:
        Path(temporary).unlink(missing_ok=True)
