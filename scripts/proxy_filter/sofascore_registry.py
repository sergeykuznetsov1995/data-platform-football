"""SofaScore journal compaction with legacy byte replay and seven-day evidence.

A gzip snapshot is published before its checkpoint in the paid journal. The
journal replacement is the commit: a crash can leave an unreferenced snapshot,
but cannot remove the evidence referenced by the durable journal.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import tempfile
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import RLock

PAID_TRIGGER_BYTES = 32_000_000
WAL_TRIGGER_BYTES = 4_000_000
REGISTRY_LIMIT_BYTES = 60_000_000
MAX_LINE_BYTES = 4 * 1024 * 1024
FORENSIC_DAYS = 7
CHECKPOINT_TYPE = "forensic_checkpoint"
FORENSIC_FIELDS = (
    "event_type", "lease_id", "source", "occurred_at", "dag_id", "run_id",
    "task_id", "map_index", "try_number", "endpoint", "endpoint_path",
    "request_id", "reason", "error", "error_type", "total_bytes", "max_bytes",
    "classification", "upstream_status", "endpoint_request_provider_bytes",
)
RECOVERY_FIELDS = (
    "source", "dag_id", "run_id", "canonical_url", "lease_id", "allocation_id",
    "base_run_id", "workload_phase", "direction",
)
SOURCES = {"sofascore", "sofascore_discovery"}


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


def _sync_directory(directory):
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _digest(stream):
    stream.seek(0)
    digest = hashlib.sha256()
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
    stream.seek(0)
    return digest.hexdigest()


def _snapshot_name(path, event):
    digest = event.get("sha256", "")
    if (
        event.get("schema_version") != 1
        or event.get("source") not in SOURCES
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        or event.get("archive") != f"{Path(path).name}.forensics-{digest}.jsonl.gz"
        or type(event.get("events_count")) is not int
        or event["events_count"] < 0
        or type(event.get("uncompressed_bytes")) is not int
        or not 0 <= event["uncompressed_bytes"] <= 1_000_000_000
    ):
        raise RuntimeError("corrupt SofaScore forensic checkpoint")
    return event["archive"]


def _open_prefix(path, lock, stack):
    # Pin both inodes before yielding anything. Readers racing cleanup retry
    # only before opening the snapshot, never after yielding an event.
    for _ in range(3):
        with lock:
            stream = open(path, "rb")
            prefix = os.fstat(stream.fileno()).st_size
            checkpoint = None
            snapshot = None
            try:
                if prefix:
                    raw = stream.readline(MAX_LINE_BYTES + 1)
                    if len(raw) > MAX_LINE_BYTES or not raw.endswith(b"\n") or stream.tell() > prefix:
                        raise RuntimeError("corrupt SofaScore registry: incomplete or oversized event")
                    first = json.loads(raw)
                    if not isinstance(first, dict):
                        raise RuntimeError("corrupt SofaScore registry: event is not an object")
                    if first.get("event_type") == CHECKPOINT_TYPE:
                        checkpoint = first
                        name = _snapshot_name(path, checkpoint)
                        descriptor = os.open(Path(path).parent / name, os.O_RDONLY | os.O_NOFOLLOW)
                        snapshot = os.fdopen(descriptor, "rb")
                stream.seek(0)
            except FileNotFoundError:
                original = os.fstat(stream.fileno())
                stream.close()
                actual = os.stat(path)
                if (actual.st_dev, actual.st_ino) != (original.st_dev, original.st_ino):
                    continue
                raise RuntimeError("missing SofaScore forensic snapshot") from None
            except BaseException:
                stream.close()
                if snapshot is not None:
                    snapshot.close()
                raise
            stack.enter_context(stream)
            if snapshot is not None:
                stack.enter_context(snapshot)
            return stream, prefix, checkpoint, snapshot
    raise RuntimeError("SofaScore registry changed while opening snapshot")


def _snapshot_events(snapshot, checkpoint):
    if os.fstat(snapshot.fileno()).st_size >= REGISTRY_LIMIT_BYTES or _digest(snapshot) != checkpoint["sha256"]:
        raise RuntimeError("corrupt SofaScore forensic snapshot: digest or size")
    count = length = 0
    try:
        with gzip.GzipFile(fileobj=snapshot, mode="rb") as decoded:
            while raw := decoded.readline(MAX_LINE_BYTES + 1):
                length += len(raw)
                count += 1
                if (
                    not raw.endswith(b"\n") or len(raw) > MAX_LINE_BYTES
                    or length > checkpoint["uncompressed_bytes"]
                    or count > checkpoint["events_count"]
                ):
                    raise RuntimeError("corrupt SofaScore forensic snapshot: event bounds")
                event = json.loads(raw)
                if (
                    not isinstance(event, dict) or event.get("source") not in SOURCES
                    or event.get("event_type") in {"bytes", CHECKPOINT_TYPE}
                ):
                    raise RuntimeError("corrupt SofaScore forensic snapshot: event type")
                yield event, raw
    except (OSError, EOFError, ValueError) as exc:
        raise RuntimeError("corrupt SofaScore forensic snapshot") from exc
    if (count, length) != (checkpoint["events_count"], checkpoint["uncompressed_bytes"]):
        raise RuntimeError("corrupt SofaScore forensic snapshot: count or length")


def _expanded_events(stream, end, checkpoint, snapshot):
    first = True
    for event, raw in _events(stream, end):
        if event.get("event_type") == CHECKPOINT_TYPE:
            if not first or checkpoint is None or event != checkpoint:
                raise RuntimeError("corrupt SofaScore registry: misplaced checkpoint")
            yield from _snapshot_events(snapshot, checkpoint)
        else:
            yield event, raw
        first = False


def iter_events(path):
    """Read logical paid events, including the committed forensic snapshot.

    The reader pins its files before yielding. It validates the compressed
    digest, gzip stream and exact event count; callers must not use a partial
    result if iteration raises. Legacy byte readers can keep reading JSONL.
    """
    with ExitStack() as stack:
        stream, end, checkpoint, snapshot = _open_prefix(str(path), RLock(), stack)
        for event, _ in _expanded_events(stream, end, checkpoint, snapshot):
            yield event


def validate_checkpoint(path):
    """Validate archived evidence before a dedicated gateway admits traffic."""
    with ExitStack() as stack:
        try:
            _, _, checkpoint, snapshot = _open_prefix(path, RLock(), stack)
        except FileNotFoundError:
            return
        if snapshot is not None:
            for _ in _snapshot_events(snapshot, checkpoint):
                pass


def _archives(path):
    directory = Path(path).parent
    pattern = re.compile(re.escape(Path(path).name) + r"\.forensics-[0-9a-f]{64}\.jsonl\.gz")
    return [p for p in directory.iterdir() if pattern.fullmatch(p.name)]


def _cleanup_archives(path, keep):
    changed = False
    for archive in _archives(path):
        if archive.name != keep:
            archive.unlink()
            changed = True
    if changed:
        _sync_directory(Path(path).parent)


def _small_cleanup(path, stream, prefix, checkpoint, snapshot, kind, lock, before_replace):
    keep = checkpoint["archive"] if checkpoint else None
    backup = Path(path + ".compacted.bak")
    needs_cleanup = (kind == "wal" and backup.exists()) or (kind == "paid" and any(p.name != keep for p in _archives(path)))
    if not needs_cleanup:
        return
    # Verify the pinned prefix/snapshot without holding up paid appends.
    stream.seek(0)
    iterator = _expanded_events(stream, prefix, checkpoint, snapshot) if kind == "paid" else _events(stream, prefix)
    for _ in iterator:
        pass
    with lock:
        if before_replace is not None:
            before_replace()
        actual = os.stat(path)
        original = os.fstat(stream.fileno())
        if (actual.st_dev, actual.st_ino) != (original.st_dev, original.st_ino):
            raise RuntimeError("SofaScore registry changed during cleanup")
        stream.seek(prefix)
        for _ in _events(stream, actual.st_size):
            pass
        os.fsync(stream.fileno())
        _sync_directory(Path(path).parent)
        if kind == "wal":
            backup.unlink(missing_ok=True)
            _sync_directory(Path(path).parent)
        else:
            _cleanup_archives(path, keep)


def _projected_bytes(path, after, archive_bytes, temporary_paths):
    # Old snapshot generations and this operation's temps disappear only after
    # the commit. Other backups/temps/state files remain in the projected cap.
    excluded = {Path(path), *_archives(path), *(Path(p) for p in temporary_paths if p)}
    total = after + archive_bytes
    for p in Path(path).parent.iterdir():
        if p not in excluded and p.is_file() and p.name.endswith((".json", ".jsonl", ".bak", ".tmp", ".gz")):
            total += p.stat().st_size
    return total


def compact(path: str, lock: RLock, *, kind: str, now=None, trigger=None, before_replace=None):
    """Compact without discarding recovery sums or seven-day evidence."""
    if kind not in {"paid", "wal"}:
        raise ValueError("unknown registry kind")
    path = str(path)
    limit = trigger if trigger is not None else (PAID_TRIGGER_BYTES if kind == "paid" else WAL_TRIGGER_BYTES)
    current = now or datetime.now(timezone.utc)
    cutoff = (current - timedelta(days=FORENSIC_DAYS)).isoformat()
    directory = Path(path).parent
    with ExitStack() as stack:
        try:
            stream, prefix, checkpoint, snapshot = _open_prefix(path, lock, stack)
        except FileNotFoundError:
            return None
        # Keep an empty snapshot marker after expiry so small future forensic
        # tails still receive the next UTC-day refresh instead of waiting for
        # 32 MB. A small legacy journal gets this marker on its first scan.
        refresh = kind == "paid" and prefix > 0 and (
            checkpoint is None
            or str(checkpoint.get("occurred_at", ""))[:10] != current.date().isoformat()
        )
        if prefix < limit and not refresh:
            _small_cleanup(path, stream, prefix, checkpoint, snapshot, kind, lock, before_replace)
            return None
        groups = {}
        finished = set()
        before = kept = forensic_count = forensic_length = 0
        descriptor, temporary = tempfile.mkstemp(prefix=".registry-", suffix=".tmp", dir=directory)
        archive_tmp = None
        try:
            out = stack.enter_context(os.fdopen(descriptor, "wb"))
            archive_out = packed = None
            if kind == "paid":
                archive_fd, archive_tmp = tempfile.mkstemp(prefix=".registry-forensics-", suffix=".tmp", dir=directory)
                archive_out = stack.enter_context(os.fdopen(archive_fd, "w+b"))
                packed = gzip.GzipFile(filename="", fileobj=archive_out, mode="wb", mtime=0)
                stack.callback(packed.close)
            else:
                for event, _ in _events(stream, prefix):
                    if event.get("event_type") == "allocation_finished":
                        finished.add(str(event.get("lease_id", "")))
                stream.seek(0)
            iterator = _expanded_events(stream, prefix, checkpoint, snapshot) if kind == "paid" else _events(stream, prefix)
            for event, raw in iterator:
                before += 1
                if kind == "wal":
                    if str(event.get("lease_id", "")) not in finished:
                        out.write(raw)
                        kept += 1
                    continue
                if event.get("source") not in SOURCES:
                    raise RuntimeError("SofaScore compaction refuses another source's journal")
                if event.get("event_type") == "bytes":
                    count = event.get("bytes")
                    if isinstance(count, bool) or not isinstance(count, int) or count <= 0 or event.get("direction") not in {"up", "down"}:
                        raise RuntimeError("corrupt SofaScore registry: invalid bytes")
                    day = str(event.get("occurred_at", ""))[:10]
                    if len(day) != 10:
                        raise RuntimeError("corrupt SofaScore registry: missing day")
                    key = tuple(str(event.get(k, "")) for k in RECOVERY_FIELDS) + (day,)
                    if key not in groups:
                        groups[key] = {k: event[k] for k in RECOVERY_FIELDS if event.get(k)}
                        groups[key].update(event_type="bytes", occurred_at=day + "T00:00:00+00:00", bytes=0)
                    groups[key]["bytes"] += count
                elif str(event.get("occurred_at", "")) >= cutoff:
                    raw = _line({k: event[k] for k in FORENSIC_FIELDS if k in event})
                    packed.write(raw)
                    forensic_count += 1
                    forensic_length += len(raw)
            archive_name = None
            archive_bytes = 0
            if packed is not None:
                packed.close()
                archive_out.flush()
                if forensic_length > 1_000_000_000:
                    raise RuntimeError("SofaScore forensic snapshot exceeds safe decoded size")
                digest = _digest(archive_out)
                archive_name = f"{Path(path).name}.forensics-{digest}.jsonl.gz"
                archive_bytes = os.fstat(archive_out.fileno()).st_size
                out.write(_line({"event_type": CHECKPOINT_TYPE, "schema_version": 1,
                                 "source": "sofascore", "occurred_at": current.isoformat(),
                                 "archive": archive_name, "sha256": digest,
                                 "events_count": forensic_count, "uncompressed_bytes": forensic_length}))
                kept += forensic_count
            for event in groups.values():
                out.write(_line(event))
                kept += 1
            out.flush()
            with lock:
                if before_replace is not None:
                    before_replace()
                actual = os.stat(path)
                original = os.fstat(stream.fileno())
                if (actual.st_dev, actual.st_ino) != (original.st_dev, original.st_ino):
                    raise RuntimeError("SofaScore registry changed during compaction")
                stream.seek(prefix)
                for event, raw in _events(stream, actual.st_size):
                    if kind == "paid" and (event.get("event_type") == CHECKPOINT_TYPE or event.get("source") not in SOURCES):
                        raise RuntimeError("invalid SofaScore registry append during compaction")
                    out.write(raw)
                    kept += 1
                out.flush()
                after = out.tell()
                previous_archive_bytes = os.fstat(snapshot.fileno()).st_size if snapshot else 0
                projected = _projected_bytes(path, after, archive_bytes, (temporary, archive_tmp)) if kind == "paid" else after
                if after >= limit or projected >= REGISTRY_LIMIT_BYTES or (
                    prefix >= limit and after + archive_bytes >= actual.st_size + previous_archive_bytes
                ):
                    raise RuntimeError(f"SofaScore registry cannot compact safely: retained={after}, limit={limit}, projected={projected}")
                if archive_name:
                    os.fsync(archive_out.fileno())
                    os.replace(archive_tmp, directory / archive_name)
                    _sync_directory(directory)
                os.fsync(out.fileno())
                os.replace(temporary, path)
                _sync_directory(directory)
                if kind == "paid":
                    _cleanup_archives(path, archive_name)
                else:
                    Path(path + ".compacted.bak").unlink(missing_ok=True)
                    _sync_directory(directory)
            return {"kind": kind, "before_bytes": prefix, "after_bytes": after,
                    "archive_bytes": archive_bytes, "events_before": before, "events_after": kept}
        finally:
            Path(temporary).unlink(missing_ok=True)
            if archive_tmp:
                Path(archive_tmp).unlink(missing_ok=True)
