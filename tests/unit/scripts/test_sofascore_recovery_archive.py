"""Regression tests for #2170: the schema-2 gzip checkpoint archives every
aggregated bytes recovery group, including the current UTC day, instead of
duplicating fresh groups into the uncompressed journal tail.
"""
import gzip
import hashlib
import json
from datetime import datetime, timedelta, timezone
from threading import RLock

import pytest

from scripts.proxy_filter import sofascore_registry as registry

NOW = datetime(2026, 10, 9, tzinfo=timezone.utc)


def event(**values):
    return {"event_type": "bytes", "event_version": "paid-proxy-v2", "source": "sofascore", "occurred_at": "2026-10-08T02:00:00+00:00", "dag_id": "history", "run_id": "r", "lease_id": "lease", "allocation_id": "a", "base_run_id": "r", "workload_phase": "matches", "canonical_url": "https://www.sofascore.com/tournament/1", "direction": "down", "bytes": 13, "padding": "x" * 1000, **values}


def write(path, events):
    path.write_text("".join(json.dumps(e) + "\n" for e in events))


def read(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def totals(events):
    values = {}
    for e in events:
        if e["event_type"] != "bytes":
            continue
        key = tuple(e.get(k, "") for k in registry.RECOVERY_FIELDS) + (e["occurred_at"][:10],)
        values[key] = values.get(key, 0) + e["bytes"]
    return values


def archived(path):
    """Decode the committed snapshot referenced by the journal checkpoint."""
    rows = read(path)
    assert rows and rows[0]["event_type"] == registry.CHECKPOINT_TYPE
    checkpoint = rows[0]
    blob = (path.parent / checkpoint["archive"]).read_bytes()
    assert hashlib.sha256(blob).hexdigest() == checkpoint["sha256"]
    decoded = gzip.decompress(blob).decode().splitlines()
    assert len(decoded) == checkpoint["events_count"]
    return [json.loads(line) for line in decoded]


def legacy_reader(path):
    """The previous reader contract: expand the checkpoint snapshot, then the
    raw tail, without knowing where recovery groups are stored."""
    rows = read(path)
    events = []
    tail = rows
    if rows and rows[0].get("event_type") == registry.CHECKPOINT_TYPE:
        events.extend(archived(path))
        tail = rows[1:]
    events.extend(tail)
    return events


def test_fresh_and_old_groups_are_both_archived_with_exact_sums(tmp_path):
    path = tmp_path / "paid.jsonl"
    fresh = [event() for _ in range(30)]  # Yesterday relative to NOW.
    same_day = [event(lease_id="today", occurred_at="2026-10-09T06:00:00+00:00", bytes=7) for _ in range(3)]
    old = [event(lease_id="old", occurred_at="2026-09-01T00:00:00+00:00", bytes=5) for _ in range(4)]
    write(path, fresh + same_day + old)
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    raw = read(path)
    assert len(raw) == 1 and raw[0]["event_type"] == registry.CHECKPOINT_TYPE
    assert raw[0]["schema_version"] == 2
    # Every recovery key lives in the snapshot: fresh, same-day and expired.
    assert totals(archived(path)) == totals(fresh + same_day + old)
    assert totals(registry.iter_events(path)) == totals(fresh + same_day + old)


def test_repeat_compaction_and_restart_never_doubles_sums(tmp_path):
    path = tmp_path / "paid.jsonl"
    rows = [event() for _ in range(30)] + [event(lease_id="old", occurred_at="2026-09-01T00:00:00+00:00")]
    write(path, rows)
    expected = totals(rows)
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    for day in range(1, 6):
        # A restart re-reads only committed files, then compacts again.
        assert totals(registry.iter_events(path)) == expected
        registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=day))
        assert totals(registry.iter_events(path)) == expected
    # A forced same-day pass cannot duplicate or rewrite the committed sums.
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    assert registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=5), force=True) is None
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before
    assert totals(registry.iter_events(path)) == expected


def test_concurrent_same_key_append_adds_instead_of_replacing(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    committed = [event() for _ in range(30)]
    write(path, committed)
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    with path.open("a") as out:
        out.write(json.dumps(event(bytes=100)) + "\n")
    expanded = registry._expanded_events
    # Same recovery key and UTC day as committed; small so the pass reduces.
    appended = event(bytes=17, padding="")
    def scan(*args):
        for index, item in enumerate(expanded(*args)):
            if index == 0:
                with path.open("a") as out:
                    out.write(json.dumps(appended) + "\n")
            yield item
    with monkeypatch.context() as patch:
        patch.setattr(registry, "_expanded_events", scan)
        registry.compact(str(path), RLock(), kind="paid", now=NOW, force=True)
    # The concurrent same-key append stays raw exactly once until the next
    # pass; logical replay adds it to the archived sum instead of replacing.
    assert totals(read(path)) == totals([appended])
    assert totals(registry.iter_events(path)) == totals(committed + [event(bytes=100), appended])
    registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=1))
    assert totals(read(path)) == {}
    assert totals(registry.iter_events(path)) == totals(committed + [event(bytes=100), appended])


def previous_format(path, rows, *, now):
    """Write what the pre-#2170 writer produced: old groups in the snapshot,
    fresh aggregated groups only in the uncompressed tail, never twice."""
    cutoff = (now - timedelta(days=registry.FORENSIC_DAYS)).date().isoformat()
    groups = {}
    forensic = []
    for e in rows:
        if e["event_type"] == "bytes":
            key = tuple(str(e.get(k, "")) for k in registry.RECOVERY_FIELDS) + (e["occurred_at"][:10],)
            if key not in groups:
                groups[key] = {k: e[k] for k in registry.RECOVERY_FIELDS if e.get(k)}
                groups[key].update(event_type="bytes", occurred_at=key[-1] + "T00:00:00+00:00", bytes=0)
            groups[key]["bytes"] += e["bytes"]
        else:
            forensic.append({k: e[k] for k in registry.FORENSIC_FIELDS if k in e})
    archived_rows = forensic + [g for g in groups.values() if g["occurred_at"][:10] < cutoff]
    tail_rows = [g for g in groups.values() if g["occurred_at"][:10] >= cutoff]
    packed = b"".join(registry._line(e) for e in archived_rows)
    blob = gzip.compress(packed, mtime=0)
    digest = hashlib.sha256(blob).hexdigest()
    name = f"{path.name}.forensics-{digest}.jsonl.gz"
    (path.parent / name).write_bytes(blob)
    checkpoint = {"event_type": registry.CHECKPOINT_TYPE, "schema_version": 2,
                  "source": "sofascore", "occurred_at": now.isoformat(),
                  "archive": name, "sha256": digest,
                  "events_count": len(archived_rows), "uncompressed_bytes": len(packed)}
    write(path, [checkpoint] + tail_rows)


def test_previous_format_output_is_read_and_recompacted_without_loss(tmp_path):
    path = tmp_path / "paid.jsonl"
    old = [event(lease_id="old", occurred_at="2026-09-01T00:00:00+00:00", bytes=5) for _ in range(4)]
    fresh = [event() for _ in range(30)]
    forensic = event(event_type="endpoint_finished", request_id="recent", padding="")
    previous_format(path, old + fresh + [forensic], now=NOW)
    # The current reader expands the previous writer's output unchanged.
    assert totals(registry.iter_events(path)) == totals(old + fresh)
    assert any(e.get("request_id") == "recent" for e in registry.iter_events(path))
    # Recompaction moves the tail emission into the archive, keeping every sum.
    registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=1), force=True)
    assert totals(read(path)) == {}
    assert totals(registry.iter_events(path)) == totals(old + fresh)
    assert any(e.get("request_id") == "recent" for e in registry.iter_events(path))
    assert len(list(tmp_path.glob("*.jsonl.gz"))) == 1


def test_previous_reader_recovers_same_sums_from_new_output(tmp_path):
    path = tmp_path / "paid.jsonl"
    rows = [event() for _ in range(30)] + [event(lease_id="today", occurred_at="2026-10-09T06:00:00+00:00", bytes=7)]
    write(path, rows)
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    # A reader written against the pre-#2170 contract (snapshot + raw tail)
    # recovers identical counters: schema 2 never changed the read side.
    assert totals(legacy_reader(path)) == totals(rows)
    assert totals(legacy_reader(path)) == totals(registry.iter_events(path))


def test_window_boundaries_keep_seven_day_evidence_but_archive_all_sums(tmp_path):
    path = tmp_path / "paid.jsonl"
    now = NOW + timedelta(hours=12)  # Cutoff is 2026-10-02T12:00:00+00:00.
    boundary_kept = event(event_type="endpoint_finished", request_id="kept", occurred_at="2026-10-02T12:00:00+00:00", padding="")
    boundary_gone = event(event_type="endpoint_finished", request_id="gone", occurred_at="2026-10-02T11:59:59+00:00", padding="")
    bytes_days = [event(occurred_at="2026-10-02T00:00:00+00:00", lease_id="d2"),
                  event(occurred_at="2026-10-01T23:59:59+00:00", lease_id="d1"),
                  event(occurred_at="2026-10-09T00:00:00+00:00", lease_id="d9")]
    write(path, [boundary_kept, boundary_gone] + bytes_days)
    registry.compact(str(path), RLock(), kind="paid", now=now)
    expanded = list(registry.iter_events(path))
    assert [e.get("request_id") for e in expanded if e["event_type"] == "endpoint_finished"] == ["kept"]
    assert totals(expanded) == totals(bytes_days)
    assert totals(read(path)) == {}
    assert {e["occurred_at"][:10] for e in archived(path) if e["event_type"] == "bytes"} == {"2026-10-01", "2026-10-02", "2026-10-09"}
    # After the evidence window passes, recovery sums still never expire.
    registry.compact(str(path), RLock(), kind="paid", now=now + timedelta(days=20))
    assert totals(registry.iter_events(path)) == totals(bytes_days)
    assert not [e for e in registry.iter_events(path) if e["event_type"] == "endpoint_finished"]


@pytest.mark.parametrize("fault", ["digest", "missing", "count"])
def test_corrupt_snapshot_blocks_compaction_and_preserves_journal(tmp_path, fault):
    path = tmp_path / "paid.jsonl"
    rows = [event() for _ in range(30)] + [event(lease_id="today", occurred_at="2026-10-09T06:00:00+00:00", bytes=7)]
    write(path, rows)
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    checkpoint = read(path)[0]
    archive = tmp_path / checkpoint["archive"]
    if fault == "digest":
        archive.write_bytes(archive.read_bytes() + b"corrupted")
    elif fault == "missing":
        archive.unlink()
    else:
        checkpoint["events_count"] += 1
        write(path, [checkpoint])
    original = path.read_bytes()
    with pytest.raises((RuntimeError, OSError)):
        list(registry.iter_events(path))
    with pytest.raises((RuntimeError, OSError)):
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=1)
    assert path.read_bytes() == original


@pytest.mark.parametrize("fault", ["fsync", "replace"])
def test_failed_archive_install_keeps_previous_sums(tmp_path, monkeypatch, fault):
    path = tmp_path / "paid.jsonl"
    rows = [event() for _ in range(30)] + [event(lease_id="today", occurred_at="2026-10-09T06:00:00+00:00", bytes=7)]
    write(path, rows)
    original = path.read_bytes()
    def fail(*args):
        raise OSError("injected disk failure")
    monkeypatch.setattr(registry.os, fault, fail)
    with pytest.raises(OSError, match="injected disk failure"):
        registry.compact(str(path), RLock(), kind="paid", now=NOW)
    assert path.read_bytes() == original
    assert not list(tmp_path.glob("*.jsonl.gz"))
    assert totals(registry.iter_events(path)) == totals(rows)
    monkeypatch.undo()
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    assert totals(registry.iter_events(path)) == totals(rows)
    assert totals(archived(path)) == totals(rows)


def test_budget_replay_reads_archived_fresh_sums_via_iter_events(tmp_path):
    # Mirror of the main budget replay: counters come from iter_events only.
    path = tmp_path / "paid.jsonl"
    rows = [event(bytes=17, run_id=f"run-{i}", lease_id=str(i)) for i in range(100)]
    write(path, rows)
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    assert len(read(path)) == 1
    replayed = sum(e["bytes"] for e in registry.iter_events(path) if e["event_type"] == "bytes")
    assert replayed == 1700
