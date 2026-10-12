import json
from datetime import datetime, timezone
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
        key = tuple(e.get(k, "") for k in ("source", "dag_id", "run_id", "lease_id", "allocation_id", "base_run_id", "workload_phase", "canonical_url", "direction")) + (e["occurred_at"][:10],)
        values[key] = values.get(key, 0) + e["bytes"]
    return values


def test_compacted_checkpoint_preserves_every_recovery_key_and_week_evidence(tmp_path):
    path = tmp_path / "paid.jsonl"
    events = [event() for _ in range(30)] + [event(direction="up"), event(occurred_at="2026-10-07T00:00:00Z"), event(lease_id="other"), event(event_type="lease_created"), event(event_type="accounting_uncertain", reason="disk_failed"), event(event_type="lease_closed"), event(event_type="lease_created", occurred_at="2026-09-01T00:00:00Z")]
    write(path, events)
    result = registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert totals(read(path)) == totals(events)
    assert result["after_bytes"] < 10000
    assert {e["event_type"] for e in registry.iter_events(path)} == {"bytes", "lease_created", "accounting_uncertain", "lease_closed"}
    assert sum(e["event_type"] == "lease_created" for e in registry.iter_events(path)) == 1
    assert path.stat().st_mode & 0o777 == 0o600
    assert not list(tmp_path.glob(".registry-*"))


def test_concurrent_append_is_copied_exactly_once(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    events = [event() for _ in range(30)]
    write(path, events)
    original = registry._events
    appended = event(lease_id="during-scan", bytes=17)
    def scan(stream, end):
        for index, item in enumerate(original(stream, end)):
            if index == 0 and end > 10000:
                with path.open("a") as out:
                    out.write(json.dumps(appended) + "\n")
            yield item
    monkeypatch.setattr(registry, "_events", scan)
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert totals(read(path)) == totals(events + [appended])


@pytest.mark.parametrize("fault", ["fsync", "replace"])
def test_failure_before_replace_keeps_original(tmp_path, monkeypatch, fault):
    path = tmp_path / "paid.jsonl"
    write(path, [event() for _ in range(30)])
    original = path.read_bytes()
    def fail(*args):
        raise OSError("injected disk failure")
    monkeypatch.setattr(registry.os, fault, fail)
    with pytest.raises(OSError):
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert path.read_bytes() == original
    assert not list(tmp_path.glob(".registry-*"))


def test_wal_retains_open_attempt_and_removes_only_finished_prefix(tmp_path):
    path = tmp_path / "wal.jsonl"
    events = [event(lease_id="done", event_type="claim_intent"), event(lease_id="done", event_type="allocation_finished")] + [event(lease_id="open", event_type="endpoint_started") for _ in range(3)]
    write(path, events)
    registry.compact(str(path), RLock(), kind="wal", now=NOW, trigger=5000)
    assert read(path) == events[2:]


@pytest.mark.parametrize("payload", [b'{"bytes":1}', b'not-json\n'])
def test_corruption_never_replaces_original(tmp_path, payload):
    path = tmp_path / "paid.jsonl"
    path.write_bytes(payload)
    with pytest.raises((RuntimeError, ValueError)):
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=1)
    assert path.read_bytes() == payload


def test_irreducible_state_is_preserved_instead_of_pruned(tmp_path):
    path = tmp_path / "paid.jsonl"
    write(path, [event(lease_id=str(i), padding="") for i in range(30)])
    original = path.read_bytes()
    with pytest.raises(RuntimeError, match="cannot compact"):
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=1000)
    assert path.read_bytes() == original


def test_another_source_is_never_rotated(tmp_path):
    path = tmp_path / "paid.jsonl"
    write(path, [event(source="whoscored")])
    original = path.read_bytes()
    with pytest.raises(RuntimeError, match="another source"):
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=1)
    assert path.read_bytes() == original


def test_pending_tail_repair_runs_before_atomic_install(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    events = [event() for _ in range(30)]
    write(path, events)
    original = registry._events
    appended = event(lease_id="repaired-tail", bytes=17, padding="")
    prefix = path.stat().st_size
    payload = (json.dumps(appended) + "\n").encode()
    def scan(stream, end):
        for index, item in enumerate(original(stream, end)):
            if index == 0 and end == prefix:
                with path.open("ab") as out:
                    out.write(payload[:len(payload)//2])
            yield item
    def repair():
        with path.open("r+b") as out:
            out.truncate(prefix)
            out.seek(prefix)
            out.write(payload)
    monkeypatch.setattr(registry, "_events", scan)
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000, before_replace=repair)
    assert totals(read(path)) == totals(events + [appended])


def test_large_week_of_forensics_is_archived_without_losing_events(tmp_path):
    path = tmp_path / "paid.jsonl"
    forensic = [event(event_type="endpoint_finished", request_id=str(i), padding="") for i in range(400)]
    write(path, [event() for _ in range(30)] + forensic)
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert path.stat().st_size < 10000
    physical = read(path)
    assert physical[0]["event_type"] == "forensic_checkpoint"
    assert totals(physical) == totals([event() for _ in range(30)])
    expanded = list(registry.iter_events(path))
    assert [e["request_id"] for e in expanded if e["event_type"] == "endpoint_finished"] == [str(i) for i in range(400)]
    assert len(list(tmp_path.glob("*.jsonl.gz"))) == 1


def _archived(path):
    write(path, [event() for _ in range(30)] + [event(event_type="lease_created")])
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    return path.parent / read(path)[0]["archive"]


@pytest.mark.parametrize("stage", ["archive_directory", "journal_replace", "journal_directory"])
def test_crash_boundaries_preserve_committed_evidence(tmp_path, monkeypatch, stage):
    path = tmp_path / "paid.jsonl"
    old_archive = _archived(path)
    extra = [event() for _ in range(30)] + [event(event_type="endpoint_finished", request_id="new")]
    with path.open("a") as out:
        out.write("".join(json.dumps(e) + "\n" for e in extra))
    expected = list(registry.iter_events(path))
    original = path.read_bytes()
    replace = registry.os.replace
    sync = registry._sync_directory
    calls = 0
    def fail_sync(directory):
        nonlocal calls
        calls += 1
        if calls == (1 if stage == "archive_directory" else 2):
            raise OSError("injected directory fsync")
        sync(directory)
    def fail_replace(source, target):
        if str(target) == str(path) and stage == "journal_replace":
            raise OSError("injected journal replace")
        replace(source, target)
    with monkeypatch.context() as patch:
        patch.setattr(registry.os, "replace", fail_replace)
        if stage != "journal_replace":
            patch.setattr(registry, "_sync_directory", fail_sync)
        with pytest.raises(OSError):
            registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert old_archive.exists()  # No cleanup before journal directory fsync.
    assert totals(registry.iter_events(path)) == totals(expected)
    def forensic(rows):
        return [{k: e[k] for k in registry.FORENSIC_FIELDS if k in e} for e in rows if e["event_type"] != "bytes"]
    assert forensic(registry.iter_events(path)) == forensic(expected)
    if stage != "journal_directory":
        assert path.read_bytes() == original
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    else:
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert len(list(tmp_path.glob("*.jsonl.gz"))) == 1


@pytest.mark.parametrize("fault", ["missing", "digest", "count", "gzip", "path", "type", "oversized"])
def test_snapshot_corruption_is_rejected_without_replacing_journal(tmp_path, fault):
    import gzip
    import hashlib
    path = tmp_path / "paid.jsonl"
    archive = _archived(path)
    rows = read(path)
    checkpoint = rows[0]
    if fault == "missing":
        archive.unlink()
    elif fault == "digest":
        archive.write_bytes(archive.read_bytes() + b"broken")
    elif fault == "count":
        checkpoint["events_count"] += 1
    elif fault == "path":
        checkpoint["archive"] = "../elsewhere.gz"
    elif fault in {"gzip", "type", "oversized"}:
        raw = b"not gzip" if fault == "gzip" else gzip.compress(
            b"x" * (registry.MAX_LINE_BYTES + 1) if fault == "oversized" else registry._line(event()), mtime=0,
        )
        digest = hashlib.sha256(raw).hexdigest()
        archive.unlink()
        checkpoint.update(sha256=digest, archive=f"paid.jsonl.forensics-{digest}.jsonl.gz")
        (tmp_path / checkpoint["archive"]).write_bytes(raw)
    write(path, rows)
    original = path.read_bytes()
    with pytest.raises((RuntimeError, OSError)):
        list(registry.iter_events(path))
    with pytest.raises((RuntimeError, OSError)):
        registry.validate_checkpoint(str(path))
    with pytest.raises((RuntimeError, OSError)):
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=1)
    assert path.read_bytes() == original


def test_archived_reader_pins_old_generation_during_cleanup(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    old_archive = _archived(path)
    reader = registry.iter_events(path)
    first = next(reader)
    with path.open("a") as out:
        out.write("".join(json.dumps(event()) + "\n" for _ in range(30)))
        out.write(json.dumps(event(event_type="endpoint_finished", request_id="next")) + "\n")
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert not old_archive.exists()
    old_events = [first, *reader]
    assert sum(e["event_type"] == "lease_created" for e in old_events) == 1
    assert totals(old_events) == totals([event() for _ in range(30)])


def test_recompaction_expires_old_forensics_and_keeps_exact_byte_keys(tmp_path):
    from datetime import timedelta
    path = tmp_path / "paid.jsonl"
    old_archive = _archived(path)
    registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=8), trigger=10000)
    assert not old_archive.exists()
    assert {e["event_type"] for e in registry.iter_events(path)} == {"bytes"}
    assert totals(read(path)) == totals([event() for _ in range(30)])


def test_orphan_cleanup_requires_valid_durable_small_journal(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    archive = _archived(path)
    orphan = tmp_path / ("paid.jsonl.forensics-" + "f" * 64 + ".jsonl.gz")
    orphan.write_bytes(b"abandoned")
    fsync = registry.os.fsync
    with monkeypatch.context() as patch:
        def fail(*args):
            raise OSError("injected fsync")
        patch.setattr(registry.os, "fsync", fail)
        with pytest.raises(OSError):
            registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert orphan.exists() and archive.exists()
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert not orphan.exists() and archive.exists()
    assert registry.os.fsync == fsync


def test_small_wal_backup_removed_only_after_installed_wal_fsync(tmp_path, monkeypatch):
    path = tmp_path / "wal.jsonl"
    write(path, [event(event_type="claim_intent", padding="")])
    backup = tmp_path / "wal.jsonl.compacted.bak"
    backup.write_bytes(b"old WAL")
    with monkeypatch.context() as patch:
        def fail(*args):
            raise OSError("injected fsync")
        patch.setattr(registry.os, "fsync", fail)
        with pytest.raises(OSError):
            registry.compact(str(path), RLock(), kind="wal", trigger=10000)
    assert backup.exists()
    registry.compact(str(path), RLock(), kind="wal", trigger=10000)
    assert not backup.exists()
    assert len(read(path)) == 1


def test_other_state_and_archive_both_count_towards_projected_limit(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    write(path, [event() for _ in range(30)] + [event(event_type="lease_created")])
    original = path.read_bytes()
    (tmp_path / "other.json").write_bytes(b"x" * 1000)
    monkeypatch.setattr(registry, "REGISTRY_LIMIT_BYTES", 1000)
    with pytest.raises(RuntimeError, match="projected"):
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert path.read_bytes() == original
    assert not list(tmp_path.glob("*.jsonl.gz"))


def test_small_raw_tail_expires_after_last_snapshot_became_empty(tmp_path):
    from datetime import timedelta
    path = tmp_path / "paid.jsonl"
    _archived(path)
    registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=8), trigger=10000)
    assert read(path)[0]["events_count"] == 0
    created = event(event_type="lease_created", occurred_at=(NOW + timedelta(days=8)).isoformat(), padding="")
    with path.open("a") as out:
        out.write(json.dumps(created) + "\n")
    registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=16), trigger=10000)
    assert not [e for e in registry.iter_events(path) if e["event_type"] == "lease_created"]
    assert totals(read(path)) == totals([event() for _ in range(30)])


def test_small_initial_journal_gets_daily_snapshot_marker(tmp_path):
    from datetime import timedelta
    path = tmp_path / "paid.jsonl"
    write(path, [event(event_type="lease_created", padding="")])
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert read(path)[0]["events_count"] == 1
    registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=8), trigger=10000)
    assert list(registry.iter_events(path)) == []
    assert read(path)[0]["events_count"] == 0


def test_orphan_cleanup_scans_snapshot_outside_writer_lock(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    _archived(path)
    orphan = tmp_path / ("paid.jsonl.forensics-" + "f" * 64 + ".jsonl.gz")
    orphan.write_bytes(b"abandoned")
    lock = RLock()
    original = registry._snapshot_events
    def checked(snapshot, checkpoint):
        assert not lock._is_owned()
        yield from original(snapshot, checkpoint)
    monkeypatch.setattr(registry, "_snapshot_events", checked)
    registry.compact(str(path), lock, kind="paid", now=NOW, trigger=10000)
    assert not orphan.exists()
