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


def test_only_owned_temporary_inode_is_exempt_and_ownership_expires(tmp_path):
    from contextlib import ExitStack
    import os

    (tmp_path / ".registry-orphan.tmp").write_bytes(b"orphan")
    with ExitStack() as stack:
        fd, name = registry._temporary(tmp_path, ".registry-", stack)
        with os.fdopen(fd, "wb") as stream:
            stream.write(b"copy")
        assert registry.registry_bytes(tmp_path) == 6
        assert registry.registry_bytes(tmp_path, include_active_temporary=True) == 10
        # Replacement at the owned path is not owned by this compactor.
        other = tmp_path / "replacement"
        other.write_bytes(b"unknown")
        os.replace(other, name)
        assert registry.registry_bytes(tmp_path) == 13
    assert registry.registry_bytes(tmp_path) == 13
    assert not registry._ACTIVE_TEMPORARIES


def test_failed_compaction_does_not_leave_exempt_files(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    write(path, [event()])
    def fail(*args):
        raise OSError("injected scan failure")
    monkeypatch.setattr(registry, "_expanded_events", fail)
    with pytest.raises(OSError, match="injected scan failure"):
        registry.compact(str(path), RLock(), kind="paid", now=NOW)
    assert not registry._ACTIVE_TEMPORARIES
    assert registry.registry_bytes(tmp_path) == path.stat().st_size


def test_compacted_checkpoint_preserves_every_recovery_key_and_week_evidence(tmp_path):
    path = tmp_path / "paid.jsonl"
    events = [event() for _ in range(30)] + [event(direction="up"), event(occurred_at="2026-10-07T00:00:00Z"), event(lease_id="other"), event(event_type="lease_created"), event(event_type="accounting_uncertain", reason="disk_failed"), event(event_type="lease_closed"), event(event_type="lease_created", occurred_at="2026-09-01T00:00:00Z")]
    write(path, events)
    result = registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert totals(registry.iter_events(path)) == totals(events)
    assert totals(read(path)) == {}  # Recovery sums live in the snapshot, not the tail.
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
    # The concurrent append is copied into the raw tail exactly once; the
    # already committed prefix sums are archived, not duplicated in the tail.
    assert totals(read(path)) == totals([appended])
    assert totals(registry.iter_events(path)) == totals(events + [appended])


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
    # A concurrently appended raw event must be copied verbatim into the
    # journal tail. When that retained tail alone exceeds the trigger, the
    # compaction must fail instead of pruning evidence to fit.
    payload = json.dumps(event(lease_id="late", padding="y" * 5000)) + "\n"
    def append_large():
        with path.open("a") as out:
            out.write(payload)
    with pytest.raises(RuntimeError, match="cannot compact"):
        registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=1000, before_replace=append_large)
    # The journal generation is never replaced; the committed append survives.
    assert path.read_bytes() == original + payload.encode()
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=1000)
    expected = [event(lease_id=str(i)) for i in range(30)] + [event(lease_id="late")]
    assert totals(registry.iter_events(path)) == totals(expected)


def test_forced_same_day_rotation_below_trigger_preserves_recovery_and_evidence(tmp_path):
    path = tmp_path / "paid.jsonl"
    original = [event(), event(event_type="lease_created")]
    write(path, original)
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    appended = [event() for _ in range(6)] + [event(event_type="accounting_uncertain", reason="disk_failed")]
    with path.open("a") as out:
        out.write("".join(json.dumps(e) + "\n" for e in appended))
    before = path.stat().st_size
    assert before < registry.PAID_TRIGGER_BYTES
    assert registry.compact(str(path), RLock(), kind="paid", now=NOW) is None
    result = registry.compact(str(path), RLock(), kind="paid", now=NOW, force=True)
    assert result["after_bytes"] < before
    assert totals(registry.iter_events(path)) == totals(original + appended)
    assert totals(read(path)) == {}
    assert {e["event_type"] for e in registry.iter_events(path)} == {"bytes", "lease_created", "accounting_uncertain"}


def test_forced_rotation_without_reduction_keeps_committed_files(tmp_path):
    path = tmp_path / "paid.jsonl"
    write(path, [event(lease_id=str(i), padding="") for i in range(30)])
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    assert registry.compact(str(path), RLock(), kind="paid", now=NOW, force=True) is None
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


@pytest.mark.parametrize("fault", ["fsync", "replace"])
def test_forced_below_trigger_failure_keeps_original(tmp_path, monkeypatch, fault):
    path = tmp_path / "paid.jsonl"
    write(path, [event()])
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    with path.open("a") as out:
        out.write(json.dumps(event()) + "\n")
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    def fail(*args):
        raise OSError("injected disk failure")
    monkeypatch.setattr(registry.os, fault, fail)
    with pytest.raises(OSError, match="injected disk failure"):
        registry.compact(str(path), RLock(), kind="paid", now=NOW, force=True)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_forced_below_trigger_copies_concurrent_append_once(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    write(path, [event()])
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    with path.open("a") as out:
        out.write(json.dumps(event()) + "\n")
    expanded = registry._expanded_events
    appended = event(lease_id="during-forced-scan", bytes=17, padding="")
    def scan(*args):
        for index, item in enumerate(expanded(*args)):
            if index == 0:
                with path.open("a") as out:
                    out.write(json.dumps(appended) + "\n")
            yield item
    monkeypatch.setattr(registry, "_expanded_events", scan)
    registry.compact(str(path), RLock(), kind="paid", now=NOW, force=True)
    # Only the concurrent append remains raw; committed sums stay archived.
    assert totals(read(path)) == totals([appended])
    assert totals(registry.iter_events(path)) == totals([event(), event(), appended])


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
    # The repaired tail event is copied raw exactly once; prefix sums archive.
    assert totals(read(path)) == totals([appended])
    assert totals(registry.iter_events(path)) == totals(events + [appended])


def test_large_week_of_forensics_is_archived_without_losing_events(tmp_path):
    path = tmp_path / "paid.jsonl"
    forensic = [event(event_type="endpoint_finished", request_id=str(i), padding="") for i in range(400)]
    write(path, [event() for _ in range(30)] + forensic)
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert path.stat().st_size < 10000
    physical = read(path)
    assert physical[0]["event_type"] == "forensic_checkpoint"
    expanded = list(registry.iter_events(path))
    assert totals(expanded) == totals([event() for _ in range(30)])
    assert [e["request_id"] for e in expanded if e["event_type"] == "endpoint_finished"] == [str(i) for i in range(400)]
    assert len(list(tmp_path.glob("*.jsonl.gz"))) == 1


def _archived(path, **values):
    write(path, [event(**values) for _ in range(30)] + [event(event_type="lease_created")])
    registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    return path.parent / read(path)[0]["archive"]


@pytest.mark.parametrize("stage", ["archive_directory", "journal_replace", "journal_directory"])
@pytest.mark.parametrize("old_recovery", [False, True])
def test_crash_boundaries_preserve_committed_evidence(tmp_path, monkeypatch, stage, old_recovery):
    path = tmp_path / "paid.jsonl"
    old_archive = _archived(path, **({"occurred_at": "2026-09-01T00:00:00+00:00"} if old_recovery else {}))
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


@pytest.mark.parametrize("fault", ["missing", "digest", "count", "gzip", "path", "type", "oversized", "version"])
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
    elif fault == "version":
        checkpoint["schema_version"] = 3
    elif fault in {"gzip", "type", "oversized"}:
        raw = b"not gzip" if fault == "gzip" else gzip.compress(
            b"x" * (registry.MAX_LINE_BYTES + 1) if fault == "oversized" else registry._line(event(source="whoscored")), mtime=0,
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
    assert totals(registry.iter_events(path)) == totals([event() for _ in range(30)])
    assert totals(read(path)) == {}


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
    assert read(path)[0]["events_count"] == 1  # Recovery bytes never expire.
    created = event(event_type="lease_created", occurred_at=(NOW + timedelta(days=8)).isoformat(), padding="")
    with path.open("a") as out:
        out.write(json.dumps(created) + "\n")
    registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=16), trigger=10000)
    assert not [e for e in registry.iter_events(path) if e["event_type"] == "lease_created"]
    assert totals(registry.iter_events(path)) == totals([event() for _ in range(30)])


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


def test_old_daily_sums_move_to_archive_without_losing_any_recovery_key(tmp_path):
    from datetime import timedelta
    path = tmp_path / "paid.jsonl"
    old = event(occurred_at="2026-09-01T00:00:00+00:00", padding="")
    rows = [old]
    for field in registry.RECOVERY_FIELDS:
        value = "up" if field == "direction" else "sofascore_discovery" if field == "source" else "other"
        rows.append({**old, field: value})
    # The cutoff is Oct 2 at noon. Recovery sums are archived for every UTC
    # day, fresh days included; only seven-day non-bytes evidence expires.
    rows += [event(occurred_at="2026-10-01T23:59:59+00:00", padding=""),
             event(occurred_at="2026-10-02T00:00:00+00:00", padding=""),
             event(event_type="endpoint_finished", request_id="recent", padding="")]
    write(path, rows)
    now = NOW + timedelta(hours=12)
    registry.compact(str(path), RLock(), kind="paid", now=now)
    assert read(path)[0]["schema_version"] == 2
    assert not [e for e in read(path) if e["event_type"] == "bytes"]
    assert totals(registry.iter_events(path)) == totals(rows)
    assert any(e.get("request_id") == "recent" for e in registry.iter_events(path))
    # Daily refresh must retain archived recovery even after evidence expires.
    registry.compact(str(path), RLock(), kind="paid", now=now + timedelta(days=20))
    assert totals(registry.iter_events(path)) == totals(rows)
    assert totals(read(path)) == {}
    assert not any(e.get("request_id") == "recent" for e in registry.iter_events(path))


@pytest.mark.parametrize("fault", ["fsync", "replace"])
def test_archiving_old_sums_failure_preserves_recovery(tmp_path, monkeypatch, fault):
    path = tmp_path / "paid.jsonl"
    rows = [event(occurred_at="2026-09-01T00:00:00+00:00", lease_id=str(i), padding="") for i in range(30)]
    write(path, rows)
    original = path.read_bytes()
    def fail(*args):
        raise OSError("injected disk failure")
    with monkeypatch.context() as patch:
        patch.setattr(registry.os, fault, fail)
        with pytest.raises(OSError):
            registry.compact(str(path), RLock(), kind="paid", now=NOW)
    assert path.read_bytes() == original
    assert totals(registry.iter_events(path)) == totals(rows)
    registry.compact(str(path), RLock(), kind="paid", now=NOW)
    assert totals(registry.iter_events(path)) == totals(rows)
    archive = tmp_path / read(path)[0]["archive"]
    archive.write_bytes(archive.read_bytes() + b"corrupted")
    with pytest.raises(RuntimeError, match="digest"):
        registry.validate_checkpoint(str(path))


def test_archiving_old_sums_copies_concurrent_append_once(tmp_path, monkeypatch):
    path = tmp_path / "paid.jsonl"
    old = event(occurred_at="2026-09-01T00:00:00+00:00", padding="")
    write(path, [old])
    expanded = registry._expanded_events
    appended = {**old, "lease_id": "concurrent", "bytes": 17}
    def scan(*args):
        for item in expanded(*args):
            with path.open("a") as out:
                out.write(json.dumps(appended) + "\n")
            yield item
    with monkeypatch.context() as patch:
        patch.setattr(registry, "_expanded_events", scan)
        registry.compact(str(path), RLock(), kind="paid", now=NOW)
    assert totals(registry.iter_events(path)) == totals([old, appended])
    from datetime import timedelta
    registry.compact(str(path), RLock(), kind="paid", now=NOW + timedelta(days=1))
    assert totals(registry.iter_events(path)) == totals([old, appended])


def test_v1_checkpoint_remains_readable_but_cannot_hide_recovery_bytes(tmp_path):
    import gzip
    import hashlib
    path = tmp_path / "paid.jsonl"
    for row in (event(event_type="endpoint_finished", padding=""), event(padding="")):
        raw = registry._line(row)
        packed = gzip.compress(raw, mtime=0)
        digest = hashlib.sha256(packed).hexdigest()
        name = f"paid.jsonl.forensics-{digest}.jsonl.gz"
        (tmp_path / name).write_bytes(packed)
        write(path, [{"event_type": registry.CHECKPOINT_TYPE, "schema_version": 1,
                      "source": "sofascore", "archive": name, "sha256": digest,
                      "events_count": 1, "uncompressed_bytes": len(raw)}])
        if row["event_type"] == "bytes":
            with pytest.raises(RuntimeError, match="event type"):
                list(registry.iter_events(path))
        else:
            assert list(registry.iter_events(path)) == [row]
            old = event(occurred_at="2026-09-01T00:00:00+00:00", padding="")
            with path.open("a") as out:
                out.write(json.dumps(old) + "\n")
            registry.compact(str(path), RLock(), kind="paid", now=NOW)
            assert read(path)[0]["schema_version"] == 2
            assert totals(registry.iter_events(path)) == totals([old])
            assert next(registry.iter_events(path)) == {k: row[k] for k in registry.FORENSIC_FIELDS if k in row}


# --- Crash recovery and cross-process compaction locking -------------------

import os
import signal
import subprocess
import sys
import time

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _child_compaction_script(kind="paid", stage="replace"):
    # The child compacts but blocks at a chosen stage with its working
    # temporaries (partially) written. It signals readiness with a flag file
    # and waits for a "go" file it may never get. A fixed NOW keeps forensic
    # retention assertions independent of the actual wall-clock date.
    return f"""
import os, sys, time
sys.path.insert(0, {REPO_ROOT!r})
from datetime import datetime, timezone
from scripts.proxy_filter import sofascore_registry as registry
from threading import RLock

path, flag, go = sys.argv[1], sys.argv[2], sys.argv[3]
now = datetime(2026, 10, 9, tzinfo=timezone.utc)

def wait_for_go():
    with open(flag, "w") as marker:
        marker.write("ready")
    deadline = time.monotonic() + 30
    while not os.path.exists(go):
        if time.monotonic() > deadline:
            raise RuntimeError("child timed out waiting for go")
        time.sleep(0.01)

stage = {stage!r}
if stage == "replace":
    # After every temporary has been fully written and fsynced, before the
    # final commit.
    real_replace = os.replace
    def blocking_replace(source, target):
        if str(target) == str(path):
            wait_for_go()
        return real_replace(source, target)
    registry.os.replace = blocking_replace
else:
    # Midstream in _expanded_events, after part of the forensic snapshot
    # output has already been produced into the working temporaries.
    original = registry._expanded_events
    def scan(*args):
        for index, item in enumerate(original(*args)):
            if index == 3:
                wait_for_go()
            yield item
    registry._expanded_events = scan
result = registry.compact(path, RLock(), kind={kind!r}, trigger=10000, now=now)
print("RESULT", result, flush=True)
"""


def _start_child(tmp_path, path, kind="paid", stage="replace", timeout=10.0):
    flag = tmp_path / "child-ready"
    go = tmp_path / "child-go"
    child = subprocess.Popen(
        [sys.executable, "-c", _child_compaction_script(kind, stage), str(path), str(flag), str(go)],
        cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        if not _wait_for(lambda: flag.exists(), timeout=timeout):
            raise AssertionError("child never reached the commit point")
    except BaseException:
        # Never leak a live or zombie child if readiness cannot be confirmed.
        child.kill()
        child.wait(timeout=10)
        raise
    return child, go


def _work_temps(directory):
    return [p for p in directory.iterdir()
            if p.name.startswith(".registry-work-") and not p.is_symlink()]


@pytest.mark.parametrize("stage", ["replace", "midstream"])
def test_sigkill_mid_compaction_orphan_reclaimed_and_replay_exact(tmp_path, stage):
    path = tmp_path / "paid.jsonl"
    # Incompressible forensic events first, so the midstream kill happens
    # after part of the snapshot output is already flushed to its temporary.
    # The error field is retained forensic evidence; incompressible payloads
    # overflow the gzip buffer so part of the snapshot reaches its temporary.
    forensic = [event(event_type="endpoint_finished", request_id=f"keep-{i}",
                      error=os.urandom(20000).hex()) for i in range(8)]
    events = forensic + [event() for _ in range(30)]
    write(path, events)
    original = path.read_bytes()
    expected = totals(registry.iter_events(path))

    child, _go = _start_child(tmp_path, path, stage=stage)
    orphans = _work_temps(tmp_path)
    assert orphans  # The child blocked with its working temporaries on disk.
    if stage == "midstream":
        assert any(p.stat().st_size > 0 for p in orphans)  # Partial output produced.
    child.send_signal(signal.SIGKILL)
    child.wait(timeout=10)

    # The durable journal is untouched; the orphan temps now count toward caps.
    assert path.read_bytes() == original
    assert totals(registry.iter_events(path)) == expected
    assert _work_temps(tmp_path) == orphans
    assert registry.registry_bytes(tmp_path) >= path.stat().st_size + sum(p.stat().st_size for p in orphans)

    # The next compaction reclaims only the new-namespace orphans and replays.
    result = registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert result is not None
    assert _work_temps(tmp_path) == []
    assert totals(registry.iter_events(path)) == expected
    assert {e.get("request_id") for e in registry.iter_events(path)} >= {f"keep-{i}" for i in range(8)}


def test_concurrent_compactor_returns_none_and_preserves_active_temps(tmp_path):
    path = tmp_path / "paid.jsonl"
    events = [event() for _ in range(30)] + [event(event_type="endpoint_finished", request_id="keep")]
    write(path, events)
    expected = totals(registry.iter_events(path))
    legacy = tmp_path / ".registry-legacy-orphan.tmp"
    legacy.write_bytes(b"verified legacy orphan")
    foreign = tmp_path / ".registry-work-foreign00.tmp"
    foreign.symlink_to(path)  # A symlink in the new namespace is never unlinked.

    child, go = _start_child(tmp_path, path)
    active = _work_temps(tmp_path)
    assert active
    try:
        started = time.monotonic()
        assert registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000) is None
        assert time.monotonic() - started < 5  # Contention returns promptly.
        # The contending call never deletes the live compactor's working
        # files, legacy temporaries, foreign symlinks, or the lock file.
        assert _work_temps(tmp_path) == active
        assert legacy.read_bytes() == b"verified legacy orphan"
        assert foreign.is_symlink()
        lock_file = tmp_path / registry.COMPACTION_LOCK_NAME
        assert lock_file.is_file() and not lock_file.is_symlink()
    finally:
        go.write_text("go")
    out, err = child.communicate(timeout=30)
    assert child.returncode == 0, err
    assert "RESULT {" in out

    # The first compactor finished a valid committed generation.
    assert totals(registry.iter_events(path)) == expected
    assert _work_temps(tmp_path) == []
    assert legacy.exists()  # Legacy orphans remain the operator's concern.
    assert foreign.is_symlink()
    assert (tmp_path / registry.COMPACTION_LOCK_NAME).exists()

    # After the child exits, the directory is compactable again.
    with path.open("a") as tail:
        tail.write("".join(json.dumps(event()) + "\n" for _ in range(30)))
    assert registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000) is not None
    assert totals(registry.iter_events(path)) == totals(events + [event() for _ in range(30)])


def test_contention_closes_lock_descriptor_exactly_once(tmp_path, monkeypatch):
    from contextlib import ExitStack
    closed = []
    real_close = os.close
    def tracking_close(descriptor):
        closed.append(descriptor)
        real_close(descriptor)
    def contended(descriptor, how):
        raise BlockingIOError("another compactor owns this directory")
    monkeypatch.setattr(registry.os, "close", tracking_close)
    monkeypatch.setattr(registry.fcntl, "flock", contended)
    with ExitStack() as stack:
        assert registry._acquire_compaction_lock(tmp_path, stack) is None
    # A double close would raise OSError during unwind; exactly one close happened.
    assert len(closed) == 1  # Closed exactly once, before returning None.


def test_repeated_real_contention_does_not_leak_descriptors(tmp_path):
    import fcntl
    from contextlib import ExitStack
    holder = os.open(tmp_path / registry.COMPACTION_LOCK_NAME, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = len(os.listdir("/proc/self/fd"))
        for _ in range(25):
            with ExitStack() as stack:
                assert registry._acquire_compaction_lock(tmp_path, stack) is None
            assert registry.compact(str(tmp_path / "paid.jsonl"), RLock(), kind="paid", now=NOW) is None
        assert len(os.listdir("/proc/self/fd")) == before
    finally:
        os.close(holder)


def test_flock_failure_propagates_and_closes_descriptor(tmp_path, monkeypatch):
    from contextlib import ExitStack
    closed = []
    real_close = os.close
    def tracking_close(descriptor):
        closed.append(descriptor)
        real_close(descriptor)
    def broken(descriptor, how):
        raise OSError("injected flock failure")
    monkeypatch.setattr(registry.os, "close", tracking_close)
    monkeypatch.setattr(registry.fcntl, "flock", broken)
    with ExitStack() as stack:
        with pytest.raises(OSError, match="injected flock failure"):
            registry._acquire_compaction_lock(tmp_path, stack)
    assert len(closed) == 1


def test_symlink_lock_file_fails_closed(tmp_path):
    lock = tmp_path / registry.COMPACTION_LOCK_NAME
    target = tmp_path / "elsewhere.lock"
    target.write_bytes(b"foreign")
    lock.symlink_to(target)
    with pytest.raises(OSError):  # O_NOFOLLOW refuses the symlink.
        registry.compact(str(tmp_path / "paid.jsonl"), RLock(), kind="paid", now=NOW)
    assert lock.is_symlink() and target.read_bytes() == b"foreign"


def test_nonregular_lock_file_fails_closed_and_is_preserved(tmp_path):
    lock = tmp_path / registry.COMPACTION_LOCK_NAME
    os.mkfifo(lock)
    with pytest.raises(RuntimeError, match="not a regular file"):
        registry.compact(str(tmp_path / "paid.jsonl"), RLock(), kind="paid", now=NOW)
    assert not lock.is_file() and lock.exists()  # Never unlinked or replaced.


def test_foreign_entries_survive_cleanup_on_successful_acquisition(tmp_path):
    path = tmp_path / "paid.jsonl"
    events = [event() for _ in range(30)] + [event(event_type="endpoint_finished", request_id="keep")]
    write(path, events)
    foreign_file = tmp_path / ".registry-work-foreign.tmp"  # 7 chars: outside exact-8 rule.
    foreign_file.write_bytes(b"foreign regular file")
    matching_link = tmp_path / ".registry-work-zzzzzzzz.tmp"  # 8 chars but a symlink.
    matching_link.symlink_to(path)
    stale_owned = tmp_path / ".registry-work-abcdef12.tmp"  # 8-char regular: reclaimed.
    stale_owned.write_bytes(b"crashed generation")
    result = registry.compact(str(path), RLock(), kind="paid", now=NOW, trigger=10000)
    assert result is not None
    assert foreign_file.read_bytes() == b"foreign regular file"
    assert matching_link.is_symlink()
    assert not stale_owned.exists()  # The exact-8char rule only reclaimed this one.
    assert totals(registry.iter_events(path)) == totals(events)


def test_start_child_readiness_failure_kills_and_reaps_child(tmp_path, monkeypatch):
    path = tmp_path / "wal.jsonl"
    write(path, [event(event_type="claim_intent", padding="")])  # Below trigger: child exits without flag.
    spawned = []
    real_popen = subprocess.Popen
    def tracking_popen(*args, **kwargs):
        proc = real_popen(*args, **kwargs)
        spawned.append(proc)
        return proc
    monkeypatch.setattr(subprocess, "Popen", tracking_popen)
    with pytest.raises(AssertionError, match="never reached"):
        _start_child(tmp_path, path, kind="wal", timeout=0.5)
    assert spawned and spawned[0].poll() is not None  # Killed/exited and reaped: no zombie.


def test_missing_parent_returns_none_without_creating_directories(tmp_path):
    missing = tmp_path / "absent" / "paid.jsonl"
    assert registry.compact(str(missing), RLock(), kind="paid", now=NOW) is None
    assert registry.compact(str(missing), RLock(), kind="wal", now=NOW) is None
    assert not (tmp_path / "absent").exists()
    assert not (tmp_path / registry.COMPACTION_LOCK_NAME).exists()
