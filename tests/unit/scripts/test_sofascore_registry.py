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
    assert {e["event_type"] for e in read(path)} == {"bytes", "lease_created", "accounting_uncertain", "lease_closed"}
    assert sum(e["event_type"] == "lease_created" for e in read(path)) == 1
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
