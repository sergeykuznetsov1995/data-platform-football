"""Batched manifest commits bound Iceberg snapshot growth (#1003)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest

from scrapers.sofascore.adapters import (
    MANIFEST_COLUMNS,
    MANIFEST_KEY_COLUMNS,
    TrinoManifestStore,
)
from scrapers.sofascore.manifest import (
    BatchingManifestStore,
    EndpointManifest,
    InMemoryManifestStore,
    ManifestKey,
    ManifestStatus,
)


def _key(target_id="1", endpoint="event"):
    return ManifestKey("17", "76986", "event", target_id, endpoint, "finished-v1")


def _record(key, status=ManifestStatus.SUCCESS, **overrides):
    values = {
        "key": key,
        "status": status,
        "run_id": "run-1",
        "task_id": "capture",
        "attempts": 1,
        "row_count": 1,
        "http_status": 200,
        "raw_content_hash": "a" * 64,
        "raw_blob_key": "blobs/a.json.gz",
    }
    values.update(overrides)
    return EndpointManifest(**values)


class _CountingInner(InMemoryManifestStore):
    """InMemory store that also counts bulk/single write calls."""

    def __init__(self):
        super().__init__()
        self.bulk_calls = []
        self.single_calls = 0
        self.fail_bulk = False
        self.poison_keys = set()

    def upsert(self, record):
        if record.key in self.poison_keys:
            raise RuntimeError(f"poison row {record.key.target_id}")
        self.single_calls += 1
        super().upsert(record)

    def upsert_many(self, records):
        if self.fail_bulk:
            raise RuntimeError("bulk MERGE failed")
        self.bulk_calls.append(list(records))
        for record in records:
            super().upsert(record)


def test_upsert_buffers_until_flush_and_get_reads_pending():
    inner = _CountingInner()
    store = BatchingManifestStore(inner, max_pending=10)
    record = _record(_key("100"))
    store.upsert(record)
    assert inner.bulk_calls == [] and inner.single_calls == 0
    assert inner.get(record.key) is None
    assert store.get(record.key) is record
    store.flush()
    assert len(inner.bulk_calls) == 1
    assert inner.get(record.key) == record
    assert store.pending_count == 0


def test_reaching_max_pending_auto_flushes_one_bulk_write():
    inner = _CountingInner()
    store = BatchingManifestStore(inner, max_pending=5)
    for index in range(5):
        store.upsert(_record(_key(str(index))))
    assert len(inner.bulk_calls) == 1
    assert len(inner.bulk_calls[0]) == 5
    assert store.pending_count == 0


def test_rewriting_same_key_keeps_last_record_and_one_slot():
    inner = _CountingInner()
    store = BatchingManifestStore(inner, max_pending=10)
    key = _key("7")
    store.upsert(_record(key, status=ManifestStatus.RETRYABLE_FAILURE,
                         http_status=503, raw_content_hash=None,
                         raw_blob_key=None, row_count=0))
    final = _record(key, attempts=2)
    store.upsert(final)
    assert store.pending_count == 1
    assert store.get(key) is final
    store.flush()
    assert inner.get(key) == final
    assert len(inner.bulk_calls) == 1 and len(inner.bulk_calls[0]) == 1


def test_list_for_run_flushes_first():
    inner = _CountingInner()
    store = BatchingManifestStore(inner, max_pending=10)
    store.upsert(_record(_key("1")))
    store.upsert(_record(_key("2")))
    listed = list(store.list_for_run("run-1"))
    assert len(listed) == 2
    assert store.pending_count == 0
    assert len(inner.bulk_calls) == 1


def test_flush_is_noop_when_empty():
    inner = _CountingInner()
    store = BatchingManifestStore(inner, max_pending=10)
    store.flush()
    assert inner.bulk_calls == [] and inner.single_calls == 0


def test_max_pending_one_restores_record_at_a_time_commits():
    inner = _CountingInner()
    store = BatchingManifestStore(inner, max_pending=1)
    for index in range(3):
        store.upsert(_record(_key(str(index))))
    assert len(inner.bulk_calls) == 3
    assert all(len(call) == 1 for call in inner.bulk_calls)


def test_bulk_failure_falls_back_per_record_and_poison_row_stays_pending():
    inner = _CountingInner()
    inner.fail_bulk = True
    poison_key = _key("13")
    inner.poison_keys.add(poison_key)
    store = BatchingManifestStore(inner, max_pending=10)
    healthy = [_record(_key(str(index))) for index in range(3)]
    for record in healthy:
        store.upsert(record)
    store.upsert(_record(poison_key))
    with pytest.raises(RuntimeError, match="poison row"):
        store.flush()
    # Healthy records committed despite the failed bulk write; the poison
    # record was not dropped — it stays pending for the task retry.
    for record in healthy:
        assert inner.get(record.key) == record
    assert store.pending_count == 1
    assert store.get(poison_key) is not None


def test_inner_without_upsert_many_uses_per_record_writes():
    inner = InMemoryManifestStore()
    store = BatchingManifestStore(inner, max_pending=10)
    records = [_record(_key(str(index))) for index in range(4)]
    for record in records:
        store.upsert(record)
    store.flush()
    for record in records:
        assert inner.get(record.key) == record
    assert store.pending_count == 0


def test_concurrent_upserts_never_lose_records():
    inner = _CountingInner()
    store = BatchingManifestStore(inner, max_pending=7)
    records = [_record(_key(str(index))) for index in range(80)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(store.upsert, records))
    store.flush()
    for record in records:
        assert inner.get(record.key) is not None
    assert sum(len(call) for call in inner.bulk_calls) == 80


def test_rejects_non_positive_max_pending():
    with pytest.raises(ValueError, match="max_pending"):
        BatchingManifestStore(InMemoryManifestStore(), max_pending=0)


class _RecordingManager:
    catalog = "iceberg"

    def __init__(self):
        self.calls = []

    def create_schema(self, schema):
        pass

    def _execute(self, sql, fetch=False, params=None):
        return []

    def insert_dataframe_atomic(self, schema, table, df, *, merge_keys):
        self.calls.append((schema, table, df.copy(), tuple(merge_keys)))
        return len(df)


def test_trino_upsert_many_is_one_merge_with_deduped_rows():
    manager = _RecordingManager()
    store = TrinoManifestStore(manager, ensure_table=False)
    duplicate_key = _key("1")
    stale = _record(duplicate_key, attempts=1)
    fresh = _record(duplicate_key, attempts=2)
    other = _record(_key("2"))
    store.upsert_many([stale, other, fresh])
    assert len(manager.calls) == 1
    schema, table, frame, merge_keys = manager.calls[0]
    assert (schema, table) == ("ops", "sofascore_capture_manifest")
    assert merge_keys == MANIFEST_KEY_COLUMNS
    assert list(frame.columns) == list(MANIFEST_COLUMNS)
    assert len(frame) == 2
    duplicated = frame[frame["target_id"] == "1"]
    assert len(duplicated) == 1
    assert int(duplicated.iloc[0]["attempts"]) == 2


def test_trino_upsert_many_with_no_records_is_a_noop():
    manager = _RecordingManager()
    store = TrinoManifestStore(manager, ensure_table=False)
    store.upsert_many([])
    assert manager.calls == []


def test_build_capture_runtime_wraps_trino_backend_in_batching(monkeypatch):
    import scrapers.base.trino_manager as trino_manager
    from scrapers.sofascore.pipeline import build_capture_runtime

    class _StubManager:
        catalog = "iceberg"

        def create_schema(self, schema):
            pass

        def _execute(self, sql, fetch=False, params=None):
            return []

        def insert_dataframe_atomic(self, *args, **kwargs):
            return 0

    monkeypatch.setattr(trino_manager, "TrinoTableManager", _StubManager)
    monkeypatch.setenv("SOFASCORE_MANIFEST_BATCH_SIZE", "37")
    monkeypatch.delenv("SOFASCORE_PROXY_BUDGET_ARTIFACT", raising=False)
    monkeypatch.delenv("SOFASCORE_PROXY_BUDGET_LEDGER", raising=False)
    runtime = build_capture_runtime(
        run_id="run-1",
        task_id="task-1",
        manifest_backend="trino",
    )
    assert isinstance(runtime.manifest_store, BatchingManifestStore)
    assert runtime.manifest_store.max_pending == 37
    assert isinstance(runtime.manifest_store.inner, TrinoManifestStore)
    assert runtime.engine.manifest_store is runtime.manifest_store
