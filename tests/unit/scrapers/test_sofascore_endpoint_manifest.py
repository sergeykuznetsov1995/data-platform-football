from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from scrapers.sofascore.manifest import (
    EndpointManifest,
    JsonFileManifestStore,
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


@pytest.mark.parametrize("http_status", [403, 429, 500, 503])
def test_error_http_statuses_can_never_be_success(http_status):
    with pytest.raises(ValueError, match="retryable_failure"):
        _record(_key(), http_status=http_status)


def test_manifest_has_exact_five_status_contract():
    assert {status.value for status in ManifestStatus} == {
        "success",
        "legitimate_empty",
        "not_supported",
        "retryable_failure",
        "schema_error",
    }


def test_success_requires_raw_and_rows():
    with pytest.raises(ValueError, match="row_count"):
        _record(_key(), row_count=0)
    with pytest.raises(ValueError, match="raw lineage"):
        _record(_key(), raw_content_hash=None)


def test_json_manifest_upsert_is_idempotent_and_cross_thread_atomic(tmp_path):
    store = JsonFileManifestStore(tmp_path / "manifest.json")
    records = [_record(_key(str(index))) for index in range(40)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(store.upsert, records + records))
    assert len(store.list_for_run("run-1")) == 40
    assert store.get(_key("7")) == records[7]
