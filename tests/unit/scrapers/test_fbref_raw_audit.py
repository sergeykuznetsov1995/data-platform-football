import gzip
import hashlib
import json
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from threading import Barrier
from unittest.mock import MagicMock

import pytest

import scrapers.fbref.raw_audit as raw_audit_module
from scrapers.fbref.raw_audit import (
    RAW_AUDIT_MAX_ATTEMPTS,
    RAW_INVENTORY_EVIDENCE_LIMIT,
    RawAuditError,
    audit_raw_fetches,
    capture_and_write_raw_inventory,
    capture_raw_inventory,
    load_inventory_baseline,
    load_successful_run_attempts,
    write_audit_artifact,
    write_inventory_baseline,
)
from scrapers.fbref.raw_store import RawPageStore, match_page_target


RUN_ID = "8ca16a99-4039-44a6-a47d-206037f11e70"
REFRESH_ID = "bb254c88-c23a-4cd8-bd88-1c68c45baa2e"
ATTEMPT_ID = "f1c2d5bb-a992-4423-baad-49841a1f1140"


def _seed(tmp_path):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    record = store.commit_fetch(
        match_page_target("a071faa8"),
        b"<html>production evidence</html>",
        logical_refresh_id=REFRESH_ID,
        attempt_id=ATTEMPT_ID,
        http_status=200,
        wire_bytes=42,
        provider_billed_bytes=44,
    )
    attempt = {
        "logical_refresh_id": REFRESH_ID,
        "attempt_id": ATTEMPT_ID,
        "target_id": record.target_id,
        "content_hash": record.content_hash,
        "raw_manifest_key": store.fetch_manifest_key(REFRESH_ID),
        "decoded_bytes": record.decoded_bytes,
        "compressed_bytes": record.encoded_bytes,
        "wire_bytes": 42,
        "provider_billed_bytes": 44,
    }
    return store, record, attempt


def test_raw_audit_verifies_every_object_without_raw_store_writes(
    tmp_path, monkeypatch
):
    store, record, attempt = _seed(tmp_path)
    baseline = capture_raw_inventory(store)
    write_bytes = MagicMock(side_effect=AssertionError("unexpected raw write"))
    write_json = MagicMock(side_effect=AssertionError("unexpected raw write"))
    monkeypatch.setattr(store, "_write_bytes", write_bytes)
    monkeypatch.setattr(store, "_write_json", write_json)

    result = audit_raw_fetches(
        store,
        [attempt],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
    )

    assert result["status"] == "passed"
    assert result["audited_attempt_count"] == 1
    assert (
        result["raw_inventory_before"]["metadata_fingerprint_sha256"]
        == result["raw_inventory_after"]["metadata_fingerprint_sha256"]
    )
    assert "objects" not in result["raw_inventory_before"]
    assert "objects" not in result["raw_inventory_after"]
    assert result["attempts"][0]["content_blob_key"] == record.blob_key
    write_bytes.assert_not_called()
    write_json.assert_not_called()


def test_raw_audit_reports_corrupt_gzip_and_does_not_hide_failure(tmp_path):
    store, record, attempt = _seed(tmp_path)
    path = store._path(record.blob_key)
    with store.filesystem.open_output_stream(path, compression=None) as stream:
        stream.write(b"not gzip")
    baseline = capture_raw_inventory(store)

    result = audit_raw_fetches(
        store,
        [attempt],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
    )

    assert result["status"] == "failed"
    assert result["audited_attempt_count"] == 0
    assert result["failures"][0]["error_class"] == "RawPageCorrupt"


def test_raw_audit_validates_304_response_and_effective_content(tmp_path):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    target = match_page_target("a071faa8")
    base = store.commit_fetch(
        target,
        b"<html>cached</html>",
        logical_refresh_id="6bc011ca-d9ec-4e48-88bd-4f4782b67a8c",
        http_status=200,
    )
    record = store.commit_fetch(
        target,
        b"",
        logical_refresh_id=REFRESH_ID,
        attempt_id=ATTEMPT_ID,
        http_status=304,
        base_content_hash=base.content_hash,
        wire_bytes=5,
    )
    attempt = {
        "logical_refresh_id": REFRESH_ID,
        "attempt_id": ATTEMPT_ID,
        "target_id": record.target_id,
        "content_hash": record.content_hash,
        "raw_manifest_key": store.fetch_manifest_key(REFRESH_ID),
        "decoded_bytes": record.decoded_bytes,
        "compressed_bytes": record.encoded_bytes,
        "wire_bytes": 5,
    }

    result = audit_raw_fetches(
        store,
        [attempt],
        control_run_id=RUN_ID,
        baseline_inventory=capture_raw_inventory(store),
    )

    assert result["status"] == "passed"
    assert result["attempts"][0]["not_modified"] is True
    assert result["attempts"][0]["response_bytes"] == 0


def test_raw_audit_accepts_exact_refresh_zero_network_recovery(tmp_path):
    store, record, source = _seed(tmp_path)
    recovery_attempt_id = "d361d908-ea21-47aa-a7fc-e214ca9357b6"
    source.update(
        {
            "run_id": RUN_ID,
            "attempt_number": 1,
            "status": "failed",
            "http_status": record.http_status,
            "http_request_count": record.http_requests,
            "http_status_history": list(record.http_status_history),
            "transport_version": record.transport_version,
            "session_version": record.session_version,
            "latency_ms": record.latency_ms,
            "etag": record.etag,
            "last_modified": record.last_modified,
        }
    )
    recovery = {
        "run_id": RUN_ID,
        "logical_refresh_id": REFRESH_ID,
        "attempt_id": recovery_attempt_id,
        "attempt_number": 2,
        "target_id": record.target_id,
        "content_hash": record.content_hash,
        "raw_manifest_key": store.fetch_manifest_key(REFRESH_ID),
        "decoded_bytes": 0,
        "compressed_bytes": 0,
        "wire_bytes": 0,
        "provider_billed_bytes": None,
        "http_status": record.http_status,
        "http_request_count": 0,
        "http_status_history": [],
        "transport_version": "raw-recovery",
        "session_version": None,
        "latency_ms": 0,
        "raw_recovery_source_attempts": [source],
    }

    result = audit_raw_fetches(
        store,
        [recovery],
        control_run_id=RUN_ID,
        baseline_inventory=capture_raw_inventory(store),
    )

    assert result["status"] == "passed"
    assert result["attempts"][0]["attempt_id"] == recovery_attempt_id
    assert result["attempts"][0]["raw_source_attempt_id"] == ATTEMPT_ID
    assert result["attempts"][0]["recovered_from_raw"] is True


def test_raw_audit_rejects_recovery_with_network_counters(tmp_path):
    store, record, source = _seed(tmp_path)
    source.update(
        {
            "run_id": RUN_ID,
            "attempt_number": 1,
            "status": "failed",
            "http_status": record.http_status,
            "http_request_count": record.http_requests,
            "http_status_history": list(record.http_status_history),
            "transport_version": record.transport_version,
            "session_version": record.session_version,
            "latency_ms": record.latency_ms,
            "etag": record.etag,
            "last_modified": record.last_modified,
        }
    )
    recovery = {
        **source,
        "attempt_id": "d361d908-ea21-47aa-a7fc-e214ca9357b6",
        "attempt_number": 2,
        "decoded_bytes": 0,
        "compressed_bytes": 0,
        "wire_bytes": 1,
        "provider_billed_bytes": None,
        "http_request_count": 0,
        "http_status_history": [],
        "transport_version": "raw-recovery",
        "session_version": None,
        "latency_ms": 0,
        "raw_recovery_source_attempts": [source],
    }

    result = audit_raw_fetches(
        store,
        [recovery],
        control_run_id=RUN_ID,
        baseline_inventory=capture_raw_inventory(store),
    )

    assert result["status"] == "failed"
    assert "non-zero wire_bytes" in result["failures"][0]["error"]


def test_artifact_has_matching_sha256_sidecar(tmp_path):
    result = {
        "control_run_id": RUN_ID,
        "status": "passed",
        "attempts": [],
    }

    path, sidecar = write_audit_artifact(result, tmp_path / "artifacts")

    payload = path.read_bytes()
    assert json.loads(payload)["status"] == "passed"
    assert sidecar.read_text().split()[0] == hashlib.sha256(payload).hexdigest()
    assert gzip.decompress(gzip.compress(payload)) == payload


def test_artifact_id_prevents_replay_from_overwriting_source_artifact(tmp_path):
    result = {
        "control_run_id": RUN_ID,
        "status": "passed",
        "attempts": [],
    }

    source, _ = write_audit_artifact(result, tmp_path / "artifacts")
    replay, _ = write_audit_artifact(
        result,
        tmp_path / "artifacts",
        artifact_id="manual__replay-1",
    )

    assert source.name.startswith("raw_integrity-")
    assert source.name.endswith(".json")
    assert replay != source
    assert replay.exists()
    assert source.exists()


def test_audit_artifact_recovers_after_digest_first_crash(
    tmp_path, monkeypatch
):
    result = {
        "control_run_id": RUN_ID,
        "status": "passed",
        "attempts": [],
    }
    output = tmp_path / "artifacts"
    original = raw_audit_module._atomic_create_or_verify
    crashed = False

    def crash_after_digest(target, payload, **kwargs):
        nonlocal crashed
        created = original(target, payload, **kwargs)
        if str(target).endswith(".sha256") and not crashed:
            crashed = True
            raise RuntimeError("simulated artifact crash")
        return created

    monkeypatch.setattr(
        raw_audit_module, "_atomic_create_or_verify", crash_after_digest
    )
    with pytest.raises(RuntimeError, match="simulated artifact crash"):
        write_audit_artifact(result, output)
    monkeypatch.setattr(
        raw_audit_module, "_atomic_create_or_verify", original
    )

    path, sidecar = write_audit_artifact(result, output)

    payload = path.read_bytes()
    assert sidecar.read_text(encoding="ascii") == (
        f"{hashlib.sha256(payload).hexdigest()}  {path.name}\n"
    )
    assert stat.S_IMODE(path.stat().st_mode) == 0o440


def test_concurrent_audit_artifacts_can_never_cross_pair(tmp_path):
    output = tmp_path / "artifacts"
    results = [
        {
            "control_run_id": RUN_ID,
            "status": status,
            "attempts": [],
        }
        for status in ("passed", "failed")
    ]
    barrier = Barrier(2)

    def publish(result):
        barrier.wait()
        return write_audit_artifact(
            result, output, artifact_id="same-airflow-run"
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        pairs = list(executor.map(publish, results))

    assert pairs[0][0] != pairs[1][0]
    for path, sidecar in pairs:
        payload = path.read_bytes()
        assert sidecar.read_text(encoding="ascii") == (
            f"{hashlib.sha256(payload).hexdigest()}  {path.name}\n"
        )


def test_baseline_detects_new_object_not_linked_to_successful_attempt(tmp_path):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    baseline = capture_raw_inventory(store)
    store._write_bytes("blobs/sha256/aa/unlinked.html.gz", b"orphan")

    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_nonempty=False,
    )

    assert result["status"] == "failed"
    assert result["baseline_delta"]["unlinked_created_object_count"] == 1
    assert any(
        failure["error_class"] == "UnlinkedRawObject"
        for failure in result["failures"]
    )


def test_production_audit_rejects_missing_baseline_by_default(tmp_path):
    store, _record, attempt = _seed(tmp_path)

    result = audit_raw_fetches(store, [attempt], control_run_id=RUN_ID)

    assert result["status"] == "failed"
    assert any(
        failure["error_class"] == "MissingBaselineInventory"
        for failure in result["failures"]
    )


def test_diagnostic_audit_can_explicitly_allow_missing_baseline(tmp_path):
    store, _record, attempt = _seed(tmp_path)

    result = audit_raw_fetches(
        store,
        [attempt],
        control_run_id=RUN_ID,
        require_baseline=False,
    )

    assert result["status"] == "passed"


def test_baseline_detects_content_overwrite_of_preexisting_immutable_object(
    tmp_path,
):
    store, _record, attempt = _seed(tmp_path)
    baseline = capture_raw_inventory(store)
    target = match_page_target("a071faa8")
    history_key = store._v2_target_history_manifest_key(target, REFRESH_ID)
    # Equivalent JSON remains semantically readable, so only the content-hash
    # inventory can prove that the append-only object was overwritten.
    original = store._read_bytes(history_key)
    same_size_rewrite = original.replace(b"\n", b" ", 1)
    assert same_size_rewrite != original
    assert len(same_size_rewrite) == len(original)
    with store.filesystem.open_output_stream(
        store._path(history_key), compression=None
    ) as stream:
        stream.write(same_size_rewrite)

    result = audit_raw_fetches(
        store,
        [attempt],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
    )

    assert result["status"] == "failed"
    assert result["baseline_delta"]["immutable_content_changes"] == [
        history_key
    ]
    assert any(
        failure["error_class"] == "ImmutableRawObjectChanged"
        for failure in result["failures"]
    )


def test_baseline_rejects_same_content_rewrite_of_immutable_object(tmp_path):
    store, _record, attempt = _seed(tmp_path)
    baseline = capture_raw_inventory(store)
    target = match_page_target("a071faa8")
    history_key = store._v2_target_history_manifest_key(target, REFRESH_ID)
    path = store._path(history_key)
    current = os.stat(path).st_mtime_ns
    os.utime(path, ns=(current + 1_000_000_000, current + 1_000_000_000))

    result = audit_raw_fetches(
        store,
        [attempt],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
    )

    assert result["status"] == "failed"
    assert result["baseline_delta"]["immutable_metadata_changes"] == [
        history_key
    ]
    assert any(
        failure["error_class"] == "ImmutableRawObjectMetadataChanged"
        for failure in result["failures"]
    )


def test_only_exact_audited_target_mirror_may_change_since_baseline(tmp_path):
    store, _older, _attempt = _seed(tmp_path)
    baseline = capture_raw_inventory(store)
    target = match_page_target("a071faa8")
    newer_refresh = "7c9369c6-8556-44a0-8188-c4a2953b51a9"
    newer_attempt = "e90344bf-a6e1-4192-9b07-6df46044981e"
    newer = store.commit_fetch(
        target,
        b"<html>new content</html>",
        logical_refresh_id=newer_refresh,
        attempt_id=newer_attempt,
        http_status=200,
    )
    row = {
        "logical_refresh_id": newer_refresh,
        "attempt_id": newer_attempt,
        "target_id": newer.target_id,
        "content_hash": newer.content_hash,
        "raw_manifest_key": store.fetch_manifest_key(newer_refresh),
        "decoded_bytes": newer.decoded_bytes,
        "compressed_bytes": newer.encoded_bytes,
    }

    result = audit_raw_fetches(
        store,
        [row],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
    )

    mirror_key = store._v2_target_manifest_key(target)
    assert result["status"] == "passed"
    assert result["baseline_delta"]["allowed_target_mirror_changes"] == [
        mirror_key
    ]
    assert result["baseline_delta"]["immutable_content_changes"] == []


def test_unaudited_target_mirror_change_is_never_allowlisted(tmp_path):
    store, _record, audited_attempt = _seed(tmp_path)
    unaudited_target = match_page_target("096e63eb")
    store.commit_fetch(
        unaudited_target,
        b"<html>other target</html>",
        logical_refresh_id="867855e2-3db6-49fa-b56a-c2e6ff1c8502",
        attempt_id="055fbfd6-6839-4302-8ffd-1aef8a1f50ac",
        http_status=200,
    )
    baseline = capture_raw_inventory(store)
    unaudited_mirror = store._v2_target_manifest_key(unaudited_target)
    original = store._read_bytes(unaudited_mirror)
    same_size_rewrite = original.replace(b"\n", b" ", 1)
    assert len(same_size_rewrite) == len(original)
    with store.filesystem.open_output_stream(
        store._path(unaudited_mirror), compression=None
    ) as stream:
        stream.write(same_size_rewrite)

    result = audit_raw_fetches(
        store,
        [audited_attempt],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
    )

    assert result["status"] == "failed"
    assert result["baseline_delta"]["allowed_target_mirror_changes"] == []
    assert result["baseline_delta"]["immutable_content_changes"] == [
        unaudited_mirror
    ]


def test_replay_zero_delta_rejects_even_linked_raw_creation(tmp_path):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    baseline = capture_raw_inventory(store)
    record = store.commit_fetch(
        match_page_target("a071faa8"),
        b"<html>unexpected replay write</html>",
        logical_refresh_id=REFRESH_ID,
        attempt_id=ATTEMPT_ID,
        http_status=200,
    )
    attempt = {
        "logical_refresh_id": REFRESH_ID,
        "attempt_id": ATTEMPT_ID,
        "target_id": record.target_id,
        "content_hash": record.content_hash,
        "raw_manifest_key": store.fetch_manifest_key(REFRESH_ID),
        "decoded_bytes": record.decoded_bytes,
        "compressed_bytes": record.encoded_bytes,
    }

    result = audit_raw_fetches(
        store,
        [attempt],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_zero_delta=True,
    )

    assert result["baseline_delta"]["unlinked_created_object_count"] == 0
    assert any(
        failure["error_class"] == "RawDeltaForbidden"
        for failure in result["failures"]
    )


def test_live_audit_rejects_empty_successful_attempt_set(tmp_path):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())

    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=capture_raw_inventory(store),
    )

    assert result["status"] == "failed"
    assert result["failures"][0]["error_class"] == "EmptyRunEvidence"


def test_raw_audit_rejects_baseline_from_another_store(tmp_path):
    first = RawPageStore.from_uri((tmp_path / "first").as_uri())
    second = RawPageStore.from_uri((tmp_path / "second").as_uri())

    result = audit_raw_fetches(
        second,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=capture_raw_inventory(first),
        require_nonempty=False,
    )

    assert result["status"] == "failed"
    assert result["failures"][0]["error_class"] == "InvalidBaselineInventory"


def test_raw_audit_rejects_regressed_latest_target_pointer(tmp_path):
    store, older, _attempt = _seed(tmp_path)
    target = match_page_target("a071faa8")
    newer_refresh = "7c9369c6-8556-44a0-8188-c4a2953b51a9"
    newer_attempt = "e90344bf-a6e1-4192-9b07-6df46044981e"
    newer = store.commit_fetch(
        target,
        b"<html>newer</html>",
        logical_refresh_id=newer_refresh,
        attempt_id=newer_attempt,
        http_status=200,
        fetched_at="2099-07-15T13:00:00+00:00",
    )
    store._write_json(store._v2_target_manifest_key(target), older.__dict__)
    attempt = {
        "logical_refresh_id": newer_refresh,
        "attempt_id": newer_attempt,
        "target_id": newer.target_id,
        "content_hash": newer.content_hash,
        "raw_manifest_key": store.fetch_manifest_key(newer_refresh),
        "decoded_bytes": newer.decoded_bytes,
        "compressed_bytes": newer.encoded_bytes,
    }

    result = audit_raw_fetches(
        store,
        [attempt],
        control_run_id=RUN_ID,
        baseline_inventory=capture_raw_inventory(store),
    )

    assert result["status"] == "failed"
    assert "pointer regressed" in result["failures"][0]["error"]


def test_control_attempt_loader_consumes_every_stable_cursor_page():
    rows = [
        {
            "ordinal": ordinal,
            "attempt_number": 1,
            "attempt_id": attempt_uuid,
        }
        for ordinal, attempt_uuid in enumerate(
            [
                f"00000000-0000-4000-8000-{ordinal:012d}"
                for ordinal in range(251)
            ]
        )
    ]

    class Control:
        calls = []

        def list_successful_fetch_attempts(self, _run_id, *, limit, after):
            self.calls.append((limit, after))
            start = 0 if after is None else int(after[0]) + 1
            return rows[start : start + limit]

    control = Control()

    loaded = load_successful_run_attempts(control, RUN_ID)

    assert loaded == rows
    assert control.calls == [
        (250, None),
        (250, (249, 1, rows[249]["attempt_id"])),
    ]


def test_control_attempt_loader_attaches_raw_recovery_lineage():
    recovery_attempt = {
        "ordinal": 0,
        "attempt_number": 2,
        "attempt_id": "d361d908-ea21-47aa-a7fc-e214ca9357b6",
        "logical_refresh_id": REFRESH_ID,
        "transport_version": "raw-recovery",
    }
    source_attempt = {"attempt_id": ATTEMPT_ID, "attempt_number": 1}

    class Control:
        def list_successful_fetch_attempts(self, _run_id, *, limit, after):
            assert limit == 250
            assert after is None
            return [recovery_attempt]

        def list_fetch_attempts_for_refresh(self, run_id, refresh_id):
            assert run_id == RUN_ID
            assert refresh_id == REFRESH_ID
            return [source_attempt, recovery_attempt]

    loaded = load_successful_run_attempts(Control(), RUN_ID)

    assert loaded[0]["raw_recovery_source_attempts"] == [
        source_attempt,
        recovery_attempt,
    ]


def test_ephemeral_health_namespace_is_never_part_of_raw_inventory(tmp_path):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    store._write_bytes("immutable/one.bin", b"one")
    store._write_bytes("_health/preflight.bin", b"temporary")

    baseline = capture_raw_inventory(store)

    assert baseline["object_count"] == 1
    assert [item["key"] for item in baseline["objects"]] == [
        "immutable/one.bin"
    ]
    store._write_bytes("_health/concurrent.bin", b"also-temporary")
    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_nonempty=False,
    )
    assert result["status"] == "passed"
    assert result["baseline_delta"]["created_object_count"] == 0


def test_s3_inventory_uses_bounded_pages_and_persists_etag_tokens(monkeypatch):
    pages = [
        {
            "Contents": [
                {
                    "Key": "raw/immutable/a.bin",
                    "Size": 3,
                    "LastModified": datetime(
                        2026, 1, 1, tzinfo=timezone.utc
                    ),
                    "ETag": '"etag-a"',
                },
                {
                    "Key": "raw/_health/preflight.bin",
                    "Size": 1,
                    "LastModified": datetime(
                        2026, 1, 1, tzinfo=timezone.utc
                    ),
                    "ETag": '"health"',
                },
            ]
        },
        {
            "Contents": [
                {
                    "Key": "raw/immutable/b.bin",
                    "Size": 4,
                    "LastModified": datetime(
                        2026, 1, 2, tzinfo=timezone.utc
                    ),
                    "ETag": '"etag-b"',
                }
            ]
        },
    ]
    paginator = MagicMock()
    paginator.paginate.return_value = iter(pages)
    client = MagicMock()
    client.get_paginator.return_value = paginator
    monkeypatch.setattr(
        raw_audit_module, "_s3_list_client", lambda: client
    )
    store = MagicMock(root="bucket/raw")

    objects = list(raw_audit_module._walk_s3_raw_files(store))

    client.get_paginator.assert_called_once_with("list_objects_v2")
    paginator.paginate.assert_called_once_with(
        Bucket="bucket",
        Prefix="raw/",
        PaginationConfig={"PageSize": 1_000},
    )
    assert [item.path for item in objects] == [
        "bucket/raw/immutable/a.bin",
        "bucket/raw/immutable/b.bin",
    ]
    assert [item.version_token for item in objects] == [
        "s3-etag:etag-a",
        "s3-etag:etag-b",
    ]


def test_audit_reuses_baseline_hashes_then_uses_metadata_guard(
    tmp_path, monkeypatch
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    for number in range(32):
        store._write_bytes(f"bulk/{number:04d}.bin", str(number).encode())
    baseline = capture_raw_inventory(store)
    original = raw_audit_module._hash_raw_object
    hash_spy = MagicMock(side_effect=original)
    monkeypatch.setattr(raw_audit_module, "_hash_raw_object", hash_spy)

    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_nonempty=False,
    )

    assert result["status"] == "passed"
    assert hash_spy.call_count == 0
    assert result["raw_inventory_after"]["content_hashed"] is False
    assert "objects" not in result["raw_inventory_before"]
    assert "objects" not in result["raw_inventory_after"]


def test_inventory_fails_closed_at_explicit_object_bound(tmp_path):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    for number in range(3):
        store._write_bytes(f"bounded/{number}.bin", b"x")

    with pytest.raises(RawAuditError, match="object limit exceeded"):
        capture_raw_inventory(store, max_objects=2)


def test_streaming_baseline_and_disk_backed_audit_do_not_materialize_objects(
    tmp_path, monkeypatch
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    object_count = 1_537  # Cross several SQLite update batches.
    for number in range(object_count):
        store._write_bytes(f"production/{number:06d}.bin", b"payload")
    path, baseline, idempotent = capture_and_write_raw_inventory(
        store, tmp_path / "acceptance" / "baseline.json"
    )

    assert idempotent is False
    assert baseline.summary["object_count"] == object_count
    assert "objects" not in baseline.summary
    assert json.loads(path.read_text(encoding="utf-8")) == dict(
        baseline.summary
    )
    assert path.stat().st_size < 4_096
    assert baseline.summary["raw_store_identity"]["root"] == store.root
    assert (
        baseline.summary["index_semantic_fingerprint_sha256"]
        == baseline.summary["fingerprint_sha256"]
    )
    assert path.with_name(f"{path.name}.sqlite3").is_file()
    original = raw_audit_module._hash_raw_object
    hash_spy = MagicMock(side_effect=original)
    monkeypatch.setattr(raw_audit_module, "_hash_raw_object", hash_spy)

    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_nonempty=False,
    )

    assert result["status"] == "passed"
    assert hash_spy.call_count == 0


@pytest.mark.parametrize("crash_after", ["index", "digest", "commit"])
def test_streaming_baseline_recovers_from_every_publish_boundary_without_rehash(
    tmp_path, monkeypatch, crash_after
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    for number in range(16):
        store._write_bytes(f"immutable/{number:04d}.bin", b"payload")
    destination = tmp_path / "acceptance" / "baseline.json"
    original_publish = raw_audit_module._publish_prepared_file
    original_atomic = raw_audit_module._atomic_create_or_verify
    crashed = False

    def publish_then_crash(prepared, target, **kwargs):
        nonlocal crashed
        result = original_publish(prepared, target, **kwargs)
        target = str(target)
        boundary = "index" if target.endswith(".sqlite3") else "commit"
        if boundary == crash_after and not crashed:
            crashed = True
            raise RuntimeError(f"simulated crash after {boundary}")
        return result

    def create_then_crash(target, payload, **kwargs):
        nonlocal crashed
        result = original_atomic(target, payload, **kwargs)
        boundary = "digest"
        if boundary == crash_after and not crashed:
            crashed = True
            raise RuntimeError(f"simulated crash after {boundary}")
        return result

    monkeypatch.setattr(
        raw_audit_module, "_publish_prepared_file", publish_then_crash
    )
    monkeypatch.setattr(
        raw_audit_module, "_atomic_create_or_verify", create_then_crash
    )
    with pytest.raises(RuntimeError, match="simulated crash"):
        capture_and_write_raw_inventory(store, destination)
    assert crashed is True

    monkeypatch.setattr(
        raw_audit_module, "_publish_prepared_file", original_publish
    )
    monkeypatch.setattr(
        raw_audit_module, "_atomic_create_or_verify", original_atomic
    )
    original_hash = raw_audit_module._hash_raw_object
    hash_spy = MagicMock(side_effect=original_hash)
    monkeypatch.setattr(raw_audit_module, "_hash_raw_object", hash_spy)

    path, baseline, idempotent = capture_and_write_raw_inventory(
        store, destination
    )

    assert path == destination
    assert baseline.summary["object_count"] == 16
    assert idempotent is True
    assert hash_spy.call_count == 0
    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_nonempty=False,
    )
    assert result["status"] == "passed"
    assert hash_spy.call_count == 0


def test_partial_streaming_baseline_is_bound_to_original_raw_root(
    tmp_path, monkeypatch
):
    first_store = RawPageStore.from_uri((tmp_path / "raw-a").as_uri())
    second_store = RawPageStore.from_uri((tmp_path / "raw-b").as_uri())
    for store in (first_store, second_store):
        store._write_bytes("immutable/same.bin", b"same")
    destination = tmp_path / "acceptance" / "baseline.json"
    original_publish = raw_audit_module._publish_prepared_file

    def crash_after_index(prepared, target, **kwargs):
        result = original_publish(prepared, target, **kwargs)
        if str(target).endswith(".sqlite3"):
            raise RuntimeError("simulated crash after index")
        return result

    monkeypatch.setattr(
        raw_audit_module, "_publish_prepared_file", crash_after_index
    )
    with pytest.raises(RuntimeError, match="simulated crash"):
        capture_and_write_raw_inventory(first_store, destination)
    monkeypatch.setattr(
        raw_audit_module, "_publish_prepared_file", original_publish
    )

    with pytest.raises(RawAuditError, match="another raw root"):
        capture_and_write_raw_inventory(second_store, destination)


def test_concurrent_streaming_baseline_writers_use_one_full_hasher(
    tmp_path, monkeypatch
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    for number in range(32):
        store._write_bytes(f"immutable/{number:04d}.bin", b"payload")
    destination = tmp_path / "acceptance" / "baseline.json"
    workers = 4
    barrier = Barrier(workers)
    original_hash = raw_audit_module._hash_raw_object
    hash_spy = MagicMock(side_effect=original_hash)
    monkeypatch.setattr(raw_audit_module, "_hash_raw_object", hash_spy)

    def capture_same():
        barrier.wait()
        return capture_and_write_raw_inventory(store, destination)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(lambda _: capture_same(), range(workers)))

    assert [path for path, _inventory, _idempotent in results] == [
        destination
    ] * workers
    assert sum(not idempotent for _path, _inventory, idempotent in results) == 1
    digests = {inventory.baseline_sha256 for _path, inventory, _ in results}
    assert len(digests) == 1
    assert hash_spy.call_count == 32


def test_audit_hashes_only_the_single_rewritten_object(tmp_path, monkeypatch):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    store._write_bytes("immutable/a.bin", b"first")
    store._write_bytes("immutable/b.bin", b"second")
    baseline = capture_raw_inventory(store)
    store._write_bytes("immutable/b.bin", b"changed-payload")
    original = raw_audit_module._hash_raw_object
    hash_spy = MagicMock(side_effect=original)
    monkeypatch.setattr(raw_audit_module, "_hash_raw_object", hash_spy)

    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_nonempty=False,
    )

    assert result["status"] == "failed"
    assert result["baseline_delta"]["content_changed_object_count"] == 1
    assert hash_spy.call_count == 1


def test_audit_detects_same_size_rewrite_with_restored_positive_mtime(
    tmp_path, monkeypatch
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    key = "immutable/versioned.bin"
    store._write_bytes(key, b"first")
    baseline = capture_raw_inventory(store)
    object_path = store._path(key)
    original_stat = os.stat(object_path)
    store._write_bytes(key, b"other")
    os.utime(
        object_path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    original_hash = raw_audit_module._hash_raw_object
    hash_spy = MagicMock(side_effect=original_hash)
    monkeypatch.setattr(raw_audit_module, "_hash_raw_object", hash_spy)

    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_nonempty=False,
    )

    assert result["status"] == "failed"
    assert result["baseline_delta"]["content_changed_object_count"] == 1
    assert hash_spy.call_count == 1


def test_baseline_capture_detects_change_before_publishing_commit(
    tmp_path, monkeypatch
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    key = "immutable/versioned.bin"
    store._write_bytes(key, b"first")
    destination = tmp_path / "acceptance" / "baseline.json"
    original_walk = raw_audit_module._walk_raw_files
    walk_count = 0

    def mutate_after_first_walk(raw_store):
        nonlocal walk_count
        walk_count += 1
        yield from original_walk(raw_store)
        if walk_count == 1:
            raw_store._write_bytes(key, b"other")

    monkeypatch.setattr(
        raw_audit_module, "_walk_raw_files", mutate_after_first_walk
    )

    with pytest.raises(RawAuditError, match="changed while baseline"):
        capture_and_write_raw_inventory(store, destination)

    assert not destination.exists()
    assert not destination.with_name(f"{destination.name}.sqlite3").exists()


def test_audit_rehashes_same_size_object_when_backend_has_no_mtime(
    tmp_path, monkeypatch
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    key = "immutable/no-version.bin"
    store._write_bytes(key, b"first")

    def zero_mtime_inventory(_store):
        return iter(
            [
                SimpleNamespace(
                    path=store._path(key),
                    size=5,
                    mtime_ns=0,
                    version_token="",
                )
            ]
        )

    monkeypatch.setattr(
        raw_audit_module, "_walk_raw_files", zero_mtime_inventory
    )
    baseline = capture_raw_inventory(store)
    store._write_bytes(key, b"other")
    original = raw_audit_module._hash_raw_object
    hash_spy = MagicMock(side_effect=original)
    monkeypatch.setattr(raw_audit_module, "_hash_raw_object", hash_spy)

    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_nonempty=False,
    )

    assert result["status"] == "failed"
    assert result["baseline_delta"]["content_changed_object_count"] == 1
    assert hash_spy.call_count == 2


def test_audit_detects_unversioned_same_size_change_during_final_guard(
    tmp_path, monkeypatch
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    key = "immutable/no-version.bin"
    store._write_bytes(key, b"first")

    def zero_mtime_inventory(_store):
        return iter(
            [
                SimpleNamespace(
                    path=store._path(key),
                    size=5,
                    mtime_ns=0,
                    version_token="",
                )
            ]
        )

    monkeypatch.setattr(
        raw_audit_module, "_walk_raw_files", zero_mtime_inventory
    )
    baseline = capture_raw_inventory(store)

    def mutate_during_audit(_store, _row):
        store._write_bytes(key, b"other")
        return {"content_blob_key": None, "response_blob_key": None}

    monkeypatch.setattr(raw_audit_module, "_audit_one", mutate_during_audit)
    result = audit_raw_fetches(
        store,
        [{"logical_refresh_id": REFRESH_ID}],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
    )

    assert result["status"] == "failed"
    failure = next(
        item
        for item in result["failures"]
        if item["error_class"] == "RawInventoryChanged"
    )
    assert failure["unstable_unversioned_key"] == key


def test_raw_audit_fails_closed_before_materializing_unbounded_attempts(
    tmp_path,
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    attempts = (
        {"logical_refresh_id": REFRESH_ID}
        for _ in range(RAW_AUDIT_MAX_ATTEMPTS + 1)
    )

    with pytest.raises(RawAuditError, match="attempt limit exceeded"):
        audit_raw_fetches(
            store,
            attempts,
            control_run_id=RUN_ID,
            baseline_inventory=capture_raw_inventory(store),
        )


def test_large_delta_keeps_exact_counts_but_caps_artifact_evidence(tmp_path):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    baseline = capture_raw_inventory(store)
    created_count = RAW_INVENTORY_EVIDENCE_LIMIT + 37
    for number in range(created_count):
        store._write_bytes(f"orphans/{number:06d}.bin", b"x")

    result = audit_raw_fetches(
        store,
        [],
        control_run_id=RUN_ID,
        baseline_inventory=baseline,
        require_nonempty=False,
    )

    delta = result["baseline_delta"]
    assert result["status"] == "failed"
    assert delta["created_object_count"] == created_count
    assert delta["unlinked_created_object_count"] == created_count
    assert len(delta["created_objects"]) == RAW_INVENTORY_EVIDENCE_LIMIT
    assert len(delta["unlinked_created_objects"]) == (
        RAW_INVENTORY_EVIDENCE_LIMIT
    )
    assert delta["evidence_truncated"] is True


def test_concurrent_identical_baseline_writers_converge_without_replace(
    tmp_path,
):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    inventory = capture_raw_inventory(store)
    destination = tmp_path / "acceptance" / "baseline.json"
    workers = 8
    barrier = Barrier(workers)

    def write_same():
        barrier.wait()
        return write_inventory_baseline(inventory, destination)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        paths = list(executor.map(lambda _: write_same(), range(workers)))

    assert paths == [destination] * workers
    loaded, digest = load_inventory_baseline(destination)
    assert loaded == inventory
    assert len(digest) == 64
    assert not list(destination.parent.glob(".*.tmp-*"))


def test_concurrent_conflicting_baseline_writer_cannot_overwrite(tmp_path):
    store = RawPageStore.from_uri((tmp_path / "raw").as_uri())
    first = capture_raw_inventory(store)
    second = deepcopy(first)
    second["captured_at"] = "2099-01-01T00:00:00+00:00"
    destination = tmp_path / "acceptance" / "baseline.json"
    barrier = Barrier(2)

    def write_one(inventory):
        barrier.wait()
        return write_inventory_baseline(inventory, destination)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(write_one, inventory)
            for inventory in (first, second)
        ]
        outcomes = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except RawAuditError as exc:
                outcomes.append(exc)

    assert sum(isinstance(item, RawAuditError) for item in outcomes) == 1
    installed, _digest = load_inventory_baseline(destination)
    assert installed in (first, second)
