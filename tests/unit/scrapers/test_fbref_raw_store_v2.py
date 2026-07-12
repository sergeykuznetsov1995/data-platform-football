import gzip
import hashlib
import threading

import pytest

from scrapers.fbref.raw_store import (
    RAW_MANIFEST_VERSION_V2,
    RAW_V1_BRIDGE_VERSION,
    RawPageCorrupt,
    RawPageStore,
    RawStoreError,
    match_page_target,
)


def _store(tmp_path) -> RawPageStore:
    return RawPageStore.from_uri(tmp_path.as_uri())


def test_v2_commit_preserves_exact_response_bytes_and_metrics(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    body = b"\x00<html>\r\nFBref \xe2\x98\x83</html>\xff"

    record = store.commit_fetch(
        target,
        body,
        logical_refresh_id="refresh-1",
        attempt_id="attempt-1",
        http_status=200,
        fetched_at="2026-07-11T10:00:00+00:00",
        headers={
            "ETag": '"abc"',
            "Last-Modified": "yesterday",
            "Content-Type": "text/html",
            "Set-Cookie": "session=secret",
            "Authorization": "Bearer secret",
            "X-Api-Key": "secret",
        },
        wire_bytes=123,
        provider_billed_bytes=456,
        latency_ms=78,
        http_requests=2,
        http_status_history=(500, 200),
        browser_bootstrap_attempts=2,
        browser_unobserved_bytes=789,
        transport_version="curl-cffi-v1",
        session_version="clearance-v2",
    )

    response, loaded = store.load_response("refresh-1")
    effective, effective_record = store.load_fetch("refresh-1")
    assert response == effective == body
    assert loaded == effective_record == record
    assert record.manifest_version == RAW_MANIFEST_VERSION_V2
    assert record.response_hash == record.content_hash == hashlib.sha256(body).hexdigest()
    assert record.response_bytes == record.decoded_bytes == len(body)
    assert record.wire_bytes == 123
    assert record.provider_billed_bytes == 456
    assert record.http_requests == 2
    assert record.http_status_history == (500, 200)
    assert record.browser_bootstrap_attempts == 2
    assert record.browser_unobserved_bytes == 789
    assert record.etag == '"abc"'
    assert record.last_modified == "yesterday"
    assert record.headers == {
        "content-type": "text/html",
        "etag": '"abc"',
        "last-modified": "yesterday",
    }
    assert gzip.decompress(store._read_bytes(record.blob_key)) == body
    assert store.has_fetch("refresh-1")


def test_v2_old_manifest_infers_one_final_http_status(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    record = store.commit_fetch(
        target,
        b"<html>old manifest</html>",
        logical_refresh_id="refresh-old",
        http_status=200,
    )
    key = store.fetch_manifest_key(record.logical_refresh_id)
    payload = store.read_manifest(key)
    payload.pop("http_requests")
    payload.pop("http_status_history")
    store._write_json(key, payload)

    _, loaded = store.load_fetch(record.logical_refresh_id)

    assert loaded.http_requests == 1
    assert loaded.http_status_history == (200,)
    assert loaded.browser_bootstrap_attempts == 0
    assert loaded.browser_unobserved_bytes == 0


def test_v2_logical_refresh_is_append_only_and_retry_safe(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    body = b"<html>one</html>"
    first = store.commit_fetch(
        target,
        body,
        logical_refresh_id="refresh-1",
        http_status=200,
        fetched_at="2026-07-11T10:00:00+00:00",
    )
    retry = store.commit_fetch(
        target,
        body,
        logical_refresh_id="refresh-1",
        http_status=200,
        fetched_at="2026-07-11T11:00:00+00:00",
    )

    assert retry == first
    assert retry.fetched_at == "2026-07-11T10:00:00+00:00"
    with pytest.raises(RawStoreError, match="immutable"):
        store.commit_fetch(
            target,
            b"<html>different</html>",
            logical_refresh_id="refresh-1",
            http_status=200,
        )


def test_v2_304_keeps_prior_effective_html_and_exact_empty_response(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    html = b"<html>cached</html>"
    first = store.commit_fetch(
        target,
        html,
        logical_refresh_id="refresh-1",
        http_status=200,
        headers={"etag": '"v1"'},
    )
    not_modified = store.commit_fetch(
        target,
        b"",
        logical_refresh_id="refresh-2",
        http_status=304,
        headers={"etag": '"v1"'},
        wire_bytes=99,
    )

    exact_response, _ = store.load_response("refresh-2")
    effective, loaded = store.load_fetch("refresh-2")
    assert not_modified == loaded
    assert exact_response == b""
    assert effective == html
    assert loaded.not_modified is True
    assert loaded.content_changed is False
    assert loaded.content_hash == first.content_hash
    assert loaded.response_hash == hashlib.sha256(b"").hexdigest()
    assert loaded.blob_key == first.blob_key
    assert loaded.response_blob_key != first.blob_key


def test_v2_304_without_prior_content_fails_closed(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(RawStoreError, match="previously committed"):
        store.commit_fetch(
            match_page_target("a071faa8"),
            b"",
            logical_refresh_id="refresh-1",
            http_status=304,
        )
    assert not store.has_fetch("refresh-1")


def test_v2_304_uses_explicit_control_committed_content_hash(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    control_committed = store.commit_fetch(
        target,
        b"<html>control A</html>",
        logical_refresh_id="refresh-A",
        http_status=200,
    )
    store.commit_fetch(
        target,
        b"<html>raw-only B</html>",
        logical_refresh_id="refresh-B",
        http_status=200,
    )

    not_modified = store.commit_fetch(
        target,
        b"",
        logical_refresh_id="refresh-304",
        http_status=304,
        base_content_hash=control_committed.content_hash,
    )

    effective, _ = store.load_fetch(not_modified.logical_refresh_id)
    assert effective == b"<html>control A</html>"
    assert not_modified.content_hash == control_committed.content_hash
    assert not_modified.previous_content_hash == control_committed.content_hash


def test_retrying_older_refresh_does_not_regress_latest_pointer(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    first_body = b"<html>first</html>"
    second_body = b"<html>second</html>"
    store.commit_fetch(
        target,
        first_body,
        logical_refresh_id="refresh-1",
        http_status=200,
    )
    newest = store.commit_fetch(
        target,
        second_body,
        logical_refresh_id="refresh-2",
        http_status=200,
    )
    store.commit_fetch(
        target,
        first_body,
        logical_refresh_id="refresh-1",
        http_status=200,
    )

    latest_body, latest = store.load_latest_response(target)
    assert latest_body == second_body
    assert latest.logical_refresh_id == newest.logical_refresh_id


def test_delayed_first_commit_cannot_regress_latest_or_304_base(
    tmp_path,
    monkeypatch,
):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    first_body = b"<html>A</html>"
    second_body = b"<html>B</html>"
    first_blocked = threading.Event()
    release_first = threading.Event()
    errors = []
    original_store_blob = store._store_verified_blob

    def delayed_store_blob(raw):
        if raw == first_body:
            first_blocked.set()
            assert release_first.wait(timeout=5)
        return original_store_blob(raw)

    monkeypatch.setattr(store, "_store_verified_blob", delayed_store_blob)

    def commit_first():
        try:
            store.commit_fetch(
                target,
                first_body,
                logical_refresh_id="refresh-A",
                http_status=200,
            )
        except Exception as exc:  # noqa: BLE001 - asserted in parent thread
            errors.append(exc)

    thread = threading.Thread(target=commit_first)
    thread.start()
    assert first_blocked.wait(timeout=5)
    second = store.commit_fetch(
        target,
        second_body,
        logical_refresh_id="refresh-B",
        http_status=200,
    )
    release_first.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert errors == []
    latest_body, latest = store.load_latest_response(target)
    assert latest_body == second_body
    assert latest.logical_refresh_id == second.logical_refresh_id
    mirror = store.read_manifest(store._v2_target_manifest_key(target))
    assert mirror["logical_refresh_id"] == second.logical_refresh_id

    not_modified = store.commit_fetch(
        target,
        b"",
        logical_refresh_id="refresh-304",
        http_status=304,
    )
    effective, _ = store.load_fetch(not_modified.logical_refresh_id)
    assert effective == second_body
    assert not_modified.previous_content_hash == second.content_hash


def test_v2_read_detects_corrupt_blob_and_unknown_billing_stays_null(tmp_path):
    store = _store(tmp_path)
    record = store.commit_fetch(
        match_page_target("a071faa8"),
        b"<html>test</html>",
        logical_refresh_id="refresh-1",
        http_status=200,
    )
    assert record.provider_billed_bytes is None
    store._write_bytes(record.response_blob_key, b"not-gzip")
    with pytest.raises(RawPageCorrupt, match="Encoded size mismatch|Invalid gzip"):
        store.load_response("refresh-1")


def test_v1_target_manifest_imports_to_bounded_v2_logical_fetch(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    html = "<html>legacy raw</html>"
    legacy = store.store_html(
        target,
        html,
        fetched_at="2026-07-01T10:00:00+00:00",
    )

    imported = store.import_fetch_from_available_raw(
        target,
        logical_refresh_id="refresh-import",
        attempt_id="attempt-import",
    )

    assert imported is not None
    body, loaded = store.load_fetch("refresh-import")
    assert body == html.encode("utf-8")
    assert loaded == imported
    assert imported.content_hash == legacy.content_hash
    assert imported.fetched_at == legacy.fetched_at
    assert imported.fetcher_version == RAW_V1_BRIDGE_VERSION
    assert imported.wire_bytes == imported.provider_billed_bytes == 0
    assert imported.http_requests == 0
    assert imported.http_status_history == ()
    assert imported.imported_from_manifest_key == store._target_manifest_key(
        target
    )


def test_v1_import_fails_closed_on_source_identity_mismatch(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    store.store_html(target, "<html>legacy raw</html>")
    key = store._target_manifest_key(target)
    payload = store.read_manifest(key)
    payload["source_ids"] = {"match_id": "deadbeef"}
    store._write_json(key, payload)

    with pytest.raises(RawPageCorrupt, match="identity mismatch"):
        store.import_fetch_from_available_raw(
            target,
            logical_refresh_id="refresh-import",
        )
    assert not store.has_fetch("refresh-import")
