from __future__ import annotations

import gzip
import hashlib
import json
from unittest.mock import patch

import pytest

from scrapers.transfermarkt.raw_store import (
    RAW_MANIFEST_VERSION,
    RawCaptureCorrupt,
    RawCaptureNotFound,
    RawResponseStore,
    RawStoreError,
)


URL = "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1"
FETCHED_AT = "2026-07-21T10:00:00+00:00"


def _store(tmp_path):
    return RawResponseStore.from_uri((tmp_path / "raw").as_uri())


def _capture(store, body=b"<html>exact</html>\n", **overrides):
    values = {
        "url": URL,
        "body": body,
        "status_code": 200,
        "headers": {"Content-Type": "text/html; charset=utf-8"},
        "fetched_at": FETCHED_AT,
        "cycle_id": "tm-child-abc",
        "scope_id": "GB1:2025",
        "endpoint": "competition_page",
        "attempt": 1,
    }
    values.update(overrides)
    return store.store_attempt(**values)


def test_exact_bytes_round_trip_and_idempotent_attempt(tmp_path):
    store = _store(tmp_path)
    body = b'{ "kind": "json", "unicode": "\\u00e9" }\n'

    first = _capture(
        store,
        body,
        headers={"Content-Type": "application/json", "ETag": '"v1"'},
    )
    second = _capture(
        store,
        body,
        headers={"ETag": '"v1"', "Content-Type": "application/json"},
    )
    loaded, record = store.load_capture(first.capture_id)

    assert loaded == body
    assert first == second == record
    assert record.manifest_version == RAW_MANIFEST_VERSION
    assert record.content_hash == hashlib.sha256(body).hexdigest()
    assert record.blob_key == (
        f"blobs/sha256/{record.content_hash[:2]}/"
        f"{record.content_hash}.body.gz"
    )


def test_gzip_bytes_are_deterministic_with_zero_mtime(tmp_path):
    store = _store(tmp_path)
    body = b"<html>byte stable</html>"

    record = _capture(store, body)
    stored = store._read_bytes(record.blob_key)

    assert stored == gzip.compress(body, compresslevel=6, mtime=0)
    assert stored[4:8] == b"\x00\x00\x00\x00"
    assert record.stored_bytes == len(stored)


def test_same_body_deduplicates_blob_across_distinct_captures(tmp_path):
    store = _store(tmp_path)
    body = b"<html>shared</html>"

    first = _capture(store, body, endpoint="squad", attempt=0)
    second = _capture(store, body, endpoint="transfers", attempt=2)

    assert first.capture_id != second.capture_id
    assert first.blob_key == second.blob_key
    blobs = list((tmp_path / "raw" / "blobs").rglob("*.body.gz"))
    manifests = list((tmp_path / "raw" / "captures").rglob("*.json"))
    assert len(blobs) == 1
    assert len(manifests) == 2


def test_existing_corrupt_blob_fails_closed_and_is_not_repaired(tmp_path):
    store = _store(tmp_path)
    record = _capture(store)
    original_manifest = store._read_bytes(
        store.capture_manifest_key(record.capture_id)
    )
    store._write_bytes(record.blob_key, b"not gzip")

    with pytest.raises(RawCaptureCorrupt):
        store.load_capture(record.capture_id)
    with pytest.raises(RawCaptureCorrupt):
        _capture(store)
    assert store._read_bytes(record.blob_key) == b"not gzip"
    assert (
        store._read_bytes(store.capture_manifest_key(record.capture_id))
        == original_manifest
    )


def test_manifest_hash_and_lengths_are_verified_on_load_and_replay(tmp_path):
    store = _store(tmp_path)
    record = _capture(store)
    key = store.capture_manifest_key(record.capture_id)
    manifest = json.loads(store._read_bytes(key))
    manifest["stored_bytes"] += 1
    store._write_bytes(key, json.dumps(manifest).encode("utf-8"))

    with pytest.raises(RawCaptureCorrupt, match="Stored length mismatch"):
        store.load_capture(record.capture_id)
    with pytest.raises(RawCaptureCorrupt, match="Stored length mismatch"):
        store.replay(record.capture_id)


def test_missing_capture_and_non_bytes_fail(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(RawCaptureNotFound):
        store.load_capture("0" * 64)
    with pytest.raises(TypeError, match="must be bytes"):
        _capture(store, body="<html />")


def test_only_safe_response_headers_are_persisted(tmp_path):
    store = _store(tmp_path)
    record = _capture(
        store,
        headers={
            "Content-Type": "text/html",
            "ETag": '"safe"',
            "Authorization": "Bearer secret",
            "Cookie": "session=secret",
            "Set-Cookie": "session=secret",
            "Proxy-Authorization": "Basic secret",
            "X-Api-Key": "secret",
        },
    )

    assert record.headers == {"content-type": "text/html", "etag": '"safe"'}
    manifest = store._read_bytes(store.capture_manifest_key(record.capture_id))
    assert b"secret" not in manifest
    assert b"cookie" not in manifest.lower()


@pytest.mark.parametrize(
    "uri",
    [
        "",
        " s3://bucket/raw ",
        "s3://access:secret@bucket/raw",
        "s3://bucket/raw?X-Amz-Credential=secret",
        "s3://bucket/raw#secret",
    ],
)
def test_invalid_or_credential_bearing_store_uri_is_rejected(uri):
    with pytest.raises(ValueError, match="credential-free"):
        RawResponseStore.from_uri(uri)


def test_credential_bearing_response_url_is_rejected(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError, match="credential-free") as error:
        _capture(store, url="https://user:password@www.transfermarkt.com/page")
    assert "password" not in str(error.value)

    with pytest.raises(ValueError, match="credential-free") as error:
        _capture(store, url="https://www.transfermarkt.com/page?token=secret")
    assert "secret" not in str(error.value)


def test_from_env_is_mandatory_by_default_and_optional_when_requested(
    monkeypatch,
):
    monkeypatch.delenv("TRANSFERMARKT_RAW_STORE_URI", raising=False)
    with pytest.raises(RawStoreError, match="TRANSFERMARKT_RAW_STORE_URI"):
        RawResponseStore.from_env()
    assert RawResponseStore.from_env(optional=True) is None


def test_s3_uses_transfermarkt_environment_conventions(monkeypatch):
    monkeypatch.setenv("S3_ACCESS_KEY", "access")
    monkeypatch.setenv("S3_SECRET_KEY", "secret")
    monkeypatch.setenv("TRANSFERMARKT_RAW_S3_ENDPOINT", "objects:9000")
    monkeypatch.setenv("TRANSFERMARKT_RAW_S3_SCHEME", "https")
    monkeypatch.setenv("TRANSFERMARKT_RAW_S3_REGION", "eu-central-1")
    sentinel = object()

    with patch(
        "scrapers.transfermarkt.raw_store.fs.S3FileSystem",
        return_value=sentinel,
    ) as constructor:
        store = RawResponseStore.from_uri("s3://raw-bucket/transfermarkt")

    assert store.filesystem is sentinel
    assert store.root == "raw-bucket/transfermarkt"
    constructor.assert_called_once_with(
        access_key="access",
        secret_key="secret",
        endpoint_override="objects:9000",
        scheme="https",
        region="eu-central-1",
        background_writes=False,
    )
