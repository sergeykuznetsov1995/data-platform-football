from __future__ import annotations

import gzip
import hashlib
import json
import multiprocessing
from pathlib import Path
from queue import Empty
from unittest.mock import patch

import pytest
from pyarrow import fs

from scrapers.transfermarkt.raw_store import (
    ATTEMPT_ENVELOPE_VERSION,
    RAW_MANIFEST_VERSION,
    RawCaptureConflict,
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


def _transport_envelope(store, **overrides):
    values = {
        "url": URL,
        "fetched_at": FETCHED_AT,
        "cycle_id": "tm-child-abc",
        "scope_id": "GB1:2025",
        "endpoint": "competition_page",
        "attempt": 2,
        "error_kind": "tls",
        "error_type": "SSLCertVerificationError",
    }
    values.update(overrides)
    return store.store_transport_error(**values)


def _concurrent_capture_worker(uri, start, result):
    try:
        store = RawResponseStore.from_uri(uri)
        start.wait(timeout=10)
        record = _capture(store)
        result.put(("ok", record.capture_id, record.raw_uri))
    except Exception as exc:  # pragma: no cover - returned to parent assertion
        result.put(("error", type(exc).__name__, str(exc)))


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
    assert record.raw_uri.endswith(f"/{record.blob_key}")


def test_response_attempt_envelope_references_v1_capture_and_replays(tmp_path):
    store = _store(tmp_path)
    body = b"<html>response-envelope</html>"
    capture = _capture(store, body)
    capture_manifest = store._read_bytes(
        store.capture_manifest_key(capture.capture_id)
    )

    first = store.store_response_envelope(capture)
    second = store.store_response_envelope(capture)
    loaded = store.load_attempt_envelope(first.envelope_id)

    assert first == second == loaded
    assert first.manifest_version == ATTEMPT_ENVELOPE_VERSION
    assert first.record_hash == first.envelope_id
    assert first.outcome_kind == "response"
    assert first.capture_id == capture.capture_id
    assert first.raw_body_hash == capture.content_hash
    assert first.status_code == capture.status_code
    assert first.capture_manifest_uri.endswith(
        store.capture_manifest_key(capture.capture_id)
    )
    assert store.verify_attempt_envelope(first.envelope_id) == first
    assert store.load_attempt(first.envelope_id) == first
    assert store.verify_attempt(first.envelope_id) == first
    assert store.replay_attempt(first.envelope_id) == body
    # The extension is additive: neither the v1 capture manifest nor its body
    # key is rewritten to carry the new envelope contract.
    assert store._read_bytes(
        store.capture_manifest_key(capture.capture_id)
    ) == capture_manifest
    assert len(list((tmp_path / "raw" / "captures").rglob("*.json"))) == 1
    assert len(list((tmp_path / "raw" / "attempts").rglob("*.json"))) == 1


def test_attempt_id_is_independent_of_raw_store_location(tmp_path):
    first_store = RawResponseStore.from_uri((tmp_path / "one").as_uri())
    second_store = RawResponseStore.from_uri((tmp_path / "two").as_uri())

    first = first_store.store_response_envelope(_capture(first_store))
    second = second_store.store_response_envelope(_capture(second_store))

    assert first.envelope_id == second.envelope_id
    assert first.capture_manifest_uri != second.capture_manifest_uri
    assert first.envelope_uri != second.envelope_uri


def test_transport_attempt_envelope_has_no_body_or_exception_text(tmp_path):
    store = _store(tmp_path)

    record = _transport_envelope(store)
    loaded = store.load_attempt_envelope(record.envelope_id)
    manifest = store._read_bytes(store.attempt_manifest_key(record.envelope_id))

    assert loaded == record
    assert record.outcome_kind == "transport_error"
    assert record.capture_id is None
    assert record.capture_manifest_uri is None
    assert record.raw_body_hash is None
    assert record.status_code is None
    assert record.error_kind == "tls"
    assert record.error_type == "SSLCertVerificationError"
    assert store.replay_attempt(record.envelope_id) is None
    assert not (tmp_path / "raw" / "blobs").exists()
    assert not (tmp_path / "raw" / "captures").exists()
    assert b"error_message" not in manifest
    assert b"password" not in manifest.lower()


def test_transport_envelope_rejects_unsafe_error_metadata(tmp_path):
    store = _store(tmp_path)

    with pytest.raises(ValueError, match="safe exception class"):
        _transport_envelope(
            store,
            error_type="TLS failure token=secret password=hunter2",
        )
    with pytest.raises(ValueError, match="allowlisted"):
        _transport_envelope(store, error_kind="arbitrary-secret-kind")


def test_attempt_envelope_hash_detects_tampering(tmp_path):
    store = _store(tmp_path)
    record = _transport_envelope(store)
    key = store.attempt_manifest_key(record.envelope_id)
    manifest = json.loads(store._read_bytes(key))
    manifest["error_kind"] = "connection"
    store._write_bytes(key, json.dumps(manifest).encode("utf-8"))

    with pytest.raises(RawCaptureCorrupt, match="identity mismatch"):
        store.load_attempt_envelope(record.envelope_id)
    with pytest.raises(RawCaptureCorrupt, match="identity mismatch"):
        store.replay_attempt(record.envelope_id)


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


def test_local_raw_uri_is_public_and_resolvable(tmp_path):
    store = _store(tmp_path)
    record = _capture(store)

    assert record.raw_uri == (
        tmp_path / "raw" / Path(record.blob_key)
    ).as_uri()
    filesystem, path = fs.FileSystem.from_uri(record.raw_uri)
    with filesystem.open_input_file(path) as stream:
        assert gzip.decompress(stream.read()) == b"<html>exact</html>\n"


def test_concurrent_processes_publish_one_immutable_capture(tmp_path):
    uri = (tmp_path / "raw").as_uri()
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    result = context.Queue()
    workers = [
        context.Process(
            target=_concurrent_capture_worker,
            args=(uri, start, result),
        )
        for _ in range(4)
    ]
    for worker in workers:
        worker.start()
    start.set()
    for worker in workers:
        worker.join(timeout=15)
        assert worker.exitcode == 0

    try:
        outcomes = [result.get(timeout=2) for _ in workers]
    except Empty as exc:  # pragma: no cover - process failure assertion aid
        raise AssertionError("capture worker returned no result") from exc
    assert {outcome[0] for outcome in outcomes} == {"ok"}
    assert len({outcome[1] for outcome in outcomes}) == 1
    assert len(list((tmp_path / "raw" / "blobs").rglob("*.body.gz"))) == 1
    assert len(list((tmp_path / "raw" / "captures").rglob("*.json"))) == 1
    capture_id = outcomes[0][1]
    assert _store(tmp_path).replay(capture_id) == b"<html>exact</html>\n"


def test_existing_semantically_valid_gzip_need_not_match_recompression(tmp_path):
    store = _store(tmp_path)
    body = b"<html>valid alternate gzip</html>"
    content_hash = hashlib.sha256(body).hexdigest()
    key = store.blob_key(content_hash)
    alternate = gzip.compress(body, compresslevel=1, mtime=123)
    assert alternate != gzip.compress(body, compresslevel=6, mtime=0)
    store._write_bytes(key, alternate)

    record = _capture(store, body)

    assert record.stored_bytes == len(alternate)
    assert store.load_capture(record.capture_id)[0] == body


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


def test_capture_identity_binds_utc_timestamp_and_safe_headers(tmp_path):
    store = _store(tmp_path)
    first = _capture(
        store,
        fetched_at="2026-07-21T12:00:00+02:00",
        headers={"ETag": '"one"'},
    )
    equivalent = _capture(
        store,
        fetched_at="2026-07-21T10:00:00Z",
        headers={"etag": '"one"'},
    )
    other_header = _capture(
        store,
        fetched_at="2026-07-21T10:00:00Z",
        headers={"ETag": '"two"'},
    )
    other_time = _capture(
        store,
        fetched_at="2026-07-21T10:00:01Z",
        headers={"ETag": '"one"'},
    )

    assert first.capture_id == equivalent.capture_id
    assert first.fetched_at == "2026-07-21T10:00:00+00:00"
    assert other_header.capture_id != first.capture_id
    assert other_time.capture_id != first.capture_id


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("headers", None),
        ("attempt", "1"),
        ("status_code", 200.0),
        ("decoded_bytes", "19"),
        ("stored_bytes", True),
        ("fetched_at", "2026-07-21T10:00:00"),
        ("raw_uri", "s3://user:secret@bucket/raw/blob"),
    ],
)
def test_malformed_manifest_types_and_identity_fail_closed(
    tmp_path,
    field,
    value,
):
    store = _store(tmp_path)
    record = _capture(store)
    key = store.capture_manifest_key(record.capture_id)
    manifest = json.loads(store._read_bytes(key))
    manifest[field] = value
    store._write_bytes(key, json.dumps(manifest).encode("utf-8"))

    with pytest.raises(RawCaptureCorrupt):
        store.load_capture(record.capture_id)


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


def test_only_known_safe_transfermarkt_query_fields_are_allowed(tmp_path):
    store = _store(tmp_path)
    record = _capture(
        store,
        url=(
            "https://www.transfermarkt.com/wettbewerbe/europa"
            "?saison_id=2025&page=2&sort=name"
        ),
    )
    assert "saison_id=2025" in record.url

    with pytest.raises(ValueError, match="credential-free"):
        _capture(store, url="https://www.transfermarkt.com/page?unknown=value")


@pytest.mark.parametrize(
    "url",
    [
        "https://tmapi.transfermarkt.technology/competition/CL/regulation",
        "https://tmapi.transfermarkt.technology/competition/CL/club?season=2025",
        "https://tmapi.transfermarkt.technology/clubs?ids%5B%5D=281&ids%5B%5D=418",
    ],
)
def test_first_party_discovery_api_routes_are_raw_storable(tmp_path, url):
    record = _capture(
        _store(tmp_path),
        body=b'{"success":true,"data":{}}',
        url=url,
        headers={"Content-Type": "application/json"},
    )

    assert record.url == url
    assert record.content_type == "application/json"


@pytest.mark.parametrize(
    "url",
    [
        "https://tmapi.transfermarkt.technology/competition/CL/regulation?season=2025",
        "https://tmapi.transfermarkt.technology/competition/CL/club",
        "https://tmapi.transfermarkt.technology/competition/CL/club?season=all",
        "https://tmapi.transfermarkt.technology/clubs?ids%5B%5D=281&token=secret",
        "https://tmapi.transfermarkt.technology/players?ids%5B%5D=1",
    ],
)
def test_unknown_discovery_api_routes_and_queries_are_rejected(tmp_path, url):
    with pytest.raises(ValueError, match="credential-free"):
        _capture(_store(tmp_path), url=url)


def test_naive_fetched_at_and_non_integral_metadata_are_rejected(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError, match="timezone"):
        _capture(store, fetched_at="2026-07-21T10:00:00")
    with pytest.raises(ValueError, match="integer"):
        _capture(store, attempt="1")


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


def test_namespaced_s3_credentials_take_precedence(monkeypatch):
    monkeypatch.setenv("TRANSFERMARKT_RAW_S3_ACCESS_KEY", "raw-access")
    monkeypatch.setenv("TRANSFERMARKT_RAW_S3_SECRET_KEY", "raw-secret")
    monkeypatch.setenv("S3_ACCESS_KEY", "platform-access")
    monkeypatch.setenv("S3_SECRET_KEY", "platform-secret")

    with patch(
        "scrapers.transfermarkt.raw_store.fs.S3FileSystem",
        return_value=object(),
    ) as constructor:
        store = RawResponseStore.from_uri("s3://raw-bucket/transfermarkt")

    assert store.uri_prefix == "s3://raw-bucket/transfermarkt"
    assert constructor.call_args.kwargs["access_key"] == "raw-access"
    assert constructor.call_args.kwargs["secret_key"] == "raw-secret"


@pytest.mark.parametrize(
    "present",
    [
        "TRANSFERMARKT_RAW_S3_ACCESS_KEY",
        "TRANSFERMARKT_RAW_S3_SECRET_KEY",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
    ],
)
def test_partial_s3_credential_pairs_fail_closed(monkeypatch, present):
    for name in (
        "TRANSFERMARKT_RAW_S3_ACCESS_KEY",
        "TRANSFERMARKT_RAW_S3_SECRET_KEY",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(present, "configured")

    with pytest.raises(RawStoreError, match="must be set together"):
        RawResponseStore.from_uri("s3://raw-bucket/transfermarkt")


def test_s3_raw_uri_contains_no_credentials(tmp_path):
    store = RawResponseStore(
        fs.LocalFileSystem(),
        str(tmp_path / "raw"),
        uri_prefix="s3://raw-bucket/transfermarkt",
    )
    record = _capture(store)

    assert record.raw_uri == f"s3://raw-bucket/transfermarkt/{record.blob_key}"
    assert "@" not in record.raw_uri


def test_object_store_manifest_publication_verifies_winning_bytes(tmp_path):
    class ObjectFilesystem:
        def create_dir(self, _path, recursive=True):
            assert recursive

    store = RawResponseStore(
        ObjectFilesystem(),
        "bucket/raw",
        uri_prefix="s3://bucket/raw",
    )
    objects = {}
    store._exists = objects.__contains__
    store._read_bytes = objects.__getitem__

    def racing_write(relative, _payload):
        objects[relative] = b"different immutable manifest"

    store._write_bytes = racing_write
    with pytest.raises(RawCaptureConflict, match="conflict"):
        store._publish_immutable_bytes(
            "captures/sha256/aa/" + "a" * 64 + ".json",
            b"expected manifest",
            require_exact_bytes=True,
        )
