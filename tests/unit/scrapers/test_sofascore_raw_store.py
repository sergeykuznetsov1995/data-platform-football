from __future__ import annotations

import gzip
import json
from unittest.mock import patch

import pytest
from pyarrow import fs

from scrapers.sofascore.raw_store import (
    PayloadTarget,
    RawPayloadCorrupt,
    RawPayloadSchemaError,
    RawPayloadStore,
)


def _store(tmp_path):
    return RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))


def _target(**overrides):
    values = {
        "source_tournament_id": "17",
        "source_season_id": "76986",
        "target_type": "event",
        "target_id": "14025003",
        "endpoint": "lineups",
        "freshness_key": "finished-v1",
    }
    values.update(overrides)
    return PayloadTarget(**values)


def test_exact_bytes_round_trip_is_content_addressed_and_deterministic(tmp_path):
    store = _store(tmp_path)
    body = b'{ "players": [1, 2], "unicode": "\\u00e9" }\n'
    first = store.store_bytes(
        _target(),
        body,
        request_url="https://www.sofascore.com/api/v1/event/14025003/lineups",
        http_status=200,
        response_headers={"Content-Type": "application/json", "ETag": "abc"},
        fetched_at="2026-07-11T00:00:00+00:00",
    )
    second = store.store_bytes(
        _target(),
        body,
        request_url="https://www.sofascore.com/api/v1/event/14025003/lineups",
        http_status=200,
        fetched_at="2026-07-11T00:00:00+00:00",
    )

    loaded, record = store.load_bytes(_target())
    assert loaded == body
    assert first.content_hash == second.content_hash == record.content_hash
    assert first.blob_key == second.blob_key
    assert gzip.decompress(store._read_bytes(record.blob_key)) == body
    assert record.content_type is None  # latest pointer is the second response


def test_path_components_are_encoded_and_cannot_escape(tmp_path):
    store = _store(tmp_path)
    key = store.target_pointer_key(
        _target(source_season_id="../../etc", endpoint="stats/overall")
    )
    assert "../" not in key
    assert "%2F" in key
    assert key.startswith("targets/17/")


def test_blob_hash_and_length_are_validated(tmp_path):
    store = _store(tmp_path)
    record = store.store_bytes(
        _target(),
        b'{"home": []}',
        request_url="https://example.invalid/api",
        http_status=200,
    )
    store._write_bytes(record.blob_key, gzip.compress(b'{"tampered": true}'))
    with pytest.raises(RawPayloadCorrupt, match="Content mismatch"):
        store.load_bytes(_target())


def test_invalid_json_remains_replayable_as_exact_bytes_but_json_load_fails(tmp_path):
    store = _store(tmp_path)
    store.store_bytes(
        _target(),
        b"<html>challenge</html>",
        request_url="https://example.invalid/api",
        http_status=200,
    )
    body, _ = store.load_bytes(_target())
    assert body == b"<html>challenge</html>"
    with pytest.raises(RawPayloadSchemaError):
        store.load_json(_target())


def test_pointer_commit_is_atomic_and_last_good_survives_failure(tmp_path):
    store = _store(tmp_path)
    target = _target()
    store.store_bytes(
        target,
        b'{"version": 1}',
        request_url="https://example.invalid/api",
        http_status=200,
    )
    original_writer = store._write_json_atomic

    def fail_pointer(*args, **kwargs):
        raise OSError("simulated pointer commit failure")

    with patch.object(store, "_write_json_atomic", side_effect=fail_pointer):
        with pytest.raises(OSError):
            store.store_bytes(
                target,
                b'{"version": 2}',
                request_url="https://example.invalid/api",
                http_status=200,
            )
    body, _ = store.load_bytes(target)
    assert json.loads(body) == {"version": 1}
    assert callable(original_writer)
