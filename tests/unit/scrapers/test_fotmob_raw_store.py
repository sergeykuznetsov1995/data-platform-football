import gzip
import json

import pytest

from scrapers.fotmob.raw_store import (
    RAW_MANIFEST_VERSION,
    FotMobRawStore,
    RawStoreError,
    RawTargetCorrupt,
    RawTargetNotFound,
)
from scrapers.fotmob.transport import canonicalize_target


def _store(tmp_path):
    return FotMobRawStore.from_uri(tmp_path.as_uri())


def test_round_trip_is_content_addressed_and_deterministic(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("leagues", {"season": "2025/2026", "id": 47})
    body = b'{"name":"Premier League","id":47}'

    first = store.store(
        target,
        body,
        fetched_at="2026-07-11T10:00:00+00:00",
        etag='"league-v1"',
        last_modified="Fri, 10 Jul 2026 12:00:00 GMT",
        source_encoded_bytes=21,
    )
    second = store.store(
        target,
        body,
        fetched_at="2026-07-11T10:00:00+00:00",
        etag='"league-v1"',
        source_encoded_bytes=21,
    )

    loaded, record = store.load(target)
    assert loaded == body
    assert record.manifest_version == RAW_MANIFEST_VERSION
    assert first.content_hash == second.content_hash == record.content_hash
    assert first.blob_key == second.blob_key
    assert record.raw_uri.endswith(f"/{record.blob_key}")
    assert record.etag == '"league-v1"'
    assert record.decoded_bytes == len(body)
    assert record.validated_at == record.fetched_at
    assert gzip.decompress(store._read_bytes(record.blob_key)) == body


def test_equivalent_query_order_uses_one_target_manifest(tmp_path):
    store = _store(tmp_path)
    first = canonicalize_target(
        "https://www.fotmob.com/api/data/leagues?season=2025%2F2026&id=47"
    )
    second = canonicalize_target("leagues", {"id": 47, "season": "2025/2026"})
    assert first == second

    store.store(first, b'{"ok":true}')
    assert store.has_target(second)
    assert store.load(second)[0] == b'{"ok":true}'


def test_validated_selected_season_blob_can_be_durably_aliased(tmp_path):
    store = _store(tmp_path)
    selected = canonicalize_target("leagues", {"id": 47})
    exact = canonicalize_target(
        "leagues", {"id": 47, "season": "2025/2026"}
    )
    body = b'{"details":{"selectedSeason":"2025/2026"}}'
    source_record = store.store(selected, body, etag='"selected-v1"')

    alias_record = store.alias(selected, exact)

    assert store.load(exact)[0] == body
    assert alias_record.target_key == exact.target_key
    assert alias_record.canonical_url == exact.canonical_url
    assert alias_record.blob_key == source_record.blob_key
    assert alias_record.raw_uri == source_record.raw_uri
    assert store._exists(store._manifest_key(selected.target_key))
    assert store._exists(store._manifest_key(exact.target_key))


def test_missing_corrupt_and_mismatched_targets_fail_closed(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("allLeagues")
    with pytest.raises(RawTargetNotFound):
        store.load(target)

    record = store.store(target, b'{"leagues":[]}')
    store._write_bytes(record.blob_key, b"not-gzip")
    with pytest.raises(RawTargetCorrupt):
        store.load(target)

    # A subsequent successful fetch of the same content repairs a damaged
    # content-addressed object instead of republishing a broken manifest.
    store.store(target, b'{"leagues":[]}')
    assert store.load(target)[0] == b'{"leagues":[]}'

    # Alter the committed canonical URL.  A SHA key is
    # not accepted as sufficient evidence when its manifest identity differs.
    manifest_key = store._manifest_key(target.target_key)
    manifest = json.loads(store._read_bytes(manifest_key))
    manifest["canonical_url"] = "https://www.fotmob.com/api/data/leagues?id=47"
    store._write_bytes(manifest_key, json.dumps(manifest).encode())
    with pytest.raises(RawTargetCorrupt):
        store.load(target)


def test_store_rejects_invalid_target_keys_and_non_bytes(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("allLeagues")
    with pytest.raises(TypeError):
        store.store(target, "{}")

    class BadTarget:
        canonical_url = "https://www.fotmob.com/api/data/allLeagues"
        target_key = "../escape"

    with pytest.raises(ValueError):
        store.store(BadTarget(), b"{}")


def test_from_env_optional_and_required(monkeypatch):
    monkeypatch.delenv("FOTMOB_RAW_STORE_URI", raising=False)
    assert FotMobRawStore.from_env() is None
    with pytest.raises(RawStoreError):
        FotMobRawStore.from_env(optional=False)
