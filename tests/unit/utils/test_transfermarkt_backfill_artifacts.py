from __future__ import annotations

import hashlib
import json

from dags.utils.transfermarkt_backfill_artifacts import BackfillArtifactStore
from scrapers.transfermarkt.raw_store import RawResponseStore


OWNER = "a" * 64


def test_backfill_artifact_is_content_addressed_and_idempotent(tmp_path):
    raw = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    store = BackfillArtifactStore(raw)

    first = store.publish_json(
        {"campaign_id": OWNER, "passed": True},
        kind="batch_dq",
        owner_id=OWNER,
    )
    second = store.publish_json(
        {"passed": True, "campaign_id": OWNER},
        kind="batch_dq",
        owner_id=OWNER,
    )

    assert first == second
    assert first.uri.startswith((tmp_path / "raw").resolve().as_uri())
    key = first.uri.split("/raw/", 1)[1]
    payload = raw._read_bytes(key)
    assert hashlib.sha256(payload).hexdigest() == first.sha256
    assert json.loads(payload) == {"campaign_id": OWNER, "passed": True}
    assert store.load_json(
        first.uri,
        expected_sha256=first.sha256,
    ) == {"campaign_id": OWNER, "passed": True}


def test_exact_canonical_bytes_keep_their_report_hash(tmp_path):
    raw = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    store = BackfillArtifactStore(raw)
    payload = b'{"schema_version":1,"target_total":2}'

    artifact = store.publish_bytes(
        payload,
        kind="campaign_report",
        owner_id=OWNER,
    )

    assert artifact.sha256 == hashlib.sha256(payload).hexdigest()
    assert artifact.size_bytes == len(payload)
