"""Ephemeral S3 contract for FBref raw-first persistence and audit."""

from __future__ import annotations

import os
import uuid

import pytest

from scrapers.fbref.raw_audit import (
    audit_raw_fetches,
    capture_and_write_raw_inventory,
)
from scrapers.fbref.raw_store import RawPageStore, RawStoreError, match_page_target


pytestmark = pytest.mark.integration


def test_seaweedfs_blob_manifest_readback_idempotency_and_audit(tmp_path):
    uri = os.environ.get("FBREF_TEST_RAW_S3_URI", "").strip()
    if not uri:
        pytest.skip("FBREF_TEST_RAW_S3_URI is not configured")
    store = RawPageStore.from_uri(uri)
    _, baseline, _ = capture_and_write_raw_inventory(
        store,
        tmp_path / "fbref-raw-baseline.json",
    )
    refresh = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    target = match_page_target(uuid.uuid4().hex[:8])
    body = b"<html><table id='s3-smoke'><tr><td>ok</td></tr></table></html>"

    record = store.commit_fetch(
        target,
        body,
        logical_refresh_id=refresh,
        attempt_id=attempt_id,
        http_status=200,
        wire_bytes=len(body) + 128,
    )
    retry = store.commit_fetch(
        target,
        body,
        logical_refresh_id=refresh,
        attempt_id=attempt_id,
        http_status=200,
        wire_bytes=len(body) + 128,
    )

    assert retry == record
    assert store.load_fetch_content(refresh) == (body, record)
    with pytest.raises(RawStoreError, match="immutable"):
        store.commit_fetch(
            target,
            b"different",
            logical_refresh_id=refresh,
            attempt_id=attempt_id,
            http_status=200,
        )

    result = audit_raw_fetches(
        store,
        [
            {
                "logical_refresh_id": refresh,
                "attempt_id": attempt_id,
                "target_id": target.target_id,
                "content_hash": record.content_hash,
                "raw_manifest_key": store.fetch_manifest_key(refresh),
                "decoded_bytes": record.decoded_bytes,
                "compressed_bytes": record.encoded_bytes,
                "wire_bytes": len(body) + 128,
            }
        ],
        control_run_id=str(uuid.uuid4()),
        baseline_inventory=baseline,
    )

    assert result["status"] == "passed"
    assert result["baseline_delta"]["unlinked_created_object_count"] == 0
    assert result["baseline_delta"]["deleted_object_count"] == 0
