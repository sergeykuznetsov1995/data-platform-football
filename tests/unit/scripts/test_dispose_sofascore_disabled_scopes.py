import json

import pytest

from dags.scripts.dispose_sofascore_disabled_scopes import (
    DisabledScopeError,
    apply_disabled_disposition,
    build_disabled_disposition,
)
from scrapers.sofascore.manifest import EndpointManifest, ManifestKey, ManifestStatus

SCOPE = ("10783", "16000")


def _record(endpoint, status, target_id="1"):
    return EndpointManifest(
        key=ManifestKey(*SCOPE, "event", target_id, endpoint, "final"),
        status=status,
        run_id="old-run",
        task_id="capture",
        attempts=1,
        row_count=1,
        http_status=200,
        raw_content_hash="hash",
        raw_blob_key="blob",
    )


def _payload(event_id=1):
    return json.dumps({
        "event": {
            "id": event_id,
            "season": {"id": 16000},
            "tournament": {"uniqueTournament": {"id": 10783}},
        }
    }).encode()


def _records(status=ManifestStatus.RETRYABLE_FAILURE):
    return {SCOPE: [_record("event", status)]}


def test_disabled_scope_disposition_is_not_supported_and_idempotent():
    calls = []
    store = type("Store", (), {
        "upsert": lambda self, record: None,
        "flush": lambda self: calls.append("flush"),
    })()
    calls = []
    store.upsert = lambda record: calls.append(record)
    records_by_scope = {
        SCOPE: [
            _record("event", ManifestStatus.RETRYABLE_FAILURE),
            _record("incidents", ManifestStatus.RETRYABLE_FAILURE),
        ]
    }
    plan = build_disabled_disposition(
        records_by_scope,
        lambda key: _payload(),
        {SCOPE: False},
        run_id="disabled-test", scopes=(SCOPE,),
    )
    apply_disabled_disposition(store, plan)

    assert len(calls) == 3
    assert {record.status for record in calls if not isinstance(record, str)} == {ManifestStatus.NOT_SUPPORTED}
    assert {record.error_type for record in calls if not isinstance(record, str)} == {"ReplayScopeNotSupported"}
    assert {record.run_id for record in calls if not isinstance(record, str)} == {"disabled-test"}


def test_disabled_disposition_rejects_capture_allowed_registry():
    with pytest.raises(DisabledScopeError, match="allows capture"):
        build_disabled_disposition(
            _records(), lambda key: _payload(), {SCOPE: True}, run_id="x", scopes=(SCOPE,)
        )


def test_disabled_disposition_rejects_event_id_mismatch_without_writes():
    with pytest.raises(DisabledScopeError, match="payload event id mismatch"):
        build_disabled_disposition(
            _records(), lambda key: _payload(event_id=999), {SCOPE: False}, run_id="x", scopes=(SCOPE,)
        )


def test_disabled_disposition_empty_scope_is_idempotent():
    plan = build_disabled_disposition(
        {SCOPE: []}, lambda key: _payload(), {SCOPE: False}, run_id="x", scopes=(SCOPE,)
    )

    assert plan == ((SCOPE, ()),)
