from __future__ import annotations

import json
from pathlib import Path

from dags.utils.transfermarkt_backfill_attempts import (
    collect_scope_attempt_evidence,
)
from utils.transfermarkt_backfill_state import AttemptOutcome
from scrapers.transfermarkt.raw_store import RawResponseStore


SCOPE_ID = "GB1__2020"
CYCLE_ID = "campaign-child"


def _paths(tmp_path: Path):
    base = tmp_path / "scope"
    entities = base / "entities"
    entities.mkdir(parents=True)
    return base, entities, base / "scope-manifest.json"


def _response(
    store: RawResponseStore,
    *,
    attempt: int,
    status: int,
    retry_after: str | None = None,
    endpoint: str = "players",
):
    headers = {"content-type": "text/html"}
    if retry_after is not None:
        headers["retry-after"] = retry_after
    capture = store.store_attempt(
        url="https://www.transfermarkt.com/page",
        body=b"source evidence",
        status_code=status,
        headers=headers,
        fetched_at=f"2026-07-2{attempt}T12:00:00+00:00",
        cycle_id=CYCLE_ID,
        scope_id=SCOPE_ID,
        endpoint=endpoint,
        attempt=attempt,
    )
    return store.store_response_envelope(capture)


def _write_entity(path: Path, envelopes, *, errors=()):
    path.write_text(json.dumps({
        "raw_attempts": [item.__dict__ for item in envelopes],
        "errors": list(errors),
    }), encoding="utf-8")


def test_404_is_one_unavailable_confirmation_with_raw_evidence(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    base, entities, manifest = _paths(tmp_path)
    envelope = _response(store, attempt=1, status=404, endpoint="listing")
    _write_entity(entities / "players-failed.json", [envelope])

    result = collect_scope_attempt_evidence(
        result_base_dir=str(base),
        entity_dir=str(entities),
        scope_manifest_path=str(manifest),
        scope_id=SCOPE_ID,
        raw_store=store,
    )

    assert result.outcome is AttemptOutcome.UNAVAILABLE_CONFIRMATION
    assert result.raw_evidence_ids == (envelope.envelope_id,)
    assert result.error_class is None
    assert result.observed_at.isoformat() == "2026-07-21T12:00:00+00:00"


def test_latest_source_error_honours_numeric_retry_after(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    base, entities, manifest = _paths(tmp_path)
    first = store.store_transport_error(
        url="https://www.transfermarkt.com/page",
        fetched_at="2026-07-20T12:00:00+00:00",
        cycle_id=CYCLE_ID,
        scope_id=SCOPE_ID,
        endpoint="players",
        attempt=1,
        error_kind="timeout",
        error_type="TimeoutError",
    )
    latest = _response(store, attempt=2, status=503, retry_after="7200")
    _write_entity(
        entities / "players-failed.json",
        [first, latest],
        errors=["source unavailable"],
    )

    result = collect_scope_attempt_evidence(
        result_base_dir=str(base),
        entity_dir=str(entities),
        scope_manifest_path=str(manifest),
        scope_id=SCOPE_ID,
        raw_store=store,
    )

    assert result.outcome is AttemptOutcome.SOURCE_ERROR
    assert result.error_class == "http_503"
    assert result.retry_after_seconds == 7200
    assert result.raw_evidence_ids == (first.envelope_id, latest.envelope_id)


def test_http_date_retry_after_is_relative_to_immutable_observation(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    base, entities, manifest = _paths(tmp_path)
    envelope = _response(
        store,
        attempt=1,
        status=503,
        retry_after="Tue, 21 Jul 2026 13:00:01 GMT",
    )
    _write_entity(entities / "players-failed.json", [envelope])

    result = collect_scope_attempt_evidence(
        result_base_dir=str(base),
        entity_dir=str(entities),
        scope_manifest_path=str(manifest),
        scope_id=SCOPE_ID,
        raw_store=store,
    )

    assert result.retry_after_seconds == 3601


def test_failure_intent_closes_crash_window_before_mutable_status(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    base, entities, manifest = _paths(tmp_path)
    campaign_id = "a" * 64
    batch_id = "b" * 64
    failure_path = entities / "players-failed-1-1.json"
    envelope = _response(store, attempt=1, status=503)
    failure_path.write_text(json.dumps({
        "failure_stage": "source",
        "failure_kind": "http",
        "network_fetches": 1,
        "raw_attempts": [envelope.__dict__],
        "errors": ["HTTP 503"],
    }), encoding="utf-8")
    (entities / ".source-attempt-1-1-players.json").write_text(json.dumps({
        "contract_version": 1,
        "status": "entered",
        "campaign_id": campaign_id,
        "child_cycle_id": CYCLE_ID,
        "scope_id": SCOPE_ID,
        "batch_id": batch_id,
        "claim_generation": 1,
        "attempt_sequence": 1,
        "parser_entity": "players",
        "failure_path": str(failure_path),
    }), encoding="utf-8")

    result = collect_scope_attempt_evidence(
        result_base_dir=str(base),
        entity_dir=str(entities),
        scope_manifest_path=str(manifest),
        scope_id=SCOPE_ID,
        raw_store=store,
        campaign_id=campaign_id,
        child_cycle_id=CYCLE_ID,
        batch_id=batch_id,
        claim_generation=1,
        attempt_sequence=1,
    )

    assert result.outcome is AttemptOutcome.SOURCE_ERROR
    assert result.error_class == "http_503"
    assert result.raw_evidence_ids == (envelope.envelope_id,)


def test_missing_raw_evidence_is_platform_error(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    base, entities, manifest = _paths(tmp_path)

    result = collect_scope_attempt_evidence(
        result_base_dir=str(base),
        entity_dir=str(entities),
        scope_manifest_path=str(manifest),
        scope_id=SCOPE_ID,
        raw_store=store,
    )

    assert result.outcome is AttemptOutcome.PLATFORM_ERROR
    assert result.raw_evidence_ids == ()
    assert result.error_class == "missing_raw_attempt_evidence"


def test_manifest_requires_and_binds_raw_attempts(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    base, entities, manifest = _paths(tmp_path)
    envelope = _response(store, attempt=1, status=200)
    _write_entity(entities / "players.json", [envelope])
    manifest.write_text("{}", encoding="utf-8")
    (base / "scope-cycle-checkpoint.json").write_text("{}", encoding="utf-8")

    result = collect_scope_attempt_evidence(
        result_base_dir=str(base),
        entity_dir=str(entities),
        scope_manifest_path=str(manifest),
        scope_id=SCOPE_ID,
        raw_store=store,
    )

    assert result.outcome is AttemptOutcome.CAPTURED
    assert result.manifest_path == str(manifest)
    assert result.checkpoint_path == str(base / "scope-cycle-checkpoint.json")


def test_previous_claim_files_cannot_mask_current_platform_failure(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    base, entities, manifest = _paths(tmp_path)
    old = _response(store, attempt=1, status=503)
    _write_entity(entities / "players-failed-1-1.json", [old])
    manifest.write_text("{}", encoding="utf-8")
    (base / "scope-status.json").write_text(json.dumps({
        "status": "failed",
        "backfill_batch_id": "old-batch",
        "backfill_claim_generation": "1",
        "backfill_attempt_sequence": "1",
    }), encoding="utf-8")

    result = collect_scope_attempt_evidence(
        result_base_dir=str(base),
        entity_dir=str(entities),
        scope_manifest_path=str(manifest),
        scope_id=SCOPE_ID,
        raw_store=store,
        campaign_id="a" * 64,
        child_cycle_id=CYCLE_ID,
        batch_id="new-batch",
        claim_generation=2,
        attempt_sequence=2,
    )

    assert result.outcome is AttemptOutcome.PLATFORM_ERROR
    assert result.raw_evidence_ids == ()
