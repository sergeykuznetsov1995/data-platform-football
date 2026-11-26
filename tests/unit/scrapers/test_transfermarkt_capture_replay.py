"""Original response envelopes through a network-free Transfermarkt CoreRun."""

from __future__ import annotations

import gzip
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from scrapers.transfermarkt.capture_replay import capture_replay
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.orchestrator import CoreRun, CoreScopePlan, SectionTarget, mv_history_url
from scrapers.transfermarkt.raw_store import RawCaptureCorrupt, RawCaptureNotFound, RawResponseStore
from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter


pytestmark = pytest.mark.unit
CONTEXT = {"scope_id": "replay-scope", "cycle_id": "replay-cycle"}
AT = "2026-11-21T11:15:30.000001+00:00"
PROFILE = "https://tmapi.transfermarkt.technology/club/127"
FIXTURES = Path("tests/fixtures/transfermarkt/stage_a_rewrite")


@pytest.fixture
def store(tmp_path):
    return RawResponseStore.from_uri((tmp_path / "raw").as_uri())


def _save(store, *, url=None, endpoint="market_value_points", body=b'{"list":[]}', status=200, **kwargs):
    raw = store.store_attempt(
        url or mv_history_url("42"), body, status, {"content-type": "application/json"},
        kwargs.pop("fetched_at", AT), endpoint=endpoint, attempt=kwargs.pop("attempt", 1), **(CONTEXT | kwargs),
    )
    return store.store_response_envelope(raw)


def _fetch(replay, envelope, **kwargs):
    args = {"label": envelope.endpoint, "context": CONTEXT, "as_json": True}
    return replay.fetch(envelope.url, **(args | kwargs))


def test_replays_original_capture_clock_identity_and_verified_bronze_write(store, tmp_path, monkeypatch):
    fixture = next(item for item in json.loads((FIXTURES / "manifest.json").read_text())["fixtures"]
                   if item["endpoint"] == "market_value_points")
    record = json.loads((FIXTURES / fixture["capture_file"]).read_text())
    body = gzip.decompress((FIXTURES / fixture["blob_file"]).read_bytes())
    original = store.store_attempt(body=body, **{field: record[field] for field in (
        "url", "endpoint", "status_code", "headers", "fetched_at", "cycle_id", "scope_id", "attempt",
    )})
    assert original.capture_id == record["capture_id"]
    envelope = store.store_response_envelope(original)
    scope = {"scope_id": original.scope_id, "cycle_id": original.cycle_id}
    ids = [envelope.envelope_id]

    replay = capture_replay(store, ids, **scope)
    assert replay.fetched_at == (original.fetched_at,)
    result = _fetch(replay, envelope, context=scope)
    assert result.status is FetchStatus.OK
    assert (result.raw_capture_id, result.raw_body_hash, result.raw_fetched_at) == (
        original.capture_id, original.content_hash, original.fetched_at)
    assert result.raw_attempt_envelope_ids == (envelope.envelope_id,)
    restored = FetchOutcome.from_checkpoint(result.as_checkpoint())
    assert result.cache_hit and restored.raw_capture_id == original.capture_id
    assert replay.load(original.capture_id, original.url) == (body, original)
    replay.assert_consumed()

    monkeypatch.setenv("TM_WRITER_LOCK_PATH", str(tmp_path / "writer.lock"))
    monkeypatch.setenv("TM_CORE_WRITE_JOURNAL_DIR", str(tmp_path / "journal"))
    monkeypatch.delenv("TM_WRITER_LOCK_SHARED_FILE_ID", raising=False)
    db = CoreDB()
    plan = CoreScopePlan("GB1", "2627", **scope, market_value_player_ids=(original.url.rsplit("/", 1)[-1],))
    replay = capture_replay(store, ids, **scope)
    report = CoreRun(replay.fetch, request_raw_loader=replay.load).run(plan, writer=CoreWriter(db),
                                                                    connection_factory=lambda: db)
    replay.assert_consumed()
    assert report.dq["outcome_status_counts"] == {"observed_present": 1}
    assert len(report.receipts) == 1 and report.receipts[0]["receipt_readback"]
    [(capture, digest, stamp)] = db.sql.execute(
        "SELECT DISTINCT raw_capture_id, source_body_hash, captured_at FROM transfermarkt_market_value_points"
    ).fetchall()
    assert (capture, digest) == (original.capture_id, original.content_hash)
    assert datetime.fromisoformat(str(stamp)) == datetime.fromisoformat(original.fetched_at)


@pytest.mark.parametrize("ids", [[], "", None, {}, ["bad"], [None]])
def test_input_must_be_an_explicit_nonempty_list_of_envelope_ids(store, ids):
    with pytest.raises(ValueError):
        capture_replay(store, ids, **CONTEXT)


def test_rejects_missing_repeated_conflicting_and_mismatched_evidence(store):
    original = _save(store)
    with pytest.raises(RawCaptureNotFound):
        capture_replay(store, ["0" * 64], **CONTEXT)
    with pytest.raises(ValueError, match="repeated envelope"):
        capture_replay(store, [original.envelope_id] * 2, **CONTEXT)
    later = _save(store, body=b'{"list":[1]}', attempt=2)
    with pytest.raises(ValueError, match="multiple originals"):
        capture_replay(store, [original.envelope_id, later.envelope_id], **CONTEXT)
    for override in ({"scope_id": "other"}, {"cycle_id": "other"}, {"scope_id": ""}):
        with pytest.raises(ValueError, match="scope"):
            capture_replay(store, [original.envelope_id], **(CONTEXT | override))
    key = store.attempt_manifest_key(original.envelope_id)
    manifest = json.loads(store._read_bytes(key))
    manifest["observed_at"] = "2026-11-26T23:59:00+00:00"
    store._write_bytes(key, json.dumps(manifest).encode())
    with pytest.raises(RawCaptureCorrupt):
        capture_replay(store, [original.envelope_id], **CONTEXT)


@pytest.mark.parametrize(("status", "body"), [
    (403, b"Forbidden"),
    (429, b"Rate limited"),
    (200, b"<title>Human verification</title>"),
    (200, b"<html><script src='/cdn-cgi/challenge-platform/a.js'></script></html>"),
    (200, b'{"error":"Cloudflare challenge"}'),
    (200, b'{"error":{"code":403,"message":"Forbidden"}}'),
    (200, b""),
])
def test_denied_page_prevents_all_fetches_writes_and_later_success(store, monkeypatch, status, body):
    bad = _save(store, status=status, body=body)
    later = _save(store, attempt=2)
    load = Mock(wraps=store.load_attempt_envelope)
    monkeypatch.setattr(store, "load_attempt_envelope", load)
    run = Mock()
    with pytest.raises(ValueError):
        replay = capture_replay(store, [bad.envelope_id, later.envelope_id], **CONTEXT)
        run(replay)
    load.assert_called_once_with(bad.envelope_id)
    run.assert_not_called()


def test_transport_error_cannot_supply_a_response(store):
    error = store.store_transport_error(url=mv_history_url("42"), fetched_at=AT, endpoint="market_value_points",
                                        attempt=1, error_kind="connection", error_type="ConnectionError", **CONTEXT)
    with pytest.raises(ValueError, match="response envelope"):
        capture_replay(store, [error.envelope_id], **CONTEXT)


def test_plan_requires_every_original_and_cannot_request_or_load_unlisted_data(store):
    first, second = _save(store), _save(store, url=mv_history_url("43"))
    ids = [first.envelope_id, second.envelope_id]
    replay = capture_replay(store, ids, **CONTEXT)
    _fetch(replay, first)
    with pytest.raises(ValueError, match="1/2"):
        replay.assert_consumed()
    with pytest.raises(ValueError, match="ID and URL"):
        replay.load(first.capture_id, second.url)
    with pytest.raises(ValueError, match="stopped"):
        _fetch(replay, second)
    for override in ({"label": "transfer_events"}, {"context": {"cycle_id": "other"}}):
        replay = capture_replay(store, ids, **CONTEXT)
        with pytest.raises(ValueError):
            _fetch(replay, first, **override)


def test_missing_request_and_failed_validation_stop_the_run_before_writing(store):
    original = _save(store)
    replay = capture_replay(store, [original.envelope_id], **CONTEXT)
    plan = CoreScopePlan("GB1", "2627", **CONTEXT, market_value_player_ids=("42", "43"))
    factory = Mock(side_effect=AssertionError("a partial program cannot write"))
    with pytest.raises(ValueError, match="missing or repeated"):
        CoreRun(replay.fetch, request_raw_loader=replay.load).run(plan, writer=object(), connection_factory=factory)
    factory.assert_not_called()
    with pytest.raises(ValueError, match="stopped"):
        _fetch(replay, original)
    replay = capture_replay(store, [original.envelope_id], **CONTEXT)
    validator = Mock(return_value="no expected fields")
    with pytest.raises(ValueError, match="validation failed"):
        _fetch(replay, original, validator=validator)
    validator.assert_called_once_with({"list": []})
    with pytest.raises(ValueError, match="incomplete"):
        replay.assert_consumed()


def test_stadium_404_only_proves_absence_with_matching_profile(store):
    stadium = _save(store, url=PROFILE + "/stadium", endpoint="club_stadium", status=404, body=b"Not found")
    with pytest.raises(ValueError, match="requires its club profile"):
        capture_replay(store, [stadium.envelope_id], **CONTEXT)
    profile = _save(store, url=PROFILE, endpoint="club_profile",
                    body=b'{"success":true,"data":{"id":127,"name":"SC Paderborn","baseDetails":{"isNationalTeam":false}}}')
    pages = (stadium, profile)
    replay = capture_replay(store, [page.envelope_id for page in pages], **CONTEXT)
    plan = CoreScopePlan("L1", "2627", **CONTEXT,
                         section_targets=tuple(SectionTarget(page.endpoint, page.url) for page in pages))
    report = CoreRun(replay.fetch, request_raw_loader=replay.load).run(plan)
    replay.assert_consumed()
    assert {entry["entity"]: entry["status"] for entry in report.outcomes} == {
        "club_profile_observations": "observed_present",
        "stadium_observations": "observed_empty",
        "club_stadium_relations": "observed_empty",
    }


def test_block_page_cannot_prove_stadium_absence_even_with_http_404(store):
    profile = _save(store, url=PROFILE, endpoint="club_profile")
    stadium = _save(store, url=PROFILE + "/stadium", endpoint="club_stadium",
                    status=404, body=b'{"success":false,"message":"Access denied"}')
    with pytest.raises(ValueError, match="denied"):
        capture_replay(store, [profile.envelope_id, stadium.envelope_id], **CONTEXT)
