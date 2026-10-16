"""Transfer batches must depend on one raw page, never earlier pagination."""

import copy
import hashlib
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest

from scrapers.fotmob.parsers import parse_transfers
from scrapers.fotmob.planner import RunMode
from scrapers.fotmob.repository import (
    FotMobRepository, ManifestStatus, TableRows, TargetCommit, deterministic_target_batch_id,
)
from scrapers.fotmob.transport import canonicalize_target
from tests.unit.scrapers.test_fotmob_repository import ReconcileWriter
from tests.unit.scrapers.test_fotmob_service import StubTransport, _service


POLICY = "transfer-page-snapshot-v1"
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures/fotmob/transfer_page_batch"


def _event(player):
    return {"playerId": player, "transferDate": "2026-07-01", "fromClubId": 1,
            "toClubId": 2, "feeText": "Free"}


def _pages(overlap):
    tail = {"hits": 51, "page": 2, "transfers": [_event(i) for i in range(100, 150)]}
    first = {"hits": 51, "page": 1, "transfers": [_event(100 if overlap else 999)]}
    payloads = [first, tail, {"hits": 51, "page": 3, "transfers": []}]
    return {canonicalize_target("transfers", {"leagueIds": "47", "page": n}).canonical_url: p
            for n, p in enumerate(payloads, 1)}


def _tail_commit(repository):
    return next(c for c in repository.commits
                if c.target_type == "transfers_page" and c.entity_id == "all:2")


def _seed_page(writer, payload, *, page=2, policy=None, omit_first=False):
    target = canonicalize_target("transfers", {"leagueIds": "47", "page": page})
    rows = [dict(r, competition_id="47", event_year=2026) for r in parse_transfers(payload)]
    if omit_first:
        rows = rows[1:]
    commit = TargetCommit(
        run_id="historical", target_type="transfers_page", target_key=target.target_key,
        competition_id="47", entity_id=f"all:{page}", status=ManifestStatus.SUCCESS,
        content_hash=hashlib.sha256(json.dumps(payload).encode()).hexdigest(),
        raw_uri=f"memory://{target.target_key}.json.gz", attempts=1,
        fetched_at=datetime(2026, 7, 11, 10), completed_at=datetime(2026, 7, 11, 10),
        observation_id=policy,
        capabilities={"page_row_policy": policy} if policy else {},
        expected_counts={"transfer_events": len(rows)},
    )
    repository = FotMobRepository(writer=writer)
    repository.commit(commit, [TableRows("fotmob_transfer_events", rows, "transfer_events")])
    repository.flush()
    return commit


class _NoTargetCacheRepository(FotMobRepository):
    # A missing/stale latest-target cache cannot weaken physical reconciliation.
    # The real stored manifest remains available to the unchanged strict guard.
    def latest_success(self, target_key):
        return None


def test_transfer_page_batch_is_stable_when_previous_page_overlap_changes():
    commits = []
    for overlap, expected_unique in [(True, 50), (False, 51)]:
        service, _, repository = _service(_pages(overlap))
        result = service.sync_transfers(47)
        assert result.ok, result.errors
        assert result.counts["events"] == expected_unique
        commit = _tail_commit(repository)
        rows = [r for r in repository.tables["fotmob_transfer_events"]
                if r["_target_batch_id"] == commit.batch_id]
        assert len(rows) == 50
        assert commit.expected_counts == {"transfer_events": 50}
        assert commit.capabilities["unique_seen"] == expected_unique
        assert commit.capabilities["source_page_rows"] == 50
        commits.append(commit)
    assert commits[0].batch_id == commits[1].batch_id
    assert commits[0].observation_id == POLICY


def test_transfer_full_page_uses_new_batch_without_overwriting_published_legacy_49():
    responses = _pages(False)
    payload = list(responses.values())[1]
    writer = ReconcileWriter()
    legacy = _seed_page(writer, payload, omit_first=True)
    original = copy.deepcopy(writer.rows["fotmob_transfer_events"])
    service, _, _ = _service(responses)
    service.repository = _NoTargetCacheRepository(writer=writer)
    result = service.sync_transfers(47, max_pages=2)
    assert result.ok, result.errors
    service.repository.flush()
    rows = writer.rows["fotmob_transfer_events"]
    assert [r for r in rows if r["_target_batch_id"] == legacy.batch_id] == original
    current = [r for r in rows if r["source_page"] == 2 and r["_target_batch_id"] != legacy.batch_id]
    assert len(current) == 50
    assert len({r["transfer_event_id"] for r in rows}) == 51
    assert not any(kwargs.get("delete_filter") for _, kwargs in writer.calls)


def test_transfer_new_policy_still_rejects_a_published_partial_49_of_50_batch():
    responses = _pages(False)
    writer = ReconcileWriter()
    current = _seed_page(writer, list(responses.values())[1], policy=POLICY)
    writer.rows["fotmob_transfer_events"].pop(0)
    original = copy.deepcopy(writer.rows)
    service, _, _ = _service(responses)
    service.repository = _NoTargetCacheRepository(writer=writer)
    with pytest.raises(RuntimeError, match="49 stored rows; expected either 0 or 50"):
        service.sync_transfers(47, max_pages=2)
        service.repository.flush()
    assert [r for r in writer.rows["fotmob_transfer_events"]
            if r["_target_batch_id"] == current.batch_id] == original["fotmob_transfer_events"]
    assert not any(kwargs.get("delete_filter") for _, kwargs in writer.calls)


def test_legacy_checkpoint_cannot_skip_full_page_materialization():
    payload = {"hits": 50, "page": 1, "transfers": [_event(i) for i in range(100, 150)]}
    target = canonicalize_target("transfers", {"leagueIds": "47", "page": 1})
    service, transport, repository = _service({target.canonical_url: payload}, mode=RunMode.BACKFILL)
    legacy = _seed_page(ReconcileWriter(), payload, page=1, omit_first=True)
    repository.commits.append(legacy)
    result = service.sync_transfers(47, max_pages=1, resume_since=datetime(2026, 7, 11))
    assert result.ok, result.errors
    assert transport.calls == [(target.canonical_url, False)]
    assert result.metadata["resumed_raw_pages"] == 0
    assert len(repository.tables["fotmob_transfer_events"]) == 50
    assert repository.commits[-1].observation_id == POLICY
    assert repository.commits[-1].batch_id != legacy.batch_id


def test_real_failed_batch_raw_page_keeps_50_rows_for_both_overlap_contexts():
    body = (FIXTURES / "league_130_page6.json").read_bytes()
    provenance = json.loads((FIXTURES / "provenance.json").read_text())
    assert hashlib.sha256(body).hexdigest() == provenance["sha256"]
    target = canonicalize_target("transfers", {"leagueIds": "130", "page": 6, "last": "1year"})
    assert target.canonical_url == provenance["canonical_url"]
    assert target.target_key == provenance["target_key"]
    assert deterministic_target_batch_id(target.target_key, provenance["sha256"]) == provenance["legacy_batch_id"]
    raw = json.loads(body)
    assert len(parse_transfers(raw)) == 50

    class CapturedTransport(StubTransport):
        def _get(self, endpoint, params, replay=False):
            result = super()._get(endpoint, params, replay)
            if result.url == target.canonical_url:
                result = replace(result, body=body, content_hash=provenance["sha256"],
                                 raw_uri=provenance["raw_uri"],
                                 fetched_at=provenance["fetched_at"])
            return result

    commits = []
    for overlap in (True, False):
        # Only the sixth page is real retained raw. Vary a synthetic prefix
        # to expose the local dependency; do not invent historical DB rows.
        prefix = raw["transfers"][0] if overlap else _event(99999999)
        responses = {}
        for page in range(1, 8):
            url = canonicalize_target("transfers", {"leagueIds": "130", "page": page,
                                                   "last": "1year"}).canonical_url
            responses[url] = raw if page == 6 else {
                "hits": 51, "page": page, "transfers": [prefix] if page < 6 else []}
        service, _, repository = _service(responses)
        service.transport = CapturedTransport(responses)
        result = service.sync_transfers(130, recent_only=True)
        assert result.ok, result.errors
        assert result.counts["events"] == (50 if overlap else 51)
        commit = next(c for c in repository.commits if c.entity_id == "1year:6")
        assert commit.content_hash == provenance["sha256"]
        assert commit.raw_uri == provenance["raw_uri"]
        assert commit.expected_counts == {"transfer_events": 50}
        assert len([r for r in repository.tables["fotmob_transfer_events"]
                    if r["_target_batch_id"] == commit.batch_id]) == 50
        commits.append(commit)
    assert commits[0].batch_id == commits[1].batch_id
    assert commits[0].batch_id != provenance["legacy_batch_id"]
