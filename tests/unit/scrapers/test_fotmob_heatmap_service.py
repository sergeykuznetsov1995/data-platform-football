"""Offline integration checks for the public match/heatmap ingestion path."""

import json
from dataclasses import replace
from datetime import timedelta
from functools import wraps
from inspect import signature

import pytest

from scrapers.fotmob import heatmap_service as heatmap_service_module
from scrapers.fotmob import service as service_module
from scrapers.fotmob.heatmaps import (
    FetchClassification,
    HeatmapOutcome,
    build_heatmap_reference,
)
from scrapers.fotmob.planner import RunMode
from scrapers.fotmob.raw_store import FotMobRawStore
from scrapers.fotmob.repository import ManifestStatus, MemoryFotMobRepository
from scrapers.fotmob.service import FotMobIngestService
from tests.unit.scrapers.test_fotmob_heatmaps import CIRCLE, encode, payload
from tests.unit.scrapers.test_fotmob_late_match import GOOD, T, bundle, seed
from tests.unit.scrapers.test_fotmob_transport import FakeResponse, _transport


SOURCE = "https://pub.fotmob.com/prod/db/api/heatmap/match/100"
PARENT = {**GOOD, "content": {**GOOD["content"], "heatmapUrl": SOURCE}}
WRAPPER = build_heatmap_reference(100, SOURCE).target
OUTCOMES = {
    "success", "url_missing", "resource_unavailable", "unexpected_format",
    "parse_failed", "not_applicable", "unknown",
}


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch):
    monkeypatch.setattr(service_module, "utc_now", lambda: T)
    monkeypatch.setattr(heatmap_service_module, "utc_now", lambda: T)


def _service(tmp_path, responses, *, store=None, repository=None,
             mode=RunMode.DAILY):
    if store is None:
        store = FotMobRawStore.from_uri(tmp_path.as_uri())
    if repository is None:
        repository = MemoryFotMobRepository()
    transport, session = _transport(responses, raw_store=store, max_attempts=1)
    service = FotMobIngestService(
        transport=transport, repository=repository, mode=mode,
        run_id="heatmap-test", max_workers=1,
    )
    return service, repository, store, transport, session


def _live(tmp_path, body=None, parent=None):
    if body is None:
        body = encode(payload())
    if parent is None:
        parent = PARENT
    setup = _service(tmp_path, [
        FakeResponse(200, encode(parent)), FakeResponse(200, body),
    ])
    result = setup[0].sync_match_payloads(bundle())
    return (*setup, result)


def _parent_with_players(players):
    return {**PARENT, "content": {**PARENT["content"], "playerStats": players}}


def _rows(repository, suffix):
    # Accept the repository's namespace prefix, but keep assets and points separate.
    return [
        row for table, rows in repository.tables.items()
        if table.endswith(suffix) for row in rows
    ]


def _assets(repository):
    return _rows(repository, "heatmap_assets")


def _points(repository):
    return _rows(repository, "heatmap_points")


def _identities(repository):
    return _rows(repository, "heatmap_player_identities")


def _commits(repository, target_type):
    return [c for c in repository.commits if c.target_type == target_type]


def _report(result):
    report = result.metadata["heatmap_report"]
    assert set(report["outcomes"]) == OUTCOMES
    assert {outcome.value for outcome in HeatmapOutcome} == OUTCOMES
    return report


def _outcome(result, expected):
    report = _report(result)
    assert report["outcomes"] == {
        outcome: int(outcome == expected) for outcome in OUTCOMES
    }
    return report


def _no_rows(repository):
    assert _assets(repository) == []
    assert _points(repository) == []
    assert _identities(repository) == []


def _candidate_ids(row):
    value = row["candidate_fotmob_player_ids_json"]
    return json.loads(value) if isinstance(value, str) else list(value)


def _parent_success(repository):
    commits = _commits(repository, "match")
    assert commits
    assert all(c.status == ManifestStatus.SUCCESS for c in commits)


def _calls(session, expected_count):
    urls = [url for url, _ in session.calls]
    assert len(urls) == expected_count
    assert "matchDetails" in urls[0]
    assert SOURCE not in urls
    if expected_count == 2:
        assert urls[1] == WRAPPER.canonical_url
    return urls


def _capture_observations(monkeypatch):
    """Observe the real child fetch without replacing its behavior."""
    observations = []
    original = heatmap_service_module._fetch_observation

    @wraps(original)
    def capture(*args, **kwargs):
        observation = original(*args, **kwargs)
        observations.append(observation)
        return observation

    monkeypatch.setattr(heatmap_service_module, "_fetch_observation", capture)
    return observations


def _capture_created_observations(monkeypatch):
    """Include deferred observations, which never reach the HTTP fetch helper."""
    observations = []
    original = heatmap_service_module.HeatmapObservation

    @wraps(original)
    def capture(*args, **kwargs):
        observation = original(*args, **kwargs)
        observations.append(observation)
        return observation

    monkeypatch.setattr(heatmap_service_module, "HeatmapObservation", capture)
    return observations


def test_success_publishes_separate_heatmap_manifest_and_typed_rows(tmp_path):
    _, repository, _, _, session, result = _live(tmp_path)

    assert result.succeeded == 1
    assert not result.errors
    assert not result.retryable
    _parent_success(repository)
    heatmaps = _commits(repository, "heatmap")
    assert len(heatmaps) == 1
    commit = heatmaps[0]
    assert commit.status == ManifestStatus.SUCCESS
    assert str(commit.entity_id) == "100"
    assets, points = _assets(repository), _points(repository)
    assert len(assets) == len(points) == 1
    for row in [assets[0], points[0]]:
        assert str(row["match_id"]) == "100"
        assert str(row["competition_id"]) == "47"
        assert row["source_season_key"] == "2025/2026"
        assert row["view_box"]
    assert assets[0]["content_hash"] == commit.content_hash
    assert points[0]["asset_content_hash"] == commit.content_hash
    assert assets[0]["view_box"] == points[0]["view_box"]
    _calls(session, 2)
    report = _outcome(result, "success")
    for key in ("matches", "matches_with_url", "unique_assets", "fetched",
                "network_fetched", "decoded", "typed", "typed_rows"):
        assert report[key] == 1, key
    assert report["replayed"] == 0


def test_identity_match_uses_opta_id_without_changing_legacy_points(tmp_path):
    parent = _parent_with_players({
        "200": {"id": 200, "optaId": "20", "name": "Opta match"},
        "20": {"id": 20, "optaId": "999", "name": "FotMob collision"},
    })
    _, repository, _, _, _, result = _live(tmp_path, parent=parent)

    assert result.succeeded == 1
    point, = _points(repository)
    assert (point["player_key"], point["player_id"]) == ("p20", 20)
    identity, = _identities(repository)
    assert identity["match_id"] == 100
    assert identity["player_key"] == "p20"
    assert identity["player_key_suffix"] == 20
    assert identity["match_status"] == "verified"
    assert identity["identity_status"] == "matched"
    assert identity["candidate_count"] == 1
    assert identity["fotmob_player_id"] == 200
    assert identity["identity_basis"] == "playerStats.optaId.unique"
    assert identity["resolver_version"] == "player_stats_opta_id_v1"
    assert _candidate_ids(identity) == [200]

    heatmap_commit = _commits(repository, "heatmap")[-1]
    identity_commit = _commits(repository, "heatmap_identity")[-1]
    parent_commit = _commits(repository, "match")[-1]
    assert identity["heatmap_batch_id"] == heatmap_commit.batch_id
    assert identity["asset_content_hash"] == heatmap_commit.content_hash
    assert identity["parent_content_hash"] == parent_commit.content_hash
    assert identity["parent_raw_uri"] == parent_commit.raw_uri
    assert identity_commit.target_key != heatmap_commit.target_key
    assert identity_commit.batch_id != heatmap_commit.batch_id


@pytest.mark.parametrize(
    "players,expected_status,expected_count,expected_candidates",
    [
        ({"20": {"id": 20, "optaId": "999"}}, "missing", 0, []),
        (
            {
                "200": {"id": 200, "optaId": "20"},
                "201": {"id": 201, "optaId": "20"},
            },
            "ambiguous",
            2,
            [200, 201],
        ),
    ],
    ids=["missing", "duplicate-ambiguous"],
)
def test_identity_missing_and_ambiguous_preserve_points_without_guessing(
    tmp_path, players, expected_status, expected_count, expected_candidates
):
    _, repository, _, _, _, result = _live(
        tmp_path, parent=_parent_with_players(players)
    )

    assert result.succeeded == 1
    assert len(_points(repository)) == 1
    identity, = _identities(repository)
    assert identity["player_key"] == "p20"
    assert identity["match_status"] == "verified"
    assert identity["identity_status"] == expected_status
    assert identity["candidate_count"] == expected_count
    assert identity["fotmob_player_id"] is None
    assert _candidate_ids(identity) == expected_candidates


def test_identity_includes_zero_circle_players(tmp_path):
    players = {
        "200": {"id": 200, "optaId": "20"},
        "300": {"id": 300, "optaId": "3"},
    }
    body = encode(payload({"p20": "", "p3": CIRCLE}))
    _, repository, _, _, _, result = _live(
        tmp_path, body=body, parent=_parent_with_players(players)
    )

    assert result.succeeded == 1
    assert len(_points(repository)) == 1
    identities = {row["player_key"]: row for row in _identities(repository)}
    assert set(identities) == {"p20", "p3"}
    assert identities["p20"]["identity_status"] == "matched"
    assert identities["p20"]["fotmob_player_id"] == 200
    assert identities["p3"]["fotmob_player_id"] == 300


def test_parent_change_updates_only_identity_batch(tmp_path):
    first_parent = _parent_with_players({"200": {"id": 200, "optaId": "20"}})
    _, repository, store, _, _, first = _live(tmp_path, parent=first_parent)
    assert first.succeeded == 1
    first_heatmap = _commits(repository, "heatmap")[-1]
    first_identity = _commits(repository, "heatmap_identity")[-1]
    second_parent = _parent_with_players({"201": {"id": 201, "optaId": "20"}})
    service, _, _, _, session = _service(
        tmp_path,
        [FakeResponse(200, encode(second_parent)), FakeResponse(200, encode(payload()))],
        store=store,
        repository=repository,
    )
    second = service.sync_match_payloads(bundle(), force_refresh=True)

    assert second.succeeded == 1
    second_heatmap = _commits(repository, "heatmap")[-1]
    second_identity = _commits(repository, "heatmap_identity")[-1]
    assert second_heatmap.batch_id == first_heatmap.batch_id
    assert second_identity.batch_id != first_identity.batch_id
    assert len(_assets(repository)) == 1
    assert len(_points(repository)) == 1
    assert len(_identities(repository)) == 2
    assert _identities(repository)[-1]["fotmob_player_id"] == 201
    _calls(session, 2)


def test_identity_failure_does_not_fallback_to_previous_parent(tmp_path, monkeypatch):
    first_parent = _parent_with_players({"200": {"id": 200, "optaId": "20"}})
    _, repository, store, _, _, first = _live(tmp_path, parent=first_parent)
    assert first.succeeded == 1
    first_identity = _identities(repository)[0]
    second_parent = _parent_with_players({"201": {"id": 201, "optaId": "20"}})
    service, _, _, _, session = _service(
        tmp_path,
        [FakeResponse(200, encode(second_parent)), FakeResponse(200, encode(payload()))],
        store=store,
        repository=repository,
    )
    original_commit = service._commit_for_fetch
    commit_signature = signature(original_commit)

    @wraps(original_commit)
    def fail_identity_write(*args, **kwargs):
        arguments = commit_signature.bind(*args, **kwargs).arguments
        if arguments["target_type"] == "heatmap_identity":
            raise OSError("synthetic identity failure")
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(service, "_commit_for_fetch", fail_identity_write)
    second = service.sync_match_payloads(bundle(), force_refresh=True)

    assert second.succeeded == 1
    second_parent_commit = _commits(repository, "match")[-1]
    assert second_parent_commit.content_hash != first_identity["parent_content_hash"]
    assert not any(
        row["parent_content_hash"] == second_parent_commit.content_hash
        for row in _identities(repository)
    )
    assert _commits(repository, "heatmap")[-1].batch_id == first_identity["heatmap_batch_id"]
    _outcome(second, "success")
    _calls(session, 2)


def test_new_heatmap_snapshot_replaces_whole_identity_set(tmp_path):
    parent = _parent_with_players({
        "200": {"id": 200, "optaId": "20"},
        "300": {"id": 300, "optaId": "3"},
    })
    _, repository, store, _, _, first = _live(tmp_path, parent=parent)
    assert first.succeeded == 1
    first_identity = _commits(repository, "heatmap_identity")[-1]
    service, _, _, _, session = _service(
        tmp_path,
        [FakeResponse(200, encode(parent)), FakeResponse(200, encode(payload({"p3": CIRCLE})))],
        store=store,
        repository=repository,
    )
    second = service.sync_match_payloads(bundle(), force_refresh=True)

    assert second.succeeded == 1
    latest = _commits(repository, "heatmap_identity")[-1]
    assert latest.batch_id != first_identity.batch_id
    current = [
        row for row in _identities(repository)
        if row["_target_batch_id"] == latest.batch_id
    ]
    assert {row["player_key"] for row in current} == {"p3"}
    assert current[0]["fotmob_player_id"] == 300
    assert len(_identities(repository)) == 2
    assert len(_points(repository)) == 2
    _calls(session, 2)


def test_first_missing_url_does_not_create_heatmap_state(tmp_path):
    service, repository, _, _, session = _service(
        tmp_path, [FakeResponse(200, encode(GOOD))],
    )
    result = service.sync_match_payloads(bundle())

    assert result.succeeded == 1
    assert not result.errors
    assert not _commits(repository, "heatmap")
    assert not any("heatmap" in table for table in repository.tables)
    _calls(session, 1)
    report = _outcome(result, "url_missing")
    assert report["matches"] == 1
    assert report["matches_with_url"] == 0
    assert report["unique_assets"] == report["fetched"] == 0


def test_missing_url_after_success_invalidates_latest_child(tmp_path):
    _, repository, store, _, _, first = _live(tmp_path)
    _outcome(first, "success")
    before_assets, before_points = list(_assets(repository)), list(_points(repository))
    service, _, _, _, session = _service(
        tmp_path, [FakeResponse(200, encode(GOOD))],
        store=store, repository=repository,
    )
    result = service.sync_match_payloads(bundle(), force_refresh=True)

    assert result.succeeded == 1
    _calls(session, 1)
    _outcome(result, "url_missing")
    child = _commits(repository, "heatmap")[-1]
    parent = _commits(repository, "match")[-1]
    assert child.status == ManifestStatus.NOT_AVAILABLE
    assert child.error_code == "url_missing"
    assert child.target_key == parent.target_key
    assert _assets(repository) == before_assets
    assert _points(repository) == before_points
    assert repository.latest_entity_success("heatmap", 100) is None


@pytest.mark.parametrize("status,body", [(404, b""), (200, b"null")],
                         ids=["http-404", "json-null"])
def test_unavailable_child_keeps_parent_successful(tmp_path, status, body):
    service, repository, _, _, session = _service(tmp_path, [
        FakeResponse(200, encode(PARENT)), FakeResponse(status, body),
    ])
    result = service.sync_match_payloads(bundle())

    assert result.succeeded == 1
    _parent_success(repository)
    assert _commits(repository, "heatmap")[-1].status == ManifestStatus.NOT_AVAILABLE
    _no_rows(repository)
    _calls(session, 2)
    assert _outcome(result, "resource_unavailable")["unavailable"] == 1


@pytest.mark.parametrize("numeric", ["NaN", "1e999"])
def test_nonfinite_coordinate_is_parse_failure(tmp_path, numeric):
    # Replace only the first circle's cx, leaving valid JSON and SVG structure.
    import re

    circle, replacements = re.subn(r'cx="[^"]*"', f'cx="{numeric}"', CIRCLE, count=1)
    assert replacements == 1
    body = encode(payload({"p20": circle}))
    json.loads(body)
    _, repository, _, _, session, result = _live(tmp_path, body)

    assert result.succeeded == 1
    _parent_success(repository)
    _outcome(result, "parse_failed")
    assert _commits(repository, "heatmap")[-1].status == ManifestStatus.SCHEMA_DRIFT
    _no_rows(repository)
    _calls(session, 2)
    assert "heatmap" in str(result.errors).lower()


def test_invalid_json_is_unexpected_format(tmp_path):
    _, repository, _, _, session, result = _live(tmp_path, b"not-json")

    assert result.succeeded == 1
    _parent_success(repository)
    _outcome(result, "unexpected_format")
    assert _commits(repository, "heatmap")[-1].status == ManifestStatus.SCHEMA_DRIFT
    _no_rows(repository)
    _calls(session, 2)
    assert "heatmap" in str(result.errors).lower()


def test_empty_players_publishes_empty_asset_without_error(tmp_path):
    _, repository, _, _, session, result = _live(tmp_path, encode(payload({})))

    assert result.succeeded == 1
    assert not result.errors
    _outcome(result, "not_applicable")
    assert len(_assets(repository)) == 1
    assert _assets(repository)[0]["circle_count"] == 0
    assert _points(repository) == []
    _parent_success(repository)
    _calls(session, 2)


def test_503_is_retryable_unknown_not_unavailable(tmp_path):
    service, repository, _, _, session = _service(tmp_path, [
        FakeResponse(200, encode(PARENT)), FakeResponse(503),
    ])
    result = service.sync_match_payloads(bundle())

    assert result.succeeded == 1
    assert result.retryable
    _parent_success(repository)
    assert _commits(repository, "heatmap")[-1].status == ManifestStatus.RETRYABLE_FAILURE
    report = _outcome(result, "unknown")
    assert report["unknown"] == 1
    assert report["unavailable"] == 0
    _no_rows(repository)
    _calls(session, 2)


def test_same_url_preserves_two_content_versions(tmp_path):
    _, repository, store, _, _, first = _live(tmp_path)
    _outcome(first, "success")
    v1 = _commits(repository, "heatmap")[-1]
    second_payload = payload({"p20": CIRCLE + CIRCLE})
    second_payload["lastModified"] = "2026-01-02T15:00:00Z"
    assert second_payload["lastModified"] != payload()["lastModified"]
    service, _, _, _, session = _service(tmp_path, [
        FakeResponse(200, encode(PARENT)), FakeResponse(200, encode(second_payload)),
    ], store=store, repository=repository)
    result = service.sync_match_payloads(bundle(), force_refresh=True)

    assert result.succeeded == 1
    assert not result.errors
    _outcome(result, "success")
    v2 = _commits(repository, "heatmap")[-1]
    assert v2.status == ManifestStatus.SUCCESS
    assert v1.content_hash != v2.content_hash
    assert v1.batch_id != v2.batch_id
    assert len(_assets(repository)) == 2
    assert len(_points(repository)) == 3
    assert {row["content_hash"] for row in _assets(repository)} == {
        v1.content_hash, v2.content_hash,
    }
    assert repository.latest_entity_success("heatmap", 100)["content_hash"] == v2.content_hash
    _calls(session, 2)


def test_replay_reuses_batch_and_preserves_physical_rows_and_observation_time(tmp_path, monkeypatch):
    _, repository, store, _, _, first = _live(tmp_path)
    _outcome(first, "success")
    original = _commits(repository, "heatmap")[-1]
    original_identity = _commits(repository, "heatmap_identity")[-1]
    counts = {table: len(rows) for table, rows in repository.tables.items()}
    observed = {
        table: [row["_observed_at"] for row in rows]
        for table, rows in repository.tables.items() if "heatmap" in table
    }
    assert observed
    monkeypatch.setattr(service_module, "utc_now", lambda: T + timedelta(hours=2))
    monkeypatch.setattr(heatmap_service_module, "utc_now", lambda: T + timedelta(hours=2))
    service, _, _, _, session = _service(
        tmp_path, [], store=store, repository=repository, mode=RunMode.REPLAY,
    )
    result = service.sync_match_payloads(bundle())

    assert result.succeeded == 1
    assert not result.errors
    assert session.calls == []
    report = _outcome(result, "success")
    assert report["replayed"] == report["fetched"] == report["typed_rows"] == 1
    assert report["network_fetched"] == 0
    assert _commits(repository, "heatmap")[-1].batch_id == original.batch_id
    assert _commits(repository, "heatmap_identity")[-1].batch_id == original_identity.batch_id
    assert {table: len(rows) for table, rows in repository.tables.items()} == counts
    assert {
        table: [row["_observed_at"] for row in rows]
        for table, rows in repository.tables.items() if "heatmap" in table
    } == observed


@pytest.mark.parametrize("initial,expected_calls", [(17, 1), (18, 2)])
def test_late_parent_and_child_share_request_quota(tmp_path, monkeypatch,
                                                  initial, expected_calls):
    service, repository, _, transport, session = _service(tmp_path, [
        FakeResponse(200, encode(PARENT)), FakeResponse(200, encode(payload())),
    ])
    seed(repository, observed=T)
    monkeypatch.setattr(service_module, "utc_now", lambda: T + timedelta(hours=6))
    monkeypatch.setattr(heatmap_service_module, "utc_now", lambda: T + timedelta(hours=6))
    observations = _capture_created_observations(monkeypatch)
    service.ledger.requests = initial
    result = service.sync_match_payloads(bundle())

    assert result.succeeded == 1
    _parent_success(repository)
    _calls(session, expected_calls)
    assert service.ledger.requests == initial + expected_calls
    assert service._late_match_requests == 1
    if initial == 17:
        report = _outcome(result, "unknown")
        assert report["unique_assets"] == report["fetched"] == 0
        assert report["network_fetched"] == 0
        assert service._late_heatmap_requests == 0
        deferred = [o for o in observations if o.outcome == HeatmapOutcome.UNKNOWN]
        assert len(deferred) == 1
        assert deferred[0].attempted is False
        _no_rows(repository)
    else:
        _outcome(result, "success")
        assert service._late_heatmap_requests == 1
        assert len(_assets(repository)) == len(_points(repository)) == 1


def test_cutoff_is_rechecked_after_parent_before_child_http(tmp_path, monkeypatch):
    service, repository, _, transport, session = _service(tmp_path, [
        FakeResponse(200, encode(PARENT)), FakeResponse(200, encode(payload())),
    ])
    seed(repository, observed=T + timedelta(hours=6))
    clock = [T + timedelta(hours=72) - timedelta(seconds=1)]
    monkeypatch.setattr(service_module, "utc_now", lambda: clock[0])
    monkeypatch.setattr(heatmap_service_module, "utc_now", lambda: clock[0])
    observations = _capture_created_observations(monkeypatch)
    service.ledger.requests = 18
    original_fetch = transport.fetch_json

    @wraps(original_fetch)
    def fetch_and_advance(*args, **kwargs):
        fetched = original_fetch(*args, **kwargs)
        if "matchDetails" in session.calls[-1][0]:
            clock[0] += timedelta(seconds=2)
        return fetched

    monkeypatch.setattr(transport, "fetch_json", fetch_and_advance)
    result = service.sync_match_payloads(bundle())

    assert result.succeeded == 1
    _parent_success(repository)
    _calls(session, 1)
    report = _outcome(result, "unknown")
    assert report["fetched"] == report["network_fetched"] == 0
    assert service._late_heatmap_requests == 0
    deferred = [o for o in observations if o.outcome == HeatmapOutcome.UNKNOWN]
    assert len(deferred) == 1
    assert deferred[0].attempted is False
    _no_rows(repository)


def test_hash_mismatch_has_no_verified_origin_or_fetched_count(tmp_path, monkeypatch):
    service, repository, _, transport, session = _service(tmp_path, [
        FakeResponse(200, encode(PARENT)), FakeResponse(200, encode(payload())),
    ])
    observations = _capture_observations(monkeypatch)
    original_fetch = transport.fetch_json

    @wraps(original_fetch)
    def corrupt_child_hash(*args, **kwargs):
        fetched = original_fetch(*args, **kwargs)
        if session.calls[-1][0] == WRAPPER.canonical_url:
            return replace(fetched, content_hash="0" * 64)
        return fetched

    monkeypatch.setattr(transport, "fetch_json", corrupt_child_hash)
    original_classify = heatmap_service_module.classify_heatmap_fetch

    def classify_corrupted(fetch):
        if fetch.target_key == WRAPPER.target_key and fetch.content_hash == "0" * 64:
            return FetchClassification(False, HeatmapOutcome.UNKNOWN)
        return original_classify(fetch)

    monkeypatch.setattr(
        heatmap_service_module, "classify_heatmap_fetch", classify_corrupted,
    )
    result = service.sync_match_payloads(bundle())

    assert result.succeeded == 1
    _parent_success(repository)
    _calls(session, 2)
    assert len(observations) == 1
    observation = observations[0]
    assert observation.outcome == HeatmapOutcome.UNKNOWN
    assert observation.hash_verified is False
    assert observation.fetch_origin is None
    report = _outcome(result, "unknown")
    assert report["fetched"] == report["network_fetched"] == report["replayed"] == 0
    assert report["typed"] == report["typed_rows"] == 0
    _no_rows(repository)


def test_writer_failure_cannot_report_unpublished_child_as_success(tmp_path, monkeypatch):
    service, repository, _, _, session = _service(tmp_path, [
        FakeResponse(200, encode(PARENT)), FakeResponse(200, encode(payload())),
    ])
    original_commit = service._commit_for_fetch
    commit_signature = signature(original_commit)
    child_writes = []

    @wraps(original_commit)
    def fail_child_write(*args, **kwargs):
        arguments = commit_signature.bind(*args, **kwargs).arguments
        if arguments["target_type"] == "heatmap":
            child_writes.append(arguments)
            raise OSError("synthetic writer failure")
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(service, "_commit_for_fetch", fail_child_write)
    result = service.sync_match_payloads(bundle())

    assert child_writes
    assert result.succeeded == 1
    assert result.errors or result.retryable
    assert "synthetic writer failure" in str((result.errors, result.retryable))
    parents = _commits(repository, "match")
    assert len(parents) == 1
    assert parents[0].status == ManifestStatus.SUCCESS
    assert not any(c.status == ManifestStatus.SUCCESS for c in _commits(repository, "heatmap"))
    report = _outcome(result, "unknown")
    assert report["typed"] == report["typed_rows"] == 0
    _no_rows(repository)
    _calls(session, 2)


def test_identity_writer_failure_preserves_successful_heatmap(tmp_path, monkeypatch):
    service, repository, _, _, session = _service(tmp_path, [
        FakeResponse(200, encode(PARENT)), FakeResponse(200, encode(payload())),
    ])
    original_commit = service._commit_for_fetch
    commit_signature = signature(original_commit)

    @wraps(original_commit)
    def fail_identity_write(*args, **kwargs):
        arguments = commit_signature.bind(*args, **kwargs).arguments
        if arguments["target_type"] == "heatmap_identity":
            raise OSError("synthetic identity writer failure")
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(service, "_commit_for_fetch", fail_identity_write)
    result = service.sync_match_payloads(bundle())

    assert result.succeeded == 1
    assert result.errors or result.retryable
    assert "synthetic identity writer failure" in str((result.errors, result.retryable))
    _parent_success(repository)
    assert _commits(repository, "heatmap")[-1].status == ManifestStatus.SUCCESS
    assert not any(
        commit.status == ManifestStatus.SUCCESS
        for commit in _commits(repository, "heatmap_identity")
    )
    assert len(_assets(repository)) == 1
    assert len(_points(repository)) == 1
    _outcome(result, "success")
    _calls(session, 2)
