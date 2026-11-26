"""A retained source stub is unavailable, never an empty accepted calendar."""
import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from scrapers.fotmob.catalog import classify_competition, competition_from_league_payload
from scrapers.fotmob.planner import RunMode, ScopeAttemptState
from scrapers.fotmob.repository import ManifestStatus, TargetCommit, deterministic_target_batch_id
from scrapers.fotmob.service import _is_calendar_unavailable_stub
from scrapers.fotmob.transport import FetchOutcome, canonicalize_target
from tests.unit.scrapers.test_fotmob_calendar_contract import calendar
from tests.unit.scrapers.test_fotmob_service import StubTransport, _league_payload, _service


def unavailable():
    path = Path(__file__).parents[2] / "fixtures/fotmob/league_10056_calendar_unavailable.json"
    return json.loads(path.read_text())


def seasons_only_unavailable():
    path = Path(__file__).parents[2] / "fixtures/fotmob/league_9862_calendar_unavailable.json"
    return json.loads(path.read_text())


def stats_links_only_unavailable():
    path = Path(__file__).parents[2] / "fixtures/fotmob/league_221_2020_calendar_unavailable.json"
    body = path.read_bytes()
    # Immutable raw selected by the history run of 2026-11-26 for 221=2020.
    assert hashlib.sha256(body).hexdigest() == "a049a1afcb0d7889ae830266f62a36aebfcff47bc99d66204c3914fd70f270b2"
    return json.loads(body)


@pytest.mark.parametrize("persist", [True, False])
def test_retained_stats_index_with_no_calendar_stays_retryable(persist):
    payload = stats_links_only_unavailable()
    target = canonicalize_target("leagues", {"id": 221, "season": "2020"})
    svc, _, repo = _service({target.canonical_url: payload}, mode=RunMode.BACKFILL)
    method = svc.sync_season if persist else svc.read_season_dependency
    op, bundle = method(221, "2020")
    assert bundle is None and op.status == "retryable" and op.succeeded == 0
    assert op.metadata["calendar_unavailability"]["reason"] == "source_calendar_unavailable"
    assert [c.status for c in repo.commits] == ([ManifestStatus.RETRYABLE_FAILURE] if persist else [])
    assert not repo.latest_success(target.target_key)
    assert not repo.tables.get("fotmob_matches")


@pytest.mark.parametrize("variant", [
    "player_stats", "team_stats", "overview_players", "shot_map", "table",
    "malformed_link", "malformed_names", "fixture_data", "known_matches", "replay",
])
def test_stats_index_does_not_hide_data_or_invalid_calendars(variant):
    payload = stats_links_only_unavailable()
    if variant == "player_stats":
        payload["stats"]["players"] = [{"name": "goals"}]
    elif variant == "team_stats":
        payload["stats"]["teams"] = [{"name": "goals"}]
    elif variant == "overview_players":
        payload["overview"]["topPlayers"]["byRating"]["players"] = [{"id": 17}]
    elif variant == "shot_map":
        payload["overview"]["shotMap"] = {"shots": [{"id": 1}]}
    elif variant == "table":
        payload["overview"]["table"] = [{"id": 1}]
    elif variant == "malformed_link":
        payload["stats"]["seasonStatLinks"][0]["RelativePath"] = {"rows": [1]}
    elif variant == "malformed_names":
        payload["stats"]["seasonsWithLinks"] = "2020"
    elif variant == "fixture_data":
        payload["fixtures"]["allMatches"] = {"matchId": 1}
    url = canonicalize_target("leagues", {"id": 221, "season": "2020"}).canonical_url
    mode = RunMode.REPLAY if variant == "replay" else RunMode.BACKFILL
    svc, transport, repo = _service({url: payload}, mode=mode)
    if variant == "known_matches":
        repo.has_committed_matches = Mock(return_value=True)
    op, bundle = svc.sync_season(221, "2020")
    assert bundle is None and op.errors
    assert repo.commits[-1].status == ManifestStatus.SCHEMA_DRIFT
    assert "calendar_unavailability" not in op.metadata
    if variant == "replay":
        assert transport.calls == [(url, True)] and svc.ledger.requests == 0


def history_with_legacy_calendar():
    target = canonicalize_target("leagues", {"id": 221, "season": "2020"})
    archive = stats_links_only_unavailable()
    recovered = _league_payload("2020")
    recovered["details"].update(id=221, latestSeason="2025")
    recovered["allAvailableSeasons"] = ["2025", "2020"]
    recovered["stats"] = {}
    old = StubTransport({target.canonical_url: archive}).replay_json(target.canonical_url)
    svc, transport, repo = _service({}, mode=RunMode.BACKFILL)
    repo.commit(TargetCommit(
        run_id="legacy", target_type="league_season", target_key=target.target_key,
        competition_id="221", source_season_key="2020", status=ManifestStatus.SUCCESS,
        content_hash=old.content_hash, raw_uri=old.raw_uri,
        fetched_at=datetime(2025, 9, 20), capabilities={"canonical_url": target.canonical_url},
        expected_counts={"matches": 0},
    ))
    return svc, transport, repo, target, old, recovered


@pytest.mark.parametrize("variant", [
    "still_missing_304", "recovered", "stale", "offline", "replay", "not_due",
    "healthy_archive", "known_matches", "wrong_archived_season",
])
def test_only_due_archived_calendar_is_revalidated_with_source_evidence(variant):
    svc, transport, repo, target, old, recovered = history_with_legacy_calendar()
    url = target.canonical_url
    fresh = StubTransport({url: recovered}).fetch_json(url)
    archived = old
    if variant == "still_missing_304":
        fresh = replace(old, outcome=FetchOutcome.NOT_MODIFIED, http_status=304, attempts=1)
    elif variant == "stale":
        fresh = replace(fresh, outcome=FetchOutcome.STALE_REPLAY, http_status=503, stale=True)
    elif variant == "offline":
        fresh = replace(old, outcome=FetchOutcome.RETRYABLE_FAILURE, http_status=503, attempts=1)
    elif variant == "replay":
        svc.mode = RunMode.REPLAY
    elif variant == "known_matches":
        repo.has_committed_matches = Mock(return_value=True)
    elif variant == "healthy_archive":
        archived = recovered
    elif variant == "wrong_archived_season":
        archived = copy.deepcopy(old.data)
        archived["details"]["selectedSeason"] = "2021"
        fresh = StubTransport({url: old.data}).fetch_json(url)
    transport.responses[url] = [archived, fresh]
    op, bundle = svc.read_season_dependency(221, "2020", revalidate_unavailable=variant != "not_due")
    online = variant in {"still_missing_304", "recovered", "stale", "offline", "wrong_archived_season"}
    assert transport.calls == ([(url, True), (url, False)] if online else [(url, True)])
    assert svc.ledger.requests == int(online)
    if variant == "recovered":
        assert bundle and op.succeeded == 1 and op.metadata["persisted"]
        assert [str(row["match_id"]) for row in repo.tables["fotmob_matches"]] == ["100"]
        assert repo.commits[-1].batch_id == deterministic_target_batch_id(target.target_key, fresh.content_hash)
    elif variant == "healthy_archive":
        assert bundle and op.succeeded == 1 and not op.metadata["persisted"]
        assert len(repo.commits) == 1
    else:
        assert bundle is None and op.succeeded == 0
        if variant in {"replay", "known_matches"}:
            assert op.errors
        else:
            assert op.retryable
        if variant == "still_missing_304":
            assert op.metadata["calendar_unavailability"]["http_status"] == 304
        if variant == "stale":
            assert op.metadata["calendar_revalidation"]["stale"] is True
        if variant in {"stale", "offline"}:
            assert op.metadata["calendar_unavailability"]["content_hash"] == old.content_hash
            assert op.metadata["calendar_revalidation"]["http_status"] == 503
        assert not repo.tables.get("fotmob_matches")


def test_history_finishes_recovered_calendar_only_after_its_match_is_collected(monkeypatch):
    from tests.unit.scrapers.test_run_fotmob_scraper import TestFotmobNativeRunner, _run_native_admitted
    mod = TestFotmobNativeRunner._module()
    monkeypatch.setenv(mod.WRITER_LOCK_ENV, "0")
    svc, transport, repo, target, old, recovered = history_with_legacy_calendar()
    campaign = "fmplan1-15baa7dff09f3aae879da775d8684bc4c4b4d6803e821f40c79f36a7cae7054b"
    svc.record_scope_completion(221, "2020", plan_signature=campaign, counts={"matches": 0},
                                coverage={"match_identity_hash": hashlib.sha256(b"").hexdigest()})
    profile = copy.deepcopy(recovered)
    profile["details"]["selectedSeason"] = "2025"
    profile["allAvailableSeasons"].append("2019")
    older = copy.deepcopy(recovered)
    older["details"]["selectedSeason"] = "2019"
    older["fixtures"]["allMatches"] = []
    match = canonicalize_target("matchDetails", {"matchId": 100}).canonical_url
    outage = replace(old, outcome=FetchOutcome.RETRYABLE_FAILURE, attempts=1, http_status=503, stale=False)
    transport.responses.update({
        canonicalize_target("allLeagues").canonical_url: {"countries": [{"leagues": [{"id": 221, "name": profile["details"]["name"]}]}]},
        canonicalize_target("leagues", {"id": 221}).canonical_url: profile,
        canonicalize_target("leagues", {"id": 221, "season": "2019"}).canonical_url: older,
        target.canonical_url: [old, recovered, recovered],
        match: [outage, {"general": {"matchId": "100"}, "content": {"matchFacts": {"events": []}}}],
    })
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    previous = ScopeAttemptState(221, "2020", "history-test", 2, now-timedelta(days=2),
                                 now-timedelta(days=1), "retryable", "source_calendar_unavailable")
    repo.scope_attempt_states = lambda *args, **kwargs: {(221, "2020"): previous}
    args = mod._argument_parser().parse_args(["--mode", "backfill", "--catalog-contract", "fotmob-catalog-v1",
                                              "--entities", "season,matches,leaderboards", "--season-limit", "1"])
    rc, limited = _run_native_admitted(mod, args, service=svc)
    assert rc == 0 and limited["selection"]["completed_scopes"] == ["221=2019"]
    deferred = next(a for a in limited["selection"]["scope_attempts"] if a["source_season_key"] == "2020")
    assert deferred["outcome"] == "deferred" and deferred["reason"] == "source_calendar_unavailable"
    assert not any(url == target.canonical_url for url, _ in transport.calls)
    previous = replace(previous, outcome=deferred["outcome"], reason=deferred["reason"])
    args.season_limit = 0
    rc, first = _run_native_admitted(mod, args, service=svc)
    assert rc and not first["selection"]["completed_scopes"]
    assert repo.tables["fotmob_matches"] and not repo.tables.get("fotmob_match_payloads")
    # The next attempt's reason concerns the match now. It reads the recovered
    # season from raw and must still collect that match before taking credit.
    previous = replace(previous, reason="match incomplete")
    rc, final = _run_native_admitted(mod, args, service=svc)
    assert rc == 0, final["errors"]
    assert final["selection"]["completed_scopes"] == ["221=2020"]
    assert [str(row["match_id"]) for row in repo.tables["fotmob_match_payloads"]] == ["100"]
    assert transport.calls.count((target.canonical_url, False)) == 1
    assert len([c for c in repo.commits if c.target_type == "scope_completion" and c.entity_id == campaign]) == 1


def test_seasons_only_fixture_matches_retained_copy_hash_and_scope():
    path = Path(__file__).parents[2] / "fixtures/fotmob/league_9862_calendar_unavailable.json"
    provenance = json.loads(path.with_suffix(".provenance.json").read_text())
    assert hashlib.sha256(path.read_bytes()).hexdigest() == provenance["sha256"]
    payload = seasons_only_unavailable()
    assert payload["details"]["id"] == provenance["competition_id"] == 9862
    assert payload["details"]["selectedSeason"] == provenance["source_season_key"] == "2017/2018"
    assert payload["tabs"] == ["seasons"]


@pytest.mark.parametrize("tabs,accepted", [
    ([], True), (["seasons"], True), (["matches"], False),
    (["seasons", "fixtures"], False), (None, False), ("seasons", False),
    ({"seasons": True}, False),
])
def test_retained_seasons_stub_requires_only_supported_tabs(tabs, accepted):
    payload = seasons_only_unavailable()
    payload["tabs"] = tabs
    assert _is_calendar_unavailable_stub(payload) is accepted
    url = canonicalize_target("leagues", {"id": 9862, "season": "2017/2018"}).canonical_url
    service, _, repo = _service({url: payload})
    result, bundle = service.sync_season(9862, "2017/2018")
    assert bundle is None and result.succeeded == 0
    assert not repo.tables.get("fotmob_matches")
    assert not any(c.target_type == "scope_completion" for c in repo.commits)
    assert not repo.latest_success(canonicalize_target(url).target_key)
    if accepted:
        assert not result.errors and result.retryable
        assert result.metadata["calendar_unavailability"]["reason"] == "source_calendar_unavailable"
        assert repo.commits[-1].status == ManifestStatus.RETRYABLE_FAILURE
        assert repo.commits[-1].error_code == "source_calendar_unavailable"
    else:
        assert result.errors and not result.retryable
        assert "calendar_unavailability" not in result.metadata
        assert repo.commits[-1].status == ManifestStatus.SCHEMA_DRIFT


def test_discovery_accepts_retained_seasons_only_stub():
    payload = seasons_only_unavailable()
    comp = competition_from_league_payload(payload)
    url = canonicalize_target("leagues", {"id": 9862}).canonical_url
    service, _, repo = _service({url: payload})
    result = service.discover_competition(classify_competition(comp, comp))
    assert result.operation.ok, result.operation.errors
    assert result.seasons and result.selected_bundle is None
    assert result.operation.metadata["calendar_available"] is False
    assert not repo.tables.get("fotmob_matches")
    assert not any(c.target_type == "scope_completion" for c in repo.commits)


SIX_CALENDARS = [(10433, "2022/2023"), (9170, "2023"), (9669, "2023/2024"),
                 (300, "2025"), (10840, "2025/2026"), (10056, "2026/2027")]


def retained_unavailable(cid):
    path = Path(__file__).parents[2] / f"fixtures/fotmob/six_calendars_unavailable/league_{cid}.json"
    return json.loads(path.read_text())


def test_retained_fixtures_match_source_hashes_and_scope_identity():
    root = Path(__file__).parents[2] / "fixtures/fotmob/six_calendars_unavailable"
    records = json.loads((root / "provenance.json").read_text())
    assert {(r["cid"], r["season"]) for r in records} == set(SIX_CALENDARS)
    for record in records:
        body = (root / f"league_{record['cid']}.json").read_bytes()
        assert hashlib.sha256(body).hexdigest() == record["record"]["content_hash"]
        details = json.loads(body)["details"]
        assert (details["id"], details["selectedSeason"]) == (record["cid"], record["season"])


@pytest.mark.parametrize("variant", ["id", "season"])
@pytest.mark.parametrize("tabs", [[], ["seasons"]])
def test_unavailable_identity_mismatch_remains_drift(variant, tabs):
    payload = unavailable()
    payload["tabs"] = tabs
    season = payload["details"]["selectedSeason"]
    payload["details"]["id" if variant == "id" else "selectedSeason"] = 999 if variant == "id" else "2025/2026"
    url = canonicalize_target("leagues", {"id": 10056, "season": season}).canonical_url
    service, _, repo = _service({url: payload})
    result, bundle = service.sync_season(10056, season)
    assert bundle is None and result.errors
    assert "calendar_unavailability" not in result.metadata
    assert repo.commits[-1].status == ManifestStatus.SCHEMA_DRIFT


@pytest.mark.parametrize("cid,season", SIX_CALENDARS)
@pytest.mark.parametrize("persist", [True, False])
@pytest.mark.parametrize("variant", ["200", "304", "prefetched"])
def test_retained_calendar_reports_evidence_without_changing_failure(cid, season, persist, variant):
    payload = retained_unavailable(cid)
    url = canonicalize_target("leagues", {"id": cid, "season": season}).canonical_url
    discovery_url = canonicalize_target("leagues", {"id": cid}).canonical_url
    service, transport, repo = _service({url: payload, discovery_url: payload})
    kwargs = {}
    if variant != "200":
        fetch = transport.fetch_json(discovery_url if variant == "prefetched" else url)
        if variant == "304":
            fetch = replace(fetch, outcome=FetchOutcome.NOT_MODIFIED, http_status=304, cache_hit=True)
        kwargs["prefetched"] = fetch
    method = service.sync_season if persist else service.read_season_dependency
    result, bundle = method(cid, season, **kwargs)
    fetch = kwargs.get("prefetched", transport._results[-1])
    evidence = result.metadata["calendar_unavailability"]
    assert evidence == {
        "reason": "source_calendar_unavailable", "url": url,
        "target_key": canonicalize_target(url).target_key,
        "content_hash": fetch.content_hash, "raw_uri": fetch.raw_uri,
        "fetched_at": fetch.fetched_at, "http_status": fetch.http_status,
        "cache_hit": variant != "200", "stale": False,
    }
    assert bundle is None and result.status == "retryable" and result.succeeded == 0
    assert not repo.tables.get("fotmob_matches")
    assert not any(c.target_type == "scope_completion" for c in repo.commits)
    assert not repo.latest_success(canonicalize_target(url).target_key)
    if persist:
        assert repo.commits[-1].status == ManifestStatus.RETRYABLE_FAILURE
    else:
        assert not repo.commits


def test_unavailable_evidence_does_not_invent_missing_provenance():
    payload = unavailable()
    season = payload["details"]["selectedSeason"]
    url = canonicalize_target("leagues", {"id": 10056, "season": season}).canonical_url
    service, transport, _ = _service({url: payload})
    fetch = replace(transport.fetch_json(url), raw_uri=None, content_hash=None, fetched_at=None)
    result, _ = service.sync_season(10056, season, prefetched=fetch)
    evidence = result.metadata["calendar_unavailability"]
    assert evidence["raw_uri"] is None and evidence["content_hash"] is None
    assert evidence["fetched_at"] is None


def test_discovery_accepts_only_season_metadata_without_a_calendar_bundle():
    payload = unavailable()
    comp = competition_from_league_payload(payload)
    url = canonicalize_target("leagues", {"id": comp.competition_id}).canonical_url
    service, _, repo = _service({url: payload})
    result = service.discover_competition(classify_competition(comp, comp))
    assert result.operation.ok, result.operation.errors
    assert len(result.seasons) == 1
    assert result.selected_bundle is None
    assert result.operation.metadata["calendar_available"] is False
    assert not repo.tables.get("fotmob_matches")
    assert repo.tables["fotmob_field_inventory"]
    assert not any(c.target_type == "scope_completion" for c in repo.commits)


@pytest.mark.parametrize("persist", [True, False])
@pytest.mark.parametrize("tabs", [[], ["seasons"]])
def test_unavailable_new_calendar_remains_retryable_without_typed_rows(persist, tabs):
    payload = unavailable()
    payload["tabs"] = tabs
    season = payload["details"]["selectedSeason"]
    url = canonicalize_target("leagues", {"id": 10056, "season": season}).canonical_url
    service, _, repo = _service({url: payload})
    method = service.sync_season if persist else service.read_season_dependency
    result, bundle = method(10056, season)
    assert bundle is None and result.succeeded == 0
    assert not result.errors and result.retryable
    assert "source_calendar_unavailable" in result.retryable[0]
    assert not result.metadata.get("infrastructure_failures")
    assert not repo.tables.get("fotmob_matches")
    assert not any(c.target_type == "scope_completion" for c in repo.commits)
    if persist:
        assert repo.commits[-1].status == ManifestStatus.RETRYABLE_FAILURE
        assert repo.commits[-1].error_code == "source_calendar_unavailable"
    else:
        assert not repo.commits


@pytest.mark.parametrize("tabs", [[], ["seasons"]])
def test_unavailable_stub_cannot_replace_thirty_known_matches(tabs):
    good = calendar()
    stub = unavailable()
    stub["tabs"] = tabs
    stub["details"].update(id=10618, selectedSeason="2026", latestSeason="2026")
    stub["allAvailableSeasons"] = ["2026"]
    url = canonicalize_target("leagues", {"id": 10618, "season": "2026"}).canonical_url
    service, _, repo = _service({url: [good, stub]})
    assert service.sync_season(10618, "2026")[0].succeeded == 1
    result, bundle = service.sync_season(10618, "2026")
    assert bundle is None and result.errors
    assert repo.commits[-1].status == ManifestStatus.SCHEMA_DRIFT
    assert len(repo.tables["fotmob_matches"]) == 30
    assert "calendar_unavailability" not in result.metadata


@pytest.mark.parametrize("variant", ["unknown", "null", "object", "extra_fixture", "tabs", "false_number"])
@pytest.mark.parametrize("tabs", [[], ["seasons"]])
def test_malformed_or_unclassified_page_does_not_use_availability_policy(variant, tabs):
    payload = unavailable()
    payload["tabs"] = tabs
    if variant == "unknown":
        payload["newRoot"] = 1
    elif variant == "null":
        payload["fixtures"]["allMatches"] = None
    elif variant == "object":
        payload["fixtures"]["allMatches"] = {}
    elif variant == "extra_fixture":
        payload["fixtures"]["data"] = {}
    elif variant == "tabs":
        payload["tabs"] = ["fixtures"]
    else:
        payload["fixtures"]["hasOngoingMatch"] = 0
    season = payload["details"]["selectedSeason"]
    url = canonicalize_target("leagues", {"id": 10056, "season": season}).canonical_url
    service, _, repo = _service({url: payload})
    result, bundle = service.sync_season(10056, season)
    assert result.errors and bundle is None
    assert repo.commits[-1].status == ManifestStatus.SCHEMA_DRIFT
    assert not repo.tables.get("fotmob_matches")
    assert "calendar_unavailability" not in result.metadata


@pytest.mark.parametrize("tabs", [[], ["seasons"]])
def test_discovery_of_unavailable_calendar_still_rejects_unknown_paths(tabs):
    payload = unavailable()
    payload["tabs"] = tabs
    payload["newRoot"] = 1
    comp = competition_from_league_payload(payload)
    url = canonicalize_target("leagues", {"id": comp.competition_id}).canonical_url
    service, _, repo = _service({url: payload})
    result = service.discover_competition(classify_competition(comp, comp))
    assert result.operation.errors and not result.seasons
    assert not repo.tables.get("fotmob_competition_seasons")


@pytest.mark.parametrize("tabs", [[], ["seasons"]])
def test_unknown_history_cannot_authorize_source_absence(tabs):
    payload = unavailable()
    payload["tabs"] = tabs
    season = payload["details"]["selectedSeason"]
    url = canonicalize_target("leagues", {"id": 10056, "season": season}).canonical_url
    service, _, repo = _service({url: payload})
    repo.has_committed_matches = Mock(side_effect=OSError("history unavailable"))
    result, bundle = service.sync_season(10056, season)
    assert bundle is None and result.retryable and not result.errors
    assert not repo.commits
    assert not repo.tables.get("fotmob_matches")
    assert "calendar_unavailability" not in result.metadata


@pytest.mark.parametrize("tabs", [[], ["seasons"]])
def test_replay_keeps_strict_calendar_failure(tabs):
    payload = unavailable()
    payload["tabs"] = tabs
    season = payload["details"]["selectedSeason"]
    url = canonicalize_target("leagues", {"id": 10056, "season": season}).canonical_url
    service, _, repo = _service({url: payload}, mode=RunMode.REPLAY)
    result, bundle = service.sync_season(10056, season)
    assert bundle is None and result.errors and not result.retryable
    assert repo.commits[-1].status == ManifestStatus.SCHEMA_DRIFT
    assert "calendar_unavailability" not in result.metadata


@pytest.mark.parametrize("healthy", [False, True])
def test_runner_does_not_count_unavailable_calendar_as_scope_progress(monkeypatch, healthy):
    from datetime import datetime, timedelta, timezone
    from tests.unit.scrapers.test_run_fotmob_scraper import TestFotmobNativeRunner, _run_native_admitted

    mod = TestFotmobNativeRunner._module()
    monkeypatch.setenv(mod.WRITER_LOCK_ENV, "0")
    bodies = {10056: unavailable()}
    if healthy:
        bodies[47] = copy.deepcopy(_league_payload())
    responses = {canonicalize_target("allLeagues").canonical_url: {
        "countries": [{"leagues": [{"id": cid, "name": body["details"]["name"]} for cid, body in bodies.items()]}]
    }}
    for cid, body in bodies.items():
        responses[canonicalize_target("leagues", {"id": cid}).canonical_url] = body
        responses[canonicalize_target("leagues", {"id": cid, "season": body["details"]["selectedSeason"]}).canonical_url] = body
        responses[canonicalize_target("transfers", {"leagueIds": str(cid), "page": 1, "last": "1year"}).canonical_url] = {"hits": 0, "transfers": []}
    svc, _, repo = _service(responses)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    states = {(10056, bodies[10056]["details"]["selectedSeason"]): ScopeAttemptState(
        10056, bodies[10056]["details"]["selectedSeason"], "availability-test", 1,
        now-timedelta(hours=1), now-timedelta(hours=1), "success", "legacy zero calendar")}
    repo.scope_attempt_states = lambda *args, **kwargs: states
    args = mod._argument_parser().parse_args(["--mode", "refresh", "--catalog-contract", "fotmob-catalog-v1", "--entities", "season,transfers"])
    rc, report = _run_native_admitted(mod, args, service=svc)
    completed = report["selection"]["completed_scopes"]
    assert not any(scope.startswith("10056=") for scope in completed)
    assert len(completed) == int(healthy)
    assert rc == (0 if healthy else 1)
    assert report["status"] == ("partial_success" if healthy else "incomplete")
    assert report["selection"]["completed_transfer_competition_ids"]
    assert any("source_calendar_unavailable" in error for error in report["errors"])


@pytest.mark.parametrize("healthy", [False, True])
def test_runner_lists_all_six_unavailable_scopes_and_keeps_retry_backoff(monkeypatch, healthy):
    from datetime import datetime, timedelta, timezone
    from tests.unit.scrapers.test_run_fotmob_scraper import TestFotmobNativeRunner, _run_native_admitted

    mod = TestFotmobNativeRunner._module()
    monkeypatch.setenv(mod.WRITER_LOCK_ENV, "0")
    bodies = {cid: retained_unavailable(cid) for cid, _ in SIX_CALENDARS}
    if healthy:
        bodies[47] = copy.deepcopy(_league_payload())
    responses = {canonicalize_target("allLeagues").canonical_url: {
        "countries": [{"leagues": [{"id": cid, "name": body["details"]["name"]} for cid, body in bodies.items()]}]
    }}
    for cid, body in bodies.items():
        responses[canonicalize_target("leagues", {"id": cid}).canonical_url] = body
        responses[canonicalize_target("leagues", {"id": cid, "season": body["details"]["selectedSeason"]}).canonical_url] = body
        responses[canonicalize_target("transfers", {"leagueIds": str(cid), "page": 1, "last": "1year"}).canonical_url] = {"hits": 0, "transfers": []}
    svc, _, repo = _service(responses)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    states = {(cid, season): ScopeAttemptState(
        cid, season, "availability-test", 3, now-timedelta(days=1), now-timedelta(hours=1),
        "retryable", "legacy incomplete reason", ("old-1", "old-2", "old-3"),
    ) for cid, season in SIX_CALENDARS}
    repo.scope_attempt_states = lambda *args, **kwargs: states
    args = mod._argument_parser().parse_args(["--mode", "refresh", "--catalog-contract", "fotmob-catalog-v1", "--entities", "season,transfers"])
    rc, report = _run_native_admitted(mod, args, service=svc)
    selection = report["selection"]
    missing = sorted(f"{cid}={season}" for cid, season in SIX_CALENDARS)
    assert selection["calendar_unavailable_scopes"] == missing
    assert set(missing) <= set(selection["planned_scopes"])
    assert set(missing).isdisjoint(selection["completed_scopes"])
    assert report["complete"] is False
    assert rc == (0 if healthy else 1)
    assert report["status"] == ("partial_success" if healthy else "incomplete")
    assert "calendar_unavailable=6" in mod._wave_metrics_line(report, rc)
    assert len(report["errors"]) == 18
    for attempt in selection["scope_attempts"]:
        if attempt["competition_id"] == 47:
            continue
        assert attempt["outcome"] == "retryable"
        assert attempt["reason"] == "source_calendar_unavailable"
        assert attempt["attempt_count"] == 4
        assert len(attempt["attempt_identities"]) == 4
        retry = datetime.fromisoformat(attempt["next_retry_at"])
        last = datetime.fromisoformat(attempt["last_attempt_at"])
        assert abs((retry-last).total_seconds()-86400) < 5

    # The next response supplies a real calendar; prior diagnostics do not leak.
    for cid, season in SIX_CALENDARS:
        good = copy.deepcopy(_league_payload(season))
        good["details"].update(id=cid, latestSeason=season)
        good["allAvailableSeasons"] = [season]
        responses[canonicalize_target("leagues", {"id": cid}).canonical_url] = good
        responses[canonicalize_target("leagues", {"id": cid, "season": season}).canonical_url] = good
    next_service, _, _ = _service(responses)
    next_rc, next_report = _run_native_admitted(mod, args, service=next_service)
    assert next_rc == 0 and next_report["complete"] is True
    assert next_report["selection"]["calendar_unavailable_scopes"] == []
    assert set(missing) <= set(next_report["selection"]["completed_scopes"])
    assert "calendar_unavailable=0" in mod._wave_metrics_line(next_report, next_rc)

    # Discovery still sees all six stubs, but their retries are not due.
    # A healthy unrelated scope cannot be reported as calendar recovery.
    for cid, season in SIX_CALENDARS:
        responses[canonicalize_target("leagues", {"id": cid}).canonical_url] = bodies[cid]
        responses[canonicalize_target("leagues", {"id": cid, "season": season}).canonical_url] = bodies[cid]
    cooling_service, _, cooling_repo = _service(responses)
    cooling_repo.scope_attempt_states = lambda *args, **kwargs: {
        key: replace(value, next_retry_at=now+timedelta(days=1)) for key, value in states.items()
    }
    _, cooling_report = _run_native_admitted(mod, args, service=cooling_service)
    assert cooling_report["selection"]["calendar_unavailable_scopes"] == []
    assert set(missing).isdisjoint(cooling_report["selection"]["planned_scopes"])
    assert set(missing).isdisjoint(cooling_report["selection"]["completed_scopes"])
    work_plan = next(o for o in cooling_report["operations"] if o["entity"] == "season_work_plan")
    assert work_plan["metadata"]["obligation_scopes"] == 6 + int(healthy)
