"""A retained source stub is unavailable, never an empty accepted calendar."""
import copy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from scrapers.fotmob.catalog import classify_competition, competition_from_league_payload
from scrapers.fotmob.planner import RunMode, ScopeAttemptState
from scrapers.fotmob.repository import ManifestStatus
from scrapers.fotmob.transport import FetchOutcome, canonicalize_target
from tests.unit.scrapers.test_fotmob_calendar_contract import calendar
from tests.unit.scrapers.test_fotmob_service import _league_payload, _service


def unavailable():
    path = Path(__file__).parents[2] / "fixtures/fotmob/league_10056_calendar_unavailable.json"
    return json.loads(path.read_text())


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
def test_unavailable_identity_mismatch_remains_drift(variant):
    payload = unavailable()
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
def test_unavailable_new_calendar_remains_retryable_without_typed_rows(persist):
    payload = unavailable()
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


def test_unavailable_stub_cannot_replace_thirty_known_matches():
    good = calendar()
    stub = unavailable()
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
def test_malformed_or_unclassified_page_does_not_use_availability_policy(variant):
    payload = unavailable()
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


def test_discovery_of_unavailable_calendar_still_rejects_unknown_paths():
    payload = unavailable()
    payload["newRoot"] = 1
    comp = competition_from_league_payload(payload)
    url = canonicalize_target("leagues", {"id": comp.competition_id}).canonical_url
    service, _, repo = _service({url: payload})
    result = service.discover_competition(classify_competition(comp, comp))
    assert result.operation.errors and not result.seasons
    assert not repo.tables.get("fotmob_competition_seasons")


def test_unknown_history_cannot_authorize_source_absence():
    payload = unavailable()
    season = payload["details"]["selectedSeason"]
    url = canonicalize_target("leagues", {"id": 10056, "season": season}).canonical_url
    service, _, repo = _service({url: payload})
    repo.has_committed_matches = Mock(side_effect=OSError("history unavailable"))
    result, bundle = service.sync_season(10056, season)
    assert bundle is None and result.retryable and not result.errors
    assert not repo.commits
    assert not repo.tables.get("fotmob_matches")
    assert "calendar_unavailability" not in result.metadata


def test_replay_keeps_strict_calendar_failure():
    payload = unavailable()
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
