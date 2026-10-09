"""A retained source stub is unavailable, never an empty accepted calendar."""
import copy
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from scrapers.fotmob.catalog import classify_competition, competition_from_league_payload
from scrapers.fotmob.planner import RunMode, ScopeAttemptState
from scrapers.fotmob.repository import ManifestStatus
from scrapers.fotmob.transport import canonicalize_target
from tests.unit.scrapers.test_fotmob_calendar_contract import calendar
from tests.unit.scrapers.test_fotmob_service import _league_payload, _service


def unavailable():
    path = Path(__file__).parents[2] / "fixtures/fotmob/league_10056_calendar_unavailable.json"
    return json.loads(path.read_text())


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


def test_replay_keeps_strict_calendar_failure():
    payload = unavailable()
    season = payload["details"]["selectedSeason"]
    url = canonicalize_target("leagues", {"id": 10056, "season": season}).canonical_url
    service, _, repo = _service({url: payload}, mode=RunMode.REPLAY)
    result, bundle = service.sync_season(10056, season)
    assert bundle is None and result.errors and not result.retryable
    assert repo.commits[-1].status == ManifestStatus.SCHEMA_DRIFT


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
