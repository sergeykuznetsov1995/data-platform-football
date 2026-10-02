"""Regression for C1 match 5991933: a transient write failure poisoned its scope."""
from datetime import datetime, timedelta
from copy import deepcopy

import pytest
from trino.exceptions import TrinoConnectionError

from scrapers.fotmob.planner import RunMode, ScopeAttemptState, TransportBudget
from scrapers.fotmob.repository import ManifestStatus, MemoryFotMobRepository
from scrapers.fotmob.service import FotMobIngestService
from scrapers.fotmob.transport import canonicalize_target
from tests.unit.scrapers.test_fotmob_service import StubTransport, _league_payload
from tests.unit.scrapers.test_run_fotmob_scraper import (
    TestFotmobNativeRunner as RunnerHarness, _run_native_admitted,
)


def _responses():
    return {
        canonicalize_target("allLeagues").canonical_url: {
            "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}]},
        canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
        canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url: {
            "content": {"matchFacts": {"events": []}, "stats": {"x": 1}}},
    }


@pytest.mark.parametrize("failure_site, failure_kind", [
    ("match", "infra"), ("completion", "infra"),
    ("match", "schema"), ("completion", "schema"), ("match", "mixed"), ("match", "drift-write"),
])
def test_write_failure_scope_classification_and_recovery(monkeypatch, failure_site, failure_kind):
    mod = RunnerHarness._module()
    infra = failure_kind != "schema"
    retryable_scope = failure_kind == "infra"
    failure = TrinoConnectionError("Connection refused") if infra else ValueError("bad shape")

    class InterruptedRepository(MemoryFotMobRepository):
        broken = True

        def scope_attempt_states(self, signature):
            states = super().scope_attempt_states(signature)
            if states:
                return states
            # Production attempt_count was 37: lifetime successes must not make
            # the first transient failure wait for the 24-hour backoff tier.
            return {(47, "2025/2026"): ScopeAttemptState(
                47, "2025/2026", signature, 36,
                datetime(2025, 1, 1), None, "success", "scope completion committed")}

        def commit(self, commit, datasets=()):
            if (self.broken and failure_site == "match" and commit.target_type == "match"
                    and commit.status == (ManifestStatus.SCHEMA_DRIFT
                        if failure_kind == "drift-write" else ManifestStatus.SUCCESS)):
                if str(commit.entity_id) == "101":
                    raise ValueError("bad shape alongside infrastructure failure")
                raise failure
            return super().commit(commit, datasets)

    repository = InterruptedRepository()

    class InterruptedService(FotMobIngestService):
        def record_scope_completion(self, *args, **kwargs):
            if repository.broken and failure_site == "completion":
                raise failure
            return super().record_scope_completion(*args, **kwargs)

    def run(run_id):
        responses = _responses()
        if failure_kind == "mixed":
            league = responses[canonicalize_target("leagues", {"id": 47}).canonical_url]
            another = deepcopy(league["fixtures"]["allMatches"][0])
            another["id"] = 101
            another["pageUrl"] = "/matches/alpha-vs-beta/x#101"
            league["fixtures"]["allMatches"].append(another)
            responses[canonicalize_target("matchDetails", {"matchId": "101"}).canonical_url] = {
                "content": {"matchFacts": {"events": []}, "stats": {"x": 1}}}
        if failure_kind == "drift-write":
            responses[canonicalize_target("matchDetails", {"matchId": "100"}).canonical_url] = {
                "error": True, "message": "Internal error", "matchId": "100"}
        service = InterruptedService(
            transport=StubTransport(responses), repository=repository,
            mode=RunMode.DAILY, budget=TransportBudget(100, 10_000_000),
            run_id=run_id, max_workers=1)
        args = mod._argument_parser().parse_args([
            "--mode", "refresh", "--catalog-contract", "fotmob-catalog-v1",
            "--entities", "season,matches", "--run-id", run_id])
        return _run_native_admitted(mod, args, service=service)

    rc, report = run("infra-regression-1")
    assert rc == 1 and report["status"] == "incomplete", report
    attempt = report["selection"]["scope_attempts"][0]
    assert attempt["outcome"] == ("retryable" if retryable_scope else "terminal")
    if failure_site == "match":
        manifest = [c for c in repository.commits if c.target_type == "match" and str(c.entity_id) == "100"][-1]
        assert manifest.status == (ManifestStatus.RETRYABLE_FAILURE if infra
                                   else ManifestStatus.SCHEMA_DRIFT)
    if not retryable_scope:
        assert attempt["next_retry_at"] is None
        return
    retry_at = datetime.fromisoformat(attempt["next_retry_at"])
    last_at = datetime.fromisoformat(attempt["last_attempt_at"])
    assert timedelta(0) < retry_at - last_at <= timedelta(minutes=15)
    assert attempt["attempt_count"] == 37
    # Run the actual planner before and after eligibility, with the same
    # durable repository, not a mocked operation result.
    repository.broken = False
    _, early = run("infra-regression-too-early")
    assert not early["selection"]["planned_scopes"]
    assert not early["selection"]["scope_attempts"]
    real_datetime = datetime

    class LaterDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime.now(tz) + timedelta(minutes=16)

    from scrapers.fotmob import planner
    monkeypatch.setattr(planner, "datetime", LaterDatetime)
    monkeypatch.setattr(mod, "datetime", LaterDatetime)
    rc, recovered = run("infra-regression-2")
    assert rc == 0, recovered["errors"]
    assert recovered["selection"]["scope_attempts"][0]["outcome"] == "success"
    assert repository.tables["fotmob_match_payloads"]


def test_missing_raw_diagnostic_remains_compatible_with_replay_proof():
    from tests.unit.scrapers.test_fotmob_service import _service

    service, _, _ = _service({}, mode=RunMode.REPLAY)
    result = service.sync_player_snapshots([123], capture_terminal_outcomes=True)
    assert RunnerHarness._module()._player_replay_missing_raw_ids(result) == (123,)


@pytest.mark.parametrize("infra", [True, False])
def test_leaderboard_fetch_exception_keeps_failure_classification(monkeypatch, infra):
    from scrapers.fotmob.domain import ScopeRef
    from scrapers.fotmob.parsers import parse_season_bundle
    from tests.unit.scrapers.test_fotmob_service import _service

    service, _, _ = _service({})
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))

    def fail_fetch(*args, **kwargs):
        if infra:
            raise TrinoConnectionError("Connection refused")
        raise ValueError("invalid target")

    monkeypatch.setattr(service, "_fetch", fail_fetch)
    result = service.sync_leaderboards(bundle)
    assert not result.ok
    assert bool(result.retryable) is infra
    assert bool(result.errors) is not infra
    assert bool(result.metadata.get("infrastructure_failures")) is infra
