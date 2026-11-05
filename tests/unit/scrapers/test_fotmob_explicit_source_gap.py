"""Explicit history campaigns retain honest, raw-backed source-gap completion.

Counts/scope identities reproduce report_4048 and report_4051. Those reports do
not contain raw match IDs; every match ID below is deliberately synthetic.
"""

import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
import requests

from scrapers.fotmob.planner import RunMode, TransportBudget
from scrapers.fotmob.repository import (
    FotMobRepository, ManifestStatus, MemoryFotMobRepository, PARSER_VERSION,
)
from scrapers.fotmob.service import FotMobIngestService, OperationResult
from scrapers.fotmob.transport import FetchOutcome, canonicalize_target
from tests.unit.scrapers.test_fotmob_service import StubTransport, _league_payload
from tests.unit.scrapers.test_run_fotmob_scraper import (
    TestFotmobNativeRunner as RunnerHarness,
    _run_native_admitted,
)


class ExplicitHistory:
    def __init__(self, monkeypatch, cid=9845, season="2017/2018", total=2, missing=1):
        self.mod = RunnerHarness._module()
        monkeypatch.setenv(self.mod.WRITER_LOCK_ENV, "0")
        self.cid, self.season = cid, season
        self.scope = f"{cid}={season}"
        self.repo = MemoryFotMobRepository()
        self.ids = [900000 + index for index in range(total)]
        self.missing_ids = self.ids[:missing]
        league = copy.deepcopy(_league_payload(season))
        league["details"].update(id=cid, latestSeason=season)
        league["allAvailableSeasons"] = [season]
        template = league["fixtures"]["allMatches"][0]
        league["fixtures"]["allMatches"] = []
        for match_id in self.ids:
            match = copy.deepcopy(template)
            match.update(id=match_id, pageUrl=f"/matches/synthetic#{match_id}")
            match["status"]["utcTime"] = "2017-01-01T12:00:00.000Z"
            league["fixtures"]["allMatches"].append(match)
        self.league = league
        self.responses = {
            canonicalize_target("allLeagues").canonical_url: {
                "countries": [{"leagues": [{"id": cid, "name": "Synthetic league"}]}]
            },
            canonicalize_target("leagues", {"id": cid}).canonical_url: league,
            canonicalize_target("leagues", {"id": cid, "season": season}).canonical_url: league,
        }
        for match_id in self.ids:
            self.responses[self.url(match_id)] = (
                {"error": True, "message": "Data not found", "matchId": str(match_id)}
                if match_id in self.missing_ids else self.details(match_id)
            )
        self.runs = 0

    @staticmethod
    def url(match_id):
        return canonicalize_target("matchDetails", {"matchId": str(match_id)}).canonical_url

    @staticmethod
    def details(match_id):
        return {"general": {"matchId": str(match_id)}, "content": {"matchFacts": {"events": []}}}

    def service(self, *, max_requests=100):
        self.runs += 1
        transport = StubTransport(self.responses)
        service = FotMobIngestService(
            transport=transport, repository=self.repo, mode=RunMode.BACKFILL,
            budget=TransportBudget(max_requests=max_requests, max_direct_bytes=10_000_000),
            run_id=f"explicit-history-{self.runs}", max_workers=2,
        )
        return service, transport

    def run(self, *, extra=(), service=None):
        if service is None:
            service, transport = self.service()
        else:
            transport = service.transport
        args = self.mod._argument_parser().parse_args([
            "--mode", "backfill", "--scope", self.scope,
            "--entities", "season,matches", "--run-id", service.run_id, *extra,
        ])
        rc, report = _run_native_admitted(self.mod, args, service=service)
        return rc, report, transport


def _gap_evidence(report):
    operations = [op for op in report["operations"] if op["entity"] == "scope_source_gap"]
    assert len(operations) == 1
    return operations[0]["metadata"]["source_gap_matches"]


@pytest.mark.parametrize("cid,season,total,missing", [(9845, "2017/2018", 2, 1), (9942, "2019", 31, 22)])
def test_explicit_old_source_gap_completes_first_attempt_and_survives_restart(
    monkeypatch, cid, season, total, missing,
):
    history = ExplicitHistory(monkeypatch, cid, season, total, missing)
    rc, first, first_transport = history.run()
    assert rc == 0, first["errors"]
    assert first["complete"] is True
    assert first["selection"]["completed_scopes"] == [history.scope]
    assert first["selection"]["source_gap_scopes"] == [history.scope]
    assert {url for url, _ in first_transport.calls if "/matchDetails?" in url} == {
        history.url(match_id) for match_id in history.ids
    }
    assert len(history.repo.tables["fotmob_match_payloads"]) == total - missing
    evidence = _gap_evidence(first)
    assert {str(item["match_id"]) for item in evidence} == set(map(str, history.missing_ids))
    for item in evidence:
        target = canonicalize_target(history.url(item["match_id"]))
        fetch = next(fetch for fetch in first_transport._results if fetch.target_key == target.target_key)
        assert item["target_key"] == target.target_key
        assert item["raw_uri"] == fetch.raw_uri
        assert item["content_hash"] == fetch.content_hash
        assert item["run_id"] == "explicit-history-1"
        assert item["reason"] == "source_data_not_found"
        assert item["observed_at"] == (fetch.source_observed_at or fetch.validated_at or fetch.fetched_at)
    markers = [commit for commit in history.repo.commits if commit.target_type == "scope_source_gap"]
    assert len(markers) == 1 and markers[0].status == ManifestStatus.NOT_AVAILABLE
    assert markers[0].capabilities["source_gap_matches"] == evidence
    assert not any(commit.target_type == "scope_completion" for commit in history.repo.commits)

    rc, second, restarted_transport = history.run()
    assert rc == 0 and second["complete"] is True
    assert second["selection"]["completed_scopes"] == [history.scope]
    assert second["selection"]["source_gap_scopes"] == [history.scope]
    assert _gap_evidence(second) == evidence
    assert not any("/matchDetails?" in url for url, _ in restarted_transport.calls)
    assert len([commit for commit in history.repo.commits if commit.target_type == "scope_source_gap"]) == 1


def _assert_pending(history, rc, report):
    assert rc == 1, report["errors"]
    assert report["complete"] is False
    assert history.scope not in report["selection"]["completed_scopes"]
    assert history.scope not in report["selection"].get("source_gap_scopes", [])
    assert not any(commit.target_type == "scope_completion" for commit in history.repo.commits)


@pytest.mark.parametrize("failure", ["http503", "timeout", "wrong_id", "unknown_key"])
def test_only_strict_source_absence_can_complete_explicit_history(monkeypatch, failure):
    history = ExplicitHistory(monkeypatch)
    service, transport = history.service()
    missing_url = history.url(history.missing_ids[0])
    if failure == "timeout":
        fetch_json = transport.fetch_json

        def fetch(endpoint, params=None, **kwargs):
            if canonicalize_target(endpoint, params).canonical_url == missing_url:
                raise requests.Timeout("synthetic timeout")
            return fetch_json(endpoint, params, **kwargs)

        monkeypatch.setattr(transport, "fetch_json", fetch)
    elif failure == "http503":
        successful = transport.fetch_json(missing_url)
        transport.calls.clear()
        transport._results.clear()
        history.responses[missing_url] = replace(
            successful, outcome=FetchOutcome.RETRYABLE_FAILURE, http_status=503,
            json_data=None, error="HTTP 503", body=None,
        )
    elif failure == "wrong_id":
        history.responses[missing_url]["matchId"] = "999999"
    else:
        history.responses[missing_url]["unexpected"] = True
    rc, report, _ = history.run(service=service)
    _assert_pending(history, rc, report)
    assert not any(commit.target_type == "scope_source_gap" for commit in history.repo.commits)


@pytest.mark.parametrize("limit", ["match", "requests"])
def test_unobserved_matches_keep_scope_pending(monkeypatch, limit):
    history = ExplicitHistory(monkeypatch, total=3, missing=3)
    service, transport = history.service(max_requests=3 if limit == "requests" else 100)
    rc, report, _ = history.run(
        service=service, extra=("--match-limit", "1") if limit == "match" else (),
    )
    _assert_pending(history, rc, report)
    assert len([url for url, _ in transport.calls if "/matchDetails?" in url]) == 1
    assert not any(commit.target_type == "scope_source_gap" for commit in history.repo.commits)


def test_failure_of_another_requested_entity_keeps_source_gap_pending(monkeypatch):
    history = ExplicitHistory(monkeypatch)
    service, _ = history.service()
    service.sync_leaderboards = Mock(return_value=OperationResult(
        "leaderboards", attempted=1, errors=["synthetic leaderboard failure"],
    ))
    rc, report, _ = history.run(service=service, extra=("--entities", "season,matches,leaderboards"))
    _assert_pending(history, rc, report)
    service.sync_leaderboards.assert_called_once()
    assert not any(commit.target_type == "scope_source_gap" for commit in history.repo.commits)


@pytest.mark.parametrize("field", ["raw_uri", "content_hash", "fetched_at"])
def test_missing_raw_proof_cannot_complete_scope(monkeypatch, field):
    history = ExplicitHistory(monkeypatch)
    service, transport = history.service()
    url = history.url(history.missing_ids[0])
    missing = {field: None}
    if field == "fetched_at":
        missing.update(validated_at=None, source_observed_at=None)
    history.responses[url] = replace(transport.fetch_json(url), **missing)
    transport.calls.clear()
    transport._results.clear()
    rc, report, _ = history.run(service=service)
    _assert_pending(history, rc, report)
    assert not any(commit.target_type == "scope_source_gap" for commit in history.repo.commits)


@pytest.mark.parametrize("failure", ["commit", "flush"])
def test_failed_marker_publication_cannot_add_completed_scope(monkeypatch, failure):
    history = ExplicitHistory(monkeypatch)
    if failure == "commit":
        commit = history.repo.commit

        def fail_gap_marker(target, datasets=()):
            if target.target_type == "scope_source_gap":
                raise RuntimeError("synthetic marker commit failure")
            return commit(target, datasets)

        monkeypatch.setattr(history.repo, "commit", fail_gap_marker)
    else:
        def fail_gap_flush():
            if any(commit.target_type == "scope_source_gap" for commit in history.repo.commits):
                raise RuntimeError("synthetic flush failure")
            return []

        monkeypatch.setattr(history.repo, "flush", fail_gap_flush)
    rc, report, _ = history.run()
    _assert_pending(history, rc, report)
    assert any("synthetic" in error for error in report["errors"])


@pytest.mark.parametrize("age_hours", [24, 71])
def test_fresh_finished_missing_match_keeps_existing_confirmation_policy(monkeypatch, age_hours):
    history = ExplicitHistory(monkeypatch, total=1, missing=1)
    kickoff = datetime.now(timezone.utc) - timedelta(hours=age_hours)
    history.league["fixtures"]["allMatches"][0]["status"]["utcTime"] = kickoff.isoformat()
    service, transport = history.service()
    url = history.url(history.ids[0])
    history.responses[url] = replace(transport.fetch_json(url), fetched_at=datetime.now(timezone.utc).isoformat())
    transport.calls.clear()
    transport._results.clear()
    rc, report, _ = history.run(service=service)
    _assert_pending(history, rc, report)
    assert not any(commit.target_type == "scope_source_gap" for commit in history.repo.commits)


@pytest.mark.parametrize("recovered", [False, True])
def test_expired_source_gap_is_rechecked_and_can_recover(monkeypatch, recovered):
    history = ExplicitHistory(monkeypatch)
    rc, first, _ = history.run()
    assert rc == 0 and first["complete"] is True
    old_evidence = _gap_evidence(first)
    expired_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=31)
    history.repo.commits[:] = [
        replace(commit, completed_at=expired_at)
        if commit.target_type == "scope_source_gap" else commit
        for commit in history.repo.commits
    ]
    if recovered:
        for match_id in history.missing_ids:
            history.responses[history.url(match_id)] = history.details(match_id)
    rc, second, transport = history.run()
    assert rc == 0, second["errors"]
    assert second["complete"] is True
    assert second["selection"]["completed_scopes"] == [history.scope]
    assert set(history.url(match_id) for match_id in history.missing_ids) <= {
        url for url, _ in transport.calls
    }
    if recovered:
        assert second["selection"]["source_gap_scopes"] == []
        assert not any(op["entity"] == "scope_source_gap" for op in second["operations"])
        assert any(commit.target_type == "scope_completion" and commit.status == ManifestStatus.SUCCESS
                   for commit in history.repo.commits)
    else:
        assert second["selection"]["source_gap_scopes"] == [history.scope]
        assert {str(item["match_id"]) for item in _gap_evidence(second)} == {
            str(item["match_id"]) for item in old_evidence
        }
        assert {item["run_id"] for item in _gap_evidence(second)} == {"explicit-history-2"}


def test_fresh_gap_eventually_confirms_without_fabricated_run_identities(monkeypatch):
    history = ExplicitHistory(monkeypatch, total=1, missing=1)
    history.league["fixtures"]["allMatches"][0]["status"]["utcTime"] = (
        datetime.now(timezone.utc) - timedelta(hours=24)
    ).isoformat()
    for _ in range(2):
        rc, report, _ = history.run()
        _assert_pending(history, rc, report)
    rc, report, _ = history.run()
    assert rc == 0, report["errors"]
    assert report["selection"]["source_gap_scopes"] == [history.scope]
    assert _gap_evidence(report)[0]["run_id"] == "explicit-history-3"


def test_evidence_flush_must_succeed_before_gap_marker_exists(monkeypatch):
    history = ExplicitHistory(monkeypatch)

    def fail_dependency_flush():
        if any(commit.target_type == "match" for commit in history.repo.commits):
            raise OSError("synthetic dependency flush failure")
        return []

    monkeypatch.setattr(history.repo, "flush", fail_dependency_flush)
    rc, report, _ = history.run()
    _assert_pending(history, rc, report)
    assert not any(commit.target_type == "scope_source_gap" for commit in history.repo.commits)


def test_unverified_skips_cannot_fill_the_remaining_match_coverage(monkeypatch):
    history = ExplicitHistory(monkeypatch, total=3, missing=1)
    service, _ = history.service()
    sync = service.sync_match_payloads

    def partial_with_fake_skips(bundle, **kwargs):
        result = sync(bundle, **{**kwargs, "limit": 1})
        if kwargs.get("phase") == "primary":
            result.skipped += 2
        return result

    monkeypatch.setattr(service, "sync_match_payloads", partial_with_fake_skips)
    rc, report, _ = history.run(service=service)
    _assert_pending(history, rc, report)
    assert not any(commit.target_type == "scope_source_gap" for commit in history.repo.commits)


def test_stale_absence_cannot_close_explicit_scope(monkeypatch):
    history = ExplicitHistory(monkeypatch)
    service, transport = history.service()
    url = history.url(history.missing_ids[0])
    history.responses[url] = replace(transport.fetch_json(url), stale=True)
    rc, report, _ = history.run(service=service)
    _assert_pending(history, rc, report)


def test_trino_and_buffered_gap_records_match_memory_and_filter_identity(monkeypatch):
    history = ExplicitHistory(monkeypatch)
    rc, report, _ = history.run()
    assert rc == 0
    marker = next(commit for commit in history.repo.commits if commit.target_type == "scope_source_gap")
    signature = report["selection"]["scope_plan_signature"]
    columns = (
        "competition_id", "source_season_key", "entity_id", "parser_version",
        "target_type", "status", "capabilities_json", "completed_at", "run_id",
    )
    valid = marker.manifest_row()
    rows = [valid, *[
        {**valid, key: value} for key, value in (
            ("entity_id", "different-plan"), ("parser_version", "old-parser"),
            ("target_type", "scope_completion"), ("status", "success"),
            ("capabilities_json", "not-json"),
        )
    ]]
    trino = Mock()
    trino.execute_query.return_value = [tuple(row.get(column) for column in columns) for row in rows]
    writer = Mock()
    writer._get_trino_manager.return_value = trino
    repository = FotMobRepository(writer=writer, batch_size=100)
    assert repository.source_gap_scope_records(signature) == history.repo.source_gap_scope_records(signature)
    query = trino.execute_query.call_args.args[0]
    assert "target_type = 'scope_source_gap'" in query
    assert "status = 'not_available'" in query
    assert PARSER_VERSION in query
    trino.execute_query.return_value = []
    repository._pending_manifest.append(valid)
    assert repository.source_gap_scope_records(signature) == history.repo.source_gap_scope_records(signature)
    assert history.repo.completed_scope_keys(signature) == set()


def test_partial_history_retry_spends_limit_on_unobserved_matches(monkeypatch):
    history = ExplicitHistory(monkeypatch, total=204, missing=194)
    service, _ = history.service(max_requests=400)
    rc, first, _ = history.run(service=service, extra=("--match-limit", "199"))
    _assert_pending(history, rc, first)
    original = [commit for commit in history.repo.commits if commit.target_type == "match"
                and commit.status == ManifestStatus.NOT_AVAILABLE]
    assert len(original) == 194
    rc, second, transport = history.run(extra=("--match-limit", "5"))
    assert rc == 0, second["errors"]
    assert second["complete"] is True
    assert {url for url, _ in transport.calls if "/matchDetails?" in url} == {
        history.url(match_id) for match_id in history.ids[-5:]
    }
    assert {proof["run_id"] for proof in _gap_evidence(second)} == {"explicit-history-1"}
    assert len(history.repo.tables["fotmob_match_payloads"]) == 10
    for match_id in history.missing_ids:
        target = canonicalize_target(history.url(match_id))
        assert history.repo.latest_match_source(target.target_key) is None


@pytest.mark.parametrize("invalid", [
    "expired", "future", "target_key", "raw_uri", "content_hash", "run_id",
    "stale", "failure", "young", "unknown_kickoff",
])
def test_partial_history_rechecks_invalid_or_young_absence(monkeypatch, invalid):
    history = ExplicitHistory(monkeypatch)
    rc, first, _ = history.run(extra=("--match-limit", "1"))
    _assert_pending(history, rc, first)
    changes = {
        "expired": {"completed_at": datetime.now(timezone.utc) - timedelta(days=31)},
        "future": {"completed_at": datetime.now(timezone.utc) + timedelta(days=1)},
        "target_key": {"target_key": "different-target"},
        "raw_uri": {"raw_uri": None},
        "content_hash": {"content_hash": None},
        "run_id": {"run_id": ""},
        "stale": {"stale": True},
        "failure": {"status": ManifestStatus.RETRYABLE_FAILURE},
    }
    if invalid in changes:
        history.repo.commits[:] = [
            replace(commit, **changes[invalid]) if commit.target_type == "match" else commit
            for commit in history.repo.commits
        ]
    else:
        history.league["fixtures"]["allMatches"][0]["status"]["utcTime"] = (
            (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            if invalid == "young" else None
        )
    if invalid == "unknown_kickoff":
        service, _ = history.service()
        assert service._match_source_gap_proof(
            {"match_id": history.missing_ids[0], "finished": True, "utc_time": None},
            datetime.now(timezone.utc).replace(tzinfo=None),
        ) is None
        return
    rc, second, transport = history.run(extra=("--match-limit", "1"))
    _assert_pending(history, rc, second)
    assert history.url(history.missing_ids[0]) in {url for url, _ in transport.calls}


@pytest.mark.parametrize("mode,reuse", [
    (RunMode.BACKFILL, False), (RunMode.DAILY, False), (RunMode.DAILY, True),
])
def test_absence_reuse_is_opt_in_and_history_only(monkeypatch, mode, reuse):
    history = ExplicitHistory(monkeypatch)
    history.run(extra=("--match-limit", "1"))
    service, transport = history.service()
    service.mode = mode
    _, bundle = service.sync_season(history.cid, history.season)
    assert bundle is not None
    result = service.sync_match_payloads(
        bundle, limit=1, phase="primary", reuse_history_source_gaps=reuse,
    )
    assert result.not_available == 1
    assert history.url(history.missing_ids[0]) in {url for url, _ in transport.calls}
    assert {proof["run_id"] for proof in result.metadata["source_gap_matches"]} == {service.run_id}


def test_recoverable_success_takes_precedence_over_prior_absence(monkeypatch):
    history = ExplicitHistory(monkeypatch)
    history.run(extra=("--match-limit", "1"))
    service, transport = history.service()
    match_id = history.missing_ids[0]
    history.responses[history.url(match_id)] = history.details(match_id)
    recovered = transport.replay_json("matchDetails", {"matchId": str(match_id)})
    transport.calls.clear()
    monkeypatch.setattr(service, "_recover_match_fetch", lambda match: (
        recovered if str(match["match_id"]) == str(match_id) else None
    ))
    rc, report, _ = history.run(service=service)
    assert rc == 0, report["errors"]
    assert report["selection"]["source_gap_scopes"] == []
    assert history.repo.latest_match_source(recovered.target_key) is not None
    assert history.url(match_id) not in {url for url, _ in transport.calls}


def test_gap_closure_rejects_changed_observation_time(monkeypatch):
    history = ExplicitHistory(monkeypatch)
    service, _ = history.service()
    sync = service.sync_match_payloads

    def tamper(bundle, **kwargs):
        result = sync(bundle, **kwargs)
        for proof in result.metadata.get("source_gap_matches", []):
            proof["observed_at"] = "2000-01-01T00:00:00"
        return result

    monkeypatch.setattr(service, "sync_match_payloads", tamper)
    rc, report, _ = history.run(service=service)
    _assert_pending(history, rc, report)
    assert any("no committed source gap proof" in error for error in report["errors"])


def test_durable_gap_identity_survives_flush_selection_and_closure(monkeypatch):
    from tests.unit.scrapers.test_fotmob_repository import RecordingWriter

    history = ExplicitHistory(monkeypatch, total=2, missing=2)
    history.run(extra=("--match-limit", "1"))
    prior = next(commit for commit in history.repo.commits if commit.target_type == "match")
    service, transport = history.service()
    _, bundle = service.sync_season(history.cid, history.season)

    class DurableWriter(RecordingWriter):
        def _get_trino_manager(self):
            return self

        def table_exists(self, *args):
            return False

        def execute_query(self, query):
            # Return exactly the requested SQL columns, like Trino. In
            # particular, a fixture must not inject run_id when SELECT omits it.
            if "WHERE target_type = 'match'" not in query or "LIMIT 1" not in query:
                return []
            rows = [row for frame, options in self.calls
                    if options["table"] == "fotmob_ingest_manifest"
                    for row in frame.to_dict("records")
                    if row["target_type"] == "match"
                    and f"entity_id = '{row['entity_id']}'" in query]
            if not rows:
                return []
            columns = query.split("SELECT", 1)[1].split("FROM", 1)[0].strip().split(", ")
            return [tuple(rows[-1].get(column) for column in columns)]

    writer = DurableWriter()
    repository = FotMobRepository(writer=writer, batch_size=100)
    repository.commit(prior)
    repository.flush()
    assert not repository._pending_manifest
    assert repository.latest_entity_attempt("match", prior.entity_id)["run_id"] == prior.run_id
    service.repository = repository
    transport.calls.clear()
    result = service.sync_match_payloads(bundle, limit=1, phase="primary", reuse_history_source_gaps=True)
    assert result.not_available == 2
    assert {url for url, _ in transport.calls} == {history.url(history.ids[1])}
    repository.flush()
    assert not repository._pending_manifest
    _, evidence = service.record_scope_source_gap(
        bundle, plan_signature="durable-history", scope_entities=("season", "matches"),
        source_gap_matches=result.metadata["source_gap_matches"],
    )
    assert {proof["run_id"] for proof in evidence["source_gap_matches"]} == {
        prior.run_id, service.run_id,
    }
