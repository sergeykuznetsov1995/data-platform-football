"""Offline regressions for derived late-match obligations and raw recovery."""
import copy
import json
from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from scrapers.fotmob import service as service_module
from scrapers.fotmob.domain import ScopeRef
from scrapers.fotmob.match_refresh import match_refresh, source_observed_at
from scrapers.fotmob.parsers import parse_season_bundle
from scrapers.fotmob.planner import BudgetExceeded, BudgetLedger, RunMode, TransportBudget
from scrapers.fotmob.raw_store import FotMobRawStore
from scrapers.fotmob.repository import FotMobRepository, ManifestStatus, TargetCommit
from scrapers.fotmob.service import FotMobIngestService
from scrapers.fotmob.transport import FetchOutcome, canonicalize_target
from tests.unit.scrapers.test_fotmob_service import _league_payload, _service
from tests.unit.scrapers.test_fotmob_repository import ReconcileWriter
from tests.unit.scrapers.test_fotmob_transport import FakeResponse, _transport


T = datetime(2026, 1, 1, 15)
TARGET = canonicalize_target("matchDetails", {"matchId": "100"})
GOOD = {"general": {"matchId": 100}, "content": {
    "matchFacts": {"infoBox": {"Attendance": 14757}}, "stats": {},
}}


def bundle():
    return parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))


def seed(repository, *, observed=T, status=ManifestStatus.SUCCESS, stale=False):
    repository.record(TargetCommit(
        run_id="first", target_type="match", target_key=TARGET.target_key,
        entity_id="100", status=status, stale=stale, fetched_at=observed,
        content_hash="a" * 64, completed_at=observed,
    ))


@pytest.mark.parametrize(("hours", "observed", "kind"), [
    (5, T, "wait"), (6, T, "late"), (47, T + timedelta(hours=6), "wait"),
    (48, T + timedelta(hours=6), "late"), (72, T, "done"),
    (100, T, "done"), (50, T + timedelta(hours=49), "done"),
])
def test_late_match_policy(hours, observed, kind):
    assert match_refresh(bundle().matches[0], observed, T + timedelta(hours=hours)).kind == kind


@pytest.mark.parametrize("status", [ManifestStatus.NOT_AVAILABLE, ManifestStatus.SCHEMA_DRIFT, ManifestStatus.RETRYABLE_FAILURE])
def test_late_match_failure_does_not_reopen_primary_debt(status):
    service, _, repository = _service({})
    seed(repository)
    seed(repository, observed=T + timedelta(hours=80), status=status)
    seed(repository, observed=T + timedelta(hours=90), stale=True)
    assert service.match_refresh_state(bundle().matches[0], T + timedelta(hours=100)).kind == "done"
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == T


def test_late_match_attempts_survive_byte_overflow():
    ledger = BudgetLedger(TransportBudget(max_requests=10, max_direct_bytes=1))
    with pytest.raises(BudgetExceeded):
        ledger.account_fetch(attempts=3, direct_bytes=2, proxy_bytes=0)
    assert ledger.requests == 3


@pytest.mark.parametrize("failure", [False, True])
def test_late_match_quota_reserves_retries_and_counts_errors(monkeypatch, failure):
    service, transport, repository = _service({TARGET.canonical_url: GOOD})
    transport.max_attempts = 3
    seed(repository)
    service.ledger.requests = 26
    now = T + timedelta(hours=6)
    monkeypatch.setattr(service_module, "utc_now", lambda: now)
    assert service.sync_match_payloads(bundle()).succeeded == 0
    assert not transport.calls
    service.ledger.requests = 27
    original = transport.fetch_json

    def retried(*args, **kwargs):
        fetch = replace(original(*args, **kwargs), attempts=3, retries=2, fetched_at=now.isoformat())
        transport._results[-1] = fetch
        if failure:
            raise OSError("raw storage failed after HTTP")
        return fetch

    monkeypatch.setattr(transport, "fetch_json", retried)
    result = service.sync_match_payloads(bundle())
    assert result.succeeded == (0 if failure else 1)
    assert service.ledger.requests == 30
    assert service._late_match_requests == 3
    assert 9 * service._late_match_requests == service.ledger.requests - service._late_match_requests
    if failure:
        service.sync_match_payloads(bundle())
        assert len(transport.calls) == 1
    monkeypatch.setattr(service_module, "utc_now", lambda: T + timedelta(hours=72))
    service.sync_match_payloads(bundle())
    assert len(transport.calls) == 1


@pytest.mark.parametrize("status", [200, 304])
def test_late_match_identical_response_persists_observation_without_batch(tmp_path, monkeypatch, status):
    writer = ReconcileWriter()
    repository = FotMobRepository(writer=writer, batch_size=100)
    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    body = json.dumps(GOOD).encode()
    transport, session = _transport([
        FakeResponse(200, body), FakeResponse(status, body if status == 200 else b""),
    ], raw_store=raw_store, max_attempts=1)
    service = FotMobIngestService(transport=transport, repository=repository, run_id="same-wave")
    service.ledger.requests = 90
    clock = [T]
    from scrapers.fotmob import transport as transport_module

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].replace(tzinfo=tz) if tz else clock[0]

    monkeypatch.setattr(transport_module, "datetime", Clock)
    monkeypatch.setattr(service_module, "utc_now", lambda: clock[0])
    original_store = raw_store.store

    def store(*args, **kwargs):
        # Freeze the source-validation clock, including 304's new validation.
        kwargs["validated_at"] = clock[0].isoformat()
        kwargs.setdefault("fetched_at", clock[0].isoformat())
        return original_store(*args, **kwargs)

    monkeypatch.setattr(raw_store, "store", store)
    # The fake SQL writer's preload is deliberately skipped; tested below via
    # a fresh repository and a projection-aware fake query.
    assert service.sync_match_payloads(bundle()).succeeded == 1
    clock[0] += timedelta(hours=6)
    assert service.sync_match_payloads(bundle()).succeeded == 1
    repository.flush()
    assert len(writer.rows["fotmob_match_payloads"]) == 1
    manifests = writer.rows["fotmob_ingest_manifest"]
    assert len(manifests) == 2
    assert len({row["batch_id"] for row in manifests}) == 1
    assert len(session.calls) == 2
    restarted = FotMobRepository(writer=writer)
    query = writer.trino.execute_query

    def projected(sql):
        if "WHERE target_type = 'match'" in sql:
            columns = [col.strip() for col in sql.split("SELECT", 1)[1].split("FROM", 1)[0].split(",")]
            return [tuple(row.get(col) for col in columns) for row in manifests]
        return query(sql)

    monkeypatch.setattr(writer.trino, "execute_query", projected)
    assert source_observed_at(restarted.latest_match_source(TARGET.target_key)) == clock[0]


@pytest.mark.parametrize("incomplete", [False, True])
def test_late_match_recovers_raw_after_cutoff_without_http(tmp_path, monkeypatch, incomplete):
    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    payload = {"general": {"matchId": 100}, "content": {}} if incomplete else GOOD
    raw_store.store(TARGET, json.dumps(payload).encode(), fetched_at=(T + timedelta(hours=48)).isoformat())
    transport, session = _transport([], raw_store=raw_store, max_attempts=1)
    from scrapers.fotmob.repository import MemoryFotMobRepository
    repository = MemoryFotMobRepository()
    seed(repository, observed=T + timedelta(hours=6))
    service = FotMobIngestService(transport=transport, repository=repository, run_id="recovery")
    service.deadline_at = T
    monkeypatch.setattr(service_module, "utc_now", lambda: T + timedelta(hours=100))
    result = service.sync_match_payloads(bundle())
    assert not session.calls
    assert result.succeeded == (0 if incomplete else 1)
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == (
        T + timedelta(hours=6 if incomplete else 48)
    )
    assert bool(repository.tables.get("fotmob_match_payloads")) == (not incomplete)


@pytest.mark.parametrize("stop", ["bytes", "deadline"])
def test_late_match_publishes_received_payload_before_budget_or_deadline(tmp_path, monkeypatch, stop):
    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    transport, session = _transport([FakeResponse(200, json.dumps(GOOD).encode())], raw_store=raw_store, max_attempts=1)
    from scrapers.fotmob.repository import MemoryFotMobRepository
    repository = MemoryFotMobRepository()
    seed(repository)
    now = T + timedelta(hours=48)
    monkeypatch.setattr(service_module, "utc_now", lambda: now)
    service = FotMobIngestService(
        transport=transport, repository=repository, run_id="final",
        budget=TransportBudget(max_requests=100, max_direct_bytes=1 if stop == "bytes" else 100000),
    )
    service.ledger.requests = 90
    original = transport.fetch_json

    def received(*args, **kwargs):
        fetch = original(*args, **kwargs)
        service.deadline_at = T if stop == "deadline" else None
        return replace(fetch, source_observed_at=now.isoformat())

    monkeypatch.setattr(transport, "fetch_json", received)
    result = service.sync_match_payloads(bundle())
    assert result.succeeded == 1
    assert len(session.calls) == 1
    assert service.ledger.requests == 91
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == now


def test_late_match_explicit_ids_skip_discovery_and_use_one_attempt(monkeypatch):
    import importlib
    from scrapers.fotmob.repository import MemoryFotMobRepository
    from tests.unit.scrapers.test_run_fotmob_scraper import _run_native_admitted
    runner = importlib.import_module("dags.scripts.run_fotmob_scraper")
    transport, session = _transport([FakeResponse(200, json.dumps(GOOD).encode())], max_attempts=4)
    repository = MemoryFotMobRepository()
    repository.tables["fotmob_matches"] = [bundle().matches[0]]
    service = FotMobIngestService(transport=transport, repository=repository, run_id="explicit")
    monkeypatch.setattr(service, "discover_catalog", lambda: pytest.fail("unexpected discovery"))
    args = runner._argument_parser().parse_args(["--mode", "refresh", "--match-ids", "100", "100"])
    rc, report = _run_native_admitted(runner, args, service=service)
    assert rc == 0 and report["status"] == "success"
    assert [url for url, _ in session.calls] == [TARGET.canonical_url]
    assert service.ledger.requests == 1
    assert repository.tables["fotmob_match_payloads"][0]["match_facts_json"]["infoBox"]["Attendance"] == 14757


def test_late_match_explicit_ids_do_not_retry_http_errors(monkeypatch):
    import importlib
    from scrapers.fotmob.repository import MemoryFotMobRepository
    from tests.unit.scrapers.test_run_fotmob_scraper import _run_native_admitted
    runner = importlib.import_module("dags.scripts.run_fotmob_scraper")
    transport, session = _transport([FakeResponse(503)], max_attempts=4)
    repository = MemoryFotMobRepository()
    repository.tables["fotmob_matches"] = [bundle().matches[0]]
    service = FotMobIngestService(transport=transport, repository=repository, run_id="explicit-failure")
    monkeypatch.setattr(service, "discover_catalog", lambda: pytest.fail("unexpected discovery"))
    args = runner._argument_parser().parse_args(["--mode", "refresh", "--match-ids", "100"])
    rc, _ = _run_native_admitted(runner, args, service=service)
    assert rc == 1
    assert len(session.calls) == 1 and service.ledger.requests == 1


@pytest.mark.parametrize("failure_table", ["fotmob_match_payloads", "fotmob_field_inventory", "fotmob_ingest_manifest"])
def test_late_match_restart_reconciles_publication_interruption(tmp_path, monkeypatch, failure_table):
    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    raw_store.store(TARGET, json.dumps(GOOD).encode(), fetched_at=(T + timedelta(hours=48)).isoformat())
    writer = ReconcileWriter(fail_after_commit=failure_table)
    repository = FotMobRepository(writer=writer, batch_size=100)
    transport, session = _transport([], raw_store=raw_store, max_attempts=1)
    service = FotMobIngestService(transport=transport, repository=repository, run_id="interrupted")
    monkeypatch.setattr(service_module, "utc_now", lambda: T + timedelta(hours=100))
    assert service.sync_match_payloads(bundle()).succeeded == 1
    try:
        repository.flush()
    except RuntimeError:
        pass
    # A fresh process only trusts durable manifest observations.
    restarted = FotMobRepository(writer=writer, batch_size=100)
    query = writer.trino.execute_query

    def projected(sql):
        if "WHERE target_type = 'match'" in sql or "ROW_NUMBER()" in sql:
            columns = (*restarted._READ_COLUMNS, "target_type", "entity_id") if "ROW_NUMBER()" in sql else (*restarted._READ_COLUMNS, "target_type")
            return [tuple(row.get(column) for column in columns)
                    for row in writer.rows.get("fotmob_ingest_manifest", [])]
        if "ORDER BY completed_at DESC, batch_id DESC" in sql:
            return []
        return query(sql)

    monkeypatch.setattr(writer.trino, "execute_query", projected)
    new_service = FotMobIngestService(transport=transport, repository=restarted, run_id="restarted")
    new_service.deadline_at = T
    new_service.sync_match_payloads(bundle())
    restarted.flush()
    assert not session.calls
    assert len(writer.rows["fotmob_match_payloads"]) == 1
    assert len(writer.rows["fotmob_ingest_manifest"]) == 1
    assert source_observed_at(restarted.latest_match_source(TARGET.target_key)) == T + timedelta(hours=48)


@pytest.mark.parametrize("season_limit", [0, 1])
def test_late_match_runner_prioritizes_primary_scopes_before_late_pass(monkeypatch, season_limit):
    import importlib
    from tests.unit.scrapers.test_run_fotmob_scraper import _run_native_admitted
    runner = importlib.import_module("dags.scripts.run_fotmob_scraper")
    now = T + timedelta(hours=6)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.replace(tzinfo=tz) if tz else now

    monkeypatch.setattr(runner, "datetime", Clock)
    monkeypatch.setattr(service_module, "utc_now", lambda: now)
    profiles = {value: copy.deepcopy(_league_payload()) for value in (42, 47)}
    profiles[42]["details"].update(id=42, name="League 42")
    profiles[42]["fixtures"]["allMatches"][0]["id"] = 101
    responses = {canonicalize_target("allLeagues").canonical_url: {
        "countries": [{"leagues": [{"id": value, "name": f"League {value}"} for value in (42, 47)]}],
    }}
    for value, profile in profiles.items():
        responses[canonicalize_target("leagues", {"id": value}).canonical_url] = profile
    responses[TARGET.canonical_url] = GOOD
    other_target = canonicalize_target("matchDetails", {"matchId": "101"})
    responses[other_target.canonical_url] = {**GOOD, "general": {"matchId": 101}}
    service, transport, repository = _service(responses)
    seed(repository)
    service.ledger.requests = 90
    original = transport._get
    monkeypatch.setattr(transport, "_get", lambda *a, **kw: replace(original(*a, **kw), fetched_at=now.isoformat()))
    args = runner._argument_parser().parse_args([
        "--mode", "refresh", "--entities", "season,matches", "--season-limit", str(season_limit),
    ])
    rc, report = _run_native_admitted(runner, args, service=service)
    assert rc == 0, report["errors"]
    match_calls = [url for url, _ in transport.calls if "matchDetails" in url]
    assert match_calls == ([other_target.canonical_url] if season_limit else [other_target.canonical_url, TARGET.canonical_url])


def test_late_match_runner_reopens_campaign_and_history_markers(monkeypatch):
    import importlib
    from scrapers.fotmob.planner import deterministic_plan_signature
    from tests.unit.scrapers.test_run_fotmob_scraper import _run_native_admitted
    runner = importlib.import_module("dags.scripts.run_fotmob_scraper")
    now = T + timedelta(hours=48)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.replace(tzinfo=tz) if tz else now

    monkeypatch.setattr(runner, "datetime", Clock)
    monkeypatch.setattr(service_module, "utc_now", lambda: now)
    latest = _league_payload()
    old = _league_payload("2024/2025")
    responses = {
        canonicalize_target("allLeagues").canonical_url: {
            "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}],
        },
        canonicalize_target("leagues", {"id": 47}).canonical_url: latest,
        canonicalize_target("leagues", {"id": 47, "season": "2024/2025"}).canonical_url: old,
        TARGET.canonical_url: GOOD,
    }
    service, transport, repository = _service(responses, mode=RunMode.BACKFILL)
    service.ledger.requests = 90
    seed(repository, observed=T + timedelta(hours=6))
    old_bundle = parse_season_bundle(old, ScopeRef(47, "2024/2025"))
    repository.tables["fotmob_matches"] = list(old_bundle.matches)
    policy = {
        "match_policy": "finished_only", "leaderboard_policy": "all_advertised",
        "team_policy": "global_observed_snapshot", "player_policy": "global_observed_snapshot",
    }
    for scope_policy in (policy, {**policy, "scope_lane": "history"}):
        service.record_scope_completion(
            47, "2024/2025", plan_signature=deterministic_plan_signature({"season", "matches"}, policy=scope_policy),
            coverage={"scope_entities": ["season", "matches"]}, counts={"matches": 1},
        )
    original = transport._get
    monkeypatch.setattr(transport, "_get", lambda *a, **kw: replace(original(*a, **kw), fetched_at=now.isoformat()))
    args = runner._argument_parser().parse_args([
        "--mode", "backfill", "--catalog-contract", "fotmob-catalog-v1", "--entities", "season,matches",
    ])
    rc, report = _run_native_admitted(runner, args, service=service)
    assert rc == 0, report["errors"]
    assert report["selection"]["planned_scopes"] == ["47=2024/2025"]
    assert len([url for url, _ in transport.calls if "matchDetails" in url]) == 1
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == now


def test_late_match_ordinary_exception_accounting_is_per_worker():
    transport, _ = _transport([FakeResponse(503, b"x"), FakeResponse(503, b"x")], max_attempts=3)

    def interrupted(_delay):
        raise RuntimeError("deadline during retry wait")

    transport.sleep_fn = interrupted
    from scrapers.fotmob.repository import MemoryFotMobRepository
    service = FotMobIngestService(transport=transport, repository=MemoryFotMobRepository())
    results = service._fetch_many([("one", "leagues", {"id": 1}), ("two", "leagues", {"id": 2})])
    assert all(isinstance(result, RuntimeError) for result in results.values())
    assert service.ledger.requests == 2
    assert service.ledger.direct_bytes == 2


def test_late_match_304_validation_survives_raw_metadata_write_failure(tmp_path, monkeypatch):
    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    raw_store.store(TARGET, json.dumps(GOOD).encode(), fetched_at=T.isoformat())
    transport, session = _transport([FakeResponse(304)], raw_store=raw_store, max_attempts=1)
    now = T + timedelta(hours=48)
    from scrapers.fotmob import transport as transport_module
    from scrapers.fotmob.repository import MemoryFotMobRepository

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.replace(tzinfo=tz) if tz else now

    monkeypatch.setattr(transport_module, "datetime", Clock)
    monkeypatch.setattr(service_module, "utc_now", lambda: now)
    def failed_store(*args, **kwargs):
        raise OSError("metadata write failed")

    monkeypatch.setattr(raw_store, "store", failed_store)
    repository = MemoryFotMobRepository()
    seed(repository)
    service = FotMobIngestService(transport=transport, repository=repository)
    service.ledger.requests = 90
    assert service.sync_match_payloads(bundle()).succeeded == 1
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == now
    assert service.sync_match_payloads(bundle()).skipped == 1
    assert len(session.calls) == 1


def test_late_match_runner_recovers_before_discovery_deadline_and_budget(tmp_path, monkeypatch):
    import importlib
    from scrapers.fotmob.repository import MemoryFotMobRepository
    from tests.unit.scrapers.test_run_fotmob_scraper import _run_native_admitted
    runner = importlib.import_module("dags.scripts.run_fotmob_scraper")
    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    raw_store.store(TARGET, json.dumps(GOOD).encode(), fetched_at=(T + timedelta(hours=48)).isoformat())
    transport, session = _transport([], raw_store=raw_store, max_attempts=1)
    repository = MemoryFotMobRepository()
    repository.tables["fotmob_matches"] = list(bundle().matches)
    seed(repository)
    service = FotMobIngestService(
        transport=transport, repository=repository,
        budget=TransportBudget(max_requests=0, max_direct_bytes=0),
    )
    service.deadline_at = T
    monkeypatch.setattr(service_module, "utc_now", lambda: T + timedelta(hours=100))
    args = runner._argument_parser().parse_args(["--mode", "refresh", "--entities", "season,matches"])
    rc, report = _run_native_admitted(runner, args, service=service)
    # Publication succeeds even though discovery cannot run under this deadline.
    assert rc == 1
    assert not session.calls
    assert repository.tables["fotmob_match_payloads"][0]["match_facts_json"]["infoBox"]["Attendance"] == 14757
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == T + timedelta(hours=48)
