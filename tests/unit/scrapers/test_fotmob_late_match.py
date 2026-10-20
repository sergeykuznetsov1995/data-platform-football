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


@pytest.mark.parametrize("persisted", [False, True], ids=["buffer", "sql"])
def test_late_match_processed_missing_raw_allows_scheduled_http(tmp_path, monkeypatch, persisted):
    from scrapers.fotmob import repository as repository_module

    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    missing = {"error": True, "message": "Data not found", "matchId": 100}
    transport, session = _transport([
        FakeResponse(200, json.dumps(missing).encode()),
        FakeResponse(200, json.dumps(GOOD).encode()),
    ], raw_store=raw_store, max_attempts=1)
    writer = ReconcileWriter()
    repository = FotMobRepository(writer=writer, batch_size=100)
    query = writer.trino.execute_query

    def projected(sql):
        if "WHERE target_type = 'match'" in sql or "ROW_NUMBER()" in sql:
            rows = list(writer.rows.get("fotmob_ingest_manifest", []))
            if "status IN ('success', 'not_modified')" in sql:
                rows = [row for row in rows if row["status"] in {"success", "not_modified"}]
            if "ROW_NUMBER()" in sql:
                latest = {}
                for row in sorted(rows, key=lambda row: (row["completed_at"], row["batch_id"])):
                    key = ((row["target_type"], row["entity_id"])
                           if "PARTITION BY target_type, entity_id" in sql else row["target_key"])
                    latest[key] = row
                rows = list(latest.values())
            if "LIMIT 1" in sql:
                rows = sorted(rows, key=lambda row: (row["completed_at"], row["batch_id"]), reverse=True)[:1]
            columns = [column.strip() for column in sql.split("SELECT", 1)[1].split("FROM", 1)[0].split(",")]
            return [tuple(row.get(column) for column in columns) for row in rows]
        return query(sql)

    monkeypatch.setattr(writer.trino, "execute_query", projected)
    seed(repository)
    clock = [T + timedelta(hours=6)]
    monkeypatch.setattr(service_module, "utc_now", lambda: clock[0])
    monkeypatch.setattr(repository_module, "utc_now", lambda: clock[0])
    original_store = raw_store.store

    def store(*args, **kwargs):
        kwargs["fetched_at"] = kwargs["validated_at"] = clock[0].isoformat()
        return original_store(*args, **kwargs)

    monkeypatch.setattr(raw_store, "store", store)
    service = FotMobIngestService(transport=transport, repository=repository, run_id="missing")
    service.ledger.requests = 90
    assert service.sync_match_payloads(bundle()).not_available == 1
    assert repository._pending_manifest[-1]["error_code"] == "source_data_not_found"
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == T
    if persisted:
        repository.flush()
        repository = FotMobRepository(writer=writer, batch_size=100)

    replay_calls = []
    replay = transport.replay_json

    def tracked_replay(*args, **kwargs):
        replay_calls.append((args, kwargs))
        return replay(*args, **kwargs)

    monkeypatch.setattr(transport, "replay_json", tracked_replay)

    clock[0] += timedelta(hours=1)
    restarted = FotMobIngestService(transport=transport, repository=repository, run_id="retry-missing")
    restarted.ledger.requests = 90
    result = restarted.sync_match_payloads(bundle())
    assert result.succeeded == 1 and result.not_available == 0
    assert not replay_calls
    assert len(session.calls) == 2
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == clock[0]
    repository.flush()
    assert sum(row["status"] == "not_available" for row in writer.rows["fotmob_ingest_manifest"]) == 1
    assert json.loads(writer.rows["fotmob_match_payloads"][0]["match_facts_json"])["infoBox"]["Attendance"] == 14757


@pytest.mark.parametrize("changed", ["body", "validation", "both"])
def test_late_match_recovery_rejects_raw_changed_during_replay(tmp_path, monkeypatch, changed):
    from scrapers.fotmob.repository import MemoryFotMobRepository

    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    observed = T + timedelta(hours=48)
    raw_store.store(TARGET, json.dumps(GOOD).encode(), fetched_at=observed.isoformat())
    transport, session = _transport([], raw_store=raw_store, max_attempts=1)
    repository = MemoryFotMobRepository()
    seed(repository, observed=T + timedelta(hours=6))
    service = FotMobIngestService(transport=transport, repository=repository, run_id="raced-recovery")
    monkeypatch.setattr(service_module, "utc_now", lambda: T + timedelta(hours=100))
    older = copy.deepcopy(GOOD)
    older["content"]["matchFacts"]["infoBox"]["Attendance"] = None
    original_load = raw_store.load
    reads = []

    def raced_load(target):
        pair = original_load(target)
        reads.append(pair[1])
        if len(reads) == 1:
            # A delayed writer replaces the target between the eligibility
            # check and replay; the first read still returns the newer pair.
            raw_store.store(
                TARGET, json.dumps(GOOD if changed == "validation" else older).encode(),
                fetched_at=(observed if changed == "body" else T + timedelta(hours=6)).isoformat(),
            )
        return pair

    monkeypatch.setattr(raw_store, "load", raced_load)
    result = service.sync_match_payloads(bundle())
    assert len(reads) == 2
    assert result.ok and result.succeeded == 0 and result.skipped == 1
    assert not session.calls
    assert not repository.tables.get("fotmob_match_payloads")
    assert len(repository.commits) == 1
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == T + timedelta(hours=6)


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


def test_late_match_recovers_response_received_after_http_cutoff(tmp_path, monkeypatch):
    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    transport, session = _transport(
        [FakeResponse(200, json.dumps(GOOD).encode())], raw_store=raw_store, max_attempts=1,
    )
    from scrapers.fotmob.repository import MemoryFotMobRepository
    repository = MemoryFotMobRepository()
    seed(repository, observed=T + timedelta(hours=6))
    service = FotMobIngestService(transport=transport, repository=repository)
    service.ledger.requests = 90
    clock = [T + timedelta(hours=72) - timedelta(seconds=1)]
    received_at = T + timedelta(hours=72, seconds=1)
    monkeypatch.setattr(service_module, "utc_now", lambda: clock[0])
    original_store = raw_store.store

    def received(*args, **kwargs):
        clock[0] = received_at
        kwargs["fetched_at"] = kwargs["validated_at"] = received_at.isoformat()
        return original_store(*args, **kwargs)

    monkeypatch.setattr(raw_store, "store", received)

    def interrupted(*args, **kwargs):
        raise RuntimeError("interrupted before publication")

    monkeypatch.setattr(service, "_commit_for_fetch", interrupted)
    with pytest.raises(RuntimeError, match="interrupted before publication"):
        service.sync_match_payloads(bundle())
    assert len(session.calls) == 1
    restarted = FotMobIngestService(transport=transport, repository=repository)
    restarted.deadline_at = T
    clock[0] = T + timedelta(hours=100)
    result = restarted.sync_match_payloads(bundle())
    assert result.ok and result.succeeded == 1
    assert len(session.calls) == 1
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == received_at
    assert repository.tables["fotmob_match_payloads"][0]["match_facts_json"]["infoBox"]["Attendance"] == 14757


def test_late_match_rechecks_cutoff_before_each_http(monkeypatch):
    other = canonicalize_target("matchDetails", {"matchId": "101"})
    season_bundle = bundle()
    season_bundle = replace(season_bundle, matches=(
        *season_bundle.matches, {**season_bundle.matches[0], "match_id": 101},
    ))
    service, transport, repository = _service({
        TARGET.canonical_url: GOOD,
        other.canonical_url: {**GOOD, "general": {"matchId": 101}},
    })
    seed(repository)
    repository.record(replace(repository.commits[-1], target_key=other.target_key, entity_id="101"))
    service.ledger.requests = 90
    clock = [T + timedelta(hours=72) - timedelta(seconds=1)]
    monkeypatch.setattr(service_module, "utc_now", lambda: clock[0])
    original = transport.fetch_json

    def received(*args, **kwargs):
        fetch = original(*args, **kwargs)
        clock[0] += timedelta(seconds=2)
        return replace(fetch, fetched_at=clock[0].isoformat())

    monkeypatch.setattr(transport, "fetch_json", received)
    result = service.sync_match_payloads(season_bundle)
    assert result.ok and result.succeeded == 1 and result.skipped == 1
    assert transport.calls == [(TARGET.canonical_url, False)]
    assert source_observed_at(repository.latest_match_source(other.target_key)) == T


def test_late_match_deadline_defers_http_and_still_recovers_raw(tmp_path, monkeypatch):
    other = canonicalize_target("matchDetails", {"matchId": "101"})
    season_bundle = bundle()
    season_bundle = replace(season_bundle, matches=(
        *season_bundle.matches, {**season_bundle.matches[0], "match_id": 101},
    ))
    raw_store = FotMobRawStore.from_uri(tmp_path.as_uri())
    now = T + timedelta(hours=48)
    raw_store.store(
        other, json.dumps({**GOOD, "general": {"matchId": 101}}).encode(),
        fetched_at=now.isoformat(),
    )
    transport, session = _transport([], raw_store=raw_store, max_attempts=1)
    from scrapers.fotmob.repository import MemoryFotMobRepository
    repository = MemoryFotMobRepository()
    seed(repository)
    repository.record(replace(repository.commits[-1], target_key=other.target_key, entity_id="101"))
    service = FotMobIngestService(transport=transport, repository=repository)
    service.deadline_at = now
    monkeypatch.setattr(service_module, "utc_now", lambda: now)
    result = service.sync_match_payloads(season_bundle, phase="late")
    assert result.ok and result.succeeded == 1
    assert result.metadata["deferred_reason"] == "deadline"
    assert not session.calls
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == T
    assert source_observed_at(repository.latest_match_source(other.target_key)) == now


def test_late_match_runner_deadline_is_soft_deferral(monkeypatch):
    import importlib
    from tests.unit.scrapers.test_run_fotmob_scraper import _run_native_admitted
    runner = importlib.import_module("dags.scripts.run_fotmob_scraper")
    now = T + timedelta(hours=48)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.replace(tzinfo=tz) if tz else now

    monkeypatch.setattr(runner, "datetime", Clock)
    monkeypatch.setattr(service_module, "utc_now", lambda: now)
    service, transport, repository = _service({
        canonicalize_target("allLeagues").canonical_url: {
            "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}],
        },
        canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
        TARGET.canonical_url: GOOD,
    })
    seed(repository)
    service.ledger.requests = 90
    original = service.sync_match_payloads

    def expire_after_primary(*args, **kwargs):
        result = original(*args, **kwargs)
        if kwargs.get("phase") == "primary":
            service.deadline_at = now
        return result

    monkeypatch.setattr(service, "sync_match_payloads", expire_after_primary)
    args = runner._argument_parser().parse_args([
        "--mode", "refresh", "--catalog-contract", "fotmob-catalog-v1",
        "--entities", "season,matches",
    ])
    rc, report = _run_native_admitted(runner, args, service=service)
    assert rc == 0, report["errors"]
    assert report["status"] == "partial_success" and not report["complete"]
    assert report["selection"]["deferrals"] == [{
        "kind": "deadline", "target_type": "scope", "targets": ["47=2025/2026"],
        "reason": "run deadline deferred late matches",
    }]
    assert not any("matchDetails" in url for url, _ in transport.calls)


@pytest.mark.parametrize("observation_metadata", [False, True])
def test_review3_current_keeps_newer_source_when_older_wave_commits_last(observation_metadata):
    import duckdb
    from scrapers.fotmob.repository import PARSER_VERSION

    with duckdb.connect() as db:
        db.execute("CREATE SCHEMA bronze")
        # Execute generated view SQL with equivalents of Trino's scalar functions.
        db.execute("CREATE MACRO from_iso8601_timestamp(value) AS CAST(value AS TIMESTAMPTZ)")
        db.execute("CREATE MACRO json_extract_scalar(value, path) AS json_extract_string(value, path)")
        db.execute("""CREATE TABLE bronze.fotmob_ingest_manifest (
            batch_id VARCHAR, target_key VARCHAR, parser_version VARCHAR, status VARCHAR,
            target_type VARCHAR, entity_id VARCHAR, competition_id VARCHAR,
            source_season_key VARCHAR, capabilities_json VARCHAR,
            fetched_at TIMESTAMP, completed_at TIMESTAMP
        )""")
        for table in ("fotmob_matches", "fotmob_match_payloads"):
            db.execute(f"""CREATE TABLE bronze.{table} (
                competition_id VARCHAR, source_season_key VARCHAR, match_id VARCHAR,
                _target_batch_id VARCHAR, _observed_at TIMESTAMP, _ingested_at TIMESTAMP
            )""")
        db.execute("INSERT INTO bronze.fotmob_matches VALUES ('47', '2025/2026', '100', 'calendar', ?, ?)", [T, T])
        db.execute("INSERT INTO bronze.fotmob_ingest_manifest VALUES ('calendar', 'calendar', ?, 'success', 'league_season', NULL, '47', '2025/2026', '{}', ?, ?)", [PARSER_VERSION, T, T])
        for batch, observed, completed in (
            ("newer", T + timedelta(hours=50), T + timedelta(hours=51)),
            ("older", T + timedelta(hours=49), T + timedelta(hours=52)),
        ):
            capabilities = json.dumps({"source_observed_at": observed.isoformat()}) if observation_metadata else "{}"
            # A 304 inherits an old fetched_at but validates at observed.
            fetched = T if observation_metadata else observed
            db.execute("INSERT INTO bronze.fotmob_ingest_manifest VALUES (?, ?, ?, 'success', 'match', '100', '47', '2025/2026', ?, ?, ?)", [batch, TARGET.target_key, PARSER_VERSION, capabilities, fetched, completed])
            db.execute("INSERT INTO bronze.fotmob_match_payloads VALUES ('47', '2025/2026', '100', ?, ?, ?)", [batch, observed, completed])

        class Trino:
            def table_exists(self, schema, table):
                return table in {"fotmob_matches", "fotmob_match_payloads"}

            def get_table_columns(self, schema, table):
                return [row[0] for row in db.execute(f"DESCRIBE {schema}.{table}").fetchall()]

            def _execute(self, sql):
                db.execute(sql.replace("iceberg.bronze.", "bronze."))

        writer = ReconcileWriter()
        writer.trino = Trino()
        FotMobRepository(writer=writer).ensure_current_views()
        assert db.execute("SELECT _target_batch_id FROM bronze.fotmob_match_payloads_current").fetchall() == [("newer",)]


@pytest.mark.parametrize("stop", ["budget", "deadline"])
def test_review3_explicit_refresh_reports_unfinished_ids(monkeypatch, stop):
    from dags.scripts import run_fotmob_scraper as runner
    from scrapers.fotmob.repository import MemoryFotMobRepository
    from tests.unit.scrapers.test_run_fotmob_scraper import _run_native_admitted

    transport, session = _transport([FakeResponse(200, json.dumps(GOOD).encode())], max_attempts=4)
    repository = MemoryFotMobRepository()
    repository.tables["fotmob_matches"] = [
        bundle().matches[0], {**bundle().matches[0], "match_id": 101},
    ]
    service = FotMobIngestService(
        transport=transport, repository=repository,
        budget=TransportBudget(max_requests=1, max_direct_bytes=100000),
    )
    if stop == "deadline":
        service.deadline_at = T
    monkeypatch.setattr(service_module, "utc_now", lambda: T + timedelta(hours=48))
    args = runner._argument_parser().parse_args([
        "--mode", "refresh", "--match-ids", "100", "101", "--max-requests", "1",
    ])
    rc, report = _run_native_admitted(runner, args, service=service)
    assert rc == 1
    assert not report["complete"] and report["status"] != "success"
    assert len(session.calls) == (1 if stop == "budget" else 0)
    assert any("refresh incomplete" in error for error in report["errors"])


@pytest.mark.parametrize("failure", ["quota", "retryable", "terminal", "deadline"])
def test_review3_scope_finalizes_after_late_pass(monkeypatch, failure):
    from dags.scripts import run_fotmob_scraper as runner
    from tests.unit.scrapers.test_run_fotmob_scraper import _run_native_admitted

    now = T + timedelta(hours=48)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.replace(tzinfo=tz) if tz else now

    monkeypatch.setattr(runner, "datetime", Clock)
    monkeypatch.setattr(service_module, "utc_now", lambda: now)
    service, transport, repository = _service({
        canonicalize_target("allLeagues").canonical_url: {
            "countries": [{"leagues": [{"id": 47, "name": "Premier League"}]}],
        },
        canonicalize_target("leagues", {"id": 47}).canonical_url: _league_payload(),
        TARGET.canonical_url: GOOD,
    })
    seed(repository)
    if failure != "quota":
        service.ledger.requests = 90
    original = service.sync_match_payloads

    def late_pass(*args, **kwargs):
        if kwargs.get("phase") == "late":
            assert not any(c.target_type == "scope_completion" for c in repository.commits)
            assert not any(c.target_type == "scope_attempt" for c in repository.commits)
            if failure == "deadline":
                service.deadline_at = now
        return original(*args, **kwargs)

    monkeypatch.setattr(service, "sync_match_payloads", late_pass)
    original_fetch = transport.fetch_json

    def fetch(endpoint, *args, **kwargs):
        if endpoint == "matchDetails":
            if failure == "retryable":
                raise OSError("storage unavailable")
            if failure == "terminal":
                raise ValueError("invalid match payload")
        return original_fetch(endpoint, *args, **kwargs)

    monkeypatch.setattr(transport, "fetch_json", fetch)
    args = runner._argument_parser().parse_args([
        "--mode", "refresh", "--catalog-contract", "fotmob-catalog-v1",
        "--entities", "season,matches",
    ])
    rc, report = _run_native_admitted(runner, args, service=service)
    assert report["selection"]["completed_scopes"] == []
    assert not any(c.target_type == "scope_completion" for c in repository.commits)
    attempts = report["selection"]["scope_attempts"]
    assert len(attempts) == 1
    attempt = attempts[0]
    assert attempt["outcome"] == {
        "quota": "deferred", "deadline": "deferred", "retryable": "retryable", "terminal": "terminal",
    }[failure]
    if failure != "terminal":
        assert attempt["next_retry_at"] is not None
    assert not report["complete"]
    assert rc == (0 if failure in {"quota", "deadline"} else 1)


@pytest.mark.parametrize("wait", ["retry_after", "rate_limit"])
def test_review3_late_transport_stops_after_wait_crosses_cutoff(monkeypatch, wait):
    from scrapers.fotmob.repository import MemoryFotMobRepository

    clock = [T + timedelta(hours=72) - timedelta(seconds=1)]
    transport, session = _transport([
        FakeResponse(503, b"down", {"Retry-After": "2"}),
        FakeResponse(200, json.dumps(GOOD).encode()),
    ], max_attempts=2)
    repository = MemoryFotMobRepository()
    seed(repository)
    service = FotMobIngestService(transport=transport, repository=repository)
    service.ledger.requests = 90
    monkeypatch.setattr(service_module, "utc_now", lambda: clock[0])

    def advance(_delay):
        clock[0] += timedelta(seconds=2)

    if wait == "retry_after":
        transport.sleep_fn = advance
    else:
        class Limiter:
            def acquire(self, **kwargs):
                advance(2)
                return True

        transport.rate_limiter = Limiter()
    result = service.sync_match_payloads(bundle(), phase="late")
    assert result.ok
    expected = 1 if wait == "retry_after" else 0
    assert len(session.calls) == expected
    assert service.ledger.requests == 90 + expected
    assert service._late_match_requests == expected
    assert source_observed_at(repository.latest_match_source(TARGET.target_key)) == T
