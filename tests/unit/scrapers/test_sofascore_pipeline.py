from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pyarrow import fs

from scrapers.sofascore.adapters import project_legacy_match_status, raw_lineage_map
from scrapers.sofascore.capture_engine import (
    CaptureResult,
    HttpPayload,
    RetryPolicy,
    SofaScoreCaptureEngine,
)
from scrapers.sofascore.manifest import (
    EndpointManifest,
    InMemoryManifestStore,
    ManifestStatus,
)
from scrapers.sofascore.pipeline import (
    CaptureRuntime,
    DeferredCaptureSink,
    EVENT_PATHS,
    PLAYER_PATHS,
    PLAYER_TARGET_TYPES,
    build_capture_runtime,
    build_event_spec,
    build_player_spec,
    endpoint_resume_plan,
    finalize_materialized_results,
    ingest_prefetched_records,
    materialize_player_datasets,
    promote_repaired_results,
    replay_event_specs,
    replay_player_specs,
)
from scrapers.sofascore.raw_store import RawPayloadStore


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
EVENT_ID = "14023925"
PLAYER_ID = "11111"
FIXTURES = {
    "event": FIXTURE_ROOT / f"sofascore_event_{EVENT_ID}.json",
    "lineups": FIXTURE_ROOT / f"sofascore_event_{EVENT_ID}_lineups.json",
    "statistics": FIXTURE_ROOT / f"sofascore_event_{EVENT_ID}_statistics.json",
    "shotmap": FIXTURE_ROOT / f"sofascore_event_{EVENT_ID}_shotmap.json",
    "incidents": FIXTURE_ROOT / f"sofascore_event_{EVENT_ID}_incidents.json",
}
PLAYER_FIXTURES = {
    "player_profile": FIXTURE_ROOT / f"sofascore_player_{PLAYER_ID}.json",
    "player_season_statistics": (
        FIXTURE_ROOT / f"sofascore_player_{PLAYER_ID}_season_statistics.json"
    ),
}


class UnlimitedLimiter:
    def acquire(self):
        return True


class NoNetworkTransport:
    def __init__(self):
        self.calls = 0

    def request(self, url, *, provider_budget):
        self.calls += 1
        raise AssertionError("offline/prefetched pipeline performed a network call")


class SuccessSink:
    def __init__(self):
        self.calls = []

    def write(self, key, datasets, raw):
        self.calls.append((key, datasets, raw))


class FailOnceSink(SuccessSink):
    def __init__(self):
        super().__init__()
        self.fail = True

    def write(self, key, datasets, raw):
        super().write(key, datasets, raw)
        if self.fail:
            raise RuntimeError("simulated Iceberg commit failure")


def _spec(endpoint: str, *, freshness_key="final"):
    return build_event_spec(
        source_tournament_id=17,
        source_season_id=76986,
        target_id=EVENT_ID,
        endpoint=endpoint,
        freshness_key=freshness_key,
        paid_proxy=False,
    )


def _player_spec(endpoint: str, *, freshness_key="final"):
    return build_player_spec(
        source_tournament_id=17,
        source_season_id=76986,
        target_id=PLAYER_ID,
        endpoint=endpoint,
        freshness_key=freshness_key,
        paid_proxy=False,
    )


def _runtime(tmp_path, sink=None):
    raw = RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))
    manifest = InMemoryManifestStore()
    transport = NoNetworkTransport()
    engine = SofaScoreCaptureEngine(
        raw_store=raw,
        manifest_store=manifest,
        transport=transport,
        run_id="fixture-run",
        task_id="match-capture",
        sink=sink or DeferredCaptureSink(),
        rate_limiter=UnlimitedLimiter(),
        retry_policy=RetryPolicy(max_attempts=1),
        max_workers=2,
    )
    return CaptureRuntime(engine, manifest, raw), transport


def _record(endpoint: str) -> dict:
    body = FIXTURES[endpoint].read_bytes()
    return {
        "match_id": EVENT_ID,
        "endpoint": endpoint,
        "status": 200,
        "headers": {"content-type": "application/json"},
        "body": body,
    }


def _player_record(endpoint: str) -> dict:
    return {
        "player_id": PLAYER_ID,
        "endpoint": endpoint,
        "status": 200,
        "headers": {"content-type": "application/json"},
        "body": PLAYER_FIXTURES[endpoint].read_bytes(),
    }


def test_runtime_reads_verified_budget_configuration_without_blocking_replay(
    tmp_path,
    monkeypatch,
):
    artifact = Path(__file__).resolve().parents[3] / (
        "configs/sofascore/proxy_budget_canary.json"
    )
    monkeypatch.setenv("SOFASCORE_PROXY_BUDGET_ARTIFACT", str(artifact))
    monkeypatch.setenv(
        "SOFASCORE_PROXY_BUDGET_LEDGER", str(tmp_path / "budget.json")
    )
    monkeypatch.setenv(
        "SOFASCORE_MANIFEST_PATH", str(tmp_path / "manifest.json")
    )

    runtime = build_capture_runtime(
        run_id="offline-run",
        task_id="offline-replay",
        raw_store_uri=f"file://{tmp_path / 'raw'}",
        manifest_backend="json",
    )

    assert runtime.engine.budget is None
    assert "explicit workload_class" in runtime.budget_error


def test_runtime_wires_a_reviewed_policy_and_shared_ledger(
    tmp_path,
    monkeypatch,
):
    policy = object()
    ledger = object()
    monkeypatch.setenv("SOFASCORE_PROXY_BUDGET_ARTIFACT", "/canary.json")
    monkeypatch.setenv(
        "SOFASCORE_PROXY_BUDGET_LEDGER", str(tmp_path / "budget.json")
    )
    monkeypatch.setenv(
        "SOFASCORE_MANIFEST_PATH", str(tmp_path / "manifest.json")
    )

    with (
        patch(
            "scrapers.sofascore.pipeline.load_verified_policy",
            return_value=policy,
        ) as load_policy,
        patch("scrapers.sofascore.pipeline.SharedBudgetLedger", return_value=ledger),
    ):
        runtime = build_capture_runtime(
            run_id="paid-run",
            task_id="match-capture",
            raw_store_uri=f"file://{tmp_path / 'raw'}",
            manifest_backend="json",
            workload_class="match_batch_25",
        )

    assert runtime.engine.budget is ledger
    assert runtime.budget_error is None
    load_policy.assert_called_once_with(
        "/canary.json", workload_class="match_batch_25"
    )


class MetadataScraper:
    @staticmethod
    def _add_metadata(frame, entity_type):
        frame = frame.copy()
        frame["_source"] = "sofascore"
        frame["_entity_type"] = entity_type
        frame["_batch_id"] = "fixture-batch"
        return frame


@pytest.mark.parametrize(
    ("endpoint", "datasets"),
    [
        ("event", {"events", "event_participants", "venue"}),
        ("lineups", {"player_ratings", "lineups", "event_player_stats"}),
        ("statistics", {"match_stats"}),
        ("shotmap", {"event_shotmap"}),
        ("incidents", {"incidents"}),
    ],
)
def test_fixture_endpoint_specs_validate_and_parse_every_branch(endpoint, datasets):
    spec = _spec(endpoint)
    payload = json.loads(FIXTURES[endpoint].read_text(encoding="utf-8"))

    assert spec.url == (
        "https://www.sofascore.com"
        + EVENT_PATHS[endpoint].format(target_id=EVENT_ID)
    )
    assert spec.key.as_tuple() == (
        "17", "76986", "event", EVENT_ID, endpoint, "final"
    )
    assert spec.schema_validator(payload) is True
    assert spec.empty_predicate(payload) is False
    assert spec.not_supported_http_statuses == ()
    assert set(spec.parsers) == datasets
    parsed = {name: parser(payload) for name, parser in spec.parsers.items()}
    assert all(isinstance(rows, list) and rows for rows in parsed.values())
    assert all(
        isinstance(row, dict)
        for rows in parsed.values()
        for row in rows
    )


def test_endpoint_spec_schema_drift_is_rejected_for_each_fixture_shape():
    for endpoint in EVENT_PATHS:
        spec = _spec(endpoint)
        assert spec.schema_validator([]) is False
        wrong_payload = {"unexpected": []}
        assert spec.schema_validator(wrong_payload) is False


def test_lineups_missing_players_array_is_schema_drift_not_legitimate_empty():
    spec = _spec("lineups")

    missing_home = {"home": {}, "away": {"players": []}}
    missing_away = {"home": {"players": []}, "away": {}}
    explicit_empty = {
        "home": {"players": []},
        "away": {"players": []},
    }

    assert spec.schema_validator(missing_home) is False
    assert spec.schema_validator(missing_away) is False
    assert spec.schema_validator(explicit_empty) is True
    assert spec.empty_predicate(explicit_empty) is True


def test_event_specs_enforce_coverage_required_paths_and_preserved_arrays():
    statistics = _spec("statistics")
    # This passes the endpoint's top-level list/type check but violates the
    # versioned preserved-array contract below a non-empty group.
    missing_items = {
        "statistics": [
            {
                "period": "ALL",
                "groups": [{"groupName": "Match overview"}],
            }
        ]
    }
    assert statistics.schema_validator(missing_items) is False

    # Explicitly empty arrays remain a structurally valid legitimate empty.
    explicit_empty = {"statistics": []}
    assert statistics.schema_validator(explicit_empty) is True
    assert statistics.empty_predicate(explicit_empty) is True


def test_lineups_missing_players_array_persists_raw_schema_error(tmp_path):
    runtime, transport = _runtime(tmp_path)
    spec = _spec("lineups")
    body = json.dumps(
        {"home": {}, "away": {"players": []}},
        separators=(",", ":"),
    ).encode("utf-8")

    result = runtime.engine.ingest_prefetched(
        spec,
        HttpPayload(
            200,
            body,
            headers={"content-type": "application/json"},
            provider_bytes=0,
        ),
        authorization=None,
    )

    assert transport.calls == 0
    assert result.manifest.status == ManifestStatus.SCHEMA_ERROR
    assert result.manifest.error_type == "SchemaValidationError"
    stored, raw = runtime.raw_store.load_bytes(spec.raw_target)
    assert stored == body
    assert raw.content_hash == hashlib.sha256(body).hexdigest()


def test_lineups_raw_to_normalized_cardinality_loss_is_schema_error(tmp_path):
    runtime, transport = _runtime(tmp_path)
    spec = _spec("lineups")
    payload = json.loads(FIXTURES["lineups"].read_text(encoding="utf-8"))
    payload["home"]["players"].append("schema-drift-not-a-player-object")
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    result = runtime.engine.ingest_prefetched(
        spec,
        HttpPayload(
            200,
            body,
            headers={"content-type": "application/json"},
            provider_bytes=0,
        ),
        authorization=None,
    )

    assert transport.calls == 0
    assert result.manifest.status == ManifestStatus.SCHEMA_ERROR
    assert result.manifest.error_type == "SchemaValidationError"
    assert "normalized_cardinality_drift" in result.manifest.error_message
    stored, _ = runtime.raw_store.load_bytes(spec.raw_target)
    assert stored == body


@pytest.mark.parametrize(
    ("endpoint", "dataset", "target_type"),
    [
        ("player_profile", "player_profile", "player"),
        (
            "player_season_statistics",
            "player_season_stats",
            "season_player",
        ),
    ],
)
def test_player_fixture_specs_use_exact_urls_and_existing_flatteners(
    endpoint,
    dataset,
    target_type,
):
    spec = _player_spec(endpoint)
    payload = json.loads(
        PLAYER_FIXTURES[endpoint].read_text(encoding="utf-8")
    )

    assert spec.url == (
        "https://www.sofascore.com"
        + PLAYER_PATHS[endpoint].format(
            target_id=PLAYER_ID,
            source_tournament_id="17",
            source_season_id="76986",
        )
    )
    assert PLAYER_TARGET_TYPES[endpoint] == target_type
    assert spec.key.as_tuple() == (
        "17",
        "76986",
        target_type,
        PLAYER_ID,
        endpoint,
        "final",
    )
    assert spec.schema_validator(payload) is True
    assert spec.empty_predicate(payload) is False
    assert spec.not_supported_http_statuses == (
        () if endpoint == "player_profile" else (404,)
    )
    assert set(spec.parsers) == {dataset}
    rows = spec.parsers[dataset](payload)
    assert len(rows) == 1
    assert rows[0]["player_id"] == PLAYER_ID
    if endpoint == "player_season_statistics":
        assert rows[0]["unique_tournament_id"] == 17
        assert rows[0]["sofascore_season_id"] == 76986
        assert rows[0]["rating"] == 7.42
        assert rows[0]["total_goals"] == 12


def test_player_specs_fail_closed_on_wrong_identity_and_schema_drift():
    profile = _player_spec("player_profile")
    season_stats = _player_spec("player_season_statistics")

    assert profile.schema_validator({"player": {"id": 99999}}) is False
    assert profile.schema_validator({"unexpected": {}}) is False
    assert season_stats.schema_validator({"statistics": []}) is False
    assert season_stats.schema_validator({"statistics": {}, "team": []}) is False

    with pytest.raises(ValueError, match="unsupported player endpoint"):
        _player_spec("latest_default_statistics")
    with pytest.raises(ValueError, match="must be integers"):
        build_player_spec(
            source_tournament_id="epl",
            source_season_id=76986,
            target_id=PLAYER_ID,
            endpoint="player_profile",
            freshness_key="final",
            paid_proxy=False,
        )


@pytest.mark.parametrize(
    ("spec", "expected_status", "resume_expected"),
    [
        (_spec("event"), ManifestStatus.RETRYABLE_FAILURE, True),
        (_spec("lineups"), ManifestStatus.RETRYABLE_FAILURE, True),
        (_player_spec("player_profile"), ManifestStatus.RETRYABLE_FAILURE, True),
        (
            _player_spec("player_season_statistics"),
            ManifestStatus.NOT_SUPPORTED,
            False,
        ),
    ],
)
def test_required_404_stays_resumable_but_optional_player_stats_is_terminal(
    tmp_path,
    spec,
    expected_status,
    resume_expected,
):
    runtime, transport = _runtime(tmp_path)

    result = runtime.engine.ingest_prefetched(
        spec,
        HttpPayload(
            404,
            b'{"error":"not found"}',
            headers={"content-type": "application/json"},
            provider_bytes=0,
        ),
        authorization=None,
    )

    assert transport.calls == 0
    assert result.manifest.status == expected_status
    pending = endpoint_resume_plan(runtime.manifest_store, [spec])
    assert (spec.key.target_id in pending) is resume_expected


@pytest.mark.parametrize(
    ("endpoint", "payload"),
    [
        ("player_profile", {"player": None}),
        (
            "player_season_statistics",
            {"team": None, "statistics": {}},
        ),
    ],
)
def test_player_explicit_empty_payload_is_legitimate_empty(
    tmp_path,
    endpoint,
    payload,
):
    runtime, transport = _runtime(tmp_path)
    spec = _player_spec(endpoint)
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    result = runtime.engine.ingest_prefetched(
        spec,
        HttpPayload(
            200,
            body,
            headers={"content-type": "application/json"},
            provider_bytes=0,
        ),
        authorization=None,
    )

    assert transport.calls == 0
    assert spec.schema_validator(payload) is True
    assert spec.empty_predicate(payload) is True
    assert result.manifest.status == ManifestStatus.LEGITIMATE_EMPTY
    stored, _ = runtime.raw_store.load_bytes(spec.raw_target)
    assert stored == body


def test_player_fixtures_are_raw_first_materializable_and_offline_replayable(
    tmp_path,
):
    runtime, transport = _runtime(tmp_path)
    specs = {
        (PLAYER_ID, endpoint): _player_spec(endpoint)
        for endpoint in PLAYER_PATHS
    }
    records = {
        endpoint: _player_record(endpoint)
        for endpoint in PLAYER_PATHS
    }

    results = ingest_prefetched_records(runtime, specs=specs, records=records)

    assert transport.calls == 0
    assert len(results) == 2
    assert all(
        result.manifest.status == ManifestStatus.RETRYABLE_FAILURE
        and result.manifest.error_type == "DeferredMaterialization"
        for result in results
    )
    for result in results:
        endpoint = result.manifest.key.endpoint
        expected = PLAYER_FIXTURES[endpoint].read_bytes()
        stored, raw = runtime.raw_store.load_bytes(
            specs[(PLAYER_ID, endpoint)].raw_target
        )
        assert stored == expected
        assert raw.content_hash == hashlib.sha256(expected).hexdigest()

    frames = materialize_player_datasets(
        MetadataScraper(),
        results,
        league="ENG-Premier League",
        season="2526",
    )
    assert set(frames) == {"player_profile", "player_season_stats"}
    assert len(frames["player_profile"]) == 1
    assert len(frames["player_season_stats"]) == 1
    for frame in frames.values():
        row = frame.iloc[0]
        assert row["player_id"] == PLAYER_ID
        assert row["source_tournament_id"] == "17"
        assert row["source_season_id"] == "76986"
        assert row["raw_content_hash"]
        assert row["raw_blob_key"]
        assert row["league"] == "ENG-Premier League"
        assert row["season"] == "2526"

    # A successful Bronze commit may finalize both long manifest rows. Forced
    # offline replay then parses those same immutable blobs with no transport.
    finalize_materialized_results(runtime, results)
    assert endpoint_resume_plan(runtime.manifest_store, specs.values()) == {}
    requests_before = runtime.engine.metrics.snapshot()["request_count"]
    runtime.engine.sink = SuccessSink()
    replayed = replay_player_specs(runtime, list(specs.values()))
    assert len(replayed) == 2
    assert all(result.manifest.status == ManifestStatus.SUCCESS for result in replayed)
    assert all(result.replay_hit and not result.network_used for result in replayed)
    assert transport.calls == 0
    assert runtime.engine.metrics.snapshot()["request_count"] == requests_before


def test_repair_promotes_terminal_lineage_to_final_after_materialization(tmp_path):
    runtime, transport = _runtime(tmp_path)
    final_spec = _spec("event")
    repair_spec = _spec("event", freshness_key="repair-run-1")
    original = FIXTURES["event"].read_bytes()
    repaired_payload = json.loads(original)
    repaired_payload["event"]["repairMarker"] = "new-source-snapshot"
    repaired_body = json.dumps(
        repaired_payload, separators=(",", ":")
    ).encode("utf-8")

    old = runtime.engine.ingest_prefetched(
        final_spec,
        HttpPayload(200, original, provider_bytes=0),
        authorization=None,
    )
    finalize_materialized_results(runtime, [old])
    old_final = runtime.manifest_store.get(final_spec.key)
    repair = runtime.engine.ingest_prefetched(
        repair_spec,
        HttpPayload(200, repaired_body, provider_bytes=0),
        authorization=None,
    )

    # A failed/not-yet-run Bronze MERGE leaves the old canonical row untouched
    # and promotion itself refuses the deferred repair state.
    with pytest.raises(ValueError, match="committed terminal repair manifest"):
        promote_repaired_results(runtime, [repair])
    assert runtime.manifest_store.get(final_spec.key) == old_final

    finalize_materialized_results(runtime, [repair])
    promoted = promote_repaired_results(runtime, [repair])

    assert transport.calls == 0
    assert len(promoted) == 1
    canonical = runtime.manifest_store.get(final_spec.key)
    repaired = runtime.manifest_store.get(repair_spec.key)
    assert canonical.status == ManifestStatus.SUCCESS
    assert canonical.key.freshness_key == "final"
    assert canonical.raw_content_hash == repaired.raw_content_hash
    assert canonical.raw_content_hash != old_final.raw_content_hash
    assert canonical.attempts == repaired.attempts
    assert canonical.run_id == repaired.run_id
    canonical_body, canonical_raw = runtime.raw_store.load_bytes(
        final_spec.raw_target
    )
    assert canonical_body == repaired_body
    assert canonical_raw.content_hash == canonical.raw_content_hash

    # Retrying the same post-MERGE promotion is an exact lineage no-op.
    assert promote_repaired_results(runtime, [repair]) == promoted
    assert runtime.manifest_store.get(final_spec.key) == canonical

    weekly = promote_repaired_results(
        runtime,
        [repair],
        canonical_freshness_key="week-2026-W28",
    )
    assert weekly[0].key.freshness_key == "week-2026-W28"
    assert weekly[0].raw_content_hash == repaired.raw_content_hash


def test_player_materialization_rejects_target_mismatch(tmp_path):
    runtime, _ = _runtime(tmp_path)
    spec = _player_spec("player_profile")
    result = ingest_prefetched_records(
        runtime,
        specs={(PLAYER_ID, "player_profile"): spec},
        records={"profile": _player_record("player_profile")},
    )[0]
    row = dict(result.datasets["player_profile"].rows[0])
    row["player_id"] = "99999"
    from scrapers.sofascore.capture_engine import ParsedDataset

    poisoned = CaptureResult(
        manifest=result.manifest,
        datasets={"player_profile": ParsedDataset("player_profile", [row])},
        raw=result.raw,
    )
    with pytest.raises(ValueError, match="player row target mismatch"):
        materialize_player_datasets(
            MetadataScraper(),
            [poisoned],
            league="ENG-Premier League",
            season="2526",
        )


def _runner_player_scraper():
    scraper = MagicMock()
    scraper.__enter__.return_value = scraper
    scraper.__exit__.return_value = False
    scraper._resolve_player_ids_from_bronze.return_value = [PLAYER_ID]
    scraper._add_metadata.side_effect = MetadataScraper._add_metadata
    scraper.save_to_iceberg.side_effect = lambda **kwargs: (
        "iceberg.bronze." + kwargs["table_name"]
    )
    return scraper


def _patch_complete_season_player_universe(monkeypatch):
    from scrapers.sofascore import season_pipeline

    plan = MagicMock(complete=True)
    monkeypatch.setattr(
        season_pipeline, "plan_season_partition", lambda *args, **kwargs: plan
    )
    monkeypatch.setattr(
        season_pipeline, "squad_player_ids", lambda *args, **kwargs: ()
    )


def test_player_runner_offline_replay_writes_both_tables_without_network(
    tmp_path,
    monkeypatch,
):
    from dags.scripts import run_sofascore_scraper as runner

    monkeypatch.setenv("SOFASCORE_PLAYER_FRESHNESS_KEY", "fixture-week")
    _patch_complete_season_player_universe(monkeypatch)
    monkeypatch.setattr(runner, "_source_context", lambda *args: (17, 76986))
    runtime, transport = _runtime(tmp_path)
    specs = {
        (PLAYER_ID, endpoint): _player_spec(
            endpoint, freshness_key="fixture-week"
        )
        for endpoint in PLAYER_PATHS
    }
    ingest_prefetched_records(
        runtime,
        specs=specs,
        records={
            endpoint: _player_record(endpoint) for endpoint in PLAYER_PATHS
        },
    )
    # Offline replay is a new logical runner process: reuse durable raw/manifest
    # state, but start with a fresh metrics/transport instance.
    replay_transport = NoNetworkTransport()
    replay_engine = SofaScoreCaptureEngine(
        raw_store=runtime.raw_store,
        manifest_store=runtime.manifest_store,
        transport=replay_transport,
        run_id="offline-replay-run",
        task_id="player-capture",
        sink=DeferredCaptureSink(),
        rate_limiter=UnlimitedLimiter(),
        retry_policy=RetryPolicy(max_attempts=1),
        max_workers=2,
    )
    runtime = CaptureRuntime(
        replay_engine, runtime.manifest_store, runtime.raw_store
    )
    scraper = _runner_player_scraper()
    output = tmp_path / "player-replay.json"

    with patch("scrapers.sofascore.SofaScoreScraper", return_value=scraper):
        rc = runner._run_player_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(output),
            capture_runtime=runtime,
            workload_plan=None,
            offline_replay=True,
        )

    assert rc == 0
    assert transport.calls == 0
    assert replay_transport.calls == 0
    assert [
        call.kwargs["table_name"]
        for call in scraper.save_to_iceberg.call_args_list
    ] == [
        "sofascore_player_universe",
        "sofascore_player_profile",
        "sofascore_player_season_stats",
    ]
    assert all(runtime.manifest_store.get(spec.key).is_terminal for spec in specs.values())
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["traffic"]["request_count"] == 0
    assert result["profile_players"] == 1
    assert result["season_stats_players"] == 1


def test_player_runner_manifest_noop_is_exact_zero_traffic_before_browser(
    tmp_path,
    monkeypatch,
):
    from dags.scripts import run_sofascore_scraper as runner

    monkeypatch.setenv("SOFASCORE_PLAYER_FRESHNESS_KEY", "fixture-week")
    _patch_complete_season_player_universe(monkeypatch)
    monkeypatch.setattr(runner, "_source_context", lambda *args: (17, 76986))
    runtime, transport = _runtime(tmp_path, sink=SuccessSink())
    specs = [
        _player_spec(endpoint, freshness_key="fixture-week")
        for endpoint in PLAYER_PATHS
    ]
    for spec in specs:
        runtime.manifest_store.upsert(
            EndpointManifest(
                key=spec.key,
                status=ManifestStatus.SUCCESS,
                run_id="earlier-run",
                task_id="player-capture",
                attempts=1,
                row_count=1,
                http_status=200,
                raw_content_hash="a" * 64,
                raw_blob_key=f"blobs/{spec.key.endpoint}.json.gz",
            )
        )
    scraper = _runner_player_scraper()
    output = tmp_path / "player-noop.json"

    with patch("scrapers.sofascore.SofaScoreScraper", return_value=scraper):
        rc = runner._run_player_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(output),
            capture_runtime=runtime,
            workload_plan=None,
            offline_replay=False,
        )

    assert rc == 0
    assert transport.calls == 0
    # The complete universe is an idempotent local MERGE performed before any
    # player batch; no profile/stat write or paid lease is opened on this no-op.
    assert scraper.save_to_iceberg.call_count == 1
    assert (
        scraper.save_to_iceberg.call_args.kwargs["table_name"]
        == "sofascore_player_universe"
    )
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["traffic"] == {
        "paid_proxy_bytes": 0,
        "paid_proxy_mb": 0.0,
        "browser_sessions": 0,
        "browser_navigations": 0,
        "request_count": 0,
        "cache_hit_rate": 1.0,
        "endpoint_completeness": 1.0,
    }


def test_match_runner_long_manifest_noop_is_exact_zero_before_browser(
    tmp_path,
    monkeypatch,
):
    from dags.scripts import run_sofascore_scraper as runner

    monkeypatch.setattr(
        runner,
        "_resolve_match_ids_from_bronze",
        lambda *args, **kwargs: [EVENT_ID],
    )
    monkeypatch.setattr(runner, "_source_context", lambda *args: (17, 76986))
    runtime, transport = _runtime(tmp_path, sink=SuccessSink())
    specs = [_spec(endpoint) for endpoint in EVENT_PATHS]
    for spec in specs:
        runtime.manifest_store.upsert(_successful_manifest(spec))
    output = tmp_path / "match-noop.json"
    browser = MagicMock(side_effect=AssertionError("no-op opened a scraper"))

    with patch("scrapers.sofascore.SofaScoreScraper", browser):
        rc = runner._run_match_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(output),
            capture_runtime=runtime,
            workload_plan=None,
            offline_replay=False,
        )

    assert rc == 0
    assert transport.calls == 0
    browser.assert_not_called()
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["traffic"] == {
        "paid_proxy_bytes": 0,
        "paid_proxy_mb": 0.0,
        "browser_sessions": 0,
        "browser_navigations": 0,
        "request_count": 0,
        "cache_hit_rate": 1.0,
        "endpoint_completeness": 1.0,
    }


def test_compatibility_status_failure_keeps_long_manifest_replayable_without_network(
    tmp_path,
    monkeypatch,
):
    from dags.scripts import run_sofascore_scraper as runner

    runtime, transport = _runtime(tmp_path)
    specs = {(EVENT_ID, endpoint): _spec(endpoint) for endpoint in EVENT_PATHS}
    ingest_prefetched_records(
        runtime,
        specs=specs,
        records={endpoint: _record(endpoint) for endpoint in EVENT_PATHS},
    )
    monkeypatch.setattr(
        runner,
        "_resolve_match_ids_from_bronze",
        lambda *args, **kwargs: [EVENT_ID],
    )
    monkeypatch.setattr(runner, "_source_context", lambda *args: (17, 76986))
    monkeypatch.setattr(
        runner,
        "_tournament_canonical_url",
        lambda *args: "https://www.sofascore.com/tournament/premier-league/17",
    )
    from dags.utils import sofascore_dq

    passed = SimpleNamespace(require=lambda: None)
    for validator in (
        "validate_table_rows",
        "validate_lineup_semantics",
        "validate_event_participants",
        "validate_season_alignment",
    ):
        monkeypatch.setattr(sofascore_dq, validator, lambda *args, **kwargs: passed)

    def make_scraper(*, fail_status):
        scraper = MagicMock()
        scraper.__enter__.return_value = scraper
        scraper.__exit__.return_value = False
        scraper._add_metadata.side_effect = MetadataScraper._add_metadata

        def save(**kwargs):
            if (
                fail_status
                and kwargs["table_name"] == "sofascore_match_capture_status"
            ):
                raise RuntimeError("compatibility status write failed")
            return "iceberg.bronze." + kwargs["table_name"]

        scraper.save_to_iceberg.side_effect = save
        return scraper

    first_scraper = make_scraper(fail_status=True)
    first_output = tmp_path / "status-failed.json"
    with patch("scrapers.sofascore.SofaScoreScraper", return_value=first_scraper):
        first_rc = runner._run_match_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(first_output),
            capture_runtime=runtime,
            workload_plan=None,
            offline_replay=True,
        )

    assert first_rc == 1
    assert "compatibility status write failed" in json.loads(
        first_output.read_text(encoding="utf-8")
    )["errors"][0]
    assert all(
        not runtime.manifest_store.get(spec.key).is_terminal
        for spec in specs.values()
    )
    assert transport.calls == 0
    requests_before_retry = runtime.engine.metrics.snapshot()["request_count"]

    retry_scraper = make_scraper(fail_status=False)
    retry_output = tmp_path / "status-repaired.json"
    with patch("scrapers.sofascore.SofaScoreScraper", return_value=retry_scraper):
        retry_rc = runner._run_match_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(retry_output),
            capture_runtime=runtime,
            workload_plan=None,
            offline_replay=True,
        )

    assert retry_rc == 0
    assert all(
        runtime.manifest_store.get(spec.key).is_terminal
        for spec in specs.values()
    )
    assert transport.calls == 0
    repaired = json.loads(retry_output.read_text(encoding="utf-8"))
    assert repaired["capture_status_rows"] == 1
    assert repaired["traffic"]["request_count"] == requests_before_retry
    assert repaired["traffic"]["paid_proxy_bytes"] == 0
    assert repaired["traffic"]["browser_sessions"] == 0


def test_endpoint_resume_keeps_only_missing_or_nonterminal_endpoints(tmp_path):
    runtime, _ = _runtime(tmp_path, sink=SuccessSink())
    specs = [_spec(endpoint) for endpoint in EVENT_PATHS]
    event = specs[0]
    lineups = specs[1]
    runtime.manifest_store.upsert(
        EndpointManifest(
            key=event.key,
            status=ManifestStatus.SUCCESS,
            run_id="old-run",
            task_id="capture",
            attempts=1,
            row_count=1,
            http_status=200,
            raw_content_hash="a" * 64,
            raw_blob_key="blobs/a.json.gz",
        )
    )
    runtime.manifest_store.upsert(
        EndpointManifest(
            key=lineups.key,
            status=ManifestStatus.RETRYABLE_FAILURE,
            run_id="old-run",
            task_id="capture",
            attempts=2,
            row_count=0,
            http_status=429,
        )
    )

    plan = endpoint_resume_plan(runtime.manifest_store, specs)
    assert plan == {
        EVENT_ID: ("lineups", "statistics", "shotmap", "incidents")
    }


def test_prefetched_fixture_is_exact_raw_then_deferred_until_bronze_finalize(tmp_path):
    runtime, transport = _runtime(tmp_path)
    spec = _spec("event")
    body = FIXTURES["event"].read_bytes()
    results = ingest_prefetched_records(
        runtime,
        specs={(EVENT_ID, "event"): spec},
        records={"event": _record("event")},
    )

    assert transport.calls == 0
    assert len(results) == 1
    result = results[0]
    assert result.manifest.status == ManifestStatus.RETRYABLE_FAILURE
    assert result.manifest.error_type == "DeferredMaterialization"
    assert result.manifest.row_count == 4  # event + two teams + venue
    assert result.raw is not None
    assert result.raw.content_hash == hashlib.sha256(body).hexdigest()
    stored, stored_record = runtime.raw_store.load_bytes(spec.raw_target)
    assert stored == body
    assert stored_record.content_hash == result.raw.content_hash

    # The long manifest is not terminal until every related Bronze MERGE has
    # succeeded and the runner explicitly finalizes the capture.
    assert runtime.manifest_store.get(spec.key).is_terminal is False
    finalize_materialized_results(runtime, results)
    committed = runtime.manifest_store.get(spec.key)
    assert committed.status == ManifestStatus.SUCCESS
    assert committed.raw_content_hash == hashlib.sha256(body).hexdigest()
    assert committed.raw_blob_key == stored_record.blob_key

    lineage = raw_lineage_map(results)
    assert lineage[spec.key] == {
        "raw_content_hash": hashlib.sha256(body).hexdigest(),
        "raw_blob_key": stored_record.blob_key,
        "request_url": spec.url,
        "http_status": 200,
        "fetched_at": stored_record.fetched_at,
    }


def test_all_fixtures_share_one_raw_first_pipeline_and_finalize_independently(tmp_path):
    runtime, transport = _runtime(tmp_path)
    specs = {(EVENT_ID, endpoint): _spec(endpoint) for endpoint in EVENT_PATHS}
    records = {endpoint: _record(endpoint) for endpoint in EVENT_PATHS}
    results = ingest_prefetched_records(runtime, specs=specs, records=records)

    assert transport.calls == 0
    assert len(results) == 5
    assert {result.manifest.key.endpoint for result in results} == set(EVENT_PATHS)
    assert all(result.manifest.status == ManifestStatus.RETRYABLE_FAILURE for result in results)
    assert all(result.raw is not None for result in results)
    finalize_materialized_results(runtime, results)
    assert endpoint_resume_plan(runtime.manifest_store, specs.values()) == {}


def test_database_failure_replays_offline_without_transport_or_second_payload(tmp_path):
    sink = FailOnceSink()
    runtime, transport = _runtime(tmp_path, sink=sink)
    spec = _spec("statistics")
    body = FIXTURES["statistics"].read_bytes()
    response = HttpPayload(
        200,
        body,
        headers={"content-type": "application/json"},
        provider_bytes=0,
    )
    first = runtime.engine.ingest_prefetched(
        spec,
        response,
        authorization=None,
    )
    assert first.manifest.status == ManifestStatus.RETRYABLE_FAILURE
    assert first.manifest.error_type == "RuntimeError"
    assert transport.calls == 0
    request_count = runtime.engine.metrics.snapshot()["request_count"]

    sink.fail = False
    replayed = replay_event_specs(runtime, [spec])
    assert len(replayed) == 1
    assert replayed[0].manifest.status == ManifestStatus.SUCCESS
    assert replayed[0].replay_hit is True
    assert replayed[0].network_used is False
    assert replayed[0].raw.content_hash == hashlib.sha256(body).hexdigest()
    assert transport.calls == 0
    assert runtime.engine.metrics.snapshot()["request_count"] == request_count
    stored, _ = runtime.raw_store.load_bytes(spec.raw_target)
    assert stored == body


@pytest.mark.parametrize("offline_replay", [False, True])
def test_empty_bronze_schedule_fails_before_any_browser_or_source_fallback(
    tmp_path,
    monkeypatch,
    offline_replay,
):
    from dags.scripts import run_sofascore_scraper as runner

    monkeypatch.setattr(
        runner,
        "_resolve_match_ids_from_bronze",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        runner,
        "_source_context",
        lambda *args, **kwargs: pytest.fail("source context must not run"),
    )
    output = tmp_path / f"empty-{offline_replay}.json"
    browser = MagicMock(side_effect=AssertionError("browser must not start"))
    with patch("scrapers.sofascore.SofaScoreScraper", browser):
        rc = runner._run_match_capture(
            leagues=["ENG-Premier League"],
            season=2025,
            limit=None,
            output_path=str(output),
            capture_runtime=MagicMock(),
            workload_plan=None,
            offline_replay=offline_replay,
        )

    assert rc == 1
    browser.assert_not_called()
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert "refusing browser/source fallback" in payload["errors"][0]
    assert payload["traffic"] == {
        "paid_proxy_bytes": 0,
        "paid_proxy_mb": 0.0,
        "browser_sessions": 0,
        "browser_navigations": 0,
        "request_count": 0,
    }


def _successful_manifest(spec):
    return EndpointManifest(
        key=spec.key,
        status=ManifestStatus.SUCCESS,
        run_id="run",
        task_id="capture",
        attempts=1,
        row_count=1,
        http_status=200,
        raw_content_hash="a" * 64,
        raw_blob_key=f"blobs/{spec.key.endpoint}.json.gz",
    )


def test_resumed_compatibility_projection_loads_all_five_long_states():
    from dags.scripts.run_sofascore_scraper import (
        _complete_manifest_records_for_projection,
    )

    specs = {(EVENT_ID, endpoint): _spec(endpoint) for endpoint in EVENT_PATHS}
    store = InMemoryManifestStore()
    for spec in specs.values():
        store.upsert(_successful_manifest(spec))

    # Only incidents was fetched in this run; the other four were endpoint-level
    # resume hits and must still be read from the canonical long manifest.
    current = CaptureResult(manifest=store.get(specs[(EVENT_ID, "incidents")].key))
    records = _complete_manifest_records_for_projection(
        store,
        specs,
        [current],
    )
    assert {record.key.endpoint for record in records} == set(EVENT_PATHS)
    row = project_legacy_match_status(
        records,
        league="ENG-Premier League",
        season="2526",
        endpoints=tuple(EVENT_PATHS),
    )[0]
    assert row["capture_complete"] is True
    assert all(row[f"{endpoint}_status"] == "success" for endpoint in EVENT_PATHS)


def test_resumed_projection_fails_closed_when_any_long_state_is_missing():
    from dags.scripts.run_sofascore_scraper import (
        _complete_manifest_records_for_projection,
    )

    specs = {(EVENT_ID, endpoint): _spec(endpoint) for endpoint in EVENT_PATHS}
    store = InMemoryManifestStore()
    for (target_id, endpoint), spec in specs.items():
        if endpoint != "shotmap":
            store.upsert(_successful_manifest(spec))
    current = CaptureResult(manifest=store.get(specs[(EVENT_ID, "incidents")].key))
    with pytest.raises(RuntimeError, match="shotmap.*manifest"):
        _complete_manifest_records_for_projection(store, specs, [current])
