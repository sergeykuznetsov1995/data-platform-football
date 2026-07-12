from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from pyarrow import fs

from scrapers.sofascore.adapters import (
    LEGACY_CORE_ENDPOINTS,
    MANIFEST_COLUMNS,
    MANIFEST_KEY_COLUMNS,
    PooledCaptureTransport,
    PrefetchedCaptureTransport,
    TrinoManifestStore,
    manifest_to_row,
    project_legacy_match_status,
    raw_lineage_map,
    render_manifest_ddl,
)
from scrapers.sofascore.capture_engine import (
    EndpointSpec,
    HttpPayload,
    RetryPolicy,
    SofaScoreCaptureEngine,
)
from scrapers.sofascore.manifest import (
    EndpointManifest,
    InMemoryManifestStore,
    ManifestKey,
    ManifestStatus,
)
from scrapers.sofascore.raw_store import RawPayloadStore
from scripts.proxy_filter.budget import (
    BudgetAccountingError,
    SharedBudgetLedger,
    load_verified_policy,
)


def _key(endpoint="event", target_id="1", freshness="finished-v1"):
    return ManifestKey("17", "76986", "event", target_id, endpoint, freshness)


def _record(endpoint="event", status=ManifestStatus.SUCCESS, **overrides):
    values = {
        "key": _key(endpoint),
        "status": status,
        "run_id": "run-1",
        "task_id": "capture",
        "attempts": 1,
        "row_count": 1,
        "http_status": 200,
        "raw_content_hash": "a" * 64,
        "raw_blob_key": "blobs/a.json.gz",
        "request_url": f"https://example.invalid/{endpoint}",
        "updated_at": "2026-07-11T00:00:00+00:00",
    }
    values.update(overrides)
    return EndpointManifest(**values)


class FakeManager:
    catalog = "iceberg"

    def __init__(self):
        self.schemas = []
        self.executions = []
        self.rows = []
        self.merges = []

    def create_schema(self, schema):
        self.schemas.append(schema)

    def _execute(self, sql, fetch=False, params=None):
        self.executions.append((sql, fetch, params))
        return self.rows if fetch else None

    def insert_dataframe_atomic(self, schema, table, df, *, merge_keys):
        self.merges.append((schema, table, df.copy(), tuple(merge_keys)))
        return len(df)


def test_manifest_ddl_and_upsert_use_canonical_natural_key_merge():
    manager = FakeManager()
    store = TrinoManifestStore(manager)
    assert manager.schemas == ["ops"]
    ddl = manager.executions[0][0]
    assert ddl == render_manifest_ddl()
    assert "CREATE TABLE IF NOT EXISTS iceberg.ops.sofascore_capture_manifest" in ddl
    assert "partitioning = ARRAY['source_tournament_id', 'source_season_id']" in ddl

    record = _record()
    store.upsert(record)
    schema, table, frame, keys = manager.merges[0]
    assert (schema, table) == ("ops", "sofascore_capture_manifest")
    assert keys == MANIFEST_KEY_COLUMNS
    assert tuple(frame.columns) == MANIFEST_COLUMNS
    assert frame.iloc[0]["status"] == "success"


def test_trino_get_uses_bound_six_field_key_and_rehydrates_exact_state():
    manager = FakeManager()
    store = TrinoManifestStore(manager, ensure_table=False)
    record = _record("lineups", status=ManifestStatus.LEGITIMATE_EMPTY, row_count=0)
    row = manifest_to_row(record)
    manager.rows = [tuple(row[column] for column in MANIFEST_COLUMNS)]
    loaded = store.get(record.key)
    assert loaded == record
    sql, fetch, params = manager.executions[-1]
    assert fetch is True
    assert params == record.key.as_tuple()
    assert sql.count(" = ?") == 6
    assert "LIMIT 1" not in sql


def test_trino_get_fails_closed_when_ops_manifest_key_is_duplicated():
    manager = FakeManager()
    store = TrinoManifestStore(manager, ensure_table=False)
    record = _record()
    row = manifest_to_row(record)
    encoded = tuple(row[column] for column in MANIFEST_COLUMNS)
    manager.rows = [encoded, encoded]

    with pytest.raises(RuntimeError, match="natural key is duplicated"):
        store.get(record.key)


def test_trino_list_rejects_unknown_persisted_status():
    manager = FakeManager()
    store = TrinoManifestStore(manager, ensure_table=False)
    row = manifest_to_row(_record())
    row["status"] = "pending"
    manager.rows = [tuple(row[column] for column in MANIFEST_COLUMNS)]
    with pytest.raises(ValueError, match="pending"):
        store.list_for_run("run-1")


def test_pooled_transport_passes_budget_token_to_one_warmed_fetcher():
    seen = []

    def fetch(url, token):
        seen.append((url, token))
        return HttpPayload(200, b'{"events":[]}', provider_bytes=0)

    transport = PooledCaptureTransport(fetch)
    payload = transport.request("https://example.invalid/event", provider_budget=None)
    assert payload.body == b'{"events":[]}'
    assert seen == [("https://example.invalid/event", None)]


def test_prefetched_transport_is_zero_network_and_rejects_after_fact_paid_token():
    url = "https://example.invalid/event"
    transport = PrefetchedCaptureTransport(
        {url: HttpPayload(200, b'{"events":[]}', provider_bytes=0)}
    )
    assert transport.request(url, provider_budget=None).status_code == 200
    assert transport.calls == 1
    paid = MagicMock()
    with pytest.raises(BudgetAccountingError, match="preauthorization"):
        transport.request(url, provider_budget=paid)


class Limiter:
    def __init__(self):
        self.calls = 0

    def acquire(self):
        self.calls += 1
        return True


class NoNetwork:
    def request(self, url, *, provider_budget):
        raise AssertionError("ingest_prefetched must not call transport")


def _budget(tmp_path):
    metrics = {
        "browser_sessions": 1,
        "navigations": 1,
        "request_count": 1,
        "source_request_count": 1,
        "completed_matches": 25,
        "completed_players": 50,
        "matches_per_second": 1.0,
        "players_per_second": 1.0,
        "p50_duration_ms": 1,
        "p95_duration_ms": 1,
        "cache_hit_rate": 0.0,
        "replay_hit_rate": 0.0,
        "endpoint_completeness": 1.0,
    }
    samples = [
        {
            "run_id": f"canary-{index}",
            "budget_eligible": True,
            "cohort": "25_matches_50_players",
            "mode": "cold",
            "proxy_exit_hash": f"exit-hash-{index % 5:02d}",
            "total_provider_bytes": 100,
            "endpoint_provider_bytes": {"event": 100},
            "endpoint_request_provider_bytes": {"event": [100]},
            "metrics": metrics,
        }
        for index in range(20)
    ]
    samples.extend(
        {
            "run_id": f"benchmark-{mode}",
            "budget_eligible": False,
            "cohort": "25_matches_50_players",
            "mode": mode,
            "proxy_exit_hash": "exit-hash-benchmark",
            "total_provider_bytes": 0,
            "endpoint_provider_bytes": {"event": 0},
            "endpoint_request_provider_bytes": {"event": [0]},
            "metrics": {},
        }
        for mode in ("no_op", "offline_replay", "single_endpoint_resume")
    )
    artifact = tmp_path / "canary.json"
    artifact.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "sofascore",
                "meter": "proxy_filter_provider_path_v2",
                "budget_derivation": "max_measured_total_and_per_run_endpoint_max_v1",
                "verified": True,
                "samples": samples,
            }
        )
    )
    return SharedBudgetLedger(
        tmp_path / "ledger.json",
        load_verified_policy(artifact, allow_legacy_v1=True),
    )


def _paid_spec():
    return EndpointSpec(
        key=_key(),
        url="https://www.sofascore.com/api/v1/event/1",
        schema_validator=lambda value: isinstance(value.get("events"), list),
        empty_predicate=lambda value: value["events"] == [],
        parsers={"events": lambda value: value["events"]},
        paid_proxy=True,
    )


def test_preauthorized_warmed_capture_ingests_without_second_network_call(tmp_path):
    raw_store = RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))
    manifests = InMemoryManifestStore()
    limiter = Limiter()
    budget = _budget(tmp_path)
    engine = SofaScoreCaptureEngine(
        raw_store=raw_store,
        manifest_store=manifests,
        transport=NoNetwork(),
        run_id="dag-run",
        task_id="match-capture",
        budget=budget,
        rate_limiter=limiter,
        retry_policy=RetryPolicy(max_attempts=1),
    )
    spec = _paid_spec()
    authorization = engine.authorize_request(spec)
    assert authorization.max_provider_bytes == 100
    assert limiter.calls == 1

    result = engine.ingest_prefetched(
        spec,
        HttpPayload(
            200,
            b'{"events":[{"id":1}]}',
            headers={"content-type": "application/json"},
            provider_bytes=71,
            browser_sessions=0,
            navigations=0,
        ),
        authorization=authorization,
    )
    assert result.manifest.status == ManifestStatus.SUCCESS
    assert result.raw is not None
    assert result.raw.content_hash == result.manifest.raw_content_hash
    assert budget.snapshot("dag-run")["spent_provider_bytes"] == 71
    lineage = raw_lineage_map([result])
    assert lineage[spec.key]["raw_blob_key"] == result.raw.blob_key
    assert engine.metrics.snapshot()["endpoint_completeness"] == 1.0


def test_legacy_projection_preserves_terminal_and_retryable_semantics():
    records = [
        _record("event"),
        _record(
            "lineups",
            status=ManifestStatus.LEGITIMATE_EMPTY,
            row_count=0,
        ),
        _record(
            "statistics",
            status=ManifestStatus.NOT_SUPPORTED,
            row_count=0,
            http_status=404,
            raw_content_hash=None,
            raw_blob_key=None,
        ),
        _record(
            "shotmap",
            status=ManifestStatus.RETRYABLE_FAILURE,
            row_count=0,
            http_status=429,
            raw_content_hash=None,
            raw_blob_key=None,
        ),
    ]
    row = project_legacy_match_status(
        records,
        league="ENG-Premier League",
        season="2526",
    )[0]
    assert tuple(name.removesuffix("_status") for name in row if name.endswith("_status")) == LEGACY_CORE_ENDPOINTS
    assert row["event_status"] == "success"
    assert row["lineups_status"] == "not_available"
    assert row["statistics_status"] == "not_available"
    assert row["shotmap_status"] == "rate_limited"
    assert row["capture_complete"] is False


def test_legacy_projection_can_add_incidents_without_changing_core_default():
    records = [_record(endpoint) for endpoint in (*LEGACY_CORE_ENDPOINTS, "incidents")]
    core = project_legacy_match_status(records, league="EPL", season="2526")[0]
    extended = project_legacy_match_status(
        records,
        league="EPL",
        season="2526",
        endpoints=(*LEGACY_CORE_ENDPOINTS, "incidents"),
    )[0]
    assert "incidents_status" not in core
    assert extended["incidents_status"] == "success"
    assert extended["capture_complete"] is True
