from __future__ import annotations

import json
import threading
import time
from collections import deque

import pytest
from pyarrow import fs

from scrapers.sofascore.capture_engine import (
    EndpointSpec,
    HttpPayload,
    OfflineReplayMiss,
    RetryPolicy,
    SofaScoreCaptureEngine,
)
from scrapers.sofascore.manifest import (
    InMemoryManifestStore,
    ManifestKey,
    ManifestStatus,
)
from scrapers.sofascore.raw_store import RawPayloadStore
from scripts.proxy_filter.budget import (
    ProductionBudgetUnavailable,
    SharedBudgetLedger,
    load_verified_policy,
)


class UnlimitedLimiter:
    def __init__(self):
        self.calls = 0

    def acquire(self):
        self.calls += 1
        return True


class FakeTransport:
    def __init__(self, responses=()):
        self.responses = deque(responses)
        self.calls = []

    def request(self, url, *, provider_budget):
        self.calls.append((url, provider_budget))
        if not self.responses:
            raise AssertionError("unexpected network call")
        response = self.responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response


class RecordingSink:
    def __init__(self, raw_store, *, fail_times=0):
        self.raw_store = raw_store
        self.fail_times = fail_times
        self.calls = []

    def write(self, key, datasets, raw):
        # This assertion proves raw commit happens before any normalized write.
        body, stored = self.raw_store.load_bytes(raw.target)
        assert stored.content_hash == raw.content_hash
        assert body
        self.calls.append((key, datasets, raw))
        if self.fail_times:
            self.fail_times -= 1
            raise RuntimeError("simulated Iceberg failure")


def _raw_store(tmp_path):
    return RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))


def _key(target_id="1", endpoint="event"):
    return ManifestKey("17", "76986", "event", target_id, endpoint, "finished-v1")


def _validator(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("events"), list):
        raise ValueError("expected events array")


def _spec(target_id="1", endpoint="event", *, paid=False, parser=None, **kwargs):
    return EndpointSpec(
        key=_key(target_id, endpoint),
        url=f"https://www.sofascore.com/api/v1/event/{target_id}/{endpoint}",
        schema_validator=_validator,
        empty_predicate=lambda payload: payload["events"] == [],
        parsers=parser
        or {
            "events": lambda payload: [
                {"id": row["id"], "name": row.get("name")}
                for row in payload["events"]
            ]
        },
        paid_proxy=paid,
        **kwargs,
    )


def _engine(
    tmp_path,
    transport,
    *,
    raw_store=None,
    manifests=None,
    sink=None,
    budget=None,
    limiter=None,
    sleep=lambda _: None,
    max_workers=2,
):
    raw_store = raw_store or _raw_store(tmp_path)
    return SofaScoreCaptureEngine(
        raw_store=raw_store,
        manifest_store=manifests or InMemoryManifestStore(),
        transport=transport,
        run_id="dag-run",
        task_id="capture",
        sink=sink,
        budget=budget,
        rate_limiter=limiter or UnlimitedLimiter(),
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=1, max_delay_seconds=9),
        sleep=sleep,
        max_workers=max_workers,
    )


def test_one_exact_payload_feeds_multiple_parsers_and_raw_precedes_sink(tmp_path):
    raw_store = _raw_store(tmp_path)
    transport = FakeTransport(
        [
            HttpPayload(
                200,
                b'{"events":[{"id":7,"name":"A"}]}',
                provider_bytes=0,
                browser_sessions=1,
                navigations=1,
            )
        ]
    )
    seen_payload_ids = []

    def ids(payload):
        seen_payload_ids.append(id(payload))
        return [{"id": row["id"]} for row in payload["events"]]

    def names(payload):
        seen_payload_ids.append(id(payload))
        return [{"id": row["id"], "name": row["name"]} for row in payload["events"]]

    sink = RecordingSink(raw_store)
    engine = _engine(tmp_path, transport, raw_store=raw_store, sink=sink)
    result = engine.capture(_spec(parser={"ids": ids, "names": names}))

    assert result.manifest.status == ManifestStatus.SUCCESS
    assert result.manifest.row_count == 2
    assert len(set(seen_payload_ids)) == 1
    assert len(transport.calls) == 1
    assert len(sink.calls) == 1
    assert engine.metrics.snapshot()["request_count"] == 1


def test_terminal_endpoint_resume_is_exact_zero_byte_zero_session_noop(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifests = InMemoryManifestStore()
    first = _engine(
        tmp_path,
        FakeTransport([HttpPayload(200, b'{"events":[{"id":1}]}', provider_bytes=0)]),
        raw_store=raw_store,
        manifests=manifests,
    )
    first.capture(_spec())

    offline_transport = FakeTransport()
    noop = _engine(
        tmp_path,
        offline_transport,
        raw_store=raw_store,
        manifests=manifests,
    )
    result = noop.capture(_spec())
    metrics = noop.metrics.snapshot()
    assert result.cache_hit is True
    assert metrics["request_count"] == 0
    assert metrics["paid_proxy_bytes"] == 0
    assert metrics["browser_sessions"] == 0
    assert metrics["navigations"] == 0
    assert metrics["cache_hit_rate"] == 1.0
    assert offline_transport.calls == []


def test_sink_failure_replays_committed_raw_without_network(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifests = InMemoryManifestStore()
    failing_sink = RecordingSink(raw_store, fail_times=1)
    first_transport = FakeTransport(
        [HttpPayload(200, b'{"events":[{"id":1}]}', provider_bytes=0)]
    )
    first = _engine(
        tmp_path,
        first_transport,
        raw_store=raw_store,
        manifests=manifests,
        sink=failing_sink,
    )
    failed = first.capture(_spec())
    assert failed.manifest.status == ManifestStatus.RETRYABLE_FAILURE

    succeeding_sink = RecordingSink(raw_store)
    no_network = FakeTransport()
    replay = _engine(
        tmp_path,
        no_network,
        raw_store=raw_store,
        manifests=manifests,
        sink=succeeding_sink,
    ).capture(_spec(), offline=True)
    assert replay.manifest.status == ManifestStatus.SUCCESS
    assert replay.replay_hit is True
    assert replay.network_used is False
    assert no_network.calls == []


def test_offline_missing_raw_fails_before_network(tmp_path):
    transport = FakeTransport()
    engine = _engine(tmp_path, transport)
    with pytest.raises(OfflineReplayMiss):
        engine.capture(_spec(), offline=True)
    assert transport.calls == []


@pytest.mark.parametrize("status", [403, 429, 500, 503])
def test_retryable_http_errors_never_commit_success(tmp_path, status):
    responses = [
        HttpPayload(status, b'{"error":"blocked"}', provider_bytes=0)
        for _ in range(3)
    ]
    sleeps = []
    limiter = UnlimitedLimiter()
    engine = _engine(
        tmp_path,
        FakeTransport(responses),
        limiter=limiter,
        sleep=sleeps.append,
    )
    result = engine.capture(_spec())
    assert result.manifest.status == ManifestStatus.RETRYABLE_FAILURE
    assert result.manifest.http_status == status
    assert result.manifest.attempts == 3
    assert limiter.calls == 3  # rate limit is applied to every actual request
    assert sleeps == [1, 2]


def test_retry_after_header_overrides_exponential_backoff(tmp_path):
    sleeps = []
    transport = FakeTransport(
        [
            HttpPayload(429, b"{}", headers={"Retry-After": "7"}, provider_bytes=0),
            HttpPayload(200, b'{"events":[{"id":1}]}', provider_bytes=0),
        ]
    )
    result = _engine(tmp_path, transport, sleep=sleeps.append).capture(_spec())
    assert result.manifest.status == ManifestStatus.SUCCESS
    assert result.manifest.attempts == 2
    assert sleeps == [7]


def test_legitimate_empty_not_supported_and_schema_drift_are_distinct(tmp_path):
    empty = _engine(
        tmp_path / "empty",
        FakeTransport([HttpPayload(200, b'{"events":[]}', provider_bytes=0)]),
    ).capture(_spec())
    assert empty.manifest.status == ManifestStatus.LEGITIMATE_EMPTY

    unsupported = _engine(
        tmp_path / "unsupported",
        FakeTransport([HttpPayload(404, b'{"error":"missing"}', provider_bytes=0)]),
    ).capture(_spec())
    assert unsupported.manifest.status == ManifestStatus.NOT_SUPPORTED

    drift = _engine(
        tmp_path / "drift",
        FakeTransport([HttpPayload(200, b'{"matches":[]}', provider_bytes=0)]),
    ).capture(_spec())
    assert drift.manifest.status == ManifestStatus.SCHEMA_ERROR
    assert drift.manifest.raw_content_hash

    invalid_json = _engine(
        tmp_path / "invalid",
        FakeTransport([HttpPayload(200, b"<html>challenge</html>", provider_bytes=0)]),
    ).capture(_spec())
    assert invalid_json.manifest.status == ManifestStatus.SCHEMA_ERROR


def test_schema_error_replays_after_parser_fix_without_source_access(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifests = InMemoryManifestStore()
    first = _engine(
        tmp_path,
        FakeTransport([HttpPayload(200, b'{"matches":[{"id":9}]}', provider_bytes=0)]),
        raw_store=raw_store,
        manifests=manifests,
    ).capture(_spec())
    assert first.manifest.status == ManifestStatus.SCHEMA_ERROR

    fixed = EndpointSpec(
        key=_key(),
        url="https://www.sofascore.com/api/v1/event/1/event",
        schema_validator=lambda payload: isinstance(payload.get("matches"), list),
        empty_predicate=lambda payload: payload["matches"] == [],
        parsers={"events": lambda payload: payload["matches"]},
        paid_proxy=False,
    )
    no_network = FakeTransport()
    replay = _engine(
        tmp_path,
        no_network,
        raw_store=raw_store,
        manifests=manifests,
    ).capture(fixed, offline=True)
    assert replay.manifest.status == ManifestStatus.SUCCESS
    assert replay.replay_hit is True
    assert no_network.calls == []


def test_endpoint_resume_fetches_only_missing_endpoint(tmp_path):
    raw_store = _raw_store(tmp_path)
    manifests = InMemoryManifestStore()
    initial = _engine(
        tmp_path,
        FakeTransport([HttpPayload(200, b'{"events":[{"id":1}]}', provider_bytes=0)]),
        raw_store=raw_store,
        manifests=manifests,
    )
    initial.capture(_spec(endpoint="event"))

    transport = FakeTransport(
        [HttpPayload(200, b'{"events":[{"id":1}]}', provider_bytes=0)]
    )
    resumed_engine = _engine(
        tmp_path,
        transport,
        raw_store=raw_store,
        manifests=manifests,
    )
    results = resumed_engine.capture_many(
        [_spec(endpoint="event"), _spec(endpoint="lineups")]
    )
    assert [result.cache_hit for result in results] == [True, False]
    assert len(transport.calls) == 1
    assert transport.calls[0][0].endswith("/lineups")


def _verified_budget(tmp_path):
    metrics = {
        "browser_sessions": 1,
        "navigations": 1,
        "request_count": 1,
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
    samples = []
    for index in range(20):
        samples.append(
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
        )
    for mode in ("no_op", "offline_replay", "single_endpoint_resume"):
        samples.append(
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
        )
    artifact = tmp_path / "canary.json"
    artifact.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "sofascore",
                "meter": "proxy_filter_provider_path_v2",
                "verified": True,
                "samples": samples,
            }
        )
    )
    return SharedBudgetLedger(
        tmp_path / "ledger.json", load_verified_policy(artifact)
    )


def test_paid_capture_requires_verified_budget_before_transport(tmp_path):
    transport = FakeTransport()
    engine = _engine(tmp_path, transport)
    with pytest.raises(ProductionBudgetUnavailable):
        engine.capture(_spec(paid=True))
    assert transport.calls == []


def test_paid_response_uses_provider_reservation_and_real_meter(tmp_path):
    budget = _verified_budget(tmp_path)
    transport = FakeTransport(
        [
            HttpPayload(
                200,
                b'{"events":[{"id":1}]}',
                provider_bytes=73,
                browser_sessions=1,
                navigations=1,
            )
        ]
    )
    engine = _engine(tmp_path, transport, budget=budget)
    result = engine.capture(_spec(paid=True))
    token = transport.calls[0][1]
    assert token.run_id == "dag-run"
    assert token.max_provider_bytes == 100
    assert result.manifest.provider_bytes == 73
    assert budget.snapshot("dag-run")["spent_provider_bytes"] == 73
    assert engine.metrics.snapshot()["paid_proxy_bytes"] == 73


def test_bounded_concurrency_never_exceeds_configured_workers(tmp_path):
    class ConcurrentTransport:
        def __init__(self):
            self.active = 0
            self.maximum = 0
            self.lock = threading.Lock()

        def request(self, url, *, provider_budget):
            with self.lock:
                self.active += 1
                self.maximum = max(self.maximum, self.active)
            time.sleep(0.01)
            with self.lock:
                self.active -= 1
            return HttpPayload(200, b'{"events":[{"id":1}]}', provider_bytes=0)

    transport = ConcurrentTransport()
    engine = _engine(tmp_path, transport, max_workers=2)
    results = engine.capture_many([_spec(str(index)) for index in range(8)])
    assert len(results) == 8
    assert 1 < transport.maximum <= 2
