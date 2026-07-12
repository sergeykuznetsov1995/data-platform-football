from __future__ import annotations

from collections import deque
from types import SimpleNamespace

import pytest
from pyarrow import fs

from scrapers.sofascore.capture_engine import (
    EndpointSpec,
    RetryPolicy,
    SofaScoreCaptureEngine,
)
from scrapers.sofascore.live_capture import (
    LeaseBackedCamoufoxTransport,
    capture_live_dynamic_specs,
    capture_live_specs,
    hash_proxy_exit,
)
from scrapers.sofascore.manifest import InMemoryManifestStore, ManifestKey
from scrapers.sofascore.pipeline import CaptureRuntime, DeferredCaptureSink
from scrapers.sofascore.raw_store import RawPayloadStore
from scripts.proxy_filter.budget import BudgetAccountingError
from scrapers.sofascore.workload_plan import WorkloadAllocation, _signed_plan


pytestmark = pytest.mark.unit


class _Limiter:
    def __init__(self):
        self.calls = 0

    def acquire(self):
        self.calls += 1
        return True


class _Budget:
    def __init__(self, hard_run_bytes=10_000):
        self.policy = SimpleNamespace(
            hard_run_bytes=hard_run_bytes,
            artifact_id="a" * 64,
        )
        self.reservations = {}
        self.spent = 0
        self.counter = 0
        self.run_ids = []

    def reserve(self, run_id, endpoint):
        self.run_ids.append(run_id)
        self.counter += 1
        token = f"reservation-{self.counter}"
        self.reservations[token] = endpoint
        return token, self.policy.hard_run_bytes

    def finish(self, run_id, token, *, reported_provider_bytes=None):
        assert token in self.reservations
        del self.reservations[token]
        amount = int(reported_provider_bytes or 0)
        self.spent += amount
        assert self.spent <= self.policy.hard_run_bytes
        return amount

    def cancel(self, run_id, token):
        self.reservations.pop(token, None)


def _stats(
    total,
    *,
    closed=False,
    source="sofascore",
    max_bytes=10_000,
    dagrun_budget_bytes=10_000,
    artifact_id="a" * 64,
    fingerprint="f" * 16,
    plan_digest="b" * 64,
    allocation=None,
    base_run_id="run-1",
    workload_phase="targets",
):
    allocation = allocation or WorkloadAllocation(
        allocation_id="alloc-" + "1" * 32,
        task_id="capture",
        scope="match",
        workload_class="match_batch_25",
        batch_index=0,
        units=("1",),
        budget_bytes=max_bytes,
    )
    return SimpleNamespace(
        lease_id="lease-1",
        up_bytes=total // 4,
        down_bytes=total - total // 4,
        total_bytes=total,
        max_bytes=max_bytes,
        dagrun_total_bytes=total,
        dagrun_budget_bytes=dagrun_budget_bytes,
        daily_total_bytes=total,
        daily_budget_bytes=1_000_000,
        active_tunnels=0,
        closed=closed,
        expired=False,
        budget_exceeded=False,
        source=source,
        upstream_fingerprint=fingerprint,
        budget_artifact_id=artifact_id,
        plan_digest=plan_digest if source == "sofascore" else "",
        allocation_id=(allocation.allocation_id if source == "sofascore" else ""),
        allocation_task_id=(allocation.task_id if source == "sofascore" else ""),
        allocation_scope=(allocation.scope if source == "sofascore" else ""),
        allocation_class=(
            allocation.workload_class if source == "sofascore" else ""
        ),
        allocation_batch_index=(
            allocation.batch_index if source == "sofascore" else -1
        ),
        allocation_units=(allocation.units if source == "sofascore" else ()),
        allocation_budget_bytes=(
            allocation.budget_bytes if source == "sofascore" else 0
        ),
        allocation_spent_provider_bytes=total,
        allocation_remaining_provider_bytes=max(0, max_bytes - total),
        endpoint_request_provider_bytes={},
        base_run_id=(base_run_id if source == "sofascore" else ""),
        workload_phase=(workload_phase if source == "sofascore" else ""),
        phase_plan_digest=(plan_digest if source == "sofascore" else ""),
        parent_run_cap_bytes=(dagrun_budget_bytes if source == "sofascore" else 0),
        parent_run_spent_provider_bytes=(total if source == "sofascore" else 0),
    )


class _LeaseClient:
    def __init__(self, totals, *, final_total, token="lease-secret", events=None):
        self.totals = deque(totals)
        self.final_total = final_total
        self.lease = SimpleNamespace(
            lease_id="lease-1",
            token=token,
            proxy_url="http://proxy_filter:8900",
            max_bytes=10_000,
            expires_at=2_000_000_000.0,
            source="sofascore",
        )
        self.acquire_calls = []
        self.stats_calls = 0
        self.close_calls = 0
        self.source = "sofascore"
        self.events = events
        self.plan = None
        self.allocation = None
        self.endpoint_counter = 0

    def acquire(self, **kwargs):
        self.acquire_calls.append(kwargs)
        self.source = kwargs["source"]
        self.lease.source = self.source
        self.plan = kwargs.get("workload_plan")
        if self.plan is not None:
            self.allocation = next(
                item
                for item in self.plan.allocations
                if item.allocation_id == kwargs["allocation_id"]
            )
            self.lease.max_bytes = self.allocation.budget_bytes
        return self.lease

    def _stats(self, total, *, closed=False, **values):
        if self.plan is not None:
            base_run_id, workload_phase = self.plan.run_id.rsplit("::", 1)
        else:
            base_run_id, workload_phase = "", ""
        return _stats(
            total,
            closed=closed,
            source=self.source,
            max_bytes=(
                self.allocation.budget_bytes if self.allocation else 10_000
            ),
            dagrun_budget_bytes=(
                self.plan.run_cap_bytes if self.plan else 10_000
            ),
            plan_digest=(self.plan.plan_digest if self.plan else "b" * 64),
            allocation=self.allocation,
            base_run_id=base_run_id,
            workload_phase=workload_phase,
            **values,
        )

    def stats(self, lease):
        self.stats_calls += 1
        return self._stats(self.totals.popleft())

    def begin_endpoint(self, lease, endpoint):
        self.endpoint_counter += 1
        return f"endpoint-{self.endpoint_counter}"

    def finish_endpoint(self, lease, request_id):
        return self.stats(lease)

    def close(self, lease, **kwargs):
        self.close_calls += 1
        if self.events is not None:
            self.events.append("lease_close")
        return self._stats(self.final_total, closed=True)

    def playwright_proxy(self, lease):
        return {
            "server": lease.proxy_url,
            "username": "lease",
            "password": lease.token,
        }


class _Capture:
    def __init__(self, responses, *, exit_ip="203.0.113.7", events=None):
        self.responses = deque(responses)
        self.exit_ip = exit_ip
        self.enter_calls = 0
        self.exit_calls = 0
        self.warm_calls = 0
        self.fetch_paths = []
        self._navigation_count = 0
        self._source_request_count = 0
        self._limiter = None
        self.proxy = None
        self.events = events

    def configure(self, *, proxy, request_limiter):
        self.proxy = proxy
        self._limiter = request_limiter
        return self

    def __enter__(self):
        self.enter_calls += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit_calls += 1
        if self.events is not None:
            self.events.append("browser_close")
        return False

    def _request(self):
        assert self._limiter()
        self._source_request_count += 1

    def warm_exact_json(self, url):
        self._request()
        self.warm_calls += 1
        self._navigation_count += 1

    def fetch_api_json(self, path):
        self._request()
        self.fetch_paths.append(path)
        return self.responses.popleft()

    def probe_proxy_exit(self):
        self._request()
        return self.exit_ip


class _WarmFailureCapture(_Capture):
    def warm_exact_json(self, url):
        self._request()
        self.warm_calls += 1
        self._navigation_count += 1
        raise RuntimeError("anchor proof failed")


class _TransportFactory:
    def __init__(self, client, capture, **transport_kwargs):
        self.client = client
        self.capture = capture
        self.transport_kwargs = transport_kwargs
        self.calls = 0
        self.transport = None

    def __call__(self, engine, **kwargs):
        self.calls += 1

        if self.transport_kwargs.get("mode", "production") == "production" and not kwargs.get(
            "workload_plan"
        ):
            allocation = WorkloadAllocation(
                allocation_id="alloc-" + "1" * 32,
                task_id=engine.task_id,
                scope="match",
                workload_class="match_batch_25",
                batch_index=0,
                units=("1",),
                budget_bytes=engine.budget.policy.hard_run_bytes,
            )
            plan = _signed_plan(
                artifact_id=engine.budget.policy.artifact_id,
                dag_id="dag_ingest_sofascore",
                run_id=engine.run_id,
                player_universe_ids=(),
                allocations=(allocation,),
                control_token="c" * 32,
            )
            kwargs.update(
                workload_plan=plan,
                allocation_id=allocation.allocation_id,
                attempt_id="1",
            )

        def capture_factory(**capture_kwargs):
            return self.capture.configure(**capture_kwargs)

        self.transport = LeaseBackedCamoufoxTransport(
            engine,
            **kwargs,
            control_url="http://proxy_filter:8899",
            lease_client_factory=lambda _url: self.client,
            capture_factory=capture_factory,
            **self.transport_kwargs,
        )
        return self.transport


def _runtime(tmp_path, *, hard_run_bytes=10_000):
    raw = RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))
    manifests = InMemoryManifestStore()
    limiter = _Limiter()
    engine = SofaScoreCaptureEngine(
        raw_store=raw,
        manifest_store=manifests,
        transport=SimpleNamespace(),
        sink=DeferredCaptureSink(),
        run_id="run-1::targets",
        task_id="capture",
        budget=_Budget(hard_run_bytes),
        rate_limiter=limiter,
        retry_policy=RetryPolicy(
            max_attempts=3,
            base_delay_seconds=0,
            max_delay_seconds=0,
        ),
        sleep=lambda _delay: None,
    )
    return CaptureRuntime(engine, manifests, raw), limiter


def _spec(target_id, endpoint="event"):
    return EndpointSpec(
        key=ManifestKey("17", "76986", "event", str(target_id), endpoint, "final"),
        url=f"https://www.sofascore.com/api/v1/event/{target_id}/{endpoint}",
        schema_validator=lambda payload: (
            isinstance(payload, dict) and isinstance(payload.get("items"), list)
        ),
        empty_predicate=lambda payload: payload["items"] == [],
        parsers={"items": lambda payload: payload["items"]},
        paid_proxy=True,
    )


def _record(body, status=200, *, challenge=False):
    return {
        "status": status,
        "body": body,
        "headers": {"content-type": "application/json"},
        "challenge": challenge,
    }


def test_empty_live_plan_never_constructs_lease_or_browser(tmp_path):
    runtime, _ = _runtime(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("empty plan opened a paid transport")

    results, traffic = capture_live_specs(
        runtime,
        [],
        canonical_url="https://www.sofascore.com/event/1",
        scope="ENG-Premier League:2526",
        entity="match_capture",
        transport_factory=forbidden,
    )

    assert results == []
    assert traffic["paid_proxy_bytes"] == 0
    assert traffic["browser_sessions"] == 0
    assert traffic["source_request_count"] == 0


def test_valid_retained_raw_replays_without_lease_or_browser(tmp_path):
    runtime, _ = _runtime(tmp_path)
    spec = _spec(1)
    runtime.raw_store.store_bytes(
        spec.raw_target,
        b'{"items":[{"id":1}]}',
        request_url=spec.url,
        http_status=200,
        response_headers={"content-type": "application/json"},
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("raw replay opened a paid transport")

    results, traffic = capture_live_specs(
        runtime,
        [spec],
        canonical_url="https://www.sofascore.com/event/1",
        scope="ENG-Premier League:2526",
        entity="match_capture",
        transport_factory=forbidden,
    )

    assert len(results) == 1
    assert results[0].replay_hit is True
    assert traffic["paid_proxy_bytes"] == 0
    assert traffic["browser_sessions"] == 0


def test_initial_stats_failure_closes_lease_before_any_browser(tmp_path):
    runtime, _ = _runtime(tmp_path)
    client = _LeaseClient([1], final_total=1)
    capture = _Capture([_record(b'{"items":[]}')])
    factory = _TransportFactory(client, capture)

    with pytest.raises(BudgetAccountingError, match="already has provider traffic"):
        capture_live_specs(
            runtime,
            [_spec(1)],
            canonical_url="https://www.sofascore.com/event/1",
            scope="ENG-Premier League:2526",
            entity="match_capture",
            transport_factory=factory,
        )

    assert client.close_calls == 1
    assert capture.enter_calls == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("source", "sofascore_canary"),
        ("budget_artifact_id", "b" * 64),
        ("dagrun_budget_bytes", 9999),
        ("max_bytes", 9999),
    ],
)
def test_stats_provenance_mismatch_closes_lease(field, value, tmp_path):
    runtime, _ = _runtime(tmp_path)

    class MismatchedClient(_LeaseClient):
        def stats(self, lease):
            self.stats_calls += 1
            stats = self._stats(0)
            setattr(stats, field, value)
            return stats

    client = MismatchedClient([], final_total=0)
    capture = _Capture([_record(b'{"items":[]}')])
    factory = _TransportFactory(client, capture)

    with pytest.raises(BudgetAccountingError, match="policy provenance"):
        capture_live_specs(
            runtime,
            [_spec(1)],
            canonical_url="https://www.sofascore.com/event/1",
            scope="ENG-Premier League:2526",
            entity="match_capture",
            transport_factory=factory,
        )

    assert client.close_calls == 1
    assert capture.enter_calls == 0


def test_sticky_upstream_fingerprint_cannot_change_mid_lease(tmp_path):
    runtime, _ = _runtime(tmp_path)

    class DriftingClient(_LeaseClient):
        def stats(self, lease):
            self.stats_calls += 1
            return self._stats(
                0,
                fingerprint=("f" if self.stats_calls == 1 else "e") * 16,
            )

    client = DriftingClient([], final_total=0)
    capture = _Capture([_record(b'{"items":[]}')])
    factory = _TransportFactory(client, capture)

    with pytest.raises(BudgetAccountingError):
        capture_live_specs(
            runtime,
            [_spec(1)],
            canonical_url="https://www.sofascore.com/event/1",
            scope="ENG-Premier League:2526",
            entity="match_capture",
            transport_factory=factory,
        )

    assert client.close_calls == 1
    assert capture.enter_calls == 0


def test_two_endpoints_share_one_lease_browser_and_are_not_double_paced(tmp_path):
    runtime, limiter = _runtime(tmp_path)
    close_order = []
    client = _LeaseClient(
        [0, 0, 100, 100, 250],
        final_total=250,
        events=close_order,
    )
    capture = _Capture(
        [_record(b'{"items":[{"id":1}]}'), _record(b'{"items":[{"id":2}]}')],
        events=close_order,
    )
    factory = _TransportFactory(client, capture)

    results, traffic = capture_live_specs(
        runtime,
        [_spec(1, "event"), _spec(2, "lineups")],
        canonical_url="https://www.sofascore.com/event/1",
        scope="ENG-Premier League:2526",
        entity="match_capture",
        transport_factory=factory,
    )

    assert len(results) == 2
    assert factory.calls == client.close_calls == 1
    assert capture.enter_calls == capture.exit_calls == capture.warm_calls == 1
    assert close_order == ["lease_close", "browser_close"]
    assert client.acquire_calls[0]["source"] == "sofascore"
    assert client.acquire_calls[0]["dag_id"] == "dag_ingest_sofascore"
    assert capture.proxy["server"] == "http://proxy_filter:8900"
    assert "lease-secret" not in capture.proxy["server"]
    # One warm navigation + the two real JSON requests. The engine's logical
    # authorization does not consume the limiter a second time.
    assert limiter.calls == 3
    assert traffic["endpoint_request_count"] == 2
    assert traffic["source_request_count"] == 3
    assert traffic["paid_proxy_bytes"] == traffic["provider_total_bytes"] == 250
    assert traffic["endpoint_provider_bytes"] == {"event": 100, "lineups": 150}
    assert traffic["endpoint_request_provider_bytes"] == {
        "event": [100],
        "lineups": [150],
    }
    assert sum(traffic["endpoint_provider_bytes"].values()) == 250
    assert traffic["browser_sessions"] == 1
    assert traffic["browser_navigations"] == 1


def test_sequential_batch_reports_are_deltas_not_cumulative(tmp_path):
    runtime, _ = _runtime(tmp_path)
    first_factory = _TransportFactory(
        _LeaseClient([0, 0, 100], final_total=100),
        _Capture([_record(b'{"items":[{"id":1}]}')]),
    )
    second_factory = _TransportFactory(
        _LeaseClient([0, 0, 70], final_total=70),
        _Capture([_record(b'{"items":[{"id":2}]}')]),
    )

    _, first = capture_live_specs(
        runtime,
        [_spec(1, "event")],
        canonical_url="https://www.sofascore.com/event/1",
        scope="ENG-Premier League:2526",
        entity="match_capture",
        transport_factory=first_factory,
    )
    _, second = capture_live_specs(
        runtime,
        [_spec(2, "lineups")],
        canonical_url="https://www.sofascore.com/event/2",
        scope="ENG-Premier League:2526",
        entity="match_capture",
        transport_factory=second_factory,
    )

    assert first["provider_total_bytes"] == 100
    assert second["provider_total_bytes"] == 70
    assert first["browser_sessions"] == second["browser_sessions"] == 1
    assert first["browser_navigations"] == second["browser_navigations"] == 1
    assert first["endpoint_request_count"] == 1
    assert second["endpoint_request_count"] == 1
    assert first["source_request_count"] == 2
    assert second["source_request_count"] == 2


def test_signed_batch_temporarily_binds_engine_and_isolates_local_budget(tmp_path):
    runtime, _ = _runtime(tmp_path)
    original_run = runtime.engine.run_id
    original_task = runtime.engine.task_id
    allocation = WorkloadAllocation(
        allocation_id="alloc-" + "9" * 32,
        task_id="capture_match_batch_00000",
        scope="match",
        workload_class="match_batch_25",
        batch_index=0,
        units=("1",),
        budget_bytes=10_000,
    )
    plan = _signed_plan(
        artifact_id="a" * 64,
        dag_id="dag_ingest_sofascore",
        run_id="scheduled__signed-run::targets",
        player_universe_ids=(),
        allocations=(allocation,),
        control_token="c" * 32,
    )
    client = _LeaseClient([0, 0, 100], final_total=100)
    factory = _TransportFactory(
        client,
        _Capture([_record(b'{"items":[{"id":1}]}')]),
    )

    results, _ = capture_live_specs(
        runtime,
        [_spec(1)],
        canonical_url="https://www.sofascore.com/event/1",
        scope="ENG-Premier League:2526",
        entity="match_capture",
        workload_plan=plan,
        allocation_id=allocation.allocation_id,
        attempt_id="1",
        transport_factory=factory,
    )

    assert len(results) == 1
    assert runtime.engine.run_id == original_run
    assert runtime.engine.task_id == original_task
    assert runtime.engine.budget.run_ids == [
        f"{plan.run_id}::{allocation.allocation_id}"
    ]
    assert client.acquire_calls[0]["run_id"] == plan.run_id
    assert client.acquire_calls[0]["task_id"] == allocation.task_id


def test_signed_players_phase_is_accepted_by_production_transport(tmp_path):
    runtime, _ = _runtime(tmp_path)
    runtime.engine.run_id = "scheduled__signed-run::players"
    allocation = WorkloadAllocation(
        allocation_id="alloc-" + "8" * 32,
        task_id="capture",
        scope="player",
        workload_class="player_batch_50",
        batch_index=0,
        units=("1",),
        budget_bytes=10_000,
    )
    plan = _signed_plan(
        artifact_id="a" * 64,
        dag_id="dag_ingest_sofascore",
        run_id=runtime.engine.run_id,
        player_universe_ids=("1",),
        allocations=(allocation,),
        control_token="c" * 32,
    )
    client = _LeaseClient([0, 0, 100], final_total=100)
    factory = _TransportFactory(
        client,
        _Capture([_record(b'{"items":[{"id":1}]}')]),
    )

    capture_live_specs(
        runtime,
        [_spec(1)],
        canonical_url="https://www.sofascore.com/event/1",
        scope="ENG-Premier League:2526",
        entity="player_capture",
        workload_plan=plan,
        allocation_id=allocation.allocation_id,
        attempt_id="1",
        transport_factory=factory,
    )

    assert client.acquire_calls[0]["run_id"].endswith("::players")


def test_challenge_rewarms_same_browser_and_engine_retry_is_accounted(tmp_path):
    runtime, limiter = _runtime(tmp_path)
    client = _LeaseClient([0, 0, 40, 40, 90], final_total=90)
    capture = _Capture(
        [
            _record(b'{"error":{"reason":"challenge"}}', 403, challenge=True),
            _record(b'{"items":[{"id":1}]}'),
        ]
    )
    factory = _TransportFactory(client, capture)

    results, traffic = capture_live_specs(
        runtime,
        [_spec(1)],
        canonical_url="https://www.sofascore.com/event/1",
        scope="ENG-Premier League:2526",
        entity="match_capture",
        transport_factory=factory,
    )

    assert len(results) == 1
    assert capture.enter_calls == 1
    assert capture.warm_calls == 2
    assert limiter.calls == 4  # warm+fetch, rewarm+fetch
    assert traffic["endpoint_request_count"] == 2
    assert traffic["source_request_count"] == 4
    assert traffic["browser_sessions"] == 1
    assert traffic["browser_navigations"] == 2
    assert traffic["provider_total_bytes"] == 90
    assert traffic["endpoint_provider_bytes"] == {"event": 90}
    assert traffic["endpoint_request_provider_bytes"] == {"event": [40, 50]}


def test_retryable_transport_failure_keeps_per_attempt_provider_breakdown(tmp_path):
    runtime, _ = _runtime(tmp_path)
    client = _LeaseClient(
        [0, 0, 10, 10, 20, 20, 30],
        final_total=30,
    )
    capture = _Capture([None, None, None])
    factory = _TransportFactory(client, capture)

    with pytest.raises(RuntimeError, match="did not reach a publishable state"):
        capture_live_specs(
            runtime,
            [_spec(1)],
            canonical_url="https://www.sofascore.com/event/1",
            scope="ENG-Premier League:2526",
            entity="match_capture",
            transport_factory=factory,
        )

    metrics = runtime.engine.metrics.snapshot()
    assert metrics["paid_proxy_bytes"] == 30
    assert metrics["endpoint_provider_bytes"] == {"event": 30}
    assert metrics["endpoint_request_provider_bytes"] == {
        "event": [10, 10, 10]
    }
    assert sum(metrics["endpoint_provider_bytes"].values()) == 30


def test_warm_exception_retains_session_navigation_and_source_metrics(tmp_path):
    runtime, limiter = _runtime(tmp_path)
    client = _LeaseClient([0, 0, 100], final_total=100)
    capture = _WarmFailureCapture([])
    factory = _TransportFactory(client, capture)

    with pytest.raises(RuntimeError, match="did not reach a publishable state"):
        capture_live_specs(
            runtime,
            [_spec(1)],
            canonical_url="https://www.sofascore.com/event/1",
            scope="ENG-Premier League:2526",
            entity="match_capture",
            transport_factory=factory,
        )

    metrics = runtime.engine.metrics.snapshot()
    assert limiter.calls == 1
    assert metrics["request_count"] == 1
    assert metrics["source_request_count"] == 1
    assert metrics["browser_sessions"] == 1
    assert metrics["navigations"] == 1
    assert metrics["paid_proxy_bytes"] == 100
    assert client.close_calls == 1
    assert capture.enter_calls == capture.exit_calls == capture.warm_calls == 1


def test_dynamic_plan_expands_without_opening_another_session(tmp_path):
    runtime, _ = _runtime(tmp_path)
    first = _spec(1, "event")
    second = _spec(2, "lineups")
    client = _LeaseClient([0, 0, 80, 80, 180], final_total=180)
    capture = _Capture(
        [_record(b'{"items":[{"id":1}]}'), _record(b'{"items":[{"id":2}]}')]
    )
    factory = _TransportFactory(client, capture)

    def planner():
        first_raw = runtime.raw_store.has_payload(first.raw_target)
        specs = (first, second) if first_raw else (first,)
        missing = tuple(
            spec.key
            for spec in specs
            if not runtime.raw_store.has_payload(spec.raw_target)
        )
        return SimpleNamespace(specs=specs, missing_raw_keys=missing)

    captured, final_plan, traffic = capture_live_dynamic_specs(
        runtime,
        planner,
        canonical_url="https://www.sofascore.com/event/1",
        scope="ENG-Premier League:2526",
        entity="season_capture",
        transport_factory=factory,
    )

    assert len(captured) == 2
    assert final_plan.missing_raw_keys == ()
    assert factory.calls == capture.enter_calls == capture.warm_calls == 1
    assert traffic["provider_total_bytes"] == 180


def test_final_meter_tail_is_rejected_as_unattributed_traffic(tmp_path):
    runtime, _ = _runtime(tmp_path)
    client = _LeaseClient([0, 0, 100], final_total=101)
    capture = _Capture([_record(b'{"items":[{"id":1}]}')])
    factory = _TransportFactory(client, capture)

    with pytest.raises(BudgetAccountingError, match="unattributed"):
        capture_live_specs(
            runtime,
            [_spec(1)],
            canonical_url="https://www.sofascore.com/event/1",
            scope="ENG-Premier League:2526",
            entity="match_capture",
            transport_factory=factory,
        )


def test_browser_failure_redacts_lease_token_from_manifest_and_exception(tmp_path):
    runtime, _ = _runtime(tmp_path)
    client = _LeaseClient([0, 0, 0], final_total=0, token="lease-secret")

    class FailingCapture(_Capture):
        def warm_exact_json(self, url):
            self._request()
            raise RuntimeError("proxy password=lease-secret")

    factory = _TransportFactory(
        client,
        FailingCapture([_record(b'{"items":[]}')]),
    )

    with pytest.raises(RuntimeError) as captured:
        capture_live_specs(
            runtime,
            [_spec(1)],
            canonical_url="https://www.sofascore.com/event/1",
            scope="ENG-Premier League:2526",
            entity="match_capture",
            transport_factory=factory,
        )

    assert "lease-secret" not in str(captured.value)
    manifest = runtime.manifest_store.get(_spec(1).key)
    assert manifest is not None
    assert "lease-secret" not in str(manifest.error_message)


def test_canary_probe_keeps_only_hash_and_uses_canary_lease(monkeypatch, tmp_path):
    monkeypatch.setenv("PROXY_FILTER_SOFASCORE_CANARY_HARD_CAP_BYTES", "10000")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_canary_sofascore_proxy")
    runtime, limiter = _runtime(tmp_path)
    client = _LeaseClient([0, 0, 120], final_total=120)
    capture = _Capture([_record(b'{"items":[{"id":1}]}')])
    factory = _TransportFactory(
        client,
        capture,
        mode="canary",
        exit_probe_enabled=True,
    )

    _, traffic = capture_live_specs(
        runtime,
        [_spec(1)],
        canonical_url="https://www.sofascore.com/event/1",
        scope="fixed-canary",
        entity="cold",
        transport_factory=factory,
    )

    assert client.acquire_calls[0]["source"] == "sofascore_canary"
    assert traffic["proxy_exit_hash"] == hash_proxy_exit("203.0.113.7")
    assert "203.0.113.7" not in repr(traffic)
    assert limiter.calls == 3  # warm navigation + probe + exact endpoint


@pytest.mark.parametrize("value", ["0", "false", "no", ""])
def test_paid_transport_rejects_disabled_resource_blocking(
    monkeypatch, tmp_path, value
):
    monkeypatch.setenv("SOFASCORE_BLOCK_RESOURCES", value)
    runtime, _ = _runtime(tmp_path)
    client = _LeaseClient([0], final_total=0)
    factory = _TransportFactory(
        client,
        _Capture([]),
        mode="canary",
        exit_probe_enabled=True,
    )

    with pytest.raises(ValueError, match="resource blocking enabled"):
        factory(
            runtime.engine,
            canonical_url="https://www.sofascore.com/football",
            scope="fixed-canary",
            entity="cold",
        )

    assert client.acquire_calls == []


@pytest.mark.parametrize("value", ["", "not-an-ip", "203.0.113.7 secret"])
def test_exit_hash_rejects_non_ip_without_echoing_value(value):
    with pytest.raises(ValueError) as captured:
        hash_proxy_exit(value)
    if value:
        assert value not in str(captured.value)
