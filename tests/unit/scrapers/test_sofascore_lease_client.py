from __future__ import annotations

import pytest

from scrapers.sofascore.lease_client import (
    SofascoreLeaseClient,
    SofascoreLeaseProtocolError,
    SofascoreLeaseRejected,
    SofascoreProxyLease,
    _phase_run_id,
)
from scrapers.sofascore.workload_plan import (
    WorkloadAllocation,
    _signed_plan,
)

CONTROL_TOKEN = "c" * 32


class _Response:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


class _Session:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.trust_env = True

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _plan(
    *,
    run_id="scheduled__2026-07-11::targets",
    task_id="capture_match_batch_00000",
    budget=4096,
):
    phase = run_id.rsplit("::", 1)[-1]
    scope = "player" if phase == "players" else "match"
    allocation = WorkloadAllocation(
        allocation_id="alloc-" + "1" * 32,
        task_id=task_id,
        scope=scope,
        workload_class=("player_batch_50" if scope == "player" else "match_batch_25"),
        batch_index=0,
        units=("1",),
        budget_bytes=budget,
    )
    plan = _signed_plan(
        artifact_id="a" * 64,
        dag_id="dag_ingest_sofascore",
        run_id=run_id,
        player_universe_ids=(("1",) if scope == "player" else ()),
        allocations=(allocation,),
        control_token=CONTROL_TOKEN,
    )
    return plan, allocation


def test_player_phase_uses_same_parent_run_identity():
    assert _phase_run_id("scheduled__2026-07-11::players") == (
        "scheduled__2026-07-11",
        "players",
    )


def _lease_payload(*, token="unsafe/token+", plan=None, allocation=None):
    if plan is None or allocation is None:
        plan, allocation = _plan()
    return {
        "id": "lease-1",
        "token": token,
        "proxy_url": "http://proxy_filter:8900",
        "max_bytes": 4096,
        "expires_at": 2_000_000_000.0,
        "plan_digest": plan.plan_digest,
        "allocation_id": allocation.allocation_id,
        "allocation_budget_bytes": allocation.budget_bytes,
    }


def _stats_payload(*, closed=False, up=125, down=875, source="sofascore"):
    payload = {
        "id": "lease-1",
        "up_bytes": up,
        "down_bytes": down,
        "total_bytes": up + down,
        "max_bytes": 4096,
        "dagrun_total_bytes": up + down,
        "dagrun_budget_bytes": 4096,
        "daily_total_bytes": up + down,
        "daily_budget_bytes": 104857600,
        "active_tunnels": 0,
        "reserved_bytes": 0,
        "closed": closed,
        "expired": False,
        "budget_exceeded": False,
        "source": source,
        "upstream_fingerprint": "0123456789abcdef",
        "budget_artifact_id": "a" * 64,
        "plan_digest": "b" * 64,
        "allocation_id": "alloc-" + "1" * 32,
        "allocation_task_id": "capture_match_batch_00000",
        "allocation_scope": "match",
        "allocation_class": "match_batch_25",
        "allocation_batch_index": 0,
        "allocation_units": ["1"],
        "allocation_budget_bytes": 4096,
        "allocation_spent_provider_bytes": up + down,
        "allocation_remaining_provider_bytes": 4096 - up - down,
        "endpoint_request_provider_bytes": {},
        "base_run_id": "scheduled__2026-07-11",
        "workload_phase": "targets",
        "phase_plan_digest": "b" * 64,
        "parent_run_cap_bytes": 4096,
        "parent_run_spent_provider_bytes": up + down,
    }
    if source == "sofascore_canary":
        payload.update(
            plan_digest="",
            allocation_id="",
            allocation_task_id="",
            allocation_scope="",
            allocation_class="",
            allocation_batch_index=-1,
            allocation_units=[],
            allocation_budget_bytes=0,
            allocation_spent_provider_bytes=0,
            allocation_remaining_provider_bytes=0,
            base_run_id="",
            workload_phase="",
            phase_plan_digest="",
            parent_run_cap_bytes=0,
            parent_run_spent_provider_bytes=0,
        )
    return payload


def _production_lease(**values):
    defaults = {
        "lease_id": "lease-1",
        "token": "secret-token",
        "proxy_url": "http://proxy_filter:8900",
        "max_bytes": 4096,
        "expires_at": 2_000_000_000.0,
        "artifact_id": "a" * 64,
        "plan_digest": "b" * 64,
        "allocation_id": "alloc-" + "1" * 32,
        "allocation_task_id": "capture_match_batch_00000",
        "allocation_scope": "match",
        "allocation_class": "match_batch_25",
        "allocation_batch_index": 0,
        "allocation_units": ("1",),
        "allocation_budget_bytes": 4096,
        "base_run_id": "scheduled__2026-07-11",
        "workload_phase": "targets",
    }
    defaults.update(values)
    return SofascoreProxyLease(**defaults)


def test_acquire_is_stateful_once_trusts_no_environment_and_builds_proxy_auth():
    plan, allocation = _plan(budget=8192)
    session = _Session(_Response(201, _lease_payload(plan=plan, allocation=allocation)))
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899",
        session=session,
        control_token=CONTROL_TOKEN,
    )

    lease = client.acquire(
        max_bytes=8192,
        ttl_seconds=3600,
        dag_id="dag_ingest_sofascore",
        run_id="scheduled__2026-07-11::targets",
        task_id=allocation.task_id,
        scope="competition-season",
        entity="17/76986",
        workload_plan=plan,
        allocation_id=allocation.allocation_id,
        attempt_id="1",
    )

    assert session.trust_env is False
    assert len(session.calls) == 1
    method, url, request = session.calls[0]
    assert (method, url) == ("POST", "http://proxy_filter:8899/v1/leases")
    assert request["json"]["source"] == "sofascore"
    assert request["json"]["max_bytes"] == 8192
    assert request["json"]["workload_plan"] == plan.to_dict()
    assert request["json"]["allocation"] == allocation.to_dict()
    assert "token" not in request["json"]
    assert client.authenticated_proxy_url(lease) == (
        "http://lease:unsafe%2Ftoken%2B@proxy_filter:8900"
    )
    assert client.playwright_proxy(lease) == {
        "server": "http://proxy_filter:8900",
        "username": "lease",
        "password": "unsafe/token+",
    }
    assert lease.token not in repr(lease)


def test_stats_and_close_use_bearer_and_validate_exact_counters():
    session = _Session(
        _Response(200, _stats_payload()),
        _Response(200, _stats_payload(closed=True)),
    )
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )
    lease = _production_lease()

    stats = client.stats(lease)
    final = client.close(lease)

    assert stats.total_bytes == 1000
    assert stats.up_bytes + stats.down_bytes == stats.total_bytes
    assert final.closed is True
    assert session.calls[0][2]["headers"] == {
        "X-Proxy-Control-Token": CONTROL_TOKEN,
        "Authorization": "Bearer secret-token",
    }
    assert session.calls[1][0:2] == (
        "DELETE",
        "http://proxy_filter:8899/v1/leases/lease-1/close",
    )


def test_endpoint_boundaries_are_authenticated_and_return_exact_stats():
    payload = _stats_payload(up=10, down=20)
    payload["endpoint_request_provider_bytes"] = {"event": [30]}
    session = _Session(
        _Response(201, {"request_id": "request-1"}),
        _Response(200, payload),
    )
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )
    lease = _production_lease()

    request_id = client.begin_endpoint(lease, "event")
    stats = client.finish_endpoint(lease, request_id)

    assert stats.endpoint_request_provider_bytes == {"event": (30,)}
    assert session.calls[0][0:2] == (
        "POST",
        "http://proxy_filter:8899/v1/leases/lease-1/endpoints",
    )
    assert session.calls[1][0:2] == (
        "DELETE",
        "http://proxy_filter:8899/v1/leases/lease-1/endpoints/request-1",
    )
    assert all(
        call[2]["headers"]["Authorization"] == "Bearer secret-token"
        for call in session.calls
    )


@pytest.mark.parametrize(
    "field",
    ("active_tunnels", "reserved_bytes"),
)
def test_close_rejects_nonfinal_provider_counters(field):
    payload = _stats_payload(closed=True)
    payload[field] = 1
    session = _Session(_Response(200, payload))
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )
    lease = _production_lease()

    with pytest.raises(SofascoreLeaseProtocolError, match="did not close cleanly"):
        client.close(lease)


def test_canary_mode_is_explicit_and_cannot_be_reused_by_production_dag():
    session = _Session(
        _Response(201, _lease_payload(token="canary-token")),
        _Response(200, _stats_payload(source="sofascore_canary")),
    )
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )

    lease = client.acquire(
        max_bytes=4096,
        ttl_seconds=3600,
        dag_id="dag_canary_sofascore_proxy",
        run_id="manual__cold-01",
        task_id="capture_fixed_cohort",
        source="sofascore_canary",
    )
    stats = client.stats(lease)

    assert lease.source == "sofascore_canary"
    assert stats.source == "sofascore_canary"
    assert session.calls[0][2]["json"]["source"] == "sofascore_canary"
    with pytest.raises(
        ValueError, match="source=sofascore.*dag_id=dag_ingest_sofascore"
    ):
        client.acquire(
            max_bytes=4096,
            ttl_seconds=3600,
            dag_id="dag_canary_sofascore_proxy",
            run_id="run",
            task_id="task",
        )
    with pytest.raises(
        ValueError,
        match="source=sofascore_canary.*dag_id=dag_canary_sofascore_proxy",
    ):
        client.acquire(
            max_bytes=4096,
            ttl_seconds=3600,
            dag_id="dag_ingest_sofascore",
            run_id="run",
            task_id="task",
            source="sofascore_canary",
        )
    with pytest.raises(
        ValueError, match="source must be sofascore or sofascore_canary"
    ):
        client.acquire(
            max_bytes=4096,
            ttl_seconds=3600,
            dag_id="dag_ingest_sofascore",
            run_id="run",
            task_id="task",
            source="other",
        )


def test_production_acquire_requires_signed_allocation_without_http_call():
    session = _Session()
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )
    with pytest.raises(ValueError, match="signed workload plan"):
        client.acquire(
            max_bytes=4096,
            ttl_seconds=30,
            dag_id="dag_ingest_sofascore",
            run_id="run",
            task_id="task",
        )
    assert session.calls == []


def test_rejection_redacts_credentials_and_token_values():
    plan, allocation = _plan(run_id="run::targets", task_id="task", budget=10)
    session = _Session(
        _Response(
            429,
            {
                "code": "budget_exceeded",
                "error": (
                    "proxy=http://provider-user:provider-pass@pool.invalid:1 "
                    "token=server-secret"
                ),
            },
        )
    )
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )

    with pytest.raises(SofascoreLeaseRejected) as captured:
        client.acquire(
            max_bytes=10,
            ttl_seconds=10,
            dag_id="dag_ingest_sofascore",
            run_id="run::targets",
            task_id="task",
            workload_plan=plan,
            allocation_id=allocation.allocation_id,
            attempt_id="1",
        )

    message = str(captured.value)
    assert captured.value.code == "budget_exceeded"
    assert "provider-user" not in message
    assert "provider-pass" not in message
    assert "server-secret" not in message
    assert "[REDACTED]" in message


def test_transport_failure_is_not_retried_and_is_redacted():
    plan, allocation = _plan(run_id="run::targets", task_id="task", budget=10)
    session = _Session(RuntimeError("http://u:p@proxy.invalid token=secret"))
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )

    with pytest.raises(SofascoreLeaseProtocolError) as captured:
        client.acquire(
            max_bytes=10,
            ttl_seconds=10,
            dag_id="dag_ingest_sofascore",
            run_id="run::targets",
            task_id="task",
            workload_plan=plan,
            allocation_id=allocation.allocation_id,
            attempt_id="1",
        )

    assert len(session.calls) == 1
    assert "u:p" not in str(captured.value)
    assert "secret" not in str(captured.value)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(total_bytes=999),
        lambda payload: payload.update(dagrun_budget_bytes=0),
        lambda payload: payload.update(source="other"),
        lambda payload: payload.update(up_bytes=True),
        lambda payload: payload.update(upstream_repins=-1),
        lambda payload: payload.update(upstream_repins=True),
    ],
)
def test_stats_fail_closed_on_schema_or_counter_drift(mutate):
    payload = _stats_payload()
    mutate(payload)
    session = _Session(_Response(200, payload))
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )
    lease = _production_lease(token="token")

    with pytest.raises(SofascoreLeaseProtocolError):
        client.stats(lease)


def test_stats_parse_upstream_repins_and_default_to_zero_when_absent():
    # #946: the dead-exit failover counter rides along in stats.  A proxy that
    # predates the field must parse as 0, keeping fingerprint drift fail-closed.
    with_repins = _stats_payload()
    with_repins["upstream_repins"] = 2
    session = _Session(_Response(200, with_repins))
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )
    lease = _production_lease(token="token")
    assert client.stats(lease).upstream_repins == 2

    without_repins = _stats_payload()
    assert "upstream_repins" not in without_repins
    session = _Session(_Response(200, without_repins))
    client = SofascoreLeaseClient(
        "http://proxy_filter:8899", session=session, control_token=CONTROL_TOKEN
    )
    assert client.stats(lease).upstream_repins == 0


def test_control_url_rejects_embedded_credentials():
    with pytest.raises(ValueError, match="credential-free"):
        SofascoreLeaseClient(
            "http://user:pass@proxy_filter:8899", control_token=CONTROL_TOKEN
        )


def test_control_token_is_required_and_never_taken_from_proxy_environment(
    monkeypatch,
):
    monkeypatch.delenv("SOFASCORE_PROXY_CONTROL_TOKEN", raising=False)
    with pytest.raises(ValueError, match="CONTROL_TOKEN"):
        SofascoreLeaseClient("http://proxy_filter:8899")
