from __future__ import annotations

import pytest

from scrapers.fbref.proxy_lease import (
    FBREF_DAG_IDS,
    FBrefProxyLeaseClient,
    FBrefProxyLeaseError,
    METER_ID,
)


TOKEN = "t" * 32
CONTEXT = {
    "source": "fbref",
    "dag_id": "dag_ingest_fbref",
    "run_id": "control-run",
    "task_id": "run_live_waves",
    "scope": "worker-0",
    "canonical_url": "https://fbref.com/en/",
}


class _Response:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def _lease_body():
    return {
        "id": "lease-1",
        "token": "lease-token",
        "proxy_url": "http://fbref_proxy_filter:8900",
        "max_bytes": 1000,
        "expires_at": 9999999999.0,
    }


def _stats_body(**overrides):
    body = {
        "meter": METER_ID,
        "id": "lease-1",
        "source": "fbref",
        "dag_id": "dag_ingest_fbref",
        "run_id": "control-run",
        "task_id": "run_live_waves",
        "scope": "worker-0",
        "canonical_url": "https://fbref.com/en/",
        "up_bytes": 40,
        "down_bytes": 60,
        "total_bytes": 100,
        "active_tunnels": 0,
        "reserved_bytes": 0,
        "active_provider_readers": 0,
        "provider_reserved_bytes": 0,
        "pending_client_hellos": 0,
        "staged_client_bytes": 0,
        "closed": False,
        "expired": False,
        "budget_exceeded": False,
        "accounting_uncertain": False,
    }
    body.update(overrides)
    return body


def _extension_body(**overrides):
    body = _stats_body(
        max_bytes=2000,
        expires_at=9999999999.0,
        expired=False,
    )
    body.update(overrides)
    return body


def test_acquire_never_returns_upstream_credentials_to_the_fetcher():
    session = _Session([_Response(201, _lease_body())])
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )

    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)
    proxy = client.playwright_proxy(lease)

    assert proxy == {
        "server": "http://fbref_proxy_filter:8900",
        "username": "lease",
        "password": "lease-token",
    }
    _, _, request = session.calls[0]
    assert request["headers"] == {"X-Proxy-Control-Token": TOKEN}
    assert request["json"]["source"] == "fbref"


def test_paid_fbref_dag_allowlist_is_exact_and_bootstrap_can_acquire():
    assert FBREF_DAG_IDS == frozenset(
        {
            "dag_ingest_fbref",
            "dag_bootstrap_fbref",
            "dag_backfill_fbref",
            "dag_accept_fbref_bronze",
        }
    )


@pytest.mark.parametrize(
    "dag_id",
    [
        "dag_bootstrap_fbref",
        "dag_accept_fbref_bronze",
    ],
)
def test_additional_fbref_dags_can_acquire_from_the_isolated_meter(dag_id):
    session = _Session([_Response(201, _lease_body())])
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )

    context = {**CONTEXT, "dag_id": dag_id}
    client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=context)

    assert session.calls[0][2]["json"]["dag_id"] == dag_id


def test_control_token_never_falls_back_to_another_sources_secret(monkeypatch):
    monkeypatch.delenv("FBREF_PROXY_CONTROL_TOKEN", raising=False)
    monkeypatch.setenv("PROXY_FILTER_CONTROL_TOKEN", "p" * 32)
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", "s" * 32)

    with pytest.raises(
        FBrefProxyLeaseError,
        match="FBREF_PROXY_CONTROL_TOKEN",
    ):
        FBrefProxyLeaseClient("http://fbref_proxy_filter:8899")


def test_control_token_uses_only_the_dedicated_fbref_secret(monkeypatch):
    monkeypatch.setenv("FBREF_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("PROXY_FILTER_CONTROL_TOKEN", "p" * 32)
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", "s" * 32)

    client = FBrefProxyLeaseClient("http://fbref_proxy_filter:8899")

    assert client._control_token == TOKEN


def test_internal_control_session_ignores_ambient_http_proxy():
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
    )

    assert client._client().trust_env is False


@pytest.mark.parametrize(
    "proxy_url",
    [
        "http://shared-proxy-filter:8900",
        "http://fbref_proxy_filter:8900/unexpected",
        "http://lease:token@fbref_proxy_filter:8900",
    ],
)
def test_acquire_rejects_a_misdirected_data_plane(proxy_url):
    body = _lease_body()
    body["proxy_url"] = proxy_url
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=_Session([_Response(201, body)]),
    )

    with pytest.raises(FBrefProxyLeaseError, match="unsafe lease"):
        client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)


def test_stats_require_exact_meter_and_run_provenance():
    session = _Session(
        [_Response(201, _lease_body()), _Response(200, _stats_body())]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    stats = client.stats(lease, expected=CONTEXT)

    assert stats.total_bytes == 100
    assert session.calls[-1][2]["headers"]["Authorization"] == (
        "Bearer lease-token"
    )


def test_extend_returns_same_frozen_identity_with_larger_cap():
    session = _Session(
        [_Response(201, _lease_body()), _Response(200, _extension_body())]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )
    original = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    extended = client.extend(original, max_bytes=2000, expected=CONTEXT)

    assert extended is not original
    assert extended.lease_id == original.lease_id
    assert extended.token == original.token
    assert extended.proxy_url == original.proxy_url
    assert extended.expires_at == original.expires_at
    assert extended.max_bytes == 2000
    method, url, request = session.calls[-1]
    assert method == "POST"
    assert url.endswith("/v1/leases/lease-1/extend")
    assert request["json"] == {"max_bytes": 2000}
    assert request["headers"]["Authorization"] == "Bearer lease-token"


@pytest.mark.parametrize(
    "change",
    [
        {"id": "another-lease"},
        {"max_bytes": 1999},
        {"expires_at": 9999999998.0},
        {"active_tunnels": 1},
        {"active_provider_readers": 1},
        {"provider_reserved_bytes": 1},
        {"pending_client_hellos": 1},
        {"staged_client_bytes": 1},
        {"accounting_uncertain": True},
        {"total_bytes": 1001, "up_bytes": 500, "down_bytes": 501},
        {"proxy_url": "http://other-filter:8900"},
    ],
)
def test_extend_rejects_invalid_or_lost_acknowledgement(change):
    body = _extension_body(**change)
    session = _Session([_Response(201, _lease_body()), _Response(200, body)])
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )
    original = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="provenance"):
        client.extend(original, max_bytes=2000, expected=CONTEXT)

    assert original.max_bytes == 1000


def test_extend_lost_response_keeps_the_original_frozen_handle():
    session = _Session([_Response(201, _lease_body())])
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )
    original = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="request failed"):
        client.extend(original, max_bytes=2000, expected=CONTEXT)

    assert original.max_bytes == 1000


@pytest.mark.parametrize(
    "change",
    [
        {"meter": "estimated"},
        {"run_id": "another-run"},
        {"total_bytes": 99},
        {"active_tunnels": -1},
        {"close_complete": True},
    ],
)
def test_stats_fail_closed_on_untrusted_counter(change):
    session = _Session(
        [
            _Response(201, _lease_body()),
            _Response(200, _stats_body(**change)),
        ]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="provenance"):
        client.stats(lease, expected=CONTEXT)


def test_close_waits_for_proxy_filter_final_counter():
    pending = _stats_body(
        active_tunnels=1,
        closed=True,
        close_complete=False,
    )
    final = _stats_body(
        up_bytes=50,
        down_bytes=75,
        total_bytes=125,
        closed=True,
        close_complete=True,
    )
    session = _Session(
        [
            _Response(201, _lease_body()),
            _Response(409, pending),
            _Response(200, final),
        ]
    )
    ticks = iter((0.0, 0.0, 0.1, 0.1))
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
        sleep=lambda _seconds: None,
        monotonic=lambda: next(ticks),
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    stats = client.close(lease, expected=CONTEXT)

    assert stats.close_complete is True
    assert stats.total_bytes == 125
    assert [call[0] for call in session.calls] == ["POST", "DELETE", "DELETE"]


def test_acquire_rejects_non_fbref_dag_before_network():
    session = _Session([])
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )

    with pytest.raises(FBrefProxyLeaseError, match="provenance"):
        client.acquire(
            max_bytes=1000,
            ttl_seconds=7200,
            metadata=replace_context(dag_id="dag_replay_fbref"),
        )
    assert session.calls == []


def replace_context(**values):
    result = dict(CONTEXT)
    result.update(values)
    return result


def test_close_accepts_the_permanent_uncertainty_latch():
    # One mid-response abort latches accounting_uncertain forever; the filter
    # retains the unproven tail as durable escrow and by design never reports
    # close_complete for that lease. Its 409 counters are the final
    # client-visible ledger (#1099, mirrors #1096 for Transfermarkt).
    latched = _stats_body(
        up_bytes=50,
        down_bytes=75,
        total_bytes=125,
        closed=True,
        close_complete=False,
        close_error=(
            "provider byte accounting is uncertain; durable escrow retained"
        ),
    )
    session = _Session(
        [
            _Response(201, _lease_body()),
            _Response(409, latched),
        ]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
        sleep=lambda _seconds: None,
        monotonic=lambda: 0.0,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    stats = client.close(lease, expected=CONTEXT)

    assert stats.close_complete is False
    assert stats.total_bytes == 125
    assert [call[0] for call in session.calls] == ["POST", "DELETE"]


def test_close_still_times_out_on_pending_without_the_latch():
    pending = _stats_body(
        active_tunnels=1,
        closed=True,
        close_complete=False,
    )
    session = _Session(
        [
            _Response(201, _lease_body()),
            _Response(409, dict(pending)),
            _Response(409, dict(pending)),
        ]
    )
    ticks = iter((0.0, 0.0, 1000.0))
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
        sleep=lambda _seconds: None,
        monotonic=lambda: next(ticks),
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="did not return final"):
        client.close(lease, expected=CONTEXT)


def test_wait_idle_requires_two_identical_open_tunnel_samples():
    first = _stats_body(
        up_bytes=50,
        down_bytes=75,
        total_bytes=125,
        active_tunnels=1,
        active_provider_readers=1,
        reserved_bytes=64,
        provider_reserved_bytes=64,
    )
    session = _Session(
        [
            _Response(201, _lease_body()),
            _Response(200, dict(first)),
            _Response(200, dict(first)),
        ]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
        sleep=lambda _seconds: None,
        monotonic=lambda: 0.0,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    stats = client.wait_idle(lease, expected=CONTEXT, expected_tunnels=1)

    assert stats.total_bytes == 125
    assert [call[0] for call in session.calls] == ["POST", "GET", "GET"]


@pytest.mark.parametrize(
    "field",
    ["source", "dag_id", "run_id", "task_id", "canonical_url", "scope"],
)
@pytest.mark.parametrize("mutation", ["missing", "changed"])
def test_wait_idle_rejects_changed_provenance_in_either_stable_probe(
    field, mutation
):
    stable = _stats_body(
        active_tunnels=1,
        active_provider_readers=1,
        reserved_bytes=64,
        provider_reserved_bytes=64,
    )
    second = dict(stable)
    if mutation == "missing":
        second.pop(field)
    else:
        second[field] = f"changed-{field}"
    session = _Session(
        [
            _Response(201, _lease_body()),
            _Response(200, stable),
            _Response(200, second),
        ]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
        sleep=lambda _seconds: None,
        monotonic=lambda: 0.0,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="schema|provenance"):
        client.wait_idle(lease, expected=CONTEXT, expected_tunnels=1)

    assert [call[0] for call in session.calls] == ["POST", "GET", "GET"]


def test_zero_tunnel_idle_proof_rejects_any_observed_reservation():
    reserved = _stats_body(
        reserved_bytes=64,
        provider_reserved_bytes=64,
    )
    idle = _stats_body()
    session = _Session(
        [
            _Response(201, _lease_body()),
            _Response(200, reserved),
            _Response(200, idle),
            _Response(200, idle),
        ]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
        sleep=lambda _seconds: None,
        monotonic=lambda: 0.0,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="unexpected.*state"):
        client.wait_idle(lease, expected=CONTEXT, expected_tunnels=0)

    assert [call[0] for call in session.calls] == ["POST", "GET"]


@pytest.mark.parametrize(
    "change",
    [
        {"active_tunnels": 2, "active_provider_readers": 2},
        {"active_tunnels": 1, "active_provider_readers": 2},
        {
            "active_tunnels": 1,
            "active_provider_readers": 1,
            "reserved_bytes": 5,
            "provider_reserved_bytes": 4,
        },
        {"active_tunnels": 1, "active_provider_readers": 1,
         "staged_client_bytes": 1},
        {"active_tunnels": 1, "active_provider_readers": 1,
         "accounting_uncertain": True},
    ],
)
def test_wait_idle_rejects_ambiguous_open_tunnel_state(change):
    body = _stats_body(**change)
    session = _Session([_Response(201, _lease_body()), _Response(200, body)])
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="idle"):
        client.wait_idle(lease, expected=CONTEXT, expected_tunnels=1)


def test_close_strict_rejects_uncertainty_409_immediately():
    latched = _stats_body(
        closed=True,
        close_complete=False,
        accounting_uncertain=True,
        close_error=(
            "provider byte accounting is uncertain; durable escrow retained"
        ),
    )
    session = _Session(
        [_Response(201, _lease_body()), _Response(409, latched)]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="rejected"):
        client.close_strict(lease, expected=CONTEXT)

    assert [call[0] for call in session.calls] == ["POST", "DELETE"]


def test_close_strict_accepts_only_zero_lifecycle_fields():
    final = _stats_body(
        up_bytes=50,
        down_bytes=75,
        total_bytes=125,
        closed=True,
        close_complete=True,
    )
    session = _Session(
        [_Response(201, _lease_body()), _Response(200, final)]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    stats = client.close_strict(lease, expected=CONTEXT)

    assert stats.total_bytes == 125
    assert stats.close_complete is True


@pytest.mark.parametrize(
    "field",
    ["source", "dag_id", "run_id", "task_id", "canonical_url", "scope"],
)
@pytest.mark.parametrize("mutation", ["missing", "changed"])
def test_close_strict_rejects_missing_or_changed_full_provenance(field, mutation):
    final = _stats_body(closed=True, close_complete=True)
    if mutation == "missing":
        final.pop(field)
    else:
        final[field] = f"changed-{field}"
    session = _Session([_Response(201, _lease_body()), _Response(200, final)])
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="schema|provenance"):
        client.close_strict(lease, expected=CONTEXT)


def test_wait_drained_rejects_uncertain_or_hidden_provider_work():
    body = _stats_body(
        active_provider_readers=1,
        provider_reserved_bytes=64,
    )
    session = _Session(
        [_Response(201, _lease_body()), _Response(200, body)]
    )
    client = FBrefProxyLeaseClient(
        "http://fbref_proxy_filter:8899",
        control_token=TOKEN,
        session=session,
        drain_timeout_seconds=0.01,
        sleep=lambda _seconds: None,
        monotonic=iter((0.0, 1.0)).__next__,
    )
    lease = client.acquire(max_bytes=1000, ttl_seconds=7200, metadata=CONTEXT)

    with pytest.raises(FBrefProxyLeaseError, match="drain"):
        client.wait_drained(lease, expected=CONTEXT)
