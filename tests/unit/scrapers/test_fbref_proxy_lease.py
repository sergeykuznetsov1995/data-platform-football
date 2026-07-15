from __future__ import annotations

import pytest

from scrapers.fbref.proxy_lease import (
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
        "up_bytes": 40,
        "down_bytes": 60,
        "total_bytes": 100,
        "active_tunnels": 0,
        "reserved_bytes": 0,
        "closed": False,
        "budget_exceeded": False,
    }
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


@pytest.mark.parametrize(
    "change",
    [
        {"meter": "estimated"},
        {"run_id": "another-run"},
        {"total_bytes": 99},
        {"active_tunnels": -1},
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
