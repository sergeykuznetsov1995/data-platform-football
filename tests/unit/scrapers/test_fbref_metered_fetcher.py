from __future__ import annotations

from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from scrapers.fbref.camoufox_fetch import (
    CamoufoxFbrefTransport,
    GEOIP_BYTE_RESERVATION_BYTES,
)
from scrapers.fbref.fetcher import FBrefFetcher, FetchError, FetchResponse
from scrapers.fbref.proxy_lease import (
    FBrefLeaseStats,
    FBrefProxyLease,
    FBrefProxyLeaseError,
)


CONTEXT = {
    "source": "fbref",
    "dag_id": "dag_ingest_fbref",
    "run_id": "control-run",
    "task_id": "run_live_waves",
    "canonical_url": "https://fbref.com/en/",
}


def _stats(total: int, *, closed: bool = False) -> FBrefLeaseStats:
    return FBrefLeaseStats(
        lease_id="lease-1",
        source="fbref",
        dag_id="dag_ingest_fbref",
        run_id="control-run",
        up_bytes=total // 2,
        down_bytes=total - total // 2,
        active_tunnels=0,
        reserved_bytes=0,
        closed=closed,
        budget_exceeded=False,
        close_complete=closed,
    )


class _LeaseClient:
    def __init__(self, totals):
        self.totals = iter(totals)
        self.acquired = []
        self.closed = []
        self.extended = []
        self.events = []

    def acquire(self, *, max_bytes, ttl_seconds, metadata):
        self.events.append("acquire")
        self.acquired.append((max_bytes, ttl_seconds, dict(metadata)))
        return FBrefProxyLease(
            lease_id="lease-1",
            token="secret-token",
            proxy_url="http://fbref_proxy_filter:8900",
            max_bytes=max_bytes,
            expires_at=9999999999.0,
        )

    @staticmethod
    def playwright_proxy(lease):
        return {
            "server": lease.proxy_url,
            "username": "lease",
            "password": lease.token,
        }

    def wait_drained(self, lease, *, expected):
        assert lease.lease_id == "lease-1"
        assert expected == CONTEXT
        self.events.append("wait_drained")
        return _stats(next(self.totals))

    def extend(self, lease, *, max_bytes, expected):
        assert lease.lease_id == "lease-1"
        assert expected == CONTEXT
        self.events.append("extend")
        self.extended.append(max_bytes)
        return replace(lease, max_bytes=max_bytes)

    def close(self, lease, *, expected):
        self.events.append("close")
        self.closed.append(lease.lease_id)
        return _stats(next(self.totals), closed=True)


def _response() -> FetchResponse:
    return FetchResponse(
        url="https://fbref.com/en/comps/9",
        status_code=200,
        body=b"<html><table></table></html>",
        headers={"content-type": "text/html"},
        latency_ms=5,
        http_wire_bytes=70,
        decoded_html_bytes=28,
        http_requests=1,
        http_status_history=(200,),
    )


def _fetcher(client) -> FBrefFetcher:
    fetcher = FBrefFetcher(
        proxy_file="/credentials/must-not-be-read.txt",
        provider_context=CONTEXT,
        provider_max_bytes=1000,
        lease_client=client,
    )
    assert fetcher._next_proxy() == {
        "server": "http://fbref_proxy_filter:8900",
        "username": "lease",
        "password": "secret-token",
    }
    return fetcher


def test_paid_fetch_uses_authoritative_provider_delta_and_drains_tunnel():
    client = _LeaseClient([125])
    fetcher = _fetcher(client)
    session = MagicMock()
    fetcher._http_session = session
    fetcher._fetch_without_provider_meter = MagicMock(return_value=_response())

    response = fetcher.fetch(
        "https://fbref.com/en/comps/9",
        page_kind="competition",
    )

    assert response.provider_billed_bytes == 125
    session.close.assert_called_once_with()
    assert fetcher._provider_lease is not None
    assert client.acquired[0][0] == 1000


def test_geoip_conservative_bytes_do_not_double_provider_accounting():
    client = _LeaseClient([125])
    fetcher = _fetcher(client)
    fetcher._http_session = MagicMock()
    fetcher._fetch_without_provider_meter = MagicMock(
        return_value=replace(
            _response(),
            browser_unobserved_bytes=GEOIP_BYTE_RESERVATION_BYTES,
        )
    )

    response = fetcher.fetch(
        "https://fbref.com/en/comps/9",
        page_kind="competition",
    )

    assert response.browser_unobserved_bytes == GEOIP_BYTE_RESERVATION_BYTES
    # The proxy-filter counter is authoritative; the conservative local byte
    # charge is diagnostics/cap pressure, not added to provider settlement.
    assert response.provider_billed_bytes == 125


def test_paid_fetch_error_carries_authoritative_provider_delta():
    client = _LeaseClient([77])
    fetcher = _fetcher(client)
    fetcher._http_session = MagicMock()
    fetcher._fetch_without_provider_meter = MagicMock(
        side_effect=FetchError(
            "blocked",
            error_class="http_status",
            http_status=403,
            wire_bytes=50,
            target_requests=1,
            http_status_history=(403,),
        )
    )

    with pytest.raises(FetchError) as raised:
        fetcher.fetch(
            "https://fbref.com/en/comps/9",
            page_kind="competition",
        )

    assert raised.value.error_class == "http_status"
    assert raised.value.provider_billed_bytes == 77


def test_unresolved_drain_and_close_are_terminal_and_retain_lease_handle():
    client = _LeaseClient([])
    fetcher = _fetcher(client)
    original_lease = fetcher._provider_lease
    fetcher._http_session = MagicMock()
    fetcher._fetch_without_provider_meter = MagicMock(
        side_effect=FetchError(
            "target status=503 body_sha256=abc",
            error_class="http_status",
            http_status=503,
            wire_bytes=70,
            browser_document_bytes=20,
            browser_asset_bytes=10,
            browser_unobserved_bytes=5,
            target_requests=1,
            http_status_history=(503,),
            latency_ms=9,
        )
    )
    client.wait_drained = MagicMock(
        side_effect=FBrefProxyLeaseError("drain counter unavailable")
    )
    client.close = MagicMock(
        side_effect=FBrefProxyLeaseError("final close counter unavailable")
    )

    with pytest.raises(FetchError) as raised:
        fetcher.fetch(
            "https://fbref.com/en/comps/9",
            page_kind="competition",
        )

    error = raised.value
    assert error.error_class == "hard_transport_policy"
    assert error.provider_billed_bytes is None
    assert error.http_status == 503
    assert error.wire_bytes == 70
    assert error.browser_document_bytes == 20
    assert error.browser_asset_bytes == 10
    assert error.browser_unobserved_bytes == 5
    assert error.http_requests == 1
    assert error.http_status_history == (503,)
    assert error.latency_ms == 9
    assert "drain counter unavailable" in str(error)
    assert "final close counter unavailable" in str(error)
    assert "http_status: target status=503 body_sha256=abc" in str(error)
    assert fetcher._provider_lease is original_lease
    assert len(client.acquired) == 1
    client.wait_drained.assert_called_once_with(
        original_lease,
        expected=CONTEXT,
    )
    client.close.assert_called_once_with(original_lease, expected=CONTEXT)


def test_warm_http_request_is_impossible_before_successful_extension():
    client = _LeaseClient([])
    fetcher = _fetcher(client)
    session = MagicMock()
    fetcher._http_session = session

    with pytest.raises(FetchError) as raised:
        fetcher._fetch_without_provider_meter(
            "https://fbref.com/en/comps/9",
            page_kind="competition",
        )

    assert raised.value.error_class == "hard_transport_policy"
    session.get.assert_not_called()


def test_lease_rotation_requests_only_the_remaining_hard_cap():
    client = _LeaseClient([250, 250])
    fetcher = _fetcher(client)
    fetcher._close_provider_lease()

    fetcher._next_proxy()

    assert [item[0] for item in client.acquired] == [1000, 750]
    assert client.closed == ["lease-1"]


def test_browser_rotations_share_one_proxy_enforced_phase_cap():
    client = _LeaseClient([150, 200])
    fetcher = FBrefFetcher(
        proxy_file="/credentials/must-not-be-read.txt",
        provider_context=CONTEXT,
        provider_max_bytes=1000,
        max_browser_bytes=400,
        lease_client=client,
    )

    fetcher._next_proxy()
    fetcher._next_proxy()
    fetcher._next_proxy()

    assert [item[0] for item in client.acquired] == [400, 250, 50]
    assert fetcher._provider_bootstrap_spent_bytes == 350


def test_successful_phase_boundary_closes_drains_extends_then_builds_http(
    monkeypatch,
):
    events = []
    client = _LeaseClient([100])
    fetcher = FBrefFetcher(
        proxy_file="/credentials/must-not-be-read.txt",
        provider_context=CONTEXT,
        provider_max_bytes=1000,
        max_browser_bytes=400,
        lease_client=client,
    )
    fetcher._next_proxy()
    client.events.clear()
    transport = MagicMock()
    transport.fetch.return_value = "<html><body>source</body></html>"
    transport.get_clearance.return_value = {
        "cookies": {"cf_clearance": "test"},
        "user_agent": "test-agent",
        "proxy": client.playwright_proxy(fetcher._provider_lease),
    }
    transport.close.side_effect = lambda: events.append("transport_close")
    transport.traffic_delta.return_value = {
        "real_bytes_downloaded": 100,
        "real_requests_count": 1,
    }
    fetcher._transport = transport
    session = MagicMock()

    def create_http(_fetcher, _clearance):
        events.append("create_http")
        return session

    monkeypatch.setattr(FBrefFetcher, "_create_http_session", create_http)
    original_wait = client.wait_drained
    original_extend = client.extend

    def wait_drained(*args, **kwargs):
        events.append("wait_drained")
        return original_wait(*args, **kwargs)

    def extend(*args, **kwargs):
        events.append("extend")
        return original_extend(*args, **kwargs)

    client.wait_drained = wait_drained
    client.extend = extend

    fetcher._ensure_clearance()

    assert events == ["transport_close", "wait_drained", "extend", "create_http"]
    assert fetcher._provider_lease.max_bytes == 1000
    assert fetcher._provider_http_ready is True
    assert fetcher._http_session is session


def test_run_cap_equal_to_browser_cap_needs_no_noop_extension():
    client = _LeaseClient([100])
    fetcher = FBrefFetcher(
        proxy_file="/credentials/must-not-be-read.txt",
        provider_context=CONTEXT,
        provider_max_bytes=400,
        max_browser_bytes=400,
        lease_client=client,
    )
    fetcher._next_proxy()

    fetcher._extend_provider_lease_for_http()

    assert fetcher._provider_lease.max_bytes == 400
    assert fetcher._provider_http_ready is True
    assert client.extended == []
    assert client.events[-1] == "wait_drained"


@pytest.mark.parametrize("failure_stage", ["browser_close", "provider_drain", "extend"])
def test_phase_boundary_failure_is_terminal_and_never_builds_http(
    monkeypatch, failure_stage
):
    client = _LeaseClient([100])
    fetcher = FBrefFetcher(
        proxy_file="/credentials/must-not-be-read.txt",
        provider_context=CONTEXT,
        provider_max_bytes=1000,
        max_browser_bytes=400,
        lease_client=client,
    )
    fetcher._next_proxy()
    original_lease = fetcher._provider_lease
    transport = MagicMock()
    transport.fetch.return_value = "<html><body>source</body></html>"
    transport.get_clearance.return_value = {
        "cookies": {"cf_clearance": "test"},
        "user_agent": "test-agent",
        "proxy": client.playwright_proxy(original_lease),
    }
    transport.traffic_delta.return_value = {
        "real_bytes_downloaded": 100,
        "real_requests_count": 1,
    }
    if failure_stage == "browser_close":
        transport.close.side_effect = TimeoutError("browser close timed out")
    elif failure_stage == "provider_drain":
        client.wait_drained = MagicMock(
            side_effect=FBrefProxyLeaseError("provider drain timed out")
        )
    else:
        client.extend = MagicMock(
            side_effect=FBrefProxyLeaseError("extension response lost")
        )
    fetcher._transport = transport
    create_http = MagicMock()
    monkeypatch.setattr(FBrefFetcher, "_create_http_session", create_http)

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "hard_transport_policy"
    assert fetcher._provider_lease is original_lease
    assert fetcher._provider_http_ready is False
    assert client.extended == []
    create_http.assert_not_called()


def test_real_transport_exit_failure_is_visible_and_never_extends_to_http(
    monkeypatch,
):
    client = _LeaseClient([100])
    fetcher = FBrefFetcher(
        proxy_file="/credentials/must-not-be-read.txt",
        provider_context=CONTEXT,
        provider_max_bytes=1000,
        max_browser_bytes=400,
        lease_client=client,
    )
    fetcher._next_proxy()
    original_lease = fetcher._provider_lease
    transport = CamoufoxFbrefTransport(
        geoip=False,
        max_network_requests=5,
        max_network_bytes=400,
    )
    cm = MagicMock()
    cm.__exit__.side_effect = RuntimeError("real browser close failed")
    transport._cm = cm
    transport._browser = MagicMock()
    transport._context = MagicMock()
    transport._page = MagicMock()
    transport.fetch = MagicMock(  # type: ignore[method-assign]
        return_value="<html><body>source</body></html>"
    )
    transport.get_clearance = MagicMock(  # type: ignore[method-assign]
        return_value={
            "cookies": {"cf_clearance": "test"},
            "user_agent": "test-agent",
            "proxy": client.playwright_proxy(original_lease),
        }
    )
    transport._kill_browser_processes = MagicMock()  # type: ignore[method-assign]
    fetcher._transport = transport
    create_http = MagicMock()
    monkeypatch.setattr(FBrefFetcher, "_create_http_session", create_http)

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "hard_transport_policy"
    assert "browser_finalization_failed" in str(raised.value)
    assert transport._cm is cm
    assert transport._kill_browser_processes.call_count >= 1
    assert fetcher._provider_lease is original_lease
    assert fetcher._provider_http_ready is False
    assert client.extended == []
    assert "wait_drained" not in client.events
    create_http.assert_not_called()


def test_proxy_phase_cap_exhaustion_is_hard_even_without_local_byte_flag():
    client = _LeaseClient([400])
    fetcher = FBrefFetcher(
        proxy_file="/credentials/must-not-be-read.txt",
        provider_context=CONTEXT,
        provider_max_bytes=1000,
        max_browser_bytes=400,
        lease_client=client,
    )
    fetcher._next_proxy()
    transport = MagicMock()
    transport.fetch.side_effect = RuntimeError("proxy closed the tunnel")
    transport.traffic_delta.return_value = {
        "real_bytes_downloaded": 399,
        "real_requests_count": 1,
        "byte_budget_exhausted": False,
    }
    fetcher._transport = transport

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "hard_transport_policy"
    assert "browser_provider_cap_exhausted" in str(raised.value)
    assert fetcher._provider_bootstrap_spent_bytes == 400
    assert client.extended == []


def test_server_clamped_browser_lease_cap_is_also_a_terminal_hard_stop():
    client = _LeaseClient([300])
    fetcher = FBrefFetcher(
        proxy_file="/credentials/must-not-be-read.txt",
        provider_context=CONTEXT,
        provider_max_bytes=1000,
        max_browser_bytes=400,
        lease_client=client,
    )
    fetcher._next_proxy()
    fetcher._provider_lease = replace(fetcher._provider_lease, max_bytes=300)
    transport = MagicMock()
    transport.fetch.side_effect = RuntimeError("proxy cap closed the tunnel")
    transport.traffic_delta.return_value = {
        "real_bytes_downloaded": 299,
        "real_requests_count": 1,
        "byte_budget_exhausted": False,
    }
    fetcher._transport = transport

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "hard_transport_policy"
    assert "browser_provider_cap_exhausted" in str(raised.value)
    assert fetcher._provider_bootstrap_spent_bytes == 300


def test_new_clearance_reservation_resets_only_the_finished_phase_spend():
    client = _LeaseClient([100, 200])
    fetcher = FBrefFetcher(
        proxy_file="/credentials/must-not-be-read.txt",
        provider_context=CONTEXT,
        provider_max_bytes=1000,
        max_browser_bytes=400,
        lease_client=client,
    )
    fetcher._next_proxy()
    fetcher._extend_provider_lease_for_http()
    assert fetcher._provider_bootstrap_spent_bytes == 100
    fetcher._transport = MagicMock()
    replacement_transport = MagicMock()
    fetcher._create_transport = MagicMock(return_value=replacement_transport)

    fetcher.reset_clearance()
    fetcher._next_proxy()

    assert fetcher._provider_bootstrap_spent_bytes == 0
    assert [item[0] for item in client.acquired] == [400, 400]
    assert fetcher._transport is replacement_transport


def test_failed_lease_close_retains_handle_and_blocks_new_acquire():
    client = _LeaseClient([])
    fetcher = _fetcher(client)
    original = fetcher._provider_lease
    client.close = MagicMock(side_effect=RuntimeError("close unavailable"))

    with pytest.raises(RuntimeError, match="close unavailable"):
        fetcher._next_proxy()

    assert fetcher._provider_lease is original
    assert len(client.acquired) == 1
    assert fetcher._provider_total_bytes == 0

    client.close.side_effect = None
    client.close.return_value = _stats(125, closed=True)
    fetcher._close_provider_lease()

    assert fetcher._provider_lease is None
    assert fetcher._provider_total_bytes == 125
    assert len(client.acquired) == 1


def test_proxy_file_cannot_enable_an_unmetered_paid_proxy(monkeypatch):
    monkeypatch.delenv("FBREF_PROXY_CONTROL_URL", raising=False)
    monkeypatch.delenv("FBREF_PROXY_CONTROL_TOKEN", raising=False)

    with pytest.raises(FBrefProxyLeaseError, match="direct proxy credentials"):
        FBrefFetcher(
            proxy_file="/tmp/upstream-secret.txt",
            provider_context=CONTEXT,
            provider_max_bytes=1000,
        )


def test_live_provider_context_cannot_fall_back_to_direct(monkeypatch):
    monkeypatch.delenv("FBREF_PROXY_CONTROL_URL", raising=False)

    with pytest.raises(FBrefProxyLeaseError, match="direct proxy credentials"):
        FBrefFetcher(
            provider_context=CONTEXT,
            provider_max_bytes=1000,
        )
