from __future__ import annotations

from unittest.mock import MagicMock

import pytest

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

    def acquire(self, *, max_bytes, ttl_seconds, metadata):
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
        return _stats(next(self.totals))

    def close(self, lease, *, expected):
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


def test_lease_rotation_requests_only_the_remaining_hard_cap():
    client = _LeaseClient([250, 250])
    fetcher = _fetcher(client)
    fetcher._close_provider_lease()

    fetcher._next_proxy()

    assert [item[0] for item in client.acquired] == [1000, 750]
    assert client.closed == ["lease-1"]


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
