from __future__ import annotations

import time
import pytest

from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrTimeout,
)
from scrapers.whoscored.transport import (
    CachedPayload,
    FailureKind,
    ProxyFilterClient,
    ProxyLease,
    TransportBudgets,
    TransportRoute,
    WhoScoredTransport,
    WhoScoredTransportError,
    is_cloudflare_response,
)


CF_HTML = b"<html><title>Just a moment...</title><script src='/cdn-cgi/challenge-platform/x'></script></html>"
OK_HTML = b"<html><script>var matchCentreData = {}</script></html>"


class FakeHTTPResponse:
    def __init__(self, status_code=200, content=OK_HTML, headers=None, wire_bytes=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.download_size = len(content) if wire_bytes is None else wire_bytes


class FakeHTTPSession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.closed = False

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        self.closed = True


class FakeFSClient:
    def __init__(self, *results):
        self.results = list(results)
        self.created = []
        self.destroyed = []
        self.get_calls = []

    def create_session(self, session_id, proxy_url=None):
        self.created.append((session_id, proxy_url))

    def get(self, url, session_id, **kwargs):
        self.get_calls.append((url, session_id, kwargs))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def destroy_session(self, session_id):
        self.destroyed.append(session_id)

    def get_traffic_stats(self):
        return {"sessions_created": len(self.created)}


class FakeProxyClient:
    def __init__(self, *, up=100, down=900):
        self.created = []
        self.closed = []
        self.up = up
        self.down = down

    def create_lease(self, *, max_bytes, ttl_seconds):
        self.created.append((max_bytes, ttl_seconds))
        return ProxyLease(
            lease_id="lease-1",
            token="secret",
            proxy_url="http://lease:secret@proxy_filter:8899",
            max_bytes=max_bytes,
            expires_at=time.time() + ttl_seconds,
        )

    def close(self, lease):
        self.closed.append(lease)
        return {
            "id": lease.lease_id,
            "up_bytes": self.up,
            "down_bytes": self.down,
            "total_bytes": self.up + self.down,
        }


class MemoryRawCache:
    def __init__(self, payload=None):
        self.payload = payload
        self.stored = []

    def load(self, key):
        return self.payload

    def store(self, key, payload, sha256):
        self.stored.append((key, payload, sha256))


def _transport(
    direct_http,
    *,
    direct_fs=None,
    paid_fs=None,
    proxy=None,
    paid_http=None,
    raw_cache=None,
    budgets=None,
    attempts=2,
):
    factory_calls = []

    def factory(proxy_url):
        factory_calls.append(proxy_url)
        assert paid_http is not None
        return paid_http

    transport = WhoScoredTransport(
        direct_http_session=direct_http,
        direct_fs_client=direct_fs or FakeFSClient(),
        paid_fs_client=paid_fs or FakeFSClient(),
        proxy_client=proxy,
        paid_proxy_url="http://proxy_filter:8899" if proxy else None,
        http_session_factory=factory,
        raw_cache=raw_cache,
        budgets=budgets,
        direct_browser_attempts=attempts,
    )
    return transport, factory_calls


@pytest.mark.unit
def test_direct_http_success_never_starts_browser_or_proxy():
    direct = FakeHTTPSession(FakeHTTPResponse(wire_bytes=777))
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    transport, factory_calls = _transport(direct, direct_fs=fs, proxy=proxy)

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.route is TransportRoute.DIRECT_HTTP
    assert result.wire_bytes == 777
    assert fs.created == []
    assert proxy.created == []
    assert factory_calls == []
    assert transport.get_traffic_stats()["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_valid_raw_cache_skips_every_network_route():
    cache = MemoryRawCache(CachedPayload(content=OK_HTML))
    direct = FakeHTTPSession()
    transport, _ = _transport(direct, raw_cache=cache)

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.route is TransportRoute.RAW_CACHE
    assert direct.calls == []
    assert transport.get_traffic_stats()["cache_hits"] == 1


@pytest.mark.unit
def test_success_is_stored_in_raw_cache_before_return():
    cache = MemoryRawCache()
    direct = FakeHTTPSession(FakeHTTPResponse(content=b"source"))
    transport, _ = _transport(direct, raw_cache=cache)

    result = transport.fetch("https://www.whoscored.com/Players/1/Show", cache_key="p1")

    assert cache.stored[0][0] == "p1"
    assert cache.stored[0][1].content == b"source"
    assert cache.stored[0][2] == result.sha256


@pytest.mark.unit
def test_parser_rejection_is_stored_raw_before_it_is_raised():
    cache = MemoryRawCache()
    direct = FakeHTTPSession(FakeHTTPResponse(content=b"new source layout"))
    transport, _ = _transport(direct, raw_cache=cache)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(
            "https://www.whoscored.com/Players/1/Show",
            cache_key="p1",
            validator=lambda _: False,
        )

    assert exc.value.kind is FailureKind.CONTENT
    assert cache.stored[0][0] == "p1"
    assert cache.stored[0][1].content == b"new source layout"


@pytest.mark.unit
def test_parser_rejected_raw_cache_never_refetches_the_source():
    cache = MemoryRawCache(CachedPayload(content=b"new source layout"))
    direct = FakeHTTPSession(FakeHTTPResponse(content=b"second download"))
    transport, _ = _transport(direct, raw_cache=cache)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(
            "https://www.whoscored.com/Players/1/Show",
            cache_key="p1",
            validator=lambda _: False,
        )

    assert exc.value.kind is FailureKind.CONTENT
    assert direct.calls == []
    assert transport.get_traffic_stats()["cache_invalid"] == 1


@pytest.mark.unit
@pytest.mark.parametrize("status", [404, 429, 500])
def test_ordinary_http_error_never_enables_browser_or_proxy(status):
    direct = FakeHTTPSession(
        FakeHTTPResponse(status_code=status, content=b"ordinary origin error")
    )
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/x")

    assert exc.value.kind is FailureKind.HTTP_STATUS
    assert fs.created == []
    assert proxy.created == []


@pytest.mark.unit
def test_direct_cloudflare_uses_fresh_direct_browser_with_media_disabled():
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"server": "cloudflare"})
    )
    fs = FakeFSClient(
        {"html": OK_HTML.decode(), "status": 200, "cookies": [], "userAgent": "x"}
    )
    transport, _ = _transport(direct, direct_fs=fs)

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.created) == 1
    assert fs.created[0][1] is None
    assert fs.get_calls[0][2]["disable_media"] is True
    assert fs.destroyed == [fs.created[0][0]]


@pytest.mark.unit
def test_timeout_from_direct_browser_does_not_enable_paid_proxy():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(FlareSolverrTimeout("down"))
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.TIMEOUT
    assert proxy.created == []


@pytest.mark.unit
def test_paid_curl_only_after_every_direct_browser_attempt_is_cf():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    paid = FakeHTTPSession(FakeHTTPResponse(content=b"paid success"))
    paid_fs = FakeFSClient()
    proxy = FakeProxyClient(up=123, down=456)
    transport, factory_calls = _transport(
        direct,
        direct_fs=direct_fs,
        paid_fs=paid_fs,
        proxy=proxy,
        paid_http=paid,
    )

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live?d=1")

    assert result.route is TransportRoute.PAID_HTTP
    assert len(direct_fs.created) == 2
    assert len(proxy.created) == 1
    assert factory_calls == ["http://lease:secret@proxy_filter:8899"]
    assert paid_fs.created == []
    stats = transport.get_traffic_stats()
    assert stats["paid_proxy_bytes"] == 579
    assert stats["paid_proxy_bytes_by_url"] == {
        "www.whoscored.com/Matches/1/Live": 579
    }


@pytest.mark.unit
def test_paid_browser_uses_same_lease_but_separate_fs_client():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    paid_http = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"server": "cloudflare"})
    )
    paid_fs = FakeFSClient({"html": OK_HTML.decode(), "status": 200})
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=direct_fs,
        paid_fs=paid_fs,
        proxy=proxy,
        paid_http=paid_http,
    )

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.route is TransportRoute.PAID_FLARESOLVERR
    assert paid_fs.created[0][1] == "http://lease:secret@proxy_filter:8899"
    assert paid_fs.get_calls[0][2]["disable_media"] is True
    assert all(proxy_url is None for _, proxy_url in direct_fs.created)


@pytest.mark.unit
def test_validator_rejection_is_not_treated_as_cloudflare():
    direct = FakeHTTPSession(FakeHTTPResponse(content=b"wrong page"))
    proxy = FakeProxyClient()
    fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/x", validator=lambda _: False)

    assert exc.value.kind is FailureKind.CONTENT
    assert fs.created == []
    assert proxy.created == []


@pytest.mark.unit
def test_cloudflare_classifier_does_not_misclassify_header_only_429():
    assert not is_cloudflare_response(
        429, {"server": "cloudflare", "cf-ray": "abc"}, b"rate limited"
    )
    assert is_cloudflare_response(429, {}, CF_HTML)


@pytest.mark.unit
def test_paid_limit_is_zero_when_no_urls_are_eligible():
    budgets = TransportBudgets.for_eligible_urls(0)
    assert budgets.max_paid_urls == 0


@pytest.mark.unit
def test_proxy_control_client_uses_dedicated_authenticated_lease_listener():
    response = FakeHTTPResponse()
    response.raise_for_status = lambda: None
    response.json = lambda: {
        "id": "abc",
        "token": "s/ecret",
        "proxy_url": "http://proxy_filter:8900",
        "max_bytes": 1234,
        "expires_at": 99.0,
    }
    session = SimpleControlSession(response)
    client = ProxyFilterClient(
        "http://proxy_filter:8899", session=session, timeout=3
    )

    lease = client.create_lease(max_bytes=1234, ttl_seconds=60)

    assert session.trust_env is False
    assert session.posts[0][0] == "http://proxy_filter:8899/v1/leases"
    assert lease.proxy_url == "http://lease:s%2Fecret@proxy_filter:8900"


class SimpleControlSession:
    def __init__(self, response):
        self.response = response
        self.posts = []
        self.trust_env = True

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.response
