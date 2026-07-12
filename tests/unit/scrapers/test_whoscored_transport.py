from __future__ import annotations

import json
import sys
import time
import types
import pytest

from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrResponseTooLarge,
    FlareSolverrTimeout,
)
from scrapers.whoscored.transport import (
    CachedPayload,
    FailureKind,
    FetchRequest,
    JsonlRequestLedger,
    ProxyBudgetRejected,
    ProxyFilterClient,
    ProxyLease,
    TransportBudgets,
    TransportContext,
    TransportRoute,
    WhoScoredTransport,
    WhoScoredTransportError,
    is_cloudflare_response,
    is_whoscored_structured_feed_access_gate,
)


CF_HTML = b"<html><title>Just a moment...</title><script src='/cdn-cgi/challenge-platform/x'></script></html>"
CF_ATTENTION_HTML = b"""<!doctype html>
<html><head><title>Attention Required! | Cloudflare</title>
<link rel="stylesheet" href="/cdn-cgi/styles/cf.errors.css"></head></html>"""
OK_HTML = b"<html><script>var matchCentreData = {}</script></html>"
MASKED_STATS_HTML = b"""
<html><body><script src="/Content/js/verify-client.js"></script>
The page you requested does not exist.</body></html>
"""
MOVED_STATS_HTML = b"""
<html><head><script src="https://cx-resources.oddschecker.com/fingerprint/verify-client.js?v1.0.1"></script>
<title>Object moved</title></head><body>
<h2>Object moved to <a href="/404.html?aspxerrorpath=/statisticsfeed/1/getteamstatistics">here</a>.</h2>
</body></html>
"""
TEAM_STATS_URL = (
    "https://www.whoscored.com/statisticsfeed/1/getteamstatistics?stageId=23752"
)
PLAYER_STATS_URL = (
    "https://www.whoscored.com/statisticsfeed/1/getplayerstatistics?stageId=23752"
)
TEAM_STATS_BOOTSTRAP = (
    "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/9967/"
    "Stages/23752/TeamStatistics"
)
PLAYER_STATS_BOOTSTRAP = (
    "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/9967/"
    "Stages/23753/TeamStatistics"
)


def _batch_solution(url, content=b'{"teamTableStats":[]}'):
    return {
        "ok": True,
        "content": content,
        "headers": {"content-type": "application/json"},
        "status": 200,
        "responseBytes": len(content),
        "finalUrl": url,
    }


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

    def get(self, url, timeout, **kwargs):
        self.calls.append((url, timeout, kwargs))
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
        self.xhr_calls = []
        self.xhr_many_calls = []
        self.closed = False

    def create_session(self, session_id, proxy_url=None):
        self.created.append((session_id, proxy_url))

    def get(self, url, session_id, **kwargs):
        self.get_calls.append((url, session_id, kwargs))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def xhr_get(self, url, session_id, **kwargs):
        self.xhr_calls.append((url, session_id, kwargs))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def xhr_get_many(self, urls, session_id, **kwargs):
        self.xhr_many_calls.append((list(urls), session_id, kwargs))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def destroy_session(self, session_id):
        self.destroyed.append(session_id)

    def get_traffic_stats(self):
        return {"sessions_created": len(self.created)}

    def close(self):
        self.closed = True


class FakeProxyClient:
    def __init__(self, *, up=100, down=900, stats_override=None):
        self.created = []
        self.closed = []
        self.up = up
        self.down = down
        self.stats_override = stats_override
        self.session_closed = False

    def create_lease(self, *, max_bytes, ttl_seconds, context=None, canonical_url=""):
        self.created.append((max_bytes, ttl_seconds, context, canonical_url))
        return ProxyLease(
            lease_id="lease-1",
            token="secret",
            proxy_url="http://lease:secret@proxy_filter:8899",
            max_bytes=max_bytes,
            expires_at=time.time() + ttl_seconds,
        )

    def close(self, lease):
        self.closed.append(lease)
        report = {
            "id": lease.lease_id,
            "up_bytes": self.up,
            "down_bytes": self.down,
            "total_bytes": self.up + self.down,
            "canonical_url": self.created[-1][3],
        }
        if self.stats_override is not None:
            report.update(self.stats_override)
        return report

    def close_session(self):
        self.session_closed = True


class MemoryRawCache:
    def __init__(self, payload=None):
        self.payload = payload
        self.stored = []

    def load(self, key):
        return self.payload

    def store(self, key, payload, sha256):
        self.stored.append((key, payload, sha256))


class KeyedMemoryRawCache:
    def __init__(self):
        self.payloads = {}
        self.stored = []

    def load(self, key):
        return self.payloads.get(key)

    def store(self, key, payload, sha256):
        self.payloads[key] = payload
        self.stored.append((key, payload, sha256))


class MemoryLedger:
    def __init__(self):
        self.events = []

    def append(self, event):
        self.events.append(dict(event))


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
    http_attempts=3,
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
        direct_http_attempts=http_attempts,
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
def test_curl_sessions_never_inherit_ambient_proxy_configuration(monkeypatch):
    created = []

    class FakeCurlSession:
        def __init__(self, **kwargs):
            created.append(dict(kwargs))
            self.headers = {}
            self.proxies = {}

    requests_module = types.ModuleType("curl_cffi.requests")
    requests_module.Session = FakeCurlSession
    package = types.ModuleType("curl_cffi")
    package.requests = requests_module
    monkeypatch.setitem(sys.modules, "curl_cffi", package)
    monkeypatch.setitem(sys.modules, "curl_cffi.requests", requests_module)
    transport = WhoScoredTransport.__new__(WhoScoredTransport)
    transport._http_session_factory = None
    transport.impersonate = "chrome120"

    direct = transport._new_http_session(None)
    paid = transport._new_http_session("http://lease:token@proxy_filter:8900")

    assert created == [
        {"impersonate": "chrome120", "trust_env": False},
        {"impersonate": "chrome120", "trust_env": False},
    ]
    assert direct.proxies == {}
    assert paid.proxies == {
        "http": "http://lease:token@proxy_filter:8900",
        "https": "http://lease:token@proxy_filter:8900",
    }


@pytest.mark.unit
def test_valid_raw_cache_skips_every_network_route():
    cache = MemoryRawCache(CachedPayload(content=OK_HTML))
    direct = FakeHTTPSession()
    transport, _ = _transport(direct, raw_cache=cache)
    network_gates = []

    result = transport.fetch(
        "https://www.whoscored.com/Matches/1/Live",
        before_network=lambda: network_gates.append("acquired"),
    )

    assert result.route is TransportRoute.RAW_CACHE
    assert direct.calls == []
    assert network_gates == []
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
def test_parser_rejected_raw_cache_retries_direct_once_without_proxy():
    cache = MemoryRawCache(CachedPayload(content=b"new source layout"))
    direct = FakeHTTPSession(FakeHTTPResponse(content=b"second download"))
    transport, _ = _transport(direct, raw_cache=cache)
    network_gates = []

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(
            "https://www.whoscored.com/Players/1/Show",
            cache_key="p1",
            validator=lambda _: False,
            before_network=lambda: network_gates.append("acquired"),
        )

    assert exc.value.kind is FailureKind.CONTENT
    assert len(direct.calls) == 1
    assert network_gates == ["acquired"]
    assert cache.stored[-1][1].content == b"second download"
    assert transport.get_traffic_stats()["cache_invalid"] == 1


@pytest.mark.unit
@pytest.mark.parametrize("status", [404, 429, 500])
def test_ordinary_http_error_never_enables_browser_or_proxy(status):
    response = FakeHTTPResponse(status_code=status, content=b"ordinary origin error")
    direct = FakeHTTPSession(*([response] * (3 if status >= 500 else 1)))
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/x")

    assert exc.value.kind is FailureKind.HTTP_STATUS
    assert len(direct.calls) == (3 if status >= 500 else 1)
    assert fs.created == []
    assert proxy.created == []


@pytest.mark.unit
def test_transient_direct_502_retries_with_a_token_and_never_uses_proxy():
    direct = FakeHTTPSession(
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(content=OK_HTML),
    )
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)
    tokens = []

    result = transport.fetch(
        "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/10498",
        before_network=lambda: tokens.append("token"),
    )

    assert result.route is TransportRoute.DIRECT_HTTP
    assert len(direct.calls) == 3
    assert tokens == ["token", "token", "token"]
    assert fs.created == []
    assert proxy.created == []


@pytest.mark.unit
def test_structured_feed_access_gate_is_exact_and_source_scoped():
    assert is_whoscored_structured_feed_access_gate(
        TEAM_STATS_URL, 200, MASKED_STATS_HTML
    )
    assert not is_whoscored_structured_feed_access_gate(
        "https://www.whoscored.com/Matches/1/Live", 200, MASKED_STATS_HTML
    )
    assert not is_whoscored_structured_feed_access_gate(
        TEAM_STATS_URL, 404, MASKED_STATS_HTML
    )
    assert not is_whoscored_structured_feed_access_gate(
        TEAM_STATS_URL, 200, b"The page you requested does not exist"
    )


@pytest.mark.unit
def test_exact_same_origin_masked_feed_redirect_is_a_direct_browser_gate():
    headers = {
        "location": "/404.html?aspxerrorpath=/statisticsfeed/1/getteamstatistics",
        "server": "cloudflare",
    }

    assert is_whoscored_structured_feed_access_gate(
        TEAM_STATS_URL,
        302,
        MOVED_STATS_HTML,
        headers,
    )
    for location in (
        "https://evil.test/404.html?aspxerrorpath=/statisticsfeed/1/getteamstatistics",
        "/404.html?aspxerrorpath=/statisticsfeed/1/getplayerstatistics",
        "/404.html?aspxerrorpath=/statisticsfeed/1/getteamstatistics&next=evil",
        "/other?aspxerrorpath=/statisticsfeed/1/getteamstatistics",
    ):
        assert not is_whoscored_structured_feed_access_gate(
            TEAM_STATS_URL,
            302,
            MOVED_STATS_HTML,
            {"location": location},
        )


@pytest.mark.unit
def test_masked_structured_feed_uses_bootstrap_then_same_session_xhr_direct_only():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
            "cookies": [],
            "userAgent": "browser",
        },
        {
            "content": b'{"teamTableStats":[]}',
            "headers": {"content-type": "application/json"},
            "status": 200,
            "responseBytes": 21,
            "finalUrl": TEAM_STATS_URL,
        },
    )
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    result = transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert result.content == b'{"teamTableStats":[]}'
    assert len(fs.created) == 1
    assert fs.get_calls[0][0] == TEAM_STATS_BOOTSTRAP
    assert fs.xhr_calls[0][0] == TEAM_STATS_URL
    assert fs.get_calls[0][1] == fs.xhr_calls[0][1]
    assert direct.calls[0][2]["headers"]["X-Requested-With"] == "XMLHttpRequest"
    assert direct.calls[0][2]["headers"]["Referer"] == TEAM_STATS_BOOTSTRAP
    assert direct.calls[0][2]["allow_redirects"] is False
    assert proxy.created == []
    assert transport.get_traffic_stats()["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_structured_redirect_is_not_followed_or_promoted_to_browser_or_paid():
    direct = FakeHTTPSession(
        FakeHTTPResponse(302, b"redirect", {"location": "https://evil.test/"})
    )
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )

    assert exc.value.kind is FailureKind.HTTP_STATUS
    assert exc.value.status_code == 302
    assert direct.calls[0][2]["allow_redirects"] is False
    assert fs.created == []
    assert proxy.created == []


@pytest.mark.unit
def test_exact_masked_redirect_is_not_followed_but_uses_direct_browser():
    direct = FakeHTTPSession(
        FakeHTTPResponse(
            302,
            MOVED_STATS_HTML,
            {
                "location": (
                    "/404.html?aspxerrorpath=/statisticsfeed/1/getteamstatistics"
                )
            },
        )
    )
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
            "cookies": [],
            "userAgent": "browser",
        },
        {
            "content": b'{"teamTableStats":[]}',
            "headers": {"content-type": "application/json"},
            "status": 200,
            "responseBytes": 21,
            "finalUrl": TEAM_STATS_URL,
        },
    )
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    result = transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert direct.calls[0][2]["allow_redirects"] is False
    assert len(fs.created) == 1
    assert proxy.created == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "bootstrap_url",
    [
        TEAM_STATS_BOOTSTRAP + "?next=https://evil.test",
        TEAM_STATS_BOOTSTRAP + "#fragment",
        TEAM_STATS_BOOTSTRAP.replace("/Regions/247/", "/Regions/x/"),
        TEAM_STATS_BOOTSTRAP.replace("/TeamStatistics", "/PlayerStatistics"),
        TEAM_STATS_BOOTSTRAP + "/extra",
        TEAM_STATS_BOOTSTRAP.replace("/Stages/", "/Stages%2f"),
    ],
)
def test_structured_bootstrap_must_match_exact_numeric_team_statistics_path(
    bootstrap_url,
):
    direct = FakeHTTPSession(FakeHTTPResponse())
    fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(TEAM_STATS_URL, browser_bootstrap_url=bootstrap_url)

    assert exc.value.kind is FailureKind.CONFIG
    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
def test_structured_feed_allowlist_rejects_arbitrary_prefixed_path_before_network():
    direct = FakeHTTPSession(FakeHTTPResponse())
    fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(
            "https://www.whoscored.com/statisticsfeed/1/arbitrary",
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )

    assert exc.value.kind is FailureKind.CONFIG
    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
def test_structured_feeds_reuse_one_validated_stage_bootstrap():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
            "cookies": [],
            "userAgent": "browser",
        },
        {
            "content": b'{"teamTableStats":[]}',
            "headers": {"content-type": "application/json"},
            "status": 200,
            "responseBytes": 21,
            "finalUrl": TEAM_STATS_URL,
        },
        {
            "content": b'{"teamTableStats":[]}',
            "headers": {"content-type": "application/json"},
            "status": 200,
            "responseBytes": 21,
            "finalUrl": TEAM_STATS_URL + "&category=offensive",
        },
    )
    transport, _ = _transport(direct, direct_fs=fs)

    transport.fetch(TEAM_STATS_URL, browser_bootstrap_url=TEAM_STATS_BOOTSTRAP)
    transport.fetch(
        TEAM_STATS_URL + "&category=offensive",
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
    )

    assert len(fs.created) == 1
    assert len(fs.get_calls) == 1
    assert len(fs.xhr_calls) == 2
    assert fs.xhr_calls[0][1] == fs.xhr_calls[1][1]
    # The source-specific gate is stable for this exact bootstrap, so the
    # second raw miss avoids a duplicate direct request.
    assert len(direct.calls) == 1


@pytest.mark.unit
def test_structured_batch_uses_one_direct_gate_then_bounded_browser_batch():
    second_url = TEAM_STATS_URL + "&category=offensive"
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
            "cookies": [],
            "userAgent": "browser",
        },
        [_batch_solution(TEAM_STATS_URL), _batch_solution(second_url)],
    )
    cache = KeyedMemoryRawCache()
    ledger = MemoryLedger()
    transport, _ = _transport(direct, direct_fs=fs, raw_cache=cache)
    transport.request_ledger = ledger
    gates = []
    requests = [
        FetchRequest(
            url=url,
            cache_key=f"feed-{index}",
            validator=lambda response: json.loads(response.content) is not None,
            before_network=lambda: gates.append("acquired"),
            scope="INT-World Cup=2026",
            entity="team_stage_statistics",
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )
        for index, url in enumerate((TEAM_STATS_URL, second_url))
    ]

    results = transport.fetch_many(requests)

    assert [result.route for result in results] == [
        TransportRoute.DIRECT_FLARESOLVERR,
        TransportRoute.DIRECT_FLARESOLVERR,
    ]
    assert len(direct.calls) == 1
    assert len(fs.get_calls) == 1
    assert [call[0] for call in fs.xhr_many_calls] == [[TEAM_STATS_URL, second_url]]
    assert gates == ["acquired", "acquired"]
    assert [item[0] for item in cache.stored] == ["feed-0", "feed-1"]
    traffic = transport.get_traffic_stats()
    assert traffic["browser_batches"] == 1
    assert traffic["browser_batch_items"] == 2
    success_events = [
        event
        for event in ledger.events
        if event["status"] == "success"
        and "/statisticsfeed/" in event["url"]
        and event["cache_key"] in {"feed-0", "feed-1"}
    ]
    assert {event["cache_key"] for event in success_events} == {"feed-0", "feed-1"}
    assert all(event["response_bytes"] == 21 for event in success_events)
    for cache_key in ("feed-0", "feed-1"):
        logical_events = [
            event for event in ledger.events if event["cache_key"] == cache_key
        ]
        assert len({event["request_id"] for event in logical_events}) == 1


@pytest.mark.unit
def test_eight_structured_source_urls_consume_exactly_eight_rate_tokens():
    urls = [TEAM_STATS_URL + f"&category=token-{index}" for index in range(8)]
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        [_batch_solution(url) for url in urls],
    )
    transport, _ = _transport(direct, direct_fs=fs)
    tokens = []

    transport.fetch_many(
        [
            FetchRequest(
                url=url,
                cache_key=f"token-feed-{index}",
                before_network=lambda: tokens.append("token"),
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
            for index, url in enumerate(urls)
        ]
    )

    assert len(direct.calls) == 1
    assert len(fs.xhr_many_calls) == 1
    assert tokens == ["token"] * 8


@pytest.mark.unit
def test_partial_browser_batch_caches_success_and_retry_fetches_only_failed_target():
    second_url = TEAM_STATS_URL + "&category=offensive"
    cache = KeyedMemoryRawCache()
    first_direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    first_fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        [
            _batch_solution(TEAM_STATS_URL),
            {"ok": False, "kind": "fetch_failed", "responseBytes": 0},
        ],
    )
    first, _ = _transport(
        first_direct,
        direct_fs=first_fs,
        raw_cache=cache,
        attempts=1,
    )
    requests = [
        FetchRequest(
            url=url,
            cache_key=f"feed-{index}",
            validator=lambda response: json.loads(response.content) is not None,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )
        for index, url in enumerate((TEAM_STATS_URL, second_url))
    ]

    with pytest.raises(WhoScoredTransportError) as exc:
        first.fetch_many(requests)

    assert exc.value.kind is FailureKind.BROWSER
    assert set(cache.payloads) == {"feed-0"}
    assert [call[0] for call in first_fs.xhr_many_calls] == [
        [TEAM_STATS_URL, second_url]
    ]

    retry_direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    retry_fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        [_batch_solution(second_url)],
    )
    retry, _ = _transport(
        retry_direct,
        direct_fs=retry_fs,
        raw_cache=cache,
        attempts=1,
    )

    retried = retry.fetch_many(requests)

    assert retried[0].route is TransportRoute.RAW_CACHE
    assert retried[1].route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(retry_direct.calls) == 1
    assert [call[0] for call in retry_fs.xhr_many_calls] == [[second_url]]
    assert set(cache.payloads) == {"feed-0", "feed-1"}


@pytest.mark.unit
def test_transient_browser_bootstrap_redirect_retries_direct_without_paid():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {"html": MASKED_STATS_HTML.decode(), "status": 302},
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        [_batch_solution(TEAM_STATS_URL)],
    )
    proxy = FakeProxyClient()
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        attempts=2,
    )

    result = transport.fetch_many(
        [
            FetchRequest(
                url=TEAM_STATS_URL,
                cache_key="bootstrap-redirect",
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                validator=lambda response: json.loads(response.content) is not None,
            )
        ]
    )

    assert result[0].route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.get_calls) == 2
    assert len(fs.created) == 2
    assert len(fs.destroyed) == 1
    assert proxy.created == []
    assert factory_calls == []


@pytest.mark.unit
def test_batch_runtime_error_then_cf_never_enables_paid_fallback():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    bootstrap_solution = {
        "html": "<html><body>Team Statistics</body></html>",
        "status": 200,
    }
    cf_solution = {
        "ok": True,
        "content": CF_HTML,
        "headers": {"server": "cloudflare", "cf-ray": "abc"},
        "status": 403,
        "responseBytes": len(CF_HTML),
        "finalUrl": TEAM_STATS_URL,
    }
    fs = FakeFSClient(
        bootstrap_solution,
        [{"ok": False, "kind": "fetch_failed", "responseBytes": 0}],
        bootstrap_solution,
        [cf_solution],
    )
    proxy = FakeProxyClient()
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        attempts=2,
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch_many(
            [
                FetchRequest(
                    url=TEAM_STATS_URL,
                    cache_key="mixed-evidence",
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                )
            ]
        )

    assert exc.value.kind is FailureKind.BROWSER
    assert len(fs.xhr_many_calls) == 2
    assert proxy.created == []
    assert factory_calls == []


@pytest.mark.unit
def test_batch_endpoint_error_clears_gate_before_next_direct_request():
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=b'{"recovered":true}'),
    )
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        FlareSolverrTimeout("batch endpoint unavailable"),
    )
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        attempts=1,
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch_many(
            [
                FetchRequest(
                    url=TEAM_STATS_URL,
                    cache_key="endpoint-timeout",
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                )
            ]
        )

    recovered = transport.fetch(
        TEAM_STATS_URL + "&category=recovered",
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )
    assert exc.value.kind is FailureKind.TIMEOUT
    assert recovered.route is TransportRoute.DIRECT_HTTP
    assert len(direct.calls) == 2
    assert proxy.created == []


@pytest.mark.unit
def test_batch_aggregate_413_is_nonretryable_budget_failure_and_never_paid():
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=b'{"recovered":true}'),
    )
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        FlareSolverrResponseTooLarge("FlareSolverr HTTP 413: aggregate too large"),
    )
    proxy = FakeProxyClient()
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        attempts=2,
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch_many(
            [
                FetchRequest(
                    url=TEAM_STATS_URL,
                    cache_key="aggregate-limit",
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                )
            ]
        )

    # The terminal error also clears bootstrap gate evidence. A later logical
    # URL must try direct again instead of treating the limit as CF evidence.
    recovered = transport.fetch(
        TEAM_STATS_URL + "&category=after-budget",
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert exc.value.kind is FailureKind.BUDGET
    assert exc.value.retryable is False
    assert recovered.route is TransportRoute.DIRECT_HTTP
    assert len(direct.calls) == 2
    assert proxy.created == []
    assert factory_calls == []


@pytest.mark.unit
def test_structured_transport_chunks_large_scope_at_server_batch_limit():
    urls = [TEAM_STATS_URL + f"&category=item-{index}" for index in range(17)]
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        [_batch_solution(url) for url in urls[:8]],
        [_batch_solution(url) for url in urls[8:16]],
        [_batch_solution(url) for url in urls[16:]],
    )
    transport, _ = _transport(direct, direct_fs=fs, raw_cache=KeyedMemoryRawCache())

    results = transport.fetch_many(
        [
            FetchRequest(
                url=url,
                cache_key=f"feed-{index}",
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
            for index, url in enumerate(urls)
        ]
    )

    assert len(results) == 17
    assert len(direct.calls) == 1
    assert [len(call[0]) for call in fs.xhr_many_calls] == [8, 8, 1]
    assert [url for call in fs.xhr_many_calls for url in call[0]] == urls


@pytest.mark.unit
def test_raw_cache_stays_ahead_of_an_open_direct_gate_circuit():
    cache = MemoryRawCache()
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        {
            "content": b'{"teamTableStats":[]}',
            "status": 200,
            "responseBytes": 21,
        },
    )
    transport, _ = _transport(direct, direct_fs=fs, raw_cache=cache)

    transport.fetch(TEAM_STATS_URL, browser_bootstrap_url=TEAM_STATS_BOOTSTRAP)
    cache.payload = CachedPayload(content=b'{"cached":true}')
    cached = transport.fetch(
        TEAM_STATS_URL + "&category=passing",
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert cached.route is TransportRoute.RAW_CACHE
    assert len(direct.calls) == 1
    assert len(fs.xhr_calls) == 1


@pytest.mark.unit
def test_direct_gate_circuit_is_isolated_by_exact_bootstrap():
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=MASKED_STATS_HTML),
    )
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        {
            "content": b'{"teamTableStats":[]}',
            "status": 200,
            "responseBytes": 21,
        },
        {
            "html": "<html><body>Player Statistics</body></html>",
            "status": 200,
        },
        {
            "content": b'{"teamTableStats":[]}',
            "status": 200,
            "responseBytes": 21,
        },
    )
    transport, _ = _transport(direct, direct_fs=fs)

    transport.fetch(TEAM_STATS_URL, browser_bootstrap_url=TEAM_STATS_BOOTSTRAP)
    transport.fetch(PLAYER_STATS_URL, browser_bootstrap_url=PLAYER_STATS_BOOTSTRAP)

    assert len(direct.calls) == 2
    assert [call[0] for call in fs.get_calls] == [
        TEAM_STATS_BOOTSTRAP,
        PLAYER_STATS_BOOTSTRAP,
    ]
    assert len(fs.xhr_calls) == 2


@pytest.mark.unit
def test_stale_direct_gate_circuit_rechecks_direct_and_avoids_paid_proxy():
    second_url = TEAM_STATS_URL + "&category=offensive"
    third_url = TEAM_STATS_URL + "&category=passing"
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=b'{"recovered":true}'),
        FakeHTTPResponse(content=b'{"stillDirect":true}'),
    )
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        {
            "content": b'{"teamTableStats":[]}',
            "status": 200,
            "responseBytes": 21,
        },
        FlareSolverrCFChallengeFailed("browser cf"),
    )
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}')),
        attempts=1,
    )

    def validator(response):
        return json.loads(response.content) is not None

    transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=validator,
    )
    recovered = transport.fetch(
        second_url,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=validator,
    )
    direct_again = transport.fetch(
        third_url,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=validator,
    )

    assert recovered.route is TransportRoute.DIRECT_HTTP
    assert direct_again.route is TransportRoute.DIRECT_HTTP
    # First gate, pre-paid stale recheck, then a normal direct request proving
    # that successful recovery left the circuit closed.
    assert len(direct.calls) == 3
    assert len(fs.xhr_calls) == 2
    assert proxy.created == []


@pytest.mark.unit
def test_paid_structured_feed_requires_fresh_direct_gate_after_browser_cf():
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=MASKED_STATS_HTML),
    )
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        FlareSolverrCFChallengeFailed("browser cf"),
    )
    proxy = FakeProxyClient()
    paid_http = FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}'))
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        paid_http=paid_http,
        attempts=1,
    )

    result = transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert result.route is TransportRoute.PAID_HTTP
    assert len(direct.calls) == 2
    assert len(proxy.created) == 1
    assert len(paid_http.calls) == 1
    assert all(call[2]["allow_redirects"] is False for call in direct.calls)
    assert paid_http.calls[0][2]["allow_redirects"] is False


@pytest.mark.unit
def test_parser_and_browser_errors_do_not_leave_direct_gate_circuit_open():
    parser_direct = FakeHTTPSession(
        FakeHTTPResponse(content=b'{"unexpected":true}'),
        FakeHTTPResponse(content=b'{"recovered":true}'),
    )
    parser_fs = FakeFSClient()
    parser_proxy = FakeProxyClient()
    parser_transport, _ = _transport(
        parser_direct,
        direct_fs=parser_fs,
        proxy=parser_proxy,
    )

    with pytest.raises(WhoScoredTransportError) as parser_exc:
        parser_transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            validator=lambda _: False,
        )
    parser_recovery = parser_transport.fetch(
        TEAM_STATS_URL + "&category=passing",
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert parser_exc.value.kind is FailureKind.CONTENT
    assert parser_recovery.route is TransportRoute.DIRECT_HTTP
    assert len(parser_direct.calls) == 2
    assert parser_fs.created == []
    assert parser_proxy.created == []

    browser_direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=b'{"recovered":true}'),
    )
    browser_fs = FakeFSClient(FlareSolverrTimeout("browser unavailable"))
    browser_proxy = FakeProxyClient()
    browser_transport, _ = _transport(
        browser_direct,
        direct_fs=browser_fs,
        proxy=browser_proxy,
        attempts=1,
    )

    with pytest.raises(WhoScoredTransportError) as browser_exc:
        browser_transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )
    browser_recovery = browser_transport.fetch(
        TEAM_STATS_URL + "&category=defensive",
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert browser_exc.value.kind is FailureKind.TIMEOUT
    assert browser_recovery.route is TransportRoute.DIRECT_HTTP
    assert len(browser_direct.calls) == 2
    assert browser_proxy.created == []


@pytest.mark.unit
def test_browser_xhr_bootstrap_rejects_non_allowlisted_target_before_network():
    direct = FakeHTTPSession(FakeHTTPResponse())
    fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(
            "https://www.whoscored.com/Matches/1/Live",
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )

    assert exc.value.kind is FailureKind.CONFIG
    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
def test_masked_html_on_non_feed_path_never_enables_browser_or_proxy():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(
            "https://www.whoscored.com/Matches/1/Live",
            validator=lambda _: False,
        )

    assert exc.value.kind is FailureKind.CONTENT
    assert fs.created == []
    assert proxy.created == []


@pytest.mark.unit
def test_direct_cloudflare_uses_fresh_direct_browser_with_media_disabled():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"server": "cloudflare"}))
    fs = FakeFSClient(
        {"html": OK_HTML.decode(), "status": 200, "cookies": [], "userAgent": "x"}
    )
    transport, _ = _transport(direct, direct_fs=fs)

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.created) == 1
    assert fs.created[0][1] is None
    assert fs.get_calls[0][2]["disable_media"] is True
    # A successful direct session is reused to amortize CF bootstrap traffic.
    assert fs.destroyed == []
    transport.close()
    assert fs.destroyed == [fs.created[0][0]]


@pytest.mark.unit
def test_lost_create_response_destroys_possible_orphan_browser_session():
    class CreateThenTimeoutFS(FakeFSClient):
        def create_session(self, session_id, proxy_url=None):
            self.created.append((session_id, proxy_url))
            raise FlareSolverrTimeout("create response lost")

    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = CreateThenTimeoutFS()
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.TIMEOUT
    assert len(fs.created) == 1
    assert fs.destroyed == [fs.created[0][0]]
    assert transport.get_traffic_stats()["browser_sessions"] == 0
    assert proxy.created == []


@pytest.mark.unit
def test_transport_close_releases_http_browser_and_proxy_control_pools():
    direct = FakeHTTPSession()
    direct_fs = FakeFSClient()
    paid_fs = FakeFSClient()
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=direct_fs,
        paid_fs=paid_fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(),
    )

    transport.close()

    assert direct.closed is True
    assert direct_fs.closed is True
    assert paid_fs.closed is True
    assert proxy.session_closed is True


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
    ledger = MemoryLedger()
    transport.request_ledger = ledger

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live?d=1")

    assert result.route is TransportRoute.PAID_HTTP
    assert len(direct_fs.created) == 2
    assert len(proxy.created) == 1
    assert factory_calls == ["http://lease:secret@proxy_filter:8899"]
    assert paid_fs.created == []
    stats = transport.get_traffic_stats()
    assert stats["paid_proxy_bytes"] == 579
    assert stats["paid_proxy_bytes_by_url"] == {
        "https://www.whoscored.com/Matches/1/Live?d=1": 579
    }
    accounted = [event for event in ledger.events if event["status"] == "accounted"]
    assert len(accounted) == 1
    assert accounted[0]["route"] == "paid_lease"
    assert accounted[0]["paid_routes_attempted"] == ["paid_http"]
    assert accounted[0]["final_paid_route"] == "paid_http"


@pytest.mark.unit
def test_paid_lease_is_finalized_when_http_session_creation_fails():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    proxy = FakeProxyClient(up=0, down=0)

    def failing_factory(_proxy_url):
        raise RuntimeError("session unavailable")

    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=direct_fs,
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
        paid_proxy_url="http://proxy_filter:8899",
        http_session_factory=failing_factory,
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.PROXY
    assert exc.value.retryable is True
    assert len(proxy.created) == 1
    assert len(proxy.closed) == 1


@pytest.mark.unit
def test_paid_lease_and_stats_use_full_deterministic_query_url():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    proxy = FakeProxyClient(up=100, down=200)
    transport, _ = _transport(
        direct,
        direct_fs=direct_fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b"paid success")),
    )

    transport.fetch("HTTPS://WWW.WHOSCORED.COM/Matches/1/Live?z=2&a=&a=1#fragment")

    canonical = "https://www.whoscored.com/Matches/1/Live?a=&a=1&z=2"
    assert proxy.created[0][3] == canonical
    assert transport.get_traffic_stats()["paid_proxy_bytes_by_url"] == {canonical: 300}


@pytest.mark.unit
def test_rendered_browser_challenge_without_source_headers_is_confirmed_cf():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    direct_fs = FakeFSClient(
        {"html": CF_HTML.decode(), "status": 403},
        {"html": CF_HTML.decode(), "status": 403},
    )
    paid = FakeHTTPSession(FakeHTTPResponse(content=b"paid success"))
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=direct_fs,
        proxy=proxy,
        paid_http=paid,
    )

    assert (
        transport.fetch("https://www.whoscored.com/Matches/1/Live").route
        is TransportRoute.PAID_HTTP
    )
    assert len(direct_fs.created) == 2


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
    ledger = MemoryLedger()
    transport.request_ledger = ledger

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.route is TransportRoute.PAID_FLARESOLVERR
    assert paid_fs.created[0][1] == "http://lease:secret@proxy_filter:8899"
    assert paid_fs.get_calls[0][2]["disable_media"] is True
    assert all(proxy_url is None for _, proxy_url in direct_fs.created)
    accounted = [event for event in ledger.events if event["status"] == "accounted"]
    assert accounted[0]["route"] == "paid_lease"
    assert accounted[0]["paid_routes_attempted"] == [
        "paid_http",
        "paid_flaresolverr",
    ]
    assert accounted[0]["final_paid_route"] == "paid_flaresolverr"


@pytest.mark.unit
@pytest.mark.parametrize(
    "stats_override",
    [
        {"up_bytes": -1, "total_bytes": 899},
        {"down_bytes": True, "total_bytes": 100},
        {"total_bytes": 9999},
        {"id": "different-lease"},
        {"canonical_url": "https://www.whoscored.com/Matches/999/Live"},
    ],
)
def test_malformed_paid_lease_accounting_fails_closed_without_counting_bytes(
    stats_override,
):
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    proxy = FakeProxyClient(stats_override=stats_override)
    ledger = MemoryLedger()
    transport, _ = _transport(
        direct,
        direct_fs=direct_fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b"paid success")),
    )
    transport.request_ledger = ledger

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.PROXY
    assert exc.value.route is TransportRoute.PAID_LEASE
    assert transport.get_traffic_stats()["paid_proxy_bytes"] == 0
    assert not any(event["status"] == "accounted" for event in ledger.events)


@pytest.mark.unit
def test_paid_accounting_failure_takes_precedence_over_source_failure():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    proxy = FakeProxyClient(stats_override={"canonical_url": "https://evil.test/"})
    transport, _ = _transport(
        direct,
        direct_fs=direct_fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(status_code=500, content=b"bad")),
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.PROXY
    assert exc.value.route is TransportRoute.PAID_LEASE
    assert "prior source error" in str(exc.value)
    assert isinstance(exc.value.__cause__, WhoScoredTransportError)


@pytest.mark.unit
def test_paid_browser_identity_is_never_replayed_into_direct_http_session():
    class CookieJar:
        def __init__(self):
            self.calls = []

        def set(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    direct = FakeHTTPSession()
    direct.cookies = CookieJar()
    direct.headers = {"User-Agent": "direct-agent"}
    paid_fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
            "cookies": [{"name": "cf_clearance", "value": "paid-cookie"}],
            "userAgent": "paid-agent",
        },
        {
            "content": b'{"teamTableStats":[]}',
            "headers": {"content-type": "application/json"},
            "status": 200,
            "responseBytes": 21,
        },
    )
    transport, _ = _transport(direct, paid_fs=paid_fs)
    transport._activate_request(cache_key="identity", scope=None, entity=None)

    transport._browser_fetch(
        TEAM_STATS_URL,
        client=paid_fs,
        route=TransportRoute.PAID_FLARESOLVERR,
        proxy_url="http://lease:secret@proxy_filter:8899",
        bootstrap_url=TEAM_STATS_BOOTSTRAP,
    )

    assert direct.cookies.calls == []
    assert direct.headers == {"User-Agent": "direct-agent"}


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
    assert not is_cloudflare_response(429, {}, CF_HTML)
    assert is_cloudflare_response(429, {"cf-ray": "abc"}, CF_HTML)


@pytest.mark.unit
def test_cloudflare_attention_block_enables_direct_browser_only_with_cf_header():
    assert not is_cloudflare_response(403, {}, CF_ATTENTION_HTML)
    assert is_cloudflare_response(
        403,
        {"server": "cloudflare", "cf-ray": "live-response"},
        CF_ATTENTION_HTML,
    )

    direct = FakeHTTPSession(
        FakeHTTPResponse(
            403,
            CF_ATTENTION_HTML,
            {"server": "cloudflare", "cf-ray": "live-response"},
        )
    )
    fs = FakeFSClient(
        {"html": OK_HTML.decode(), "status": 200, "cookies": [], "userAgent": "x"}
    )
    transport, _ = _transport(direct, direct_fs=fs)

    assert (
        transport.fetch("https://www.whoscored.com/Regions/247/Tournaments/36").route
        is TransportRoute.DIRECT_FLARESOLVERR
    )


@pytest.mark.unit
def test_default_paid_budgets_are_exact_decimal_dagrun_and_url_limits():
    budgets = TransportBudgets()
    assert budgets.max_paid_bytes_per_task == 8_000_000
    assert budgets.max_paid_bytes_per_url == 2_000_000
    assert budgets.max_paid_bytes_per_lease == 2_000_000


@pytest.mark.unit
def test_shared_proxy_budget_rejection_fails_without_permanent_blacklist_state():
    class RejectingProxy(FakeProxyClient):
        def create_lease(self, **kwargs):
            raise ProxyBudgetRejected("DagRun budget exhausted")

    transport, _ = _transport(
        FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"})),
        direct_fs=FakeFSClient(FlareSolverrCFChallengeFailed("cf")),
        proxy=RejectingProxy(),
        paid_http=FakeHTTPSession(),
        attempts=1,
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.BUDGET
    assert exc.value.retryable is False
    assert transport.stats.paid_urls == set()


@pytest.mark.unit
def test_direct_browser_session_is_reused_and_cookies_replayed():
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "one"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "two"}),
    )
    fs = FakeFSClient(
        {
            "html": OK_HTML.decode(),
            "status": 200,
            "cookies": [{"name": "cf_clearance", "value": "token"}],
            "userAgent": "solved-agent",
        },
        {"html": OK_HTML.decode(), "status": 200},
    )
    transport, _ = _transport(direct, direct_fs=fs)

    transport.fetch("https://www.whoscored.com/Matches/1/Live")
    transport.fetch("https://www.whoscored.com/Matches/2/Live")

    assert len(fs.created) == 1
    assert len(fs.get_calls) == 2


@pytest.mark.unit
def test_request_ledger_has_airflow_identity_route_bytes_and_raw_hash():
    ledger = MemoryLedger()
    source_response = FakeHTTPResponse(content=b"source", wire_bytes=777)
    source_response.response_size = 888
    source_response.upload_size = 0
    source_response.request_size = 123
    direct = FakeHTTPSession(source_response)
    context = TransportContext(
        dag_id="dag_ingest_whoscored",
        run_id="scheduled__2026-07-11",
        task_id="matches",
        map_index=3,
        try_number=2,
        scope="ENG-Premier League/2526",
        entity="match",
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=FakeFSClient(),
        paid_fs_client=FakeFSClient(),
        context=context,
        request_ledger=ledger,
    )

    result = transport.fetch(
        "https://www.whoscored.com/Matches/1/Live?cachebust=1",
        cache_key="whoscored:match:1",
    )

    event = ledger.events[-1]
    assert event["dag_id"] == "dag_ingest_whoscored"
    assert event["run_id"] == "scheduled__2026-07-11"
    assert event["task_id"] == "matches"
    assert event["map_index"] == 3 and event["try_number"] == 2
    assert event["scope"] == "ENG-Premier League/2526"
    assert event["entity"] == "match"
    assert event["route"] == "direct_http"
    assert result.wire_bytes == 888
    assert event["request_bytes"] == 123
    assert event["response_bytes"] == 888
    assert event["raw_sha256"] == result.sha256
    assert event["url"] == ("https://www.whoscored.com/Matches/1/Live?cachebust=1")


@pytest.mark.unit
def test_jsonl_request_ledger_fsync_contract_uses_private_append_only_file(tmp_path):
    path = tmp_path / "requests.jsonl"
    ledger = JsonlRequestLedger(str(path))
    ledger.append({"event_id": "one", "response_bytes": 10})
    ledger.append({"event_id": "two", "response_bytes": 20})

    assert [json.loads(line)["event_id"] for line in path.read_text().splitlines()] == [
        "one",
        "two",
    ]
    assert path.stat().st_mode & 0o777 == 0o600


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
    client = ProxyFilterClient("http://proxy_filter:8899", session=session, timeout=3)

    context = TransportContext(
        dag_id="dag", run_id="run", task_id="task", map_index=4, try_number=2
    )
    lease = client.create_lease(
        max_bytes=1234,
        ttl_seconds=60,
        context=context,
        canonical_url="https://www.whoscored.com/Matches/1/Live",
    )

    assert session.trust_env is False
    assert session.posts[0][0] == "http://proxy_filter:8899/v1/leases"
    assert session.posts[0][1]["json"] == {
        "max_bytes": 1234,
        "ttl_seconds": 60,
        "canonical_url": "https://www.whoscored.com/Matches/1/Live",
        "dag_id": "dag",
        "run_id": "run",
        "task_id": "task",
        "map_index": 4,
        "try_number": 2,
        "scope": "",
        "entity": "",
    }
    assert lease.proxy_url == "http://lease:s%2Fecret@proxy_filter:8900"
    client.close_session()
    assert session.closed is True


class SimpleControlSession:
    def __init__(self, response):
        self.response = response
        self.posts = []
        self.trust_env = True
        self.closed = False

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.response

    def close(self):
        self.closed = True
