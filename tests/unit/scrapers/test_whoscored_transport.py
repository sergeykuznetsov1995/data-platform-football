from __future__ import annotations

import hashlib
import json
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest
import requests

import scrapers.whoscored.transport as transport_module
from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrError,
    FlareSolverrResponseTooLarge,
    FlareSolverrRuntimeIdentityError,
    FlareSolverrTimeout,
)
from scrapers.whoscored.source_circuit import (
    CircuitPermit,
    SharedSourceCircuit,
    SourceCircuitOpen,
)
from scrapers.whoscored.transport import (
    CachedPayload,
    CloudflareChallenge,
    FailureKind,
    FetchRequest,
    JsonlRequestLedger,
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
        return {"url": url, **result}

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

    def destroy_session_strict(self, session_id):
        self.destroyed.append(session_id)

    def get_traffic_stats(self):
        return {"sessions_created": len(self.created)}

    def close(self):
        self.closed = True


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


class FakeSourceCircuit:
    def __init__(self, *, probe=False, opened=False, recover_on_wait=False):
        self.probe = probe
        self.opened = opened
        self.recover_on_wait = recover_on_wait
        self.calls = []
        self.generation = 1

    def admit(self, *, wait=False):
        self.calls.append(("admit", wait))
        if self.opened:
            if not (wait and self.recover_on_wait):
                raise SourceCircuitOpen(state="open", retry_at=9_999.0)
            self.opened = False
            self.probe = True
        return CircuitPermit(
            generation=self.generation,
            probe_nonce=("a" * 32 if self.probe else None),
        )

    def succeed(self, permit):
        self.calls.append(("succeed", permit.is_probe))
        self.probe = False

    def trip(self, permit):
        self.calls.append(("trip", permit.is_probe))
        self.opened = True

    def inconclusive(self, permit):
        self.calls.append(("inconclusive", permit.is_probe))

    def abandon(self, permit):
        self.calls.append(("abandon", permit.is_probe))


def _transport(
    direct_http,
    *,
    direct_fs=None,
    raw_cache=None,
    budgets=None,
    attempts=2,
    http_attempts=3,
    http_retry_backoff=0.0,
    retry_backoff=0.0,
    retry_jitter=0.0,
    browser_session_owner=None,
    source_circuit=None,
    source_circuit_wait=False,
):
    factory_calls = []

    def factory(proxy_url):
        factory_calls.append(proxy_url)
        raise AssertionError("direct-only transport must not create a new session")

    transport = WhoScoredTransport(
        direct_http_session=direct_http,
        direct_fs_client=direct_fs or FakeFSClient(),
        http_session_factory=factory,
        raw_cache=raw_cache,
        budgets=budgets,
        direct_http_attempts=http_attempts,
        direct_http_retry_backoff_seconds=http_retry_backoff,
        direct_browser_attempts=attempts,
        browser_retry_backoff_seconds=retry_backoff,
        browser_retry_jitter_seconds=retry_jitter,
        browser_session_owner=browser_session_owner,
        source_circuit=source_circuit,
        source_circuit_wait=source_circuit_wait,
        context=TransportContext(),
    )
    return transport, factory_calls


@pytest.mark.unit
def test_direct_http_success_never_starts_browser_or_proxy():
    direct = FakeHTTPSession(FakeHTTPResponse(wire_bytes=777))
    fs = FakeFSClient()
    transport, factory_calls = _transport(direct, direct_fs=fs)

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.route is TransportRoute.DIRECT_HTTP
    assert result.wire_bytes == 777
    assert fs.created == []
    assert factory_calls == []


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
def test_direct_route_uses_residential_pool_when_proxy_file_set(monkeypatch, tmp_path):
    pool = tmp_path / "pool.txt"
    pool.write_text(
        "pool.example.io:10000:poolu:poolpw\n"
        "pool.example.io:10001:poolu:poolpw\n"
    )
    monkeypatch.setenv("WHOSCORED_PROXY_FILE", str(pool))
    monkeypatch.setenv("WHOSCORED_TRANSPORT_POLICY", "direct_only")

    seen = []

    def factory(proxy_url):
        seen.append(proxy_url)
        return FakeHTTPSession()

    transport = WhoScoredTransport(
        direct_fs_client=FakeFSClient(),
        http_session_factory=factory,
        context=TransportContext(transport_policy="direct_only"),
    )

    assert transport._pool_proxy_url is not None
    assert transport._pool_proxy_url.startswith("http://poolu:poolpw@pool.example.io:")
    # The direct curl session was built to egress through that pool member.
    assert seen == [transport._pool_proxy_url]


@pytest.mark.unit
@pytest.mark.parametrize("pool_file", ["", "missing", "empty"])
def test_real_direct_transport_refuses_host_ip_without_pool(
    monkeypatch, tmp_path, pool_file
):
    # #1471: a real direct_only transport (no injected session) must fail
    # before any request instead of silently egressing from the host IP.
    if pool_file == "":
        monkeypatch.setenv("WHOSCORED_PROXY_FILE", "")
    elif pool_file == "missing":
        monkeypatch.setenv("WHOSCORED_PROXY_FILE", str(tmp_path / "absent.txt"))
    else:
        empty = tmp_path / "empty.txt"
        empty.write_text("")
        monkeypatch.setenv("WHOSCORED_PROXY_FILE", str(empty))
    monkeypatch.setenv("WHOSCORED_TRANSPORT_POLICY", "direct_only")
    fs = FakeFSClient()

    with pytest.raises(ValueError, match="WHOSCORED_PROXY_FILE"):
        WhoScoredTransport(
            direct_fs_client=fs,
            context=TransportContext(transport_policy="direct_only"),
        )
    assert fs.created == [] and fs.get_calls == []


@pytest.mark.unit
def test_direct_route_stays_host_ip_without_proxy_file(monkeypatch):
    monkeypatch.delenv("WHOSCORED_PROXY_FILE", raising=False)
    monkeypatch.setenv("WHOSCORED_TRANSPORT_POLICY", "direct_only")

    seen = []

    def factory(proxy_url):
        seen.append(proxy_url)
        return FakeHTTPSession()

    transport = WhoScoredTransport(
        direct_fs_client=FakeFSClient(),
        http_session_factory=factory,
        context=TransportContext(transport_policy="direct_only"),
    )

    assert transport._pool_proxy_url is None
    assert seen == [None]


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
def test_valid_structured_raw_cache_precedes_unresolved_source_probe():
    cache = MemoryRawCache(CachedPayload(content=b'{"cached":true}'))
    circuit = FakeSourceCircuit(probe=True)
    direct = FakeHTTPSession()
    transport, _ = _transport(
        direct,
        raw_cache=cache,
        source_circuit=circuit,
    )
    transport._source_circuit_permit = CircuitPermit(
        generation=1,
        probe_nonce="a" * 32,
    )

    result = transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert result.route is TransportRoute.RAW_CACHE
    assert circuit.calls == []
    assert direct.calls == []


@pytest.mark.unit
def test_success_is_stored_in_raw_cache_before_return():
    cache = MemoryRawCache()
    direct = FakeHTTPSession(FakeHTTPResponse(content=b"source"))
    transport, _ = _transport(direct, raw_cache=cache)

    result = transport.fetch("https://www.whoscored.com/Players/1/Show", cache_key="p1")

    assert cache.stored[0][0] == "p1"
    assert cache.stored[0][1].content == b"source"
    assert cache.stored[0][2] == result.sha256
    assert cache.stored[0][1].observed_at == result.observed_at
    assert datetime.fromisoformat(result.observed_at).tzinfo is not None


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
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/x")

    assert exc.value.kind is FailureKind.HTTP_STATUS
    assert len(direct.calls) == (3 if status >= 500 else 1)
    assert fs.created == []


@pytest.mark.unit
def test_transient_direct_502_retries_with_a_token_and_never_uses_proxy():
    direct = FakeHTTPSession(
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(content=OK_HTML),
    )
    fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=fs)
    tokens = []

    result = transport.fetch(
        "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/10498",
        before_network=lambda: tokens.append("token"),
    )

    assert result.route is TransportRoute.DIRECT_HTTP
    assert len(direct.calls) == 3
    assert tokens == ["token", "token", "token"]
    assert fs.created == []


@pytest.mark.unit
def test_production_direct_http_defaults_space_two_502s_before_success(monkeypatch):
    direct = FakeHTTPSession(
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(content=OK_HTML),
    )
    fs = FakeFSClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    result = transport.fetch(
        "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/10498",
        before_network=lambda: tokens.append("token"),
    )

    assert result.route is TransportRoute.DIRECT_HTTP
    assert len(direct.calls) == 3
    assert sleeps == [2.0, 4.0]
    assert tokens == ["token"] * 3
    assert fs.created == []


@pytest.mark.unit
def test_production_direct_http_defaults_fail_closed_after_three_502s(monkeypatch):
    direct = FakeHTTPSession(
        *[
            FakeHTTPResponse(status_code=502, content=b"temporary upstream error")
            for _ in range(3)
        ]
    )
    fs = FakeFSClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/10498",
            before_network=lambda: tokens.append("token"),
        )

    assert raised.value.kind is FailureKind.HTTP_STATUS
    assert raised.value.status_code == 502
    assert len(direct.calls) == 3
    assert sleeps == [2.0, 4.0]
    assert tokens == ["token"] * 3
    assert fs.created == []


@pytest.mark.unit
def test_production_direct_http_timeout_waits_and_reacquires_token(monkeypatch):
    direct = FakeHTTPSession(
        requests.exceptions.Timeout("temporary timeout"),
        FakeHTTPResponse(content=OK_HTML),
    )
    fs = FakeFSClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    result = transport.fetch(
        "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/10498",
        before_network=lambda: tokens.append("token"),
    )

    assert result.route is TransportRoute.DIRECT_HTTP
    assert len(direct.calls) == 2
    assert sleeps == [2.0]
    assert tokens == ["token"] * 2
    assert fs.created == []


@pytest.mark.unit
@pytest.mark.parametrize("status", [404, 429])
def test_production_direct_http_nonretryable_status_does_not_sleep(monkeypatch, status):
    direct = FakeHTTPSession(
        FakeHTTPResponse(status_code=status, content=b"ordinary origin error")
    )
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=FakeFSClient(),
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            "https://www.whoscored.com/Regions/247/Tournaments/36/Seasons/10498",
            before_network=lambda: tokens.append("token"),
        )

    assert raised.value.kind is FailureKind.HTTP_STATUS
    assert len(direct.calls) == 1
    assert sleeps == []
    assert tokens == ["token"]


@pytest.mark.unit
@pytest.mark.parametrize("value", [True, -1, float("nan"), float("inf"), "2"])
def test_invalid_direct_http_retry_backoff_fails_before_network(value):
    direct = FakeHTTPSession()
    fs = FakeFSClient()

    with pytest.raises(ValueError, match="direct_http_retry_backoff_seconds"):
        WhoScoredTransport(
            direct_http_session=direct,
            direct_fs_client=fs,
            direct_http_retry_backoff_seconds=value,
        )

    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
@pytest.mark.parametrize("value", ["", "true", "2", "yes"])
def test_invalid_source_circuit_wait_environment_fails_before_network(
    monkeypatch, value
):
    direct = FakeHTTPSession()
    fs = FakeFSClient()
    monkeypatch.setenv("WHOSCORED_SOURCE_CIRCUIT_WAIT", value)

    with pytest.raises(ValueError, match="WHOSCORED_SOURCE_CIRCUIT_WAIT"):
        WhoScoredTransport(
            direct_http_session=direct,
            direct_fs_client=fs,
        )

    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
def test_source_circuit_wait_requires_shared_path_before_network(monkeypatch):
    direct = FakeHTTPSession()
    fs = FakeFSClient()
    monkeypatch.setenv("WHOSCORED_SOURCE_CIRCUIT_WAIT", "1")
    monkeypatch.delenv("WHOSCORED_SOURCE_CIRCUIT_PATH", raising=False)

    with pytest.raises(ValueError, match="requires WHOSCORED_SOURCE_CIRCUIT_PATH"):
        WhoScoredTransport(
            direct_http_session=direct,
            direct_fs_client=fs,
        )

    assert direct.calls == []
    assert fs.created == []


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
    transport, _ = _transport(direct, direct_fs=fs)

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


@pytest.mark.unit
def test_structured_redirect_is_not_followed_or_promoted_to_browser_or_paid():
    direct = FakeHTTPSession(
        FakeHTTPResponse(302, b"redirect", {"location": "https://evil.test/"})
    )
    fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )

    assert exc.value.kind is FailureKind.HTTP_STATUS
    assert exc.value.status_code == 302
    assert direct.calls[0][2]["allow_redirects"] is False
    assert fs.created == []


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
    transport, _ = _transport(direct, direct_fs=fs)

    result = transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert direct.calls[0][2]["allow_redirects"] is False
    assert len(fs.created) == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "bootstrap_url",
    [
        TEAM_STATS_BOOTSTRAP + "?next=https://evil.test",
        TEAM_STATS_BOOTSTRAP + "#fragment",
        TEAM_STATS_BOOTSTRAP.replace("/Regions/247/", "/Regions/x/"),
        TEAM_STATS_BOOTSTRAP.replace("/TeamStatistics", "/PlayerStatistics"),
        TEAM_STATS_BOOTSTRAP + "/extra/path",
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
def test_source_circuit_does_not_trip_on_expected_direct_access_gate():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        {
            "content": b'{"teamTableStats":[]}',
            "headers": {"content-type": "application/json"},
            "status": 200,
            "responseBytes": 21,
            "finalUrl": TEAM_STATS_URL,
        },
    )
    circuit = FakeSourceCircuit()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        source_circuit=circuit,
    )

    result = transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert circuit.calls == [("admit", False), ("succeed", False)]


@pytest.mark.unit
def test_browser_access_gate_never_counts_as_cf_or_authorizes_paid():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        *[
            {
                "content": MASKED_STATS_HTML,
                "headers": {},
                "status": 200,
                "responseBytes": len(MASKED_STATS_HTML),
                "finalUrl": TEAM_STATS_URL,
            }
            for _ in range(4)
        ],
    )
    circuit = FakeSourceCircuit()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        attempts=4,
        source_circuit=circuit,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            validator=lambda response: json.loads(response.content) is not None,
        )

    assert not isinstance(raised.value, CloudflareChallenge)
    assert raised.value.kind is FailureKind.CONTENT
    assert len(fs.xhr_calls) == 1
    assert circuit.calls == [("admit", False)]


@pytest.mark.unit
def test_authoritative_browser_cf_trips_shared_circuit_once_and_stops_retries():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        {
            "content": CF_HTML,
            "headers": {"server": "cloudflare", "cf-ray": "ray"},
            "status": 403,
            "responseBytes": len(CF_HTML),
            "finalUrl": TEAM_STATS_URL,
        },
    )
    circuit = FakeSourceCircuit()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        attempts=4,
        source_circuit=circuit,
    )

    with pytest.raises(CloudflareChallenge) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            validator=lambda response: json.loads(response.content) is not None,
        )

    assert raised.value.source_wide is True
    assert len(fs.created) == 1
    assert len(fs.xhr_calls) == 1
    assert circuit.calls == [("admit", False), ("trip", False)]


@pytest.mark.unit
@pytest.mark.parametrize("batched", [False, True])
def test_structured_bootstrap_typed_cf_is_authoritative_source_evidence(batched):
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(FlareSolverrCFChallengeFailed("cloudflare blocked"))
    circuit = FakeSourceCircuit()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        attempts=4,
        source_circuit=circuit,
    )

    with pytest.raises(CloudflareChallenge) as raised:
        if batched:
            transport.fetch_many(
                [
                    FetchRequest(
                        url=TEAM_STATS_URL,
                        cache_key="bootstrap-cf",
                        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                    )
                ]
            )
        else:
            transport.fetch(
                TEAM_STATS_URL,
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )

    assert raised.value.source_wide is True
    assert len(fs.get_calls) == 1
    assert fs.xhr_calls == []
    assert fs.xhr_many_calls == []
    assert circuit.calls == [("admit", False), ("trip", False)]


@pytest.mark.unit
def test_open_source_circuit_rejects_before_rate_token_or_network():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient()
    circuit = FakeSourceCircuit(opened=True)
    tokens = []
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        source_circuit=circuit,
    )

    with pytest.raises(CloudflareChallenge) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            before_network=lambda: tokens.append("token"),
        )

    assert raised.value.source_wide is True
    assert circuit.calls == [("admit", False)]
    assert tokens == []
    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
def test_corrupt_source_circuit_fails_closed_before_token_or_network(tmp_path):
    state_path = tmp_path / "circuit" / "state.json"
    state_path.parent.mkdir()
    state_path.write_text("{broken\n", encoding="utf-8")
    state_path.chmod(0o600)
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient()
    tokens = []
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        source_circuit=SharedSourceCircuit(state_path),
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            before_network=lambda: tokens.append("token"),
        )

    assert raised.value.kind is FailureKind.CONFIG
    assert tokens == []
    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
def test_complete_raw_cache_replay_ignores_unresolved_source_probe():
    second_url = TEAM_STATS_URL + "&category=offensive"
    cache = KeyedMemoryRawCache()
    cache.payloads = {
        "warm-0": CachedPayload(content=b'{"cached":0}'),
        "warm-1": CachedPayload(content=b'{"cached":1}'),
    }
    direct = FakeHTTPSession()
    fs = FakeFSClient()
    circuit = FakeSourceCircuit(probe=True)
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        raw_cache=cache,
        source_circuit=circuit,
    )
    transport._source_circuit_permit = CircuitPermit(
        generation=1,
        probe_nonce="a" * 32,
    )

    results = transport.fetch_many(
        [
            FetchRequest(
                url=url,
                cache_key=f"warm-{index}",
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                validator=lambda response: json.loads(response.content) is not None,
            )
            for index, url in enumerate((TEAM_STATS_URL, second_url))
        ]
    )

    assert [result.route for result in results] == [
        TransportRoute.RAW_CACHE,
        TransportRoute.RAW_CACHE,
    ]
    assert circuit.calls == []
    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
def test_half_open_batch_probes_one_feed_before_returning_to_bounded_batches():
    second_url = TEAM_STATS_URL + "&category=offensive"
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        [_batch_solution(TEAM_STATS_URL)],
        [_batch_solution(second_url)],
    )
    circuit = FakeSourceCircuit(probe=True)
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        source_circuit=circuit,
    )

    results = transport.fetch_many(
        [
            FetchRequest(
                url=url,
                cache_key=f"probe-{index}",
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
            for index, url in enumerate((TEAM_STATS_URL, second_url))
        ]
    )

    assert len(results) == 2
    assert [call[0] for call in fs.xhr_many_calls] == [
        [TEAM_STATS_URL],
        [second_url],
    ]
    assert circuit.calls == [
        ("admit", False),
        ("succeed", True),
        ("admit", False),
        ("succeed", False),
    ]


@pytest.mark.unit
def test_half_open_direct_502_is_one_inconclusive_physical_attempt():
    direct = FakeHTTPSession(
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error")
    )
    fs = FakeFSClient()
    circuit = FakeSourceCircuit(probe=True)
    tokens = []
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        http_attempts=3,
        source_circuit=circuit,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            before_network=lambda: tokens.append("token"),
        )

    assert raised.value.status_code == 502
    assert len(direct.calls) == 1
    assert fs.created == []
    assert tokens == ["token"]
    assert circuit.calls == [("admit", False), ("inconclusive", True)]


@pytest.mark.unit
def test_half_open_serial_browser_502_is_one_inconclusive_xhr():
    soft_502 = b"<html><title>whoscored.com | 502: Bad gateway</title></html>"
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        {
            "content": soft_502,
            "headers": {},
            "status": 200,
            "responseBytes": len(soft_502),
            "finalUrl": TEAM_STATS_URL,
        },
    )
    circuit = FakeSourceCircuit(probe=True)
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        attempts=4,
        source_circuit=circuit,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )

    assert raised.value.status_code == 502
    assert len(fs.xhr_calls) == 1
    assert len(fs.destroyed) == 1
    assert circuit.calls == [("admit", False), ("inconclusive", True)]


@pytest.mark.unit
def test_half_open_batch_browser_502_is_one_inconclusive_xhr():
    transient_502 = _batch_solution(TEAM_STATS_URL)
    transient_502["status"] = 502
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        [transient_502],
    )
    circuit = FakeSourceCircuit(probe=True)
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        attempts=4,
        source_circuit=circuit,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch_many(
            [
                FetchRequest(
                    url=TEAM_STATS_URL,
                    cache_key="half-open-502",
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                )
            ]
        )

    assert raised.value.status_code == 502
    assert [call[0] for call in fs.xhr_many_calls] == [[TEAM_STATS_URL]]
    assert len(fs.destroyed) == 1
    assert circuit.calls == [("admit", False), ("inconclusive", True)]


@pytest.mark.unit
def test_batch_authoritative_browser_cf_trips_once_and_never_pays():
    second_url = TEAM_STATS_URL + "&category=offensive"
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))

    def cf_solution(url):
        return {
            "ok": True,
            "content": CF_HTML,
            "headers": {"server": "cloudflare", "cf-ray": "ray"},
            "status": 403,
            "responseBytes": len(CF_HTML),
            "finalUrl": url,
        }

    fs = FakeFSClient(
        {
            "html": "<html><body>Team Statistics</body></html>",
            "status": 200,
        },
        [cf_solution(TEAM_STATS_URL), cf_solution(second_url)],
    )
    circuit = FakeSourceCircuit()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        attempts=4,
        source_circuit=circuit,
    )

    with pytest.raises(CloudflareChallenge) as raised:
        transport.fetch_many(
            [
                FetchRequest(
                    url=url,
                    cache_key=f"blocked-{index}",
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                )
                for index, url in enumerate((TEAM_STATS_URL, second_url))
            ]
        )

    assert raised.value.source_wide is True
    assert len(fs.xhr_many_calls) == 1
    assert circuit.calls == [("admit", False), ("trip", False)]


@pytest.mark.unit
def test_batch_source_cf_trips_before_terminal_sibling_error():
    second_url = TEAM_STATS_URL + "&category=offensive"
    cf_solution = {
        "ok": True,
        "content": CF_HTML,
        "headers": {"server": "cloudflare", "cf-ray": "ray"},
        "status": 403,
        "responseBytes": len(CF_HTML),
        "finalUrl": second_url,
    }
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        [_batch_solution(TEAM_STATS_URL, b'{"bad":true}'), cf_solution],
    )
    circuit = FakeSourceCircuit()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        attempts=4,
        source_circuit=circuit,
    )

    with pytest.raises(CloudflareChallenge) as raised:
        transport.fetch_many(
            [
                FetchRequest(
                    url=TEAM_STATS_URL,
                    cache_key="mixed-terminal",
                    validator=lambda _response: False,
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                ),
                FetchRequest(
                    url=second_url,
                    cache_key="mixed-cf",
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                ),
            ]
        )

    assert raised.value.source_wide is True
    assert circuit.calls == [("admit", False), ("trip", False)]
    assert len(fs.xhr_many_calls) == 1
    assert len(fs.destroyed) == 1


@pytest.mark.unit
def test_capacity_wait_mode_resumes_with_one_half_open_probe_after_cooldown():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        {
            "content": CF_HTML,
            "headers": {"server": "cloudflare", "cf-ray": "ray"},
            "status": 403,
            "responseBytes": len(CF_HTML),
            "finalUrl": TEAM_STATS_URL,
        },
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        {
            "content": b'{"teamTableStats":[]}',
            "headers": {"content-type": "application/json"},
            "status": 200,
            "responseBytes": 21,
            "finalUrl": TEAM_STATS_URL,
        },
    )
    circuit = FakeSourceCircuit(recover_on_wait=True)
    tokens = []
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        attempts=4,
        source_circuit=circuit,
        source_circuit_wait=True,
    )

    result = transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
        before_network=lambda: tokens.append("token"),
    )

    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.created) == 2
    assert tokens == ["token", "token"]
    assert circuit.calls == [
        ("admit", True),
        ("trip", False),
        ("admit", True),
        ("succeed", True),
    ]


@pytest.mark.unit
def test_capacity_wait_mode_rechunks_blocked_batch_to_one_half_open_probe():
    second_url = TEAM_STATS_URL + "&category=offensive"

    def cf_solution(url):
        return {
            "ok": True,
            "content": CF_HTML,
            "headers": {"server": "cloudflare", "cf-ray": "ray"},
            "status": 403,
            "responseBytes": len(CF_HTML),
            "finalUrl": url,
        }

    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        [cf_solution(TEAM_STATS_URL), cf_solution(second_url)],
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        [_batch_solution(TEAM_STATS_URL)],
        [_batch_solution(second_url)],
    )
    circuit = FakeSourceCircuit(recover_on_wait=True)
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        attempts=4,
        source_circuit=circuit,
        source_circuit_wait=True,
    )

    results = transport.fetch_many(
        [
            FetchRequest(
                url=url,
                cache_key=f"cooldown-{index}",
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
            for index, url in enumerate((TEAM_STATS_URL, second_url))
        ]
    )

    assert len(results) == 2
    assert [len(call[0]) for call in fs.xhr_many_calls] == [2, 1, 1]
    assert len(fs.created) == 2
    assert len(fs.destroyed) == 1
    assert circuit.calls == [
        ("admit", True),
        ("trip", False),
        ("admit", True),
        ("succeed", True),
        ("admit", True),
        ("succeed", False),
    ]


@pytest.mark.unit
def test_structured_batch_direct_probe_spaces_two_502s_before_success(monkeypatch):
    direct = FakeHTTPSession(
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(content=b'{"teamTableStats":[]}'),
    )
    fs = FakeFSClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    results = transport.fetch_many(
        [
            FetchRequest(
                url=TEAM_STATS_URL,
                cache_key="direct-probe-recovers",
                validator=lambda response: json.loads(response.content) is not None,
                before_network=lambda: tokens.append("token"),
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
        ]
    )

    assert results[0].route is TransportRoute.DIRECT_HTTP
    assert len(direct.calls) == 3
    assert sleeps == [2.0, 4.0]
    assert tokens == ["token"] * 3
    assert fs.created == []


@pytest.mark.unit
def test_structured_batch_direct_probe_retries_timeout_with_new_token(monkeypatch):
    direct = FakeHTTPSession(
        requests.exceptions.Timeout("temporary timeout"),
        FakeHTTPResponse(content=b'{"teamTableStats":[]}'),
    )
    fs = FakeFSClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    results = transport.fetch_many(
        [
            FetchRequest(
                url=TEAM_STATS_URL,
                cache_key="direct-probe-timeout",
                validator=lambda response: json.loads(response.content) is not None,
                before_network=lambda: tokens.append("token"),
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
        ]
    )

    assert results[0].route is TransportRoute.DIRECT_HTTP
    assert len(direct.calls) == 2
    assert sleeps == [2.0]
    assert tokens == ["token"] * 2
    assert fs.created == []


@pytest.mark.unit
def test_structured_batch_direct_probe_three_502s_never_use_browser_or_paid(
    monkeypatch,
):
    direct = FakeHTTPSession(
        *[
            FakeHTTPResponse(status_code=502, content=b"temporary upstream error")
            for _ in range(3)
        ]
    )
    fs = FakeFSClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch_many(
            [
                FetchRequest(
                    url=TEAM_STATS_URL,
                    cache_key="direct-probe-exhausted",
                    validator=lambda response: json.loads(response.content) is not None,
                    before_network=lambda: tokens.append("token"),
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                )
            ]
        )

    assert raised.value.kind is FailureKind.HTTP_STATUS
    assert raised.value.status_code == 502
    assert len(direct.calls) == 3
    assert sleeps == [2.0, 4.0]
    assert tokens == ["token"] * 3
    assert fs.created == []


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
def test_browser_batch_retries_only_transient_502_target_without_paid():
    second_url = TEAM_STATS_URL + "&category=offensive"
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    bootstrap = {
        "html": "<html><body>Team Statistics</body></html>",
        "status": 200,
    }
    transient_502 = _batch_solution(second_url)
    transient_502["status"] = 502
    fs = FakeFSClient(
        bootstrap,
        [_batch_solution(TEAM_STATS_URL), transient_502],
        bootstrap,
        [_batch_solution(second_url)],
    )
    cache = KeyedMemoryRawCache()
    tokens = []
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        raw_cache=cache,
        attempts=2,
    )
    requests = [
        FetchRequest(
            url=url,
            cache_key=f"transient-502-{index}",
            validator=lambda response: json.loads(response.content) is not None,
            before_network=lambda: tokens.append("token"),
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )
        for index, url in enumerate((TEAM_STATS_URL, second_url))
    ]

    results = transport.fetch_many(requests)

    assert [result.status_code for result in results] == [200, 200]
    assert [call[0] for call in fs.xhr_many_calls] == [
        [TEAM_STATS_URL, second_url],
        [second_url],
    ]
    assert len(fs.created) == 2
    assert len(fs.destroyed) == 1
    assert tokens == ["token"] * 3
    assert set(cache.payloads) == {"transient-502-0", "transient-502-1"}
    assert factory_calls == []


@pytest.mark.unit
def test_browser_batch_repeated_502_fails_closed_without_paid():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    bootstrap = {
        "html": "<html><body>Team Statistics</body></html>",
        "status": 200,
    }
    first_502 = _batch_solution(TEAM_STATS_URL)
    first_502["status"] = 502
    second_502 = _batch_solution(TEAM_STATS_URL)
    second_502["status"] = 502
    fs = FakeFSClient(
        bootstrap,
        [first_502],
        bootstrap,
        [second_502],
    )
    tokens = []
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        attempts=2,
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch_many(
            [
                FetchRequest(
                    url=TEAM_STATS_URL,
                    cache_key="repeated-502",
                    validator=lambda response: json.loads(response.content) is not None,
                    before_network=lambda: tokens.append("token"),
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                )
            ]
        )

    assert exc.value.kind is FailureKind.HTTP_STATUS
    assert exc.value.status_code == 502
    assert exc.value.retryable is True
    assert [call[0] for call in fs.xhr_many_calls] == [
        [TEAM_STATS_URL],
        [TEAM_STATS_URL],
    ]
    assert len(fs.created) == 2
    assert len(fs.destroyed) == 2
    assert tokens == ["token"] * 2
    assert factory_calls == []


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
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
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
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
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
    transport, _ = _transport(
        direct,
        direct_fs=fs,
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
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
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
def test_parser_and_browser_errors_do_not_leave_direct_gate_circuit_open():
    parser_direct = FakeHTTPSession(
        FakeHTTPResponse(content=b'{"unexpected":true}'),
        FakeHTTPResponse(content=b'{"recovered":true}'),
    )
    parser_fs = FakeFSClient()
    parser_transport, _ = _transport(
        parser_direct,
        direct_fs=parser_fs,
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

    browser_direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=b'{"recovered":true}'),
    )
    browser_fs = FakeFSClient(FlareSolverrTimeout("browser unavailable"))
    browser_transport, _ = _transport(
        browser_direct,
        direct_fs=browser_fs,
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
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch(
            "https://www.whoscored.com/Matches/1/Live",
            validator=lambda _: False,
        )

    assert exc.value.kind is FailureKind.CONTENT
    assert fs.created == []


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
def test_default_browser_session_name_is_unchanged(monkeypatch):
    monkeypatch.setattr(
        "scrapers.whoscored.transport.uuid.uuid4",
        lambda: types.SimpleNamespace(hex="0123456789abcdef"),
    )
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient({"html": OK_HTML.decode(), "status": 200})
    transport, _ = _transport(direct, direct_fs=fs)

    transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert fs.created == [("ws-direct_flaresolverr-0123456789", None)]


@pytest.mark.unit
def test_capacity_owner_isolated_browser_session_name(monkeypatch):
    monkeypatch.setattr(
        "scrapers.whoscored.transport.uuid.uuid4",
        lambda: types.SimpleNamespace(hex="0123456789abcdef"),
    )
    owner = "a1b2c3d4e5f60718"
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient({"html": OK_HTML.decode(), "status": 200})
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        browser_session_owner=owner,
    )

    transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert fs.created == [(f"ws-cap-{owner}-direct_flaresolverr-0123456789", None)]


class _OwnerStringSubclass(str):
    pass


@pytest.mark.unit
@pytest.mark.parametrize(
    "owner",
    [
        "",
        "a" * 15,
        "a" * 33,
        "A" * 16,
        "a" * 15 + "-",
        "a" * 15 + "/",
        "a" * 15 + "é",
        1234567890123456,
        _OwnerStringSubclass("a" * 16),
    ],
)
def test_invalid_capacity_owner_fails_before_session_side_effect(owner):
    direct = FakeHTTPSession()
    fs = FakeFSClient()

    with pytest.raises(ValueError, match="browser_session_owner"):
        WhoScoredTransport(
            direct_http_session=direct,
            direct_fs_client=fs,
            browser_session_owner=owner,
        )

    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
def test_direct_browser_retries_cloudflare_origin_502_rendered_as_http_200():
    soft_502 = b"<html><title>whoscored.com | 502: Bad gateway</title></html>"
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(
        {"html": soft_502.decode(), "status": 200},
        {"html": OK_HTML.decode(), "status": 200},
    )
    tokens = []
    transport, _ = _transport(direct, direct_fs=fs, attempts=2)

    result = transport.fetch(
        "https://www.whoscored.com/Matches/1/Live",
        before_network=lambda: tokens.append(1),
    )

    assert result.content == OK_HTML
    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.get_calls) == 2
    # The initial browser attempt shares the logical request's acquired token;
    # the additional physical retry acquires one more.
    assert len(tokens) == 2


@pytest.mark.unit
def test_production_browser_defaults_recover_two_masked_502s_with_backoff(
    monkeypatch,
):
    soft_502 = b"<html><title>whoscored.com | 502: Bad gateway</title></html>"
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(
        {"html": soft_502.decode(), "status": 200},
        {"html": soft_502.decode(), "status": 200},
        {"html": OK_HTML.decode(), "status": 200},
    )
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    monkeypatch.setattr(
        "scrapers.whoscored.transport.random.uniform", lambda _low, _high: 0.0
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    result = transport.fetch(
        "https://www.whoscored.com/Matches/1/Live",
        before_network=lambda: tokens.append("token"),
    )

    assert result.content == OK_HTML
    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.get_calls) == 3
    assert len(fs.created) == 3
    assert len(fs.destroyed) == 2
    assert sleeps == [2.0, 4.0]
    assert tokens == ["token"] * 3


@pytest.mark.unit
def test_production_batch_defaults_recover_two_masked_502s_with_backoff(
    monkeypatch,
):
    bootstrap = {"html": "<html>Team Statistics</html>", "status": 200}
    first_502 = _batch_solution(TEAM_STATS_URL)
    first_502["status"] = 502
    second_502 = _batch_solution(TEAM_STATS_URL)
    second_502["status"] = 502
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        bootstrap,
        [first_502],
        bootstrap,
        [second_502],
        bootstrap,
        [_batch_solution(TEAM_STATS_URL)],
    )
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    monkeypatch.setattr(
        "scrapers.whoscored.transport.random.uniform", lambda _low, _high: 0.0
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    result = transport.fetch_many(
        [
            FetchRequest(
                url=TEAM_STATS_URL,
                cache_key="production-default-502",
                validator=lambda response: json.loads(response.content) is not None,
                before_network=lambda: tokens.append("token"),
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
        ]
    )

    assert result[0].route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.xhr_many_calls) == 3
    assert len(fs.created) == 3
    assert len(fs.destroyed) == 2
    assert sleeps == [2.0, 4.0]
    assert tokens == ["token"] * 3


@pytest.mark.unit
def test_production_browser_defaults_recover_three_cf_challenges_with_jitter(
    monkeypatch,
):
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cloudflare challenge"),
        FlareSolverrCFChallengeFailed("cloudflare challenge"),
        FlareSolverrCFChallengeFailed("cloudflare challenge"),
        {"html": OK_HTML.decode(), "status": 200},
    )
    cache = MemoryRawCache()
    sleeps = []
    tokens = []
    jitter_values = iter((0.5, 1.0, 2.0))
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    monkeypatch.setattr(
        "scrapers.whoscored.transport.random.uniform",
        lambda low, high: next(jitter_values),
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
        raw_cache=cache,
    )

    result = transport.fetch(
        "https://www.whoscored.com/Matches/1/Live",
        before_network=lambda: tokens.append("token"),
    )

    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.get_calls) == 4
    assert len(fs.created) == 4
    assert len(fs.destroyed) == 3
    assert sleeps == [2.5, 5.0, 10.0]
    assert tokens == ["token"] * 4
    assert len(cache.stored) == 1


@pytest.mark.unit
def test_production_batch_defaults_recover_three_cf_challenges_with_jitter(
    monkeypatch,
):
    bootstrap = {"html": "<html>Team Statistics</html>", "status": 200}
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cloudflare challenge"),
        FlareSolverrCFChallengeFailed("cloudflare challenge"),
        FlareSolverrCFChallengeFailed("cloudflare challenge"),
        bootstrap,
        [_batch_solution(TEAM_STATS_URL)],
    )
    sleeps = []
    tokens = []
    jitter_values = iter((0.5, 1.0, 2.0))
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    monkeypatch.setattr(
        "scrapers.whoscored.transport.random.uniform",
        lambda low, high: next(jitter_values),
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    result = transport.fetch_many(
        [
            FetchRequest(
                url=TEAM_STATS_URL,
                cache_key="production-default-cf",
                validator=lambda response: json.loads(response.content) is not None,
                before_network=lambda: tokens.append("token"),
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
        ]
    )

    assert result[0].route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.get_calls) == 4
    assert len(fs.xhr_many_calls) == 1
    assert len(fs.created) == 4
    assert len(fs.destroyed) == 3
    assert sleeps == [2.5, 5.0, 10.0]
    assert tokens == ["token"] * 4


@pytest.mark.unit
def test_production_batch_retries_only_four_cf_items_until_fourth_attempt(
    monkeypatch,
):
    urls = [TEAM_STATS_URL + f"&category=cf-{index}" for index in range(8)]

    def cf_solution(url):
        solution = _batch_solution(url, CF_HTML)
        solution.update(
            {
                "status": 403,
                "headers": {"server": "cloudflare", "cf-ray": "abc"},
            }
        )
        return solution

    bootstrap = {"html": "<html>Team Statistics</html>", "status": 200}
    first_outcomes = [
        *[_batch_solution(url) for url in urls[:4]],
        *[cf_solution(url) for url in urls[4:]],
    ]
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        bootstrap,
        first_outcomes,
        bootstrap,
        [cf_solution(url) for url in urls[4:]],
        bootstrap,
        [cf_solution(url) for url in urls[4:]],
        bootstrap,
        [_batch_solution(url) for url in urls[4:]],
    )
    cache = KeyedMemoryRawCache()
    sleeps = []
    tokens = []
    jitter_values = iter((0.5, 1.0, 2.0))
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    monkeypatch.setattr(
        "scrapers.whoscored.transport.random.uniform",
        lambda low, high: next(jitter_values),
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
        raw_cache=cache,
    )

    results = transport.fetch_many(
        [
            FetchRequest(
                url=url,
                cache_key=f"partial-cf-{index}",
                validator=lambda response: json.loads(response.content) is not None,
                before_network=lambda: tokens.append("token"),
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
            for index, url in enumerate(urls)
        ]
    )

    assert all(result.route is TransportRoute.DIRECT_FLARESOLVERR for result in results)
    assert [len(call[0]) for call in fs.xhr_many_calls] == [8, 4, 4, 4]
    assert len(fs.created) == 4
    assert len(fs.destroyed) == 3
    assert sleeps == [2.5, 5.0, 10.0]
    assert tokens == ["token"] * 20
    assert len(cache.stored) == 8
    assert set(cache.payloads) == {f"partial-cf-{index}" for index in range(8)}


@pytest.mark.unit
def test_production_browser_four_cf_challenges_fail_closed_without_paid(
    monkeypatch,
):
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(
        *[FlareSolverrCFChallengeFailed("cloudflare challenge") for _ in range(4)]
    )
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    monkeypatch.setattr(
        "scrapers.whoscored.transport.random.uniform", lambda _low, _high: 0.0
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
    )

    with pytest.raises(CloudflareChallenge):
        transport.fetch(
            "https://www.whoscored.com/Matches/1/Live",
            before_network=lambda: tokens.append("token"),
        )

    assert len(fs.get_calls) == 4
    assert len(fs.created) == 4
    assert len(fs.destroyed) == 4
    assert sleeps == [2.0, 4.0, 8.0]
    assert tokens == ["token"] * 4


@pytest.mark.unit
def test_stale_flaresolverr_identity_is_config_failure_before_cache():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    runtime_error = FlareSolverrRuntimeIdentityError(
        "FlareSolverr runtime identity does not match the attested extension"
    )
    fs = FakeFSClient(runtime_error)
    cache = MemoryRawCache()
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        raw_cache=cache,
        attempts=4,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            validator=lambda response: json.loads(response.content) is not None,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )

    assert raised.value.kind is FailureKind.CONFIG
    assert raised.value.retryable is False
    assert len(fs.get_calls) == 1
    assert len(fs.created) == 1
    assert cache.stored == []
    assert factory_calls == []



@pytest.mark.unit
@pytest.mark.parametrize(
    ("message", "expected_retryable", "expected_batch_calls"),
    [
        (
            "FlareSolverr HTTP 502: WhoScored page request header is unavailable.",
            False,
            1,
        ),
        ("FlareSolverr HTTP 502: ordinary browser failure", True, 3),
    ],
)
def test_missing_site_header_batch_failure_is_not_retried(
    message, expected_retryable, expected_batch_calls
):
    """The stage-stat XHR fails synchronously in the extension when the page's
    RequireJS site-header token is absent, and a bootstrap reload does not
    recover it (#1017): every retry only burns wall-clock and proxy bytes, so
    that failure must leave the browser loop on the first raise while ordinary
    browser failures keep retrying.
    """
    bootstrap = {
        "html": "<html><body>Team Statistics</body></html>",
        "status": 200,
        "cookies": [],
        "userAgent": "browser",
    }
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        *[item for _ in range(3) for item in (bootstrap, FlareSolverrError(message))]
    )
    transport, factory_calls = _transport(
        direct, direct_fs=fs, raw_cache=KeyedMemoryRawCache(), attempts=3
    )
    request = FetchRequest(
        url=TEAM_STATS_URL,
        cache_key="feed-0",
        validator=lambda response: json.loads(response.content) is not None,
        scope="INT-World Cup=2026",
        entity="team_stage_statistics",
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch_many([request])

    assert raised.value.kind is FailureKind.BROWSER
    assert raised.value.retryable is expected_retryable
    assert len(fs.xhr_many_calls) == expected_batch_calls
    assert factory_calls == []

@pytest.mark.unit
@pytest.mark.parametrize("value", [True, -1, float("nan"), float("inf"), "2"])
def test_invalid_browser_retry_jitter_fails_before_network(value):
    direct = FakeHTTPSession()
    fs = FakeFSClient()

    with pytest.raises(ValueError, match="browser_retry_jitter_seconds"):
        WhoScoredTransport(
            direct_http_session=direct,
            direct_fs_client=fs,
            browser_retry_jitter_seconds=value,
        )

    assert direct.calls == []
    assert fs.created == []


@pytest.mark.unit
def test_browser_retry_jitter_never_exceeds_total_backoff_cap(monkeypatch):
    transport, _ = _transport(
        FakeHTTPSession(),
        retry_backoff=30.0,
        retry_jitter=2.0,
    )
    sleeps = []
    jitter_calls = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )

    def maximum_jitter(low, high):
        jitter_calls.append((low, high))
        return high

    monkeypatch.setattr("scrapers.whoscored.transport.random.uniform", maximum_jitter)

    transport._wait_before_browser_retry(0)

    assert jitter_calls == [(0.0, 2.0)]
    assert sleeps == [30.0]


@pytest.mark.unit
def test_zero_browser_backoff_skips_jitter_rng_and_sleep(monkeypatch):
    transport, _ = _transport(
        FakeHTTPSession(),
        retry_backoff=0.0,
        retry_jitter=2.0,
    )
    sleeps = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )

    def unexpected_jitter(_low, _high):
        raise AssertionError("jitter RNG must not run when browser backoff is zero")

    monkeypatch.setattr(
        "scrapers.whoscored.transport.random.uniform", unexpected_jitter
    )

    transport._wait_before_browser_retry(3)

    assert sleeps == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "transient_error",
    [
        FlareSolverrError("internal browser failure"),
        FlareSolverrTimeout("browser timeout"),
    ],
)
def test_serial_retryable_browser_failure_rotates_and_retries_without_paid(
    transient_error,
):
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(
        transient_error,
        {"html": OK_HTML.decode(), "status": 200},
    )
    tokens = []
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        attempts=2,
    )

    result = transport.fetch(
        "https://www.whoscored.com/Matches/1/Live",
        before_network=lambda: tokens.append("token"),
    )

    assert result.route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(fs.get_calls) == 2
    assert len(fs.created) == 2
    assert len(fs.destroyed) == 1
    assert tokens == ["token"] * 2
    assert factory_calls == []


@pytest.mark.unit
def test_direct_browser_soft_origin_502_exhaustion_never_enables_paid_proxy():
    soft_502 = b"<html><title>whoscored.com | 502: Bad gateway</title></html>"
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(
        {"html": soft_502.decode(), "status": 200},
        {"html": soft_502.decode(), "status": 200},
    )
    transport, _ = _transport(direct, direct_fs=fs, attempts=2)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.HTTP_STATUS
    assert exc.value.status_code == 502
    assert exc.value.retryable is True
    assert len(fs.get_calls) == 2


@pytest.mark.unit
def test_lost_create_response_destroys_possible_orphan_browser_session():
    class CreateThenTimeoutFS(FakeFSClient):
        def create_session(self, session_id, proxy_url=None):
            self.created.append((session_id, proxy_url))
            raise FlareSolverrTimeout("create response lost")

    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = CreateThenTimeoutFS()
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.TIMEOUT
    assert len(fs.created) == 2
    assert fs.destroyed == [created[0] for created in fs.created]
    assert transport.get_traffic_stats()["browser_sessions"] == 0


@pytest.mark.unit
def test_system_exit_during_browser_create_destroys_deterministic_session_id():
    class CreateThenTerminateFS(FakeFSClient):
        def create_session(self, session_id, proxy_url=None):
            self.created.append((session_id, proxy_url))
            raise SystemExit(143)

    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = CreateThenTerminateFS()
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(SystemExit) as raised:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert raised.value.code == 143
    assert len(fs.created) == 1
    assert fs.destroyed == [fs.created[0][0]]
    assert transport._browser_sessions == {}


@pytest.mark.unit
def test_system_exit_after_remote_create_before_return_destroys_tracked_session():
    class TerminateOnBrowserSessionIncrement:
        def __init__(self, wrapped):
            object.__setattr__(self, "_wrapped", wrapped)
            object.__setattr__(self, "_terminated", False)

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

        def __setattr__(self, name, value):
            if name == "browser_sessions" and not self._terminated:
                object.__setattr__(self, "_terminated", True)
                raise SystemExit(143)
            setattr(self._wrapped, name, value)

    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=fs)
    transport.stats = TerminateOnBrowserSessionIncrement(transport.stats)

    with pytest.raises(SystemExit) as raised:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert raised.value.code == 143
    assert len(fs.created) == 1
    assert fs.destroyed == [fs.created[0][0]]
    assert transport._browser_sessions == {}
    assert transport.get_traffic_stats()["browser_sessions"] == 0


@pytest.mark.unit
def test_system_exit_during_destroy_keeps_session_tracked_for_close_retry():
    class TerminateFirstDestroyFS(FakeFSClient):
        def destroy_session(self, session_id):
            self.destroyed.append(session_id)
            if len(self.destroyed) == 1:
                raise SystemExit(143)

    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = TerminateFirstDestroyFS(
        {"html": OK_HTML.decode(), "status": 200, "cookies": [], "userAgent": "x"}
    )
    transport, _ = _transport(direct, direct_fs=fs)
    transport.fetch("https://www.whoscored.com/Matches/1/Live")
    session_id = fs.created[0][0]

    with pytest.raises(SystemExit) as raised:
        transport.close()

    assert raised.value.code == 143
    assert (
        transport._browser_sessions[TransportRoute.DIRECT_FLARESOLVERR].session_id
        == session_id
    )

    transport.close()

    assert fs.destroyed == [session_id, session_id]
    assert transport._browser_sessions == {}
    assert direct.closed is True
    assert fs.closed is True


@pytest.mark.unit
def test_supervised_browser_session_is_fsynced_before_create_and_after_destroy(
    tmp_path,
    monkeypatch,
):
    owner = "a" * 24
    ledger_path = tmp_path / "remote-resources.jsonl"
    monkeypatch.setenv(transport_module.SUPERVISOR_SESSION_OWNER_ENV, owner)
    monkeypatch.setenv(
        transport_module.SUPERVISOR_RESOURCE_LEDGER_ENV,
        str(ledger_path),
    )
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(
        {"html": OK_HTML.decode(), "status": 200, "cookies": [], "userAgent": "x"}
    )
    transport, _ = _transport(direct, direct_fs=fs)

    transport.fetch("https://www.whoscored.com/Matches/1/Live")

    events = [json.loads(line) for line in ledger_path.read_text().splitlines()]
    assert [event["event"] for event in events] == ["owned"]
    session_id = events[0]["session_id"]
    assert session_id.startswith(f"ws-cap-{owner}-")
    assert fs.created == [(session_id, None)]

    transport.close()

    events = [json.loads(line) for line in ledger_path.read_text().splitlines()]
    assert [event["event"] for event in events] == ["owned", "released"]
    assert fs.destroyed == [session_id]


@pytest.mark.unit
def test_ordinary_destroy_error_remains_best_effort_and_untracks_session(caplog):
    class FailDestroyFS(FakeFSClient):
        def destroy_session(self, session_id):
            self.destroyed.append(session_id)
            raise RuntimeError(f"destroy failed {session_id}")

    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FailDestroyFS(
        {"html": OK_HTML.decode(), "status": 200, "cookies": [], "userAgent": "x"}
    )
    transport, _ = _transport(direct, direct_fs=fs)
    transport.fetch("https://www.whoscored.com/Matches/1/Live")
    session_id = fs.created[0][0]

    with caplog.at_level("DEBUG"):
        transport.close()

    assert fs.destroyed == [session_id]
    assert session_id not in caplog.text
    assert transport._browser_sessions == {}
    assert direct.closed is True
    assert fs.closed is True


@pytest.mark.unit
def test_transport_close_releases_http_and_browser_pools():
    direct = FakeHTTPSession()
    direct_fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=direct_fs)

    transport.close()

    assert direct.closed is True
    assert direct_fs.closed is True


@pytest.mark.unit
def test_timeout_from_direct_browser_does_not_enable_paid_proxy():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(
        FlareSolverrTimeout("down"),
        FlareSolverrTimeout("still down"),
    )
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.TIMEOUT
    assert len(fs.get_calls) == 2
    assert len(fs.created) == 2
    assert len(fs.destroyed) == 2


@pytest.mark.unit
def test_rendered_browser_challenge_without_source_headers_is_confirmed_cf():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}))
    direct_fs = FakeFSClient(
        {"html": CF_HTML.decode(), "status": 403},
        {"html": CF_HTML.decode(), "status": 403},
    )
    transport, _ = _transport(direct, direct_fs=direct_fs)

    with pytest.raises(CloudflareChallenge) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.route is TransportRoute.DIRECT_FLARESOLVERR
    assert len(direct.calls) == 1
    assert len(direct_fs.created) == 2


@pytest.mark.unit
def test_validator_rejection_is_not_treated_as_cloudflare():
    direct = FakeHTTPSession(FakeHTTPResponse(content=b"wrong page"))
    fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=fs)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/x", validator=lambda _: False)

    assert exc.value.kind is FailureKind.CONTENT
    assert fs.created == []


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


class SimpleControlSession:
    def __init__(self, response):
        self.response = response
        self.posts = []
        self.gets = []
        self.deletes = []
        self.trust_env = True
        self.closed = False

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.response

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return self.response

    def delete(self, url, **kwargs):
        self.deletes.append((url, kwargs))
        return self.response

    def close(self):
        self.closed = True


PROXY_407 = RuntimeError(
    "Failed to perform, curl: (56) CONNECT tunnel failed, response 407."
)


def _pool_transport(monkeypatch, tmp_path, *sessions, http_attempts=1, pool_size=2):
    from scrapers.whoscored.transport import ProxyUnavailable  # noqa: F401

    pool = tmp_path / "pool.txt"
    pool.write_text(
        "".join(f"pool.example.io:{10000 + i}:poolu:poolpw\n" for i in range(pool_size))
    )
    monkeypatch.setenv("WHOSCORED_PROXY_FILE", str(pool))
    queue = list(sessions)
    seen = []

    def factory(proxy_url):
        seen.append(proxy_url)
        return queue.pop(0)

    transport = WhoScoredTransport(
        direct_fs_client=FakeFSClient(),
        http_session_factory=factory,
        direct_http_attempts=http_attempts,
        direct_http_retry_backoff_seconds=0,
        context=TransportContext(transport_policy="direct_only"),
    )
    return transport, seen


@pytest.mark.unit
def test_proxy_unavailable_is_not_a_transport_error():
    from scrapers.whoscored.transport import ProxyUnavailable

    # Per-match handlers catch WhoScoredTransportError and would turn a dead
    # pool into a retryable failure row for every candidate (V1).
    assert not issubclass(ProxyUnavailable, WhoScoredTransportError)


@pytest.mark.unit
@pytest.mark.parametrize(
    "error",
    [
        PROXY_407,
        RuntimeError("curl: (7) Failed to connect to pool.example.io port 10000"),
        RuntimeError("curl: (97) proxy handshake failed"),
    ],
)
def test_second_proxy_failure_after_member_swap_raises_proxy_unavailable(
    monkeypatch, tmp_path, error
):
    from scrapers.whoscored.transport import ProxyUnavailable

    first = FakeHTTPSession(error)
    second = FakeHTTPSession(error)
    transport, seen = _pool_transport(monkeypatch, tmp_path, first, second)

    with pytest.raises(ProxyUnavailable):
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    # Authorization repeats the source; transient failures validate via ipify.
    assert len(seen) == 2 and seen[0] != seen[1]
    assert len(first.calls) == 1 and len(second.calls) == 1
    assert first.closed is True
    stats = transport.get_traffic_stats()
    assert stats["failures"] == {"proxy": 2 if error is PROXY_407 else 1}
    assert stats["route_successes"] == {}


@pytest.mark.unit
def test_proxy_failure_recovers_on_swapped_member(monkeypatch, tmp_path):
    first = FakeHTTPSession(PROXY_407)
    second = FakeHTTPSession(FakeHTTPResponse())
    transport, seen = _pool_transport(monkeypatch, tmp_path, first, second)

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.route is TransportRoute.DIRECT_HTTP
    assert len(seen) == 2
    assert transport.get_traffic_stats()["route_successes"] == {"direct_http": 1}


@pytest.mark.unit
def test_non_proxy_curl_failure_keeps_retryable_timeout_without_swap(
    monkeypatch, tmp_path
):
    first = FakeHTTPSession(RuntimeError("curl: (28) Operation timed out"))
    transport, seen = _pool_transport(monkeypatch, tmp_path, first)

    with pytest.raises(WhoScoredTransportError) as caught:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert caught.value.kind is FailureKind.TIMEOUT
    assert caught.value.retryable is True
    assert len(seen) == 1


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["tls", "certificate", "http502"])
@pytest.mark.parametrize("http_attempts", [1, 3])
def test_schedule_pool_failure_probes_replacement_before_single_replay(
    monkeypatch, tmp_path, failure, http_attempts
):
    from curl_cffi.requests.exceptions import CertificateVerifyError, SSLError

    first = FakeHTTPSession(
        SSLError("curl: (35) TLS connect error", code=35)
        if failure == "tls" else CertificateVerifyError("curl: (60) certificate verify failed", code=60)
        if failure == "certificate" else FakeHTTPResponse(status_code=502)
    )
    second = FakeHTTPSession(FakeHTTPResponse(), FakeHTTPResponse(content=b'{"tournaments":[]}'))
    transport, seen = _pool_transport(
        monkeypatch, tmp_path, first, second, http_attempts=http_attempts
    )
    original_proxy = transport._pool_proxy_url
    transport._stage_http_context = ("bootstrap", "token", first, original_proxy)
    tokens = []
    source = "https://www.whoscored.com/tournaments/25590/data/?d=202610"

    result = transport.fetch(source, before_network=lambda: tokens.append(1))

    assert result.status_code == 200
    assert [call[0] for call in first.calls + second.calls] == [
        source, transport_module.EGRESS_PROBE_URL, source,
    ]
    assert len(seen) == 2 and seen[0] != seen[1]
    assert first.closed
    assert transport._stage_http_context is None
    assert len(tokens) == 2
    assert transport.get_traffic_stats()["route_successes"] == {"direct_http": 1}
    assert not transport._direct_fs.get_calls


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["tls", "certificate", "http502", "timeout", "http500", "http503"])
def test_schedule_pool_replay_failure_never_requests_source_a_third_time(
    monkeypatch, tmp_path, failure
):
    from curl_cffi.requests.exceptions import CertificateVerifyError, SSLError

    def failed():
        return (SSLError("curl: (35) TLS connect error", code=35)
                if failure == "tls" else CertificateVerifyError("curl: (60) certificate verify failed", code=60)
                if failure == "certificate" else FakeHTTPResponse(status_code=502))

    first = FakeHTTPSession(failed())
    replay_failure = {
        "timeout": RuntimeError("curl: (28) timed out"),
        "http500": FakeHTTPResponse(status_code=500),
        "http503": FakeHTTPResponse(status_code=503),
    }.get(failure, failed())
    second = FakeHTTPSession(FakeHTTPResponse(), replay_failure)
    transport, seen = _pool_transport(monkeypatch, tmp_path, first, second, http_attempts=3)
    tokens = []
    with pytest.raises(WhoScoredTransportError) as caught:
        transport.fetch(
            "https://www.whoscored.com/tournaments/25590/data/?d=202610",
            before_network=lambda: tokens.append(1),
        )
    assert caught.value.kind is (
        FailureKind.TIMEOUT if failure in {"tls", "certificate", "timeout"} else FailureKind.HTTP_STATUS
    )
    assert caught.value.retryable is False
    assert len(seen) == 2 and seen[0] != seen[1]
    assert [len(s.calls) for s in (first, second)] == [1, 2]
    assert len(tokens) == 2
    assert not transport._direct_fs.get_calls


@pytest.mark.unit
@pytest.mark.parametrize("code", [28])
def test_schedule_other_curl_errors_do_not_renew_session(monkeypatch, tmp_path, code):
    from curl_cffi.requests.exceptions import RequestException

    first = FakeHTTPSession(
        RequestException(f"curl: ({code}) other failure", code=code),
        FakeHTTPResponse(),
    )
    transport, seen = _pool_transport(monkeypatch, tmp_path, first, http_attempts=3)
    transport.fetch("https://www.whoscored.com/tournaments/25590/data/?d=202610")
    assert len(seen) == 1 and len(first.calls) == 2
    assert not first.closed


@pytest.mark.unit
@pytest.mark.parametrize("code", [35, 60])
def test_tls_failure_without_pool_keeps_error_policy(code):
    from curl_cffi.requests.exceptions import CertificateVerifyError, SSLError

    error_type = SSLError if code == 35 else CertificateVerifyError
    session = FakeHTTPSession(error_type(f"curl: ({code}) TLS error", code=code))
    transport, seen = _transport(session, http_attempts=1)

    with pytest.raises(WhoScoredTransportError) as caught:
        transport.fetch("https://www.whoscored.com/tournaments/25590/data/?d=202610")

    assert caught.value.kind is FailureKind.TIMEOUT
    assert caught.value.retryable is True
    assert len(session.calls) == 1
    assert not session.closed
    assert seen == []


@pytest.mark.unit
@pytest.mark.parametrize("url", [
    "https://www.whoscored.com/Matches/1/Live",
    "https://www.whoscored.com/tournaments/25590/data/",
    "https://www.whoscored.com/tournaments/25590/data/?d=202699",
    "https://www.whoscored.com/tournaments/25590/data/?d=000010",
    "http://www.whoscored.com/tournaments/25590/data/?d=202610",
    "https://www.whoscored.com/tournaments/25590/data/?d=202610&d=202611",
    "https://user@www.whoscored.com/tournaments/25590/data/?d=202610",
    "https://www.whoscored.com:443/tournaments/25590/data/?d=202610",
    "https://www.whoscored.com/tournaments/25590/data/?d=202610#fragment",
])
@pytest.mark.parametrize("code", [35, 60])
def test_pool_tls_error_recovers_for_other_urls(monkeypatch, tmp_path, url, code):
    from curl_cffi.requests.exceptions import SSLError

    first = FakeHTTPSession(SSLError(f"curl: ({code}) TLS error", code=code))
    second = FakeHTTPSession(FakeHTTPResponse(), FakeHTTPResponse())
    transport, seen = _pool_transport(monkeypatch, tmp_path, first, second, http_attempts=3)
    transport.fetch(url)
    assert len(seen) == 2 and seen[0] != seen[1]
    assert len(first.calls) == 1 and len(second.calls) == 2
    assert first.closed


@pytest.mark.unit
def test_pool_5xx_swaps_member_once_and_keeps_retry_policy(monkeypatch, tmp_path):
    first = FakeHTTPSession(FakeHTTPResponse(status_code=502, content=b"bad gateway"))
    second = FakeHTTPSession(FakeHTTPResponse(), FakeHTTPResponse())
    transport, seen = _pool_transport(
        monkeypatch, tmp_path, first, second, http_attempts=2
    )

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.status_code == 200
    assert len(seen) == 2 and seen[0] != seen[1]
    stats = transport.get_traffic_stats()
    assert stats["route_requests"] == {"direct_http": 2}
    assert stats["route_successes"] == {"direct_http": 1}


@pytest.mark.unit
def test_pool_5xx_rotation_excludes_current_before_random_choice(monkeypatch, tmp_path):
    # Always picking the first entry made the bounded random search miss the
    # healthy second member and eventually exhaust the session factory.
    monkeypatch.setattr("scrapers.utils.proxy_manager.random.choice", lambda pool: pool[0])
    test_pool_5xx_swaps_member_once_and_keeps_retry_policy(monkeypatch, tmp_path)


@pytest.mark.unit
def test_egress_probe_hits_ipify_only(monkeypatch, tmp_path):
    session = FakeHTTPSession(FakeHTTPResponse(content=b"203.0.113.7"))
    transport, seen = _pool_transport(monkeypatch, tmp_path, session)

    transport.probe_egress()

    assert [call[0] for call in session.calls] == ["https://api.ipify.org"]
    assert len(seen) == 1
    # A probe is not a source answer.
    assert transport.get_traffic_stats()["route_successes"] == {}


@pytest.mark.unit
def test_egress_probe_swaps_once_then_raises_proxy_unavailable(monkeypatch, tmp_path):
    from scrapers.whoscored.transport import ProxyUnavailable

    first = FakeHTTPSession(PROXY_407)
    second = FakeHTTPSession(FakeHTTPResponse(status_code=407, content=b""))
    transport, seen = _pool_transport(monkeypatch, tmp_path, first, second)

    with pytest.raises(ProxyUnavailable, match="HTTP 407"):
        transport.probe_egress()

    assert len(seen) == 2 and seen[0] != seen[1]
    assert all("whoscored" not in call[0] for call in first.calls + second.calls)


@pytest.mark.unit
def test_egress_probe_is_noop_without_pool():
    session = FakeHTTPSession()
    transport, _ = _transport(session)

    transport.probe_egress()

    assert session.calls == []

# --- #1599: bounded recovery on distinct pool members ------------------------

PROXY_502 = RuntimeError("curl: (56) CONNECT tunnel failed, response 502")


def _ordered_pool_transport(monkeypatch, tmp_path, sessions, *, choices=None):
    from scrapers.utils.proxy_manager import ProxyManager

    urls = [f"http://test:secret@pool.invalid:{10000 + i}" for i in range(len(sessions))]

    def select_member(self, *, excluded_http_urls=None):
        return next(
            (url for url in (choices or urls) if url not in (excluded_http_urls or set())),
            None,
        )

    monkeypatch.setattr(ProxyManager, "get_http_proxy_url", select_member)
    transport, seen = _pool_transport(monkeypatch, tmp_path, *sessions, pool_size=len(sessions))
    return transport, seen, urls


@pytest.mark.unit
@pytest.mark.parametrize("failures", [0, 1, 2, 3, 9])
@pytest.mark.parametrize("error", [PROXY_502, RuntimeError("curl: (60) certificate verify failed")])
def test_egress_recovery_reaches_first_healthy_member(monkeypatch, tmp_path, failures, error):
    sessions = [FakeHTTPSession(error) for _ in range(failures)]
    sessions.append(FakeHTTPSession(FakeHTTPResponse()))
    transport, seen, urls = _ordered_pool_transport(monkeypatch, tmp_path, sessions)
    permit = object()
    transport._source_circuit_permit = permit
    transport._source_circuit_browser_blocked = True
    transport._direct_gate_circuits.add("source-gate")

    transport.probe_egress()

    assert seen == urls
    assert all(len(session.calls) == 1 for session in sessions)
    assert all(session.closed for session in sessions[:-1])
    assert transport._pool_proxy_url == urls[-1]
    assert transport.get_traffic_stats()["route_successes"] == {}
    assert transport._source_circuit_permit is permit
    assert transport._source_circuit_browser_blocked is True
    assert transport._direct_gate_circuits == {"source-gate"}


@pytest.mark.unit
@pytest.mark.parametrize("members", [1, 2, 3, 4, 10, 11])
def test_egress_recovery_exhausts_distinct_members_only(monkeypatch, tmp_path, members):
    sessions = [FakeHTTPSession(PROXY_502) for _ in range(members)]
    transport, seen, urls = _ordered_pool_transport(monkeypatch, tmp_path, sessions)
    with pytest.raises(transport_module.ProxyUnavailable, match="access check exhausted"):
        transport.probe_egress()
    assert seen == urls[:10]
    assert sum(len(session.calls) for session in sessions) == min(members, 10)


@pytest.mark.unit
def test_egress_recovery_does_not_revisit_or_leak_credentials(monkeypatch, tmp_path, caplog):
    urls = [f"http://test:secret@pool.invalid:{10000 + i}" for i in range(3)]
    secret_error = RuntimeError(f"curl: (56) CONNECT tunnel failed, response 502 {urls[0]}")
    sessions = [FakeHTTPSession(secret_error) for _ in range(3)]
    transport, seen, _ = _ordered_pool_transport(
        monkeypatch, tmp_path, sessions, choices=[urls[0], urls[0], urls[1], urls[0], urls[1], urls[2]]
    )
    with pytest.raises(transport_module.ProxyUnavailable) as caught:
        transport.probe_egress()
    assert seen == urls
    assert "secret" not in str(caught.value) + caplog.text
    assert "pool.invalid" not in str(caught.value) + caplog.text
    assert "reason=budget_exhausted" in caplog.text
    assert "members=3" in caplog.text


@pytest.mark.unit
def test_egress_recovery_407_stays_bounded_to_two_members(monkeypatch, tmp_path):
    sessions = [FakeHTTPSession(PROXY_407) for _ in range(3)]
    transport, seen, urls = _ordered_pool_transport(monkeypatch, tmp_path, sessions)
    with pytest.raises(transport_module.ProxyUnavailable):
        transport.probe_egress()
    assert seen == urls[:2]
    assert sessions[2].calls == []


@pytest.mark.unit
@pytest.mark.parametrize("replay_fails", [False, True])
def test_pool_recovery_probes_before_single_source_replay(monkeypatch, tmp_path, replay_fails):
    sessions = [FakeHTTPSession(PROXY_502), FakeHTTPSession(PROXY_502),
                FakeHTTPSession(FakeHTTPResponse(), PROXY_502 if replay_fails else FakeHTTPResponse())]
    transport, seen, urls = _ordered_pool_transport(monkeypatch, tmp_path, sessions)
    source = "https://www.whoscored.com/Matches/1/Live"
    if replay_fails:
        with pytest.raises(transport_module.ProxyUnavailable):
            transport.fetch(source)
    else:
        assert transport.fetch(source).status_code == 200
    assert seen == urls
    assert [call[0] for session in sessions for call in session.calls] == [
        source, transport_module.EGRESS_PROBE_URL, transport_module.EGRESS_PROBE_URL, source]
    assert transport.get_traffic_stats()["route_successes"] == ({} if replay_fails else {"direct_http": 1})


@pytest.mark.unit
@pytest.mark.parametrize("members", [4, 11])
def test_pool_recovery_failed_validation_never_replays_source(monkeypatch, tmp_path, members):
    sessions = [FakeHTTPSession(PROXY_502) for _ in range(members)]
    transport, seen, urls = _ordered_pool_transport(monkeypatch, tmp_path, sessions)
    source = "https://www.whoscored.com/Matches/1/Live"
    with pytest.raises(transport_module.ProxyUnavailable):
        transport.fetch(source)
    assert seen == urls[:10]
    assert [call[0] for session in sessions for call in session.calls] == [
        source, *([transport_module.EGRESS_PROBE_URL] * (min(members, 10) - 1))]


@pytest.mark.unit
def test_egress_recovery_member_identity_survives_next_operation(monkeypatch, tmp_path, caplog):
    import logging
    sessions = [FakeHTTPSession(PROXY_502), FakeHTTPSession(FakeHTTPResponse(), FakeHTTPResponse())]
    transport, _, _ = _ordered_pool_transport(monkeypatch, tmp_path, sessions)
    caplog.set_level(logging.INFO, logger="scrapers.whoscored.transport")
    transport.probe_egress()
    caplog.clear()
    transport.probe_egress()
    assert "attempt=1 member=2" in caplog.text
    assert all(record.levelno == logging.INFO for record in caplog.records)


@pytest.mark.unit
def test_egress_recovery_stops_when_no_distinct_replacement_remains(monkeypatch, tmp_path):
    sessions = [FakeHTTPSession(PROXY_502)]
    transport, seen, urls = _ordered_pool_transport(monkeypatch, tmp_path, sessions)
    calls = []
    select = transport._pool_manager.get_http_proxy_url

    def record_choice(*, excluded_http_urls=None):
        calls.append(set(excluded_http_urls or ()))
        return select(excluded_http_urls=excluded_http_urls)

    monkeypatch.setattr(transport._pool_manager, "get_http_proxy_url", record_choice)
    with pytest.raises(transport_module.ProxyUnavailable, match="budget_exhausted"):
        transport.probe_egress()
    assert calls == []
    assert seen == urls[:1]
    assert len(sessions[0].calls) == 1
    assert sessions[0].closed is False


@pytest.mark.unit
def test_rotation_without_eligible_member_keeps_session_and_exclusions(monkeypatch, tmp_path):
    from scrapers.utils.proxy_manager import ProxyManager

    monkeypatch.setattr("scrapers.utils.proxy_manager.random.choice", lambda pool: pool[0])
    session = FakeHTTPSession()
    transport, seen = _pool_transport(monkeypatch, tmp_path, session)
    manager = ProxyManager()
    manager.add_proxy_url(seen[0])
    transport._pool_manager = manager
    excluded = {"http://other.invalid:8080"}

    assert transport._rotate_pool_proxy(excluded) is False
    assert excluded == {"http://other.invalid:8080"}
    assert transport._direct_http is session
    assert transport._pool_proxy_url == seen[0]
    assert len(seen) == 1
    assert session.closed is False


@pytest.mark.unit
def test_pool_recovery_suppresses_credentials_in_traceback(monkeypatch, tmp_path, caplog):
    import traceback
    secret_error = RuntimeError("curl: (56) CONNECT tunnel failed, response 502 http://user:private-password@proxy.invalid")
    sessions = [FakeHTTPSession(secret_error), FakeHTTPSession(FakeHTTPResponse(), secret_error)]
    transport, _, _ = _ordered_pool_transport(monkeypatch, tmp_path, sessions)
    with pytest.raises(transport_module.ProxyUnavailable) as caught:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")
    rendered = "".join(traceback.format_exception(caught.type, caught.value, caught.tb))
    assert "private-password" not in rendered + caplog.text
    assert "proxy.invalid" not in rendered + caplog.text


@pytest.mark.unit
def test_egress_recovery_curl_timeout_does_not_hide_healthy_third(monkeypatch, tmp_path):
    from curl_cffi.requests.exceptions import Timeout

    sessions = [FakeHTTPSession(PROXY_502), FakeHTTPSession(Timeout("ipify timed out", code=28)),
                FakeHTTPSession(FakeHTTPResponse())]
    transport, seen, urls = _ordered_pool_transport(monkeypatch, tmp_path, sessions)

    transport.probe_egress()

    assert seen == urls
    assert [len(session.calls) for session in sessions] == [1, 1, 1]
    assert transport.get_traffic_stats()["route_successes"] == {}


@pytest.mark.unit
def test_source_curl_timeout_keeps_timeout_policy_without_pool_rotation(monkeypatch, tmp_path):
    from curl_cffi.requests.exceptions import Timeout

    sessions = [FakeHTTPSession(Timeout("source timed out", code=28)), FakeHTTPSession()]
    transport, seen, urls = _ordered_pool_transport(monkeypatch, tmp_path, sessions)

    with pytest.raises(WhoScoredTransportError) as caught:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert caught.value.kind is FailureKind.TIMEOUT
    assert caught.value.retryable is True
    assert seen == urls[:1]
    assert sessions[1].calls == []


@pytest.mark.unit
@pytest.mark.parametrize("authorized_at", [0, 1, 3])
def test_late_407_never_restarts_extended_egress_budget(monkeypatch, tmp_path, authorized_at):
    sessions = [FakeHTTPSession(PROXY_502) for _ in range(10)]
    sessions[authorized_at] = FakeHTTPSession(PROXY_407)
    transport, seen, urls = _ordered_pool_transport(monkeypatch, tmp_path, sessions)
    with pytest.raises(transport_module.ProxyUnavailable):
        transport.probe_egress()
    assert seen == urls[:max(2, authorized_at + 1)]
    assert all(not session.calls for session in sessions[len(seen):])


@pytest.mark.unit
@pytest.mark.parametrize("failure", ["tls", "certificate", "http502"])
def test_pool_recovery_preserves_single_half_open_source_probe(monkeypatch, tmp_path, failure):
    first = FakeHTTPSession(
        RuntimeError("curl: (35) TLS connect error")
        if failure == "tls" else RuntimeError("curl: (60) certificate verify failed")
        if failure == "certificate" else FakeHTTPResponse(status_code=502)
    )
    transport, seen = _pool_transport(monkeypatch, tmp_path, first, http_attempts=3)
    circuit = FakeSourceCircuit(probe=True)
    transport._source_circuit = circuit
    with pytest.raises(WhoScoredTransportError) as caught:
        transport.fetch(TEAM_STATS_URL)
    assert caught.value.kind is (FailureKind.TIMEOUT if failure in {"tls", "certificate"} else FailureKind.HTTP_STATUS)
    assert len(seen) == 1
    assert len(first.calls) == 1
    assert circuit.calls == [("admit", False), ("inconclusive", True)]


# --- #1478: source-issued statistics token, same pooled session only ----------

STAGE_TOKEN = "a" * 43 + "="


def _statistics_html(script=None):
    if script is None:
        script = (
            "window.require.config.params.site = {"
            'gSiteHeaderName: "Model-last-Mode", '
            f'gSiteHeaderValue: "{STAGE_TOKEN}"' + "};"
        )
    return (
        "<html><head><script>" + script + "</script></head>"
        '<body><div id="statistics-team-table-summary"></div></body></html>'
    ).encode()


@pytest.mark.unit
def test_statistics_preparation_adds_source_token_to_same_http_session():
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=_statistics_html()),
        FakeHTTPResponse(content=b'{"teamTableStats":[]}'),
    )
    direct.cookies = requests.cookies.RequestsCookieJar()
    direct.cookies.set("source-session", "fixture", domain="www.whoscored.com")
    cache = KeyedMemoryRawCache()
    transport, _ = _transport(direct, raw_cache=cache)
    gate = []

    prepared = transport.prepare_stage_statistics(
        TEAM_STATS_BOOTSTRAP,
        cache_key="bootstrap",
        before_network=lambda: gate.append(1),
        scope="scope",
        entity="statistics_bootstrap",
    )
    result = transport.fetch(
        TEAM_STATS_URL,
        cache_key="feed",
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert transport.impersonate == "chrome142"
    assert prepared.sha256 == hashlib.sha256(_statistics_html()).hexdigest()
    assert prepared.route is result.route is TransportRoute.DIRECT_HTTP
    assert cache.stored[0][1].content == _statistics_html()
    assert gate == [1]
    assert direct.calls[1][2]["headers"]["Model-last-Mode"] == STAGE_TOKEN
    assert direct.calls[1][2]["headers"]["Referer"] == TEAM_STATS_BOOTSTRAP
    assert direct.cookies.get("source-session") == "fixture"
    stats = transport.get_traffic_stats()
    assert stats["stage_xhr_token"] == {"available": 1, "curl_2xx_responses": 1}
    assert STAGE_TOKEN not in json.dumps(stats)
    assert "source-session" not in json.dumps(stats)


@pytest.mark.unit
@pytest.mark.parametrize(
    "script",
    [
        "",
        'var gSiteHeaderName="wrong"; var gSiteHeaderValue="' + STAGE_TOKEN + '";',
        'var gSiteHeaderName="Model-last-Mode"; var gSiteHeaderValue="bad";',
        'var gSiteHeaderName="Model-last-Mode"; var gSiteHeaderValue=compute();',
        'var gSiteHeaderName="Model-last-Mode"; var gSiteHeaderValue="'
        + STAGE_TOKEN
        + '" + "more";',
        '// gSiteHeaderName="Model-last-Mode"; gSiteHeaderValue="' + STAGE_TOKEN + '";',
        '/* gSiteHeaderName="Model-last-Mode"; gSiteHeaderValue="'
        + STAGE_TOKEN
        + '"; */',
        'var template=`gSiteHeaderName="Model-last-Mode"; gSiteHeaderValue="'
        + STAGE_TOKEN
        + '";`; ',
        "var example=\"gSiteHeaderName: 'Model-last-Mode', gSiteHeaderValue: '"
        + STAGE_TOKEN
        + "'\";",
    ],
)
def test_complete_statistics_html_without_literal_token_is_source_absence(script):
    from scrapers.whoscored.transport import StageStatisticsUnavailable

    cache = KeyedMemoryRawCache()
    transport, _ = _transport(
        FakeHTTPSession(FakeHTTPResponse(content=_statistics_html(script))),
        raw_cache=cache,
    )

    with pytest.raises(StageStatisticsUnavailable) as caught:
        transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")

    assert caught.value.kind is FailureKind.CONTENT
    assert caught.value.observed_at == cache.stored[0][1].observed_at
    assert not caught.value.retryable
    assert len(cache.stored) == 1
    assert transport._stage_http_context is None
    assert transport.get_traffic_stats()["stage_xhr_token"] == {"source_unavailable": 1}


@pytest.mark.unit
@pytest.mark.parametrize(
    "content",
    [
        b'<html><body><div id="statistics-team-table-summary">',
        b"<html><body>The page you requested does not exist.</body></html>",
        b"<html><body>Maintenance</body></html>",
    ],
)
def test_incomplete_statistics_html_is_not_source_absence(content):
    from scrapers.whoscored.transport import StageStatisticsUnavailable

    fs = FakeFSClient()
    transport, _ = _transport(
        FakeHTTPSession(FakeHTTPResponse(content=content)), direct_fs=fs
    )
    with pytest.raises(WhoScoredTransportError) as caught:
        transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    assert caught.value.kind is FailureKind.CONTENT
    assert not isinstance(caught.value, StageStatisticsUnavailable)
    assert not fs.created


@pytest.mark.unit
def test_statistics_preparation_cf_uses_pool_and_replays_cookies():
    direct = FakeHTTPSession(
        FakeHTTPResponse(
            status_code=403, content=CF_HTML, headers={"server": "cloudflare"}
        ),
        FakeHTTPResponse(content=b"{}"),
    )
    direct.cookies = requests.cookies.RequestsCookieJar()
    direct.headers = {}
    fs = FakeFSClient(
        {
            "html": _statistics_html().decode(),
            "status": 200,
            "cookies": [
                {
                    "name": "cf_clearance",
                    "value": "fixture",
                    "domain": "www.whoscored.com",
                    "path": "/",
                }
            ],
            "userAgent": "Chromium/142 fixture",
        }
    )
    transport, _ = _transport(direct, direct_fs=fs)
    transport._pool_proxy_url = "http://fixture.invalid:10000"

    response = transport.prepare_stage_statistics(
        TEAM_STATS_BOOTSTRAP, cache_key="bootstrap"
    )
    transport.fetch(TEAM_STATS_URL, browser_bootstrap_url=TEAM_STATS_BOOTSTRAP)

    assert response.route is TransportRoute.DIRECT_FLARESOLVERR
    assert fs.created[0][1] == transport._pool_proxy_url
    assert direct.cookies.get("cf_clearance") == "fixture"
    assert direct.headers["User-Agent"] == "Chromium/142 fixture"
    assert direct.calls[-1][2]["headers"]["Model-last-Mode"] == STAGE_TOKEN


@pytest.mark.unit
def test_statistics_preparation_preserves_cf_and_open_circuit_fail_closed():
    from scrapers.whoscored.transport import StageStatisticsUnavailable

    circuit = FakeSourceCircuit(opened=True)
    direct = FakeHTTPSession()
    fs = FakeFSClient()
    transport, _ = _transport(direct, direct_fs=fs, source_circuit=circuit)
    with pytest.raises(CloudflareChallenge) as caught:
        transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    assert isinstance(caught.value.__cause__, SourceCircuitOpen)
    assert not direct.calls and not fs.created
    assert not isinstance(caught.value, StageStatisticsUnavailable)

    circuit.opened = False
    direct.responses = [
        FakeHTTPResponse(
            status_code=403, content=CF_HTML, headers={"server": "cloudflare"}
        )
    ]
    fs.results = [FlareSolverrCFChallengeFailed("challenge")]
    with pytest.raises(CloudflareChallenge):
        transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    assert circuit.opened


@pytest.mark.unit
def test_statistics_raw_cache_precedes_circuit_and_does_not_bind_old_cookies():
    cache = KeyedMemoryRawCache()
    cache.payloads["bootstrap"] = CachedPayload(content=_statistics_html())
    circuit = FakeSourceCircuit(opened=True)
    direct = FakeHTTPSession()
    transport, _ = _transport(direct, raw_cache=cache, source_circuit=circuit)
    gate = []
    result = transport.prepare_stage_statistics(
        TEAM_STATS_BOOTSTRAP,
        cache_key="bootstrap",
        before_network=lambda: gate.append(1),
    )
    assert result.route is TransportRoute.RAW_CACHE
    assert not direct.calls and not circuit.calls and not gate
    assert transport._stage_http_context is None


@pytest.mark.unit
@pytest.mark.parametrize(
    "url,referer",
    [
        (TEAM_STATS_URL, PLAYER_STATS_BOOTSTRAP),
        (
            TEAM_STATS_URL.replace("www.whoscored.com", "different.invalid"),
            TEAM_STATS_BOOTSTRAP,
        ),
        (TEAM_STATS_URL.replace("23752", "23753"), TEAM_STATS_BOOTSTRAP),
        (TEAM_STATS_URL + "&stageId=23753", TEAM_STATS_BOOTSTRAP),
    ],
)
def test_statistics_token_never_crosses_bootstrap_host_or_stage(url, referer):
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=_statistics_html()), FakeHTTPResponse(content=b"{}")
    )
    transport, _ = _transport(direct)
    transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    transport._http_fetch(
        url, session=direct, route=TransportRoute.DIRECT_HTTP, referer=referer
    )
    assert "Model-last-Mode" not in direct.calls[-1][2].get("headers", {})


@pytest.mark.unit
def test_statistics_token_not_replayed_after_pool_rotation(monkeypatch, tmp_path):
    first = FakeHTTPSession(FakeHTTPResponse(content=_statistics_html()), PROXY_407)
    second = FakeHTTPSession(FakeHTTPResponse(content=b"{}"))
    transport, seen = _pool_transport(monkeypatch, tmp_path, first, second)
    transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    transport.fetch(TEAM_STATS_URL, browser_bootstrap_url=TEAM_STATS_BOOTSTRAP)
    assert len(seen) == 2
    assert "Model-last-Mode" not in second.calls[0][2]["headers"]
    assert transport._stage_http_context is None


@pytest.mark.unit
def test_browser_batch_uses_pool_and_recreates_session_after_rotation(
    monkeypatch, tmp_path
):
    first = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    second = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    transport, seen = _pool_transport(monkeypatch, tmp_path, first, second)
    fs = FakeFSClient(
        {"html": OK_HTML.decode(), "status": 200},
        [_batch_solution(TEAM_STATS_URL)],
        {"html": OK_HTML.decode(), "status": 200},
        [_batch_solution(TEAM_STATS_URL)],
    )
    transport._direct_fs = fs

    def request(key):
        return [
            FetchRequest(
                url=TEAM_STATS_URL,
                cache_key=key,
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
        ]

    transport.fetch_many(request("one"))
    assert transport._rotate_pool_proxy()
    transport.fetch_many(request("two"))
    assert [member for _, member in fs.created] == seen
    assert fs.created[0][0] != fs.created[1][0]
    assert fs.destroyed == [fs.created[0][0]]


@pytest.mark.unit
def test_cached_statistics_bootstrap_falls_back_to_pooled_browser_xhr():
    cache = KeyedMemoryRawCache()
    cache.payloads["bootstrap"] = CachedPayload(content=_statistics_html())
    direct = FakeHTTPSession(
        FakeHTTPResponse(
            status_code=403, content=CF_HTML, headers={"server": "cloudflare"}
        )
    )
    fs = FakeFSClient(
        {"html": _statistics_html().decode(), "status": 200},
        [_batch_solution(TEAM_STATS_URL)],
    )
    transport, _ = _transport(direct, direct_fs=fs, raw_cache=cache)
    transport._pool_proxy_url = "http://fixture.invalid:10000"
    prepared = transport.prepare_stage_statistics(
        TEAM_STATS_BOOTSTRAP, cache_key="bootstrap"
    )
    result = transport.fetch_many(
        [
            FetchRequest(
                url=TEAM_STATS_URL,
                cache_key="feed",
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                validator=lambda response: json.loads(response.content) is not None,
            )
        ]
    )
    assert prepared.route is TransportRoute.RAW_CACHE
    assert "Model-last-Mode" not in direct.calls[0][2]["headers"]
    assert result[0].route is TransportRoute.DIRECT_FLARESOLVERR
    assert fs.created[0][1] == transport._pool_proxy_url
    assert len(fs.xhr_many_calls) == 1


@pytest.mark.unit
def test_cached_statistics_bootstrap_preserves_binding_of_current_session():
    cache = KeyedMemoryRawCache()
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=_statistics_html()), FakeHTTPResponse(content=b"{}")
    )
    transport, _ = _transport(direct, raw_cache=cache)
    transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    assert (
        transport.prepare_stage_statistics(
            TEAM_STATS_BOOTSTRAP, cache_key="bootstrap"
        ).route
        is TransportRoute.RAW_CACHE
    )
    transport.fetch(TEAM_STATS_URL, browser_bootstrap_url=TEAM_STATS_BOOTSTRAP)
    assert direct.calls[-1][2]["headers"]["Model-last-Mode"] == STAGE_TOKEN


@pytest.mark.unit
def test_statistics_path_feed_uses_token_for_matching_stage_only():
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=_statistics_html()),
        FakeHTTPResponse(content=b"{}"),
        FakeHTTPResponse(content=b"{}"),
    )
    transport, _ = _transport(direct)
    transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    for stage in ("23752", "23753"):
        transport._http_fetch(
            f"https://www.whoscored.com/stagestatfeed/{stage}/stageteams/",
            session=direct,
            route=TransportRoute.DIRECT_HTTP,
            referer=TEAM_STATS_BOOTSTRAP,
        )
    assert direct.calls[1][2]["headers"]["Model-last-Mode"] == STAGE_TOKEN
    assert "Model-last-Mode" not in direct.calls[2][2]["headers"]


@pytest.mark.unit
def test_statistics_cached_source_absence_keeps_original_observation_time():
    from scrapers.whoscored.transport import StageStatisticsUnavailable

    observed_at = "2026-10-01T12:00:00+00:00"
    cache = KeyedMemoryRawCache()
    cache.payloads["bootstrap"] = CachedPayload(
        content=_statistics_html(""), observed_at=observed_at
    )
    direct = FakeHTTPSession()
    transport, _ = _transport(
        direct, raw_cache=cache, source_circuit=FakeSourceCircuit(opened=True)
    )
    with pytest.raises(StageStatisticsUnavailable) as caught:
        transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    assert caught.value.observed_at == observed_at
    assert caught.value.route is TransportRoute.RAW_CACHE
    assert not direct.calls


@pytest.mark.unit
@pytest.mark.parametrize("status_code", [301, 302, 303, 307, 308])
def test_statistics_bootstrap_redirect_cannot_bind_destination_token(status_code):
    from scrapers.whoscored.transport import StageBootstrapRedirect, StageStatisticsUnavailable

    direct = FakeHTTPSession(
        FakeHTTPResponse(
            status_code=status_code,
            content=_statistics_html(),
            headers={"lOcAtIoN": "https://user:password@www.whoscored.com/x;jsessionid=SECRET/y;token=TOKEN?q=PRIVATE#fragment"},
        )
    )
    transport, _ = _transport(direct)
    with pytest.raises(WhoScoredTransportError) as caught:
        transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    assert isinstance(caught.value, StageBootstrapRedirect)
    assert caught.value.status_code == status_code
    assert caught.value.redirect_location == "https://www.whoscored.com/x/y"
    assert caught.value.kind is FailureKind.HTTP_STATUS
    assert not isinstance(caught.value, StageStatisticsUnavailable)
    assert direct.calls[0][2]["allow_redirects"] is False
    assert transport._stage_http_context is None


def test_source_statistics_link_bootstraps_without_redirect_and_binds_xhr():
    # Shape observed in saved source HTML from the failed weekly run 12 Oct.
    bootstrap = (
        "https://www.whoscored.com/regions/252/tournaments/2/seasons/10743/"
        "stages/24533/teamstatistics/england-premier-league-2025-2026"
    )
    feed = TEAM_STATS_URL.replace("23752", "24533")
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=_statistics_html()), FakeHTTPResponse(content=b"{}")
    )
    transport, _ = _transport(direct)
    transport.prepare_stage_statistics(bootstrap, cache_key="bootstrap")
    response = transport.fetch(feed, browser_bootstrap_url=bootstrap)
    assert response.route is TransportRoute.DIRECT_HTTP
    assert direct.calls[0][0] == bootstrap
    assert direct.calls[0][2]["allow_redirects"] is False
    assert direct.calls[1][2]["headers"]["Model-last-Mode"] == STAGE_TOKEN
    assert direct.calls[1][2]["headers"]["Referer"] == bootstrap


@pytest.mark.parametrize(
    "foreign",
    [
        "http://www.whoscored.com/regions/252/tournaments/2/seasons/10743/stages/24533/teamstatistics/england",
        "https://other.example/regions/252/tournaments/2/seasons/10743/stages/24533/teamstatistics/england",
        "https://www.whoscored.com:444/regions/252/tournaments/2/seasons/10743/stages/24533/teamstatistics/england",
        "https://user:secret@www.whoscored.com/regions/252/tournaments/2/seasons/10743/stages/24533/teamstatistics/england",
        "/regions/253/tournaments/2/seasons/10743/stages/24533/teamstatistics/england",
        "/regions/252/tournaments/3/seasons/10743/stages/24533/teamstatistics/england",
        "/regions/252/tournaments/2/seasons/11141/stages/24533/teamstatistics/england",
        "/regions/252/tournaments/2/seasons/10743/stages/25544/teamstatistics/england",
        "/regions/252/tournaments/2/seasons/10743/stages/24533/playerstatistics/england",
        "/regions/252/tournaments/2/seasons/10743/stages/24533/teamstatistics/england?stageId=25544",
        "/regions/252/tournaments/2/seasons/10743/stages/24533/teamstatistics/england#other",
        "/regions/252/tournaments/2/seasons/10743/stages/24533/teamstatistics/../matches",
        "https://www.whoscored.com:invalid/path",
        "https://[malformed/path",
    ],
)
def test_source_statistics_link_ignores_foreign_or_malformed_navigation(foreign):
    canonical = (
        "/regions/252/tournaments/2/seasons/10743/stages/24533/"
        "teamstatistics/england-premier-league-2025-2026"
    )
    html = f'<a href="{foreign}">wrong</a><a href="{canonical}">Team Statistics</a>'
    ids = dict(region_id=252, tournament_id=2, season_id=10743, stage_id=24533)
    assert transport_module.stage_statistics_url_from_html(html, **ids) == (
        "https://www.whoscored.com" + canonical
    )
    assert transport_module.stage_statistics_url_from_html(
        f'<a href="{foreign}">wrong</a>', **ids
    ) is None


@pytest.mark.parametrize(
    "season,stage,slug",
    [(10743, 24533, "england-premier-league-2025-2026"),
     (11141, 25544, "england-premier-league-2026-2027")],
)
def test_source_statistics_link_from_saved_navigation(season, stage, slug):
    path = Path(__file__).resolve().parents[2] / "fixtures/whoscored/statistics_navigation_excerpt.html"
    url = transport_module.stage_statistics_url_from_html(
        path.read_text(), region_id=252, tournament_id=2, season_id=season, stage_id=stage
    )
    assert url == (
        f"https://www.whoscored.com/regions/252/tournaments/2/seasons/{season}/"
        f"stages/{stage}/teamstatistics/{slug}"
    )


@pytest.mark.parametrize("season,stage,year", [(10621, 24121, 2025), (10980, 25039, 2026)])
def test_source_unicode_statistics_link_becomes_exact_wire_uri(season, stage, year):
    source = (
        f"/regions/31/tournaments/95/seasons/{season}/stages/{stage}/"
        f"teamstatistics/brazil-brasileirão-{year}"
    )
    uri = "https://www.whoscored.com" + source.replace("ã", "%C3%A3")
    url = transport_module.stage_statistics_url_from_html(
        (Path(__file__).parents[2] / "fixtures/whoscored/statistics_navigation_excerpt.html").read_text(),
        region_id=31, tournament_id=95, season_id=season, stage_id=stage,
    )
    assert url == uri
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=_statistics_html()), FakeHTTPResponse(content=b"{}")
    )
    transport, _ = _transport(direct)
    transport.prepare_stage_statistics(url, cache_key="bootstrap")
    transport.fetch(TEAM_STATS_URL.replace("23752", str(stage)), browser_bootstrap_url=url)
    assert direct.calls[0][0] == uri
    assert direct.calls[0][2]["allow_redirects"] is False
    assert direct.calls[1][2]["headers"]["Referer"] == uri
    assert direct.calls[1][2]["headers"]["Model-last-Mode"] == STAGE_TOKEN


@pytest.mark.parametrize("slug", ["bad%C3", "bad%FF", "bad%2Fslug", "bad%252Fslug", "bad%2Eslug", "bad%00slug", "bad\\slug", "bad%3Fslug"])
def test_source_statistics_link_rejects_ambiguous_encoded_slug(slug):
    path = "/regions/31/tournaments/95/seasons/10980/stages/25039/teamstatistics/" + slug
    assert transport_module.stage_statistics_url_from_html(
        f'<a href="{path}">Team Statistics</a>',
        region_id=31, tournament_id=95, season_id=10980, stage_id=25039,
    ) is None


# --- #1478 review: cache age and browser navigation evidence ------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "url",
    [
        TEAM_STATS_URL,
        PLAYER_STATS_URL + "&currentPage=2",
        "https://www.whoscored.com/Matches/123/Live",
    ],
)
def test_cached_json_replay_preserves_original_observation_for_every_page(url):
    observed_at = "2026-10-01T12:00:00+00:00"
    cache = KeyedMemoryRawCache()
    cache.payloads["json"] = CachedPayload(
        content=b'{"teamTableStats":[]}', observed_at=observed_at
    )
    direct = FakeHTTPSession()
    transport, _ = _transport(direct, raw_cache=cache)
    result = transport.fetch(
        url,
        cache_key="json",
        validator=lambda response: json.loads(response.content) is not None,
    )
    assert result.observed_at == observed_at
    assert result.route is TransportRoute.RAW_CACHE
    assert not direct.calls


class _SourceURLFSApiSession:
    """Offline API envelope exercises shared get/_post and source adaptation."""

    def __init__(self, html, *, final_url=None, include_url=True):
        self.html = html
        self.final_url = final_url
        self.include_url = include_url
        self.posts = []

    def post(self, url, *, json, timeout):
        self.posts.append((url, dict(json), timeout))
        document = {"status": "ok"}
        if json["cmd"] == "request.get":
            document["solution"] = {
                "response": self.html.decode(),
                "status": 200,
                "cookies": [
                    {
                        "name": "source-session",
                        "value": "from-browser",
                        "domain": "www.whoscored.com",
                        "path": "/",
                    }
                ],
                "userAgent": "Chromium/142 fixture",
            }
            if self.include_url:
                document["solution"]["url"] = self.final_url
        content = transport_module.json.dumps(
            document, sort_keys=True, separators=(",", ":")
        ).encode()
        return types.SimpleNamespace(
            ok=True,
            status_code=200,
            text=content.decode(),
            content=content,
            json=lambda: document,
        )

    def close(self):
        pass


@pytest.mark.unit
@pytest.mark.parametrize("script", [None, ""])
def test_source_browser_url_adapter_accepts_exact_page_before_token_or_absence(script):
    from scrapers.whoscored.transport import StageStatisticsUnavailable

    api = _SourceURLFSApiSession(
        _statistics_html(script), final_url=TEAM_STATS_BOOTSTRAP
    )
    fs = transport_module.FlareSolverrClient(url="http://fixture.invalid:8191")
    fs.session = api
    direct = FakeHTTPSession(
        FakeHTTPResponse(
            status_code=403, content=CF_HTML, headers={"server": "cloudflare"}
        ),
        FakeHTTPResponse(content=b"{}"),
    )
    direct.cookies = requests.cookies.RequestsCookieJar()
    direct.headers = {}
    cache = KeyedMemoryRawCache()
    transport, _ = _transport(direct, direct_fs=fs, raw_cache=cache)
    if script == "":
        with pytest.raises(StageStatisticsUnavailable):
            transport.prepare_stage_statistics(
                TEAM_STATS_BOOTSTRAP, cache_key="bootstrap"
            )
    else:
        result = transport.prepare_stage_statistics(
            TEAM_STATS_BOOTSTRAP, cache_key="bootstrap"
        )
        assert result.route is TransportRoute.DIRECT_FLARESOLVERR
        transport.fetch(TEAM_STATS_URL, browser_bootstrap_url=TEAM_STATS_BOOTSTRAP)
        assert direct.calls[-1][2]["headers"]["Model-last-Mode"] == STAGE_TOKEN
    assert direct.cookies.get("source-session") == "from-browser"
    assert len(cache.stored) >= 1
    stats = fs.get_traffic_stats()
    assert stats["requests"] == 1
    assert stats["fs_response_bytes"] > 0
    assert stats["sessions_created"] == 1
    assert stats["top_traffic_urls"][0]["requests"] == 1
    assert api.posts[1][1]["disableMedia"] is True


@pytest.mark.unit
@pytest.mark.parametrize(
    "final_url,include_url",
    [
        (PLAYER_STATS_BOOTSTRAP, True),
        (TEAM_STATS_BOOTSTRAP.replace("www.whoscored.com", "external.invalid"), True),
        (None, False),
        (None, True),
    ],
)
@pytest.mark.parametrize("script", [None, ""])
def test_source_browser_url_adapter_rejects_wrong_or_missing_page_before_replay(
    final_url, include_url, script
):
    from scrapers.whoscored.transport import StageStatisticsUnavailable

    api = _SourceURLFSApiSession(
        _statistics_html(script), final_url=final_url, include_url=include_url
    )
    fs = transport_module.FlareSolverrClient(url="http://fixture.invalid:8191")
    fs.session = api
    direct = FakeHTTPSession(
        FakeHTTPResponse(
            status_code=403, content=CF_HTML, headers={"server": "cloudflare"}
        )
    )
    direct.cookies = requests.cookies.RequestsCookieJar()
    direct.headers = {}
    cache = KeyedMemoryRawCache()
    transport, _ = _transport(direct, direct_fs=fs, raw_cache=cache)
    with pytest.raises(WhoScoredTransportError) as caught:
        transport.prepare_stage_statistics(TEAM_STATS_BOOTSTRAP, cache_key="bootstrap")
    assert not isinstance(caught.value, StageStatisticsUnavailable)
    assert caught.value.kind is FailureKind.BROWSER
    assert not caught.value.retryable
    assert not direct.cookies and not direct.headers
    assert transport._stage_http_context is None
    assert not cache.stored
    assert [request[1]["cmd"] for request in api.posts] == [
        "sessions.create",
        "request.get",
        "sessions.destroy",
    ]
    assert fs.get_traffic_stats()["requests"] == 1
    assert transport.get_traffic_stats()["route_requests"]["direct_flaresolverr"] == 1


@pytest.mark.unit
@pytest.mark.parametrize("batch", [False, True])
def test_browser_statistics_feed_rejects_wrong_bootstrap_before_xhr(batch):
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        {
            "html": _statistics_html().decode(),
            "status": 200,
            "url": PLAYER_STATS_BOOTSTRAP,
        }
    )
    direct.cookies = requests.cookies.RequestsCookieJar()
    transport, _ = _transport(direct, direct_fs=fs)
    with pytest.raises(WhoScoredTransportError) as caught:
        if batch:
            transport.fetch_many(
                [
                    FetchRequest(
                        url=TEAM_STATS_URL,
                        cache_key="feed",
                        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                    )
                ]
            )
        else:
            transport.fetch(
                TEAM_STATS_URL,
                cache_key="feed",
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            )
    assert caught.value.kind is FailureKind.BROWSER
    assert not fs.xhr_calls and not fs.xhr_many_calls
    assert not direct.cookies
    assert len(fs.destroyed) == 1


@pytest.mark.unit
def test_source_browser_url_adapter_retains_common_runtime_authority():
    fs = transport_module.FlareSolverrClient(
        url="http://fixture.invalid:8191",
        expected_version="3.4.6",
        expected_extension_sha256="a" * 64,
    )
    api = _SourceURLFSApiSession(_statistics_html(), final_url=TEAM_STATS_BOOTSTRAP)
    api.get = lambda *args, **kwargs: types.SimpleNamespace(
        ok=True,
        json=lambda: {
            "status": "ok",
            "version": "unexpected",
            "extension_sha256": "a" * 64,
        },
    )
    fs.session = api
    with pytest.raises(FlareSolverrRuntimeIdentityError):
        fs.get(TEAM_STATS_BOOTSTRAP, "source-session")
    assert not api.posts
    assert fs.get_traffic_stats()["requests"] == 0


@pytest.mark.unit
def test_transport_source_browser_constructor_remains_patchable(monkeypatch):
    created = []

    def constructor(**kwargs):
        created.append(kwargs)
        return FakeFSClient()

    monkeypatch.setattr(transport_module, "FlareSolverrClient", constructor)
    transport = WhoScoredTransport(direct_http_session=FakeHTTPSession())
    assert isinstance(transport._direct_fs, FakeFSClient)
    assert len(created) == 1



@pytest.mark.unit
def test_history_workers_own_disjoint_pool_lines_through_rotation(monkeypatch, tmp_path):
    # #1481: worker i owns lines j with j % N == i; a rotation after a proxy
    # failure stays inside that subset and never takes a neighbour's address.
    pool = tmp_path / "pool.txt"
    pool.write_text("".join(f"pool.example.io:{10000 + i}:poolu:poolpw\n" for i in range(7)))
    monkeypatch.setenv("WHOSCORED_PROXY_FILE", str(pool))
    monkeypatch.setenv("WHOSCORED_TRANSPORT_POLICY", "direct_only")
    monkeypatch.setenv("WHOSCORED_PROXY_MEMBER", "5")

    def ports(worker):
        monkeypatch.setenv("WHOSCORED_PROXY_WORKER", worker)
        transport = WhoScoredTransport(
            direct_fs_client=FakeFSClient(),
            http_session_factory=lambda proxy_url: FakeHTTPSession(),
            context=TransportContext(transport_policy="direct_only"),
        )
        seen = [transport._pool_proxy_url]
        tried = set(seen)
        while transport._rotate_pool_proxy(tried):
            seen.append(transport._pool_proxy_url)
            tried.add(transport._pool_proxy_url)
        return [int(url.rsplit(":", 1)[1]) - 10000 for url in seen]

    owned = [ports(f"{i}/3") for i in range(3)]
    assert owned[0][0] == [0, 3, 6][5 % 3]
    assert [sorted(lines) for lines in owned] == [[0, 3, 6], [1, 4], [2, 5]]
