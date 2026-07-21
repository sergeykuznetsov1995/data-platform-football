from __future__ import annotations

import hashlib
import json
import sys
import time
import traceback
import types
from datetime import datetime

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
from scrapers.whoscored.runtime_contract import RuntimeContractError
from scrapers.whoscored.transport import (
    CachedPayload,
    CloudflareChallenge,
    FailureKind,
    FetchRequest,
    JsonlRequestLedger,
    PaidGatewayBatchItem,
    PaidGatewayBatchReceipt,
    PaidGatewayBatchResponse,
    PaidGatewayError,
    PaidGatewayProtocolError,
    PaidGatewayReceipt,
    PaidGatewayRejected,
    PaidGatewayResponse,
    ProxyBudgetRejected,
    ProxyFilterClient,
    ProxyLease,
    TransportBudgets,
    TransportContext,
    TransportPolicy,
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
CONTROL_TOKEN = "c" * 32
_REAL_PAID_RUNTIME_AUTHORITY = transport_module.assert_paid_runtime_available
_REAL_PAID_ALERT_AUTHORITY = transport_module.assert_paid_alert_runtime_available


@pytest.fixture(autouse=True)
def _stub_paid_runtime_authority(monkeypatch):
    """Route-mechanics tests fake the paid authority below its own boundary."""

    monkeypatch.setattr(
        transport_module,
        "assert_paid_runtime_available",
        lambda _metadata: None,
    )
    monkeypatch.setattr(
        transport_module,
        "assert_paid_alert_runtime_available",
        lambda _context: None,
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

    def destroy_session_strict(self, session_id):
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
            "provider_billed_bytes": self.up + self.down,
            "canonical_url": self.created[-1][3],
            "close_complete": True,
        }
        if self.stats_override is not None:
            report.update(self.stats_override)
        return report

    def close_session(self):
        self.session_closed = True


class LegacyRouteGatewayAdapter:
    """Exercise old route fixtures through the new high-level gateway seam.

    Proxy/browser capabilities remain inside this test-only adapter.  The
    production transport sees only ``fetch`` and a credential-free receipt.
    """

    def __init__(self, *, proxy, paid_http, paid_fs, session_factory):
        self.proxy = proxy
        self.paid_http = paid_http
        self.paid_fs = paid_fs
        self.session_factory = session_factory
        self.closed = False

    @staticmethod
    def _receipt(context, url, lease, stats, route):
        def counter(field):
            value = stats.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise PaidGatewayProtocolError("invalid accounting")
            return value

        if stats.get("id") != lease.lease_id:
            raise PaidGatewayProtocolError("invalid accounting")
        reported_url = stats.get("canonical_url")
        canonical = transport_module._canonical_url_key(url)
        if (
            not isinstance(reported_url, str)
            or transport_module._canonical_url_key(reported_url) != canonical
            or stats.get("close_complete") is not True
        ):
            raise PaidGatewayProtocolError("invalid accounting")
        up = counter("up_bytes")
        down = counter("down_bytes")
        total = counter("total_bytes")
        billed = counter("provider_billed_bytes")
        if total != up + down or billed != total:
            raise PaidGatewayProtocolError("invalid accounting")
        campaign = context.proxy_campaign
        return PaidGatewayReceipt(
            campaign_id=str(campaign.get("proxy_campaign_id") or "test-campaign"),
            approval_id=str(campaign.get("proxy_approval_id") or "test-approval"),
            approval_sha256=str(
                campaign.get("proxy_approval_sha256") or "a" * 64
            ),
            allocation_id=str(
                campaign.get("proxy_allocation_id") or "test-allocation"
            ),
            attempt_id_hash="b" * 64,
            canonical_url_sha256=hashlib.sha256(canonical.encode()).hexdigest(),
            lease_id_hash=hashlib.sha256(lease.lease_id.encode()).hexdigest(),
            route=route,
            up_bytes=up,
            down_bytes=down,
            total_bytes=total,
            provider_billed_bytes=billed,
            close_complete=True,
            cleanup_complete=True,
        )

    def fetch(
        self,
        url,
        *,
        context,
        max_response_bytes,
        max_provider_bytes,
        timeout_ms,
        browser_bootstrap_url=None,
    ):
        try:
            lease = self.proxy.create_lease(
                max_bytes=max_provider_bytes,
                ttl_seconds=60,
                context=context,
                canonical_url=transport_module._canonical_url_key(url),
            )
        except ProxyBudgetRejected:
            raise PaidGatewayRejected("budget_rejected") from None
        except Exception as exc:
            raise PaidGatewayError(type(exc).__name__) from None
        paid_http = None
        route = TransportRoute.PAID_HTTP
        content = b""
        status = 0
        headers = {}
        session_id = "ws-test-paid"
        browser_attempted = False
        pending = None
        stats = None
        try:
            paid_http = self.session_factory(lease.proxy_url)
            raw = paid_http.get(
                url,
                timeout=timeout_ms / 1000,
                headers=(
                    {"Referer": browser_bootstrap_url}
                    if browser_bootstrap_url
                    else {}
                ),
                allow_redirects=False,
            )
            content = bytes(raw.content or b"")
            status = int(raw.status_code)
            headers = dict(raw.headers or {})
            challenge = transport_module.is_cloudflare_response(
                status, headers, content
            ) or transport_module.is_whoscored_structured_feed_access_gate(
                url, status, content, headers
            )
            if challenge:
                route = TransportRoute.PAID_FLARESOLVERR
                browser_attempted = True
                self.paid_fs.create_session(session_id, proxy_url=lease.proxy_url)
                if browser_bootstrap_url:
                    self.paid_fs.get(
                        browser_bootstrap_url,
                        session_id,
                        max_timeout_ms=timeout_ms,
                        disable_media=True,
                    )
                    solution = self.paid_fs.xhr_get(
                        url, session_id, max_timeout_ms=timeout_ms
                    )
                    content = bytes(solution.get("content") or b"")
                    status = int(solution.get("status") or 0)
                    headers = dict(solution.get("headers") or {})
                else:
                    solution = self.paid_fs.get(
                        url,
                        session_id,
                        max_timeout_ms=timeout_ms,
                        disable_media=True,
                    )
                    content = str(solution.get("html") or "").encode()
                    status = int(solution.get("status") or 0)
                    headers = {}
            if len(content) > max_response_bytes:
                raise PaidGatewayProtocolError("response too large")
        except Exception as exc:
            pending = exc
        finally:
            close = getattr(paid_http, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:
                    pending = pending or exc
            if browser_attempted:
                try:
                    self.paid_fs.destroy_session_strict(session_id)
                except Exception as exc:
                    pending = pending or exc
            try:
                stats = self.proxy.close(lease)
            except Exception as exc:
                pending = PaidGatewayProtocolError(type(exc).__name__)
        if stats is None:
            raise PaidGatewayProtocolError("missing accounting")
        receipt = self._receipt(context, url, lease, stats, route)
        if pending is not None:
            if isinstance(pending, (PaidGatewayError, PaidGatewayProtocolError)):
                raise pending
            raise PaidGatewayError(type(pending).__name__) from None
        return PaidGatewayResponse(
            url=url,
            content=content,
            status_code=status,
            headers=headers,
            route=route,
            receipt=receipt,
        )

    def close(self):
        self.closed = True
        fs_close = getattr(self.paid_fs, "close", None)
        if callable(fs_close):
            fs_close()
        proxy_close = getattr(self.proxy, "close_session", None)
        if callable(proxy_close):
            proxy_close()


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
    paid_fs=None,
    proxy=None,
    paid_http=None,
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
    transport_policy=None,
):
    factory_calls = []
    resolved_policy = TransportPolicy.parse(
        transport_policy
        or (TransportPolicy.DIRECT_THEN_PAID if proxy else TransportPolicy.DIRECT_ONLY)
    )

    def factory(proxy_url):
        factory_calls.append(proxy_url)
        assert paid_http is not None
        return paid_http

    paid_gateway = (
        LegacyRouteGatewayAdapter(
            proxy=proxy,
            paid_http=paid_http,
            paid_fs=paid_fs or FakeFSClient(),
            session_factory=factory,
        )
        if proxy
        else None
    )

    transport = WhoScoredTransport(
        direct_http_session=direct_http,
        direct_fs_client=direct_fs or FakeFSClient(),
        paid_fs_client=(paid_fs or FakeFSClient()) if proxy is None else None,
        paid_gateway_client=paid_gateway,
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
        context=TransportContext(transport_policy=resolved_policy.value),
        transport_policy=resolved_policy,
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
def test_paid_batch_gate_is_off_by_default_and_requires_exact_boolean_env(monkeypatch):
    monkeypatch.delenv("WHOSCORED_PAID_BATCH_ENABLED", raising=False)
    transport, _factory_calls = _transport(
        FakeHTTPSession(FakeHTTPResponse()),
        transport_policy=TransportPolicy.DIRECT_ONLY,
    )
    try:
        assert transport._paid_batch_enabled is False
    finally:
        transport.close()

    monkeypatch.setenv("WHOSCORED_PAID_BATCH_ENABLED", "yes")
    with pytest.raises(ValueError, match="must be 0 or 1"):
        _transport(
            FakeHTTPSession(FakeHTTPResponse()),
            transport_policy=TransportPolicy.DIRECT_ONLY,
        )


@pytest.mark.unit
def test_paid_batch_records_one_hashed_aggregate_receipt_event():
    second_url = TEAM_STATS_URL + "&category=offensive"
    urls = (TEAM_STATS_URL, second_url)
    endpoint_bytes = {
        hashlib.sha256(
            transport_module._canonical_url_key(url).encode("utf-8")
        ).hexdigest(): amount
        for url, amount in zip(urls, (300, 600))
    }
    manifest = transport_module._paid_gateway_target_manifest_sha256(
        urls, browser_bootstrap_url=TEAM_STATS_BOOTSTRAP
    )
    receipt = PaidGatewayBatchReceipt(
        campaign_id="campaign-one",
        approval_id="approval-one",
        approval_sha256="a" * 64,
        allocation_id="allocation-one",
        attempt_id_hash="b" * 64,
        target_manifest_sha256=manifest,
        lease_id_hash="d" * 64,
        route=TransportRoute.PAID_FLARESOLVERR,
        up_bytes=50,
        down_bytes=950,
        total_bytes=1_000,
        provider_billed_bytes=1_000,
        bootstrap_provider_billed_bytes=100,
        endpoint_provider_billed_bytes=endpoint_bytes,
        close_complete=True,
        cleanup_complete=True,
    )

    class BatchGateway:
        def __init__(self):
            self.calls = []

        def fetch_batch(self, requested_urls, **kwargs):
            self.calls.append((tuple(requested_urls), kwargs))
            return PaidGatewayBatchResponse(
                target_manifest_sha256=manifest,
                results=tuple(
                    PaidGatewayBatchItem(
                        url=url,
                        content=b'{"teamTableStats":[]}',
                        status_code=200,
                        headers={"content-type": "application/json"},
                    )
                    for url in urls
                ),
                route=TransportRoute.PAID_FLARESOLVERR,
                receipt=receipt,
            )

    def cf_solution(url):
        return {
            "ok": True,
            "content": CF_HTML,
            "headers": {"server": "cloudflare", "cf-ray": "blocked"},
            "status": 403,
            "responseBytes": len(CF_HTML),
            "finalUrl": url,
        }

    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=MASKED_STATS_HTML),
    )
    direct_fs = FakeFSClient(
        {"html": "<html>Team Statistics</html>", "status": 200},
        [cf_solution(url) for url in urls],
    )
    gateway = BatchGateway()
    ledger = MemoryLedger()
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=direct_fs,
        paid_gateway_client=gateway,
        direct_http_attempts=1,
        direct_browser_attempts=1,
        browser_retry_backoff_seconds=0,
        paid_batch_enabled=True,
        context=TransportContext(transport_policy="direct_then_paid"),
        transport_policy=TransportPolicy.DIRECT_THEN_PAID,
        request_ledger=ledger,
    )

    results = transport.fetch_many(
        [
            FetchRequest(
                url=url,
                cache_key=f"paid-batch-{index}",
                browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                validator=lambda response: json.loads(response.content) is not None,
            )
            for index, url in enumerate(urls)
        ]
    )

    assert [result.route for result in results] == [
        TransportRoute.PAID_FLARESOLVERR,
        TransportRoute.PAID_FLARESOLVERR,
    ]
    assert [result.resource_bytes for result in results] == [400, 600]
    assert len(gateway.calls) == 1
    assert gateway.calls[0][0] == urls
    accounted = [event for event in ledger.events if event["status"] == "accounted"]
    assert len(accounted) == 1
    assert accounted[0]["lease_id_hash"] == "d" * 64
    assert "lease_id" not in accounted[0]
    assert accounted[0]["request_bytes"] == 50
    assert accounted[0]["response_bytes"] == 950
    assert accounted[0]["paid_proxy_bytes"] == 1_000
    assert accounted[0]["gateway_target_manifest_sha256"] == manifest
    assert accounted[0]["gateway_endpoint_provider_bytes"] == endpoint_bytes
    assert accounted[0]["gateway_bootstrap_provider_bytes"] == 100
    assert transport.get_traffic_stats()["paid_proxy_bytes"] == 1_000


@pytest.mark.unit
def test_internal_flaresolverr_clients_bind_attested_runtime_hash(monkeypatch):
    expected_hash = "a" * 64
    monkeypatch.setattr(
        transport_module,
        "attested_runtime_file_sha256",
        lambda relative: (
            expected_hash
            if relative == "scripts/flaresolverr_extended.py"
            else pytest.fail("unexpected runtime identity path")
        ),
    )

    transport = WhoScoredTransport(
        direct_http_session=FakeHTTPSession(),
    )

    assert transport._direct_fs._expected_version == "3.4.6"
    assert transport._direct_fs._expected_extension_sha256 == expected_hash
    assert transport._paid_fs._expected_version == "3.4.6"
    assert transport._paid_fs._expected_extension_sha256 == expected_hash


@pytest.mark.unit
def test_production_marker_cannot_lose_flaresolverr_attested_hash(monkeypatch):
    def unavailable(_relative):
        raise RuntimeContractError("guard missing")

    monkeypatch.setattr(
        transport_module,
        "attested_runtime_file_sha256",
        unavailable,
    )
    monkeypatch.setattr(
        transport_module,
        "require_production_runtime_class",
        lambda *, operation: "production-v1",
    )
    monkeypatch.setattr(
        transport_module.sys,
        "_whoscored_runtime_class",
        "production-v1",
        raising=False,
    )
    direct = FakeHTTPSession()

    with pytest.raises(RuntimeContractError, match="no attested FlareSolverr"):
        WhoScoredTransport(direct_http_session=direct)

    assert direct.calls == []


@pytest.mark.unit
def test_proxy_endpoint_without_explicit_policy_cannot_authorize_paid_traffic():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "direct"}))
    direct_fs = FakeFSClient(FlareSolverrCFChallengeFailed("browser blocked"))
    proxy = FakeProxyClient()
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=direct_fs,
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
        paid_proxy_url="http://proxy_filter:8899",
        direct_browser_attempts=1,
        browser_retry_backoff_seconds=0,
    )

    with pytest.raises(CloudflareChallenge):
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert transport.transport_policy is TransportPolicy.DIRECT_ONLY
    assert proxy.created == []


@pytest.mark.unit
def test_explicit_paid_policy_cannot_override_direct_context():
    with pytest.raises(ValueError, match="authenticated TransportContext"):
        WhoScoredTransport(
            direct_http_session=FakeHTTPSession(),
            direct_fs_client=FakeFSClient(),
            paid_fs_client=FakeFSClient(),
            proxy_client=FakeProxyClient(),
            paid_proxy_url="http://proxy_filter:8899",
            context=TransportContext(),
            transport_policy=TransportPolicy.DIRECT_THEN_PAID,
        )


@pytest.mark.unit
def test_forged_paid_context_reaches_the_real_authority_before_clients(
    monkeypatch,
):
    monkeypatch.setattr(
        transport_module,
        "assert_paid_runtime_available",
        _REAL_PAID_RUNTIME_AUTHORITY,
    )
    with pytest.raises(ValueError, match="proxy campaign approval"):
        WhoScoredTransport(
            direct_http_session=FakeHTTPSession(),
            direct_fs_client=FakeFSClient(),
            paid_fs_client=FakeFSClient(),
            proxy_client=FakeProxyClient(),
            paid_proxy_url="http://proxy_filter:8899",
            context=TransportContext(transport_policy="direct_then_paid"),
        )


@pytest.mark.unit
def test_runner_alert_check_requires_no_receipt_environment(monkeypatch):
    from dags.utils.alerts import PAID_ALERT_RECEIPT_ENV

    for name in PAID_ALERT_RECEIPT_ENV.values():
        monkeypatch.delenv(name, raising=False)
    context = TransportContext(
        dag_id="dag_ingest_whoscored",
        run_id="manual__paid-1",
        task_id="ingest_active_scope",
        map_index=0,
        try_number=1,
        transport_policy="direct_then_paid",
        proxy_campaign={
            "proxy_campaign_id": "campaign-1",
            "proxy_approval_id": "approval-1",
            "proxy_approval_sha256": "a" * 64,
        },
    )

    _REAL_PAID_ALERT_AUTHORITY(context)


@pytest.mark.unit
def test_signed_allocation_replaces_local_cardinality_heuristic_limits():
    context = TransportContext(
        transport_policy="direct_then_paid",
        proxy_campaign={
            "proxy_allocation": {
                "budget_bytes": 123_456_789,
                "request_limit": 321,
                "lease_limit": 123,
            }
        },
    )
    transport = WhoScoredTransport(
        direct_http_session=FakeHTTPSession(FakeHTTPResponse()),
        direct_fs_client=FakeFSClient(),
        paid_gateway_client=types.SimpleNamespace(fetch=pytest.fail),
        context=context,
    )

    assert transport.budgets.max_paid_bytes_per_task == 123_456_789
    assert transport.budgets.max_paid_urls == 321
    assert transport.budgets.max_paid_browser_bootstraps == 123


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
def test_production_direct_http_defaults_space_two_502s_before_success(monkeypatch):
    direct = FakeHTTPSession(
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(content=OK_HTML),
    )
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []


@pytest.mark.unit
def test_production_direct_http_defaults_fail_closed_after_three_502s(monkeypatch):
    direct = FakeHTTPSession(
        *[
            FakeHTTPResponse(status_code=502, content=b"temporary upstream error")
            for _ in range(3)
        ]
    )
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []
    assert transport.get_traffic_stats().get("paid_route_requests", 0) == 0


@pytest.mark.unit
def test_production_direct_http_timeout_waits_and_reacquires_token(monkeypatch):
    direct = FakeHTTPSession(
        requests.exceptions.Timeout("temporary timeout"),
        FakeHTTPResponse(content=OK_HTML),
    )
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []


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
        paid_fs_client=FakeFSClient(),
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
            paid_fs_client=FakeFSClient(),
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
            paid_fs_client=FakeFSClient(),
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
            paid_fs_client=FakeFSClient(),
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
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}')),
        attempts=4,
        source_circuit=circuit,
        transport_policy=TransportPolicy.DIRECT_ONLY,
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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}')),
        attempts=4,
        source_circuit=circuit,
        transport_policy=TransportPolicy.DIRECT_ONLY,
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
    assert proxy.created == []


@pytest.mark.unit
def test_serial_mixed_browser_evidence_never_authorizes_paid_with_source_circuit():
    bootstrap = {
        "html": "<html><body>Team Statistics</body></html>",
        "status": 200,
    }
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        bootstrap,
        FlareSolverrTimeout("browser timed out"),
        bootstrap,
        FlareSolverrCFChallengeFailed("later browser cf"),
    )
    circuit = FakeSourceCircuit()
    proxy = FakeProxyClient()
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}')),
        attempts=2,
        source_circuit=circuit,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            validator=lambda response: json.loads(response.content) is not None,
        )

    assert raised.value.kind is FailureKind.TIMEOUT
    assert len(direct.calls) == 1
    assert len(fs.xhr_calls) == 2
    assert circuit.calls == [("admit", False), ("trip", False)]
    assert proxy.created == []
    assert factory_calls == []


@pytest.mark.unit
@pytest.mark.parametrize("batched", [False, True])
def test_structured_bootstrap_typed_cf_is_authoritative_source_evidence(batched):
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(FlareSolverrCFChallengeFailed("cloudflare blocked"))
    circuit = FakeSourceCircuit()
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        attempts=4,
        source_circuit=circuit,
        transport_policy=TransportPolicy.DIRECT_ONLY,
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
    assert proxy.created == []


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
def test_open_source_circuit_never_substitutes_for_current_browser_evidence():
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh-direct"}))
    direct_fs = FakeFSClient()
    paid_http = FakeHTTPSession(FakeHTTPResponse(content=b'{"ok":true}'))
    proxy = FakeProxyClient(up=40, down=60)
    circuit = FakeSourceCircuit(opened=True)
    transport, factory_calls = _transport(
        direct,
        direct_fs=direct_fs,
        proxy=proxy,
        paid_http=paid_http,
        source_circuit=circuit,
        attempts=4,
    )

    with pytest.raises(CloudflareChallenge):
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
            validator=lambda response: json.loads(response.content) is not None,
        )

    assert len(direct.calls) == 1
    assert direct_fs.created == []
    assert circuit.calls == [("admit", False)]
    assert proxy.created == []
    assert factory_calls == []


@pytest.mark.unit
def test_open_source_circuit_timeout_never_authorizes_paid_route():
    direct = FakeHTTPSession(requests.Timeout("direct timeout"))
    proxy = FakeProxyClient()
    circuit = FakeSourceCircuit(opened=True)
    transport, _ = _transport(
        direct,
        direct_fs=FakeFSClient(),
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b'{"ok":true}')),
        source_circuit=circuit,
        http_attempts=1,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )

    assert raised.value.kind is FailureKind.TIMEOUT
    assert proxy.created == []


@pytest.mark.unit
def test_open_source_circuit_batch_never_substitutes_for_browser_evidence():
    second_url = f"{TEAM_STATS_URL}&page=2"
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh-one"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh-two"}),
    )
    direct_fs = FakeFSClient()
    paid_http = FakeHTTPSession(
        FakeHTTPResponse(content=b'{"teamTableStats":[]}'),
        FakeHTTPResponse(content=b'{"teamTableStats":[]}'),
    )
    proxy = FakeProxyClient(up=20, down=30)
    circuit = FakeSourceCircuit(opened=True)
    transport, _ = _transport(
        direct,
        direct_fs=direct_fs,
        proxy=proxy,
        paid_http=paid_http,
        source_circuit=circuit,
        attempts=4,
    )

    with pytest.raises(CloudflareChallenge):
        transport.fetch_many(
            [
                FetchRequest(
                    url=url,
                    cache_key=f"open-circuit-{index}",
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                    validator=lambda response: json.loads(response.content) is not None,
                )
                for index, url in enumerate((TEAM_STATS_URL, second_url))
            ]
        )

    assert len(direct.calls) == 1
    assert direct_fs.created == []
    assert proxy.created == []
    assert circuit.calls == [("admit", False)]


@pytest.mark.unit
def test_half_open_source_circuit_runs_one_browser_probe_before_paid_route():
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh-recheck"}),
    )
    direct_fs = FakeFSClient(
        *(FlareSolverrCFChallengeFailed("source blocked") for _ in range(4))
    )
    proxy = FakeProxyClient()
    circuit = FakeSourceCircuit(probe=True)
    transport, _ = _transport(
        direct,
        direct_fs=direct_fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b'{"ok":true}')),
        source_circuit=circuit,
        attempts=4,
    )

    result = transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert result.route is TransportRoute.PAID_HTTP
    assert len(direct_fs.get_calls) == 4
    assert circuit.calls == [
        ("admit", False),
        ("trip", True),
        ("admit", False),
    ]
    assert len(proxy.created) == 1


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
    proxy = FakeProxyClient()
    tokens = []
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        attempts=4,
        source_circuit=circuit,
        transport_policy=TransportPolicy.DIRECT_ONLY,
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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}')),
        attempts=4,
        source_circuit=circuit,
        transport_policy=TransportPolicy.DIRECT_ONLY,
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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        attempts=4,
        source_circuit=circuit,
        transport_policy=TransportPolicy.DIRECT_ONLY,
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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
    transport, _ = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
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
    assert proxy.created == []


@pytest.mark.unit
def test_structured_batch_direct_probe_spaces_two_502s_before_success(monkeypatch):
    direct = FakeHTTPSession(
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(status_code=502, content=b"temporary upstream error"),
        FakeHTTPResponse(content=b'{"teamTableStats":[]}'),
    )
    fs = FakeFSClient()
    proxy = FakeProxyClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []


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
        paid_fs_client=FakeFSClient(),
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
    proxy = FakeProxyClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []
    assert transport.get_traffic_stats().get("paid_route_requests", 0) == 0


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
    proxy = FakeProxyClient()
    cache = KeyedMemoryRawCache()
    tokens = []
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
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
    assert proxy.created == []
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
    proxy = FakeProxyClient()
    tokens = []
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
    assert proxy.created == []
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
def test_batch_prepaid_recheck_spaces_502s_then_fails_without_paid(monkeypatch):
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        *[
            FakeHTTPResponse(status_code=502, content=b"temporary upstream error")
            for _ in range(3)
        ],
    )
    cf_solution = {
        "ok": True,
        "content": CF_HTML,
        "headers": {"server": "cloudflare", "cf-ray": "abc"},
        "status": 403,
        "responseBytes": len(CF_HTML),
        "finalUrl": TEAM_STATS_URL,
    }
    fs = FakeFSClient(
        {"html": "<html><body>Team Statistics</body></html>", "status": 200},
        [cf_solution],
    )
    proxy = FakeProxyClient()
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        paid_http=FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}')),
        attempts=1,
        http_retry_backoff=2.0,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch_many(
            [
                FetchRequest(
                    url=TEAM_STATS_URL,
                    cache_key="prepaid-recheck-502",
                    validator=lambda response: json.loads(response.content) is not None,
                    before_network=lambda: tokens.append("token"),
                    browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
                )
            ]
        )

    assert raised.value.kind is FailureKind.HTTP_STATUS
    assert raised.value.status_code == 502
    assert len(direct.calls) == 4
    assert len(fs.xhr_many_calls) == 1
    assert sleeps == [2.0, 4.0]
    assert tokens == ["token"] * 4
    assert proxy.created == []
    assert factory_calls == []
    assert transport.get_traffic_stats().get("paid_route_requests", 0) == 0


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
def test_paid_structured_access_gate_advances_same_lease_to_paid_browser():
    bootstrap = {
        "html": "<html><body>Team Statistics</body></html>",
        "status": 200,
    }
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=MASKED_STATS_HTML),
    )
    direct_fs = FakeFSClient(
        bootstrap,
        FlareSolverrCFChallengeFailed("direct browser cf"),
    )
    paid_http = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    paid_fs = FakeFSClient(bootstrap, _batch_solution(TEAM_STATS_URL))
    proxy = FakeProxyClient()
    transport, factory_calls = _transport(
        direct,
        direct_fs=direct_fs,
        paid_fs=paid_fs,
        proxy=proxy,
        paid_http=paid_http,
        attempts=1,
    )

    result = transport.fetch(
        TEAM_STATS_URL,
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        validator=lambda response: json.loads(response.content) is not None,
    )

    assert result.route is TransportRoute.PAID_FLARESOLVERR
    assert len(direct.calls) == 2
    assert len(proxy.created) == 1
    assert factory_calls == ["http://lease:secret@proxy_filter:8899"]
    assert len(paid_http.calls) == 1
    assert paid_http.calls[0][2]["allow_redirects"] is False
    assert paid_fs.created[0][1] == "http://lease:secret@proxy_filter:8899"
    assert len(paid_fs.get_calls) == 1
    assert len(paid_fs.xhr_calls) == 1


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
            paid_fs_client=FakeFSClient(),
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
    proxy = FakeProxyClient()
    tokens = []
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy, attempts=2)

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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
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
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
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
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
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
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []
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
    proxy = FakeProxyClient()
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
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []


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
    proxy = FakeProxyClient()
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
        paid_fs_client=FakeFSClient(),
        proxy_client=proxy,
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
    assert proxy.created == []


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
        paid_fs_client=FakeFSClient(),
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
def test_production_four_cf_challenges_require_fresh_direct_cf_before_paid(
    monkeypatch,
):
    direct = FakeHTTPSession(
        FakeHTTPResponse(content=MASKED_STATS_HTML),
        FakeHTTPResponse(content=MASKED_STATS_HTML),
    )
    fs = FakeFSClient(
        *[FlareSolverrCFChallengeFailed("cloudflare challenge") for _ in range(4)]
    )
    proxy = FakeProxyClient()
    paid_http = FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}'))
    factory_calls = []
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    monkeypatch.setattr(
        "scrapers.whoscored.transport.random.uniform", lambda _low, _high: 0.0
    )

    def factory(proxy_url):
        factory_calls.append(proxy_url)
        return paid_http

    paid_gateway = LegacyRouteGatewayAdapter(
        proxy=proxy,
        paid_http=paid_http,
        paid_fs=FakeFSClient(),
        session_factory=factory,
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
        paid_gateway_client=paid_gateway,
        context=TransportContext(transport_policy="direct_then_paid"),
        transport_policy=TransportPolicy.DIRECT_THEN_PAID,
        http_session_factory=factory,
    )

    result = transport.fetch(
        TEAM_STATS_URL,
        validator=lambda response: json.loads(response.content) is not None,
        before_network=lambda: tokens.append("token"),
        browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
    )

    assert result.route is TransportRoute.PAID_HTTP
    assert len(fs.get_calls) == 4
    assert len(direct.calls) == 2
    assert sleeps == [2.0, 4.0, 8.0]
    assert tokens == ["token"] * 5
    assert len(proxy.created) == 1
    assert factory_calls == ["http://lease:secret@proxy_filter:8899"]


@pytest.mark.unit
def test_three_cf_then_browser_error_on_fourth_attempt_never_enables_paid(
    monkeypatch,
):
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cloudflare challenge"),
        FlareSolverrCFChallengeFailed("cloudflare challenge"),
        FlareSolverrCFChallengeFailed("cloudflare challenge"),
        FlareSolverrError("ordinary browser failure"),
    )
    proxy = FakeProxyClient()
    factory_calls = []
    sleeps = []
    tokens = []
    monkeypatch.setattr(
        "scrapers.whoscored.transport.time.sleep", lambda delay: sleeps.append(delay)
    )
    monkeypatch.setattr(
        "scrapers.whoscored.transport.random.uniform", lambda _low, _high: 0.0
    )

    def factory(proxy_url):
        factory_calls.append(proxy_url)
        return FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}'))

    paid_gateway = LegacyRouteGatewayAdapter(
        proxy=proxy,
        paid_http=None,
        paid_fs=FakeFSClient(),
        session_factory=factory,
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=fs,
        paid_gateway_client=paid_gateway,
        context=TransportContext(transport_policy="direct_then_paid"),
        transport_policy=TransportPolicy.DIRECT_THEN_PAID,
        http_session_factory=factory,
    )

    with pytest.raises(WhoScoredTransportError) as raised:
        transport.fetch(
            TEAM_STATS_URL,
            before_network=lambda: tokens.append("token"),
            browser_bootstrap_url=TEAM_STATS_BOOTSTRAP,
        )

    assert raised.value.kind is FailureKind.BROWSER
    assert len(fs.get_calls) == 4
    assert sleeps == [2.0, 4.0, 8.0]
    assert tokens == ["token"] * 4
    assert proxy.created == []
    assert factory_calls == []


@pytest.mark.unit
def test_stale_flaresolverr_identity_is_config_failure_before_cache_or_paid():
    direct = FakeHTTPSession(FakeHTTPResponse(content=MASKED_STATS_HTML))
    runtime_error = FlareSolverrRuntimeIdentityError(
        "FlareSolverr runtime identity does not match the attested extension"
    )
    fs = FakeFSClient(runtime_error)
    proxy = FakeProxyClient()
    cache = MemoryRawCache()
    paid_http = FakeHTTPSession(FakeHTTPResponse(content=b'{"paid":true}'))
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
        paid_http=paid_http,
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
    assert proxy.created == []
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
            paid_fs_client=FakeFSClient(),
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
    proxy = FakeProxyClient()
    tokens = []
    transport, factory_calls = _transport(
        direct,
        direct_fs=fs,
        proxy=proxy,
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
    assert proxy.created == []
    assert factory_calls == []


@pytest.mark.unit
def test_direct_browser_soft_origin_502_exhaustion_never_enables_paid_proxy():
    soft_502 = b"<html><title>whoscored.com | 502: Bad gateway</title></html>"
    direct = FakeHTTPSession(FakeHTTPResponse(403, CF_HTML, {"cf-ray": "x"}))
    fs = FakeFSClient(
        {"html": soft_502.decode(), "status": 200},
        {"html": soft_502.decode(), "status": 200},
    )
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy, attempts=2)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.HTTP_STATUS
    assert exc.value.status_code == 502
    assert exc.value.retryable is True
    assert len(fs.get_calls) == 2
    assert proxy.created == []


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
    assert len(fs.created) == 2
    assert fs.destroyed == [created[0] for created in fs.created]
    assert transport.get_traffic_stats()["browser_sessions"] == 0
    assert proxy.created == []


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
    fs = FakeFSClient(
        FlareSolverrTimeout("down"),
        FlareSolverrTimeout("still down"),
    )
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=fs, proxy=proxy)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.TIMEOUT
    assert len(fs.get_calls) == 2
    assert len(fs.created) == 2
    assert len(fs.destroyed) == 2
    assert proxy.created == []


@pytest.mark.unit
def test_fresh_direct_recheck_recovery_blocks_paid_page_fallback():
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        FakeHTTPResponse(200, OK_HTML),
    )
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=direct_fs, proxy=proxy)

    result = transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert result.route is TransportRoute.DIRECT_HTTP
    assert len(direct.calls) == 2
    assert len(direct_fs.created) == 2
    assert proxy.created == []


@pytest.mark.unit
def test_fresh_direct_recheck_timeout_blocks_paid_page_fallback():
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        requests.Timeout("recheck one"),
        requests.Timeout("recheck two"),
        requests.Timeout("recheck three"),
    )
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    proxy = FakeProxyClient()
    transport, _ = _transport(direct, direct_fs=direct_fs, proxy=proxy)

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.TIMEOUT
    assert len(direct.calls) == 4
    assert len(direct_fs.created) == 2
    assert proxy.created == []


@pytest.mark.unit
def test_paid_curl_only_after_fresh_direct_recheck_is_cf():
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
    )
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
    assert len(direct.calls) == 2
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
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
    )
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    proxy = FakeProxyClient(up=0, down=0)

    def failing_factory(_proxy_url):
        raise RuntimeError("session unavailable")

    paid_gateway = LegacyRouteGatewayAdapter(
        proxy=proxy,
        paid_http=None,
        paid_fs=FakeFSClient(),
        session_factory=failing_factory,
    )
    transport = WhoScoredTransport(
        direct_http_session=direct,
        direct_fs_client=direct_fs,
        paid_gateway_client=paid_gateway,
        context=TransportContext(transport_policy="direct_then_paid"),
        transport_policy=TransportPolicy.DIRECT_THEN_PAID,
        http_session_factory=failing_factory,
        direct_browser_attempts=2,
        browser_retry_backoff_seconds=0,
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.PROXY
    assert exc.value.retryable is True
    assert len(proxy.created) == 1
    assert len(proxy.closed) == 1


@pytest.mark.unit
def test_paid_lease_creation_error_never_exposes_proxy_credentials():
    secret = "create-route-secret"

    class LeakingCreateProxy(FakeProxyClient):
        def create_lease(self, **_kwargs):
            raise RuntimeError(f"socks5://operator:{secret}@paid.example:1080")

    transport, _ = _transport(
        FakeHTTPSession(
            FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
            FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
        ),
        direct_fs=FakeFSClient(
            FlareSolverrCFChallengeFailed("cf one"),
            FlareSolverrCFChallengeFailed("cf two"),
        ),
        proxy=LeakingCreateProxy(),
        paid_http=FakeHTTPSession(),
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    assert exc.value.kind is FailureKind.PROXY
    assert secret not in str(exc.value)
    assert "operator" not in str(exc.value)
    rendered = "".join(
        traceback.format_exception(type(exc.value), exc.value, exc.value.__traceback__)
    )
    assert secret not in rendered
    assert "operator" not in rendered


@pytest.mark.unit
def test_paid_finalize_error_redacts_itself_and_prior_paid_route_error():
    close_secret = "close-route-secret"
    request_secret = "request-route-secret"

    class LeakingCloseProxy(FakeProxyClient):
        def close(self, _lease):
            raise RuntimeError(
                f"http://operator:{close_secret}@proxy_filter:8899/finalize"
            )

    transport, _ = _transport(
        FakeHTTPSession(
            FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
            FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
        ),
        direct_fs=FakeFSClient(
            FlareSolverrCFChallengeFailed("cf one"),
            FlareSolverrCFChallengeFailed("cf two"),
        ),
        proxy=LeakingCloseProxy(),
        paid_http=FakeHTTPSession(
            requests.exceptions.ProxyError(
                f"https://operator:{request_secret}@paid.example:8443"
            )
        ),
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    rendered = "".join(
        traceback.format_exception(type(exc.value), exc.value, exc.value.__traceback__)
    )
    assert exc.value.route is TransportRoute.PAID_LEASE
    assert close_secret not in rendered
    assert request_secret not in rendered
    assert "operator" not in rendered
    assert "paid application gateway fetch failed" in rendered
    assert exc.value.__cause__ is None


@pytest.mark.unit
def test_paid_lease_and_stats_use_full_deterministic_query_url():
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
    )
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
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
    )
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
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
    )
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
    assert accounted[0]["paid_routes_attempted"] == ["paid_flaresolverr"]
    assert accounted[0]["final_paid_route"] == "paid_flaresolverr"


@pytest.mark.unit
def test_paid_browser_error_traceback_never_exposes_proxy_credentials():
    secret = "paid-browser-route-secret"
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
    )
    direct_fs = FakeFSClient(
        FlareSolverrCFChallengeFailed("cf one"),
        FlareSolverrCFChallengeFailed("cf two"),
    )
    paid_http = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"server": "cloudflare"})
    )
    paid_fs = FakeFSClient(
        FlareSolverrError(f"http://operator:{secret}@paid.example:8899")
    )
    transport, _ = _transport(
        direct,
        direct_fs=direct_fs,
        paid_fs=paid_fs,
        proxy=FakeProxyClient(),
        paid_http=paid_http,
    )

    with pytest.raises(WhoScoredTransportError) as exc:
        transport.fetch("https://www.whoscored.com/Matches/1/Live")

    rendered = "".join(
        traceback.format_exception(type(exc.value), exc.value, exc.value.__traceback__)
    )
    assert exc.value.route is TransportRoute.PAID_LEASE
    assert secret not in rendered
    assert "operator" not in rendered
    assert exc.value.__cause__ is None


@pytest.mark.unit
@pytest.mark.parametrize(
    "stats_override",
    [
        {"up_bytes": -1, "total_bytes": 899},
        {"down_bytes": True, "total_bytes": 100},
        {"total_bytes": 9999},
        {"provider_billed_bytes": 9999},
        {"close_complete": False},
        {"id": "different-lease"},
        {"canonical_url": "https://www.whoscored.com/Matches/999/Live"},
    ],
)
def test_malformed_paid_lease_accounting_fails_closed_without_counting_bytes(
    stats_override,
):
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
    )
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
    direct = FakeHTTPSession(
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
        FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
    )
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
    assert "PaidGatewayProtocolError" in str(exc.value)
    assert exc.value.__cause__ is None


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
        FakeHTTPSession(
            FakeHTTPResponse(403, CF_HTML, {"cf-ray": "initial"}),
            FakeHTTPResponse(403, CF_HTML, {"cf-ray": "fresh"}),
        ),
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
def test_proxy_lease_repr_never_exposes_bearer_credentials():
    lease = ProxyLease(
        lease_id="lease-visible-id",
        token="bearer-super-secret",
        proxy_url="http://lease:embedded-secret@proxy_filter:8899",
        max_bytes=1234,
        expires_at=99.0,
    )

    rendered = repr(lease)
    assert "lease-visible-id" in rendered
    assert "bearer-super-secret" not in rendered
    assert "embedded-secret" not in rendered
    assert "proxy_filter" not in rendered


@pytest.mark.unit
@pytest.mark.parametrize(
    ("proxy_url", "secret"),
    (
        ("http://alice:plain-secret@proxy_filter:8899", "plain-secret"),
        ("https://alice:p%40ss-secret@proxy_filter:8899", "p%40ss-secret"),
        ("http://alice%3Aencoded-secret%40proxy_filter:8899", "encoded-secret"),
        ("socks5://alice:socks-secret@proxy_filter:8899", "socks-secret"),
        ("http://proxy_filter:not-a-port-secret", "not-a-port-secret"),
    ),
)
def test_proxy_client_rejects_credentialed_or_non_http_origins_without_echo(
    proxy_url, secret
):
    with pytest.raises(ValueError, match="invalid filtering proxy URL") as exc:
        ProxyFilterClient(proxy_url, control_token=CONTROL_TOKEN)

    assert secret not in str(exc.value)


@pytest.mark.unit
def test_proxy_client_rejects_credentialed_control_origin_without_echo():
    secret = "control-secret"
    with pytest.raises(ValueError, match="invalid filtering proxy control URL") as exc:
        ProxyFilterClient(
            "http://proxy_filter:8899",
            control_url=f"https://operator:{secret}@proxy_filter:8900",
            control_token=CONTROL_TOKEN,
        )

    assert secret not in str(exc.value)


@pytest.mark.unit
def test_proxy_lease_response_rejects_malformed_origin_without_echo():
    secret = "response-secret"
    response = FakeHTTPResponse()
    response.raise_for_status = lambda: None
    response.json = lambda: {
        "id": "abc",
        "token": "lease-secret",
        "proxy_url": f"http://operator:{secret}@proxy_filter:8900",
        "max_bytes": 1234,
        "expires_at": 99.0,
    }
    client = ProxyFilterClient(
        "http://proxy_filter:8899",
        session=SimpleControlSession(response),
        control_token=CONTROL_TOKEN,
    )

    with pytest.raises(ValueError, match="invalid filtering proxy lease URL") as exc:
        client.create_lease(max_bytes=1234, ttl_seconds=60)

    assert secret not in str(exc.value)
    assert "lease-secret" not in str(exc.value)


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
        "http://proxy_filter:8899",
        session=session,
        timeout=3,
        control_token=CONTROL_TOKEN,
    )

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
        "transport_policy": "direct_only",
    }
    assert session.posts[0][1]["headers"] == {"X-Proxy-Control-Token": CONTROL_TOKEN}
    assert lease.proxy_url == "http://lease:s%2Fecret@proxy_filter:8900"
    client.stats(lease)
    client.close(lease)
    expected_headers = {
        "X-Proxy-Control-Token": CONTROL_TOKEN,
        "Authorization": "Bearer s/ecret",
    }
    assert session.gets[0][1]["headers"] == expected_headers
    assert session.deletes[0][1]["headers"] == expected_headers
    client.close_session()
    assert session.closed is True


@pytest.mark.unit
def test_proxy_control_client_fails_closed_without_auth_and_surfaces_401(monkeypatch):
    for name in (
        "WHOSCORED_PROXY_CONTROL_TOKEN",
        "PROXY_FILTER_CONTROL_TOKEN",
        "SOFASCORE_PROXY_CONTROL_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="WHOSCORED_PROXY_CONTROL_TOKEN"):
        ProxyFilterClient("http://proxy_filter:8899")

    response = FakeHTTPResponse(status_code=401)
    response.raise_for_status = lambda: (_ for _ in ()).throw(
        requests.HTTPError("401 Client Error")
    )
    response.json = lambda: {"error": "unauthorized"}
    session = SimpleControlSession(response)
    client = ProxyFilterClient(
        "http://proxy_filter:8899",
        session=session,
        control_token=CONTROL_TOKEN,
    )
    with pytest.raises(requests.HTTPError, match="401"):
        client.create_lease(max_bytes=2_000_000, ttl_seconds=60)
    assert session.posts[0][1]["headers"] == {"X-Proxy-Control-Token": CONTROL_TOKEN}
    assert session.posts[0][1]["json"]["max_bytes"] == 2_000_000


@pytest.mark.unit
def test_proxy_control_client_reads_dedicated_whoscored_control_token(monkeypatch):
    monkeypatch.setenv("WHOSCORED_PROXY_CONTROL_TOKEN", CONTROL_TOKEN)
    response = FakeHTTPResponse()
    response.raise_for_status = lambda: None
    response.json = lambda: {
        "id": "lease-env",
        "token": "lease-token",
        "proxy_url": "http://proxy_filter:8900",
        "max_bytes": 1000,
    }
    session = SimpleControlSession(response)
    client = ProxyFilterClient("http://proxy_filter:8899", session=session)

    client.create_lease(max_bytes=1000, ttl_seconds=60)

    assert session.posts[0][1]["headers"] == {"X-Proxy-Control-Token": CONTROL_TOKEN}


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


@pytest.mark.unit
def test_proxy_control_client_binds_batch_claim_and_switches_owner_atomically():
    response = FakeHTTPResponse()
    response.raise_for_status = lambda: None
    response.json = lambda: {
        "id": "lease-batch",
        "token": "lease-token",
        "proxy_url": "http://proxy_filter:8900",
        "max_bytes": 1000,
    }
    session = SimpleControlSession(response)
    client = ProxyFilterClient(
        "http://proxy_filter:8899",
        session=session,
        control_token=CONTROL_TOKEN,
    )
    labels = ("bootstrap:" + "a" * 64, "target:" + "b" * 64)

    lease = client.create_lease(
        max_bytes=1000,
        ttl_seconds=60,
        target_manifest_sha256="c" * 64,
        logical_target_units=1,
        expected_endpoint_labels=labels,
    )

    assert session.posts[0][1]["json"] == {
        "max_bytes": 1000,
        "ttl_seconds": 60,
        "canonical_url": "",
        "target_manifest_sha256": "c" * 64,
        "logical_target_units": 1,
        "expected_endpoint_labels": list(labels),
    }
    response.json = lambda: {"request_id": "request-next"}
    assert client.switch_endpoint(lease, "request-current", labels[1]) == (
        "request-next"
    )
    assert session.posts[1][0].endswith(
        "/v1/leases/lease-batch/endpoints/request-current/switch"
    )
    assert session.posts[1][1]["json"] == {"endpoint": labels[1]}
