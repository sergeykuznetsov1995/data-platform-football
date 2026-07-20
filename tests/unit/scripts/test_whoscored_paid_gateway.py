from __future__ import annotations

import base64
import hashlib
import http.client
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO

import pytest

import scrapers.whoscored.transport as transport_module
from scrapers.whoscored.proxy_campaign import (
    PROXY_CAMPAIGN_METER,
    WHOSCORED_PROXY_ALLOWED_HOSTS,
    approval_from_campaign_authority_context,
    approval_from_context,
    assert_paid_runtime_available,
    deterministic_proxy_attempt_id,
    proxy_campaign_authority_context,
    sign_proxy_campaign_approval,
)
from scrapers.whoscored.transport import (
    PAID_GATEWAY_SCHEMA_VERSION,
    PaidCampaignContext,
    PaidGatewayClient,
    PaidGatewayError,
    PaidGatewayRejected,
    PaidGatewayReceipt,
    ProxyLease,
    TransportBudgets,
    TransportContext,
    TransportPolicy,
    TransportRoute,
    WhoScoredTransport,
    WhoScoredTransportError,
    _canonical_url_key,
)
from scripts.whoscored_paid_gateway import (
    GatewayError,
    GatewayFetchRequest,
    BoundedGatewayServer,
    PaidGatewayApplication,
    PaidGatewayService,
    SettledGatewayError,
    _handler,
    _read_exact_body,
)


SECRET = "gateway-campaign-secret-which-is-long-enough"
NOW = datetime(2026, 7, 16, 12, tzinfo=timezone.utc)
URL = "https://www.whoscored.com/Matches/1/Live"
STRUCTURED_URL = (
    "https://www.whoscored.com/statisticsfeed/1/getteamstatistics"
)
BOOTSTRAP_URL = "https://www.whoscored.com/Regions/247/Tournaments/36"
CF_HTML = (
    b"<html><title>Just a moment...</title>"
    b"<script src='/cdn-cgi/challenge-platform/x'></script></html>"
)


def _authority_document(*, allowed_paths=None):
    paths = allowed_paths or ["/Matches"]
    allocation = {
        "allocation_id": "capture-1",
        "phase": "capture",
        "workload_class": "match_capture",
        "work_item_id": "match-1",
        "task_id": "capture_matches",
        "budget_bytes": 2_000_000,
        "request_limit": 2,
        "lease_limit": 1,
        "allowed_path_families": list(paths),
    }
    unsigned = {
        "schema_version": 2,
        "source": "whoscored",
        "approval_id": "approval-1",
        "campaign_id": "campaign-1",
        "run_id": "run-1",
        "issued_at": (NOW - timedelta(hours=1)).isoformat(),
        "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "transport_policy": "direct_then_paid",
        "runtime_sha256": "a" * 64,
        "classifier_sha256": "b" * 64,
        "caps": {
            "total_provider_bytes": 2_000_000,
            "discovery_provider_bytes": 0,
            "capture_provider_bytes": 2_000_000,
            "daily_provider_bytes": 2_000_000,
        },
        "limits": {"requests": 2, "leases": 1, "concurrency": 1},
        "allowed_dag_ids": ["dag_backfill_whoscored"],
        "allowed_hosts": sorted(WHOSCORED_PROXY_ALLOWED_HOSTS),
        "allowed_path_families": list(paths),
        "allocations": [allocation],
        "meter": PROXY_CAMPAIGN_METER,
        "signature_algorithm": "hmac-sha256",
    }
    signed = sign_proxy_campaign_approval(unsigned, SECRET)
    attempt_id = deterministic_proxy_attempt_id(
        dag_id="dag_backfill_whoscored",
        run_id="run-1",
        task_id="capture_matches",
        map_index=0,
        try_number=1,
    )
    context = {
        "dag_id": "dag_backfill_whoscored",
        "run_id": "run-1",
        "task_id": "capture_matches",
        "map_index": 0,
        "try_number": 1,
        "scope": "ENG-Premier League=2526",
        "entity": "match-1",
        "transport_policy": "direct_then_paid",
        "proxy_campaign_approval": signed,
        "proxy_campaign_id": "campaign-1",
        "proxy_approval_id": "approval-1",
        "proxy_approval_sha256": signed["approval_sha256"],
        "proxy_allocation": allocation,
        "proxy_allocation_id": "capture-1",
        "proxy_work_item_id": "match-1",
        "proxy_attempt_id": attempt_id,
    }
    approval, parsed_allocation, parsed_attempt = approval_from_context(
        context, secret=SECRET, now=NOW
    )
    return approval, parsed_allocation, parsed_attempt, context


def _request(
    context,
    *,
    url=URL,
    browser_bootstrap_url=None,
    max_provider_bytes=1_000_000,
):
    return GatewayFetchRequest.from_dict(
        {
            "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
            "url": url,
            "browser_bootstrap_url": browser_bootstrap_url,
            "max_response_bytes": 1024 * 1024,
            "max_provider_bytes": max_provider_bytes,
            "timeout_ms": 30_000,
            "context": context,
        }
    )


class FakeResponse:
    def __init__(self, content, *, status=200, headers=None, url=URL):
        self.content = content
        self.status_code = status
        self.headers = headers or {}
        self.url = url
        self.closed = False

    def iter_content(self, chunk_size):
        yield self.content

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, *responses, events=None, label="http"):
        self.responses = list(responses)
        self.events = events if events is not None else []
        self.label = label
        self.calls = []
        self.closed = False

    def get(self, url, **kwargs):
        self.events.append(f"{self.label}:get")
        self.calls.append((url, kwargs))
        return self.responses.pop(0)

    def close(self):
        self.events.append(f"{self.label}:close")
        self.closed = True


class FakeProxy:
    def __init__(self, events=None):
        self.events = events if events is not None else []
        self.created = []
        self.closed = []

    def create_lease(self, **kwargs):
        self.events.append("lease:create")
        self.created.append(kwargs)
        return ProxyLease(
            lease_id="lease-secret-id",
            token="lease-secret-token",
            proxy_url="http://lease:lease-secret-token@proxy_filter:8899",
            max_bytes=kwargs["max_bytes"],
            expires_at=1.0,
        )

    def close(self, lease):
        self.events.append("lease:close")
        self.closed.append(lease)
        canonical = self.created[-1]["canonical_url"]
        return {
            "id": lease.lease_id,
            "canonical_url": canonical,
            "up_bytes": 100,
            "down_bytes": 900,
            "total_bytes": 1000,
            "provider_billed_bytes": 1000,
            "close_complete": True,
        }


class FakeBrowser:
    def __init__(self, *, html=b"<html>ok</html>", destroy_error=False, events=None):
        self.html = html
        self.destroy_error = destroy_error
        self.events = events if events is not None else []
        self.created = []
        self.destroyed = []

    def create_session(self, session_id, proxy_url=None, **_kwargs):
        self.events.append("browser:create")
        self.created.append((session_id, proxy_url))

    def get(self, url, session_id, **kwargs):
        self.events.append("browser:get")
        return {
            "html": self.html.decode(),
            "status": 200,
            "finalUrl": url,
        }

    def xhr_get(self, url, session_id, **kwargs):
        raise AssertionError("unexpected XHR")

    def destroy_session_strict(self, session_id, **_kwargs):
        self.events.append("browser:destroy")
        self.destroyed.append(session_id)
        if self.destroy_error:
            raise RuntimeError("cleanup failed")


def _verified_authority(context):
    return approval_from_context(context, secret=SECRET, now=NOW)


@pytest.mark.unit
def test_code_owned_paid_gate_rejects_before_direct_or_lease():
    _approval, _allocation, _attempt, context = _authority_document()
    proxy = FakeProxy()
    direct = FakeSession(FakeResponse(CF_HTML, status=403, headers={"cf-ray": "x"}))
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=FakeBrowser(),
        authority=lambda document: assert_paid_runtime_available(
            document, secret=SECRET, now=NOW
        ),
        direct_session_factory=lambda: direct,
    )

    with pytest.raises(GatewayError, match="authority_rejected"):
        service.fetch(_request(context))

    assert direct.calls == []
    assert proxy.created == []


@pytest.mark.unit
def test_fresh_direct_success_never_creates_paid_lease():
    _approval, _allocation, _attempt, context = _authority_document()
    proxy = FakeProxy()
    direct = FakeSession(FakeResponse(b"<html>already open</html>"), label="direct")
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=FakeBrowser(),
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
    )

    with pytest.raises(GatewayError, match="fresh_direct_not_cloudflare"):
        service.fetch(_request(context))

    assert direct.closed is True
    assert proxy.created == []


def _structured_gate_response(status: int) -> FakeResponse:
    content = b"<script src='verify-client.js'></script>"
    headers = {}
    if status == 200:
        content += b"<p>Page you requested does not exist</p>"
    else:
        headers["Location"] = (
            "/404.html?aspxerrorpath="
            "%2Fstatisticsfeed%2F1%2Fgetteamstatistics"
        )
    return FakeResponse(
        content,
        status=status,
        headers=headers,
        url=STRUCTURED_URL,
    )


@pytest.mark.unit
@pytest.mark.parametrize("status", [200, 302])
def test_fresh_direct_exact_structured_gate_allows_lease(status):
    _approval, _allocation, _attempt, context = _authority_document(
        allowed_paths=[
            "/Matches",
            "/Regions",
            "/statisticsfeed/1/getteamstatistics",
        ]
    )
    proxy = FakeProxy()
    direct = FakeSession(_structured_gate_response(status), label="direct")
    paid = FakeSession(
        FakeResponse(b'{"ok":true}', url=STRUCTURED_URL),
        label="paid",
    )
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=FakeBrowser(),
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
        http_session_factory=lambda _proxy_url: paid,
    )

    result = service.fetch(_request(context, url=STRUCTURED_URL))

    assert result.route is TransportRoute.PAID_HTTP
    assert len(proxy.created) == 1


@pytest.mark.unit
@pytest.mark.parametrize("status", [200, 302])
def test_paid_http_exact_structured_gate_transitions_to_bounded_xhr(status):
    _approval, _allocation, _attempt, context = _authority_document(
        allowed_paths=[
            "/Matches",
            "/Regions",
            "/statisticsfeed/1/getteamstatistics",
        ]
    )

    class XhrBrowser(FakeBrowser):
        def xhr_get(self, url, session_id, **kwargs):
            self.events.append("browser:xhr")
            return {
                "content": b'{"rows":[]}',
                "status": 200,
                "headers": {"content-type": "application/json"},
                "finalUrl": url,
            }

    direct = FakeSession(_structured_gate_response(status), label="direct")
    paid = FakeSession(_structured_gate_response(status), label="paid")
    proxy = FakeProxy()
    browser = XhrBrowser()
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=browser,
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
        http_session_factory=lambda _proxy_url: paid,
    )

    result = service.fetch(
        _request(
            context,
            url=STRUCTURED_URL,
            browser_bootstrap_url=BOOTSTRAP_URL,
        )
    )

    assert result.route is TransportRoute.PAID_FLARESOLVERR
    assert result.content == b'{"rows":[]}'
    assert "browser:xhr" in browser.events


@pytest.mark.unit
def test_http_result_and_receipt_are_returned_only_after_cleanup():
    _approval, _allocation, _attempt, context = _authority_document()
    events = []
    direct = FakeSession(
        FakeResponse(CF_HTML, status=403, headers={"cf-ray": "x"}),
        events=events,
        label="direct",
    )
    paid = FakeSession(
        FakeResponse(b"<html>paid ok</html>"), events=events, label="paid"
    )
    proxy = FakeProxy(events)
    browser = FakeBrowser(events=events)
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=browser,
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
        http_session_factory=lambda proxy_url: paid,
    )

    result = service.fetch(_request(context))

    assert result.content == b"<html>paid ok</html>"
    assert result.route is TransportRoute.PAID_HTTP
    assert result.receipt.provider_billed_bytes == 1000
    assert result.receipt.cleanup_complete is True
    assert "lease-secret-id" not in json.dumps(result.receipt.to_dict())
    assert "lease-secret-token" not in json.dumps(result.receipt.to_dict())
    assert events[-2:] == ["paid:close", "lease:close"]
    assert browser.created == []


@pytest.mark.unit
def test_gateway_rejects_receipt_above_request_and_lease_caps():
    _approval, _allocation, _attempt, context = _authority_document()
    direct = FakeSession(
        FakeResponse(CF_HTML, status=403, headers={"cf-ray": "x"})
    )
    paid = FakeSession(FakeResponse(b"ok"))
    proxy = FakeProxy()
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=FakeBrowser(),
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
        http_session_factory=lambda _proxy_url: paid,
    )

    with pytest.raises(GatewayError, match="accounting_invalid"):
        service.fetch(_request(context, max_provider_bytes=500))

    assert len(proxy.closed) == 1


@pytest.mark.unit
def test_browser_capability_is_destroyed_before_lease_receipt():
    _approval, _allocation, _attempt, context = _authority_document()
    events = []
    direct = FakeSession(
        FakeResponse(CF_HTML, status=403, headers={"cf-ray": "d"}),
        events=events,
        label="direct",
    )
    paid = FakeSession(
        FakeResponse(CF_HTML, status=403, headers={"cf-ray": "p"}),
        events=events,
        label="paid",
    )
    proxy = FakeProxy(events)
    browser = FakeBrowser(events=events)
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=browser,
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
        http_session_factory=lambda proxy_url: paid,
    )

    result = service.fetch(_request(context))

    assert result.route is TransportRoute.PAID_FLARESOLVERR
    assert len(browser.created) == 1
    assert browser.destroyed == [browser.created[0][0]]
    assert events.index("browser:destroy") < events.index("lease:close")


@pytest.mark.unit
def test_gateway_serializes_shared_sessions_across_worker_threads():
    _approval, _allocation, _attempt, context = _authority_document()
    state = {"active": 0, "maximum": 0}
    state_lock = threading.Lock()

    class CountingSession(FakeSession):
        def get(self, url, **kwargs):
            with state_lock:
                state["active"] += 1
                state["maximum"] = max(state["maximum"], state["active"])
            try:
                time.sleep(0.03)
                return super().get(url, **kwargs)
            finally:
                with state_lock:
                    state["active"] -= 1

    proxy = FakeProxy()
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=FakeBrowser(),
        authority=_verified_authority,
        direct_session_factory=lambda: CountingSession(
            FakeResponse(CF_HTML, status=403, headers={"cf-ray": "x"})
        ),
        http_session_factory=lambda _proxy_url: CountingSession(
            FakeResponse(b"ok")
        ),
    )
    errors = []

    def run():
        try:
            service.fetch(_request(context))
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    workers = [threading.Thread(target=run) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=2)

    assert errors == []
    assert state["maximum"] == 1
    assert len(proxy.created) == 2


@pytest.mark.unit
def test_browser_cleanup_failure_returns_no_body_or_receipt():
    _approval, _allocation, _attempt, context = _authority_document()
    direct = FakeSession(
        FakeResponse(CF_HTML, status=403, headers={"cf-ray": "d"})
    )
    paid = FakeSession(
        FakeResponse(CF_HTML, status=403, headers={"cf-ray": "p"})
    )
    proxy = FakeProxy()
    browser = FakeBrowser(destroy_error=True)
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=browser,
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
        http_session_factory=lambda proxy_url: paid,
    )

    with pytest.raises(GatewayError, match="cleanup_failed"):
        service.fetch(_request(context))

    assert len(proxy.closed) == 1


@pytest.mark.unit
def test_source_failure_returns_receipt_only_after_successful_cleanup():
    _approval, _allocation, _attempt, context = _authority_document()
    events = []

    class FailingBrowser(FakeBrowser):
        def get(self, url, session_id, **kwargs):
            self.events.append("browser:get")
            raise RuntimeError("source failed")

    direct = FakeSession(
        FakeResponse(CF_HTML, status=403, headers={"cf-ray": "d"}),
        events=events,
        label="direct",
    )
    paid = FakeSession(
        FakeResponse(CF_HTML, status=403, headers={"cf-ray": "p"}),
        events=events,
        label="paid",
    )
    proxy = FakeProxy(events)
    browser = FailingBrowser(events=events)
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=browser,
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
        http_session_factory=lambda proxy_url: paid,
    )

    with pytest.raises(SettledGatewayError) as raised:
        service.fetch(_request(context))

    assert raised.value.receipt.provider_billed_bytes == 1000
    assert events.index("browser:destroy") < events.index("lease:close")


@pytest.mark.unit
def test_forged_attempt_and_disallowed_url_fail_before_network():
    _approval, _allocation, _attempt, context = _authority_document()
    proxy = FakeProxy()
    direct = FakeSession()
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=FakeBrowser(),
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
    )
    forged = dict(context)
    forged["proxy_attempt_id"] = "attempt-" + "0" * 64

    with pytest.raises(GatewayError, match="authority_rejected"):
        service.fetch(_request(forged))
    with pytest.raises(GatewayError, match="target_not_allowed"):
        service.fetch(_request(context, url="https://www.whoscored.com/Players/1"))

    assert direct.calls == []
    assert proxy.created == []


@pytest.mark.unit
def test_fetch_independently_requires_gateway_owned_alert_state():
    _approval, _allocation, _attempt, context = _authority_document()
    proxy = FakeProxy()
    direct = FakeSession()

    def missing_alert(**_identity):
        raise RuntimeError("no durable gateway receipt")

    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=FakeBrowser(),
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
        alert_requirement=missing_alert,
    )

    with pytest.raises(GatewayError, match="alert_preflight_required"):
        service.fetch(_request(context))

    assert direct.calls == []
    assert proxy.created == []


class FakeGatewayHTTPResponse:
    def __init__(self, document, status=200):
        self.status_code = status
        self.content = json.dumps(
            document,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        self.closed = False

    def iter_content(self, chunk_size):
        yield self.content

    def close(self):
        self.closed = True


class FakeGatewayHTTPSession:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.trust_env = True
        self.closed = False

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response

    def close(self):
        self.closed = True


def _transport_context(context):
    base = {
        key: context[key]
        for key in (
            "dag_id",
            "run_id",
            "task_id",
            "map_index",
            "try_number",
            "scope",
            "entity",
            "transport_policy",
        )
    }
    campaign = {key: value for key, value in context.items() if key not in base}
    return TransportContext(**base, proxy_campaign=campaign)


@pytest.mark.unit
def test_runner_client_sends_only_high_level_fetch_and_validates_receipt():
    approval, allocation, attempt_id, context_document = _authority_document()
    context = _transport_context(context_document)
    content = b"paid body"
    receipt = PaidGatewayReceipt(
        campaign_id=approval.campaign_id,
        approval_id=approval.approval_id,
        approval_sha256=approval.approval_sha256,
        allocation_id=allocation.allocation_id,
        attempt_id_hash=hashlib.sha256(attempt_id.encode()).hexdigest(),
        canonical_url_sha256=hashlib.sha256(
            _canonical_url_key(URL).encode()
        ).hexdigest(),
        lease_id_hash="c" * 64,
        route=TransportRoute.PAID_HTTP,
        up_bytes=10,
        down_bytes=20,
        total_bytes=30,
        provider_billed_bytes=30,
        close_complete=True,
        cleanup_complete=True,
    )
    document = {
        "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
        "url": URL,
        "status_code": 200,
        "headers": {"content-type": "text/html"},
        "body_base64": base64.b64encode(content).decode(),
        "body_sha256": hashlib.sha256(content).hexdigest(),
        "route": "paid_http",
        "receipt": receipt.to_dict(),
    }
    session = FakeGatewayHTTPSession(FakeGatewayHTTPResponse(document))
    client = PaidGatewayClient(
        "http://paid_gateway:8898", token="g" * 32, session=session
    )

    result = client.fetch(
        URL,
        context=context,
        max_response_bytes=1024,
        max_provider_bytes=1000,
        timeout_ms=10_000,
    )

    assert result.content == content
    payload = session.calls[0][1]["json"]
    assert set(payload) == {
        "schema_version",
        "url",
        "browser_bootstrap_url",
        "max_response_bytes",
        "max_provider_bytes",
        "timeout_ms",
        "context",
    }
    assert not {"proxy_url", "lease", "token", "session", "method", "headers"} & set(payload)
    assert session.trust_env is False
    assert session.response.closed is True


@pytest.mark.unit
def test_runner_client_closes_stream_when_iteration_fails():
    _approval, _allocation, _attempt_id, context_document = _authority_document()
    context = _transport_context(context_document)

    class BrokenStream(FakeGatewayHTTPResponse):
        def iter_content(self, chunk_size):
            raise OSError("secret-url-and-token-must-not-leak")

    response = BrokenStream({"ignored": True})
    session = FakeGatewayHTTPSession(response)
    client = PaidGatewayClient(
        "http://paid_gateway:8898", token="g" * 32, session=session
    )

    with pytest.raises(PaidGatewayError) as raised:
        client.fetch(
            URL,
            context=context,
            max_response_bytes=1024,
            max_provider_bytes=1000,
            timeout_ms=10_000,
        )

    assert "secret-url" not in str(raised.value)
    assert response.closed is True


@pytest.mark.unit
def test_runner_campaign_rpc_sends_allocation_free_signed_context():
    approval, _allocation, _attempt_id, _context_document = _authority_document()
    campaign_context = PaidCampaignContext.from_approval(approval)
    response = FakeGatewayHTTPResponse(
        {
            "schema_version": 1,
            "operation": "snapshot",
            "result": {
                "campaign": {
                    "campaign_id": approval.campaign_id,
                    "status": "active",
                }
            },
        }
    )
    session = FakeGatewayHTTPSession(response)
    client = PaidGatewayClient(
        "http://paid_gateway:8898", token="g" * 32, session=session
    )

    snapshot = client.snapshot(context=campaign_context)

    assert snapshot["campaign_id"] == approval.campaign_id
    url, call = session.calls[0]
    request = json.loads(call["data"])
    assert url.endswith("/v1/campaign-control")
    assert set(request) == {"schema_version", "operation", "context", "arguments"}
    assert request["operation"] == "snapshot"
    assert request["arguments"] == {}
    assert set(request["context"]) == {
        "dag_id",
        "run_id",
        "transport_policy",
        "proxy_campaign_approval",
        "proxy_campaign_id",
        "proxy_approval_id",
        "proxy_approval_sha256",
    }
    assert not {
        "proxy_allocation",
        "proxy_attempt_id",
        "task_id",
        "map_index",
        "try_number",
    } & set(request["context"])
    assert response.closed is True


@pytest.mark.unit
def test_runner_client_preserves_settled_error_accounting_receipt():
    approval, allocation, attempt_id, context_document = _authority_document()
    context = _transport_context(context_document)
    receipt = PaidGatewayReceipt(
        campaign_id=approval.campaign_id,
        approval_id=approval.approval_id,
        approval_sha256=approval.approval_sha256,
        allocation_id=allocation.allocation_id,
        attempt_id_hash=hashlib.sha256(attempt_id.encode()).hexdigest(),
        canonical_url_sha256=hashlib.sha256(
            _canonical_url_key(URL).encode()
        ).hexdigest(),
        lease_id_hash="d" * 64,
        route=TransportRoute.PAID_FLARESOLVERR,
        up_bytes=12,
        down_bytes=34,
        total_bytes=46,
        provider_billed_bytes=46,
        close_complete=True,
        cleanup_complete=True,
    )
    error = {
        "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
        "error": {"code": "browser_fetch_failed"},
        "receipt": receipt.to_dict(),
    }
    session = FakeGatewayHTTPSession(FakeGatewayHTTPResponse(error, status=502))
    client = PaidGatewayClient(
        "http://paid_gateway:8898", token="g" * 32, session=session
    )

    with pytest.raises(PaidGatewayRejected) as raised:
        client.fetch(
            URL,
            context=context,
            max_response_bytes=1024,
            max_provider_bytes=1000,
            timeout_ms=10_000,
        )

    assert raised.value.code == "browser_fetch_failed"
    assert raised.value.receipt == receipt


@pytest.mark.unit
def test_old_whoscored_proxy_path_is_rejected_before_lease(monkeypatch):
    monkeypatch.setattr(
        transport_module, "assert_paid_runtime_available", lambda metadata: None
    )
    monkeypatch.setattr(
        transport_module, "assert_paid_alert_runtime_available", lambda context: None
    )
    proxy = FakeProxy()

    with pytest.raises(ValueError, match="only the isolated paid application gateway"):
        WhoScoredTransport(
            direct_http_session=FakeSession(),
            proxy_client=proxy,
            paid_proxy_url="http://proxy_filter:8899",
            context=TransportContext(transport_policy="direct_then_paid"),
            transport_policy=TransportPolicy.DIRECT_THEN_PAID,
        )

    assert proxy.created == []


@pytest.mark.unit
def test_transport_accounts_settled_gateway_error_receipt(monkeypatch):
    approval, allocation, attempt_id, context_document = _authority_document()
    context = _transport_context(context_document)
    receipt = PaidGatewayReceipt(
        campaign_id=approval.campaign_id,
        approval_id=approval.approval_id,
        approval_sha256=approval.approval_sha256,
        allocation_id=allocation.allocation_id,
        attempt_id_hash=hashlib.sha256(attempt_id.encode()).hexdigest(),
        canonical_url_sha256=hashlib.sha256(
            _canonical_url_key(URL).encode()
        ).hexdigest(),
        lease_id_hash="e" * 64,
        route=TransportRoute.PAID_FLARESOLVERR,
        up_bytes=20,
        down_bytes=80,
        total_bytes=100,
        provider_billed_bytes=100,
        close_complete=True,
        cleanup_complete=True,
    )

    class SettledErrorGateway:
        def fetch(self, *args, **kwargs):
            raise PaidGatewayRejected(
                "browser_fetch_failed", receipt=receipt
            )

    monkeypatch.setattr(
        transport_module, "assert_paid_runtime_available", lambda metadata: None
    )
    monkeypatch.setattr(
        transport_module, "assert_paid_alert_runtime_available", lambda value: None
    )
    transport = WhoScoredTransport(
        direct_http_session=FakeSession(),
        direct_fs_client=FakeBrowser(),
        paid_gateway_client=SettledErrorGateway(),
        context=context,
        budgets=TransportBudgets(
            max_response_bytes=1024,
            max_paid_bytes_per_url=1000,
            max_paid_bytes_per_lease=1000,
            max_paid_bytes_per_task=1000,
            max_paid_urls=1,
            max_paid_browser_bootstraps=1,
        ),
    )

    with pytest.raises(WhoScoredTransportError):
        transport._paid_fetch(URL, None, cache_key="test")

    assert transport.get_traffic_stats()["paid_proxy_bytes"] == 100


@pytest.mark.unit
def test_http_application_requires_auth_and_never_exposes_cleanup_failure():
    _approval, _allocation, _attempt, context = _authority_document()
    proxy = FakeProxy()
    direct = FakeSession(FakeResponse(b"open"))
    service = PaidGatewayService(
        proxy_client=proxy,
        browser_client=FakeBrowser(),
        authority=_verified_authority,
        direct_session_factory=lambda: direct,
    )
    application = PaidGatewayApplication(token="a" * 32, service=service)
    request_body = json.dumps(
        {
            "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
            "url": URL,
            "browser_bootstrap_url": None,
            "max_response_bytes": 1024,
            "max_provider_bytes": 1000,
            "timeout_ms": 10_000,
            "context": context,
        }
    ).encode()

    status, unauthorized = application.handle(authorization="", body=request_body)
    assert status == 401
    assert b"authentication_required" in unauthorized
    assert proxy.created == []

    status, rejected = application.handle(
        authorization="Bearer " + "a" * 32, body=request_body
    )
    assert status == 409
    assert b"fresh_direct_not_cloudflare" in rejected
    assert b"lease-secret" not in rejected


@pytest.mark.unit
def test_application_campaign_control_has_exact_bounded_operation_surface():
    approval, _allocation, _attempt, _full_context = _authority_document()
    context = proxy_campaign_authority_context(approval)

    class RpcProxy(FakeProxy):
        def campaign_control(self, operation, *, context, arguments):
            assert operation == "snapshot"
            assert arguments == {}
            assert context.as_dict() == context_document
            return {"campaign": {"campaign_id": approval.campaign_id}}

    context_document = context
    def campaign_authority(document, **options):
        return approval_from_campaign_authority_context(
            document,
            secret=SECRET,
            require_active=options["require_active"],
            now=NOW,
        )
    service = PaidGatewayService(
        proxy_client=RpcProxy(),
        browser_client=FakeBrowser(),
        authority=_verified_authority,
        campaign_authority=campaign_authority,
    )
    application = PaidGatewayApplication(token="a" * 32, service=service)
    body = json.dumps(
        {
            "schema_version": 1,
            "operation": "snapshot",
            "context": context,
            "arguments": {},
        },
        separators=(",", ":"),
    ).encode()

    status, response = application.handle_campaign_control(
        authorization="Bearer " + "a" * 32,
        body=body,
    )

    assert status == 200
    assert json.loads(response) == {
        "schema_version": 1,
        "operation": "snapshot",
        "result": {"campaign": {"campaign_id": approval.campaign_id}},
    }

    malformed = json.loads(body)
    malformed["arguments"] = {"arbitrary_method": "DELETE"}
    status, _response = application.handle_campaign_control(
        authorization="Bearer " + "a" * 32,
        body=json.dumps(malformed).encode(),
    )
    assert status == 400


@pytest.mark.unit
def test_application_preflight_uses_approval_only_context_and_exact_identity():
    approval, _allocation, _attempt, _full_context = _authority_document()
    context = proxy_campaign_authority_context(approval)
    deliveries = []

    def campaign_authority(document, **options):
        return approval_from_campaign_authority_context(
            document,
            secret=SECRET,
            require_active=options["require_active"],
            now=NOW,
        )

    def deliver(**identity):
        deliveries.append(identity)
        return identity

    service = PaidGatewayService(
        proxy_client=FakeProxy(),
        browser_client=FakeBrowser(),
        authority=_verified_authority,
        campaign_authority=campaign_authority,
        alert_delivery=deliver,
    )
    application = PaidGatewayApplication(token="a" * 32, service=service)
    body = json.dumps(
        {"schema_version": 1, "context": context}, separators=(",", ":")
    ).encode()

    status, response = application.handle_preflight_alert(
        authorization="Bearer " + "a" * 32,
        body=body,
    )

    assert status == 200
    assert json.loads(response)["status"] == "delivered"
    assert deliveries == [
        {
            "campaign_id": approval.campaign_id,
            "approval_id": approval.approval_id,
            "approval_sha256": approval.approval_sha256,
            "dag_id": approval.allowed_dag_ids[0],
            "run_id": approval.run_id,
            "alert_task_id": "validate_whoscored_paid_alert_delivery",
        }
    ]


@pytest.mark.unit
def test_health_probe_is_small_and_has_no_secret_or_side_effect():
    class NoFetchService:
        def fetch(self, request):
            raise AssertionError("health must not call the fetch service")

    application = PaidGatewayApplication(token="h" * 32, service=NoFetchService())

    status, body = application.health()

    assert status == 200
    assert json.loads(body) == {"schema_version": 1, "status": "ok"}
    assert b"token" not in body
    assert b"campaign" not in body


@pytest.mark.unit
def test_http_health_endpoint_is_side_effect_free():
    class NoFetchService:
        def fetch(self, request):
            raise AssertionError("health must not fetch")

    application = PaidGatewayApplication(token="h" * 32, service=NoFetchService())
    server = BoundedGatewayServer(
        ("127.0.0.1", 0), _handler(application), max_workers=1
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=2
        )
        connection.request("GET", "/health")
        response = connection.getresponse()
        body = response.read()
        connection.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert response.status == 200
    assert json.loads(body) == {"schema_version": 1, "status": "ok"}


@pytest.mark.unit
def test_declared_http_body_must_be_read_exactly():
    assert _read_exact_body(BytesIO(b"abc"), 3) == b"abc"
    assert _read_exact_body(BytesIO(b"ab"), 3) is None


@pytest.mark.unit
def test_paid_http_session_impersonates_a_browser_through_the_lease_proxy():
    # The paid fetch must present a browser TLS/HTTP fingerprint (curl_cffi) so
    # Cloudflare challenges it less and libcurl negotiates br/zstd, and it must
    # route through the lease proxy (never a bare direct connection).
    from scripts import whoscored_paid_gateway as gw

    session = gw._new_paid_http_session("http://proxy_filter:8900")
    try:
        assert type(session).__module__.startswith("curl_cffi")
        proxies = dict(getattr(session, "proxies", {}) or {})
        assert proxies.get("https") == "http://proxy_filter:8900"
        assert proxies.get("http") == "http://proxy_filter:8900"
        # Response contract _bounded_content / the fetch path relies on.
        for attr in ("get", "close"):
            assert callable(getattr(session, attr, None))
    finally:
        close = getattr(session, "close", None)
        if callable(close):
            close()
