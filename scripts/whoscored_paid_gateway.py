#!/usr/bin/env python3
"""Authenticated, single-operation L7 gateway for WhoScored paid fetches.

The Airflow runner can ask only ``POST /v1/fetch``.  It never receives a proxy
lease URL/token or a FlareSolverr session identifier.  This process validates
the existing HMAC-bound campaign context and exact target, owns the filtering
proxy and browser lifecycle, and releases both before returning a bounded body
and credential-free accounting receipt.

This release admits only the small measurement canary.  The separate
code-owned full-crawl sentinel remains closed until canary reconciliation.
"""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/scripts/", 1)[0]
)
if _whoscored_production:
    if (
        getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None)
        != 2
    ):
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = _whoscored_bootstrap_sys._load_whoscored_runtime_contract(
    _whoscored_root
)

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import socket
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Mapping, Optional, Protocol
from urllib.parse import urlsplit

import requests

from scrapers.base.flaresolverr_client import (
    FlareSolverrClient,
    FlareSolverrError,
    FlareSolverrErrorPage,
    is_chromium_error_page,
)
from scrapers.whoscored.proxy_campaign import (
    PROXY_CAMPAIGN_AUTHORITY_CONTEXT_FIELDS,
    PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS,
    PROXY_CAMPAIGN_CONTROL_RESULT_FIELDS,
    PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION,
    ProxyCampaignApproval,
    ProxyWorkAllocation,
    assert_paid_campaign_authority_available,
    assert_paid_runtime_authority_available,
    strict_json_loads,
)
from scrapers.whoscored.runtime_contract import (
    attested_runtime_file_sha256,
    require_production_runtime_class,
)
from scrapers.whoscored.transport import (
    MAX_PAID_GATEWAY_RESPONSE_BYTES,
    PAID_GATEWAY_SCHEMA_VERSION,
    PAID_GATEWAY_TOKEN_ENV,
    PINNED_FLARESOLVERR_VERSION,
    PaidGatewayReceipt,
    PaidGatewayResponse,
    ProxyBudgetRejected,
    ProxyConcurrencyLimited,
    ProxyFilterClient,
    ProxyLease,
    TransportRoute,
    _canonical_url_key,
    is_cloudflare_response,
    is_whoscored_structured_feed_access_gate,
)
from dags.utils.alerts import (
    ensure_paid_alert_delivery,
    paid_alert_task_id_for_dag,
    require_paid_alert_delivery,
)
from scripts.flaresolverr_extended import (
    PAID_GATEWAY_CAPABILITY_SCHEMA,
    build_paid_gateway_capability_headers,
)


MAX_GATEWAY_REQUEST_BYTES = 5 * 1024 * 1024
MAX_GATEWAY_PROVIDER_BYTES_PER_FETCH = 2_000_000
MAX_GATEWAY_TIMEOUT_MS = 60_000
GATEWAY_CLEANUP_GRACE_SECONDS = 15.0
MAX_GATEWAY_BODY_READ_SECONDS = 10.0
MAX_GATEWAY_URL_BYTES = 16 * 1024
FLARESOLVERR_GATEWAY_SECRET_ENV = "WHOSCORED_FLARESOLVERR_GATEWAY_SECRET"
_REQUEST_FIELDS = frozenset(
    {
        "schema_version",
        "url",
        "browser_bootstrap_url",
        "max_response_bytes",
        "max_provider_bytes",
        "timeout_ms",
        "context",
    }
)
_PREFLIGHT_REQUEST_FIELDS = frozenset({"schema_version", "context"})
_CAMPAIGN_CONTROL_REQUEST_FIELDS = frozenset(
    {"schema_version", "operation", "context", "arguments"}
)
_CONTEXT_FIELDS = frozenset(
    {
        "dag_id",
        "run_id",
        "task_id",
        "map_index",
        "try_number",
        "scope",
        "entity",
        "transport_policy",
        "proxy_campaign_approval",
        "proxy_campaign_id",
        "proxy_approval_id",
        "proxy_approval_sha256",
        "proxy_allocation",
        "proxy_allocation_id",
        "proxy_work_item_id",
        "proxy_attempt_id",
    }
)
_SAFE_RESPONSE_HEADERS = frozenset(
    {
        "cache-control",
        "content-language",
        "content-type",
        "etag",
        "expires",
        "last-modified",
    }
)
_CONTROL_RE = re.compile(r"[\x00-\x20\x7f]")


class GatewayError(RuntimeError):
    """A fail-closed gateway error safe to reduce to a short public code."""

    def __init__(self, code: str, *, http_status: int = 502) -> None:
        super().__init__(code)
        self.code = code
        self.http_status = http_status


class SettledGatewayError(GatewayError):
    """A failed source fetch whose paid accounting was fully cleaned."""

    def __init__(self, error: GatewayError, receipt: PaidGatewayReceipt) -> None:
        super().__init__(error.code, http_status=error.http_status)
        self.receipt = receipt


class _LeaseClient(Protocol):
    def create_lease(
        self,
        *,
        max_bytes: int,
        ttl_seconds: int,
        context: Any,
        canonical_url: str,
    ) -> ProxyLease: ...

    def close(self, lease: ProxyLease) -> dict[str, Any]: ...

    def campaign_control(
        self,
        operation: str,
        *,
        context: Any,
        arguments: Mapping[str, object],
    ) -> Mapping[str, object]: ...


class _BrowserClient(Protocol):
    def create_session(
        self,
        session_id: str,
        proxy_url: Optional[str] = None,
        *,
        timeout_seconds: Optional[float] = None,
    ) -> None: ...

    def destroy_session_strict(
        self,
        session_id: str,
        *,
        timeout_seconds: Optional[float] = None,
    ) -> None: ...

    def get(self, url: str, session_id: str, **kwargs: Any) -> dict[str, Any]: ...

    def xhr_get(self, url: str, session_id: str, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class GatewayFetchRequest:
    url: str
    browser_bootstrap_url: Optional[str]
    max_response_bytes: int
    max_provider_bytes: int
    timeout_ms: int
    context: Mapping[str, object]

    @staticmethod
    def _bounded_integer(value: object, field: str, ceiling: int) -> int:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 1 <= value <= ceiling
        ):
            raise GatewayError("invalid_request", http_status=400)
        return value

    @staticmethod
    def _url(value: object, *, nullable: bool = False) -> Optional[str]:
        if nullable and value is None:
            return None
        if (
            type(value) is not str
            or not value
            or value != value.strip()
            or len(value.encode("utf-8")) > MAX_GATEWAY_URL_BYTES
            or _CONTROL_RE.search(value) is not None
            or "\\" in value
        ):
            raise GatewayError("invalid_target", http_status=400)
        # Full campaign/allocation validation below owns the allow-list.  This
        # first pass removes parser ambiguity before any authority or I/O.
        try:
            parts = urlsplit(value)
            port = parts.port
        except (TypeError, ValueError):
            raise GatewayError("invalid_target", http_status=400) from None
        if (
            parts.scheme != "https"
            or not parts.hostname
            or port is not None
            or parts.username is not None
            or parts.password is not None
            or bool(parts.fragment)
        ):
            raise GatewayError("invalid_target", http_status=400)
        return value

    @classmethod
    def from_dict(cls, value: object) -> "GatewayFetchRequest":
        if not isinstance(value, Mapping) or frozenset(value) != _REQUEST_FIELDS:
            raise GatewayError("invalid_request", http_status=400)
        if value.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION:
            raise GatewayError("invalid_request", http_status=400)
        context = value.get("context")
        if not isinstance(context, Mapping) or frozenset(context) != _CONTEXT_FIELDS:
            raise GatewayError("invalid_context", http_status=403)
        return cls(
            url=str(cls._url(value.get("url"))),
            browser_bootstrap_url=cls._url(
                value.get("browser_bootstrap_url"), nullable=True
            ),
            max_response_bytes=cls._bounded_integer(
                value.get("max_response_bytes"),
                "max_response_bytes",
                MAX_PAID_GATEWAY_RESPONSE_BYTES,
            ),
            max_provider_bytes=cls._bounded_integer(
                value.get("max_provider_bytes"),
                "max_provider_bytes",
                MAX_GATEWAY_PROVIDER_BYTES_PER_FETCH,
            ),
            timeout_ms=cls._bounded_integer(
                value.get("timeout_ms"),
                "timeout_ms",
                MAX_GATEWAY_TIMEOUT_MS,
            ),
            context=dict(context),
        )


@dataclass(frozen=True)
class _CampaignContextEnvelope:
    document: Mapping[str, object]

    def as_dict(self) -> dict[str, object]:
        return dict(self.document)


def _safe_headers(value: Mapping[str, object]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, raw in value.items():
        lowered = str(key).lower()
        if lowered not in _SAFE_RESPONSE_HEADERS:
            continue
        rendered = str(raw)
        if len(rendered) <= 4096 and "\r" not in rendered and "\n" not in rendered:
            result[lowered] = rendered
    return result


def _bounded_content(
    response: object,
    limit: int,
    *,
    deadline: Optional[float] = None,
) -> bytes:
    iterator = getattr(response, "iter_content", None)
    if callable(iterator):
        chunks: list[bytes] = []
        size = 0
        for raw_chunk in iterator(chunk_size=64 * 1024):
            if deadline is not None and time.monotonic() >= deadline:
                raise GatewayError("operation_deadline_exceeded", http_status=504)
            chunk = bytes(raw_chunk or b"")
            size += len(chunk)
            if size > limit:
                raise GatewayError("source_body_too_large", http_status=413)
            chunks.append(chunk)
        return b"".join(chunks)
    content = bytes(getattr(response, "content", b"") or b"")
    if deadline is not None and time.monotonic() >= deadline:
        raise GatewayError("operation_deadline_exceeded", http_status=504)
    if len(content) > limit:
        raise GatewayError("source_body_too_large", http_status=413)
    return content


# The paid HTTP fetch impersonates a real browser's TLS/HTTP fingerprint via
# curl_cffi (already pinned in the image, and the fingerprint the direct
# transport uses).  A browser fingerprint is challenged by Cloudflare far less
# than plain `requests`, so fewer paid fetches escalate to the expensive
# FlareSolverr browser bootstrap, and libcurl negotiates br/zstd so the billed
# wire bytes shrink.  curl_cffi honours session-level proxies (verified: a get()
# with no per-call proxy still routes through session.proxies), so the lease
# proxy is never bypassed.  The Response API (url/status_code/headers/content/
# iter_content/close) matches what the fetch path and _bounded_content read.
_PAID_HTTP_IMPERSONATE = "chrome120"


def _new_paid_http_session(proxy_url: str) -> Any:
    from curl_cffi.requests import Session as CurlSession

    return CurlSession(
        impersonate=_PAID_HTTP_IMPERSONATE,
        trust_env=False,
        proxies={"http": proxy_url, "https": proxy_url},
    )


def _new_direct_http_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.proxies = {}
    return session


def _source_headers(request: GatewayFetchRequest) -> dict[str, str]:
    if not request.browser_bootstrap_url:
        return {}
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": request.browser_bootstrap_url,
    }


class _PaidCapabilitySession(requests.Session):
    """Sign exact FlareSolverr request bytes for the paid-exclusive instance."""

    def __init__(self, *, secret: str, instance_id: str) -> None:
        super().__init__()
        self.trust_env = False
        self._secret = secret
        self._instance_id = instance_id

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        if "data" in kwargs:
            raise ValueError("paid FlareSolverr session accepts JSON only")
        payload = kwargs.pop("json", None)
        body = json.dumps(payload, ensure_ascii=True, allow_nan=False).encode("utf-8")
        parts = urlsplit(url)
        supplied_headers = dict(kwargs.pop("headers", {}) or {})
        capability_names = {
            name.lower()
            for name in build_paid_gateway_capability_headers(
                self._secret,
                instance_id=self._instance_id,
                method="POST",
                path=parts.path,
                query_string=parts.query,
                body=body,
            )
        }
        if any(str(name).lower() in capability_names for name in supplied_headers):
            raise ValueError("paid FlareSolverr capability headers are gateway-owned")
        supplied_headers.update(
            build_paid_gateway_capability_headers(
                self._secret,
                instance_id=self._instance_id,
                method="POST",
                path=parts.path,
                query_string=parts.query,
                body=body,
            )
        )
        supplied_headers.setdefault("Content-Type", "application/json")
        return super().post(
            url,
            data=body,
            headers=supplied_headers,
            **kwargs,
        )


class PaidExclusiveFlareSolverrClient:
    """Bind one gateway process to one attested paid-exclusive FS process."""

    _IDENTITY_PATH = "/v1/whoscored/runtime-identity"

    def __init__(
        self,
        url: str,
        *,
        secret: str,
        expected_extension_sha256: str,
        identity_session: Optional[requests.Session] = None,
    ) -> None:
        origin = url.rstrip("/")
        owns_probe = identity_session is None
        probe = identity_session or requests.Session()
        probe.trust_env = False
        try:
            response = probe.get(f"{origin}{self._IDENTITY_PATH}", timeout=10.0)
            response.raise_for_status()
            identity = response.json()
        except Exception:
            raise GatewayError("flaresolverr_identity_unavailable") from None
        finally:
            if owns_probe:
                probe.close()
        expected_fields = {
            "status",
            "version",
            "extension_sha256",
            "paid_exclusive",
            "capability_schema",
            "capability_instance_id",
        }
        instance_id = (
            identity.get("capability_instance_id")
            if isinstance(identity, Mapping)
            else None
        )
        if (
            not isinstance(identity, Mapping)
            or set(identity) != expected_fields
            or identity.get("status") != "ok"
            or identity.get("version") != PINNED_FLARESOLVERR_VERSION
            or identity.get("extension_sha256") != expected_extension_sha256
            or identity.get("paid_exclusive") is not True
            or identity.get("capability_schema") != PAID_GATEWAY_CAPABILITY_SCHEMA
            or type(instance_id) is not str
            or re.fullmatch(r"[0-9a-f]{32}", instance_id) is None
        ):
            raise GatewayError("flaresolverr_identity_mismatch")
        client = FlareSolverrClient(url=origin)
        client.session = _PaidCapabilitySession(secret=secret, instance_id=instance_id)
        self._client = client

    def create_session(
        self,
        session_id: str,
        proxy_url: Optional[str] = None,
        *,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        previous = self._client.default_timeout
        if timeout_seconds is not None:
            self._client.default_timeout = max(0.001, float(timeout_seconds))
        try:
            self._client.create_session(session_id, proxy_url=proxy_url)
        finally:
            self._client.default_timeout = previous

    def destroy_session_strict(
        self,
        session_id: str,
        *,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        previous = self._client.default_timeout
        if timeout_seconds is not None:
            self._client.default_timeout = max(0.001, float(timeout_seconds))
        try:
            self._client.destroy_session_strict(session_id)
        finally:
            self._client.default_timeout = previous

    def get(
        self,
        url: str,
        session_id: str,
        max_timeout_ms: Optional[int] = None,
        return_only_cookies: bool = False,
        disable_media: bool = False,
    ) -> dict[str, Any]:
        """Fixed request.get wrapper which preserves the exact final URL."""

        timeout_ms = max_timeout_ms or self._client.default_max_timeout_ms
        payload: dict[str, Any] = {
            "cmd": "request.get",
            "url": url,
            "session": session_id,
            "maxTimeout": timeout_ms,
        }
        if return_only_cookies:
            payload["returnOnlyCookies"] = True
        if disable_media:
            payload["disableMedia"] = True
        data = self._client._post(
            payload,
            timeout=timeout_ms / 1000.0 + 5.0,
        )
        solution = data.get("solution")
        if not isinstance(solution, Mapping):
            raise FlareSolverrError("paid browser solution is invalid")
        final_url = solution.get("url")
        if type(final_url) is not str or final_url != url:
            raise FlareSolverrError("paid browser final URL differs")
        return {
            "html": solution.get("response", ""),
            "cookies": solution.get("cookies", []),
            "userAgent": solution.get("userAgent", ""),
            "status": solution.get("status", 0),
            "finalUrl": final_url,
        }

    def xhr_get(
        self,
        url: str,
        session_id: str,
        max_timeout_ms: Optional[int] = None,
    ) -> dict[str, Any]:
        timeout_ms = max_timeout_ms or self._client.default_max_timeout_ms
        data = self._client._post(
            {
                "url": url,
                "session": session_id,
                "maxTimeout": timeout_ms,
            },
            timeout=timeout_ms / 1000.0 + 5.0,
            endpoint_path="/v1/xhr",
        )
        solution = data.get("solution") or {}
        decoded = self._client._decode_xhr_solution(solution, expected_url=url)
        self._client._record_url_request(url, self._client._last_post_bytes)
        return decoded

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def close(self) -> None:
        self._client.close()


class PaidGatewayService:
    """Pure high-level fetch service with injectable, testable side effects."""

    def __init__(
        self,
        *,
        proxy_client: _LeaseClient,
        browser_client: _BrowserClient,
        authority: Callable[
            [Mapping[str, object]],
            tuple[ProxyCampaignApproval, ProxyWorkAllocation, str],
        ] = assert_paid_runtime_authority_available,
        campaign_authority: Optional[Callable[..., ProxyCampaignApproval]] = None,
        direct_session_factory: Callable[[], Any] = _new_direct_http_session,
        http_session_factory: Callable[[str], Any] = _new_paid_http_session,
        lease_ttl_seconds: int = 60,
        alert_delivery: Optional[Callable[..., Mapping[str, object]]] = None,
        alert_requirement: Optional[Callable[..., Mapping[str, object]]] = None,
    ) -> None:
        if (
            isinstance(lease_ttl_seconds, bool)
            or not isinstance(lease_ttl_seconds, int)
            or not 1 <= lease_ttl_seconds <= 60
        ):
            raise ValueError("gateway lease TTL must be in 1..60 seconds")
        self.proxy_client = proxy_client
        self.browser_client = browser_client
        self.authority = authority
        authoritative_default = authority is assert_paid_runtime_authority_available
        if campaign_authority is not None:
            self.campaign_authority = campaign_authority
        elif authoritative_default:
            self.campaign_authority = assert_paid_campaign_authority_available
        else:
            self.campaign_authority = lambda context, **_kwargs: (
                ProxyCampaignApproval.from_dict(context.get("proxy_campaign_approval"))
            )
        self.direct_session_factory = direct_session_factory
        self.http_session_factory = http_session_factory
        self.lease_ttl_seconds = lease_ttl_seconds
        # ProxyFilterClient and the paid-exclusive FlareSolverr client both own
        # mutable requests.Session state. One service operation owns them at a
        # time; queue wait is charged to the caller's single deadline.
        self._operation_lock = threading.Lock()
        self.alert_delivery = alert_delivery or (
            ensure_paid_alert_delivery
            if authoritative_default
            else lambda **_kwargs: {}
        )
        self.alert_requirement = alert_requirement or (
            require_paid_alert_delivery
            if authoritative_default
            else lambda **_kwargs: {}
        )

    @staticmethod
    def _remaining_seconds(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise GatewayError("operation_deadline_exceeded", http_status=504)
        return remaining

    @classmethod
    def _browser_timeout_ms(cls, deadline: float) -> int:
        remaining = cls._remaining_seconds(deadline)
        # Leave a bounded window for FlareSolverr's HTTP response envelope;
        # its in-browser maxTimeout is therefore always inside the same
        # gateway deadline instead of extending it.
        if remaining <= 5.0:
            raise GatewayError("operation_deadline_exceeded", http_status=504)
        return max(1, int((remaining - 5.0) * 1000))

    def _fresh_direct_recheck(
        self, request: GatewayFetchRequest, *, deadline: float
    ) -> None:
        """Require fresh direct CF proof before creating a paid lease."""

        session: Any = None
        response: Any = None
        pending: Optional[GatewayError] = None
        cleanup_failed = False
        try:
            session = self.direct_session_factory()
            response = session.get(
                request.url,
                timeout=self._remaining_seconds(deadline),
                headers=_source_headers(request),
                allow_redirects=False,
                stream=True,
            )
            final_url = getattr(response, "url", request.url)
            if type(final_url) is not str or final_url != request.url:
                raise GatewayError("fresh_direct_target_mismatch", http_status=409)
            content = _bounded_content(
                response,
                request.max_response_bytes,
                deadline=deadline,
            )
            status = int(getattr(response, "status_code", 0) or 0)
            headers = dict(getattr(response, "headers", {}) or {})
            if not (
                is_cloudflare_response(status, headers, content)
                or is_whoscored_structured_feed_access_gate(
                    request.url, status, content, headers
                )
            ):
                raise GatewayError("fresh_direct_not_cloudflare", http_status=409)
        except GatewayError as exc:
            pending = exc
        except Exception:
            pending = GatewayError("fresh_direct_recheck_failed", http_status=409)
        finally:
            close_response = getattr(response, "close", None)
            if callable(close_response):
                try:
                    close_response()
                except Exception:
                    cleanup_failed = True
            close_session = getattr(session, "close", None)
            if callable(close_session):
                try:
                    close_session()
                except Exception:
                    cleanup_failed = True
        if cleanup_failed:
            raise GatewayError("fresh_direct_cleanup_failed")
        if pending is not None:
            raise pending

    @staticmethod
    def _validate_target(
        approval: ProxyCampaignApproval,
        allocation: ProxyWorkAllocation,
        url: str,
    ) -> None:
        if not approval.allows_url(url, allocation_id=allocation.allocation_id):
            raise GatewayError("target_not_allowed", http_status=403)

    @staticmethod
    def _receipt(
        *,
        request: GatewayFetchRequest,
        approval: ProxyCampaignApproval,
        allocation: ProxyWorkAllocation,
        attempt_id: str,
        lease: ProxyLease,
        route: TransportRoute,
        stats: Mapping[str, object],
    ) -> PaidGatewayReceipt:
        def counter(field: str) -> int:
            value = stats.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise GatewayError("accounting_invalid")
            return value

        if stats.get("id") != lease.lease_id:
            raise GatewayError("accounting_invalid")
        reported_url = stats.get("canonical_url")
        if (
            type(reported_url) is not str
            or _canonical_url_key(reported_url) != _canonical_url_key(request.url)
            or stats.get("close_complete") is not True
        ):
            raise GatewayError("accounting_invalid")
        up = counter("up_bytes")
        down = counter("down_bytes")
        total = counter("total_bytes")
        provider_billed = counter("provider_billed_bytes")
        if (
            total != up + down
            or provider_billed != total
            or total > request.max_provider_bytes
            or isinstance(lease.max_bytes, bool)
            or not isinstance(lease.max_bytes, int)
            or lease.max_bytes <= 0
            or total > lease.max_bytes
        ):
            raise GatewayError("accounting_invalid")
        return PaidGatewayReceipt(
            campaign_id=approval.campaign_id,
            approval_id=approval.approval_id,
            approval_sha256=approval.approval_sha256,
            allocation_id=allocation.allocation_id,
            attempt_id_hash=hashlib.sha256(attempt_id.encode("utf-8")).hexdigest(),
            canonical_url_sha256=hashlib.sha256(
                _canonical_url_key(request.url).encode("utf-8")
            ).hexdigest(),
            lease_id_hash=hashlib.sha256(lease.lease_id.encode("utf-8")).hexdigest(),
            route=route,
            up_bytes=up,
            down_bytes=down,
            total_bytes=total,
            provider_billed_bytes=provider_billed,
            close_complete=True,
            cleanup_complete=True,
        )

    @staticmethod
    def _alert_identity(
        approval: ProxyCampaignApproval,
        context: Mapping[str, object],
    ) -> dict[str, str]:
        dag_id = str(context.get("dag_id") or "")
        return {
            "campaign_id": approval.campaign_id,
            "approval_id": approval.approval_id,
            "approval_sha256": approval.approval_sha256,
            "dag_id": dag_id,
            "run_id": str(context.get("run_id") or ""),
            "alert_task_id": paid_alert_task_id_for_dag(dag_id),
        }

    def preflight_alert(self, context: Mapping[str, object]) -> Mapping[str, object]:
        """Authorise and deliver one idempotent gateway-owned alert."""

        try:
            approval = self.campaign_authority(
                context,
                require_active=True,
                enforce_release_gates=True,
            )
            identity = self._alert_identity(approval, context)
        except Exception:
            raise GatewayError("authority_rejected", http_status=403) from None
        if not self._operation_lock.acquire(timeout=MAX_GATEWAY_TIMEOUT_MS / 1000.0):
            raise GatewayError("operation_deadline_exceeded", http_status=504)
        try:
            try:
                return self.alert_delivery(**identity)
            except Exception:
                raise GatewayError("alert_delivery_failed", http_status=409) from None
        finally:
            self._operation_lock.release()

    def campaign_control(
        self,
        *,
        operation: str,
        context: Mapping[str, object],
        arguments: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Proxy one bounded authenticated ledger operation to the filter."""

        expected_arguments = PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS.get(operation)
        if expected_arguments is None or frozenset(arguments) != expected_arguments:
            raise GatewayError("invalid_campaign_operation", http_status=400)
        require_active = operation not in {
            "seal_for_reconciliation",
            "sealed_snapshot",
        }
        try:
            approval = self.campaign_authority(
                context,
                require_active=require_active,
                enforce_release_gates=False,
            )
        except Exception:
            raise GatewayError("authority_rejected", http_status=403) from None
        if operation in {"complete_allocation", "seal_for_reconciliation"} and (
            arguments.get("dag_id") != approval.allowed_dag_ids[0]
            or arguments.get("run_id") != approval.run_id
        ):
            raise GatewayError("authority_rejected", http_status=403)
        if not self._operation_lock.acquire(timeout=MAX_GATEWAY_TIMEOUT_MS / 1000.0):
            raise GatewayError("operation_deadline_exceeded", http_status=504)
        try:
            try:
                result = self.proxy_client.campaign_control(
                    operation,
                    context=_CampaignContextEnvelope(context),
                    arguments=arguments,
                )
            except Exception:
                raise GatewayError(
                    "campaign_control_rejected", http_status=409
                ) from None
        finally:
            self._operation_lock.release()
        if frozenset(result) != PROXY_CAMPAIGN_CONTROL_RESULT_FIELDS[operation]:
            raise GatewayError("campaign_control_invalid")
        return result

    def fetch(self, request: GatewayFetchRequest) -> PaidGatewayResponse:
        deadline = time.monotonic() + request.timeout_ms / 1000.0
        if not self._operation_lock.acquire(timeout=self._remaining_seconds(deadline)):
            raise GatewayError("operation_deadline_exceeded", http_status=504)
        try:
            return self._fetch_locked(request, deadline=deadline)
        finally:
            self._operation_lock.release()

    def _fetch_locked(
        self, request: GatewayFetchRequest, *, deadline: float
    ) -> PaidGatewayResponse:
        # This existing shared API authenticates approval/allocation/attempt
        # and enforces every code-owned release sentinel before any side effect.
        try:
            approval, allocation, attempt_id = self.authority(request.context)
        except Exception:
            raise GatewayError("authority_rejected", http_status=403) from None
        self._validate_target(approval, allocation, request.url)
        if request.browser_bootstrap_url is not None:
            self._validate_target(approval, allocation, request.browser_bootstrap_url)
        if request.max_provider_bytes > allocation.budget_bytes:
            raise GatewayError("budget_rejected", http_status=429)

        try:
            self.alert_requirement(**self._alert_identity(approval, request.context))
        except Exception:
            raise GatewayError("alert_preflight_required", http_status=409) from None

        self._fresh_direct_recheck(request, deadline=deadline)

        canonical_url = _canonical_url_key(request.url)
        remaining = self._remaining_seconds(deadline)
        if remaining < 1.0:
            raise GatewayError("operation_deadline_exceeded", http_status=504)
        lease_ttl = min(
            self.lease_ttl_seconds,
            int(remaining),
        )
        original_proxy_timeout = getattr(self.proxy_client, "timeout", None)
        bounded_proxy_timeout = isinstance(
            original_proxy_timeout, (int, float)
        ) and not isinstance(original_proxy_timeout, bool)
        if bounded_proxy_timeout:
            self.proxy_client.timeout = min(  # type: ignore[attr-defined]
                float(original_proxy_timeout), remaining
            )
        try:
            lease = self.proxy_client.create_lease(
                max_bytes=request.max_provider_bytes,
                ttl_seconds=lease_ttl,
                context=_CampaignContextEnvelope(request.context),
                canonical_url=canonical_url,
            )
        except (ProxyBudgetRejected, ProxyConcurrencyLimited):
            raise GatewayError("budget_rejected", http_status=429) from None
        except Exception:
            raise GatewayError("lease_create_failed") from None
        finally:
            if bounded_proxy_timeout:
                self.proxy_client.timeout = original_proxy_timeout  # type: ignore[attr-defined]

        route = TransportRoute.PAID_HTTP
        result_content: Optional[bytes] = None
        result_status: Optional[int] = None
        result_headers: dict[str, str] = {}
        paid_http: Any = None
        raw_response: Any = None
        browser_session_id = "ws-gw-" + secrets.token_hex(16)
        browser_session_attempted = False
        pending_error: Optional[GatewayError] = None
        cleanup_error = False
        lease_stats: Optional[Mapping[str, object]] = None
        try:
            paid_http = self.http_session_factory(lease.proxy_url)
            raw_response = paid_http.get(
                request.url,
                timeout=self._remaining_seconds(deadline),
                headers=_source_headers(request),
                allow_redirects=False,
                stream=True,
            )
            final_url = getattr(raw_response, "url", request.url)
            if type(final_url) is not str or final_url != request.url:
                raise GatewayError("paid_http_target_mismatch")
            direct_content = _bounded_content(
                raw_response,
                request.max_response_bytes,
                deadline=deadline,
            )
            direct_status = int(getattr(raw_response, "status_code", 0) or 0)
            direct_headers = dict(getattr(raw_response, "headers", {}) or {})
            if is_cloudflare_response(
                direct_status, direct_headers, direct_content
            ) or is_whoscored_structured_feed_access_gate(
                request.url,
                direct_status,
                direct_content,
                direct_headers,
            ):
                close_response = getattr(raw_response, "close", None)
                if callable(close_response):
                    close_response()
                raw_response = None
                close_http = getattr(paid_http, "close", None)
                if callable(close_http):
                    close_http()
                paid_http = None
                route = TransportRoute.PAID_FLARESOLVERR
                browser_session_attempted = True
                self.browser_client.create_session(
                    browser_session_id,
                    proxy_url=lease.proxy_url,
                    timeout_seconds=self._remaining_seconds(deadline),
                )
                if request.browser_bootstrap_url:
                    bootstrap = self.browser_client.get(
                        request.browser_bootstrap_url,
                        browser_session_id,
                        max_timeout_ms=self._browser_timeout_ms(deadline),
                        disable_media=True,
                    )
                    bootstrap_content = str(bootstrap.get("html") or "").encode("utf-8")
                    if (
                        len(bootstrap_content) > MAX_PAID_GATEWAY_RESPONSE_BYTES
                        or is_chromium_error_page(
                            bootstrap_content.decode("utf-8", errors="ignore")
                        )
                        or not 200 <= int(bootstrap.get("status") or 0) < 300
                        or bootstrap.get("finalUrl") != request.browser_bootstrap_url
                    ):
                        raise GatewayError("browser_bootstrap_failed")
                    solution = self.browser_client.xhr_get(
                        request.url,
                        browser_session_id,
                        max_timeout_ms=self._browser_timeout_ms(deadline),
                    )
                    result_content = bytes(solution.get("content") or b"")
                    result_status = int(solution.get("status") or 0)
                    result_headers = _safe_headers(dict(solution.get("headers") or {}))
                    if solution.get("finalUrl") != request.url:
                        raise GatewayError("browser_target_mismatch")
                else:
                    solution = self.browser_client.get(
                        request.url,
                        browser_session_id,
                        max_timeout_ms=self._browser_timeout_ms(deadline),
                        disable_media=True,
                    )
                    result_content = str(solution.get("html") or "").encode("utf-8")
                    result_status = int(solution.get("status") or 0)
                    result_headers = {}
                    if (
                        is_chromium_error_page(
                            result_content.decode("utf-8", errors="ignore")
                        )
                        or solution.get("finalUrl") != request.url
                    ):
                        raise FlareSolverrErrorPage(
                            "browser returned an invalid target page"
                        )
            else:
                result_content = direct_content
                result_status = direct_status
                result_headers = _safe_headers(direct_headers)
            if (
                result_content is None
                or len(result_content) > request.max_response_bytes
            ):
                raise GatewayError("source_body_too_large", http_status=413)
            if result_status is None or not 100 <= result_status <= 599:
                raise GatewayError("source_status_invalid")
        except GatewayError as exc:
            pending_error = exc
        except FlareSolverrError:
            pending_error = GatewayError("browser_fetch_failed")
        except Exception:
            pending_error = GatewayError("source_fetch_failed")
        finally:
            close_response = getattr(raw_response, "close", None)
            if callable(close_response):
                try:
                    close_response()
                except Exception:
                    cleanup_error = True
            close_http = getattr(paid_http, "close", None)
            if callable(close_http):
                try:
                    close_http()
                except Exception:
                    cleanup_error = True
            if browser_session_attempted:
                try:
                    self.browser_client.destroy_session_strict(
                        browser_session_id,
                        timeout_seconds=self._remaining_seconds(
                            deadline + GATEWAY_CLEANUP_GRACE_SECONDS
                        ),
                    )
                except Exception:
                    cleanup_error = True
            close_proxy_timeout = None
            close_timeout_bounded = False
            try:
                cleanup_remaining = self._remaining_seconds(
                    deadline + GATEWAY_CLEANUP_GRACE_SECONDS
                )
                close_proxy_timeout = getattr(self.proxy_client, "timeout", None)
                close_timeout_bounded = isinstance(
                    close_proxy_timeout, (int, float)
                ) and not isinstance(close_proxy_timeout, bool)
                if close_timeout_bounded:
                    self.proxy_client.timeout = min(  # type: ignore[attr-defined]
                        float(close_proxy_timeout), cleanup_remaining
                    )
                lease_stats = self.proxy_client.close(lease)
            except Exception:
                cleanup_error = True
            finally:
                if close_timeout_bounded:
                    self.proxy_client.timeout = close_proxy_timeout  # type: ignore[attr-defined]

        # Accounting/cleanup takes precedence over a useful source body.  No
        # receipt or body crosses the boundary until every capability is gone.
        if cleanup_error or lease_stats is None:
            raise GatewayError("cleanup_failed")
        receipt = self._receipt(
            request=request,
            approval=approval,
            allocation=allocation,
            attempt_id=attempt_id,
            lease=lease,
            route=route,
            stats=lease_stats,
        )
        if pending_error is not None:
            raise SettledGatewayError(pending_error, receipt)
        if result_content is None or result_status is None:
            raise GatewayError("source_result_missing")
        return PaidGatewayResponse(
            url=request.url,
            content=result_content,
            status_code=result_status,
            headers=result_headers,
            route=route,
            receipt=receipt,
        )

    def close(self) -> None:
        browser_close = getattr(self.browser_client, "close", None)
        if callable(browser_close):
            browser_close()
        proxy_close = getattr(self.proxy_client, "close_session", None)
        if callable(proxy_close):
            proxy_close()


class PaidGatewayApplication:
    def __init__(self, *, token: str, service: PaidGatewayService) -> None:
        if (
            type(token) is not str
            or not 32 <= len(token) <= 512
            or any(
                ord(character) < 0x21 or ord(character) > 0x7E for character in token
            )
        ):
            raise ValueError(
                "paid gateway token must be 32..512 printable ASCII characters"
            )
        self._token = token
        self.service = service

    def _authorized(self, authorization: str) -> bool:
        expected = f"Bearer {self._token}"
        return bool(
            type(authorization) is str
            and len(authorization) == len(expected)
            and all(0x20 <= ord(character) <= 0x7E for character in authorization)
            and hmac.compare_digest(authorization, expected)
        )

    def handle(self, *, authorization: str, body: bytes) -> tuple[int, bytes]:
        if not self._authorized(authorization):
            return self._error(401, "authentication_required")
        if not 0 < len(body) <= MAX_GATEWAY_REQUEST_BYTES:
            return self._error(413, "invalid_request")
        try:
            value = strict_json_loads(body.decode("utf-8"))
            request = GatewayFetchRequest.from_dict(value)
            response = self.service.fetch(request)
            document = {
                "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
                "url": response.url,
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "body_base64": base64.b64encode(response.content).decode("ascii"),
                "body_sha256": hashlib.sha256(response.content).hexdigest(),
                "route": response.route.value,
                "receipt": response.receipt.to_dict(),
            }
            return 200, json.dumps(
                document,
                ensure_ascii=True,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
        except SettledGatewayError as exc:
            return self._error(
                exc.http_status,
                exc.code,
                receipt=exc.receipt,
            )
        except GatewayError as exc:
            return self._error(exc.http_status, exc.code)
        except Exception:
            return self._error(400, "invalid_request")

    def handle_preflight_alert(
        self, *, authorization: str, body: bytes
    ) -> tuple[int, bytes]:
        if not self._authorized(authorization):
            return self._error(401, "authentication_required")
        if not 0 < len(body) <= MAX_GATEWAY_REQUEST_BYTES:
            return self._error(413, "invalid_request")
        try:
            value = strict_json_loads(body.decode("utf-8"))
            if (
                not isinstance(value, Mapping)
                or frozenset(value) != _PREFLIGHT_REQUEST_FIELDS
                or value.get("schema_version") != PAID_GATEWAY_SCHEMA_VERSION
            ):
                raise GatewayError("invalid_request", http_status=400)
            context = value.get("context")
            if (
                not isinstance(context, Mapping)
                or frozenset(context) != PROXY_CAMPAIGN_AUTHORITY_CONTEXT_FIELDS
            ):
                raise GatewayError("invalid_context", http_status=403)
            delivered = self.service.preflight_alert(dict(context))
            document = {
                "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
                "status": "delivered",
                "campaign_id": str(delivered.get("campaign_id") or ""),
                "approval_id": str(delivered.get("approval_id") or ""),
                "approval_sha256": str(delivered.get("approval_sha256") or ""),
            }
            return 200, json.dumps(
                document, sort_keys=True, separators=(",", ":")
            ).encode("ascii")
        except GatewayError as exc:
            return self._error(exc.http_status, exc.code)
        except Exception:
            return self._error(400, "invalid_request")

    def handle_campaign_control(
        self, *, authorization: str, body: bytes
    ) -> tuple[int, bytes]:
        if not self._authorized(authorization):
            return self._error(401, "authentication_required")
        if not 0 < len(body) <= MAX_GATEWAY_REQUEST_BYTES:
            return self._error(413, "invalid_request")
        try:
            value = strict_json_loads(body.decode("utf-8"))
            if (
                not isinstance(value, Mapping)
                or frozenset(value) != _CAMPAIGN_CONTROL_REQUEST_FIELDS
                or value.get("schema_version") != PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION
            ):
                raise GatewayError("invalid_request", http_status=400)
            operation = value.get("operation")
            context = value.get("context")
            arguments = value.get("arguments")
            expected_arguments = PROXY_CAMPAIGN_CONTROL_ARGUMENT_FIELDS.get(operation)
            if (
                expected_arguments is None
                or not isinstance(context, Mapping)
                or frozenset(context) != PROXY_CAMPAIGN_AUTHORITY_CONTEXT_FIELDS
                or not isinstance(arguments, Mapping)
                or frozenset(arguments) != expected_arguments
            ):
                raise GatewayError("invalid_request", http_status=400)
            result = self.service.campaign_control(
                operation=operation,
                context=dict(context),
                arguments=dict(arguments),
            )
            document = {
                "schema_version": PROXY_CAMPAIGN_CONTROL_SCHEMA_VERSION,
                "operation": operation,
                "result": dict(result),
            }
            return 200, json.dumps(
                document,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except GatewayError as exc:
            return self._error(exc.http_status, exc.code)
        except Exception:
            return self._error(400, "invalid_request")

    @staticmethod
    def _error(
        status: int,
        code: str,
        *,
        receipt: Optional[PaidGatewayReceipt] = None,
    ) -> tuple[int, bytes]:
        document = {
            "schema_version": PAID_GATEWAY_SCHEMA_VERSION,
            "error": {"code": code},
        }
        if receipt is not None:
            document["receipt"] = receipt.to_dict()
        return status, json.dumps(
            document, sort_keys=True, separators=(",", ":")
        ).encode("ascii")

    @staticmethod
    def health() -> tuple[int, bytes]:
        """Side-effect-free Compose probe containing no runtime identity."""

        return 200, b'{"schema_version":1,"status":"ok"}'


class BoundedGatewayServer(ThreadingHTTPServer):
    """Small fixed worker boundary; excess/slow clients cannot grow threads."""

    daemon_threads = True
    request_queue_size = 4

    def __init__(
        self,
        server_address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        *,
        max_workers: int = 4,
    ) -> None:
        if not 1 <= max_workers <= 8:
            raise ValueError("gateway worker limit must be in 1..8")
        self._worker_slots = threading.BoundedSemaphore(max_workers)
        super().__init__(server_address, handler)

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if not self._worker_slots.acquire(blocking=False):
            try:
                request.settimeout(1.0)
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\n"
                    b"Connection: close\r\nContent-Length: 0\r\n\r\n"
                )
            except OSError:
                pass
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._worker_slots.release()
            raise

    def process_request_thread(
        self, request: socket.socket, client_address: Any
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._worker_slots.release()


def _handler(application: PaidGatewayApplication) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "WhoScoredPaidGateway/1"
        sys_version = ""

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(10.0)

        def do_GET(self) -> None:  # noqa: N802 - stdlib callback name
            if self.path != "/health":
                self.send_error(404)
                return
            status, response = application.health()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(response)

        def do_POST(self) -> None:  # noqa: N802 - stdlib callback name
            if self.path not in {
                "/v1/fetch",
                "/v1/preflight-alert",
                "/v1/campaign-control",
            }:
                self.send_error(404)
                return
            if self.headers.get("Transfer-Encoding"):
                self.send_error(400)
                return
            raw_lengths = self.headers.get_all("Content-Length", failobj=[])
            if (
                len(raw_lengths) != 1
                or not raw_lengths[0].isascii()
                or not raw_lengths[0].isdigit()
                or raw_lengths[0] != str(int(raw_lengths[0] or "0"))
            ):
                self.send_error(411)
                return
            length = int(raw_lengths[0])
            if not 0 < length <= MAX_GATEWAY_REQUEST_BYTES:
                self.send_error(413)
                return
            body = _read_exact_body(
                self.rfile,
                length,
                deadline=time.monotonic() + MAX_GATEWAY_BODY_READ_SECONDS,
                timeout_setter=self.connection.settimeout,
            )
            if body is None:
                self.send_error(400)
                return
            handler = {
                "/v1/fetch": application.handle,
                "/v1/preflight-alert": application.handle_preflight_alert,
                "/v1/campaign-control": application.handle_campaign_control,
            }[self.path]
            status, response = handler(
                authorization=self.headers.get("Authorization", ""), body=body
            )
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, format: str, *args: object) -> None:
            # Source URLs and campaign context are intentionally never logged.
            return

    return Handler


def _read_exact_body(
    stream: Any,
    length: int,
    *,
    deadline: Optional[float] = None,
    timeout_setter: Optional[Callable[[float], None]] = None,
) -> Optional[bytes]:
    """Read one declared HTTP body or reject a truncated peer."""

    chunks: list[bytes] = []
    remaining = length
    reader = getattr(stream, "read1", None)
    if not callable(reader):
        reader = stream.read
    while remaining:
        if deadline is not None:
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                return None
            if timeout_setter is not None:
                timeout_setter(timeout)
        try:
            chunk = reader(min(remaining, 64 * 1024))
        except (OSError, TimeoutError):
            return None
        if not isinstance(chunk, bytes) or not chunk or len(chunk) > remaining:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8898)
    parser.add_argument("--proxy-url", required=True)
    parser.add_argument("--proxy-control-url", required=True)
    parser.add_argument("--flaresolverr-url", required=True)
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    if not 1 <= args.port <= 65535:
        raise SystemExit("--port must be in 1..65535")
    require_production_runtime_class(operation="WhoScored paid application gateway")
    token = os.environ.get(PAID_GATEWAY_TOKEN_ENV, "")
    fs_secret = os.environ.get(FLARESOLVERR_GATEWAY_SECRET_ENV, "")
    expected_extension_sha256 = attested_runtime_file_sha256(
        "scripts/flaresolverr_extended.py"
    )
    browser = PaidExclusiveFlareSolverrClient(
        args.flaresolverr_url,
        secret=fs_secret,
        expected_extension_sha256=expected_extension_sha256,
    )
    proxy = ProxyFilterClient(
        args.proxy_url,
        control_url=args.proxy_control_url,
    )
    service = PaidGatewayService(proxy_client=proxy, browser_client=browser)
    application = PaidGatewayApplication(token=token, service=service)
    server = BoundedGatewayServer(
        (args.host, args.port), _handler(application), max_workers=4
    )
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()
        service.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
