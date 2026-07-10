"""Network transport for WhoScored with an explicitly bounded paid fallback.

The module deliberately owns *transport only*.  Parsers decide what valid
WhoScored content looks like and may pass a validator to :meth:`fetch`; a raw
store may be plugged in through :class:`RawCacheHook`, but this module does not
choose a storage backend.

The route order is fixed and fail-closed::

    raw cache -> direct curl -> direct FlareSolverr (fresh session) ->
    filtered paid curl -> filtered paid FlareSolverr (fresh session)

Paid traffic is reached only after the direct HTTP response and every direct
browser attempt were independently classified as Cloudflare challenges.  A
normal HTTP error, timeout, parser/validator rejection or browser crash never
turns a proxy on.
"""
from __future__ import annotations

import hashlib
import logging
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Optional, Protocol, runtime_checkable
from urllib.parse import quote, urlsplit, urlunsplit

import requests

from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrClient,
    FlareSolverrError,
    FlareSolverrErrorPage,
    FlareSolverrTabCrashed,
    FlareSolverrTimeout,
    is_chromium_error_page,
)

logger = logging.getLogger(__name__)


class TransportRoute(str, Enum):
    RAW_CACHE = "raw_cache"
    DIRECT_HTTP = "direct_http"
    DIRECT_FLARESOLVERR = "direct_flaresolverr"
    PAID_HTTP = "paid_http"
    PAID_FLARESOLVERR = "paid_flaresolverr"


class FailureKind(str, Enum):
    CLOUDFLARE = "cloudflare"
    HTTP_STATUS = "http_status"
    TIMEOUT = "timeout"
    BROWSER = "browser"
    CONTENT = "content"
    BUDGET = "budget"
    PROXY = "proxy"
    CACHE = "cache"
    CONFIG = "config"


class WhoScoredTransportError(RuntimeError):
    """Base typed transport failure surfaced to the ingest state machine."""

    def __init__(
        self,
        message: str,
        *,
        kind: FailureKind,
        url: str,
        route: Optional[TransportRoute] = None,
        status_code: Optional[int] = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.url = url
        self.route = route
        self.status_code = status_code
        self.retryable = retryable


class CloudflareChallenge(WhoScoredTransportError):
    def __init__(
        self,
        message: str,
        *,
        url: str,
        route: TransportRoute,
        status_code: Optional[int] = None,
    ) -> None:
        super().__init__(
            message,
            kind=FailureKind.CLOUDFLARE,
            url=url,
            route=route,
            status_code=status_code,
            retryable=True,
        )


class TransportBudgetExceeded(WhoScoredTransportError):
    def __init__(self, message: str, *, url: str, route: TransportRoute) -> None:
        super().__init__(
            message,
            kind=FailureKind.BUDGET,
            url=url,
            route=route,
            retryable=False,
        )


@dataclass(frozen=True)
class CachedPayload:
    """Raw cache value.  ``content`` must be the source response, not parsed data."""

    content: bytes
    status_code: int = 200
    headers: Mapping[str, str] = field(default_factory=dict)


@runtime_checkable
class RawCacheHook(Protocol):
    """Minimal content-addressed raw-store hook used by the transport."""

    def load(self, key: str) -> Optional[CachedPayload]:
        """Return a cached successful source response, or ``None``."""

    def store(self, key: str, payload: CachedPayload, sha256: str) -> None:
        """Persist a validated response before a caller writes parsed rows."""


@dataclass(frozen=True)
class TransportResponse:
    url: str
    content: bytes
    status_code: int
    headers: Mapping[str, str]
    route: TransportRoute
    wire_bytes: int
    sha256: str

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class TransportBudgets:
    """Per-transport hard limits; the proxy also enforces each lease server-side."""

    max_response_bytes: int = 4 * 1024 * 1024
    max_paid_bytes_per_url: int = 4 * 1024 * 1024
    max_paid_bytes_per_lease: int = 8 * 1024 * 1024
    max_paid_bytes_per_task: int = 24 * 1024 * 1024
    max_paid_urls: int = 20
    max_paid_browser_bootstraps: int = 2
    lease_ttl_seconds: int = 60

    @classmethod
    def for_eligible_urls(cls, eligible_urls: int, **overrides: int) -> "TransportBudgets":
        paid_limit = 0 if eligible_urls <= 0 else min(
            20, max(3, (eligible_urls + 49) // 50)
        )
        paid_limit = int(overrides.pop("max_paid_urls", paid_limit))
        return cls(max_paid_urls=paid_limit, **overrides)


@dataclass
class TransportStats:
    route_requests: Counter[str] = field(default_factory=Counter)
    route_wire_bytes: Counter[str] = field(default_factory=Counter)
    failures: Counter[str] = field(default_factory=Counter)
    cache_hits: int = 0
    cache_invalid: int = 0
    browser_sessions: int = 0
    paid_urls: set[str] = field(default_factory=set)
    paid_proxy_up_bytes: int = 0
    paid_proxy_down_bytes: int = 0
    paid_proxy_bytes_by_url: dict[str, int] = field(default_factory=dict)

    @property
    def paid_proxy_bytes(self) -> int:
        return self.paid_proxy_up_bytes + self.paid_proxy_down_bytes

    def as_dict(self) -> dict[str, Any]:
        return {
            "route_requests": dict(self.route_requests),
            "route_wire_bytes": dict(self.route_wire_bytes),
            "failures": dict(self.failures),
            "cache_hits": self.cache_hits,
            "cache_invalid": self.cache_invalid,
            "browser_sessions": self.browser_sessions,
            "paid_urls": len(self.paid_urls),
            "paid_proxy_up_bytes": self.paid_proxy_up_bytes,
            "paid_proxy_down_bytes": self.paid_proxy_down_bytes,
            "paid_proxy_bytes": self.paid_proxy_bytes,
            "paid_proxy_bytes_by_url": dict(self.paid_proxy_bytes_by_url),
        }


@dataclass(frozen=True)
class ProxyLease:
    lease_id: str
    token: str
    proxy_url: str
    max_bytes: int
    expires_at: float


class ProxyFilterClient:
    """Small client for the filtering proxy's lease control plane.

    No retry adapter is installed: lease creation is stateful and a hidden
    retry could allocate multiple paid exits.
    """

    def __init__(
        self,
        proxy_url: str,
        *,
        control_url: Optional[str] = None,
        timeout: float = 5.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.proxy_url = proxy_url.rstrip("/")
        self.control_url = (control_url or proxy_url).rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.trust_env = False

    def create_lease(self, *, max_bytes: int, ttl_seconds: int) -> ProxyLease:
        response = self.session.post(
            f"{self.control_url}/v1/leases",
            json={"max_bytes": max_bytes, "ttl_seconds": ttl_seconds},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        lease_id = str(data["id"])
        token = str(data["token"])
        lease_proxy_url = str(data.get("proxy_url") or self.proxy_url)
        return ProxyLease(
            lease_id=lease_id,
            token=token,
            proxy_url=_proxy_url_with_lease(lease_proxy_url, token),
            max_bytes=int(data.get("max_bytes", max_bytes)),
            expires_at=float(data.get("expires_at", time.time() + ttl_seconds)),
        )

    def stats(self, lease: ProxyLease) -> dict[str, Any]:
        response = self.session.get(
            f"{self.control_url}/v1/leases/{lease.lease_id}/stats",
            headers={"Authorization": f"Bearer {lease.token}"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return dict(response.json())

    def close(self, lease: ProxyLease) -> dict[str, Any]:
        response = self.session.delete(
            f"{self.control_url}/v1/leases/{lease.lease_id}",
            headers={"Authorization": f"Bearer {lease.token}"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return dict(response.json())


def _proxy_url_with_lease(proxy_url: str, token: str) -> str:
    parts = urlsplit(proxy_url)
    if not parts.scheme or not parts.hostname:
        raise ValueError(f"invalid filtering proxy URL: {proxy_url!r}")
    host = parts.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parts.port:
        host = f"{host}:{parts.port}"
    netloc = f"lease:{quote(token, safe='')}@{host}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


_CF_BODY_MARKERS = (
    "just a moment",
    "checking your browser",
    "cf-browser-verification",
    "challenge-platform",
    "challenge-running",
    "cf_chl_",
    "cf-chl-",
    "challenges.cloudflare.com",
    "turnstile",
)


def is_cloudflare_response(
    status_code: int,
    headers: Mapping[str, str],
    content: bytes | str,
) -> bool:
    """Classify a response narrowly enough that ordinary errors never buy traffic."""

    lowered_headers = {str(k).lower(): str(v).lower() for k, v in headers.items()}
    body = content.decode("utf-8", errors="ignore") if isinstance(content, bytes) else content
    body = body[:250_000].lower()
    marker = any(value in body for value in _CF_BODY_MARKERS)
    cf_header = "cf-ray" in lowered_headers or "cloudflare" in lowered_headers.get(
        "server", ""
    )
    if status_code == 200:
        return marker
    if status_code == 429:
        # A Cloudflare-fronted origin also puts ``server: cloudflare`` on a
        # normal rate-limit response.  Header-only 429s must back off, not buy
        # a residential request.
        return marker
    return status_code in (403, 503) and (marker or cf_header)


Validator = Callable[[TransportResponse], Optional[bool]]


class WhoScoredTransport:
    """Direct-first WhoScored fetcher with typed failure and traffic accounting."""

    def __init__(
        self,
        *,
        flaresolverr_url: str = "http://flaresolverr:8191",
        paid_proxy_url: Optional[str] = None,
        proxy_control_url: Optional[str] = None,
        raw_cache: Optional[RawCacheHook] = None,
        budgets: Optional[TransportBudgets] = None,
        request_timeout: float = 30.0,
        browser_timeout_ms: int = 60_000,
        direct_browser_attempts: int = 2,
        impersonate: str = "chrome120",
        direct_http_session: Any = None,
        direct_fs_client: Optional[FlareSolverrClient] = None,
        paid_fs_client: Optional[FlareSolverrClient] = None,
        proxy_client: Optional[ProxyFilterClient] = None,
        http_session_factory: Optional[Callable[[Optional[str]], Any]] = None,
    ) -> None:
        if direct_browser_attempts < 1:
            raise ValueError("direct_browser_attempts must be >= 1")
        self.raw_cache = raw_cache
        self.budgets = budgets or TransportBudgets()
        self.request_timeout = request_timeout
        self.browser_timeout_ms = browser_timeout_ms
        self.direct_browser_attempts = direct_browser_attempts
        self.impersonate = impersonate
        self.paid_proxy_url = paid_proxy_url
        self._http_session_factory = http_session_factory
        self._direct_http = direct_http_session or self._new_http_session(None)
        self._direct_fs = direct_fs_client or FlareSolverrClient(url=flaresolverr_url)
        self._paid_fs = paid_fs_client or FlareSolverrClient(url=flaresolverr_url)
        self._proxy_client = proxy_client or (
            ProxyFilterClient(paid_proxy_url, control_url=proxy_control_url)
            if paid_proxy_url
            else None
        )
        self.stats = TransportStats()
        self._paid_browser_bootstraps = 0

    def _new_http_session(self, proxy_url: Optional[str]) -> Any:
        if self._http_session_factory is not None:
            return self._http_session_factory(proxy_url)
        try:
            from curl_cffi.requests import Session
        except ImportError as exc:  # pragma: no cover - production dependency
            raise RuntimeError("curl_cffi is required for WhoScoredTransport") from exc
        session = Session(impersonate=self.impersonate)
        session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            }
        )
        if proxy_url:
            session.proxies = {"http": proxy_url, "https": proxy_url}
        return session

    def fetch(
        self,
        url: str,
        *,
        cache_key: Optional[str] = None,
        validator: Optional[Validator] = None,
    ) -> TransportResponse:
        """Fetch one logical URL through the fixed route state machine."""

        key = cache_key or url
        cached = self._load_cached(key, url, validator)
        if cached is not None:
            return cached

        try:
            direct = self._http_fetch(
                url, session=self._direct_http, route=TransportRoute.DIRECT_HTTP
            )
            self._validate(direct, validator)
        except CloudflareChallenge:
            self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
        except WhoScoredTransportError as exc:
            if exc.kind is FailureKind.CONTENT:
                self._store_response(key, direct)
            raise
        else:
            return self._store_and_return(key, direct)

        direct_cf_failures = 0
        last_cf: Optional[CloudflareChallenge] = None
        for _ in range(self.direct_browser_attempts):
            try:
                browser = self._browser_fetch(
                    url,
                    client=self._direct_fs,
                    route=TransportRoute.DIRECT_FLARESOLVERR,
                    proxy_url=None,
                )
                self._validate(browser, validator)
            except CloudflareChallenge as exc:
                direct_cf_failures += 1
                last_cf = exc
                self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                continue
            except WhoScoredTransportError as exc:
                # A tab crash, timeout, ordinary browser error or parser
                # rejection must never silently buy residential bandwidth.
                if exc.kind is FailureKind.CONTENT:
                    self._store_response(key, browser)
                raise
            return self._store_and_return(key, browser)

        if direct_cf_failures != self.direct_browser_attempts:
            assert last_cf is not None
            raise last_cf
        if self._proxy_client is None:
            assert last_cf is not None
            raise last_cf
        paid = self._paid_fetch(url, validator, cache_key=key)
        return self._store_and_return(key, paid)

    def _load_cached(
        self, key: str, url: str, validator: Optional[Validator]
    ) -> Optional[TransportResponse]:
        if self.raw_cache is None:
            return None
        try:
            payload = self.raw_cache.load(key)
        except Exception as exc:
            raise WhoScoredTransportError(
                f"raw cache load failed: {exc}",
                kind=FailureKind.CACHE,
                url=url,
                route=TransportRoute.RAW_CACHE,
            ) from exc
        if payload is None:
            return None
        response = self._response(
            url=url,
            content=payload.content,
            status_code=payload.status_code,
            headers=payload.headers,
            route=TransportRoute.RAW_CACHE,
            wire_bytes=0,
        )
        try:
            self._validate(response, validator)
        except WhoScoredTransportError as exc:
            self.stats.cache_invalid += 1
            if exc.kind is FailureKind.CONTENT:
                # This is a parser/content contract failure, not a corrupt
                # transport object. Re-fetching the identical page defeats
                # raw-first replay and can spend proxy bytes after drift.
                raise
            logger.warning("Ignoring transport-invalid raw cache entry for %s", key)
            return None
        self.stats.cache_hits += 1
        self.stats.route_requests[TransportRoute.RAW_CACHE.value] += 1
        return response

    def _store_and_return(self, key: str, response: TransportResponse) -> TransportResponse:
        self._store_response(key, response)
        return response

    def _store_response(self, key: str, response: TransportResponse) -> None:
        """Persist one transport-safe response, including parser drift payloads."""
        if self.raw_cache is not None:
            payload = CachedPayload(
                content=response.content,
                status_code=response.status_code,
                headers=response.headers,
            )
            try:
                self.raw_cache.store(key, payload, response.sha256)
            except Exception as exc:
                raise WhoScoredTransportError(
                    f"raw cache store failed: {exc}",
                    kind=FailureKind.CACHE,
                    url=response.url,
                    route=response.route,
                ) from exc

    def _http_fetch(
        self,
        url: str,
        *,
        session: Any,
        route: TransportRoute,
    ) -> TransportResponse:
        try:
            raw = session.get(url, timeout=self.request_timeout)
        except Exception as exc:
            self.stats.failures[FailureKind.TIMEOUT.value] += 1
            raise WhoScoredTransportError(
                f"HTTP request failed: {type(exc).__name__}: {exc}",
                kind=FailureKind.TIMEOUT,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        content = bytes(raw.content or b"")
        response = self._response(
            url=url,
            content=content,
            status_code=int(raw.status_code),
            headers=dict(raw.headers or {}),
            route=route,
            wire_bytes=_response_wire_bytes(raw, content),
        )
        self._record_response(response)
        return response

    def _browser_fetch(
        self,
        url: str,
        *,
        client: FlareSolverrClient,
        route: TransportRoute,
        proxy_url: Optional[str],
    ) -> TransportResponse:
        session_id = f"ws-{route.value}-{uuid.uuid4().hex[:10]}"
        try:
            client.create_session(session_id, proxy_url=proxy_url)
            self.stats.browser_sessions += 1
            solution = client.get(
                url,
                session_id,
                max_timeout_ms=self.browser_timeout_ms,
                disable_media=True,
            )
        except FlareSolverrCFChallengeFailed as exc:
            raise CloudflareChallenge(
                str(exc), url=url, route=route
            ) from exc
        except FlareSolverrTimeout as exc:
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.TIMEOUT,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        except FlareSolverrTabCrashed as exc:
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        except (FlareSolverrErrorPage, FlareSolverrError) as exc:
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        finally:
            client.destroy_session(session_id)

        content = str(solution.get("html") or "").encode("utf-8")
        status = int(solution.get("status") or 0)
        response = self._response(
            url=url,
            content=content,
            status_code=status,
            headers={},
            route=route,
            wire_bytes=len(content),
        )
        self._record_response(response)
        if is_chromium_error_page(response.text):
            raise WhoScoredTransportError(
                "FlareSolverr returned a Chromium network-error page",
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            )
        return response

    def _paid_fetch(
        self,
        url: str,
        validator: Optional[Validator],
        *,
        cache_key: str,
    ) -> TransportResponse:
        budgets = self.budgets
        logical_url = _logical_url_key(url)
        if (
            logical_url not in self.stats.paid_urls
            and len(self.stats.paid_urls) >= budgets.max_paid_urls
        ):
            raise TransportBudgetExceeded(
                f"paid URL limit reached ({budgets.max_paid_urls})",
                url=url,
                route=TransportRoute.PAID_HTTP,
            )
        remaining = budgets.max_paid_bytes_per_task - self.stats.paid_proxy_bytes
        url_remaining = budgets.max_paid_bytes_per_url - self.stats.paid_proxy_bytes_by_url.get(
            logical_url, 0
        )
        lease_budget = min(
            url_remaining,
            budgets.max_paid_bytes_per_lease,
            remaining,
        )
        if lease_budget <= 0:
            raise TransportBudgetExceeded(
                "paid task byte budget exhausted",
                url=url,
                route=TransportRoute.PAID_HTTP,
            )
        assert self._proxy_client is not None
        try:
            lease = self._proxy_client.create_lease(
                max_bytes=lease_budget,
                ttl_seconds=budgets.lease_ttl_seconds,
            )
        except Exception as exc:
            raise WhoScoredTransportError(
                f"filtering proxy lease creation failed: {exc}",
                kind=FailureKind.PROXY,
                url=url,
                route=TransportRoute.PAID_HTTP,
                retryable=True,
            ) from exc

        self.stats.paid_urls.add(logical_url)
        paid_http = self._new_http_session(lease.proxy_url)
        result: Optional[TransportResponse] = None
        pending_error: Optional[BaseException] = None
        try:
            try:
                result = self._http_fetch(
                    url, session=paid_http, route=TransportRoute.PAID_HTTP
                )
                self._validate(result, validator)
            except CloudflareChallenge:
                self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                if self._paid_browser_bootstraps >= budgets.max_paid_browser_bootstraps:
                    raise TransportBudgetExceeded(
                        "paid browser bootstrap budget exhausted",
                        url=url,
                        route=TransportRoute.PAID_FLARESOLVERR,
                    )
                self._paid_browser_bootstraps += 1
                result = self._browser_fetch(
                    url,
                    client=self._paid_fs,
                    route=TransportRoute.PAID_FLARESOLVERR,
                    proxy_url=lease.proxy_url,
                )
                self._validate(result, validator)
        except Exception as exc:  # retain while lease stats are finalized
            pending_error = exc
        finally:
            close = getattr(paid_http, "close", None)
            if callable(close):
                close()
            try:
                lease_stats = self._proxy_client.close(lease)
                self._record_paid_lease(logical_url, lease_stats)
            except Exception as exc:
                if pending_error is None:
                    pending_error = WhoScoredTransportError(
                        f"could not finalize filtering proxy lease: {exc}",
                        kind=FailureKind.PROXY,
                        url=url,
                        route=TransportRoute.PAID_HTTP,
                        retryable=True,
                    )
        if self.stats.paid_proxy_bytes > budgets.max_paid_bytes_per_task:
            pending_error = TransportBudgetExceeded(
                "paid task byte budget exceeded",
                url=url,
                route=result.route if result else TransportRoute.PAID_HTTP,
            )
        if (
            self.stats.paid_proxy_bytes_by_url.get(logical_url, 0)
            > budgets.max_paid_bytes_per_url
        ):
            pending_error = TransportBudgetExceeded(
                "paid per-URL byte budget exceeded",
                url=url,
                route=result.route if result else TransportRoute.PAID_HTTP,
            )
        if pending_error is not None:
            if result is not None:
                try:
                    # A valid source document remains worth persisting when
                    # lease finalisation or an aggregate byte ceiling fails.
                    # The retry can then finish without another paid request.
                    self._validate(result, validator=None)
                except WhoScoredTransportError:
                    pass
                else:
                    self._store_response(cache_key, result)
            raise pending_error
        assert result is not None
        return result

    def _record_paid_lease(self, url: str, stats: Mapping[str, Any]) -> None:
        up = int(stats.get("up_bytes", 0) or 0)
        down = int(stats.get("down_bytes", 0) or 0)
        total = int(stats.get("total_bytes", up + down) or 0)
        # ``up`` + ``down`` is the canonical exact split.  Fall back to total
        # for older filter versions which only exposed one counter.
        if up + down == 0 and total:
            down = total
        self.stats.paid_proxy_up_bytes += up
        self.stats.paid_proxy_down_bytes += down
        self.stats.paid_proxy_bytes_by_url[url] = (
            self.stats.paid_proxy_bytes_by_url.get(url, 0) + up + down
        )

    def _response(
        self,
        *,
        url: str,
        content: bytes,
        status_code: int,
        headers: Mapping[str, str],
        route: TransportRoute,
        wire_bytes: int,
    ) -> TransportResponse:
        return TransportResponse(
            url=url,
            content=content,
            status_code=status_code,
            headers=headers,
            route=route,
            wire_bytes=wire_bytes,
            sha256=hashlib.sha256(content).hexdigest(),
        )

    def _record_response(self, response: TransportResponse) -> None:
        self.stats.route_requests[response.route.value] += 1
        self.stats.route_wire_bytes[response.route.value] += response.wire_bytes

    def _validate(
        self, response: TransportResponse, validator: Optional[Validator]
    ) -> None:
        if len(response.content) > self.budgets.max_response_bytes:
            raise TransportBudgetExceeded(
                f"response exceeded {self.budgets.max_response_bytes} bytes",
                url=response.url,
                route=response.route,
            )
        if is_cloudflare_response(
            response.status_code, response.headers, response.content
        ):
            raise CloudflareChallenge(
                "Cloudflare challenge response",
                url=response.url,
                route=response.route,
                status_code=response.status_code,
            )
        if not 200 <= response.status_code < 300:
            raise WhoScoredTransportError(
                f"HTTP {response.status_code}",
                kind=FailureKind.HTTP_STATUS,
                url=response.url,
                route=response.route,
                status_code=response.status_code,
                retryable=response.status_code >= 500,
            )
        if not response.content:
            raise WhoScoredTransportError(
                "empty response",
                kind=FailureKind.CONTENT,
                url=response.url,
                route=response.route,
            )
        if validator is None:
            return
        try:
            accepted = validator(response)
        except WhoScoredTransportError:
            raise
        except Exception as exc:
            raise WhoScoredTransportError(
                f"content validator failed: {exc}",
                kind=FailureKind.CONTENT,
                url=response.url,
                route=response.route,
            ) from exc
        if accepted is False:
            raise WhoScoredTransportError(
                "content validator rejected response",
                kind=FailureKind.CONTENT,
                url=response.url,
                route=response.route,
            )

    def get_traffic_stats(self) -> dict[str, Any]:
        return self.stats.as_dict()

    def close(self) -> None:
        close = getattr(self._direct_http, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "WhoScoredTransport":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False


def _response_wire_bytes(response: Any, content: bytes) -> int:
    """Best available transfer size; curl_cffi exposes ``download_size``."""

    for value in (
        getattr(response, "download_size", None),
        getattr(response, "downloaded", None),
    ):
        try:
            if value is not None and int(value) >= 0:
                return int(value)
        except (TypeError, ValueError):
            pass
    return len(content)


def _logical_url_key(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.netloc}{parts.path}" if parts.netloc else url.split("?", 1)[0]


__all__ = [
    "CachedPayload",
    "CloudflareChallenge",
    "FailureKind",
    "ProxyFilterClient",
    "ProxyLease",
    "RawCacheHook",
    "TransportBudgetExceeded",
    "TransportBudgets",
    "TransportResponse",
    "TransportRoute",
    "TransportStats",
    "WhoScoredTransport",
    "WhoScoredTransportError",
    "is_cloudflare_response",
]
