"""Network transport for WhoScored with an explicitly bounded paid fallback.

The module deliberately owns *transport only*.  Parsers decide what valid
WhoScored content looks like and may pass a validator to :meth:`fetch`; a raw
store may be plugged in through :class:`RawCacheHook`, but this module does not
choose a storage backend.

The route order is fixed and fail-closed::

    raw cache -> direct curl (or an exact-bootstrap gate circuit) ->
    reusable direct FlareSolverr -> fresh direct curl recheck ->
    filtered paid curl -> filtered paid FlareSolverr

Paid traffic is reached only after the direct HTTP response and every direct
browser attempt were independently classified as Cloudflare challenges.  A
normal HTTP error, timeout, parser/validator rejection or browser crash never
turns a proxy on.
"""

from __future__ import annotations

import hashlib
import fcntl
import json
import logging
import os
import re
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

import requests

from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrClient,
    FlareSolverrError,
    FlareSolverrErrorPage,
    FlareSolverrResponseTooLarge,
    FlareSolverrTabCrashed,
    FlareSolverrTimeout,
    MAX_XHR_BATCH_URLS,
    is_chromium_error_page,
)

logger = logging.getLogger(__name__)


class TransportRoute(str, Enum):
    RAW_CACHE = "raw_cache"
    DIRECT_HTTP = "direct_http"
    DIRECT_FLARESOLVERR = "direct_flaresolverr"
    PAID_HTTP = "paid_http"
    PAID_FLARESOLVERR = "paid_flaresolverr"
    PAID_LEASE = "paid_lease"


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


DEFAULT_PAID_BYTES_PER_DAGRUN = 8_000_000
DEFAULT_PAID_BYTES_PER_URL = 2_000_000


@dataclass(frozen=True)
class TransportContext:
    """Stable request identity shared by Airflow, transport and proxy filter."""

    dag_id: str = ""
    run_id: str = ""
    task_id: str = ""
    map_index: int = -1
    try_number: int = 0
    scope: str = ""
    entity: str = ""

    @classmethod
    def from_env(cls) -> "TransportContext":
        def _integer(name: str, default: int) -> int:
            try:
                return int(os.environ.get(name, default))
            except (TypeError, ValueError):
                return default

        return cls(
            dag_id=os.environ.get("AIRFLOW_CTX_DAG_ID", ""),
            run_id=os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", ""),
            task_id=os.environ.get("AIRFLOW_CTX_TASK_ID", ""),
            map_index=_integer("AIRFLOW_CTX_MAP_INDEX", -1),
            try_number=_integer("AIRFLOW_CTX_TRY_NUMBER", 0),
            scope=os.environ.get("WHOSCORED_SCOPE", ""),
            entity=os.environ.get("WHOSCORED_ENTITY", ""),
        )

    def request_context(
        self, *, scope: Optional[str] = None, entity: Optional[str] = None
    ) -> "TransportContext":
        return TransportContext(
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            map_index=self.map_index,
            try_number=self.try_number,
            scope=self.scope if scope is None else scope,
            entity=self.entity if entity is None else entity,
        )

    @property
    def dagrun_key(self) -> str:
        if self.dag_id and self.run_id:
            return f"{self.dag_id}/{self.run_id}"
        return ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "map_index": self.map_index,
            "try_number": self.try_number,
            "scope": self.scope,
            "entity": self.entity,
        }


@runtime_checkable
class RequestLedger(Protocol):
    """Append-only sink; implementations must make one append durable."""

    def append(self, event: Mapping[str, Any]) -> None:
        """Persist one complete request/accounting event."""


class JsonlRequestLedger:
    """Process-safe append-only JSONL request ledger with an fsync commit."""

    def __init__(self, path: str) -> None:
        if not path:
            raise ValueError("request ledger path must not be empty")
        self.path = Path(path)
        self._lock = threading.Lock()

    def append(self, event: Mapping[str, Any]) -> None:
        rendered = (
            json.dumps(
                dict(event), ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            + b"\n"
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        with self._lock:
            descriptor = os.open(self.path, flags, 0o600)
            try:
                os.fchmod(descriptor, 0o600)
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                pending = memoryview(rendered)
                while pending:
                    written = os.write(descriptor, pending)
                    if written <= 0:
                        raise OSError("request ledger write made no progress")
                    pending = pending[written:]
                os.fsync(descriptor)
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)


class ProxyBudgetRejected(RuntimeError):
    """The shared filtering proxy rejected a lease at a hard byte ceiling."""


class ProxyConcurrencyLimited(RuntimeError):
    """The single paid slot is in use by another task in the DagRun."""


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
    request_bytes: int = 0
    response_bytes: int = 0
    resource_bytes: int = 0

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class TransportBudgets:
    """Per-transport hard limits; the proxy also enforces each lease server-side."""

    max_response_bytes: int = 4 * 1024 * 1024
    max_paid_bytes_per_url: int = DEFAULT_PAID_BYTES_PER_URL
    max_paid_bytes_per_lease: int = DEFAULT_PAID_BYTES_PER_URL
    # Kept under the old name because the service tightens the existing
    # object between entity batches.  The filtering proxy enforces this value
    # across *all* tasks in the same dag_id/run_id, not merely this process.
    max_paid_bytes_per_task: int = DEFAULT_PAID_BYTES_PER_DAGRUN
    max_paid_urls: int = 20
    max_paid_browser_bootstraps: int = 2
    lease_ttl_seconds: int = 60

    @classmethod
    def for_eligible_urls(
        cls, eligible_urls: int, **overrides: int
    ) -> "TransportBudgets":
        paid_limit = (
            0 if eligible_urls <= 0 else min(20, max(3, (eligible_urls + 49) // 50))
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
    browser_batches: int = 0
    browser_batch_items: int = 0
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
            "browser_batches": self.browser_batches,
            "browser_batch_items": self.browser_batch_items,
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

    def create_lease(
        self,
        *,
        max_bytes: int,
        ttl_seconds: int,
        context: Optional[TransportContext] = None,
        canonical_url: str = "",
    ) -> ProxyLease:
        request: dict[str, Any] = {
            "max_bytes": max_bytes,
            "ttl_seconds": ttl_seconds,
            "canonical_url": canonical_url,
        }
        if context is not None:
            request.update(context.as_dict())
        response = self.session.post(
            f"{self.control_url}/v1/leases",
            json=request,
            timeout=self.timeout,
        )
        if int(getattr(response, "status_code", 0) or 0) == 429:
            try:
                rejected = dict(response.json())
            except (TypeError, ValueError):
                rejected = {}
            message = str(rejected.get("error") or "paid proxy lease rejected")
            if rejected.get("code") == "budget_exceeded":
                raise ProxyBudgetRejected(message)
            if rejected.get("code") == "concurrency_limited":
                raise ProxyConcurrencyLimited(message)
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

    def close_session(self) -> None:
        """Release the control-plane HTTP connection pool."""

        self.session.close()


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


_CF_STRONG_BODY_MARKERS = (
    "<title>just a moment",
    "<title>attention required! | cloudflare</title>",
    "/cdn-cgi/challenge-platform/",
    "cf-browser-verification",
    "challenge-running",
    "cf_chl_",
    "cf-chl-",
)
_CF_ORIGIN_ERROR_TITLE = re.compile(
    r"<title>\s*whoscored\.com\s*\|\s*(50[0-9])\s*:[^<]*</title>",
    re.IGNORECASE,
)

_WHOSCORED_STRUCTURED_FEED_PATHS = (
    re.compile(r"\A/statisticsfeed/1/get(?:team|player)statistics\Z"),
    re.compile(r"\A/stagestatfeed/[1-9][0-9]*/stageteams/\Z"),
    re.compile(r"\A/stageplayerstatfeed/[1-9][0-9]*/playerstats/\Z"),
)
_WHOSCORED_STAGE_BOOTSTRAP_PATH = re.compile(
    r"\A/Regions/[1-9][0-9]*/Tournaments/[1-9][0-9]*/"
    r"Seasons/[1-9][0-9]*/Stages/[1-9][0-9]*/TeamStatistics\Z"
)


def _is_whoscored_structured_feed_url(url: str) -> bool:
    parts = urlsplit(url)
    return bool(
        parts.scheme.lower() == "https"
        and parts.hostname == "www.whoscored.com"
        and parts.port is None
        and parts.username is None
        and parts.password is None
        and not parts.fragment
        and any(
            pattern.fullmatch(parts.path)
            for pattern in _WHOSCORED_STRUCTURED_FEED_PATHS
        )
    )


def _is_whoscored_stage_bootstrap_url(url: str) -> bool:
    parts = urlsplit(url)
    return bool(
        parts.scheme.lower() == "https"
        and parts.hostname == "www.whoscored.com"
        and parts.port is None
        and parts.username is None
        and parts.password is None
        and not parts.query
        and not parts.fragment
        and _WHOSCORED_STAGE_BOOTSTRAP_PATH.fullmatch(parts.path)
    )


def is_whoscored_structured_feed_access_gate(
    url: str,
    status_code: int,
    content: bytes | str,
    headers: Optional[Mapping[str, str]] = None,
) -> bool:
    """Detect WhoScored's anti-automation shell, narrowly.

    The structured-feed origin returns a tiny generic 404 document instead of
    JSON when an XHR lacks the browser execution context. Depending on which
    source cookies are already present, the same shell is either returned as
    HTTP 200 or as a no-follow HTTP 302 to the exact same-origin ``404.html``
    path. It is eligible for direct FlareSolverr only when the source-specific
    body markers are present and, for 302, the location names the requested
    allow-listed feed path. An arbitrary redirect/parser rejection never
    enables browser or paid fallback.
    """

    if status_code not in {200, 302} or not _is_whoscored_structured_feed_url(url):
        return False
    body = (
        content.decode("utf-8", errors="ignore")
        if isinstance(content, bytes)
        else content
    )
    lowered = body[:100_000].lower()
    verify_marker = "verify-client.js" in lowered
    if status_code == 200:
        return verify_marker and "page you requested does not exist" in lowered
    if not verify_marker:
        return False

    lowered_headers = {
        str(key).lower(): str(value) for key, value in (headers or {}).items()
    }
    location = lowered_headers.get("location", "")
    if not location:
        return False
    requested = urlsplit(url)
    redirected = urlsplit(urljoin("https://www.whoscored.com", location))
    if (
        redirected.scheme.lower() != "https"
        or redirected.hostname != "www.whoscored.com"
        or redirected.port is not None
        or redirected.username is not None
        or redirected.password is not None
        or redirected.path != "/404.html"
        or redirected.fragment
    ):
        return False
    query = parse_qsl(redirected.query, keep_blank_values=True)
    return query == [("aspxerrorpath", requested.path)]


def is_cloudflare_response(
    status_code: int,
    headers: Mapping[str, str],
    content: bytes | str,
) -> bool:
    """Classify a response narrowly enough that ordinary errors never buy traffic."""

    lowered_headers = {str(k).lower(): str(v).lower() for k, v in headers.items()}
    body = (
        content.decode("utf-8", errors="ignore")
        if isinstance(content, bytes)
        else content
    )
    body = body[:250_000].lower()
    marker = any(value in body for value in _CF_STRONG_BODY_MARKERS)
    cf_header = bool(lowered_headers.get("cf-ray")) or (
        "cloudflare" in lowered_headers.get("server", "")
    )
    if status_code == 200:
        # FlareSolverr may return a rendered interstitial as HTTP 200 without
        # source headers. Only source-specific challenge markup is accepted.
        return marker
    # WhoScored is fronted by Cloudflare even for ordinary origin errors.
    # Paid fallback requires both independently useful signals: a supported
    # challenge status/header and the actual challenge document.
    return status_code in (403, 429, 503) and cf_header and marker


def _has_cloudflare_challenge_markup(content: bytes | str) -> bool:
    body = (
        content.decode("utf-8", errors="ignore")
        if isinstance(content, bytes)
        else content
    )
    body = body[:250_000].lower()
    return any(value in body for value in _CF_STRONG_BODY_MARKERS)


def _cloudflare_origin_error_status(content: bytes | str) -> Optional[int]:
    """Recover an origin 5xx that FlareSolverr reports as HTTP 200.

    FlareSolverr returns the rendered document status, while Cloudflare's
    branded origin-error document can therefore arrive as 200.  Match only
    WhoScored's exact error title so an application page containing an
    arbitrary ``502`` string is never reclassified.
    """

    body = (
        content.decode("utf-8", errors="ignore")
        if isinstance(content, bytes)
        else content
    )
    match = _CF_ORIGIN_ERROR_TITLE.search(body[:16_384])
    return int(match.group(1)) if match is not None else None


Validator = Callable[[TransportResponse], Optional[bool]]


@dataclass(frozen=True)
class FetchRequest:
    """One logical URL in a bounded structured-feed batch."""

    url: str
    cache_key: str
    validator: Optional[Validator] = None
    scope: Optional[str] = None
    entity: Optional[str] = None
    before_network: Optional[Callable[[], None]] = None
    browser_bootstrap_url: Optional[str] = None


@dataclass
class _BrowserSession:
    session_id: str
    proxy_url: Optional[str]
    created_at: float
    requests: int = 0
    bootstrap_url: str = ""


@dataclass(frozen=True)
class _BrowserBatchOutcome:
    response: Optional[TransportResponse] = None
    error: Optional[WhoScoredTransportError] = None

    def __post_init__(self) -> None:
        if (self.response is None) == (self.error is None):
            raise ValueError("browser batch outcome requires one response or error")


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
        direct_http_attempts: int = 3,
        direct_browser_attempts: int = 2,
        impersonate: str = "chrome120",
        direct_http_session: Any = None,
        direct_fs_client: Optional[FlareSolverrClient] = None,
        paid_fs_client: Optional[FlareSolverrClient] = None,
        proxy_client: Optional[ProxyFilterClient] = None,
        http_session_factory: Optional[Callable[[Optional[str]], Any]] = None,
        context: Optional[TransportContext] = None,
        request_ledger: Optional[RequestLedger] = None,
        browser_session_ttl_seconds: int = 300,
        browser_session_max_requests: int = 96,
    ) -> None:
        if direct_http_attempts < 1:
            raise ValueError("direct_http_attempts must be >= 1")
        if direct_browser_attempts < 1:
            raise ValueError("direct_browser_attempts must be >= 1")
        self.raw_cache = raw_cache
        self.budgets = budgets or TransportBudgets()
        self.request_timeout = request_timeout
        self.browser_timeout_ms = browser_timeout_ms
        self.direct_http_attempts = direct_http_attempts
        self.direct_browser_attempts = direct_browser_attempts
        self.impersonate = impersonate
        self.paid_proxy_url = paid_proxy_url
        self.context = context or TransportContext.from_env()
        ledger_path = os.environ.get("WHOSCORED_REQUEST_LEDGER_PATH", "").strip()
        self.request_ledger = request_ledger or (
            JsonlRequestLedger(ledger_path) if ledger_path else None
        )
        self.browser_session_ttl_seconds = max(1, browser_session_ttl_seconds)
        self.browser_session_max_requests = max(1, browser_session_max_requests)
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
        self._browser_sessions: dict[TransportRoute, _BrowserSession] = {}
        # A structured feed behind one validated stage bootstrap consistently
        # returns the same source-specific access gate to curl while its XHR
        # succeeds in the direct browser session.  Remember that evidence only
        # for the exact bootstrap and only for this transport lifetime.  The
        # raw cache is still consulted before this circuit on every request.
        self._direct_gate_circuits: set[str] = set()
        self._active_context = self.context
        self._active_cache_key = ""
        self._active_request_id = ""

    def _new_http_session(self, proxy_url: Optional[str]) -> Any:
        if self._http_session_factory is not None:
            return self._http_session_factory(proxy_url)
        try:
            from curl_cffi.requests import Session
        except ImportError as exc:  # pragma: no cover - production dependency
            raise RuntimeError("curl_cffi is required for WhoScoredTransport") from exc
        # A route labelled ``direct`` must never inherit HTTP(S)_PROXY from the
        # worker environment. Paid sessions receive their one explicit,
        # byte-bounded lease below; they must not merge it with ambient proxy
        # configuration either.
        session = Session(impersonate=self.impersonate, trust_env=False)
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

    def _browser_session(
        self,
        client: FlareSolverrClient,
        *,
        route: TransportRoute,
        proxy_url: Optional[str],
        required_requests: int = 1,
    ) -> _BrowserSession:
        existing = self._browser_sessions.get(route)
        now = time.monotonic()
        if existing is not None and (
            existing.proxy_url != proxy_url
            or now - existing.created_at >= self.browser_session_ttl_seconds
            or (
                existing.requests > 0
                and existing.requests + max(1, required_requests)
                > self.browser_session_max_requests
            )
        ):
            self._drop_browser_session(client, route)
            existing = None
        if existing is not None:
            return existing
        session_id = f"ws-{route.value}-{uuid.uuid4().hex[:10]}"
        try:
            client.create_session(session_id, proxy_url=proxy_url)
        except Exception:
            # The POST may have reached FlareSolverr even when the response was
            # lost.  Destroy the deterministic id so a retry cannot leak an
            # orphan Chromium process outside ``_browser_sessions``.
            try:
                client.destroy_session(session_id)
            except Exception:
                logger.debug(
                    "Could not destroy possibly-created WhoScored browser session %s",
                    session_id,
                    exc_info=True,
                )
            raise
        self.stats.browser_sessions += 1
        created = _BrowserSession(
            session_id=session_id,
            proxy_url=proxy_url,
            created_at=now,
        )
        self._browser_sessions[route] = created
        return created

    def _drop_browser_session(
        self, client: FlareSolverrClient, route: TransportRoute
    ) -> None:
        existing = self._browser_sessions.pop(route, None)
        if existing is not None:
            try:
                client.destroy_session(existing.session_id)
            except Exception:  # destroy is best-effort and idempotent
                logger.debug(
                    "Could not destroy WhoScored browser session %s",
                    existing.session_id,
                    exc_info=True,
                )

    def _replay_browser_identity(self, solution: Mapping[str, Any]) -> None:
        """Replay solved CF cookies into direct HTTP when the session supports it."""
        cookies = solution.get("cookies") or []
        jar = getattr(self._direct_http, "cookies", None)
        if jar is not None:
            for item in cookies:
                if not isinstance(item, Mapping) or not item.get("name"):
                    continue
                kwargs: dict[str, Any] = {}
                if item.get("domain"):
                    kwargs["domain"] = item["domain"]
                if item.get("path"):
                    kwargs["path"] = item["path"]
                try:
                    jar.set(str(item["name"]), str(item.get("value", "")), **kwargs)
                except Exception:
                    logger.debug("Could not replay browser cookie", exc_info=True)
        user_agent = str(solution.get("userAgent") or "").strip()
        headers = getattr(self._direct_http, "headers", None)
        if user_agent and headers is not None:
            headers["User-Agent"] = user_agent

    def _activate_request(
        self,
        *,
        cache_key: str,
        scope: Optional[str],
        entity: Optional[str],
        request_id: Optional[str] = None,
    ) -> None:
        """Select the exact URL identity used by ledger writes below."""

        self._active_cache_key = cache_key
        self._active_request_id = request_id or uuid.uuid4().hex
        self._active_context = self.context.request_context(
            scope=scope,
            entity=entity or self.context.entity or _entity_from_cache_key(cache_key),
        )

    def fetch(
        self,
        url: str,
        *,
        cache_key: Optional[str] = None,
        validator: Optional[Validator] = None,
        scope: Optional[str] = None,
        entity: Optional[str] = None,
        before_network: Optional[Callable[[], None]] = None,
        browser_bootstrap_url: Optional[str] = None,
    ) -> TransportResponse:
        """Fetch one logical URL through the fixed route state machine."""

        if browser_bootstrap_url is not None and (
            not _is_whoscored_structured_feed_url(url)
            or not _is_whoscored_stage_bootstrap_url(browser_bootstrap_url)
        ):
            raise WhoScoredTransportError(
                "browser XHR bootstrap is restricted to WhoScored stage statistics",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            )

        key = cache_key or url
        self._activate_request(
            cache_key=key,
            scope=scope,
            entity=entity,
        )
        cache_invalid_before = self.stats.cache_invalid
        cached = self._load_cached(key, url, validator)
        if cached is not None:
            return cached
        cache_entry_was_invalid = self.stats.cache_invalid > cache_invalid_before
        if before_network is not None:
            # The gate belongs after cache validation, not before ``has``.
            # A parser-invalid but integrity-valid raw object intentionally
            # retries direct once and must consume the same rate-limit token
            # as an ordinary cache miss.
            before_network()

        direct_gate_key = (
            _canonical_url_key(browser_bootstrap_url)
            if browser_bootstrap_url is not None
            else None
        )

        def _direct_once() -> TransportResponse:
            response = self._http_fetch(
                url,
                session=self._direct_http,
                route=TransportRoute.DIRECT_HTTP,
                referer=browser_bootstrap_url,
            )
            try:
                self._validate(response, validator)
            except WhoScoredTransportError as exc:
                if exc.kind is FailureKind.CONTENT:
                    self._store_response(key, response)
                raise
            return response

        def _direct_with_retries(
            *, acquire_first_token: bool = False
        ) -> TransportResponse:
            if acquire_first_token and before_network is not None:
                before_network()
            for attempt in range(self.direct_http_attempts):
                try:
                    return _direct_once()
                except CloudflareChallenge:
                    # Cloudflare is route evidence, not a transient ordinary
                    # HTTP failure. Move to the direct browser immediately.
                    raise
                except WhoScoredTransportError as exc:
                    retryable_direct = exc.retryable and exc.kind in {
                        FailureKind.HTTP_STATUS,
                        FailureKind.TIMEOUT,
                    }
                    if not retryable_direct or attempt + 1 >= self.direct_http_attempts:
                        raise
                    if before_network is not None:
                        # Every physical retry consumes a source-rate token.
                        before_network()
            raise AssertionError("direct HTTP retry loop returned no result")

        # Parser-invalid cache entries retain their existing direct-origin
        # retry contract.  Only an actual raw miss may use prior gate evidence.
        skip_direct = bool(
            direct_gate_key
            and not cache_entry_was_invalid
            and direct_gate_key in self._direct_gate_circuits
        )
        if not skip_direct:
            try:
                direct = _direct_with_retries()
            except CloudflareChallenge:
                self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                if direct_gate_key is not None:
                    self._direct_gate_circuits.add(direct_gate_key)
            except WhoScoredTransportError:
                if direct_gate_key is not None:
                    self._direct_gate_circuits.discard(direct_gate_key)
                raise
            else:
                if direct_gate_key is not None:
                    self._direct_gate_circuits.discard(direct_gate_key)
                return self._store_and_return(key, direct)

        direct_cf_failures = 0
        last_cf: Optional[CloudflareChallenge] = None
        last_transient: Optional[WhoScoredTransportError] = None
        for attempt in range(self.direct_browser_attempts):
            try:
                browser = self._browser_fetch(
                    url,
                    client=self._direct_fs,
                    route=TransportRoute.DIRECT_FLARESOLVERR,
                    proxy_url=None,
                    bootstrap_url=browser_bootstrap_url,
                )
                self._validate(browser, validator)
            except CloudflareChallenge as exc:
                self._drop_browser_session(
                    self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                )
                direct_cf_failures += 1
                last_cf = exc
                self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                continue
            except WhoScoredTransportError as exc:
                # A tab crash, timeout, ordinary browser error or parser
                # rejection must never silently buy residential bandwidth.
                if direct_gate_key is not None:
                    self._direct_gate_circuits.discard(direct_gate_key)
                if (
                    exc.retryable
                    and exc.kind is FailureKind.HTTP_STATUS
                    and attempt + 1 < self.direct_browser_attempts
                ):
                    last_transient = exc
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    if before_network is not None:
                        before_network()
                    continue
                if exc.kind is FailureKind.CONTENT:
                    self._store_response(key, browser)
                raise
            return self._store_and_return(key, browser)

        if last_transient is not None and direct_cf_failures == 0:
            raise last_transient
        if direct_cf_failures != self.direct_browser_attempts:
            assert last_cf is not None
            raise last_cf
        if self._proxy_client is None:
            assert last_cf is not None
            raise last_cf

        if direct_gate_key is not None:
            # Circuit evidence is an optimisation, never authority to spend.
            # A browser CF failure invalidates the old evidence: clear it and
            # recheck direct HTTP immediately before creating a paid lease.
            self._direct_gate_circuits.discard(direct_gate_key)
            try:
                direct = _direct_with_retries(acquire_first_token=True)
            except CloudflareChallenge:
                self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                # The paid transition is now backed by fresh direct evidence.
                self._direct_gate_circuits.add(direct_gate_key)
            except WhoScoredTransportError:
                # Timeout, status and content failures are not CF evidence and
                # therefore stop before paid traffic with the circuit clear.
                raise
            else:
                return self._store_and_return(key, direct)

        paid = self._paid_fetch(
            url,
            validator,
            cache_key=key,
            browser_bootstrap_url=browser_bootstrap_url,
        )
        return self._store_and_return(key, paid)

    def fetch_many(self, requests: Sequence[FetchRequest]) -> list[TransportResponse]:
        """Fetch one structured-feed group with bounded direct-browser batches.

        Every item checks raw cache first. The first raw miss confirms the
        official direct-origin access gate; later misses under that exact
        bootstrap use the existing gate circuit instead of repeating a known-
        useless direct request. Only those URLs reach the restricted browser
        endpoint, in batches of at most eight. The method returns only after
        every validator succeeds, so callers cannot publish a partial scope.
        Valid raw objects may still be retained on failure so an Airflow retry
        is resumable without duplicate traffic.
        """

        items = tuple(requests)
        if not items:
            return []
        if len({item.cache_key for item in items}) != len(items):
            raise WhoScoredTransportError(
                "structured batch cache keys must be unique",
                kind=FailureKind.CONFIG,
                url=items[0].url,
            )
        if len({item.url for item in items}) != len(items):
            raise WhoScoredTransportError(
                "structured batch URLs must be unique",
                kind=FailureKind.CONFIG,
                url=items[0].url,
            )
        bootstrap_urls = {item.browser_bootstrap_url for item in items}
        if len(bootstrap_urls) != 1 or None in bootstrap_urls:
            raise WhoScoredTransportError(
                "structured batch requires one exact browser bootstrap URL",
                kind=FailureKind.CONFIG,
                url=items[0].url,
            )
        bootstrap_url = items[0].browser_bootstrap_url
        assert bootstrap_url is not None
        if not _is_whoscored_stage_bootstrap_url(bootstrap_url) or any(
            not _is_whoscored_structured_feed_url(item.url) for item in items
        ):
            raise WhoScoredTransportError(
                "browser XHR batch is restricted to WhoScored stage statistics",
                kind=FailureKind.CONFIG,
                url=items[0].url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            )

        results: list[Optional[TransportResponse]] = [None] * len(items)
        gated: list[tuple[int, FetchRequest, bool]] = []
        gate_key = _canonical_url_key(bootstrap_url)
        request_ids = {item.cache_key: uuid.uuid4().hex for item in items}

        # Raw cache remains first for every logical URL. The first fresh direct
        # response carrying the exact official access gate opens the same
        # bootstrap-scoped circuit as the serial path; later raw misses avoid
        # known-useless duplicate direct requests. Paid still requires a fresh
        # direct recheck below.
        for index, item in enumerate(items):
            self._activate_request(
                cache_key=item.cache_key,
                scope=item.scope,
                entity=item.entity,
                request_id=request_ids[item.cache_key],
            )
            cache_invalid_before = self.stats.cache_invalid
            cached = self._load_cached(item.cache_key, item.url, item.validator)
            if cached is not None:
                results[index] = cached
                continue
            cache_entry_was_invalid = self.stats.cache_invalid > cache_invalid_before
            if not cache_entry_was_invalid and gate_key in self._direct_gate_circuits:
                gated.append((index, item, False))
                continue
            if item.before_network is not None:
                item.before_network()
            direct = self._http_fetch(
                item.url,
                session=self._direct_http,
                route=TransportRoute.DIRECT_HTTP,
                referer=bootstrap_url,
            )
            try:
                self._validate(direct, item.validator)
            except CloudflareChallenge:
                self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                self._direct_gate_circuits.add(gate_key)
                gated.append((index, item, True))
            except WhoScoredTransportError as exc:
                self._direct_gate_circuits.discard(gate_key)
                if exc.kind is FailureKind.CONTENT:
                    self._store_response(item.cache_key, direct)
                raise
            else:
                self._direct_gate_circuits.discard(gate_key)
                self._store_response(item.cache_key, direct)
                results[index] = direct

        for offset in range(0, len(gated), MAX_XHR_BATCH_URLS):
            pending = gated[offset : offset + MAX_XHR_BATCH_URLS]
            last_cf: Optional[CloudflareChallenge] = None
            last_item_errors: dict[str, WhoScoredTransportError] = {}
            non_cf_evidence: dict[str, WhoScoredTransportError] = {}
            cf_failure_counts: Counter[str] = Counter()
            for _ in range(self.direct_browser_attempts):
                if not pending:
                    break
                rate_limited_pending: list[tuple[int, FetchRequest, bool]] = []
                for index, item, network_gate_acquired in pending:
                    if not network_gate_acquired and item.before_network is not None:
                        item.before_network()
                    rate_limited_pending.append((index, item, True))
                pending = rate_limited_pending
                try:
                    browser_outcomes = self._browser_fetch_many(
                        [item for _, item, _ in pending],
                        client=self._direct_fs,
                        route=TransportRoute.DIRECT_FLARESOLVERR,
                        proxy_url=None,
                        bootstrap_url=bootstrap_url,
                        request_ids=request_ids,
                    )
                except CloudflareChallenge as exc:
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    self.stats.failures[FailureKind.CLOUDFLARE.value] += len(pending)
                    last_cf = exc
                    for _, item, _ in pending:
                        last_item_errors[item.cache_key] = exc
                        cf_failure_counts[item.cache_key] += 1
                    continue
                except WhoScoredTransportError as exc:
                    # Endpoint-level timeout/protocol failures do not produce
                    # per-item outcomes. Retry only explicitly transient
                    # browser/bootstrap failures; retain them as non-CF
                    # evidence so a later challenge can never authorize paid.
                    self._direct_gate_circuits.discard(gate_key)
                    if not exc.retryable:
                        raise
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    for _, item, _ in pending:
                        last_item_errors[item.cache_key] = exc
                        non_cf_evidence[item.cache_key] = exc
                    continue

                retry: list[tuple[int, FetchRequest, bool]] = []
                terminal_error: Optional[WhoScoredTransportError] = None
                for (index, item, network_gate_acquired), outcome in zip(
                    pending, browser_outcomes
                ):
                    if outcome.error is not None:
                        last_item_errors[item.cache_key] = outcome.error
                        non_cf_evidence[item.cache_key] = outcome.error
                        if outcome.error.retryable:
                            retry.append((index, item, network_gate_acquired))
                        else:
                            terminal_error = terminal_error or outcome.error
                        continue
                    browser = outcome.response
                    assert browser is not None
                    self._activate_request(
                        cache_key=item.cache_key,
                        scope=item.scope,
                        entity=item.entity,
                        request_id=request_ids[item.cache_key],
                    )
                    try:
                        self._validate(browser, item.validator)
                    except CloudflareChallenge as exc:
                        self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                        last_cf = exc
                        last_item_errors[item.cache_key] = exc
                        cf_failure_counts[item.cache_key] += 1
                        retry.append((index, item, network_gate_acquired))
                    except WhoScoredTransportError as exc:
                        if exc.kind is FailureKind.CONTENT:
                            self._store_response(item.cache_key, browser)
                        terminal_error = terminal_error or exc
                    else:
                        self._store_response(item.cache_key, browser)
                        results[index] = browser
                        last_item_errors.pop(item.cache_key, None)
                        non_cf_evidence.pop(item.cache_key, None)
                if terminal_error is not None:
                    self._direct_gate_circuits.discard(gate_key)
                    raise terminal_error
                pending = retry
                if pending:
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )

            if not pending:
                continue
            non_cf_errors: list[WhoScoredTransportError] = []
            for _, item, _ in pending:
                item_error = non_cf_evidence.get(item.cache_key)
                if item_error is not None and not isinstance(
                    item_error, CloudflareChallenge
                ):
                    non_cf_errors.append(item_error)
                    continue
                if cf_failure_counts[item.cache_key] != self.direct_browser_attempts:
                    incomplete_error = last_item_errors.get(item.cache_key)
                    if incomplete_error is not None:
                        non_cf_errors.append(incomplete_error)
            if non_cf_errors:
                self._direct_gate_circuits.discard(gate_key)
                raise non_cf_errors[0]
            if self._proxy_client is None:
                assert last_cf is not None
                raise last_cf

            # Batch browser CF evidence is never authority to spend. Recheck
            # each URL through direct HTTP immediately before its bounded paid
            # lease, exactly like the serial state machine.
            self._direct_gate_circuits.discard(gate_key)
            for index, item, _ in pending:
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                fresh_direct = self._http_fetch(
                    item.url,
                    session=self._direct_http,
                    route=TransportRoute.DIRECT_HTTP,
                    referer=bootstrap_url,
                )
                try:
                    self._validate(fresh_direct, item.validator)
                except CloudflareChallenge:
                    self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                except WhoScoredTransportError as exc:
                    if exc.kind is FailureKind.CONTENT:
                        self._store_response(item.cache_key, fresh_direct)
                    raise
                else:
                    self._store_response(item.cache_key, fresh_direct)
                    results[index] = fresh_direct
                    continue
                paid = self._paid_fetch(
                    item.url,
                    item.validator,
                    cache_key=item.cache_key,
                    browser_bootstrap_url=bootstrap_url,
                )
                self._store_response(item.cache_key, paid)
                results[index] = paid

        if any(response is None for response in results):
            raise WhoScoredTransportError(
                "structured batch finished without every response",
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            )
        return [response for response in results if response is not None]

    def _load_cached(
        self, key: str, url: str, validator: Optional[Validator]
    ) -> Optional[TransportResponse]:
        if self.raw_cache is None:
            return None
        try:
            payload = self.raw_cache.load(key)
        except Exception as exc:
            self._record_ledger(
                url=url,
                route=TransportRoute.RAW_CACHE,
                status="error",
                failure_kind=FailureKind.CACHE,
                error=exc,
            )
            raise WhoScoredTransportError(
                f"raw cache load failed: {exc}",
                kind=FailureKind.CACHE,
                url=url,
                route=TransportRoute.RAW_CACHE,
            ) from exc
        if payload is None:
            self._record_ledger(
                url=url,
                route=TransportRoute.RAW_CACHE,
                status="miss",
                cache_hit=False,
            )
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
                # The cached object was still parsed first, but a transient
                # HTTP 200 body (partial edge response, maintenance shell)
                # must not poison an immutable target forever. Retry origin
                # direct exactly once. A second validator rejection is stored
                # for diagnosis and raised immediately; it never enables a
                # browser or paid-proxy route.
                logger.warning(
                    "Parser-invalid raw cache entry for %s; retrying direct origin",
                    key,
                )
                return None
            logger.warning("Ignoring transport-invalid raw cache entry for %s", key)
            return None
        self.stats.cache_hits += 1
        self.stats.route_requests[TransportRoute.RAW_CACHE.value] += 1
        return response

    def _store_and_return(
        self, key: str, response: TransportResponse
    ) -> TransportResponse:
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
        referer: Optional[str] = None,
    ) -> TransportResponse:
        request_kwargs: dict[str, Any] = {"timeout": self.request_timeout}
        if _is_whoscored_structured_feed_url(url):
            request_headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }
            if referer:
                request_headers["Referer"] = referer
            request_kwargs["headers"] = request_headers
            # curl/requests otherwise follow redirects before validation,
            # which would defeat the browser endpoint's no-follow policy.
            request_kwargs["allow_redirects"] = False
        try:
            raw = session.get(url, **request_kwargs)
        except Exception as exc:
            self.stats.failures[FailureKind.TIMEOUT.value] += 1
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.TIMEOUT,
                error=exc,
            )
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
            request_bytes=_request_wire_bytes(raw),
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
        bootstrap_url: Optional[str] = None,
    ) -> TransportResponse:
        existing = self._browser_sessions.get(route)
        needs_bootstrap = bool(
            bootstrap_url
            and (existing is None or existing.bootstrap_url != bootstrap_url)
        )
        try:
            session = self._browser_session(
                client,
                route=route,
                proxy_url=proxy_url,
                required_requests=2 if needs_bootstrap else 1,
            )
        except FlareSolverrCFChallengeFailed as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CLOUDFLARE,
                error=exc,
            )
            raise CloudflareChallenge(str(exc), url=url, route=route) from exc
        except FlareSolverrResponseTooLarge as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BUDGET,
                error=exc,
            )
            raise TransportBudgetExceeded(str(exc), url=url, route=route) from exc
        except FlareSolverrTimeout as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.TIMEOUT,
                error=exc,
            )
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.TIMEOUT,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        except FlareSolverrError as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BROWSER,
                error=exc,
            )
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        session_id = session.session_id
        try:
            if bootstrap_url and session.bootstrap_url != bootstrap_url:
                bootstrap_solution = client.get(
                    bootstrap_url,
                    session_id,
                    max_timeout_ms=self.browser_timeout_ms,
                    disable_media=True,
                )
                session.requests += 1
                bootstrap_content = str(bootstrap_solution.get("html") or "").encode(
                    "utf-8"
                )
                bootstrap_response = self._response(
                    url=bootstrap_url,
                    content=bootstrap_content,
                    status_code=int(bootstrap_solution.get("status") or 0),
                    headers={},
                    route=route,
                    wire_bytes=len(bootstrap_content),
                    response_bytes=len(bootstrap_content),
                )
                self._record_response(bootstrap_response)
                if is_chromium_error_page(bootstrap_response.text):
                    raise FlareSolverrErrorPage(
                        "FlareSolverr returned a Chromium network-error page"
                    )
                try:
                    self._validate(bootstrap_response, validator=None)
                except WhoScoredTransportError as exc:
                    # A browser session can occasionally receive WhoScored's
                    # own 302 verification shell while the next clean session
                    # reaches the exact same fixed TeamStatistics page. This
                    # is retryable direct-browser evidence, not Cloudflare
                    # proof and never authority for paid traffic.
                    if (
                        exc.kind is FailureKind.HTTP_STATUS
                        and bootstrap_response.status_code in {301, 302, 303, 307, 308}
                    ):
                        raise WhoScoredTransportError(
                            "WhoScored browser bootstrap returned a redirect",
                            kind=FailureKind.BROWSER,
                            url=bootstrap_url,
                            route=route,
                            status_code=bootstrap_response.status_code,
                            retryable=True,
                        ) from exc
                    raise
                if route is TransportRoute.DIRECT_FLARESOLVERR:
                    self._replay_browser_identity(bootstrap_solution)
                session.bootstrap_url = bootstrap_url

            if bootstrap_url:
                solution = client.xhr_get(
                    url,
                    session_id,
                    max_timeout_ms=self.browser_timeout_ms,
                )
                content = bytes(solution.get("content") or b"")
                headers = dict(solution.get("headers") or {})
                status = int(solution.get("status") or 0)
                response_bytes = int(solution.get("responseBytes") or len(content))
            else:
                solution = client.get(
                    url,
                    session_id,
                    max_timeout_ms=self.browser_timeout_ms,
                    disable_media=True,
                )
                content = str(solution.get("html") or "").encode("utf-8")
                headers = {}
                status = int(solution.get("status") or 0)
                response_bytes = len(content)
            session.requests += 1
        except FlareSolverrCFChallengeFailed as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CLOUDFLARE,
                error=exc,
            )
            raise CloudflareChallenge(str(exc), url=url, route=route) from exc
        except FlareSolverrResponseTooLarge as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BUDGET,
                error=exc,
            )
            raise TransportBudgetExceeded(str(exc), url=url, route=route) from exc
        except FlareSolverrTimeout as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.TIMEOUT,
                error=exc,
            )
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.TIMEOUT,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        except FlareSolverrTabCrashed as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BROWSER,
                error=exc,
            )
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        except (FlareSolverrErrorPage, FlareSolverrError) as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BROWSER,
                error=exc,
            )
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            ) from exc
        response = self._response(
            url=url,
            content=content,
            status_code=status,
            headers=headers,
            route=route,
            wire_bytes=response_bytes,
            response_bytes=response_bytes,
        )
        self._record_response(response)
        if is_chromium_error_page(response.text):
            self._drop_browser_session(client, route)
            error = WhoScoredTransportError(
                "FlareSolverr returned a Chromium network-error page",
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=True,
            )
            self._record_response_ledger(response, status="error", error=error)
            raise error
        if not bootstrap_url and route is TransportRoute.DIRECT_FLARESOLVERR:
            self._replay_browser_identity(solution)
        return response

    def _browser_fetch_many(
        self,
        requests: Sequence[FetchRequest],
        *,
        client: FlareSolverrClient,
        route: TransportRoute,
        proxy_url: Optional[str],
        bootstrap_url: str,
        request_ids: Mapping[str, str],
    ) -> list[_BrowserBatchOutcome]:
        """Execute a restricted XHR batch with explicit per-item outcomes."""

        items = tuple(requests)
        if not items:
            return []
        existing = self._browser_sessions.get(route)
        needs_bootstrap = existing is None or existing.bootstrap_url != bootstrap_url

        def record_error(kind: FailureKind, error: BaseException) -> None:
            for item in items:
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                self._record_ledger(
                    url=item.url,
                    route=route,
                    status="error",
                    failure_kind=kind,
                    error=error,
                )

        try:
            session = self._browser_session(
                client,
                route=route,
                proxy_url=proxy_url,
                required_requests=len(items) + (1 if needs_bootstrap else 0),
            )
        except FlareSolverrCFChallengeFailed as exc:
            record_error(FailureKind.CLOUDFLARE, exc)
            raise CloudflareChallenge(str(exc), url=items[0].url, route=route) from exc
        except FlareSolverrResponseTooLarge as exc:
            record_error(FailureKind.BUDGET, exc)
            raise TransportBudgetExceeded(
                str(exc), url=items[0].url, route=route
            ) from exc
        except FlareSolverrTimeout as exc:
            record_error(FailureKind.TIMEOUT, exc)
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.TIMEOUT,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc
        except FlareSolverrError as exc:
            record_error(FailureKind.BROWSER, exc)
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc

        try:
            if session.bootstrap_url != bootstrap_url:
                first = items[0]
                self._activate_request(
                    cache_key=first.cache_key,
                    scope=first.scope,
                    entity=first.entity,
                    request_id=request_ids[first.cache_key],
                )
                bootstrap_solution = client.get(
                    bootstrap_url,
                    session.session_id,
                    max_timeout_ms=self.browser_timeout_ms,
                    disable_media=True,
                )
                session.requests += 1
                bootstrap_content = str(bootstrap_solution.get("html") or "").encode(
                    "utf-8"
                )
                bootstrap_response = self._response(
                    url=bootstrap_url,
                    content=bootstrap_content,
                    status_code=int(bootstrap_solution.get("status") or 0),
                    headers={},
                    route=route,
                    wire_bytes=len(bootstrap_content),
                    response_bytes=len(bootstrap_content),
                )
                self._record_response(bootstrap_response)
                if is_chromium_error_page(bootstrap_response.text):
                    raise FlareSolverrErrorPage(
                        "FlareSolverr returned a Chromium network-error page"
                    )
                try:
                    self._validate(bootstrap_response, validator=None)
                except WhoScoredTransportError as exc:
                    if (
                        exc.kind is FailureKind.HTTP_STATUS
                        and bootstrap_response.status_code in {301, 302, 303, 307, 308}
                    ):
                        raise WhoScoredTransportError(
                            "WhoScored browser bootstrap returned a redirect",
                            kind=FailureKind.BROWSER,
                            url=bootstrap_url,
                            route=route,
                            status_code=bootstrap_response.status_code,
                            retryable=True,
                        ) from exc
                    raise
                if route is TransportRoute.DIRECT_FLARESOLVERR:
                    self._replay_browser_identity(bootstrap_solution)
                session.bootstrap_url = bootstrap_url

            self.stats.browser_batches += 1
            self.stats.browser_batch_items += len(items)
            solutions = client.xhr_get_many(
                [item.url for item in items],
                session.session_id,
                max_timeout_ms=self.browser_timeout_ms,
            )
            if len(solutions) != len(items):
                raise FlareSolverrError("FlareSolverr XHR batch is incomplete")
            session.requests += len(items)
        except FlareSolverrCFChallengeFailed as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.CLOUDFLARE, exc)
            raise CloudflareChallenge(str(exc), url=items[0].url, route=route) from exc
        except FlareSolverrResponseTooLarge as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.BUDGET, exc)
            raise TransportBudgetExceeded(
                str(exc), url=items[0].url, route=route
            ) from exc
        except FlareSolverrTimeout as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.TIMEOUT, exc)
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.TIMEOUT,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc
        except FlareSolverrTabCrashed as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.BROWSER, exc)
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc
        except (FlareSolverrErrorPage, FlareSolverrError) as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.BROWSER, exc)
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=route,
                retryable=True,
            ) from exc

        outcomes: list[_BrowserBatchOutcome] = []
        drop_session = False
        for item, solution in zip(items, solutions):
            if solution.get("ok") is False:
                remote_kind = str(solution.get("kind") or "")
                failure_kind = (
                    FailureKind.TIMEOUT
                    if remote_kind == "timeout"
                    else (
                        FailureKind.BUDGET
                        if remote_kind == "response_too_large"
                        else FailureKind.BROWSER
                    )
                )
                error = WhoScoredTransportError(
                    f"restricted browser XHR item failed: {remote_kind}",
                    kind=failure_kind,
                    url=item.url,
                    route=route,
                    retryable=remote_kind in {"timeout", "fetch_failed"},
                )
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                self._record_ledger(
                    url=item.url,
                    route=route,
                    status="error",
                    failure_kind=failure_kind,
                    error=error,
                )
                drop_session = drop_session or remote_kind in {
                    "timeout",
                    "fetch_failed",
                }
                outcomes.append(_BrowserBatchOutcome(error=error))
                continue
            if solution.get("ok") is not True:
                error = WhoScoredTransportError(
                    "restricted browser XHR item has invalid status",
                    kind=FailureKind.BROWSER,
                    url=item.url,
                    route=route,
                    retryable=False,
                )
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                self._record_ledger(
                    url=item.url,
                    route=route,
                    status="error",
                    failure_kind=FailureKind.BROWSER,
                    error=error,
                )
                outcomes.append(_BrowserBatchOutcome(error=error))
                drop_session = True
                continue
            content = bytes(solution.get("content") or b"")
            response_bytes = int(solution.get("responseBytes") or len(content))
            response = self._response(
                url=item.url,
                content=content,
                status_code=int(solution.get("status") or 0),
                headers=dict(solution.get("headers") or {}),
                route=route,
                wire_bytes=response_bytes,
                response_bytes=response_bytes,
            )
            self._record_response(response)
            if is_chromium_error_page(response.text):
                error = WhoScoredTransportError(
                    "FlareSolverr returned a Chromium network-error page",
                    kind=FailureKind.BROWSER,
                    url=item.url,
                    route=route,
                    retryable=True,
                )
                self._activate_request(
                    cache_key=item.cache_key,
                    scope=item.scope,
                    entity=item.entity,
                    request_id=request_ids[item.cache_key],
                )
                self._record_response_ledger(response, status="error", error=error)
                outcomes.append(_BrowserBatchOutcome(error=error))
                drop_session = True
            else:
                outcomes.append(_BrowserBatchOutcome(response=response))
        if drop_session:
            self._drop_browser_session(client, route)
        return outcomes

    def _paid_fetch(
        self,
        url: str,
        validator: Optional[Validator],
        *,
        cache_key: str,
        browser_bootstrap_url: Optional[str] = None,
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
        url_remaining = (
            budgets.max_paid_bytes_per_url
            - self.stats.paid_proxy_bytes_by_url.get(logical_url, 0)
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
                context=self._active_context,
                canonical_url=_canonical_url_key(url),
            )
        except ProxyBudgetRejected as exc:
            raise TransportBudgetExceeded(
                str(exc), url=url, route=TransportRoute.PAID_HTTP
            ) from exc
        except Exception as exc:
            raise WhoScoredTransportError(
                f"filtering proxy lease creation failed: {exc}",
                kind=FailureKind.PROXY,
                url=url,
                route=TransportRoute.PAID_HTTP,
                retryable=True,
            ) from exc

        self.stats.paid_urls.add(logical_url)
        paid_http: Any = None
        result: Optional[TransportResponse] = None
        pending_error: Optional[BaseException] = None
        paid_routes_attempted: list[TransportRoute] = []
        try:
            try:
                try:
                    paid_http = self._new_http_session(lease.proxy_url)
                except Exception as exc:
                    raise WhoScoredTransportError(
                        f"paid HTTP session creation failed: {type(exc).__name__}",
                        kind=FailureKind.PROXY,
                        url=url,
                        route=TransportRoute.PAID_HTTP,
                        retryable=True,
                    ) from exc
                paid_routes_attempted.append(TransportRoute.PAID_HTTP)
                result = self._http_fetch(
                    url,
                    session=paid_http,
                    route=TransportRoute.PAID_HTTP,
                    referer=browser_bootstrap_url,
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
                paid_routes_attempted.append(TransportRoute.PAID_FLARESOLVERR)
                result = self._browser_fetch(
                    url,
                    client=self._paid_fs,
                    route=TransportRoute.PAID_FLARESOLVERR,
                    proxy_url=lease.proxy_url,
                    bootstrap_url=browser_bootstrap_url,
                )
                self._validate(result, validator)
        except Exception as exc:  # retain while lease stats are finalized
            pending_error = exc
        finally:
            close = getattr(paid_http, "close", None) if paid_http is not None else None
            if callable(close):
                try:
                    close()
                except Exception as exc:
                    if pending_error is None:
                        pending_error = WhoScoredTransportError(
                            f"could not close paid HTTP session: {type(exc).__name__}",
                            kind=FailureKind.PROXY,
                            url=url,
                            route=TransportRoute.PAID_HTTP,
                            retryable=True,
                        )
            self._drop_browser_session(self._paid_fs, TransportRoute.PAID_FLARESOLVERR)
            try:
                lease_stats = self._proxy_client.close(lease)
                self._record_paid_lease(
                    logical_url,
                    lease_stats,
                    expected_lease_id=lease.lease_id,
                    routes_attempted=paid_routes_attempted,
                    final_route=result.route if result is not None else None,
                )
            except Exception as exc:
                prior = pending_error
                detail = f"could not finalize filtering proxy lease: {exc}"
                if prior is not None:
                    detail += f"; prior source error: {type(prior).__name__}: {prior}"
                accounting_error = WhoScoredTransportError(
                    detail,
                    kind=FailureKind.PROXY,
                    url=url,
                    route=TransportRoute.PAID_LEASE,
                    retryable=True,
                )
                if prior is not None:
                    accounting_error.__cause__ = prior
                # Exact paid accounting is a fail-closed invariant and takes
                # precedence over a source/parser error from the same lease.
                pending_error = accounting_error
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
                    self._validate(result, validator=None, record=False)
                except WhoScoredTransportError:
                    pass
                else:
                    self._store_response(cache_key, result)
            raise pending_error
        assert result is not None
        return result

    def _record_paid_lease(
        self,
        url: str,
        stats: Mapping[str, Any],
        *,
        expected_lease_id: str,
        routes_attempted: Sequence[TransportRoute],
        final_route: Optional[TransportRoute],
    ) -> None:
        def exact_nonnegative_integer(field: str) -> int:
            value = stats.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(
                    f"filtering proxy lease {field} must be a non-negative integer"
                )
            return value

        reported_lease_id = stats.get("id")
        if (
            not isinstance(reported_lease_id, str)
            or reported_lease_id != expected_lease_id
        ):
            raise ValueError("filtering proxy lease id does not match the active lease")
        reported_url = stats.get("canonical_url")
        if not isinstance(reported_url, str) or not reported_url.strip():
            raise ValueError("filtering proxy lease canonical_url is missing")
        if _canonical_url_key(reported_url) != url:
            raise ValueError(
                "filtering proxy lease canonical_url does not match the request"
            )

        up = exact_nonnegative_integer("up_bytes")
        down = exact_nonnegative_integer("down_bytes")
        total = exact_nonnegative_integer("total_bytes")
        if total != up + down:
            raise ValueError(
                "filtering proxy lease total_bytes does not equal up_bytes + down_bytes"
            )
        self.stats.paid_proxy_up_bytes += up
        self.stats.paid_proxy_down_bytes += down
        self.stats.paid_proxy_bytes_by_url[url] = (
            self.stats.paid_proxy_bytes_by_url.get(url, 0) + up + down
        )
        self._record_ledger(
            url=url,
            # One lease can include the HTTP challenge response and the final
            # browser request. Its byte counter cannot be split exactly by
            # source route, so never mislabel the aggregate as paid_http.
            route=TransportRoute.PAID_LEASE,
            status="accounted",
            request_bytes=up,
            response_bytes=down,
            resource_bytes=up + down,
            paid_proxy_bytes=up + down,
            extra={
                "lease_id": expected_lease_id,
                "paid_routes_attempted": [route.value for route in routes_attempted],
                "final_paid_route": final_route.value if final_route else "",
            },
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
        request_bytes: int = 0,
        response_bytes: Optional[int] = None,
        resource_bytes: int = 0,
    ) -> TransportResponse:
        return TransportResponse(
            url=url,
            content=content,
            status_code=status_code,
            headers=headers,
            route=route,
            wire_bytes=wire_bytes,
            sha256=hashlib.sha256(content).hexdigest(),
            request_bytes=max(0, int(request_bytes)),
            response_bytes=(
                max(0, int(response_bytes))
                if response_bytes is not None
                else max(0, int(wire_bytes))
            ),
            resource_bytes=max(0, int(resource_bytes)),
        )

    def _record_response(self, response: TransportResponse) -> None:
        self.stats.route_requests[response.route.value] += 1
        self.stats.route_wire_bytes[response.route.value] += response.wire_bytes

    def _validate(
        self,
        response: TransportResponse,
        validator: Optional[Validator],
        *,
        record: bool = True,
    ) -> None:
        try:
            self._validate_impl(response, validator)
        except WhoScoredTransportError as exc:
            if record:
                self._record_response_ledger(response, status="error", error=exc)
            raise
        if record:
            self._record_response_ledger(response, status="success")

    def _validate_impl(
        self, response: TransportResponse, validator: Optional[Validator]
    ) -> None:
        if len(response.content) > self.budgets.max_response_bytes:
            raise TransportBudgetExceeded(
                f"response exceeded {self.budgets.max_response_bytes} bytes",
                url=response.url,
                route=response.route,
            )
        soft_origin_status = (
            _cloudflare_origin_error_status(response.content)
            if response.route
            in (
                TransportRoute.DIRECT_FLARESOLVERR,
                TransportRoute.PAID_FLARESOLVERR,
            )
            and response.status_code == 200
            else None
        )
        if soft_origin_status is not None:
            raise WhoScoredTransportError(
                f"HTTP {soft_origin_status} rendered as HTTP 200",
                kind=FailureKind.HTTP_STATUS,
                url=response.url,
                route=response.route,
                status_code=soft_origin_status,
                retryable=True,
            )
        browser_challenge = (
            response.route
            in (
                TransportRoute.DIRECT_FLARESOLVERR,
                TransportRoute.PAID_FLARESOLVERR,
            )
            and response.status_code in (403, 429, 503)
            and _has_cloudflare_challenge_markup(response.content)
        )
        if (
            browser_challenge
            or is_cloudflare_response(
                response.status_code, response.headers, response.content
            )
            or is_whoscored_structured_feed_access_gate(
                response.url,
                response.status_code,
                response.content,
                response.headers,
            )
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

    def _record_response_ledger(
        self,
        response: TransportResponse,
        *,
        status: str,
        error: Optional[WhoScoredTransportError] = None,
    ) -> None:
        self._record_ledger(
            url=response.url,
            route=response.route,
            status=status,
            status_code=response.status_code,
            failure_kind=error.kind if error is not None else None,
            error=error,
            cache_hit=response.route is TransportRoute.RAW_CACHE,
            raw_sha256=response.sha256,
            request_bytes=response.request_bytes,
            response_bytes=response.response_bytes,
            resource_bytes=response.resource_bytes,
        )

    def _record_ledger(
        self,
        *,
        url: str,
        route: TransportRoute,
        status: str,
        status_code: Optional[int] = None,
        failure_kind: Optional[FailureKind] = None,
        error: Optional[BaseException] = None,
        cache_hit: bool = False,
        raw_sha256: str = "",
        request_bytes: int = 0,
        response_bytes: int = 0,
        resource_bytes: int = 0,
        paid_proxy_bytes: int = 0,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None:
        if self.request_ledger is None:
            return
        event: dict[str, Any] = {
            "event_version": "whoscored-request-v1",
            "event_id": uuid.uuid4().hex,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "request_id": self._active_request_id,
            **self._active_context.as_dict(),
            "cache_key": self._active_cache_key,
            "url": _canonical_url_key(url),
            "route": route.value,
            "status": status,
            "status_code": status_code,
            "failure_kind": failure_kind.value if failure_kind else "",
            "error_type": type(error).__name__ if error is not None else "",
            "cache_hit": cache_hit,
            "raw_sha256": raw_sha256,
            "request_bytes": max(0, int(request_bytes)),
            "response_bytes": max(0, int(response_bytes)),
            "resource_bytes": max(0, int(resource_bytes)),
            "paid_proxy_bytes": max(0, int(paid_proxy_bytes)),
        }
        if extra:
            event.update(dict(extra))
        try:
            self.request_ledger.append(event)
        except Exception as exc:
            # Missing telemetry means paid limits cannot be audited. Fail the
            # request instead of silently serving unaccounted production data.
            raise WhoScoredTransportError(
                f"request ledger append failed: {exc}",
                kind=FailureKind.CONFIG,
                url=url,
                route=route,
                retryable=True,
            ) from exc

    def get_traffic_stats(self) -> dict[str, Any]:
        return self.stats.as_dict()

    def close(self) -> None:
        self._drop_browser_session(self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR)
        self._drop_browser_session(self._paid_fs, TransportRoute.PAID_FLARESOLVERR)
        self._direct_gate_circuits.clear()
        resources = [self._direct_http, self._direct_fs, self._paid_fs]
        seen: set[int] = set()
        for resource in resources:
            if id(resource) in seen:
                continue
            seen.add(id(resource))
            close = getattr(resource, "close", None)
            if callable(close):
                close()
        proxy_close = getattr(self._proxy_client, "close_session", None)
        if callable(proxy_close):
            proxy_close()

    def __enter__(self) -> "WhoScoredTransport":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False


def _response_wire_bytes(response: Any, content: bytes) -> int:
    """Best available receive size; curl_cffi exposes ``response_size``."""

    for value in (
        # curl_cffi's response_size is download_size + HTTP header_size.
        getattr(response, "response_size", None),
        getattr(response, "download_size", None),
        getattr(response, "downloaded", None),
    ):
        try:
            if value is not None and int(value) >= 0:
                return int(value)
        except (TypeError, ValueError):
            pass
    return len(content)


def _request_wire_bytes(response: Any) -> int:
    """Best available request/upload transfer size exposed by curl clients."""
    for value in (
        # curl_cffi's request_size includes the request headers and body. A GET
        # still exposes upload_size=0, so checking upload_size first would hide
        # every non-empty HTTP request.
        getattr(response, "request_size", None),
        getattr(response, "upload_size", None),
        getattr(response, "uploaded", None),
    ):
        try:
            if value is not None and int(value) >= 0:
                return int(value)
        except (TypeError, ValueError):
            pass
    return 0


def _logical_url_key(url: str) -> str:
    return _canonical_url_key(url)


def _canonical_url_key(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme and parts.netloc:
        query = urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True)))
        return urlunsplit(
            (
                parts.scheme.lower(),
                parts.netloc.lower(),
                parts.path or "/",
                query,
                "",
            )
        )
    return url.split("?", 1)[0].split("#", 1)[0]


def _entity_from_cache_key(cache_key: str) -> str:
    if cache_key.startswith("whoscored:"):
        tokens = cache_key.split(":", 2)
        if len(tokens) > 1:
            return tokens[1]
    return ""


__all__ = [
    "CachedPayload",
    "CloudflareChallenge",
    "FailureKind",
    "JsonlRequestLedger",
    "ProxyFilterClient",
    "ProxyBudgetRejected",
    "ProxyConcurrencyLimited",
    "ProxyLease",
    "RawCacheHook",
    "RequestLedger",
    "TransportBudgetExceeded",
    "TransportBudgets",
    "TransportResponse",
    "TransportRoute",
    "TransportStats",
    "TransportContext",
    "WhoScoredTransport",
    "WhoScoredTransportError",
    "is_cloudflare_response",
]
