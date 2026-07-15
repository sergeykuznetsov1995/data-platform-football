"""Single production FBref transport: Camoufox clearance plus warm HTTP."""

from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional, Sequence

from scrapers.fbref.camoufox_fetch import CamoufoxFbrefTransport
from scrapers.fbref.settings import (
    DEFAULT_BROWSER_BYTE_LIMIT_BYTES,
    DEFAULT_BROWSER_REQUESTS_PER_SOLVE,
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    DEFAULT_HTTP_BODY_LIMIT_BYTES,
)
from scrapers.utils.proxy_manager import Proxy, ProxyManager, classify_error


FETCHER_VERSION = "fbref-camoufox-warm-http-v2"
DEFAULT_BOOTSTRAP_URL = "https://fbref.com/en/"
MAX_HTML_BYTES = DEFAULT_HTTP_BODY_LIMIT_BYTES
# The browser cap bounds ONE clearance attempt; the run's reservation covers
# every attempt (see DEFAULT_BOOTSTRAP_REQUEST_RESERVATION).
DEFAULT_BROWSER_REQUEST_LIMIT = DEFAULT_BROWSER_REQUESTS_PER_SOLVE
DEFAULT_BROWSER_BYTE_LIMIT = DEFAULT_BROWSER_BYTE_LIMIT_BYTES
MAX_TARGET_HTTP_ATTEMPTS = 2
RETRYABLE_HTTP_STATUSES = frozenset({500, 502, 503, 504})
DEFAULT_STATUS_RETRY_DELAY_SECONDS = DEFAULT_DOMAIN_INTERVAL_SECONDS
DEFAULT_PROXY_COOLDOWN_WAIT_SECONDS = 60.0
_FAILURE_EVIDENCE_HEADERS = (
    "content-type",
    "content-length",
    "server",
    "via",
    "cf-ray",
    "x-cache",
)

# curl_cffi 0.15 exposes this only from its internal ``curl`` module. Returning
# the libcurl sentinel from a native content callback stops the transfer at the
# current chunk instead of buffering the rest of an oversized response. Keep a
# fallback so import-light offline tests do not require the optional transport.
try:  # pragma: no cover - curl_cffi is installed in the production image
    from curl_cffi.curl import CURL_WRITEFUNC_ERROR as _CURL_WRITEFUNC_ERROR
except ImportError:  # pragma: no cover - exercised by the import-light suite
    _CURL_WRITEFUNC_ERROR = 0xFFFFFFFF


class _CumulativeBodyBuffer:
    """Bound response bodies across every HTTP attempt for one logical fetch."""

    def __init__(self, limit: int) -> None:
        self.limit = int(limit)
        self.total_seen = 0
        self.attempt_seen = 0
        self.exceeded = False
        self._body = bytearray()

    def begin_attempt(self) -> None:
        self.attempt_seen = 0
        self.exceeded = False
        self._body.clear()

    def receive(self, chunk: bytes) -> int:
        payload = bytes(chunk)
        size = len(payload)
        self.attempt_seen += size
        self.total_seen += size
        if self.total_seen > self.limit:
            self.exceeded = True
            return _CURL_WRITEFUNC_ERROR
        self._body.extend(payload)
        return size

    @property
    def body(self) -> bytes:
        return bytes(self._body)

logger = logging.getLogger(__name__)


def _response_wire_size(response) -> int:
    """Best available libcurl request + response byte count."""

    components = []
    for attr in ("download_size", "header_size", "request_size", "upload_size"):
        value = getattr(response, attr, None)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            components.append(max(0, int(value)))
    if components and sum(components) > 0:
        return sum(components)
    content = getattr(response, "content", None)
    if isinstance(content, (bytes, bytearray)):
        return len(content)
    return len((getattr(response, "text", "") or "").encode("utf-8"))


def _partial_response_wire_size(response, observed_body_bytes: int) -> int:
    """Include callback-observed body when curl reports a write-abort as zero."""

    names = ("download_size", "header_size", "request_size", "upload_size")
    values = {
        name: getattr(response, name, None)
        for name in names
    }
    if all(
        isinstance(value, (int, float)) and not isinstance(value, bool)
        for value in values.values()
    ):
        return (
            max(0, int(values["header_size"]))
            + max(0, int(values["request_size"]))
            + max(0, int(values["upload_size"]))
            + max(
                max(0, int(values["download_size"])),
                max(0, int(observed_body_bytes)),
            )
        )
    return max(
        int(_response_wire_size(response)),
        max(0, int(observed_body_bytes)),
    )


class FetchError(RuntimeError):
    """A classified target fetch failure."""

    def __init__(
        self,
        message: str,
        *,
        error_class: str,
        http_status: Optional[int] = None,
        wire_bytes: int = 0,
        browser_document_bytes: int = 0,
        browser_asset_bytes: int = 0,
        browser_requests: int = 0,
        browser_bootstrap_attempts: int = 0,
        browser_unobserved_bytes: int = 0,
        provider_billed_bytes: Optional[int] = None,
        target_requests: int = 0,
        target_request_made: bool = False,
        http_requests: Optional[int] = None,
        http_status_history: Optional[Sequence[int]] = None,
        latency_ms: int = 0,
    ) -> None:
        super().__init__(message)
        self.error_class = error_class
        self.http_status = http_status
        self.wire_bytes = wire_bytes
        self.browser_document_bytes = browser_document_bytes
        self.browser_asset_bytes = browser_asset_bytes
        self.browser_requests = browser_requests
        self.browser_bootstrap_attempts = max(
            0, int(browser_bootstrap_attempts)
        )
        self.browser_unobserved_bytes = max(
            0, int(browser_unobserved_bytes)
        )
        self.provider_billed_bytes = provider_billed_bytes
        legacy_request_count = int(
            target_requests or int(bool(target_request_made))
        )
        request_count = (
            legacy_request_count
            if http_requests is None
            else int(http_requests)
        )
        if request_count < 0 or (
            http_requests is not None
            and legacy_request_count
            and request_count != legacy_request_count
        ):
            raise ValueError("FetchError has inconsistent HTTP request counts")
        self.target_requests = request_count
        # Compatibility for older callers; accounting uses the exact count.
        self.target_request_made = self.target_requests > 0
        self.http_requests = self.target_requests
        history = tuple(
            int(status) for status in (http_status_history or ())
        )
        if not history and self.http_requests == 1 and self.http_status is not None:
            history = (int(self.http_status),)
        self.http_status_history = history
        self.latency_ms = max(0, int(latency_ms))


@dataclass(frozen=True)
class FetchResponse:
    url: str
    status_code: int
    body: bytes
    headers: Mapping[str, str]
    latency_ms: int
    http_wire_bytes: int
    decoded_html_bytes: int
    http_requests: int = 1
    browser_document_bytes: int = 0
    browser_asset_bytes: int = 0
    browser_requests: int = 0
    browser_bootstrap_attempts: int = 0
    browser_unobserved_bytes: int = 0
    provider_billed_bytes: Optional[int] = None
    http_status_history: tuple[int, ...] = field(default_factory=tuple)
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        request_count = int(self.http_requests)
        bootstrap_attempts = int(self.browser_bootstrap_attempts)
        unobserved_bytes = int(self.browser_unobserved_bytes)
        history = tuple(int(status) for status in self.http_status_history)
        if not history and request_count == 1:
            history = (int(self.status_code),)
        if (
            request_count < 0
            or bootstrap_attempts < 0
            or unobserved_bytes < 0
            or len(history) != request_count
            or any(not 100 <= status <= 599 for status in history)
            or (history and history[-1] != int(self.status_code))
        ):
            raise ValueError("FetchResponse has inconsistent HTTP observations")
        object.__setattr__(self, "http_requests", request_count)
        object.__setattr__(
            self, "browser_bootstrap_attempts", bootstrap_attempts
        )
        object.__setattr__(self, "browser_unobserved_bytes", unobserved_bytes)
        object.__setattr__(self, "http_status_history", history)


class FBrefFetcher:
    """Own one clearance lease and fetch every target through warm HTTP.

    Camoufox only navigates the stable bootstrap page.  Target responses are
    read from ``curl_cffi.Response.content`` and committed to raw storage before
    any parser sees them.
    """

    def __init__(
        self,
        *,
        proxy_file: Optional[str] = None,
        bootstrap_url: str = DEFAULT_BOOTSTRAP_URL,
        max_html_bytes: int = MAX_HTML_BYTES,
        max_browser_requests: int = DEFAULT_BROWSER_REQUEST_LIMIT,
        max_browser_bytes: int = DEFAULT_BROWSER_BYTE_LIMIT,
        max_target_http_attempts: int = MAX_TARGET_HTTP_ATTEMPTS,
        status_retry_delay_seconds: float = DEFAULT_STATUS_RETRY_DELAY_SECONDS,
        min_healthy_proxies: int = 1,
        proxy_validation_timeout_seconds: float = 5.0,
        proxy_cooldown_wait_seconds: float = DEFAULT_PROXY_COOLDOWN_WAIT_SECONDS,
        sleep=time.sleep,
    ) -> None:
        self.bootstrap_url = bootstrap_url
        self.max_html_bytes = int(max_html_bytes)
        if self.max_html_bytes <= 0:
            raise ValueError("max_html_bytes must be positive")
        attempts = int(max_target_http_attempts)
        if not 1 <= attempts <= MAX_TARGET_HTTP_ATTEMPTS:
            raise ValueError(
                f"max_target_http_attempts must be 1-{MAX_TARGET_HTTP_ATTEMPTS}"
            )
        retry_delay = float(status_retry_delay_seconds)
        if retry_delay < 0:
            raise ValueError("status_retry_delay_seconds must be non-negative")
        self.max_target_http_attempts = attempts
        self.status_retry_delay_seconds = retry_delay
        self._sleep = sleep
        self._max_browser_requests = int(max_browser_requests)
        self._max_browser_bytes = int(max_browser_bytes)
        self._proxy_cooldown_wait_seconds = float(proxy_cooldown_wait_seconds)
        if self._max_browser_requests <= 0 or self._max_browser_bytes <= 0:
            raise ValueError("browser request/byte limits must be positive")
        if self._proxy_cooldown_wait_seconds < 0:
            raise ValueError("proxy_cooldown_wait_seconds must be non-negative")
        self._proxy_manager: Optional[ProxyManager] = None
        self._current_proxy: Optional[Proxy] = None
        if proxy_file:
            path = Path(proxy_file)
            if not path.is_file():
                raise FileNotFoundError(f"FBref proxy file not found: {path}")
            manager = ProxyManager(rotation_strategy="random")
            loaded = manager.load_from_file_custom_format(str(path))
            if loaded <= 0:
                raise ValueError(f"FBref proxy file contains no proxies: {path}")
            health = manager.validate_proxies(
                timeout=float(proxy_validation_timeout_seconds),
                max_workers=min(50, loaded),
                ban_failed=True,
            )
            minimum = int(min_healthy_proxies)
            if minimum <= 0:
                raise ValueError("min_healthy_proxies must be positive")
            if int(health["alive"]) < minimum:
                raise ValueError(
                    "FBref proxy preflight found "
                    f"{health['alive']} healthy exits; {minimum} required"
                )
            self._proxy_manager = manager
        self._transport = self._create_transport()
        self._http_session = None
        self._bootstrap_stats: Optional[dict] = None

    def _create_transport(self) -> CamoufoxFbrefTransport:
        return CamoufoxFbrefTransport(
            proxy_provider=self._next_proxy,
            proxy_result_callback=self._record_proxy_result,
            geoip=True,
            headless=True,
            humanize=True,
            block_resources=True,
            max_network_requests=self._max_browser_requests,
            max_network_bytes=self._max_browser_bytes,
        )

    def __enter__(self) -> "FBrefFetcher":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def close(self) -> None:
        if self._http_session is not None:
            try:
                self._http_session.close()
            finally:
                self._http_session = None
        if self._transport is not None:
            self._transport.close()

    def reset_clearance(
        self, *, error_type: Optional[str] = "cloudflare"
    ) -> None:
        """Drop the dead clearance while retaining in-run proxy quarantine."""

        if (
            error_type
            and self._proxy_manager is not None
            and self._current_proxy is not None
        ):
            self._proxy_manager.record_result(
                self._current_proxy,
                success=False,
                error_type=error_type,
            )
        if self._http_session is not None:
            try:
                self._http_session.close()
            finally:
                self._http_session = None
        if self._transport is not None:
            self._transport.close()
        self._current_proxy = None
        self._bootstrap_stats = None
        self._transport = self._create_transport()

    def _next_proxy(self) -> Optional[dict]:
        if self._proxy_manager is None:
            self._current_proxy = None
            return None
        proxy = self._proxy_manager.get_proxy(
            max_cooldown_wait_seconds=self._proxy_cooldown_wait_seconds,
            sleep=self._sleep,
        )
        self._current_proxy = proxy
        if proxy is None:
            raise RuntimeError("fbref_proxy_pool_unavailable")
        result = {"server": f"{proxy.proxy_type.value}://{proxy.host}:{proxy.port}"}
        if proxy.username:
            result["username"] = proxy.username
            result["password"] = proxy.password or ""
        return result

    def _record_proxy_result(
        self, success: bool, error_type: Optional[str] = None
    ) -> None:
        if self._proxy_manager is None or self._current_proxy is None:
            return
        self._proxy_manager.record_result(
            self._current_proxy,
            success=success,
            error_type=error_type,
        )

    @staticmethod
    def _proxy_url(proxy: Optional[Mapping[str, str]]) -> Optional[str]:
        if not proxy or not proxy.get("server"):
            return None
        server = str(proxy["server"])
        scheme, separator, host = server.partition("://")
        if not separator:
            scheme, host = "http", server
        return f"{scheme}://{host}"

    @staticmethod
    def _proxy_auth(
        proxy: Optional[Mapping[str, str]],
    ) -> Optional[tuple[str, str]]:
        if not proxy or not proxy.get("username"):
            return None
        return str(proxy["username"]), str(proxy.get("password") or "")

    @staticmethod
    def _create_http_session(clearance: Mapping[str, object]):
        from curl_cffi.requests import Session

        proxy = clearance.get("proxy")
        proxy_url = FBrefFetcher._proxy_url(proxy)
        proxy_auth = FBrefFetcher._proxy_auth(proxy)
        session = Session(
            impersonate=os.environ.get("FBREF_HTTP_IMPERSONATE", "firefox135"),
            proxy=proxy_url,
            proxy_auth=proxy_auth,
            # Never allow container HTTP(S)_PROXY variables to break the
            # clearance lease's exit-IP affinity.
            trust_env=False,
            # Status retries are performed explicitly so request and byte
            # accounting remains exact.
            retry=0,
        )
        session.cookies.update(dict(clearance["cookies"]))
        session.headers.update({
            "User-Agent": str(clearance.get("user_agent") or ""),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
        })
        return session

    def _full_browser_reservation_breakdown(
        self,
    ) -> tuple[int, int, int, int, int]:
        attempts = max(
            1,
            (
                self._max_browser_requests
                + DEFAULT_BROWSER_REQUESTS_PER_SOLVE
                - 1
            )
            // DEFAULT_BROWSER_REQUESTS_PER_SOLVE,
        )
        return (
            0,
            0,
            self._max_browser_requests,
            attempts,
            self._max_browser_bytes,
        )

    @staticmethod
    def _close_browser_and_collect_traffic(transport) -> tuple[dict, object]:
        """Stop all browser traffic before exporting its final accounting."""

        close_error = None
        try:
            transport.close()
        except Exception as exc:  # noqa: BLE001 - hard lifecycle boundary
            close_error = exc
            kill = getattr(transport, "_kill_browser_processes", None)
            if callable(kill):
                try:
                    kill("clearance-finalize", 0)
                except Exception:  # noqa: BLE001 - retain original close error
                    logger.exception("FBref browser emergency kill failed")
        try:
            stats = dict(transport.traffic_delta())
        except Exception as exc:  # noqa: BLE001 - caller charges full reserve
            return {}, close_error or exc
        return stats, close_error

    def _ensure_clearance(self) -> None:
        if self._http_session is not None:
            return
        transport = self._transport
        html = None
        bootstrap_error = None
        try:
            html = transport.fetch(self.bootstrap_url)
        except Exception as exc:
            bootstrap_error = exc

        if bootstrap_error is not None or not html:
            bootstrap_stats, finalize_error = (
                self._close_browser_and_collect_traffic(transport)
            )
            breakdown = (
                self._full_browser_reservation_breakdown()
                if finalize_error is not None
                else self._browser_breakdown(bootstrap_stats)
            )
            raise FetchError(
                (
                    "Camoufox clearance bootstrap failed: "
                    f"{type(bootstrap_error).__name__}"
                    if bootstrap_error is not None
                    else "Camoufox could not establish an FBref clearance lease"
                ),
                error_class="clearance_failed",
                browser_document_bytes=breakdown[0],
                browser_asset_bytes=breakdown[1],
                browser_requests=breakdown[2],
                browser_bootstrap_attempts=breakdown[3],
                browser_unobserved_bytes=breakdown[4],
            ) from bootstrap_error

        clearance_error = None
        try:
            clearance = transport.get_clearance()
        except Exception as exc:
            clearance = None
            clearance_error = exc
        if clearance_error is None and not clearance:
            clearance_error = RuntimeError("no usable clearance exported")

        http_session = None
        if clearance_error is None:
            try:
                http_session = self._create_http_session(clearance)
            except Exception as exc:
                clearance_error = exc

        bootstrap_stats, finalize_error = (
            self._close_browser_and_collect_traffic(transport)
        )
        breakdown = (
            self._full_browser_reservation_breakdown()
            if finalize_error is not None
            else self._browser_breakdown(bootstrap_stats)
        )
        if finalize_error is not None:
            if http_session is not None:
                http_session.close()
            raise FetchError(
                "Camoufox browser finalization/accounting failed: "
                f"{type(finalize_error).__name__}",
                error_class="clearance_failed",
                browser_document_bytes=breakdown[0],
                browser_asset_bytes=breakdown[1],
                browser_requests=breakdown[2],
                browser_bootstrap_attempts=breakdown[3],
                browser_unobserved_bytes=breakdown[4],
            ) from finalize_error
        if clearance_error is not None:
            raise FetchError(
                "Camoufox clearance export failed: "
                f"{type(clearance_error).__name__}",
                error_class="clearance_export_failed",
                browser_document_bytes=breakdown[0],
                browser_asset_bytes=breakdown[1],
                browser_requests=breakdown[2],
                browser_bootstrap_attempts=breakdown[3],
                browser_unobserved_bytes=breakdown[4],
            ) from clearance_error
        self._http_session = http_session
        self._bootstrap_stats = bootstrap_stats

    @staticmethod
    def _browser_breakdown(
        stats: Optional[dict],
    ) -> tuple[int, int, int, int, int]:
        if not stats:
            return 0, 0, 0, 0, 0
        by_type = stats.get("real_bytes_by_resource_type") or {}
        document = int(by_type.get("document", 0) or 0)
        total = int(stats.get("real_bytes_downloaded", 0) or 0)
        return (
            document,
            max(0, total - document),
            int(stats.get("real_requests_count", 0) or 0),
            int(stats.get("browser_bootstrap_attempts", 0) or 0),
            int(stats.get("budget_unobserved_bytes", 0) or 0),
        )

    @staticmethod
    def _raw_html_failure(html: str) -> Optional[str]:
        """Reject block pages without imposing a dataset/table contract.

        Awarded, restricted, empty, and not-applicable FBref pages are still
        valid raw evidence. Their availability is classified offline after
        the exact response has been committed.
        """

        lowered = html.casefold()
        if any(
            marker in lowered
            for marker in (
                "just a moment",
                "checking your browser",
                "cf-browser-verification",
                "challenge-running",
            )
        ):
            return "cloudflare_challenge"
        if not any(marker in lowered for marker in ("<html", "<!doctype html")):
            return "not_html_document"
        return None

    @staticmethod
    def _warm_session_error_class(
        exc: Exception, partial_status: Optional[int]
    ) -> str:
        """Classify transport/proxy poison without masking target failures."""

        status_types = {
            401: "forbidden",
            403: "forbidden",
            429: "rate_limit",
        }
        error_type = status_types.get(partial_status)
        if error_type is None:
            error_type = classify_error(str(exc))
        if error_type in {
            "cloudflare",
            "connection",
            "forbidden",
            "rate_limit",
            "timeout",
        }:
            return f"warm_session_{error_type}"
        return "http_exception"

    @staticmethod
    def _safe_header_value(value: object) -> str:
        rendered = " ".join(str(value or "").split())[:160]
        return "".join(
            character
            if character.isalnum() or character in " ._:/;=()[]-"
            else "?"
            for character in rendered
        )

    @classmethod
    def _failure_response_evidence(cls, response, body: bytes) -> str:
        headers = {
            str(key).casefold(): value
            for key, value in dict(getattr(response, "headers", {}) or {}).items()
        }
        evidence = [
            f"body_bytes={len(body)}",
            f"body_sha256={hashlib.sha256(body).hexdigest()}",
        ]
        for name in _FAILURE_EVIDENCE_HEADERS:
            value = cls._safe_header_value(headers.get(name))
            if value:
                evidence.append(f"{name.replace('-', '_')}={value}")
        return ",".join(evidence)

    def fetch(
        self,
        url: str,
        *,
        page_kind: str,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> FetchResponse:
        self._ensure_clearance()
        headers = {}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified

        started = time.perf_counter()
        response = None
        target_requests = 0
        wire_bytes = 0
        status_history: list[int] = []
        body_buffer = _CumulativeBodyBuffer(self.max_html_bytes)
        for attempt in range(self.max_target_http_attempts):
            target_requests += 1
            body_buffer.begin_attempt()
            try:
                response = self._http_session.get(
                    url,
                    headers=headers or None,
                    timeout=30,
                    allow_redirects=False,
                    content_callback=body_buffer.receive,
                )
            except Exception as exc:
                partial_response = getattr(exc, "response", None)
                partial_status = (
                    None
                    if partial_response is None
                    else int(partial_response.status_code or 0) or None
                )
                failure_status_history = tuple(status_history) + (
                    () if partial_status is None else (partial_status,)
                )
                if partial_response is not None:
                    wire_bytes += _partial_response_wire_size(
                        partial_response,
                        body_buffer.attempt_seen,
                    )
                else:
                    # The callback proves these body bytes crossed the
                    # transport boundary even if curl_cffi did not attach its
                    # partial Response metrics to the write error.
                    wire_bytes += body_buffer.attempt_seen
                (
                    browser_document,
                    browser_asset,
                    browser_requests,
                    browser_bootstrap_attempts,
                    browser_unobserved_bytes,
                ) = (
                    self._browser_breakdown(self._bootstrap_stats)
                )
                self._bootstrap_stats = None
                latency_ms = int((time.perf_counter() - started) * 1000)
                if body_buffer.exceeded:
                    raise FetchError(
                        "FBref cumulative response bodies exceeded "
                        f"{self.max_html_bytes} bytes for {url}",
                        error_class="response_too_large",
                        http_status=(
                            partial_status
                        ),
                        wire_bytes=wire_bytes,
                        browser_document_bytes=browser_document,
                        browser_asset_bytes=browser_asset,
                        browser_requests=browser_requests,
                        browser_bootstrap_attempts=browser_bootstrap_attempts,
                        browser_unobserved_bytes=browser_unobserved_bytes,
                        target_requests=target_requests,
                        http_status_history=failure_status_history,
                        latency_ms=latency_ms,
                    ) from exc
                raise FetchError(
                    "Warm HTTP request failed after "
                    f"{target_requests} attempt(s): {type(exc).__name__}",
                    error_class=self._warm_session_error_class(
                        exc, partial_status
                    ),
                    wire_bytes=wire_bytes,
                    browser_document_bytes=browser_document,
                    browser_asset_bytes=browser_asset,
                    browser_requests=browser_requests,
                    browser_bootstrap_attempts=browser_bootstrap_attempts,
                    browser_unobserved_bytes=browser_unobserved_bytes,
                    target_requests=target_requests,
                    http_status_history=failure_status_history,
                    latency_ms=latency_ms,
                ) from exc
            # Network-free fakes and older adapters may expose ``content``
            # without invoking the callback.  Feed it through the same bound;
            # real curl_cffi responses using content_callback have no body
            # buffered on Response.
            if body_buffer.attempt_seen == 0:
                fallback_body = bytes(getattr(response, "content", None) or b"")
                if fallback_body:
                    body_buffer.receive(fallback_body)
            wire_bytes += int(_response_wire_size(response))
            status = int(response.status_code)
            status_history.append(status)
            if body_buffer.exceeded:
                (
                    browser_document,
                    browser_asset,
                    browser_requests,
                    browser_bootstrap_attempts,
                    browser_unobserved_bytes,
                ) = (
                    self._browser_breakdown(self._bootstrap_stats)
                )
                self._bootstrap_stats = None
                latency_ms = int((time.perf_counter() - started) * 1000)
                raise FetchError(
                    "FBref cumulative response bodies exceeded "
                    f"{self.max_html_bytes} bytes for {url}",
                    error_class="response_too_large",
                    http_status=status,
                    wire_bytes=wire_bytes,
                    browser_document_bytes=browser_document,
                    browser_asset_bytes=browser_asset,
                    browser_requests=browser_requests,
                    browser_bootstrap_attempts=browser_bootstrap_attempts,
                    browser_unobserved_bytes=browser_unobserved_bytes,
                    target_requests=target_requests,
                    http_status_history=tuple(status_history),
                    latency_ms=latency_ms,
                )
            if (
                status in RETRYABLE_HTTP_STATUSES
                and attempt + 1 < self.max_target_http_attempts
            ):
                self._sleep(self.status_retry_delay_seconds)
                continue
            break

        if response is None:  # pragma: no cover - loop always attempts once
            raise RuntimeError("Warm HTTP loop produced no response")
        latency_ms = int((time.perf_counter() - started) * 1000)
        status = int(response.status_code)
        body = body_buffer.body
        (
            browser_document,
            browser_asset,
            browser_requests,
            browser_bootstrap_attempts,
            browser_unobserved_bytes,
        ) = (
            self._browser_breakdown(self._bootstrap_stats)
        )
        # Bootstrap bytes belong to the lease, not every page. Emit them once.
        self._bootstrap_stats = None

        if status == 304:
            return FetchResponse(
                url=url,
                status_code=status,
                body=b"",
                headers=dict(response.headers),
                latency_ms=latency_ms,
                http_wire_bytes=wire_bytes,
                decoded_html_bytes=0,
                http_requests=target_requests,
                http_status_history=tuple(status_history),
                browser_document_bytes=browser_document,
                browser_asset_bytes=browser_asset,
                browser_requests=browser_requests,
                browser_bootstrap_attempts=browser_bootstrap_attempts,
                browser_unobserved_bytes=browser_unobserved_bytes,
            )
        if status != 200:
            evidence = self._failure_response_evidence(response, body)
            raise FetchError(
                f"FBref returned HTTP {status} for {url}; "
                f"attempts={target_requests}; "
                f"status_history={','.join(map(str, status_history))}; "
                f"{evidence}",
                error_class="http_status",
                http_status=status,
                wire_bytes=wire_bytes,
                browser_document_bytes=browser_document,
                browser_asset_bytes=browser_asset,
                browser_requests=browser_requests,
                browser_bootstrap_attempts=browser_bootstrap_attempts,
                browser_unobserved_bytes=browser_unobserved_bytes,
                target_requests=target_requests,
                http_status_history=tuple(status_history),
                latency_ms=latency_ms,
            )
        if not body:
            raise FetchError(
                f"FBref returned an empty body for {url}",
                error_class="empty_body",
                http_status=status,
                wire_bytes=wire_bytes,
                browser_document_bytes=browser_document,
                browser_asset_bytes=browser_asset,
                browser_requests=browser_requests,
                browser_bootstrap_attempts=browser_bootstrap_attempts,
                browser_unobserved_bytes=browser_unobserved_bytes,
                target_requests=target_requests,
                http_status_history=tuple(status_history),
                latency_ms=latency_ms,
            )
        if len(body) > self.max_html_bytes:
            raise FetchError(
                f"FBref body exceeded {self.max_html_bytes} bytes for {url}",
                error_class="response_too_large",
                http_status=status,
                wire_bytes=wire_bytes,
                browser_document_bytes=browser_document,
                browser_asset_bytes=browser_asset,
                browser_requests=browser_requests,
                browser_bootstrap_attempts=browser_bootstrap_attempts,
                browser_unobserved_bytes=browser_unobserved_bytes,
                target_requests=target_requests,
                http_status_history=tuple(status_history),
                latency_ms=latency_ms,
            )
        try:
            html = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise FetchError(
                f"FBref response is not UTF-8 for {url}",
                error_class="invalid_encoding",
                http_status=status,
                wire_bytes=wire_bytes,
                browser_document_bytes=browser_document,
                browser_asset_bytes=browser_asset,
                browser_requests=browser_requests,
                browser_bootstrap_attempts=browser_bootstrap_attempts,
                browser_unobserved_bytes=browser_unobserved_bytes,
                target_requests=target_requests,
                http_status_history=tuple(status_history),
                latency_ms=latency_ms,
            ) from exc
        content_type = str(response.headers.get("content-type") or "").casefold()
        if content_type and "html" not in content_type:
            raise FetchError(
                f"FBref returned non-HTML content for {url}: {content_type}",
                error_class="invalid_content_type",
                http_status=status,
                wire_bytes=wire_bytes,
                browser_document_bytes=browser_document,
                browser_asset_bytes=browser_asset,
                browser_requests=browser_requests,
                browser_bootstrap_attempts=browser_bootstrap_attempts,
                browser_unobserved_bytes=browser_unobserved_bytes,
                target_requests=target_requests,
                http_status_history=tuple(status_history),
                latency_ms=latency_ms,
            )
        reason = self._raw_html_failure(html)
        if reason is not None:
            raise FetchError(
                f"FBref raw response contract failed for {url}: {reason}",
                error_class=f"raw_contract_{reason}",
                http_status=status,
                wire_bytes=wire_bytes,
                browser_document_bytes=browser_document,
                browser_asset_bytes=browser_asset,
                browser_requests=browser_requests,
                browser_bootstrap_attempts=browser_bootstrap_attempts,
                browser_unobserved_bytes=browser_unobserved_bytes,
                target_requests=target_requests,
                http_status_history=tuple(status_history),
                latency_ms=latency_ms,
            )
        return FetchResponse(
            url=url,
            status_code=status,
            body=body,
            headers=dict(response.headers),
            latency_ms=latency_ms,
            http_wire_bytes=wire_bytes,
            decoded_html_bytes=len(body),
            http_requests=target_requests,
            http_status_history=tuple(status_history),
            browser_document_bytes=browser_document,
            browser_asset_bytes=browser_asset,
            browser_requests=browser_requests,
            browser_bootstrap_attempts=browser_bootstrap_attempts,
            browser_unobserved_bytes=browser_unobserved_bytes,
            metadata={"page_kind": page_kind, "fetcher_version": FETCHER_VERSION},
        )


__all__ = [
    "DEFAULT_BROWSER_BYTE_LIMIT",
    "DEFAULT_BROWSER_REQUEST_LIMIT",
    "DEFAULT_BOOTSTRAP_URL",
    "DEFAULT_PROXY_COOLDOWN_WAIT_SECONDS",
    "FETCHER_VERSION",
    "FBrefFetcher",
    "FetchError",
    "FetchResponse",
    "MAX_HTML_BYTES",
    "MAX_TARGET_HTTP_ATTEMPTS",
    "RETRYABLE_HTTP_STATUSES",
]
