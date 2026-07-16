"""Single production FBref transport: Camoufox clearance plus warm HTTP."""

from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass, field, replace
from typing import Mapping, Optional, Sequence

from scrapers.fbref.camoufox_fetch import CamoufoxFbrefTransport
from scrapers.fbref.proxy_lease import (
    DEFAULT_LEASE_TTL_SECONDS,
    FBrefLeaseStats,
    FBrefProxyLease,
    FBrefProxyLeaseClient,
    FBrefProxyLeaseError,
)
from scrapers.fbref.settings import (
    DEFAULT_BROWSER_BYTE_LIMIT_BYTES,
    DEFAULT_BROWSER_REQUESTS_PER_SOLVE,
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    DEFAULT_HTTP_BODY_LIMIT_BYTES,
)
from scrapers.utils.proxy_manager import classify_error


FETCHER_VERSION = "fbref-camoufox-metered-warm-http-v5"
DEFAULT_BOOTSTRAP_URL = "https://fbref.com/en/"
MAX_HTML_BYTES = DEFAULT_HTTP_BODY_LIMIT_BYTES
# The browser cap bounds ONE clearance attempt; the run's reservation covers
# every attempt (see DEFAULT_BOOTSTRAP_REQUEST_RESERVATION).
DEFAULT_BROWSER_REQUEST_LIMIT = DEFAULT_BROWSER_REQUESTS_PER_SOLVE
DEFAULT_BROWSER_BYTE_LIMIT = DEFAULT_BROWSER_BYTE_LIMIT_BYTES
MAX_TARGET_HTTP_ATTEMPTS = 2
RETRYABLE_HTTP_STATUSES = frozenset({500, 502, 503, 504})
DEFAULT_STATUS_RETRY_DELAY_SECONDS = DEFAULT_DOMAIN_INTERVAL_SECONDS
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
        proxy_control_url: Optional[str] = None,
        proxy_control_token: Optional[str] = None,
        provider_context: Optional[Mapping[str, object]] = None,
        provider_max_bytes: Optional[int] = None,
        provider_lease_ttl_seconds: Optional[int] = None,
        lease_client: Optional[FBrefProxyLeaseClient] = None,
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
        if self._max_browser_requests <= 0 or self._max_browser_bytes <= 0:
            raise ValueError("browser request/byte limits must be positive")
        self._lease_client: Optional[FBrefProxyLeaseClient] = None
        self._provider_context: dict[str, object] = {}
        self._provider_max_bytes = 0
        self._provider_lease_ttl_seconds = 0
        self._provider_lease: Optional[FBrefProxyLease] = None
        self._provider_lease_observed_bytes = 0
        self._provider_total_bytes = 0
        self._provider_bootstrap_max_bytes = 0
        self._provider_bootstrap_spent_bytes = 0
        self._provider_http_ready = False
        self._clearance: Optional[dict] = None
        configured_control_url = str(
            proxy_control_url
            or os.environ.get("FBREF_PROXY_CONTROL_URL")
            or ""
        ).strip()
        paid_proxy_requested = bool(
            proxy_file
            or configured_control_url
            or lease_client is not None
            or provider_context is not None
            or provider_max_bytes is not None
        )
        if paid_proxy_requested:
            if lease_client is None:
                if not configured_control_url:
                    raise FBrefProxyLeaseError(
                        "FBref paid proxy requires FBREF_PROXY_CONTROL_URL; "
                        "direct proxy credentials are forbidden"
                    )
                lease_client = FBrefProxyLeaseClient(
                    configured_control_url,
                    control_token=proxy_control_token,
                )
            context = dict(provider_context or {})
            required = ("dag_id", "run_id", "task_id", "canonical_url")
            if not all(str(context.get(name) or "").strip() for name in required):
                raise FBrefProxyLeaseError(
                    "FBref paid proxy requires complete run provenance"
                )
            context["source"] = "fbref"
            maximum = int(provider_max_bytes or 0)
            ttl = int(
                provider_lease_ttl_seconds
                or os.environ.get("FBREF_PROXY_LEASE_TTL_SECONDS")
                or DEFAULT_LEASE_TTL_SECONDS
            )
            if maximum <= 0 or ttl <= 0:
                raise ValueError("FBref paid proxy byte and TTL caps must be positive")
            self._lease_client = lease_client
            self._provider_context = context
            self._provider_max_bytes = maximum
            self._provider_bootstrap_max_bytes = min(
                maximum,
                self._max_browser_bytes,
            )
            self._provider_lease_ttl_seconds = ttl
        self._transport = self._create_transport()
        self._http_session = None
        self._bootstrap_stats: Optional[dict] = None

    def _create_transport(self) -> CamoufoxFbrefTransport:
        return CamoufoxFbrefTransport(
            proxy_provider=self._next_proxy,
            geoip=True,
            headless=True,
            humanize=True,
            block_resources=True,
            max_network_requests=self._max_browser_requests,
            max_network_bytes=self._max_browser_bytes,
            preemptive_proxy_auth=self._lease_client is not None,
        )

    def __enter__(self) -> "FBrefFetcher":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def close(self) -> None:
        close_error = None
        if self._http_session is not None:
            try:
                self._http_session.close()
            except Exception as exc:  # noqa: BLE001 - finish lease regardless
                close_error = exc
            finally:
                self._http_session = None
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception as exc:  # noqa: BLE001 - finish lease regardless
                close_error = close_error or exc
        try:
            self._close_provider_lease()
        except Exception as exc:  # noqa: BLE001 - retain first lifecycle error
            close_error = close_error or exc
        if close_error is not None:
            raise close_error

    def reset_clearance(self) -> None:
        """Drop a dead clearance and its sticky metered lease."""

        if self._http_session is not None:
            try:
                self._http_session.close()
            finally:
                self._http_session = None
        if self._transport is not None:
            self._transport.close()
        self._close_provider_lease()
        self._provider_http_ready = False
        # A pipeline clearance refresh reserves a new browser phase.  Keep
        # rotations inside one transport cumulative, but do not carry the old
        # phase's spend into the newly reserved transport.
        self._provider_bootstrap_spent_bytes = 0
        self._bootstrap_stats = None
        self._clearance = None
        self._transport = self._create_transport()

    def _next_proxy(self) -> Optional[dict]:
        if self._lease_client is not None:
            if self._provider_http_ready:
                raise FBrefProxyLeaseError(
                    "FBref browser cannot rotate an HTTP-enabled paid lease"
                )
            self._close_provider_lease()
            run_remaining = self._provider_max_bytes - self._provider_total_bytes
            bootstrap_remaining = (
                self._provider_bootstrap_max_bytes
                - self._provider_bootstrap_spent_bytes
            )
            remaining = min(run_remaining, bootstrap_remaining)
            if remaining <= 0:
                raise FBrefProxyLeaseError(
                    "FBref browser provider byte budget exhausted"
                )
            lease = self._lease_client.acquire(
                max_bytes=remaining,
                ttl_seconds=self._provider_lease_ttl_seconds,
                metadata=self._provider_context,
            )
            self._provider_lease = lease
            self._provider_lease_observed_bytes = 0
            return self._lease_client.playwright_proxy(lease)
        return None

    def _observe_provider_stats(self, stats: FBrefLeaseStats) -> int:
        if self._provider_lease is None:
            raise FBrefProxyLeaseError("FBref proxy meter returned orphan stats")
        current = int(stats.total_bytes)
        if current < self._provider_lease_observed_bytes:
            raise FBrefProxyLeaseError("FBref proxy meter counter moved backwards")
        delta = current - self._provider_lease_observed_bytes
        self._provider_lease_observed_bytes = current
        self._provider_total_bytes += delta
        if not self._provider_http_ready:
            self._provider_bootstrap_spent_bytes += delta
        if self._provider_total_bytes > self._provider_max_bytes:
            raise FBrefProxyLeaseError("FBref proxy meter exceeded the run cap")
        if self._provider_bootstrap_spent_bytes > self._provider_bootstrap_max_bytes:
            raise FBrefProxyLeaseError(
                "FBref proxy meter exceeded the browser phase cap"
            )
        return delta

    def _wait_and_observe_provider(self) -> Optional[FBrefLeaseStats]:
        if self._lease_client is None or self._provider_lease is None:
            return None
        stats = self._lease_client.wait_drained(
            self._provider_lease,
            expected=self._provider_context,
        )
        self._observe_provider_stats(stats)
        return stats

    def _close_provider_lease(self) -> None:
        lease_client = getattr(self, "_lease_client", None)
        provider_lease = getattr(self, "_provider_lease", None)
        if lease_client is None or provider_lease is None:
            return
        lease = provider_lease
        stats = lease_client.close(
            lease,
            expected=self._provider_context,
        )
        self._observe_provider_stats(stats)
        # Clear ownership only after close + authoritative observation both
        # succeeded. On failure, _next_proxy must retry reconciliation instead
        # of acquiring a second paid lease with an unknown first balance.
        self._provider_lease = None
        self._provider_lease_observed_bytes = 0
        self._provider_http_ready = False

    def _extend_provider_lease_for_http(self) -> None:
        """Drain the browser, then widen the same lease for warm HTTP."""

        lease_client = getattr(self, "_lease_client", None)
        lease = getattr(self, "_provider_lease", None)
        if lease_client is None:
            return
        if lease is None:
            raise FBrefProxyLeaseError(
                "FBref browser clearance has no paid provider lease"
            )
        if self._provider_http_ready:
            return

        # This is the mandatory browser/HTTP phase boundary.  A timeout or a
        # late provider reservation aborts before the extend call and before a
        # curl session can send a target request.
        stats = self._wait_and_observe_provider()
        if (
            stats is None
            or stats.closed
            or stats.budget_exceeded
            or lease.expires_at <= time.time()
        ):
            raise FBrefProxyLeaseError(
                "FBref browser lease is not usable for warm HTTP"
            )
        previous_lease_spend = (
            self._provider_total_bytes - self._provider_lease_observed_bytes
        )
        desired_max = self._provider_max_bytes - previous_lease_spend
        if desired_max < lease.max_bytes:
            raise FBrefProxyLeaseError(
                "FBref provider lease exceeds its remaining run budget"
            )
        if desired_max == lease.max_bytes:
            # The run cap itself was smaller than the browser phase cap.  The
            # drained lease already has the exact final ceiling, so there is no
            # upward mutation for the control API to perform.
            self._provider_http_ready = True
            return
        extended = lease_client.extend(
            lease,
            max_bytes=desired_max,
            expected=self._provider_context,
        )
        if (
            extended.lease_id != lease.lease_id
            or extended.token != lease.token
            or extended.proxy_url != lease.proxy_url
            or extended.expires_at != lease.expires_at
            or extended.max_bytes != desired_max
        ):
            raise FBrefProxyLeaseError(
                "FBref proxy meter returned a different extended lease"
            )
        self._provider_lease = extended
        self._provider_http_ready = True

    def _finish_metered_fetch(self) -> None:
        """Close the HTTP tunnel, then read a final provider counter.

        Closing per logical fetch costs one new TCP/TLS connection on the next
        page, but it makes every emitted ``provider_billed_bytes`` exact: no
        keep-alive tail can be silently charged after the response is stored.
        The Cloudflare clearance cookie and sticky lease exit are still reused.
        """

        if self._lease_client is None:
            return
        if self._http_session is not None:
            try:
                self._http_session.close()
            finally:
                self._http_session = None
        self._wait_and_observe_provider()

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
        retained_clearance = getattr(self, "_clearance", None)
        if retained_clearance is not None:
            if (
                getattr(self, "_lease_client", None) is not None
                and not self._provider_http_ready
            ):
                raise FetchError(
                    "FBref paid lease was not extended for warm HTTP",
                    error_class="hard_transport_policy",
                )
            self._http_session = self._create_http_session(retained_clearance)
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
            hard_policy = self._hard_transport_policy_reason(bootstrap_stats)
            if finalize_error is not None and hard_policy is None:
                hard_policy = "browser_finalization_failed"
            if (
                finalize_error is None
                and getattr(self, "_lease_client", None) is not None
            ):
                provider_stats = None
                try:
                    provider_stats = self._wait_and_observe_provider()
                except Exception:
                    hard_policy = hard_policy or "browser_provider_drain_failed"
                if (
                    (
                        self._provider_bootstrap_max_bytes > 0
                        and self._provider_bootstrap_spent_bytes
                        >= self._provider_bootstrap_max_bytes
                    )
                    or (
                        self._provider_lease is not None
                        and self._provider_lease_observed_bytes
                        >= self._provider_lease.max_bytes
                    )
                    or bool(
                        provider_stats is not None
                        and provider_stats.budget_exceeded
                    )
                ):
                    hard_policy = hard_policy or "browser_provider_cap_exhausted"
            breakdown = (
                self._full_browser_reservation_breakdown()
                if finalize_error is not None
                else self._browser_breakdown(bootstrap_stats)
            )
            raise FetchError(
                (
                    "Camoufox hard transport policy failed: "
                    f"{hard_policy}"
                    if hard_policy is not None
                    else "Camoufox clearance bootstrap failed: "
                    f"{type(bootstrap_error).__name__}"
                    if bootstrap_error is not None
                    else "Camoufox could not establish an FBref clearance lease"
                ),
                error_class=(
                    "hard_transport_policy"
                    if hard_policy is not None
                    else "clearance_failed"
                ),
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

        bootstrap_stats, finalize_error = (
            self._close_browser_and_collect_traffic(transport)
        )
        hard_policy = self._hard_transport_policy_reason(bootstrap_stats)
        if finalize_error is not None and hard_policy is None:
            hard_policy = "browser_finalization_failed"
        breakdown = (
            self._full_browser_reservation_breakdown()
            if finalize_error is not None
            else self._browser_breakdown(bootstrap_stats)
        )
        if hard_policy is not None:
            raise FetchError(
                f"Camoufox hard transport policy failed: {hard_policy}",
                error_class="hard_transport_policy",
                browser_document_bytes=breakdown[0],
                browser_asset_bytes=breakdown[1],
                browser_requests=breakdown[2],
                browser_bootstrap_attempts=breakdown[3],
                browser_unobserved_bytes=breakdown[4],
            )
        if finalize_error is not None:
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
        try:
            self._extend_provider_lease_for_http()
        except Exception as exc:
            raise FetchError(
                "FBref browser/HTTP provider phase boundary failed: "
                f"{type(exc).__name__}",
                error_class="hard_transport_policy",
                browser_document_bytes=breakdown[0],
                browser_asset_bytes=breakdown[1],
                browser_requests=breakdown[2],
                browser_bootstrap_attempts=breakdown[3],
                browser_unobserved_bytes=breakdown[4],
            ) from exc
        try:
            http_session = self._create_http_session(clearance)
        except Exception as exc:
            raise FetchError(
                "FBref warm HTTP session creation failed: "
                f"{type(exc).__name__}",
                error_class="clearance_export_failed",
                browser_document_bytes=breakdown[0],
                browser_asset_bytes=breakdown[1],
                browser_requests=breakdown[2],
                browser_bootstrap_attempts=breakdown[3],
                browser_unobserved_bytes=breakdown[4],
            ) from exc
        self._http_session = http_session
        self._clearance = dict(clearance)
        self._bootstrap_stats = bootstrap_stats

    @staticmethod
    def _hard_transport_policy_reason(stats: Optional[dict]) -> Optional[str]:
        source = stats or {}
        if source.get("geoip_lookup_failed"):
            return "geoip_lookup_failed"
        if source.get("redirect_blocked"):
            return "redirect_blocked"
        if source.get("network_policy_failed"):
            return str(
                source.get("network_policy_failure")
                or "unexpected_network"
            )
        if source.get("request_budget_exhausted"):
            return "request_budget_exhausted"
        if source.get("byte_budget_exhausted"):
            return str(
                source.get("byte_budget_failure")
                or "byte_budget_exhausted"
            )
        return None

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

    def _fetch_without_provider_meter(
        self,
        url: str,
        *,
        page_kind: str,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> FetchResponse:
        self._ensure_clearance()
        if (
            getattr(self, "_lease_client", None) is not None
            and not self._provider_http_ready
        ):
            raise FetchError(
                "FBref warm HTTP is blocked before paid lease extension",
                error_class="hard_transport_policy",
            )
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

    def _provider_meter_failure(
        self,
        error: Exception,
        *,
        original: Optional[object],
        before_provider_bytes: int,
    ) -> FetchError:
        """Turn a missing authoritative counter into a fail-closed fetch."""

        # A close is a second, stronger accounting read: proxy-filter revokes
        # every tunnel and returns only after its final byte event is durable.
        exact_close = False
        close_error = None
        try:
            self._close_provider_lease()
            exact_close = True
        except Exception as exc:  # noqa: BLE001 - return one terminal error
            close_error = exc
            logger.exception("FBref paid lease final accounting failed")
        self._clearance = None
        values = {
            "http_status": None,
            "wire_bytes": 0,
            "browser_document_bytes": 0,
            "browser_asset_bytes": 0,
            "browser_requests": 0,
            "browser_bootstrap_attempts": 0,
            "browser_unobserved_bytes": 0,
            "target_requests": 0,
            "http_status_history": (),
            "latency_ms": 0,
        }
        if isinstance(original, FetchError):
            values.update(
                {
                    "http_status": original.http_status,
                    "wire_bytes": original.wire_bytes,
                    "browser_document_bytes": original.browser_document_bytes,
                    "browser_asset_bytes": original.browser_asset_bytes,
                    "browser_requests": original.browser_requests,
                    "browser_bootstrap_attempts": (
                        original.browser_bootstrap_attempts
                    ),
                    "browser_unobserved_bytes": original.browser_unobserved_bytes,
                    "target_requests": original.http_requests,
                    "http_status_history": original.http_status_history,
                    "latency_ms": original.latency_ms,
                }
            )
        elif isinstance(original, FetchResponse):
            values.update(
                {
                    "http_status": original.status_code,
                    "wire_bytes": original.http_wire_bytes,
                    "browser_document_bytes": original.browser_document_bytes,
                    "browser_asset_bytes": original.browser_asset_bytes,
                    "browser_requests": original.browser_requests,
                    "browser_bootstrap_attempts": (
                        original.browser_bootstrap_attempts
                    ),
                    "browser_unobserved_bytes": original.browser_unobserved_bytes,
                    "target_requests": original.http_requests,
                    "http_status_history": original.http_status_history,
                    "latency_ms": original.latency_ms,
                }
            )
        details = [f"counter={type(error).__name__}: {error}"]
        if close_error is not None:
            details.append(
                f"final_close={type(close_error).__name__}: {close_error}"
            )
        if isinstance(original, FetchError):
            details.append(
                f"target_error={original.error_class}: {original}"
            )
        elif isinstance(original, FetchResponse):
            details.append(f"target_response_status={original.status_code}")
        elif original is not None:
            details.append(
                f"target_error={type(original).__name__}: {original}"
            )
        return FetchError(
            "FBref paid transport accounting is uncertain; "
            + "; ".join(details),
            # An unknown paid counter can outlive this target.  It is always a
            # run-level stop, even when the target error itself was ordinary.
            error_class="hard_transport_policy",
            provider_billed_bytes=(
                max(0, self._provider_total_bytes - before_provider_bytes)
                if exact_close
                else None
            ),
            **values,
        )

    def fetch(
        self,
        url: str,
        *,
        page_kind: str,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> FetchResponse:
        """Fetch one page and attach the exact proxy-filter byte delta."""

        if getattr(self, "_lease_client", None) is None:
            return self._fetch_without_provider_meter(
                url,
                page_kind=page_kind,
                etag=etag,
                last_modified=last_modified,
            )
        before = int(self._provider_total_bytes)
        try:
            response = self._fetch_without_provider_meter(
                url,
                page_kind=page_kind,
                etag=etag,
                last_modified=last_modified,
            )
        except Exception as original:
            try:
                self._finish_metered_fetch()
            except Exception as meter_error:
                raise self._provider_meter_failure(
                    meter_error,
                    original=original,
                    before_provider_bytes=before,
                ) from original
            billed = int(self._provider_total_bytes) - before
            if billed < 0:
                raise FBrefProxyLeaseError(
                    "FBref provider aggregate counter moved backwards"
                ) from original
            if isinstance(original, FetchError):
                original.provider_billed_bytes = billed
                raise
            raise FetchError(
                f"FBref transport failed: {type(original).__name__}",
                error_class="transport_internal_error",
                provider_billed_bytes=billed,
            ) from original
        try:
            self._finish_metered_fetch()
        except Exception as meter_error:
            raise self._provider_meter_failure(
                meter_error,
                original=response,
                before_provider_bytes=before,
            ) from meter_error
        billed = int(self._provider_total_bytes) - before
        if billed < 0:
            raise FBrefProxyLeaseError(
                "FBref provider aggregate counter moved backwards"
            )
        return replace(response, provider_billed_bytes=billed)


__all__ = [
    "DEFAULT_BROWSER_BYTE_LIMIT",
    "DEFAULT_BROWSER_REQUEST_LIMIT",
    "DEFAULT_BOOTSTRAP_URL",
    "FETCHER_VERSION",
    "FBrefFetcher",
    "FetchError",
    "FetchResponse",
    "MAX_HTML_BYTES",
    "MAX_TARGET_HTTP_ATTEMPTS",
    "RETRYABLE_HTTP_STATUSES",
]
