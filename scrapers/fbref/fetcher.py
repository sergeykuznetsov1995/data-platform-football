"""Single production FBref transport: Camoufox clearance plus warm HTTP."""

from __future__ import annotations

import hashlib
import logging
import math
import os
import time
from dataclasses import dataclass, field, replace
from typing import Mapping, Optional, Sequence

from scrapers.fbref.browser_runtime import HTTP_IMPERSONATE_TARGET
from scrapers.fbref.camoufox_fetch import CamoufoxFbrefTransport
from scrapers.fbref.proxy_lease import (
    DEFAULT_LEASE_TTL_SECONDS,
    FBrefLeaseStats,
    FBrefProxyLease,
    FBrefProxyLeaseClient,
    FBrefProxyLeaseError,
    METER_ID,
)
from scrapers.fbref.settings import (
    DEFAULT_BROWSER_BYTE_LIMIT_BYTES,
    DEFAULT_BROWSER_REQUESTS_PER_SOLVE,
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    DEFAULT_HTTP_BODY_LIMIT_BYTES,
)
from scrapers.utils.proxy_manager import classify_error


FETCHER_VERSION = "fbref-camoufox-metered-warm-http-v8"
DEFAULT_BOOTSTRAP_URL = "https://fbref.com/en/"
MAX_HTML_BYTES = DEFAULT_HTTP_BODY_LIMIT_BYTES
# The browser cap bounds ONE clearance attempt; the run's reservation covers
# every attempt (see DEFAULT_BOOTSTRAP_REQUEST_RESERVATION).
DEFAULT_BROWSER_REQUEST_LIMIT = DEFAULT_BROWSER_REQUESTS_PER_SOLVE
DEFAULT_BROWSER_BYTE_LIMIT = DEFAULT_BROWSER_BYTE_LIMIT_BYTES
MAX_TARGET_HTTP_ATTEMPTS = 2
RETRYABLE_HTTP_STATUSES = frozenset({500, 502, 503, 504})
DEFAULT_STATUS_RETRY_DELAY_SECONDS = DEFAULT_DOMAIN_INTERVAL_SECONDS
# Decodo and the local provider lease are both bounded to 120 minutes in the
# production profile.  Stop admitting pages five minutes earlier so curl,
# strict provider close, and durable tail settlement complete before either
# deadline can expire.
PERSISTENT_SESSION_MAX_AGE_SECONDS = 115 * 60
_PERSISTENT_SESSION_CLOSE_MARGIN_SECONDS = 5 * 60
_FAILURE_EVIDENCE_HEADERS = (
    "content-type",
    "content-length",
    "location",
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

# A dead proxy raises a transport exception whose libcurl prose ("Could not
# connect to proxy", "Recv failure") matches none of the substrings the text
# classifier looks for, so the sessionwide failure used to be filed as an
# unclassified per-target one and failed the whole wave (#1122). The exception
# TYPE says what the text does not. Same guarded-import pattern as above so the
# import-light suite keeps working without the optional transport.
def _transport_error_types(*names: str) -> tuple[type[BaseException], ...]:
    """Collect the transport exception types this curl_cffi actually ships.

    Resolved name by name: a renamed or dropped class costs its own coverage,
    never the whole tuple, so an upgrade cannot silently restore #1122.
    """
    try:  # pragma: no cover - curl_cffi is installed in the production image
        from curl_cffi.requests import exceptions as curl_exceptions
    except ImportError:  # pragma: no cover - exercised by import-light suite
        return ()
    found = tuple(
        resolved
        for resolved in (getattr(curl_exceptions, name, None) for name in names)
        if isinstance(resolved, type) and issubclass(resolved, BaseException)
    )
    missing = [name for name in names if not hasattr(curl_exceptions, name)]
    if missing:  # pragma: no cover - only on a transport upgrade
        logging.getLogger(__name__).warning(
            "curl_cffi no longer exposes %s; transport failures using it fall "
            "back to text classification",
            ", ".join(missing),
        )
    return found


# ProxyError and friends are what a dead proxy raises. Their subclasses come
# along by isinstance: SSLError and DNSError extend ConnectionError,
# ConnectTimeout and ReadTimeout extend Timeout.
_TRANSPORT_TIMEOUT_ERRORS = _transport_error_types("Timeout")
_TRANSPORT_CONNECTION_ERRORS = _transport_error_types(
    "ProxyError", "ConnectionError"
)


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
        redirect_location: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.error_class = error_class
        # Where a 3xx pointed, when the source bothered to say.  A redirect
        # without it is not a usable "the page moved" statement.
        self.redirect_location = redirect_location
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


@dataclass(frozen=True)
class PersistentMeteredSessionReceipt:
    """Exact, non-secret evidence for one finalized clearance session."""

    session_id: str
    meter: str
    baseline_provider_bytes: int
    page_provider_bytes: int
    authoritative_provider_bytes: int
    tail_provider_bytes: int


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
        persistent_http_session: bool = False,
        sleep=time.sleep,
        monotonic=time.monotonic,
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
        if not callable(monotonic):
            raise ValueError("monotonic must be callable")
        self._monotonic = monotonic
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
        if not isinstance(persistent_http_session, bool):
            raise ValueError("persistent_http_session must be boolean")
        self.persistent_http_session = persistent_http_session
        self._persistent_session_id: Optional[str] = None
        self._persistent_baseline_provider_bytes = 0
        self._persistent_page_cursor = 0
        self._persistent_page_provider_bytes = 0
        self._persistent_session_deadline: Optional[float] = None
        self._persistent_receipt: Optional[
            PersistentMeteredSessionReceipt
        ] = None
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
            required = (
                "dag_id",
                "run_id",
                "task_id",
                "canonical_url",
                "scope",
            )
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
            if (
                self.persistent_http_session
                and ttl <= _PERSISTENT_SESSION_CLOSE_MARGIN_SECONDS
            ):
                raise ValueError(
                    "FBref persistent provider TTL must leave time for strict close"
                )
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
            # A real Xvfb display preserves the native browser fingerprint;
            # FBref's managed challenge rejects Firefox headless mode often
            # enough that it is not a production transport.
            headless="virtual",
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
        if (
            getattr(self, "persistent_http_session", False)
            and getattr(self, "_persistent_session_id", None) is not None
            and getattr(self, "_persistent_receipt", None) is None
        ):
            self.finalize_metered_session()
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

        if (
            getattr(self, "persistent_http_session", False)
            and getattr(self, "_persistent_session_id", None) is not None
            and getattr(self, "_persistent_receipt", None) is None
        ):
            raise FBrefProxyLeaseError(
                "FBref persistent session must be finalized before reset"
            )

        if self._http_session is not None:
            try:
                self._http_session.close()
            finally:
                self._http_session = None
        if self._transport is not None:
            self._transport.close()
        try:
            self._close_provider_lease()
        except FBrefProxyLeaseError as exc:
            # A reset runs *because* the session already failed, and the most
            # common cause -- an unreachable filter -- is also what makes the
            # close fail.  Propagating here escapes `fetch_wave` past its
            # `except FetchError`, so the wave's untouched targets are never
            # requeued and stay claimed until their lease expires.  Ownership
            # deliberately survives the failure: `_next_proxy` reconciles it
            # before it may buy another lease, so nothing is charged blind.
            # Same trade as the strict-close fallback (#1099): keep the wave
            # recoverable, keep the ledger owned.
            logger.warning(
                "FBref clearance reset could not close lease %s (%s: %s); "
                "ownership retained for reconciliation",
                getattr(getattr(self, "_provider_lease", None), "lease_id", "?"),
                type(exc).__name__,
                exc,
            )
        self._provider_http_ready = False
        # A pipeline clearance refresh reserves a new browser phase.  Keep
        # rotations inside one transport cumulative, but do not carry the old
        # phase's spend into the newly reserved transport.
        self._provider_bootstrap_spent_bytes = 0
        self._bootstrap_stats = None
        self._clearance = None
        self._transport = self._create_transport()

    def reconfigure_clearance_limits(
        self,
        *,
        max_browser_requests: int,
        max_browser_bytes: int,
    ) -> None:
        """Rebuild the browser without forgetting authenticated provider spend."""

        requests = int(max_browser_requests)
        bytes_ = int(max_browser_bytes)
        if requests <= 0 or bytes_ <= 0:
            raise ValueError("browser request/byte limits must be positive")
        if (
            getattr(self, "persistent_http_session", False)
            and getattr(self, "_persistent_session_id", None) is not None
            and getattr(self, "_persistent_receipt", None) is None
        ):
            raise FBrefProxyLeaseError(
                "FBref persistent session must be finalized before reconfigure"
            )

        close_error = None
        if self._http_session is not None:
            try:
                self._http_session.close()
            except Exception as exc:  # noqa: BLE001 - revoke provider too
                close_error = exc
            finally:
                self._http_session = None
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception as exc:  # noqa: BLE001 - revoke provider too
                close_error = close_error or exc
        try:
            self._close_provider_lease()
        except Exception as exc:  # noqa: BLE001 - retain exact ownership error
            close_error = close_error or exc
        if close_error is not None:
            raise close_error

        self._max_browser_requests = requests
        self._max_browser_bytes = bytes_
        provider_remaining = max(
            0, self._provider_max_bytes - self._provider_total_bytes
        )
        self._provider_bootstrap_max_bytes = min(provider_remaining, bytes_)
        self._provider_bootstrap_spent_bytes = 0
        self._provider_http_ready = False
        self._bootstrap_stats = None
        self._clearance = None
        self._transport = self._create_transport()

    def ensure_clearance(self) -> bool:
        """Prepare warm HTTP and report whether a browser page was fetched.

        The pipeline uses the boolean to reserve a new domain-throttle slot
        between the browser bootstrap page and the first warm target. A
        retained cookie only recreates the HTTP adapter and returns ``False``.
        """

        if (
            not getattr(self, "persistent_http_session", False)
            or getattr(self, "_persistent_session_id", None) is None
            or getattr(self, "_persistent_receipt", None) is not None
        ):
            return self._ensure_clearance()

        before = int(self._persistent_page_cursor)
        try:
            return self._ensure_clearance()
        except Exception as original:
            try:
                billed = self._persistent_page_checkpoint(close_tunnel=True)
            except Exception as meter_error:
                failure = self._provider_meter_failure(
                    meter_error,
                    original=original,
                    before_provider_bytes=before,
                )
                if failure.provider_billed_bytes is not None:
                    self._record_persistent_page_checkpoint()
                raise failure from original
            if isinstance(original, FetchError):
                original.provider_billed_bytes = billed
                raise
            raise FetchError(
                f"FBref clearance failed: {type(original).__name__}",
                error_class="clearance_failed",
                provider_billed_bytes=billed,
            ) from original

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

    def _close_provider_lease(self, *, strict: Optional[bool] = None) -> None:
        lease_client = getattr(self, "_lease_client", None)
        provider_lease = getattr(self, "_provider_lease", None)
        if lease_client is None or provider_lease is None:
            return
        lease = provider_lease
        strict_close = (
            bool(strict)
            if strict is not None
            else bool(
                getattr(self, "persistent_http_session", False)
                and getattr(self, "_persistent_session_id", None) is not None
            )
        )
        if strict_close:
            try:
                stats = lease_client.close_strict(
                    lease,
                    expected=self._provider_context,
                )
            except FBrefProxyLeaseError as strict_error:
                # One mid-response abort latches the lease accounting-uncertain
                # for good, so the strict proof can never arrive. The filter
                # keeps the unproven tail as durable escrow and its counters are
                # the final client-visible ledger, so the tolerant close still
                # protects the budget while letting the wave continue with an
                # ordinary target error (#1099, mirrors #1096 for Transfermarkt).
                logger.warning(
                    "FBref strict lease close failed for lease %s (%s); "
                    "falling back to the escrow-backed close",
                    lease.lease_id,
                    strict_error,
                )
                stats = lease_client.close(
                    lease,
                    expected=self._provider_context,
                )
        else:
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

    def begin_metered_session(self, session_id: str) -> int:
        """Capture the cumulative provider baseline before browser traffic."""

        identity = str(session_id).strip()
        if not identity:
            raise ValueError("session_id must not be empty")
        if (
            not getattr(self, "persistent_http_session", False)
            or getattr(self, "_lease_client", None) is None
        ):
            raise FBrefProxyLeaseError(
                "FBref persistent metering is not enabled for this fetcher"
            )
        if self._persistent_session_id is not None:
            if self._persistent_session_id == identity:
                return self._persistent_baseline_provider_bytes
            if self._persistent_receipt is None:
                raise FBrefProxyLeaseError(
                    "Previous FBref persistent session is not finalized"
                )
        baseline = int(self._provider_total_bytes)
        self._persistent_session_id = identity
        self._persistent_baseline_provider_bytes = baseline
        self._persistent_page_cursor = baseline
        self._persistent_page_provider_bytes = 0
        self._persistent_receipt = None
        started_at = float(self._monotonic())
        if not math.isfinite(started_at):
            raise FBrefProxyLeaseError(
                "FBref persistent monotonic clock is invalid"
            )
        safe_age = min(
            PERSISTENT_SESSION_MAX_AGE_SECONDS,
            self._provider_lease_ttl_seconds
            - _PERSISTENT_SESSION_CLOSE_MARGIN_SECONDS,
        )
        if safe_age <= 0:
            raise FBrefProxyLeaseError(
                "FBref persistent provider TTL has no safe local lifetime"
            )
        self._persistent_session_deadline = started_at + safe_age
        return baseline

    def persistent_session_rollover_due(
        self, *, within_seconds: float = 0.0
    ) -> bool:
        """Return true before the current provider/sticky lifetime expires."""

        if isinstance(within_seconds, bool):
            raise ValueError("within_seconds must be a non-negative number")
        guard = float(within_seconds)
        if not math.isfinite(guard) or guard < 0:
            raise ValueError("within_seconds must be a non-negative number")
        if (
            self._persistent_session_id is None
            or self._persistent_receipt is not None
        ):
            return False
        deadline = self._persistent_session_deadline
        if deadline is None or not math.isfinite(deadline):
            raise FBrefProxyLeaseError(
                "FBref persistent session deadline is unavailable"
            )
        now = float(self._monotonic())
        if not math.isfinite(now):
            raise FBrefProxyLeaseError(
                "FBref persistent monotonic clock is invalid"
            )
        return now + guard >= deadline

    def _persistent_page_checkpoint(self, *, close_tunnel: bool) -> int:
        if (
            self._persistent_session_id is None
            or self._persistent_receipt is not None
        ):
            raise FBrefProxyLeaseError("FBref persistent session is not active")
        if close_tunnel:
            close_error = None
            if self._http_session is not None:
                try:
                    self._http_session.close()
                except Exception as exc:  # noqa: BLE001 - revoke regardless
                    close_error = exc
                finally:
                    self._http_session = None
            if self._transport is not None:
                try:
                    self._transport.close()
                except Exception as exc:  # noqa: BLE001 - revoke regardless
                    close_error = close_error or exc
            try:
                self._close_provider_lease(strict=True)
            except Exception as exc:  # noqa: BLE001 - preserve ownership
                close_error = close_error or exc
            if close_error is not None:
                raise close_error
        else:
            if self._provider_lease is None or self._lease_client is None:
                raise FBrefProxyLeaseError(
                    "FBref persistent page has no active paid tunnel"
                )
            stats = self._lease_client.wait_idle(
                self._provider_lease,
                expected=self._provider_context,
                expected_tunnels=1,
            )
            self._observe_provider_stats(stats)
        return self._record_persistent_page_checkpoint()

    def _record_persistent_page_checkpoint(self) -> int:
        current = int(self._provider_total_bytes)
        delta = current - int(self._persistent_page_cursor)
        if delta < 0:
            raise FBrefProxyLeaseError(
                "FBref persistent provider counter moved backwards"
            )
        self._persistent_page_cursor = current
        self._persistent_page_provider_bytes += delta
        return delta

    def finalize_metered_session(self) -> PersistentMeteredSessionReceipt:
        """Close provider ownership and return retry-stable exact evidence."""

        if self._persistent_receipt is not None:
            return self._persistent_receipt
        if self._persistent_session_id is None:
            raise FBrefProxyLeaseError("FBref persistent session was not started")

        close_error = None
        if self._http_session is not None:
            try:
                self._http_session.close()
            except Exception as exc:  # noqa: BLE001 - still revoke provider
                close_error = exc
            finally:
                self._http_session = None
        if self._transport is not None:
            try:
                self._transport.close()
            except Exception as exc:  # noqa: BLE001 - still revoke provider
                close_error = close_error or exc
        try:
            self._close_provider_lease(strict=True)
        except Exception as exc:  # noqa: BLE001 - ownership remains on failure
            close_error = close_error or exc
        if close_error is not None:
            raise close_error

        authoritative = (
            int(self._provider_total_bytes)
            - int(self._persistent_baseline_provider_bytes)
        )
        page_bytes = int(self._persistent_page_provider_bytes)
        tail = authoritative - page_bytes
        if authoritative < 0 or tail < 0:
            raise FBrefProxyLeaseError(
                "FBref persistent session counters do not reconcile"
            )
        self._persistent_receipt = PersistentMeteredSessionReceipt(
            session_id=self._persistent_session_id,
            meter=METER_ID,
            baseline_provider_bytes=self._persistent_baseline_provider_bytes,
            page_provider_bytes=page_bytes,
            authoritative_provider_bytes=authoritative,
            tail_provider_bytes=tail,
        )
        return self._persistent_receipt

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
        browser_headers = clearance.get("browser_headers")
        if not isinstance(browser_headers, Mapping):
            browser_headers = {}
        session = Session(
            # Firefox 147 is curl_cffi's nearest supported TLS fingerprint for
            # the isolated Firefox 152 browser. The exported 152 User-Agent is
            # retained below, as recommended for skipped browser versions.
            impersonate=HTTP_IMPERSONATE_TARGET,
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
            "Accept": str(
                browser_headers.get("accept")
                or "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "*/*;q=0.8"
            ),
            "Accept-Language": str(
                browser_headers.get("accept-language")
                or "en-US,en;q=0.9"
            ),
            "Accept-Encoding": str(
                browser_headers.get("accept-encoding")
                or "gzip, deflate, br, zstd"
            ),
            "Sec-Fetch-Dest": str(
                browser_headers.get("sec-fetch-dest") or "document"
            ),
            "Sec-Fetch-Mode": str(
                browser_headers.get("sec-fetch-mode") or "navigate"
            ),
            "Sec-Fetch-Site": str(
                browser_headers.get("sec-fetch-site") or "none"
            ),
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

    def _ensure_clearance(self) -> bool:
        if self._http_session is not None:
            return False
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
            return False
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
            drain_relief = None
            if finalize_error is not None and hard_policy is None:
                hard_policy = "browser_finalization_failed"
            if (
                finalize_error is None
                and getattr(self, "_lease_client", None) is not None
            ):
                provider_stats = None
                try:
                    provider_stats = self._wait_and_observe_provider()
                except Exception as exc:
                    # This verdict ends the wave and the exception is the only
                    # evidence of why the paid lease would not drain.  Swallowed
                    # silently it reads exactly like a policy breach: the geo-IP
                    # path cost a day of blind red runs for the same reason,
                    # until c343f5e2 put its type in the log (#1188).
                    logger.warning(
                        "FBref paid lease drain failed before the wave "
                        "verdict: %s: %s",
                        type(exc).__name__,
                        exc,
                    )
                    # An exit that never answered cannot have spent the ledger
                    # it refuses to close: measured spend is 352-415 bytes of a
                    # 16 MiB lease.  Owner's call 19.08 is to treat that narrow
                    # case as a session miss, so the wave re-solves on a fresh
                    # proxy under the existing guards instead of discarding
                    # everything it already collected (#1188).  Every other
                    # drain failure still stops the wave: an unresolved paid
                    # ledger with real traffic behind it is a policy breach.
                    #
                    # `wait_drained` raises for budget and lifecycle states too,
                    # not only for an unreachable meter, and the cap check below
                    # cannot re-impose the verdict once the drain failed (it has
                    # no fresh stats to read).  So the exemption also requires
                    # that nothing is known to have been spent -- any observed
                    # byte means the exit did answer somebody.
                    unspent = not self._provider_lease_observed_bytes and not int(
                        (bootstrap_stats or {}).get("real_bytes_downloaded", 0)
                        or 0
                    )
                    if self._exit_never_answered(bootstrap_stats) and unspent:
                        drain_relief = f"{type(exc).__name__}: {exc}"
                    else:
                        hard_policy = (
                            hard_policy or "browser_provider_drain_failed"
                        )
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
                    # `fetch_attempt.error_message` only ever stores
                    # `str(FetchError)` (#1107), so an exemption that says
                    # nothing here is invisible in the control DB: it reads as
                    # any other clearance miss, and nobody can count the paid
                    # leases abandoned unaccounted.  Name it.
                    else "Camoufox abandoned an unreachable exit's lease: "
                    f"{drain_relief}"
                    if drain_relief is not None
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
            # fetch_attempt.error_message only ever stores str(FetchError), so
            # the chained cause text must be inlined here or it is lost (#1107).
            cause_text = str(exc)[:300]
            raise FetchError(
                "FBref browser/HTTP provider phase boundary failed: "
                f"{type(exc).__name__}"
                + (f": {cause_text}" if cause_text else ""),
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
        return True

    @staticmethod
    def _exit_never_answered(stats: Optional[dict]) -> bool:
        """True when the geo-IP probe failed because nothing answered at all.

        Both flags together are the transport-level signature written by
        ``camoufox_fetch``: the lookup failed *and* it failed by not
        connecting, as opposed to answering from a disallowed country.

        The second flag is belt-and-braces rather than load-bearing: an exit
        that answered from the wrong country is already condemned by
        ``geoip_lookup_failed`` before the drain runs, so the caller's verdict
        would not change without it.  It stays because the predicate is about
        what the transport observed, and a reader must not have to prove that
        coincidence to trust the exemption.
        """

        source = stats or {}
        return bool(
            source.get("geoip_lookup_failed")
            and source.get("geoip_transport_failure")
        )

    @staticmethod
    def _hard_transport_policy_reason(stats: Optional[dict]) -> Optional[str]:
        source = stats or {}
        if source.get("geoip_lookup_failed"):
            # An exit that could not be reached is a dead lease, not a policy
            # breach: the warm HTTP path already treats the same ProxyError as
            # a re-solvable session failure, and every geo-IP death measured
            # 17-18.08 carried exactly that (3 of 3 with a recorded type, each
            # ProxyError "Cannot connect to proxy").  Falling through here
            # yields error_class 'clearance_failed', which re-solves on a fresh
            # proxy under the existing exhaustion guards instead of discarding
            # the wave and everything it already collected (#1188).  Those
            # guards bound 2 re-solves per target and 3 *consecutive* exhausted
            # targets -- `pipeline` resets that counter after every fetched
            # page, so a wave alternating success and dead exit is not bounded
            # by it at all; only the run's own byte and request budgets stop it.
            #
            # The paid path used to undo this rescue: the same dead exit also
            # leaves the lease unaccounted, `wait_drained` raises, and
            # `browser_provider_drain_failed` put hard_transport_policy back.
            # The owner lifted that on 19.08 for this narrow case only — see
            # `_exit_never_answered` at the drain site — because an exit that
            # never connected cannot have spent the ledger it will not close
            # (measured: 352-415 bytes of a 16 MiB lease).  A drain failure
            # from any other cause still ends the wave.
            if not source.get("geoip_transport_failure"):
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
        if error_type == "unknown":
            # The prose stays the primary signal: a proxy that reports "HTTP
            # code 429 after CONNECT" carries no response object, so its only
            # evidence of rate limiting is that sentence. The exception type is
            # the fallback for the prose libcurl writes when a proxy is simply
            # dead -- "Could not connect to proxy", which matches none of the
            # classifier's substrings and used to be filed as an unclassified
            # per-target failure that killed the whole wave (#1122).
            if isinstance(exc, _TRANSPORT_TIMEOUT_ERRORS):
                error_type = "timeout"
            elif isinstance(exc, _TRANSPORT_CONNECTION_ERRORS):
                error_type = "connection"
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
            redirect_location = self._safe_header_value(
                {
                    str(key).lower(): value
                    for key, value in dict(
                        getattr(response, "headers", {}) or {}
                    ).items()
                }.get("location")
            )
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
                redirect_location=redirect_location,
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

    def _fetch_persistent_metered(
        self,
        url: str,
        *,
        page_kind: str,
        etag: Optional[str],
        last_modified: Optional[str],
    ) -> FetchResponse:
        if self._persistent_session_id is None or self._persistent_receipt is not None:
            raise FBrefProxyLeaseError(
                "FBref persistent fetch requires an active metered session"
            )
        before = int(self._persistent_page_cursor)
        try:
            response = self._fetch_without_provider_meter(
                url,
                page_kind=page_kind,
                etag=etag,
                last_modified=last_modified,
            )
        except Exception as original:
            try:
                billed = self._persistent_page_checkpoint(close_tunnel=True)
            except Exception as meter_error:
                failure = self._provider_meter_failure(
                    meter_error,
                    original=original,
                    before_provider_bytes=before,
                )
                if failure.provider_billed_bytes is not None:
                    self._record_persistent_page_checkpoint()
                raise failure from original
            if isinstance(original, FetchError):
                original.provider_billed_bytes = billed
                raise
            raise FetchError(
                f"FBref transport failed: {type(original).__name__}",
                error_class="transport_internal_error",
                provider_billed_bytes=billed,
            ) from original

        try:
            billed = self._persistent_page_checkpoint(close_tunnel=False)
        except Exception as meter_error:
            # The strict close is the only recovery that can turn a failed idle
            # proof into an exact terminal counter. Close curl before asking the
            # filter to revoke the provider side.
            if self._http_session is not None:
                try:
                    self._http_session.close()
                finally:
                    self._http_session = None
            failure = self._provider_meter_failure(
                meter_error,
                original=response,
                before_provider_bytes=before,
            )
            if failure.provider_billed_bytes is not None:
                self._record_persistent_page_checkpoint()
            raise failure from meter_error
        return replace(response, provider_billed_bytes=billed)

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
        if getattr(self, "persistent_http_session", False):
            return self._fetch_persistent_metered(
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
    "PERSISTENT_SESSION_MAX_AGE_SECONDS",
    "PersistentMeteredSessionReceipt",
    "RETRYABLE_HTTP_STATUSES",
]
