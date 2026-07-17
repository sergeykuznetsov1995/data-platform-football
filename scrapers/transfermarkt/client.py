"""Proxy-only, traffic-bounded HTTP client for Transfermarkt.

The client owns network policy only.  It deliberately knows nothing about
leagues, pandas, or Iceberg, which keeps retries and proxy accounting testable
without exercising the scraper orchestration.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import socket
import time
from collections import defaultdict
from typing import Any, Callable, Dict, Mapping, MutableMapping, Optional
from urllib.parse import quote, urlsplit, urlunsplit

from scrapers.transfermarkt.models import (
    PROVIDER_GRANT_ENV_VAR,
    PROVIDER_GRANT_FLOOR_BYTES,
    PROVIDER_GRANT_SOFT_MARGIN_BYTES,
    SCOPE_HARD_PROVIDER_BYTE_CAP,
    FetchOutcome,
    FetchStatus,
    LeaseTrafficSnapshot,
    ProxyLease,
    ProxyRequiredError,
    SharedTrafficLedger,
    TrafficBudgetExceeded,
    TrafficMeterError,
    stable_payload_hash,
)
from scrapers.utils.proxy_manager import ErrorType


logger = logging.getLogger(__name__)


DEFAULT_HEADERS: Mapping[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-CH-UA": '"Google Chrome";v="133", "Chromium";v="133", "Not_A Brand";v="24"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
}

_CHALLENGE_MARKERS = (
    "cf-chl-",
    "challenge-platform",
    "just a moment...",
    "cloudflare ray id",
)

# Deep catalogue pagination draws sporadic upstream 502/504s; each retry rotates
# to a fresh exit, so a required page needs more than a couple of exits before a
# whole cycle is abandoned.  The run-wide retry ledger is the real bound.
_MAX_FETCH_ATTEMPTS = 8
# A 403/429 is worth another exit, but not the full ladder: a source that means
# it will say so from every exit, and each try burns one.
_MAX_BLOCKED_ATTEMPTS = 4

_URL_CREDENTIALS_RE = re.compile(
    r"(?P<scheme>(?:https?|socks[45])://)(?P<credentials>[^/@\s]+)@",
    re.IGNORECASE,
)


def redact_sensitive(value: Any) -> str:
    """Remove proxy credentials from third-party exception text."""

    return _URL_CREDENTIALS_RE.sub(r"\g<scheme>****:****@", str(value))


def _payload_hash(value: Any) -> str:
    """Stable SHA-256 for checkpoint manifests (never Python's salted hash)."""

    return stable_payload_hash(value)


def _tls_requests_compatible_proxy_url(proxy_url: str) -> str:
    """Return a proxy URL accepted by ``tls_requests``.

    Docker Compose service names commonly contain underscores (for example
    ``proxy_filter``).  They are valid Docker DNS labels, but
    ``tls_requests`` applies strict IDNA validation and rejects such a proxy
    before opening a socket.  Resolve only that incompatible host form to its
    container IP while preserving the percent-encoded Basic-auth userinfo.

    This is still the same mandatory proxy endpoint.  Resolution failure is
    fail-closed; the caller never constructs an unproxied client.
    """

    parsed = urlsplit(str(proxy_url))
    host = parsed.hostname or ""
    if "_" not in host:
        return str(proxy_url)

    try:
        resolved_host = socket.gethostbyname(host)
        port = parsed.port
    except (OSError, ValueError) as exc:
        raise ProxyRequiredError(
            "Transfermarkt metered proxy DNS resolution failed"
        ) from exc

    rendered_host = f"[{resolved_host}]" if ":" in resolved_host else resolved_host
    rendered_port = f":{port}" if port is not None else ""
    raw_userinfo, separator, _ = parsed.netloc.rpartition("@")
    auth = f"{raw_userinfo}@" if separator else ""
    netloc = f"{auth}{rendered_host}{rendered_port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def _ledger_from_provider_grant(
    *,
    metered: bool,
    retry_limit: Optional[int],
) -> SharedTrafficLedger:
    """Build the per-entity provider ledger from the parent-issued byte grant.

    The entity runner reserves ``provider_reserve_bytes`` in the shared scope
    ledger and exports the granted amount as ``TM_PROVIDER_BYTE_BUDGET``.
    Sizing this client's ledger to that exact grant closes the hole where one
    entity's client could eat traffic up to the whole scope cap.  A required
    metered run (``TM_REQUIRE_METERED_PROXY``) refuses to start without a
    readable grant; a present-but-unreadable value always fails closed.
    """

    raw = os.environ.get(PROVIDER_GRANT_ENV_VAR)
    text = str(raw).strip() if raw is not None else ''
    if not text:
        if metered:
            # A metered client bills the provider on every request; without a
            # grant it would silently inherit the full per-scope default
            # ledger, which is exactly the escalation the grant exists to
            # prevent.  Refuse before any I/O.
            raise ProxyRequiredError(
                f'{PROVIDER_GRANT_ENV_VAR} is required for metered runs; '
                'refusing paid I/O without a per-entity provider grant'
            )
        return SharedTrafficLedger(retry_limit=retry_limit)
    try:
        grant = int(text)
    except ValueError as exc:
        raise TrafficMeterError(
            f'{PROVIDER_GRANT_ENV_VAR} is unreadable; refusing paid I/O'
        ) from exc
    if grant < PROVIDER_GRANT_FLOOR_BYTES:
        # A grant smaller than one page's worth of provider bytes would be
        # pierced by the very first response; refuse it before any I/O.
        raise TrafficMeterError(
            f'{PROVIDER_GRANT_ENV_VAR} is below the '
            f'{PROVIDER_GRANT_FLOOR_BYTES}-byte floor; refusing paid I/O'
        )
    hard = min(grant, SCOPE_HARD_PROVIDER_BYTE_CAP)
    # Full 1 MiB of soft headroom for normal grants; small grants keep a
    # proportional graceful stop instead of collapsing to a degenerate one.
    soft = min(hard - 1, max(hard - PROVIDER_GRANT_SOFT_MARGIN_BYTES, hard // 2))
    return SharedTrafficLedger(
        hard_provider_bytes=hard,
        soft_provider_bytes=soft,
        retry_limit=retry_limit,
    )


def _control_token_from_environment() -> str:
    """Return the Transfermarkt-only proxy-filter control token."""

    return str(os.environ.get("TM_PROXY_CONTROL_TOKEN", "")).strip()


class ProxyFilterLeaseProvider:
    """Adapter for production proxy-filter's ``/v1/leases`` control API.

    ``control_client`` is injectable so unit tests never need a socket.  In
    production it may be a ``requests.Session``; the dependency is imported
    lazily only when a lease is actually requested.
    """

    def __init__(
        self,
        control_base_url: str,
        *,
        control_client: Optional[Any] = None,
        control_token: Optional[str] = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        base = str(control_base_url).rstrip("/")
        parsed = urlsplit(base)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("proxy lease control URL must be absolute HTTP(S)")
        resolved = str(
            control_token
            if control_token is not None
            else _control_token_from_environment()
        )
        if len(resolved) < 32:
            raise ProxyRequiredError(
                "TM_PROXY_CONTROL_TOKEN must contain at least 32 characters: "
                "the proxy filter refuses a lease without its control token"
            )
        self.control_base_url = base
        self._control_client = control_client
        self._control_token = resolved
        self.timeout_seconds = float(timeout_seconds)

    def _client(self):
        if self._control_client is None:
            import requests

            self._control_client = requests.Session()
        return self._control_client

    def _request(
        self,
        method: str,
        path: str,
        *,
        token: Optional[str] = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        # The control plane authenticates the caller by its own token; a lease's
        # bearer token only proves which lease the call is about.
        headers = {"X-Proxy-Control-Token": self._control_token}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = self._client().request(
            method,
            f"{self.control_base_url}{path}",
            json=dict(payload) if payload is not None else None,
            headers=headers,
            timeout=self.timeout_seconds,
        )
        status = int(getattr(response, "status_code", 0) or 0)
        try:
            body = response.json()
        except Exception as exc:  # noqa: BLE001 - adapter boundary
            raise TrafficMeterError(
                f"proxy lease API returned invalid JSON (HTTP {status})"
            ) from exc
        if status < 200 or status >= 300 or not isinstance(body, dict):
            error = redact_sensitive(body.get("error", "unknown error"))
            raise ProxyRequiredError(
                f"proxy lease API rejected {method} {path} "
                f"(HTTP {status}): {error}"
            )
        return body

    def acquire(
        self,
        *,
        max_bytes: int,
        ttl_seconds: int,
        metadata: Mapping[str, Any],
    ) -> ProxyLease:
        required = ("dag_id", "run_id", "task_id", "canonical_url")
        if not all(str(metadata.get(key) or "").strip() for key in required):
            raise ProxyRequiredError(
                "paid proxy lease requires dag_id, run_id, task_id and canonical_url"
            )
        request = dict(metadata)
        request.update({"max_bytes": int(max_bytes), "ttl_seconds": int(ttl_seconds)})
        body = self._request("POST", "/v1/leases", payload=request)
        try:
            lease = ProxyLease(
                lease_id=str(body["id"]),
                token=str(body["token"]),
                proxy_url=str(body["proxy_url"]),
                max_bytes=int(body["max_bytes"]),
                expires_at=float(body["expires_at"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise TrafficMeterError("proxy lease API response schema mismatch") from exc
        parsed = urlsplit(lease.proxy_url)
        if (
            not lease.lease_id
            or not lease.token
            or parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or lease.max_bytes <= 0
        ):
            raise TrafficMeterError("proxy lease API returned an unusable lease")
        return lease

    def stats(self, lease: ProxyLease) -> LeaseTrafficSnapshot:
        body = self._request(
            "GET", f"/v1/leases/{lease.lease_id}/stats", token=lease.token,
        )
        try:
            return LeaseTrafficSnapshot.from_mapping(body)
        except (TypeError, ValueError) as exc:
            raise TrafficMeterError("invalid proxy lease traffic counters") from exc

    def close(self, lease: ProxyLease) -> LeaseTrafficSnapshot:
        body = self._request(
            "DELETE", f"/v1/leases/{lease.lease_id}/close", token=lease.token,
        )
        try:
            return LeaseTrafficSnapshot.from_mapping(body)
        except (TypeError, ValueError) as exc:
            raise TrafficMeterError("invalid final proxy lease traffic counters") from exc

    @staticmethod
    def authenticated_proxy_url(lease: ProxyLease) -> str:
        """Build the proxy URL expected by proxy-filter's Basic auth path."""

        parsed = urlsplit(lease.proxy_url)
        host = parsed.hostname or ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = f":{parsed.port}" if parsed.port is not None else ""
        netloc = f"lease:{quote(lease.token, safe='')}@{host}{port}"
        return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))


class TransfermarktHttpClient:
    """Lazy sticky ``tls_requests`` client with fail-closed proxy policy.

    One client/proxy is reused across a successful series.  A blocked,
    transport, schema, or 5xx attempt is recorded through
    :class:`ProxyManager`, the client is discarded, and the next retry rotates
    to another available proxy.  No unproxied client is ever constructed.
    """

    def __init__(
        self,
        *,
        proxy_manager=None,
        proxy: Optional[str] = None,
        lease_provider: Optional[Any] = None,
        traffic_ledger: Optional[SharedTrafficLedger] = None,
        retry_budget: Optional[int] = None,
        lease_metadata: Optional[Mapping[str, Any]] = None,
        lease_ttl_seconds: int = 300,
        cache: Optional[MutableMapping[str, Mapping[str, Any]]] = None,
        rate_limiter=None,
        timeout_seconds: float = 12.0,
        circuit_failures: int = 5,
        circuit_reset_seconds: float = 120.0,
        client_factory: Optional[Callable[..., Any]] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        random_fn: Callable[[], float] = random.random,
        time_fn: Callable[[], float] = time.time,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        if lease_provider is not None and (proxy_manager is not None or proxy):
            raise ValueError(
                "lease_provider is exclusive with unmanaged proxy configuration"
            )
        self._proxy_manager = proxy_manager
        self._explicit_proxy = proxy
        self._lease_provider = lease_provider
        if traffic_ledger is None:
            traffic_ledger = _ledger_from_provider_grant(
                metered=lease_provider is not None,
                retry_limit=retry_budget,
            )
        elif (
            retry_budget is not None
            and traffic_ledger.retry_limit != int(retry_budget)
        ):
            raise ValueError(
                "retry_budget differs from the shared traffic ledger limit"
            )
        self._traffic_ledger = traffic_ledger
        self._lease_metadata = dict(lease_metadata or {})
        self._lease_ttl_seconds = max(1, int(lease_ttl_seconds))
        self._lease: Optional[ProxyLease] = None
        self._cache = cache
        self._rate_limiter = rate_limiter
        self.timeout_seconds = float(timeout_seconds)
        self._circuit_failures = int(circuit_failures)
        self._circuit_reset_seconds = max(0.0, float(circuit_reset_seconds))
        self._client_factory = client_factory
        self._sleep = sleep_fn
        self._random = random_fn
        self._time = time_fn
        self._monotonic = monotonic_fn

        self._client = None
        self._proxy_obj = None
        self._avoid_proxy_key: Optional[str] = None
        self._avoid_explicit_proxy = False
        self._consecutive_endpoint_failures = 0
        self._circuit_open = False
        self._circuit_opened_at: Optional[float] = None

        self._request_attempt_budget: Optional[int] = None
        self._decoded_body_budget: Optional[int] = None
        self._cycle_decoded_body_budget: Optional[int] = None
        self._budget_exhausted = False

        self._attempts = 0
        self._successful_attempts = 0
        self._failed_attempts = 0
        self._retries = 0
        self._decoded_body_bytes = 0
        self._estimated_wire_bytes = 0
        self._wire_known_responses = 0
        self._wire_unknown_responses = 0
        self._provider_up_bytes = 0
        self._provider_down_bytes = 0
        self._provider_metering_available = lease_provider is not None
        self._cache_hits = 0
        self._duration_seconds = 0.0
        self._status_counts: Dict[str, int] = defaultdict(int)
        self._by_label: Dict[str, Dict[str, Any]] = {}
        self._bytes_by_host: Dict[str, int] = defaultdict(int)

    # ------------------------------------------------------------------
    # Budget lifecycle
    # ------------------------------------------------------------------

    def set_decoded_body_budget(self, decoded_body_bytes: int) -> None:
        """Set an operation byte ceiling relative to traffic already consumed."""

        self._decoded_body_budget = self._decoded_body_bytes + max(
            0, int(decoded_body_bytes),
        )

    def set_cycle_decoded_body_budget(self, decoded_body_bytes: int) -> None:
        """Set an independent run-wide byte ceiling.

        Operation scopes are reset by the scraper before each phase.  The
        cycle ceiling deliberately is not, so a sequence of individually
        valid phases cannot exceed the shared paid-traffic budget.
        """

        self._cycle_decoded_body_budget = self._decoded_body_bytes + max(
            0, int(decoded_body_bytes),
        )

    def begin_request_scope(
        self,
        *,
        request_attempt_budget: int,
    ) -> None:
        """Set a hard attempt ceiling relative to traffic already consumed."""

        self._request_attempt_budget = self._attempts + max(
            0, int(request_attempt_budget),
        )

    def _check_request_budget(self) -> None:
        if (
            self._request_attempt_budget is not None
            and self._attempts >= self._request_attempt_budget
        ):
            self._budget_exhausted = True
            self.close()
            raise TrafficBudgetExceeded(
                "Transfermarkt request-attempt budget exhausted "
                f"({self._attempts}/{self._request_attempt_budget})"
            )

    def _check_decoded_budget(self, *, before_request: bool = False) -> None:
        limits = [
            ("operation", self._decoded_body_budget),
            ("cycle", self._cycle_decoded_body_budget),
        ]
        for scope, limit in limits:
            if limit is None:
                continue
            # A response is accounted only after it arrives, so cap+1 is
            # detected immediately post-response.  Once the counter is
            # exactly at the cap, the next request is rejected before I/O;
            # this is the no-N+1 guarantee.
            exhausted = (
                self._decoded_body_bytes >= limit
                if before_request
                else self._decoded_body_bytes > limit
            )
            if not exhausted:
                continue
            self._budget_exhausted = True
            self.close()
            raise TrafficBudgetExceeded(
                "Transfermarkt decoded-body budget exhausted "
                f"(scope={scope}; {self._decoded_body_bytes}/{limit} bytes)"
            )

    def _check_provider_budget(self, *, retry: bool) -> None:
        if self._lease_provider is None:
            return
        try:
            self._traffic_ledger.ensure_request_allowed(retry=retry)
        except TrafficBudgetExceeded:
            self._budget_exhausted = True
            self.close()
            raise

    # ------------------------------------------------------------------
    # Client/proxy lifecycle
    # ------------------------------------------------------------------

    def _new_tls_client(self, proxy_url: str):
        if self._client_factory is not None:
            return self._client_factory(
                proxy=proxy_url,
                headers=dict(DEFAULT_HEADERS),
                client_identifier="chrome_133",
            )
        import tls_requests

        return tls_requests.Client(
            proxy=_tls_requests_compatible_proxy_url(proxy_url),
            headers=dict(DEFAULT_HEADERS),
            client_identifier="chrome_133",
        )

    def _ensure_client(
        self,
        *,
        url: str,
        label: str,
        context: Mapping[str, Any],
        retry: bool,
    ):
        if self._client is not None:
            return self._client, self._proxy_obj

        proxy_obj = None
        proxy_url = None
        if self._lease_provider is not None:
            self._check_provider_budget(retry=retry)
            metadata = dict(self._lease_metadata)
            metadata.setdefault("canonical_url", url)
            metadata.setdefault("entity", label)
            metadata.update({
                key: value for key, value in context.items()
                if key in ("scope", "map_index", "try_number")
                and value is not None
            })
            remaining = self._traffic_ledger.remaining_hard_bytes
            if remaining <= 0:
                raise TrafficBudgetExceeded(
                    "Transfermarkt provider hard byte budget exhausted"
                )
            self._lease = self._lease_provider.acquire(
                max_bytes=remaining,
                ttl_seconds=self._lease_ttl_seconds,
                metadata=metadata,
            )
            if self._lease.max_bytes > remaining:
                lease = self._lease
                self._lease = None
                try:
                    self._lease_provider.close(lease)
                finally:
                    raise TrafficMeterError(
                        "proxy lease exceeds the shared remaining byte budget"
                    )
            proxy_url = self._lease_provider.authenticated_proxy_url(self._lease)
        elif self._proxy_manager is not None:
            # Random pools may hand back the just-blocked proxy.  Probe a
            # bounded number of selections so a retry really rotates when an
            # alternative exists.
            selections = max(1, int(self._proxy_manager.total_count) * 2)
            for _ in range(selections):
                candidate = self._proxy_manager.get_proxy()
                if candidate is None:
                    break
                key = f"{candidate.host}:{candidate.port}"
                proxy_obj = candidate
                if key != self._avoid_proxy_key:
                    break
            if (
                proxy_obj is not None
                and f"{proxy_obj.host}:{proxy_obj.port}" == self._avoid_proxy_key
            ):
                # Guarantee avoidance even when a random/custom selector keeps
                # returning the same failed proxy.
                best_getter = getattr(self._proxy_manager, 'get_best_proxies', None)
                alternatives = (
                    best_getter(limit=int(self._proxy_manager.total_count))
                    if callable(best_getter) else []
                )
                proxy_obj = next(
                    (
                        candidate for candidate in alternatives
                        if f"{candidate.host}:{candidate.port}" != self._avoid_proxy_key
                    ),
                    None,
                )
            if proxy_obj is not None:
                proxy_url = proxy_obj.url
        elif self._explicit_proxy:
            if not self._avoid_explicit_proxy:
                proxy_url = self._explicit_proxy

        if not proxy_url:
            raise ProxyRequiredError(
                "Transfermarkt requires a residential proxy; the configured "
                "pool is missing, empty, or all proxies are banned"
            )

        # Preserve the selected proxy when TLS client construction itself
        # fails so the caller can account/penalise/avoid it safely.
        self._proxy_obj = proxy_obj
        self._client = self._new_tls_client(proxy_url)
        return self._client, self._proxy_obj

    def _has_alternate_proxy(self, proxy_obj) -> bool:
        if self._lease_provider is not None:
            return self._traffic_ledger.remaining_hard_bytes > 0
        return (
            proxy_obj is not None
            and self._proxy_manager is not None
            and self._proxy_manager.available_count >= 2
        )

    def _avoid_on_next_client(self, proxy_obj) -> None:
        if proxy_obj is None:
            if self._explicit_proxy:
                self._avoid_explicit_proxy = True
        else:
            self._avoid_proxy_key = f"{proxy_obj.host}:{proxy_obj.port}"

    def _clear_failed_proxy_avoidance(self) -> None:
        self._avoid_proxy_key = None
        self._avoid_explicit_proxy = False

    def _discard_client(self) -> None:
        client = self._client
        self._client = None
        self._proxy_obj = None
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001 - cleanup must not mask outcome
                pass

    def _observe_lease_snapshot(
        self,
        snapshot: LeaseTrafficSnapshot,
    ) -> tuple[int, int]:
        if self._lease is None:
            return 0, 0
        delta_up, delta_down = self._traffic_ledger.observe_lease(
            self._lease.lease_id, snapshot,
        )
        self._provider_up_bytes += delta_up
        self._provider_down_bytes += delta_down
        return delta_up, delta_down

    def _read_lease_delta(self) -> tuple[int, int]:
        if self._lease_provider is None or self._lease is None:
            return 0, 0
        try:
            snapshot = self._lease_provider.stats(self._lease)
            return self._observe_lease_snapshot(snapshot)
        except (TrafficBudgetExceeded, TrafficMeterError):
            raise
        except Exception as exc:  # noqa: BLE001 - injected adapter boundary
            raise TrafficMeterError(
                f"could not read paid proxy counters: {redact_sensitive(exc)}"
            ) from exc

    def _close_lease(self, *, label: str) -> None:
        lease = self._lease
        if self._lease_provider is None or lease is None:
            return
        try:
            snapshot = self._lease_provider.close(lease)
            delta_up, delta_down = self._observe_lease_snapshot(snapshot)
            if delta_up or delta_down:
                self._traffic_ledger.record_unattributed_provider(
                    entity=label,
                    provider_up_bytes=delta_up,
                    provider_down_bytes=delta_down,
                )
        finally:
            self._lease = None

    def _discard_failed_transport(self, *, label: str) -> None:
        self._discard_client()
        if self._lease_provider is not None:
            self._close_lease(label=label)

    def close(self) -> None:
        self._discard_client()
        self._close_lease(label="lease_close")

    def _record_proxy(
        self,
        *,
        proxy_obj,
        success: bool,
        error_type: Optional[str],
        elapsed: float,
    ) -> None:
        if self._proxy_manager is not None and proxy_obj is not None:
            self._proxy_manager.record_result(
                proxy_obj,
                success=success,
                error_type=error_type,
                response_time=elapsed,
            )

    # ------------------------------------------------------------------
    # Traffic telemetry
    # ------------------------------------------------------------------

    def _label_stats(self, label: str) -> Dict[str, Any]:
        if label not in self._by_label:
            self._by_label[label] = {
                "attempts": 0,
                "requests": 0,
                "successes": 0,
                "failures": 0,
                "retries": 0,
                "cache_hits": 0,
                "duration_seconds": 0.0,
                "decoded_response_body_bytes": 0,
                "wire_response_bytes": 0,
                "provider_up_bytes": 0,
                "provider_down_bytes": 0,
                "provider_metered_bytes": 0,
                "status_counts": {},
            }
        return self._by_label[label]

    def _record_attempt(
        self,
        *,
        label: str,
        status_code: int,
        decoded_bytes: int,
        wire_bytes: Optional[int],
        provider_up_bytes: int,
        provider_down_bytes: int,
        host: str,
        success: bool,
        retry: bool,
        duration_seconds: float,
    ) -> None:
        self._attempts += 1
        self._decoded_body_bytes += decoded_bytes
        self._bytes_by_host[host] += decoded_bytes
        key = str(status_code)
        self._status_counts[key] += 1
        if success:
            self._successful_attempts += 1
        else:
            self._failed_attempts += 1
        if retry:
            self._retries += 1
        self._duration_seconds += max(0.0, float(duration_seconds))
        if wire_bytes is None:
            self._wire_unknown_responses += 1
        else:
            self._wire_known_responses += 1
            self._estimated_wire_bytes += wire_bytes

        stats = self._label_stats(label)
        stats["attempts"] += 1
        stats["requests"] += 1
        stats["decoded_response_body_bytes"] += decoded_bytes
        stats["wire_response_bytes"] += max(0, int(provider_down_bytes))
        stats["provider_up_bytes"] += max(0, int(provider_up_bytes))
        stats["provider_down_bytes"] += max(0, int(provider_down_bytes))
        stats["provider_metered_bytes"] += max(
            0, int(provider_up_bytes) + int(provider_down_bytes),
        )
        stats["duration_seconds"] += max(0.0, float(duration_seconds))
        stats["successes" if success else "failures"] += 1
        if retry:
            stats["retries"] += 1
        label_statuses = stats["status_counts"]
        label_statuses[key] = label_statuses.get(key, 0) + 1

        self._traffic_ledger.record_attempt(
            entity=label,
            decoded_bytes=decoded_bytes,
            provider_up_bytes=provider_up_bytes,
            provider_down_bytes=provider_down_bytes,
            retry=retry,
            duration_seconds=duration_seconds,
        )

        self._check_decoded_budget()

    def _record_cache_hit(self, *, label: str, duration_seconds: float) -> None:
        self._cache_hits += 1
        self._duration_seconds += max(0.0, float(duration_seconds))
        stats = self._label_stats(label)
        stats["cache_hits"] += 1
        stats["duration_seconds"] += max(0.0, float(duration_seconds))
        self._traffic_ledger.record_cache_hit(
            entity=label, duration_seconds=duration_seconds,
        )

    def _record_duration_overhead(
        self,
        *,
        label: str,
        duration_seconds: float,
    ) -> None:
        duration = max(0.0, float(duration_seconds))
        if not duration:
            return
        self._duration_seconds += duration
        self._label_stats(label)["duration_seconds"] += duration
        self._traffic_ledger.record_duration_overhead(
            entity=label, duration_seconds=duration,
        )

    @staticmethod
    def _response_bytes(resp) -> bytes:
        try:
            content = resp.content
            if isinstance(content, bytes):
                return content
            if isinstance(content, bytearray):
                return bytes(content)
        except Exception:  # noqa: BLE001 - telemetry fallback below
            pass
        try:
            return str(resp.text or "").encode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            return b""

    @staticmethod
    def _content_length(resp) -> Optional[int]:
        try:
            raw = resp.headers.get("Content-Length")
            value = int(raw)
            return value if value >= 0 else None
        except (AttributeError, TypeError, ValueError):
            return None

    @staticmethod
    def _host(url: str) -> str:
        from urllib.parse import urlsplit

        return urlsplit(url).netloc or url

    def get_traffic_stats(self) -> Dict[str, Any]:
        """Return decoded bytes plus explicitly labelled wire estimate."""

        if self._wire_unknown_responses:
            estimated_wire: Optional[int] = None
        else:
            estimated_wire = self._estimated_wire_bytes
        by_host = sorted(self._bytes_by_host.items(), key=lambda item: -item[1])
        circuit_state = "open" if self._circuit_open else "closed"
        by_label: Dict[str, Dict[str, Any]] = {}
        for label, raw in self._by_label.items():
            item = dict(raw)
            denominator = item["requests"] + item["cache_hits"]
            item["cache_hit_rate"] = (
                item["cache_hits"] / denominator if denominator else 0.0
            )
            item["duration_seconds"] = round(item["duration_seconds"], 6)
            if not self._provider_metering_available:
                item["wire_response_bytes"] = None
                item["provider_up_bytes"] = None
                item["provider_down_bytes"] = None
                item["provider_metered_bytes"] = None
            by_label[label] = item
        total_operations = self._attempts + self._cache_hits
        provider_total = self._provider_up_bytes + self._provider_down_bytes
        shared_ledger = self._traffic_ledger.snapshot()
        return {
            "decoded_response_body_bytes": self._decoded_body_bytes,
            "decoded_response_body_mb": round(
                self._decoded_body_bytes / 1024 / 1024, 4,
            ),
            # Compatibility alias used by the existing traffic guard.  It is
            # decoded response-body traffic, not claimed provider billing.
            "proxy_response_bytes": self._decoded_body_bytes,
            "proxy_response_mb": round(self._decoded_body_bytes / 1024 / 1024, 4),
            "estimated_wire_response_bytes": estimated_wire,
            "estimated_wire_response_mb": (
                round(estimated_wire / 1024 / 1024, 4)
                if estimated_wire is not None else None
            ),
            "estimated_wire_known_responses": self._wire_known_responses,
            "estimated_wire_unknown_responses": self._wire_unknown_responses,
            "wire_response_bytes": (
                self._provider_down_bytes
                if self._provider_metering_available else None
            ),
            "provider_up_bytes": (
                self._provider_up_bytes
                if self._provider_metering_available else None
            ),
            "provider_down_bytes": (
                self._provider_down_bytes
                if self._provider_metering_available else None
            ),
            "provider_metered_bytes": (
                provider_total if self._provider_metering_available else None
            ),
            "provider_metering_available": self._provider_metering_available,
            "network_fetches": self._attempts,
            "request_attempts": self._attempts,
            "requests": self._attempts,
            "successful_attempts": self._successful_attempts,
            "failed_attempts": self._failed_attempts,
            "retries": self._retries,
            "cache_hits": self._cache_hits,
            "cache_hit_rate": (
                self._cache_hits / total_operations if total_operations else 0.0
            ),
            "duration_seconds": round(self._duration_seconds, 6),
            "status_counts": dict(sorted(self._status_counts.items())),
            "by_label": by_label,
            "budget_exhausted": self._budget_exhausted,
            "request_attempt_budget": self._request_attempt_budget,
            "decoded_body_budget_bytes": self._decoded_body_budget,
            "cycle_decoded_body_budget_bytes": self._cycle_decoded_body_budget,
            "hard_provider_byte_budget": shared_ledger[
                "hard_provider_byte_budget"
            ],
            "soft_provider_byte_stop": shared_ledger[
                "soft_provider_byte_stop"
            ],
            "shared_traffic_ledger": shared_ledger,
            "circuit_state": circuit_state,
            "circuit_breaker_state": circuit_state,
            "top_traffic_urls": [
                {
                    "url": host,
                    "bytes": nbytes,
                    "mb": round(nbytes / 1024 / 1024, 4),
                }
                for host, nbytes in by_host[:10]
            ],
        }

    # ------------------------------------------------------------------
    # Fetch policy
    # ------------------------------------------------------------------

    @staticmethod
    def _looks_like_html_challenge(text: str) -> bool:
        lower = text.lower()
        return any(marker in lower for marker in _CHALLENGE_MARKERS)

    @staticmethod
    def _looks_like_json_challenge(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        error = payload.get("error")
        if not error:
            return False
        try:
            rendered = json.dumps(error, ensure_ascii=False).lower()
        except (TypeError, ValueError):
            rendered = str(error).lower()
        return "challenge" in rendered or "cloudflare" in rendered

    @staticmethod
    def _validator_problem(
        validator: Optional[Callable[[Any], Optional[str]]],
        value: Any,
    ) -> Optional[str]:
        """Run source validation without turning its bugs into proxy failures."""

        if validator is None:
            return None
        try:
            problem = validator(value)
        except Exception as exc:  # noqa: BLE001 - validator is caller supplied
            return redact_sensitive(
                f"validator raised {type(exc).__name__}: {exc}"
            )
        return redact_sensitive(problem) if problem else None

    def _backoff(self, attempt: int) -> None:
        self._sleep((0.5 * (2 ** max(0, attempt - 1))) + (0.25 * self._random()))

    def _finish(self, outcome: FetchOutcome[Any]) -> FetchOutcome[Any]:
        if outcome.is_success:
            self._consecutive_endpoint_failures = 0
        else:
            self._consecutive_endpoint_failures += 1
            if self._consecutive_endpoint_failures >= self._circuit_failures:
                self._circuit_open = True
                self._circuit_opened_at = self._time()
                self.close()
        return outcome

    def _circuit_admits_a_probe(self) -> bool:
        """Let one request through once the source has had time to recover.

        The source fails in waves.  A breaker that never reopens turns a wave
        into the end of the entity: every later page is refused without being
        tried, and a run that had paid for hundreds of pages dies holding them.
        A failed probe re-opens the breaker on its own, so a source that is
        still down costs one request per cooldown, not a retry storm.
        """
        if self._circuit_opened_at is None:
            return False
        if self._time() - self._circuit_opened_at < self._circuit_reset_seconds:
            return False
        self._circuit_open = False
        self._circuit_opened_at = None
        self._consecutive_endpoint_failures = self._circuit_failures - 1
        return True

    def _load_cached_outcome(self, cache_key: Optional[str]) -> Optional[FetchOutcome[Any]]:
        if self._cache is None or not cache_key:
            return None
        raw = self._cache.get(cache_key)
        if not isinstance(raw, Mapping):
            return None
        try:
            expires_at = float(raw["expires_at"])
            if expires_at <= self._time():
                self._cache.pop(cache_key, None)
                return None
            outcome_raw = raw["outcome"]
            if not isinstance(outcome_raw, Mapping):
                raise ValueError("cached outcome is not an object")
            return FetchOutcome.from_checkpoint(outcome_raw)
        except (KeyError, TypeError, ValueError):
            # A corrupt cache is an ordinary miss; it is never returned as an
            # authoritative source result or allowed to poison a checkpoint.
            self._cache.pop(cache_key, None)
            return None

    def _store_cached_outcome(
        self,
        *,
        cache_key: Optional[str],
        cache_ttl_seconds: Optional[float],
        outcome: FetchOutcome[Any],
    ) -> None:
        if (
            self._cache is None
            or not cache_key
            or cache_ttl_seconds is None
            or float(cache_ttl_seconds) <= 0
            or not outcome.is_success
        ):
            return
        self._cache[cache_key] = {
            "cache_version": 1,
            "expires_at": self._time() + float(cache_ttl_seconds),
            "outcome": outcome.as_checkpoint(),
        }

    def fetch(
        self,
        url: str,
        *,
        as_json: bool,
        # Residential exits and the source itself answer 502/504 for a large
        # share of attempts; three tries lose a page outright often enough that
        # a scope dies without fetching anything. The run-wide retry ledger,
        # not this cap, is what actually bounds the paid traffic.
        max_attempts: int = 6,
        label: str = "endpoint",
        context: Optional[Mapping[str, Any]] = None,
        validator: Optional[Callable[[Any], Optional[str]]] = None,
        cache_key: Optional[str] = None,
        cache_ttl_seconds: Optional[float] = None,
    ) -> FetchOutcome[Any]:
        """Fetch one logical endpoint with bounded status-aware retries."""

        context = dict(context or {})
        cache_started = self._monotonic()
        cached = self._load_cached_outcome(cache_key)
        if cached is not None:
            duration = self._monotonic() - cache_started
            self._record_cache_hit(label=label, duration_seconds=duration)
            return cached.as_cache_hit(duration_seconds=duration)
        if self._circuit_open and not self._circuit_admits_a_probe():
            return FetchOutcome(
                status=FetchStatus.RETRY_EXHAUSTED,
                error=(
                    'in-run endpoint circuit is open after '
                    f'{self._circuit_failures} failures; it admits a probe '
                    f'after {self._circuit_reset_seconds:.0f}s'
                ),
                label=label,
                context=context,
            )

        attempts_cap = max(1, min(_MAX_FETCH_ATTEMPTS, int(max_attempts)))
        endpoint_started = self._monotonic()
        decoded_for_endpoint = 0
        wire_for_endpoint = 0
        provider_for_endpoint = 0
        attempt_duration_for_endpoint = 0.0
        endpoint_attempt_start = self._attempts
        last_status: Optional[int] = None
        last_error: Optional[str] = None
        terminal_status = FetchStatus.RETRY_EXHAUSTED

        for attempt in range(1, attempts_cap + 1):
            self._check_request_budget()
            self._check_decoded_budget(before_request=True)
            self._check_provider_budget(retry=attempt > 1)
            started = self._monotonic()
            proxy_obj = self._proxy_obj
            try:
                client, proxy_obj = self._ensure_client(
                    url=url, label=label, context=context, retry=attempt > 1,
                )
                if self._rate_limiter is not None:
                    self._rate_limiter.acquire()
                resp = client.get(
                    url,
                    timeout=self.timeout_seconds,
                )
                elapsed = self._monotonic() - started
                attempt_duration_for_endpoint += elapsed
                status_code = int(getattr(resp, "status_code", 0) or 0)
                last_status = status_code
                body = self._response_bytes(resp)
                body_n = len(body)
                decoded_for_endpoint += body_n
                wire_n = self._content_length(resp)
                provider_up, provider_down = self._read_lease_delta()
                if self._provider_metering_available:
                    wire_for_endpoint += provider_down
                    provider_for_endpoint += provider_up + provider_down

                if status_code == 200:
                    if as_json:
                        try:
                            value = resp.json()
                        except Exception as exc:  # noqa: BLE001
                            value = None
                            body_text = body.decode("utf-8", errors="replace")
                            if self._looks_like_html_challenge(body_text):
                                last_error = "Cloudflare invalid-JSON challenge"
                                terminal_status = FetchStatus.BLOCKED
                            else:
                                last_error = redact_sensitive(
                                    f"json_decode: {type(exc).__name__}: {exc}"
                                )
                                terminal_status = FetchStatus.SCHEMA_ERROR
                        else:
                            if not isinstance(value, dict):
                                last_error = (
                                    "schema: expected JSON object, got "
                                    f"{type(value).__name__}"
                                )
                                terminal_status = FetchStatus.SCHEMA_ERROR
                            elif self._looks_like_json_challenge(value):
                                last_error = "Cloudflare JSON challenge"
                                terminal_status = FetchStatus.BLOCKED
                            elif problem := self._validator_problem(validator, value):
                                last_error = f"schema: {problem}"
                                terminal_status = FetchStatus.SCHEMA_ERROR
                            else:
                                self._record_proxy(
                                    proxy_obj=proxy_obj,
                                    success=True,
                                    error_type=None,
                                    elapsed=elapsed,
                                )
                                self._record_attempt(
                                    label=label,
                                    status_code=200,
                                    decoded_bytes=body_n,
                                    wire_bytes=wire_n,
                                    provider_up_bytes=provider_up,
                                    provider_down_bytes=provider_down,
                                    host=self._host(url),
                                    success=True,
                                    retry=attempt > 1,
                                    duration_seconds=elapsed,
                                )
                                self._clear_failed_proxy_avoidance()
                                total_duration = (
                                    self._monotonic() - endpoint_started
                                )
                                self._record_duration_overhead(
                                    label=label,
                                    duration_seconds=(
                                        total_duration
                                        - attempt_duration_for_endpoint
                                    ),
                                )
                                outcome = self._finish(FetchOutcome(
                                    status=FetchStatus.OK,
                                    value=value,
                                    status_code=200,
                                    attempts=self._attempts - endpoint_attempt_start,
                                    label=label,
                                    context=context,
                                    decoded_body_bytes=decoded_for_endpoint,
                                    wire_body_bytes=(
                                        wire_for_endpoint
                                        if self._provider_metering_available else None
                                    ),
                                    provider_metered_bytes=(
                                        provider_for_endpoint
                                        if self._provider_metering_available else None
                                    ),
                                    duration_seconds=total_duration,
                                    payload_hash=_payload_hash(value),
                                ))
                                self._store_cached_outcome(
                                    cache_key=cache_key,
                                    cache_ttl_seconds=cache_ttl_seconds,
                                    outcome=outcome,
                                )
                                return outcome
                    else:
                        try:
                            value = resp.text
                        except Exception:  # noqa: BLE001
                            value = body.decode("utf-8", errors="replace")
                        if self._looks_like_html_challenge(value):
                            last_error = "Cloudflare HTML challenge"
                            terminal_status = FetchStatus.BLOCKED
                        elif problem := self._validator_problem(validator, value):
                            last_error = f"schema: {problem}"
                            terminal_status = FetchStatus.SCHEMA_ERROR
                        else:
                            self._record_proxy(
                                proxy_obj=proxy_obj,
                                success=True,
                                error_type=None,
                                elapsed=elapsed,
                            )
                            self._record_attempt(
                                label=label,
                                status_code=200,
                                decoded_bytes=body_n,
                                wire_bytes=wire_n,
                                provider_up_bytes=provider_up,
                                provider_down_bytes=provider_down,
                                host=self._host(url),
                                success=True,
                                retry=attempt > 1,
                                duration_seconds=elapsed,
                            )
                            self._clear_failed_proxy_avoidance()
                            total_duration = self._monotonic() - endpoint_started
                            self._record_duration_overhead(
                                label=label,
                                duration_seconds=(
                                    total_duration - attempt_duration_for_endpoint
                                ),
                            )
                            outcome = self._finish(FetchOutcome(
                                status=FetchStatus.OK,
                                value=value,
                                status_code=200,
                                attempts=self._attempts - endpoint_attempt_start,
                                label=label,
                                context=context,
                                decoded_body_bytes=decoded_for_endpoint,
                                wire_body_bytes=(
                                    wire_for_endpoint
                                    if self._provider_metering_available else None
                                ),
                                provider_metered_bytes=(
                                    provider_for_endpoint
                                    if self._provider_metering_available else None
                                ),
                                duration_seconds=total_duration,
                                payload_hash=_payload_hash(value),
                            ))
                            self._store_cached_outcome(
                                cache_key=cache_key,
                                cache_ttl_seconds=cache_ttl_seconds,
                                outcome=outcome,
                            )
                            return outcome

                    # Only an actual challenge merits another paid attempt on
                    # a different proxy.  Plain schema drift is authoritative
                    # source breakage and fails immediately without blaming or
                    # burning the proxy.
                    is_challenge = terminal_status == FetchStatus.BLOCKED
                    retry_allowed = (
                        is_challenge
                        and self._has_alternate_proxy(proxy_obj)
                        and attempt < min(attempts_cap, 2)
                    )
                    error_type = (
                        ErrorType.CLOUDFLARE.value
                        if is_challenge else ErrorType.UNKNOWN.value
                    )
                elif status_code == 404:
                    # A missing route/player is not proof that an existing
                    # career/history collection is authoritatively empty.
                    # Only a schema-valid HTTP 200 empty collection may drive
                    # negative caching/deletion.
                    terminal_status = FetchStatus.RETRY_EXHAUSTED
                    last_error = "HTTP 404 is not an authoritative empty payload"
                    error_type = ErrorType.UNKNOWN.value
                    retry_allowed = False
                    self._record_proxy(
                        proxy_obj=proxy_obj,
                        success=True,
                        error_type=None,
                        elapsed=elapsed,
                    )
                    self._record_attempt(
                        label=label,
                        status_code=404,
                        decoded_bytes=body_n,
                        wire_bytes=wire_n,
                        provider_up_bytes=provider_up,
                        provider_down_bytes=provider_down,
                        host=self._host(url),
                        success=False,
                        retry=attempt > 1,
                        duration_seconds=elapsed,
                    )
                    break
                elif status_code in (403, 429):
                    terminal_status = FetchStatus.BLOCKED
                    last_error = f"HTTP {status_code}"
                    error_type = (
                        ErrorType.FORBIDDEN.value
                        if status_code == 403 else ErrorType.RATE_LIMIT.value
                    )
                    # A block is a statement about the exit, not the page.  Two
                    # attempts meant one alternate exit, and a third of the
                    # residential pool cannot reach the source at any moment —
                    # so a good page was routinely abandoned as blocked.
                    retry_allowed = (
                        self._has_alternate_proxy(proxy_obj)
                        and attempt < min(attempts_cap, _MAX_BLOCKED_ATTEMPTS)
                    )
                elif status_code >= 500 or status_code == 0:
                    terminal_status = FetchStatus.RETRY_EXHAUSTED
                    last_error = f"HTTP {status_code}"
                    error_type = ErrorType.UNKNOWN.value
                    retry_allowed = attempt < attempts_cap
                else:
                    terminal_status = FetchStatus.RETRY_EXHAUSTED
                    last_error = f"HTTP {status_code}"
                    error_type = ErrorType.UNKNOWN.value
                    retry_allowed = False

                proxy_success = terminal_status == FetchStatus.SCHEMA_ERROR
                self._record_proxy(
                    proxy_obj=proxy_obj,
                    success=proxy_success,
                    error_type=None if proxy_success else error_type,
                    elapsed=elapsed,
                )
                self._record_attempt(
                    label=label,
                    status_code=status_code,
                    decoded_bytes=body_n,
                    wire_bytes=wire_n,
                    provider_up_bytes=provider_up,
                    provider_down_bytes=provider_down,
                    host=self._host(url),
                    success=False,
                    retry=attempt > 1,
                    duration_seconds=elapsed,
                )
                if not proxy_success:
                    self._avoid_on_next_client(proxy_obj)
                    self._discard_failed_transport(label=label)
                if not retry_allowed:
                    break
            except TrafficBudgetExceeded:
                self._budget_exhausted = True
                self._discard_client()
                try:
                    self._close_lease(label=label)
                except Exception as close_exc:  # noqa: BLE001 - preserve budget error
                    logger.error(
                        "could not close exhausted paid proxy lease: %s",
                        redact_sensitive(close_exc),
                    )
                raise
            except ProxyRequiredError as exc:
                if self._attempts == endpoint_attempt_start:
                    raise
                last_status = 0
                last_error = redact_sensitive(exc)
                terminal_status = FetchStatus.RETRY_EXHAUSTED
                break
            except TrafficMeterError:
                self._budget_exhausted = True
                self._discard_client()
                self._close_lease(label=label)
                raise
            except Exception as exc:  # noqa: BLE001 - tls package exceptions vary
                elapsed = self._monotonic() - started
                attempt_duration_for_endpoint += elapsed
                proxy_obj = self._proxy_obj or proxy_obj
                last_status = 0
                last_error = redact_sensitive(
                    f"transport: {type(exc).__name__}: {exc}"
                )
                terminal_status = FetchStatus.RETRY_EXHAUSTED
                error_name = type(exc).__name__.lower()
                error_type = (
                    ErrorType.TIMEOUT.value
                    if "timeout" in error_name or "timedout" in last_error.lower()
                    else ErrorType.CONNECTION.value
                )
                provider_up, provider_down = self._read_lease_delta()
                if self._provider_metering_available:
                    wire_for_endpoint += provider_down
                    provider_for_endpoint += provider_up + provider_down
                self._record_proxy(
                    proxy_obj=proxy_obj,
                    success=False,
                    error_type=error_type,
                    elapsed=elapsed,
                )
                self._record_attempt(
                    label=label,
                    status_code=0,
                    decoded_bytes=0,
                    wire_bytes=None,
                    provider_up_bytes=provider_up,
                    provider_down_bytes=provider_down,
                    host=self._host(url),
                    success=False,
                    retry=attempt > 1,
                    duration_seconds=elapsed,
                )
                self._avoid_on_next_client(proxy_obj)
                self._discard_failed_transport(label=label)
                if attempt >= attempts_cap:
                    break

            logger.warning(
                "%s attempt %d/%d failed (%s): %s",
                label,
                attempt,
                attempts_cap,
                context or url,
                last_error,
            )
            self._backoff(attempt)

        total_duration = self._monotonic() - endpoint_started
        self._record_duration_overhead(
            label=label,
            duration_seconds=total_duration - attempt_duration_for_endpoint,
        )
        return self._finish(FetchOutcome(
            status=terminal_status,
            status_code=last_status,
            error=last_error,
            attempts=self._attempts - endpoint_attempt_start,
            label=label,
            context=context,
            decoded_body_bytes=decoded_for_endpoint,
            wire_body_bytes=(
                wire_for_endpoint if self._provider_metering_available else None
            ),
            provider_metered_bytes=(
                provider_for_endpoint if self._provider_metering_available else None
            ),
            duration_seconds=total_duration,
        ))
