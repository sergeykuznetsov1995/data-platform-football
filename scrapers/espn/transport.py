"""Bounded, direct-only native HTTPS transport for ESPN JSON endpoints."""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
import zlib
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, Mapping, Optional
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import requests
from urllib3.exceptions import HTTPError as Urllib3HTTPError
from urllib3.exceptions import ReadTimeoutError as Urllib3ReadTimeoutError

from .raw_store import (
    EspnRawStore,
    RawJsonRecord,
    RawStoreError,
    RawTargetCorrupt,
    RawTargetNotFound,
)
from .transport_contracts import (
    AmbientProxyError,
    ByteReservation,
    BudgetExceeded,
    CanonicalTarget,
    CircuitOpen,
    DEFAULT_BURST,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_COMPETITIONS,
    DEFAULT_MAX_REQUESTS,
    DEFAULT_MAX_SUMMARY_EVENTS,
    DEFAULT_MAX_TASK_BYTES,
    DEFAULT_RATE_PER_MINUTE,
    DEFAULT_READ_TIMEOUT,
    DEFAULT_RESPONSE_CAP_BYTES,
    DirectTransportError,
    EndpointType,
    ESPN_SITE_API_FAILOVER_ORIGIN,
    ESPN_SITE_API_PRIMARY_ORIGIN,
    EspnTransportError,
    FetchResult,
    HttpStatusError,
    InvalidJsonError,
    Params,
    RequestLedgerEntry,
    ResponseTooLarge,
    RetryExhausted,
    TaskBudget,
    _nonnegative_int,
    canonicalize_target,
    normalize_transport_origin,
)


RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429})
PROXY_VARIABLES = frozenset({"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"})
_SITE_DATA_PATH = re.compile(
    r"\A/apis/site/v2/sports/soccer/[A-Za-z0-9][A-Za-z0-9._-]*/"
    r"(?P<endpoint>scoreboard|summary)\Z"
)


def _transport_origin(url: str) -> str:
    parsed = urlsplit(url)
    return normalize_transport_origin(
        urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    )


def _official_site_failover_url(
    target: CanonicalTarget,
    endpoint: EndpointType,
    *,
    enabled: bool,
    event_id: object = None,
) -> Optional[str]:
    """Map only an exact blocked data route to ESPN's official site mirror."""

    parsed = urlsplit(target.canonical_url)
    if (
        not enabled
        or _transport_origin(target.canonical_url) != ESPN_SITE_API_PRIMARY_ORIGIN
    ):
        return None
    match = _SITE_DATA_PATH.fullmatch(parsed.path)
    if match is None or match.group("endpoint") != endpoint.value:
        return None
    if endpoint not in {EndpointType.SCOREBOARD, EndpointType.SUMMARY}:
        return None
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if len(query) != len({key for key, _ in query}):
        return None
    params = dict(query)
    if endpoint is EndpointType.SCOREBOARD:
        if set(params) not in ({"limit"}, {"dates", "limit"}):
            return None
        if not re.fullmatch(r"[1-9][0-9]{0,3}", params["limit"]):
            return None
        if int(params["limit"]) > 1000:
            return None
        if "dates" in params and not re.fullmatch(
            r"[0-9]{8}(?:-[0-9]{8})?", params["dates"]
        ):
            return None
    else:
        if set(params) != {"event"} or not re.fullmatch(
            r"[1-9][0-9]*", params["event"]
        ):
            return None
        if event_id is None or str(event_id) != params["event"]:
            return None
    mirror = urlsplit(ESPN_SITE_API_FAILOVER_ORIGIN)
    return urlunsplit((parsed.scheme, mirror.netloc, parsed.path, parsed.query, ""))


class _ReadLimitExceeded(Exception):
    def __init__(self, direct_bytes: int, *, task_budget: bool) -> None:
        self.direct_bytes = direct_bytes
        self.task_budget = task_budget


class _ResponseEncodingError(Exception):
    def __init__(self, direct_bytes: int, cause: Exception) -> None:
        self.direct_bytes = direct_bytes
        self.__cause__ = cause


class _ResponseReadTimeout(Exception):
    def __init__(self, direct_bytes: int) -> None:
        self.direct_bytes = direct_bytes


class _ResponseReadFailure(Exception):
    def __init__(self, direct_bytes: int, error_type: str) -> None:
        self.direct_bytes = direct_bytes
        self.error_type = error_type


class _TokenBucket:
    def __init__(
        self,
        rate_per_minute: int,
        burst: int,
        monotonic_fn: Callable[[], float],
        sleep_fn: Callable[[float], None],
    ) -> None:
        self.rate_per_second = rate_per_minute / 60.0
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.monotonic_fn = monotonic_fn
        self.sleep_fn = sleep_fn
        self.updated_at = monotonic_fn()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = self.monotonic_fn()
            elapsed = max(0.0, now - self.updated_at)
            self.tokens = min(
                self.capacity, self.tokens + elapsed * self.rate_per_second
            )
            self.updated_at = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            delay = (1.0 - self.tokens) / self.rate_per_second
            self.sleep_fn(delay)
            # A fake sleep/clock is deliberately supported by accounting for
            # the requested wait, rather than polling wall time.
            self.tokens = 0.0
            self.updated_at = self.monotonic_fn()


class EspnHttpClient:
    """Cache-first HTTPS client with hard retry, rate, circuit and task bounds."""

    def __init__(
        self,
        raw_store: Optional[EspnRawStore] = None,
        *,
        raw_store_uri: Optional[str] = None,
        session: Optional[requests.Session] = None,
        budget: Optional[TaskBudget] = None,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = DEFAULT_READ_TIMEOUT,
        response_cap_bytes: int = DEFAULT_RESPONSE_CAP_BYTES,
        rate_per_minute: int = DEFAULT_RATE_PER_MINUTE,
        burst: int = DEFAULT_BURST,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        circuit_failure_threshold: int = 5,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
        utcnow_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        request_permit: Optional[Callable[[], None]] = None,
        allow_site_origin_failover: bool = False,
        environ: Optional[Mapping[str, str]] = None,
    ) -> None:
        environment = os.environ if environ is None else environ
        proxy_names = sorted(
            key
            for key, value in environment.items()
            if key.upper() in PROXY_VARIABLES and str(value).strip()
        )
        if proxy_names:
            raise AmbientProxyError(
                "Ambient proxy variables are forbidden for ESPN transport: "
                + ", ".join(proxy_names)
            )
        if raw_store is not None and raw_store_uri is not None:
            raise ValueError("provide raw_store or raw_store_uri, not both")
        self.raw_store = (
            raw_store
            or (EspnRawStore.from_uri(raw_store_uri) if raw_store_uri else None)
            or EspnRawStore.from_env()
        )
        self.connect_timeout = float(connect_timeout)
        self.read_timeout = float(read_timeout)
        self.response_cap_bytes = _nonnegative_int(
            response_cap_bytes, "response_cap_bytes"
        )
        self.rate_per_minute = _nonnegative_int(rate_per_minute, "rate_per_minute")
        self.burst = _nonnegative_int(burst, "burst")
        self.max_attempts = _nonnegative_int(max_attempts, "max_attempts")
        self.circuit_failure_threshold = _nonnegative_int(
            circuit_failure_threshold, "circuit_failure_threshold"
        )
        if (
            min(
                self.connect_timeout,
                self.read_timeout,
                self.response_cap_bytes,
                self.rate_per_minute,
                self.burst,
                self.max_attempts,
                self.circuit_failure_threshold,
            )
            <= 0
        ):
            raise ValueError("ESPN transport bounds must all be positive")
        hard_bounds = {
            "connect_timeout": (self.connect_timeout, DEFAULT_CONNECT_TIMEOUT),
            "read_timeout": (self.read_timeout, DEFAULT_READ_TIMEOUT),
            "response_cap_bytes": (
                self.response_cap_bytes,
                DEFAULT_RESPONSE_CAP_BYTES,
            ),
            "rate_per_minute": (self.rate_per_minute, DEFAULT_RATE_PER_MINUTE),
            "burst": (self.burst, DEFAULT_BURST),
            "max_attempts": (self.max_attempts, DEFAULT_MAX_ATTEMPTS),
            "circuit_failure_threshold": (self.circuit_failure_threshold, 5),
        }
        for name, (value, hard_maximum) in hard_bounds.items():
            if value > hard_maximum:
                raise ValueError(f"{name} cannot exceed {hard_maximum}")
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.session.proxies.clear()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
                "User-Agent": "data-platform-football/espn-native-v2",
            }
        )
        self.budget = budget or TaskBudget()
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.utcnow_fn = utcnow_fn
        if request_permit is not None and not callable(request_permit):
            raise TypeError("request_permit must be callable or None")
        self.request_permit = request_permit
        if type(allow_site_origin_failover) is not bool:
            raise TypeError("allow_site_origin_failover must be a boolean")
        self.allow_site_origin_failover = allow_site_origin_failover
        self._rate_limiter = _TokenBucket(
            self.rate_per_minute,
            self.burst,
            monotonic_fn,
            sleep_fn,
        )
        self._ledger: list[RequestLedgerEntry] = []
        self._lock = threading.RLock()
        self._consecutive_retryable_failures = 0

    @property
    def ledger(self) -> tuple[RequestLedgerEntry, ...]:
        with self._lock:
            return tuple(self._ledger)

    @property
    def circuit_is_open(self) -> bool:
        with self._lock:
            return (
                self._consecutive_retryable_failures >= self.circuit_failure_threshold
            )

    def _append_ledger(self, entry: RequestLedgerEntry) -> RequestLedgerEntry:
        if entry.proxy_bytes != 0:
            raise AssertionError("ESPN proxy_bytes must always be zero")
        with self._lock:
            self._ledger.append(entry)
        return entry

    def _record_retryable_failure(self) -> None:
        with self._lock:
            self._consecutive_retryable_failures += 1

    def _record_nonretryable_or_success(self) -> None:
        with self._lock:
            self._consecutive_retryable_failures = 0

    def _entry(
        self,
        target: CanonicalTarget,
        endpoint: EndpointType,
        *,
        started: float,
        attempts: int,
        status: Optional[int],
        direct_bytes: int,
        disposition: str,
        record: Optional[RawJsonRecord] = None,
        error: Optional[str] = None,
        transport_origin: Optional[str] = None,
    ) -> RequestLedgerEntry:
        return self._append_ledger(
            RequestLedgerEntry(
                url_fingerprint=target.url_fingerprint,
                endpoint=endpoint,
                attempts=attempts,
                status=status,
                direct_bytes=direct_bytes,
                proxy_bytes=0,
                latency_ms=max(0.0, (self.monotonic_fn() - started) * 1000.0),
                raw_uri=record.raw_uri if record else None,
                content_hash=record.content_hash if record else None,
                disposition=disposition,
                error=error,
                transport_origin=transport_origin,
            )
        )

    def _cached_result(
        self,
        target: CanonicalTarget,
        endpoint: EndpointType,
        body: bytes,
        record: RawJsonRecord,
        *,
        started: float,
        disposition: str = "cache_hit",
    ) -> FetchResult:
        # v1 aliases predate failover, so their logical HTTPS origin is also
        # their actual transport origin. Keep the alias bytes untouched while
        # allowing a new v2 checkpoint to reuse that audited legacy capture.
        transport_origin = record.transport_origin or _transport_origin(
            target.canonical_url
        )
        try:
            data = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            entry = self._entry(
                target,
                endpoint,
                started=started,
                attempts=0,
                status=record.http_status,
                direct_bytes=0,
                disposition="corrupt_cache",
                record=record,
                error="cached raw body is not valid JSON",
                transport_origin=transport_origin,
            )
            raise InvalidJsonError(
                "Cached ESPN raw body is not valid JSON", ledger_entry=entry
            ) from exc
        self._entry(
            target,
            endpoint,
            started=started,
            attempts=0,
            status=record.http_status,
            direct_bytes=0,
            disposition=disposition,
            record=record,
            transport_origin=transport_origin,
        )
        return FetchResult(
            target=target,
            endpoint=endpoint,
            json_data=data,
            body=body,
            attempts=0,
            status=record.http_status,
            cache_hit=True,
            direct_bytes=0,
            proxy_bytes=0,
            raw_uri=record.raw_uri,
            content_hash=record.content_hash,
            fetched_at=record.fetched_at,
            transport_origin=transport_origin,
        )

    def replay_json(
        self,
        url: str,
        endpoint: EndpointType | str,
        params: Optional[Params] = None,
        *,
        competition_id: object = None,
        event_id: object = None,
    ) -> FetchResult:
        started = self.monotonic_fn()
        target = canonicalize_target(url, params)
        if _transport_origin(target.canonical_url) == ESPN_SITE_API_FAILOVER_ORIGIN:
            raise ValueError("ESPN site mirror is reserved for bounded failover")
        endpoint_type = EndpointType.parse(endpoint)
        self.budget.admit(
            endpoint_type, competition_id=competition_id, event_id=event_id
        )
        body, record = self.raw_store.load(target)
        if record.endpoint != endpoint_type.value:
            raise RawTargetCorrupt(
                f"Raw ESPN endpoint mismatch: {record.endpoint} != {endpoint_type.value}"
            )
        return self._cached_result(target, endpoint_type, body, record, started=started)

    def fetch_json(
        self,
        url: str,
        endpoint: EndpointType | str,
        params: Optional[Params] = None,
        *,
        competition_id: object = None,
        event_id: object = None,
        force_refresh: bool = False,
    ) -> FetchResult:
        started = self.monotonic_fn()
        target = canonicalize_target(url, params)
        if _transport_origin(target.canonical_url) == ESPN_SITE_API_FAILOVER_ORIGIN:
            raise ValueError("ESPN site mirror is reserved for bounded failover")
        endpoint_type = EndpointType.parse(endpoint)
        try:
            self.budget.admit(
                endpoint_type, competition_id=competition_id, event_id=event_id
            )
        except BudgetExceeded as exc:
            entry = self._entry(
                target,
                endpoint_type,
                started=started,
                attempts=0,
                status=None,
                direct_bytes=0,
                disposition="budget_exhausted",
                error=str(exc),
            )
            exc.ledger_entry = entry
            raise

        if not force_refresh:
            try:
                body, record = self.raw_store.load(target)
                if record.endpoint != endpoint_type.value:
                    raise RawTargetCorrupt(
                        "raw endpoint metadata does not match request"
                    )
                return self._cached_result(
                    target, endpoint_type, body, record, started=started
                )
            except (RawTargetNotFound, RawTargetCorrupt):
                # A corrupt alias/blob is a cache miss. The store quarantines a
                # damaged content-addressed object before a same-hash rewrite.
                pass

        if self.circuit_is_open:
            entry = self._entry(
                target,
                endpoint_type,
                started=started,
                attempts=0,
                status=None,
                direct_bytes=0,
                disposition="circuit_open",
                error="five consecutive retryable failures",
            )
            raise CircuitOpen("ESPN transport circuit is open", ledger_entry=entry)

        attempts = 0
        status: Optional[int] = None
        total_direct_bytes = 0
        last_error = ""
        request_url = target.canonical_url
        failover_url = _official_site_failover_url(
            target,
            endpoint_type,
            enabled=self.allow_site_origin_failover,
            event_id=event_id,
        )
        failover_used = False
        last_transport_origin: Optional[str] = None
        while attempts < self.max_attempts:
            reservation = None
            try:
                reservation = self.budget.admit_request(self.response_cap_bytes)
            except BudgetExceeded as exc:
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=status,
                    direct_bytes=total_direct_bytes,
                    disposition="budget_exhausted",
                    error=str(exc),
                    transport_origin=last_transport_origin,
                )
                exc.ledger_entry = entry
                raise
            response = None
            try:
                self._rate_limiter.acquire()
                if self.request_permit is not None:
                    self.request_permit()
                attempts += 1
                last_transport_origin = _transport_origin(request_url)
                response = self.session.get(
                    request_url,
                    timeout=(self.connect_timeout, self.read_timeout),
                    stream=True,
                    allow_redirects=False,
                )
                status = int(response.status_code)
                if status in RETRYABLE_HTTP_STATUSES or 500 <= status <= 599:
                    body = b""
                elif not 200 <= status <= 299:
                    body = b""
                else:
                    body, direct_bytes = self._read_response(response, reservation)
                    total_direct_bytes += direct_bytes
            except _ResponseReadTimeout as exc:
                total_direct_bytes += exc.direct_bytes
                self._record_retryable_failure()
                status = None
                last_error = "timeout"
                if self.circuit_is_open or attempts >= self.max_attempts:
                    break
                self.sleep_fn(self._retry_delay(attempts, None))
                continue
            except _ResponseReadFailure as exc:
                total_direct_bytes += exc.direct_bytes
                self._record_nonretryable_or_success()
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=None,
                    direct_bytes=total_direct_bytes,
                    disposition="terminal_failure",
                    error=f"non-timeout transport error: {exc.error_type}",
                    transport_origin=last_transport_origin,
                )
                raise DirectTransportError(
                    "Non-retryable ESPN transport error: " + exc.error_type,
                    ledger_entry=entry,
                ) from None
            except (requests.Timeout, Urllib3ReadTimeoutError, TimeoutError):
                self._record_retryable_failure()
                status = None
                last_error = "timeout"
                if self.circuit_is_open or attempts >= self.max_attempts:
                    break
                self.sleep_fn(self._retry_delay(attempts, None))
                continue
            except _ReadLimitExceeded as exc:
                self._record_nonretryable_or_success()
                total_direct_bytes += exc.direct_bytes
                disposition = (
                    "budget_exhausted" if exc.task_budget else "response_too_large"
                )
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=status,
                    direct_bytes=total_direct_bytes,
                    disposition=disposition,
                    error=disposition,
                    transport_origin=last_transport_origin,
                )
                error_type = BudgetExceeded if exc.task_budget else ResponseTooLarge
                raise error_type(disposition, ledger_entry=entry) from exc
            except _ResponseEncodingError as exc:
                self._record_nonretryable_or_success()
                total_direct_bytes += exc.direct_bytes
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=status,
                    direct_bytes=total_direct_bytes,
                    disposition="terminal_failure",
                    error="invalid response encoding",
                    transport_origin=last_transport_origin,
                )
                raise DirectTransportError(
                    "Invalid ESPN response encoding", ledger_entry=entry
                ) from exc
            except BudgetExceeded as exc:
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=status,
                    direct_bytes=total_direct_bytes,
                    disposition="budget_exhausted",
                    error=str(exc),
                    transport_origin=last_transport_origin,
                )
                exc.ledger_entry = entry
                raise
            except (DirectTransportError, zlib.error) as exc:
                self._record_nonretryable_or_success()
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=status,
                    direct_bytes=total_direct_bytes,
                    disposition="terminal_failure",
                    error=f"invalid response encoding: {type(exc).__name__}",
                    transport_origin=last_transport_origin,
                )
                raise DirectTransportError(
                    "Invalid ESPN response encoding", ledger_entry=entry
                ) from exc
            except requests.RequestException as exc:
                self._record_nonretryable_or_success()
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=None,
                    direct_bytes=total_direct_bytes,
                    disposition="terminal_failure",
                    error=f"non-timeout transport error: {type(exc).__name__}",
                    transport_origin=last_transport_origin,
                )
                raise DirectTransportError(
                    "Non-retryable ESPN transport error: " + type(exc).__name__,
                    ledger_entry=entry,
                ) from None
            except Urllib3HTTPError as exc:
                self._record_nonretryable_or_success()
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=None,
                    direct_bytes=total_direct_bytes,
                    disposition="terminal_failure",
                    error=f"non-timeout transport error: {type(exc).__name__}",
                    transport_origin=last_transport_origin,
                )
                raise DirectTransportError(
                    "Non-retryable ESPN transport error: " + type(exc).__name__,
                    ledger_entry=entry,
                ) from None
            finally:
                if response is not None:
                    response.close()
                if reservation is not None:
                    reservation.release()

            if (
                status == 403
                and failover_url is not None
                and not failover_used
                and attempts < self.max_attempts
            ):
                self._record_nonretryable_or_success()
                request_url = failover_url
                failover_used = True
                continue

            if status in RETRYABLE_HTTP_STATUSES or 500 <= status <= 599:
                self._record_retryable_failure()
                last_error = f"retryable HTTP {status}"
                if self.circuit_is_open or attempts >= self.max_attempts:
                    break
                self.sleep_fn(
                    self._retry_delay(attempts, response.headers.get("Retry-After"))
                )
                continue

            if not 200 <= status <= 299:
                self._record_nonretryable_or_success()
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=status,
                    direct_bytes=total_direct_bytes,
                    disposition="terminal_http_failure",
                    error=f"non-retryable HTTP {status}",
                    transport_origin=last_transport_origin,
                )
                raise HttpStatusError(
                    status,
                    f"Non-retryable ESPN HTTP {status}",
                    ledger_entry=entry,
                )

            self._record_nonretryable_or_success()
            fetched_at = self.utcnow_fn().astimezone(timezone.utc).isoformat()
            try:
                record = self.raw_store.store(
                    target,
                    endpoint_type,
                    body,
                    fetched_at=fetched_at,
                    http_status=status,
                    direct_bytes=total_direct_bytes,
                    transport_origin=last_transport_origin,
                )
            except (RawStoreError, OSError) as exc:
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=status,
                    direct_bytes=total_direct_bytes,
                    disposition="raw_store_failure",
                    error=f"raw store failure: {type(exc).__name__}",
                    transport_origin=last_transport_origin,
                )
                raise DirectTransportError(
                    "ESPN raw response could not be committed", ledger_entry=entry
                ) from exc
            # Raw commit is deliberately complete before consumer parsing.
            try:
                data = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                entry = self._entry(
                    target,
                    endpoint_type,
                    started=started,
                    attempts=attempts,
                    status=status,
                    direct_bytes=total_direct_bytes,
                    disposition="invalid_json",
                    record=record,
                    error="response is not valid JSON",
                    transport_origin=last_transport_origin,
                )
                raise InvalidJsonError(
                    "ESPN response is not valid JSON", ledger_entry=entry
                ) from exc
            self._entry(
                target,
                endpoint_type,
                started=started,
                attempts=attempts,
                status=status,
                direct_bytes=total_direct_bytes,
                disposition="success",
                record=record,
                transport_origin=last_transport_origin,
            )
            return FetchResult(
                target=target,
                endpoint=endpoint_type,
                json_data=data,
                body=body,
                attempts=attempts,
                status=status,
                cache_hit=False,
                direct_bytes=total_direct_bytes,
                proxy_bytes=0,
                raw_uri=record.raw_uri,
                content_hash=record.content_hash,
                fetched_at=record.fetched_at,
                transport_origin=record.transport_origin,
            )

        entry = self._entry(
            target,
            endpoint_type,
            started=started,
            attempts=attempts,
            status=status,
            direct_bytes=total_direct_bytes,
            disposition="retry_exhausted",
            error=last_error,
            transport_origin=last_transport_origin,
        )
        raise RetryExhausted(
            f"ESPN retryable request failed after {attempts} attempts: {last_error}",
            ledger_entry=entry,
        )

    def _read_response(
        self, response, reservation: ByteReservation
    ) -> tuple[bytes, int]:
        headers = response.headers or {}
        raw_limit = reservation.limit
        content_length = headers.get("Content-Length")
        declared = None
        if content_length is not None:
            try:
                declared = int(content_length)
            except (TypeError, ValueError):
                declared = None
            if declared is not None and declared > raw_limit:
                raise _ReadLimitExceeded(
                    0, task_budget=raw_limit < self.response_cap_bytes
                )

        raw = response.raw
        if hasattr(raw, "decode_content"):
            raw.decode_content = False
        chunks = bytearray()
        while True:
            if declared is not None and len(chunks) == declared:
                break
            if len(chunks) >= raw_limit:
                raise _ReadLimitExceeded(
                    len(chunks), task_budget=raw_limit < self.response_cap_bytes
                )
            try:
                chunk = raw.read(min(64 * 1024, raw_limit - len(chunks)))
            except (requests.Timeout, Urllib3ReadTimeoutError, TimeoutError) as exc:
                raise _ResponseReadTimeout(len(chunks)) from exc
            except (requests.RequestException, Urllib3HTTPError, OSError) as exc:
                raise _ResponseReadFailure(len(chunks), type(exc).__name__) from None
            if not chunk:
                break
            if len(chunk) > raw_limit - len(chunks):
                raise _ReadLimitExceeded(
                    len(chunks), task_budget=raw_limit < self.response_cap_bytes
                )
            reservation.charge(len(chunk))
            chunks.extend(chunk)
        encoded = bytes(chunks)
        encoding = str(headers.get("Content-Encoding", "")).lower().strip()
        try:
            if encoding in {"gzip", "x-gzip"}:
                body = self._bounded_decompress(encoded, 16 + zlib.MAX_WBITS)
            elif encoding == "deflate":
                try:
                    body = self._bounded_decompress(encoded, zlib.MAX_WBITS)
                except zlib.error:
                    body = self._bounded_decompress(encoded, -zlib.MAX_WBITS)
            elif not encoding or encoding == "identity":
                body = encoded
            else:
                raise ValueError(f"unsupported Content-Encoding {encoding!r}")
        except (ValueError, zlib.error) as exc:
            raise _ResponseEncodingError(len(encoded), exc) from exc
        if len(body) > self.response_cap_bytes:
            raise _ReadLimitExceeded(len(encoded), task_budget=False)
        return body, len(encoded)

    def _bounded_decompress(self, encoded: bytes, window_bits: int) -> bytes:
        decoder = zlib.decompressobj(window_bits)
        body = decoder.decompress(encoded, self.response_cap_bytes + 1)
        if len(body) > self.response_cap_bytes or decoder.unconsumed_tail:
            raise _ReadLimitExceeded(len(encoded), task_budget=False)
        if not decoder.eof or decoder.unused_data:
            raise zlib.error("compressed payload is truncated or has trailing data")
        body += decoder.flush(self.response_cap_bytes + 1 - len(body))
        if len(body) > self.response_cap_bytes:
            raise _ReadLimitExceeded(len(encoded), task_budget=False)
        return body

    def _retry_delay(self, attempts: int, retry_after: Optional[str]) -> float:
        if retry_after is not None:
            value = str(retry_after).strip()
            if re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value):
                delay = float(value)
                if math.isfinite(delay):
                    return min(60.0, delay)
            else:
                try:
                    retry_at = parsedate_to_datetime(value)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    now = self.utcnow_fn()
                    if now.tzinfo is None:
                        now = now.replace(tzinfo=timezone.utc)
                    delay = (retry_at - now).total_seconds()
                    if math.isfinite(delay) and delay >= 0:
                        return min(60.0, delay)
                except (TypeError, ValueError, OverflowError):
                    pass
        return float(2 ** (attempts - 1))


__all__ = [
    "AmbientProxyError",
    "BudgetExceeded",
    "CanonicalTarget",
    "CircuitOpen",
    "DEFAULT_BURST",
    "DEFAULT_CONNECT_TIMEOUT",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_COMPETITIONS",
    "DEFAULT_MAX_REQUESTS",
    "DEFAULT_MAX_SUMMARY_EVENTS",
    "DEFAULT_MAX_TASK_BYTES",
    "DEFAULT_RATE_PER_MINUTE",
    "DEFAULT_READ_TIMEOUT",
    "DEFAULT_RESPONSE_CAP_BYTES",
    "DirectTransportError",
    "EndpointType",
    "EspnHttpClient",
    "EspnTransportError",
    "FetchResult",
    "HttpStatusError",
    "InvalidJsonError",
    "RequestLedgerEntry",
    "ResponseTooLarge",
    "RetryExhausted",
    "TaskBudget",
    "canonicalize_target",
]
