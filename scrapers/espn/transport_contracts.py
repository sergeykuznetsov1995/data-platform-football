"""Public value types, URL identity and task budgets for ESPN transport."""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Sequence, Union
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 20.0
DEFAULT_RESPONSE_CAP_BYTES = 16 * 1024 * 1024
DEFAULT_RATE_PER_MINUTE = 30
DEFAULT_BURST = 4
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_MAX_COMPETITIONS = 20
DEFAULT_MAX_SUMMARY_EVENTS = 100
DEFAULT_MAX_REQUESTS = 600
DEFAULT_MAX_TASK_BYTES = 64 * 1024 * 1024
ESPN_SITE_API_PRIMARY_ORIGIN = "https://site.api.espn.com"
ESPN_SITE_API_FAILOVER_ORIGIN = "https://site.web.api.espn.com"
ESPN_SITE_API_CAPTURE_ORIGINS = frozenset(
    {ESPN_SITE_API_PRIMARY_ORIGIN, ESPN_SITE_API_FAILOVER_ORIGIN}
)

ParamValue = Union[str, int, float, bool, None, Sequence[object]]
Params = Union[Mapping[str, ParamValue], Sequence[tuple[str, ParamValue]]]


def normalize_transport_origin(value: object) -> str:
    """Return a value-redacted canonical ESPN HTTPS origin."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError("transport_origin must be an ESPN HTTPS origin")
    parsed = urlsplit(value.strip())
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("transport_origin must be an ESPN HTTPS origin") from None
    hostname = (parsed.hostname or "").lower()
    if (
        parsed.scheme.lower() != "https"
        or not hostname
        or not (hostname == "espn.com" or hostname.endswith(".espn.com"))
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("transport_origin must be an ESPN HTTPS origin")
    return f"https://{hostname}"


class EndpointType(str, Enum):
    CATALOG = "catalog"
    SCOREBOARD = "scoreboard"
    SUMMARY = "summary"

    @classmethod
    def parse(cls, value: object) -> "EndpointType":
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ValueError(
                "ESPN endpoint type must be catalog, scoreboard or summary"
            )
        try:
            return cls(value.strip().lower())
        except ValueError as exc:
            raise ValueError(
                "ESPN endpoint type must be catalog, scoreboard or summary"
            ) from exc


@dataclass(frozen=True, slots=True)
class CanonicalTarget:
    canonical_url: str = field(repr=False)
    url_fingerprint: str

    @property
    def sanitized_url(self) -> str:
        parsed = urlsplit(self.canonical_url)
        keys = sorted(
            {key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}
        )
        query = "&".join(keys)
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))

    @property
    def target_key(self) -> str:
        return self.url_fingerprint


@dataclass(frozen=True, slots=True)
class RequestLedgerEntry:
    url_fingerprint: str
    endpoint: EndpointType
    attempts: int
    status: Optional[int]
    direct_bytes: int
    proxy_bytes: int
    latency_ms: float
    raw_uri: Optional[str]
    content_hash: Optional[str]
    disposition: str
    error: Optional[str] = None
    transport_origin: Optional[str] = None

    def __post_init__(self) -> None:
        if self.transport_origin is not None:
            object.__setattr__(
                self,
                "transport_origin",
                normalize_transport_origin(self.transport_origin),
            )

    @property
    def terminal_disposition(self) -> str:
        return self.disposition

    @property
    def raw_hash(self) -> Optional[str]:
        return self.content_hash


@dataclass(frozen=True, slots=True)
class FetchResult:
    target: CanonicalTarget
    endpoint: EndpointType
    json_data: Any
    body: bytes
    attempts: int
    status: int
    cache_hit: bool
    direct_bytes: int
    proxy_bytes: int
    raw_uri: str
    content_hash: str
    fetched_at: str
    transport_origin: Optional[str] = None

    def __post_init__(self) -> None:
        if self.transport_origin is not None:
            object.__setattr__(
                self,
                "transport_origin",
                normalize_transport_origin(self.transport_origin),
            )

    @property
    def data(self) -> Any:
        return self.json_data

    @property
    def http_status(self) -> int:
        return self.status

    @property
    def raw_hash(self) -> str:
        return self.content_hash

    @property
    def ok(self) -> bool:
        return 200 <= self.status <= 299


class EspnTransportError(RuntimeError):
    def __init__(self, message: str, *, ledger_entry=None) -> None:
        super().__init__(message)
        self.ledger_entry: Optional[RequestLedgerEntry] = ledger_entry


class AmbientProxyError(EspnTransportError):
    pass


class BudgetExceeded(EspnTransportError):
    pass


class CircuitOpen(EspnTransportError):
    pass


class ResponseTooLarge(EspnTransportError):
    pass


class RetryExhausted(EspnTransportError):
    pass


class HttpStatusError(EspnTransportError):
    def __init__(self, status: int, message: str, *, ledger_entry=None) -> None:
        super().__init__(message, ledger_entry=ledger_entry)
        self.status = status


class InvalidJsonError(EspnTransportError):
    pass


class DirectTransportError(EspnTransportError):
    pass


def _nonnegative_int(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


@dataclass(slots=True)
class TaskBudget:
    """Per-task hard limits and consumption shared by all logical fetches."""

    max_competitions: int = DEFAULT_MAX_COMPETITIONS
    max_summary_events: int = DEFAULT_MAX_SUMMARY_EVENTS
    max_requests: int = DEFAULT_MAX_REQUESTS
    max_bytes: int = DEFAULT_MAX_TASK_BYTES
    requests_used: int = field(default=0, init=False)
    bytes_used: int = field(default=0, init=False)
    bytes_reserved: int = field(default=0, init=False)
    _competition_ids: set[object] = field(default_factory=set, init=False, repr=False)
    _summary_event_ids: set[object] = field(default_factory=set, init=False, repr=False)
    _lock: threading.RLock = field(
        default_factory=threading.RLock, init=False, repr=False
    )

    def __post_init__(self) -> None:
        hard_maxima = {
            "max_competitions": DEFAULT_MAX_COMPETITIONS,
            "max_summary_events": DEFAULT_MAX_SUMMARY_EVENTS,
            "max_requests": DEFAULT_MAX_REQUESTS,
            "max_bytes": DEFAULT_MAX_TASK_BYTES,
        }
        for name, hard_maximum in hard_maxima.items():
            value = _nonnegative_int(getattr(self, name), name)
            if value > hard_maximum:
                raise ValueError(f"{name} cannot exceed {hard_maximum}")

    @property
    def competitions_used(self) -> int:
        return len(self._competition_ids)

    @property
    def summary_events_used(self) -> int:
        return len(self._summary_event_ids)

    @property
    def bytes_remaining(self) -> int:
        with self._lock:
            return self.max_bytes - self.bytes_used - self.bytes_reserved

    def admit(
        self,
        endpoint: EndpointType,
        *,
        competition_id: object = None,
        event_id: object = None,
    ) -> None:
        with self._lock:
            competitions = set(self._competition_ids)
            events = set(self._summary_event_ids)
            if competition_id is not None:
                competitions.add(competition_id)
            elif endpoint is EndpointType.SCOREBOARD:
                raise BudgetExceeded("scoreboard requests require competition_id")
            if endpoint is EndpointType.SUMMARY and event_id is not None:
                events.add(event_id)
            elif endpoint is EndpointType.SUMMARY:
                raise BudgetExceeded("Summary requests require event_id")
            if len(competitions) > self.max_competitions:
                raise BudgetExceeded(
                    f"ESPN competition budget exceeded ({self.max_competitions})"
                )
            if len(events) > self.max_summary_events:
                raise BudgetExceeded(
                    f"ESPN Summary event budget exceeded ({self.max_summary_events})"
                )
            self._competition_ids = competitions
            self._summary_event_ids = events

    def admit_request(
        self, response_cap_bytes: int = DEFAULT_RESPONSE_CAP_BYTES
    ) -> "ByteReservation":
        _nonnegative_int(response_cap_bytes, "response_cap_bytes")
        with self._lock:
            if self.requests_used >= self.max_requests:
                raise BudgetExceeded(
                    f"ESPN request budget exhausted ({self.max_requests})"
                )
            remaining = self.max_bytes - self.bytes_used - self.bytes_reserved
            if remaining <= 0:
                raise BudgetExceeded(f"ESPN byte budget exhausted ({self.max_bytes})")
            self.requests_used += 1
            reserved = min(response_cap_bytes, remaining)
            self.bytes_reserved += reserved
            return ByteReservation(self, reserved)

    def consume_bytes(self, count: int) -> None:
        """Compatibility API for non-reserved accounting; never overcommits."""
        _nonnegative_int(count, "response bytes")
        with self._lock:
            if self.bytes_used + self.bytes_reserved + count > self.max_bytes:
                raise BudgetExceeded(f"ESPN byte budget exceeded ({self.max_bytes})")
            self.bytes_used += count


class ByteReservation:
    """One response's exclusive byte allowance; unused bytes are releasable."""

    def __init__(self, budget: TaskBudget, limit: int) -> None:
        self.budget = budget
        self.limit = limit
        self.remaining = limit
        self._released = False

    def charge(self, count: int) -> None:
        _nonnegative_int(count, "response bytes")
        with self.budget._lock:
            if self._released or count > self.remaining:
                raise BudgetExceeded("ESPN response exceeded its reserved byte budget")
            self.remaining -= count
            self.budget.bytes_reserved -= count
            self.budget.bytes_used += count

    def release(self) -> None:
        with self.budget._lock:
            if not self._released:
                self.budget.bytes_reserved -= self.remaining
                self.remaining = 0
                self._released = True


def _iter_params(params: Optional[Params]) -> list[tuple[str, str]]:
    if params is None:
        return []
    source = params.items() if isinstance(params, Mapping) else params
    flattened: list[tuple[str, str]] = []
    for raw_key, raw_value in source:
        key = str(raw_key)
        if raw_value is None:
            continue
        if isinstance(raw_value, Sequence) and not isinstance(
            raw_value, (str, bytes, bytearray)
        ):
            flattened.extend((key, str(item)) for item in raw_value if item is not None)
        else:
            flattened.append((key, str(raw_value)))
    return flattened


def canonicalize_target(url: str, params: Optional[Params] = None) -> CanonicalTarget:
    candidate = str(url).strip()
    if not candidate:
        raise ValueError("ESPN URL must not be empty")
    parsed = urlsplit(candidate)
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("Invalid ESPN HTTPS URL") from None
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or not (
            parsed.hostname.lower() == "espn.com"
            or parsed.hostname.lower().endswith(".espn.com")
        )
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
    ):
        raise ValueError("ESPN transport requires a direct allowlisted HTTPS URL")
    query = parse_qsl(parsed.query, keep_blank_values=True)
    query.extend(_iter_params(params))
    query.sort(key=lambda pair: (pair[0], pair[1]))
    canonical_url = urlunsplit(
        (
            "https",
            (parsed.hostname or "").lower(),
            parsed.path or "/",
            urlencode(query),
            "",
        )
    )
    return CanonicalTarget(
        canonical_url=canonical_url,
        url_fingerprint=hashlib.sha256(canonical_url.encode("utf-8")).hexdigest(),
    )


__all__ = [
    "AmbientProxyError",
    "ByteReservation",
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
    "ESPN_SITE_API_CAPTURE_ORIGINS",
    "ESPN_SITE_API_FAILOVER_ORIGIN",
    "ESPN_SITE_API_PRIMARY_ORIGIN",
    "EspnTransportError",
    "FetchResult",
    "HttpStatusError",
    "InvalidJsonError",
    "Params",
    "RequestLedgerEntry",
    "ResponseTooLarge",
    "RetryExhausted",
    "TaskBudget",
    "_nonnegative_int",
    "canonicalize_target",
    "normalize_transport_origin",
]
