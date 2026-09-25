"""Public value types, origins and URL identity for ESPN transport."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Sequence, Union
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 20.0
DEFAULT_RESPONSE_CAP_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_ATTEMPTS = 4
# #1500: site.api is closed by Akamai for the parser User-Agent since 04.08,
# while site.web.api and core answer the same signature.  web.api is the
# primary site origin, site.api only a reserve probed once a day.  Discovery
# and lists use core only; core has no reserve.
ESPN_SITE_WEB_API_ORIGIN = "https://site.web.api.espn.com"
ESPN_SITE_API_ORIGIN = "https://site.api.espn.com"
ESPN_CORE_API_ORIGIN = "https://sports.core.api.espn.com"
ESPN_SITE_CLUSTER = ("site", ESPN_SITE_WEB_API_ORIGIN, ESPN_SITE_API_ORIGIN)
ESPN_CORE_CLUSTER = ("core", ESPN_CORE_API_ORIGIN, None)

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
    requested_at: Optional[str] = None
    host: Optional[str] = None
    lane: Optional[str] = None
    step: Optional[int] = None
    content_encoding: Optional[str] = None
    origin_attempts: tuple[tuple[str, Optional[int]], ...] = ()

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


class OriginBlocked(EspnTransportError):
    """403 on the chosen origin: it is closed, the request is deferred."""


class AllOriginsBlocked(OriginBlocked):
    """No origin of the cluster is open; the caller retries in a later wave."""


class LaneClosed(EspnTransportError):
    """The lane is frozen (auto-reset cooldown or all origins blocked)."""


class DailyCapExceeded(EspnTransportError):
    """The lane spent its daily request or byte cap."""


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
    "AllOriginsBlocked",
    "AmbientProxyError",
    "CanonicalTarget",
    "DEFAULT_CONNECT_TIMEOUT",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_READ_TIMEOUT",
    "DEFAULT_RESPONSE_CAP_BYTES",
    "DailyCapExceeded",
    "DirectTransportError",
    "EndpointType",
    "ESPN_CORE_API_ORIGIN",
    "ESPN_CORE_CLUSTER",
    "ESPN_SITE_API_ORIGIN",
    "ESPN_SITE_CLUSTER",
    "ESPN_SITE_WEB_API_ORIGIN",
    "EspnTransportError",
    "FetchResult",
    "HttpStatusError",
    "InvalidJsonError",
    "LaneClosed",
    "OriginBlocked",
    "Params",
    "RequestLedgerEntry",
    "ResponseTooLarge",
    "RetryExhausted",
    "_nonnegative_int",
    "canonicalize_target",
    "normalize_transport_origin",
]
