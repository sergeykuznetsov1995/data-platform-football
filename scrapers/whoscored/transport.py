"""Direct-only network transport for WhoScored (residential pool, no paid route).

The module deliberately owns *transport only*.  Parsers decide what valid
WhoScored content looks like and may pass a validator to :meth:`fetch`; a raw
store may be plugged in through :class:`RawCacheHook`, but this module does not
choose a storage backend.

The route order is fixed and fail-closed::

    raw cache -> direct curl (or an exact-bootstrap gate circuit) ->
    reusable direct FlareSolverr

The paid gateway route was removed with the WhoScored ceremony (#1487): when
every direct attempt is a Cloudflare challenge the last challenge is raised.
"""

from __future__ import annotations

import hashlib
import fcntl
import json
import logging
import math
import os
import random
import re
import stat
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field, replace
from enum import Enum
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import (
    Any,
    Callable,
    Mapping,
    NoReturn,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)
from urllib.parse import (
    parse_qsl,
    quote,
    unquote,
    urlencode,
    urljoin,
    urlsplit,
    urlunsplit,
)


from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrClient as _SharedFlareSolverrClient,
    FlareSolverrError,
    FlareSolverrErrorPage,
    FlareSolverrResponseTooLarge,
    FlareSolverrRuntimeIdentityError,
    FlareSolverrTabCrashed,
    FlareSolverrTimeout,
    MAX_XHR_BATCH_URLS,
    is_chromium_error_page,
)
from scrapers.whoscored.source_circuit import (
    CircuitPermit,
    SharedSourceCircuit,
    SourceCircuitError,
    SourceCircuitOpen,
)

logger = logging.getLogger(__name__)


class FlareSolverrClient(_SharedFlareSolverrClient):
    """WhoScored adapter retaining the browser's reported navigation URL.

    The common get() still owns request policy, runtime authority and traffic
    accounting. Only the URL it discards is retained for source validation.
    """

    def _post(
        self,
        payload: dict,
        timeout: Optional[float] = None,
        *,
        endpoint_path: str = "/v1",
    ) -> dict:
        data = super()._post(payload, timeout=timeout, endpoint_path=endpoint_path)
        if payload.get("cmd") == "request.get":
            solution = data.get("solution")
            self._source_final_url = (
                solution.get("url") if isinstance(solution, Mapping) else None
            )
        return data

    def get(
        self,
        url: str,
        session_id: str,
        max_timeout_ms: Optional[int] = None,
        return_only_cookies: bool = False,
        disable_media: bool = False,
    ) -> dict:
        self._source_final_url = None
        solution = super().get(
            url,
            session_id,
            max_timeout_ms=max_timeout_ms,
            return_only_cookies=return_only_cookies,
            disable_media=disable_media,
        )
        return {**solution, "url": self._source_final_url}


DEFAULT_DIRECT_BROWSER_ATTEMPTS = 4
DEFAULT_DIRECT_HTTP_RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_BROWSER_RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_BROWSER_RETRY_JITTER_SECONDS = 2.0
MAX_BROWSER_RETRY_BACKOFF_SECONDS = 30.0
SOURCE_CIRCUIT_PATH_ENV = "WHOSCORED_SOURCE_CIRCUIT_PATH"
SOURCE_CIRCUIT_WAIT_ENV = "WHOSCORED_SOURCE_CIRCUIT_WAIT"
SUPERVISOR_SESSION_OWNER_ENV = "WHOSCORED_SUPERVISOR_SESSION_OWNER"
SUPERVISOR_RESOURCE_LEDGER_ENV = "WHOSCORED_SUPERVISOR_RESOURCE_LEDGER_PATH"
CAPACITY_BROWSER_SESSION_OWNER_PATTERN = r"[a-z0-9]{16,32}"
_CAPACITY_BROWSER_SESSION_OWNER_RE = re.compile(
    rf"\A{CAPACITY_BROWSER_SESSION_OWNER_PATTERN}\Z",
    re.ASCII,
)


def capacity_browser_session_prefix(owner: str) -> str:
    """Return the isolated capacity-session prefix for one validated owner."""

    if (
        type(owner) is not str
        or _CAPACITY_BROWSER_SESSION_OWNER_RE.fullmatch(owner) is None
    ):
        raise ValueError(
            "browser_session_owner must be an exact str of 16..32 "
            "lowercase ASCII letters or digits"
        )
    return f"ws-cap-{owner}-"


class TransportRoute(str, Enum):
    RAW_CACHE = "raw_cache"
    DIRECT_HTTP = "direct_http"
    DIRECT_FLARESOLVERR = "direct_flaresolverr"


# Raised by the FlareSolverr WhoScored extension when the page's RequireJS
# site-header token is absent; a bootstrap reload does not recover it (#1017).
_SOURCE_STAGE_HEADER_UNAVAILABLE = "WhoScored page request header is unavailable."


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


class TransportPolicy(str, Enum):
    """Route policy recorded in reports; only direct_only exists (#1487)."""

    DIRECT_ONLY = "direct_only"

    @classmethod
    def parse(cls, value: object) -> "TransportPolicy":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip())
        except ValueError as exc:
            choices = ", ".join(policy.value for policy in cls)
            raise ValueError(f"transport_policy must be one of: {choices}") from exc


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
        redirect_location: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.url = url
        self.route = route
        self.status_code = status_code
        self.retryable = retryable
        self.redirect_location = redirect_location


class StageBootstrapRedirect(WhoScoredTransportError):
    """A stage bootstrap moved; its destination has not been verified."""


class CloudflareChallenge(WhoScoredTransportError):
    def __init__(
        self,
        message: str,
        *,
        url: str,
        route: TransportRoute,
        status_code: Optional[int] = None,
        source_wide: bool = False,
    ) -> None:
        super().__init__(
            message,
            kind=FailureKind.CLOUDFLARE,
            url=url,
            route=route,
            status_code=status_code,
            retryable=True,
        )
        self.source_wide = bool(source_wide)


class StageStatisticsUnavailable(WhoScoredTransportError):
    """A complete source statistics page does not publish a usable XHR token."""

    def __init__(self, response: TransportResponse) -> None:
        super().__init__(
            "WhoScored statistics page request header is unavailable",
            kind=FailureKind.CONTENT,
            url=response.url,
            route=response.route,
        )
        self.observed_at = response.observed_at


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
class TransportContext:
    """Stable request identity shared by Airflow, transport and proxy filter."""

    dag_id: str = ""
    run_id: str = ""
    task_id: str = ""
    map_index: int = -1
    try_number: int = 0
    scope: str = ""
    entity: str = ""
    transport_policy: str = TransportPolicy.DIRECT_ONLY.value

    @classmethod
    def from_env(cls) -> "TransportContext":
        def _integer(name: str, default: int) -> int:
            try:
                return int(os.environ.get(name, default))
            except (TypeError, ValueError):
                return default

        resolved_policy = TransportPolicy.parse(
            os.environ.get("WHOSCORED_TRANSPORT_POLICY", "").strip()
            or TransportPolicy.DIRECT_ONLY.value
        ).value
        return cls(
            dag_id=os.environ.get("AIRFLOW_CTX_DAG_ID", ""),
            run_id=os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", ""),
            task_id=os.environ.get("AIRFLOW_CTX_TASK_ID", ""),
            map_index=_integer("AIRFLOW_CTX_MAP_INDEX", -1),
            try_number=_integer("AIRFLOW_CTX_TRY_NUMBER", 0),
            scope=os.environ.get("WHOSCORED_SCOPE", ""),
            entity=os.environ.get("WHOSCORED_ENTITY", ""),
            transport_policy=resolved_policy,
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
            transport_policy=self.transport_policy,
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
            "transport_policy": self.transport_policy,
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
        flags = (
            os.O_APPEND
            | os.O_CREAT
            | os.O_WRONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        with self._lock:
            descriptor = os.open(self.path, flags, 0o600)
            try:
                metadata = os.fstat(descriptor)
                if not stat.S_ISREG(metadata.st_mode):
                    raise OSError("request ledger is not a regular file")
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


class ProxyUnavailable(RuntimeError):
    """The residential pool cannot carry traffic: stop the run (#1476).

    Deliberately not a ``WhoScoredTransportError``: the per-match handlers
    catch that base and would record a retryable failure row (and a higher
    ``attempt_no``) for every remaining candidate of a dead proxy.
    """


# curl: 7 = could not connect, 56 = CONNECT/recv failure, 97 = proxy handshake.
_PROXY_CURL_CODES = frozenset({7, 56, 97})
_PROXY_FAILURE_TEXT = re.compile(
    r"CONNECT tunnel failed|\b407\b|curl: \((?:7|56|97)\)",
    re.IGNORECASE,
)
# Egress probe through the current pool member; never a whoscored.com URL.
EGRESS_PROBE_URL = "https://api.ipify.org"


def _is_proxy_failure(exc: BaseException) -> bool:
    code = getattr(exc, "code", None)
    try:
        if code is not None and int(code) in _PROXY_CURL_CODES:
            return True
    except (TypeError, ValueError):
        pass
    return bool(_PROXY_FAILURE_TEXT.search(str(exc)))


def _is_tls_connect_failure(
    exc: Optional[BaseException], *, pooled: bool = False
) -> bool:
    """Recognise curl35 and, for pooled recovery only, certificate curl60."""
    if exc is None:
        return False
    code = getattr(exc, "code", None)
    if code is not None:
        try:
            return int(code) in ({35, 60} if pooled else {35})
        except (TypeError, ValueError):
            return False
    pattern = r"curl: \((?:35|60)\)" if pooled else r"curl: \(35\)"
    return bool(re.search(pattern, str(exc)))


def _is_schedule_month_url(url: str) -> bool:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https" or parsed.netloc != "www.whoscored.com"
        or parsed.fragment
        or not re.fullmatch(r"/tournaments/[1-9][0-9]*/data/?", parsed.path.lower())
    ):
        return False
    query = parse_qsl(parsed.query, keep_blank_values=True)
    return (
        len(query) == 1 and query[0][0] == "d"
        and re.fullmatch(r"[0-9]{4}(?:0[1-9]|1[0-2])", query[0][1]) is not None
        and int(query[0][1][:4]) > 0
    )


@dataclass(frozen=True)
class CachedPayload:
    """Raw cache value.  ``content`` must be the source response, not parsed data."""

    content: bytes
    status_code: int = 200
    headers: Mapping[str, str] = field(default_factory=dict)
    observed_at: Optional[str] = None


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
    observed_at: str = ""

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class TransportBudgets:
    """Per-transport hard limits."""

    max_response_bytes: int = 4 * 1024 * 1024


@dataclass
class TransportStats:
    route_requests: Counter[str] = field(default_factory=Counter)
    # 2xx network answers per route (raw-cache hits excluded, #1476).
    route_successes: Counter[str] = field(default_factory=Counter)
    route_wire_bytes: Counter[str] = field(default_factory=Counter)
    failures: Counter[str] = field(default_factory=Counter)
    cache_hits: int = 0
    cache_invalid: int = 0
    browser_sessions: int = 0
    browser_batches: int = 0
    browser_batch_items: int = 0
    stage_xhr_token: Counter[str] = field(default_factory=Counter)

    def as_dict(self) -> dict[str, Any]:
        return {
            "route_requests": dict(self.route_requests),
            "route_successes": dict(self.route_successes),
            "route_wire_bytes": dict(self.route_wire_bytes),
            "failures": dict(self.failures),
            "cache_hits": self.cache_hits,
            "cache_invalid": self.cache_invalid,
            "browser_sessions": self.browser_sessions,
            "browser_batches": self.browser_batches,
            "browser_batch_items": self.browser_batch_items,
            "stage_xhr_token": dict(self.stage_xhr_token),
        }


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
)
_WHOSCORED_STAGE_BOOTSTRAP_PATH = re.compile(
    r"\A/Regions/([1-9][0-9]*)/Tournaments/([1-9][0-9]*)/"
    r"Seasons/([1-9][0-9]*)/Stages/([1-9][0-9]*)/TeamStatistics"
    r"(?:/[^/]+)?\Z",
    re.IGNORECASE | re.ASCII,
)


# Source statistics paths (moved from the removed proxy_campaign module, #1487).
_INVALID_PATH_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_AMBIGUOUS_PATH_ESCAPE = re.compile(r"%(?:25|2e|2f|5c)", re.IGNORECASE)
_SOURCE_STATISTICS_PATH = re.compile(
    r"\A/regions/[1-9][0-9]*/tournaments/[1-9][0-9]*/"
    r"seasons/[1-9][0-9]*/stages/[1-9][0-9]*/teamstatistics"
    r"(?:/([^/]+))?\Z",
    re.IGNORECASE | re.ASCII,
)


def _is_canonical_target_path(value: object) -> bool:
    """Reject path spellings that an HTTP layer may normalize differently."""

    if not isinstance(value, str) or not value.startswith("/"):
        return False
    if (
        "//" in value
        or "\\" in value
        or _INVALID_PATH_ESCAPE.search(value) is not None
        # Encoded percent is rejected too: it can expose an encoded dot/slash
        # after a second decoding pass in a downstream proxy or origin.
        or _AMBIGUOUS_PATH_ESCAPE.search(value) is not None
        or any(part in (".", "..") for part in value.split("/"))
    ):
        return False
    return True


def canonical_stage_statistics_path(path: str) -> str | None:
    """Encode one source slug without normalizing route keywords or IDs."""
    if not _is_canonical_target_path(path):
        return None
    match = _SOURCE_STATISTICS_PATH.fullmatch(path)
    if match is None:
        return None
    slug = match.group(1)
    if slug is None:
        return path
    try:
        decoded = unquote(slug, encoding="utf-8", errors="strict")
        if not decoded or not decoded[0].isalnum() or any(
            not character.isalnum() and character not in "-_" for character in decoded
        ):
            return None
        return path[:match.start(1)] + quote(decoded, safe="-_")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return None


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
    if any(character.isspace() for character in url):
        return False
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return False
    return bool(
        parts.scheme.lower() == "https"
        and parts.hostname == "www.whoscored.com"
        and port is None
        and parts.username is None
        and parts.password is None
        and not parts.query
        and not parts.fragment
        and _WHOSCORED_STAGE_BOOTSTRAP_PATH.fullmatch(parts.path)
        and canonical_stage_statistics_path(parts.path) == parts.path
    )


class _StageStatisticsLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)


def stage_statistics_url_from_html(
    html: str,
    *,
    region_id: Optional[int],
    tournament_id: Optional[int],
    season_id: int,
    stage_id: int,
) -> Optional[str]:
    """Use a source navigation link only for the exact requested stage.

    Source pages supply lowercase paths with a season slug. Constructing a
    slugless bootstrap yields HTTP 301; following that redirect would weaken
    the existing source-token binding. Selecting the published link needs no
    extra request and keeps redirect following disabled.
    """
    source = _StageStatisticsLinks()
    source.feed(html)
    source.close()
    expected = tuple(str(value) for value in (region_id, tournament_id, season_id, stage_id))
    for href in source.links:
        try:
            url = urljoin("https://www.whoscored.com/", href)
            parts = urlsplit(url)
            path = canonical_stage_statistics_path(parts.path)
            if path is None:
                continue
            url = parts._replace(path=path).geturl()
            if not _is_whoscored_stage_bootstrap_url(url):
                continue
            match = _WHOSCORED_STAGE_BOOTSTRAP_PATH.fullmatch(urlsplit(url).path)
            if match is not None and match.groups() == expected:
                return url
        except ValueError:
            # Malformed or foreign anchors do not authorize another route.
            continue
    return None


class _StageStatisticsHTML(HTMLParser):
    """Read source markup only; scripts are never evaluated."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.scripts: list[str] = []
        self._script: Optional[list[str]] = None
        self.statistics = False
        self.closed_body = False
        self.closed_html = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag == "script":
            self._script = []
        if tag in {"div", "table", "section"}:
            element_id = dict(attrs).get("id") or ""
            if re.fullmatch(
                r"(?:team-|player-)?statistics(?:-[a-z0-9-]+)?", element_id
            ):
                self.statistics = True

    def handle_data(self, data: str) -> None:
        if self._script is not None:
            self._script.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._script is not None:
            self.scripts.append("".join(self._script))
            self._script = None
        if tag == "body":
            self.closed_body = True
        if tag == "html":
            self.closed_html = True


def _stage_statistics_source(response: TransportResponse) -> _StageStatisticsHTML:
    source = _StageStatisticsHTML()
    source.feed(response.text)
    source.close()
    if not (source.statistics and source.closed_body and source.closed_html):
        raise WhoScoredTransportError(
            "incomplete or unrecognised WhoScored statistics HTML",
            kind=FailureKind.CONTENT,
            url=response.url,
            route=response.route,
        )
    return source


def _stage_statistics_token(source: _StageStatisticsHTML) -> Optional[str]:
    # Preserve quoted literals while discarding JS comments. Accept only literal
    # assignments/object properties terminated by JS punctuation, never calls,
    # concatenations, escaped strings or executable expressions.
    lexeme = re.compile(
        r""""(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|`(?:\\.|[^`\\])*`|"""
        r"""//[^\r\n]*|/\*[\s\S]*?\*/|[A-Za-z_$][A-Za-z0-9_$]*|[^\s]"""
    )
    values: dict[str, list[str]] = {}
    for script in source.scripts:
        tokens = [
            match[0]
            for match in lexeme.finditer(script)
            if not match[0].startswith(("//", "/*"))
        ]
        for index in range(len(tokens) - 2):
            key = tokens[index]
            if key.startswith(('"', "'")):
                key = key[1:-1]
                if tokens[index + 1] != ":":
                    continue
            if key not in {"gSiteHeaderName", "gSiteHeaderValue"}:
                continue
            if tokens[index + 1] not in {"=", ":"}:
                continue
            value = re.fullmatch(r"""(["'])([^"'\\\r\n]*)\1""", tokens[index + 2])
            if value is None:
                continue
            if index + 3 < len(tokens) and tokens[index + 3] not in {",", ";", "}"}:
                continue
            values.setdefault(key, []).append(value[2])
    if values.get("gSiteHeaderName") == ["Model-last-Mode"]:
        tokens = values.get("gSiteHeaderValue", [])
        if len(tokens) == 1 and re.fullmatch(r"[A-Za-z0-9+/]{43}=", tokens[0]):
            return tokens[0]
    return None


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
    enables browser fallback.
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
    # A challenge requires both independently useful signals: a supported
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
        raw_cache: Optional[RawCacheHook] = None,
        budgets: Optional[TransportBudgets] = None,
        request_timeout: float = 30.0,
        browser_timeout_ms: int = 60_000,
        direct_http_attempts: int = 3,
        direct_http_retry_backoff_seconds: float = (
            DEFAULT_DIRECT_HTTP_RETRY_BACKOFF_SECONDS
        ),
        direct_browser_attempts: int = DEFAULT_DIRECT_BROWSER_ATTEMPTS,
        browser_retry_backoff_seconds: float = (DEFAULT_BROWSER_RETRY_BACKOFF_SECONDS),
        browser_retry_jitter_seconds: float = DEFAULT_BROWSER_RETRY_JITTER_SECONDS,
        impersonate: str = "chrome142",
        direct_http_session: Any = None,
        direct_fs_client: Optional[FlareSolverrClient] = None,
        http_session_factory: Optional[Callable[[Optional[str]], Any]] = None,
        context: Optional[TransportContext] = None,
        request_ledger: Optional[RequestLedger] = None,
        browser_session_ttl_seconds: int = 300,
        browser_session_max_requests: int = 96,
        browser_session_owner: Optional[str] = None,
        source_circuit: Optional[SharedSourceCircuit] = None,
        source_circuit_wait: Optional[bool] = None,
        transport_policy: Optional[TransportPolicy | str] = None,
    ) -> None:
        if direct_http_attempts < 1:
            raise ValueError("direct_http_attempts must be >= 1")
        if direct_browser_attempts < 1:
            raise ValueError("direct_browser_attempts must be >= 1")
        if (
            isinstance(direct_http_retry_backoff_seconds, bool)
            or not isinstance(direct_http_retry_backoff_seconds, (int, float))
            or not math.isfinite(float(direct_http_retry_backoff_seconds))
            or direct_http_retry_backoff_seconds < 0
        ):
            raise ValueError(
                "direct_http_retry_backoff_seconds must be finite and >= 0"
            )
        if (
            isinstance(browser_retry_backoff_seconds, bool)
            or not isinstance(browser_retry_backoff_seconds, (int, float))
            or not math.isfinite(float(browser_retry_backoff_seconds))
            or browser_retry_backoff_seconds < 0
        ):
            raise ValueError("browser_retry_backoff_seconds must be finite and >= 0")
        if (
            isinstance(browser_retry_jitter_seconds, bool)
            or not isinstance(browser_retry_jitter_seconds, (int, float))
            or not math.isfinite(float(browser_retry_jitter_seconds))
            or browser_retry_jitter_seconds < 0
        ):
            raise ValueError("browser_retry_jitter_seconds must be finite and >= 0")
        supervisor_owner = os.environ.get(SUPERVISOR_SESSION_OWNER_ENV, "").strip()
        supervisor_ledger_path = os.environ.get(
            SUPERVISOR_RESOURCE_LEDGER_ENV, ""
        ).strip()
        if bool(supervisor_owner) != bool(supervisor_ledger_path):
            raise ValueError(
                "supervised browser session owner and resource ledger must be "
                "configured together"
            )
        if supervisor_owner and browser_session_owner is not None:
            raise ValueError(
                "supervised and capacity browser session ownership cannot be mixed"
            )
        resolved_session_owner = supervisor_owner or browser_session_owner
        browser_session_prefix = (
            capacity_browser_session_prefix(resolved_session_owner)
            if resolved_session_owner is not None
            else None
        )
        if source_circuit_wait is None:
            wait_value = os.environ.get(SOURCE_CIRCUIT_WAIT_ENV, "0").strip()
            if wait_value not in {"0", "1"}:
                raise ValueError(f"{SOURCE_CIRCUIT_WAIT_ENV} must be 0 or 1")
            source_circuit_wait = wait_value == "1"
        elif type(source_circuit_wait) is not bool:
            raise ValueError("source_circuit_wait must be a boolean")
        circuit_path = os.environ.get(SOURCE_CIRCUIT_PATH_ENV, "").strip()
        if source_circuit is None and circuit_path:
            source_circuit = SharedSourceCircuit(circuit_path)
        if source_circuit_wait and source_circuit is None:
            raise ValueError(
                f"{SOURCE_CIRCUIT_WAIT_ENV}=1 requires {SOURCE_CIRCUIT_PATH_ENV}"
            )
        self.context = context or TransportContext.from_env()
        context_policy = TransportPolicy.parse(self.context.transport_policy)
        if transport_policy is not None:
            requested_policy = TransportPolicy.parse(transport_policy)
            if requested_policy is not context_policy:
                raise ValueError(
                    "transport_policy differs from the authenticated TransportContext"
                )
        resolved_policy = context_policy
        self.transport_policy = resolved_policy
        self.raw_cache = raw_cache
        self.budgets = budgets or TransportBudgets()
        self.request_timeout = request_timeout
        self.browser_timeout_ms = browser_timeout_ms
        self.direct_http_attempts = direct_http_attempts
        self.direct_http_retry_backoff_seconds = float(
            direct_http_retry_backoff_seconds
        )
        self.direct_browser_attempts = direct_browser_attempts
        self.browser_retry_backoff_seconds = float(browser_retry_backoff_seconds)
        self.browser_retry_jitter_seconds = float(browser_retry_jitter_seconds)
        self.impersonate = impersonate
        ledger_path = os.environ.get("WHOSCORED_REQUEST_LEDGER_PATH", "").strip()
        self.request_ledger = request_ledger or (
            JsonlRequestLedger(ledger_path) if ledger_path else None
        )
        self.browser_session_ttl_seconds = max(1, browser_session_ttl_seconds)
        self.browser_session_max_requests = max(1, browser_session_max_requests)
        self._capacity_browser_session_prefix = browser_session_prefix
        self._supervisor_session_owner = supervisor_owner
        self._supervisor_resource_ledger = (
            JsonlRequestLedger(supervisor_ledger_path)
            if supervisor_ledger_path
            else None
        )
        self._source_circuit = source_circuit
        self._source_circuit_wait = source_circuit_wait
        self._source_circuit_permit: Optional[CircuitPermit] = None
        self._http_session_factory = http_session_factory
        # Only a real direct_only transport (no injected session) must refuse
        # the silent host-IP fallback: WhoScored blocks that IP at Cloudflare.
        self._pool_manager: Any = None
        self._pool_proxy_url = self._resolve_pool_proxy_url(
            required=(
                direct_http_session is None and http_session_factory is None
            )
        )
        self._pool_member_ids = (
            {self._pool_proxy_url: 1} if self._pool_proxy_url else {}
        )
        self._direct_http = direct_http_session or self._new_http_session(
            self._pool_proxy_url
        )
        self._owns_direct_http_session = direct_http_session is None
        if direct_http_session is not None:
            # An injected session is not ours to replace on a member swap.
            self._pool_manager = None
        self._direct_fs = direct_fs_client or FlareSolverrClient(url=flaresolverr_url)
        self.stats = TransportStats()
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
        # Tokens are capabilities of the exact session + pool member + page.
        # Never put them on session.headers, where unrelated requests inherit them.
        self._stage_http_context: Optional[tuple[str, str, Any, Optional[str], str]] = (
            None
        )


    def _begin_source_operation(self) -> None:
        """Finish an unresolved prior probe before a new public operation."""

        if self._source_circuit is None or self._source_circuit_permit is None:
            return
        permit = self._source_circuit_permit
        self._source_circuit_permit = None
        try:
            self._source_circuit.inconclusive(permit)
        except SourceCircuitError as exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit could not preserve an uncertain probe",
                kind=FailureKind.CONFIG,
                url="https://www.whoscored.com/",
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc

    def _admit_structured_source(self, url: str) -> None:
        if (
            self._source_circuit is None
            or self._source_circuit_permit is not None
            or not (
                _is_whoscored_structured_feed_url(url)
                or _is_whoscored_stage_bootstrap_url(url)
            )
        ):
            return
        try:
            self._source_circuit_permit = self._source_circuit.admit(
                wait=self._source_circuit_wait
            )
        except SourceCircuitOpen as exc:
            raise CloudflareChallenge(
                "WhoScored source cooldown is active",
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
                source_wide=True,
            ) from exc
        except SourceCircuitError as exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit state is unavailable",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc

    def _source_probe_active(self) -> bool:
        return bool(
            self._source_circuit_permit is not None
            and self._source_circuit_permit.is_probe
        )

    def _source_succeeded(self, url: str) -> None:
        if (
            self._source_circuit is None
            or self._source_circuit_permit is None
            or not (
                _is_whoscored_structured_feed_url(url)
                or _is_whoscored_stage_bootstrap_url(url)
            )
        ):
            return
        permit = self._source_circuit_permit
        self._source_circuit_permit = None
        try:
            self._source_circuit.succeed(permit)
        except SourceCircuitError as exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit could not record a successful probe",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc

    def _source_tripped(self, exc: CloudflareChallenge) -> None:
        if (
            not exc.source_wide
            or self._source_circuit is None
            or self._source_circuit_permit is None
        ):
            return
        permit = self._source_circuit_permit
        self._source_circuit_permit = None
        try:
            self._source_circuit.trip(permit)
        except SourceCircuitError as circuit_exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit could not record a source block",
                kind=FailureKind.CONFIG,
                url=exc.url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from circuit_exc

    def _source_inconclusive(self, url: str) -> bool:
        """Reopen one uncertain half-open probe without escalating it."""

        if (
            self._source_circuit is None
            or self._source_circuit_permit is None
            or not self._source_circuit_permit.is_probe
            or not (
                _is_whoscored_structured_feed_url(url)
                or _is_whoscored_stage_bootstrap_url(url)
            )
        ):
            return False
        permit = self._source_circuit_permit
        self._source_circuit_permit = None
        try:
            self._source_circuit.inconclusive(permit)
        except SourceCircuitError as exc:
            raise WhoScoredTransportError(
                "WhoScored source circuit could not preserve an uncertain probe",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        return True

    def _wait_before_browser_retry(self, attempt: int) -> None:
        """Apply one bounded exponential delay before a physical retry."""

        if self.browser_retry_backoff_seconds == 0:
            return
        multiplier = 2 ** min(max(0, int(attempt)), 10)
        base_delay = min(
            self.browser_retry_backoff_seconds * multiplier,
            MAX_BROWSER_RETRY_BACKOFF_SECONDS,
        )
        jitter = (
            random.uniform(
                0.0,
                min(
                    self.browser_retry_jitter_seconds,
                    MAX_BROWSER_RETRY_BACKOFF_SECONDS,
                ),
            )
            if self.browser_retry_jitter_seconds
            else 0.0
        )
        delay = min(base_delay + jitter, MAX_BROWSER_RETRY_BACKOFF_SECONDS)
        time.sleep(delay)

    def _wait_before_direct_http_retry(self, attempt: int) -> None:
        """Space transient origin retries so one short 5xx wave can clear."""

        if self.direct_http_retry_backoff_seconds == 0:
            return
        multiplier = 2 ** min(max(0, int(attempt)), 10)
        delay = min(
            self.direct_http_retry_backoff_seconds * multiplier,
            MAX_BROWSER_RETRY_BACKOFF_SECONDS,
        )
        time.sleep(delay)

    def _resolve_pool_proxy_url(self, *, required: bool = False) -> Optional[str]:
        """Residential-pool egress for the direct route.

        WhoScored blocks the datacentre host IP at Cloudflare while the same
        region's residential pool passes.  When ``WHOSCORED_PROXY_FILE`` points
        at a ``host:port:user:pass`` pool the direct curl and direct
        FlareSolverr routes egress through one pool member chosen once per
        process (sticky residential, matching the platform's per-task proxy
        pattern).  Unset/empty keeps the legacy host-IP direct route only for
        injected test sessions; a ``required`` (real direct_only) transport
        fails before any source request instead.
        """
        path = os.environ.get("WHOSCORED_PROXY_FILE", "").strip()
        if not path:
            if required:
                raise ValueError(
                    "WHOSCORED_PROXY_FILE is not set: direct_only refuses to "
                    "egress from the host IP"
                )
            return None
        from scrapers.utils.proxy_manager import ProxyManager

        manager = ProxyManager(rotation_strategy="random")
        try:
            manager.load_from_file_custom_format(path)
        except OSError as exc:
            raise ValueError(
                f"WHOSCORED_PROXY_FILE is not readable: {type(exc).__name__}"
            ) from exc
        worker = os.environ.get("WHOSCORED_PROXY_WORKER", "").strip()
        if worker:
            # Parallel history workers own disjoint pool lines, so a later
            # rotation can never move onto a neighbour's address (#1481).
            index, _, count = worker.partition("/")
            if not (index.isdigit() and count.isdigit() and int(index) < int(count)):
                raise ValueError("WHOSCORED_PROXY_WORKER must be INDEX/COUNT")
            manager._proxies = [proxy for line, proxy in enumerate(manager._proxies)
                                if line % int(count) == int(index)]
        member = os.environ.get("WHOSCORED_PROXY_MEMBER", "").strip()
        if member.isdigit() and manager.total_count:
            proxy_url = manager._proxies[int(member) % manager.total_count].http_url
        else:
            proxy_url = manager.get_http_proxy_url()
        if proxy_url is None and required:
            raise ValueError("WHOSCORED_PROXY_FILE has no usable pool member")
        if proxy_url is not None:
            self._pool_manager = manager
        return proxy_url

    def _is_pool_session(self, session: Any) -> bool:
        return self._pool_manager is not None and session is self._direct_http

    def _renew_schedule_http_session(self, url: str) -> None:
        """Discard a failed TLS handle without rotating its sticky member."""
        if not self._owns_direct_http_session:
            return
        previous = self._direct_http
        replacement = None
        try:
            replacement = self._new_http_session(self._pool_proxy_url)
            if replacement is None or replacement is previous:
                raise ValueError("HTTP session factory did not create a new session")
            for name in ("headers", "cookies"):
                old = getattr(previous, name, None)
                new = getattr(replacement, name, None)
                if old is not None and new is not None:
                    new.update(old)
        except Exception as exc:
            if replacement is not None and replacement is not previous:
                close = getattr(replacement, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        pass
            raise WhoScoredTransportError(
                f"Could not renew schedule HTTP session ({type(exc).__name__})",
                kind=FailureKind.CONFIG,
                url=url,
                route=TransportRoute.DIRECT_HTTP,
            ) from None
        self._direct_http = replacement
        self._stage_http_context = None
        close = getattr(previous, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:
                logger.debug("Could not close TLS session (%s)", type(exc).__name__)
        logger.warning("WhoScored schedule TLS session renewed on the same pool member")

    def _rotate_pool_proxy(self, excluded: Optional[set[str]] = None) -> bool:
        """Swap the sticky pool member for a different one (#1476).

        Exclusions belong to one recovery operation and never leave memory.
        Returns ``False`` when no eligible member remains.
        """
        manager = self._pool_manager
        if manager is None:
            return False
        current = self._pool_proxy_url
        excluded_urls = set(excluded or ())
        if current is not None:
            excluded_urls.add(current)
        replacement = manager.get_http_proxy_url(excluded_http_urls=excluded_urls)
        if replacement is None:
            return False
        previous = self._direct_http
        self._pool_proxy_url = replacement
        self._pool_member_ids.setdefault(replacement, len(self._pool_member_ids) + 1)
        self._direct_http = self._new_http_session(replacement)
        self._stage_http_context = None
        # A browser session keeps the member it was opened with.
        self._drop_browser_session(self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR)
        close = getattr(previous, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:  # best-effort cleanup of the old member
                logger.debug(
                    "Could not close replaced pool session (%s)",
                    type(exc).__name__,
                )
        logger.warning("WhoScored pool member replaced after a proxy failure")
        return True

    def probe_egress(self) -> None:
        """Check ipify on at most ten distinct pool members before work.

        Authentication failures keep the previous two-member budget. Injected
        sessions without a residential pool retain the no-op behavior.
        """
        if self._pool_manager is not None:
            self._probe_pool_egress(set(), replace_first=False)

    def _probe_pool_egress(self, tested: set[str], *, replace_first: bool) -> None:
        # A failed source request already consumed the first member when this
        # helper is called by _http_fetch. The remaining checks share its cap.
        limit = min(self._pool_manager.total_count, 10)
        replaced = False
        detail = "no replacement checked"
        reason = "budget_exhausted"
        while len(tested) < limit:
            if replace_first:
                if not self._rotate_pool_proxy(tested):
                    reason = "replacement_unavailable"
                    break
                replaced = True
            member = self._pool_proxy_url
            if member is None or member in tested:
                reason = "replacement_unavailable"
                break
            tested.add(member)
            started = time.monotonic()
            success = False
            authorization = False
            transient = False
            try:
                raw = self._direct_http.get(
                    EGRESS_PROBE_URL, timeout=min(float(self.request_timeout), 30.0)
                )
                status = int(raw.status_code)
                detail = f"HTTP {status}"
                success = 200 <= status < 300
                authorization = status == 407
                transient = status in {502, 503, 504}
            except Exception as exc:
                # Never expose proxy URLs, credentials or raw exception text.
                detail = type(exc).__name__
                connect_status = re.search(
                    r"CONNECT tunnel failed, response (\d{3})\b", str(exc), re.IGNORECASE
                )
                if connect_status:
                    detail += f" CONNECT {connect_status.group(1)}"
                authorization = bool(re.search(r"\b407\b", str(exc)))
                transient = (
                    _is_proxy_failure(exc)
                    or _is_tls_connect_failure(exc, pooled=True)
                    # curl_cffi Timeout is an OSError, not built-in TimeoutError.
                    # Code 28 is transient here; source timeouts keep their policy.
                    or getattr(exc, "code", None) == 28
                    or isinstance(exc, (ConnectionError, TimeoutError))
                ) and not authorization
            log_probe = logger.info if success else logger.warning
            log_probe(
                "WhoScored egress probe attempt=%d member=%d duration_ms=%d "
                "result=%s replaced=%s",
                len(tested), self._pool_member_ids[member],
                int((time.monotonic() - started) * 1000), detail, replaced,
            )
            if success:
                logger.info(
                    "WhoScored egress recovery members=%d replacement_succeeded=%s reason=success",
                    len(tested), replaced,
                )
                return
            if authorization or not transient:
                limit = min(limit, 2)
                reason = "authorization" if authorization else "budget_exhausted"
            replace_first = True
        logger.warning(
            "WhoScored egress recovery members=%d replacement_succeeded=False reason=%s",
            len(tested), reason,
        )
        raise ProxyUnavailable(
            f"WhoScored access check exhausted: members={len(tested)} "
            f"reason={reason}; {detail}"
        ) from None

    def _new_http_session(self, proxy_url: Optional[str]) -> Any:
        if self._http_session_factory is not None:
            return self._http_session_factory(proxy_url)
        try:
            from curl_cffi.requests import Session
        except ImportError as exc:  # pragma: no cover - production dependency
            raise RuntimeError("curl_cffi is required for WhoScoredTransport") from exc
        # A route labelled ``direct`` must never inherit HTTP(S)_PROXY from the
        # worker environment.
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

    def _record_supervisor_session(self, event: str, session_id: str) -> None:
        ledger = self._supervisor_resource_ledger
        if ledger is None:
            return
        prefix = capacity_browser_session_prefix(self._supervisor_session_owner)
        if event not in {"owned", "released"} or not session_id.startswith(prefix):
            raise RuntimeError("invalid supervised browser session lifecycle event")
        ledger.append(
            {
                "schema_version": 1,
                "event": event,
                "resource": "flaresolverr_session",
                "owner": self._supervisor_session_owner,
                "session_id": session_id,
                "dag_id": self.context.dag_id,
                "run_id": self.context.run_id,
                "task_id": self.context.task_id,
                "try_number": self.context.try_number,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
        )

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
        suffix = f"{route.value}-{uuid.uuid4().hex[:10]}"
        session_id = (
            f"{self._capacity_browser_session_prefix}{suffix}"
            if self._capacity_browser_session_prefix is not None
            else f"ws-{suffix}"
        )
        created = _BrowserSession(
            session_id=session_id,
            proxy_url=proxy_url,
            created_at=now,
        )
        # Track ownership before the external side effect.  A signal can then
        # interrupt create or local accounting without losing the only id that
        # close() needs for an idempotent server-side destroy.
        self._browser_sessions[route] = created
        try:
            self._record_supervisor_session("owned", session_id)
        except BaseException:
            self._browser_sessions.pop(route, None)
            raise
        try:
            client.create_session(session_id, proxy_url=proxy_url)
            self.stats.browser_sessions += 1
        except BaseException:
            # Destroy even when create never reached the server; the endpoint
            # is idempotent.  A BaseException from cleanup deliberately keeps
            # tracking intact so the outer termination close can retry it.
            self._drop_browser_session(client, route)
            raise
        return created

    def _drop_browser_session(
        self, client: FlareSolverrClient, route: TransportRoute
    ) -> None:
        existing = self._browser_sessions.get(route)
        if existing is not None:
            if self._supervisor_resource_ledger is not None:
                try:
                    strict_destroy = getattr(client, "destroy_session_strict")
                    strict_destroy(existing.session_id)
                    self._record_supervisor_session("released", existing.session_id)
                except Exception as exc:
                    # Keep ownership durable and locally tracked. The parent
                    # process-group supervisor retries exact cleanup after the
                    # runner exits or is killed.
                    logger.error(
                        "Could not release supervised WhoScored browser session "
                        "for route %s (%s)",
                        route.value,
                        type(exc).__name__,
                    )
                    return
                self._browser_sessions.pop(route, None)
                return
            try:
                client.destroy_session(existing.session_id)
            except Exception as exc:  # destroy is best-effort and idempotent
                logger.debug(
                    "Could not destroy WhoScored browser session for route %s (%s)",
                    route.value,
                    type(exc).__name__,
                )
            # An ordinary destroy failure remains best-effort, but a
            # BaseException skips this line and keeps the id tracked so the
            # termination unwind can retry the idempotent destroy in close().
            if self._browser_sessions.get(route) is existing:
                self._browser_sessions.pop(route, None)

    def _replay_browser_identity(self, solution: Mapping[str, Any]) -> None:
        """Replay solved CF cookies into direct HTTP when the session supports it."""
        self._stage_http_context = None
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

    def prepare_stage_statistics(
        self,
        bootstrap_url: str,
        *,
        cache_key: str,
        before_network: Optional[Callable[[], None]] = None,
        scope: Optional[str] = None,
        entity: Optional[str] = None,
    ) -> TransportResponse:
        """Cache-first bootstrap, preserving ordinary-page absence as evidence."""
        if not _is_whoscored_stage_bootstrap_url(bootstrap_url):
            raise WhoScoredTransportError(
                "statistics bootstrap URL is invalid",
                kind=FailureKind.CONFIG,
                url=bootstrap_url,
            )
        # Clear stale capabilities before preparing a different page, including
        # a page that later fails validation or has no token.
        previous = self._stage_http_context
        if previous is not None and previous[0] != bootstrap_url:
            self._stage_http_context = None
        try:
            response = self.fetch(
                bootstrap_url,
                cache_key=cache_key,
                validator=lambda response: bool(_stage_statistics_source(response)),
                before_network=before_network,
                scope=scope,
                entity=entity,
            )
        except BaseException as exc:
            self._stage_http_context = None
            if (
                isinstance(exc, WhoScoredTransportError)
                and exc.kind is FailureKind.HTTP_STATUS
                and exc.status_code in {301, 302, 303, 307, 308}
            ):
                raise StageBootstrapRedirect(
                    str(exc), kind=exc.kind, url=exc.url, route=exc.route,
                    status_code=exc.status_code,
                    redirect_location=exc.redirect_location,
                ) from exc
            raise
        token = _stage_statistics_token(_stage_statistics_source(response))
        if token is None:
            self._stage_http_context = None
            self.stats.stage_xhr_token["source_unavailable"] += 1
            raise StageStatisticsUnavailable(response)
        self.stats.stage_xhr_token["available"] += 1
        if response.route in {
            TransportRoute.DIRECT_HTTP,
            TransportRoute.DIRECT_FLARESOLVERR,
        }:
            self._stage_http_context = (
                bootstrap_url,
                token,
                self._direct_http,
                self._pool_proxy_url,
                response.sha256,
            )
        elif (
            response.route is not TransportRoute.RAW_CACHE
            or previous is None
            or previous != self._stage_http_context
            or previous[4] != response.sha256
            or previous[2] is not self._direct_http
            or previous[3] != self._pool_proxy_url
        ):
            # Raw HTML retains source evidence, but has no cookies belonging to
            # a fresh session. Let the normal browser fallback establish them.
            self._stage_http_context = None
        self._direct_gate_circuits.discard(_canonical_url_key(bootstrap_url))
        return response

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
        self._begin_source_operation()
        self._admit_structured_source(url)
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
                before_network=before_network,
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
            if acquire_first_token:
                self._admit_structured_source(url)
                if before_network is not None:
                    before_network()
            for attempt in range(self.direct_http_attempts):
                try:
                    return _direct_once()
                except CloudflareChallenge:
                    # Cloudflare is route evidence, not a transient ordinary
                    # HTTP failure. Move to the direct browser immediately.
                    raise
                except WhoScoredTransportError as exc:
                    if self._source_inconclusive(url):
                        raise
                    retryable_direct = exc.retryable and exc.kind in {
                        FailureKind.HTTP_STATUS,
                        FailureKind.TIMEOUT,
                    }
                    if not retryable_direct or attempt + 1 >= self.direct_http_attempts:
                        raise
                    if (
                        _is_tls_connect_failure(
                            exc.__cause__, pooled=self._is_pool_session(self._direct_http)
                        )
                        and _is_schedule_month_url(url)
                    ):
                        self._renew_schedule_http_session(url)
                    self._wait_before_direct_http_retry(attempt)
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
                stored = self._store_and_return(key, direct)
                self._source_succeeded(url)
                return stored

        last_cf: Optional[CloudflareChallenge] = None
        last_transient: Optional[WhoScoredTransportError] = None
        for attempt in range(self.direct_browser_attempts):
            try:
                browser = self._browser_fetch(
                    url,
                    client=self._direct_fs,
                    route=TransportRoute.DIRECT_FLARESOLVERR,
                    proxy_url=self._pool_proxy_url,
                    bootstrap_url=browser_bootstrap_url,
                )
                self._validate(browser, validator)
            except CloudflareChallenge as exc:
                self._drop_browser_session(
                    self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                )
                last_cf = exc
                self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                self._source_tripped(exc)
                if not exc.source_wide and self._source_inconclusive(url):
                    raise
                if (
                    exc.source_wide
                    and self._source_circuit is not None
                    and not self._source_circuit_wait
                ):
                    raise
                if attempt + 1 < self.direct_browser_attempts:
                    if (
                        exc.source_wide
                        and self._source_circuit is not None
                        and self._source_circuit_wait
                    ):
                        self._admit_structured_source(url)
                    else:
                        self._wait_before_browser_retry(attempt)
                    if before_network is not None:
                        before_network()
                continue
            except WhoScoredTransportError as exc:
                # A tab crash, timeout, ordinary browser error or parser
                # rejection must never silently buy residential bandwidth.
                if direct_gate_key is not None:
                    self._direct_gate_circuits.discard(direct_gate_key)
                if self._source_inconclusive(url):
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    if exc.kind is FailureKind.CONTENT:
                        self._store_response(key, browser)
                    raise
                if (
                    exc.retryable
                    and exc.kind
                    in {
                        FailureKind.HTTP_STATUS,
                        FailureKind.TIMEOUT,
                        FailureKind.BROWSER,
                    }
                    and attempt + 1 < self.direct_browser_attempts
                ):
                    last_transient = exc
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    self._wait_before_browser_retry(attempt)
                    if before_network is not None:
                        before_network()
                    continue
                if exc.kind is FailureKind.CONTENT:
                    self._store_response(key, browser)
                raise
            stored = self._store_and_return(key, browser)
            self._source_succeeded(url)
            return stored

        # Any non-CF browser outcome wins over a later challenge.
        if last_transient is not None:
            raise last_transient
        assert last_cf is not None
        raise last_cf

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
        # known-useless duplicate direct requests.
        misses: list[tuple[int, FetchRequest, bool]] = []
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
            misses.append((index, item, cache_entry_was_invalid))

        if not misses:
            return [response for response in results if response is not None]

        # An unresolved prior half-open probe is settled only after every raw
        # object has had its chance to satisfy this logical batch.  Circuit
        # state can therefore never block a complete warm-cache replay.
        self._begin_source_operation()
        for index, item, cache_entry_was_invalid in misses:
            self._activate_request(
                cache_key=item.cache_key,
                scope=item.scope,
                entity=item.entity,
                request_id=request_ids[item.cache_key],
            )
            self._admit_structured_source(item.url)
            if (
                not cache_entry_was_invalid
                and gate_key in self._direct_gate_circuits
            ):
                gated.append((index, item, False))
                continue
            if item.before_network is not None:
                item.before_network()
            for attempt in range(self.direct_http_attempts):
                direct: Optional[TransportResponse] = None
                try:
                    direct = self._http_fetch(
                        item.url,
                        session=self._direct_http,
                        route=TransportRoute.DIRECT_HTTP,
                        referer=bootstrap_url,
                        before_network=item.before_network,
                    )
                    self._validate(direct, item.validator)
                except CloudflareChallenge:
                    self.stats.failures[FailureKind.CLOUDFLARE.value] += 1
                    self._direct_gate_circuits.add(gate_key)
                    gated.append((index, item, True))
                    break
                except WhoScoredTransportError as exc:
                    if exc.kind is FailureKind.CONTENT:
                        assert direct is not None
                        self._store_response(item.cache_key, direct)
                    if self._source_inconclusive(item.url):
                        self._direct_gate_circuits.discard(gate_key)
                        raise
                    retryable_direct = exc.retryable and exc.kind in {
                        FailureKind.HTTP_STATUS,
                        FailureKind.TIMEOUT,
                    }
                    if retryable_direct and attempt + 1 < self.direct_http_attempts:
                        self._wait_before_direct_http_retry(attempt)
                        if item.before_network is not None:
                            item.before_network()
                        continue
                    self._direct_gate_circuits.discard(gate_key)
                    raise
                else:
                    assert direct is not None
                    self._direct_gate_circuits.discard(gate_key)
                    self._store_response(item.cache_key, direct)
                    results[index] = direct
                    self._source_succeeded(item.url)
                    break

        offset = 0
        while offset < len(gated):
            if self._source_circuit_permit is None:
                self._admit_structured_source(gated[offset][1].url)
            batch_size = 1 if self._source_probe_active() else MAX_XHR_BATCH_URLS
            pending = gated[offset : offset + batch_size]
            offset += len(pending)
            last_cf: Optional[CloudflareChallenge] = None
            last_item_errors: dict[str, WhoScoredTransportError] = {}
            non_cf_evidence: dict[str, WhoScoredTransportError] = {}
            cf_failure_counts: Counter[str] = Counter()
            rechunk_after_cooldown: list[tuple[int, FetchRequest, bool]] = []
            browser_attempts = range(self.direct_browser_attempts)
            for attempt in browser_attempts:
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
                        proxy_url=self._pool_proxy_url,
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
                    pending = [(index, item, False) for index, item, _ in pending]
                    self._source_tripped(exc)
                    if not exc.source_wide and self._source_inconclusive(
                        pending[0][1].url
                    ):
                        raise
                    if (
                        exc.source_wide
                        and self._source_circuit is not None
                            and not self._source_circuit_wait
                    ):
                        raise
                    if (
                        exc.source_wide
                        and self._source_circuit is not None
                        and self._source_circuit_wait
                    ):
                        rechunk_after_cooldown = list(pending)
                        pending = []
                        break
                    if attempt + 1 < self.direct_browser_attempts:
                        if not (
                            exc.source_wide
                            and self._source_circuit is not None
                            and self._source_circuit_wait
                        ):
                            self._wait_before_browser_retry(attempt)
                    continue
                except WhoScoredTransportError as exc:
                    # Endpoint-level timeout/protocol failures do not produce
                    # per-item outcomes. Retry only explicitly transient
                    # browser/bootstrap failures; retain them as non-CF
                    # evidence ahead of a later challenge.
                    self._direct_gate_circuits.discard(gate_key)
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    if not exc.retryable:
                        self._source_inconclusive(pending[0][1].url)
                        raise
                    for _, item, _ in pending:
                        last_item_errors[item.cache_key] = exc
                        non_cf_evidence[item.cache_key] = exc
                    pending = [(index, item, False) for index, item, _ in pending]
                    if self._source_inconclusive(pending[0][1].url):
                        raise
                    if attempt + 1 < self.direct_browser_attempts:
                        self._wait_before_browser_retry(attempt)
                    continue

                retry: list[tuple[int, FetchRequest, bool]] = []
                terminal_error: Optional[WhoScoredTransportError] = None
                source_wide_cf: Optional[CloudflareChallenge] = None
                for (index, item, network_gate_acquired), outcome in zip(
                    pending, browser_outcomes
                ):
                    if outcome.error is not None:
                        last_item_errors[item.cache_key] = outcome.error
                        non_cf_evidence[item.cache_key] = outcome.error
                        if outcome.error.retryable:
                            retry.append((index, item, False))
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
                        retry.append((index, item, False))
                        if exc.source_wide:
                            source_wide_cf = source_wide_cf or exc
                    except WhoScoredTransportError as exc:
                        if exc.kind is FailureKind.CONTENT:
                            self._store_response(item.cache_key, browser)
                        last_item_errors[item.cache_key] = exc
                        non_cf_evidence[item.cache_key] = exc
                        if exc.retryable:
                            retry.append((index, item, False))
                        else:
                            terminal_error = terminal_error or exc
                    else:
                        self._store_response(item.cache_key, browser)
                        results[index] = browser
                        last_item_errors.pop(item.cache_key, None)
                        non_cf_evidence.pop(item.cache_key, None)
                if source_wide_cf is not None:
                    # Record authoritative source evidence before any sibling
                    # item error can win exception precedence.  The blocked
                    # browser session is never reused after that evidence.
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    self._source_tripped(source_wide_cf)
                    if (
                        self._source_circuit is not None
                            and not self._source_circuit_wait
                    ):
                        raise source_wide_cf
                    if self._source_circuit is not None and self._source_circuit_wait:
                        if terminal_error is not None:
                            self._direct_gate_circuits.discard(gate_key)
                            raise terminal_error
                        rechunk_after_cooldown = list(retry)
                        pending = []
                        break
                if self._source_probe_active() and (terminal_error or retry):
                    uncertain = (
                        terminal_error or last_item_errors[retry[0][1].cache_key]
                    )
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    self._direct_gate_circuits.discard(gate_key)
                    self._source_inconclusive(pending[0][1].url)
                    raise uncertain
                if terminal_error is not None:
                    self._direct_gate_circuits.discard(gate_key)
                    raise terminal_error
                pending = retry
                if pending:
                    self._drop_browser_session(
                        self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR
                    )
                    if attempt + 1 < self.direct_browser_attempts:
                        if not (
                            source_wide_cf is not None
                            and self._source_circuit is not None
                        ):
                            self._wait_before_browser_retry(attempt)
                else:
                    self._source_succeeded(items[0].url)

            if rechunk_after_cooldown:
                # Re-enter the outer chunker only after shared admission.  The
                # half-open permit then forces exactly one XHR, never the old
                # multi-item batch that observed the block.
                gated[offset:offset] = rechunk_after_cooldown
                continue
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
            assert last_cf is not None
            raise last_cf

        if any(response is None for response in results):
            raise WhoScoredTransportError(
                "structured batch finished without every response",
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=TransportRoute.DIRECT_FLARESOLVERR,
            )
        self._source_succeeded(items[0].url)
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
        if payload.observed_at:
            response = replace(response, observed_at=payload.observed_at)
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
                # browser route.
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
                observed_at=response.observed_at,
            )
            try:
                self.raw_cache.store(key, payload, response.sha256)
            except Exception as exc:
                raise WhoScoredTransportError(
                    "raw cache store failed: "
                    f"{str(exc)}",
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
        before_network: Optional[Callable[[], None]] = None,
    ) -> TransportResponse:
        request_kwargs: dict[str, Any] = {"timeout": self.request_timeout}
        if _is_whoscored_stage_bootstrap_url(url):
            # A token from a redirect destination is not evidence for this page.
            request_kwargs["allow_redirects"] = False
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
            bound = self._stage_http_context
            if (
                bound is not None
                and route is TransportRoute.DIRECT_HTTP
                and referer == bound[0]
                and session is bound[2] is self._direct_http
                and self._pool_proxy_url == bound[3]
                and (
                    [
                        value
                        for key, value in parse_qsl(urlsplit(url).query)
                        if key == "stageId"
                    ]
                    if urlsplit(url).path.startswith("/statisticsfeed/")
                    else [urlsplit(url).path.split("/")[2]]
                )
                == [urlsplit(referer).path.split("/")[8]]
            ):
                request_headers["Model-last-Mode"] = bound[1]
            request_kwargs["headers"] = request_headers
            # curl/requests otherwise follow redirects before validation,
            # which would defeat the browser endpoint's no-follow policy.
            request_kwargs["allow_redirects"] = False
        pooled = self._is_pool_session(session)
        for attempt in range(2):
            authorization = False
            try:
                raw = session.get(url, **request_kwargs)
            except Exception as exc:
                if not (
                    pooled and (
                        _is_proxy_failure(exc) or _is_tls_connect_failure(exc, pooled=True)
                    )
                ) or (attempt and _is_tls_connect_failure(exc, pooled=pooled)):
                    try:
                        self._raise_http_request_failure(url, route=route, exc=exc)
                    except WhoScoredTransportError as failure:
                        if attempt:
                            # The replacement passed ipify; a source timeout
                            # does not establish pool failure. Replay is spent.
                            failure.retryable = False
                        raise
                if self._source_probe_active():
                    # The caller preserves an uncertain half-open result;
                    # it is not evidence that the pool is unavailable.
                    self._raise_http_request_failure(url, route=route, exc=exc)
                self._record_proxy_failure(url, route=route, exc=exc)
                if attempt:
                    raise ProxyUnavailable(
                        "WhoScored access check exhausted: reason=source_replay_failed; "
                        f"{type(exc).__name__}"
                    ) from None
                authorization = bool(re.search(r"\b407\b", str(exc)))
            else:
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
                if (
                    "Model-last-Mode" in request_kwargs.get("headers", {})
                    and 200 <= response.status_code < 300
                ):
                    self.stats.stage_xhr_token["curl_2xx_responses"] += 1
                if not (
                    pooled
                    and (
                        response.status_code == 502
                        or (attempt and response.status_code >= 500)
                    )
                ):
                    if pooled and response.status_code >= 500:
                        self._rotate_pool_proxy()
                    return response
                # Record the failed source response (including byte limits)
                # before testing another member; ipify is not a source success.
                try:
                    self._validate(response, None)
                except WhoScoredTransportError as exc:
                    if exc.kind is not FailureKind.HTTP_STATUS or self._source_probe_active():
                        raise
                    if attempt:
                        exc.retryable = False
                        raise
            if authorization:
                # Preserve the separate two-member authorization path.
                if not self._rotate_pool_proxy():
                    raise ProxyUnavailable(
                        "WhoScored access check exhausted: reason=replacement_unavailable"
                    ) from None
            else:
                tested = {self._pool_proxy_url} if self._pool_proxy_url else set()
                self._probe_pool_egress(tested, replace_first=True)
            # A new member has neither the old cookie jar nor its source token.
            if "headers" in request_kwargs:
                request_kwargs["headers"] = {
                    key: value
                    for key, value in request_kwargs["headers"].items()
                    if key != "Model-last-Mode"
                }
            if before_network is not None:
                before_network()
            session = self._direct_http
        raise AssertionError("pool recovery loop returned no result")

    def _record_proxy_failure(
        self, url: str, *, route: TransportRoute, exc: BaseException
    ) -> None:
        self.stats.failures[FailureKind.PROXY.value] += 1
        self._record_ledger(
            url=url,
            route=route,
            status="error",
            failure_kind=FailureKind.PROXY,
            error=exc,
        )

    def _raise_http_request_failure(
        self, url: str, *, route: TransportRoute, exc: BaseException
    ) -> NoReturn:
        self.stats.failures[FailureKind.TIMEOUT.value] += 1
        self._record_ledger(
            url=url,
            route=route,
            status="error",
            failure_kind=FailureKind.TIMEOUT,
            error=exc,
        )
        detail = f"{type(exc).__name__}: {exc}"
        raise WhoScoredTransportError(
            f"HTTP request failed: {detail}",
            kind=FailureKind.TIMEOUT,
            url=url,
            route=route,
            retryable=True,
        ) from exc

    def _require_browser_statistics_url(
        self,
        solution: Mapping[str, Any],
        url: str,
        *,
        client: FlareSolverrClient,
        route: TransportRoute,
    ) -> None:
        """Require exact page evidence before accepting cookies or source absence."""
        if solution.get("url") != url:
            self._drop_browser_session(client, route)
            raise WhoScoredTransportError(
                "WhoScored browser statistics bootstrap URL is missing or different",
                kind=FailureKind.BROWSER,
                url=url,
                route=route,
                retryable=False,
            )

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
        except FlareSolverrRuntimeIdentityError as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CONFIG,
                error=exc,
            )
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.CONFIG,
                url=url,
                route=route,
                retryable=False,
            ) from exc
        except FlareSolverrCFChallengeFailed as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CLOUDFLARE,
                error=exc,
            )
            raise CloudflareChallenge(
                str(exc),
                url=url,
                route=route,
                source_wide=route is TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        except FlareSolverrResponseTooLarge as exc:
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BUDGET,
                error=exc,
            )
            raise TransportBudgetExceeded(
                str(exc), url=url, route=route
            ) from exc
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
                self._require_browser_statistics_url(
                    bootstrap_solution, bootstrap_url, client=client, route=route
                )
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
                    # proof.
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
        except FlareSolverrRuntimeIdentityError as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CONFIG,
                error=exc,
            )
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.CONFIG,
                url=url,
                route=route,
                retryable=False,
            ) from exc
        except FlareSolverrCFChallengeFailed as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.CLOUDFLARE,
                error=exc,
            )
            raise CloudflareChallenge(
                str(exc),
                url=url,
                route=route,
                source_wide=route is TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        except FlareSolverrResponseTooLarge as exc:
            self._drop_browser_session(client, route)
            self._record_ledger(
                url=url,
                route=route,
                status="error",
                failure_kind=FailureKind.BUDGET,
                error=exc,
            )
            raise TransportBudgetExceeded(
                str(exc), url=url, route=route
            ) from exc
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
        if not bootstrap_url and _is_whoscored_stage_bootstrap_url(url):
            self._require_browser_statistics_url(
                solution, url, client=client, route=route
            )
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
        except FlareSolverrRuntimeIdentityError as exc:
            record_error(FailureKind.CONFIG, exc)
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.CONFIG,
                url=items[0].url,
                route=route,
                retryable=False,
            ) from exc
        except FlareSolverrCFChallengeFailed as exc:
            record_error(FailureKind.CLOUDFLARE, exc)
            raise CloudflareChallenge(
                str(exc),
                url=items[0].url,
                route=route,
                source_wide=route is TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        except FlareSolverrResponseTooLarge as exc:
            record_error(FailureKind.BUDGET, exc)
            raise TransportBudgetExceeded(
                str(exc),
                url=items[0].url,
                route=route,
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
                self._require_browser_statistics_url(
                    bootstrap_solution, bootstrap_url, client=client, route=route
                )
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
        except FlareSolverrRuntimeIdentityError as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.CONFIG, exc)
            raise WhoScoredTransportError(
                str(exc),
                kind=FailureKind.CONFIG,
                url=items[0].url,
                route=route,
                retryable=False,
            ) from exc
        except FlareSolverrCFChallengeFailed as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.CLOUDFLARE, exc)
            raise CloudflareChallenge(
                str(exc),
                url=items[0].url,
                route=route,
                source_wide=route is TransportRoute.DIRECT_FLARESOLVERR,
            ) from exc
        except FlareSolverrResponseTooLarge as exc:
            self._drop_browser_session(client, route)
            record_error(FailureKind.BUDGET, exc)
            raise TransportBudgetExceeded(
                str(exc),
                url=items[0].url,
                route=route,
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
            # [1017] Stage-stat XHR fails synchronously in the extension when the
            # page's RequireJS site-header token is absent ("WhoScored page
            # request header is unavailable."). A full bootstrap reload does NOT
            # recover it, so 4 retries only burn wall-clock (→ AirflowTaskTimeout)
            # and re-download the bootstrap page (proxy bytes). service.py already
            # degrades these stage-stat feeds to NOT_AVAILABLE on the first raise
            # (_is_source_stage_statistics_unavailable) — schedule/matches/events
            # still commit and the scope stays success. Mark it non-retryable so
            # it fails fast into that graceful path. Any other browser error keeps
            # retryable=True. Latent behind master's #1012 residential transport.
            _exc_text = str(exc)
            _no_retry = _SOURCE_STAGE_HEADER_UNAVAILABLE in _exc_text
            raise WhoScoredTransportError(
                _exc_text,
                kind=FailureKind.BROWSER,
                url=items[0].url,
                route=route,
                retryable=not _no_retry,
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
            # Bind raw ordering before parser and S3 commit work.  Supported
            # production uses one LocalExecutor host and a per-target lock;
            # this timestamp also prevents a slow commit from being mistaken
            # for a newer source observation.
            observed_at=datetime.now(timezone.utc).isoformat(),
        )

    def _record_response(self, response: TransportResponse) -> None:
        self.stats.route_requests[response.route.value] += 1
        if 200 <= response.status_code < 300:
            self.stats.route_successes[response.route.value] += 1
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
            if response.route is TransportRoute.DIRECT_FLARESOLVERR
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
            response.route is TransportRoute.DIRECT_FLARESOLVERR
            and response.status_code in (403, 429, 503)
            and _has_cloudflare_challenge_markup(response.content)
        )
        structured_access_gate = is_whoscored_structured_feed_access_gate(
            response.url,
            response.status_code,
            response.content,
            response.headers,
        )
        if structured_access_gate and not browser_challenge:
            if response.route is TransportRoute.DIRECT_HTTP:
                # This stable origin mask is only route-selection evidence:
                # the same feed may succeed inside its official stage page.
                raise CloudflareChallenge(
                    "WhoScored structured-feed HTTP access gate",
                    url=response.url,
                    route=response.route,
                    status_code=response.status_code,
                    source_wide=False,
                )
            # Seeing the mask in a browser is not a Cloudflare challenge and
            # must never count toward the shared cooldown.
            raise WhoScoredTransportError(
                "WhoScored structured-feed access gate rendered in browser",
                kind=FailureKind.CONTENT,
                url=response.url,
                route=response.route,
                status_code=response.status_code,
            )
        if browser_challenge or is_cloudflare_response(
            response.status_code, response.headers, response.content
        ):
            raise CloudflareChallenge(
                "Cloudflare challenge response",
                url=response.url,
                route=response.route,
                status_code=response.status_code,
                source_wide=bool(
                    response.route is TransportRoute.DIRECT_FLARESOLVERR
                    and browser_challenge
                ),
            )
        if not 200 <= response.status_code < 300:
            redirect_location = None
            if response.status_code in {301, 302, 303, 307, 308}:
                location = next(
                    (v for k, v in response.headers.items() if k.lower() == "location"),
                    "",
                )
                try:
                    parts = urlsplit(urljoin(response.url, location) if location else "")
                    # Strip credentials, per-segment path parameters, query and fragment.
                    redirect_location = parts._replace(
                        netloc=parts.netloc.rsplit("@", 1)[-1],
                        path="/".join(
                            segment.split(";", 1)[0] for segment in parts.path.split("/")
                        ),
                        query="", fragment=""
                    ).geturl()
                except ValueError:
                    redirect_location = "<invalid>"
            raise WhoScoredTransportError(
                f"HTTP {response.status_code}",
                kind=FailureKind.HTTP_STATUS,
                url=response.url,
                route=response.route,
                status_code=response.status_code,
                retryable=response.status_code >= 500,
                redirect_location=redirect_location,
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
                "content validator failed: "
                f"{str(exc)}",
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
        }
        try:
            self.request_ledger.append(event)
        except Exception as exc:
            # Missing telemetry means traffic cannot be audited. Fail the
            # request instead of silently serving unaccounted production data.
            raise WhoScoredTransportError(
                "request ledger append failed: "
                f"{str(exc)}",
                kind=FailureKind.CONFIG,
                url=url,
                route=route,
                retryable=True,
            ) from exc

    def get_traffic_stats(self) -> dict[str, Any]:
        return self.stats.as_dict()

    def close(self) -> None:
        if self._source_circuit is not None and self._source_circuit_permit is not None:
            permit = self._source_circuit_permit
            self._source_circuit_permit = None
            try:
                self._source_circuit.abandon(permit)
            except SourceCircuitError:
                logger.error("WhoScored source circuit probe cleanup failed")
        self._drop_browser_session(self._direct_fs, TransportRoute.DIRECT_FLARESOLVERR)
        self._direct_gate_circuits.clear()
        self._stage_http_context = None
        resources = [self._direct_http, self._direct_fs]
        seen: set[int] = set()
        for resource in resources:
            if id(resource) in seen:
                continue
            seen.add(id(resource))
            close = getattr(resource, "close", None)
            if callable(close):
                close()

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
    "ProxyUnavailable",
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
