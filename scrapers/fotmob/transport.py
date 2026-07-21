"""Measured, cache-aware HTTP transport for FotMob JSON endpoints."""

from __future__ import annotations

import gzip
import hashlib
import json
import random
import threading
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any, Callable, Mapping, Optional, Sequence, Union
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
from urllib3.exceptions import HTTPError as Urllib3HTTPError

from scrapers.fotmob.raw_store import (
    FotMobRawStore,
    RawJsonRecord,
    RawStoreError,
    RawTargetNotFound,
)


FOTMOB_API_BASE = "https://www.fotmob.com/api/data"
FOTMOB_ALLOWED_HOSTS = frozenset({"www.fotmob.com", "data.fotmob.com"})
RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429})
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

ParamValue = Union[str, int, float, bool, None, Sequence[object]]
Params = Union[Mapping[str, ParamValue], Sequence[tuple[str, ParamValue]]]


class FetchOutcome(str, Enum):
    SUCCESS = "success"
    NOT_MODIFIED = "not_modified"
    STALE_REPLAY = "stale_replay"
    NOT_AVAILABLE = "not_available"
    RETRYABLE_FAILURE = "retryable_failure"
    TERMINAL_FAILURE = "terminal_failure"


@dataclass(frozen=True)
class CanonicalTarget:
    canonical_url: str
    target_key: str


@dataclass(frozen=True)
class FetchResult:
    """One logical fetch, including all network and replay accounting."""

    outcome: FetchOutcome
    target_key: str
    url: str
    http_status: Optional[int]
    json_data: Any
    body: Optional[bytes]
    attempts: int
    retries: int
    cache_hit: bool
    stale: bool
    terminal: bool
    etag: Optional[str]
    last_modified: Optional[str]
    raw_uri: Optional[str]
    content_hash: Optional[str]
    fetched_at: Optional[str]
    encoded_bytes: int
    decoded_bytes: int
    direct_bytes: int
    proxy_bytes: int
    error: Optional[str] = None

    @property
    def status(self) -> str:
        """JSON-friendly alias used by ingest manifests."""
        return self.outcome.value

    @property
    def ok(self) -> bool:
        return self.outcome in {
            FetchOutcome.SUCCESS,
            FetchOutcome.NOT_MODIFIED,
            FetchOutcome.STALE_REPLAY,
        }

    @property
    def data(self) -> Any:
        """Compatibility alias for callers expecting a short payload name."""
        return self.json_data


@dataclass(frozen=True)
class TransportStats:
    logical_targets: int = 0
    attempts: int = 0
    retries: int = 0
    status_counts: Mapping[str, int] = field(default_factory=dict)
    outcome_counts: Mapping[str, int] = field(default_factory=dict)
    encoded_bytes: int = 0
    decoded_bytes: int = 0
    direct_bytes: int = 0
    proxy_bytes: int = 0
    cache_hits: int = 0
    stale_replays: int = 0
    not_modified: int = 0


def _iter_params(params: Optional[Params]) -> list[tuple[str, str]]:
    if params is None:
        return []
    source = params.items() if isinstance(params, Mapping) else params
    flattened: list[tuple[str, str]] = []
    for raw_key, raw_value in source:
        key = str(raw_key)
        if raw_value is None:
            # Match requests' treatment of ``params={"x": None}``.
            continue
        if isinstance(raw_value, Sequence) and not isinstance(
            raw_value,
            (str, bytes, bytearray),
        ):
            for item in raw_value:
                if item is not None:
                    flattened.append((key, str(item)))
        else:
            flattened.append((key, str(raw_value)))
    return flattened


def canonicalize_target(
    url_or_endpoint: str,
    params: Optional[Params] = None,
    *,
    api_base: str = FOTMOB_API_BASE,
) -> CanonicalTarget:
    """Validate and canonicalize one allowlisted FotMob JSON target.

    Short names (``"leagues"``) are resolved under ``/api/data``.  Query
    pairs from both the URL and ``params`` are sorted, preserving repeated
    keys, so equivalent calls share one raw manifest and validator set.
    """
    candidate = str(url_or_endpoint).strip()
    if not candidate:
        raise ValueError("FotMob endpoint must not be empty")
    if "://" not in candidate:
        if candidate.startswith("/"):
            candidate = urljoin("https://www.fotmob.com", candidate)
        else:
            candidate = f"{api_base.rstrip('/')}/{candidate.lstrip('/')}"

    parsed = urlsplit(candidate)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"Invalid FotMob URL: {url_or_endpoint!r}") from exc
    hostname = (parsed.hostname or "").lower()
    if (
        parsed.scheme.lower() != "https"
        or hostname not in FOTMOB_ALLOWED_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
    ):
        raise ValueError(f"URL is outside the FotMob HTTPS allowlist: {candidate!r}")

    path = parsed.path or "/"
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_pairs.extend(_iter_params(params))
    query_pairs.sort(key=lambda pair: (pair[0], pair[1]))
    query = urlencode(query_pairs, doseq=False)
    canonical_url = urlunsplit(("https", hostname, path, query, ""))
    target_key = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()
    return CanonicalTarget(canonical_url=canonical_url, target_key=target_key)


def _decode_wire_body(encoded: bytes, content_encoding: Optional[str]) -> bytes:
    body = encoded
    encodings = [
        item.strip().lower()
        for item in (content_encoding or "").split(",")
        if item.strip()
    ]
    for encoding in reversed(encodings):
        if encoding in {"identity", ""}:
            continue
        if encoding in {"gzip", "x-gzip"}:
            body = gzip.decompress(body)
            continue
        if encoding == "deflate":
            try:
                body = zlib.decompress(body)
            except zlib.error:
                body = zlib.decompress(body, -zlib.MAX_WBITS)
            continue
        raise ValueError(f"Unsupported Content-Encoding: {encoding}")
    return body


def _read_encoded_response(response: requests.Response) -> bytes:
    raw = getattr(response, "raw", None)
    if raw is not None:
        try:
            return raw.read(decode_content=False)
        except TypeError:
            return raw.read()
    # Mainly useful for small fake responses in downstream unit tests.  Real
    # requests always exposes ``raw`` when called with ``stream=True``.
    return response.content


def _parse_retry_after(value: Optional[str], now: datetime) -> Optional[float]:
    if not value:
        return None
    candidate = value.strip()
    try:
        return max(0.0, float(candidate))
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(candidate)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (parsed - now).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


class FotMobTransport:
    """Direct-only HTTP transport with validation, retries and raw replay."""

    def __init__(
        self,
        raw_store: Optional[FotMobRawStore] = None,
        *,
        session: Optional[requests.Session] = None,
        max_attempts: int = 4,
        timeout: Union[float, tuple[float, float]] = 30.0,
        max_stale_seconds: Optional[float] = None,
        rate_limiter: Optional[object] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        jitter_fn: Callable[[float, float], float] = random.uniform,
        backoff_base: float = 1.0,
        jitter_seconds: float = 0.25,
        max_retry_delay: float = 60.0,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if max_stale_seconds is not None and max_stale_seconds < 0:
            raise ValueError("max_stale_seconds must be non-negative")
        self.raw_store = raw_store
        self.session = session or requests.Session()
        # ``trust_env=False`` is the important direct-only invariant: requests
        # must never inherit HTTP(S)_PROXY, ALL_PROXY or .netrc credentials.
        self.session.trust_env = False
        self.session.proxies.clear()
        self.session.headers.update(
            {
                "User-Agent": DEFAULT_USER_AGENT,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                # Restrict encodings to formats decoded and measured below.
                "Accept-Encoding": "gzip, deflate",
                "Referer": "https://www.fotmob.com/",
                "Origin": "https://www.fotmob.com",
            }
        )
        self.max_attempts = max_attempts
        self.timeout = timeout
        self.max_stale_seconds = max_stale_seconds
        self.rate_limiter = rate_limiter
        self.sleep_fn = sleep_fn
        self._default_sleep = sleep_fn is time.sleep
        self.jitter_fn = jitter_fn
        self.backoff_base = max(0.0, backoff_base)
        self.jitter_seconds = max(0.0, jitter_seconds)
        self.max_retry_delay = max(0.0, max_retry_delay)
        self._stats_lock = threading.Lock()
        self._cancelled = threading.Event()
        self._stats: dict[str, Any] = {}
        self.reset_stats()

    def cancel(self) -> None:
        """Interrupt retry/rate-limit waits and close pooled connections."""

        self._cancelled.set()
        close = getattr(self.session, "close", None)
        if close is not None:
            close()

    def _raise_if_cancelled(self) -> None:
        if self._cancelled.is_set():
            raise RuntimeError("FotMob transport cancelled")

    def _sleep_interruptibly(self, delay: float) -> None:
        delay = max(0.0, float(delay))
        if self._default_sleep:
            if self._cancelled.wait(delay):
                self._raise_if_cancelled()
            return
        self.sleep_fn(delay)
        self._raise_if_cancelled()

    def _acquire_rate_limit(self) -> None:
        if self.rate_limiter is None:
            return
        acquire = self.rate_limiter.acquire
        while True:
            self._raise_if_cancelled()
            try:
                acquired = acquire(timeout=0.25)
            except TypeError:
                # Compatibility for small injected limiters. Production's
                # limiter supports bounded waits and remains cancellable.
                acquired = acquire()
            if acquired is not False:
                return

    def reset_stats(self) -> None:
        with self._stats_lock:
            self._stats = {
                "logical_targets": 0,
                "attempts": 0,
                "retries": 0,
                "status_counts": {},
                "outcome_counts": {},
                "encoded_bytes": 0,
                "decoded_bytes": 0,
                "direct_bytes": 0,
                "proxy_bytes": 0,
                "cache_hits": 0,
                "stale_replays": 0,
                "not_modified": 0,
            }

    def snapshot_stats(self) -> TransportStats:
        with self._stats_lock:
            values = dict(self._stats)
            values["status_counts"] = dict(self._stats["status_counts"])
            values["outcome_counts"] = dict(self._stats["outcome_counts"])
        return TransportStats(**values)

    def _start_target(self) -> None:
        with self._stats_lock:
            self._stats["logical_targets"] += 1

    def _record_attempt(self, status: Union[int, str], encoded_bytes: int) -> None:
        status_key = str(status)
        with self._stats_lock:
            self._stats["attempts"] += 1
            counts = self._stats["status_counts"]
            counts[status_key] = counts.get(status_key, 0) + 1
            self._stats["encoded_bytes"] += encoded_bytes
            self._stats["direct_bytes"] += encoded_bytes

    def _finish(self, result: FetchResult) -> FetchResult:
        with self._stats_lock:
            self._stats["retries"] += result.retries
            counts = self._stats["outcome_counts"]
            counts[result.status] = counts.get(result.status, 0) + 1
            self._stats["decoded_bytes"] += result.decoded_bytes
            self._stats["proxy_bytes"] += result.proxy_bytes
            if result.cache_hit:
                self._stats["cache_hits"] += 1
            if result.outcome == FetchOutcome.STALE_REPLAY:
                self._stats["stale_replays"] += 1
            if result.outcome == FetchOutcome.NOT_MODIFIED:
                self._stats["not_modified"] += 1
        return result

    @staticmethod
    def _json(body: bytes) -> Any:
        # json.loads(bytes) handles UTF-8/16/32 and BOM detection itself.
        return json.loads(body)

    def _load_cache(
        self,
        target: CanonicalTarget,
    ) -> tuple[Optional[bytes], Optional[RawJsonRecord], Optional[str]]:
        if self.raw_store is None:
            return None, None, None
        try:
            body, record = self.raw_store.load(target)
            # Validate JSON before using validators or treating it as replayable.
            self._json(body)
            return body, record, None
        except RawTargetNotFound:
            return None, None, None
        except (RawStoreError, OSError, ValueError, json.JSONDecodeError) as exc:
            return None, None, f"raw cache unavailable: {type(exc).__name__}: {exc}"

    def _stale_is_eligible(self, record: Optional[RawJsonRecord]) -> bool:
        if record is None:
            return False
        if self.max_stale_seconds is None:
            return True
        try:
            validated = datetime.fromisoformat(record.validated_at)
            if validated.tzinfo is None:
                validated = validated.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - validated).total_seconds()
            return age <= self.max_stale_seconds
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _cache_fields(record: Optional[RawJsonRecord]) -> dict[str, Optional[str]]:
        if record is None:
            return {
                "etag": None,
                "last_modified": None,
                "raw_uri": None,
                "content_hash": None,
                "fetched_at": None,
            }
        return {
            "etag": record.etag,
            "last_modified": record.last_modified,
            "raw_uri": record.raw_uri,
            "content_hash": record.content_hash,
            "fetched_at": record.fetched_at,
        }

    def _result(
        self,
        *,
        outcome: FetchOutcome,
        target: CanonicalTarget,
        http_status: Optional[int],
        body: Optional[bytes],
        json_data: Any,
        attempts: int,
        network_bytes: int,
        cache_hit: bool = False,
        stale: bool = False,
        terminal: bool = False,
        record: Optional[RawJsonRecord] = None,
        error: Optional[str] = None,
        expects_json: bool = True,
    ) -> FetchResult:
        # FotMob answers HTTP 200 with a literal JSON ``null`` body for dead
        # catalog entries (placeholder/promo competitions).  A null payload
        # carries no data for any parser, so every data-bearing outcome —
        # fresh 200, 304 replay or stale replay — degrades to the same
        # terminal NOT_AVAILABLE state as a 204/404 instead of crashing a
        # downstream parser into a false schema_drift.
        if (
            expects_json
            and json_data is None
            and outcome
            in {
                FetchOutcome.SUCCESS,
                FetchOutcome.NOT_MODIFIED,
                FetchOutcome.STALE_REPLAY,
            }
        ):
            outcome = FetchOutcome.NOT_AVAILABLE
            body = None
            cache_hit = False
            stale = False
            terminal = True
            error = error or "FotMob returned a null JSON body"
        fields = self._cache_fields(record)
        return FetchResult(
            outcome=outcome,
            target_key=target.target_key,
            url=target.canonical_url,
            http_status=http_status,
            json_data=json_data,
            body=body,
            attempts=attempts,
            retries=max(0, attempts - 1),
            cache_hit=cache_hit,
            stale=stale,
            terminal=terminal,
            encoded_bytes=network_bytes,
            decoded_bytes=len(body) if body is not None else 0,
            direct_bytes=network_bytes,
            proxy_bytes=0,
            error=error,
            **fields,
        )

    def _stale_or_failure(
        self,
        *,
        target: CanonicalTarget,
        http_status: Optional[int],
        attempts: int,
        network_bytes: int,
        cached_body: Optional[bytes],
        cached_record: Optional[RawJsonRecord],
        allow_stale_on_error: bool,
        error: str,
    ) -> FetchResult:
        if (
            allow_stale_on_error
            and cached_body is not None
            and self._stale_is_eligible(cached_record)
        ):
            return self._finish(
                self._result(
                    outcome=FetchOutcome.STALE_REPLAY,
                    target=target,
                    http_status=http_status,
                    body=cached_body,
                    json_data=self._json(cached_body),
                    attempts=attempts,
                    network_bytes=network_bytes,
                    cache_hit=True,
                    stale=True,
                    record=cached_record,
                    error=error,
                )
            )
        return self._finish(
            self._result(
                outcome=FetchOutcome.RETRYABLE_FAILURE,
                target=target,
                http_status=http_status,
                body=None,
                json_data=None,
                attempts=attempts,
                network_bytes=network_bytes,
                record=cached_record,
                error=error,
            )
        )

    def _stale_or_terminal(
        self,
        *,
        target: CanonicalTarget,
        http_status: Optional[int],
        attempts: int,
        network_bytes: int,
        cached_body: Optional[bytes],
        cached_record: Optional[RawJsonRecord],
        allow_stale_on_error: bool,
        error: str,
    ) -> FetchResult:
        """Replay last-good data for an invalid fresh payload/commit failure."""
        if (
            allow_stale_on_error
            and cached_body is not None
            and self._stale_is_eligible(cached_record)
        ):
            return self._finish(
                self._result(
                    outcome=FetchOutcome.STALE_REPLAY,
                    target=target,
                    http_status=http_status,
                    body=cached_body,
                    json_data=self._json(cached_body),
                    attempts=attempts,
                    network_bytes=network_bytes,
                    cache_hit=True,
                    stale=True,
                    record=cached_record,
                    error=error,
                )
            )
        return self._finish(
            self._result(
                outcome=FetchOutcome.TERMINAL_FAILURE,
                target=target,
                http_status=http_status,
                body=None,
                json_data=None,
                attempts=attempts,
                network_bytes=network_bytes,
                terminal=True,
                record=cached_record,
                error=error,
            )
        )

    def _retry_delay(self, attempt: int, retry_after: Optional[str]) -> float:
        specified = _parse_retry_after(retry_after, datetime.now(timezone.utc))
        if specified is None:
            specified = self.backoff_base * (2 ** max(0, attempt - 1))
        jitter = self.jitter_fn(0.0, self.jitter_seconds)
        return min(self.max_retry_delay, max(0.0, specified + jitter))

    def replay_json(
        self,
        url_or_endpoint: str,
        params: Optional[Params] = None,
    ) -> FetchResult:
        """Read and validate a target without making a network request."""
        self._raise_if_cancelled()
        target = canonicalize_target(url_or_endpoint, params)
        self._start_target()
        body, record, cache_error = self._load_cache(target)
        if body is None or record is None:
            return self._finish(
                self._result(
                    outcome=FetchOutcome.TERMINAL_FAILURE,
                    target=target,
                    http_status=None,
                    body=None,
                    json_data=None,
                    attempts=0,
                    network_bytes=0,
                    terminal=True,
                    error=cache_error or "raw target not found",
                )
            )
        return self._finish(
            self._result(
                outcome=FetchOutcome.SUCCESS,
                target=target,
                http_status=None,
                body=body,
                json_data=self._json(body),
                attempts=0,
                network_bytes=0,
                cache_hit=True,
                record=record,
            )
        )

    def replay_target_key(self, target_key: str) -> FetchResult:
        """Replay the exact raw target recorded by an ingest manifest.

        This is primarily for Next.js player payloads: their canonical URL
        contains a rotating build id, while the durable manifest already
        records the precise raw target used by the original fetch.  Replaying
        by key avoids guessing a single build id for a multi-day backfill.
        """

        normalized = str(target_key).strip().casefold()
        if len(normalized) != 64 or any(
            character not in "0123456789abcdef" for character in normalized
        ):
            raise ValueError("raw replay target_key must be lowercase SHA-256")
        self._raise_if_cancelled()
        self._start_target()
        placeholder = CanonicalTarget(
            canonical_url=("https://www.fotmob.com/_raw/sha256/" + normalized),
            target_key=normalized,
        )
        if self.raw_store is None:
            return self._finish(
                self._result(
                    outcome=FetchOutcome.TERMINAL_FAILURE,
                    target=placeholder,
                    http_status=None,
                    body=None,
                    json_data=None,
                    attempts=0,
                    network_bytes=0,
                    terminal=True,
                    error="raw store is unavailable for target-key replay",
                )
            )
        try:
            body, record = self.raw_store.load_target_key(normalized)
            target = canonicalize_target(record.canonical_url)
            if target.target_key != normalized:
                raise ValueError("raw target canonical URL digest differs")
            parsed = self._json(body)
        except (RawStoreError, OSError, ValueError, json.JSONDecodeError) as exc:
            return self._finish(
                self._result(
                    outcome=FetchOutcome.TERMINAL_FAILURE,
                    target=placeholder,
                    http_status=None,
                    body=None,
                    json_data=None,
                    attempts=0,
                    network_bytes=0,
                    terminal=True,
                    error=(
                        f"raw target-key replay failed: {type(exc).__name__}: {exc}"
                    ),
                )
            )
        return self._finish(
            self._result(
                outcome=FetchOutcome.SUCCESS,
                target=target,
                http_status=None,
                body=body,
                json_data=parsed,
                attempts=0,
                network_bytes=0,
                cache_hit=True,
                record=record,
            )
        )

    def alias_cached_json(
        self,
        source_url_or_endpoint: str,
        target_url_or_endpoint: str,
        *,
        source_params: Optional[Params] = None,
        target_params: Optional[Params] = None,
    ) -> Optional[RawJsonRecord]:
        """Alias one validated cached JSON target without network accounting.

        This is deliberately a transport primitive because it shares the same
        canonical URL rules as fetch/replay.  Returning ``None`` when no raw
        store is configured keeps unit and library use possible, while a
        configured store fails closed on a missing or corrupt source target.
        """

        if self.raw_store is None:
            return None
        source = canonicalize_target(source_url_or_endpoint, source_params)
        target = canonicalize_target(target_url_or_endpoint, target_params)
        return self.raw_store.alias(source, target)

    def fetch_document(self, url: str = "https://www.fotmob.com/") -> FetchResult:
        """Fetch a small allowlisted non-JSON discovery document.

        FotMob's rotating Next.js ``buildId`` is exposed in the homepage HTML.
        Resolving it once per player-snapshot batch avoids one HTML request per
        player while retaining the same direct-only, retry and byte-accounting
        invariants as JSON requests.  Documents are not put in the JSON raw
        store; only entity-bearing Next JSON payloads are content-addressed.
        """

        target = canonicalize_target(url)
        self._raise_if_cancelled()
        self._start_target()
        attempts = 0
        network_bytes = 0
        last_status: Optional[int] = None
        last_error: Optional[str] = None
        while attempts < self.max_attempts:
            attempts += 1
            self._acquire_rate_limit()
            self._raise_if_cancelled()
            response: Optional[requests.Response] = None
            try:
                response = self.session.get(
                    target.canonical_url,
                    headers={},
                    timeout=self.timeout,
                    stream=True,
                    allow_redirects=False,
                )
                canonicalize_target(
                    getattr(response, "url", None) or target.canonical_url
                )
                last_status = int(response.status_code)
                encoded = _read_encoded_response(response)
                network_bytes += len(encoded)
                self._record_attempt(last_status, len(encoded))
                if last_status in {204, 404}:
                    return self._finish(
                        self._result(
                            outcome=FetchOutcome.NOT_AVAILABLE,
                            target=target,
                            http_status=last_status,
                            body=None,
                            json_data=None,
                            expects_json=False,
                            attempts=attempts,
                            network_bytes=network_bytes,
                            terminal=True,
                            error=f"FotMob returned {last_status}",
                        )
                    )
                retryable = (
                    last_status in RETRYABLE_HTTP_STATUSES or 500 <= last_status <= 599
                )
                if retryable:
                    last_error = f"FotMob returned retryable HTTP {last_status}"
                    if attempts < self.max_attempts:
                        self._sleep_interruptibly(
                            self._retry_delay(
                                attempts, response.headers.get("Retry-After")
                            )
                        )
                        continue
                    return self._finish(
                        self._result(
                            outcome=FetchOutcome.RETRYABLE_FAILURE,
                            target=target,
                            http_status=last_status,
                            body=None,
                            json_data=None,
                            expects_json=False,
                            attempts=attempts,
                            network_bytes=network_bytes,
                            error=last_error,
                        )
                    )
                if not 200 <= last_status <= 299:
                    return self._finish(
                        self._result(
                            outcome=FetchOutcome.TERMINAL_FAILURE,
                            target=target,
                            http_status=last_status,
                            body=None,
                            json_data=None,
                            expects_json=False,
                            attempts=attempts,
                            network_bytes=network_bytes,
                            terminal=True,
                            error=f"FotMob returned terminal HTTP {last_status}",
                        )
                    )
                try:
                    body = _decode_wire_body(
                        encoded, response.headers.get("Content-Encoding")
                    )
                except (OSError, EOFError, zlib.error, ValueError) as exc:
                    return self._finish(
                        self._result(
                            outcome=FetchOutcome.TERMINAL_FAILURE,
                            target=target,
                            http_status=last_status,
                            body=None,
                            json_data=None,
                            expects_json=False,
                            attempts=attempts,
                            network_bytes=network_bytes,
                            terminal=True,
                            error=(
                                f"document decode failed: {type(exc).__name__}: {exc}"
                            ),
                        )
                    )
                return self._finish(
                    self._result(
                        outcome=FetchOutcome.SUCCESS,
                        target=target,
                        http_status=last_status,
                        body=body,
                        json_data=None,
                        expects_json=False,
                        attempts=attempts,
                        network_bytes=network_bytes,
                    )
                )
            except (requests.RequestException, Urllib3HTTPError, OSError) as exc:
                self._record_attempt("exception", 0)
                self._raise_if_cancelled()
                last_error = f"request failed: {type(exc).__name__}: {exc}"
                if attempts < self.max_attempts:
                    self._sleep_interruptibly(self._retry_delay(attempts, None))
                    continue
                return self._finish(
                    self._result(
                        outcome=FetchOutcome.RETRYABLE_FAILURE,
                        target=target,
                        http_status=last_status,
                        body=None,
                        json_data=None,
                        expects_json=False,
                        attempts=attempts,
                        network_bytes=network_bytes,
                        error=last_error,
                    )
                )
            finally:
                if response is not None:
                    response.close()
        raise AssertionError("unreachable fetch_document state")  # pragma: no cover

    def fetch_json(
        self,
        url_or_endpoint: str,
        params: Optional[Params] = None,
        *,
        allow_stale_on_error: bool = True,
    ) -> FetchResult:
        self._raise_if_cancelled()
        target = canonicalize_target(url_or_endpoint, params)
        self._start_target()
        cached_body, cached_record, cache_error = self._load_cache(target)
        headers: dict[str, str] = {}
        if cached_record is not None:
            if cached_record.etag:
                headers["If-None-Match"] = cached_record.etag
            if cached_record.last_modified:
                headers["If-Modified-Since"] = cached_record.last_modified

        attempts = 0
        network_bytes = 0
        last_status: Optional[int] = None
        last_error = cache_error
        while attempts < self.max_attempts:
            attempts += 1
            self._acquire_rate_limit()
            self._raise_if_cancelled()
            response: Optional[requests.Response] = None
            try:
                response = self.session.get(
                    target.canonical_url,
                    headers=headers,
                    timeout=self.timeout,
                    stream=True,
                    allow_redirects=False,
                )
                response_url = getattr(response, "url", target.canonical_url)
                if response_url:
                    # Defence in depth for injected/custom sessions. Redirects
                    # are disabled, but an adapter must not silently change host.
                    try:
                        canonicalize_target(response_url)
                    except ValueError as exc:
                        encoded = _read_encoded_response(response)
                        network_bytes += len(encoded)
                        self._record_attempt("invalid_target", len(encoded))
                        return self._finish(
                            self._result(
                                outcome=FetchOutcome.TERMINAL_FAILURE,
                                target=target,
                                http_status=None,
                                body=None,
                                json_data=None,
                                attempts=attempts,
                                network_bytes=network_bytes,
                                terminal=True,
                                record=cached_record,
                                error=f"response URL rejected: {exc}",
                            )
                        )
                last_status = int(response.status_code)
                encoded = _read_encoded_response(response)
                network_bytes += len(encoded)
                self._record_attempt(last_status, len(encoded))

                if last_status in {204, 404}:
                    return self._finish(
                        self._result(
                            outcome=FetchOutcome.NOT_AVAILABLE,
                            target=target,
                            http_status=last_status,
                            body=None,
                            json_data=None,
                            attempts=attempts,
                            network_bytes=network_bytes,
                            terminal=True,
                            error=f"FotMob returned {last_status}",
                        )
                    )

                if last_status == 304:
                    if cached_body is None or cached_record is None:
                        return self._finish(
                            self._result(
                                outcome=FetchOutcome.TERMINAL_FAILURE,
                                target=target,
                                http_status=last_status,
                                body=None,
                                json_data=None,
                                attempts=attempts,
                                network_bytes=network_bytes,
                                terminal=True,
                                error="FotMob returned 304 without a valid raw body",
                            )
                        )
                    record = cached_record
                    if self.raw_store is not None:
                        try:
                            record = self.raw_store.store(
                                target,
                                cached_body,
                                fetched_at=cached_record.fetched_at,
                                validated_at=datetime.now(timezone.utc).isoformat(),
                                etag=response.headers.get("ETag") or cached_record.etag,
                                last_modified=(
                                    response.headers.get("Last-Modified")
                                    or cached_record.last_modified
                                ),
                                source_encoded_bytes=(
                                    cached_record.source_encoded_bytes
                                ),
                            )
                        except Exception:
                            # The body itself remains durably committed and was
                            # just validated by the source.  A best-effort
                            # freshness timestamp must not turn a valid 304
                            # replay into data loss.
                            record = cached_record
                    return self._finish(
                        self._result(
                            outcome=FetchOutcome.NOT_MODIFIED,
                            target=target,
                            http_status=last_status,
                            body=cached_body,
                            json_data=self._json(cached_body),
                            attempts=attempts,
                            network_bytes=network_bytes,
                            cache_hit=True,
                            record=record,
                        )
                    )

                retryable = (
                    last_status in RETRYABLE_HTTP_STATUSES or 500 <= last_status <= 599
                )
                if retryable:
                    last_error = f"FotMob returned retryable HTTP {last_status}"
                    if attempts < self.max_attempts:
                        self._sleep_interruptibly(
                            self._retry_delay(
                                attempts,
                                response.headers.get("Retry-After"),
                            )
                        )
                        continue
                    return self._stale_or_failure(
                        target=target,
                        http_status=last_status,
                        attempts=attempts,
                        network_bytes=network_bytes,
                        cached_body=cached_body,
                        cached_record=cached_record,
                        allow_stale_on_error=allow_stale_on_error,
                        error=last_error,
                    )

                if not 200 <= last_status <= 299:
                    return self._finish(
                        self._result(
                            outcome=FetchOutcome.TERMINAL_FAILURE,
                            target=target,
                            http_status=last_status,
                            body=None,
                            json_data=None,
                            attempts=attempts,
                            network_bytes=network_bytes,
                            terminal=True,
                            record=cached_record,
                            error=f"FotMob returned terminal HTTP {last_status}",
                        )
                    )

                try:
                    body = _decode_wire_body(
                        encoded,
                        response.headers.get("Content-Encoding"),
                    )
                except (OSError, EOFError, zlib.error, ValueError) as exc:
                    last_error = f"response decode failed: {type(exc).__name__}: {exc}"
                    return self._stale_or_terminal(
                        target=target,
                        http_status=last_status,
                        attempts=attempts,
                        network_bytes=network_bytes,
                        cached_body=cached_body,
                        cached_record=cached_record,
                        allow_stale_on_error=allow_stale_on_error,
                        error=last_error,
                    )

                try:
                    parsed_json = self._json(body)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    last_error = f"invalid JSON response: {type(exc).__name__}: {exc}"
                    return self._stale_or_terminal(
                        target=target,
                        http_status=last_status,
                        attempts=attempts,
                        network_bytes=network_bytes,
                        cached_body=cached_body,
                        cached_record=cached_record,
                        allow_stale_on_error=allow_stale_on_error,
                        error=last_error,
                    )

                fetched_at = datetime.now(timezone.utc).isoformat()
                etag = response.headers.get("ETag")
                last_modified = response.headers.get("Last-Modified")
                if self.raw_store is not None:
                    try:
                        record = self.raw_store.store(
                            target,
                            body,
                            fetched_at=fetched_at,
                            etag=etag,
                            last_modified=last_modified,
                            source_encoded_bytes=len(encoded),
                        )
                    except Exception as exc:
                        return self._stale_or_terminal(
                            target=target,
                            http_status=last_status,
                            attempts=attempts,
                            network_bytes=network_bytes,
                            cached_body=cached_body,
                            cached_record=cached_record,
                            allow_stale_on_error=allow_stale_on_error,
                            error=(
                                f"raw-first commit failed: {type(exc).__name__}: {exc}"
                            ),
                        )
                else:
                    content_hash = hashlib.sha256(body).hexdigest()
                    record = RawJsonRecord(
                        manifest_version="fotmob-raw-v1",
                        source="fotmob",
                        target_key=target.target_key,
                        canonical_url=target.canonical_url,
                        content_hash=content_hash,
                        hash_algorithm="sha256",
                        blob_key="",
                        raw_uri=None,
                        compression="none",
                        fetched_at=fetched_at,
                        etag=etag,
                        last_modified=last_modified,
                        decoded_bytes=len(body),
                        source_encoded_bytes=len(encoded),
                        stored_bytes=0,
                        validated_at=fetched_at,
                    )
                return self._finish(
                    self._result(
                        outcome=FetchOutcome.SUCCESS,
                        target=target,
                        http_status=last_status,
                        body=body,
                        json_data=parsed_json,
                        attempts=attempts,
                        network_bytes=network_bytes,
                        record=record,
                    )
                )
            except (requests.RequestException, Urllib3HTTPError, OSError) as exc:
                self._record_attempt("exception", 0)
                self._raise_if_cancelled()
                last_error = f"request failed: {type(exc).__name__}: {exc}"
                if attempts < self.max_attempts:
                    self._sleep_interruptibly(self._retry_delay(attempts, None))
                    continue
                return self._stale_or_failure(
                    target=target,
                    http_status=last_status,
                    attempts=attempts,
                    network_bytes=network_bytes,
                    cached_body=cached_body,
                    cached_record=cached_record,
                    allow_stale_on_error=allow_stale_on_error,
                    error=last_error,
                )
            finally:
                if response is not None:
                    response.close()

        # Defensive: every loop branch above returns or retries.
        return self._stale_or_failure(  # pragma: no cover
            target=target,
            http_status=last_status,
            attempts=attempts,
            network_bytes=network_bytes,
            cached_body=cached_body,
            cached_record=cached_record,
            allow_stale_on_error=allow_stale_on_error,
            error=last_error or "unknown transport failure",
        )
