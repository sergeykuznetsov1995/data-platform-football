"""Raw-first, endpoint-resumable SofaScore capture engine.

This module is transport- and Iceberg-adapter agnostic.  A runner supplies one
warmed transport and one manifest/sink implementation; DAG, CLI and backfill can
therefore share the exact same fetch/replay/state machinery.
"""

from __future__ import annotations

import email.utils
import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import timezone
from typing import Callable, Mapping, Optional, Protocol, Sequence

from scrapers.sofascore.manifest import (
    EndpointManifest,
    ManifestKey,
    ManifestStatus,
    ManifestStore,
    utc_now_iso,
)
from scrapers.sofascore.raw_store import (
    PayloadTarget,
    RawPayloadNotFound,
    RawPayloadRecord,
    RawPayloadSchemaError,
    RawPayloadStore,
)
from scrapers.utils.rate_limiter import RateLimiter, get_rate_limiter
from scripts.proxy_filter.budget import (
    BudgetAccountingError,
    ProductionBudgetUnavailable,
    ProxyBudgetExceeded,
    SharedBudgetLedger,
)


class SchemaValidationError(ValueError):
    """The source JSON no longer satisfies an endpoint's asserted contract."""


class OfflineReplayMiss(RuntimeError):
    """Offline mode was requested but no usable successful raw payload exists."""


class TransportError(RuntimeError):
    """A metered transport failure.

    Paid transports must always provide ``provider_bytes`` (including zero), so
    the run ledger remains correct even when navigation/fetch raises.
    """

    def __init__(
        self,
        message: str,
        *,
        provider_bytes: Optional[int],
        retryable: bool = True,
        browser_sessions: int = 0,
        navigations: int = 0,
        source_requests: int = 1,
    ) -> None:
        super().__init__(message)
        self.provider_bytes = provider_bytes
        self.retryable = retryable
        self.browser_sessions = browser_sessions
        self.navigations = navigations
        self.source_requests = source_requests
        if browser_sessions < 0 or navigations < 0 or source_requests < 0:
            raise ValueError("transport metrics must be non-negative")


@dataclass(frozen=True)
class ProviderBudgetToken:
    run_id: str
    endpoint: str
    reservation_token: str
    max_provider_bytes: int


@dataclass(frozen=True)
class HttpPayload:
    status_code: int
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)
    provider_bytes: Optional[int] = None
    browser_sessions: int = 0
    navigations: int = 0
    source_requests: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.body, bytes):
            raise TypeError("HTTP body must be exact bytes")
        if self.provider_bytes is not None and self.provider_bytes < 0:
            raise ValueError("provider_bytes must be non-negative")
        if (
            self.browser_sessions < 0
            or self.navigations < 0
            or self.source_requests < 0
        ):
            raise ValueError("transport metrics must be non-negative")


class CaptureTransport(Protocol):
    def request(
        self,
        url: str,
        *,
        provider_budget: Optional[ProviderBudgetToken],
    ) -> HttpPayload: ...


@dataclass(frozen=True)
class ParsedDataset:
    """Rows from one parser fed by the shared raw JSON object."""

    name: str
    rows: Sequence[Mapping[str, object]]

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("dataset name must not be empty")
        if isinstance(self.rows, (str, bytes, bytearray, Mapping)):
            raise TypeError("dataset rows must be a sequence of row mappings")
        if any(not isinstance(row, Mapping) for row in self.rows):
            raise TypeError("every parsed row must be a mapping")

    @property
    def row_count(self) -> int:
        return len(self.rows)


Parser = Callable[[object], Sequence[Mapping[str, object]]]
SchemaValidator = Callable[[object], object]
EmptyPredicate = Callable[[object], bool]


class CaptureSink(Protocol):
    def write(
        self,
        key: ManifestKey,
        datasets: Mapping[str, ParsedDataset],
        raw: RawPayloadRecord,
    ) -> None: ...


class NullCaptureSink:
    def write(
        self,
        key: ManifestKey,
        datasets: Mapping[str, ParsedDataset],
        raw: RawPayloadRecord,
    ) -> None:
        return None


@dataclass(frozen=True)
class EndpointSpec:
    """Validation, materialization and transport policy for one exact payload.

    ``raw_only`` is an explicit coverage outcome, not a placeholder parser.
    Valid non-empty responses commit exact raw lineage as terminal ``success``
    with one retained-payload unit; no dataset is created and the sink is not
    called. Existing normalized specs remain parser-required.
    """

    key: ManifestKey
    url: str
    schema_validator: SchemaValidator
    empty_predicate: EmptyPredicate
    parsers: Mapping[str, Parser]
    paid_proxy: bool = True
    supported: bool = True
    unsupported_reason: Optional[str] = None
    not_supported_http_statuses: tuple[int, ...] = (404,)
    legitimate_empty_http_statuses: tuple[int, ...] = (204,)
    raw_only: bool = False

    def __post_init__(self) -> None:
        if not self.url.strip():
            raise ValueError("endpoint URL must not be empty")
        if not callable(self.schema_validator) or not callable(self.empty_predicate):
            raise TypeError("every endpoint needs explicit schema and empty validators")
        if self.raw_only and not self.supported:
            raise ValueError("raw-only endpoints must be supported")
        if self.raw_only and self.parsers:
            raise ValueError("raw-only endpoints must not declare parsers")
        if self.supported and not self.parsers and not self.raw_only:
            raise ValueError("supported endpoints need at least one parser")
        if not self.supported and not (self.unsupported_reason or "").strip():
            raise ValueError("unsupported endpoints need an explicit reason")
        if any(not name.strip() or not callable(parser) for name, parser in self.parsers.items()):
            raise ValueError("parser names must be non-empty and parsers callable")

    @property
    def raw_target(self) -> PayloadTarget:
        return PayloadTarget(**self.key.__dict__)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("retry delays must be non-negative")

    def delay(self, attempt: int, headers: Mapping[str, str], now: float) -> float:
        retry_after = next(
            (value for key, value in headers.items() if key.lower() == "retry-after"),
            None,
        )
        if retry_after is not None:
            try:
                return min(self.max_delay_seconds, max(0.0, float(retry_after)))
            except ValueError:
                try:
                    parsed = email.utils.parsedate_to_datetime(retry_after)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    return min(
                        self.max_delay_seconds,
                        max(0.0, parsed.timestamp() - now),
                    )
                except (TypeError, ValueError, OverflowError):
                    pass
        return min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** max(0, attempt - 1)),
        )


@dataclass(frozen=True)
class CaptureResult:
    manifest: EndpointManifest
    datasets: Mapping[str, ParsedDataset] = field(default_factory=dict)
    raw: Optional[RawPayloadRecord] = None
    cache_hit: bool = False
    replay_hit: bool = False
    network_used: bool = False


class CaptureMetrics:
    """Thread-safe run metrics; no-op values are exact integer zeroes."""

    def __init__(self, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._monotonic = monotonic
        self._started_at = monotonic()
        self.endpoints = 0
        self.network_requests = 0
        self.source_requests = 0
        self.provider_bytes = 0
        self.endpoint_provider_bytes: dict[str, int] = {}
        self.endpoint_request_provider_bytes: dict[str, list[int]] = {}
        self.browser_sessions = 0
        self.navigations = 0
        self.cache_hits = 0
        self.replay_hits = 0
        self.completed_rows = 0
        self.status_counts = {status.value: 0 for status in ManifestStatus}
        self._completed_matches: set[str] = set()
        self._completed_players: set[str] = set()
        self._durations_ms: list[int] = []
        self._lock = threading.Lock()

    def endpoint(self) -> None:
        with self._lock:
            self.endpoints += 1

    def _provider_observation(
        self,
        endpoint: str,
        provider_bytes: Optional[int],
    ) -> None:
        if provider_bytes is None:
            return
        amount = int(provider_bytes)
        self.endpoint_provider_bytes[endpoint] = (
            self.endpoint_provider_bytes.get(endpoint, 0) + amount
        )
        self.endpoint_request_provider_bytes.setdefault(endpoint, []).append(amount)

    def response(self, endpoint: str, payload: HttpPayload) -> None:
        with self._lock:
            self.network_requests += 1
            self.source_requests += payload.source_requests
            self.provider_bytes += int(payload.provider_bytes or 0)
            self._provider_observation(endpoint, payload.provider_bytes)
            self.browser_sessions += payload.browser_sessions
            self.navigations += payload.navigations

    def transport_error(self, endpoint: str, error: TransportError) -> None:
        with self._lock:
            self.network_requests += 1
            self.source_requests += error.source_requests
            self.provider_bytes += int(error.provider_bytes or 0)
            self._provider_observation(endpoint, error.provider_bytes)
            self.browser_sessions += error.browser_sessions
            self.navigations += error.navigations

    def finish(self, result: CaptureResult, duration_ms: int) -> None:
        with self._lock:
            self.cache_hits += int(result.cache_hit)
            self.replay_hits += int(result.replay_hit)
            self.completed_rows += result.manifest.row_count
            self.status_counts[result.manifest.status.value] += 1
            if result.manifest.is_terminal:
                if result.manifest.key.target_type == "event":
                    self._completed_matches.add(result.manifest.key.target_id)
                elif result.manifest.key.target_type == "player":
                    self._completed_players.add(result.manifest.key.target_id)
            self._durations_ms.append(duration_ms)

    @staticmethod
    def _percentile(values: list[int], percentile: float) -> int:
        if not values:
            return 0
        ordered = sorted(values)
        return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]

    def snapshot(self) -> dict:
        with self._lock:
            completed = self.endpoints or 1
            elapsed = max(0.0, self._monotonic() - self._started_at)
            return {
                "endpoints": self.endpoints,
                "request_count": self.network_requests,
                "endpoint_request_count": self.network_requests,
                "source_request_count": self.source_requests,
                "paid_proxy_bytes": self.provider_bytes,
                "paid_proxy_mb": self.provider_bytes / 1_048_576,
                "endpoint_provider_bytes": dict(self.endpoint_provider_bytes),
                "endpoint_request_provider_bytes": {
                    endpoint: list(values)
                    for endpoint, values in self.endpoint_request_provider_bytes.items()
                },
                "browser_sessions": self.browser_sessions,
                "navigations": self.navigations,
                "cache_hits": self.cache_hits,
                "replay_hits": self.replay_hits,
                "cache_hit_rate": (self.cache_hits + self.replay_hits) / completed,
                "replay_hit_rate": self.replay_hits / completed,
                "row_count": self.completed_rows,
                "completed_matches": len(self._completed_matches),
                "completed_players": len(self._completed_players),
                "elapsed_seconds": elapsed,
                "matches_per_second": (
                    len(self._completed_matches) / elapsed if elapsed else 0.0
                ),
                "players_per_second": (
                    len(self._completed_players) / elapsed if elapsed else 0.0
                ),
                "status_counts": dict(self.status_counts),
                "endpoint_completeness": (
                    sum(
                        self.status_counts[name]
                        for name in (
                            ManifestStatus.SUCCESS.value,
                            ManifestStatus.LEGITIMATE_EMPTY.value,
                            ManifestStatus.NOT_SUPPORTED.value,
                        )
                    )
                    / completed
                ),
                "p50_duration_ms": self._percentile(self._durations_ms, 0.50),
                "p95_duration_ms": self._percentile(self._durations_ms, 0.95),
            }


class SofaScoreCaptureEngine:
    def __init__(
        self,
        *,
        raw_store: RawPayloadStore,
        manifest_store: ManifestStore,
        transport: CaptureTransport,
        run_id: str,
        task_id: str,
        sink: Optional[CaptureSink] = None,
        budget: Optional[SharedBudgetLedger] = None,
        rate_limiter: Optional[RateLimiter] = None,
        retry_policy: Optional[RetryPolicy] = None,
        max_workers: int = 4,
        sleep: Callable[[float], None] = time.sleep,
        wall_time: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.raw_store = raw_store
        self.manifest_store = manifest_store
        self.transport = transport
        self.run_id = str(run_id).strip()
        self.task_id = str(task_id).strip()
        if not self.run_id or not self.task_id:
            raise ValueError("run_id and task_id must not be empty")
        self.sink = sink or NullCaptureSink()
        self.budget = budget
        self.rate_limiter = rate_limiter or get_rate_limiter("sofascore")
        self.retry_policy = retry_policy or RetryPolicy()
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self.max_workers = max_workers
        self.sleep = sleep
        self.wall_time = wall_time
        self.monotonic = monotonic
        self.metrics = CaptureMetrics(monotonic)

    def _record(
        self,
        spec: EndpointSpec,
        *,
        status: ManifestStatus,
        attempts: int,
        row_count: int = 0,
        response: Optional[HttpPayload] = None,
        raw: Optional[RawPayloadRecord] = None,
        error: Optional[BaseException] = None,
        duration_ms: int = 0,
        provider_bytes: int = 0,
        parsed: bool = False,
    ) -> EndpointManifest:
        previous = self.manifest_store.get(spec.key)
        # Offline/raw replay must not erase the provider traffic provenance of
        # the network capture that produced the immutable blob. Metrics still
        # count replay as zero traffic; only the persisted endpoint observation
        # retains its original provider bytes.
        persisted_provider_bytes = max(0, provider_bytes)
        if (
            response is None
            and persisted_provider_bytes == 0
            and previous is not None
            and previous.provider_bytes > 0
        ):
            persisted_provider_bytes = previous.provider_bytes
        record = EndpointManifest(
            key=spec.key,
            status=status,
            run_id=self.run_id,
            task_id=self.task_id,
            attempts=attempts,
            row_count=row_count,
            http_status=response.status_code if response else (raw.http_status if raw else None),
            raw_content_hash=raw.content_hash if raw else None,
            raw_blob_key=raw.blob_key if raw else None,
            request_url=raw.request_url if raw else spec.url,
            error_type=type(error).__name__ if error else None,
            error_message=str(error)[:4000] if error else None,
            duration_ms=max(0, duration_ms),
            provider_bytes=persisted_provider_bytes,
            fetched_at=raw.fetched_at if raw else None,
            parsed_at=utc_now_iso() if parsed else None,
        )
        self.manifest_store.upsert(record)
        return record

    def _finish_result(self, result: CaptureResult, started: float) -> CaptureResult:
        self.metrics.finish(result, max(0, int((self.monotonic() - started) * 1000)))
        return result

    def authorize_request(
        self,
        spec: EndpointSpec,
    ) -> Optional[ProviderBudgetToken]:
        """Authorize and pace one actual source request before it starts.

        Batch runners call this before an already-warmed Camoufox session issues
        each endpoint request, then pass the token to :meth:`ingest_prefetched`.
        This preserves the hard budget even when capture and materialization are
        separated in time.
        """
        existing = self.manifest_store.get(spec.key)
        if existing and existing.is_terminal:
            raise ValueError("terminal endpoint must be resumed without source access")
        token: Optional[str] = None
        maximum: Optional[int] = None
        if spec.paid_proxy:
            if self.budget is None:
                raise ProductionBudgetUnavailable(
                    "paid SofaScore capture requires a verified shared proxy budget"
                )
            token, maximum = self.budget.reserve(self.run_id, spec.key.endpoint)
        # A browser transport sees navigation and passive/exact XHRs that the
        # endpoint engine cannot enumerate. It owns pacing for those real HTTP
        # requests; applying this logical limiter too would double-throttle each
        # exact JSON endpoint after it had already reserved its budget token.
        transport_paces = bool(getattr(self.transport, "paces_requests", False))
        if not transport_paces and not self.rate_limiter.acquire():
            if spec.paid_proxy and token is not None:
                self.budget.cancel(self.run_id, token)
            raise TransportError(
                "SofaScore rate limiter refused a request",
                provider_bytes=0,
                source_requests=0,
            )
        if not spec.paid_proxy:
            return None
        return ProviderBudgetToken(
            self.run_id,
            spec.key.endpoint,
            token,
            maximum,
        )

    def _finish_authorized_response(
        self,
        spec: EndpointSpec,
        authorization: Optional[ProviderBudgetToken],
        provider_bytes: Optional[int],
    ) -> int:
        if spec.paid_proxy:
            if self.budget is None:
                raise ProductionBudgetUnavailable(
                    "paid SofaScore capture requires a verified shared proxy budget"
                )
            if authorization is None:
                raise BudgetAccountingError("paid response has no prior budget authorization")
            if (
                authorization.run_id != self.run_id
                or authorization.endpoint != spec.key.endpoint
            ):
                raise BudgetAccountingError("provider budget token target mismatch")
            if provider_bytes is None:
                raise BudgetAccountingError(
                    "paid transport response omitted real provider bytes"
                )
            return self.budget.finish(
                self.run_id,
                authorization.reservation_token,
                reported_provider_bytes=provider_bytes,
            )
        if authorization is not None:
            raise BudgetAccountingError("direct endpoint received a paid budget token")
        if provider_bytes not in (None, 0):
            raise BudgetAccountingError(
                "direct endpoint unexpectedly reported paid provider traffic"
            )
        return 0

    def _request(self, spec: EndpointSpec) -> HttpPayload:
        transport_budget = self.authorize_request(spec)
        try:
            response = self.transport.request(spec.url, provider_budget=transport_budget)
        except TransportError as exc:
            self.metrics.transport_error(spec.key.endpoint, exc)
            if spec.paid_proxy:
                if exc.provider_bytes is None:
                    # Unknown paid traffic cannot be retried safely: the shared
                    # budget might already have been spent outside the ledger.
                    raise BudgetAccountingError(
                        "paid transport failure omitted provider-byte accounting"
                    ) from exc
                self._finish_authorized_response(
                    spec, transport_budget, exc.provider_bytes
                )
            raise
        except Exception:
            if spec.paid_proxy and transport_budget is not None:
                # Only a transport-level typed error can prove whether paid
                # bytes moved. Leave the reservation outstanding, fail closed.
                raise BudgetAccountingError(
                    "untyped paid transport failure; reservation retained for audit"
                )
            raise
        self._finish_authorized_response(
            spec, transport_budget, response.provider_bytes
        )
        self.metrics.response(spec.key.endpoint, response)
        return response

    @staticmethod
    def _decode_json(body: bytes) -> object:
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawPayloadSchemaError("response is not valid UTF-8 JSON") from exc

    @staticmethod
    def _validate(spec: EndpointSpec, payload: object) -> None:
        result = spec.schema_validator(payload)
        if result is False:
            raise SchemaValidationError(
                f"schema validator rejected endpoint {spec.key.endpoint!r}"
            )

    @staticmethod
    def _parse(spec: EndpointSpec, payload: object) -> dict[str, ParsedDataset]:
        datasets: dict[str, ParsedDataset] = {}
        for name, parser in spec.parsers.items():
            rows = parser(payload)
            datasets[name] = ParsedDataset(name=name, rows=rows)
        return datasets

    def _materialize(
        self,
        spec: EndpointSpec,
        body: bytes,
        raw: RawPayloadRecord,
        *,
        attempts: int,
        response: Optional[HttpPayload],
        provider_bytes: int,
        started: float,
        replay_hit: bool,
    ) -> CaptureResult:
        def elapsed() -> int:
            return max(0, int((self.monotonic() - started) * 1000))
        try:
            payload = self._decode_json(body)
            self._validate(spec, payload)
            if spec.empty_predicate(payload):
                manifest = self._record(
                    spec,
                    status=ManifestStatus.LEGITIMATE_EMPTY,
                    attempts=attempts,
                    response=response,
                    raw=raw,
                    duration_ms=elapsed(),
                    provider_bytes=provider_bytes,
                    parsed=True,
                )
                return CaptureResult(
                    manifest=manifest,
                    raw=raw,
                    replay_hit=replay_hit,
                    network_used=response is not None,
                )
            if spec.raw_only:
                # The existing manifest contract requires success.row_count > 0.
                # Count one retained payload unit, but deliberately create no
                # ParsedDataset and never invoke the normalized-table sink.
                manifest = self._record(
                    spec,
                    status=ManifestStatus.SUCCESS,
                    attempts=attempts,
                    row_count=1,
                    response=response,
                    raw=raw,
                    duration_ms=elapsed(),
                    provider_bytes=provider_bytes,
                    parsed=True,
                )
                return CaptureResult(
                    manifest=manifest,
                    raw=raw,
                    replay_hit=replay_hit,
                    network_used=response is not None,
                )
            datasets = self._parse(spec, payload)
            row_count = sum(dataset.row_count for dataset in datasets.values())
            if row_count <= 0:
                raise SchemaValidationError(
                    "non-empty payload produced zero normalized rows; refusing silent loss"
                )
        except Exception as exc:
            manifest = self._record(
                spec,
                status=ManifestStatus.SCHEMA_ERROR,
                attempts=attempts,
                response=response,
                raw=raw,
                error=exc,
                duration_ms=elapsed(),
                provider_bytes=provider_bytes,
                parsed=True,
            )
            return CaptureResult(
                manifest=manifest,
                raw=raw,
                replay_hit=replay_hit,
                network_used=response is not None,
            )

        try:
            self.sink.write(spec.key, datasets, raw)
        except Exception as exc:
            # Raw exists and is valid: a retry replays it without another request.
            manifest = self._record(
                spec,
                status=ManifestStatus.RETRYABLE_FAILURE,
                attempts=attempts,
                row_count=row_count,
                response=response,
                raw=raw,
                error=exc,
                duration_ms=elapsed(),
                provider_bytes=provider_bytes,
                parsed=True,
            )
            return CaptureResult(
                manifest=manifest,
                datasets=datasets,
                raw=raw,
                replay_hit=replay_hit,
                network_used=response is not None,
            )

        manifest = self._record(
            spec,
            status=ManifestStatus.SUCCESS,
            attempts=attempts,
            row_count=row_count,
            response=response,
            raw=raw,
            duration_ms=elapsed(),
            provider_bytes=provider_bytes,
            parsed=True,
        )
        return CaptureResult(
            manifest=manifest,
            datasets=datasets,
            raw=raw,
            replay_hit=replay_hit,
            network_used=response is not None,
        )

    def capture(
        self,
        spec: EndpointSpec,
        *,
        offline: bool = False,
        force_replay: bool = False,
    ) -> CaptureResult:
        """Capture or replay one exact endpoint.

        Terminal manifest rows skip source access.  Non-terminal rows with a
        successful raw response replay that blob first (notably after an Iceberg
        failure or parser deployment), so a payload is never paid for twice.
        """
        started = self.monotonic()
        self.metrics.endpoint()
        existing = self.manifest_store.get(spec.key)
        attempts_before = existing.attempts if existing else 0
        if existing and existing.is_terminal and not force_replay:
            return self._finish_result(
                CaptureResult(manifest=existing, cache_hit=True), started
            )
        if existing and existing.is_terminal and force_replay:
            replay_without_json = (
                existing.status == ManifestStatus.NOT_SUPPORTED
                or (
                    existing.status == ManifestStatus.LEGITIMATE_EMPTY
                    and existing.http_status in spec.legitimate_empty_http_statuses
                )
            )
            if replay_without_json:
                raw = None
                try:
                    _, raw = self.raw_store.load_bytes(spec.raw_target)
                except RawPayloadNotFound:
                    if existing.raw_content_hash or existing.raw_blob_key:
                        raise OfflineReplayMiss(
                            "terminal endpoint raw lineage is missing for "
                            f"{spec.key.stable_id()}"
                        ) from None
                if raw is not None:
                    if (
                        raw.http_status != existing.http_status
                        or raw.content_hash != existing.raw_content_hash
                        or raw.blob_key != existing.raw_blob_key
                    ):
                        raise RawPayloadSchemaError(
                            "terminal endpoint manifest/raw lineage mismatch for "
                            f"{spec.key.stable_id()}"
                        )
                if existing.status == ManifestStatus.NOT_SUPPORTED:
                    if spec.supported and (
                        existing.http_status not in spec.not_supported_http_statuses
                    ):
                        raise RawPayloadSchemaError(
                            "saved not-supported HTTP status is not valid for "
                            f"{spec.key.stable_id()}"
                        )
                return self._finish_result(
                    CaptureResult(
                        manifest=existing,
                        raw=raw,
                        replay_hit=True,
                    ),
                    started,
                )
        if not spec.supported:
            manifest = self._record(
                spec,
                status=ManifestStatus.NOT_SUPPORTED,
                attempts=attempts_before,
                error=RuntimeError(spec.unsupported_reason),
            )
            return self._finish_result(CaptureResult(manifest=manifest), started)

        try:
            body, raw = self.raw_store.load_bytes(spec.raw_target)
        except RawPayloadNotFound:
            body = raw = None
        if raw is not None and raw.http_status in spec.not_supported_http_statuses:
            manifest = self._record(
                spec,
                status=ManifestStatus.NOT_SUPPORTED,
                attempts=attempts_before,
                raw=raw,
                provider_bytes=0,
            )
            return self._finish_result(
                CaptureResult(manifest=manifest, raw=raw, replay_hit=True),
                started,
            )
        if raw is not None and raw.http_status in spec.legitimate_empty_http_statuses:
            manifest = self._record(
                spec,
                status=ManifestStatus.LEGITIMATE_EMPTY,
                attempts=attempts_before,
                raw=raw,
                provider_bytes=0,
            )
            return self._finish_result(
                CaptureResult(manifest=manifest, raw=raw, replay_hit=True),
                started,
            )
        if raw is not None and 200 <= raw.http_status < 300:
            result = self._materialize(
                spec,
                body,
                raw,
                attempts=attempts_before,
                response=None,
                provider_bytes=0,
                started=started,
                replay_hit=True,
            )
            return self._finish_result(result, started)
        if offline:
            raise OfflineReplayMiss(
                f"no successful raw payload for {spec.key.stable_id()}"
            )

        provider_total = 0
        last_response: Optional[HttpPayload] = None
        last_raw: Optional[RawPayloadRecord] = None
        last_error: Optional[BaseException] = None
        for local_attempt in range(1, self.retry_policy.max_attempts + 1):
            attempts = attempts_before + local_attempt
            try:
                response = self._request(spec)
                last_response = response
                provider_total += int(response.provider_bytes or 0)
                raw = self.raw_store.store_bytes(
                    spec.raw_target,
                    response.body,
                    request_url=spec.url,
                    http_status=response.status_code,
                    response_headers=response.headers,
                )
                last_raw = raw
            except (ProductionBudgetUnavailable, ProxyBudgetExceeded, BudgetAccountingError):
                raise
            except TransportError as exc:
                last_error = exc
                provider_total += int(exc.provider_bytes or 0)
                if not exc.retryable or local_attempt == self.retry_policy.max_attempts:
                    break
                self.sleep(
                    self.retry_policy.delay(local_attempt, {}, self.wall_time())
                )
                continue

            status = response.status_code
            if status in {403, 429} or status >= 500:
                last_error = TransportError(
                    f"retryable SofaScore HTTP status {status}",
                    provider_bytes=response.provider_bytes,
                )
                if local_attempt < self.retry_policy.max_attempts:
                    self.sleep(
                        self.retry_policy.delay(
                            local_attempt, response.headers, self.wall_time()
                        )
                    )
                    continue
                break
            if status in spec.not_supported_http_statuses:
                manifest = self._record(
                    spec,
                    status=ManifestStatus.NOT_SUPPORTED,
                    attempts=attempts,
                    response=response,
                    raw=raw,
                    duration_ms=int((self.monotonic() - started) * 1000),
                    provider_bytes=provider_total,
                )
                return self._finish_result(
                    CaptureResult(manifest=manifest, raw=raw, network_used=True),
                    started,
                )
            if status in spec.legitimate_empty_http_statuses:
                manifest = self._record(
                    spec,
                    status=ManifestStatus.LEGITIMATE_EMPTY,
                    attempts=attempts,
                    response=response,
                    raw=raw,
                    duration_ms=int((self.monotonic() - started) * 1000),
                    provider_bytes=provider_total,
                )
                return self._finish_result(
                    CaptureResult(manifest=manifest, raw=raw, network_used=True),
                    started,
                )
            if not 200 <= status < 300:
                last_error = TransportError(
                    f"unexpected SofaScore HTTP status {status}",
                    provider_bytes=response.provider_bytes,
                    retryable=False,
                )
                break

            result = self._materialize(
                spec,
                response.body,
                raw,
                attempts=attempts,
                response=response,
                provider_bytes=provider_total,
                started=started,
                replay_hit=False,
            )
            return self._finish_result(result, started)

        attempts = attempts_before + min(
            self.retry_policy.max_attempts,
            max(1, (last_response is not None) + (last_error is not None)),
        )
        # The exact cumulative attempts are more useful than the expression above
        # when retries occurred; derive it from the number of requests observed in
        # this endpoint loop by tracking the previous count.
        attempts = attempts_before + local_attempt
        manifest = self._record(
            spec,
            status=ManifestStatus.RETRYABLE_FAILURE,
            attempts=attempts,
            response=last_response,
            raw=last_raw,
            error=last_error or RuntimeError("endpoint capture failed"),
            duration_ms=int((self.monotonic() - started) * 1000),
            provider_bytes=provider_total,
        )
        return self._finish_result(
            CaptureResult(manifest=manifest, raw=last_raw, network_used=True),
            started,
        )

    def ingest_prefetched(
        self,
        spec: EndpointSpec,
        response: HttpPayload,
        *,
        authorization: Optional[ProviderBudgetToken],
    ) -> CaptureResult:
        """Commit one response fetched by a shared warmed session.

        The runner must call :meth:`authorize_request` immediately before the
        source request. No transport method is invoked here. Exact bytes are
        committed raw first, then parsed and written once; the returned result
        carries the raw record for endpoint→lineage maps.
        """
        started = self.monotonic()
        self.metrics.endpoint()
        existing = self.manifest_store.get(spec.key)
        if existing and existing.is_terminal:
            raise ValueError("refusing prefetched bytes for an already terminal endpoint")
        if not spec.supported:
            raise ValueError("unsupported endpoint must not be fetched")
        charged = self._finish_authorized_response(
            spec, authorization, response.provider_bytes
        )
        self.metrics.response(spec.key.endpoint, response)
        raw = self.raw_store.store_bytes(
            spec.raw_target,
            response.body,
            request_url=spec.url,
            http_status=response.status_code,
            response_headers=response.headers,
        )
        attempts = (existing.attempts if existing else 0) + 1
        status = response.status_code
        if status in {403, 429} or status >= 500:
            error = TransportError(
                f"retryable SofaScore HTTP status {status}",
                provider_bytes=response.provider_bytes,
            )
            manifest = self._record(
                spec,
                status=ManifestStatus.RETRYABLE_FAILURE,
                attempts=attempts,
                response=response,
                raw=raw,
                error=error,
                duration_ms=int((self.monotonic() - started) * 1000),
                provider_bytes=charged,
            )
            return self._finish_result(
                CaptureResult(
                    manifest=manifest,
                    raw=raw,
                    network_used=True,
                ),
                started,
            )
        if status in spec.not_supported_http_statuses:
            manifest = self._record(
                spec,
                status=ManifestStatus.NOT_SUPPORTED,
                attempts=attempts,
                response=response,
                raw=raw,
                duration_ms=int((self.monotonic() - started) * 1000),
                provider_bytes=charged,
            )
            return self._finish_result(
                CaptureResult(manifest=manifest, raw=raw, network_used=True),
                started,
            )
        if status in spec.legitimate_empty_http_statuses:
            manifest = self._record(
                spec,
                status=ManifestStatus.LEGITIMATE_EMPTY,
                attempts=attempts,
                response=response,
                raw=raw,
                duration_ms=int((self.monotonic() - started) * 1000),
                provider_bytes=charged,
            )
            return self._finish_result(
                CaptureResult(manifest=manifest, raw=raw, network_used=True),
                started,
            )
        if not 200 <= status < 300:
            error = TransportError(
                f"unexpected SofaScore HTTP status {status}",
                provider_bytes=response.provider_bytes,
                retryable=False,
            )
            manifest = self._record(
                spec,
                status=ManifestStatus.RETRYABLE_FAILURE,
                attempts=attempts,
                response=response,
                raw=raw,
                error=error,
                duration_ms=int((self.monotonic() - started) * 1000),
                provider_bytes=charged,
            )
            return self._finish_result(
                CaptureResult(manifest=manifest, raw=raw, network_used=True),
                started,
            )
        result = self._materialize(
            spec,
            response.body,
            raw,
            attempts=attempts,
            response=response,
            provider_bytes=charged,
            started=started,
            replay_hit=False,
        )
        return self._finish_result(result, started)

    def capture_many(
        self,
        specs: Sequence[EndpointSpec],
        *,
        offline: bool = False,
        force_replay: bool = False,
    ) -> list[CaptureResult]:
        """Bounded concurrent capture with deterministic result ordering."""
        keys = [spec.key for spec in specs]
        if len(set(keys)) != len(keys):
            raise ValueError("capture_many received duplicate endpoint manifest keys")
        if not specs:
            return []
        results: list[Optional[CaptureResult]] = [None] * len(specs)
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(specs))) as pool:
            futures = {
                pool.submit(
                    self.capture,
                    spec,
                    offline=offline,
                    force_replay=force_replay,
                ): index
                for index, spec in enumerate(specs)
            }
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        return [result for result in results if result is not None]
