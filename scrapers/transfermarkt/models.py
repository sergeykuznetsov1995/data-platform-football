"""Typed transport contracts for the Transfermarkt scraper.

The legacy scraper used ``None`` for every kind of endpoint result.  That made
a schema-valid empty collection indistinguishable from a burned proxy, an
unexpected 404, a malformed payload, or an exhausted retry loop.  These small
models keep that distinction explicit without coupling the HTTP client to
pandas or the Bronze writers.
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Dict, Generic, Mapping, Optional, TypeVar


T = TypeVar("T")


HARD_PROVIDER_BYTE_BUDGET = 15_728_640
"""Discovery-contour provider-metered ceiling (15 MiB).

The registry-discovery pipeline pins this pair explicitly; the production
ingest contour uses the ``SCOPE_*``/``PARENT_DAILY_*`` canon below.
"""

SOFT_PROVIDER_BYTE_STOP = 14_680_064
"""Discovery-contour soft stop before the 15 MiB ceiling (14 MiB)."""


# ---------------------------------------------------------------------------
# Production ingest budget canon.
#
# This module is the single source for every paid-traffic number in the
# Transfermarkt production contour: the entity runner, the scope-cycle
# wrapper, the ingest DAG, the approval-preparation script, the benchmark
# and the committed standing policy all import these values.  It is
# deliberately stdlib-only so any of them can import it cheaply.
# ---------------------------------------------------------------------------

SCOPE_HARD_PROVIDER_BYTE_CAP = 25_165_824
"""Hard provider-metered cap for ONE exact scope cycle (24 MiB).

Sized at 1.2x the measured upper estimate of a cold big-league scope
(~18-21 MiB provider-metered across its four entities).
"""

SCOPE_SOFT_PROVIDER_BYTE_STOP = 23_068_672
"""Graceful per-scope stop (22 MiB): no new paid request starts past it."""

PARENT_DAILY_HARD_PROVIDER_BYTE_CAP = 88_080_384
"""Hard provider-metered cap for one parent (daily) cycle (84 MiB).

The external proxy-filter allowance is 100 MB/day (~95.4 MiB); this keeps a
~12% reserve under it.  Raising the external allowance (e.g. to 400 MB/day)
means editing this pair AND ``EXTERNAL_DAILY_PROVIDER_BYTE_LIMIT`` (the
import-time canon assert bounds the pair by it), plus the proxy-filter's own
per-DagRun cap in the deployment.  Evidence already committed under an older,
smaller pair stays valid: readiness reads the persisted ledger caps as a
ceiling, not as an equality.
"""

PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP = 83_886_080
"""Graceful parent-cycle stop (80 MiB) before the daily hard cap."""

EXTERNAL_DAILY_PROVIDER_BYTE_LIMIT = 100_000_000
"""Documented external proxy-filter allowance (100 MB/day)."""

SCOPE_REQUEST_LIMIT = 1_610
"""Attempt ceiling for one scope cycle (150 + 650 + 650 + 160)."""

SCOPE_RETRY_LIMIT = 800
"""Paid-retry ceiling for one scope cycle.

Measured ~0.45 retries/page; in bytes 800 x ~10 KiB = ~7.8 MiB, well under
the scope byte cap, so bytes remain the binding constraint.  Note the
per-entity request budgets are deliberately below the bad-wave worst case:
a full 500-career window at 0.45 retries/page needs ~500 + 225 = 725
attempts, over the 650 budgeted for one career entity — a wave that bad does
not finish in one cycle and the remainder is picked up by the next cycle
through the pending checkpoint, by design.
"""

DEFAULT_ENTITY_TIMEOUT_SECONDS = 3_600
"""Subprocess timeout for the small entities (players, coaches)."""

CAREER_ENTITY_TIMEOUT_SECONDS = 5_400
"""Subprocess timeout for the career entities (market value / transfers).

The client is rate-limited to 12 requests/minute (5 s spacing), so a full
650-attempt career budget alone waits 650 x 5 s = 3250 s; parsing, retry
backoff and checkpoint I/O add roughly 1.4 s per attempt, and a 1.3x margin
keeps a slow wave from dying in TimeoutExpired (which burns attempt-guard
budget without result evidence): 650 x (5 + 1.4) x 1.3 = 5408 -> 5400 s.
Players (150 attempts) and coaches (160) stay on 3600 s.
"""

ENTITY_TIMEOUT_SECONDS: Dict[str, int] = {
    'players': DEFAULT_ENTITY_TIMEOUT_SECONDS,
    'market_value_history': CAREER_ENTITY_TIMEOUT_SECONDS,
    'transfers': CAREER_ENTITY_TIMEOUT_SECONDS,
    'coaches': DEFAULT_ENTITY_TIMEOUT_SECONDS,
}

SCOPE_OPS_OVERHEAD_SECONDS = 900
"""Wall clock one scope cycle needs outside its four entity subprocesses.

Approval consumption, the scope-manifest and proxy-ledger MERGEs, checkpoint
and evidence fsyncs.
"""

SCOPE_WALL_CLOCK_TIMEOUT_SECONDS = (
    sum(ENTITY_TIMEOUT_SECONDS.values()) + SCOPE_OPS_OVERHEAD_SECONDS
)
"""Execution timeout of one mapped scope task (3600+5400+5400+3600+900).

Derived, never a literal: an execution timeout below the sum of the entity
timeouts it supervises means Airflow SIGKILLs a runner in the middle of paid
I/O, losing the attempt-guard write and the entity's evidence.  A realistic
full-budget scope finishes in ~2 h 52 m; this 5 h 15 m ceiling only has to be
worst-case compatible.
"""

MAX_SCOPE_BATCH = 8
"""Most scope cycles one parent (daily) cycle may map."""

PARENT_REQUEST_LIMIT = MAX_SCOPE_BATCH * SCOPE_REQUEST_LIMIT
"""Parent-cycle attempt ceiling, derived — never hand-edited."""

PARENT_RETRY_LIMIT = MAX_SCOPE_BATCH * SCOPE_RETRY_LIMIT
"""Parent-cycle paid-retry ceiling, derived — never hand-edited."""

MAX_ROSTER_WINDOW = 500
"""Most careers (market value / transfers) one scope cycle may buy."""

PROVIDER_GRANT_ENV_VAR = 'TM_PROVIDER_BYTE_BUDGET'
"""Per-entity provider-byte grant the runner exports to its client."""

PROVIDER_GRANT_SOFT_MARGIN_BYTES = 1_048_576
"""Soft-stop headroom (1 MiB) inside a per-entity provider grant."""

PROVIDER_GRANT_FLOOR_BYTES = 65_536
"""Smallest usable per-entity provider grant (64 KiB).

A grant below one page's worth of provider bytes would build a degenerate
ledger whose very first response pierces it; the client refuses such a grant
before any I/O instead.
"""

PRODUCTION_ENTITY_BUDGETS: Dict[str, Dict[str, Any]] = {
    # 'requests' counts attempts, not pages; 'provider_reserve_bytes' is the
    # amount reserved in the shared scope ledger in PROVIDER-metered bytes —
    # the same currency the entity later settles with.
    #
    # The reserves deliberately over-subscribe the scope cap (10 + 6 + 8 + 8
    # = 32 MiB > 24 MiB): each reservation is min'ed to the ledger remainder,
    # so early entities get their full ask while the last one (coaches) may
    # receive only what is left — and when nothing is left the runner fails
    # closed with a RuntimeError before any paid I/O.
    'players': {
        'decoded_mb': 16.0, 'requests': 150,
        'provider_reserve_bytes': 10_485_760,  # 16 MiB decoded x ~0.57
    },
    'market_value_history': {
        'decoded_mb': 4.0, 'requests': 650,
        'provider_reserve_bytes': 6_291_456,  # 650 pages x ~8 KiB + margin
    },
    'transfers': {
        'decoded_mb': 12.0, 'requests': 650,
        'provider_reserve_bytes': 8_388_608,  # 650 pages x ~10 KiB + margin
    },
    'coaches': {
        'decoded_mb': 14.0, 'requests': 160,
        'provider_reserve_bytes': 8_388_608,  # 14 MiB decoded x ~0.57
    },
}


def _assert_budget_canon() -> None:
    """Fail the import when the budget canon is internally inconsistent."""

    if not (
        0
        < SCOPE_SOFT_PROVIDER_BYTE_STOP
        < SCOPE_HARD_PROVIDER_BYTE_CAP
        <= PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP
        < PARENT_DAILY_HARD_PROVIDER_BYTE_CAP
        <= EXTERNAL_DAILY_PROVIDER_BYTE_LIMIT
    ):
        raise AssertionError(
            'Transfermarkt provider byte budgets must satisfy '
            'scope soft < scope hard <= parent soft < parent hard '
            '<= external daily limit'
        )
    for entity, budget in PRODUCTION_ENTITY_BUDGETS.items():
        if not 0 < int(budget['provider_reserve_bytes']) < SCOPE_HARD_PROVIDER_BYTE_CAP:
            raise AssertionError(
                f'{entity} provider reserve must fit inside one scope cap'
            )
    if not (
        0
        < PROVIDER_GRANT_FLOOR_BYTES
        <= PROVIDER_GRANT_SOFT_MARGIN_BYTES
        < min(
            int(budget['provider_reserve_bytes'])
            for budget in PRODUCTION_ENTITY_BUDGETS.values()
        )
    ):
        raise AssertionError(
            'provider grant floor/margin must fit inside every entity reserve'
        )
    if sum(
        int(budget['requests']) for budget in PRODUCTION_ENTITY_BUDGETS.values()
    ) != SCOPE_REQUEST_LIMIT:
        raise AssertionError(
            'entity request budgets must sum to SCOPE_REQUEST_LIMIT'
        )
    if set(ENTITY_TIMEOUT_SECONDS) != set(PRODUCTION_ENTITY_BUDGETS):
        raise AssertionError('every entity needs exactly one timeout')
    if (
        sum(ENTITY_TIMEOUT_SECONDS.values())
        > SCOPE_WALL_CLOCK_TIMEOUT_SECONDS
    ):
        raise AssertionError(
            'the scope task timeout must cover every entity timeout it '
            'supervises'
        )


_assert_budget_canon()


def stable_payload_hash(value: Any) -> str:
    """Return the stable content hash used by caches and checkpoints."""

    if isinstance(value, str):
        raw = value.encode("utf-8", errors="replace")
    else:
        raw = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class FetchStatus(str, Enum):
    """Terminal state of one logical endpoint fetch."""

    OK = "ok"
    AUTHORITATIVE_EMPTY = "authoritative_empty"
    NOT_APPLICABLE = "not_applicable"
    # Kept for persisted checkpoints produced before native-v2. New code must
    # write AUTHORITATIVE_EMPTY; readers accept both during the migration.
    VALID_EMPTY = "valid_empty"
    BLOCKED = "blocked"
    RETRY_EXHAUSTED = "retry_exhausted"
    SCHEMA_ERROR = "schema_error"

    @classmethod
    def from_checkpoint(cls, value: Any) -> "FetchStatus":
        """Read both the old ``valid_empty`` and the production-v2 spelling."""

        parsed = cls(str(value))
        if parsed is cls.VALID_EMPTY:
            return cls.AUTHORITATIVE_EMPTY
        return parsed


@dataclass(frozen=True)
class FetchOutcome(Generic[T]):
    """Result of a logical fetch, including all retries for that URL."""

    status: FetchStatus
    value: Optional[T] = None
    status_code: Optional[int] = None
    error: Optional[str] = None
    attempts: int = 0
    label: str = "endpoint"
    context: Mapping[str, Any] = field(default_factory=dict)
    decoded_body_bytes: int = 0
    wire_body_bytes: Optional[int] = None
    provider_metered_bytes: Optional[int] = None
    duration_seconds: float = 0.0
    cache_hit: bool = False
    payload_hash: Optional[str] = None

    @property
    def is_success(self) -> bool:
        """Whether the source gave an authoritative result.

        Authoritative empty and typed not-applicable outcomes are safe to
        checkpoint and must not be retried forever. ``valid_empty`` remains a
        temporary read-compatible alias for old checkpoints.
        """

        return self.status in (
            FetchStatus.OK,
            FetchStatus.AUTHORITATIVE_EMPTY,
            FetchStatus.NOT_APPLICABLE,
            FetchStatus.VALID_EMPTY,
        )

    def with_status(
        self,
        status: FetchStatus,
        *,
        error: Optional[str] = None,
        value: Optional[T] = None,
    ) -> "FetchOutcome[T]":
        """Return a copy with a parser/materialisation status override."""

        return FetchOutcome(
            status=status,
            value=value,
            status_code=self.status_code,
            error=error,
            attempts=self.attempts,
            label=self.label,
            context=self.context,
            decoded_body_bytes=self.decoded_body_bytes,
            wire_body_bytes=self.wire_body_bytes,
            provider_metered_bytes=self.provider_metered_bytes,
            duration_seconds=self.duration_seconds,
            cache_hit=self.cache_hit,
            payload_hash=self.payload_hash,
        )

    def as_checkpoint(self) -> Dict[str, Any]:
        """Return a JSON-safe cache/checkpoint record with an integrity hash."""

        payload_hash = self.payload_hash
        if self.value is not None and payload_hash is None:
            payload_hash = stable_payload_hash(self.value)
        return {
            "version": 2,
            "status": self.status.value,
            "value": self.value,
            "status_code": self.status_code,
            "error": self.error,
            "attempts": self.attempts,
            "label": self.label,
            "context": dict(self.context),
            "decoded_body_bytes": self.decoded_body_bytes,
            "wire_body_bytes": self.wire_body_bytes,
            "provider_metered_bytes": self.provider_metered_bytes,
            "duration_seconds": self.duration_seconds,
            "payload_hash": payload_hash,
        }

    @classmethod
    def from_checkpoint(cls, value: Mapping[str, Any]) -> "FetchOutcome[Any]":
        """Restore a successful outcome and reject corrupt cached payloads."""

        status = FetchStatus.from_checkpoint(value.get("status"))
        if status not in (
            FetchStatus.OK,
            FetchStatus.AUTHORITATIVE_EMPTY,
            FetchStatus.NOT_APPLICABLE,
        ):
            raise ValueError(f"unsafe cached fetch status: {status.value}")
        payload = value.get("value")
        expected_hash = value.get("payload_hash")
        if payload is not None:
            actual_hash = stable_payload_hash(payload)
            if not expected_hash or actual_hash != expected_hash:
                raise ValueError("cached Transfermarkt payload hash mismatch")
        return cls(
            status=status,
            value=payload,
            status_code=value.get("status_code"),
            error=value.get("error"),
            attempts=int(value.get("attempts", 0)),
            label=str(value.get("label") or "endpoint"),
            context=dict(value.get("context") or {}),
            decoded_body_bytes=int(value.get("decoded_body_bytes", 0)),
            wire_body_bytes=(
                int(value["wire_body_bytes"])
                if value.get("wire_body_bytes") is not None else None
            ),
            provider_metered_bytes=(
                int(value["provider_metered_bytes"])
                if value.get("provider_metered_bytes") is not None else None
            ),
            duration_seconds=float(value.get("duration_seconds", 0.0)),
            cache_hit=True,
            payload_hash=expected_hash,
        )

    def as_cache_hit(self, *, duration_seconds: float) -> "FetchOutcome[T]":
        """Mark a restored immutable outcome without mutating its payload."""

        return replace(
            self,
            attempts=0,
            duration_seconds=max(0.0, float(duration_seconds)),
            cache_hit=True,
        )


@dataclass(frozen=True)
class FetchRecord:
    """Checkpoint-safe projection stored per ``(endpoint, source_id)``."""

    status: FetchStatus
    row_count: int
    payload_hash: Optional[str]
    error: Optional[str]
    status_code: Optional[int]
    attempts: int

    def as_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "row_count": self.row_count,
            "payload_hash": self.payload_hash,
            "error": self.error,
            "status_code": self.status_code,
            "attempts": self.attempts,
        }


class TransfermarktError(RuntimeError):
    """Base class for fail-closed Transfermarkt errors."""


class ProxyRequiredError(TransfermarktError):
    """Raised before I/O when no usable residential proxy exists."""


class TrafficBudgetExceeded(TransfermarktError):
    """Raised as soon as a request, decoded, or provider budget is exhausted."""


class TrafficMeterError(TransfermarktError):
    """Raised when paid traffic cannot be measured safely."""


@dataclass(frozen=True)
class LeaseTrafficSnapshot:
    """Monotonic traffic counters returned by proxy-filter's lease API."""

    up_bytes: int = 0
    down_bytes: int = 0
    closed: bool = False
    budget_exceeded: bool = False

    @property
    def provider_bytes(self) -> int:
        return self.up_bytes + self.down_bytes

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "LeaseTrafficSnapshot":
        up = max(0, int(value.get("up_bytes", 0)))
        down = max(0, int(value.get("down_bytes", 0)))
        reported_total = value.get("total_bytes")
        if reported_total is not None and int(reported_total) != up + down:
            raise ValueError("lease total_bytes differs from up_bytes + down_bytes")
        return cls(
            up_bytes=up,
            down_bytes=down,
            closed=bool(value.get("closed", False)),
            budget_exceeded=bool(value.get("budget_exceeded", False)),
        )


@dataclass(frozen=True)
class ProxyLease:
    """One sticky, byte-bounded proxy-filter lease."""

    lease_id: str
    token: str
    proxy_url: str
    max_bytes: int
    expires_at: float


class SharedTrafficLedger:
    """Thread-safe traffic ledger shared per exact scope cycle.

    Every entity/client of one scope cycle accounts against the same ledger;
    the defaults are the per-scope production caps, not the parent (daily)
    aggregate — the parent is tracked by the scope-cycle wrapper's own ledger.
    """

    def __init__(
        self,
        *,
        hard_provider_bytes: int = SCOPE_HARD_PROVIDER_BYTE_CAP,
        soft_provider_bytes: int = SCOPE_SOFT_PROVIDER_BYTE_STOP,
        retry_limit: Optional[int] = None,
    ) -> None:
        hard = int(hard_provider_bytes)
        soft = int(soft_provider_bytes)
        if hard <= 0 or soft <= 0 or soft >= hard:
            raise ValueError("provider byte budgets require 0 < soft < hard")
        retries = None if retry_limit is None else int(retry_limit)
        if retries is not None and retries < 0:
            raise ValueError("retry_limit must be non-negative")
        self.hard_provider_bytes = hard
        self.soft_provider_bytes = soft
        self.retry_limit = retries
        self._lock = threading.RLock()
        self._provider_up_bytes = 0
        self._provider_down_bytes = 0
        self._decoded_bytes = 0
        self._requests = 0
        self._retries = 0
        self._cache_hits = 0
        self._duration_seconds = 0.0
        self._by_entity: Dict[str, Dict[str, Any]] = {}
        self._lease_totals: Dict[str, tuple[int, int]] = {}

    @property
    def provider_bytes(self) -> int:
        with self._lock:
            return self._provider_up_bytes + self._provider_down_bytes

    @property
    def remaining_hard_bytes(self) -> int:
        return max(0, self.hard_provider_bytes - self.provider_bytes)

    def ensure_request_allowed(self, *, retry: bool = False) -> None:
        """Reject a paid request before I/O when a shared cap is exhausted."""

        with self._lock:
            used = self._provider_up_bytes + self._provider_down_bytes
            if used >= self.hard_provider_bytes:
                raise TrafficBudgetExceeded(
                    "Transfermarkt provider hard byte budget exhausted "
                    f"({used}/{self.hard_provider_bytes} bytes)"
                )
            if used >= self.soft_provider_bytes:
                raise TrafficBudgetExceeded(
                    "Transfermarkt provider soft byte stop reached "
                    f"({used}/{self.soft_provider_bytes} bytes)"
                )
            if (
                retry
                and self.retry_limit is not None
                and self._retries >= self.retry_limit
            ):
                raise TrafficBudgetExceeded(
                    "Transfermarkt paid retry budget exhausted "
                    f"({self._retries}/{self.retry_limit} retries)"
                )

    def observe_lease(
        self,
        lease_id: str,
        snapshot: LeaseTrafficSnapshot,
    ) -> tuple[int, int]:
        """Account a lease snapshot once and return its directional deltas."""

        with self._lock:
            previous_up, previous_down = self._lease_totals.get(lease_id, (0, 0))
            if snapshot.up_bytes < previous_up or snapshot.down_bytes < previous_down:
                raise TrafficMeterError(
                    f"non-monotonic traffic counters for lease {lease_id}"
                )
            delta_up = snapshot.up_bytes - previous_up
            delta_down = snapshot.down_bytes - previous_down
            self._lease_totals[lease_id] = (snapshot.up_bytes, snapshot.down_bytes)
            self._provider_up_bytes += delta_up
            self._provider_down_bytes += delta_down
            if self.provider_bytes > self.hard_provider_bytes:
                raise TrafficBudgetExceeded(
                    "Transfermarkt provider hard byte budget exceeded "
                    f"({self.provider_bytes}/{self.hard_provider_bytes} bytes)"
                )
            return delta_up, delta_down

    def _entity(self, entity: str) -> Dict[str, Any]:
        return self._by_entity.setdefault(
            entity,
            {
                "decoded_bytes": 0,
                "wire_bytes": 0,
                "provider_up_bytes": 0,
                "provider_down_bytes": 0,
                "provider_bytes": 0,
                "requests": 0,
                "retries": 0,
                "cache_hits": 0,
                "duration_seconds": 0.0,
            },
        )

    def record_attempt(
        self,
        *,
        entity: str,
        decoded_bytes: int,
        provider_up_bytes: int,
        provider_down_bytes: int,
        retry: bool,
        duration_seconds: float,
    ) -> None:
        """Attribute already-observed traffic and one network attempt."""

        with self._lock:
            decoded = max(0, int(decoded_bytes))
            up = max(0, int(provider_up_bytes))
            down = max(0, int(provider_down_bytes))
            duration = max(0.0, float(duration_seconds))
            self._decoded_bytes += decoded
            self._requests += 1
            self._retries += int(bool(retry))
            self._duration_seconds += duration
            stats = self._entity(entity)
            stats["decoded_bytes"] += decoded
            # Downstream is the closest provider-boundary wire measurement.
            stats["wire_bytes"] += down
            stats["provider_up_bytes"] += up
            stats["provider_down_bytes"] += down
            stats["provider_bytes"] += up + down
            stats["requests"] += 1
            stats["retries"] += int(bool(retry))
            stats["duration_seconds"] += duration

    def record_unattributed_provider(
        self,
        *,
        entity: str,
        provider_up_bytes: int,
        provider_down_bytes: int,
    ) -> None:
        """Attribute final close-handshake deltas without inventing a request."""

        with self._lock:
            stats = self._entity(entity)
            up = max(0, int(provider_up_bytes))
            down = max(0, int(provider_down_bytes))
            stats["wire_bytes"] += down
            stats["provider_up_bytes"] += up
            stats["provider_down_bytes"] += down
            stats["provider_bytes"] += up + down

    def record_cache_hit(self, *, entity: str, duration_seconds: float) -> None:
        with self._lock:
            duration = max(0.0, float(duration_seconds))
            self._cache_hits += 1
            self._duration_seconds += duration
            stats = self._entity(entity)
            stats["cache_hits"] += 1
            stats["duration_seconds"] += duration

    def record_duration_overhead(
        self,
        *,
        entity: str,
        duration_seconds: float,
    ) -> None:
        """Add retry backoff/cache bookkeeping time not inside HTTP attempts."""

        with self._lock:
            duration = max(0.0, float(duration_seconds))
            self._duration_seconds += duration
            self._entity(entity)["duration_seconds"] += duration

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            by_entity: Dict[str, Dict[str, Any]] = {}
            for entity, raw in self._by_entity.items():
                item = dict(raw)
                denominator = item["requests"] + item["cache_hits"]
                item["cache_hit_rate"] = (
                    item["cache_hits"] / denominator if denominator else 0.0
                )
                item["duration_seconds"] = round(item["duration_seconds"], 6)
                by_entity[entity] = item
            denominator = self._requests + self._cache_hits
            provider = self._provider_up_bytes + self._provider_down_bytes
            return {
                "hard_provider_byte_budget": self.hard_provider_bytes,
                "soft_provider_byte_stop": self.soft_provider_bytes,
                "retry_limit": self.retry_limit,
                "remaining_retries": (
                    None
                    if self.retry_limit is None
                    else max(0, self.retry_limit - self._retries)
                ),
                "provider_up_bytes": self._provider_up_bytes,
                "provider_down_bytes": self._provider_down_bytes,
                "provider_metered_bytes": provider,
                "remaining_hard_bytes": max(0, self.hard_provider_bytes - provider),
                "soft_stop_reached": provider >= self.soft_provider_bytes,
                "decoded_bytes": self._decoded_bytes,
                "wire_bytes": self._provider_down_bytes,
                "requests": self._requests,
                "retries": self._retries,
                "cache_hits": self._cache_hits,
                "cache_hit_rate": (
                    self._cache_hits / denominator if denominator else 0.0
                ),
                "duration_seconds": round(self._duration_seconds, 6),
                "by_entity": by_entity,
            }
