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
"""Emergency provider-metered ceiling for one shared parent cycle (15 MiB)."""

SOFT_PROVIDER_BYTE_STOP = 14_680_064
"""Stop starting paid requests before the emergency ceiling is reached (14 MiB)."""


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
    """Thread-safe traffic ledger shared by every entity/client in a cycle."""

    def __init__(
        self,
        *,
        hard_provider_bytes: int = HARD_PROVIDER_BYTE_BUDGET,
        soft_provider_bytes: int = SOFT_PROVIDER_BYTE_STOP,
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
