"""Fair, budget-aware scheduling for exhaustive Transfermarkt capture."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import math
from typing import Iterable, Mapping

from scrapers.transfermarkt.models import (
    PARENT_DAILY_HARD_PROVIDER_BYTE_CAP,
    PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP,
    SCOPE_HARD_PROVIDER_BYTE_CAP,
)


QUEUE_ORDER = ('resume', 'current', 'growth', 'debt')
WAVE_SIZE = 8
MIN_SCOPE_RESERVATION_BYTES = 2 * 1024 * 1024
RESERVATION_SAFETY_FACTOR = 1.5


class SchedulingError(ValueError):
    """The persisted daily budget or candidate queue is invalid."""


@dataclass(frozen=True)
class CrawlCandidate:
    scope_id: str
    queue: str
    due_at: datetime
    last_attempt_at: datetime | None = None
    pending_work: int = 0
    estimated_provider_bytes: int | None = None

    def __post_init__(self) -> None:
        if not self.scope_id.strip():
            raise SchedulingError('scope_id is required')
        if self.queue not in QUEUE_ORDER:
            raise SchedulingError(f'unknown crawl queue: {self.queue}')
        if self.due_at.tzinfo is None or self.due_at.utcoffset() is None:
            raise SchedulingError('due_at must be timezone-aware')
        if self.last_attempt_at is not None and (
            self.last_attempt_at.tzinfo is None
            or self.last_attempt_at.utcoffset() is None
        ):
            raise SchedulingError('last_attempt_at must be timezone-aware')
        if self.pending_work < 0:
            raise SchedulingError('pending_work must not be negative')


@dataclass(frozen=True)
class ScheduledScope:
    candidate: CrawlCandidate
    reserved_provider_bytes: int


@dataclass(frozen=True)
class WavePlan:
    day_key: str
    scopes: tuple[ScheduledScope, ...]
    next_queue_cursor: int
    used_provider_bytes: int
    reserved_provider_bytes: int
    hard_provider_byte_cap: int
    soft_provider_byte_stop: int

    @property
    def remaining_hard_bytes(self) -> int:
        return max(
            0,
            self.hard_provider_byte_cap
            - self.used_provider_bytes
            - self.reserved_provider_bytes,
        )


def provider_day_key(value: datetime | date) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise SchedulingError('provider day timestamp must be timezone-aware')
        value = value.astimezone(timezone.utc).date()
    return value.isoformat()


def scope_reservation_bytes(candidate: CrawlCandidate) -> int:
    estimate = candidate.estimated_provider_bytes
    if estimate is None or estimate <= 0:
        return SCOPE_HARD_PROVIDER_BYTE_CAP
    guarded = math.ceil(int(estimate) * RESERVATION_SAFETY_FACTOR)
    return min(
        SCOPE_HARD_PROVIDER_BYTE_CAP,
        max(MIN_SCOPE_RESERVATION_BYTES, guarded),
    )


def _candidate_key(candidate: CrawlCandidate) -> tuple:
    never = candidate.last_attempt_at is None
    last = candidate.last_attempt_at or datetime.min.replace(tzinfo=timezone.utc)
    return (
        candidate.due_at,
        not never,
        -candidate.pending_work,
        last,
        candidate.scope_id,
    )


def plan_wave(
    candidates: Iterable[CrawlCandidate],
    *,
    now: datetime,
    used_provider_bytes: int,
    queue_cursor: int = 0,
    max_wave_size: int = WAVE_SIZE,
    hard_provider_byte_cap: int = PARENT_DAILY_HARD_PROVIDER_BYTE_CAP,
    soft_provider_byte_stop: int = PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP,
) -> WavePlan:
    """Return one fair wave without exceeding the shared UTC-day ledger.

    Every non-empty queue receives at most one candidate per round.  The next
    wave starts at the following queue, so a continually refilled resume/current
    queue cannot starve new historical coverage or career debt.
    """

    if now.tzinfo is None or now.utcoffset() is None:
        raise SchedulingError('now must be timezone-aware')
    if isinstance(used_provider_bytes, bool) or used_provider_bytes < 0:
        raise SchedulingError('used_provider_bytes must be non-negative')
    if not 1 <= max_wave_size <= WAVE_SIZE:
        raise SchedulingError(f'max_wave_size must be between 1 and {WAVE_SIZE}')
    if not 0 < soft_provider_byte_stop < hard_provider_byte_cap:
        raise SchedulingError('daily soft/hard provider caps are invalid')
    if used_provider_bytes >= soft_provider_byte_stop:
        return WavePlan(
            provider_day_key(now), (), queue_cursor % len(QUEUE_ORDER),
            used_provider_bytes, 0, hard_provider_byte_cap,
            soft_provider_byte_stop,
        )

    queues: dict[str, list[CrawlCandidate]] = {name: [] for name in QUEUE_ORDER}
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.scope_id in seen:
            raise SchedulingError(f'duplicate candidate scope: {candidate.scope_id}')
        seen.add(candidate.scope_id)
        if candidate.due_at <= now:
            queues[candidate.queue].append(candidate)
    for values in queues.values():
        values.sort(key=_candidate_key)

    cursor = queue_cursor % len(QUEUE_ORDER)
    selected: list[ScheduledScope] = []
    reserved = 0
    empty_rounds = 0
    while len(selected) < max_wave_size and empty_rounds < len(QUEUE_ORDER):
        queue = QUEUE_ORDER[cursor]
        cursor = (cursor + 1) % len(QUEUE_ORDER)
        if not queues[queue]:
            empty_rounds += 1
            continue
        empty_rounds = 0
        candidate = queues[queue].pop(0)
        reservation = scope_reservation_bytes(candidate)
        if used_provider_bytes + reserved + reservation > hard_provider_byte_cap:
            # This queue cannot safely admit its head today.  Other queues may
            # contain known-cheaper refreshes, so continue the fair round.
            queues[queue].clear()
            continue
        selected.append(ScheduledScope(candidate, reservation))
        reserved += reservation

    return WavePlan(
        day_key=provider_day_key(now),
        scopes=tuple(selected),
        next_queue_cursor=cursor,
        used_provider_bytes=used_provider_bytes,
        reserved_provider_bytes=reserved,
        hard_provider_byte_cap=hard_provider_byte_cap,
        soft_provider_byte_stop=soft_provider_byte_stop,
    )


def candidate_from_mapping(value: Mapping[str, object]) -> CrawlCandidate:
    """Strict adapter for persisted planner rows."""

    def timestamp(name: str, *, optional: bool = False) -> datetime | None:
        raw = value.get(name)
        if raw is None and optional:
            return None
        if isinstance(raw, datetime):
            result = raw
        else:
            try:
                result = datetime.fromisoformat(str(raw).replace('Z', '+00:00'))
            except ValueError as exc:
                raise SchedulingError(f'{name} is not an ISO timestamp') from exc
        if result.tzinfo is None or result.utcoffset() is None:
            raise SchedulingError(f'{name} must be timezone-aware')
        return result

    return CrawlCandidate(
        scope_id=str(value.get('scope_id') or ''),
        queue=str(value.get('queue') or ''),
        due_at=timestamp('due_at'),  # type: ignore[arg-type]
        last_attempt_at=timestamp('last_attempt_at', optional=True),
        pending_work=int(value.get('pending_work') or 0),
        estimated_provider_bytes=(
            int(value['estimated_provider_bytes'])
            if value.get('estimated_provider_bytes') is not None else None
        ),
    )
