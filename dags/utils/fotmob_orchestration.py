"""Pure scheduling policy for the single isolated FotMob owner.

The scheduler state is deliberately tiny and JSON round-trippable so Airflow
can persist it in one Variable.  Selection itself has no Airflow dependency;
that keeps the daily reservation and fair background alternation testable.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from typing import Any, Mapping

from utils.fotmob_publication import (
    FOTMOB_CATALOG_CONTRACT_SCHEMA,
    FOTMOB_DAILY_ENTITIES,
)


UTC = timezone.utc
BACKGROUND_HOLD_START = time(13, 30)
BACKGROUND_DEADLINE = time(13, 45)
DAILY_WINDOW_START = time(14, 0)
DAILY_WINDOW_END = time(15, 0)


class FotMobLane(str, Enum):
    """The three mutually exclusive workloads owned by the scheduler."""

    DAILY = "daily"
    REFRESH = "refresh"
    BACKFILL = "backfill"


BACKGROUND_LANES = frozenset({FotMobLane.REFRESH, FotMobLane.BACKFILL})


@dataclass(frozen=True, slots=True)
class FotMobSchedulerState:
    """Durable cursor changed only after a successful child publication."""

    next_background_lane: FotMobLane
    daily_date: date | None
    generation: int
    updated_at: datetime

    @classmethod
    def initial(cls) -> "FotMobSchedulerState":
        return cls(
            next_background_lane=FotMobLane.REFRESH,
            daily_date=None,
            generation=0,
            updated_at=datetime(1970, 1, 1, tzinfo=UTC),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "next_background_lane": self.next_background_lane.value,
            "daily_date": self.daily_date.isoformat() if self.daily_date else None,
            "generation": self.generation,
            "updated_at": _as_utc(self.updated_at).isoformat(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FotMobSchedulerState":
        if not isinstance(value, Mapping):
            raise ValueError("FotMob scheduler state must be an object")
        expected = {
            "next_background_lane",
            "daily_date",
            "generation",
            "updated_at",
        }
        if set(value) != expected:
            raise ValueError("FotMob scheduler state has an unexpected shape")
        try:
            lane = FotMobLane(str(value["next_background_lane"]))
        except ValueError as exc:
            raise ValueError("FotMob scheduler background lane is invalid") from exc
        if lane not in BACKGROUND_LANES:
            raise ValueError("FotMob scheduler background lane must be refresh/backfill")
        raw_date = value["daily_date"]
        if raw_date is None:
            parsed_date = None
        elif isinstance(raw_date, str) and raw_date:
            try:
                parsed_date = date.fromisoformat(raw_date)
            except ValueError as exc:
                raise ValueError("FotMob scheduler daily date is invalid") from exc
        else:
            raise ValueError("FotMob scheduler daily date is invalid")
        generation = value["generation"]
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise ValueError("FotMob scheduler generation must be non-negative")
        try:
            updated_at = datetime.fromisoformat(str(value["updated_at"]))
        except ValueError as exc:
            raise ValueError("FotMob scheduler updated_at is invalid") from exc
        return cls(
            next_background_lane=lane,
            daily_date=parsed_date,
            generation=generation,
            updated_at=_as_utc(updated_at),
        )


@dataclass(frozen=True, slots=True)
class LaneDecision:
    lane: FotMobLane | None
    reason: str


def _as_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("FotMob scheduler timestamps must be timezone-aware")
    return value.astimezone(UTC)


def choose_lane(
    now_utc: datetime,
    state: FotMobSchedulerState,
    child_running: bool,
) -> LaneDecision:
    """Choose one workload without mutating the durable cursor."""

    now = _as_utc(now_utc)
    if child_running:
        return LaneDecision(None, "ingest_child_running")

    wall_time = now.time().replace(tzinfo=None)
    if BACKGROUND_HOLD_START <= wall_time < DAILY_WINDOW_START:
        return LaneDecision(None, "daily_window_approaching")
    if DAILY_WINDOW_START <= wall_time < DAILY_WINDOW_END:
        if state.daily_date == now.date():
            return LaneDecision(None, "daily_already_completed")
        return LaneDecision(FotMobLane.DAILY, "daily_window")
    return LaneDecision(state.next_background_lane, "background_fair_turn")


def advance_after_success(
    state: FotMobSchedulerState,
    lane: FotMobLane,
    completed_at: datetime,
    *,
    daily_date: date | None = None,
) -> FotMobSchedulerState:
    """Return the next durable state after one successful publication."""

    completed = _as_utc(completed_at)
    normalized_lane = FotMobLane(lane)
    next_lane = state.next_background_lane
    last_daily = state.daily_date
    if normalized_lane is FotMobLane.DAILY:
        last_daily = daily_date or completed.date()
    elif normalized_lane is FotMobLane.REFRESH:
        next_lane = FotMobLane.BACKFILL
    elif normalized_lane is FotMobLane.BACKFILL:
        next_lane = FotMobLane.REFRESH
    return FotMobSchedulerState(
        next_background_lane=next_lane,
        daily_date=last_daily,
        generation=state.generation + 1,
        updated_at=completed,
    )


_LANE_CAPS = {
    FotMobLane.DAILY: (10_000, 512, 60),
    FotMobLane.REFRESH: (15_000, 1_024, 60),
    FotMobLane.BACKFILL: (10_000, 512, 45),
}


def _background_deadline(now: datetime) -> datetime:
    target = datetime.combine(now.date(), BACKGROUND_DEADLINE, tzinfo=UTC)
    if target <= now:
        target += timedelta(days=1)
    return target


def build_child_conf(lane: FotMobLane, now_utc: datetime) -> dict[str, Any]:
    """Build the dynamic-contract child profile with admitted traffic caps."""

    normalized_lane = FotMobLane(lane)
    now = _as_utc(now_utc)
    max_requests, max_direct_mib, rpm = _LANE_CAPS[normalized_lane]
    deadline = (
        ""
        if normalized_lane is FotMobLane.DAILY
        else _background_deadline(now).isoformat()
    )
    return {
        "mode": normalized_lane.value,
        "scope": "",
        "catalog_contract": FOTMOB_CATALOG_CONTRACT_SCHEMA,
        "entities": ",".join(FOTMOB_DAILY_ENTITIES),
        "max_requests": max_requests,
        "max_direct_mib": max_direct_mib,
        "max_proxy_mib": 0,
        "competition_limit": 0,
        "season_limit": 0,
        "requests_per_minute": rpm,
        "deadline": deadline,
    }


__all__ = [
    "FotMobLane",
    "FotMobSchedulerState",
    "LaneDecision",
    "advance_after_success",
    "build_child_conf",
    "choose_lane",
]
