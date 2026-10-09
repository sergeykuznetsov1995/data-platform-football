"""Continuous current collection with a daily Transfermarkt delivery quiet window.

UTC 01:00–03:00 is reserved for delivery (04:00–06:00 Moscow). New portions
stop at 00:15 to leave their full 45-minute budget before delivery. Runtime
admission must also call ``work_deadline``: a queued run can start later than
the scheduler's decision, and a timetable alone cannot bound task execution.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
from typing import Any

MAX_PORTION_SECONDS = 45 * 60
IDLE_POLL_SECONDS = 5 * 60


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Transfermarkt runtime timestamps must be timezone aware")
    # Pendulum 2 can retain a broken tz object when astimezone receives the
    # stdlib UTC timezone. Use a stdlib datetime for arithmetic in both versions.
    return datetime.fromtimestamp(value.timestamp(), tz=timezone.utc)


def _budget(max_seconds: float) -> float:
    budget = float(max_seconds)
    if not math.isfinite(budget) or not 0 < budget <= MAX_PORTION_SECONDS:
        raise ValueError("Transfermarkt portion budget must be in (0, 2700] seconds")
    return budget


def next_admitted_time(now: datetime) -> datetime:
    """Return now, or 03:00 UTC if starting a portion is currently forbidden."""
    current = _utc(now)
    cutoff = current.replace(hour=0, minute=15, second=0, microsecond=0)
    resume = current.replace(hour=3, minute=0, second=0, microsecond=0)
    return resume if cutoff <= current < resume else current


def available_work_seconds(now: datetime, max_seconds: float = MAX_PORTION_SECONDS) -> float:
    """Budget for a newly starting portion; do not wait through the quiet window."""
    current = _utc(now)
    budget = _budget(max_seconds)
    if next_admitted_time(current) != current:
        return 0.0
    delivery_start = current.replace(hour=1, minute=0, second=0, microsecond=0)
    if current >= delivery_start:
        delivery_start += timedelta(days=1)
    return min(budget, (delivery_start - current).total_seconds())


def work_deadline(started_at: datetime, max_seconds: float = MAX_PORTION_SECONDS) -> datetime:
    """Hard deadline including requests, retries, and committing the portion."""
    started = _utc(started_at)
    return started + timedelta(seconds=available_work_seconds(started, max_seconds))


def remaining_work_seconds(deadline: datetime, now: datetime) -> float:
    """Remaining time for admitted work; never continue during delivery itself."""
    current = _utc(now)
    if 1 <= current.hour < 3:
        return 0.0
    return max(0.0, (_utc(deadline) - current).total_seconds())


try:
    import pendulum
    from airflow.timetables.base import DataInterval, DagRunInfo, TimeRestriction
    from airflow.timetables.simple import ContinuousTimetable
except ModuleNotFoundError as exc:
    # The scraper's isolated Python environment need not include Airflow. Pure
    # runtime helpers remain usable there; never substitute a fake timetable.
    if exc.name is not None and exc.name.split(".")[0] not in {"airflow", "pendulum"}:
        raise
    TransfermarktCurrentTimetable = None
else:
    class TransfermarktCurrentTimetable(ContinuousTimetable):
        """One continuous run at a time, admitting none from 00:15 to 03:00 UTC."""

        active_runs_limit = 1
        description = "Continuous Transfermarkt current; delivery quiet window 04:00–06:00 MSK"

        @property
        def summary(self) -> str:
            return "@continuous (TM: no new portions 00:15–03:00 UTC)"

        def serialize(self) -> dict[str, Any]:
            return {}

        @classmethod
        def deserialize(cls, data: dict[str, Any]) -> TransfermarktCurrentTimetable:
            if data != {}:
                raise ValueError("Invalid Transfermarkt current timetable configuration")
            return cls()

        def next_dagrun_info(
            self,
            *,
            last_automated_data_interval: DataInterval | None,
            restriction: TimeRestriction,
        ) -> DagRunInfo | None:
            if restriction.earliest is None:
                return None
            now = pendulum.now("UTC")
            start = (
                last_automated_data_interval.end
                if last_automated_data_interval is not None
                else restriction.earliest
            )
            candidate = max(now, restriction.earliest, start)
            if last_automated_data_interval is not None:
                # Empty/short runs must not create a busy loop of registry and
                # metadata reads. A full work portion already outlasts this
                # interval and therefore continues immediately.
                candidate = max(candidate, start + timedelta(seconds=IDLE_POLL_SECONDS))
            admitted = pendulum.instance(next_admitted_time(candidate))
            if restriction.latest is not None and admitted > restriction.latest:
                return None
            return DagRunInfo.interval(start, admitted)
