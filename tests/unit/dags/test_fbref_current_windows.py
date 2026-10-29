"""History slices sit between current reservations (#1958)."""

from datetime import datetime, timedelta, timezone

import pytest

from utils import fbref_current_windows as windows
from utils import fbref_pipeline_tasks as tasks


def test_history_schedule_has_three_half_past_slices():
    assert windows.HISTORY_SCHEDULE == "30 4,16,22 * * *"


@pytest.mark.parametrize(
    "hour,deadline", [(4, "05:15"), (16, "17:15"), (22, "23:15")]
)
def test_history_slice_starts_after_a_reservation_and_ends_at_the_margin(hour, deadline):
    start = datetime(2026, 10, 9, hour, 30, tzinfo=timezone.utc)
    assert windows.history_slice_window(start) == (start, start + timedelta(minutes=30))
    reservations = list(windows.current_window_reservations(start))
    assert not any(begin <= start < end for begin, end in reservations)
    assert max(end for _, end in reservations if end <= start) == start
    verdict = tasks.guard_fbref_history_window(
        max_batches=1, shard_size=1, overhead_minutes=0, now=start
    )
    assert verdict["deadline"] == f"2026-10-09T{deadline}:00+00:00"


@pytest.mark.parametrize(
    "at", ["2026-10-09T04:29:59", "2026-10-09T05:00:00", "2026-10-09T12:00:00",
           "2026-10-09T23:00:00", "2026-10-09T22:30:00-02:00"]
)
def test_history_slice_window_is_closed_elsewhere(at):
    assert windows.history_slice_window(datetime.fromisoformat(at)) is None
