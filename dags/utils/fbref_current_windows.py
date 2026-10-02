"""Shared UTC scheduling policy for current refresh and history admission.

Budgets stop new batches; they do not interrupt a running batch. Reservations
include observed completion/setup time, not a hard worst-case guarantee.
"""

from datetime import datetime, timedelta, timezone


CURRENT_WINDOW_HOURS_UTC = (0, 6, 12, 18)
CURRENT_SCHEDULE = "0 " + ",".join(map(str, CURRENT_WINDOW_HOURS_UTC)) + " * * *"
CURRENT_MAX_BATCHES = 20
CURRENT_WAVE_DEADLINE_SECONDS = 16200
SMALL_MAX_BATCHES = 9
SMALL_WAVE_DEADLINE_SECONDS = 10800
BOOTSTRAP_WAVE_DEADLINE_SECONDS = 19800
# Observed batch 64m14s + reconciliation 4m06s + DAG tail 12m41s + setup 3m33s.
# Round the observed 84m34s up to 90 minutes; overrides can exceed this.
CURRENT_RESERVATION_HEADROOM_SECONDS = 90 * 60


def current_window_profile(data_interval_end=None) -> dict[str, int]:
    """Select by interval END in UTC, independent of task start/logical date.

    Missing intervals (manual ingest) conservatively use the small profile.
    Naive datetimes are interpreted as UTC, never the worker's local timezone.
    """
    if data_interval_end is not None:
        end = data_interval_end
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        if end.astimezone(timezone.utc).hour == 6:
            return {
                "max_batches": CURRENT_MAX_BATCHES,
                "wave_deadline_seconds": CURRENT_WAVE_DEADLINE_SECONDS,
            }
    return {
        "max_batches": SMALL_MAX_BATCHES,
        "wave_deadline_seconds": SMALL_WAVE_DEADLINE_SECONDS,
    }


def current_window_reservations(now: datetime):
    """Yield previous/current/next day reservations, including midnight tails."""
    midnight = now.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    for day in (-1, 0, 1):
        for hour in CURRENT_WINDOW_HOURS_UTC:
            start = midnight + timedelta(days=day, hours=hour)
            budget = current_window_profile(start)["wave_deadline_seconds"]
            yield start, start + timedelta(
                seconds=budget + CURRENT_RESERVATION_HEADROOM_SECONDS
            )
