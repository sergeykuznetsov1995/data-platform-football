"""Single source of the nightly rewrite window (owner decision 08.11).

06:00–08:00 MSK == 03:00–05:00 UTC. Dependency-free so host CLIs can import it.
"""

from __future__ import annotations

from datetime import datetime, timezone

WINDOW_START_HOUR_UTC = 3
WINDOW_END_HOUR_UTC = 5
WINDOW_LABEL = "03:00–05:00 UTC (06:00–08:00 MSK)"


def nightly_deadline(now=None):
    """Return today's window end, or raise TimeoutError outside the window."""
    now = now or datetime.now(timezone.utc)
    if not WINDOW_START_HOUR_UTC <= now.hour < WINDOW_END_HOUR_UTC:
        raise TimeoutError(f"Nightly maintenance is permitted only at {WINDOW_LABEL}")
    return now.replace(hour=WINDOW_END_HOUR_UTC, minute=0, second=0, microsecond=0)
