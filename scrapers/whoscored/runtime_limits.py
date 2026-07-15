"""Shared, lightweight WhoScored source-limit contracts."""

from __future__ import annotations

import os
from typing import Mapping, Optional

# HTML page requests back matches, previews, profiles, and parts of schedule
# ingestion.  This is a per-service-instance limiter; Airflow's source pool
# supplies the independently enforced concurrency multiplier.
SOURCE_PAGE_REQUESTS_PER_MINUTE = 30

SOURCE_POOL_SLOTS_ENV = "WHOSCORED_SOURCE_POOL_SLOTS"
SOURCE_POOL_SLOTS_DEFAULT = 2
SOURCE_POOL_SLOTS_MIN = 2
SOURCE_POOL_SLOTS_MAX = 4
SOURCE_PAGE_REQUESTS_PER_DAY_PER_SLOT = SOURCE_PAGE_REQUESTS_PER_MINUTE * 60 * 24


def source_pool_slots(
    environment: Optional[Mapping[str, str]] = None,
) -> int:
    """Return the validated deployed WhoScored source-pool capacity."""

    values = os.environ if environment is None else environment
    raw_value = values.get(SOURCE_POOL_SLOTS_ENV)
    if raw_value is None:
        return SOURCE_POOL_SLOTS_DEFAULT
    try:
        slots = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{SOURCE_POOL_SLOTS_ENV} must be an integer in "
            f"{SOURCE_POOL_SLOTS_MIN}..{SOURCE_POOL_SLOTS_MAX}"
        ) from exc
    if not SOURCE_POOL_SLOTS_MIN <= slots <= SOURCE_POOL_SLOTS_MAX:
        raise ValueError(
            f"{SOURCE_POOL_SLOTS_ENV} must be an integer in "
            f"{SOURCE_POOL_SLOTS_MIN}..{SOURCE_POOL_SLOTS_MAX}"
        )
    return slots


def source_page_request_hard_ceiling_per_day(slots: int) -> int:
    """Return the page-request ceiling enforced by a validated slot count."""

    if (
        isinstance(slots, bool)
        or not isinstance(slots, int)
        or not SOURCE_POOL_SLOTS_MIN <= slots <= SOURCE_POOL_SLOTS_MAX
    ):
        raise ValueError(
            "WhoScored source pool slots must be an integer in "
            f"{SOURCE_POOL_SLOTS_MIN}..{SOURCE_POOL_SLOTS_MAX}"
        )
    return slots * SOURCE_PAGE_REQUESTS_PER_DAY_PER_SLOT
