"""Lightweight shared limits for scheduled WhoScored profile work."""

from __future__ import annotations

import os
from typing import Mapping, Optional


MAX_DAILY_PROFILE_CANDIDATES = 3_000


def daily_profile_candidate_hard_cap(
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    """Return the canonical configured cap, bounded by the source timeout."""

    values = os.environ if environ is None else environ
    raw = values.get("WHOSCORED_DAILY_PROFILE_MAX_LIMIT", "3000")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "WHOSCORED_DAILY_PROFILE_MAX_LIMIT must be an integer in 1..3000"
        ) from exc
    if str(raw).strip() != str(value) or not 1 <= value <= MAX_DAILY_PROFILE_CANDIDATES:
        raise ValueError(
            "WHOSCORED_DAILY_PROFILE_MAX_LIMIT must be an integer in 1..3000"
        )
    return value
