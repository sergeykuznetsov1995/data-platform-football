"""Match refresh obligations derived only from published source observations."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional


MATCH_SETTLE_MARGIN = timedelta(hours=3)
LATE_MATCH_THRESHOLDS = (timedelta(hours=6), timedelta(hours=48))
LATE_MATCH_CUTOFF = timedelta(hours=72)


def match_time(value: Any) -> Optional[datetime]:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
    except (ValueError, TypeError):
        return None
    return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed


def source_observed_at(row: Optional[Mapping[str, Any]]) -> Optional[datetime]:
    if not row or row.get("stale"):
        return None
    capabilities = row.get("capabilities_json") or {}
    if isinstance(capabilities, str):
        capabilities = json.loads(capabilities)
    return match_time(capabilities.get("source_observed_at") or row.get("fetched_at"))


@dataclass(frozen=True)
class MatchRefresh:
    kind: str
    next_due: Optional[datetime] = None


def match_refresh(
    match: Mapping[str, Any], last_observed: Optional[datetime], now: datetime,
    *, allow_after_cutoff: bool = False,
) -> MatchRefresh:
    """Two successful observations after T+6/T+48, with HTTP stopped at T+72.

    A delayed first observation can satisfy both thresholds. Failed attempts
    never close an obligation; no attempt counters or separate checkpoints.
    Recovery of a received response may close an obligation after the cutoff.
    """
    if (
        not match.get("finished") or match.get("cancelled")
        or match.get("postponed") or match.get("match_id") is None
    ):
        return MatchRefresh("done")
    kickoff = match_time(match.get("utc_time"))
    if kickoff is None or kickoff + MATCH_SETTLE_MARGIN > now:
        return MatchRefresh("done")
    if last_observed is None:
        return MatchRefresh("primary")
    end = kickoff + MATCH_SETTLE_MARGIN
    threshold = next(
        (end + delay for delay in LATE_MATCH_THRESHOLDS if last_observed < end + delay),
        None,
    )
    if threshold is None or (not allow_after_cutoff and now >= end + LATE_MATCH_CUTOFF):
        return MatchRefresh("done")
    return MatchRefresh("late" if now >= threshold else "wait", threshold)
