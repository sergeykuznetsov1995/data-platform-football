"""Bounded continuation of a refresh batch from committed Bronze evidence.

The initial estimate is only admission control. Replay can finish much sooner;
new, separately identified plans spend the remaining actual allowance. An
immutable wall deadline independently bounds queue waits and retries.
"""
from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Any, Callable


def refill_deadline(deadline_epoch: float, run_started: datetime | None) -> float:
    """New night work must leave room for metadata and the delivery window.

    Only continuation is constrained here; this does not claim a hard deadline
    for the legacy sweep/initial mapped tasks. A 04:00 end leaves 25 minutes
    for metadata and >90 minutes for the 73-minute deployment reserve.
    """
    if run_started is None:
        return deadline_epoch
    started = run_started.astimezone(timezone.utc)
    if started.hour < 6:
        return min(deadline_epoch, started.replace(hour=4, minute=0, second=0, microsecond=0).timestamp())
    return deadline_epoch


def _partition(row: tuple) -> tuple[str, str, str]:
    return str(row[0]), str(row[1]), str(row[5]) if len(row) > 5 else ""


def refill_window(
    *,
    initial_elapsed_s: float,
    deadline_epoch: float,
    pending: Callable[[], list[tuple]],
    plan: Callable[[list[tuple], float, int], dict[str, str] | None],
    execute: Callable[[dict[str, str]], dict[str, Any]],
    budget_s: float = 7200,
    max_rounds: int = 128,
    now: Callable[[], float] = time.time,
    persist: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    for value in (initial_elapsed_s, deadline_epoch, budget_s):
        if not math.isfinite(value) or value < 0:
            raise ValueError("refresh window accounting must be finite and nonnegative")
    outcomes: list[dict[str, Any]] = []
    blocked: dict[tuple[str, str, str], str] = {}
    used = 0.0
    status = "partial"
    reason = "round_cap"

    def report():
        return {"status": status, "stop_reason": reason,
                "elapsed_s": round(used, 3), "outcomes": list(outcomes),
                "blocked_scopes": sorted(blocked.values()),
                "window_deadline_epoch": deadline_epoch,
                "window_budget_s": budget_s}

    rows: list[tuple] | None = None
    for round_number in range(1, max_rounds + 1):
        allowance = budget_s - initial_elapsed_s - used
        wall_left = deadline_epoch - now()
        if allowance <= 0:
            reason = "time_budget"
            break
        if wall_left <= 0:
            reason = "wall_deadline"
            break
        rows = pending() if rows is None else rows
        candidates = [row for row in rows if _partition(row) not in blocked]
        if not candidates:
            reason = "no_progress" if rows else "empty_queue"
            status = "partial" if rows else "success"
            break
        env = plan(candidates, min(allowance, wall_left), round_number)
        if env is None:
            reason = "insufficient_headroom"
            break
        # Planning itself can take time (Trino): never grant it a fresh window.
        if now() >= deadline_epoch:
            reason = "wall_deadline"
            break
        started = now()
        outcome = execute(env)
        elapsed = max(float(outcome.get("elapsed_s") or 0), now() - started)
        if not math.isfinite(elapsed) or elapsed < 0:
            raise ValueError("invalid scope elapsed time")
        used += elapsed
        outcomes.append(outcome)
        if outcome.get("status") not in {"success", "refreshed", "partial"}:
            status, reason = "failed", "scope_failure"
            if persist:
                persist(report())
            break
        key = (f"SS-{env['SOFASCORE_TOURNAMENT_ID']}",
               env['SOFASCORE_CANONICAL_SEASON'], env['SOFASCORE_SOURCE_SEASON_ID'])
        before = sum(int(row[2]) for row in rows if _partition(row) == key)
        refreshed = pending()
        after = sum(int(row[2]) for row in refreshed if _partition(row) == key)
        # Reusing a signed-plan identity is forbidden; making endless fresh
        # identities for unchanged work is equally wrong. Skip this scope for
        # this run while allowing other due scopes to make progress.
        if after >= before:
            blocked[key] = env['SOFASCORE_SCOPE_KEY']
        rows = refreshed
        if persist:
            persist(report())
    result = report()
    if persist:
        persist(result)
    return result
