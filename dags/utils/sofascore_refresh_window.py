"""Bounded continuation of a refresh batch from committed Bronze evidence.

The initial estimate is only admission control. Replay can finish much sooner;
new, separately identified plans spend the remaining actual allowance. An
immutable wall deadline independently bounds queue waits and retries.
"""
from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
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


_TRANSIENT_SCOPE_ERROR = "status=retryable_failure error=TransportError:"


def transient_scope_failure(result_path: str, run_id: str, not_before: float | None = None) -> bool:
    """#1751: the scope lost only its transport, with exact byte accounting.

    Every error must be a retryable TransportError; any accounting doubt,
    control-channel failure, schema error or other error keeps the scope red.
    ``not_before`` rejects a stale result left by an earlier try.
    """
    try:
        result = json.loads(Path(result_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if not isinstance(result, dict) or not run_id or result.get("run_id") != run_id:
        return False
    if not_before is not None:
        try:
            if datetime.fromisoformat(str(result.get("started_at"))).timestamp() < not_before:
                return False
        except ValueError:
            return False
    phases = result.get("phases")
    if not phases or not all(isinstance(p, dict) for p in phases):
        return False
    errors = [*(result.get("errors") or []), *(e for p in phases for e in (p.get("errors") or []))]
    return bool(errors) and all(_TRANSIENT_SCOPE_ERROR in str(e) for e in errors) and all(
        p.get("accounting_uncertain") == 0 and p.get("control_channel_failures") == 0
        and not (p.get("status_counts") or {}).get("schema_error") for p in phases)


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
    max_transient_failures: int = 3,
) -> dict[str, Any]:
    for value in (initial_elapsed_s, deadline_epoch, budget_s):
        if not math.isfinite(value) or value < 0:
            raise ValueError("refresh window accounting must be finite and nonnegative")
    outcomes: list[dict[str, Any]] = []
    blocked: dict[tuple[str, str, str], str] = {}
    used = 0.0
    transient = 0
    streak = 0
    failed = False
    status = "partial"
    reason = "round_cap"

    def report():
        # #1778: a red scope keeps the run red at every persisted point.
        return {"status": "failed" if failed else status,
                "stop_reason": reason if status == "failed" or not failed else "scope_failure",
                "elapsed_s": round(used, 3), "outcomes": list(outcomes),
                "blocked_scopes": sorted(blocked.values()),
                "transient_failures": sum(o.get("status") == "transient_failure" for o in outcomes),
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
        key = (f"SS-{env['SOFASCORE_TOURNAMENT_ID']}",
               env['SOFASCORE_CANONICAL_SEASON'], env['SOFASCORE_SOURCE_SEASON_ID'])
        # #1751: a provider blip (502/522, captcha) costs one scope, not the
        # window. The scope waits for the next run; only a streak means the
        # transport itself is down, and that stays red.
        if outcome.get("status") == "transient_failure":
            blocked[key] = env['SOFASCORE_SCOPE_KEY']
            transient += 1
            streak += 1
            if transient >= max_transient_failures:
                status, reason = "failed", "transient_failures"
                if persist:
                    persist(report())
                break
            if streak >= max_transient_failures:
                break
            if persist:
                persist(report())
            continue
        transient = 0
        # #1778: one red scope costs that scope, not the rest of the window;
        # the run stays red, and a streak of failures of any kind stops it.
        if outcome.get("status") not in {"success", "refreshed", "partial"}:
            blocked[key] = env['SOFASCORE_SCOPE_KEY']
            failed = True
            streak += 1
            if persist:
                persist(report())
            if streak >= max_transient_failures:
                break
            continue
        streak = 0
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
