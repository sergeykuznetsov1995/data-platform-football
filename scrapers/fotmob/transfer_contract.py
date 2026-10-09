"""Additional evidence checks for the dedicated transfer-only profile."""

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone

from .transfers import (
    TRANSFER_MAX_DIRECT_MIB,
    TRANSFER_MAX_REQUESTS,
    TRANSFER_PROFILE,
    transfer_journal_signature,
)


def _instant(value):
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include UTC offset")
    return parsed.astimezone(timezone.utc)


def validate_transfer_report(report: Mapping) -> list[str]:
    """Validate budget and per-competition freshness, without external reads.

    The generic catalog validator still checks the full catalog and structural
    classification evidence. A partial run is allowed; a false fresh/completed
    claim or a widened profile is not.
    """
    errors = []
    selection = report.get("selection") or {}
    contract = selection.get("catalog_contract") or {}
    if (
        report.get("mode") != "transfers"
        or selection.get("profile") != TRANSFER_PROFILE
        or selection.get("entities") != ["transfers"]
        or selection.get("scope_lane") != "current"
        or selection.get("explicit_scopes") != []
        or selection.get("planned_scopes") != []
        or selection.get("scope_attempts") != []
        or contract.get("scopes") != []
        or selection.get("competition_limit") != 0
        or selection.get("season_limit") != 0
        or selection.get("journal_transfer_plan_signature") != transfer_journal_signature()
    ):
        errors.append("transfer-only selection profile differs")
    status = report.get("transfer_status")
    if not isinstance(status, Mapping):
        return errors + ["missing durable transfer status"]
    if selection.get("flush_succeeded") is not True or not any(
        isinstance(op, Mapping) and op.get("entity") == "commit_flush"
        and op.get("status") == "success" and op.get("succeeded") == 1
        and not any(op.get(key) for key in ("errors", "retryable", "terminal"))
        for op in report.get("operations", [])
    ):
        errors.append("transfer report lacks successful Bronze flush")
    if selection.get("catalog_complete") is not True or status.get("catalog_complete") is not True:
        errors.append("transfer catalog is not completely validated")
    ids = contract.get("included_ids")
    if not isinstance(ids, list) or not ids or ids != status.get("included_ids"):
        return errors + ["transfer freshness cohort differs from catalog"]
    if report.get("family_summary") != status.get("family_summary"):
        errors.append("transfer family summary differs from durable status")
    daily = report.get("daily_budget")
    if not isinstance(daily, Mapping) or daily != status.get("daily_budget"):
        return errors + ["transfer daily budget evidence differs"]
    caps = {
        "requests": TRANSFER_MAX_REQUESTS,
        "direct_bytes": TRANSFER_MAX_DIRECT_MIB * 1024 * 1024,
        "proxy_bytes": 0,
    }
    run_budget = report.get("budget") or {}
    for key, cap in caps.items():
        total = daily.get(key)
        run_total = run_budget.get(key)
        run_cap = run_budget.get("max_" + key)
        if (
            type(total) is not int or not 0 <= total <= cap
            or daily.get("max_" + key) != cap
            or type(run_total) is not int or not 0 <= run_total <= total
            or type(run_cap) is not int or not 0 <= run_cap <= cap
            or (type(run_total) is int and type(run_cap) is int and run_total > run_cap)
        ):
            errors.append(f"transfer daily {key} budget is invalid")
    if daily.get("reserved") is not False:
        errors.append("transfer budget reservation is not finalized")
    try:
        checked_at = _instant(status.get("checked_at"))
        catalog_at = _instant(status.get("catalog_checked_at"))
        day = checked_at.date().isoformat()
        if daily.get("day") != day or status.get("day") != day:
            errors.append("transfer daily budget date differs")
        timestamps = status.get("completion_timestamps")
        if not isinstance(timestamps, Mapping) or set(timestamps) - {str(i) for i in ids}:
            return errors + ["transfer completion timestamps are invalid"]
        fresh = stale = unknown = 0
        ages, completed_today = [], []
        for competition_id in ids:
            stamp = timestamps.get(str(competition_id))
            if stamp is None:
                unknown += 1
                continue
            completed_at = _instant(stamp)
            age = (checked_at - completed_at).total_seconds()
            if age < 0:
                errors.append("transfer completion timestamp is in the future")
                unknown += 1
                continue
            ages.append(age)
            if age > timedelta(hours=48).total_seconds():
                stale += 1
            else:
                fresh += 1
            if completed_at.date().isoformat() == day:
                completed_today.append(competition_id)
        catalog_fresh = (
            status.get("catalog_complete") is True
            and timedelta(0) <= checked_at - catalog_at <= timedelta(hours=48)
        )
        expected_summary = {
            "included_count": len(ids), "fresh_count": fresh,
            "stale_count": stale, "unknown_count": unknown,
            "max_age_seconds": max(ages) if ages else None,
            "status": "green" if catalog_fresh and not stale and not unknown else "red",
            "checked_at": status.get("checked_at"),
        }
        if report.get("family_summary") != expected_summary:
            errors.append("transfer family freshness does not recompute")
        expected_complete = bool(
            catalog_fresh and catalog_at.date().isoformat() == day
            and len(completed_today) == len(ids)
        )
        if status.get("daily_complete") is not expected_complete:
            errors.append("transfer daily completion does not recompute")
        if selection.get("completed_transfer_competition_ids") != sorted(completed_today):
            errors.append("transfer completed IDs differ from timestamps")
        if status.get("completed_transfer_competition_ids") != sorted(completed_today):
            errors.append("transfer durable completed IDs differ from timestamps")
        if report.get("complete") is not expected_complete:
            errors.append("transfer run completeness differs from daily evidence")
    except (TypeError, ValueError, OverflowError):
        errors.append("transfer freshness timestamps are invalid")
    return errors
