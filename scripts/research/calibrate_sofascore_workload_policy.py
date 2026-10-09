"""Offline #1365 calibration from existing gateway allocation ledgers.

No source, gateway, environment or runtime writes. Run with ``python -m
scripts.research.calibrate_sofascore_workload_policy --help``. Policy values
stay BEFORE the existing 15% headroom in the plan/capture pipeline.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from scrapers.sofascore.workload_plan import (
    TEAM_COUNT_BANDS,
    WORKLOAD_METER,
    allocation_budget_bytes,
    load_static_workload_policy,
)

STANDARD_DAGS = frozenset({
    "dag_ingest_sofascore",
    "dag_backfill_sofascore_all_mens",
    "dag_refresh_sofascore_all_mens",
})
MIN_FULL_OBSERVATIONS = 30


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("calibration timestamps must have a timezone")
    return parsed.astimezone(timezone.utc)


def _request_map(lease: Mapping[str, Any]) -> dict[str, list[int]] | None:
    values = lease.get("endpoint_request_provider_bytes")
    if lease.get("meter") != WORKLOAD_METER or not isinstance(values, dict):
        return None
    if any(not isinstance(items, list) or not items or any(
        type(value) is not int or value < 0 for value in items
    ) for items in values.values()):
        return None
    if sum(sum(items) for items in values.values()) != lease.get("attempt_provider_bytes"):
        return None
    return values


def calibrate_policy(
    policy: Mapping[str, Any],
    ledgers: Sequence[Mapping[str, Any]],
    *,
    as_of: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Use full successful maxima; insufficient observations cannot lower caps.

    A compacted season is still a full single-unit allocation. A compacted
    match/player batch has unknown size and cannot justify a lower cap.
    A run's creation and last update must both lie in the 30-day window;
    this conservative interval also works after per-lease times are compacted.
    """
    if as_of.tzinfo is None:
        raise ValueError("as_of must have a timezone")
    as_of = as_of.astimezone(timezone.utc)
    start = as_of - timedelta(days=30)
    result = copy.deepcopy(dict(policy))
    classes = result["workload_classes"]
    totals: dict[str, list[int]] = {name: [] for name in classes}
    requests: dict[str, dict[str, list[int]]] = {name: {} for name in classes}
    request_full: dict[str, Counter] = {name: Counter() for name in classes}
    excluded: Counter = Counter()
    seen: dict[tuple[str, str, str], str] = {}
    for ledger in ledgers:
        if ledger.get("schema_version") != 1:
            raise ValueError("unsupported allocation ledger schema")
        for run in ledger["runs"].values():
            if run.get("dag_id") not in STANDARD_DAGS:
                excluded["nonstandard_dag"] += len(run["allocations"])
                continue
            created = _timestamp(run["created_at"])
            updated = _timestamp(run["updated_at"])
            if not start <= created <= updated <= as_of:
                excluded["outside_window_or_ambiguous_interval"] += len(run["allocations"])
                continue
            for allocation_id, allocation in run["allocations"].items():
                identity = (run["dag_id"], run["run_id"], allocation_id)
                fingerprint = hashlib.sha256(json.dumps(allocation, sort_keys=True).encode()).hexdigest()
                if identity in seen:
                    if seen[identity] != fingerprint:
                        raise ValueError("conflicting duplicate allocation observations")
                    excluded["duplicate_allocation"] += 1
                    continue
                seen[identity] = fingerprint
                name = allocation.get("class")
                if name not in classes or allocation.get("scope") != classes[name]["scope"]:
                    excluded["unknown_class_or_scope"] += 1
                    continue
                spent = allocation.get("spent_provider_bytes")
                if allocation.get("completed") is not True or type(spent) is not int or spent <= 0:
                    excluded["incomplete_or_zero_bytes"] += 1
                    continue
                units = allocation.get("units")
                full = (
                    isinstance(units, list) and len(units) == classes[name]["max_units"]
                ) or (
                    units is None and run.get("compacted") is True
                    and classes[name]["scope"] == "season"
                )
                if full:
                    totals[name].append(spent)
                else:
                    excluded["partial_or_unknown_batch_size"] += 1
                observed_full_endpoints: set[str] = set()
                for lease in allocation.get("lease_stats", []):
                    if lease.get("class") != name or lease.get("scope") != classes[name]["scope"]:
                        excluded["invalid_request_measurement"] += 1
                        continue
                    request_map = _request_map(lease)
                    if request_map is None or not created <= _timestamp(lease["finished_at"]) <= updated:
                        excluded["invalid_request_measurement"] += 1
                        continue
                    for endpoint, values in request_map.items():
                        if endpoint not in classes[name]["required_endpoints"]:
                            raise ValueError("request measurement has an unknown endpoint")
                        requests[name].setdefault(endpoint, []).extend(values)
                        if full and lease.get("completed") is True:
                            observed_full_endpoints.add(endpoint)
                request_full[name].update(observed_full_endpoints)

    report: dict[str, Any] = {
        "method": "offline_complete_allocation_max_v1",
        "window_start": start.isoformat(), "window_end": as_of.isoformat(),
        "minimum_full_observations_to_lower": MIN_FULL_OBSERVATIONS,
        "headroom_percent_applied_only_at_plan_and_capture": 15,
        "excluded": dict(sorted(excluded.items())), "classes": {},
    }
    for name, entry in classes.items():
        old = entry["hard_task_bytes"]
        values = totals[name]
        observed = max(values, default=0)
        cap = observed if len(values) >= MIN_FULL_OBSERVATIONS else max(old, observed)
        entry["hard_task_bytes"] = cap
        endpoint_report = {}
        old_bounds = entry.get("request_bound_bytes", {})
        new_bounds = {}
        for endpoint in entry["required_endpoints"]:
            request_values = requests[name].get(endpoint, [])
            request_max = max(request_values, default=0)
            old_bound = old_bounds.get(endpoint, old)
            n_full = request_full[name][endpoint]
            can_lower = n_full >= MIN_FULL_OBSERVATIONS and request_max > 0
            bound = request_max if can_lower else max(old_bound, request_max)
            new_bounds[endpoint] = bound
            endpoint_report[endpoint] = {
                "request_count": len(request_values), "full_allocation_count": n_full,
                "observed_max_bytes": request_max or None,
                "raw_bound_bytes": bound, "effective_bound_bytes": allocation_budget_bytes(bound),
                "retained_floor_for_insufficient_evidence": not can_lower,
            }
        entry["request_bound_bytes"] = new_bounds
        report["classes"][name] = {
            "scope": entry["scope"], "shape": entry["shape"],
            "full_success_count": len(values), "observed_max_bytes": observed or None,
            "old_hard_task_bytes": old, "candidate_hard_task_bytes": cap,
            "retained_floor_for_insufficient_evidence": len(values) < MIN_FULL_OBSERVATIONS,
            "request_bounds": endpoint_report,
        }

    # Prefix maxima implement the smallest nondecreasing season budgets.
    for season_format in {c["shape"]["season_format"] for c in classes.values() if c["scope"] == "season"}:
        floor = 0
        for band in TEAM_COUNT_BANDS:
            for entry in classes.values():
                if entry["scope"] == "season" and entry["shape"]["season_format"] == season_format and entry["shape"]["team_count_band"] == band:
                    floor = max(floor, entry["hard_task_bytes"])
                    entry["hard_task_bytes"] = floor
    for name, entry in classes.items():
        report["classes"][name]["hard_task_bytes"] = entry["hard_task_bytes"]
        report["classes"][name]["allocation_budget_bytes"] = allocation_budget_bytes(entry["hard_task_bytes"])
    result["updated_at"] = as_of.date().isoformat()
    result["calibration"] = {key: report[key] for key in (
        "method", "window_start", "window_end", "minimum_full_observations_to_lower",
    )}
    return result, report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, action="append", required=True)
    parser.add_argument("--as-of", type=_timestamp, required=True)
    parser.add_argument("--output-policy", type=Path, required=True)
    parser.add_argument("--output-report", type=Path, required=True)
    args = parser.parse_args()
    load_static_workload_policy(args.policy)
    baseline = args.policy.read_bytes()
    ledgers = [path.read_bytes() for path in args.ledger]
    policy, report = calibrate_policy(json.loads(baseline), [json.loads(raw) for raw in ledgers], as_of=args.as_of)
    report["baseline_policy_sha256"] = hashlib.sha256(baseline).hexdigest()
    report["ledger_snapshots"] = [{"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()} for path, raw in zip(args.ledger, ledgers)]
    for path, payload in ((args.output_policy, policy), (args.output_report, report)):
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    load_static_workload_policy(args.output_policy)


if __name__ == "__main__":
    main()
