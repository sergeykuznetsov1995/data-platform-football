"""Offline calibration must not infer full batches from compacted totals."""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.research.calibrate_sofascore_workload_policy import calibrate_policy
from scrapers.sofascore.workload_plan import WORKLOAD_METER, match_workload_class

pytestmark = pytest.mark.unit
AS_OF = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)
MATCH = match_workload_class()
SEASON = "season_c5a3a2e2e467619d"
POLICY = Path(__file__).resolve().parents[3] / "configs/sofascore/workload_policy.json"


def _policy():
    value = json.loads(POLICY.read_text())
    for entry in value["workload_classes"].values():
        entry["hard_task_bytes"] = 1000
        entry.pop("request_bound_bytes", None)
    return value


def _allocation(name=MATCH, spent=900, units=25, *, requests=None):
    scope = "match" if name == MATCH else "season"
    value = {
        "class": name, "scope": scope, "completed": True,
        "spent_provider_bytes": spent, "units": list(range(units)),
    }
    if requests is not None:
        value["lease_stats"] = [{
            "class": name, "scope": scope, "completed": True,
            "meter": WORKLOAD_METER, "attempt_provider_bytes": sum(sum(v) for v in requests.values()),
            "endpoint_request_provider_bytes": requests,
            "finished_at": "2026-10-08T12:00:00+00:00",
        }]
    return value


def _ledger(allocations, **run_changes):
    run = {
        "dag_id": "dag_backfill_sofascore_all_mens", "run_id": "scheduled__test",
        "created_at": "2026-10-08T10:00:00+00:00", "updated_at": "2026-10-08T13:00:00+00:00",
        "allocations": {f"alloc-{i}": a for i, a in enumerate(allocations)},
    }
    run.update(run_changes)
    return {"schema_version": 1, "runs": {"run": run}}


@pytest.mark.parametrize("count,expected", [(0, 1000), (29, 1000), (30, 900)])
def test_lowering_needs_thirty_full_successes(count, expected):
    output, report = calibrate_policy(_policy(), [_ledger([_allocation() for _ in range(count)])], as_of=AS_OF)
    assert output["workload_classes"][MATCH]["hard_task_bytes"] == expected
    assert report["classes"][MATCH]["full_success_count"] == count


def test_successful_full_maximum_can_raise_with_one_observation():
    output, _ = calibrate_policy(_policy(), [_ledger([_allocation(spent=1400)])], as_of=AS_OF)
    assert output["workload_classes"][MATCH]["hard_task_bytes"] == 1400


def test_partial_failed_old_future_and_nonstandard_samples_cannot_lower():
    failed = _allocation()
    failed["completed"] = False
    samples = [_ledger([_allocation(units=24) for _ in range(30)]), _ledger([failed], run_id="scheduled__failed")]
    samples += [_ledger([_allocation()], **changes) for changes in (
        {"created_at": "2026-09-08T10:00:00+00:00"},
        {"updated_at": "2026-10-10T10:00:00+00:00"},
        {"dag_id": "paid_canary"},
    )]
    output, report = calibrate_policy(_policy(), samples, as_of=AS_OF)
    assert output["workload_classes"][MATCH]["hard_task_bytes"] == 1000
    assert report["classes"][MATCH]["full_success_count"] == 0


def test_compacted_seasons_remain_full_but_match_sizes_are_unknown():
    seasons = [_allocation(SEASON, units=1) for _ in range(30)]
    matches = [_allocation() for _ in range(30)]
    for allocation in seasons + matches:
        allocation.pop("units")
    output, report = calibrate_policy(_policy(), [_ledger(seasons + matches, compacted=True)], as_of=AS_OF)
    assert report["classes"][SEASON]["full_success_count"] == 30
    assert report["classes"][MATCH]["full_success_count"] == 0
    assert output["workload_classes"][MATCH]["hard_task_bytes"] == 1000


def test_monotonic_floor_applies_inside_format_and_preserves_sparse_bands():
    baseline = _policy()
    first = "season_0c85282697b86c88"  # split_year 1_7, no observations
    baseline["workload_classes"][first]["hard_task_bytes"] = 1600
    output, _ = calibrate_policy(baseline, [_ledger([_allocation(SEASON, spent=1200, units=1)])], as_of=AS_OF)
    for entry in output["workload_classes"].values():
        if entry["scope"] == "season":
            assert entry["hard_task_bytes"] == (1600 if entry["shape"]["season_format"] == "split_year" else 1000)


def test_request_maximum_includes_warmup_and_gets_headroom_only_at_capture():
    allocations = [_allocation(requests={"event": [200, 20], "statistics": [10]}) for _ in range(30)]
    output, report = calibrate_policy(_policy(), [_ledger(allocations)], as_of=AS_OF)
    entry = output["workload_classes"][MATCH]
    assert entry["request_bound_bytes"]["event"] == 200
    assert entry["request_bound_bytes"]["statistics"] == 10
    assert entry["request_bound_bytes"]["lineups"] == 1000
    assert report["classes"][MATCH]["request_bounds"]["event"]["effective_bound_bytes"] == 230
    assert entry["hard_task_bytes"] == 900


def test_repeated_requests_in_one_allocation_do_not_satisfy_thirty_sample_floor():
    output, report = calibrate_policy(_policy(), [_ledger([_allocation(requests={"event": [1] * 100})])], as_of=AS_OF)
    assert output["workload_classes"][MATCH]["request_bound_bytes"]["event"] == 1000
    assert report["classes"][MATCH]["request_bounds"]["event"]["full_allocation_count"] == 1


def test_request_maps_that_disagree_with_meter_cannot_lower_bounds():
    allocations = [_allocation(requests={"event": [1]}) for _ in range(30)]
    for allocation in allocations:
        allocation["lease_stats"][0]["attempt_provider_bytes"] = 2
    output, report = calibrate_policy(_policy(), [_ledger(allocations)], as_of=AS_OF)
    assert output["workload_classes"][MATCH]["request_bound_bytes"]["event"] == 1000
    assert report["excluded"]["invalid_request_measurement"] == 30


def test_duplicate_ledgers_do_not_create_new_observations():
    ledger = _ledger([_allocation() for _ in range(15)])
    output, report = calibrate_policy(_policy(), [ledger, copy.deepcopy(ledger)], as_of=AS_OF)
    assert output["workload_classes"][MATCH]["hard_task_bytes"] == 1000
    assert report["classes"][MATCH]["full_success_count"] == 15
    conflicting = copy.deepcopy(ledger)
    conflicting["runs"]["run"]["allocations"]["alloc-0"]["spent_provider_bytes"] += 1
    with pytest.raises(ValueError, match="conflicting duplicate"):
        calibrate_policy(_policy(), [ledger, conflicting], as_of=AS_OF)
