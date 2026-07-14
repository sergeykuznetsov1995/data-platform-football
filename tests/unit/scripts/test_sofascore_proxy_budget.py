from __future__ import annotations

import copy
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from scrapers.sofascore.runtime_fingerprint import runtime_fingerprint
from scrapers.sofascore.workload_plan import (
    WORKLOAD_ARTIFACT_SCHEMA_VERSION,
    match_workload_class,
    production_match_shape,
    workload_shape_digest,
)
from scripts.proxy_filter.budget import (
    BudgetAccountingError,
    ProductionBudgetUnavailable,
    ProxyBudgetExceeded,
    SharedBudgetLedger,
    anonymize_proxy_exit,
    append_canary_sample,
    load_verified_policy,
    _validate_v3_sample_status_evidence,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
SHIPPED_ARTIFACT = REPO_ROOT / "configs" / "sofascore" / "proxy_budget_canary.json"

# A v3 class is named after its measured shape, never after a tournament: the
# fixtures below reproduce exactly the class this deployment signs for matches.
MATCH_SHAPE = production_match_shape()
MATCH_SHAPE_DIGEST = workload_shape_digest(MATCH_SHAPE)
MATCH_WORKLOAD_CLASS = match_workload_class()
MATCH_ENDPOINTS = tuple(MATCH_SHAPE["required_endpoints"])
MEASURED_TOURNAMENT_ID = 17


def test_verified_policy_rejects_not_supported_required_season_schedule():
    raw_class = {
        "scope": "season",
        "required_endpoints": ["schedule_last", "schedule_next", "standings_total"],
    }
    sample = {
        "evidence": {
            "planned_endpoints": 3,
            "raw_payload_count": 3,
            "season_plan_complete": True,
            "endpoint_status_counts": {
                "schedule_last": {"not_supported": 1},
                "schedule_next": {"success": 1},
                "standings_total": {"not_supported": 1},
            },
        }
    }

    with pytest.raises(ProductionBudgetUnavailable, match="not_supported"):
        _validate_v3_sample_status_evidence(
            "season_4a1738f5b7504ec2", raw_class, sample
        )


def _request_map(index: int) -> dict[str, list[int]]:
    # ``event`` carries the cold browser warm-up in the first sample, so the
    # class maximum is driven by one observation, exactly as in production.
    sizes = {
        "event": [1_000 if index == 0 else 100 + index, 10],
        "incidents": [50 + index],
        "lineups": [200 + index],
        "shotmap": [30 + index],
        "statistics": [40 + index],
    }
    return {endpoint: sizes[endpoint] for endpoint in MATCH_ENDPOINTS}


def _sample(index: int, *, exits: int = 5) -> dict:
    request_map = _request_map(index)
    requests = sum(len(values) for values in request_map.values())
    return {
        "run_id": f"cold-{index}",
        "workload_class": MATCH_WORKLOAD_CLASS,
        "source_tournament_id": MEASURED_TOURNAMENT_ID,
        "mode": "cold",
        "budget_eligible": True,
        "units": 25,
        "proxy_exit_hash": hashlib.sha256(
            f"exit-{index % exits}".encode()
        ).hexdigest(),
        "total_provider_bytes": sum(sum(values) for values in request_map.values()),
        "endpoint_provider_bytes": {
            endpoint: sum(values) for endpoint, values in request_map.items()
        },
        "endpoint_request_provider_bytes": request_map,
        "evidence": {
            "runtime_fingerprint_digest": runtime_fingerprint()["digest"],
            "planned_endpoints": requests,
            "raw_payload_count": requests,
            "endpoint_status_counts": {
                endpoint: {"success": len(values)}
                for endpoint, values in request_map.items()
            },
        },
    }


def _payload(*, runs: int = 20, exits: int = 5, verified: bool = True) -> dict:
    samples = [_sample(index, exits=exits) for index in range(runs)]
    return {
        "schema_version": WORKLOAD_ARTIFACT_SCHEMA_VERSION,
        "source": "sofascore",
        "meter": "proxy_filter_provider_path_v2",
        "budget_derivation": "max_observed_task_bytes_per_workload_class_v2",
        "runtime_fingerprint": runtime_fingerprint(),
        "verified": verified,
        "workload_classes": {
            MATCH_WORKLOAD_CLASS: {
                "scope": "match",
                "max_units": 25,
                "required_endpoints": list(MATCH_ENDPOINTS),
                "shape": dict(MATCH_SHAPE),
                "shape_digest": MATCH_SHAPE_DIGEST,
                "measured_tournament_ids": [MEASURED_TOURNAMENT_ID],
                "hard_task_bytes": max(
                    sample["total_provider_bytes"] for sample in samples
                ) if samples else None,
                "samples": samples,
            }
        },
        "benchmark_samples": [],
    }


def _artifact(path: Path, **kwargs) -> Path:
    path.write_text(json.dumps(_payload(**kwargs)), encoding="utf-8")
    return path


def test_shipped_historical_observations_do_not_authorize_paid_capture():
    # The shipped artifact is fail-closed today because it is a superseded v2
    # candidate; after the v3 re-bootstrap it stays fail-closed as an unverified
    # one.  Either way it must never authorize paid capture.
    with pytest.raises(
        ProductionBudgetUnavailable, match="not verified|re-bootstrap v3"
    ):
        load_verified_policy(
            SHIPPED_ARTIFACT,
            workload_class=MATCH_WORKLOAD_CLASS,
        )


def test_v3_requires_explicit_class_and_v1_is_fail_closed(tmp_path):
    artifact = _artifact(tmp_path / "v3.json")
    with pytest.raises(ProductionBudgetUnavailable, match="explicit workload_class"):
        load_verified_policy(artifact)

    legacy = tmp_path / "v1.json"
    legacy.write_text(json.dumps({"schema_version": 1, "verified": True}))
    with pytest.raises(ProductionBudgetUnavailable, match="cannot authorize production"):
        load_verified_policy(legacy)


def test_v2_artifact_cannot_authorize_production(tmp_path):
    # v2 classes are keyed by tournament, so their samples are not evidence for
    # a shape; no flag may re-interpret them as a production budget.
    payload = _payload()
    payload["schema_version"] = 2
    payload["workload_classes"]["match_batch_25_t17"] = payload[
        "workload_classes"
    ].pop(MATCH_WORKLOAD_CLASS)
    artifact = tmp_path / "v2.json"
    artifact.write_text(json.dumps(payload), encoding="utf-8")

    for kwargs in (
        {"workload_class": "match_batch_25_t17"},
        {"workload_class": MATCH_WORKLOAD_CLASS},
        {},
        {"allow_legacy_v1": True},
    ):
        with pytest.raises(
            ProductionBudgetUnavailable, match="re-bootstrap v3"
        ):
            load_verified_policy(artifact, **kwargs)


@pytest.mark.parametrize(
    ("runs", "exits", "message"),
    [(19, 5, "20 cold samples"), (20, 4, "5 distinct exits")],
)
def test_each_class_needs_twenty_cold_runs_and_five_exits(
    tmp_path, runs, exits, message
):
    artifact = _artifact(tmp_path / "candidate.json", runs=runs, exits=exits)
    with pytest.raises(ProductionBudgetUnavailable, match=message):
        load_verified_policy(artifact, workload_class=MATCH_WORKLOAD_CLASS)


def test_class_adapter_allows_any_endpoint_to_spend_measured_task_remainder(
    tmp_path,
):
    artifact = _artifact(tmp_path / "candidate.json")
    raw = artifact.read_bytes()
    policy = load_verified_policy(
        artifact,
        workload_class=MATCH_WORKLOAD_CLASS,
    )

    # The maximum observed cold task: the warm-up sample (1_000 + 10 event bytes
    # plus 50 + 200 + 30 + 40 for the remaining endpoints).
    assert policy.hard_run_bytes == 1_330
    for endpoint in MATCH_ENDPOINTS:
        assert policy.reservation_for(endpoint) == policy.hard_run_bytes
    assert policy.sample_count == 20
    assert policy.distinct_proxy_exits == 5
    assert policy.workload_class == MATCH_WORKLOAD_CLASS
    assert policy.parent_artifact_id == hashlib.sha256(raw).hexdigest()
    assert policy.artifact_id != policy.parent_artifact_id


def test_hard_task_bytes_cannot_contain_multiplier_or_stale_max(tmp_path):
    payload = _payload()
    payload["workload_classes"][MATCH_WORKLOAD_CLASS]["hard_task_bytes"] += 1
    artifact = tmp_path / "candidate.json"
    artifact.write_text(json.dumps(payload))
    with pytest.raises(ProductionBudgetUnavailable, match="max observed bytes"):
        load_verified_policy(artifact, workload_class=MATCH_WORKLOAD_CLASS)

    payload = _payload()
    payload["budget_multiplier"] = 1.2
    artifact.write_text(json.dumps(payload))
    with pytest.raises(ProductionBudgetUnavailable, match="multiplier"):
        load_verified_policy(artifact, workload_class=MATCH_WORKLOAD_CLASS)


def test_append_is_atomic_class_scoped_and_never_self_verifies(tmp_path):
    payload = _payload(runs=0, verified=False)
    artifact = tmp_path / "candidate.json"
    artifact.write_text(json.dumps(payload))
    sample = _sample(0)

    append_canary_sample(
        artifact,
        sample,
        workload_class=MATCH_WORKLOAD_CLASS,
    )

    stored = json.loads(artifact.read_text())
    measured = stored["workload_classes"][MATCH_WORKLOAD_CLASS]
    assert stored["verified"] is False
    assert measured["samples"] == [sample]
    assert measured["hard_task_bytes"] == sample["total_provider_bytes"]
    assert not list(tmp_path.glob("*.tmp-*"))


def test_append_rejects_duplicate_or_inexact_class_observation(tmp_path):
    artifact = tmp_path / "candidate.json"
    artifact.write_text(json.dumps(_payload(runs=0, verified=False)))
    sample = _sample(0)
    append_canary_sample(artifact, sample, workload_class=MATCH_WORKLOAD_CLASS)
    with pytest.raises(ValueError, match="globally unique"):
        append_canary_sample(artifact, sample, workload_class=MATCH_WORKLOAD_CLASS)

    broken = copy.deepcopy(_sample(1))
    del broken["endpoint_request_provider_bytes"]["lineups"]
    with pytest.raises(ValueError, match="not exact"):
        append_canary_sample(artifact, broken, workload_class=MATCH_WORKLOAD_CLASS)


def test_networkless_benchmark_requires_zero_lease_network_and_allocation(tmp_path):
    artifact = tmp_path / "candidate.json"
    artifact.write_text(json.dumps(_payload(runs=0, verified=False)))
    sample = {
        "run_id": "no-op-1",
        "workload_class": MATCH_WORKLOAD_CLASS,
        "mode": "no_op",
        "budget_eligible": False,
        "proxy_exit_hash": None,
        "total_provider_bytes": 0,
        "endpoint_provider_bytes": {},
        "endpoint_request_provider_bytes": {},
        "lease_count": 0,
        "network_request_count": 0,
        "allocation_bytes": 0,
    }
    append_canary_sample(artifact, sample, workload_class=MATCH_WORKLOAD_CLASS)
    stored = json.loads(artifact.read_text())
    assert stored["benchmark_samples"] == [sample]

    broken = dict(sample, run_id="no-op-2", allocation_bytes=1)
    with pytest.raises(ValueError, match="zero lease/network/allocation"):
        append_canary_sample(artifact, broken, workload_class=MATCH_WORKLOAD_CLASS)


def _policy(tmp_path: Path):
    return load_verified_policy(
        _artifact(tmp_path / "verified.json"),
        workload_class=MATCH_WORKLOAD_CLASS,
    )


def test_shared_ledger_stops_retry_at_exact_class_boundary(tmp_path):
    policy = _policy(tmp_path)
    ledger = SharedBudgetLedger(tmp_path / "ledger.json", policy)
    token, limit = ledger.reserve("task", "event")
    assert limit == policy.hard_run_bytes
    first_spend = 1_000
    ledger.consume("task", token, first_spend)
    ledger.finish("task", token, reported_provider_bytes=first_spend)
    lineup, lineup_limit = ledger.reserve("task", "lineups")
    assert lineup_limit == policy.hard_run_bytes - first_spend
    ledger.consume("task", lineup, lineup_limit)
    ledger.finish("task", lineup, reported_provider_bytes=lineup_limit)
    with pytest.raises(ProxyBudgetExceeded, match="before endpoint"):
        ledger.reserve("task", "event")


def test_provider_read_claim_is_atomic_and_refundable(tmp_path):
    ledger = SharedBudgetLedger(tmp_path / "ledger.json", _policy(tmp_path))
    token, limit = ledger.reserve("task", "event")
    assert ledger.claim("task", token, limit + 65_536) == limit
    with pytest.raises(ProxyBudgetExceeded, match="next tunnel read"):
        ledger.claim("task", token, 1)
    ledger.refund("task", token, 17)
    assert ledger.claim("task", token, 65_536) == 17
    assert ledger.finish("task", token, reported_provider_bytes=limit) == limit


def test_concurrent_provider_pumps_share_the_last_window(tmp_path):
    ledger = SharedBudgetLedger(tmp_path / "ledger.json", _policy(tmp_path))
    token, limit = ledger.reserve("task", "event")
    barrier = threading.Barrier(2)

    def claim():
        barrier.wait()
        return ledger.claim("task", token, 700)

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda _: claim(), range(2)))
    assert sum(claims) == limit


def test_ledger_files_are_private_and_raw_token_is_not_persisted(tmp_path):
    ledger = SharedBudgetLedger(tmp_path / "ledger.json", _policy(tmp_path))
    token, _ = ledger.reserve("task", "event")
    assert ledger.path.stat().st_mode & 0o777 == 0o600
    assert ledger.lock_path.stat().st_mode & 0o777 == 0o600
    assert token not in ledger.path.read_text()


def test_meter_mismatch_is_rejected(tmp_path):
    ledger = SharedBudgetLedger(tmp_path / "ledger.json", _policy(tmp_path))
    token, _ = ledger.reserve("task", "event")
    ledger.consume("task", token, 10)
    with pytest.raises(BudgetAccountingError, match="meter mismatch"):
        ledger.finish("task", token, reported_provider_bytes=9)


def test_exit_anonymizer_never_returns_the_raw_address():
    raw = "203.0.113.7"
    hashed = anonymize_proxy_exit(raw)
    assert len(hashed) == 64
    assert raw not in hashed
