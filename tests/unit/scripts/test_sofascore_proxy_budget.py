from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from scripts.proxy_filter.budget import (
    ProductionBudgetUnavailable,
    ProxyBudgetExceeded,
    SharedBudgetLedger,
    anonymize_proxy_exit,
    append_canary_sample,
    load_verified_policy,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
SHIPPED_ARTIFACT = REPO_ROOT / "configs" / "sofascore" / "proxy_budget_canary.json"


def _artifact(path, *, runs=20, exits=5, verified=True):
    metrics = {
        "browser_sessions": 1,
        "navigations": 1,
        "request_count": 100,
        "completed_matches": 25,
        "completed_players": 50,
        "matches_per_second": 2.5,
        "players_per_second": 5.0,
        "p50_duration_ms": 10,
        "p95_duration_ms": 20,
        "cache_hit_rate": 0.0,
        "replay_hit_rate": 0.0,
        "endpoint_completeness": 1.0,
    }
    samples = []
    for index in range(runs):
        event = 100 + index
        lineups = 200 + index
        samples.append(
            {
                "run_id": f"canary-{index}",
                "budget_eligible": True,
                "cohort": "25_matches_50_players",
                "mode": "cold",
                "proxy_exit_hash": f"exit-hash-{index % exits:02d}",
                "total_provider_bytes": event + lineups,
                "endpoint_provider_bytes": {"event": event, "lineups": lineups},
                "endpoint_request_provider_bytes": {
                    "event": [event],
                    "lineups": [lineups],
                },
                "metrics": metrics,
            }
        )
    for mode in ("no_op", "offline_replay", "single_endpoint_resume"):
        samples.append(
            {
                "run_id": f"benchmark-{mode}",
                "budget_eligible": False,
                "cohort": "25_matches_50_players",
                "mode": mode,
                "proxy_exit_hash": "exit-hash-benchmark",
                "total_provider_bytes": 0,
                "endpoint_provider_bytes": {"event": 0, "lineups": 0},
                "endpoint_request_provider_bytes": {"event": [0], "lineups": [0]},
                "metrics": {},
            }
        )
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "sofascore",
                "meter": "proxy_filter_provider_path_v2",
                "verified": verified,
                "samples": samples,
            }
        )
    )
    return path


def test_shipped_historical_observations_do_not_authorize_paid_capture():
    with pytest.raises(ProductionBudgetUnavailable, match="not verified"):
        load_verified_policy(SHIPPED_ARTIFACT)


@pytest.mark.parametrize(
    ("runs", "exits", "message"),
    [(19, 5, "20 budget-eligible logical runs"), (20, 4, "5 distinct proxy exits")],
)
def test_small_or_narrow_canary_fails_closed(tmp_path, runs, exits, message):
    path = _artifact(tmp_path / "canary.json", runs=runs, exits=exits)
    with pytest.raises(ProductionBudgetUnavailable, match=message):
        load_verified_policy(path)


def test_policy_is_exact_nearest_rank_p95_without_handwritten_multiplier(tmp_path):
    policy = load_verified_policy(_artifact(tmp_path / "canary.json"))
    # 20 values -> nearest-rank p95 is the 19th ordered sample (index 18).
    assert policy.hard_run_bytes == (100 + 18) + (200 + 18)
    assert policy.reservation_for("event") == 118
    assert policy.reservation_for("lineups") == 218
    assert policy.sample_count == 20
    assert policy.distinct_proxy_exits == 5


def test_zero_byte_noop_benchmark_does_not_dilute_paid_budget_p95(tmp_path):
    artifact = _artifact(tmp_path / "canary.json")
    payload = json.loads(artifact.read_text())
    payload["samples"].append(
        {
            "run_id": "no-op-extra",
            "budget_eligible": False,
            "cohort": "25_matches_50_players",
            "mode": "no_op",
            "proxy_exit_hash": "exit-hash-noop",
            "total_provider_bytes": 0,
            "endpoint_provider_bytes": {"event": 0, "lineups": 0},
            "endpoint_request_provider_bytes": {"event": [0], "lineups": [0]},
            "metrics": {},
        }
    )
    artifact.write_text(json.dumps(payload))
    policy = load_verified_policy(artifact)
    assert policy.sample_count == 20
    assert policy.hard_run_bytes == 336


def test_shared_ledger_stops_retry_before_crossing_whole_run_budget(tmp_path):
    policy = load_verified_policy(_artifact(tmp_path / "canary.json"))
    ledger = SharedBudgetLedger(tmp_path / "ledger.json", policy)
    event_token, event_limit = ledger.reserve("dag-run", "event")
    ledger.consume("dag-run", event_token, event_limit)
    ledger.finish("dag-run", event_token, reported_provider_bytes=event_limit)

    # Remaining p95 run allowance is 218 bytes: lineups fits exactly.
    lineup_token, lineup_limit = ledger.reserve("dag-run", "lineups")
    assert lineup_limit == 218
    ledger.consume("dag-run", lineup_token, lineup_limit)
    ledger.finish("dag-run", lineup_token, reported_provider_bytes=lineup_limit)

    # A retry is rejected before a new provider connection is authorized.
    with pytest.raises(ProxyBudgetExceeded, match="before endpoint"):
        ledger.reserve("dag-run", "event")
    snapshot = ledger.snapshot("dag-run")
    assert snapshot["spent_provider_bytes"] == policy.hard_run_bytes


def test_provider_chunk_over_reservation_is_not_committed(tmp_path):
    policy = load_verified_policy(_artifact(tmp_path / "canary.json"))
    ledger = SharedBudgetLedger(tmp_path / "ledger.json", policy)
    token, limit = ledger.reserve("run", "event")
    with pytest.raises(ProxyBudgetExceeded, match="endpoint"):
        ledger.consume("run", token, limit + 1)
    assert ledger.snapshot("run")["spent_provider_bytes"] == 0


def test_provider_read_claim_is_atomic_and_short_read_tail_is_refundable(tmp_path):
    policy = load_verified_policy(_artifact(tmp_path / "canary.json"))
    ledger = SharedBudgetLedger(tmp_path / "ledger.json", policy)
    token, limit = ledger.reserve("run", "event")

    assert ledger.claim("run", token, limit + 65536) == limit
    with pytest.raises(ProxyBudgetExceeded, match="before the next tunnel read"):
        ledger.claim("run", token, 1)
    assert ledger.snapshot("run")["spent_provider_bytes"] == limit

    ledger.refund("run", token, 17)
    assert ledger.snapshot("run")["spent_provider_bytes"] == limit - 17
    assert ledger.claim("run", token, 65536) == 17
    assert ledger.finish(
        "run", token, reported_provider_bytes=limit
    ) == limit


def test_concurrent_pumps_split_the_atomic_last_provider_window(tmp_path):
    """Both tunnel directions may claim concurrently, but their final chunks
    must partition the one endpoint allowance instead of each observing it."""
    policy = load_verified_policy(_artifact(tmp_path / "canary.json"))
    ledger = SharedBudgetLedger(tmp_path / "ledger.json", policy)
    token, limit = ledger.reserve("run", "event")
    barrier = threading.Barrier(2)

    def claim():
        barrier.wait()
        return ledger.claim("run", token, 100)

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda _: claim(), range(2)))

    assert sorted(claims) == [limit - 100, 100]
    assert sum(claims) == limit
    assert ledger.snapshot("run")["spent_provider_bytes"] == limit
    with pytest.raises(ProxyBudgetExceeded, match="before the next tunnel read"):
        ledger.claim("run", token, 1)


def test_collector_hashes_exits_and_never_self_approves(tmp_path):
    artifact = _artifact(tmp_path / "canary.json", verified=True)
    payload = json.loads(artifact.read_text())
    payload["samples"] = []
    artifact.write_text(json.dumps(payload))
    append_canary_sample(
        artifact,
        {
            "run_id": "new-run",
            "budget_eligible": True,
            "cohort": "25_matches_50_players",
            "mode": "cold",
            "proxy_exit_hash": anonymize_proxy_exit("203.0.113.7"),
            "total_provider_bytes": 30,
            "endpoint_provider_bytes": {"event": 30},
            "endpoint_request_provider_bytes": {"event": [30]},
            "metrics": {
                "browser_sessions": 1,
                "navigations": 1,
                "request_count": 1,
                "completed_matches": 25,
                "completed_players": 50,
                "matches_per_second": 1.0,
                "players_per_second": 1.0,
                "p50_duration_ms": 1,
                "p95_duration_ms": 1,
                "cache_hit_rate": 0.0,
                "replay_hit_rate": 0.0,
                "endpoint_completeness": 1.0,
            },
        },
    )
    stored = json.loads(artifact.read_text())
    assert stored["verified"] is False
    assert "203.0.113.7" not in artifact.read_text()
