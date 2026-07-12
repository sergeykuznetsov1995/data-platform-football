from __future__ import annotations

import json
from pathlib import Path

import pytest

from scrapers.sofascore.runtime_fingerprint import runtime_fingerprint
from scrapers.sofascore.workload_plan import (
    MATCH_BATCH_SIZE,
    MATCH_WORKLOAD_CLASS,
    PLAYER_BATCH_SIZE,
    PLAYER_UNIVERSE_TASK_ID,
    PLAYER_WORKLOAD_CLASS,
    AllocationAccountingError,
    AllocationBudgetExceeded,
    AllocationLedger,
    ConcurrentAllocation,
    DuplicateAllocation,
    SeasonWorkload,
    SignedDagRunPlan,
    UnknownAllocation,
    WorkloadPlanError,
    WorkloadPlanSignatureError,
    WorkloadPolicyUnavailable,
    build_signed_dagrun_plan,
    load_verified_workload_policy,
    match_workload_class,
    player_workload_class,
    production_season_shape,
    season_shape_digest,
    season_workload_class,
    stable_partitions,
)


CONTROL_TOKEN = "unit-test-control-token-that-is-longer-than-32-bytes"
SEASON_SHAPE = {
    "format": "league",
    "schedule_pages": 3,
    "rounds": True,
    "standings_groups": 1,
    "source_tournament_id": "17",
}


def _class_samples(
    *,
    units: int,
    endpoints: tuple[str, ...],
    workload_class: str,
    source_tournament_id: int,
    count: int = 20,
    exits: int = 5,
) -> list[dict]:
    fingerprint_digest = runtime_fingerprint()["digest"]
    samples = []
    for index in range(count):
        request_map = {
            endpoint: [100 + index + endpoint_index, 10]
            for endpoint_index, endpoint in enumerate(endpoints)
        }
        samples.append(
            {
                "run_id": f"cold-{units}-{index}",
                "workload_class": workload_class,
                "source_tournament_id": source_tournament_id,
                "mode": "cold",
                "budget_eligible": True,
                "units": units,
                "proxy_exit_hash": f"anonymized-exit-{index % exits:02d}",
                "total_provider_bytes": sum(
                    sum(values) for values in request_map.values()
                ),
                "endpoint_request_provider_bytes": request_map,
                "evidence": {
                    "runtime_fingerprint_digest": fingerprint_digest,
                },
            }
        )
    return samples


def _class_payload(
    *,
    scope: str,
    max_units: int,
    endpoints: tuple[str, ...],
    workload_class: str,
    source_tournament_id: int,
    shape_digest: str | None = None,
    shape: dict | None = None,
) -> dict:
    samples = _class_samples(
        units=max_units,
        endpoints=endpoints,
        workload_class=workload_class,
        source_tournament_id=source_tournament_id,
    )
    payload = {
        "scope": scope,
        "source_tournament_id": source_tournament_id,
        "max_units": max_units,
        "required_endpoints": list(endpoints),
        "hard_task_bytes": max(item["total_provider_bytes"] for item in samples),
        "samples": samples,
    }
    if shape_digest is not None:
        payload["shape_digest"] = shape_digest
    if shape is not None:
        payload["shape"] = shape
    return payload


def _artifact_payload() -> dict:
    season_class = season_workload_class(17, SEASON_SHAPE)
    digest = season_shape_digest(SEASON_SHAPE)
    return {
        "schema_version": 2,
        "source": "sofascore",
        "meter": "proxy_filter_provider_path_v2",
        "budget_derivation": "max_observed_task_bytes_per_workload_class_v2",
        "runtime_fingerprint": runtime_fingerprint(),
        "verified": True,
        "workload_classes": {
            MATCH_WORKLOAD_CLASS: _class_payload(
                scope="match",
                max_units=25,
                endpoints=("event", "lineups"),
                workload_class=MATCH_WORKLOAD_CLASS,
                source_tournament_id=17,
            ),
            PLAYER_WORKLOAD_CLASS: _class_payload(
                scope="player",
                max_units=50,
                endpoints=("player_profile", "player_season_statistics"),
                workload_class=PLAYER_WORKLOAD_CLASS,
                source_tournament_id=17,
            ),
            season_class: _class_payload(
                scope="season",
                max_units=1,
                endpoints=("rounds", "schedule", "standings"),
                workload_class=season_class,
                source_tournament_id=17,
                shape_digest=digest,
                shape=SEASON_SHAPE,
            ),
        },
    }


def _write_artifact(path: Path, payload: dict | None = None) -> Path:
    path.write_text(
        json.dumps(payload or _artifact_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _policy(tmp_path: Path):
    return load_verified_workload_policy(_write_artifact(tmp_path / "workloads.json"))


def _plan(tmp_path: Path, **overrides):
    values = {
        "dag_id": "dag_ingest_sofascore",
        "run_id": "scheduled__2026-07-11T00:00:00+00:00",
        "pending_match_ids": [5, 2, 10],
        "player_universe_ids": [100, 2, 30, 4],
        "pending_player_ids": [30, 2],
        "season_workloads": [SeasonWorkload(17, 76986, SEASON_SHAPE)],
        "source_tournament_id": 17,
        "control_token": CONTROL_TOKEN,
    }
    values.update(overrides)
    return build_signed_dagrun_plan(_policy(tmp_path), **values)


def test_v2_policy_derives_each_hard_task_cap_from_exact_observed_max(tmp_path):
    policy = _policy(tmp_path)

    match = policy.classes[MATCH_WORKLOAD_CLASS]
    player = policy.classes[PLAYER_WORKLOAD_CLASS]
    season = policy.classes[season_workload_class(17, SEASON_SHAPE)]

    assert match.hard_task_bytes == (119 + 10) + (120 + 10)
    assert player.hard_task_bytes == (119 + 10) + (120 + 10)
    assert season.hard_task_bytes == (119 + 10) + (120 + 10) + (121 + 10)
    assert match.sample_count == 20
    assert match.distinct_proxy_exits == 5
    assert season.shape_digest == season_shape_digest(SEASON_SHAPE)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda value: value["workload_classes"][MATCH_WORKLOAD_CLASS][
                "samples"
            ].pop(),
            "20 cold samples",
        ),
        (
            lambda value: [
                sample.update(proxy_exit_hash="same-anonymized-exit")
                for sample in value["workload_classes"][MATCH_WORKLOAD_CLASS]["samples"]
            ],
            "5 distinct exits",
        ),
        (
            lambda value: value["workload_classes"][MATCH_WORKLOAD_CLASS].update(
                hard_task_bytes=999999
            ),
            "must equal max observed",
        ),
        (
            lambda value: value.update(budget_multiplier=1.25),
            "cannot use a multiplier",
        ),
        (
            lambda value: value["workload_classes"][MATCH_WORKLOAD_CLASS]["samples"][
                0
            ].update(total_provider_bytes=1),
            "must equal the exact request map",
        ),
        (
            lambda value: value["workload_classes"][MATCH_WORKLOAD_CLASS]["samples"][0][
                "endpoint_request_provider_bytes"
            ].pop("lineups"),
            "endpoint mismatch",
        ),
        (
            lambda value: value["runtime_fingerprint"].update(digest="0" * 64),
            "does not match current runtime",
        ),
        (
            lambda value: value["workload_classes"][MATCH_WORKLOAD_CLASS]["samples"][
                0
            ]["evidence"].update(runtime_fingerprint_digest="0" * 64),
            "another runtime fingerprint",
        ),
    ],
)
def test_v2_policy_fails_closed_for_unmeasured_or_inexact_classes(
    tmp_path, mutate, message
):
    payload = _artifact_payload()
    mutate(payload)

    with pytest.raises(WorkloadPolicyUnavailable, match=message):
        load_verified_workload_policy(_write_artifact(tmp_path / "bad.json", payload))


def test_stable_partitions_include_every_id_exactly_once():
    ids = list(range(71, 0, -1))

    first = stable_partitions(ids, MATCH_BATCH_SIZE)
    second = stable_partitions(list(reversed(ids)), MATCH_BATCH_SIZE)

    assert first == second
    assert [len(batch) for batch in first] == [25, 25, 21]
    flattened = [item for batch in first for item in batch]
    assert flattened == [str(item) for item in range(1, 72)]
    assert len(flattened) == len(set(flattened)) == len(ids)


def test_full_player_universe_is_signed_before_pending_ids_are_sliced(tmp_path):
    universe = list(range(130, 0, -1))
    pending = list(range(125, 4, -2))
    plan = _plan(
        tmp_path,
        pending_match_ids=[],
        player_universe_ids=universe,
        pending_player_ids=pending,
        season_workloads=[],
    )

    assert plan.player_universe_ids == tuple(str(item) for item in range(1, 131))
    player_batches = [item for item in plan.allocations if item.scope == "player"]
    assert [len(item.units) for item in player_batches] == [50, 11]
    assert [unit for item in player_batches for unit in item.units] == [
        str(item) for item in sorted(pending)
    ]
    assert plan.dq_dependencies[0] == PLAYER_UNIVERSE_TASK_ID
    assert plan.dq_dependencies[1:] == tuple(item.task_id for item in player_batches)
    reloaded = SignedDagRunPlan.from_dict(plan.to_dict(), control_token=CONTROL_TOKEN)
    assert reloaded == plan


def test_match_and_player_batches_never_cross_fixed_limits(tmp_path):
    plan = _plan(
        tmp_path,
        pending_match_ids=list(range(1, 64)),
        player_universe_ids=list(range(1, 122)),
        pending_player_ids=list(range(1, 122)),
        season_workloads=[],
    )
    match = [item for item in plan.allocations if item.scope == "match"]
    player = [item for item in plan.allocations if item.scope == "player"]
    assert [len(item.units) for item in match] == [25, 25, 13]
    assert [len(item.units) for item in player] == [50, 50, 21]
    assert max(len(item.units) for item in match) <= MATCH_BATCH_SIZE
    assert max(len(item.units) for item in player) <= PLAYER_BATCH_SIZE


def test_run_cap_is_exact_sum_of_unique_allocation_caps(tmp_path):
    plan = _plan(tmp_path)
    assert plan.run_cap_bytes == sum(item.budget_bytes for item in plan.allocations)
    assert len({item.allocation_id for item in plan.allocations}) == len(
        plan.allocations
    )
    assert len({item.task_id for item in plan.allocations}) == len(plan.allocations)


def test_plan_is_deterministic_and_hmac_tampering_fails(tmp_path):
    first = _plan(tmp_path)
    second = _plan(tmp_path)
    assert first == second

    tampered = first.to_dict()
    tampered["run_cap_bytes"] = first.run_cap_bytes + 1
    with pytest.raises(WorkloadPlanSignatureError, match="digest"):
        SignedDagRunPlan.from_dict(tampered, control_token=CONTROL_TOKEN)
    with pytest.raises(WorkloadPlanSignatureError, match="signature"):
        first.verify("a-different-control-token-that-is-long-enough")


def test_freshness_snapshot_is_hmac_signed_and_scope_complete(tmp_path):
    plan = _plan(
        tmp_path,
        freshness_keys={
            "season": "day-2026-07-11",
            "match": "repair-run-42",
            "player": "week-2026-W28",
        },
    )
    assert plan.freshness_key("season") == "day-2026-07-11"
    assert plan.freshness_key("match") == "repair-run-42"
    assert plan.freshness_key("player") == "week-2026-W28"

    tampered = plan.to_dict()
    tampered["freshness_keys"]["player"] = "week-2099-W01"
    with pytest.raises(WorkloadPlanSignatureError, match="digest"):
        SignedDagRunPlan.from_dict(tampered, control_token=CONTROL_TOKEN)

    with pytest.raises(WorkloadPlanError, match="exactly season, match and player"):
        _plan(tmp_path, freshness_keys={"season": "day-2026-07-11"})


def test_season_budget_is_specific_to_tournament_and_exact_shape(tmp_path):
    plan = _plan(tmp_path)
    season = next(item for item in plan.allocations if item.scope == "season")
    assert season.workload_class == season_workload_class(17, SEASON_SHAPE)

    changed_shape = dict(SEASON_SHAPE, schedule_pages=4)
    with pytest.raises(WorkloadPolicyUnavailable, match="has no class"):
        _plan(
            tmp_path,
            season_workloads=[SeasonWorkload(17, 76986, changed_shape)],
        )


def test_match_and_player_caps_are_specific_to_source_tournament(tmp_path):
    assert MATCH_WORKLOAD_CLASS == match_workload_class(17)
    assert PLAYER_WORKLOAD_CLASS == player_workload_class(17)
    assert match_workload_class(16) != MATCH_WORKLOAD_CLASS
    assert player_workload_class(16) != PLAYER_WORKLOAD_CLASS

    with pytest.raises(WorkloadPolicyUnavailable, match="has no class"):
        _plan(tmp_path, source_tournament_id=16, season_workloads=[])


def test_policy_rejects_sample_from_another_tournament(tmp_path):
    payload = _artifact_payload()
    payload["workload_classes"][MATCH_WORKLOAD_CLASS]["samples"][0][
        "source_tournament_id"
    ] = 16
    with pytest.raises(WorkloadPolicyUnavailable, match="measured for tournament"):
        load_verified_workload_policy(_write_artifact(tmp_path / "bad.json", payload))


def test_production_season_shape_is_bounded_and_tournament_specific():
    epl = production_season_shape(
        17, season_format="split_year", max_pages_per_direction=50
    )
    world_cup = production_season_shape(
        16, season_format="calendar_year", max_pages_per_direction=50
    )

    assert epl["source_tournament_id"] == "17"
    assert epl["schedule_page_chain"]["max_pages_per_direction"] == 50
    assert str(epl["dynamic_evidence"]).endswith("_v1")
    assert season_workload_class(17, epl) != season_workload_class(16, world_cup)


def test_noop_plan_has_zero_cap_and_does_not_create_ledger_or_lease(tmp_path):
    plan = _plan(
        tmp_path,
        pending_match_ids=[],
        player_universe_ids=[],
        pending_player_ids=[],
        season_workloads=[],
    )
    ledger_path = tmp_path / "allocations.json"

    assert plan.allocations == ()
    assert plan.run_cap_bytes == 0
    assert plan.requires_proxy_lease is False
    assert plan.dq_dependencies == ()
    assert not ledger_path.exists()
    ledger = AllocationLedger(ledger_path, control_token=CONTROL_TOKEN)
    with pytest.raises(UnknownAllocation):
        ledger.claim(plan, "made-up", attempt_id="try-1")
    assert not ledger_path.exists()


def test_unknown_and_concurrent_allocations_are_rejected(tmp_path):
    plan = _plan(tmp_path)
    ledger = AllocationLedger(
        tmp_path / "allocations.json", control_token=CONTROL_TOKEN
    )
    allocation = plan.allocations[0]

    with pytest.raises(UnknownAllocation):
        ledger.claim(plan, "alloc-unknown", attempt_id="try-1")
    first = ledger.claim(plan, allocation.allocation_id, attempt_id="try-1")
    with pytest.raises(ConcurrentAllocation):
        ledger.claim(plan, allocation.allocation_id, attempt_id="try-2")

    ledger.finish(
        plan,
        first,
        lease_id="lease-1",
        endpoint_request_provider_bytes={},
        completed=True,
    )
    with pytest.raises(DuplicateAllocation, match="already complete"):
        ledger.claim(plan, allocation.allocation_id, attempt_id="try-2")


def test_retry_reuses_same_allocation_and_only_its_remaining_bytes(tmp_path):
    plan = _plan(tmp_path)
    allocation = plan.allocations[0]
    path = tmp_path / "allocations.json"
    first_ledger = AllocationLedger(path, control_token=CONTROL_TOKEN)
    first = first_ledger.claim(
        plan, allocation.allocation_id, attempt_id="airflow-try-1"
    )
    first_ledger.consume(plan, first, 100)
    first_ledger.finish(
        plan,
        first,
        lease_id="lease-try-1",
        endpoint_request_provider_bytes={"event": [60], "lineups": [40]},
        completed=False,
        proxy_exit_hash="hashed-proxy-exit-01",
    )

    # A process restart reads the durable ledger. Building the same DagRun
    # produces exactly the same allocation; it cannot mint a fresh cap.
    rebuilt = _plan(tmp_path)
    assert rebuilt.allocations[0].allocation_id == allocation.allocation_id
    second_ledger = AllocationLedger(path, control_token=CONTROL_TOKEN)
    retry = second_ledger.claim(
        rebuilt, allocation.allocation_id, attempt_id="airflow-try-2"
    )
    assert retry.spent_provider_bytes == 100
    assert retry.remaining_provider_bytes == allocation.budget_bytes - 100
    with pytest.raises(AllocationBudgetExceeded):
        second_ledger.consume(rebuilt, retry, retry.remaining_provider_bytes + 1)


def test_proxy_wal_can_resume_exact_active_claim_after_process_restart(tmp_path):
    plan = _plan(tmp_path)
    allocation = plan.allocations[0]
    path = tmp_path / "allocations.json"
    durable_claim_token = "proxy-wal-claim-token-that-is-at-least-32-bytes"
    first_ledger = AllocationLedger(path, control_token=CONTROL_TOKEN)
    first = first_ledger.claim(
        plan,
        allocation.allocation_id,
        attempt_id="try-1",
        claim_token=durable_claim_token,
    )
    first_ledger.consume(plan, first, 40)

    restarted = AllocationLedger(path, control_token=CONTROL_TOKEN)
    resumed = restarted.resume_claim(
        plan,
        allocation.allocation_id,
        claim_token=durable_claim_token,
    )
    assert resumed.claim_token == first.claim_token
    assert resumed.spent_provider_bytes == 40
    assert resumed.remaining_provider_bytes == allocation.budget_bytes - 40
    with pytest.raises(AllocationAccountingError, match="recovery token"):
        restarted.resume_claim(
            plan,
            allocation.allocation_id,
            claim_token="not-the-durable-wal-token",
        )


def test_same_run_cannot_replace_plan_to_mint_another_allocation(tmp_path):
    plan = _plan(tmp_path)
    path = tmp_path / "allocations.json"
    ledger = AllocationLedger(path, control_token=CONTROL_TOKEN)
    allocation = plan.allocations[0]
    claim = ledger.claim(plan, allocation.allocation_id, attempt_id="try-1")
    ledger.finish(
        plan,
        claim,
        lease_id="lease-1",
        endpoint_request_provider_bytes={},
        completed=False,
    )

    changed = _plan(tmp_path, pending_match_ids=[5, 2, 10, 99])
    changed_allocation = changed.allocations[0]
    with pytest.raises(AllocationAccountingError, match="different immutable"):
        ledger.claim(changed, changed_allocation.allocation_id, attempt_id="try-2")


def test_lease_stats_keep_plan_and_allocation_provenance_across_restart(tmp_path):
    plan = _plan(tmp_path)
    allocation = plan.allocations[1]
    path = tmp_path / "allocations.json"
    ledger = AllocationLedger(path, control_token=CONTROL_TOKEN)
    claim = ledger.claim(plan, allocation.allocation_id, attempt_id="try-1")
    ledger.consume(plan, claim, 125)
    stats = ledger.finish(
        plan,
        claim,
        lease_id="provider-lease-id-is-not-persisted-raw",
        endpoint_request_provider_bytes={
            "player_profile": [75],
            "player_season_statistics": [50],
        },
        completed=False,
        proxy_exit_hash="hashed-proxy-exit-02",
    )

    assert stats["artifact_id"] == plan.artifact_id
    assert stats["plan_digest"] == plan.plan_digest
    assert stats["allocation_id"] == allocation.allocation_id
    assert stats["class"] == allocation.workload_class
    assert stats["attempt_provider_bytes"] == 125
    assert "provider-lease-id-is-not-persisted-raw" not in path.read_text()
    assert CONTROL_TOKEN not in path.read_text()

    snapshot = AllocationLedger(path, control_token=CONTROL_TOKEN).snapshot(plan)
    persisted = snapshot["allocations"][allocation.allocation_id]
    assert persisted["spent_provider_bytes"] == 125
    assert persisted["lease_stats"][0]["run_cap_bytes"] == plan.run_cap_bytes


def test_pending_players_must_come_from_full_universe(tmp_path):
    with pytest.raises(WorkloadPlanError, match="absent from the full"):
        _plan(
            tmp_path,
            player_universe_ids=[1, 2, 3],
            pending_player_ids=[1, 4],
        )


@pytest.mark.parametrize(
    "field_values",
    [[1, 1], ["2", 2]],
)
def test_duplicate_ids_fail_instead_of_being_silently_dropped(field_values):
    with pytest.raises(WorkloadPlanError, match="duplicate IDs"):
        stable_partitions(field_values, 25)
