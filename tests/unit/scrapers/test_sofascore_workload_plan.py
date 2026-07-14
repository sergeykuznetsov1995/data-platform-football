from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scrapers.sofascore.runtime_fingerprint import runtime_fingerprint
from scrapers.sofascore.workload_plan import (
    MATCH_BATCH_SIZE,
    MATCH_REQUIRED_ENDPOINTS,
    PLAYER_BATCH_SIZE,
    PLAYER_REQUIRED_ENDPOINTS,
    PLAYER_UNIVERSE_TASK_ID,
    SEASON_DYNAMIC_ENDPOINTS,
    SEASON_STATIC_ENDPOINTS,
    TEAM_COUNT_BANDS,
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
    production_match_shape,
    production_player_shape,
    production_season_shape,
    season_workload_class,
    stable_partitions,
    team_count_band,
    workload_shape_digest,
)


CONTROL_TOKEN = "unit-test-control-token-that-is-longer-than-32-bytes"
MATCH_SHAPE = production_match_shape()
PLAYER_SHAPE = production_player_shape()
SEASON_SHAPE = production_season_shape(
    season_format="split_year",
    team_count_band="16_20",
    max_pages_per_direction=50,
)
MATCH_CLASS = match_workload_class()
PLAYER_CLASS = player_workload_class()
SEASON_CLASS = season_workload_class(SEASON_SHAPE)


def _shape_endpoints(shape: dict) -> tuple[str, ...]:
    if shape["scope"] == "season":
        return tuple(
            sorted(list(shape["static_endpoints"]) + list(shape["dynamic_endpoints"]))
        )
    return tuple(sorted(shape["required_endpoints"]))


def _class_samples(
    *,
    units: int,
    endpoints: tuple[str, ...],
    workload_class: str,
    tournaments: tuple[int, ...],
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
                "run_id": f"cold-{workload_class}-{index}",
                "workload_class": workload_class,
                "source_tournament_id": tournaments[index % len(tournaments)],
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
    shape: dict,
    tournaments: tuple[int, ...],
) -> tuple[str, dict]:
    if scope == "match":
        name = match_workload_class()
    elif scope == "player":
        name = player_workload_class()
    else:
        name = season_workload_class(shape)
    endpoints = _shape_endpoints(shape)
    samples = _class_samples(
        units=max_units,
        endpoints=endpoints,
        workload_class=name,
        tournaments=tournaments,
    )
    payload = {
        "scope": scope,
        "max_units": max_units,
        "required_endpoints": list(endpoints),
        "shape": dict(shape),
        "shape_digest": workload_shape_digest(shape),
        "measured_tournament_ids": list(tournaments),
        "hard_task_bytes": max(item["total_provider_bytes"] for item in samples),
        "samples": samples,
    }
    return name, payload


def _artifact_payload(
    *,
    match_tournaments: tuple[int, ...] = (16, 17),
    player_tournaments: tuple[int, ...] = (16, 17),
    season_tournaments: tuple[int, ...] = (17,),
) -> dict:
    classes = dict(
        item
        for item in (
            _class_payload(
                scope="match",
                max_units=MATCH_BATCH_SIZE,
                shape=MATCH_SHAPE,
                tournaments=match_tournaments,
            ),
            _class_payload(
                scope="player",
                max_units=PLAYER_BATCH_SIZE,
                shape=PLAYER_SHAPE,
                tournaments=player_tournaments,
            ),
            _class_payload(
                scope="season",
                max_units=1,
                shape=SEASON_SHAPE,
                tournaments=season_tournaments,
            ),
        )
    )
    return {
        "schema_version": 3,
        "source": "sofascore",
        "meter": "proxy_filter_provider_path_v2",
        "budget_derivation": "max_observed_task_bytes_per_workload_class_v2",
        "runtime_fingerprint": runtime_fingerprint(),
        "verified": True,
        "workload_classes": classes,
    }


def _write_artifact(path: Path, payload: dict | None = None) -> Path:
    path.write_text(
        json.dumps(payload or _artifact_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _policy(tmp_path: Path, payload: dict | None = None, name: str = "workloads.json"):
    return load_verified_workload_policy(_write_artifact(tmp_path / name, payload))


def _plan_kwargs(**overrides) -> dict:
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
    return values


def _plan(tmp_path: Path, **overrides):
    return build_signed_dagrun_plan(_policy(tmp_path), **_plan_kwargs(**overrides))


def _observed_max(endpoint_count: int) -> int:
    # The 20th sample is the largest: request bytes grow with the sample index.
    return sum(100 + 19 + index + 10 for index in range(endpoint_count))


def test_v3_policy_derives_each_hard_task_cap_from_exact_observed_max(tmp_path):
    policy = _policy(tmp_path)

    match = policy.classes[MATCH_CLASS]
    player = policy.classes[PLAYER_CLASS]
    season = policy.classes[SEASON_CLASS]

    assert match.hard_task_bytes == _observed_max(len(MATCH_REQUIRED_ENDPOINTS))
    assert player.hard_task_bytes == _observed_max(len(PLAYER_REQUIRED_ENDPOINTS))
    assert season.hard_task_bytes == _observed_max(
        len(SEASON_STATIC_ENDPOINTS) + len(SEASON_DYNAMIC_ENDPOINTS)
    )
    assert match.sample_count == 20
    assert match.distinct_proxy_exits == 5
    assert match.shape_digest == workload_shape_digest(MATCH_SHAPE)
    assert match.measured_tournament_ids == ("16", "17")
    assert season.shape_digest == workload_shape_digest(SEASON_SHAPE)
    assert season.measured_tournament_ids == ("17",)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda value: value["workload_classes"][MATCH_CLASS]["samples"].pop(),
            "20 cold samples",
        ),
        (
            lambda value: [
                sample.update(proxy_exit_hash="same-anonymized-exit")
                for sample in value["workload_classes"][MATCH_CLASS]["samples"]
            ],
            "5 distinct exits",
        ),
        (
            lambda value: value["workload_classes"][MATCH_CLASS].update(
                hard_task_bytes=999999
            ),
            "must equal max observed",
        ),
        (
            lambda value: value.update(budget_multiplier=1.25),
            "cannot use a multiplier",
        ),
        (
            lambda value: value["workload_classes"][MATCH_CLASS]["samples"][0].update(
                total_provider_bytes=1
            ),
            "must equal the exact request map",
        ),
        (
            lambda value: value["workload_classes"][MATCH_CLASS]["samples"][0][
                "endpoint_request_provider_bytes"
            ].pop("lineups"),
            "endpoint mismatch",
        ),
        (
            lambda value: value["runtime_fingerprint"].update(digest="0" * 64),
            "does not match current runtime",
        ),
        (
            lambda value: value["workload_classes"][MATCH_CLASS]["samples"][0][
                "evidence"
            ].update(runtime_fingerprint_digest="0" * 64),
            "another runtime fingerprint",
        ),
        (
            lambda value: value.update(schema_version=2),
            "schema_version must be 3",
        ),
        (
            lambda value: value["workload_classes"][MATCH_CLASS].update(
                required_endpoints=["event"]
            ),
            "must equal the endpoints of its shape",
        ),
    ],
)
def test_v3_policy_fails_closed_for_unmeasured_or_inexact_classes(
    tmp_path, mutate, message
):
    payload = _artifact_payload()
    mutate(payload)

    with pytest.raises(WorkloadPolicyUnavailable, match=message):
        load_verified_workload_policy(_write_artifact(tmp_path / "bad.json", payload))


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda shape: shape.update(source_tournament_id="17"),
            "must not bind a source tournament",
        ),
        (
            lambda shape: shape.update(measured_at="2026-07-14"),
            "shape key mismatch",
        ),
        (
            lambda shape: shape.pop("band_scheme"),
            "shape key mismatch",
        ),
        (
            lambda shape: shape.update(season_format="calendar_year"),
            "shape_digest does not match its shape",
        ),
    ],
)
def test_v3_policy_rejects_shapes_outside_the_measured_whitelist(
    tmp_path, mutate, message
):
    payload = _artifact_payload()
    mutate(payload["workload_classes"][SEASON_CLASS]["shape"])

    with pytest.raises(WorkloadPolicyUnavailable, match=message):
        load_verified_workload_policy(_write_artifact(tmp_path / "bad.json", payload))


def test_v3_policy_rejects_two_classes_with_the_same_shape_in_one_scope(tmp_path):
    payload = _artifact_payload()
    payload["workload_classes"][f"{SEASON_CLASS}_copy"] = copy.deepcopy(
        payload["workload_classes"][SEASON_CLASS]
    )

    with pytest.raises(WorkloadPolicyUnavailable, match="duplicate shape_digest"):
        load_verified_workload_policy(_write_artifact(tmp_path / "bad.json", payload))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["workload_classes"][MATCH_CLASS].update(
            measured_tournament_ids=[16, 17, 99]
        ),
        lambda value: value["workload_classes"][MATCH_CLASS]["samples"][0].update(
            source_tournament_id=99
        ),
    ],
)
def test_v3_policy_requires_measured_tournaments_to_equal_sample_tournaments(
    tmp_path, mutate
):
    payload = _artifact_payload()
    mutate(payload)

    with pytest.raises(
        WorkloadPolicyUnavailable, match="must equal the tournaments of its cold samples"
    ):
        load_verified_workload_policy(_write_artifact(tmp_path / "bad.json", payload))


def _season_samples(*, drop: str | None, count: int = 20) -> list[dict]:
    endpoints = tuple(
        endpoint for endpoint in _shape_endpoints(SEASON_SHAPE) if endpoint != drop
    )
    return _class_samples(
        units=1,
        endpoints=endpoints,
        workload_class=SEASON_CLASS,
        tournaments=(17,),
        count=count,
    )


def test_season_class_verifies_when_optional_referee_endpoint_is_absent(tmp_path):
    # The collector legitimately omits the dynamic ``referee_profile`` request
    # when the schedule payload exposes no referee IDs.  The loader must accept
    # such a sample (symmetry with the collector), otherwise a future-season
    # class could never become verified.
    payload = _artifact_payload()
    samples = _season_samples(drop="referee_profile")
    season = payload["workload_classes"][SEASON_CLASS]
    season["samples"] = samples
    season["hard_task_bytes"] = max(item["total_provider_bytes"] for item in samples)

    policy = load_verified_workload_policy(
        _write_artifact(tmp_path / "season.json", payload)
    )

    measured = policy.classes[SEASON_CLASS]
    # The measured shape still declares every endpoint, but the cap is the max
    # over the actually observed (static + squads) endpoints of one sample.
    assert "referee_profile" in measured.required_endpoints
    assert measured.hard_task_bytes == _observed_max(
        len(SEASON_STATIC_ENDPOINTS) + len(SEASON_DYNAMIC_ENDPOINTS) - 1
    )


def test_season_class_fails_closed_when_a_static_endpoint_is_missing(tmp_path):
    payload = _artifact_payload()
    samples = _season_samples(drop="standings_total")
    season = payload["workload_classes"][SEASON_CLASS]
    season["samples"] = samples
    season["hard_task_bytes"] = max(item["total_provider_bytes"] for item in samples)

    with pytest.raises(WorkloadPolicyUnavailable, match="endpoint mismatch"):
        load_verified_workload_policy(_write_artifact(tmp_path / "bad.json", payload))


def test_prod_loader_rejects_a_class_skewed_below_the_even_floor(tmp_path):
    # A verified artifact that authorizes transfer must carry each measured
    # tournament's even share of the fixed class minimum (20 // 2 = 10).
    payload = _artifact_payload()
    match = payload["workload_classes"][MATCH_CLASS]
    for index, sample in enumerate(match["samples"]):
        # 19 samples on tournament 16, 1 on tournament 17.
        sample["source_tournament_id"] = 16 if index else 17

    with pytest.raises(WorkloadPolicyUnavailable, match="skewed"):
        load_verified_workload_policy(_write_artifact(tmp_path / "bad.json", payload))


def test_class_measured_on_two_tournaments_authorizes_a_new_one(tmp_path):
    plan = _plan(tmp_path, source_tournament_id=8, season_workloads=[])

    match = [item for item in plan.allocations if item.scope == "match"]
    player = [item for item in plan.allocations if item.scope == "player"]
    assert {item.workload_class for item in match} == {MATCH_CLASS}
    assert {item.workload_class for item in player} == {PLAYER_CLASS}


def test_class_measured_on_one_tournament_does_not_generalize(tmp_path):
    single = _artifact_payload(match_tournaments=(17,), player_tournaments=(17,))
    policy = _policy(tmp_path, single, name="single.json")

    with pytest.raises(WorkloadPolicyUnavailable, match="measured only for tournament"):
        build_signed_dagrun_plan(
            policy,
            **_plan_kwargs(source_tournament_id=8, season_workloads=[]),
        )

    plan = build_signed_dagrun_plan(
        policy, **_plan_kwargs(source_tournament_id=17, season_workloads=[])
    )
    assert {item.workload_class for item in plan.allocations} == {
        MATCH_CLASS,
        PLAYER_CLASS,
    }

    # The season class of the shipped artifact is measured on tournament 17 only.
    with pytest.raises(WorkloadPolicyUnavailable, match="measured only for tournament"):
        _plan(
            tmp_path,
            source_tournament_id=8,
            pending_match_ids=[],
            player_universe_ids=[],
            pending_player_ids=[],
            season_workloads=[SeasonWorkload(8, 12345, SEASON_SHAPE)],
        )


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


def test_season_budget_is_specific_to_the_exact_measured_shape(tmp_path):
    plan = _plan(tmp_path)
    season = next(item for item in plan.allocations if item.scope == "season")
    assert season.workload_class == SEASON_CLASS

    other_band = production_season_shape(
        season_format="split_year",
        team_count_band="21_32",
        max_pages_per_direction=50,
    )
    with pytest.raises(WorkloadPolicyUnavailable, match="has no class"):
        _plan(
            tmp_path,
            season_workloads=[SeasonWorkload(17, 76986, other_band)],
        )


def test_workload_classes_are_named_after_scope_and_shape_digest():
    assert MATCH_CLASS == (
        f"match_batch_{MATCH_BATCH_SIZE}_"
        f"{workload_shape_digest(MATCH_SHAPE)[:16]}"
    )
    assert PLAYER_CLASS == (
        f"player_batch_{PLAYER_BATCH_SIZE}_"
        f"{workload_shape_digest(PLAYER_SHAPE)[:16]}"
    )
    assert SEASON_CLASS == f"season_{workload_shape_digest(SEASON_SHAPE)[:16]}"


def test_production_batch_shapes_track_the_captured_endpoints():
    from scrapers.sofascore.pipeline import EVENT_PATHS, PLAYER_PATHS

    assert MATCH_SHAPE["required_endpoints"] == sorted(EVENT_PATHS)
    assert PLAYER_SHAPE["required_endpoints"] == sorted(PLAYER_PATHS)
    assert sorted(MATCH_REQUIRED_ENDPOINTS) == sorted(EVENT_PATHS)
    assert sorted(PLAYER_REQUIRED_ENDPOINTS) == sorted(PLAYER_PATHS)
    assert MATCH_SHAPE["batch_size"] == MATCH_BATCH_SIZE
    assert PLAYER_SHAPE["batch_size"] == PLAYER_BATCH_SIZE


def test_production_season_shape_is_bounded_by_format_and_team_count_band():
    epl = production_season_shape(
        season_format="split_year",
        team_count_band=team_count_band(20),
        max_pages_per_direction=50,
    )
    la_liga = production_season_shape(
        season_format="split_year",
        team_count_band=team_count_band(20),
        max_pages_per_direction=50,
    )
    world_cup = production_season_shape(
        season_format="calendar_year",
        team_count_band=team_count_band(48),
        max_pages_per_direction=50,
    )

    assert "source_tournament_id" not in epl
    assert epl["band_scheme"] == "team_count_band_v1"
    assert epl["schedule_page_chain"]["max_pages_per_direction"] == 50
    assert str(epl["dynamic_evidence"]).endswith("_v1")
    assert season_workload_class(epl) == season_workload_class(la_liga)
    assert season_workload_class(epl) != season_workload_class(world_cup)

    with pytest.raises(WorkloadPlanError, match="team_count_band"):
        production_season_shape(
            season_format="split_year",
            team_count_band="17_19",
            max_pages_per_direction=50,
        )


@pytest.mark.parametrize(
    ("team_count", "band"),
    [
        (8, "8_15"),
        (15, "8_15"),
        (16, "16_20"),
        (20, "16_20"),
        (21, "21_32"),
        (32, "21_32"),
        (33, "33_48"),
        (48, "33_48"),
    ],
)
def test_team_count_band_grid_is_contiguous(team_count, band):
    assert team_count_band(team_count) == band
    assert band in TEAM_COUNT_BANDS


@pytest.mark.parametrize("team_count", [0, 7, 49, 100])
def test_team_count_outside_the_measured_grid_fails_closed(team_count):
    with pytest.raises(WorkloadPlanError, match="outside the measured"):
        team_count_band(team_count)


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
