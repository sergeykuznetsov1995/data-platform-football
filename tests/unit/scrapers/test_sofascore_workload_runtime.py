from __future__ import annotations

import pytest

from scrapers.sofascore.workload_plan import (
    MATCH_BATCH_SIZE,
    PLAYER_BATCH_SIZE,
    SeasonWorkload,
    WorkloadBudgetPolicy,
    WorkloadClassBudget,
    WorkloadPlanError,
    WorkloadPolicyUnavailable,
    allocation_budget_bytes,
    match_workload_class,
    player_workload_class,
    production_match_shape,
    production_player_shape,
    production_season_shape,
    season_workload_class,
    workload_shape_digest,
)
from scrapers.sofascore.workload_runtime import (
    PartitionWorkload,
    allocations_for_partition,
    build_partitioned_plan,
    load_plan,
    partition_key,
    plan_path_for_run,
    target_ids,
    write_plan,
)


TOKEN = "runtime-test-control-token-with-at-least-32-bytes"
EPL_SEASON_SHAPE = production_season_shape(
    season_format="split_year",
    team_count_band="16_20",
    max_pages_per_direction=50,
)
WORLD_CUP_SEASON_SHAPE = production_season_shape(
    season_format="calendar_year",
    team_count_band="33_48",
    max_pages_per_direction=50,
)


def _policy() -> WorkloadBudgetPolicy:
    classes = {
        match_workload_class(): WorkloadClassBudget(
            match_workload_class(),
            "match",
            MATCH_BATCH_SIZE,
            1_000,
            ("event",),
            workload_shape_digest(production_match_shape()),
        ),
        player_workload_class(): WorkloadClassBudget(
            player_workload_class(),
            "player",
            PLAYER_BATCH_SIZE,
            2_000,
            ("player_profile",),
            workload_shape_digest(production_player_shape()),
        ),
    }
    for shape in (EPL_SEASON_SHAPE, WORLD_CUP_SEASON_SHAPE):
        name = season_workload_class(shape)
        classes[name] = WorkloadClassBudget(
            name,
            "season",
            1,
            3_000,
            ("schedule_last",),
            workload_shape_digest(shape),
        )
    return WorkloadBudgetPolicy("a" * 64, classes)


def _partitions():
    return (
        PartitionWorkload(
            "ENG-Premier League",
            "2526",
            17,
            pending_match_ids=tuple(str(value) for value in range(1, 28)),
            player_universe_ids=tuple(str(value) for value in range(1, 54)),
            pending_player_ids=tuple(str(value) for value in range(1, 54)),
            season_workload=SeasonWorkload(17, 76986, EPL_SEASON_SHAPE),
        ),
        PartitionWorkload(
            "INT-World Cup",
            "2026",
            16,
            pending_match_ids=("100", "101"),
            player_universe_ids=("7", "8"),
            pending_player_ids=("7", "8"),
            season_workload=SeasonWorkload(16, 58210, WORLD_CUP_SEASON_SHAPE),
        ),
    )


def test_partitioned_plan_never_mixes_competitions_and_keeps_full_universe():
    plan = build_partitioned_plan(
        _policy(),
        dag_id="dag_ingest_sofascore",
        run_id="run-1",
        partitions=_partitions(),
        control_token=TOKEN,
    )

    epl_matches = allocations_for_partition(
        plan,
        league="ENG-Premier League",
        canonical_season="2526",
        scope="match",
    )
    wc_matches = allocations_for_partition(
        plan,
        league="INT-World Cup",
        canonical_season="2026",
        scope="match",
    )
    assert [len(target_ids(item)) for item in epl_matches] == [25, 2]
    assert [target_ids(item) for item in wc_matches] == [("100", "101")]
    assert set(target_ids(epl_matches[0])).isdisjoint(target_ids(wc_matches[0]))
    assert len(plan.player_universe_ids) == 55
    # Каждая аллокация = замеренный максимум класса + структурный headroom (#1044).
    assert plan.run_cap_bytes == (
        3 * allocation_budget_bytes(1_000)
        + 3 * allocation_budget_bytes(2_000)
        + 2 * allocation_budget_bytes(3_000)
    )
    # Match/player bytes are shape-driven, so both competitions share one class.
    assert {item.workload_class for item in epl_matches + wc_matches} == {
        match_workload_class()
    }
    assert {
        item.workload_class for item in plan.allocations if item.scope == "player"
    } == {player_workload_class()}
    seasons = {
        item.workload_class for item in plan.allocations if item.scope == "season"
    }
    assert seasons == {
        season_workload_class(EPL_SEASON_SHAPE),
        season_workload_class(WORLD_CUP_SEASON_SHAPE),
    }


def test_static_season_class_authorizes_any_league_of_its_shape():
    """#1245: the class is evidence of a shape, not of a measured league."""

    la_liga = PartitionWorkload(
        "ESP-La Liga",
        "2526",
        8,
        season_workload=SeasonWorkload(8, 61643, EPL_SEASON_SHAPE),
    )

    plan = build_partitioned_plan(
        _policy(),
        dag_id="dag_ingest_sofascore",
        run_id="run-1",
        partitions=(la_liga,),
        control_token=TOKEN,
    )
    assert [item.workload_class for item in plan.allocations] == [
        season_workload_class(EPL_SEASON_SHAPE)
    ]

    unknown_shape = PartitionWorkload(
        "ESP-La Liga",
        "2526",
        8,
        season_workload=SeasonWorkload(
            8,
            61643,
            production_season_shape(
                season_format="split_year",
                team_count_band="8_15",
                max_pages_per_direction=50,
            ),
        ),
    )
    with pytest.raises(WorkloadPolicyUnavailable, match="has no class"):
        build_partitioned_plan(
            _policy(),
            dag_id="dag_ingest_sofascore",
            run_id="run-1",
            partitions=(unknown_shape,),
            control_token=TOKEN,
        )


def test_immutable_plan_file_is_retry_safe_and_tamper_safe(tmp_path):
    plan = build_partitioned_plan(
        _policy(),
        dag_id="dag_ingest_sofascore",
        run_id="run-1",
        partitions=_partitions(),
        control_token=TOKEN,
    )
    path = plan_path_for_run(plan.dag_id, plan.run_id, directory=tmp_path / "plans")
    assert write_plan(path, plan) == path
    assert write_plan(path, plan) == path
    assert load_plan(path, control_token=TOKEN) == plan
    assert path.stat().st_mode & 0o777 == 0o600

    path.write_text("{}")
    with pytest.raises(WorkloadPlanError, match="different bytes"):
        write_plan(path, plan)


def test_partition_key_is_not_a_delimiter_based_collision():
    assert partition_key("A|B", "C") != partition_key("A", "B|C")


def test_signed_season_chunk_keeps_caps_and_freezes_retry_identity(tmp_path):
    from dataclasses import replace
    from scrapers.sofascore.season_chunks import SeasonScheduleChunk, decode_season_chunk_unit

    chunk = SeasonScheduleChunk("17", "76986", (50, 100), (0, 0), "a" * 64)
    workload = SeasonWorkload(17, 76986, EPL_SEASON_SHAPE, schedule_chunk=chunk)
    partition = PartitionWorkload("ENG-Premier League", "2526", 17, season_workload=workload)

    def build(work):
        return build_partitioned_plan(
            _policy(), dag_id="dag_ingest_sofascore", run_id="chunk-run::season",
            partitions=(work,), control_token=TOKEN,
            freshness_keys={"season": "final", "match": "final", "player": "final"},
        )

    plan = build(partition)
    legacy = build(replace(partition, season_workload=SeasonWorkload(17, 76986, EPL_SEASON_SHAPE)))
    assert len(plan.allocations) == 1
    allocation = plan.allocations[0]
    assert decode_season_chunk_unit(target_ids(allocation)[0]) == chunk
    assert allocation.workload_class == legacy.allocations[0].workload_class
    assert allocation.budget_bytes == legacy.allocations[0].budget_bytes
    assert plan.run_cap_bytes == legacy.run_cap_bytes
    assert plan.artifact_id == legacy.artifact_id
    assert plan.to_dict()["schema_version"] == legacy.to_dict()["schema_version"]
    assert allocation.allocation_id != legacy.allocations[0].allocation_id
    path = tmp_path / "plan.json"
    write_plan(path, plan)
    assert write_plan(path, build(partition)) == path
    assert load_plan(path, control_token=TOKEN) == plan
    changed = replace(chunk, last=(100, 150))
    with pytest.raises(WorkloadPlanError, match="different bytes"):
        write_plan(path, build(replace(partition, season_workload=replace(workload, schedule_chunk=changed))))


def test_season_chunk_source_identity_must_match_workload_and_partition():
    from scrapers.sofascore.season_chunks import SeasonScheduleChunk

    chunk = SeasonScheduleChunk("17", "76986", (0, 49), (0, 1), "a" * 64)
    with pytest.raises(WorkloadPlanError, match="chunk.*identity"):
        SeasonWorkload(16, 76986, EPL_SEASON_SHAPE, schedule_chunk=chunk)
    with pytest.raises(WorkloadPlanError, match="chunk.*identity"):
        SeasonWorkload(17, 999, EPL_SEASON_SHAPE, schedule_chunk=chunk)
    with pytest.raises(WorkloadPlanError, match="tournament"):
        build_partitioned_plan(
            _policy(), dag_id="dag_ingest_sofascore", run_id="mismatch::season",
            partitions=(PartitionWorkload("L", "2526", 16,
                season_workload=SeasonWorkload(17, 76986, EPL_SEASON_SHAPE, schedule_chunk=chunk)),),
            control_token=TOKEN,
        )


@pytest.mark.parametrize("changed_field", ["bounds", "evidence", "freshness"])
def test_chunk_bound_evidence_and_freshness_cannot_change_after_signing(changed_field):
    from dataclasses import replace
    from scrapers.sofascore.season_chunks import SeasonScheduleChunk, encode_season_chunk_unit
    from scrapers.sofascore.workload_plan import (
        SignedDagRunPlan, WorkloadPlanSignatureError, qualify_work_unit,
    )

    chunk = SeasonScheduleChunk("17", "76986", (50, 100), (0, 0), "a" * 64)
    plan = build_partitioned_plan(
        _policy(), dag_id="dag_ingest_sofascore", run_id="chunk-run::season",
        partitions=(PartitionWorkload("L", "2526", 17,
            season_workload=SeasonWorkload(17, 76986, EPL_SEASON_SHAPE, schedule_chunk=chunk)),),
        control_token=TOKEN,
    )
    payload = plan.to_dict()
    if changed_field == "freshness":
        payload["freshness_keys"]["season"] = "another-day"
    else:
        changed = replace(chunk, **({"last": (100, 150)} if changed_field == "bounds"
                                   else {"evidence_digest": "b" * 64}))
        payload["allocations"][0]["units"] = [qualify_work_unit(
            partition_key("L", "2526"), encode_season_chunk_unit(changed),
        )]
    with pytest.raises(WorkloadPlanSignatureError, match="digest"):
        SignedDagRunPlan.from_dict(payload, control_token=TOKEN)


@pytest.mark.parametrize("targets", [
    ("season-chunk-v9.invalid",), ("017:76986",), ("17:76986", "17:999"),
])
def test_runtime_partition_selection_rejects_invalid_season_units(targets):
    from dataclasses import replace
    from scrapers.sofascore.workload_plan import qualify_work_unit

    plan = build_partitioned_plan(
        _policy(), dag_id="dag_ingest_sofascore", run_id="chunk-run::season",
        partitions=(PartitionWorkload("L", "2526", 17,
            season_workload=SeasonWorkload(17, 76986, EPL_SEASON_SHAPE)),),
        control_token=TOKEN,
    )
    invalid = replace(plan.allocations[0], units=tuple(
        qualify_work_unit(partition_key("L", "2526"), unit) for unit in targets
    ))
    with pytest.raises(WorkloadPlanError, match="season.*unit"):
        allocations_for_partition(replace(plan, allocations=(invalid,)),
            league="L", canonical_season="2526", scope="season")
