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


def _policy(*, season_measured: tuple[str, ...] = ("16", "17")) -> WorkloadBudgetPolicy:
    classes = {
        match_workload_class(): WorkloadClassBudget(
            match_workload_class(),
            "match",
            MATCH_BATCH_SIZE,
            1_000,
            ("event",),
            20,
            5,
            workload_shape_digest(production_match_shape()),
            ("16", "17"),
        ),
        player_workload_class(): WorkloadClassBudget(
            player_workload_class(),
            "player",
            PLAYER_BATCH_SIZE,
            2_000,
            ("player_profile",),
            20,
            5,
            workload_shape_digest(production_player_shape()),
            ("16", "17"),
        ),
    }
    for shape, measured in (
        (EPL_SEASON_SHAPE, season_measured),
        (WORLD_CUP_SEASON_SHAPE, ("16",)),
    ):
        name = season_workload_class(shape)
        classes[name] = WorkloadClassBudget(
            name,
            "season",
            1,
            3_000,
            ("schedule_last",),
            20,
            5,
            workload_shape_digest(shape),
            measured,
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
    assert plan.run_cap_bytes == 3 * 1_000 + 3 * 2_000 + 2 * 3_000
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


def test_season_class_measured_on_one_tournament_blocks_a_new_league():
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

    with pytest.raises(WorkloadPolicyUnavailable, match="measured only for tournament"):
        build_partitioned_plan(
            _policy(season_measured=("17",)),
            dag_id="dag_ingest_sofascore",
            run_id="run-1",
            partitions=(la_liga,),
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
