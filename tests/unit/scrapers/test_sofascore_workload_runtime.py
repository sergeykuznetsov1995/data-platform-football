from __future__ import annotations

import hashlib

import pytest

from scrapers.sofascore.workload_plan import (
    MATCH_WORKLOAD_CLASS,
    PLAYER_WORKLOAD_CLASS,
    SeasonWorkload,
    WorkloadBudgetPolicy,
    WorkloadClassBudget,
    WorkloadPlanError,
    production_season_shape,
    match_workload_class,
    player_workload_class,
    season_workload_class,
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


def _policy() -> WorkloadBudgetPolicy:
    epl_shape = production_season_shape(
        17, season_format="split_year", max_pages_per_direction=50
    )
    wc_shape = production_season_shape(
        16, season_format="calendar_year", max_pages_per_direction=50
    )
    classes = {
        MATCH_WORKLOAD_CLASS: WorkloadClassBudget(
            MATCH_WORKLOAD_CLASS,
            "match",
            25,
            1_000,
            ("event",),
            20,
            5,
            source_tournament_id="17",
        ),
        PLAYER_WORKLOAD_CLASS: WorkloadClassBudget(
            PLAYER_WORKLOAD_CLASS,
            "player",
            50,
            2_000,
            ("player_profile",),
            20,
            5,
            source_tournament_id="17",
        ),
        match_workload_class(16): WorkloadClassBudget(
            match_workload_class(16),
            "match",
            25,
            4_000,
            ("event",),
            20,
            5,
            source_tournament_id="16",
        ),
        player_workload_class(16): WorkloadClassBudget(
            player_workload_class(16),
            "player",
            50,
            5_000,
            ("player_profile",),
            20,
            5,
            source_tournament_id="16",
        ),
    }
    for tournament_id, shape in ((17, epl_shape), (16, wc_shape)):
        name = season_workload_class(tournament_id, shape)
        classes[name] = WorkloadClassBudget(
            name,
            "season",
            1,
            3_000,
            ("schedule_last",),
            20,
            5,
            hashlib.sha256(
                __import__("json")
                .dumps(
                    shape,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                .encode()
            ).hexdigest(),
            str(tournament_id),
        )
    return WorkloadBudgetPolicy("a" * 64, classes)


def _partitions():
    epl_shape = production_season_shape(
        17, season_format="split_year", max_pages_per_direction=50
    )
    wc_shape = production_season_shape(
        16, season_format="calendar_year", max_pages_per_direction=50
    )
    return (
        PartitionWorkload(
            "ENG-Premier League",
            "2526",
            17,
            pending_match_ids=tuple(str(value) for value in range(1, 28)),
            player_universe_ids=tuple(str(value) for value in range(1, 54)),
            pending_player_ids=tuple(str(value) for value in range(1, 54)),
            season_workload=SeasonWorkload(17, 76986, epl_shape),
        ),
        PartitionWorkload(
            "INT-World Cup",
            "2026",
            16,
            pending_match_ids=("100", "101"),
            player_universe_ids=("7", "8"),
            pending_player_ids=("7", "8"),
            season_workload=SeasonWorkload(16, 58210, wc_shape),
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
    assert plan.run_cap_bytes == 2 * 1_000 + 2 * 2_000 + 4_000 + 5_000 + 2 * 3_000
    assert {item.workload_class for item in epl_matches} == {
        match_workload_class(17)
    }
    assert {item.workload_class for item in wc_matches} == {
        match_workload_class(16)
    }


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
