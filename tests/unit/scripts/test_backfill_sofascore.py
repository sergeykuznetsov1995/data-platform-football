from unittest.mock import patch

import pytest

from scripts import backfill_sofascore


@pytest.mark.unit
def test_default_backfill_run_ids_are_unique():
    first = backfill_sofascore._new_invocation_run_id()
    second = backfill_sofascore._new_invocation_run_id()
    assert first != second
    assert "::" not in first
    assert first.startswith("manual__sofascore-backfill__")


@pytest.mark.unit
def test_backfill_runs_season_then_matches_through_same_runner(tmp_path):
    calls = []

    def run(argv):
        calls.append(argv)
        return 0

    with patch.object(backfill_sofascore, "run_capture", side_effect=run):
        rc = backfill_sofascore.main(
            [
                "--league",
                "ENG-Premier League",
                "--seasons",
                "2014,2015",
                "--output-dir",
                str(tmp_path),
                "--offline-replay",
                "--raw-store-uri",
                f"file://{tmp_path / 'raw'}",
            ]
        )

    assert rc == 0
    assert [call[call.index("--entity") + 1] for call in calls] == [
        "all",
        "match_capture",
        "player_capture",
        "all",
        "match_capture",
        "player_capture",
    ]
    assert all("--offline-replay" in call for call in calls)
    assert all("--raw-store-uri" in call for call in calls)
    assert all(
        call[call.index("--manifest-backend") + 1] == "trino"
        for call in calls
    )


@pytest.mark.unit
def test_backfill_stops_before_match_capture_when_season_fails(tmp_path):
    with (
        patch.object(backfill_sofascore, "run_capture", return_value=1) as run,
        patch.object(
            backfill_sofascore,
            "prepare_workload_plan",
            return_value=tmp_path / "season-plan.json",
        ) as prepare,
    ):
        rc = backfill_sofascore.main(
            [
                "--seasons",
                "2014",
                "--output-dir",
                str(tmp_path),
                "--workload-artifact",
                str(tmp_path / "artifact.json"),
            ]
        )

    assert rc == 1
    assert run.call_count == 1
    assert prepare.call_count == 1
    assert "all" in run.call_args.args[0]


@pytest.mark.unit
def test_backfill_passes_named_registry_season_without_numeric_coercion(tmp_path):
    with (
        patch.object(backfill_sofascore, "run_capture", return_value=0) as run,
        patch.object(
            backfill_sofascore,
            "prepare_workload_plan",
            side_effect=[
                tmp_path / "season-plan.json",
                tmp_path / "target-plan.json",
                tmp_path / "player-plan.json",
            ],
        ) as prepare,
    ):
        rc = backfill_sofascore.main(
            [
                "--seasons",
                "Apertura 2025",
                "--output-dir",
                str(tmp_path),
                "--workload-artifact",
                str(tmp_path / "artifact.json"),
            ]
        )

    assert rc == 0
    first = run.call_args_list[0].args[0]
    assert first[first.index("--season") + 1] == "Apertura 2025"
    assert "--allow-inactive-season" in first
    assert first[first.index("--output") + 1].endswith("Apertura-2025-season.json")
    assert [call.kwargs["phase"] for call in prepare.call_args_list] == [
        "season",
        "targets",
        "players",
    ]
    assert all(
        call.kwargs["allow_inactive_season"] is True
        for call in prepare.call_args_list
    )
    assert [
        call.args[0][call.args[0].index("--entity") + 1] for call in run.call_args_list
    ] == ["all", "match_capture", "player_capture"]


@pytest.mark.unit
def test_backfill_explicit_run_id_scopes_all_fresh_phase_files(tmp_path):
    with (
        patch.object(backfill_sofascore, "run_capture", return_value=0),
        patch.object(
            backfill_sofascore,
            "prepare_workload_plan",
            side_effect=lambda **kwargs: kwargs["output_path"],
        ) as prepare,
    ):
        rc = backfill_sofascore.main(
            [
                "--seasons",
                "2014",
                "--run-id",
                "manual__reviewed-backfill-42",
                "--output-dir",
                str(tmp_path),
                "--workload-artifact",
                str(tmp_path / "artifact.json"),
                "--force-replace",
            ]
        )

    assert rc == 0
    assert prepare.call_count == 3
    base_ids = {call.kwargs["base_run_id"] for call in prepare.call_args_list}
    assert base_ids == {"manual__reviewed-backfill-42--ENG-Premier League--2014"}
    plan_paths = [call.kwargs["output_path"] for call in prepare.call_args_list]
    assert len(set(plan_paths)) == 3
    assert all("2014-" in path.name for path in plan_paths)
