from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dags.scripts.prepare_sofascore_workload import (
    CompetitionSeason,
    _observed_player_ids,
    _parse_rotation_boundary,
    player_rotation_cohort,
    player_rotation_due,
    player_rotation_modulus,
    prepare_workload_plan,
)
from scrapers.sofascore.workload_plan import (
    WorkloadBudgetPolicy,
    WorkloadClassBudget,
    WorkloadPolicyUnavailable,
    match_workload_class,
    player_workload_class,
    production_match_shape,
    production_player_shape,
    production_season_shape,
    season_workload_class,
    workload_shape_digest,
)
from scrapers.sofascore.workload_runtime import load_plan, target_ids


TOKEN = "prepare-test-control-token-that-is-at-least-32-bytes"
# ENG-Premier League 2526 has team_count=20 in competitions.yaml -> band 16_20.
EPL_SEASON_SHAPE = production_season_shape(
    season_format="split_year",
    team_count_band="16_20",
    max_pages_per_direction=50,
)


@pytest.fixture(autouse=True)
def _pin_test_workload_policy(monkeypatch):
    monkeypatch.setenv("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", "b" * 64)


def _policy(season_shape=EPL_SEASON_SHAPE, tournament="17"):
    season_class = season_workload_class(season_shape)
    match_class = match_workload_class()
    player_class = player_workload_class()
    return WorkloadBudgetPolicy(
        "b" * 64,
        {
            match_class: WorkloadClassBudget(
                match_class,
                "match",
                25,
                100,
                ("event",),
                20,
                5,
                workload_shape_digest(production_match_shape()),
                (tournament,),
            ),
            player_class: WorkloadClassBudget(
                player_class,
                "player",
                50,
                200,
                ("player_profile",),
                20,
                5,
                workload_shape_digest(production_player_shape()),
                (tournament,),
            ),
            season_class: WorkloadClassBudget(
                season_class,
                "season",
                1,
                300,
                ("schedule_last",),
                20,
                5,
                workload_shape_digest(season_shape),
                (tournament,),
            ),
        },
    )


def _catalog():
    tournament = SimpleNamespace(
        capture_allowed=True,
        unique_tournament_id=17,
    )
    source_season = SimpleNamespace(
        season_id=76986,
        format="split_year",
    )
    catalog = MagicMock()
    catalog.competition.return_value = tournament
    catalog.resolve_source_season.return_value = source_season
    return catalog


def _common_patches(plan):
    runtime = SimpleNamespace(raw_store=MagicMock(), manifest_store=MagicMock())
    return (
        patch(
            "dags.scripts.prepare_sofascore_workload.load_verified_workload_policy",
            return_value=_policy(),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.build_capture_runtime",
            return_value=runtime,
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.SofaScoreCatalog.load",
            return_value=_catalog(),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.plan_season_partition",
            return_value=plan,
        ),
    )


@pytest.mark.parametrize(
    "configured_pin",
    [None, "", "b" * 63, "B" * 64, "g" * 64, "b" * 64 + "\n", "0" * 64],
)
def test_prepare_rejects_missing_noncanonical_or_zero_pin_before_loading_policy(
    tmp_path, monkeypatch, configured_pin
):
    if configured_pin is None:
        monkeypatch.delenv("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", raising=False)
    else:
        monkeypatch.setenv("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", configured_pin)
    policy_loads = []

    def load_policy(_path):
        policy_loads.append(True)
        raise AssertionError("policy must not be loaded without a canonical pin")

    destination = tmp_path / "must-not-exist.json"
    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.load_verified_workload_policy",
            side_effect=load_policy,
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.build_capture_runtime",
            side_effect=AssertionError("preparation must not start"),
        ),
        pytest.raises(
            WorkloadPolicyUnavailable,
            match="64 lowercase hexadecimal|zero placeholder",
        ),
    ):
        prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-pin-rejection",
            phase="season",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=destination,
        )

    assert policy_loads == []
    assert not destination.exists()


def test_prepare_rejects_verified_policy_that_does_not_match_artifact_pin(
    tmp_path, monkeypatch
):
    required_artifact_id = "a" * 64
    loaded_artifact_id = "b" * 64
    monkeypatch.setenv(
        "SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", required_artifact_id
    )
    destination = tmp_path / "must-not-exist.json"

    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.load_verified_workload_policy",
            return_value=_policy(),
        ) as load_policy,
        patch(
            "dags.scripts.prepare_sofascore_workload.build_capture_runtime",
            side_effect=AssertionError("preparation must not start"),
        ),
        pytest.raises(WorkloadPolicyUnavailable, match="required artifact pin")
        as exc_info,
    ):
        prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-pin-mismatch",
            phase="season",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=destination,
        )

    load_policy.assert_called_once_with(tmp_path / "artifact.json")
    assert required_artifact_id not in str(exc_info.value)
    assert loaded_artifact_id not in str(exc_info.value)
    assert not destination.exists()


def test_observed_universe_uses_match_bronze_resolver_not_old_universe():
    scraper = MagicMock()
    scraper.__enter__.return_value = scraper
    scraper.__exit__.return_value = False
    scraper._resolve_player_ids_from_bronze.return_value = ["11", "10", "11"]
    with patch(
        "dags.scripts.prepare_sofascore_workload.SofaScoreScraper",
        return_value=scraper,
    ) as scraper_class:
        observed = _observed_player_ids("ENG-Premier League", "2526")

    assert observed == {"10", "11"}
    scraper_class.assert_called_once_with(
        leagues=["ENG-Premier League"],
        seasons=["2526"],
    )
    scraper._resolve_player_ids_from_bronze.assert_called_once_with(
        "ENG-Premier League",
        "2526",
        limit=None,
    )


def test_season_phase_signs_only_the_bounded_season_allocation(tmp_path, monkeypatch):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("SOFASCORE_SEASON_FRESHNESS_KEY", "day-fixed")
    monkeypatch.setenv("SOFASCORE_PLAYER_FRESHNESS_KEY", "week-fixed")
    season_plan = SimpleNamespace(missing_raw_keys=("schedule-last-0",))
    patches = _common_patches(season_plan)
    with patches[0], patches[1], patches[2], patches[3]:
        path = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-1",
            phase="season",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / "season-plan.json",
        )

    signed = load_plan(path, control_token=TOKEN)
    assert signed.run_id == "scheduled-1::season"
    assert len(signed.allocations) == 1
    assert signed.allocations[0].scope == "season"
    # The class is the (split_year, 16_20) shape read from competitions.yaml —
    # never a tournament-specific one.
    assert signed.allocations[0].workload_class == season_workload_class(
        EPL_SEASON_SHAPE
    )
    assert signed.run_cap_bytes == 300
    assert dict(signed.freshness_keys) == {
        "season": "day-fixed",
        "match": "final",
        "player": "week-fixed",
    }


def test_force_repair_gets_a_measured_allocation_even_when_old_raw_is_complete(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    patches = _common_patches(SimpleNamespace(missing_raw_keys=()))
    with patches[0], patches[1], patches[2], patches[3]:
        path = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="manual-repair-1",
            phase="season",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / "repair-plan.json",
            force_replace=True,
        )

    signed = load_plan(path, control_token=TOKEN)
    assert len(signed.allocations) == 1
    assert signed.freshness_key("season") == "repair-manual-repair-1"
    assert signed.freshness_key("match") == "repair-manual-repair-1"
    assert signed.freshness_key("player") == "repair-manual-repair-1"

    # A retry reuses exact signed bytes even if manifests changed meanwhile.
    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.load_verified_workload_policy",
            return_value=_policy(),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.build_capture_runtime",
            side_effect=AssertionError("immutable retry must not resnapshot"),
        ),
    ):
        retried = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="manual-repair-1",
            phase="season",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=path,
            force_replace=True,
        )
    assert retried == path
    assert load_plan(retried, control_token=TOKEN).freshness_keys == signed.freshness_keys


def test_late_retry_reuses_original_signed_day_and_week_snapshot(tmp_path, monkeypatch):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("SOFASCORE_SEASON_FRESHNESS_KEY", "day-before-midnight")
    monkeypatch.setenv("SOFASCORE_PLAYER_FRESHNESS_KEY", "week-before-boundary")
    patches = _common_patches(SimpleNamespace(missing_raw_keys=("schedule",)))
    with patches[0], patches[1], patches[2], patches[3]:
        path = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-late-retry",
            phase="season",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / "immutable.json",
        )

    monkeypatch.setenv("SOFASCORE_SEASON_FRESHNESS_KEY", "day-after-midnight")
    monkeypatch.setenv("SOFASCORE_PLAYER_FRESHNESS_KEY", "week-after-boundary")
    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.load_verified_workload_policy",
            return_value=_policy(),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.build_capture_runtime",
            side_effect=AssertionError("immutable retry must not resnapshot"),
        ),
    ):
        retried = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-late-retry",
            phase="season",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=path,
        )

    signed = load_plan(retried, control_token=TOKEN)
    assert signed.freshness_key("season") == "day-before-midnight"
    assert signed.freshness_key("player") == "week-before-boundary"


@pytest.mark.parametrize("phase", ["season", "targets", "players"])
def test_out_of_window_single_year_partition_is_clean_empty_plan(
    tmp_path, monkeypatch, phase
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    patches = _common_patches(
        SimpleNamespace(missing_raw_keys=("must-not-be-read",))
    )
    with (
        patches[0],
        patches[1],
        patches[2] as catalog_patch,
        patch(
            "dags.scripts.prepare_sofascore_workload.plan_season_partition",
            side_effect=AssertionError("inactive raw must not be inspected"),
        ),
        patch("utils.medallion_config.is_single_year_competition", return_value=True),
        patch("utils.medallion_config.get_active_season", return_value=None),
    ):
        path = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-inactive",
            phase=phase,
            competition_seasons=[CompetitionSeason("INT-World Cup", "2026")],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / f"{phase}.json",
        )

    signed = load_plan(path, control_token=TOKEN)
    assert signed.run_id == f"scheduled-inactive::{phase}"
    assert signed.allocations == ()
    assert signed.run_cap_bytes == 0
    catalog_patch.return_value.resolve_source_season.assert_not_called()


def test_target_phase_batches_matches_only_and_never_snapshots_players(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    season_plan = SimpleNamespace(missing_raw_keys=())
    patches = _common_patches(season_plan)
    matches = {str(value) for value in range(1, 28)}

    def pending(_runtime, ids, _builder):
        return tuple(sorted(ids, key=int))

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patch(
            "dags.scripts.prepare_sofascore_workload._observed_player_ids",
            side_effect=AssertionError("targets phase must not read players"),
        ) as observed_probe,
        patch(
            "dags.scripts.prepare_sofascore_workload.squad_player_ids",
            side_effect=AssertionError("targets phase must not read squads"),
        ) as squad_probe,
        patch(
            "dags.scripts.prepare_sofascore_workload._finished_match_ids",
            return_value=matches,
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload._pending_targets",
            side_effect=pending,
        ),
    ):
        path = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-1",
            phase="targets",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / "target-plan.json",
        )

    signed = load_plan(path, control_token=TOKEN)
    matches = [item for item in signed.allocations if item.scope == "match"]
    players = [item for item in signed.allocations if item.scope == "player"]
    assert [len(target_ids(item)) for item in matches] == [25, 2]
    assert players == []
    assert signed.player_universe_ids == ()
    observed_probe.assert_not_called()
    squad_probe.assert_not_called()


def test_players_phase_rereads_fresh_match_universe_then_signs_all_players(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    patches = _common_patches(SimpleNamespace(missing_raw_keys=()))
    observed = {"98", "99"}
    registered = {str(value) for value in range(1, 54)}

    def pending(_runtime, ids, builder):
        specs = builder(next(iter(ids))) if ids else ()
        if specs and specs[0].key.endpoint in {"event", "lineups"}:
            return ()
        return tuple(sorted(ids, key=int))

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patch(
            "dags.scripts.prepare_sofascore_workload.squad_player_ids",
            return_value=tuple(registered),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload._observed_player_ids",
            return_value=observed,
        ) as observed_probe,
        patch(
            "dags.scripts.prepare_sofascore_workload._finished_match_ids",
            return_value={"7"},
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload._pending_targets",
            side_effect=pending,
        ),
    ):
        path = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-1",
            phase="players",
            competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / "player-plan.json",
        )

    signed = load_plan(path, control_token=TOKEN)
    assert signed.run_id == "scheduled-1::players"
    assert [
        len(target_ids(item)) for item in signed.allocations if item.scope == "player"
    ] == [50, 5]
    assert not [item for item in signed.allocations if item.scope == "match"]
    assert len(signed.player_universe_ids) == 55
    assert signed.dq_dependencies[0] == "materialize_full_player_universe"
    observed_probe.assert_called_once_with("ENG-Premier League", "2526")


def test_target_phase_fails_before_ids_if_season_raw_is_incomplete(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    patches = _common_patches(SimpleNamespace(missing_raw_keys=("missing",)))
    with patches[0], patches[1], patches[2], patches[3]:
        with pytest.raises(RuntimeError, match="season raw is incomplete"):
            prepare_workload_plan(
                dag_id="dag_ingest_sofascore",
                base_run_id="scheduled-1",
                phase="targets",
                competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
                artifact_path=tmp_path / "artifact.json",
            )


def test_players_phase_fails_closed_until_every_match_endpoint_is_terminal(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    patches = _common_patches(SimpleNamespace(missing_raw_keys=()))
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patch(
            "dags.scripts.prepare_sofascore_workload._finished_match_ids",
            return_value={"7"},
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload._pending_targets",
            return_value=("7",),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload._observed_player_ids",
            side_effect=AssertionError("must fail before reading player evidence"),
        ) as player_probe,
    ):
        with pytest.raises(RuntimeError, match="match raw/manifest is incomplete"):
            prepare_workload_plan(
                dag_id="dag_ingest_sofascore",
                base_run_id="scheduled-1",
                phase="players",
                competition_seasons=[CompetitionSeason("ENG-Premier League", "2526")],
                artifact_path=tmp_path / "artifact.json",
                output_path=tmp_path / "player-plan.json",
            )
    player_probe.assert_not_called()


# ---------------------------------------------------------------------------
# #946 4a-S3 — the season class follows the shape, and the band comes from
# competitions.yaml (never from a guess or a tournament id).
# ---------------------------------------------------------------------------


def _world_cup_catalog():
    catalog = MagicMock()
    catalog.competition.return_value = SimpleNamespace(
        capture_allowed=True,
        unique_tournament_id=16,
    )
    catalog.resolve_source_season.return_value = SimpleNamespace(
        season_id=41087,
        format="calendar_year",
    )
    return catalog


WORLD_CUP_SEASON_SHAPE = production_season_shape(
    season_format="calendar_year",
    team_count_band="33_48",
    max_pages_per_direction=50,
)


def test_season_class_uses_the_configured_team_count_band(tmp_path, monkeypatch):
    # INT-World Cup 2026: 48 teams (competitions.yaml) -> band 33_48, and its
    # season_format comes from the registry -> a class of its own.
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.load_verified_workload_policy",
            return_value=_policy(WORLD_CUP_SEASON_SHAPE, tournament="16"),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.build_capture_runtime",
            return_value=SimpleNamespace(
                raw_store=MagicMock(), manifest_store=MagicMock()
            ),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.SofaScoreCatalog.load",
            return_value=_world_cup_catalog(),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.plan_season_partition",
            return_value=SimpleNamespace(missing_raw_keys=("schedule-last-0",)),
        ),
    ):
        path = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-wc",
            phase="season",
            competition_seasons=[CompetitionSeason("INT-World Cup", "2026")],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / "wc-season-plan.json",
            allow_inactive_season=True,
        )

    signed = load_plan(path, control_token=TOKEN)
    assert [item.workload_class for item in signed.allocations] == [
        season_workload_class(WORLD_CUP_SEASON_SHAPE)
    ]
    assert season_workload_class(WORLD_CUP_SEASON_SHAPE) != season_workload_class(
        EPL_SEASON_SHAPE
    )


def test_unmeasured_band_cannot_be_signed_against_another_leagues_class(
    tmp_path, monkeypatch
):
    # Only the 16_20/split_year class is verified: a 48-team calendar-year
    # season must not borrow it. Fail-closed, no plan on disk.
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    from scrapers.sofascore.workload_plan import WorkloadPolicyUnavailable

    destination = tmp_path / "wc-borrowed-plan.json"
    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.load_verified_workload_policy",
            return_value=_policy(tournament="16"),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.build_capture_runtime",
            return_value=SimpleNamespace(
                raw_store=MagicMock(), manifest_store=MagicMock()
            ),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.SofaScoreCatalog.load",
            return_value=_world_cup_catalog(),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload.plan_season_partition",
            return_value=SimpleNamespace(missing_raw_keys=("schedule-last-0",)),
        ),
        pytest.raises(WorkloadPolicyUnavailable),
    ):
        prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-wc-borrow",
            phase="season",
            competition_seasons=[CompetitionSeason("INT-World Cup", "2026")],
            artifact_path=tmp_path / "artifact.json",
            output_path=destination,
            allow_inactive_season=True,
        )
    assert not destination.exists()


def test_league_without_a_configured_season_fails_the_phase_loudly(
    tmp_path, monkeypatch
):
    # A league that is enabled in the SofaScore registry but absent from
    # competitions.yaml has no team_count -> no band -> no class. It must fail
    # the phase, never be silently skipped.
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    from utils.medallion_config import MedallionConfigError

    patches = _common_patches(SimpleNamespace(missing_raw_keys=()))
    destination = tmp_path / "unknown-plan.json"
    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        pytest.raises(MedallionConfigError, match="not found in competitions.yaml"),
    ):
        prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-unknown",
            phase="season",
            competition_seasons=[CompetitionSeason("XX-Unregistered League", "2526")],
            artifact_path=tmp_path / "artifact.json",
            output_path=destination,
        )
    assert not destination.exists()


# ---------------------------------------------------------------------------
# #946 4d — weekly player rotation.
# ---------------------------------------------------------------------------

ROTATION_LEAGUES = [
    "ENG-Premier League",
    "ESP-La Liga",
    "ITA-Serie A",
    "GER-Bundesliga",
    "FRA-Ligue 1",
    "NED-Eredivisie",
    "POR-Primeira Liga",
    "BEL-Pro League",
    "TUR-Super Lig",
    "SCO-Premiership",
    "AUT-Bundesliga",
    "SUI-Super League",
]


@pytest.fixture
def rotation_env(monkeypatch):
    monkeypatch.setenv("SOFASCORE_PLAYER_ROTATION_MODULUS", "4")
    monkeypatch.setenv("SOFASCORE_PLAYER_ROTATION_MIN_LEAGUES", "10")


def test_cohort_is_stable_across_processes(rotation_env):
    import subprocess
    import sys

    expected = {
        league: player_rotation_cohort(league, modulus=4) for league in ROTATION_LEAGUES
    }
    # A salted hash() would differ between the DAG gate process and this
    # planner subprocess; PYTHONHASHSEED=0 vs random must not matter.
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    code = (
        "import json,sys;"
        f"sys.path[:0] = [{str(root)!r}, {str(root / 'dags')!r}];"
        "from dags.scripts.prepare_sofascore_workload import player_rotation_cohort;"
        "print(json.dumps({lg: player_rotation_cohort(lg, modulus=4) "
        f"for lg in {ROTATION_LEAGUES!r}}}))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
        cwd=str(root),
        env={"PYTHONHASHSEED": "random", "PATH": "/usr/bin:/bin"},
    )
    assert json.loads(out.stdout) == expected


def test_every_league_is_due_exactly_once_per_modulus_of_weeks(rotation_env):
    # 4 consecutive ISO weeks (2026-01-05 is a Monday, ISO week 2).
    weeks = [date(2026, 1, 5), date(2026, 1, 12), date(2026, 1, 19), date(2026, 1, 26)]
    due_counts = {league: 0 for league in ROTATION_LEAGUES}
    for day in weeks:
        for league in ROTATION_LEAGUES:
            if player_rotation_due(
                league,
                rotation_date=day,
                club_league_count=len(ROTATION_LEAGUES),
            ):
                due_counts[league] += 1
    assert set(due_counts.values()) == {1}


def test_rotation_is_uniform_across_a_53_week_iso_year(rotation_env):
    # 2020 is a 53-week ISO year: iso_week % modulus would collide W53 with W1
    # of 2021 (53 % 4 == 1 == 1 % 4), double-booking one cohort and starving
    # another. Walk 60 consecutive Saturdays across the 2020->2021 boundary and
    # require each cohort's due weeks to be evenly spaced by exactly `modulus`.
    modulus = 4
    start = date(2020, 12, 5)  # Saturday, inside the 53-week ISO year
    weeks = [start + timedelta(weeks=i) for i in range(60)]
    due_indices: dict[str, list[int]] = {league: [] for league in ROTATION_LEAGUES}
    for idx, day in enumerate(weeks):
        for league in ROTATION_LEAGUES:
            if player_rotation_due(
                league,
                rotation_date=day,
                club_league_count=len(ROTATION_LEAGUES),
            ):
                due_indices[league].append(idx)

    # Every league is captured (no cohort starved), and each is due on a strict
    # `modulus`-week cadence with no doubles (gap 0) and no skips (gap > modulus).
    for league, idxs in due_indices.items():
        assert idxs, f"{league} was never due across 60 weeks"
        gaps = {later - earlier for earlier, later in zip(idxs, idxs[1:])}
        assert gaps == {modulus}, (league, idxs)


def test_rotation_is_inert_below_the_league_threshold(rotation_env):
    # Today's scope (EPL + World Cup) keeps its weekly cadence unchanged.
    assert player_rotation_modulus(1) == 1
    assert player_rotation_modulus(len(ROTATION_LEAGUES)) == 4
    for day in (date(2026, 1, 5), date(2026, 1, 12)):
        assert (
            player_rotation_due(
                "ENG-Premier League", rotation_date=day, club_league_count=1
            )
            is True
        )


def test_force_tournament_and_missing_date_are_always_due(rotation_env):
    week = date(2026, 1, 5)
    not_due = [
        league
        for league in ROTATION_LEAGUES
        if not player_rotation_due(
            league, rotation_date=week, club_league_count=len(ROTATION_LEAGUES)
        )
    ]
    assert not_due, "fixture must contain a league outside this week's cohort"
    victim = not_due[0]
    assert (
        player_rotation_due(
            victim,
            rotation_date=week,
            club_league_count=len(ROTATION_LEAGUES),
            force=True,
        )
        is True
    )
    assert (
        player_rotation_due(
            victim,
            rotation_date=week,
            club_league_count=len(ROTATION_LEAGUES),
            is_tournament=True,
        )
        is True
    )
    assert (
        player_rotation_due(
            victim,
            rotation_date=None,
            club_league_count=len(ROTATION_LEAGUES),
        )
        is True
    )


def test_rotation_boundary_parses_a_date_or_the_master_interval_datetime():
    # The gate resolves the master data_interval_end (a tz-aware datetime); the
    # planner must accept both it and a plain ds so both use the same boundary.
    assert _parse_rotation_boundary("2026-01-10") == date(2026, 1, 10)
    assert _parse_rotation_boundary("2024-01-06T14:00:00+00:00") == date(2024, 1, 6)
    assert _parse_rotation_boundary("2024-01-06T14:00:00Z") == date(2024, 1, 6)
    assert _parse_rotation_boundary("  ") is None
    assert _parse_rotation_boundary("not-a-date") is None


def _players_plan_for(tmp_path, leagues, **kwargs):
    """Sign a players plan for ``leagues`` with every Bronze probe stubbed."""

    patches = _common_patches(SimpleNamespace(missing_raw_keys=()))

    def pending(_runtime, ids, builder):
        specs = builder(next(iter(ids))) if ids else ()
        if specs and specs[0].key.endpoint in {"event", "lineups"}:
            return ()
        return tuple(sorted(ids, key=int))

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3],
        patch(
            "dags.scripts.prepare_sofascore_workload.squad_player_ids",
            return_value=("1", "2", "3"),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload._observed_player_ids",
            return_value={"4"},
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload._finished_match_ids",
            return_value={"7"},
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload._pending_targets",
            side_effect=pending,
        ),
        patch("utils.medallion_config.get_season_team_count", return_value=20),
    ):
        path = prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id=kwargs.pop("base_run_id", "scheduled-rot"),
            phase="players",
            competition_seasons=[
                CompetitionSeason(league, "2526") for league in leagues
            ],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / f"{kwargs.pop('name', 'players')}.json",
            **kwargs,
        )
    return _planned_leagues(load_plan(path, control_token=TOKEN))


def _planned_leagues(signed):
    from scrapers.sofascore.workload_plan import parse_qualified_work_unit
    from scrapers.sofascore.workload_runtime import parse_partition_key

    return {
        parse_partition_key(parse_qualified_work_unit(unit)[0])[0]
        for unit in signed.player_universe_ids
    }


def test_players_plan_contains_only_the_due_cohort(tmp_path, monkeypatch, rotation_env):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    week = date(2026, 1, 5)
    expected = {
        league
        for league in ROTATION_LEAGUES
        if player_rotation_due(
            league, rotation_date=week, club_league_count=len(ROTATION_LEAGUES)
        )
    }
    assert 0 < len(expected) < len(ROTATION_LEAGUES)

    planned = _players_plan_for(
        tmp_path,
        ROTATION_LEAGUES,
        players_rotation_date=week,
        name="cohort",
    )
    assert planned == expected


def test_players_plan_without_a_rotation_date_keeps_every_league(
    tmp_path, monkeypatch, rotation_env
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    planned = _players_plan_for(
        tmp_path,
        ROTATION_LEAGUES,
        base_run_id="scheduled-norot",
        name="norotation",
    )
    assert planned == set(ROTATION_LEAGUES)


def test_forced_players_plan_keeps_every_league(tmp_path, monkeypatch, rotation_env):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    planned = _players_plan_for(
        tmp_path,
        ROTATION_LEAGUES,
        players_rotation_date=date(2026, 1, 5),
        players_force=True,
        base_run_id="manual-force",
        name="forced",
    )
    assert planned == set(ROTATION_LEAGUES)


def test_non_due_leagues_cost_no_trino_or_squad_read(
    tmp_path, monkeypatch, rotation_env
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    week = date(2026, 1, 5)
    expected = {
        league
        for league in ROTATION_LEAGUES
        if player_rotation_due(
            league, rotation_date=week, club_league_count=len(ROTATION_LEAGUES)
        )
    }
    patches = _common_patches(SimpleNamespace(missing_raw_keys=()))

    def pending(_runtime, ids, builder):
        specs = builder(next(iter(ids))) if ids else ()
        if specs and specs[0].key.endpoint in {"event", "lineups"}:
            return ()
        return tuple(sorted(ids, key=int))

    with (
        patches[0],
        patches[1],
        patches[2],
        patches[3] as season_probe,
        patch(
            "dags.scripts.prepare_sofascore_workload.squad_player_ids",
            return_value=("1", "2"),
        ),
        patch(
            "dags.scripts.prepare_sofascore_workload._observed_player_ids",
            return_value={"3"},
        ) as observed_probe,
        patch(
            "dags.scripts.prepare_sofascore_workload._finished_match_ids",
            return_value={"7"},
        ) as match_probe,
        patch(
            "dags.scripts.prepare_sofascore_workload._pending_targets",
            side_effect=pending,
        ),
        patch("utils.medallion_config.get_season_team_count", return_value=20),
    ):
        prepare_workload_plan(
            dag_id="dag_ingest_sofascore",
            base_run_id="scheduled-skip",
            phase="players",
            competition_seasons=[
                CompetitionSeason(league, "2526") for league in ROTATION_LEAGUES
            ],
            artifact_path=tmp_path / "artifact.json",
            output_path=tmp_path / "skipped.json",
            players_rotation_date=week,
        )

    probed = {call.args[0] for call in match_probe.call_args_list}
    assert probed == expected
    assert {call.args[0] for call in observed_probe.call_args_list} == expected
    # Season raw is not even inspected for a league nobody will capture.
    assert season_probe.call_count == len(expected)
