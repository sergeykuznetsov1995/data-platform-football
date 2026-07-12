from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dags.scripts.prepare_sofascore_workload import (
    CompetitionSeason,
    _observed_player_ids,
    prepare_workload_plan,
)
from scrapers.sofascore.workload_plan import (
    MATCH_WORKLOAD_CLASS,
    PLAYER_WORKLOAD_CLASS,
    WorkloadBudgetPolicy,
    WorkloadClassBudget,
    production_season_shape,
    season_shape_digest,
    season_workload_class,
)
from scrapers.sofascore.workload_runtime import load_plan, target_ids


TOKEN = "prepare-test-control-token-that-is-at-least-32-bytes"


def _policy():
    shape = production_season_shape(
        17, season_format="split_year", max_pages_per_direction=50
    )
    season_class = season_workload_class(17, shape)
    return WorkloadBudgetPolicy(
        "b" * 64,
        {
            MATCH_WORKLOAD_CLASS: WorkloadClassBudget(
                MATCH_WORKLOAD_CLASS,
                "match",
                25,
                100,
                ("event",),
                20,
                5,
                source_tournament_id="17",
            ),
            PLAYER_WORKLOAD_CLASS: WorkloadClassBudget(
                PLAYER_WORKLOAD_CLASS,
                "player",
                50,
                200,
                ("player_profile",),
                20,
                5,
                source_tournament_id="17",
            ),
            season_class: WorkloadClassBudget(
                season_class,
                "season",
                1,
                300,
                ("schedule_last",),
                20,
                5,
                season_shape_digest(shape),
                "17",
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
