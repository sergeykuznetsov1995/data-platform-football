from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dags.scripts import run_sofascore_scope_cycle as cycle


@pytest.mark.unit
def test_cycle_sets_exact_overlays_before_running_phases(tmp_path, monkeypatch):
    scope = {
        "snapshot_id": "a" * 64,
        "campaign_id": "c" * 64,
        "scope_digest": "b" * 64,
        "capture_key": "SS-17",
        "canonical_season": "2526",
    }
    registry = tmp_path / "scope" / "tournaments.json"
    medallion = tmp_path / "scope" / "medallion" / "competitions.yaml"
    registry.parent.mkdir(parents=True)
    medallion.parent.mkdir(parents=True)
    registry.write_text("{}")
    medallion.write_text("competitions: []")
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *a, **k: scope)
    monkeypatch.setattr(
        cycle,
        "render_scope_overlays",
        lambda *a, **k: cycle.ScopeOverlayPaths(registry, medallion),
    )
    calls = []

    def run_phase(phase, actual_scope, *, output_dir, workload_artifact):
        calls.append((phase, dict(actual_scope), Path(output_dir), workload_artifact))
        assert cycle.os.environ["SOFASCORE_REGISTRY_PATH"] == str(registry)
        assert cycle.os.environ["MEDALLION_CONFIG_DIR"] == str(medallion.parent)
        return {"phase": phase, "status": "success"}

    monkeypatch.setattr(cycle, "run_phase", run_phase)
    output = tmp_path / "result.json"

    assert cycle.main([
        "--snapshot", str(tmp_path / "snapshot.json"),
        "--tournament-id", "17",
        "--source-season-id", "76986",
        "--expected-snapshot-id", "a" * 64,
        "--expected-campaign-id", "c" * 64,
        "--phase", "all",
        "--output-dir", str(tmp_path / "run"),
        "--output", str(output),
        "--workload-artifact", str(tmp_path / "artifact.json"),
    ]) == 0

    assert [item[0] for item in calls] == ["season", "matches"]
    result = json.loads(output.read_text())
    assert result["status"] == "success"
    assert result["scope_digest"] == "b" * 64


@pytest.mark.unit
def test_cycle_stops_before_matches_when_season_fails(tmp_path, monkeypatch):
    scope = {
        "snapshot_id": "a" * 64,
        "campaign_id": "c" * 64,
        "scope_digest": "b" * 64,
        "capture_key": "SS-17",
        "canonical_season": "2526",
    }
    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        tmp_path / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *a, **k: scope)
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)
    monkeypatch.setattr(
        cycle,
        "run_phase",
        lambda phase, *a, **k: {"phase": phase, "status": "failed", "exit_code": 1},
    )

    assert cycle.main([
        "--snapshot", str(tmp_path / "snapshot.json"),
        "--tournament-id", "17",
        "--source-season-id", "76986",
        "--expected-snapshot-id", "a" * 64,
        "--expected-campaign-id", "c" * 64,
        "--phase", "all",
        "--output-dir", str(tmp_path / "run"),
        "--output", str(tmp_path / "result.json"),
        "--workload-artifact", str(tmp_path / "artifact.json"),
    ]) == 1


def _scope():
    return {
        "snapshot_id": "a" * 64,
        "campaign_id": "c" * 64,
        "scope_digest": "b" * 64,
        "capture_key": "SS-17",
        "canonical_season": "2526",
    }


def _cycle_argv(tmp_path, *extra):
    return [
        "--snapshot", str(tmp_path / "snapshot.json"),
        "--tournament-id", "17",
        "--source-season-id", "76986",
        "--expected-snapshot-id", "a" * 64,
        "--expected-campaign-id", "c" * 64,
        "--output-dir", str(tmp_path / "run"),
        "--output", str(tmp_path / "result.json"),
        "--workload-artifact", str(tmp_path / "artifact.json"),
        *extra,
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    "extra_argv, allow_pending, evidence",
    [
        (["--phase", "matches"], False, "pages"),
        (
            ["--phase", "matches", "--season-evidence", "bronze",
             "--allow-pending-season"],
            True,
            "bronze",
        ),
    ],
)
def test_cycle_forwards_the_refresh_lane_flags(
    tmp_path, monkeypatch, extra_argv, allow_pending, evidence
):
    seen = {}

    def load_exact_scope(*_args, **kwargs):
        seen["load"] = kwargs
        return _scope()

    def run_phase(phase, actual_scope, *, output_dir, workload_artifact):
        seen["phase"] = (phase, actual_scope["season_evidence"])
        return {"phase": phase, "status": "success"}

    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        tmp_path / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(cycle, "load_exact_scope", load_exact_scope)
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)
    monkeypatch.setattr(cycle, "run_phase", run_phase)

    assert cycle.main(_cycle_argv(tmp_path, *extra_argv)) == 0

    assert seen["load"]["allow_pending_season"] is allow_pending
    assert seen["phase"] == ("matches", evidence)


@pytest.mark.unit
@pytest.mark.parametrize("phase", ["metadata", "season", "all"])
def test_bronze_season_evidence_requires_the_matches_phase(
    tmp_path, monkeypatch, phase
):
    monkeypatch.setattr(
        cycle,
        "load_exact_scope",
        lambda *a, **k: pytest.fail("argument validation must come first"),
    )

    with pytest.raises(SystemExit):
        cycle.main(_cycle_argv(
            tmp_path, "--phase", phase, "--season-evidence", "bronze"
        ))


@pytest.mark.unit
def test_run_phase_plans_matches_from_the_scope_season_evidence(tmp_path):
    scope = {**_scope(), "run_id": "refresh-1", "season_evidence": "bronze"}
    plan = tmp_path / "targets-plan.json"

    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.prepare_workload_plan",
            return_value=plan,
        ) as planner,
        patch("dags.scripts.run_sofascore_scraper.main", return_value=0),
    ):
        result = cycle.run_phase(
            "matches",
            scope,
            output_dir=tmp_path / "run",
            workload_artifact=tmp_path / "artifact.json",
        )

    assert result == {
        "phase": "matches",
        "status": "success",
        "exit_code": 0,
        "plan": str(plan),
    }
    assert planner.call_args.kwargs["phase"] == "targets"
    assert planner.call_args.kwargs["season_evidence"] == "bronze"
