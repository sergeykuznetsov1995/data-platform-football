"""Scope-cycle regression tests using synthetic phases and DQ reports."""

import json

import pytest

from dags.scripts import run_sofascore_scope_cycle as cycle


SCOPE = {
    "snapshot_id": "a" * 64, "campaign_id": "c" * 64, "scope_digest": "b" * 64,
    "capture_key": "SS-17", "canonical_season": "2526",
    "tournament_id": 17, "source_season_id": 76986,
}


def _dq_report(status="success"):
    return {
        "status": status, "league": "SS-17", "season": "2526",
        "source_tournament_id": 17, "source_season_id": 76986,
        "checks": 1, "planned_endpoints": 5, "terminal_endpoints": 5,
        "rejected_endpoints": 0, "rejected_rows": {},
        "violations": [] if status == "success" else ["child-season mismatch"],
        "errors": [],
    }


def _args(tmp_path, phase="matches"):
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text("{}", encoding="utf-8")
    return [
        "--snapshot", str(snapshot), "--tournament-id", "17",
        "--source-season-id", "76986", "--expected-snapshot-id", "a" * 64,
        "--expected-campaign-id", "c" * 64,
        "--output-dir", str(tmp_path / "output"),
        "--output", str(tmp_path / "result.json"),
        "--workload-artifact", str(tmp_path / "workload.json"), "--phase", phase,
    ]


@pytest.fixture
def harness(monkeypatch, tmp_path):
    state = {
        "phase": {
            "phase": "matches", "status": "success", "errors": [],
            "planned_endpoints": 5, "terminal_endpoints": 5, "rejected_endpoints": 0,
        },
        "dq": _dq_report(), "calls": [],
    }
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *args, **kwargs: dict(SCOPE))
    overlay = cycle.ScopeOverlayPaths(
        tmp_path / "registry.json", tmp_path / "medallion" / "competitions.yaml"
    )
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *args, **kwargs: overlay)
    monkeypatch.setattr(cycle, "run_phase", lambda *args, **kwargs: dict(state["phase"]))

    def run_dq(scope, phases, *, require_complete, **kwargs):
        state["calls"].append((scope, phases, require_complete))
        return state["dq"]

    monkeypatch.setattr(cycle, "run_scope_bronze_dq", run_dq, raising=False)
    return state


def _result(tmp_path):
    return json.loads((tmp_path / "result.json").read_text(encoding="utf-8"))


def test_success_persists_top_level_bronze_dq(harness, tmp_path):
    assert cycle.main(_args(tmp_path)) == 0
    result = _result(tmp_path)
    assert result["status"] == "success"
    assert result["bronze_dq"] == harness["dq"]
    assert len(harness["calls"]) == 1
    scope, phases, require_complete = harness["calls"][0]
    assert all(scope[key] == value for key, value in SCOPE.items())
    assert phases and phases[0]["planned_endpoints"] == 5
    assert require_complete is True


def test_failed_dq_fails_cycle_and_persists_reason(harness, tmp_path):
    harness["dq"] = _dq_report("failed")
    assert cycle.main(_args(tmp_path)) == 1
    result = _result(tmp_path)
    assert result["status"] == "failed"
    assert result["bronze_dq"] == harness["dq"]
    assert result["errors"]
    assert any(word in repr(result["errors"]).lower() for word in ("dq", "child-season"))


def test_partial_runs_correctness_gates_without_requiring_closure(harness, tmp_path):
    partial = {"remaining_matches": 1, "stop_reason": "time_budget", "elapsed_s": 1.0}
    harness["phase"].update(status="partial", terminal_endpoints=4, partial=partial)
    harness["dq"]["terminal_endpoints"] = 4
    assert cycle.main(_args(tmp_path)) == 0
    result = _result(tmp_path)
    assert result["status"] == "partial"
    assert result["bronze_dq"] == harness["dq"]
    assert len(harness["calls"]) == 1
    assert harness["calls"][0][2] is False
    assert harness["calls"][0][1][0]["partial"] == partial


def test_verified_waiting_runs_dq_without_requiring_closure(harness, tmp_path):
    harness["phase"].update({
        "phase": "season",
        "status": "success",
        "schedule_inventory": {
            "verified": True,
            "tournament_id": 17,
            "source_season_id": 76986,
            "canonical_season": "2526",
            "finished_matches": 0,
            "schedule_events": 0,
        },
    })
    assert cycle.main(_args(tmp_path, "all")) == 0
    assert len(harness["calls"]) == 1
    assert harness["calls"][0][2] is False
    result = _result(tmp_path)
    assert result["status"] == "waiting"
    assert result["bronze_dq"] == harness["dq"]


@pytest.mark.parametrize("phase,status", [("metadata", "success"), ("matches", "failed")])
def test_metadata_and_failed_phase_do_not_fabricate_dq(harness, tmp_path, phase, status):
    harness["phase"].update(phase=phase, status=status)
    if status == "failed":
        harness["phase"]["errors"] = ["synthetic phase failure"]
    result_code = cycle.main(_args(tmp_path, phase))
    result = _result(tmp_path)
    assert not harness["calls"]
    assert not result.get("bronze_dq")
    if status == "failed":
        assert result_code == 1
        assert result["status"] == "failed"


def test_phase_report_retains_endpoint_and_rejection_accounting(tmp_path):
    fields = {
        "planned_endpoints": 5,
        "terminal_endpoints": 4,
        "rejected_endpoints": 1,
        "endpoint_completeness": 0.8,
        "rejected_rows": {"bronze.sofascore_events": {"schema_error": 2}},
        "rejected_players": 0,
    }
    path = tmp_path / "phase.json"
    path.write_text(json.dumps({"status": "success", "errors": [], **fields}), encoding="utf-8")
    report = cycle._phase_report(path)
    for name, value in fields.items():
        assert report[name] == value
