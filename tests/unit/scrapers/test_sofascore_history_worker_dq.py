"""History outcomes and controller accounting must fail closed on Bronze DQ."""

import json

import pytest

from scrapers.sofascore import history_controller, history_worker


def _report(status="success"):
    return {
        "status": status, "league": "SS-8", "season": "2526",
        "source_tournament_id": 8, "source_season_id": 825,
        "checks": 2, "violations": [] if status == "success" else ["child-season mismatch"],
        "errors": [], "planned_endpoints": 5, "terminal_endpoints": 4,
        "rejected_endpoints": 1, "rejected_rows": {"schema_error": 2},
    }


def _result(report, status="success"):
    result = {
        "status": status, "campaign_id": "c", "tournament_id": 8,
        "source_season_id": 825, "run_id": "scope", "errors": [],
        "rejected": 2, "rejected_rows": {"schema_error": 2},
        "phases": [{
            "phase": "matches", "status": status, "errors": [],
            "planned_endpoints": 5, "terminal_endpoints": 4, "rejected_endpoints": 1,
            "rejected": 2, "rejected_rows": {"schema_error": 2},
        }],
    }
    if report is not None:
        result["bronze_dq"] = report
    return result


def _env(tmp_path, result):
    path = tmp_path / "result.json"
    path.write_text(json.dumps(result), encoding="utf-8")
    return {
        "SOFASCORE_SCOPE_RESULT_PATH": str(path),
        "SOFASCORE_SCOPE_KEY": "c:8:825", "SOFASCORE_SCOPE_RUN_ID": "scope",
        "SOFASCORE_CANONICAL_SEASON": "2526", "SOFASCORE_TOURNAMENT_ID": "8",
        "SOFASCORE_SOURCE_SEASON_ID": "825", "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c",
        "SOFASCORE_CAMPAIGN_ACTION": "capture", "SOFASCORE_HISTORY_GROUP": "main",
    }


def test_success_retains_rejected_count_and_dq_report(tmp_path):
    report = _report()
    outcome = history_worker.scope_outcome(_env(tmp_path, _result(report)), 0)
    assert outcome["status"] == "success"
    assert outcome["rejected_endpoints"] == 1
    assert outcome["bronze_dq"] == report


def test_verified_partial_retains_dq_report(tmp_path, monkeypatch):
    from dags.utils import sofascore_red_share

    report = _report()
    result = _result(report, "partial")
    partial = {"remaining_matches": 1, "stop_reason": "time_budget", "elapsed_s": 1.0}
    result["partial"] = partial
    result["phases"][0]["partial"] = partial
    def proof(*args, **kwargs):
        return dict(partial)
    monkeypatch.setattr(sofascore_red_share, "season_continuation_proof", proof)
    if hasattr(history_worker, "season_continuation_proof"):
        monkeypatch.setattr(history_worker, "season_continuation_proof", proof)
    outcome = history_worker.scope_outcome(_env(tmp_path, result), 0)
    assert outcome["status"] == "partial"
    assert outcome["bronze_dq"] == report


def test_failed_dq_result_keeps_report_and_reason(tmp_path):
    report = _report("failed")
    result = _result(report, "failed")
    result["errors"] = ["Bronze DQ failed: child-season mismatch"]
    outcome = history_worker.scope_outcome(_env(tmp_path, result), 1)
    assert outcome["status"] == "failed"
    assert outcome["bronze_dq"] == report
    assert outcome["reason"]
    assert "dq" in outcome["reason"].lower() or "child-season" in outcome["reason"].lower()


@pytest.mark.parametrize(
    "report",
    [None, {}, "success", [], {"status": "unknown"}, {"status": "success"}, _report("failed")],
    ids=["missing", "empty-object", "string", "list", "unknown-status", "incomplete-report", "failed"],
)
def test_purported_success_with_invalid_dq_is_failed(tmp_path, report):
    outcome = history_worker.scope_outcome(_env(tmp_path, _result(report)), 0)
    assert outcome["status"] == "failed"
    assert outcome["reason"]
    assert any(word in outcome["reason"].lower() for word in ("dq", "bronze"))
    if isinstance(report, dict) and report.get("status") == "failed":
        assert outcome["bronze_dq"] == report


def test_failed_dq_is_accounted_as_failure_never_completion(tmp_path, monkeypatch):
    reason = "Bronze DQ failed: child-season mismatch"
    report = _report("failed")
    env = _env(tmp_path, _result(report, "failed"))
    outcome = {"status": "failed", "reason": reason, "bronze_dq": report, "rejected": 2}
    failures = []

    def unexpected_completion(*args, **kwargs):
        pytest.fail("A failed Bronze DQ scope must never be marked completed")

    def mark_failed(*args, **kwargs):
        failures.append((args, kwargs))
        return True

    monkeypatch.setattr(history_controller.state, "mark_completed", unexpected_completion)
    monkeypatch.setattr(history_controller.state, "mark_failed", mark_failed)
    history_controller.account_scope(
        env, outcome, state_path=tmp_path / "state.json",
        failures_path=tmp_path / "failures.json", release="test",
    )
    assert len(failures) == 1
    assert reason in repr(failures[0])
