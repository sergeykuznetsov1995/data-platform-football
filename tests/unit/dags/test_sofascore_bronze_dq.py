"""DQ must survive DAG validation, failure XComs, and window summaries."""

import json
from types import SimpleNamespace

import pytest

from dags import dag_backfill_sofascore_all_mens as history
from dags import dag_refresh_sofascore_all_mens as refresh


def _report(status="success", *, checks=2, terminal=4, rejected=1):
    return {
        "status": status, "league": "SS-8", "season": "2526",
        "source_tournament_id": 8, "source_season_id": 825,
        "checks": checks, "violations": [] if status == "success" else ["child-season mismatch"],
        "errors": [], "planned_endpoints": 5, "terminal_endpoints": terminal,
        "rejected_endpoints": rejected, "rejected_rows": {"schema_error": 2},
    }


def _result(report, status="success"):
    result = {
        "status": status, "campaign_id": "c", "tournament_id": 8,
        "source_season_id": 825, "run_id": "scope", "errors": [],
    }
    if report is not None:
        result["bronze_dq"] = report
    return result


class FakeTI:
    def __init__(self, pulls=None):
        self.pulls = pulls or {}
        self.pushed = {}
        self.map_index = 0
        self.task_id = "validate_refresh_scope"

    def xcom_push(self, key, value, **kwargs):
        self.pushed[key] = value

    def xcom_pull(self, task_ids=None, key="return_value", **kwargs):
        if isinstance(task_ids, (list, tuple)):
            return [self.xcom_pull(task_ids=task_id, key=key, **kwargs) for task_id in task_ids]
        return self.pulls.get((task_ids, key), self.pulls.get(key))


def _dag_run(validation_state="success"):
    tasks = [
        SimpleNamespace(task_id="run_refresh_scope", state="success", map_index=0),
        SimpleNamespace(task_id="validate_refresh_scope", state=validation_state, map_index=0),
        SimpleNamespace(task_id="window_summary", state="success", map_index=-1),
    ]
    return SimpleNamespace(
        run_id="unit-test", conf={}, get_task_instances=lambda **kwargs: tasks,
        get_task_instance=lambda task_id, **kwargs: next(
            (task for task in tasks if task.task_id == task_id), None
        ),
    )


def _env(tmp_path, result, action="refresh"):
    path = tmp_path / "scope-result.json"
    path.write_text(json.dumps(result), encoding="utf-8")
    return {
        "SOFASCORE_SCOPE_RESULT_PATH": str(path), "SOFASCORE_CAMPAIGN_ACTION": action,
        "SOFASCORE_SCOPE_KEY": "c:8:825", "SOFASCORE_SCOPE_RUN_ID": "scope",
        "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c", "SOFASCORE_CANONICAL_SEASON": "2526",
        "SOFASCORE_HISTORY_GROUP": "main", "SOFASCORE_TOURNAMENT_ID": "8",
        "SOFASCORE_SOURCE_SEASON_ID": "825",
    }


def _partial_result(report):
    result = _result(report, "partial")
    partial = {"remaining_matches": 1, "stop_reason": "time_budget", "elapsed_s": 1.0}
    result["partial"] = partial
    result["phases"] = [{"phase": "matches", "status": "partial", "partial": partial, "errors": []}]
    return result


def test_refresh_success_returns_bronze_dq(tmp_path):
    report = _report()
    outcome = refresh._validate_refresh_scope(env=_env(tmp_path, _result(report)), ti=FakeTI())
    assert outcome["bronze_dq"] == report


@pytest.mark.parametrize("report", [_report("failed"), None], ids=["failed", "missing"])
def test_refresh_dq_failure_raises_and_preserves_available_report(tmp_path, report):
    ti = FakeTI()
    with pytest.raises(refresh.AirflowException):
        refresh._validate_refresh_scope(env=_env(tmp_path, _result(report)), ti=ti)
    if report is not None:
        assert ti.pushed["bronze_dq"] == report


@pytest.mark.parametrize("dq_status", ["success", "failed", "missing"])
def test_refresh_partial_requires_successful_dq_and_keeps_partial_fields(tmp_path, monkeypatch, dq_status):
    report = None if dq_status == "missing" else _report(dq_status, terminal=4, rejected=0)
    result = _partial_result(report)
    from dags.utils import sofascore_red_share

    def proof(*args, **kwargs):
        return dict(result["partial"])
    monkeypatch.setattr(sofascore_red_share, "season_continuation_proof", proof)
    if hasattr(refresh, "season_continuation_proof"):
        monkeypatch.setattr(refresh, "season_continuation_proof", proof)
    env, ti = _env(tmp_path, result), FakeTI()
    if dq_status != "success":
        with pytest.raises(refresh.AirflowException):
            refresh._validate_refresh_scope(env=env, ti=ti)
        if report is not None:
            assert ti.pushed["bronze_dq"] == report
        return
    outcome = refresh._validate_refresh_scope(env=env, ti=ti)
    assert outcome["status"] == "partial"
    assert outcome["bronze_dq"] == report
    assert outcome["remaining_matches"] == 1
    assert outcome["stop_reason"] == "time_budget"


def _count(value):
    return len(value) if isinstance(value, (list, tuple, dict)) else value


def _assert_totals(summary):
    assert summary["checks"] == 5
    assert _count(summary["violations"]) == 1
    assert summary["planned_endpoints"] == 10
    assert summary["terminal_endpoints"] == 8
    assert summary["rejected_endpoints"] == 2
    assert summary["rejected_rows"] == {"schema_error": 4}
    json.dumps(summary)  # XCom/result summaries must be JSON-safe.


def test_refresh_window_summary_includes_failed_validation_dq_xcom():
    successful = _report(checks=2)
    failed = _report("failed", checks=3)
    ti = FakeTI({
        ("validate_refresh_scope", "return_value"): [
            {"status": "success", "scope_key": "c:8:825", "bronze_dq": successful}, None,
        ],
        # Successful validators may also push DQ. Do not count that report twice.
        ("validate_refresh_scope", "bronze_dq"): [successful, failed],
    })
    summary = refresh._window_summary(ti=ti, dag_run=_dag_run("failed"))
    _assert_totals(summary["bronze_dq"])


def test_refresh_propagates_validation_failure_even_if_scope_process_succeeded():
    ti = FakeTI({("window_summary", "return_value"): {"status": "failed"}})
    with pytest.raises(refresh.AirflowException):
        refresh._propagate_status(ti=ti, dag_run=_dag_run("failed"))


def test_refresh_success_exposes_existing_window_summary(capsys):
    report = _report()
    ti = FakeTI({
        ("validate_refresh_scope", "return_value"): [
            {"status": "refreshed", "scope_key": "c:8:825", "bronze_dq": report},
        ],
    })
    returned = refresh._propagate_status(ti=ti, dag_run=_dag_run())
    printed = capsys.readouterr().out
    assert returned["status"] == "success"
    assert returned["bronze_dq"]["checks"] == report["checks"]
    assert "bronze_dq" in printed


@pytest.fixture
def history_accounting(monkeypatch):
    calls = {"completed": [], "cleared": [], "failed": []}

    def record(bucket):
        def call(*args, **kwargs):
            calls[bucket].append((args, kwargs))
            return True
        return call

    monkeypatch.setattr(history.state, "mark_completed", record("completed"))
    monkeypatch.setattr(history.state, "clear_failed", record("cleared"))
    monkeypatch.setattr(history.state, "mark_failed", record("failed"))
    return calls


def _history_validation(env, ti):
    return history._validate_historical_scope(**env)


def test_history_successful_dq_permits_completion(tmp_path, history_accounting):
    env = _env(tmp_path, _result(_report()), action="capture")
    _history_validation(env, FakeTI())
    assert len(history_accounting["completed"]) == 1
    assert not history_accounting["failed"]


@pytest.mark.parametrize("report", [_report("failed"), None], ids=["failed", "missing"])
def test_history_failed_or_missing_dq_never_marks_complete(tmp_path, history_accounting, report):
    env = _env(tmp_path, _result(report), action="capture")
    try:
        outcome = _history_validation(env, FakeTI())
    except history.AirflowException:
        pass
    else:
        assert isinstance(outcome, dict) and outcome["status"] == "failed"
    assert not history_accounting["completed"]


def test_history_summary_exposes_dq_totals():
    reports = [_report(checks=2), _report("failed", checks=3)]
    # This small helper is the summary contract when no standalone finalizer
    # summary callable is exposed; it avoids constructing a real DAG run.
    summary = history._summarize_bronze_dq(reports)
    _assert_totals(summary)


def test_season_fields_admission_counts_core_and_league_table_freshness():
    from datetime import datetime, timedelta, timezone

    as_of = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    census = [
        ("SS-17", "2526", as_of - timedelta(days=3)),
        ("SS-8", "2526", as_of - timedelta(days=9)),
        # Naive timestamps arrive from Trino; they are read as UTC.
        ("SS-9", "2526", (as_of - timedelta(days=8)).replace(tzinfo=None)),
        ("SS-11", "2526", None),
        # Ingest-owned lanes stay in the denominator, whatever their age.
        ("ENG-Premier League", "2526", as_of - timedelta(days=1)),
        # Not core: invisible to both the ratio and the selection.
        ("SS-999", "2526", as_of - timedelta(days=30)),
    ]

    admission = refresh._season_fields_admission(
        census,
        as_of=as_of,
        core_keys=frozenset(
            {"SS-17", "SS-8", "SS-9", "SS-11", "ENG-Premier League"}
        ),
    )

    assert admission["core_ongoing"] == 5
    assert admission["fresh_7d"] == 2
    assert admission["ratio"] == 0.4
    missing = next(
        item for item in admission["entries"] if item["league"] == "SS-11"
    )
    assert missing["age_days"] is None
    assert missing["fresh"] is False


def test_freshness_excludes_only_the_three_knockout_seasons(monkeypatch, tmp_path):
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    census = [
        ("SS-281", "2627", now),
        ("SS-737", "2627", None),
        ("SS-14710", "2026", None),
        ("SS-281", "2526", None),
        ("SS-9", "2627", now),
    ]
    monkeypatch.setattr(refresh, "RESULT_DIR", str(tmp_path))
    monkeypatch.setattr(refresh, "_season_fields_census", lambda: census)
    monkeypatch.setattr(refresh.state, "read_refresh_snapshot", lambda *a, **kw: {})
    selected = []

    def plan(snapshot, pairs, **kwargs):
        selected.extend(pairs)
        return [], []

    monkeypatch.setattr(refresh.state, "plan_season_fields_batch", plan)
    summary = refresh._refresh_season_fields(run_id="cup-season-freshness")

    for line in ("before", "after", "season_fields_freshness"):
        assert summary[line]["core_ongoing"] == 2
        assert summary[line]["standings_not_applicable"] == 3
        assert summary[line]["fresh_7d"] == 1
        assert summary[line]["ratio"] == 0.5
    # The seasonal workload still collects rounds and brackets for cups.
    assert set(selected) == {("SS-737", "2627"), ("SS-14710", "2026"), ("SS-281", "2526")}


def test_only_knockout_seasons_have_no_standings_freshness_ratio():
    from datetime import datetime, timezone

    admission = refresh._season_fields_admission(
        [("SS-14710", "2026", None)],
        as_of=datetime.now(timezone.utc),
        core_keys=frozenset({"SS-14710"}),
    )

    assert admission["core_ongoing"] == admission["fresh_7d"] == 0
    assert admission["ratio"] is None


def test_season_fields_selection_orders_and_respects_backoff():
    from datetime import datetime, timedelta, timezone

    as_of = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    admission = refresh._season_fields_admission(
        [
            ("SS-17", "2526", as_of - timedelta(days=9)),
            ("SS-8", "2526", as_of - timedelta(days=30)),
            ("SS-9", "2526", None),
            ("SS-50", "2526", as_of - timedelta(days=20)),
            ("SS-40", "2526", as_of - timedelta(days=7)),
            ("SS-11", "2526", as_of - timedelta(days=2)),
            ("ENG-Premier League", "2526", as_of - timedelta(days=30)),
        ],
        as_of=as_of,
        core_keys=frozenset(
            {"SS-17", "SS-8", "SS-9", "SS-40", "SS-50", "SS-11",
             "ENG-Premier League"}
        ),
    )

    pairs, skipped = refresh._select_season_fields_work(
        admission,
        as_of=as_of,
        backoff={"SS-8|2526": (as_of + timedelta(hours=1)).isoformat()},
        max_scopes=3,
        renewal_days=6,
    )

    # Missing table first, then the OLDEST tables (review r1: ascending age
    # would buy the youngest due table first and starve the stalest under
    # the cap); the ingest league never gets a refresh purchase, the
    # too-fresh partition waits, and a scope in backoff does not consume a
    # slot.
    assert pairs == [
        ("SS-9", "2526"),
        ("SS-50", "2526"),
        ("SS-17", "2526"),
    ]
    # The 7-day-old table is fourth in line: the stalest go first.
    assert ("SS-40", "2526") not in pairs
    assert "SS-8|2526:backoff" in skipped
    assert "SS-11|2526:not_due" in skipped


def test_window_summary_carries_the_season_fields_line():
    ti = FakeTI(pulls={
        ("refresh_season_fields", "return_value"): {
            "run_id": "unit-test",
            "season_fields_freshness": {
                "core_ongoing": 40, "fresh_7d": 39, "ratio": 0.975,
            },
            "executed": 5, "failed": 0, "stop_reason": "completed",
        },
    })

    summary = refresh._window_summary(ti=ti, dag_run=_dag_run())

    assert summary["season_fields"]["season_fields_freshness"] == {
        "core_ongoing": 40, "fresh_7d": 39, "ratio": 0.975,
    }
    assert summary["season_fields"]["stop_reason"] == "completed"
    json.dumps(summary)  # the summary must stay XCom-safe.


def test_season_fields_scope_timeout_is_a_failed_outcome(monkeypatch, tmp_path):
    """#1368 review r1: a scope timeout must not abort the renewal — it is
    one failed outcome, so backoff and the report survive it."""
    import subprocess as subprocess_module

    env = {
        "SOFASCORE_CAMPAIGN_SNAPSHOT": str(tmp_path / "snapshot.json"),
        "SOFASCORE_TOURNAMENT_ID": "17",
        "SOFASCORE_SOURCE_SEASON_ID": "76986",
        "SOFASCORE_EXPECTED_SNAPSHOT_ID": "a" * 64,
        "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c" * 64,
        "SOFASCORE_SCOPE_OUTPUT_DIR": str(tmp_path / "out"),
        "SOFASCORE_SCOPE_RESULT_PATH": str(tmp_path / "result.json"),
        "SOFASCORE_WORKLOAD_ARTIFACT": str(tmp_path / "artifact.json"),
        "SOFASCORE_SCOPE_RUN_ID": "run--17-76986--fields",
        "SOFASCORE_SCOPE_KEY": "c:17:76986",
        "SOFASCORE_SCOPE_TIMEOUT_S": "60",
    }

    def expired(*args, **kwargs):
        raise subprocess_module.TimeoutExpired(cmd="scope", timeout=60)

    monkeypatch.setattr(refresh.subprocess, "run", expired)
    outcome = refresh._execute_season_fields_scope(env)
    assert outcome["exit_code"] is None
    assert outcome["status"] == "timeout"
    assert outcome["scope_key"] == "c:17:76986"
    # The caller treats this as a failure: None != 0 -> backoff applies.
    assert outcome["exit_code"] != 0
