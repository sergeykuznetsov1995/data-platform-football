"""Behavior and recovery tests for #1361; all source/external I/O is mocked."""
from __future__ import annotations

import copy
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from dags.utils import sofascore_pool_wait as pool_wait
from dags.utils import sofascore_red_share as red_share
from deploy.sofascore import pipeline_metrics as metrics
from deploy.sofascore import pipeline_watchdog as watch
from deploy.sofascore.pipeline_issues import GitHubIssues
from scripts.research.replay_sofascore_watchdog import snapshots

NOW = datetime(2026, 10, 8, 5, tzinfo=timezone.utc)
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures/sofascore_watchdog"


def snapshot(now=NOW):
    day = (now.date() - timedelta(days=1)).isoformat()
    return {"observed_at": now.isoformat(), "day": day,
            "coverage": {"day": day, "core_in24": 99, "core_total": 100},
            "red_share": {dag: [0, 20] for dag in watch.LANES.values()},
            "pool_wait": pool_wait.PoolWait(3, 0, 0, 30, 4, 0, 0)._asdict(),
            "lanes": {lane: {"expected_work": False, "closed": False} for lane in watch.LANES}}


def demand(now=NOW, **updates):
    row = {"expected_work": True, "closed": False,
           "demand_since": (now - timedelta(hours=8)).isoformat(),
           "last_progress": (now - timedelta(minutes=10)).isoformat(),
           "last_paid": (now - timedelta(minutes=10)).isoformat()}
    row.update(updates)
    return row


def pause(lane="history", start="2026-10-07T00:00:00Z", end=None):
    return {"lane": lane, "start": start, "end": end,
            "indefinite": end is None, "reason": "controlled test pause", "approval": "test permission"}


def test_canonical_red_share_and_pool_lines():
    sample = snapshot()
    sample["red_share"][red_share.HISTORY_DAG_ID] = [5, 20]
    sample["pool_wait"]["waiting_runs"] = 1
    result = watch.evaluate(sample, [], NOW, {})
    assert red_share.format_line(sample["day"], sample["red_share"]) in result["lines"]
    assert pool_wait.format_line(pool_wait.PoolWait(**sample["pool_wait"])) in result["lines"]
    assert result["rules"]["history:red_share"]["verdict"] == "active"
    assert result["rules"]["refresh:pool_wait"]["verdict"] == "active"


@pytest.mark.parametrize("in24,failed,coverage,red", [(99, 19, "ok", "ok"), (98, 20, "active", "active"), (100, 21, "ok", "active")])
def test_exact_daily_thresholds(in24, failed, coverage, red):
    sample = snapshot()
    sample["coverage"]["core_in24"] = in24
    sample["red_share"][red_share.HISTORY_DAG_ID] = [failed, 100]
    result = watch.evaluate(sample, [], NOW, {})["rules"]
    assert result["refresh:coverage"]["verdict"] == coverage
    assert result["history:red_share"]["verdict"] == red


def test_daily_measurements_deferred_before_0500():
    now = NOW - timedelta(seconds=1)
    sample = snapshot(now)
    sample["coverage"]["core_in24"] = 0
    result = watch.step(sample, [], now, {})
    assert not result["new_incidents"]
    assert result["rules"]["refresh:coverage"]["verdict"] == "deferred"


def test_zero_and_missing_data_do_not_resolve_existing_episode():
    state, sample = {}, snapshot()
    sample["red_share"][red_share.HISTORY_DAG_ID] = [20, 20]
    first = watch.step(sample, [], NOW, state)
    token = first["active_incidents"]["history:red_share"]
    for value in (None, {}, {red_share.HISTORY_DAG_ID: [0, 0]}):
        sample["red_share"] = value
        result = watch.step(sample, [], NOW, state)
        assert result["active_incidents"]["history:red_share"] == token
        assert result["rules"]["history:red_share"]["verdict"] == "unobservable"


@pytest.mark.parametrize("seconds,verdict", [(21599, "ok"), (21600, "active")])
def test_stall_threshold_and_empty_green_reports(seconds, verdict):
    sample = snapshot()
    sample["lanes"]["history"] = demand(demand_since=(NOW - timedelta(seconds=seconds)).isoformat(), last_progress=None, last_paid=None)
    result = watch.evaluate(sample, [], NOW, {})["rules"]
    assert result["history:no_progress"]["verdict"] == verdict
    assert result["history:no_paid"]["verdict"] == verdict


def test_actual_progress_without_paid_requests_is_legitimate_replay():
    sample = snapshot()
    sample["lanes"]["history"] = demand(last_paid=None)
    # Paid-source work must be explicitly distinguished from saved-raw work.
    sample["lanes"]["history"]["source_work_expected"] = False
    result = watch.evaluate(sample, [], NOW, {})["rules"]
    assert result["history:no_progress"]["verdict"] == "ok"
    assert result["history:no_paid"]["verdict"] == "ok"


def test_pause_is_scoped_and_does_not_change_numbers():
    sample = snapshot()
    for lane in watch.LANES:
        sample["lanes"][lane] = demand(last_progress=None, last_paid=None, closed=True)
        sample["red_share"][watch.LANES[lane]] = [20, 20]
    result = watch.evaluate(sample, [pause()], NOW, {})
    assert result["rules"]["history:no_progress"]["verdict"] == "suppressed"
    assert result["rules"]["history:red_share"]["verdict"] == "suppressed"
    assert result["rules"]["refresh:red_share"]["verdict"] == "active"
    assert result["rules"]["refresh:no_progress"]["verdict"] == "active"
    assert "40/40" in result["lines"][0]


def test_partial_daily_pause_cannot_hide_previous_failures():
    sample = snapshot()
    sample["red_share"][red_share.HISTORY_DAG_ID] = [10, 20]
    result = watch.evaluate(sample, [pause(start="2026-10-07T12:00:00Z")], NOW, {})
    assert result["rules"]["history:red_share"]["verdict"] == "active"


def test_resume_and_delivery_reset_silence():
    sample = snapshot()
    sample["lanes"]["history"] = demand(last_progress=None, last_paid=None)
    policy = [pause(end=NOW.isoformat())]
    result = watch.evaluate(sample, policy, NOW, {})
    assert result["rules"]["history:no_progress"]["verdict"] == "ok"
    sample["lanes"]["history"]["delivery_since"] = (NOW - timedelta(hours=2)).isoformat()
    assert watch.evaluate(sample, [], NOW, {})["rules"]["history:no_progress"]["verdict"] == "suppressed"
    sample["lanes"]["history"]["delivery_since"] = (NOW - timedelta(hours=3)).isoformat()
    assert watch.evaluate(sample, [], NOW, {})["rules"]["history:no_progress"]["verdict"] == "active"


def test_stop_requires_fifteen_minutes_and_persists_across_ticks():
    state, sample = {}, snapshot()
    sample["lanes"]["history"] = demand(closed=True)
    assert watch.step(sample, [], NOW, state)["rules"]["history:stop"]["verdict"] == "ok"
    for seconds, verdict in ((899, "ok"), (900, "active")):
        sample["observed_at"] = (NOW + timedelta(seconds=seconds)).isoformat()
        result = watch.step(sample, [], NOW + timedelta(seconds=seconds), state)
        assert result["rules"]["history:stop"]["verdict"] == verdict


@pytest.mark.parametrize("bad", [{"approval": ""}, {"reason": ""}, {"indefinite": False}, {"end": "2026-10-06T00:00:00Z"}])
def test_pause_requires_explicit_permission_and_valid_interval(bad):
    row = pause()
    row.update(bad)
    with pytest.raises(ValueError):
        watch.evaluate(snapshot(), [row], NOW, {})


def test_three_days_of_controlled_healthy_and_paused_data_have_no_false_alarms():
    state = {}
    policy = [pause(start="2026-10-07T00:00:00Z")]
    for tick in range(3 * 24 * 4):
        now = NOW + timedelta(minutes=15 * tick)
        sample = snapshot(now)
        sample["lanes"]["history"] = demand(now, closed=True, last_progress=None, last_paid=None)
        assert not watch.step(sample, policy, now, state)["new_incidents"]


def test_saved_september_degradation_creates_one_continuing_incident():
    state = {}
    results = []
    for sample in snapshots(FIXTURES / "history_scope_daily.tsv"):
        results.append(watch.step(sample, [], watch.timestamp(sample["observed_at"]), state))
    assert all(row["rules"]["history:red_share"]["verdict"] == "active" for row in results[:4])
    assert len(state["incidents"]) == 1
    assert all("refresh:coverage" in row["unobservable"] for row in results)


def test_saved_coverage_uses_measurement_without_reconstructing_denominator():
    verdicts = []
    for line in (FIXTURES / "coverage_daily.jsonl").read_text().splitlines():
        coverage = json.loads(line)
        now = watch.timestamp(coverage["day"] + "T00:00:00Z") + timedelta(days=1, hours=5)
        sample = snapshot(now)
        sample["coverage"] = coverage
        result = watch.evaluate(sample, [], now, {})
        assert coverage["line"] in result["lines"]
        verdicts.append(result["rules"]["refresh:coverage"]["verdict"])
    assert verdicts == ["active", "active", "ok"]


def test_restart_day_change_and_pause_do_not_duplicate_active_incident():
    state, sample = {}, snapshot()
    sample["red_share"][red_share.HISTORY_DAG_ID] = [10, 20]
    first = watch.step(sample, [], NOW, state)["new_incidents"]
    state = json.loads(json.dumps(state))  # process restart
    now = NOW + timedelta(days=1)
    sample.update(day="2026-10-08", observed_at=now.isoformat())
    assert not watch.step(sample, [pause()], now, state)["new_incidents"]
    assert not watch.step(sample, [], now, state)["new_incidents"]
    assert list(state["incidents"]) == first


class Publisher:
    def __init__(self):
        self.created = []
        self.project_calls = 0
        self.timeout_create = False
        self.fail_project = False

    def find(self, marker):
        return next((issue for m, issue in self.created if m == marker), None)

    def create(self, incident):
        issue = {"number": len(self.created) + 1, "node_id": "node-1"}
        self.created.append((incident["marker"], issue))
        if self.timeout_create:
            raise RuntimeError("ambiguous response after creation")
        return issue

    def add_project(self, issue):
        self.project_calls += 1
        if self.fail_project:
            raise RuntimeError("project unavailable")


def incident_state():
    state, sample = {}, snapshot()
    sample["red_share"][red_share.HISTORY_DAG_ID] = [10, 20]
    watch.step(sample, [], NOW, state)
    return state


def test_ambiguous_create_is_reconciled_after_restart():
    state, publisher = incident_state(), Publisher()
    checkpoints = []
    publisher.timeout_create = True
    assert watch.publish(state, publisher, lambda: checkpoints.append(copy.deepcopy(state)))
    state = json.loads(json.dumps(state))
    assert not watch.publish(state, publisher, lambda: None)
    assert len(publisher.created) == 1
    assert publisher.project_calls == 1


def test_project_retry_and_manually_closed_issue_never_create_second_issue():
    state, publisher = incident_state(), Publisher()
    publisher.fail_project = True
    assert watch.publish(state, publisher, lambda: None)
    publisher.fail_project = False
    assert not watch.publish(state, publisher, lambda: None)
    assert not watch.publish(state, publisher, lambda: None)
    assert len(publisher.created) == 1
    assert publisher.project_calls == 2


def test_pending_event_survives_recovery():
    state = incident_state()
    watch.step(snapshot(), [], NOW, state)
    publisher = Publisher()
    assert not watch.publish(state, publisher, lambda: None)
    assert len(publisher.created) == 1
    assert not state["episodes"]


def test_github_lookup_checks_closed_issues_without_search_index(monkeypatch):
    seen = []
    def gh(*args, **kwargs):
        seen.append(args)
        return json.dumps([[{"number": 9, "node_id": "node-9", "body": "marker", "state": "closed"}]])
    monkeypatch.setattr(GitHubIssues, "gh", gh)
    assert GitHubIssues().find("marker") == {"number": 9, "node_id": "node-9"}
    assert "--paginate" in seen[0] and "state=all" in seen[0][-1]


def test_github_timeout_is_retryable(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("gh", 60)
    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(RuntimeError, match="reconcile"):
        GitHubIssues.gh("api")


def test_replay_never_calls_network_collectors_or_publishers(tmp_path, monkeypatch):
    path = tmp_path / "snapshots.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in snapshots(FIXTURES / "history_scope_daily.tsv")))
    def forbidden(*args, **kwargs):
        raise AssertionError("external I/O in replay")
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(metrics, "collect", forbidden)
    assert watch.main(["--replay", str(path), "--state", str(tmp_path / "state.json")]) == 0
    with pytest.raises(SystemExit):
        watch.main(["--replay", str(path), "--state", str(tmp_path / "state.json"), "--publish-issues"])


@pytest.mark.parametrize("kind", ["stale", "future", "out_of_order"])
def test_invalid_observations_are_rejected(kind):
    sample, state = snapshot(), {}
    if kind == "out_of_order":
        state["last_observed_at"] = (NOW + timedelta(seconds=1)).isoformat()
    else:
        sample["observed_at"] = (NOW + timedelta(seconds=1) if kind == "future" else NOW - timedelta(minutes=31)).isoformat()
    with pytest.raises(ValueError):
        watch.step(sample, [], NOW, state)


def test_collector_adapter_failures_are_unknown_and_do_not_mask_independent_metrics(tmp_path):
    path = tmp_path / "daily.jsonl"
    path.write_text((FIXTURES / "coverage_daily.jsonl").read_text())
    def unavailable(sql):
        raise RuntimeError("metabase unavailable")
    sample = metrics.collect(tmp_path, path, NOW, query=unavailable)
    result = watch.evaluate(sample, [], NOW, {})
    assert result["rules"]["refresh:coverage"]["verdict"] == "ok"
    assert result["rules"]["history:red_share"]["verdict"] == "unobservable"
    assert "red_share" in sample["collection_errors"]


def test_readonly_queries_and_pool_metric_reuse(monkeypatch):
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "0|10\n", "")
    monkeypatch.setattr(subprocess, "run", run)
    assert metrics.psql("SELECT 1") == "0|10"
    assert "BEGIN READ ONLY" in calls[0][-1]
    assert "SET LOCAL statement_timeout" in calls[0][-1]


def test_report_metrics_deduplicate_and_ignore_incomplete_capture_status(tmp_path):
    directory = tmp_path / "results"
    directory.mkdir()
    import os
    for name, complete in (("a", 0), ("b", 4)):
        path = directory / (name + ".json")
        path.write_text(json.dumps({"run_id": "same-run", "scope_digest": "same-scope", "status": "success"}))
        child = directory / name
        child.mkdir()
        (child / "matches.json").write_text(json.dumps({"capture_status_rows": 100, "matches_complete": complete,
                                                       "traffic": {"request_count": 0}}))
        at = NOW - timedelta(days=1, minutes=5 if name == "a" else 1)
        os.utime(path, (at.timestamp(), at.timestamp()))
    row = metrics.report_metrics(tmp_path, NOW, "2026-10-07")
    assert row["daily_closed"] == 4
    assert row["last_paid"] is None
    assert row["last_progress"] is not None


def test_standalone_host_replay_needs_only_standard_library(tmp_path):
    import sys
    root = FIXTURES.parents[2]
    result = subprocess.run([sys.executable, "-S", "-B", str(root / "scripts/research/replay_sofascore_watchdog.py")],
                            cwd=tmp_path, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    final = json.loads(result.stdout.splitlines()[-1])
    assert final["incident_count"] == 1
    assert final["source_requests"] == final["publication_calls"] == 0


def test_collect_detects_paused_refresh_without_scope_tis(tmp_path):
    def query(sql):
        if "json_build_object" in sql:
            return json.dumps({"closed": True, "demand_since": None, "last_run_end": None,
                               "last_run_start": "2026-10-07T08:30:00+00:00"})
        raise RuntimeError("unused metric missing")
    sample = metrics.collect(tmp_path, tmp_path / "absent", NOW, query=query)
    assert sample["lanes"]["refresh"]["expected_work"] is True
    assert sample["lanes"]["refresh"]["demand_since"] == "2026-10-08T00:30:00+00:00"


def test_collect_does_not_call_quiet_interval_a_missed_refresh(tmp_path):
    def query(sql):
        if "json_build_object" in sql:
            return json.dumps({"closed": False, "demand_since": None, "last_run_end": NOW.isoformat(),
                               "last_run_start": "2026-10-08T00:30:00+00:00"})
        raise RuntimeError("unused metric missing")
    sample = metrics.collect(tmp_path, tmp_path / "absent", NOW, query=query)
    assert sample["lanes"]["refresh"]["expected_work"] is False


def test_delivery_end_gets_a_new_silence_grace_period():
    state, sample = {}, snapshot()
    sample["lanes"]["history"] = demand(last_progress=None, last_paid=None,
                                        delivery_since=(NOW - timedelta(minutes=10)).isoformat())
    watch.step(sample, [], NOW, state)
    now = NOW + timedelta(minutes=15)
    sample["observed_at"] = now.isoformat()
    sample["lanes"]["history"].pop("delivery_since")
    assert watch.step(sample, [], now, state)["rules"]["history:no_progress"]["verdict"] == "ok"


@pytest.mark.parametrize("excuse", ["pause", "delivery"])
def test_grace_does_not_resolve_an_existing_incident_without_recovery(excuse):
    state, sample = {}, snapshot()
    sample["lanes"]["history"] = demand(last_progress=None, last_paid=None, closed=True)
    watch.step(sample, [], NOW, state)
    now = NOW + timedelta(minutes=15)
    sample["observed_at"] = now.isoformat()
    watch.step(sample, [], now, state)
    original = dict(state["episodes"])
    assert len(original) == 3
    now = NOW + timedelta(minutes=30)
    sample["observed_at"] = now.isoformat()
    policy = [pause(start=(NOW + timedelta(minutes=20)).isoformat(), end=(NOW + timedelta(hours=1)).isoformat())] if excuse == "pause" else []
    if excuse == "delivery":
        sample["lanes"]["history"]["delivery_since"] = now.isoformat()
    watch.step(sample, policy, now, state)
    now = NOW + timedelta(hours=1, minutes=15)
    sample["observed_at"] = now.isoformat()
    sample["lanes"]["history"].pop("delivery_since", None)
    watch.step(sample, policy, now, state)
    assert state["episodes"] == original
    now += timedelta(hours=6)
    sample["observed_at"] = now.isoformat()
    watch.step(sample, policy, now, state)
    assert state["episodes"] == original
    assert len(state["incidents"]) == 3


def test_paid_requests_include_season_phase_without_double_counting_matches(tmp_path):
    import os
    directory = tmp_path / "results"
    directory.mkdir()
    path = directory / "scope.json"
    path.write_text(json.dumps({"run_id": "run", "scope_digest": "scope", "status": "success",
                               "phases": [{"phase": "season", "request_count": 10}, {"phase": "matches", "request_count": 0}]}))
    child = directory / "scope"
    child.mkdir()
    (child / "matches.json").write_text(json.dumps({"matches_complete": 1, "traffic": {"request_count": 0}}))
    os.utime(path, ((NOW - timedelta(minutes=1)).timestamp(),) * 2)
    row = metrics.report_metrics(tmp_path, NOW, "2026-10-07")
    assert row["last_paid"] is not None


def test_new_unknown_report_does_not_establish_silence(tmp_path):
    import os
    directory = tmp_path / "results"
    directory.mkdir()
    for name, ago in (("known", 8), ("unknown", 1)):
        path = directory / (name + ".json")
        path.write_text(json.dumps({"run_id": name, "scope_digest": "scope", "status": "success"}))
        os.utime(path, ((NOW - timedelta(hours=ago)).timestamp(),) * 2)
        if name == "known":
            child = directory / name
            child.mkdir()
            (child / "matches.json").write_text(json.dumps({"matches_complete": 1, "traffic": {"request_count": 1}}))
    row = metrics.report_metrics(tmp_path, NOW, "2026-10-08")
    assert "daily_closed" not in row
    assert "last_progress" not in row
    assert "last_paid" not in row
    sample = snapshot()
    sample["lanes"]["history"] = demand()
    sample["lanes"]["history"].pop("last_progress")
    sample["lanes"]["history"].pop("last_paid")
    sample["lanes"]["history"].update(row)
    rules = watch.step(sample, [], NOW, {})["rules"]
    assert rules["history:no_progress"]["verdict"] == "unobservable"
    assert rules["history:no_paid"]["verdict"] == "unobservable"


def test_pending_server_post_is_not_repeated_while_marker_is_absent():
    class DelayedPublisher(Publisher):
        def create(self, incident):
            self.pending_marker = incident["marker"]
            self.calls = getattr(self, "calls", 0) + 1
            raise RuntimeError("server POST still pending")
    publisher, state = DelayedPublisher(), incident_state()
    assert watch.publish(state, publisher, lambda: None)
    state = json.loads(json.dumps(state))
    assert watch.publish(state, publisher, lambda: None)
    assert publisher.calls == 1
    publisher.created.append((publisher.pending_marker, {"number": 1, "node_id": "node-1"}))
    assert not watch.publish(state, publisher, lambda: None)
    assert publisher.calls == 1
