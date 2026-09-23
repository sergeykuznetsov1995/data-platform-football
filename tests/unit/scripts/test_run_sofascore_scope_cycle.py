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
        "errors": [],
    }
    assert planner.call_args.kwargs["phase"] == "targets"
    assert planner.call_args.kwargs["season_evidence"] == "bronze"


def _plan_double(**kwargs):
    return kwargs["output_path"]


@pytest.mark.unit
def test_cycle_reports_why_a_phase_failed(tmp_path, monkeypatch):
    """#1260: every red attempt must say in results/<hash>.json why it is red.

    The message the capture runner already wrote to the phase report was
    dropped on the boundary between the phase and the cycle result."""
    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        tmp_path / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *a, **k: _scope())
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)
    message = (
        "match capture has nonterminal endpoint states; refusing to publish "
        "incomplete normalized data: 3 of 3 matches incomplete; "
        "statistics=rate_limited x3"
    )
    status_counts = {"success": 12, "retryable_failure": 3}

    def run_capture(argv):
        output = Path(argv[argv.index("--output") + 1])
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.name == "season.json":
            output.write_text(json.dumps({"errors": [], "traffic": {}}))
            return 0
        output.write_text(json.dumps({
            "errors": [message],
            "traffic": {
                "status_counts": status_counts,
                "endpoints": 15,
                "request_count": 0,
                "replay_hits": 15,
            },
        }))
        return 1

    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.prepare_workload_plan",
            side_effect=_plan_double,
        ),
        patch("dags.scripts.run_sofascore_scraper.main", side_effect=run_capture),
    ):
        assert cycle.main(_cycle_argv(tmp_path, "--phase", "all")) == 1

    result = json.loads((tmp_path / "result.json").read_text())
    assert result["status"] == "failed"
    assert result["errors"] == [f"matches: {message}"]
    assert result["phases"][1]["status_counts"] == status_counts
    assert result["phases"][1]["request_count"] == 0
    assert result["phases"][1]["replay_hits"] == 15
    assert result["status_counts"] == status_counts


def _control_channel_failure_text():
    """The exact error a season phase leaves after the gateway control channel
    stayed down (#1349): the real client's typed failure, wrapped by the live
    transport and carried through the engine's attempt log."""
    import requests
    from types import SimpleNamespace
    from urllib3.exceptions import MaxRetryError, NewConnectionError

    from scrapers.sofascore.capture_engine import (
        TransportError,
        _append_attempt_log,
        _attempt_entry,
    )
    from scrapers.sofascore.lease_client import (
        SofascoreLeaseClient,
        SofascoreLeaseControlUnavailable,
    )

    class _RefusingSession:
        trust_env = False

        def request(self, *args, **kwargs):
            raise requests.exceptions.ConnectionError(
                MaxRetryError(
                    None,
                    "/v1/leases/lease-1/stats",
                    NewConnectionError(None, "[Errno 111] Connection refused"),
                )
            )

    client = SofascoreLeaseClient(
        "http://sofascore_gw_history:8899",
        session=_RefusingSession(),
        control_token="c" * 32,
        sleep=lambda _s: None,
    )
    try:
        client.stats(SimpleNamespace(lease_id="lease-1", token="t" * 16))
    except SofascoreLeaseControlUnavailable as channel:
        first = TransportError(
            "SofaScore control channel failed before fetch for "
            f"/api/v1/unique-tournament/17/season/76986/events/last/0: {channel}",
            provider_bytes=0,
            source_requests=0,
            control_channel_failure=True,
        )
    last = TransportError("warmed SofaScore request failed", provider_bytes=0)
    log = [_attempt_entry(1, first), _attempt_entry(2, last)]
    _append_attempt_log(last, log)
    return (
        "capture_engine: SofaScore endpoint did not reach a publishable state: "
        "17:76986:season:76986:events_last_0:final status=retryable_failure "
        f"error=TransportError: {last}"
    )


@pytest.mark.unit
def test_cycle_result_names_a_control_channel_failure(tmp_path, monkeypatch):
    """#1349: the red-run classifier must see the control channel, not
    "budget exhausted", in results/<hash>.json errors[]."""
    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        tmp_path / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *a, **k: _scope())
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)
    message = _control_channel_failure_text()

    def run_capture(argv):
        output = Path(argv[argv.index("--output") + 1])
        output.parent.mkdir(parents=True, exist_ok=True)
        # Contract of the runner's hard failure: the reason, zero paid
        # traffic, no completed rows.
        output.write_text(json.dumps({
            "errors": [message],
            "traffic": {
                "status_counts": {"retryable_failure": 1},
                "endpoints": 1,
                "request_count": 2,
                "source_request_count": 0,
                "paid_proxy_bytes": 0,
                "control_channel_failures": 1,
                "accounting_uncertain": 0,
            },
        }))
        return 1

    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.prepare_workload_plan",
            side_effect=_plan_double,
        ),
        patch("dags.scripts.run_sofascore_scraper.main", side_effect=run_capture),
    ):
        assert cycle.main(_cycle_argv(tmp_path, "--phase", "all")) == 1

    result = json.loads((tmp_path / "result.json").read_text())
    assert result["status"] == "failed"
    [error] = result["errors"]
    assert error.startswith("season: capture_engine: ")
    assert "control channel failure (GET /v1/leases/lease-1/stats, 4 attempts" in error
    assert "attempts: [attempt 1: TransportError: SofaScore control channel" in error
    assert "budget exhausted" not in error
    season = result["phases"][0]
    assert season["control_channel_failures"] == 1
    assert season["accounting_uncertain"] == 0
    assert season["source_request_count"] == 0


@pytest.mark.unit
def test_cycle_names_a_phase_that_left_no_report(tmp_path, monkeypatch):
    """#1260: a phase killed before it wrote its report still owes a reason."""
    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        tmp_path / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *a, **k: _scope())
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)

    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.prepare_workload_plan",
            side_effect=_plan_double,
        ),
        patch("dags.scripts.run_sofascore_scraper.main", return_value=1),
    ):
        assert cycle.main(_cycle_argv(tmp_path, "--phase", "matches")) == 1

    result = json.loads((tmp_path / "result.json").read_text())
    assert result["errors"] == ["matches: exit_code=1, phase report missing"]


@pytest.mark.unit
def test_cycle_ignores_a_report_left_by_the_previous_try(tmp_path, monkeypatch):
    """#1260: an Airflow retry reuses the scope directory, so a report from the
    previous try must not be served as this try's reason."""
    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        tmp_path / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *a, **k: _scope())
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)
    stale = tmp_path / "run" / "matches.json"
    stale.parent.mkdir(parents=True)
    stale.write_text(json.dumps({
        "errors": ["stale reason from the previous try"],
        "traffic": {"status_counts": {"success": 99}},
    }))

    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.prepare_workload_plan",
            side_effect=_plan_double,
        ),
        patch("dags.scripts.run_sofascore_scraper.main", return_value=1),
    ):
        assert cycle.main(_cycle_argv(tmp_path, "--phase", "matches")) == 1

    result = json.loads((tmp_path / "result.json").read_text())
    assert result["errors"] == ["matches: exit_code=1, phase report missing"]
    assert "status_counts" not in result


@pytest.mark.unit
def test_cycle_keeps_the_stage_of_a_prefinalize_snapshot(tmp_path, monkeypatch):
    """#1260: counters taken before finalize must stay labelled as such."""
    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        tmp_path / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *a, **k: _scope())
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)

    def run_capture(argv):
        output = Path(argv[argv.index("--output") + 1])
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({
            "errors": ["match capture scrape failed hard: boom"],
            "traffic": {
                "status_counts": {"retryable_failure": 7},
                "status_counts_stage": "pre_finalize",
            },
        }))
        return 1

    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.prepare_workload_plan",
            side_effect=_plan_double,
        ),
        patch("dags.scripts.run_sofascore_scraper.main", side_effect=run_capture),
    ):
        assert cycle.main(_cycle_argv(tmp_path, "--phase", "matches")) == 1

    result = json.loads((tmp_path / "result.json").read_text())
    assert result["phases"][0]["status_counts_stage"] == "pre_finalize"
    assert result["status_counts_stage"] == "pre_finalize"
    assert result["status_counts"] == {"retryable_failure": 7}


@pytest.mark.unit
def test_cycle_result_carries_player_universe_gaps(tmp_path, monkeypatch):
    """#1351: a season published with player-universe gaps is green; the gap
    count must still reach results/<hash>.json for the watchdog."""
    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        tmp_path / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *a, **k: _scope())
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)

    def run_capture(argv):
        output = Path(argv[argv.index("--output") + 1])
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({
            "errors": [],
            "player_universe_gaps": [
                "participants omitted scheduled team ids: 44"
            ],
            "traffic": {"request_count": 0, "player_universe_gaps": 1},
        }))
        return 0

    with (
        patch(
            "dags.scripts.prepare_sofascore_workload.prepare_workload_plan",
            side_effect=_plan_double,
        ),
        patch("dags.scripts.run_sofascore_scraper.main", side_effect=run_capture),
    ):
        assert cycle.main(_cycle_argv(tmp_path, "--phase", "season")) == 0

    result = json.loads((tmp_path / "result.json").read_text())
    assert result["status"] == "success"
    assert result["phases"][0]["player_universe_gaps"] == 1


@pytest.mark.unit
def test_runner_fail_closed_before_capture_reaches_quarantine_as_zero_requests(
    tmp_path, monkeypatch
):
    """#1351 (Astra r2): the chain «runner fails closed before any capture →
    cycle result → finalize → quarantine». The runner's fail-closed reports
    said ``requests: 0`` only, the cycle result carried no request counter,
    and the scope's identical free failures never built a streak."""
    from dags.utils import sofascore_all_mens_state as state

    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        tmp_path / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(cycle, "load_exact_scope", lambda *a, **k: _scope())
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)
    # The real runner entrypoint; no registry overlay / signed plan exists,
    # so it fails closed (activation guard) before any capture runtime.
    with patch(
        "dags.scripts.prepare_sofascore_workload.prepare_workload_plan",
        side_effect=_plan_double,
    ):
        assert cycle.main(_cycle_argv(tmp_path, "--phase", "season")) == 1

    result_path = tmp_path / "result.json"
    result = json.loads(result_path.read_text())
    assert result["errors"][0].startswith("season: activation_guard: ")
    assert result["phases"][0]["source_request_count"] == 0

    reason, source_requests = state.read_scope_outcome(result_path)
    assert source_requests == 0
    failures = tmp_path / "failures.json"
    for run_id in ("r1", "r2", "r3"):
        state.mark_failed(
            failures, campaign_id="c", scope_key="c:17:76986", run_id=run_id,
            reason=reason, source_requests=source_requests, release="aaaaaaaa",
        )
    record = state.read_failures(failures, campaign_id="c")["c:17:76986"]
    assert state.is_quarantined_record(record, 3)


@pytest.mark.unit
def test_failed_plan_preparation_is_a_zero_request_phase_that_quarantines(
    tmp_path, monkeypatch
):
    """#1351 (Astra r3): ``prepare_workload_plan`` failing on the scope's
    stored state (schema-rejected raw, ``max_pages``) before the runner left
    the cycle result with ``phases=[]``: the attempt read as «unknown
    traffic», never built a quarantine streak and came back from the park
    forever. The failure is a phase with its reason and 0 source requests
    (preparation never talks to the source), so three identical ones
    quarantine the scope — through the real preparation code."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from dags.utils import sofascore_all_mens_state as state
    from scrapers.sofascore.season_pipeline import SeasonPlanningError
    from scrapers.sofascore.workload_plan import WorkloadBudgetPolicy

    # The checked-in medallion config stands in for the rendered overlay.
    paths = cycle.ScopeOverlayPaths(
        tmp_path / "tournaments.json",
        Path(__file__).resolve().parents[3]
        / "configs" / "medallion" / "competitions.yaml",
    )
    monkeypatch.setattr(
        cycle,
        "load_exact_scope",
        lambda *a, **k: {**_scope(), "capture_key": "ENG-Premier League"},
    )
    monkeypatch.setattr(cycle, "render_scope_overlays", lambda *a, **k: paths)
    monkeypatch.setenv("SOFASCORE_PROXY_BUDGET_ARTIFACT_ID", "b" * 64)
    catalog = MagicMock()
    catalog.competition.return_value = SimpleNamespace(
        capture_allowed=True, unique_tournament_id=17
    )
    catalog.resolve_source_season.return_value = SimpleNamespace(
        season_id=76986, format="split_year"
    )
    runner = MagicMock(side_effect=AssertionError("runner must not start"))
    broken = SeasonPlanningError(
        "stored schedule_last raw failed schema validation"
    )
    result_path = tmp_path / "result.json"
    failures = tmp_path / "failures.json"

    for run_id in ("r1", "r2", "r3"):
        with (
            patch(
                "dags.scripts.prepare_sofascore_workload.load_static_workload_policy",
                return_value=WorkloadBudgetPolicy("b" * 64, {}),
            ),
            patch(
                "dags.scripts.prepare_sofascore_workload.build_capture_runtime",
                return_value=SimpleNamespace(
                    raw_store=MagicMock(), manifest_store=MagicMock()
                ),
            ),
            patch(
                "dags.scripts.prepare_sofascore_workload.SofaScoreCatalog.load",
                return_value=catalog,
            ),
            patch(
                "dags.scripts.prepare_sofascore_workload.plan_season_partition",
                side_effect=broken,
            ),
            patch("dags.scripts.run_sofascore_scraper.main", runner),
        ):
            assert cycle.main(_cycle_argv(tmp_path, "--phase", "season")) == 1

        result = json.loads(result_path.read_text())
        assert result["status"] == "failed"
        [phase] = result["phases"]
        assert phase["phase"] == "season"
        assert phase["status"] == "failed"
        assert phase["source_request_count"] == 0
        assert result["errors"][0].startswith(
            "season: workload_plan_prepare: RuntimeError: every SofaScore "
            "partition was dropped from the season plan"
        )
        reason, source_requests = state.read_scope_outcome(result_path)
        assert source_requests == 0
        state.mark_failed(
            failures, campaign_id="c", scope_key="c:17:76986", run_id=run_id,
            reason=reason, source_requests=source_requests, release="aaaaaaaa",
        )

    runner.assert_not_called()
    record = state.read_failures(failures, campaign_id="c")["c:17:76986"]
    assert record["streak_no_traffic"] == 3
    assert state.is_quarantined_record(record, 3)
