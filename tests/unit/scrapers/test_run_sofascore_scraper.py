"""Fail-closed tests for the canonical SofaScore runner."""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def _runner_module():
    from dags.scripts import run_sofascore_scraper

    return run_sofascore_scraper


@pytest.mark.unit
def test_argparse_unknown_flag_is_a_hard_failure():
    runner = _runner_module()

    assert runner.main(["--bogus-flag", "x"]) == 1


@pytest.mark.unit
def test_live_cli_rejects_local_json_manifest_before_any_side_effect():
    runner = _runner_module()

    with patch(
        "scrapers.sofascore.catalog.SofaScoreCatalog.load",
        side_effect=AssertionError("catalog must not be opened"),
    ):
        assert runner.main(["--manifest-backend", "json"]) == 1


@pytest.mark.unit
def test_complete_season_noop_reports_terminal_endpoint_accounting(tmp_path, monkeypatch):
    runner = _runner_module()
    output = tmp_path / "season.json"
    plan = SimpleNamespace(
        specs=tuple(range(33)),
        pending_keys=(),
        schedule_event_ids=tuple(range(324)),
        team_ids=tuple(range(20)),
        referee_ids=tuple(range(17)),
        complete=True,
        finished_event_ids=tuple(range(321)),
        player_universe_evidence_gaps=(),
    )
    monkeypatch.setattr(
        "scrapers.sofascore.season_pipeline.plan_season_partition",
        lambda *args, **kwargs: plan,
    )
    monkeypatch.setattr(runner, "_source_context", lambda *args: (38, 77040))
    monkeypatch.setattr(runner, "_capture_identity", lambda *args: {})
    monkeypatch.setattr("scrapers.sofascore.scraper._season_label", lambda _league, _season: "2526")
    monkeypatch.setattr("utils.proxy_traffic.record_traffic_run", lambda **kwargs: None)

    rc = runner._run_season_capture_engine(
        ["SS-38"],
        2025,
        str(output),
        force_replace=False,
        entity="all",
        capture_runtime=MagicMock(),
        offline_replay=False,
    )

    assert rc == 0
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["planned_endpoints"] == 33
    assert result["terminal_endpoints"] == 33
    assert result["rejected_endpoints"] == 0
    assert result["full_complete"] is True
    assert result["traffic"]["request_count"] == 0


@pytest.mark.unit
@pytest.mark.parametrize("entity", ["all", "match_capture", "player_capture"])
def test_out_of_window_tournament_is_clean_noop_before_plan_or_runtime(
    tmp_path, monkeypatch, entity
):
    runner = _runner_module()
    output = tmp_path / f"{entity}.json"
    catalog = SimpleNamespace(
        competition=lambda _league: SimpleNamespace(
            enabled=True,
            capture_allowed=True,
            activation_eligibility=SimpleNamespace(reasons=()),
        )
    )
    monkeypatch.setattr(
        "scrapers.sofascore.catalog.SofaScoreCatalog.load",
        lambda: catalog,
    )
    # Resolve the live module object explicitly. Another runner test restores
    # ``sys.modules`` wholesale and can leave the parent-package attribute
    # pointing at a stale module instance, which makes string-path patching
    # order-dependent in the full scraper partition.
    medallion_config = importlib.import_module("utils.medallion_config")
    monkeypatch.setattr(
        medallion_config,
        "is_single_year_competition",
        lambda _league: True,
    )
    monkeypatch.setattr(
        medallion_config,
        "get_active_season",
        lambda _league: None,
    )
    runtime = MagicMock(side_effect=AssertionError("runtime must not be built"))
    monkeypatch.setattr("scrapers.sofascore.pipeline.build_capture_runtime", runtime)

    rc = runner.main(
        [
            "--entity",
            entity,
            "--league",
            "INT-World Cup",
            "--season",
            "2026",
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    assert json.loads(output.read_text(encoding="utf-8"))["skipped"] == (
        "out_of_window"
    )
    runtime.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "args"),
    [
        (
            "_run_match_capture",
            {
                "leagues": ["ENG-Premier League"],
                "season": 2025,
                "limit": None,
                "output_path": "/tmp/never-written.json",
            },
        ),
        (
            "_run_player_capture",
            {
                "leagues": ["ENG-Premier League"],
                "season": 2025,
                "limit": None,
                "output_path": "/tmp/never-written.json",
            },
        ),
        (
            "_run_legacy",
            {
                "leagues": ["ENG-Premier League"],
                "season": 2025,
                "output_path": "/tmp/never-written.json",
            },
        ),
    ],
)
def test_direct_entrypoint_requires_runtime_and_plan_before_side_effects(
    name,
    args,
):
    """A direct import cannot resurrect the removed standalone proxy path."""
    runner = _runner_module()
    trino = MagicMock(side_effect=AssertionError("Trino was opened"))
    browser = MagicMock(side_effect=AssertionError("browser was opened"))

    with (
        patch.object(runner, "_trino_connect", trino),
        patch("scrapers.sofascore.SofaScoreScraper", browser),
        pytest.raises(TypeError),
    ):
        getattr(runner, name)(**args)

    trino.assert_not_called()
    browser.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "args"),
    [
        (
            "_run_match_capture",
            {
                "leagues": ["ENG-Premier League"],
                "season": 2025,
                "limit": None,
                "output_path": "/tmp/never-written.json",
                "capture_runtime": MagicMock(),
            },
        ),
        (
            "_run_player_capture",
            {
                "leagues": ["ENG-Premier League"],
                "season": 2025,
                "limit": None,
                "output_path": "/tmp/never-written.json",
                "capture_runtime": MagicMock(),
            },
        ),
        (
            "_run_legacy",
            {
                "leagues": ["ENG-Premier League"],
                "season": 2025,
                "output_path": "/tmp/never-written.json",
                "capture_runtime": MagicMock(),
            },
        ),
    ],
)
def test_direct_entrypoint_requires_explicit_workload_plan(name, args):
    runner = _runner_module()

    with pytest.raises(TypeError):
        getattr(runner, name)(**args)


class TestBronzeMatchResolver:
    @pytest.mark.unit
    def test_filters_by_finished_status_not_live_score(self, monkeypatch):
        runner = _runner_module()
        cursor = MagicMock()
        cursor.fetchall.return_value = [("101",)]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        monkeypatch.setattr(runner, "_trino_connect", lambda: connection)

        assert runner._resolve_match_ids_from_bronze(
            "ENG-Premier League",
            "2526",
            None,
        ) == ["101"]
        sql = cursor.execute.call_args[0][0]
        assert "status_type = 'finished'" in sql
        assert "home_score" not in sql
        assert "COALESCE" not in sql

    @pytest.mark.unit
    def test_operational_error_fails_closed(self, monkeypatch):
        runner = _runner_module()
        connection = MagicMock()
        connection.cursor.return_value.execute.side_effect = RuntimeError(
            "connection reset"
        )
        monkeypatch.setattr(runner, "_trino_connect", lambda: connection)

        with pytest.raises(RuntimeError, match="schedule match-id probe failed"):
            runner._resolve_match_ids_from_bronze(
                "ENG-Premier League",
                "2526",
                None,
            )


def test_match_runner_retains_verified_publication_and_native_identity(tmp_path, monkeypatch):
    from tests.unit.scrapers.test_sofascore_pipeline import _match_batch_runtime, _run_match_pass

    # The existing helper's final telemetry must never open a real Trino connection.
    import importlib
    telemetry = importlib.import_module("utils.proxy_traffic")
    monkeypatch.setattr(telemetry, "record_traffic_run", lambda *a, **kw: True)
    monkeypatch.setattr(telemetry, "log_traffic_summary", lambda *a, **kw: None)
    monkeypatch.setenv("SOFASCORE_REFRESH_EVIDENCE_DIR", str(tmp_path / "evidence"))
    monkeypatch.setenv("SOFASCORE_EVIDENCE_CODE_REVISION", "a" * 40)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "tail__20261015T002000")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_ingest_sofascore")
    monkeypatch.setenv("AIRFLOW_CTX_EXECUTION_DATE", "2026-10-15T00:20:00Z")
    monkeypatch.setenv("AIRFLOW_CTX_TRY_NUMBER", "1")
    runtime, transport, _ = _match_batch_runtime(tmp_path, ["14023925"], None)
    rc, report, saved, _ = _run_match_pass(runtime, ["14023925"], tmp_path / "report.json", monkeypatch)
    assert rc == 0 and transport.calls == 0
    assert report["manifest_committed"] is True
    assert report["evidence_identity"]["run_id"] == "tail__20261015T002000"
    assert report["evidence_identity"]["source_tournament_id"] == 17
    assert report["evidence_identity"]["source_season_id"] == 76986
    assert {r["table"] for r in report["publication_receipts"]} == set(saved)
    for receipt in report["publication_receipts"]:
        assert receipt["rows"] == len(saved[receipt["table"]]["df"])
    copies = list((tmp_path / "evidence" / "daily-tail").rglob("*.json"))
    assert len(copies) == 1
    assert json.loads(copies[0].read_text())["publication_receipts"] == report["publication_receipts"]


def test_failed_save_is_never_recorded_as_a_publication(tmp_path, monkeypatch):
    runner = _runner_module()
    scraper = SimpleNamespace(save_to_iceberg=MagicMock(side_effect=RuntimeError("merge refused")))
    report = dict(publication_receipts=[])
    with pytest.raises(RuntimeError, match="merge refused"):
        runner._save_publication(scraper, report, df=[1], table_name="sofascore_schedule")
    assert report["publication_receipts"] == []
    assert "manifest_committed" not in report


def test_partial_save_failure_retains_only_real_receipts_and_no_commit_marker(tmp_path, monkeypatch):
    from tests.unit.scrapers.test_sofascore_pipeline import _match_batch_runtime, _run_match_pass
    telemetry = importlib.import_module("utils.proxy_traffic")
    monkeypatch.setattr(telemetry, "record_traffic_run", lambda *a, **kw: True)
    monkeypatch.setattr(telemetry, "log_traffic_summary", lambda *a, **kw: None)
    monkeypatch.setenv("SOFASCORE_REFRESH_EVIDENCE_DIR", str(tmp_path / "evidence"))
    monkeypatch.setenv("SOFASCORE_EVIDENCE_CODE_REVISION", "a" * 40)
    runner = _runner_module()
    save = runner._save_publication
    calls = []
    def refuse_third(scraper, report, **kwargs):
        calls.append(kwargs["table_name"])
        if len(calls) == 3:
            raise RuntimeError("third merge refused")
        return save(scraper, report, **kwargs)
    monkeypatch.setattr(runner, "_save_publication", refuse_third)
    runtime, transport, _ = _match_batch_runtime(tmp_path, ["14023925"], None)
    rc, report, saved, _ = _run_match_pass(runtime, ["14023925"], tmp_path / "report.json", monkeypatch)
    assert rc == 1 and transport.calls == 0
    assert len(report["publication_receipts"]) == 2
    assert {r["table"] for r in report["publication_receipts"]} == set(saved)
    assert report.get("manifest_committed") is not True
    assert report["errors"] == ["third merge refused"]
    copies = list((tmp_path / "evidence" / "daily-tail").rglob("*.json"))
    assert len(copies) == 1
    assert json.loads(copies[0].read_text())["errors"] == ["third merge refused"]


def test_season_runner_receipts_are_after_merges_and_noop_does_not_claim_publication(tmp_path, monkeypatch):
    from contextlib import nullcontext
    from tests.unit.scrapers.test_sofascore_season_pipeline import (
        test_runner_offline_season_replay_merges_then_noops_without_browser,
    )
    telemetry = importlib.import_module("utils.proxy_traffic")
    monkeypatch.setattr(telemetry, "record_traffic_run", lambda *a, **kw: True)
    monkeypatch.setattr(telemetry, "log_traffic_summary", lambda *a, **kw: None)
    writer_lock = importlib.import_module("scrapers.sofascore.writer_lock")
    monkeypatch.setattr(writer_lock, "bronze_writer_lock", nullcontext)
    monkeypatch.setenv("SOFASCORE_REFRESH_EVIDENCE_DIR", str(tmp_path / "evidence"))
    monkeypatch.setenv("SOFASCORE_EVIDENCE_CODE_REVISION", "a" * 40)
    test_runner_offline_season_replay_merges_then_noops_without_browser(
        tmp_path, monkeypatch, has_finished_matches=True,
    )
    report = json.loads((tmp_path / "season-offline.json").read_text())
    assert report["manifest_committed"] is True
    assert report["evidence_identity"]["source_season_id"] == 76986
    assert report["publication_receipts"] == [
        dict(table="sofascore_schedule", rows=2, location="iceberg.bronze.sofascore_schedule"),
        dict(table="sofascore_league_table", rows=4, location="iceberg.bronze.sofascore_league_table"),
        dict(table="sofascore_rounds", rows=3, location="iceberg.bronze.sofascore_rounds"),
        dict(table="sofascore_cup_trees", rows=2, location="iceberg.bronze.sofascore_cup_trees"),
        dict(table="sofascore_participants", rows=4, location="iceberg.bronze.sofascore_participants"),
    ]
    noop = json.loads((tmp_path / "season-noop.json").read_text())
    assert not noop.get("publication_receipts")
    assert noop.get("manifest_committed") is not True
    assert len(list((tmp_path / "evidence" / "daily-tail").rglob("*.json"))) == 2
    # The real DAG omits --entity; the actual default season report is "all".
    from tests.unit.scrapers.test_sofascore_refresh_evidence import complete_tail
    from scrapers.sofascore.tail_evidence import assess_tail
    data = complete_tail()
    assert report["entity"] == "all"
    for physical in data["reports"]:
        if physical["entity"] in {"all", "schedule", "league_table"}:
            physical["entity"] = report["entity"]
    result = assess_tail(data, "a" * 40)
    assert result["status"] == "pass", result


# --- #1374/#1244: the players lane fails closed and loads its signed plan ----


@pytest.mark.unit
@pytest.mark.parametrize(
    "dag_id, is_production",
    [
        ("dag_players_sofascore_all_mens", True),
        ("dag_players_sofascore_someone_else", False),
    ],
)
def test_the_players_lane_fails_closed_without_a_signed_plan(
    monkeypatch, dag_id, is_production
):
    """A production lane without a plan must stop, not capture unplanned."""

    runner = _runner_module()
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", dag_id)

    if is_production:
        with pytest.raises(RuntimeError, match="requires --workload-plan"):
            runner._load_runtime_workload_plan(
                None,
                entity="player_capture",
                league="SS-7",
                season="2627",
                offline_replay=False,
            )
        return

    assert runner._load_runtime_workload_plan(
        None,
        entity="player_capture",
        league="SS-7",
        season="2627",
        offline_replay=False,
    ) == (None, ())


@pytest.mark.unit
def test_a_really_signed_players_plan_reaches_the_runtime_loader(tmp_path, monkeypatch):
    """Both ends of the seam meet on a really signed players plan (#1244).

    The players-phase tests double the planner and the runner, so the path
    "players plan -> signature -> runner loader under the lane's dag_id ->
    non-empty player allocations" never ran in one piece. Here the plan is
    signed for the players lane and handed to the real runner loader."""

    from scrapers.sofascore.scraper import _season_label
    from scrapers.sofascore.workload_plan import (
        WorkloadAllocation,
        _signed_plan,
        qualify_work_unit,
    )
    from scrapers.sofascore.workload_runtime import partition_key, target_ids

    runner = _runner_module()
    control_token = "c" * 32
    # The loader resolves the partition through _season_label and matches
    # allocations by their qualified units, so the fixture qualifies its two
    # players under exactly that partition key.
    key = partition_key("SS-7", _season_label("SS-7", "2627"))
    units = tuple(sorted(
        (qualify_work_unit(key, "1"), qualify_work_unit(key, "2"))
    ))
    allocation = WorkloadAllocation(
        allocation_id="alloc-" + "1" * 32,
        task_id="capture_player_batch_00000",
        scope="player",
        workload_class="player_batch_50",
        batch_index=0,
        units=units,
        budget_bytes=4096,
    )
    plan = _signed_plan(
        artifact_id="a" * 64,
        dag_id="dag_players_sofascore_all_mens",
        run_id="players-run-1::players",
        player_universe_ids=units,
        allocations=(allocation,),
        control_token=control_token,
    )
    path = tmp_path / "players-plan.json"
    path.write_text(json.dumps(plan.to_dict()), encoding="utf-8")
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", control_token)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_players_sofascore_all_mens")
    monkeypatch.setenv("SOFASCORE_RUN_ID", "players-run-1")

    loaded, allocations = runner._load_runtime_workload_plan(
        str(path),
        entity="player_capture",
        league="SS-7",
        season="2627",
        offline_replay=False,
    )

    assert loaded.plan_digest == plan.plan_digest
    assert loaded.run_id == "players-run-1::players"
    assert [item.task_id for item in allocations] == [allocation.task_id]
    assert {item.scope for item in allocations} == {"player"}
    assert sum(len(target_ids(item)) for item in allocations) == 2
def _history_match_ids_env(monkeypatch, ids):
    monkeypatch.setenv("SOFASCORE_HISTORY_MATCH_IDS_JSON", json.dumps(ids))
    monkeypatch.setenv("SOFASCORE_HISTORY_PHASE", "matches")
    monkeypatch.setenv("SOFASCORE_HISTORY_SEASON_EVIDENCE", "bronze")


@pytest.mark.unit
def test_offline_history_match_ids_without_workload_plan_are_returned(monkeypatch):
    runner = _runner_module()
    _history_match_ids_env(monkeypatch, ["11352394", "11352395"])

    explicit_ids = runner._signed_history_match_ids(
        None, (), force_replace=False, offline_replay=True
    )

    assert tuple(explicit_ids) == ("11352394", "11352395")


@pytest.mark.unit
def test_history_match_ids_keep_the_live_signed_plan_contract(monkeypatch):
    runner = _runner_module()
    _history_match_ids_env(monkeypatch, ["11352394", "11352395"])

    with pytest.raises(RuntimeError, match="history match IDs require a signed plan"):
        runner._signed_history_match_ids(
            None, (), force_replace=False, offline_replay=False
        )

    workload_runtime = importlib.import_module("scrapers.sofascore.workload_runtime")

    class HashableAllocation(SimpleNamespace):
        def __hash__(self):
            return 1

    allocation = HashableAllocation(allocation_id="alloc-1", scope="match")
    plan = SimpleNamespace(
        verify=MagicMock(),
        allocations={allocation},
        freshness_key=lambda scope: "final",
    )
    monkeypatch.setattr(
        workload_runtime, "target_ids", lambda _allocation: ["11352394", "11352395"]
    )

    signed_ids = runner._signed_history_match_ids(
        plan, (allocation,), force_replace=False, offline_replay=False
    )

    assert tuple(signed_ids) == ("11352394", "11352395")
    plan.verify.assert_called_once()
    with pytest.raises(RuntimeError, match="history match IDs require a signed plan"):
        runner._signed_history_match_ids(
            plan, (allocation,), force_replace=False, offline_replay=True
        )
    with pytest.raises(RuntimeError, match="history match IDs require a signed plan"):
        runner._signed_history_match_ids(
            None, (), force_replace=True, offline_replay=True
        )


def _player_capture_fakes(monkeypatch, *, registered, observed):
    from contextlib import nullcontext

    runner = _runner_module()
    catalog = SimpleNamespace(
        competition=lambda _league: SimpleNamespace(
            capture_allowed=True,
            unique_tournament_id=17,
            slug="premier-league",
        ),
        resolve_source_season=lambda _tid, _token: SimpleNamespace(season_id=76986),
    )
    monkeypatch.setattr(
        "scrapers.sofascore.catalog.SofaScoreCatalog.load", lambda: catalog
    )
    season_pipeline = importlib.import_module("scrapers.sofascore.season_pipeline")
    monkeypatch.setattr(
        season_pipeline,
        "plan_season_partition",
        lambda *args, **kwargs: SimpleNamespace(player_universe_ready=True),
    )
    monkeypatch.setattr(
        season_pipeline,
        "squad_player_ids",
        lambda _raw_store, _plan: list(registered),
    )
    saved = []

    class _FakePlayerScraper:
        def __init__(self, *, leagues, seasons):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def _resolve_player_ids_from_bronze(self, league, season_short, limit=None):
            return list(observed)

        def _add_metadata(self, frame, _entity):
            return frame

        def save_to_iceberg(self, **kwargs):
            saved.append(kwargs)
            return "iceberg.bronze." + kwargs["table_name"]

    monkeypatch.setattr("scrapers.sofascore.SofaScoreScraper", _FakePlayerScraper)
    pipeline = importlib.import_module("scrapers.sofascore.pipeline")
    monkeypatch.setattr(pipeline, "endpoint_resume_plan", lambda _store, _specs: {})
    monkeypatch.setattr(
        pipeline,
        "replay_player_specs",
        MagicMock(side_effect=RuntimeError("stop-after-universe")),
    )
    dq = importlib.import_module("dags.utils.sofascore_dq")
    monkeypatch.setattr(dq, "validate_season_alignment", MagicMock())
    monkeypatch.setattr(dq, "validate_minimum_coverage", MagicMock())
    writer_lock = importlib.import_module("scrapers.sofascore.writer_lock")
    monkeypatch.setattr(writer_lock, "bronze_writer_lock", nullcontext)
    telemetry = importlib.import_module("utils.proxy_traffic")
    monkeypatch.setattr(telemetry, "record_traffic_run", lambda *a, **kw: True)
    monkeypatch.setattr(telemetry, "log_traffic_summary", lambda *a, **kw: None)
    return runner, saved


@pytest.mark.unit
def test_offline_player_ids_env_replaces_the_discovered_universe(tmp_path, monkeypatch):
    runner, saved = _player_capture_fakes(
        monkeypatch, registered=["100", "200"], observed=["300"]
    )
    monkeypatch.setenv(
        "SOFASCORE_OFFLINE_PLAYER_IDS_JSON", json.dumps(["900001", "900002"])
    )
    output = tmp_path / "player.json"

    rc = runner._run_player_capture(
        ["ENG-Premier League"],
        2025,
        None,
        str(output),
        capture_runtime=MagicMock(),
        workload_plan=None,
        offline_replay=True,
    )

    assert rc == 1
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["errors"] == ["capture_engine: stop-after-universe"]
    assert [call["table_name"] for call in saved] == ["sofascore_player_universe"]
    assert saved[0]["df"]["player_id"].tolist() == ["900001", "900002"]
    assert report["universe_players"] == 2
    assert report["players_total"] == 2


@pytest.mark.unit
def test_player_ids_env_is_offline_only_and_fails_before_source_access(
    tmp_path, monkeypatch
):
    runner, saved = _player_capture_fakes(
        monkeypatch, registered=["100"], observed=["200"]
    )
    live = MagicMock(side_effect=AssertionError("live capture must not be invoked"))
    monkeypatch.setattr(
        importlib.import_module("scrapers.sofascore.live_capture"),
        "capture_live_specs",
        live,
    )
    monkeypatch.setenv("SOFASCORE_OFFLINE_PLAYER_IDS_JSON", json.dumps(["900001"]))
    output = tmp_path / "player.json"

    rc = runner._run_player_capture(
        ["ENG-Premier League"],
        2025,
        None,
        str(output),
        capture_runtime=MagicMock(),
        workload_plan=None,
        offline_replay=False,
    )

    assert rc == 1
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["errors"] == [
        "capture_engine: SOFASCORE_OFFLINE_PLAYER_IDS_JSON is only valid with "
        "--offline-replay"
    ]
    assert saved == []
    live.assert_not_called()
