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
