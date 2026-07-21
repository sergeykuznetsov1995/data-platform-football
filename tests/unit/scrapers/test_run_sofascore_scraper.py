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
