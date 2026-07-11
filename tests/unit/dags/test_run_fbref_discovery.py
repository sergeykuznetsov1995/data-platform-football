import json
from unittest.mock import MagicMock, patch

import pytest

from dags.scripts import run_fbref_discovery as runner
from scrapers.fbref.discovery_queue import DiscoveryQueueRun
from scrapers.fbref.discovery_service import DiscoveryRunResult


def test_offline_index_never_builds_network_loader(tmp_path):
    output = tmp_path / "index.json"
    result = DiscoveryRunResult(mode="index", offline=True)
    service = MagicMock()
    service.discover_index.return_value = result

    with (
        patch.object(runner.RawPageStore, "from_uri", return_value=MagicMock()),
        patch.object(runner, "FBrefDiscoveryService", return_value=service) as factory,
        patch.object(runner, "LazyFBrefLoader") as loader_class,
    ):
        code = runner.main([
            "--raw-store-uri", tmp_path.as_uri(),
            "--output", str(output),
            "--offline",
            "index",
        ])

    assert code == 0
    loader_class.assert_not_called()
    assert factory.call_args.kwargs["loader"] is None
    assert factory.call_args.kwargs["offline"] is True
    payload = json.loads(output.read_text())
    assert payload["transport"] == {
        "transport_created": False,
        "proxy_requests": 0,
    }


def test_discover_forwards_explicit_bounds_and_selection(tmp_path, monkeypatch):
    monkeypatch.setenv("FBREF_TRANSPORT", "camoufox")
    output = tmp_path / "discover.json"
    result = DiscoveryRunResult(mode="discover", offline=False)
    service = MagicMock()
    service.discover_graph.return_value = result
    lazy_loader = MagicMock()
    lazy_loader.diagnostics.return_value = {
        "transport_created": False,
        "proxy_requests": 0,
    }

    with (
        patch.object(runner.RawPageStore, "from_uri", return_value=MagicMock()),
        patch.object(runner, "FBrefDiscoveryService", return_value=service) as factory,
        patch.object(runner, "LazyFBrefLoader", return_value=lazy_loader),
    ):
        code = runner.main([
            "--raw-store-uri", tmp_path.as_uri(),
            "--output", str(output),
            "discover",
            "--competition-id", "9",
            "--season-label", "2025-2026",
            "--max-competitions", "1",
            "--max-seasons-per-competition", "1",
            "--max-network-pages", "4",
        ])

    assert code == 0
    assert factory.call_args.kwargs["max_network_pages"] == 4
    service.discover_graph.assert_called_once_with(
        ["9"],
        max_competitions=1,
        max_seasons_per_competition=1,
        season_labels=["2025-2026"],
    )
    lazy_loader.close.assert_called_once()
    payload = json.loads(output.read_text())
    assert payload["limits"] == {
        "max_network_pages": 4,
        "max_competitions": 1,
        "max_seasons_per_competition": 1,
    }


def test_discover_batch_requires_opt_in_and_enforces_hard_caps():
    parser = runner.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([
            "--raw-store-uri", "file:///tmp/raw",
            "discover-batch",
            "--queue-id", "all-current",
        ])
    with pytest.raises(SystemExit):
        parser.parse_args([
            "--raw-store-uri", "file:///tmp/raw",
            "discover-batch",
            "--all-discovered-competitions",
            "--queue-id", "all-current",
            "--max-competitions", str(runner.MAX_BATCH_COMPETITIONS + 1),
        ])
    with pytest.raises(SystemExit):
        parser.parse_args([
            "--raw-store-uri", "file:///tmp/raw",
            "discover-batch",
            "--all-discovered-competitions",
            "--queue-id", "all-current",
            "--max-network-pages", str(runner.MAX_BATCH_NETWORK_PAGES + 1),
        ])


def test_discover_batch_lock_rejects_a_second_local_worker(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("FBREF_DISCOVERY_LOCK_DIR", str(tmp_path))

    with runner._queue_process_lock("file:///raw", "all-current"):
        with pytest.raises(RuntimeError, match="already running"):
            with runner._queue_process_lock("file:///raw", "all-current"):
                pass


def test_discover_batch_forwards_bounds_and_writes_queue_progress(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("FBREF_TRANSPORT", "camoufox")
    output = tmp_path / "batch.json"
    result = DiscoveryRunResult(
        mode="discover-batch",
        offline=False,
        errors=[{
            "target_id": "fbref:competition:2",
            "page_kind": "competition",
            "dataset": "seasons",
            "reason": "target_failed",
            "error_type": "RawStoreError",
            "message": "retry later",
        }],
    )
    queue_run = DiscoveryQueueRun(
        result=result,
        queue={
            "queue_id": "all-current",
            "total": 100,
            "completed": 5,
            "pending": 95,
            "complete": False,
            "status": "progress",
            "stop_reason": "batch_limit_reached",
        },
    )
    service = MagicMock()
    queue = MagicMock()
    queue.run.return_value = queue_run
    lazy_loader = MagicMock()
    lazy_loader.diagnostics.return_value = {
        "transport_created": False,
        "proxy_requests": 0,
    }
    raw_store = MagicMock()

    with (
        patch.object(runner.RawPageStore, "from_uri", return_value=raw_store),
        patch.object(runner, "FBrefDiscoveryService", return_value=service) as factory,
        patch.object(runner, "FBrefDiscoveryQueue", return_value=queue) as queue_class,
        patch.object(runner, "LazyFBrefLoader", return_value=lazy_loader),
    ):
        code = runner.main([
            "--raw-store-uri", tmp_path.as_uri(),
            "--output", str(output),
            "discover-batch",
            "--all-discovered-competitions",
            "--queue-id", "all-current",
            "--max-competitions", "5",
            "--max-seasons-per-competition", "2",
            "--max-attempts", "4",
            "--max-network-pages", "12",
        ])

    assert code == 0
    assert factory.call_args.kwargs["max_network_pages"] == 12
    queue_class.assert_called_once_with(raw_store, service)
    queue.run.assert_called_once_with(
        "all-current",
        max_competitions=5,
        max_seasons_per_competition=2,
        max_attempts=4,
    )
    lazy_loader.close.assert_called_once()
    payload = json.loads(output.read_text())
    assert payload["queue"]["completed"] == 5
    assert payload["limits"] == {
        "max_network_pages": 12,
        "max_competitions": 5,
        "max_seasons_per_competition": 2,
        "max_attempts": 4,
    }
    assert list(tmp_path.glob(".batch.json.*.tmp")) == []


def test_parse_or_target_error_returns_nonzero_and_is_written(tmp_path):
    output = tmp_path / "failed.json"
    result = DiscoveryRunResult(
        mode="index",
        offline=True,
        errors=[{
            "target_id": "fbref:competition_index:all",
            "page_kind": "competition_index",
            "dataset": "competitions",
            "reason": "target_failed",
            "error_type": "RawPageNotFound",
            "message": "missing",
        }],
    )
    service = MagicMock()
    service.discover_index.return_value = result

    with (
        patch.object(runner.RawPageStore, "from_uri", return_value=MagicMock()),
        patch.object(runner, "FBrefDiscoveryService", return_value=service),
    ):
        code = runner.main([
            "--raw-store-uri", tmp_path.as_uri(),
            "--output", str(output),
            "--offline",
            "index",
        ])

    assert code == 1
    payload = json.loads(output.read_text())
    assert payload["errors"][0]["error_type"] == "RawPageNotFound"


def test_unexpected_run_error_is_written_and_transport_is_closed(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("FBREF_TRANSPORT", "camoufox")
    output = tmp_path / "failed-run.json"
    service = MagicMock()
    service.discover_graph.side_effect = ValueError("scope exceeds limit")
    lazy_loader = MagicMock()
    lazy_loader.diagnostics.return_value = {
        "transport_created": False,
        "proxy_requests": 0,
    }

    with (
        patch.object(runner.RawPageStore, "from_uri", return_value=MagicMock()),
        patch.object(runner, "FBrefDiscoveryService", return_value=service),
        patch.object(runner, "LazyFBrefLoader", return_value=lazy_loader),
    ):
        code = runner.main([
            "--raw-store-uri", tmp_path.as_uri(),
            "--output", str(output),
            "discover",
            "--competition-id", "9",
        ])

    assert code == 1
    lazy_loader.close.assert_called_once()
    payload = json.loads(output.read_text())
    assert payload["errors"][0]["error_type"] == "ValueError"
    assert payload["errors"][0]["message"] == "scope exceeds limit"
