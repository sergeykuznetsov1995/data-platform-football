"""
Unit tests for ``dags/scripts/run_whoscored_scraper.py`` exit-code logic.

Regression target: previously the runner unconditionally returned ``0``,
even when individual ``scrape_*`` methods raised and were captured into
``results['errors']``. That made Airflow tasks succeed silently while
the underlying scrape was partially or wholly broken.

Fix under test (line 209): ::

    return 1 if results.get('errors') else 0

These tests stub the heavy ``scrapers.whoscored.WhoScoredScraper`` import
so the runner can be exercised without real browser / Trino.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_scraper_class(*, errors: bool):
    """Return a class whose instances act as a context-manager scraper.

    If ``errors`` is True, every ``scrape_*`` method raises so the runner
    appends to ``results['errors']``. Otherwise the methods return empty
    dicts (success, no tables).
    """
    scraper = MagicMock()
    # #616: runner calls scraper.get_traffic_stats(); stub so json.dump stays serializable.
    scraper.get_traffic_stats.return_value = {}

    if errors:
        def _boom(*a, **k):
            raise RuntimeError("forced failure")

        scraper.scrape_schedule.side_effect = _boom
        scraper.scrape_missing_players.side_effect = _boom
        scraper.scrape_season_stages.side_effect = _boom
        scraper.scrape_events.side_effect = _boom
    else:
        scraper.scrape_schedule.return_value = {}
        scraper.scrape_missing_players.return_value = {}
        scraper.scrape_season_stages.return_value = {}
        scraper.scrape_events.return_value = {}

    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=scraper)


def _run_main(args: list, scraper_cls) -> int:
    """Call ``run_whoscored_scraper.main()`` with sys.argv replaced.

    The runner does ``from scrapers.whoscored import WhoScoredScraper``
    inside ``main()`` (lazy import to avoid heavy ``scrapers/__init__.py``
    side-effects), so we need to install the stub via ``sys.modules``
    *before* main() runs. ``patch.dict('sys.modules', ...)`` handles
    that and cleans up after.
    """
    # Build a minimal fake ``scrapers.whoscored`` package surface.
    stub_pkg = MagicMock()
    stub_pkg.WhoScoredScraper = scraper_cls

    sys.argv = ["run_whoscored_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {"scrapers.whoscored": stub_pkg},
    ):
        sys.modules.pop("dags.scripts.run_whoscored_scraper", None)
        mod = importlib.import_module("dags.scripts.run_whoscored_scraper")
        importlib.reload(mod)
        return mod.main()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestRunWhoscoredExitCode:
    """Cover the ``return 1 if results.get('errors') else 0`` branch."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="whoscored_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_exit_zero_when_no_errors(self, temp_output):
        """All scrape_* methods succeed → empty errors list → exit 0."""
        scraper_cls = _build_scraper_class(errors=False)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--seasons", "2024",
                "--skip-events",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 0, "Expected exit 0 when results.errors is empty"

        with open(temp_output) as f:
            payload = json.load(f)
        assert payload["errors"] == []

    @pytest.mark.unit
    def test_exit_one_when_scrape_methods_raise(self, temp_output):
        """Every scrape_* method raises → results.errors populated → exit 1.

        This is the regression: previously the runner returned 0 here.
        """
        scraper_cls = _build_scraper_class(errors=True)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--seasons", "2024",
                "--skip-events",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 1, (
            "Expected exit 1 when results.errors is populated — "
            "this is the bug fix being verified."
        )

        with open(temp_output) as f:
            payload = json.load(f)
        # 3 cheap tasks ran and each raised once
        assert len(payload["errors"]) == 3
        assert any("schedule" in e for e in payload["errors"])
        assert any("missing_players" in e for e in payload["errors"])
        assert any("season_stages" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_exit_one_with_partial_failure(self, temp_output):
        """Mixed success/failure also yields exit 1 — partial-error must not
        be reported as success."""
        scraper = MagicMock()
        scraper.get_traffic_stats.return_value = {}
        scraper.scrape_schedule.return_value = {
            "schedule": "iceberg.bronze.whoscored_schedule"
        }
        scraper.scrape_missing_players.side_effect = RuntimeError("502 BAD")
        scraper.scrape_season_stages.return_value = {}
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper_cls = MagicMock(return_value=scraper)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--seasons", "2024",
                "--skip-events",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 1
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("missing_players" in e for e in payload["errors"])
        # schedule succeeded — should still be listed as a written table
        assert "iceberg.bronze.whoscored_schedule" in payload["tables"]
