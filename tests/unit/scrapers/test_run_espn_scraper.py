"""
Unit tests for ``dags/scripts/run_espn_scraper.py`` exit-code logic.

Issue #466: the runner previously returned ``0`` unconditionally — a failed
``read_schedule()`` left ``results['errors']`` populated but the BashOperator
still saw exit 0 → green DAG while bronze.espn_schedule silently went stale.
The fix returns ``1`` whenever ``results['errors']`` is non-empty.

The runner does ``from scrapers.espn import ESPNScraper`` lazily inside
``main()`` to avoid pulling the heavy ``scrapers/__init__.py`` into the
parser-time import graph; we install a stub via ``patch.dict('sys.modules',
...)`` accordingly.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_scraper(*, errors: bool):
    """Build a stub ESPNScraper context-manager.

    Successful path: ``read_schedule()`` returns an EMPTY DataFrame so the
    runner skips ``save_to_iceberg()`` but does NOT append to errors.
    """
    scraper = MagicMock()

    if errors:
        scraper.read_schedule.side_effect = RuntimeError("forced failure")
    else:
        scraper.read_schedule.return_value = pd.DataFrame()

    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=scraper)


def _run_main(args: list, scraper_cls) -> int:
    """Execute ``run_espn_scraper.main()`` with stubbed scraper."""
    stub_pkg = MagicMock()
    stub_pkg.ESPNScraper = scraper_cls

    sys.argv = ["run_espn_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {"scrapers.espn": stub_pkg},
    ):
        sys.modules.pop("dags.scripts.run_espn_scraper", None)
        mod = importlib.import_module("dags.scripts.run_espn_scraper")
        importlib.reload(mod)
        return mod.main()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestRunEspnExitCode:
    """Cover the ``return 1 if results.get('errors') else 0`` branch."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="espn_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_exit_zero_when_no_errors(self, temp_output):
        """Empty DataFrame → nothing saved, no errors → exit 0."""
        scraper_cls = _build_scraper(errors=False)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--season", "2024",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 0
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload["errors"] == []

    @pytest.mark.unit
    def test_exit_one_when_errors(self, temp_output):
        """read_schedule raises → error recorded → exit MUST be 1.

        Direct regression on issue #466: previously the runner returned 0
        unconditionally.
        """
        scraper_cls = _build_scraper(errors=True)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--season", "2024",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 1, (
            "Expected exit 1 when results.errors is populated — "
            "green-DAG-on-total-failure regression (#466)."
        )

        with open(temp_output) as f:
            payload = json.load(f)
        assert len(payload["errors"]) == 1
        assert "Schedule" in payload["errors"][0]
