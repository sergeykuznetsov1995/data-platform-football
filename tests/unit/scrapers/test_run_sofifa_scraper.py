"""
Unit tests for ``dags/scripts/run_sofifa_scraper.py`` exit-code logic.

Issue #466: the runner previously returned ``0`` unconditionally — failed
``read_*()`` steps left ``results['errors']`` populated but the BashOperator
still saw exit 0 → green DAG while sofifa_team_ratings / versions / leagues /
player_ratings silently went stale for weeks. The fix returns ``1`` whenever
``results['errors']`` is non-empty.

The runner does ``from scrapers.sofifa import SoFIFAScraper`` lazily inside
``main()``; we install a stub via ``patch.dict('sys.modules', ...)``.
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


_READ_METHODS = (
    'read_players',
    'read_teams',
    'read_team_ratings',
    'read_versions',
    'read_leagues',
    'read_player_ratings',
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_scraper(*, errors: bool):
    """Build a stub SoFIFAScraper context-manager.

    Successful path: every ``read_*()`` returns an EMPTY DataFrame so the
    runner skips ``save_to_iceberg()`` but does NOT append to errors.
    """
    scraper = MagicMock()
    # #616: runner calls scraper.get_traffic_stats(); stub so json.dump stays serializable.
    scraper.get_traffic_stats.return_value = {}

    for method in _READ_METHODS:
        if errors:
            getattr(scraper, method).side_effect = RuntimeError("forced failure")
        else:
            getattr(scraper, method).return_value = pd.DataFrame()

    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=scraper)


def _run_main(args: list, scraper_cls) -> int:
    """Execute ``run_sofifa_scraper.main()`` with stubbed scraper."""
    stub_pkg = MagicMock()
    stub_pkg.SoFIFAScraper = scraper_cls

    sys.argv = ["run_sofifa_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {"scrapers.sofifa": stub_pkg},
    ):
        sys.modules.pop("dags.scripts.run_sofifa_scraper", None)
        mod = importlib.import_module("dags.scripts.run_sofifa_scraper")
        importlib.reload(mod)
        return mod.main()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestRunSofifaExitCode:
    """Cover the ``return 1 if results.get('errors') else 0`` branch."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofifa_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_exit_zero_when_no_errors(self, temp_output):
        """Empty DataFrames → nothing saved, no errors → exit 0."""
        scraper_cls = _build_scraper(errors=False)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            scraper_cls,
        )

        assert rc == 0
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload["errors"] == []

    @pytest.mark.unit
    def test_exit_one_when_errors(self, temp_output):
        """Every read_* raises → 6 errors → exit MUST be 1.

        Direct regression on issue #466: previously the runner returned 0
        unconditionally.
        """
        scraper_cls = _build_scraper(errors=True)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            scraper_cls,
        )

        assert rc == 1, (
            "Expected exit 1 when results.errors is populated — "
            "green-DAG-on-total-failure regression (#466)."
        )

        with open(temp_output) as f:
            payload = json.load(f)
        assert len(payload["errors"]) == len(_READ_METHODS)

    @pytest.mark.unit
    def test_exit_one_with_partial_failure(self, temp_output):
        """One step fails (player_ratings), the rest are empty — exit MUST
        be 1 (any non-empty ``errors`` ⇒ failure)."""
        scraper = MagicMock()
        scraper.get_traffic_stats.return_value = {}
        empty = pd.DataFrame()
        for method in _READ_METHODS:
            getattr(scraper, method).return_value = empty
        scraper.read_player_ratings.side_effect = RuntimeError("Turnstile")
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper_cls = MagicMock(return_value=scraper)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            scraper_cls,
        )

        assert rc == 1
        with open(temp_output) as f:
            payload = json.load(f)
        assert len(payload["errors"]) == 1
        assert "Player ratings" in payload["errors"][0]


class TestRunSofifaVersionsParsing:
    """Cover ``--versions`` parsing (#665).

    ``soccerdata.SoFIFA`` accepts ``'latest'|'all'|int|list[int]`` and raises
    ``ValueError`` on a raw digit string, so the runner must turn explicit
    comma-separated version IDs into ``list[int]`` before constructing the
    scraper — while leaving ``'latest'``/``'all'`` as pass-through strings.
    """

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofifa_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_versions_comma_separated_parsed_to_int_list(self, temp_output):
        """``--versions "180084,190075"`` → SoFIFAScraper(versions=[180084, 190075])."""
        scraper_cls = _build_scraper(errors=False)

        _run_main(
            ["--versions", "180084,190075", "--output", temp_output],
            scraper_cls,
        )

        assert scraper_cls.call_args.kwargs["versions"] == [180084, 190075]

    @pytest.mark.unit
    def test_versions_single_id_parsed_to_int_list(self, temp_output):
        """A lone version ID still becomes a list[int] (soccerdata ``.loc[[id]]``)."""
        scraper_cls = _build_scraper(errors=False)

        _run_main(
            ["--versions", "180084", "--output", temp_output],
            scraper_cls,
        )

        assert scraper_cls.call_args.kwargs["versions"] == [180084]

    @pytest.mark.unit
    def test_versions_latest_passthrough(self, temp_output):
        """``--versions "latest"`` (the weekly path) stays a string, untouched."""
        scraper_cls = _build_scraper(errors=False)

        _run_main(
            ["--versions", "latest", "--output", temp_output],
            scraper_cls,
        )

        assert scraper_cls.call_args.kwargs["versions"] == "latest"

    @pytest.mark.unit
    def test_versions_all_passthrough(self, temp_output):
        """``--versions "all"`` stays a string, untouched."""
        scraper_cls = _build_scraper(errors=False)

        _run_main(
            ["--versions", "all", "--output", temp_output],
            scraper_cls,
        )

        assert scraper_cls.call_args.kwargs["versions"] == "all"
