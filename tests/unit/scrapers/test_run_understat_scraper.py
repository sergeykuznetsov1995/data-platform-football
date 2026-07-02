"""
Unit tests for ``dags/scripts/run_understat_scraper.py`` exit-code logic.

Same regression as the WhoScored runner: previously returned ``0``
unconditionally; the fix returns ``1`` whenever any ``scrape_*`` step
appended to ``results['errors']``.

The runner does ``from scrapers.understat import UnderstatScraper`` lazily
inside ``main()`` to avoid pulling the heavy ``scrapers/__init__.py``
into the parser-time import graph; we install a stub via
``patch.dict('sys.modules', ...)`` accordingly.
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
_READ_METHODS = (
    'read_schedule',
    'read_shot_events',
    'read_player_season_stats',
    'read_team_match_stats',
    'read_player_match_stats',
)


def _build_scraper(*, errors: bool = False, empty: bool = False):
    """Build a stub UnderstatScraper context-manager.

    Successful path: every read_*() returns a non-empty DataFrame and
    save_to_iceberg() succeeds — no errors. ``empty=True`` makes every
    read_*() return an EMPTY DataFrame: since the rollover fail-closed fix
    the runner must record an error per table (an empty frame means the
    season is missing from the source, not "nothing to do").
    """
    scraper = MagicMock()

    if errors:
        def _boom(*a, **k):
            raise RuntimeError("forced failure")

        for m in _READ_METHODS:
            getattr(scraper, m).side_effect = _boom
    elif empty:
        for m in _READ_METHODS:
            getattr(scraper, m).return_value = pd.DataFrame()
    else:
        df = pd.DataFrame({
            'league': ['ENG-Premier League'] * 3,
            'season': [2024] * 3,
            'id': ['1', '2', '3'],
        })
        for m in _READ_METHODS:
            getattr(scraper, m).return_value = df
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.understat_x'

    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return MagicMock(return_value=scraper)


def _run_main(args: list, scraper_cls) -> int:
    """Execute ``run_understat_scraper.main()`` with stubbed scraper."""
    stub_pkg = MagicMock()
    stub_pkg.UnderstatScraper = scraper_cls

    sys.argv = ["run_understat_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {"scrapers.understat": stub_pkg},
    ):
        sys.modules.pop("dags.scripts.run_understat_scraper", None)
        mod = importlib.import_module("dags.scripts.run_understat_scraper")
        importlib.reload(mod)
        return mod.main()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestRunUnderstatExitCode:
    """Cover the ``return 1 if results.get('errors') else 0`` branch."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="understat_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_exit_zero_when_no_errors(self, temp_output):
        """Non-empty DataFrames + successful saves → no errors → exit 0,
        and the leagues actually scraped land in the results JSON (the DAG
        scales its row floors by them)."""
        scraper_cls = _build_scraper()

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
        assert payload["leagues"] == ["ENG-Premier League"]

    @pytest.mark.unit
    def test_exit_one_when_all_empty(self, temp_output):
        """Rollover fail-closed: every read_*() returning an empty frame is
        an ERROR per table (frozen leagues.json cache scenario) → exit 1,
        never a silent green run."""
        scraper_cls = _build_scraper(empty=True)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--season", "2024",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 1
        with open(temp_output) as f:
            payload = json.load(f)
        assert len(payload["errors"]) == 5
        assert all("empty scrape result" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_exit_one_when_errors(self, temp_output):
        """Every read_*/scrape_* call raises → 5 errors → exit 1.

        Direct regression on the bug fix at
        ``run_understat_scraper.py:165``.
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
            "regression on the runner's exit-code fix."
        )

        with open(temp_output) as f:
            payload = json.load(f)
        # All five steps raise → all five error messages present
        assert len(payload["errors"]) == 5
        assert any("Schedule" in e for e in payload["errors"])
        assert any("Shots" in e for e in payload["errors"])
        assert any("Player stats" in e for e in payload["errors"])
        assert any("Team match stats" in e for e in payload["errors"])
        assert any("Player match stats" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_exit_one_with_partial_failure(self, temp_output):
        """One step fails, the rest succeed — exit MUST be 1 (any
        non-empty ``errors`` ⇒ failure)."""
        scraper = _build_scraper().return_value
        scraper.read_shot_events.side_effect = RuntimeError("HTTP 500")
        scraper_cls = MagicMock(return_value=scraper)

        rc = _run_main(
            [
                "--leagues", "ENG-Premier League",
                "--season", "2024",
                "--output", temp_output,
            ],
            scraper_cls,
        )

        assert rc == 1
        with open(temp_output) as f:
            payload = json.load(f)
        assert len(payload["errors"]) == 1
        assert "Shots" in payload["errors"][0]


class TestUnderstatReplaceGuard:
    """#583: completeness-guard wiring in the Understat runner.

    The guard arithmetic lives in ``BaseScraper.save_to_iceberg`` (covered by
    ``test_base_scraper.py``); here we cover the runner's *handling* — arm the
    guard on every replace save, map ``ReplaceGuardError`` to exit 3, and let
    ``--force-replace`` disarm it.
    """

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="understat_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @staticmethod
    def _guard_scraper(*, guard_blocks: bool = False):
        """Stub whose five read_*() return non-empty frames so every save runs."""
        from scrapers.base.base_scraper import ReplaceGuardError

        df = pd.DataFrame({
            'league': ['ENG-Premier League'] * 10,
            'season': [2024] * 10,
            'id': [str(i) for i in range(10)],
        })
        scraper = MagicMock()
        for m in ('read_schedule', 'read_shot_events',
                  'read_player_season_stats', 'read_team_match_stats',
                  'read_player_match_stats'):
            getattr(scraper, m).return_value = df
        if guard_blocks:
            scraper.save_to_iceberg.side_effect = ReplaceGuardError(
                'new=3 rows < 90% of existing=380 for bronze.understat_schedule '
                '— refusing replace_partitions save (would shrink the partition)'
            )
        else:
            scraper.save_to_iceberg.return_value = 'iceberg.bronze.understat_x'
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        return scraper

    @pytest.mark.unit
    def test_guard_refusal_exits_3(self, temp_output):
        """A refused save (ReplaceGuardError) → exit 3 + UNDERSTAT_REPLACE_GUARD
        marker (distinct from the exit-1 hard-failure path)."""
        scraper = self._guard_scraper(guard_blocks=True)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 3
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("UNDERSTAT_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_normal_path_arms_guard_exits_0(self, temp_output):
        """Non-force run arms min_replace_ratio=0.9 (raw COUNT(*), no key)."""
        scraper = self._guard_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] == 0.9
        assert kwargs["replace_partitions"] == ["league", "season"]
        assert "replace_guard_key" not in kwargs

    @pytest.mark.unit
    def test_force_replace_disarms_guard(self, temp_output):
        """--force-replace must pass min_replace_ratio=None to every save."""
        scraper = self._guard_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] is None
