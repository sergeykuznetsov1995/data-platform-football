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
def _build_scraper(*, errors: bool):
    """Build a stub UnderstatScraper context-manager.

    Successful path: every read_*() returns an EMPTY DataFrame so the
    runner just skips the corresponding save_to_iceberg() call but does
    NOT append to errors. ``read_player_match_stats`` returns an empty
    DataFrame too (the runner checks ``df.empty`` on it).
    """
    scraper = MagicMock()

    if errors:
        def _boom(*a, **k):
            raise RuntimeError("forced failure")

        scraper.read_schedule.side_effect = _boom
        scraper.read_shot_events.side_effect = _boom
        scraper.read_player_season_stats.side_effect = _boom
        scraper.read_team_match_stats.side_effect = _boom
        scraper.read_player_match_stats.side_effect = _boom
    else:
        empty = pd.DataFrame()
        scraper.read_schedule.return_value = empty
        scraper.read_shot_events.return_value = empty
        scraper.read_player_season_stats.return_value = empty
        scraper.read_team_match_stats.return_value = empty
        scraper.read_player_match_stats.return_value = empty

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
        """Empty DataFrames + clean dict from scrape_player_match_stats
        → no errors → exit 0."""
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
        """One step fails, the rest are empty — exit MUST be 1 (any
        non-empty ``errors`` ⇒ failure)."""
        scraper = MagicMock()
        empty = pd.DataFrame()
        scraper.read_schedule.return_value = empty
        scraper.read_shot_events.side_effect = RuntimeError("HTTP 500")
        scraper.read_player_season_stats.return_value = empty
        scraper.read_team_match_stats.return_value = empty
        scraper.read_player_match_stats.return_value = empty
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
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
