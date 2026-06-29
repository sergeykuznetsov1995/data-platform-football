"""
Unit tests for the completeness-guard wiring in
``dags/scripts/run_matchhistory_scraper.py`` (#583).

The guard arithmetic lives in ``BaseScraper.save_to_iceberg`` (covered by
``test_base_scraper.py``); here we cover the runner's *handling* — arm the guard
on the normal path, map ``ReplaceGuardError`` to exit 3, and let
``--force-replace`` disarm it.

The runner does ``from scrapers.matchhistory import MatchHistoryScraper`` lazily
inside ``main()`` to keep the heavy ``scrapers/__init__.py`` out of the
parser-time import graph; we install a stub via ``patch.dict('sys.modules', ...)``
accordingly (mirrors ``test_run_espn_scraper.py``).
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
def _build_guard_scraper(*, guard_blocks: bool = False):
    """Stub MatchHistoryScraper whose ``read_games`` returns a non-empty frame so
    the runner reaches ``save_to_iceberg``. With ``guard_blocks=True`` the
    BaseScraper-level completeness guard is simulated by raising
    ``ReplaceGuardError`` — the runner must catch it and exit 3 (#583).

    Returns the scraper instance (wrap in ``MagicMock(return_value=...)`` when
    handing it to ``_run_main``) so tests can inspect ``save_to_iceberg.call_args``.
    """
    from scrapers.base.base_scraper import ReplaceGuardError

    df = pd.DataFrame({
        'league': ['ENG-Premier League'] * 10,
        'season': [2024] * 10,
        'match_date': ['17/08/2024'] * 10,
    })
    scraper = MagicMock()
    scraper.read_games.return_value = df
    # The runner does ``df = scraper.calculate_odds_stats(df)`` before save.
    scraper.calculate_odds_stats.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=3 rows < 90% of existing=380 for bronze.matchhistory_results '
            '— refusing replace_partitions save (would shrink the partition)'
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.matchhistory_results'
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


def _run_main(args: list, scraper_cls) -> int:
    """Execute ``run_matchhistory_scraper.main()`` with a stubbed scraper."""
    stub_pkg = MagicMock()
    stub_pkg.MatchHistoryScraper = scraper_cls

    sys.argv = ["run_matchhistory_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {"scrapers.matchhistory": stub_pkg},
    ):
        sys.modules.pop("dags.scripts.run_matchhistory_scraper", None)
        mod = importlib.import_module("dags.scripts.run_matchhistory_scraper")
        importlib.reload(mod)
        return mod.main()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestMatchHistoryReplaceGuard:
    """#583: completeness-guard wiring in the MatchHistory runner."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="matchhistory_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_guard_refusal_exits_3(self, temp_output):
        """save_to_iceberg raises ReplaceGuardError → exit 3 +
        MATCHHISTORY_REPLACE_GUARD marker (distinct from a hard failure)."""
        scraper = _build_guard_scraper(guard_blocks=True)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 3
        scraper.save_to_iceberg.assert_called_once()
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("MATCHHISTORY_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_normal_path_arms_guard_exits_0(self, temp_output):
        """Non-force run passes min_replace_ratio=0.9 (raw COUNT(*), no key)."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] == 0.9
        assert kwargs["replace_partitions"] == ["league", "season"]
        # one row per match → raw COUNT(*), no replace_guard_key
        assert "replace_guard_key" not in kwargs

    @pytest.mark.unit
    def test_force_replace_disarms_guard(self, temp_output):
        """--force-replace must pass min_replace_ratio=None to the save."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] is None
