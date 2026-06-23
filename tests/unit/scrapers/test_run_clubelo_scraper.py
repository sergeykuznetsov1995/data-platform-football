"""
Unit tests for ``dags/scripts/run_clubelo_scraper.py`` completeness-guard wiring.

Issue #583: the ClubElo runner inlines the ``clubelo_ratings`` save (the
historical table is saved inside ``scrape_historical_ratings``). This file
covers the runner's *handling* of the BaseScraper-level completeness guard —
arm it on the normal path, map ``ReplaceGuardError`` to exit 3, and let
``--force-replace`` disarm it. The guard arithmetic itself lives in
``BaseScraper.save_to_iceberg`` (covered by ``test_base_scraper.py``).

The runner does ``from scrapers.clubelo import ClubEloScraper`` lazily inside
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
def _build_guard_scraper(*, guard_blocks: bool = False):
    """Stub ClubEloScraper whose ``read_by_date`` returns a non-empty frame so
    the runner reaches ``save_to_iceberg``. With ``guard_blocks=True`` the
    BaseScraper-level completeness guard is simulated by raising
    ``ReplaceGuardError`` — the runner must catch it and exit 3 (#583).

    Returns the scraper instance (not the class stub) so tests can inspect
    ``save_to_iceberg.call_args``; wrap it in ``MagicMock(return_value=...)``
    when handing it to ``_run_main``.
    """
    from scrapers.base.base_scraper import ReplaceGuardError

    df = pd.DataFrame({
        'rating_date': ['2026-06-20'] * 20,
        'country': ['ENG'] * 20,
        'club': [f'Club {i}' for i in range(20)],
        'elo': [1500 + i for i in range(20)],
    })
    scraper = MagicMock()
    scraper.read_by_date.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=3 rows < 90% of existing=380 for bronze.clubelo_ratings '
            '— refusing replace_partitions save (would shrink the partition)'
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.clubelo_ratings'
    # Default mode is "daily" → historical stage is not reached, but stub it.
    scraper.scrape_historical_ratings.return_value = {}
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


def _run_main(args: list, scraper_cls) -> int:
    """Execute ``run_clubelo_scraper.main()`` with stubbed scraper."""
    stub_pkg = MagicMock()
    stub_pkg.ClubEloScraper = scraper_cls

    sys.argv = ["run_clubelo_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {"scrapers.clubelo": stub_pkg},
    ):
        sys.modules.pop("dags.scripts.run_clubelo_scraper", None)
        mod = importlib.import_module("dags.scripts.run_clubelo_scraper")
        importlib.reload(mod)
        return mod.main()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestClubEloReplaceGuard:
    """#583: completeness-guard wiring in the ClubElo runner (current ratings)."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="clubelo_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_guard_refusal_exits_3(self, temp_output):
        """save_to_iceberg raises ReplaceGuardError → exit 3 +
        CLUBELO_REPLACE_GUARD marker (distinct from the exit-1 path)."""
        scraper = _build_guard_scraper(guard_blocks=True)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 3
        scraper.save_to_iceberg.assert_called_once()
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("CLUBELO_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_normal_path_arms_guard_exits_0(self, temp_output):
        """Non-force run passes min_replace_ratio=0.9 (raw COUNT(*), no key)."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] == 0.9
        assert kwargs["replace_partitions"] == ["rating_date"]
        # daily snapshot is full-state per rating_date → raw COUNT(*), no key
        assert "replace_guard_key" not in kwargs

    @pytest.mark.unit
    def test_force_replace_disarms_guard(self, temp_output):
        """--force-replace must pass min_replace_ratio=None to the save."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--force-replace",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] is None


class TestClubEloFullMode:
    """#716: mode=full forwards --days-back into scrape_historical_ratings so a
    UI-triggered backfill can reach the 10-season depth (~3650 days)."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="clubelo_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_full_mode_forwards_days_back(self, temp_output):
        """--mode full --days-back 3650 must reach scrape_historical_ratings
        with days_back=3650 (10-season backfill depth)."""
        scraper = _build_guard_scraper()
        scraper.scrape_historical_ratings.return_value = {
            "historical_ratings": "iceberg.bronze.clubelo_ratings_historical",
            "rows": 12345,
        }

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--mode", "full",
             "--days-back", "3650", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.scrape_historical_ratings.call_args.kwargs
        assert kwargs["days_back"] == 3650
        assert kwargs["force_replace"] is False

    @pytest.mark.unit
    def test_daily_mode_skips_history(self, temp_output):
        """Default (daily) mode must not call the heavy historical stage."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        scraper.scrape_historical_ratings.assert_not_called()
