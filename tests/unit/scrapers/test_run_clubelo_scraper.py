"""
Unit tests for ``dags/scripts/run_clubelo_scraper.py``.

Issue #583: the runner delegates the ``clubelo_ratings`` save to
``ClubEloScraper.scrape_current_ratings`` (the historical table is saved
inside ``scrape_historical_ratings``). This file covers the runner's wiring —
forward ``--force-replace``, map ``ReplaceGuardError`` to exit 3, run ONLY the
historical stage in ``--mode full`` (the daily task already scraped current
ratings earlier in the DAG chain), and surface an empty historical result as
an error in the results JSON. The guard arithmetic itself lives in
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

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_guard_scraper(*, guard_blocks: bool = False):
    """Stub ClubEloScraper. With ``guard_blocks=True`` the method-level
    completeness guard is simulated by ``scrape_current_ratings`` raising
    ``ReplaceGuardError`` — the runner must catch it and exit 3 (#583).

    Returns the scraper instance (not the class stub) so tests can inspect
    ``scrape_current_ratings.call_args``; wrap it in
    ``MagicMock(return_value=...)`` when handing it to ``_run_main``.
    """
    from scrapers.base.base_scraper import ReplaceGuardError

    scraper = MagicMock()
    if guard_blocks:
        scraper.scrape_current_ratings.side_effect = ReplaceGuardError(
            'new=3 rows < 90% of existing=380 for bronze.clubelo_ratings '
            '— refusing replace_partitions save (would shrink the partition)'
        )
    else:
        scraper.scrape_current_ratings.return_value = {
            'current_ratings': 'iceberg.bronze.clubelo_ratings',
            'rows': 20,
            'rating_date': '2026-06-20',
        }
    # Only reached in --mode full; stub it for the daily tests too.
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
        """scrape_current_ratings raises ReplaceGuardError → exit 3 +
        CLUBELO_REPLACE_GUARD marker (distinct from the exit-1 path)."""
        scraper = _build_guard_scraper(guard_blocks=True)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 3
        scraper.scrape_current_ratings.assert_called_once()
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("CLUBELO_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_normal_path_arms_guard_exits_0(self, temp_output):
        """Non-force run keeps the guard armed (force_replace=False) and the
        results JSON carries rows + rating_date from the method's result."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.scrape_current_ratings.call_args.kwargs
        assert kwargs["force_replace"] is False
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload["rows"] == 20
        assert payload["rating_date"] == "2026-06-20"
        assert payload["tables"] == ["iceberg.bronze.clubelo_ratings"]

    @pytest.mark.unit
    def test_force_replace_disarms_guard(self, temp_output):
        """--force-replace must forward force_replace=True to the method."""
        scraper = _build_guard_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--force-replace",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.scrape_current_ratings.call_args.kwargs
        assert kwargs["force_replace"] is True


class TestFullModeDaysBack:
    """#716: --mode full forwards --days-back to scrape_historical_ratings."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="clubelo_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_days_back_and_force_replace_forwarded(self, temp_output):
        """--mode full --days-back 3650 --force-replace → historical scrape gets
        days_back=3650, force_replace=True (the deep #716 backfill)."""
        scraper = _build_guard_scraper()
        scraper.scrape_historical_ratings.return_value = {
            'historical_ratings': 'iceberg.bronze.clubelo_ratings_historical',
            'rows': 42,
        }

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--mode", "full",
             "--days-back", "3650", "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.scrape_historical_ratings.call_args.kwargs
        assert kwargs["days_back"] == 3650
        assert kwargs["force_replace"] is True

    @pytest.mark.unit
    def test_days_back_defaults_to_365(self, temp_output):
        """--mode full without --days-back keeps the recurring 365-day refresh."""
        scraper = _build_guard_scraper()
        scraper.scrape_historical_ratings.return_value = {
            'historical_ratings': 'iceberg.bronze.clubelo_ratings_historical',
            'rows': 7,
        }

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--mode", "full",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        assert scraper.scrape_historical_ratings.call_args.kwargs["days_back"] == 365

    @pytest.mark.unit
    def test_full_mode_skips_current_ratings(self, temp_output):
        """--mode full runs ONLY the historical stage — the daily task already
        scraped current ratings earlier in the DAG chain (scrape_ratings >>
        gate >> full), so re-scraping them here was a duplicate HTTP call and
        a duplicate partition write."""
        scraper = _build_guard_scraper()
        scraper.scrape_historical_ratings.return_value = {
            'historical_ratings': 'iceberg.bronze.clubelo_ratings_historical',
            'rows': 7,
        }

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--mode", "full",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        scraper.scrape_current_ratings.assert_not_called()

    @pytest.mark.unit
    def test_full_mode_empty_historical_records_error(self, temp_output):
        """Empty historical result ({}) must not be silent: the stage stays
        non-critical (exit 0, #716) but the error lands in the results JSON."""
        scraper = _build_guard_scraper()
        scraper.scrape_historical_ratings.return_value = {}

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--mode", "full",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("historical" in e for e in payload["errors"])
