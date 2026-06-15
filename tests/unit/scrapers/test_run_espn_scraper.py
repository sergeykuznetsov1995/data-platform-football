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


def _build_guard_scraper(*, guard_blocks: bool = False):
    """Stub ESPNScraper whose ``read_schedule`` returns a non-empty frame so the
    runner reaches ``save_to_iceberg``. With ``guard_blocks=True`` the
    BaseScraper-level completeness guard is simulated by raising
    ``ReplaceGuardError`` — the runner must catch it and exit 3 (#583).

    Returns the scraper instance (not the class stub) so tests can inspect
    ``save_to_iceberg.call_args``; wrap it in ``MagicMock(return_value=...)``
    when handing it to ``_run_main``.
    """
    from scrapers.base.base_scraper import ReplaceGuardError

    df = pd.DataFrame({
        'league': ['ENG-Premier League'] * 10,
        'season': [2024] * 10,
        'match_date': ['2024-08-17'] * 10,
    })
    scraper = MagicMock()
    scraper.read_schedule.return_value = df
    # _standardize_schedule must return a real DataFrame: the success path does
    # ``len(df)`` for schedule_rows.
    scraper._standardize_schedule.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=3 rows < 90% of existing=380 for bronze.espn_schedule '
            '— refusing replace_partitions save (would shrink the partition)'
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.espn_schedule'
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


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


class TestEspnReplaceGuard:
    """#583: completeness-guard wiring in the ESPN runner.

    The guard arithmetic lives in ``BaseScraper.save_to_iceberg`` (covered by
    ``test_base_scraper.py``); here we cover the runner's *handling* — arm the
    guard on the normal path, map ``ReplaceGuardError`` to exit 3, and let
    ``--force-replace`` disarm it.
    """

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="espn_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_guard_refusal_exits_3(self, temp_output):
        """save_to_iceberg raises ReplaceGuardError → exit 3 + ESPN_REPLACE_GUARD
        marker (distinct from the exit-1 hard-failure path)."""
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
        assert any("ESPN_REPLACE_GUARD" in e for e in payload["errors"])

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
        # schedules store one row per match → raw COUNT(*), no replace_guard_key
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
