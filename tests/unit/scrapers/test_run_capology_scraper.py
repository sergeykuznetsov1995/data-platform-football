"""
Unit tests for ``dags/scripts/run_capology_scraper.py`` argparse hard-fail (#512).

A CLI parse error (unknown/typo'd flag, bad-typed value) must exit 1, NOT 2.
Exit 2 is the ``CAPOLOGY_FALLBACK`` soft-success code that the DAG bash wrapper
maps to ``exit 0`` — so an exit-2 parse error would silently no-op the task.

Parsing fails before any lazy ``scrapers.capology`` import, so no stub is needed.
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


def _main_rc(argv: list) -> int:
    """Run ``run_capology_scraper.main()`` with ``argv`` and return its exit code."""
    sys.argv = ["run_capology_scraper.py"] + argv
    sys.modules.pop("dags.scripts.run_capology_scraper", None)
    mod = importlib.import_module("dags.scripts.run_capology_scraper")
    importlib.reload(mod)
    return mod.main()


class TestArgparseHardFail:
    def test_unknown_flag_returns_1_not_2(self):
        assert _main_rc(['--entity', 'player_salaries', '--bogus-flag', 'x']) == 1

    def test_bad_typed_season_returns_1(self):
        # --season is type=int; a non-int must hard-fail, not soft-fallback.
        assert _main_rc(['--season', 'notanumber']) == 1


def _run_main(argv: list, scraper_cls) -> int:
    """Run ``main()`` with stubbed ``scrapers.capology`` modules.

    The runner lazily imports ``CapologyScraper`` and ``R0_2B_FALLBACK_MARKER``;
    both submodules are stubbed in ``sys.modules`` so no real scraper loads.
    ``scrapers.base.base_scraper`` (for ``ReplaceGuardError``) imports for real.
    """
    cap_pkg = MagicMock()
    cap_pkg.CapologyScraper = scraper_cls
    cap_scraper_mod = MagicMock()
    cap_scraper_mod.R0_2B_FALLBACK_MARKER = 'CAPOLOGY_FALLBACK'

    sys.argv = ["run_capology_scraper.py"] + argv
    with patch.dict(sys.modules, {
        "scrapers.capology": cap_pkg,
        "scrapers.capology.scraper": cap_scraper_mod,
    }):
        sys.modules.pop("dags.scripts.run_capology_scraper", None)
        mod = importlib.import_module("dags.scripts.run_capology_scraper")
        importlib.reload(mod)
        return mod.main()


def _salaries_scraper(*, guard_blocks: bool = False):
    """Stub whose read_player_salaries returns a non-empty frame so the runner
    reaches save_to_iceberg (3-key partition)."""
    from scrapers.base.base_scraper import ReplaceGuardError

    df = pd.DataFrame({
        'league': ['ENG-Premier League'] * 10,
        'season': [2024] * 10,
        'currency': ['GBP'] * 10,
        'player_slug': [f'p{i}' for i in range(10)],
    })
    scraper = MagicMock()
    scraper.read_player_salaries.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=3 rows < 90% of existing=380 for bronze.capology_player_salaries '
            '— refusing replace_partitions save (would shrink the partition)'
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.capology_player_salaries'
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


def _product_scraper(*, guard_blocks: bool = False):
    """Stub whose read_team_payrolls returns a non-empty frame so _run_product
    reaches save_to_iceberg (2-key partition)."""
    from scrapers.base.base_scraper import ReplaceGuardError

    df = pd.DataFrame({
        'league': ['ENG-Premier League'] * 10,
        'season': [2024] * 10,
        'club_slug': [f'c{i}' for i in range(10)],
    })
    scraper = MagicMock()
    scraper.read_team_payrolls.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=1 rows < 90% of existing=20 for bronze.capology_team_payrolls '
            '— refusing replace_partitions save (would shrink the partition)'
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.capology_team_payrolls'
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


class TestCapologyReplaceGuard:
    """#583: completeness-guard wiring in the Capology runner.

    Covers both save sites: ``_run_player_salaries`` (3-key
    ``league/season/currency``) and ``_run_product`` (2-key ``league/season``).
    The guard arithmetic lives in ``BaseScraper.save_to_iceberg`` (covered by
    ``test_base_scraper.py``); here we cover the runner's handling.
    """

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="capology_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_salaries_guard_refusal_exits_3(self, temp_output):
        """ReplaceGuardError on the salaries save → exit 3 +
        CAPOLOGY_REPLACE_GUARD marker (distinct from hard-fail 1 / fallback 2)."""
        scraper = _salaries_scraper(guard_blocks=True)

        rc = _run_main(
            ["--entity", "player_salaries", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 3
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("CAPOLOGY_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_salaries_normal_path_arms_guard_exits_0(self, temp_output):
        """Non-force salaries run arms min_replace_ratio=0.9 on the 3-key save."""
        scraper = _salaries_scraper()

        rc = _run_main(
            ["--entity", "player_salaries", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] == 0.9
        assert kwargs["replace_partitions"] == ["league", "season", "currency"]
        assert "replace_guard_key" not in kwargs

    @pytest.mark.unit
    def test_salaries_force_replace_disarms_guard(self, temp_output):
        """--force-replace must pass min_replace_ratio=None to the salaries save."""
        scraper = _salaries_scraper()

        rc = _run_main(
            ["--entity", "player_salaries", "--force-replace",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] is None

    @pytest.mark.unit
    def test_product_normal_path_arms_guard_exits_0(self, temp_output):
        """_run_product (team_payrolls) arms min_replace_ratio=0.9 on the
        2-key league/season save."""
        scraper = _product_scraper()

        rc = _run_main(
            ["--entity", "team_payrolls", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] == 0.9
        assert kwargs["replace_partitions"] == ["league", "season"]

    @pytest.mark.unit
    def test_product_guard_refusal_exits_3(self, temp_output):
        """ReplaceGuardError on the product save → exit 3 + marker."""
        scraper = _product_scraper(guard_blocks=True)

        rc = _run_main(
            ["--entity", "team_payrolls", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 3
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("CAPOLOGY_REPLACE_GUARD" in e for e in payload["errors"])
