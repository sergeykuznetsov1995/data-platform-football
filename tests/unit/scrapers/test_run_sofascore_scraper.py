"""
Unit tests for ``dags/scripts/run_sofascore_scraper.py`` argparse hard-fail (#512).

A CLI parse error (unknown/typo'd flag, bad-typed value) must exit 1, NOT 2.
Exit 2 is the SofaScore fallback soft-success code that the DAG bash wrapper
maps to ``exit 0`` — so an exit-2 parse error would silently no-op the task.

Parsing fails before any lazy ``scrapers.sofascore`` import, so no stub is needed.
"""

from __future__ import annotations

import importlib
import sys


def _main_rc(argv: list) -> int:
    """Run ``run_sofascore_scraper.main()`` with ``argv`` and return its exit code."""
    sys.argv = ["run_sofascore_scraper.py"] + argv
    sys.modules.pop("dags.scripts.run_sofascore_scraper", None)
    mod = importlib.import_module("dags.scripts.run_sofascore_scraper")
    importlib.reload(mod)
    return mod.main()


class TestArgparseHardFail:
    def test_unknown_flag_returns_1_not_2(self):
        assert _main_rc(['--entity', 'schedule', '--bogus-flag', 'x']) == 1

    def test_bad_typed_season_returns_1(self):
        # --season is type=int; a non-int must hard-fail, not soft-fallback.
        assert _main_rc(['--season', 'notanumber']) == 1
