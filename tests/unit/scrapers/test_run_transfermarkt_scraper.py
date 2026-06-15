"""
Unit tests for ``dags/scripts/run_transfermarkt_scraper.py``.

The replace-partitions completeness guard (#484/#486) was generalised into
``BaseScraper.save_to_iceberg`` in #513: the runner now passes
``min_replace_ratio`` + ``replace_guard_key='player_id'`` and the guard raises
``ReplaceGuardError`` when the scraped frame would shrink the existing bronze
partition below 90%. These tests cover the runner's *handling* of that error
(map to exit 3 + ``TM_REPLACE_GUARD`` marker) and the ``--dry-run`` /
``--force-replace`` flags — the guard arithmetic itself is unit-tested in
``test_base_scraper.py``.

The runner lazily imports THREE modules inside each ``_run_*``:
``scrapers.base.base_scraper`` (``ReplaceGuardError`` — the real class, NOT
stubbed), ``scrapers.transfermarkt`` (the class) and
``scrapers.transfermarkt.scraper`` (``R0_2B_FALLBACK_MARKER``) — the latter two
are stubbed via ``patch.dict(sys.modules)`` following the understat-runner test
pattern. ``R0_2B_FALLBACK_MARKER`` must be the real string: it is f-stringed
into ``results['errors']`` and JSON-dumped.
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

def _players_df(n: int) -> pd.DataFrame:
    return pd.DataFrame({'player_id': [str(i) for i in range(n)]})


def _build_scraper(*, df: pd.DataFrame, guard_blocks: bool = False):
    """Stub TransfermarktScraper context-manager.

    Every ``read_*`` returns ``df``. With ``guard_blocks=True`` the (now
    BaseScraper-level) completeness guard is simulated by making
    ``save_to_iceberg`` raise ``ReplaceGuardError`` — the runner must catch it
    and exit 3 (#513).
    """
    from scrapers.base.base_scraper import ReplaceGuardError

    scraper = MagicMock()
    scraper._last_endpoint_error = None
    scraper.read_players.return_value = df
    scraper.read_market_value_history.return_value = df
    scraper.read_transfers.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=3 distinct player_id < 90% of existing=600 for '
            'bronze.transfermarkt_players — refusing replace_partitions save'
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.stub_table'
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


def _run_main(args: list, scraper) -> int:
    """Execute ``run_transfermarkt_scraper.main()`` with stubbed scraper."""
    stub_pkg = MagicMock()
    stub_pkg.TransfermarktScraper = MagicMock(return_value=scraper)

    stub_scraper_mod = MagicMock()
    stub_scraper_mod.R0_2B_FALLBACK_MARKER = 'TM_FALLBACK'

    sys.argv = ["run_transfermarkt_scraper.py"] + args

    with patch.dict(
        sys.modules,
        {
            "scrapers.transfermarkt": stub_pkg,
            "scrapers.transfermarkt.scraper": stub_scraper_mod,
        },
    ):
        sys.modules.pop("dags.scripts.run_transfermarkt_scraper", None)
        mod = importlib.import_module("dags.scripts.run_transfermarkt_scraper")
        importlib.reload(mod)
        return mod.main()


@pytest.fixture
def temp_output():
    fd, path = tempfile.mkstemp(suffix=".json", prefix="transfermarkt_")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


def _load_results(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Completeness guard handling (#484 / #486, generalised #513)
# ---------------------------------------------------------------------------

class TestReplaceGuard:
    def test_guard_error_exits_3(self, temp_output):
        # save_to_iceberg raises ReplaceGuardError → runner maps it to exit 3.
        scraper = _build_scraper(df=_players_df(3), guard_blocks=True)
        rc = _run_main(
            ['--entity', 'players', '--limit', '3', '--output', temp_output],
            scraper,
        )
        assert rc == 3
        scraper.save_to_iceberg.assert_called_once()
        results = _load_results(temp_output)
        assert any('TM_REPLACE_GUARD' in e for e in results['errors'])
        # what was rejected is visible to validate_data / Telegram
        assert results['rows'] == 3
        assert results['players_with_rows'] == 3

    def test_guard_passes_exits_0(self, temp_output):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_called_once()

    def test_guard_params_passed_to_save(self, temp_output):
        # Non-force path must arm the guard: ratio 0.9, distinct player_id.
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs['min_replace_ratio'] == 0.9
        assert kwargs['replace_guard_key'] == 'player_id'
        assert kwargs['replace_partitions'] == ['league', 'season']

    @pytest.mark.parametrize('entity', ['market_value_history', 'transfers'])
    def test_guard_error_exits_3_dependent_entities(self, temp_output, entity):
        scraper = _build_scraper(df=_players_df(3), guard_blocks=True)
        rc = _run_main(
            ['--entity', entity, '--limit', '3', '--output', temp_output],
            scraper,
        )
        assert rc == 3
        scraper.save_to_iceberg.assert_called_once()
        results = _load_results(temp_output)
        assert any('TM_REPLACE_GUARD' in e for e in results['errors'])


# ---------------------------------------------------------------------------
# --dry-run / --force-replace
# ---------------------------------------------------------------------------

class TestRunnerFlags:
    def test_dry_run_skips_save(self, temp_output):
        scraper = _build_scraper(df=_players_df(3), guard_blocks=True)
        rc = _run_main(
            ['--entity', 'players', '--limit', '3', '--dry-run',
             '--output', temp_output],
            scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert results['dry_run'] is True
        assert results['rows'] == 3

    def test_force_replace_disables_guard(self, temp_output):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--limit', '3', '--force-replace',
             '--output', temp_output],
            scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_called_once()
        # --force-replace must turn the guard off at the save call.
        assert scraper.save_to_iceberg.call_args.kwargs['min_replace_ratio'] is None


# ---------------------------------------------------------------------------
# Pre-existing fallback path stays intact
# ---------------------------------------------------------------------------

class TestFallbackPath:
    def test_empty_frame_exits_2_without_save(self, temp_output):
        scraper = _build_scraper(df=pd.DataFrame())
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 2
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert results['fallback'] is True


# ---------------------------------------------------------------------------
# argparse hard-fail (#512)
# ---------------------------------------------------------------------------

class TestArgparseHardFail:
    """A CLI parse error must exit 1 (hard failure), NOT argparse's default 2.

    Exit 2 is the ``TM_FALLBACK`` soft-success code the DAG bash wrapper maps
    to ``exit 0`` — so a flag typo at exit 2 would silently no-op the task.
    """

    def test_unknown_flag_returns_1_not_2(self, temp_output):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--bogus-flag', 'x', '--output', temp_output],
            scraper,
        )
        assert rc == 1
        scraper.read_players.assert_not_called()
        scraper.save_to_iceberg.assert_not_called()

    def test_bad_typed_season_returns_1(self, temp_output):
        # --season is type=int; a non-int must hard-fail, not soft-fallback.
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--season', 'notanumber', '--output', temp_output],
            scraper,
        )
        assert rc == 1
        scraper.read_players.assert_not_called()
