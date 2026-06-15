"""
Unit tests for ``dags/scripts/run_transfermarkt_scraper.py``.

Covers the replace-partitions completeness guard (#484/#486): before
saving with ``replace_partitions=['league','season']`` the runner compares
the distinct-player count of the scraped frame against the existing bronze
partition and refuses the replace (exit 3) when the new frame would shrink
it below 90%. Also covers the ``--dry-run`` / ``--force-replace`` flags.

The runner lazily imports TWO modules inside each ``_run_*``:
``scrapers.transfermarkt`` (the class) and ``scrapers.transfermarkt.scraper``
(``R0_2B_FALLBACK_MARKER``) — both are stubbed via ``patch.dict(sys.modules)``
following the understat-runner test pattern. ``R0_2B_FALLBACK_MARKER`` must
be the real string: it is f-stringed into ``results['errors']`` and JSON-dumped.
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


def _build_scraper(*, df: pd.DataFrame, existing_players):
    """Stub TransfermarktScraper context-manager.

    Every ``read_*`` returns ``df``; ``count_bronze_partition_players``
    returns ``existing_players``.
    """
    scraper = MagicMock()
    scraper._last_endpoint_error = None
    scraper.read_players.return_value = df
    scraper.read_market_value_history.return_value = df
    scraper.read_transfers.return_value = df
    scraper.count_bronze_partition_players.return_value = existing_players
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
# Completeness guard (#484 / #486)
# ---------------------------------------------------------------------------

class TestReplaceGuard:
    def test_guard_blocks_when_new_below_90pct_of_existing(self, temp_output):
        # --limit 3 smoke run against a 600-player partition (#486).
        scraper = _build_scraper(df=_players_df(3), existing_players=600)
        rc = _run_main(
            ['--entity', 'players', '--limit', '3', '--output', temp_output],
            scraper,
        )
        assert rc == 3
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert any('TM_REPLACE_GUARD' in e for e in results['errors'])
        # what was rejected is visible to validate_data / Telegram
        assert results['rows'] == 3
        assert results['players_with_rows'] == 3

    def test_guard_passes_when_counts_match(self, temp_output):
        scraper = _build_scraper(df=_players_df(3), existing_players=3)
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_called_once()

    def test_guard_boundary_exactly_90pct_passes(self, temp_output):
        # 90 < 0.9 * 100 is False — exact 90% is allowed.
        scraper = _build_scraper(df=_players_df(90), existing_players=100)
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_called_once()

    def test_guard_skipped_when_count_unavailable(self, temp_output):
        # Trino down / table missing → count is None → proceed, flag it.
        scraper = _build_scraper(df=_players_df(3), existing_players=None)
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_called_once()
        results = _load_results(temp_output)
        assert results['guard_skipped'] == 'count_unavailable'

    def test_guard_passes_on_empty_partition(self, temp_output):
        # First run for a (league, season) — nothing to protect.
        scraper = _build_scraper(df=_players_df(3), existing_players=0)
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_called_once()

    @pytest.mark.parametrize('entity', ['market_value_history', 'transfers'])
    def test_guard_blocks_dependent_entities(self, temp_output, entity):
        scraper = _build_scraper(df=_players_df(3), existing_players=100)
        rc = _run_main(
            ['--entity', entity, '--limit', '3', '--output', temp_output],
            scraper,
        )
        assert rc == 3
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert any('TM_REPLACE_GUARD' in e for e in results['errors'])


# ---------------------------------------------------------------------------
# --dry-run / --force-replace
# ---------------------------------------------------------------------------

class TestRunnerFlags:
    def test_dry_run_skips_save_and_guard(self, temp_output):
        scraper = _build_scraper(df=_players_df(3), existing_players=600)
        rc = _run_main(
            ['--entity', 'players', '--limit', '3', '--dry-run',
             '--output', temp_output],
            scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_not_called()
        scraper.count_bronze_partition_players.assert_not_called()
        results = _load_results(temp_output)
        assert results['dry_run'] is True
        assert results['rows'] == 3

    def test_force_replace_bypasses_guard(self, temp_output):
        scraper = _build_scraper(df=_players_df(3), existing_players=600)
        rc = _run_main(
            ['--entity', 'players', '--limit', '3', '--force-replace',
             '--output', temp_output],
            scraper,
        )
        assert rc == 0
        scraper.save_to_iceberg.assert_called_once()
        # forced path must not waste a Trino round-trip
        scraper.count_bronze_partition_players.assert_not_called()


# ---------------------------------------------------------------------------
# Pre-existing fallback path stays intact
# ---------------------------------------------------------------------------

class TestFallbackPath:
    def test_empty_frame_exits_2_without_save(self, temp_output):
        scraper = _build_scraper(df=pd.DataFrame(), existing_players=600)
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
        scraper = _build_scraper(df=_players_df(3), existing_players=3)
        rc = _run_main(
            ['--entity', 'players', '--bogus-flag', 'x', '--output', temp_output],
            scraper,
        )
        assert rc == 1
        scraper.read_players.assert_not_called()
        scraper.save_to_iceberg.assert_not_called()

    def test_bad_typed_season_returns_1(self, temp_output):
        # --season is type=int; a non-int must hard-fail, not soft-fallback.
        scraper = _build_scraper(df=_players_df(3), existing_players=3)
        rc = _run_main(
            ['--entity', 'players', '--season', 'notanumber', '--output', temp_output],
            scraper,
        )
        assert rc == 1
        scraper.read_players.assert_not_called()
