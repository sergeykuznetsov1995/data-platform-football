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


def _import_runner():
    """Fresh import of the runner module (module-level has no heavy imports,
    so no scraper stubs needed — used to exercise pure helpers like
    ``_window_offset``)."""
    sys.modules.pop("dags.scripts.run_transfermarkt_scraper", None)
    return importlib.import_module("dags.scripts.run_transfermarkt_scraper")


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
# Rotating window + per-player upsert (issue #620)
# ---------------------------------------------------------------------------

class TestRosterRotationUpsert:
    @pytest.mark.parametrize('entity', ['market_value_history', 'transfers'])
    def test_dependent_entities_upsert_by_player_id(self, temp_output, entity):
        # transfers / mv_history must delete+reinsert ONLY the scraped window's
        # players so previous windows accumulate (#620). That means player_id
        # joins the replace_partitions key.
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', entity, '--limit', '100', '--output', temp_output],
            scraper,
        )
        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs['replace_partitions'] == ['league', 'season', 'player_id']
        assert kwargs['replace_guard_key'] == 'player_id'

    def test_players_partition_unchanged(self, temp_output):
        # Anchor entity stays a whole-partition replace (full crawl).
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs['replace_partitions'] == ['league', 'season']

    def test_window_offset_helper_increments_per_run(self):
        mod = _import_runner()
        # Two dates 7 days apart (one weekly DAG run) → offset differs by 1.
        assert mod._window_offset('2026-06-29') - mod._window_offset('2026-06-22') == 1
        # No date → today-based int (callable standalone).
        assert isinstance(mod._window_offset(None), int)

    @pytest.mark.parametrize(
        'entity,reader',
        [
            ('market_value_history', 'read_market_value_history'),
            ('transfers', 'read_transfers'),
        ],
    )
    def test_as_of_date_forwards_window_offset(self, temp_output, entity, reader):
        scraper = _build_scraper(df=_players_df(3))
        rc = _run_main(
            ['--entity', entity, '--limit', '100',
             '--as-of-date', '2026-06-22', '--output', temp_output],
            scraper,
        )
        assert rc == 0
        expected = _import_runner()._window_offset('2026-06-22')
        assert getattr(scraper, reader).call_args.kwargs['window_offset'] == expected


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
        # No endpoint error recorded → genuine empty_payload → soft exit 2.
        scraper = _build_scraper(df=pd.DataFrame())
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 2
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert results['fallback'] is True

    def test_empty_frame_http_block_exits_1_red(self, temp_output):
        """#790: an empty frame caused by an http block (403) is a real failure
        → exit 1 (red), NOT the soft exit 2 of a genuine empty payload."""
        scraper = _build_scraper(df=pd.DataFrame())
        scraper._last_endpoint_error = {'status': 403}
        rc = _run_main(
            ['--entity', 'players', '--output', temp_output], scraper,
        )
        assert rc == 1
        scraper.save_to_iceberg.assert_not_called()
        results = _load_results(temp_output)
        assert results['fallback'] is True
        assert results['fallback_reason'] == 'http_403'


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
