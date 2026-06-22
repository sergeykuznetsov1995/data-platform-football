"""
Unit tests for ``dags/scripts/run_sofascore_scraper.py`` argparse hard-fail (#512).

A CLI parse error (unknown/typo'd flag, bad-typed value) must exit 1, NOT 2.
Exit 2 is the SofaScore fallback soft-success code that the DAG bash wrapper
maps to ``exit 0`` — so an exit-2 parse error would silently no-op the task.

Parsing fails before any lazy ``scrapers.sofascore`` import, so no stub is needed.
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


def _run_main(argv: list, scraper_cls, *, resolver_ids=None) -> int:
    """Run ``main()`` with stubbed ``scrapers.sofascore[.scraper]`` modules.

    ``scrapers.base.base_scraper`` (for ``ReplaceGuardError``) imports for real.
    ``resolver_ids`` patches ``_resolve_match_ids_from_bronze`` so the
    player_ratings path skips its Trino lookup and reaches save_to_iceberg.
    """
    so_pkg = MagicMock()
    so_pkg.SofaScoreScraper = scraper_cls
    so_scraper_mod = MagicMock()
    so_scraper_mod.R0_2B_FALLBACK_MARKER = 'R0_2B_FALLBACK'

    sys.argv = ["run_sofascore_scraper.py"] + argv
    with patch.dict(sys.modules, {
        "scrapers.sofascore": so_pkg,
        "scrapers.sofascore.scraper": so_scraper_mod,
    }):
        sys.modules.pop("dags.scripts.run_sofascore_scraper", None)
        mod = importlib.import_module("dags.scripts.run_sofascore_scraper")
        importlib.reload(mod)
        if resolver_ids is not None:
            mod._resolve_match_ids_from_bronze = lambda *a, **k: resolver_ids
        return mod.main()


def _legacy_scraper(*, guard_blocks: bool = False):
    """Stub for the default 'all' path: read_schedule + read_league_table both
    return non-empty frames so both 2-key saves run."""
    from scrapers.base.base_scraper import ReplaceGuardError

    df = pd.DataFrame({
        'league': ['ENG-Premier League'] * 10,
        'season': [2024] * 10,
        'x': list(range(10)),
    })
    scraper = MagicMock()
    scraper.read_schedule.return_value = df
    scraper.read_league_table.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=1 rows < 90% of existing=380 for bronze.sofascore_schedule '
            '— refusing replace_partitions save (would shrink the partition)'
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.sofascore_schedule'
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


def _ratings_scraper(*, guard_blocks: bool = False):
    """Stub for the player_ratings path (single 2-key save at 16-sp indent)."""
    from scrapers.base.base_scraper import ReplaceGuardError

    df = pd.DataFrame({
        'league': ['ENG-Premier League'] * 10,
        'season': [2024] * 10,
        'match_id': list(range(10)),
    })
    scraper = MagicMock()
    scraper.read_player_ratings.return_value = df
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=1 rows < 90% of existing=200 for bronze.sofascore_player_ratings '
            '— refusing replace_partitions save (would shrink the partition)'
        )
    else:
        scraper.save_to_iceberg.return_value = (
            'iceberg.bronze.sofascore_player_ratings'
        )
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


class TestSofascoreReplaceGuard:
    """#583: completeness-guard wiring in the SofaScore runner.

    Covers the default 'all' path (``_run_legacy``, two 2-key saves) and the
    ``player_ratings`` path (single save). The guard arithmetic lives in
    ``BaseScraper.save_to_iceberg`` (covered by ``test_base_scraper.py``); here
    we cover the runner's handling. The append-only event endpoint is NOT
    guarded (no replace_partitions, #69) and is not exercised here.
    """

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofascore_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_legacy_guard_refusal_exits_3(self, temp_output):
        """ReplaceGuardError on a legacy save → exit 3 + SOFASCORE_REPLACE_GUARD
        marker (distinct from the exit-1 hard-failure path)."""
        scraper = _legacy_scraper(guard_blocks=True)

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 3
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("SOFASCORE_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_legacy_normal_path_arms_guard_exits_0(self, temp_output):
        """Non-force 'all' run arms min_replace_ratio=0.9 on the 2-key saves."""
        scraper = _legacy_scraper()

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
    def test_legacy_force_replace_disarms_guard(self, temp_output):
        """--force-replace must pass min_replace_ratio=None to the legacy saves."""
        scraper = _legacy_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] is None

    @pytest.mark.unit
    def test_player_ratings_guard_refusal_exits_3(self, temp_output):
        """player_ratings save refusal → exit 3 + marker (16-sp save path)."""
        scraper = _ratings_scraper(guard_blocks=True)

        rc = _run_main(
            ["--entity", "player_ratings", "--league", "ENG-Premier League",
             "--season", "2024", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=[1, 2, 3],
        )

        assert rc == 3
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("SOFASCORE_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_player_ratings_normal_path_arms_guard_exits_0(self, temp_output):
        """player_ratings non-force run arms min_replace_ratio=0.9."""
        scraper = _ratings_scraper()

        rc = _run_main(
            ["--entity", "player_ratings", "--league", "ENG-Premier League",
             "--season", "2024", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=[1, 2, 3],
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs["min_replace_ratio"] == 0.9
        assert kwargs["replace_partitions"] == ["league", "season"]


class TestPlayerRatingsCaptureFallback:
    """#757 B2: when bronze.sofascore_schedule is empty (fresh season — the
    soccerdata schedule is Turnstile-blocked), the runner resolves finished
    match_ids via the scraper's Camoufox capture resolver before declaring
    R0.2B fallback.
    """

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofascore_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_empty_bronze_resolves_via_capture_then_saves(self, temp_output):
        # Arrange — bronze returns [] for both season forms; capture finds ids.
        scraper = _ratings_scraper()
        scraper.resolve_finished_match_ids_via_capture.return_value = ['101', '103']

        # Act
        rc = _run_main(
            ["--entity", "player_ratings", "--league", "ENG-Premier League",
             "--season", "2024", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=[],
        )

        # Assert — capture drove the match_ids into read_player_ratings; save ran.
        assert rc == 0
        scraper.resolve_finished_match_ids_via_capture.assert_called_once()
        assert scraper.read_player_ratings.call_args.kwargs['match_ids'] == ['101', '103']

    @pytest.mark.unit
    def test_empty_bronze_and_empty_capture_exits_2(self, temp_output):
        # Arrange — neither bronze nor capture yields match_ids (off-season).
        scraper = _ratings_scraper()
        scraper.resolve_finished_match_ids_via_capture.return_value = []

        # Act
        rc = _run_main(
            ["--entity", "player_ratings", "--league", "ENG-Premier League",
             "--season", "2024", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=[],
        )

        # Assert — graceful R0.2B fallback (soft success), read_player_ratings skipped.
        assert rc == 2
        scraper.read_player_ratings.assert_not_called()
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['fallback'] is True
        assert payload['fallback_reason'] == 'no_match_ids'
