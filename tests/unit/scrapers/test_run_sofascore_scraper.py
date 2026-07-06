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


def _run_main(argv: list, scraper_cls, *, resolver_ids=None,
              existing_schedule=None, existing_capture_ids=None,
              existing_partition=None) -> int:
    """Run ``main()`` with stubbed ``scrapers.sofascore[.scraper]`` modules.

    ``scrapers.base.base_scraper`` (for ``ReplaceGuardError``) imports for real.
    ``resolver_ids`` patches ``_resolve_match_ids_from_bronze`` so the
    player_ratings path skips its Trino lookup and reaches save_to_iceberg.
    ``_read_existing_schedule`` is always patched (it would otherwise hit Trino)
    to ``existing_schedule`` (default: an empty frame → captured rows save
    as-is) so the #761 schedule merge runs offline + deterministically.
    ``existing_capture_ids`` / ``existing_partition`` patch the #842
    match_capture skip-existing probe (``_existing_match_ids_in_bronze``) and
    partition merge read (``_read_existing_partition``) — defaults (empty set /
    empty frame) mean "nothing in bronze yet": no skip, merge is a no-op.
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
        _existing = pd.DataFrame() if existing_schedule is None else existing_schedule
        mod._read_existing_schedule = lambda *a, **k: _existing
        _ids = set() if existing_capture_ids is None else set(existing_capture_ids)
        mod._existing_match_ids_in_bronze = lambda *a, **k: _ids
        _part = pd.DataFrame() if existing_partition is None else existing_partition
        mod._read_existing_partition = lambda *a, **k: _part
        return mod.main()


def _legacy_scraper(*, guard_blocks: bool = False):
    """Stub for the default 'all' path: read_schedule + read_league_table both
    return non-empty frames so both 2-key saves run."""
    from scrapers.base.base_scraper import ReplaceGuardError

    df = pd.DataFrame({
        'league': ['ENG-Premier League'] * 10,
        'season': [2024] * 10,
        'game_id': list(range(10)),
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


def _match_capture_scraper(*, guard_blocks: bool = False, empty: bool = False):
    """Stub for the #751/#753 match_capture path: read_match_capture returns all
    five frames (player_ratings, event_player_stats, match_stats, event_shotmap,
    venue) from one capture pass."""
    from scrapers.base.base_scraper import ReplaceGuardError

    if empty:
        frames = {'player_ratings': pd.DataFrame(),
                  'event_player_stats': pd.DataFrame(),
                  'match_stats': pd.DataFrame(),
                  'event_shotmap': pd.DataFrame(),
                  'venue': pd.DataFrame()}
    else:
        def _df():
            return pd.DataFrame({'league': ['ENG-Premier League'] * 4,
                                 'season': ['2526'] * 4,
                                 'match_id': ['1', '1', '2', '2']})
        # venue is keyed by game_id (one row per match), not match_id (#753).
        venue_df = pd.DataFrame({'league': ['ENG-Premier League'] * 2,
                                 'season': ['2526'] * 2,
                                 'game_id': [1, 2],
                                 'stadium': ['Etihad Stadium', 'Anfield']})
        frames = {'player_ratings': _df(), 'event_player_stats': _df(),
                  'match_stats': _df(), 'event_shotmap': _df(),
                  'venue': venue_df}

    scraper = MagicMock()
    # No HTTP error recorded → an empty capture classifies as a genuine
    # empty_payload (soft exit 2), not an http block (#790). Block tests set
    # this explicitly to a status dict.
    scraper._last_lineup_error = None
    scraper.read_match_capture.return_value = frames
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=1 rows < 90% of existing=380 — refusing replace_partitions save')
    else:
        scraper.save_to_iceberg.return_value = (
            'iceberg.bronze.sofascore_player_ratings')
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


class TestMatchCaptureRunner:
    """#751 PR1: the consolidated match_capture entity writes BOTH
    player_ratings and event_player_stats from one capture pass, each full-state
    (replace_partitions + completeness guard)."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofascore_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_normal_path_saves_five_tables_arms_guard(self, temp_output):
        scraper = _match_capture_scraper()
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
        )
        assert rc == 0
        # one save per table — all five full-state with the guard (#751 PR2, #753).
        assert scraper.save_to_iceberg.call_count == 5
        tables = [c.kwargs['table_name']
                  for c in scraper.save_to_iceberg.call_args_list]
        assert set(tables) == {'sofascore_player_ratings',
                               'sofascore_event_player_stats',
                               'sofascore_match_stats',
                               'sofascore_event_shotmap',
                               'sofascore_venue'}
        for c in scraper.save_to_iceberg.call_args_list:
            assert c.kwargs['replace_partitions'] == ['league', 'season']
            assert c.kwargs['min_replace_ratio'] == 0.9
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['rows'] == 4 and payload['eps_rows'] == 4
        assert payload['match_stats_rows'] == 4 and payload['shotmap_rows'] == 4
        # #753: venue is keyed by game_id (one row per match) → 2 rows / 2 matches.
        assert payload['venue_rows'] == 2 and payload['venue_matches'] == 2

    @pytest.mark.unit
    def test_traffic_stats_land_in_payload(self, temp_output):
        # #879: match_capture must report get_traffic_stats() (incl. the
        # camoufox share) so the canary / PROXY_TRAFFIC tooling sees the
        # residential spend of the heaviest SofaScore path.
        scraper = _match_capture_scraper()
        scraper.get_traffic_stats.return_value = {
            'proxy_response_bytes': 2 * 1024 * 1024,
            'proxy_response_mb': 2.0,
            'camoufox_bytes': 2 * 1024 * 1024,
            'camoufox_mb': 2.0,
            'requests': 4,
            'top_traffic_urls': [],
        }
        with patch.dict(sys.modules, {'utils.proxy_traffic': MagicMock()}):
            rc = _run_main(
                ["--entity", "match_capture", "--league", "ENG-Premier League",
                 "--season", "2025", "--output", temp_output],
                MagicMock(return_value=scraper),
                resolver_ids=['1', '2'],
            )
        assert rc == 0
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['traffic']['proxy_response_mb'] == 2.0
        assert payload['traffic']['camoufox_mb'] == 2.0

    @pytest.mark.unit
    def test_guard_refusal_exits_3(self, temp_output):
        scraper = _match_capture_scraper(guard_blocks=True)
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
        )
        assert rc == 3
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("SOFASCORE_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_force_replace_disarms_guard(self, temp_output):
        scraper = _match_capture_scraper()
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
        )
        assert rc == 0
        for c in scraper.save_to_iceberg.call_args_list:
            assert c.kwargs['min_replace_ratio'] is None

    @pytest.mark.unit
    def test_empty_capture_exits_2_no_save(self, temp_output):
        scraper = _match_capture_scraper(empty=True)
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
        )
        assert rc == 2
        scraper.save_to_iceberg.assert_not_called()

    @pytest.mark.unit
    def test_http_block_capture_exits_1_red(self, temp_output):
        """#790: an empty capture caused by an http block (403) is a real
        failure → exit 1 (red), NOT the soft exit 2 of a genuine empty."""
        scraper = _match_capture_scraper(empty=True)
        scraper._last_lineup_error = {'status': 403}
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
        )
        assert rc == 1
        scraper.save_to_iceberg.assert_not_called()
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['fallback_reason'] == 'http_403'

    @pytest.mark.unit
    def test_skip_existing_captures_only_new_matches(self, temp_output):
        """#842: matches already in bronze.sofascore_player_ratings are not
        re-captured — the capture call receives only the NEW match_ids."""
        scraper = _match_capture_scraper()
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2', '3'],
            existing_capture_ids={'3'},
        )
        assert rc == 0
        assert (scraper.read_match_capture.call_args.kwargs['match_ids']
                == ['1', '2'])
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['matches_total'] == 3
        assert payload['matches_skipped_existing'] == 1

    @pytest.mark.unit
    def test_all_existing_noops_without_opening_scraper(self, temp_output):
        """#842: nothing new → exit 0 BEFORE the scraper session is even
        constructed (zero proxy spend), no capture, no save."""
        scraper_cls = MagicMock()
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            scraper_cls,
            resolver_ids=['1', '2'],
            existing_capture_ids={'1', '2'},
        )
        assert rc == 0
        scraper_cls.assert_not_called()
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['matches_total'] == 2
        assert payload['matches_skipped_existing'] == 2
        assert payload['rows'] == 0 and payload['fallback'] is False

    @pytest.mark.unit
    def test_force_replace_recaptures_existing(self, temp_output):
        """#842: --force-replace restores the old full re-capture — the
        skip-existing probe must not filter anything."""
        scraper = _match_capture_scraper()
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
            existing_capture_ids={'1', '2'},
        )
        assert rc == 0
        assert (scraper.read_match_capture.call_args.kwargs['match_ids']
                == ['1', '2'])
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['matches_skipped_existing'] == 0

    @pytest.mark.unit
    def test_merge_unions_with_existing_partition(self, temp_output):
        """#842: the captured frame (new matches only) is unioned with the
        existing partition before the replace_partitions save — the saved
        frame never shrinks, so the completeness guard passes."""
        existing = pd.DataFrame({
            'league': ['ENG-Premier League'] * 4, 'season': ['2526'] * 4,
            'match_id': ['8', '8', '9', '9'],
        })
        scraper = _match_capture_scraper()
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
            existing_partition=existing,
        )
        assert rc == 0
        ratings_call = next(
            c for c in scraper.save_to_iceberg.call_args_list
            if c.kwargs['table_name'] == 'sofascore_player_ratings'
        )
        saved = ratings_call.kwargs['df']
        assert sorted(set(saved['match_id'])) == ['1', '2', '8', '9']
        assert len(saved) == 8   # 4 existing + 4 captured — never shrinks
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['rows'] == 8


def _player_capture_scraper(
    *, guard_blocks: bool = False, empty: bool = False, season_empty: bool = False,
):
    """Stub for the #751 PR3 + PR3b player_capture path: read_player_capture
    returns BOTH the player_profile and player_season_stats frames from one
    per-player capture."""
    from scrapers.base.base_scraper import ReplaceGuardError

    if empty:
        frames = {'player_profile': pd.DataFrame(),
                  'player_season_stats': pd.DataFrame()}
    else:
        season_df = pd.DataFrame() if season_empty else pd.DataFrame({
            'league': ['ENG-Premier League'] * 2, 'season': ['2526'] * 2,
            'player_id': ['1', '2']})
        frames = {
            'player_profile': pd.DataFrame({
                'league': ['ENG-Premier League'] * 3,
                'season': ['2526'] * 3,
                'player_id': ['1', '2', '3']}),
            'player_season_stats': season_df,
        }

    scraper = MagicMock()
    # No HTTP error recorded → an empty capture classifies as a genuine
    # empty_payload (soft exit 2), not an http block (#790). Block tests set
    # this explicitly to a status dict.
    scraper._last_lineup_error = None
    scraper.read_player_capture.return_value = frames
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'new=1 rows < 90% of existing=520 — refusing replace_partitions save')
    else:
        scraper.save_to_iceberg.return_value = (
            'iceberg.bronze.sofascore_player_profile')
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


class TestPlayerCaptureRunner:
    """#751 PR3 + PR3b: the player_capture entity writes player_profile AND
    player_season_stats from one per-player capture pass, full-state
    (replace_partitions + completeness guard). Season-stats is secondary — its
    save is skipped (not a fallback) when the picker captured nothing."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofascore_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_normal_path_saves_both_tables_arms_guard(self, temp_output):
        scraper = _player_capture_scraper()
        rc = _run_main(
            ["--entity", "player_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
        )
        assert rc == 0
        assert scraper.save_to_iceberg.call_count == 2
        saved = {c.kwargs['table_name']: c.kwargs
                 for c in scraper.save_to_iceberg.call_args_list}
        assert set(saved) == {'sofascore_player_profile',
                              'sofascore_player_season_stats'}
        for kwargs in saved.values():
            assert kwargs['replace_partitions'] == ['league', 'season']
            assert kwargs['min_replace_ratio'] == 0.9
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['rows'] == 3
        assert payload['season_stats_rows'] == 2

    @pytest.mark.unit
    def test_season_empty_skips_second_save(self, temp_output):
        # Picker captured no EPL overall for anyone → profile still saved, but
        # the season-stats table is NOT touched (don't wipe a good partition).
        scraper = _player_capture_scraper(season_empty=True)
        rc = _run_main(
            ["--entity", "player_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
        )
        assert rc == 0
        assert scraper.save_to_iceberg.call_count == 1
        assert scraper.save_to_iceberg.call_args_list[0].kwargs['table_name'] == \
            'sofascore_player_profile'
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['rows'] == 3
        assert payload['season_stats_rows'] == 0

    @pytest.mark.unit
    def test_guard_refusal_exits_3(self, temp_output):
        scraper = _player_capture_scraper(guard_blocks=True)
        rc = _run_main(
            ["--entity", "player_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
        )
        assert rc == 3
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("SOFASCORE_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_force_replace_disarms_guard(self, temp_output):
        scraper = _player_capture_scraper()
        rc = _run_main(
            ["--entity", "player_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
        )
        assert rc == 0
        for c in scraper.save_to_iceberg.call_args_list:
            assert c.kwargs['min_replace_ratio'] is None

    @pytest.mark.unit
    def test_empty_capture_exits_2_no_save(self, temp_output):
        scraper = _player_capture_scraper(empty=True)
        rc = _run_main(
            ["--entity", "player_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
        )
        assert rc == 2
        scraper.save_to_iceberg.assert_not_called()

    @pytest.mark.unit
    def test_http_block_capture_exits_1_red(self, temp_output):
        """#790: an empty player_capture caused by an http block (403) is a real
        failure → exit 1 (red), NOT the soft exit 2 of a genuine empty."""
        scraper = _player_capture_scraper(empty=True)
        scraper._last_lineup_error = {'status': 403}
        rc = _run_main(
            ["--entity", "player_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
        )
        assert rc == 1
        scraper.save_to_iceberg.assert_not_called()
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['fallback_reason'] == 'http_403'


def _schedule_module():
    """Import the runner module for direct helper access — its module-level
    imports are stdlib only (scrapers are lazy), so no stubbing is needed."""
    sys.modules.pop("dags.scripts.run_sofascore_scraper", None)
    return importlib.import_module("dags.scripts.run_sofascore_scraper")


class TestScheduleMergePartition:
    """#761 _merge_schedule_partition: union an existing schedule partition with
    a captured window, keyed by game_id (captured wins). Never shrinks, so the
    completeness guard passes even on a partial capture."""

    @pytest.mark.unit
    def test_empty_existing_returns_captured(self):
        merge = _schedule_module()._merge_schedule_partition
        captured = pd.DataFrame({'game_id': [1, 2], 'home_score': [1.0, 2.0]})
        out = merge(pd.DataFrame(), captured)
        assert sorted(out['game_id']) == [1, 2]

    @pytest.mark.unit
    def test_none_existing_returns_captured(self):
        merge = _schedule_module()._merge_schedule_partition
        captured = pd.DataFrame({'game_id': [1], 'home_score': [3.0]})
        out = merge(None, captured)
        assert out['game_id'].tolist() == [1]

    @pytest.mark.unit
    def test_union_keeps_captured_for_overlap_and_never_shrinks(self):
        merge = _schedule_module()._merge_schedule_partition
        existing = pd.DataFrame({
            'game_id': [1, 2, 4, 5, 6],
            'home_score': [0.0, 0.0, 3.0, 1.0, 2.0],
        })
        captured = pd.DataFrame({
            'game_id': [1, 2, 3],          # 1,2 overlap (fresh scores); 3 new
            'home_score': [1.0, 2.0, 9.0],
        })
        out = merge(existing, captured)
        assert sorted(out['game_id']) == [1, 2, 3, 4, 5, 6]
        assert len(out) >= len(existing)               # never shrinks
        assert out.set_index('game_id').loc[1, 'home_score'] == 1.0  # captured wins
        assert out.set_index('game_id').loc[2, 'home_score'] == 2.0
        assert out.set_index('game_id').loc[4, 'home_score'] == 3.0  # existing kept


class TestScheduleCaptureMergeRunner:
    """#761 _run_legacy merges the captured schedule window with the existing
    bronze partition before the replace_partitions save."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofascore_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_merge_unions_with_existing_partition(self, temp_output):
        captured = pd.DataFrame({
            'league': ['ENG-Premier League'] * 3, 'season': ['2526'] * 3,
            'game_id': [1, 2, 3], 'home_score': [1.0, 2.0, None],
        })
        existing = pd.DataFrame({
            'league': ['ENG-Premier League'] * 5, 'season': ['2526'] * 5,
            'game_id': [1, 2, 4, 5, 6], 'home_score': [0.0, 0.0, 3.0, 1.0, 2.0],
        })
        scraper = _legacy_scraper()
        scraper.read_schedule.return_value = captured
        scraper.read_league_table.return_value = pd.DataFrame()  # skip 2nd save

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2526",
             "--output", temp_output],
            MagicMock(return_value=scraper),
            existing_schedule=existing,
        )

        assert rc == 0
        schedule_call = next(
            c for c in scraper.save_to_iceberg.call_args_list
            if c.kwargs['table_name'] == 'sofascore_schedule'
        )
        saved = schedule_call.kwargs['df']
        assert sorted(saved['game_id']) == [1, 2, 3, 4, 5, 6]   # union
        assert len(saved) >= len(existing)                       # never shrinks
        assert saved.set_index('game_id').loc[1, 'home_score'] == 1.0  # captured wins


class TestMergeMatchPartition:
    """#842 _merge_match_partition: union an existing per-match partition with
    captured rows for NEW matches, keyed by match_id/game_id. A re-captured
    match replaces its existing rows wholesale; the union never shrinks, so
    the completeness guard passes."""

    @pytest.mark.unit
    def test_empty_existing_returns_captured(self):
        merge = _schedule_module()._merge_match_partition
        captured = pd.DataFrame({'match_id': ['1', '1'], 'rating': [6.5, 7.0]})
        out = merge(pd.DataFrame(), captured, key='match_id')
        assert len(out) == 2

    @pytest.mark.unit
    def test_none_existing_returns_captured(self):
        merge = _schedule_module()._merge_match_partition
        captured = pd.DataFrame({'match_id': ['1'], 'rating': [6.5]})
        out = merge(None, captured, key='match_id')
        assert out['match_id'].tolist() == ['1']

    @pytest.mark.unit
    def test_union_never_shrinks_and_recaptured_match_wins(self):
        merge = _schedule_module()._merge_match_partition
        existing = pd.DataFrame({
            'match_id': ['1', '1', '2'], 'rating': [5.0, 5.5, 6.0],
        })
        captured = pd.DataFrame({
            'match_id': ['2', '3'],       # 2 re-captured, 3 new
            'rating': [9.0, 8.0],
        })
        out = merge(existing, captured, key='match_id')
        assert sorted(out['match_id']) == ['1', '1', '2', '3']
        assert len(out) >= len(existing)                 # never shrinks
        # the re-captured match's OLD row is dropped wholesale — no stale mix.
        assert out.set_index('match_id').loc['2', 'rating'] == 9.0

    @pytest.mark.unit
    def test_key_type_mismatch_coerced_via_str(self):
        """Trino returns varchar keys; a captured frame may carry ints (venue
        game_id) — the key comparison must coerce both sides to str."""
        merge = _schedule_module()._merge_match_partition
        existing = pd.DataFrame({'game_id': ['1', '2'], 'stadium': ['A', 'B']})
        captured = pd.DataFrame({'game_id': [2, 3], 'stadium': ['B2', 'C']})
        out = merge(existing, captured, key='game_id')
        assert len(out) == 3                             # '2' replaced, not doubled
        assert set(out['stadium']) == {'A', 'B2', 'C'}


class TestFilterNewMatchIds:
    """#842 _filter_new_match_ids: drop match_ids already materialised in
    bronze.sofascore_player_ratings (the authoritative already-captured key)."""

    @pytest.mark.unit
    def test_filters_and_counts(self, monkeypatch):
        mod = _schedule_module()
        monkeypatch.setattr(mod, '_existing_match_ids_in_bronze',
                            lambda *a, **k: {'2', '3'})
        new, skipped = mod._filter_new_match_ids(
            ['1', '2', '3'], 'ENG-Premier League', '2526', '2025')
        assert new == ['1'] and skipped == 2

    @pytest.mark.unit
    def test_empty_probe_skips_nothing(self, monkeypatch):
        mod = _schedule_module()
        monkeypatch.setattr(mod, '_existing_match_ids_in_bronze',
                            lambda *a, **k: set())
        new, skipped = mod._filter_new_match_ids(
            ['1', '2'], 'ENG-Premier League', '2526', '2025')
        assert new == ['1', '2'] and skipped == 0

    @pytest.mark.unit
    def test_int_ids_compared_as_str(self, monkeypatch):
        mod = _schedule_module()
        monkeypatch.setattr(mod, '_existing_match_ids_in_bronze',
                            lambda *a, **k: {'10'})
        new, skipped = mod._filter_new_match_ids(
            [10, 11], 'ENG-Premier League', '2526', '2025')
        assert new == [11] and skipped == 1


class TestSkipExistingProbesLastWrittenTable:
    """#847: the match_capture skip-existing probe must key on the LAST table
    the pass writes (``sofascore_venue``), not the first
    (``sofascore_player_ratings``) — a mid-save crash (Trino restart) leaves
    the early tables committed and the late ones missing; probing the first
    table made such half-written matches invisible to a plain rerun (APL
    16/17: shotmap/venue stayed at 0 rows until --force-replace)."""

    @pytest.mark.unit
    def test_filter_probes_venue_keyed_by_game_id(self, monkeypatch):
        # Arrange — record which (table, id_col) the probe is asked for;
        # only match '1' has reached the last table.
        mod = _schedule_module()
        calls = []

        def _probe(table, league, season, id_col='match_id'):
            calls.append((table, id_col))
            return {'1'}

        monkeypatch.setattr(mod, '_existing_match_ids_in_bronze', _probe)

        # Act
        new, skipped = mod._filter_new_match_ids(
            ['1', '2'], 'ENG-Premier League', '2526', '2025')

        # Assert — '2' (missing from venue) is re-captured; probe hit venue.
        assert new == ['2'] and skipped == 1
        assert calls and all(t == 'sofascore_venue' for t, _ in calls)
        assert all(c == 'game_id' for _, c in calls)

    @pytest.mark.unit
    def test_probe_sql_selects_the_requested_id_col(self, monkeypatch):
        # Arrange — venue rows are keyed by game_id, not match_id.
        mod = _schedule_module()
        cur = MagicMock()
        cur.fetchall.return_value = [('101',), ('103',)]
        conn = MagicMock()
        conn.cursor.return_value = cur
        monkeypatch.setattr(mod, '_trino_connect', lambda: conn)

        # Act
        ids = mod._existing_match_ids_in_bronze(
            'sofascore_venue', 'ENG-Premier League', '2526', id_col='game_id')

        # Assert — the DISTINCT projection follows id_col.
        sql = cur.execute.call_args[0][0]
        assert 'DISTINCT CAST(game_id AS varchar)' in sql
        assert ids == {'101', '103'}
