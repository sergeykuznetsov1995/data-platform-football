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


def _season_to_short(season) -> str:
    """Offline mirror of ``scrapers.sofascore.scraper._season_to_short`` so the
    stubbed ``scrapers.sofascore.scraper`` module the runner imports it from
    behaves like the real helper (a bare MagicMock would return a MagicMock and
    break season_short). Keep in sync with the real one."""
    s = str(season)
    if len(s) != 4 or not s.isdigit():
        return s
    if isinstance(season, int) and 1900 <= season <= 2098:
        return s[-2:] + f"{(season + 1) % 100:02d}"
    if (int(s[:2]) + 1) % 100 == int(s[2:]):
        return s
    if s[2:] == "99":
        return "9900"
    return s[-2:] + f"{(int(s[-2:]) + 1) % 100:02d}"


def _season_label(league: str, season) -> str:
    """Offline mirror of ``scrapers.sofascore.scraper._season_label`` (#913).
    Without the medallion config on the test host the real helper falls back
    to ``_season_to_short`` for every league — mirror that fallback."""
    return _season_to_short(season)


class TestArgparseHardFail:
    def test_unknown_flag_returns_1_not_2(self):
        assert _main_rc(['--entity', 'schedule', '--bogus-flag', 'x']) == 1

    def test_bad_typed_season_returns_1(self):
        # --season is type=int; a non-int must hard-fail, not soft-fallback.
        assert _main_rc(['--season', 'notanumber']) == 1


def _run_main(
    argv: list,
    scraper_cls,
    *,
    resolver_ids=None,
    existing_capture_ids=None,
) -> int:
    """Run ``main()`` with stubbed ``scrapers.sofascore[.scraper]`` modules.

    ``scrapers.base.base_scraper`` (for ``ReplaceGuardError``) imports for real.
    ``resolver_ids`` patches ``_resolve_match_ids_from_bronze`` so the
    player_ratings path skips its Trino lookup and reaches save_to_iceberg.
    ``existing_capture_ids`` patches the compatibility completion-manifest
    probe. Its default empty set means no event is skipped. Data frames are
    natural-keyed deltas; the runner never reads or rewrites full partitions.
    """
    so_pkg = MagicMock()
    so_pkg.SofaScoreScraper = scraper_cls
    so_scraper_mod = MagicMock()
    so_scraper_mod.R0_2B_FALLBACK_MARKER = 'R0_2B_FALLBACK'
    so_scraper_mod._season_to_short = _season_to_short
    so_scraper_mod._season_label = _season_label
    so_pipeline_mod = MagicMock()
    so_pipeline_mod.build_capture_runtime.return_value = None
    so_catalog_mod = MagicMock()
    tournament = MagicMock()
    tournament.capture_allowed = True
    catalog = MagicMock()
    catalog.competition.return_value = tournament
    so_catalog_mod.SofaScoreCatalog.load.return_value = catalog

    sys.argv = ["run_sofascore_scraper.py"] + argv
    with patch.dict(sys.modules, {
             "scrapers.sofascore": so_pkg,
             "scrapers.sofascore.scraper": so_scraper_mod,
             "scrapers.sofascore.pipeline": so_pipeline_mod,
             "scrapers.sofascore.catalog": so_catalog_mod,
         }):
        sys.modules.pop("dags.scripts.run_sofascore_scraper", None)
        mod = importlib.import_module("dags.scripts.run_sofascore_scraper")
        importlib.reload(mod)
        if resolver_ids is not None:
            mod._resolve_match_ids_from_bronze = lambda *a, **k: resolver_ids
        _ids = set() if existing_capture_ids is None else set(existing_capture_ids)
        mod._existing_match_ids_in_bronze = lambda *a, **k: _ids
        mod._existing_complete_capture_ids = lambda *a, **k: set(_ids)
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
    scraper.read_tournament_snapshot.return_value = (df, df)
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'writer DQ guard refused the captured schedule delta'
        )
    else:
        scraper.save_to_iceberg.return_value = 'iceberg.bronze.sofascore_schedule'
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


class TestSofascoreReplaceGuard:
    """#583: completeness-guard wiring in the SofaScore runner.

    Covers the default 'all' path and the compatibility alias from
    ``player_ratings`` to the common ``match_capture`` engine. The guard
    arithmetic lives in ``BaseScraper.save_to_iceberg``; here we cover runner
    handling and prove that the alias cannot resurrect the retired standalone
    player-ratings capture path.
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
    def test_legacy_normal_path_uses_incremental_natural_keys(self, temp_output):
        scraper = _legacy_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        calls = {
            call.kwargs['table_name']: call.kwargs
            for call in scraper.save_to_iceberg.call_args_list
        }
        assert calls['sofascore_schedule']['natural_keys'] == [
            'league', 'season', 'game_id',
        ]
        assert calls['sofascore_league_table']['natural_keys'] == [
            'league', 'season', 'group', 'team',
        ]
        assert all('replace_partitions' not in value for value in calls.values())

    @pytest.mark.unit
    def test_legacy_force_replace_recaptures_but_still_merges(self, temp_output):
        scraper = _legacy_scraper()

        rc = _run_main(
            ["--leagues", "ENG-Premier League", "--season", "2024",
             "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        kwargs = scraper.save_to_iceberg.call_args.kwargs
        assert kwargs['natural_keys'] == [
            'league', 'season', 'group', 'team',
        ]

    @pytest.mark.unit
    def test_legacy_empty_source_is_hard_failure(self, temp_output):
        scraper = _legacy_scraper()
        scraper.read_tournament_snapshot.return_value = (
            pd.DataFrame(),
            pd.DataFrame(),
        )

        rc = _run_main(
            [
                "--leagues",
                "ENG-Premier League",
                "--season",
                "2025",
                "--output",
                temp_output,
            ],
            MagicMock(return_value=scraper),
        )

        assert rc == 1
        scraper.save_to_iceberg.assert_not_called()
        with open(temp_output) as handle:
            payload = json.load(handle)
        assert any("no rows" in error for error in payload["errors"])

    @pytest.mark.unit
    def test_explicit_schedule_does_not_fetch_standings(self, temp_output):
        scraper = _legacy_scraper()

        rc = _run_main(
            [
                "--entity",
                "schedule",
                "--leagues",
                "ENG-Premier League",
                "--season",
                "2025",
                "--output",
                temp_output,
            ],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        scraper.read_schedule.assert_called_once_with()
        scraper.read_league_table.assert_not_called()
        scraper.read_tournament_snapshot.assert_not_called()

    @pytest.mark.unit
    def test_player_ratings_guard_refusal_exits_3(self, temp_output):
        """The legacy name keeps match_capture's ReplaceGuard semantics."""
        scraper = _match_capture_scraper(guard_blocks=True)

        rc = _run_main(
            ["--entity", "player_ratings", "--league", "ENG-Premier League",
             "--season", "2024", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=[1, 2, 3],
        )

        assert rc == 3
        scraper.read_match_capture.assert_called_once()
        with open(temp_output) as f:
            payload = json.load(f)
        assert any("SOFASCORE_REPLACE_GUARD" in e for e in payload["errors"])

    @pytest.mark.unit
    def test_player_ratings_alias_uses_incremental_match_engine(self, temp_output):
        scraper = _match_capture_scraper()

        rc = _run_main(
            ["--entity", "player_ratings", "--league", "ENG-Premier League",
             "--season", "2024", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=[1, 2, 3],
        )

        assert rc == 0
        scraper.read_match_capture.assert_called_once()
        calls = scraper.save_to_iceberg.call_args_list
        assert calls
        assert all("replace_partitions" not in call.kwargs for call in calls)
        ratings = next(
            call for call in calls
            if call.kwargs["table_name"] == "sofascore_player_ratings"
        )
        assert ratings.kwargs["natural_keys"] == [
            "league", "season", "match_id", "player_id",
        ]


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
        scraper = _match_capture_scraper()
        scraper.resolve_finished_match_ids_via_capture.return_value = ['101', '103']

        # Act
        rc = _run_main(
            ["--entity", "player_ratings", "--league", "ENG-Premier League",
             "--season", "2024", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=[],
        )

        # Assert — capture drove the ids into the one common match engine.
        assert rc == 0
        scraper.resolve_finished_match_ids_via_capture.assert_called_once()
        assert scraper.read_match_capture.call_args.kwargs['match_ids'] == [
            '101', '103',
        ]

    @pytest.mark.unit
    def test_empty_bronze_and_empty_capture_exits_2(self, temp_output):
        # Arrange — neither bronze nor capture yields match_ids (off-season).
        scraper = _match_capture_scraper()
        scraper.resolve_finished_match_ids_via_capture.return_value = []

        # Act
        rc = _run_main(
            ["--entity", "player_ratings", "--league", "ENG-Premier League",
             "--season", "2024", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=[],
        )

        # Assert — graceful R0.2B fallback before the common engine opens.
        assert rc == 2
        scraper.read_match_capture.assert_not_called()
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['fallback'] is True
        assert payload['fallback_reason'] == 'no_match_ids'
        assert 'traffic' in payload


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
    scraper._add_metadata.side_effect = lambda df, entity: df
    scraper.read_match_capture.return_value = frames
    if guard_blocks:
        scraper.save_to_iceberg.side_effect = ReplaceGuardError(
            'writer DQ guard refused the captured delta')
    else:
        scraper.save_to_iceberg.return_value = (
            'iceberg.bronze.sofascore_player_ratings')
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


class TestMatchCaptureRunner:
    """#751 PR1: the consolidated match_capture entity writes BOTH
    player_ratings and event_player_stats from one capture pass, with every
    table saved as a natural-keyed delta."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofascore_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_normal_path_incrementally_saves_all_tables(self, temp_output):
        scraper = _match_capture_scraper()
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
        )
        assert rc == 0
        # Five data tables plus the endpoint-status commit manifest.
        assert scraper.save_to_iceberg.call_count == 6
        tables = [c.kwargs['table_name']
                  for c in scraper.save_to_iceberg.call_args_list]
        assert set(tables) == {'sofascore_player_ratings',
                               'sofascore_event_player_stats',
                               'sofascore_match_stats',
                               'sofascore_event_shotmap',
                               'sofascore_venue',
                               'sofascore_match_capture_status'}
        assert tables[-3:] == [
            'sofascore_event_player_stats',
            'sofascore_player_ratings',
            'sofascore_match_capture_status',
        ]
        for c in scraper.save_to_iceberg.call_args_list:
            assert c.kwargs['natural_keys'][:2] == ['league', 'season']
            assert 'replace_partitions' not in c.kwargs
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
    def test_force_replace_recaptures_with_incremental_merge(self, temp_output):
        scraper = _match_capture_scraper()
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
        )
        assert rc == 0
        for c in scraper.save_to_iceberg.call_args_list:
            assert c.kwargs['natural_keys'][:2] == ['league', 'season']

    @pytest.mark.unit
    def test_empty_capture_exits_2_and_keeps_incomplete_manifest(self, temp_output):
        scraper = _match_capture_scraper(empty=True)
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
        )
        assert rc == 2
        assert [
            call.kwargs['table_name']
            for call in scraper.save_to_iceberg.call_args_list
        ] == ['sofascore_match_capture_status']

    @pytest.mark.unit
    def test_terminal_empty_capture_commits_manifest_without_fallback(self, temp_output):
        scraper = _match_capture_scraper(empty=True)
        scraper.read_match_capture.return_value['capture_status'] = pd.DataFrame(
            {
                'match_id': ['1'],
                'event_status': ['success'],
                'lineups_status': ['not_available'],
                'statistics_status': ['not_available'],
                'shotmap_status': ['not_available'],
                'capture_complete': [True],
                'league': ['ENG-Premier League'],
                'season': ['2526'],
            }
        )

        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1'],
        )

        assert rc == 0
        with open(temp_output) as result_file:
            payload = json.load(result_file)
        assert payload['fallback'] is False
        assert payload['matches_complete'] == 1

    @pytest.mark.unit
    def test_primary_failure_still_saves_paid_secondary_payload(self, temp_output):
        scraper = _match_capture_scraper(empty=True)
        frames = scraper.read_match_capture.return_value
        frames['match_stats'] = pd.DataFrame(
            {
                'match_id': ['1'],
                'league': ['ENG-Premier League'],
                'season': ['2526'],
                'name': ['Possession'],
            }
        )

        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1'],
        )

        assert rc == 2
        assert [
            call.kwargs['table_name']
            for call in scraper.save_to_iceberg.call_args_list
        ] == ['sofascore_match_stats', 'sofascore_match_capture_status']

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
        assert [
            call.kwargs['table_name']
            for call in scraper.save_to_iceberg.call_args_list
        ] == ['sofascore_match_capture_status']
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
    def test_incremental_capture_saves_only_the_natural_key_delta(
        self, temp_output,
    ):
        scraper = _match_capture_scraper()
        rc = _run_main(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--output", temp_output],
            MagicMock(return_value=scraper),
            resolver_ids=['1', '2'],
        )
        assert rc == 0
        ratings_call = next(
            c for c in scraper.save_to_iceberg.call_args_list
            if c.kwargs['table_name'] == 'sofascore_player_ratings'
        )
        saved = ratings_call.kwargs['df']
        assert sorted(set(saved['match_id'])) == ['1', '2']
        assert len(saved) == 4
        assert ratings_call.kwargs['natural_keys'] == [
            'league', 'season', 'match_id', 'player_id',
        ]
        with open(temp_output) as f:
            payload = json.load(f)
        assert payload['rows'] == 4


class TestSeasonTokenNoShift:
    """#888: the runner must resolve match_ids for the SAME season it labels.
    The old inline formula (``s[2:4]+ (s[2:4]+1)``) always shifted, while the
    scraper labels rows via ``_season_to_short`` (which passes an already-short
    token through). For a short-form ``--season`` the two diverged and the
    partition was silently written under the +1 season. Both paths now use
    ``_season_label`` (#913: falls back to ``_season_to_short`` for club
    leagues) — a short-form token must resolve to itself."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofascore_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    def _record_resolve_season(self, argv, scraper, *, resolved_ids=None):
        seen = []
        so_pkg = MagicMock()
        so_pkg.SofaScoreScraper = MagicMock(return_value=scraper)
        so_scraper_mod = MagicMock()
        so_scraper_mod.R0_2B_FALLBACK_MARKER = 'R0_2B_FALLBACK'
        so_scraper_mod._season_to_short = _season_to_short
        so_scraper_mod._season_label = _season_label
        so_pipeline_mod = MagicMock()
        so_pipeline_mod.build_capture_runtime.return_value = None
        so_catalog_mod = MagicMock()
        so_catalog_mod.SofaScoreCatalog.load.return_value.competition.return_value.capture_allowed = True
        sys.argv = ["run_sofascore_scraper.py"] + argv
        with patch.dict(sys.modules, {
            "scrapers.sofascore": so_pkg,
            "scrapers.sofascore.scraper": so_scraper_mod,
            "scrapers.sofascore.pipeline": so_pipeline_mod,
            "scrapers.sofascore.catalog": so_catalog_mod,
        }):
            sys.modules.pop("dags.scripts.run_sofascore_scraper", None)
            mod = importlib.import_module("dags.scripts.run_sofascore_scraper")
            importlib.reload(mod)

            def _resolver(_league, season_short, _limit=None):
                seen.append(season_short)
                if resolved_ids is None:
                    return ['1', '2']
                return list(resolved_ids)

            mod._resolve_match_ids_from_bronze = _resolver
            mod._existing_match_ids_in_bronze = lambda *a, **k: set()
            mod._existing_complete_capture_ids = lambda *a, **k: set()
            rc = mod.main()
        return rc, seen

    @pytest.mark.unit
    def test_short_form_season_resolves_without_plus_one_shift(self, temp_output):
        # 2122 is the poison case: old inline gave '2223' (2022/23), mislabelling
        # the 2021/22 partition. It must resolve '2122'.
        rc, seen = self._record_resolve_season(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2122", "--output", temp_output],
            _match_capture_scraper(),
        )
        assert rc == 0
        assert seen and seen[0] == '2122'

    @pytest.mark.unit
    def test_start_year_season_still_resolves_to_short(self, temp_output):
        # Start-year tokens were always correct and must stay so: 2023 -> '2324'.
        rc, seen = self._record_resolve_season(
            ["--entity", "match_capture", "--league", "ENG-Premier League",
             "--season", "2023", "--output", temp_output],
            _match_capture_scraper(),
        )
        assert rc == 0
        assert seen and seen[0] == '2324'

    @pytest.mark.unit
    def test_ambiguous_2021_cli_int_means_start_year(self, temp_output):
        rc, seen = self._record_resolve_season(
            [
                "--entity",
                "match_capture",
                "--league",
                "ENG-Premier League",
                "--season",
                "2021",
                "--output",
                temp_output,
            ],
            _match_capture_scraper(),
        )
        assert rc == 0
        assert seen and seen[0] == "2122"

    @pytest.mark.unit
    def test_ambiguous_2021_never_probes_raw_2021_partition(self, temp_output):
        scraper = _match_capture_scraper(empty=True)
        scraper.resolve_finished_match_ids_via_capture.return_value = []

        rc, seen = self._record_resolve_season(
            [
                "--entity",
                "match_capture",
                "--league",
                "ENG-Premier League",
                "--season",
                "2021",
                "--output",
                temp_output,
            ],
            scraper,
            resolved_ids=[],
        )

        assert rc == 2
        assert seen == ["2122"]


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
            'writer DQ guard refused the captured delta')
    else:
        scraper.save_to_iceberg.return_value = (
            'iceberg.bronze.sofascore_player_profile')
    scraper.__enter__ = MagicMock(return_value=scraper)
    scraper.__exit__ = MagicMock(return_value=False)
    return scraper


class TestPlayerCaptureRunner:
    """#751 PR3 + PR3b: the player_capture entity writes player_profile AND
    player_season_stats from one per-player capture pass using natural-keyed
    deltas. Season-stats is secondary — its
    save is skipped (not a fallback) when no exact aggregate was captured."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofascore_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_normal_path_incrementally_saves_both_tables(self, temp_output):
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
        assert saved['sofascore_player_profile']['natural_keys'] == [
            'league', 'season', 'player_id',
        ]
        assert saved['sofascore_player_season_stats']['natural_keys'] == [
            'league', 'season', 'player_id',
            'unique_tournament_id', 'sofascore_season_id',
        ]
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
    def test_force_replace_still_uses_incremental_merge(self, temp_output):
        scraper = _player_capture_scraper()
        rc = _run_main(
            ["--entity", "player_capture", "--league", "ENG-Premier League",
             "--season", "2025", "--force-replace", "--output", temp_output],
            MagicMock(return_value=scraper),
        )
        assert rc == 0
        for c in scraper.save_to_iceberg.call_args_list:
            assert c.kwargs['natural_keys'][:2] == ['league', 'season']

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


def _runner_module():
    """Import the runner module for direct helper access — its module-level
    imports are stdlib only (scrapers are lazy), so no stubbing is needed."""
    sys.modules.pop("dags.scripts.run_sofascore_scraper", None)
    return importlib.import_module("dags.scripts.run_sofascore_scraper")


class TestScheduleCaptureIncrementalRunner:
    """Schedule capture publishes its natural-keyed delta without a readback."""

    @pytest.fixture
    def temp_output(self):
        fd, path = tempfile.mkstemp(suffix=".json", prefix="sofascore_")
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_schedule_saves_incremental_natural_key_delta(self, temp_output):
        captured = pd.DataFrame({
            'league': ['ENG-Premier League'] * 3, 'season': ['2526'] * 3,
            'game_id': [1, 2, 3], 'home_score': [1.0, 2.0, None],
        })
        scraper = _legacy_scraper()
        scraper.read_schedule.return_value = captured
        scraper.read_league_table.return_value = pd.DataFrame()  # skip 2nd save

        rc = _run_main(
            [
                "--entity",
                "schedule",
                "--leagues",
                "ENG-Premier League",
                "--season",
                "2526",
                "--output",
                temp_output,
            ],
            MagicMock(return_value=scraper),
        )

        assert rc == 0
        schedule_call = next(
            c for c in scraper.save_to_iceberg.call_args_list
            if c.kwargs['table_name'] == 'sofascore_schedule'
        )
        saved = schedule_call.kwargs['df']
        assert sorted(saved['game_id']) == [1, 2, 3]
        assert schedule_call.kwargs['natural_keys'] == [
            'league', 'season', 'game_id',
        ]
        assert saved.set_index('game_id').loc[1, 'home_score'] == 1.0  # captured wins


class TestFilterNewMatchIds:
    """#842 filter uses the explicit completion manifest."""

    @pytest.mark.unit
    def test_filters_and_counts(self, monkeypatch):
        mod = _runner_module()
        monkeypatch.setattr(
            mod, '_existing_complete_capture_ids', lambda *a, **k: {'2', '3'})
        new, skipped, seed, missing = mod._filter_new_match_ids(
            ['1', '2', '3'], 'ENG-Premier League', '2526', '2025')
        assert new == ['1'] and skipped == 2 and seed == set() and not missing

    @pytest.mark.unit
    def test_empty_probe_skips_nothing(self, monkeypatch):
        mod = _runner_module()
        monkeypatch.setattr(
            mod, '_existing_complete_capture_ids', lambda *a, **k: set())
        new, skipped, seed, missing = mod._filter_new_match_ids(
            ['1', '2'], 'ENG-Premier League', '2526', '2025')
        assert new == ['1', '2'] and skipped == 0 and seed == set() and not missing

    @pytest.mark.unit
    def test_int_ids_compared_as_str(self, monkeypatch):
        mod = _runner_module()
        monkeypatch.setattr(
            mod, '_existing_complete_capture_ids', lambda *a, **k: {'10'})
        new, skipped, seed, missing = mod._filter_new_match_ids(
            [10, 11], 'ENG-Premier League', '2526', '2025')
        assert new == [11] and skipped == 1 and seed == set() and not missing


class TestSkipExistingRequiresCompleteCapture:
    """Legacy data seeds the manifest without a full-season re-download."""

    @pytest.mark.unit
    def test_missing_manifest_seeds_only_fully_materialised_legacy_matches(
        self,
        monkeypatch,
    ):
        mod = _runner_module()
        calls = []

        def _probe(table, league, season, id_col='match_id'):
            calls.append((table, id_col))
            return {'1'}

        monkeypatch.setattr(mod, '_existing_match_ids_in_bronze', _probe)
        monkeypatch.setattr(
            mod,
            '_existing_complete_capture_ids',
            lambda *a, **k: None,
        )

        # Act
        new, skipped, seed, missing = mod._filter_new_match_ids(
            ['1', '2'], 'ENG-Premier League', '2526', '2025')

        # Match 1 exists everywhere; match 2 must be captured.
        assert new == ["2"] and skipped == 1
        assert seed == {'1'} and missing is True
        assert {table for table, _ in calls} == {
            "sofascore_player_ratings",
            "sofascore_event_player_stats",
            "sofascore_match_stats",
            "sofascore_event_shotmap",
            "sofascore_venue",
        }
        assert dict(calls)["sofascore_venue"] == "game_id"

    @pytest.mark.unit
    def test_preflight_manifest_contains_legacy_and_pending_rows(self):
        mod = _runner_module()
        scraper = MagicMock()
        scraper._add_metadata.side_effect = lambda frame, entity: frame

        mod._prepare_capture_manifest(
            scraper,
            pending_ids=['2'],
            complete_ids={'1'},
            league='ENG-Premier League',
            season='2526',
        )

        call = scraper.save_to_iceberg.call_args
        frame = call.kwargs['df'].set_index('match_id')
        assert bool(frame.loc['1', 'capture_complete']) is True
        assert bool(frame.loc['2', 'capture_complete']) is False
        assert frame.loc['2', 'event_status'] == 'pending'
        assert call.kwargs['natural_keys'] == ['league', 'season', 'match_id']

    @pytest.mark.unit
    def test_probe_sql_selects_the_requested_id_col(self, monkeypatch):
        # Arrange — venue rows are keyed by game_id, not match_id.
        mod = _runner_module()
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
        assert "DISTINCT CAST(game_id AS varchar)" in sql
        assert ids == {"101", "103"}

    @pytest.mark.unit
    def test_probe_fails_closed_when_trino_is_unavailable(self, monkeypatch):
        mod = _runner_module()
        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = RuntimeError("connection reset")
        monkeypatch.setattr(mod, "_trino_connect", lambda: conn)

        with pytest.raises(RuntimeError, match="skip-existing probe"):
            mod._existing_match_ids_in_bronze(
                "sofascore_venue",
                "ENG-Premier League",
                "2526",
                id_col="game_id",
            )


class TestBronzeMatchResolver:
    @pytest.mark.unit
    def test_filters_by_finished_status_not_live_score(self, monkeypatch):
        mod = _runner_module()
        cur = MagicMock()
        cur.fetchall.return_value = [("101",)]
        conn = MagicMock()
        conn.cursor.return_value = cur
        monkeypatch.setattr(mod, "_trino_connect", lambda: conn)

        assert mod._resolve_match_ids_from_bronze(
            "ENG-Premier League",
            "2526",
            None,
        ) == ["101"]
        sql = cur.execute.call_args[0][0]
        assert "status_type = 'finished'" in sql
        assert "home_score" not in sql
        assert "COALESCE" not in sql

    @pytest.mark.unit
    def test_operational_error_fails_closed(self, monkeypatch):
        mod = _runner_module()
        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = RuntimeError("connection reset")
        monkeypatch.setattr(mod, "_trino_connect", lambda: conn)

        with pytest.raises(RuntimeError, match="schedule match-id probe failed"):
            mod._resolve_match_ids_from_bronze(
                "ENG-Premier League",
                "2526",
                None,
            )
