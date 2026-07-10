"""
Unit tests for run_fbref_scraper.py exit code logic.

Tests that the scraper correctly returns exit code 1 when:
1. No data collected for schedule type (critical mode)
2. Scraper has failures > 0

NOTE (Apr 2026): the soccerdata branch was removed because curl_cffi cannot
bypass Cloudflare Turnstile. Tests now exercise the nodriver branch.
"""

import json
import os
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


class TestExitCodeLogic:
    """Test exit code behavior for match_data mode."""

    @pytest.fixture
    def mock_scraper(self):
        """Create a mock scraper with configurable stats."""
        scraper = MagicMock()
        scraper._stats = {'successes': 0, 'failures': 0}
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        return scraper

    @pytest.fixture
    def temp_output_file(self):
        """Create a temporary output file."""
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    def run_scraper_main(self, args: list, mock_scraper_class) -> int:
        """Run the scraper main function and return exit code."""
        sys.argv = ['run_fbref_scraper.py'] + args

        with patch(
            'scrapers.nodriver_fbref.NodriverFBrefScraper', mock_scraper_class
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as scraper_module
            importlib.reload(scraper_module)
            return scraper_module.main()

    @pytest.mark.unit
    def test_exit_code_on_empty_schedule(self, mock_scraper, temp_output_file):
        """Test that empty schedule results in exit code 1."""
        mock_scraper.scrape_schedule.return_value = {}
        mock_scraper._stats = {'successes': 0, 'failures': 0}
        mock_scraper.get_stats.return_value = mock_scraper._stats

        mock_class = MagicMock(return_value=mock_scraper)

        exit_code = self.run_scraper_main(
            [
                '--scraper-type', 'nodriver',
                '--mode', 'match_data',
                '--match-data-type', 'schedule',
                '--leagues', 'ENG-Premier League',
                '--season', '2024',
                '--output', temp_output_file,
            ],
            mock_class
        )

        # Schedule is a critical mode — empty schedule must exit 1.
        assert exit_code == 1, "Empty schedule should return exit code 1"

        with open(temp_output_file, 'r') as f:
            result = json.load(f)

        assert len(result['errors']) > 0, (
            "Error should be recorded for empty schedule"
        )

    @pytest.mark.unit
    def test_exit_code_on_successful_scrape(self, mock_scraper, temp_output_file):
        """Test that successful scrape returns exit code 0."""
        mock_scraper.scrape_schedule.return_value = {
            'schedule': 'iceberg.bronze.fbref_schedule'
        }
        mock_scraper._stats = {'successes': 1, 'failures': 0}
        mock_scraper.get_stats.return_value = mock_scraper._stats

        mock_class = MagicMock(return_value=mock_scraper)

        exit_code = self.run_scraper_main(
            [
                '--scraper-type', 'nodriver',
                '--mode', 'match_data',
                '--match-data-type', 'schedule',
                '--leagues', 'ENG-Premier League',
                '--season', '2024',
                '--output', temp_output_file,
            ],
            mock_class
        )

        assert exit_code == 0, "Successful scrape should return exit code 0"

        with open(temp_output_file, 'r') as f:
            result = json.load(f)

        assert len(result['tables']) > 0, "Tables should be recorded on success"
        assert len(result['errors']) == 0, "No errors should be recorded on success"

    @pytest.mark.unit
    def test_partial_multi_league_schedule_exits_nonzero(
        self, mock_scraper, temp_output_file
    ):
        mock_scraper.scrape_schedule.return_value = {
            'schedule': 'iceberg.bronze.fbref_schedule'
        }
        mock_scraper._stats = {
            'successes': 1,
            'failures': 1,
            'scope_errors': ['Missing schedule targets: bad-league'],
        }
        mock_scraper.get_stats.return_value = mock_scraper._stats

        rc = self.run_scraper_main(
            [
                '--scraper-type', 'nodriver',
                '--mode', 'match_data',
                '--match-data-type', 'schedule',
                '--leagues', 'good-league,bad-league',
                '--season', '2025',
                '--output', temp_output_file,
            ],
            MagicMock(return_value=mock_scraper),
        )

        assert rc == 1
        with open(temp_output_file) as handle:
            result = json.load(handle)
        assert result['tables']
        assert result['errors']


class TestSingleStatExitCode:
    """Test exit code behavior for single_stat mode."""

    @pytest.fixture
    def mock_scraper(self):
        """Create a mock scraper with configurable stats."""
        scraper = MagicMock()
        scraper._stats = {'successes': 0, 'failures': 0}
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        return scraper

    @pytest.fixture
    def temp_output_file(self):
        """Create a temporary output file."""
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    def run_scraper_main(self, args: list, mock_scraper_class) -> int:
        """Run the scraper main function and return exit code."""
        sys.argv = ['run_fbref_scraper.py'] + args

        with patch(
            'scrapers.nodriver_fbref.NodriverFBrefScraper', mock_scraper_class
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as scraper_module
            importlib.reload(scraper_module)
            return scraper_module.main()

    @pytest.mark.unit
    def test_single_stat_empty_results_error(self, mock_scraper, temp_output_file):
        """Test that empty single_stat results record an error."""
        mock_scraper.scrape_single_stat_type.return_value = {}
        mock_scraper._stats = {'successes': 0, 'failures': 1}
        mock_scraper.get_stats.return_value = mock_scraper._stats

        mock_class = MagicMock(return_value=mock_scraper)

        exit_code = self.run_scraper_main(
            [
                '--scraper-type', 'nodriver',
                '--mode', 'single_stat',
                '--stat-type', 'stats',
                '--data-category', 'player',
                '--leagues', 'ENG-Premier League',
                '--season', '2024',
                '--output', temp_output_file,
            ],
            mock_class
        )

        assert exit_code == 1, (
            "Empty single_stat with failures should return exit code 1"
        )

        with open(temp_output_file, 'r') as f:
            result = json.load(f)

        assert len(result['errors']) > 0, (
            "Error should be recorded for empty single_stat"
        )


class TestNodriverImportFailure:
    """#468: ImportError of NodriverFBrefScraper must fail the run loudly.

    The old 'fall through to selenium' fallback was dead code: the else
    branch belonged to an already-evaluated if, so control jumped straight
    to result writing and non-critical modes finished green with zero
    tables (silent failure)."""

    @pytest.fixture
    def temp_output_file(self):
        """Create a temporary output file."""
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_nodriver_import_error_exits_1_with_error_recorded(
        self, temp_output_file
    ):
        # Non-critical mode: pre-#468 this finished with exit code 0.
        sys.argv = ['run_fbref_scraper.py'] + [
            '--scraper-type', 'nodriver',
            '--mode', 'single_stat',
            '--stat-type', 'stats',
            '--data-category', 'player',
            '--leagues', 'ENG-Premier League',
            '--season', '2024',
            '--output', temp_output_file,
        ]

        # None entry makes `from scrapers.nodriver_fbref import ...`
        # raise ImportError inside main().
        with patch.dict(sys.modules, {'scrapers.nodriver_fbref': None}):
            import importlib
            import dags.scripts.run_fbref_scraper as scraper_module
            importlib.reload(scraper_module)
            with pytest.raises(SystemExit) as exc_info:
                scraper_module.main()

        assert exc_info.value.code == 1

        with open(temp_output_file, 'r') as f:
            result = json.load(f)

        assert result['tables'] == []
        assert any('NodriverFBrefScraper' in err for err in result['errors']), (
            "Import failure must be recorded in results['errors']"
        )
        assert result['scraper_type'] == 'nodriver', (
            "scraper_type must not be silently mutated to 'selenium'"
        )


class TestHttpFastPathDiagnostics:
    """#624: the runner must surface HTTP fast-path counters + fallback
    diagnostics so a prod combined_match_data run can identify the dominant
    cold-start cause (acceptance #1)."""

    def _stub_scraper(self):
        # SimpleNamespace has no `_update_real_traffic_stats`, so the flush in
        # _get_traffic_diagnostics is skipped and `_stats` is read verbatim.
        return SimpleNamespace(
            _stats={
                'http_fetch_ok': 7,
                'http_fetch_fallback': 3,
                'http_fetch_diag': [
                    {'reason': 'non_200', 'cf_mitigated': 'challenge',
                     'proxy': 'host:1'},
                    {'reason': 'non_200', 'cf_mitigated': 'challenge',
                     'proxy': 'host:1'},
                    {'reason': 'incomplete_no_tables', 'cf_mitigated': None,
                     'proxy': 'host:2'},
                ],
            }
        )

    @pytest.mark.unit
    def test_get_traffic_diagnostics_surfaces_http_fast_path(self):
        import dags.scripts.run_fbref_scraper as m

        diag = m._get_traffic_diagnostics(self._stub_scraper())

        assert diag['http_fetch_ok'] == 7
        assert diag['http_fetch_fallback'] == 3
        assert len(diag['http_fetch_diag']) == 3
        assert diag['http_fetch_diag_summary']['by_reason'] == {
            'non_200': 2, 'incomplete_no_tables': 1,
        }
        assert diag['http_fetch_diag_summary']['by_cf_mitigated'] == {
            'challenge': 2,
        }

    @pytest.mark.unit
    def test_write_traffic_summary_includes_http_fast_path(self, tmp_path):
        import dags.scripts.run_fbref_scraper as m

        out = tmp_path / 'traffic.json'
        m._write_traffic_summary(
            self._stub_scraper(),
            label='match_all_data',
            mode='combined_match_data',
            explicit_path=str(out),
        )

        payload = json.loads(out.read_text())
        assert payload['http_fetch_ok'] == 7
        assert payload['http_fetch_fallback'] == 3
        assert payload['http_fetch_diag_summary']['by_reason']['non_200'] == 2

    @pytest.mark.unit
    def test_diag_summary_counts_proxy_mismatch(self):
        # #624: a fallback where the curl session's minted proxy drifted from
        # the current nodriver proxy is a proxy-mismatch — counted only when
        # both fields are present and differ.
        import dags.scripts.run_fbref_scraper as m

        scraper = SimpleNamespace(_stats={
            'http_fetch_ok': 5,
            'http_fetch_fallback': 4,
            'http_fetch_diag': [
                {'reason': 'non_200', 'proxy_minted': 'host:1', 'proxy': 'host:1'},
                {'reason': 'non_200', 'proxy_minted': 'host:1', 'proxy': 'host:2'},
                {'reason': 'empty_body', 'proxy_minted': None, 'proxy': 'host:2'},
                {'reason': 'exception', 'proxy': 'host:3'},
            ],
        })

        diag = m._get_traffic_diagnostics(scraper)

        assert diag['http_fetch_diag_summary']['proxy_mismatch'] == 1


class TestSingleStatReplaceGuard:
    """#583: completeness-guard wiring for single_stat mode (nodriver path).

    The guard arithmetic lives in scrape_single_stat_type (covered by
    test_data_readers.py / test_nodriver_fbref_scraper.py); here we cover the
    runner's handling — thread force_replace down to the scraper and map a
    raised ReplaceGuardError to exit 3 + the FBREF_REPLACE_GUARD marker.
    """

    @pytest.fixture
    def temp_output_file(self):
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @staticmethod
    def _single_stat_args(output, *, force=False):
        args = [
            '--scraper-type', 'nodriver',
            '--mode', 'single_stat',
            '--stat-type', 'stats',
            '--data-category', 'player',
            '--leagues', 'ENG-Premier League',
            '--season', '2024',
            '--output', output,
        ]
        if force:
            args.append('--force-replace')
        return args

    @staticmethod
    def _run(scraper, args):
        sys.argv = ['run_fbref_scraper.py'] + args
        with patch(
            'scrapers.nodriver_fbref.NodriverFBrefScraper',
            MagicMock(return_value=scraper),
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as m
            importlib.reload(m)
            return m.main()

    @staticmethod
    def _stub_scraper():
        scraper = MagicMock()
        scraper._stats = {'successes': 1, 'failures': 0}
        scraper.get_stats.return_value = scraper._stats
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        return scraper

    @pytest.mark.unit
    def test_guard_refusal_exits_3(self, temp_output_file):
        """scrape_single_stat_type raises ReplaceGuardError → exit 3 +
        FBREF_REPLACE_GUARD marker (distinct from the exit-1 path)."""
        from scrapers.base.base_scraper import ReplaceGuardError

        scraper = self._stub_scraper()
        scraper.scrape_single_stat_type.side_effect = ReplaceGuardError(
            'new=2 rows < 90% of existing=380 for bronze.fbref_player_stats '
            '— refusing replace_partitions save (would shrink the partition)'
        )

        sys.argv = ['run_fbref_scraper.py'] + self._single_stat_args(temp_output_file)
        with patch(
            'scrapers.nodriver_fbref.NodriverFBrefScraper',
            MagicMock(return_value=scraper),
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as m
            importlib.reload(m)
            with pytest.raises(SystemExit) as exc:
                m.main()

        assert exc.value.code == 3
        with open(temp_output_file) as f:
            result = json.load(f)
        assert any('FBREF_REPLACE_GUARD' in e for e in result['errors'])

    @pytest.mark.unit
    def test_normal_path_threads_force_replace_false(self, temp_output_file):
        """Non-force run threads force_replace=False into scrape_single_stat_type."""
        scraper = self._stub_scraper()
        scraper.scrape_single_stat_type.return_value = {
            'player_stats': 'iceberg.bronze.fbref_player_stats'
        }

        rc = self._run(scraper, self._single_stat_args(temp_output_file))

        assert rc == 0
        assert scraper.scrape_single_stat_type.call_args.kwargs['force_replace'] is False

    @pytest.mark.unit
    def test_force_replace_threads_true(self, temp_output_file):
        """--force-replace threads force_replace=True into scrape_single_stat_type."""
        scraper = self._stub_scraper()
        scraper.scrape_single_stat_type.return_value = {
            'player_stats': 'iceberg.bronze.fbref_player_stats'
        }

        rc = self._run(scraper, self._single_stat_args(temp_output_file, force=True))

        assert rc == 0
        assert scraper.scrape_single_stat_type.call_args.kwargs['force_replace'] is True


class TestNodriverUnsupportedMode:
    """Nodriver branch must reject modes it does not implement (footgun fix).

    Before the fix, --scraper-type nodriver --mode combined_match_data fell
    through into the 'full' else-branch and silently ran a full season scrape.
    """

    @pytest.fixture
    def temp_output_file(self):
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    def _run(self, mock_scraper_class, args: list) -> int:
        sys.argv = ['run_fbref_scraper.py'] + args
        with patch(
            'scrapers.nodriver_fbref.NodriverFBrefScraper', mock_scraper_class
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as scraper_module
            importlib.reload(scraper_module)
            return scraper_module.main()

    @pytest.mark.unit
    def test_combined_match_data_exits_2_without_scraping(self, temp_output_file):
        """nodriver + combined_match_data → exit 2, scrape_all NOT called."""
        scraper = MagicMock()
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        mock_class = MagicMock(return_value=scraper)

        with pytest.raises(SystemExit) as exc:
            self._run(mock_class, [
                '--scraper-type', 'nodriver',
                '--mode', 'combined_match_data',
                '--leagues', 'ENG-Premier League',
                '--season', '2024',
                '--output', temp_output_file,
            ])

        assert exc.value.code == 2
        scraper.scrape_all.assert_not_called()
        mock_class.assert_not_called()

        with open(temp_output_file) as f:
            result = json.load(f)
        assert any('not supported by the nodriver scraper' in e
                   for e in result['errors'])

    @pytest.mark.unit
    def test_full_mode_still_runs_scrape_all(self, temp_output_file):
        """Regression: explicit 'full' mode still reaches scrape_all."""
        scraper = MagicMock()
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper.scrape_all.return_value = {
            'schedule': 'iceberg.bronze.fbref_schedule'
        }
        scraper._stats = {'successes': 1, 'failures': 0}
        scraper.get_stats.return_value = scraper._stats
        mock_class = MagicMock(return_value=scraper)

        rc = self._run(mock_class, [
            '--scraper-type', 'nodriver',
            '--mode', 'full',
            '--leagues', 'ENG-Premier League',
            '--season', '2024',
            '--output', temp_output_file,
        ])

        assert rc in (0, None)
        scraper.scrape_all.assert_called_once()


class TestCombinedSeasonStatsRunner:
    """Runner wiring for --mode combined_season_stats (selenium branch)."""

    @pytest.fixture
    def temp_output_file(self):
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.fixture
    def temp_traffic_file(self):
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @staticmethod
    def _args(output, traffic, *, force=False):
        args = [
            '--scraper-type', 'selenium',
            '--use-nodriver',
            '--mode', 'combined_season_stats',
            '--leagues', 'ENG-Premier League',
            '--season', '2025',
            '--output', output,
            '--traffic-output', traffic,
        ]
        if force:
            args.append('--force-replace')
        return args

    @staticmethod
    def _stub_scraper():
        scraper = MagicMock()
        scraper._stats = {'successes': 5, 'failures': 0}
        scraper.get_stats.return_value = scraper._stats
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        return scraper

    @staticmethod
    def _run(scraper, args):
        sys.argv = ['run_fbref_scraper.py'] + args
        with patch(
            'scrapers.fbref.FBrefScraper',
            MagicMock(return_value=scraper),
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as m
            importlib.reload(m)
            return m.main()

    @pytest.mark.unit
    def test_success_exit_0_and_traffic_json(
        self, temp_output_file, temp_traffic_file
    ):
        scraper = self._stub_scraper()
        scraper.scrape_combined_season_stats.return_value = {
            'tables': {
                'player_stats': 'iceberg.bronze.fbref_player_stats',
                'team_stats': 'iceberg.bronze.fbref_team_stats',
                'keeper_keeper': 'iceberg.bronze.fbref_keeper_keeper',
            },
            'guard_refusals': [],
            'errors': [],
        }

        rc = self._run(scraper, self._args(temp_output_file, temp_traffic_file))

        assert rc in (0, None)
        with open(temp_output_file) as f:
            result = json.load(f)
        assert 'iceberg.bronze.fbref_player_stats' in result['tables']
        assert result['errors'] == []

        with open(temp_traffic_file) as f:
            traffic = json.load(f)
        assert traffic['label'] == 'season_stats'
        assert traffic['mode'] == 'combined_season_stats'

    @pytest.mark.unit
    def test_guard_refusals_exit_3_with_marker(
        self, temp_output_file, temp_traffic_file
    ):
        scraper = self._stub_scraper()
        scraper.scrape_combined_season_stats.return_value = {
            'tables': {'team_stats': 'iceberg.bronze.fbref_team_stats'},
            'guard_refusals': [
                'fbref_player_stats: new=2 rows < 90% of existing=380',
            ],
            'errors': [],
        }

        with pytest.raises(SystemExit) as exc:
            self._run(scraper, self._args(temp_output_file, temp_traffic_file))

        assert exc.value.code == 3
        with open(temp_output_file) as f:
            result = json.load(f)
        assert any('FBREF_REPLACE_GUARD' in e for e in result['errors'])

    @pytest.mark.unit
    def test_empty_result_exits_1(self, temp_output_file, temp_traffic_file):
        """combined_season_stats is a critical mode — 0 tables must fail."""
        scraper = self._stub_scraper()
        scraper.scrape_combined_season_stats.return_value = {
            'tables': {}, 'guard_refusals': [], 'errors': [],
        }

        rc = self._run(scraper, self._args(temp_output_file, temp_traffic_file))

        assert rc == 1

    @pytest.mark.unit
    def test_force_replace_threads_true(
        self, temp_output_file, temp_traffic_file
    ):
        scraper = self._stub_scraper()
        scraper.scrape_combined_season_stats.return_value = {
            'tables': {'player_stats': 'iceberg.bronze.fbref_player_stats'},
            'guard_refusals': [], 'errors': [],
        }

        rc = self._run(
            scraper, self._args(temp_output_file, temp_traffic_file, force=True)
        )

        assert rc in (0, None)
        kwargs = scraper.scrape_combined_season_stats.call_args.kwargs
        assert kwargs['force_replace'] is True

    @pytest.mark.unit
    def test_nodriver_branch_rejects_combined_season_stats(
        self, temp_output_file
    ):
        """The nodriver scraper does not implement this mode → exit 2."""
        scraper = self._stub_scraper()
        mock_class = MagicMock(return_value=scraper)

        sys.argv = ['run_fbref_scraper.py'] + [
            '--scraper-type', 'nodriver',
            '--mode', 'combined_season_stats',
            '--leagues', 'ENG-Premier League',
            '--season', '2025',
            '--output', temp_output_file,
        ]
        with patch(
            'scrapers.nodriver_fbref.NodriverFBrefScraper', mock_class
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as m
            importlib.reload(m)
            with pytest.raises(SystemExit) as exc:
                m.main()

        assert exc.value.code == 2
        mock_class.assert_not_called()


class TestSignalHandlers:
    """Issue #877: SIGTERM/SIGINT must unwind the scraper context manager so
    the camoufox/Firefox child dies with the runner instead of orphaning
    ~1 GB inside the container."""

    @pytest.fixture
    def temp_output_file(self):
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.mark.unit
    def test_handle_termination_raises_systemexit_143(self):
        import signal as _signal
        import dags.scripts.run_fbref_scraper as m

        with pytest.raises(SystemExit) as exc:
            m._handle_termination(_signal.SIGTERM, None)
        assert exc.value.code == 128 + _signal.SIGTERM  # 143

    @pytest.mark.unit
    def test_main_installs_sigterm_and_sigint_handlers(self, temp_output_file):
        import signal as _signal

        scraper = MagicMock()
        scraper._stats = {'successes': 1, 'failures': 0}
        scraper.get_stats.return_value = scraper._stats
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper.scrape_schedule.return_value = {
            'schedule': 'iceberg.bronze.fbref_schedule'
        }
        mock_class = MagicMock(return_value=scraper)

        sys.argv = ['run_fbref_scraper.py'] + [
            '--scraper-type', 'nodriver',
            '--mode', 'match_data',
            '--match-data-type', 'schedule',
            '--leagues', 'ENG-Premier League',
            '--season', '2024',
            '--output', temp_output_file,
        ]
        with patch(
            'scrapers.nodriver_fbref.NodriverFBrefScraper', mock_class
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as m
            importlib.reload(m)
            with patch.object(m.signal, 'signal') as mock_signal:
                m.main()

        registered = {call.args[0] for call in mock_signal.call_args_list}
        assert _signal.SIGTERM in registered
        assert _signal.SIGINT in registered
        for call in mock_signal.call_args_list:
            if call.args[0] in (_signal.SIGTERM, _signal.SIGINT):
                assert call.args[1] is m._handle_termination

    @pytest.mark.unit
    def test_systemexit_unwinds_scraper_context(self, temp_output_file):
        """SystemExit raised mid-scrape must propagate (not be swallowed by
        the generic error handling) AND call the scraper __exit__ so the
        browser child is closed."""
        scraper = MagicMock()
        scraper._stats = {'successes': 0, 'failures': 0}
        scraper.get_stats.return_value = scraper._stats
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper.scrape_schedule.side_effect = SystemExit(143)
        mock_class = MagicMock(return_value=scraper)

        sys.argv = ['run_fbref_scraper.py'] + [
            '--scraper-type', 'nodriver',
            '--mode', 'match_data',
            '--match-data-type', 'schedule',
            '--leagues', 'ENG-Premier League',
            '--season', '2024',
            '--output', temp_output_file,
        ]
        with patch(
            'scrapers.nodriver_fbref.NodriverFBrefScraper', mock_class
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as m
            importlib.reload(m)
            with pytest.raises(SystemExit) as exc:
                m.main()

        assert exc.value.code == 143
        assert scraper.__exit__.called


class TestTournamentOutOfWindow:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        ('mode_args', 'expected_label'),
        [
            (['--mode', 'combined_season_stats'], 'season_stats'),
            (['--mode', 'combined_match_data'], 'match_all_data'),
            (
                ['--mode', 'match_data', '--match-data-type', 'schedule'],
                'match_schedule',
            ),
        ],
    )
    def test_writes_zero_traffic_manifest(
        self, tmp_path, mode_args, expected_label
    ):
        import importlib
        import dags.scripts.run_fbref_scraper as module

        module = importlib.reload(module)
        output = tmp_path / 'result.json'
        traffic = tmp_path / 'traffic.json'
        sys.argv = ['run_fbref_scraper.py'] + mode_args + [
            '--scraper-type', 'selenium',
            '--leagues', 'INT-World Cup',
            '--season', '2025',
            '--output', str(output),
            '--traffic-output', str(traffic),
        ]

        with patch(
            'utils.medallion_config.get_active_season', return_value=None
        ):
            rc = module.main()

        assert rc == 0
        assert json.loads(output.read_text())['skipped'] == 'out_of_window'
        traffic_payload = json.loads(traffic.read_text())
        assert traffic_payload['label'] == expected_label
        assert traffic_payload['real_proxy_mb'] == 0.0
        assert traffic_payload['skipped'] == 'out_of_window'


class TestCompletedScheduleLeagues:
    """Issue #877: Trino probe for already-backfilled schedule seasons."""

    def _fake_conn(self, rows=None, raise_exc=None):
        cursor = MagicMock()
        if raise_exc is not None:
            cursor.execute.side_effect = raise_exc
        cursor.fetchall.return_value = rows or []
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    @pytest.mark.unit
    def test_league_at_or_above_floor_is_complete(self):
        import dags.scripts.run_fbref_scraper as m

        conn, cursor = self._fake_conn(
            rows=[('ESP-La Liga', 380), ('GER-Bundesliga', 120)]
        )
        with patch.object(m, '_trino_connect', return_value=conn):
            done = m._completed_schedule_leagues(
                ['ESP-La Liga', 'GER-Bundesliga'], 2018
            )

        assert done == {'ESP-La Liga'}
        sql = cursor.execute.call_args.args[0]
        assert 'bronze.fbref_schedule' in sql
        assert cursor.execute.call_args.args[1][0] == 2018

    @pytest.mark.unit
    def test_floor_overridable_via_env(self, monkeypatch):
        import dags.scripts.run_fbref_scraper as m

        monkeypatch.setenv('FBREF_SCHEDULE_MIN_ROWS', '400')
        conn, _ = self._fake_conn(rows=[('ESP-La Liga', 380)])
        with patch.object(m, '_trino_connect', return_value=conn):
            done = m._completed_schedule_leagues(['ESP-La Liga'], 2018)

        assert done == set()

    @pytest.mark.unit
    def test_fail_open_on_trino_error(self):
        import dags.scripts.run_fbref_scraper as m

        conn, _ = self._fake_conn(raise_exc=RuntimeError('trino down'))
        with patch.object(m, '_trino_connect', return_value=conn):
            done = m._completed_schedule_leagues(['ESP-La Liga'], 2018)

        assert done == set()

    @pytest.mark.unit
    def test_fail_open_when_no_connection(self):
        import dags.scripts.run_fbref_scraper as m

        with patch.object(m, '_trino_connect', return_value=None):
            done = m._completed_schedule_leagues(['ESP-La Liga'], 2018)

        assert done == set()


class TestSkipExisting:
    """Issue #877: --skip-existing must avoid re-fetching complete schedule
    seasons (no browser start, zeroed traffic JSON, exit 0)."""

    @pytest.fixture
    def temp_output_file(self):
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        yield path
        if os.path.exists(path):
            os.unlink(path)

    @pytest.fixture
    def temp_traffic_file(self):
        fd, path = tempfile.mkstemp(suffix='.json')
        os.close(fd)
        os.unlink(path)  # runner must create it
        yield path
        if os.path.exists(path):
            os.unlink(path)

    def _make_scraper(self):
        scraper = MagicMock()
        scraper._stats = {'successes': 1, 'failures': 0}
        scraper.get_stats.return_value = scraper._stats
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper.scrape_schedule.return_value = {
            'schedule': 'iceberg.bronze.fbref_schedule'
        }
        return scraper

    def _run(self, module, args, mock_class):
        sys.argv = ['run_fbref_scraper.py'] + args
        with patch(
            'scrapers.nodriver_fbref.NodriverFBrefScraper', mock_class
        ):
            return module.main()

    def _reload(self):
        import importlib
        import dags.scripts.run_fbref_scraper as m
        importlib.reload(m)
        return m

    @pytest.mark.unit
    def test_full_skip_is_noop_exit_0(self, temp_output_file, temp_traffic_file):
        m = self._reload()
        mock_class = MagicMock(return_value=self._make_scraper())

        with patch.object(
            m, '_completed_schedule_leagues',
            return_value={'ESP-La Liga'},
        ) as probe:
            rc = self._run(m, [
                '--scraper-type', 'nodriver',
                '--mode', 'match_data',
                '--match-data-type', 'schedule',
                '--leagues', 'ESP-La Liga',
                '--season', '2018',
                '--skip-existing',
                '--output', temp_output_file,
                '--traffic-output', temp_traffic_file,
            ], mock_class)

        assert rc in (0, None)
        probe.assert_called_once()
        mock_class.assert_not_called()  # no browser start

        with open(temp_output_file) as f:
            result = json.load(f)
        assert result['diagnostics']['all_already_scraped'] is True
        assert result['diagnostics']['skipped_leagues'] == ['ESP-La Liga']
        assert result['tables'] == ['iceberg.bronze.fbref_schedule']

        with open(temp_traffic_file) as f:
            traffic = json.load(f)
        assert traffic['real_proxy_mb'] == 0.0
        assert traffic['http_mb_downloaded'] == 0.0
        assert traffic['skip_existing'] is True
        assert traffic['label'] == 'match_schedule'

    @pytest.mark.unit
    def test_partial_skip_scrapes_remaining_leagues(self, temp_output_file):
        m = self._reload()
        scraper = self._make_scraper()
        mock_class = MagicMock(return_value=scraper)

        with patch.object(
            m, '_completed_schedule_leagues',
            return_value={'ESP-La Liga'},
        ):
            rc = self._run(m, [
                '--scraper-type', 'nodriver',
                '--mode', 'match_data',
                '--match-data-type', 'schedule',
                '--leagues', 'ESP-La Liga,GER-Bundesliga',
                '--season', '2018',
                '--skip-existing',
                '--output', temp_output_file,
            ], mock_class)

        assert rc in (0, None)
        assert mock_class.call_args.kwargs['leagues'] == ['GER-Bundesliga']

    @pytest.mark.unit
    def test_fail_open_scrapes_everything(self, temp_output_file):
        m = self._reload()
        scraper = self._make_scraper()
        mock_class = MagicMock(return_value=scraper)

        with patch.object(
            m, '_completed_schedule_leagues', return_value=set()
        ):
            rc = self._run(m, [
                '--scraper-type', 'nodriver',
                '--mode', 'match_data',
                '--match-data-type', 'schedule',
                '--leagues', 'ESP-La Liga',
                '--season', '2018',
                '--skip-existing',
                '--output', temp_output_file,
            ], mock_class)

        assert rc in (0, None)
        assert mock_class.call_args.kwargs['leagues'] == ['ESP-La Liga']

    @pytest.mark.unit
    def test_current_season_never_skipped(self, temp_output_file):
        m = self._reload()
        scraper = self._make_scraper()
        mock_class = MagicMock(return_value=scraper)

        from datetime import datetime as _dt
        _now = _dt.now()
        current = _now.year if _now.month >= 8 else _now.year - 1

        with patch.object(
            m, '_completed_schedule_leagues',
            return_value={'ESP-La Liga'},
        ) as probe:
            rc = self._run(m, [
                '--scraper-type', 'nodriver',
                '--mode', 'match_data',
                '--match-data-type', 'schedule',
                '--leagues', 'ESP-La Liga',
                '--season', str(current),
                '--skip-existing',
                '--output', temp_output_file,
            ], mock_class)

        assert rc in (0, None)
        probe.assert_not_called()
        assert mock_class.call_args.kwargs['leagues'] == ['ESP-La Liga']

    @pytest.mark.unit
    def test_without_flag_probe_not_called(self, temp_output_file):
        m = self._reload()
        scraper = self._make_scraper()
        mock_class = MagicMock(return_value=scraper)

        with patch.object(
            m, '_completed_schedule_leagues',
            return_value={'ESP-La Liga'},
        ) as probe:
            rc = self._run(m, [
                '--scraper-type', 'nodriver',
                '--mode', 'match_data',
                '--match-data-type', 'schedule',
                '--leagues', 'ESP-La Liga',
                '--season', '2018',
                '--output', temp_output_file,
            ], mock_class)

        assert rc in (0, None)
        probe.assert_not_called()
        assert mock_class.call_args.kwargs['leagues'] == ['ESP-La Liga']
class TestCombinedMatchFallbackManifest:
    """Local JSON recovery is not a successful Iceberg table write."""

    @staticmethod
    def _run(scraper, output, traffic):
        sys.argv = ['run_fbref_scraper.py'] + [
            '--scraper-type', 'selenium',
            '--use-nodriver',
            '--mode', 'combined_match_data',
            '--leagues', 'ENG-Premier League',
            '--season', '2025',
            '--output', output,
            '--traffic-output', traffic,
        ]
        with patch(
            'scrapers.fbref.FBrefScraper',
            MagicMock(return_value=scraper),
        ):
            import importlib
            import dags.scripts.run_fbref_scraper as module
            importlib.reload(module)
            return module.main()

    @pytest.mark.unit
    def test_fallback_is_separate_from_tables_and_exits_nonzero(self, tmp_path):
        scraper = MagicMock()
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper._stats = {
            'successes': 1,
            'failures': 0,
            'schedule_source': 'iceberg',
            'trino_available': True,
        }
        fallback = str(tmp_path / 'fbref_batch_lineups.json')
        scraper.scrape_combined_match_data.return_value = {
            'match_events': 'iceberg.bronze.fbref_match_events',
            'lineups_fallback': fallback,
        }
        output = str(tmp_path / 'result.json')
        traffic = str(tmp_path / 'traffic.json')

        rc = self._run(scraper, output, traffic)

        assert rc == 1
        with open(output) as handle:
            result = json.load(handle)
        assert result['tables'] == [
            'iceberg.bronze.fbref_match_events'
        ]
        assert result['fallback_files'] == [fallback]
        assert any('Iceberg persistence was incomplete' in error
                   for error in result['errors'])

    @pytest.mark.unit
    def test_verified_incremental_noop_exits_zero(self, tmp_path):
        scraper = MagicMock()
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        verified = [
            'iceberg.bronze.fbref_match_events',
            'iceberg.bronze.fbref_lineups',
            'iceberg.bronze.fbref_match_team_stats',
            'iceberg.bronze.fbref_match_player_stats',
        ]
        scraper._stats = {
            'successes': 0,
            'failures': 0,
            'schedule_source': 'file',
            'trino_available': True,
            'eligible_match_ids': 10,
            'pending_match_ids': 0,
            'all_already_scraped': True,
            'verified_noop_table_paths': verified,
            'missing_noop_tables': [],
        }
        scraper.scrape_combined_match_data.return_value = {}
        output = str(tmp_path / 'result.json')
        traffic = str(tmp_path / 'traffic.json')

        rc = self._run(scraper, output, traffic)

        assert rc in (0, None)
        with open(output) as handle:
            result = json.load(handle)
        assert result['tables'] == verified
        assert result['diagnostics']['all_already_scraped'] is True

    @pytest.mark.unit
    def test_unverified_empty_schedule_cannot_fabricate_tables(self, tmp_path):
        scraper = MagicMock()
        scraper.__enter__ = MagicMock(return_value=scraper)
        scraper.__exit__ = MagicMock(return_value=False)
        scraper._stats = {
            'successes': 0,
            'failures': 0,
            'schedule_source': 'file',
            'trino_available': True,
            'eligible_match_ids': 0,
            'pending_match_ids': 0,
            'all_already_scraped': False,
            'verified_noop_table_paths': [],
            'missing_noop_tables': [],
        }
        scraper.scrape_combined_match_data.return_value = {}
        output = str(tmp_path / 'result.json')
        traffic = str(tmp_path / 'traffic.json')

        rc = self._run(scraper, output, traffic)

        assert rc == 1
        with open(output) as handle:
            result = json.load(handle)
        assert result['tables'] == []
        assert result['errors']
