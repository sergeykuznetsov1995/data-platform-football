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
