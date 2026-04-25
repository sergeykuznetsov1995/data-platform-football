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
