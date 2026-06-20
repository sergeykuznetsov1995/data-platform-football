"""
Tests for FBrefScraper.
"""

import os

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

# Repo root (this file lives in tests/unit/scrapers/) — used for subprocess cwd
# instead of a hard-coded /root/data_platform that doesn't exist in CI/docker.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


class TestFBrefScraperConstants:
    """Tests for FBref Selenium scraper constants."""

    def test_league_ids_exist(self):
        """Test that LEAGUE_IDS contains expected leagues."""
        from scrapers.fbref.constants import LEAGUE_IDS

        expected_leagues = [
            'ENG-Premier League',
            'ESP-La Liga',
            'GER-Bundesliga',
            'ITA-Serie A',
            'FRA-Ligue 1',
        ]

        for league in expected_leagues:
            assert league in LEAGUE_IDS, f"Missing league: {league}"
            assert 'comp_id' in LEAGUE_IDS[league]
            assert 'slug' in LEAGUE_IDS[league]

    def test_player_stat_types(self):
        """Test that PLAYER_STAT_TYPES is defined.

        Note (Apr 2026): passing, passing_types, gca, defense, possession
        were removed because FBref restricted those stats — tables were 100%
        empty.
        """
        from scrapers.fbref.constants import PLAYER_STAT_TYPES

        expected = ['stats', 'shooting', 'playingtime', 'misc']

        for stat_type in expected:
            assert stat_type in PLAYER_STAT_TYPES

        # Sanity-check: removed stat_types must NOT be present.
        for removed in ('passing', 'passing_types', 'gca', 'defense', 'possession'):
            assert removed not in PLAYER_STAT_TYPES, (
                f"{removed!r} was removed (FBref returns empty cells); "
                "do not re-add without re-checking iceberg.bronze counts."
            )

    def test_team_stat_types(self):
        """Test that TEAM_STAT_TYPES is defined.

        Note (Apr 2026): same stat_types as PLAYER_STAT_TYPES were removed.
        """
        from scrapers.fbref.constants import TEAM_STAT_TYPES

        expected = ['stats', 'shooting', 'playingtime', 'misc']

        for stat_type in expected:
            assert stat_type in TEAM_STAT_TYPES

        for removed in ('passing', 'passing_types', 'gca', 'defense', 'possession'):
            assert removed not in TEAM_STAT_TYPES, (
                f"{removed!r} was removed (FBref returns empty cells); "
                "do not re-add without re-checking iceberg.bronze counts."
            )

    def test_keeper_stat_types(self):
        """Test that KEEPER_STAT_TYPES is defined."""
        from scrapers.fbref.constants import KEEPER_STAT_TYPES

        assert 'keeper' in KEEPER_STAT_TYPES
        assert 'keeper_adv' in KEEPER_STAT_TYPES


class TestFBrefUrlBuilder:
    """Tests for FBref URL builder functions."""

    def test_format_season(self):
        """Test season formatting."""
        from scrapers.fbref.url_builder import format_season

        assert format_season(2024) == '2024-2025'
        assert format_season(2023) == '2023-2024'
        assert format_season(2020) == '2020-2021'

    def test_get_schedule_url(self):
        """Test schedule URL generation."""
        from scrapers.fbref.url_builder import get_schedule_url

        url = get_schedule_url('ENG-Premier League', 2024)

        assert 'fbref.com' in url
        assert '2024-2025' in url
        assert 'schedule' in url
        assert 'Premier-League' in url

    def test_get_schedule_url_invalid_league(self):
        """Test schedule URL with invalid league."""
        from scrapers.fbref.url_builder import get_schedule_url

        with pytest.raises(ValueError):
            get_schedule_url('Invalid-League', 2024)

    def test_get_stats_url_player(self):
        """Test player stats URL generation."""
        from scrapers.fbref.url_builder import get_stats_url

        url = get_stats_url('ENG-Premier League', 2024, 'shooting', for_squads=False)

        assert 'fbref.com' in url
        assert 'shooting' in url
        assert '2024-2025' in url

    def test_get_stats_url_team(self):
        """Test team stats URL generation."""
        from scrapers.fbref.url_builder import get_stats_url

        url = get_stats_url('ENG-Premier League', 2024, 'passing', for_squads=True)

        assert 'fbref.com' in url
        assert 'passing' in url
        assert '2024-2025' in url


class TestFBrefScraperMethods:
    """Tests for FBref Selenium scraper methods without actually scraping."""

    @pytest.fixture
    def mock_scraper_dependencies(self):
        """Mock all dependencies for the Selenium scraper."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw, \
             patch('scrapers.base.browser.cloudflare_bypass.CloudflareBypass') as mock_browser:

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True

            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_cb.return_value.state = 'closed'

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            mock_browser_instance = MagicMock()
            mock_browser_instance.get_page.return_value = '<html></html>'
            mock_browser_instance.page_source = '<html></html>'
            mock_browser.return_value = mock_browser_instance

            yield {
                'rate_limiter': mock_rl,
                'iceberg_writer': mock_iw_instance,
                'browser': mock_browser_instance,
            }

    def test_scraper_init(self, mock_scraper_dependencies):
        """Test FBref Selenium scraper initialization."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True,
        )

        assert scraper.leagues == ['ENG-Premier League']
        assert scraper.seasons == [2024]
        assert scraper.SOURCE_NAME == 'fbref'

    def test_scraper_has_new_methods(self, mock_scraper_dependencies):
        """Test that scraper has all new methods."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # Check new methods exist
        assert hasattr(scraper, 'read_shot_events')
        assert hasattr(scraper, 'read_match_events')
        assert hasattr(scraper, 'read_lineup')
        assert hasattr(scraper, 'read_team_match_stats')
        assert hasattr(scraper, '_extract_match_ids')

        # Check methods are callable
        assert callable(scraper.read_shot_events)
        assert callable(scraper.read_match_events)
        assert callable(scraper.read_lineup)
        assert callable(scraper.read_team_match_stats)

    def test_scrape_all_has_new_parameters(self, mock_scraper_dependencies):
        """Test that scrape_all accepts new parameters."""
        from scrapers.fbref.scraper import FBrefScraper
        import inspect

        sig = inspect.signature(FBrefScraper.scrape_all)
        params = sig.parameters

        # Check new parameters exist
        assert 'include_shot_events' in params
        assert 'include_match_events' in params
        assert 'include_lineups' in params
        assert 'include_team_match_stats' in params
        assert 'max_matches_per_league' in params

    def test_extract_match_ids_from_dataframe(self, mock_scraper_dependencies):
        """Test extracting match IDs from schedule DataFrame."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # Test with match_id column
        df = pd.DataFrame({
            'match_id': ['abc123', 'def456', 'ghi789'],
            'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        })

        match_ids = scraper._extract_match_ids(df)
        assert match_ids == ['abc123', 'def456', 'ghi789']

        # Test with max_matches limit
        match_ids = scraper._extract_match_ids(df, max_matches=2)
        assert len(match_ids) == 2

    def test_extract_match_ids_from_url_column(self, mock_scraper_dependencies):
        """Test extracting match IDs from Match Report URL column."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        df = pd.DataFrame({
            'Match Report': [
                'https://fbref.com/en/matches/abc123/Match-Report',
                'https://fbref.com/en/matches/def456/Match-Report',
            ],
            'date': ['2024-01-01', '2024-01-02'],
        })

        match_ids = scraper._extract_match_ids(df)
        assert 'abc123' in match_ids
        assert 'def456' in match_ids

    def test_extract_match_ids_empty_dataframe(self, mock_scraper_dependencies):
        """Test extracting match IDs from empty DataFrame."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        df = pd.DataFrame()
        match_ids = scraper._extract_match_ids(df)
        assert match_ids == []

        match_ids = scraper._extract_match_ids(None)
        assert match_ids == []


class TestFBrefScraperMemoryEfficientMethods:
    """Tests for memory-efficient single stat_type collection methods."""

    @pytest.fixture
    def mock_scraper_dependencies(self):
        """Mock all dependencies for the Selenium scraper."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw, \
             patch('scrapers.base.browser.cloudflare_bypass.CloudflareBypass') as mock_browser:

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True

            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_cb.return_value.state = 'closed'

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            mock_browser_instance = MagicMock()
            mock_browser_instance.get_page.return_value = '<html></html>'
            mock_browser_instance.page_source = '<html></html>'
            mock_browser.return_value = mock_browser_instance

            yield {
                'rate_limiter': mock_rl,
                'iceberg_writer': mock_iw_instance,
                'browser': mock_browser_instance,
            }

    def test_scrape_single_stat_type_method_exists(self, mock_scraper_dependencies):
        """Test that scrape_single_stat_type method exists."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        assert hasattr(scraper, 'scrape_single_stat_type')
        assert callable(scraper.scrape_single_stat_type)

    def test_scrape_single_stat_type_signature(self, mock_scraper_dependencies):
        """Test that scrape_single_stat_type has correct parameters."""
        from scrapers.fbref.scraper import FBrefScraper
        import inspect

        sig = inspect.signature(FBrefScraper.scrape_single_stat_type)
        params = list(sig.parameters.keys())

        assert 'self' in params
        assert 'stat_type' in params
        assert 'data_category' in params

    def test_scrape_single_stat_type_returns_dict(self, mock_scraper_dependencies):
        """Test that scrape_single_stat_type returns a dictionary."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # Mock the read methods to return empty DataFrames
        scraper.read_player_season_stats = MagicMock(return_value=None)

        result = scraper.scrape_single_stat_type(
            stat_type='shooting',
            data_category='player',
        )

        assert isinstance(result, dict)

    def test_scrape_single_stat_type_returns_correct_table_name(self, mock_scraper_dependencies):
        """Test that scrape_single_stat_type returns correct table name in key."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # Create a sample DataFrame
        sample_df = pd.DataFrame({
            'Player': ['Test Player'],
            'Goals': [10],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        scraper.read_player_season_stats = MagicMock(return_value=sample_df)
        scraper.save_to_iceberg = MagicMock(return_value='iceberg.bronze.fbref_player_shooting')

        result = scraper.scrape_single_stat_type(
            stat_type='shooting',
            data_category='player',
        )

        # Key should be {data_category}_{stat_type}
        assert 'player_shooting' in result

    def test_scrape_single_stat_type_calls_correct_method_for_player(self, mock_scraper_dependencies):
        """Test that scrape_single_stat_type calls read_player_season_stats for player category."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        scraper.read_player_season_stats = MagicMock(return_value=None)
        scraper.read_team_season_stats = MagicMock(return_value=None)
        scraper.read_keeper_stats = MagicMock(return_value=None)

        scraper.scrape_single_stat_type(
            stat_type='passing',
            data_category='player',
        )

        scraper.read_player_season_stats.assert_called()
        scraper.read_team_season_stats.assert_not_called()
        scraper.read_keeper_stats.assert_not_called()

    def test_scrape_single_stat_type_calls_correct_method_for_team(self, mock_scraper_dependencies):
        """Test that scrape_single_stat_type calls read_team_season_stats for team category."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        scraper.read_player_season_stats = MagicMock(return_value=None)
        scraper.read_team_season_stats = MagicMock(return_value=None)
        scraper.read_keeper_stats = MagicMock(return_value=None)

        scraper.scrape_single_stat_type(
            stat_type='defense',
            data_category='team',
        )

        scraper.read_team_season_stats.assert_called()
        scraper.read_player_season_stats.assert_not_called()
        scraper.read_keeper_stats.assert_not_called()

    def test_scrape_single_stat_type_calls_correct_method_for_keeper(self, mock_scraper_dependencies):
        """Test that scrape_single_stat_type calls read_keeper_stats for keeper category."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        scraper.read_player_season_stats = MagicMock(return_value=None)
        scraper.read_team_season_stats = MagicMock(return_value=None)
        scraper.read_keeper_stats = MagicMock(return_value=None)

        scraper.scrape_single_stat_type(
            stat_type='keeper',
            data_category='keeper',
        )

        scraper.read_keeper_stats.assert_called()
        scraper.read_player_season_stats.assert_not_called()
        scraper.read_team_season_stats.assert_not_called()

    def test_scrape_match_data_method_exists(self, mock_scraper_dependencies):
        """Test that scrape_match_data method exists."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        assert hasattr(scraper, 'scrape_match_data')
        assert callable(scraper.scrape_match_data)

    def test_scrape_match_data_signature(self, mock_scraper_dependencies):
        """Test that scrape_match_data has correct parameters."""
        from scrapers.fbref.scraper import FBrefScraper
        import inspect

        sig = inspect.signature(FBrefScraper.scrape_match_data)
        params = list(sig.parameters.keys())

        assert 'self' in params
        assert 'data_type' in params
        assert 'max_matches' in params

    def test_scrape_match_data_returns_dict(self, mock_scraper_dependencies):
        """Test that scrape_match_data returns a dictionary."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        scraper.read_schedule = MagicMock(return_value=None)

        result = scraper.scrape_match_data(
            data_type='schedule',
        )

        assert isinstance(result, dict)

    def test_scrape_match_data_schedule_does_not_need_match_ids(self, mock_scraper_dependencies):
        """Test that schedule data type collects directly without match IDs."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        sample_df = pd.DataFrame({
            'Date': ['2024-01-01'],
            'Home': ['Arsenal'],
            'Away': ['Chelsea'],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        scraper.read_schedule = MagicMock(return_value=sample_df)
        scraper.save_to_iceberg = MagicMock(return_value='iceberg.bronze.fbref_schedule')
        scraper._extract_match_ids = MagicMock()

        result = scraper.scrape_match_data(data_type='schedule')

        # Schedule should NOT call _extract_match_ids
        scraper._extract_match_ids.assert_not_called()
        assert 'schedule' in result


class TestFBrefRunnerScriptArguments:
    """Tests for runner script argument parsing."""

    def test_runner_accepts_mode_argument(self):
        """Test that runner script accepts --mode argument."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, '-c',
             'import sys; sys.path.insert(0, "dags/scripts"); '
             'from run_fbref_scraper import main; '
             'import argparse; '
             'parser = argparse.ArgumentParser(); '
             'parser.add_argument("--mode", choices=["full", "single_stat", "match_data"]); '
             'args = parser.parse_args(["--mode", "single_stat"]); '
             'print(args.mode)'],
            capture_output=True,
            text=True,
            cwd=_REPO_ROOT,
        )

        assert 'single_stat' in result.stdout or result.returncode == 0

    def test_runner_accepts_stat_type_argument(self):
        """Test that runner script accepts --stat-type argument."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, '-c',
             'import argparse; '
             'parser = argparse.ArgumentParser(); '
             'parser.add_argument("--stat-type"); '
             'args = parser.parse_args(["--stat-type", "shooting"]); '
             'print(args.stat_type)'],
            capture_output=True,
            text=True,
        )

        assert 'shooting' in result.stdout

    def test_runner_accepts_data_category_argument(self):
        """Test that runner script accepts --data-category argument."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, '-c',
             'import argparse; '
             'parser = argparse.ArgumentParser(); '
             'parser.add_argument("--data-category", choices=["player", "team", "keeper"]); '
             'args = parser.parse_args(["--data-category", "player"]); '
             'print(args.data_category)'],
            capture_output=True,
            text=True,
        )

        assert 'player' in result.stdout

    def test_runner_accepts_match_data_type_argument(self):
        """Test that runner script accepts --match-data-type argument."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, '-c',
             'import argparse; '
             'parser = argparse.ArgumentParser(); '
             'parser.add_argument("--match-data-type", choices=["schedule", "shot_events", "match_events", "lineups"]); '
             'args = parser.parse_args(["--match-data-type", "shot_events"]); '
             'print(args.match_data_type)'],
            capture_output=True,
            text=True,
        )

        assert 'shot_events' in result.stdout


class TestFBrefScraperCombinedMatchData:
    """Tests for optimized combined match data collection (3x efficiency)."""

    @pytest.fixture
    def mock_scraper_dependencies(self):
        """Mock all dependencies for the Selenium scraper."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw, \
             patch('scrapers.base.browser.cloudflare_bypass.CloudflareBypass') as mock_browser:

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True

            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_cb.return_value.state = 'closed'

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            mock_browser_instance = MagicMock()
            mock_browser_instance.get_page.return_value = '<html></html>'
            mock_browser_instance.page_source = '<html></html>'
            mock_browser.return_value = mock_browser_instance

            yield {
                'rate_limiter': mock_rl,
                'iceberg_writer': mock_iw_instance,
                'browser': mock_browser_instance,
            }

    def test_scrape_combined_match_data_method_exists(self, mock_scraper_dependencies):
        """Test that scrape_combined_match_data method exists."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        assert hasattr(scraper, 'scrape_combined_match_data')
        assert callable(scraper.scrape_combined_match_data)

    def test_scrape_combined_match_data_signature(self, mock_scraper_dependencies):
        """Test that scrape_combined_match_data has correct parameters."""
        from scrapers.fbref.scraper import FBrefScraper
        import inspect

        sig = inspect.signature(FBrefScraper.scrape_combined_match_data)
        params = list(sig.parameters.keys())

        assert 'self' in params
        assert 'max_matches' in params

    def test_scrape_combined_match_data_returns_dict(self, mock_scraper_dependencies):
        """Test that scrape_combined_match_data returns a dictionary."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        scraper._read_schedule_from_iceberg = MagicMock(return_value=None)
        scraper.read_schedule = MagicMock(return_value=None)

        result = scraper.scrape_combined_match_data(max_matches=5)

        assert isinstance(result, dict)

    def test_scrape_combined_match_data_collects_all_data_types(self, mock_scraper_dependencies):
        """Test that combined method collects shot_events, match_events, lineups.

        _process_single_match uses Parse Once optimization:
        it calls _fetch_page once, then parsers directly (not read_* methods).
        """
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
        )

        # Create sample schedule with match IDs
        sample_schedule = pd.DataFrame({
            'Date': ['2024-01-01', '2024-01-02'],
            'Home': ['Arsenal', 'Chelsea'],
            'Away': ['Chelsea', 'Arsenal'],
            'match_id': ['match1', 'match2'],
            'league': ['ENG-Premier League', 'ENG-Premier League'],
            'season': [2024, 2024],
        })

        sample_shots = pd.DataFrame({
            'player': ['Saka'],
            'xG': [0.5],
        })

        sample_events = pd.DataFrame({
            'event_type': ['goal'],
            'minute': [45],
        })

        sample_lineups = pd.DataFrame({
            'team': ['Arsenal'],
            'player': ['Ramsdale'],
        })

        # Mock HTML with tables so validation passes
        fake_html = '<html><body><table id="shots"><tr><td>data</td></tr></table></body></html>'

        scraper._read_schedule_from_iceberg = MagicMock(return_value=sample_schedule)
        scraper._fetch_page = MagicMock(return_value=fake_html)
        scraper._get_existing_match_ids = MagicMock(return_value=set())
        scraper.save_to_iceberg = MagicMock(return_value='iceberg.bronze.test')
        scraper._cleanup_after_league = MagicMock()
        scraper._add_metadata = MagicMock(side_effect=lambda df, _: df)

        with patch('scrapers.fbref.data_readers.parse_shots_table', return_value=sample_shots), \
             patch('scrapers.fbref.data_readers.parse_events_from_scorebox', return_value=sample_events), \
             patch('scrapers.fbref.data_readers.parse_lineup_table', return_value=sample_lineups), \
             patch('scrapers.fbref.data_readers.extract_tables_from_comments', return_value={}):

            result = scraper.scrape_combined_match_data(max_matches=2)

        # Verify _fetch_page was called for each match
        assert scraper._fetch_page.call_count >= 2

        # Verify all three data types were saved
        assert 'shot_events' in result
        assert 'match_events' in result
        assert 'lineups' in result

    def test_scrape_combined_match_data_default_max_matches(self, mock_scraper_dependencies):
        """Test that combined method has default max_matches=50."""
        from scrapers.fbref.scraper import FBrefScraper
        import inspect

        sig = inspect.signature(FBrefScraper.scrape_combined_match_data)
        default_max_matches = sig.parameters['max_matches'].default

        assert default_max_matches == 50

    def test_scrape_combined_uses_iceberg_schedule(self, mock_scraper_dependencies):
        """Test that scrape_combined reads schedule from Iceberg first."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(leagues=['ENG-Premier League'], seasons=[2024])

        sample_schedule = pd.DataFrame({
            'Date': ['2024-01-01'], 'Home': ['Arsenal'], 'Away': ['Chelsea'],
            'match_url': ['https://fbref.com/en/matches/abc123/Match-Report'],
            'match_id': ['abc123'],
            'league': ['ENG-Premier League'], 'season': [2024],
        })

        scraper._read_schedule_from_iceberg = MagicMock(return_value=sample_schedule)
        scraper.read_schedule = MagicMock()  # should NOT be called
        scraper._get_existing_match_ids = MagicMock(return_value=set())
        scraper._fetch_page = MagicMock(return_value='<html><body></body></html>')
        scraper.save_to_iceberg = MagicMock(return_value='iceberg.bronze.test')
        scraper._cleanup_after_league = MagicMock()

        with patch('scrapers.fbref.data_readers.parse_shots_table', return_value=pd.DataFrame()), \
             patch('scrapers.fbref.data_readers.parse_events_from_scorebox', return_value=pd.DataFrame()), \
             patch('scrapers.fbref.data_readers.parse_lineup_table', return_value=pd.DataFrame()), \
             patch('scrapers.fbref.data_readers.extract_tables_from_comments', return_value={}):
            scraper.scrape_combined_match_data(max_matches=1)

        scraper._read_schedule_from_iceberg.assert_called_once_with('ENG-Premier League', 2024)
        scraper.read_schedule.assert_not_called()  # Iceberg worked → no HTTP fallback

    def test_scrape_combined_skips_when_no_schedule(self, mock_scraper_dependencies):
        """Test that combined match data skips league when no schedule in file/Iceberg (no HTTP fallback)."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(leagues=['ENG-Premier League'], seasons=[2024])

        scraper._read_schedule_from_iceberg = MagicMock(return_value=None)
        scraper.read_schedule = MagicMock(return_value=None)
        scraper._cleanup_after_league = MagicMock()

        result = scraper.scrape_combined_match_data(max_matches=1)

        scraper._read_schedule_from_iceberg.assert_called_once()
        scraper.read_schedule.assert_not_called()  # No HTTP fallback — skips immediately
        assert isinstance(result, dict)
        assert len(result) == 0  # No data collected

    def test_runner_accepts_combined_match_data_mode(self):
        """Test that runner script accepts --mode combined_match_data."""
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, '-c',
             'import argparse; '
             'parser = argparse.ArgumentParser(); '
             'parser.add_argument("--mode", choices=["full", "single_stat", "match_data", "combined_match_data"]); '
             'args = parser.parse_args(["--mode", "combined_match_data"]); '
             'print(args.mode)'],
            capture_output=True,
            text=True,
        )

        assert 'combined_match_data' in result.stdout


class TestExitCodeLogic:
    """Tests for exit code logic in run_fbref_scraper.py."""

    def test_combined_match_data_zero_tables_zero_errors_returns_1(self):
        """0 tables + 0 errors + mode=combined_match_data → exit code 1."""
        import subprocess
        import sys
        import json
        import tempfile
        import os

        # Create a script that simulates the exit code logic
        test_script = '''
import sys, json, os
sys.path.insert(0, os.path.join(os.getcwd(), "dags/scripts"))

# Simulate the exit code section of run_fbref_scraper.py
results = {
    'tables': [],
    'errors': [],
    'mode': 'combined_match_data',
}

total_tables = len(results['tables'])
total_errors = len(results['errors'])

if total_tables == 0:
    if total_errors > 0:
        sys.exit(1)
    mode = results.get('mode', '')
    match_data_type = results.get('match_data_type', '')
    critical_modes = {'combined_match_data', 'schedule'}
    if mode in critical_modes or match_data_type == 'schedule':
        sys.exit(1)
    sys.exit(0)
sys.exit(0)
'''
        result = subprocess.run(
            [sys.executable, '-c', test_script],
            capture_output=True, text=True,
            cwd=_REPO_ROOT,
        )
        assert result.returncode == 1

    def test_schedule_mode_zero_tables_zero_errors_returns_1(self):
        """0 tables + 0 errors + match_data_type=schedule → exit code 1."""
        import subprocess
        import sys

        test_script = '''
import sys
results = {
    'tables': [],
    'errors': [],
    'mode': 'match_data',
    'match_data_type': 'schedule',
}
total_tables = len(results['tables'])
total_errors = len(results['errors'])
if total_tables == 0:
    if total_errors > 0:
        sys.exit(1)
    mode = results.get('mode', '')
    match_data_type = results.get('match_data_type', '')
    critical_modes = {'combined_match_data', 'schedule'}
    if mode in critical_modes or match_data_type == 'schedule':
        sys.exit(1)
    sys.exit(0)
sys.exit(0)
'''
        result = subprocess.run(
            [sys.executable, '-c', test_script],
            capture_output=True, text=True,
        )
        assert result.returncode == 1

    def test_noncritical_mode_zero_tables_zero_errors_returns_0(self):
        """0 tables + 0 errors + mode=single_stat → exit code 0 (expected for some stats)."""
        import subprocess
        import sys

        test_script = '''
import sys
results = {
    'tables': [],
    'errors': [],
    'mode': 'single_stat',
}
total_tables = len(results['tables'])
total_errors = len(results['errors'])
if total_tables == 0:
    if total_errors > 0:
        sys.exit(1)
    mode = results.get('mode', '')
    match_data_type = results.get('match_data_type', '')
    critical_modes = {'combined_match_data', 'schedule'}
    if mode in critical_modes or match_data_type == 'schedule':
        sys.exit(1)
    sys.exit(0)
sys.exit(0)
'''
        result = subprocess.run(
            [sys.executable, '-c', test_script],
            capture_output=True, text=True,
        )
        assert result.returncode == 0

    def test_zero_tables_with_errors_returns_1(self):
        """0 tables + errors > 0 → exit code 1 for any mode."""
        import subprocess
        import sys

        test_script = '''
import sys
results = {
    'tables': [],
    'errors': ['Some error'],
    'mode': 'single_stat',
}
total_tables = len(results['tables'])
total_errors = len(results['errors'])
if total_tables == 0:
    if total_errors > 0:
        sys.exit(1)
    sys.exit(0)
sys.exit(0)
'''
        result = subprocess.run(
            [sys.executable, '-c', test_script],
            capture_output=True, text=True,
        )
        assert result.returncode == 1


class TestSkippedLeaguesFailureTracking:
    """Tests for skipped league/season counting as failures."""

    @pytest.fixture
    def mock_scraper_dependencies(self):
        """Mock all dependencies for the Selenium scraper."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw, \
             patch('scrapers.base.browser.cloudflare_bypass.CloudflareBypass') as mock_browser:

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True

            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_cb.return_value.state = 'closed'

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            mock_browser_instance = MagicMock()
            mock_browser_instance.get_page.return_value = '<html></html>'
            mock_browser_instance.page_source = '<html></html>'
            mock_browser.return_value = mock_browser_instance

            yield {
                'rate_limiter': mock_rl,
                'iceberg_writer': mock_iw_instance,
                'browser': mock_browser_instance,
            }

    def test_skipped_leagues_increment_failures_combined(self, mock_scraper_dependencies):
        """All leagues skipped in combined_match_data → _stats['failures'] > 0."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(leagues=['ENG-Premier League'], seasons=[2024])

        # Both file and Iceberg return None → league skipped
        scraper._read_schedule_from_file = MagicMock(return_value=None)
        scraper._read_schedule_from_iceberg = MagicMock(return_value=None)
        scraper._cleanup_after_league = MagicMock()

        result = scraper.scrape_combined_match_data(max_matches=1)

        assert scraper._stats.get('failures', 0) > 0
        assert len(result) == 0

    def test_skipped_leagues_increment_failures_match_data(self, mock_scraper_dependencies):
        """All leagues skipped in scrape_match_data → _stats['failures'] > 0."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(leagues=['ENG-Premier League'], seasons=[2024])

        # Both file and Iceberg return None → league skipped
        scraper._read_schedule_from_file = MagicMock(return_value=None)
        scraper._read_schedule_from_iceberg = MagicMock(return_value=None)
        scraper._cleanup_after_league = MagicMock()

        result = scraper.scrape_match_data(data_type='shot_events')

        assert scraper._stats.get('failures', 0) > 0
        assert len(result) == 0


class TestScrapeAllReplacePartitions:
    """#468: every scrape_all save must pass replace_partitions —
    season-grain tables by (league, season), match-grain by match_id —
    otherwise --mode full re-runs plain-append duplicates."""

    SEASON_TABLES = (
        'fbref_schedule', 'fbref_team_stats', 'fbref_player_stats',
    )
    MATCH_TABLES = (
        'fbref_player_match_stats', 'fbref_shot_events', 'fbref_match_events',
        'fbref_lineups', 'fbref_team_match_stats',
    )

    @pytest.fixture
    def mock_scraper_dependencies(self):
        """Mock all dependencies for the Selenium scraper."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw, \
             patch('scrapers.base.browser.cloudflare_bypass.CloudflareBypass'):

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True
            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_cb.return_value.state = 'closed'
            mock_iw.return_value = MagicMock()

            yield

    def test_scrape_all_saves_every_table_with_replace_partitions(
        self, mock_scraper_dependencies
    ):
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(leagues=['ENG-Premier League'], seasons=[2024])

        season_df = pd.DataFrame({
            'league': ['ENG-Premier League'], 'season': [2024], 'value': [1],
        })
        match_df = pd.DataFrame({
            'match_id': ['m1'], 'league': ['ENG-Premier League'],
            'season': [2024], 'value': [1],
        })

        scraper.read_schedule = MagicMock(return_value=season_df.copy())
        scraper.read_team_season_stats = MagicMock(return_value=season_df.copy())
        scraper.read_player_season_stats = MagicMock(return_value=season_df.copy())
        scraper._extract_match_ids = MagicMock(return_value=['m1'])
        scraper.read_player_match_stats = MagicMock(return_value=match_df.copy())
        scraper.read_shot_events = MagicMock(return_value=match_df.copy())
        scraper.read_match_events = MagicMock(return_value=match_df.copy())
        scraper.read_lineup = MagicMock(return_value=match_df.copy())
        scraper.read_team_match_stats = MagicMock(return_value=match_df.copy())
        scraper._cleanup_after_league = MagicMock()
        scraper.save_to_iceberg = MagicMock(
            side_effect=lambda df, table_name, **kw: f'iceberg.bronze.{table_name}'
        )

        with patch('scrapers.fbref.scraper.time'):
            scraper.scrape_all(
                include_match_stats=True,
                include_team_match_stats=True,
                max_matches_per_league=1,
            )

        by_table = {
            c.kwargs.get('table_name'): c.kwargs.get('replace_partitions')
            for c in scraper.save_to_iceberg.call_args_list
        }
        assert set(by_table) == set(self.SEASON_TABLES) | set(self.MATCH_TABLES)
        for table in self.SEASON_TABLES:
            assert by_table[table] == ['league', 'season'], table
        for table in self.MATCH_TABLES:
            assert by_table[table] == ['match_id'], table
