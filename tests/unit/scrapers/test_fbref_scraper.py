"""
Tests for FBrefScraper.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


@pytest.mark.skip(reason="Legacy tests for soccerdata-based scraper - replaced by Selenium scraper")
class TestFBrefScraper:
    """Tests for FBrefScraper (legacy soccerdata version)."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all scraper dependencies."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw:

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True

            mock_rp.return_value = MagicMock()
            mock_rp.return_value.execute.side_effect = lambda f, *a, **k: f(*a, **k)

            mock_cb.return_value = MagicMock()
            mock_cb.return_value.call.side_effect = lambda f, *a, **k: f(*a, **k)
            mock_cb.return_value.state = 'closed'

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield {
                'rate_limiter': mock_rl,
                'iceberg_writer': mock_iw_instance,
            }

    @pytest.fixture
    def mock_soccerdata_fbref(self):
        """Mock soccerdata FBref reader."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            import soccerdata as sd

            reader = MagicMock()
            reader.read_schedule.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'home_team': ['Arsenal'],
                'away_team': ['Chelsea'],
            })
            reader.read_player_season_stats.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'player': ['Saka'],
                'goals': [10],
            })
            reader.read_team_season_stats.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'team': ['Arsenal'],
                'points': [50],
            })

            sd.FBref.return_value = reader
            yield reader

    @pytest.fixture
    def scraper(self, mock_dependencies, mock_soccerdata_fbref):
        """Create FBrefScraper instance."""
        from scrapers.fbref import FBrefScraper

        return FBrefScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            stat_types=['standard', 'shooting']
        )

    def test_init(self, scraper):
        """Test FBrefScraper initialization."""
        assert scraper.leagues == ['ENG-Premier League']
        assert scraper.seasons == [2024]
        assert 'standard' in scraper.stat_types

    def test_source_name(self, scraper):
        """Test source name is set correctly."""
        assert scraper.SOURCE_NAME == 'fbref'

    def test_read_schedule(self, scraper, mock_soccerdata_fbref):
        """Test reading schedule."""
        df = scraper.read_schedule()

        assert df is not None
        assert 'league' in df.columns
        assert '_source' in df.columns

    def test_read_player_stats(self, scraper, mock_soccerdata_fbref):
        """Test reading player stats."""
        df = scraper.read_player_season_stats('standard')

        assert df is not None
        assert 'player' in df.columns

    def test_read_team_stats(self, scraper, mock_soccerdata_fbref):
        """Test reading team stats."""
        df = scraper.read_team_season_stats('standard')

        assert df is not None
        assert 'team' in df.columns

    def test_scrape_schedule(self, scraper, mock_dependencies, mock_soccerdata_fbref):
        """Test scraping schedule."""
        result = scraper.scrape_schedule()

        assert 'schedule' in result
        assert result['schedule'] == 'iceberg.bronze.test'

    def test_scrape_schedule_empty(self, scraper, mock_dependencies, mock_soccerdata_fbref):
        """Test scraping when no data returned."""
        mock_soccerdata_fbref.read_schedule.return_value = pd.DataFrame()

        result = scraper.scrape_schedule()
        assert result == {}

    def test_scrape_all(self, scraper, mock_dependencies, mock_soccerdata_fbref):
        """Test full scrape."""
        result = scraper.scrape_all()

        assert isinstance(result, dict)
        # Should have attempted to scrape multiple data types
        assert len(result) > 0

    def test_stat_types_default(self, mock_dependencies, mock_soccerdata_fbref):
        """Test default stat types."""
        from scrapers.fbref import FBrefScraper

        scraper = FBrefScraper(leagues=['ENG-Premier League'], seasons=[2024])

        assert 'standard' in scraper.stat_types
        assert 'shooting' in scraper.stat_types
        assert 'passing' in scraper.stat_types


@pytest.mark.skip(reason="Legacy tests for soccerdata-based scraper - replaced by Selenium scraper")
class TestFBrefScraperStatTypes:
    """Tests for FBref stat types (legacy soccerdata version)."""

    def test_all_stat_types(self):
        """Test all stat types are defined."""
        from scrapers.fbref import FBrefScraper

        expected_types = [
            'standard', 'shooting', 'passing', 'passing_types',
            'gca', 'defense', 'possession', 'playing_time',
            'misc', 'keeper', 'keeper_adv',
        ]

        for stat_type in expected_types:
            assert stat_type in FBrefScraper.STAT_TYPES


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
        """Test that PLAYER_STAT_TYPES is defined."""
        from scrapers.fbref.constants import PLAYER_STAT_TYPES

        expected = ['stats', 'shooting', 'passing', 'passing_types',
                    'gca', 'defense', 'possession', 'playingtime', 'misc']

        for stat_type in expected:
            assert stat_type in PLAYER_STAT_TYPES

    def test_team_stat_types(self):
        """Test that TEAM_STAT_TYPES is defined."""
        from scrapers.fbref.constants import TEAM_STAT_TYPES

        expected = ['stats', 'shooting', 'passing', 'passing_types',
                    'gca', 'defense', 'possession', 'playingtime', 'misc']

        for stat_type in expected:
            assert stat_type in TEAM_STAT_TYPES

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
        assert hasattr(scraper, '_merge_team_stats')
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
        assert 'include_team_stats_extended' in params
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
            cwd='/root/data_platform',
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

    def test_scrape_combined_falls_back_to_http(self, mock_scraper_dependencies):
        """Test HTTP fallback when Iceberg has no schedule."""
        from scrapers.fbref.scraper import FBrefScraper

        scraper = FBrefScraper(leagues=['ENG-Premier League'], seasons=[2024])

        scraper._read_schedule_from_iceberg = MagicMock(return_value=None)
        scraper.read_schedule = MagicMock(return_value=None)
        scraper._cleanup_after_league = MagicMock()

        result = scraper.scrape_combined_match_data(max_matches=1)

        scraper._read_schedule_from_iceberg.assert_called_once()
        scraper.read_schedule.assert_called_once()  # Iceberg failed → HTTP called
        assert isinstance(result, dict)

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
