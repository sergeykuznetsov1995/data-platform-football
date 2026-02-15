"""
Tests for FBref merge functions.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


class TestFindJoinColumn:
    """Tests for _find_join_column method."""

    @pytest.fixture
    def scraper(self):
        """Create FBrefScraper instance with mocked dependencies."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter'):

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True
            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_cb.return_value.state = 'closed'

            from scrapers.fbref.scraper import FBrefScraper
            return FBrefScraper(
                leagues=['ENG-Premier League'],
                seasons=[2024],
                headless=True
            )

    def test_exact_match(self, scraper):
        """Test finding column by exact match."""
        df = pd.DataFrame({'Player': ['A'], 'Squad': ['B']})

        result = scraper._find_join_column(df, ['Player', 'player'])
        assert result == 'Player'

    def test_suffix_match(self, scraper):
        """Test finding column by suffix match."""
        df = pd.DataFrame({'Standard_Player': ['A'], 'Standard_Squad': ['B']})

        result = scraper._find_join_column(df, ['Player'])
        assert result == 'Standard_Player'

    def test_no_match(self, scraper):
        """Test when no match is found."""
        df = pd.DataFrame({'Name': ['A'], 'Team': ['B']})

        result = scraper._find_join_column(df, ['Player', 'player_id'])
        assert result is None

    def test_priority_exact_over_suffix(self, scraper):
        """Test that exact match has priority over suffix match."""
        df = pd.DataFrame({
            'Player': ['A'],
            'Standard_Player': ['B'],
        })

        result = scraper._find_join_column(df, ['Player'])
        # Exact match should be returned
        assert result == 'Player'


class TestMergePlayerStats:
    """Tests for _merge_player_stats method."""

    @pytest.fixture
    def scraper(self):
        """Create FBrefScraper instance with mocked dependencies."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter'):

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True
            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_cb.return_value.state = 'closed'

            from scrapers.fbref.scraper import FBrefScraper
            return FBrefScraper(
                leagues=['ENG-Premier League'],
                seasons=[2024],
                headless=True
            )

    def test_merge_with_normalized_columns(self, scraper):
        """Test merging DataFrames with normalized column names."""
        base_df = pd.DataFrame({
            'Player': ['Saka', 'Salah', 'Haaland'],
            'Squad': ['Arsenal', 'Liverpool', 'Man City'],
            'Goals': [10, 15, 25],
            'Assists': [5, 8, 3],
        })

        shooting_df = pd.DataFrame({
            'Player': ['Saka', 'Salah', 'Haaland'],
            'Squad': ['Arsenal', 'Liverpool', 'Man City'],
            'Shots': [50, 60, 100],
            'SoT': [25, 30, 50],
        })

        data = {
            'stats': base_df,
            'shooting': shooting_df,
        }

        result = scraper._merge_player_stats(data, 'ENG-Premier League', 2024)

        assert result is not None
        assert len(result) == 3
        assert 'Shots' in result.columns
        assert 'SoT' in result.columns
        assert result['Shots'].tolist() == [50, 60, 100]

    def test_merge_with_suffix_columns(self, scraper):
        """Test merging DataFrames where one has suffix columns."""
        base_df = pd.DataFrame({
            'Player': ['Saka', 'Salah'],
            'Squad': ['Arsenal', 'Liverpool'],
            'Goals': [10, 15],
        })

        # Simulate un-normalized columns
        shooting_df = pd.DataFrame({
            'Standard_Player': ['Saka', 'Salah'],
            'Standard_Squad': ['Arsenal', 'Liverpool'],
            'Shots': [50, 60],
        })

        data = {
            'stats': base_df,
            'shooting': shooting_df,
        }

        result = scraper._merge_player_stats(data, 'ENG-Premier League', 2024)

        assert result is not None
        assert len(result) == 2
        assert 'Shots' in result.columns

    def test_merge_no_base_stats(self, scraper):
        """Test merge returns None when base stats is missing."""
        data = {
            'shooting': pd.DataFrame({'Player': ['Saka'], 'Shots': [50]}),
        }

        result = scraper._merge_player_stats(data, 'ENG-Premier League', 2024)

        assert result is None

    def test_merge_empty_base_stats(self, scraper):
        """Test merge returns None when base stats is empty."""
        data = {
            'stats': pd.DataFrame(),
            'shooting': pd.DataFrame({'Player': ['Saka'], 'Shots': [50]}),
        }

        result = scraper._merge_player_stats(data, 'ENG-Premier League', 2024)

        assert result is None

    def test_merge_adds_metadata(self, scraper):
        """Test that merge adds league and season metadata."""
        base_df = pd.DataFrame({
            'Player': ['Saka'],
            'Squad': ['Arsenal'],
            'Goals': [10],
        })

        data = {'stats': base_df}

        result = scraper._merge_player_stats(data, 'ENG-Premier League', 2024)

        assert result is not None
        assert 'league' in result.columns
        assert 'season' in result.columns
        assert result['league'].iloc[0] == 'ENG-Premier League'
        assert result['season'].iloc[0] == 2024

    def test_merge_multiple_stat_types(self, scraper):
        """Test merging multiple stat types."""
        base_df = pd.DataFrame({
            'Player': ['Saka', 'Salah'],
            'Squad': ['Arsenal', 'Liverpool'],
            'Goals': [10, 15],
        })

        shooting_df = pd.DataFrame({
            'Player': ['Saka', 'Salah'],
            'Squad': ['Arsenal', 'Liverpool'],
            'Shots': [50, 60],
        })

        passing_df = pd.DataFrame({
            'Player': ['Saka', 'Salah'],
            'Squad': ['Arsenal', 'Liverpool'],
            'PassCmp': [200, 250],
        })

        defense_df = pd.DataFrame({
            'Player': ['Saka', 'Salah'],
            'Squad': ['Arsenal', 'Liverpool'],
            'Tackles': [20, 15],
        })

        data = {
            'stats': base_df,
            'shooting': shooting_df,
            'passing': passing_df,
            'defense': defense_df,
        }

        result = scraper._merge_player_stats(data, 'ENG-Premier League', 2024)

        assert result is not None
        assert 'Shots' in result.columns
        assert 'PassCmp' in result.columns
        assert 'Tackles' in result.columns

    def test_merge_skips_empty_dataframes(self, scraper):
        """Test that merge skips empty DataFrames."""
        base_df = pd.DataFrame({
            'Player': ['Saka'],
            'Squad': ['Arsenal'],
            'Goals': [10],
        })

        data = {
            'stats': base_df,
            'shooting': pd.DataFrame(),  # Empty
            'passing': None,  # None
        }

        result = scraper._merge_player_stats(data, 'ENG-Premier League', 2024)

        assert result is not None
        assert len(result) == 1

    def test_merge_handles_duplicate_columns(self, scraper):
        """Test that merge handles duplicate columns with suffix."""
        base_df = pd.DataFrame({
            'Player': ['Saka'],
            'Squad': ['Arsenal'],
            'Goals': [10],
            'xG': [8.5],  # Also exists in shooting
        })

        shooting_df = pd.DataFrame({
            'Player': ['Saka'],
            'Squad': ['Arsenal'],
            'Shots': [50],
            'xG': [8.5],  # Duplicate
        })

        data = {
            'stats': base_df,
            'shooting': shooting_df,
        }

        result = scraper._merge_player_stats(data, 'ENG-Premier League', 2024)

        assert result is not None
        # xG should not be added again since it's already in base
        assert 'Shots' in result.columns

    def test_merge_left_join_preserves_all_base_rows(self, scraper):
        """Test that left join preserves all rows from base."""
        base_df = pd.DataFrame({
            'Player': ['Saka', 'Salah', 'NewPlayer'],
            'Squad': ['Arsenal', 'Liverpool', 'Chelsea'],
            'Goals': [10, 15, 0],
        })

        shooting_df = pd.DataFrame({
            'Player': ['Saka', 'Salah'],  # Missing NewPlayer
            'Squad': ['Arsenal', 'Liverpool'],
            'Shots': [50, 60],
        })

        data = {
            'stats': base_df,
            'shooting': shooting_df,
        }

        result = scraper._merge_player_stats(data, 'ENG-Premier League', 2024)

        assert result is not None
        assert len(result) == 3  # All base rows preserved
        # NewPlayer should have NaN for Shots
        new_player_row = result[result['Player'] == 'NewPlayer']
        assert pd.isna(new_player_row['Shots'].iloc[0])


class TestMergeKeeperStats:
    """Tests for _merge_keeper_stats method."""

    @pytest.fixture
    def scraper(self):
        """Create FBrefScraper instance with mocked dependencies."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter'):

            mock_rl.return_value = MagicMock()
            mock_rl.return_value.acquire.return_value = True
            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_cb.return_value.state = 'closed'

            from scrapers.fbref.scraper import FBrefScraper
            return FBrefScraper(
                leagues=['ENG-Premier League'],
                seasons=[2024],
                headless=True
            )

    def test_merge_keeper_basic_and_advanced(self, scraper):
        """Test merging basic and advanced keeper stats."""
        keeper_df = pd.DataFrame({
            'Player': ['Raya', 'Alisson'],
            'Squad': ['Arsenal', 'Liverpool'],
            'Saves': [100, 80],
        })

        keeper_adv_df = pd.DataFrame({
            'Player': ['Raya', 'Alisson'],
            'Squad': ['Arsenal', 'Liverpool'],
            'PSxG': [35.5, 30.0],
        })

        data = {
            'keeper': keeper_df,
            'keeper_adv': keeper_adv_df,
        }

        result = scraper._merge_keeper_stats(data, 'ENG-Premier League', 2024)

        assert result is not None
        assert 'Saves' in result.columns
        assert 'PSxG' in result.columns

    def test_merge_keeper_no_base(self, scraper):
        """Test merge returns None when keeper base is missing."""
        data = {
            'keeper_adv': pd.DataFrame({'Player': ['Raya'], 'PSxG': [35.5]}),
        }

        result = scraper._merge_keeper_stats(data, 'ENG-Premier League', 2024)

        assert result is None
