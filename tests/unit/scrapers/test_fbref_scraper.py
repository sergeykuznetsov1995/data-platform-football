"""
Tests for FBrefScraper.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


class TestFBrefScraper:
    """Tests for FBrefScraper."""

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
        from scrapers.fbref_scraper import FBrefScraper

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
        from scrapers.fbref_scraper import FBrefScraper

        scraper = FBrefScraper(leagues=['ENG-Premier League'], seasons=[2024])

        assert 'standard' in scraper.stat_types
        assert 'shooting' in scraper.stat_types
        assert 'passing' in scraper.stat_types


class TestFBrefScraperStatTypes:
    """Tests for FBref stat types."""

    def test_all_stat_types(self):
        """Test all stat types are defined."""
        from scrapers.fbref_scraper import FBrefScraper

        expected_types = [
            'standard', 'shooting', 'passing', 'passing_types',
            'gca', 'defense', 'possession', 'playing_time',
            'misc', 'keeper', 'keeper_adv',
        ]

        for stat_type in expected_types:
            assert stat_type in FBrefScraper.STAT_TYPES
