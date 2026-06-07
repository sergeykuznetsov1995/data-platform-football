"""
Tests for ESPNScraper.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


class TestESPNScraper:
    """Tests for ESPNScraper."""

    @pytest.fixture
    def mock_dependencies(self):
        """Mock all scraper dependencies."""
        with patch('scrapers.base.base_scraper.get_rate_limiter') as mock_rl, \
             patch('scrapers.base.base_scraper.get_retry_policy') as mock_rp, \
             patch('scrapers.base.base_scraper.get_circuit_breaker') as mock_cb, \
             patch('scrapers.base.base_scraper.IcebergWriter') as mock_iw:

            mock_rl.return_value = MagicMock()
            mock_rp.return_value = MagicMock()
            mock_cb.return_value = MagicMock()
            mock_iw.return_value = MagicMock()

            yield

    @pytest.fixture
    def scraper(self, mock_dependencies):
        """Create ESPNScraper instance."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            from scrapers.espn import ESPNScraper
            return ESPNScraper(leagues=['ENG-Premier League'], seasons=[2024])

    def test_init(self, scraper):
        """Test ESPNScraper initialization."""
        assert scraper.SOURCE_NAME == 'espn'

    def test_league_ids(self, scraper):
        """Test ESPN league IDs are defined."""
        assert scraper.LEAGUE_IDS['ENG-Premier League'] == 'eng.1'
        assert scraper.LEAGUE_IDS['ESP-La Liga'] == 'esp.1'

    def test_standardize_schedule(self, scraper):
        """Test schedule standardization."""
        df = pd.DataFrame({
            'date': ['2024-08-17'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
            'home_score': [2],
            'away_score': [0],
        })

        result = scraper._standardize_schedule(df)

        assert 'match_date' in result.columns
        assert 'home_goals' in result.columns

    def test_scrape_schedule_uses_replace_partitions(self, scraper):
        """Regression #347: scrape_schedule MUST pass replace_partitions=['league',
        'season'] so daily writes replace each partition instead of appending
        (else espn_schedule accumulates ~31x duplicates in the active season)."""
        # Arrange
        mock_schedule = pd.DataFrame({
            'date': ['2024-08-17'],
            'home_team': ['Arsenal'],
            'away_team': ['Wolves'],
            'home_score': [2],
            'away_score': [0],
            'league': ['ENG-Premier League'],
            'season': [2425],
        })

        # Act
        with patch.object(scraper, 'read_schedule', return_value=mock_schedule):
            with patch.object(scraper, 'save_to_iceberg',
                              return_value='iceberg.bronze.test') as mock_save:
                scraper.scrape_schedule()

        # Assert
        mock_save.assert_called_once()
        assert mock_save.call_args.kwargs['replace_partitions'] == ['league', 'season']
