"""
Tests for SoFIFAScraper.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


class TestSoFIFAScraper:
    """Tests for SoFIFAScraper."""

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
    def mock_soccerdata_sofifa(self):
        """Mock soccerdata SoFIFA reader."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            import soccerdata as sd

            reader = MagicMock()
            reader.read_players.return_value = pd.DataFrame({
                'player': ['Haaland', 'Mbappe'],
                'overall': [91, 91],
                'potential': [94, 95],
                'pace': [89, 97],
            })

            sd.SoFIFA.return_value = reader
            yield reader

    @pytest.fixture
    def scraper(self, mock_dependencies, mock_soccerdata_sofifa):
        """Create SoFIFAScraper instance."""
        from scrapers.sofifa import SoFIFAScraper

        return SoFIFAScraper(versions=['24'])

    def test_init(self, scraper):
        """Test SoFIFAScraper initialization."""
        assert scraper.SOURCE_NAME == 'sofifa'
        assert '24' in scraper.versions

    def test_process_player_data(self, scraper):
        """Test player data processing."""
        df = pd.DataFrame({
            'overall': [91],
            'potential': [94],
        })

        result = scraper._process_player_data(df)

        assert 'potential_diff' in result.columns
        assert result['potential_diff'].iloc[0] == 3

    def test_scrape_all(self, scraper, mock_soccerdata_sofifa):
        """Test full scrape."""
        result = scraper.scrape_all()
        assert isinstance(result, dict)


class TestFIFAAttributes:
    """Tests for FIFA attribute definitions."""

    def test_pace_attributes(self):
        """Test pace attributes defined."""
        from scrapers.sofifa.scraper import PACE_ATTRIBUTES

        assert 'acceleration' in PACE_ATTRIBUTES
        assert 'sprint_speed' in PACE_ATTRIBUTES

    def test_shooting_attributes(self):
        """Test shooting attributes defined."""
        from scrapers.sofifa.scraper import SHOOTING_ATTRIBUTES

        assert 'finishing' in SHOOTING_ATTRIBUTES
        assert 'shot_power' in SHOOTING_ATTRIBUTES

    def test_goalkeeper_attributes(self):
        """Test goalkeeper attributes defined."""
        from scrapers.sofifa.scraper import GOALKEEPER_ATTRIBUTES

        assert 'gk_diving' in GOALKEEPER_ATTRIBUTES
        assert 'gk_reflexes' in GOALKEEPER_ATTRIBUTES
