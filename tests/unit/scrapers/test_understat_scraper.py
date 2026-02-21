"""
Tests for UnderstatScraper.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


class TestUnderstatScraper:
    """Tests for UnderstatScraper."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.fixture
    def mock_soccerdata_understat(self):
        """Mock soccerdata Understat reader."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            import soccerdata as sd

            reader = MagicMock()
            reader.read_schedule.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'home_team': ['Arsenal'],
                'home_xg': [2.5],
            })
            reader.read_shot_events.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'player': ['Haaland'],
                'xg': [0.75],
                'result': ['Goal'],
            })
            reader.read_player_season_stats.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'player': ['Haaland'],
                'xg': [15.5],
            })
            reader.read_team_season_stats.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'team': ['Man City'],
                'xg': [75.5],
            })

            sd.Understat.return_value = reader
            yield reader

    @pytest.fixture
    def scraper(self, mock_dependencies, mock_soccerdata_understat):
        """Create UnderstatScraper instance."""
        from scrapers.understat import UnderstatScraper

        return UnderstatScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024]
        )

    def test_init(self, scraper):
        """Test UnderstatScraper initialization."""
        assert 'ENG-Premier League' in scraper.leagues
        assert 2024 in scraper.seasons

    def test_source_name(self, scraper):
        """Test source name is set correctly."""
        assert scraper.SOURCE_NAME == 'understat'

    def test_supported_leagues_filter(self, mock_dependencies, mock_soccerdata_understat):
        """Test that unsupported leagues are filtered out."""
        from scrapers.understat import UnderstatScraper

        scraper = UnderstatScraper(
            leagues=['ENG-Premier League', 'USA-MLS'],  # MLS not supported
            seasons=[2024]
        )

        assert 'ENG-Premier League' in scraper.leagues
        assert 'USA-MLS' not in scraper.leagues

    def test_read_schedule(self, scraper, mock_soccerdata_understat):
        """Test reading schedule with xG."""
        df = scraper.read_schedule()

        assert df is not None
        assert 'home_xg' in df.columns

    def test_read_shots(self, scraper, mock_soccerdata_understat):
        """Test reading shot events."""
        df = scraper.read_shot_events()

        assert df is not None
        assert 'xg' in df.columns
        assert 'result' in df.columns

    def test_read_player_stats(self, scraper, mock_soccerdata_understat):
        """Test reading player stats."""
        df = scraper.read_player_season_stats()

        assert df is not None
        assert 'xg' in df.columns

    def test_scrape_shots(self, scraper, mock_soccerdata_understat):
        """Test scraping shots."""
        result = scraper.scrape_shots()

        assert 'shots' in result

    def test_scrape_all(self, scraper, mock_soccerdata_understat):
        """Test full scrape."""
        result = scraper.scrape_all()

        assert isinstance(result, dict)


class TestUnderstatSupportedLeagues:
    """Tests for Understat supported leagues."""

    def test_supported_leagues_list(self):
        """Test supported leagues are defined."""
        from scrapers.understat import UnderstatScraper

        assert 'ENG-Premier League' in UnderstatScraper.SUPPORTED_LEAGUES
        assert 'ESP-La Liga' in UnderstatScraper.SUPPORTED_LEAGUES
        assert 'GER-Bundesliga' in UnderstatScraper.SUPPORTED_LEAGUES
        assert 'ITA-Serie A' in UnderstatScraper.SUPPORTED_LEAGUES
        assert 'FRA-Ligue 1' in UnderstatScraper.SUPPORTED_LEAGUES
