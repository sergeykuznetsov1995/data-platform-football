"""
Tests for ClubEloScraper.
"""

import pytest
import pandas as pd
from datetime import date
from unittest.mock import MagicMock, patch


class TestClubEloScraper:
    """Tests for ClubEloScraper."""

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
    def mock_soccerdata_clubelo(self):
        """Mock soccerdata ClubElo reader."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            import soccerdata as sd

            reader = MagicMock()
            reader.read_by_date.return_value = pd.DataFrame({
                'club': ['Manchester City', 'Arsenal', 'Liverpool'],
                'country': ['ENG', 'ENG', 'ENG'],
                'elo': [2050, 1980, 1950],
                'rank': [1, 2, 3],
            })
            reader.read_team_history.return_value = pd.DataFrame({
                'from': [pd.Timestamp('2024-01-01'), pd.Timestamp('2024-06-01')],
                'to': [pd.Timestamp('2024-05-31'), pd.Timestamp('2024-12-31')],
                'elo': [2000, 2050],
            })

            sd.ClubElo.return_value = reader
            yield reader

    @pytest.fixture
    def scraper(self, mock_dependencies, mock_soccerdata_clubelo):
        """Create ClubEloScraper instance."""
        from scrapers.clubelo import ClubEloScraper

        return ClubEloScraper(leagues=['ENG-Premier League'])

    def test_init(self, scraper):
        """Test ClubEloScraper initialization."""
        assert scraper.SOURCE_NAME == 'clubelo'
        assert 'ENG-Premier League' in scraper.leagues

    def test_rate_limit(self, scraper):
        """Test ClubElo has permissive rate limit."""
        assert scraper.DEFAULT_RATE_LIMIT == 60

    def test_league_mapping(self, scraper):
        """Test league code mapping."""
        assert scraper.LEAGUE_MAPPING['ENG-Premier League'] == 'ENG'
        assert scraper.LEAGUE_MAPPING['ESP-La Liga'] == 'ESP'

    def test_read_by_date(self, scraper, mock_soccerdata_clubelo):
        """Test reading ratings by date."""
        df = scraper.read_by_date(date(2024, 1, 15))

        assert df is not None
        assert 'club' in df.columns
        assert 'elo' in df.columns

    def test_read_by_date_default(self, scraper, mock_soccerdata_clubelo):
        """Test reading ratings for today."""
        df = scraper.read_by_date()

        assert df is not None

    def test_read_team_history(self, scraper, mock_soccerdata_clubelo):
        """Test reading team ELO history."""
        df = scraper.read_team_history('Manchester City')

        assert df is not None
        assert 'team' in df.columns

    def test_scrape_current_ratings(self, scraper, mock_soccerdata_clubelo):
        """Test scraping current ratings."""
        result = scraper.scrape_current_ratings()

        assert 'current_ratings' in result

    def test_scrape_all(self, scraper, mock_soccerdata_clubelo):
        """Test full scrape."""
        result = scraper.scrape_all()

        assert isinstance(result, dict)


class TestTopEnglishClubs:
    """Tests for predefined club lists."""

    def test_top_english_clubs(self):
        """Test top English clubs are defined."""
        from scrapers.clubelo.scraper import TOP_ENGLISH_CLUBS

        assert 'Manchester City' in TOP_ENGLISH_CLUBS
        assert 'Arsenal' in TOP_ENGLISH_CLUBS
        assert 'Liverpool' in TOP_ENGLISH_CLUBS
        assert len(TOP_ENGLISH_CLUBS) == 20
