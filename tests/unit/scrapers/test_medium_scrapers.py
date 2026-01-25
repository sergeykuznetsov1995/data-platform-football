"""
Tests for medium complexity scrapers (FotMob, SofaScore, SoFIFA, ESPN).
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


class TestFotMobScraper:
    """Tests for FotMobScraper."""

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
    def mock_soccerdata_fotmob(self):
        """Mock soccerdata FotMob reader."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            import soccerdata as sd

            reader = MagicMock()
            reader.read_schedule.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'],
                'season': [2024],
                'home_team': ['Arsenal'],
            })
            reader.read_lineup.return_value = pd.DataFrame({
                'player': ['Saka', 'Rice'],
                'position': ['RW', 'CM'],
            })

            sd.FotMob.return_value = reader
            yield reader

    @pytest.fixture
    def scraper(self, mock_dependencies, mock_soccerdata_fotmob):
        """Create FotMobScraper instance."""
        from scrapers.fotmob_scraper import FotMobScraper

        return FotMobScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024]
        )

    def test_init(self, scraper):
        """Test FotMobScraper initialization."""
        assert scraper.SOURCE_NAME == 'fotmob'

    def test_read_schedule(self, scraper, mock_soccerdata_fotmob):
        """Test reading schedule."""
        df = scraper.read_schedule()
        assert df is not None

    def test_read_lineup(self, scraper, mock_soccerdata_fotmob):
        """Test reading lineups."""
        df = scraper.read_lineup()
        assert df is not None

    def test_scrape_all(self, scraper, mock_soccerdata_fotmob):
        """Test full scrape."""
        result = scraper.scrape_all()
        assert isinstance(result, dict)


class TestSofaScoreScraper:
    """Tests for SofaScoreScraper."""

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
        """Create SofaScoreScraper instance."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            from scrapers.sofascore_scraper import SofaScoreScraper
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2024])

    def test_init(self, scraper):
        """Test SofaScoreScraper initialization."""
        assert scraper.SOURCE_NAME == 'sofascore'

    def test_rate_limit(self, scraper):
        """Test SofaScore rate limit."""
        assert scraper.DEFAULT_RATE_LIMIT == 20


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
        from scrapers.sofifa_scraper import SoFIFAScraper

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
            from scrapers.espn_scraper import ESPNScraper
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


class TestFIFAAttributes:
    """Tests for FIFA attribute definitions."""

    def test_pace_attributes(self):
        """Test pace attributes defined."""
        from scrapers.sofifa_scraper import PACE_ATTRIBUTES

        assert 'acceleration' in PACE_ATTRIBUTES
        assert 'sprint_speed' in PACE_ATTRIBUTES

    def test_shooting_attributes(self):
        """Test shooting attributes defined."""
        from scrapers.sofifa_scraper import SHOOTING_ATTRIBUTES

        assert 'finishing' in SHOOTING_ATTRIBUTES
        assert 'shot_power' in SHOOTING_ATTRIBUTES

    def test_goalkeeper_attributes(self):
        """Test goalkeeper attributes defined."""
        from scrapers.sofifa_scraper import GOALKEEPER_ATTRIBUTES

        assert 'gk_diving' in GOALKEEPER_ATTRIBUTES
        assert 'gk_reflexes' in GOALKEEPER_ATTRIBUTES
