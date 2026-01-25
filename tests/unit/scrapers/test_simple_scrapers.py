"""
Tests for simple scrapers (ClubElo, MatchHistory).
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
        from scrapers.clubelo_scraper import ClubEloScraper

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


class TestMatchHistoryScraper:
    """Tests for MatchHistoryScraper."""

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
    def mock_soccerdata_matchhistory(self):
        """Mock soccerdata MatchHistory reader."""
        with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
            import soccerdata as sd

            reader = MagicMock()
            reader.read_games.return_value = pd.DataFrame({
                'league': ['ENG-Premier League'] * 3,
                'season': [2024, 2024, 2024],
                'Date': ['2024-08-17', '2024-08-18', '2024-08-19'],
                'HomeTeam': ['Arsenal', 'Liverpool', 'Man City'],
                'AwayTeam': ['Wolves', 'Ipswich', 'Chelsea'],
                'FTHG': [2, 2, 4],
                'FTAG': [0, 0, 1],
                'B365H': [1.5, 1.6, 1.4],
                'B365D': [4.0, 3.8, 5.0],
                'B365A': [6.0, 5.5, 8.0],
            })

            sd.MatchHistory.return_value = reader
            yield reader

    @pytest.fixture
    def scraper(self, mock_dependencies, mock_soccerdata_matchhistory):
        """Create MatchHistoryScraper instance."""
        from scrapers.matchhistory_scraper import MatchHistoryScraper

        return MatchHistoryScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024]
        )

    def test_init(self, scraper):
        """Test MatchHistoryScraper initialization."""
        assert scraper.SOURCE_NAME == 'matchhistory'
        assert 'ENG-Premier League' in scraper.leagues

    def test_read_games(self, scraper, mock_soccerdata_matchhistory):
        """Test reading games."""
        df = scraper.read_games()

        assert df is not None
        assert '_source' in df.columns

    def test_standardize_columns(self, scraper):
        """Test column standardization."""
        df = pd.DataFrame({
            'Date': ['2024-08-17'],
            'HomeTeam': ['Arsenal'],
            'AwayTeam': ['Wolves'],
            'FTHG': [2],
            'FTAG': [0],
        })

        result = scraper._standardize_columns(df)

        assert 'match_date' in result.columns
        assert 'home_team' in result.columns
        assert 'home_goals' in result.columns

    def test_calculate_odds_stats(self, scraper):
        """Test odds statistics calculation."""
        df = pd.DataFrame({
            'odds_home_b365': [1.5],
            'odds_home_bw': [1.55],
            'odds_draw_b365': [4.0],
            'odds_draw_bw': [3.8],
            'odds_away_b365': [6.0],
            'odds_away_bw': [5.5],
        })

        result = scraper.calculate_odds_stats(df)

        assert 'odds_home_avg' in result.columns
        assert 'prob_home_implied' in result.columns

    def test_scrape_all(self, scraper, mock_soccerdata_matchhistory):
        """Test full scrape."""
        result = scraper.scrape_all()

        assert isinstance(result, dict)


class TestTopEnglishClubs:
    """Tests for predefined club lists."""

    def test_top_english_clubs(self):
        """Test top English clubs are defined."""
        from scrapers.clubelo_scraper import TOP_ENGLISH_CLUBS

        assert 'Manchester City' in TOP_ENGLISH_CLUBS
        assert 'Arsenal' in TOP_ENGLISH_CLUBS
        assert 'Liverpool' in TOP_ENGLISH_CLUBS
        assert len(TOP_ENGLISH_CLUBS) == 20
