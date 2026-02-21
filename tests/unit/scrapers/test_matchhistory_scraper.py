"""
Unit tests for MatchHistoryScraper.

Tests scraper logic with mocked HTTP responses.
"""

from io import StringIO
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest


class TestMatchHistoryScraperUnit:
    """Unit tests for MatchHistoryScraper."""

    @pytest.fixture
    def scraper_class(self):
        """Get scraper class without instantiating."""
        from scrapers.matchhistory import MatchHistoryScraper
        return MatchHistoryScraper

    @pytest.fixture
    def mock_scraper(self, scraper_class):
        """Create scraper with mocked browser."""
        with patch.object(scraper_class, '_get_browser'):
            scraper = scraper_class(
                leagues=['ENG-Premier League'],
                seasons=[2024],
                headless=True,
            )
            scraper._session = MagicMock()
            yield scraper
            scraper.close()

    def test_init(self, scraper_class):
        """Test scraper initialization."""
        with patch.object(scraper_class, '_get_browser'):
            scraper = scraper_class(
                leagues=['ENG-Premier League'],
                seasons=[2024],
            )
            assert scraper.SOURCE_NAME == 'matchhistory'
            assert scraper.leagues == ['ENG-Premier League']
            assert scraper.seasons == [2024]
            scraper.close()

    def test_format_season(self, mock_scraper):
        """Test season formatting."""
        assert mock_scraper._format_season(2024) == '2425'
        assert mock_scraper._format_season(2023) == '2324'
        assert mock_scraper._format_season(2020) == '2021'
        assert mock_scraper._format_season(2019) == '1920'

    def test_league_codes(self, scraper_class):
        """Test league code configuration."""
        assert 'ENG-Premier League' in scraper_class.LEAGUE_CODES
        assert scraper_class.LEAGUE_CODES['ENG-Premier League'] == 'E0'
        assert scraper_class.LEAGUE_CODES['ESP-La Liga'] == 'SP1'
        assert scraper_class.LEAGUE_CODES['GER-Bundesliga'] == 'D1'
        assert scraper_class.LEAGUE_CODES['ITA-Serie A'] == 'I1'
        assert scraper_class.LEAGUE_CODES['FRA-Ligue 1'] == 'F1'

    def test_get_csv_url(self, mock_scraper):
        """Test CSV URL building."""
        url = mock_scraper._get_csv_url('ENG-Premier League', 2024)
        assert url == 'https://www.football-data.co.uk/mmz4281/2425/E0.csv'

        url = mock_scraper._get_csv_url('ESP-La Liga', 2023)
        assert url == 'https://www.football-data.co.uk/mmz4281/2324/SP1.csv'

    def test_get_csv_url_unknown_league(self, mock_scraper):
        """Test CSV URL with unknown league."""
        url = mock_scraper._get_csv_url('UNKNOWN-League', 2024)
        assert url is None

    def test_fetch_csv_with_requests_success(self, mock_scraper):
        """Test successful CSV fetch with requests."""
        csv_content = """Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR
15/01/2024,Arsenal,Chelsea,2,1,H
16/01/2024,Liverpool,Man City,1,1,D
"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = csv_content

        mock_scraper._session.get.return_value = mock_response

        df = mock_scraper._fetch_csv_with_requests('http://test.com/data.csv')

        assert df is not None
        assert len(df) == 2
        assert 'HomeTeam' in df.columns
        assert df.iloc[0]['HomeTeam'] == 'Arsenal'

    def test_fetch_csv_with_requests_503(self, mock_scraper):
        """Test 503 error handling."""
        mock_response = MagicMock()
        mock_response.status_code = 503

        mock_scraper._session.get.return_value = mock_response

        df = mock_scraper._fetch_csv_with_requests('http://test.com/data.csv')

        assert df is None

    def test_fetch_csv_with_requests_403(self, mock_scraper):
        """Test 403 error handling."""
        mock_response = MagicMock()
        mock_response.status_code = 403

        mock_scraper._session.get.return_value = mock_response

        df = mock_scraper._fetch_csv_with_requests('http://test.com/data.csv')

        assert df is None

    def test_standardize_columns(self, mock_scraper):
        """Test column standardization."""
        df = pd.DataFrame({
            'Date': ['15/01/2024'],
            'HomeTeam': ['Arsenal'],
            'AwayTeam': ['Chelsea'],
            'FTHG': [2],
            'FTAG': [1],
            'FTR': ['H'],
            'B365H': [1.5],
            'B365D': [3.5],
            'B365A': [5.0],
        })

        standardized = mock_scraper._standardize_columns(df)

        assert 'match_date' in standardized.columns
        assert 'home_team' in standardized.columns
        assert 'away_team' in standardized.columns
        assert 'home_goals' in standardized.columns
        assert 'away_goals' in standardized.columns
        assert 'result' in standardized.columns
        assert 'odds_home_b365' in standardized.columns

    def test_read_games_parses_data(self, mock_scraper):
        """Test read_games parsing."""
        csv_content = """Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,HTHG,HTAG,HS,AS
15/01/2024,Arsenal,Chelsea,2,1,H,1,0,15,8
16/01/2024,Liverpool,Man City,1,1,D,0,1,12,14
"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = csv_content

        mock_scraper._session.get.return_value = mock_response

        df = mock_scraper.read_games('ENG-Premier League', 2024)

        assert df is not None
        assert len(df) == 2
        assert 'home_team' in df.columns
        assert 'away_team' in df.columns
        assert 'home_goals' in df.columns
        assert 'league' in df.columns
        assert df.iloc[0]['home_team'] == 'Arsenal'
        assert df.iloc[0]['home_goals'] == 2

    def test_read_games_no_league(self, mock_scraper):
        """Test read_games with no league."""
        mock_scraper.leagues = []
        mock_scraper.seasons = []

        df = mock_scraper.read_games(None, None)

        assert df is None

    def test_calculate_odds_stats(self, mock_scraper):
        """Test odds statistics calculation."""
        df = pd.DataFrame({
            'odds_home_b365': [1.5],
            'odds_draw_b365': [3.5],
            'odds_away_b365': [5.0],
            'odds_home_bw': [1.6],
            'odds_draw_bw': [3.4],
            'odds_away_bw': [4.8],
        })

        result = mock_scraper.calculate_odds_stats(df)

        assert 'odds_home_avg' in result.columns
        assert 'odds_draw_avg' in result.columns
        assert 'odds_away_avg' in result.columns
        assert 'prob_home_implied' in result.columns
        assert 'prob_draw_implied' in result.columns
        assert 'prob_away_implied' in result.columns
        assert 'overround' in result.columns

        # Check calculations
        assert result.iloc[0]['odds_home_avg'] == pytest.approx(1.55, rel=0.01)
        assert result.iloc[0]['prob_home_implied'] == pytest.approx(1/1.55, rel=0.01)

    def test_scrape_all_combines_data(self, mock_scraper):
        """Test scrape_all combines all data."""
        mock_games = pd.DataFrame({
            'match_date': ['15/01/2024'],
            'home_team': ['Arsenal'],
            'away_team': ['Chelsea'],
            'home_goals': [2],
            'away_goals': [1],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        with patch.object(mock_scraper, 'read_games', return_value=mock_games):
            with patch.object(mock_scraper, 'save_to_iceberg', return_value='iceberg.bronze.test'):
                results = mock_scraper.scrape_all()

                assert 'match_results' in results

    def test_metadata_added(self, mock_scraper):
        """Test that metadata is added to DataFrames."""
        csv_content = """Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR
15/01/2024,Arsenal,Chelsea,2,1,H
"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = csv_content

        mock_scraper._session.get.return_value = mock_response

        df = mock_scraper.read_games('ENG-Premier League', 2024)

        assert '_source' in df.columns
        assert '_entity_type' in df.columns
        assert '_ingested_at' in df.columns
        assert df.iloc[0]['_source'] == 'matchhistory'


class TestMatchHistoryLeagueMapping:
    """Test league code mappings."""

    def test_major_leagues_present(self):
        """Test that all major leagues are mapped."""
        from scrapers.matchhistory import MatchHistoryScraper

        major_leagues = [
            'ENG-Premier League',
            'ESP-La Liga',
            'GER-Bundesliga',
            'ITA-Serie A',
            'FRA-Ligue 1',
        ]

        for league in major_leagues:
            assert league in MatchHistoryScraper.LEAGUE_CODES
            assert MatchHistoryScraper.LEAGUE_CODES[league] is not None

    def test_secondary_leagues_present(self):
        """Test that secondary leagues are mapped."""
        from scrapers.matchhistory import MatchHistoryScraper

        secondary_leagues = [
            'ENG-Championship',
            'ESP-Segunda',
            'GER-2. Bundesliga',
            'ITA-Serie B',
            'FRA-Ligue 2',
        ]

        for league in secondary_leagues:
            assert league in MatchHistoryScraper.LEAGUE_CODES


class TestMatchHistoryColumnMapping:
    """Test column name mappings."""

    def test_basic_columns_mapped(self):
        """Test that basic columns are mapped."""
        from scrapers.matchhistory import MatchHistoryScraper

        expected_mappings = {
            'Date': 'match_date',
            'HomeTeam': 'home_team',
            'AwayTeam': 'away_team',
            'FTHG': 'home_goals',
            'FTAG': 'away_goals',
        }

        for original, expected in expected_mappings.items():
            assert MatchHistoryScraper.COLUMN_MAPPING[original] == expected

    def test_odds_columns_mapped(self):
        """Test that betting odds columns are mapped."""
        from scrapers.matchhistory import MatchHistoryScraper

        odds_mappings = {
            'B365H': 'odds_home_b365',
            'B365D': 'odds_draw_b365',
            'B365A': 'odds_away_b365',
        }

        for original, expected in odds_mappings.items():
            assert MatchHistoryScraper.COLUMN_MAPPING[original] == expected
