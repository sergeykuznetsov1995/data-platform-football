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
    def mock_scraper(self, scraper_class, tmp_path):
        """Create scraper with mocked browser and hermetic HTTP-meta store."""
        with patch.object(scraper_class, '_get_browser'):
            scraper = scraper_class(
                leagues=['ENG-Premier League'],
                seasons=[2024],
                headless=True,
            )
            scraper._session = MagicMock()
            scraper._http_meta_path = tmp_path / 'matchhistory_http_meta.json'
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
        mock_response.content = csv_content.encode('utf-8')
        mock_response.headers = {}

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

    def test_standardize_columns_strips_bom(self, mock_scraper):
        """BOM-prefixed first header from football-data.co.uk CSV is stripped (#309).

        requests decodes the body as latin-1, so the UTF-8 BOM (EF BB BF) surfaces
        as the literal chars '\\xef\\xbb\\xbf'; the single '\\ufeff' form is also covered.
        """
        df = pd.DataFrame({
            '\xef\xbb\xbfDiv': ['E0'],   # latin-1 mojibake form (requests path)
            'HomeTeam': ['Arsenal'],
        })

        standardized = mock_scraper._standardize_columns(df)

        assert 'Div' in standardized.columns
        assert '\xef\xbb\xbfDiv' not in standardized.columns
        assert 'home_team' in standardized.columns

        # single-char BOM form (e.g. selenium page_source decoded as utf-8)
        df2 = pd.DataFrame({'﻿Div': ['E0']})
        assert 'Div' in mock_scraper._standardize_columns(df2).columns

    def test_read_games_parses_data(self, mock_scraper):
        """Test read_games parsing."""
        csv_content = """Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,HTHG,HTAG,HS,AS
15/01/2024,Arsenal,Chelsea,2,1,H,1,0,15,8
16/01/2024,Liverpool,Man City,1,1,D,0,1,12,14
"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = csv_content.encode('utf-8')
        mock_response.headers = {}

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

    def test_scrape_all_uses_replace_partitions(self, mock_scraper):
        """Regression #308: scrape_all MUST pass replace_partitions=['league',
        'season'] so daily writes replace partitions instead of appending
        (else matchhistory_results accumulates ~3.8x duplicates per season)."""
        # Arrange
        mock_games = pd.DataFrame({
            'match_date': ['15/01/2024'],
            'home_team': ['Arsenal'],
            'away_team': ['Chelsea'],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        # Act
        with patch.object(mock_scraper, 'read_games', return_value=mock_games):
            with patch.object(mock_scraper, 'save_to_iceberg',
                              return_value='iceberg.bronze.test') as mock_save:
                mock_scraper.scrape_all()

        # Assert
        mock_save.assert_called_once()
        assert mock_save.call_args.kwargs['replace_partitions'] == ['league', 'season']

    def test_metadata_added(self, mock_scraper):
        """Test that metadata is added to DataFrames."""
        csv_content = """Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR
15/01/2024,Arsenal,Chelsea,2,1,H
"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = csv_content.encode('utf-8')
        mock_response.headers = {}

        mock_scraper._session.get.return_value = mock_response

        df = mock_scraper.read_games('ENG-Premier League', 2024)

        assert '_source' in df.columns
        assert '_entity_type' in df.columns
        assert '_ingested_at' in df.columns
        assert df.iloc[0]['_source'] == 'matchhistory'

    # ------------------------------------------------------------------
    # Conditional GET (ETag/If-Modified-Since) + encoding
    # ------------------------------------------------------------------

    def test_fetch_304_returns_not_modified_sentinel(self, mock_scraper):
        """304 → NOT_MODIFIED sentinel, distinct from None (= failure)."""
        from scrapers.matchhistory import NOT_MODIFIED

        mock_response = MagicMock()
        mock_response.status_code = 304

        mock_scraper._session.get.return_value = mock_response

        result = mock_scraper._fetch_csv_with_requests('http://test.com/data.csv')

        assert result is NOT_MODIFIED

    def test_read_games_304_skips_selenium_fallback(self, mock_scraper):
        """A 304 is a clean no-op: read_games must propagate NOT_MODIFIED and
        must NOT fall back to Selenium (that path is for real failures)."""
        from scrapers.matchhistory import NOT_MODIFIED

        mock_response = MagicMock()
        mock_response.status_code = 304
        mock_scraper._session.get.return_value = mock_response

        with patch.object(mock_scraper, '_fetch_csv_with_selenium') as mock_selenium:
            result = mock_scraper.read_games('ENG-Premier League', 2024)

        assert result is NOT_MODIFIED
        mock_selenium.assert_not_called()

    def test_fetch_sends_conditional_headers_from_meta(self, mock_scraper):
        """Stored validators are sent as If-None-Match/If-Modified-Since."""
        url = 'https://www.football-data.co.uk/mmz4281/2425/E0.csv'
        mock_scraper._http_meta = {
            url: {'etag': '"abc-123"', 'last_modified': 'Mon, 25 May 2026 19:01:01 GMT'},
        }
        mock_response = MagicMock()
        mock_response.status_code = 304
        mock_scraper._session.get.return_value = mock_response

        mock_scraper._fetch_csv_with_requests(url)

        headers = mock_scraper._session.get.call_args.kwargs['headers']
        assert headers['If-None-Match'] == '"abc-123"'
        assert headers['If-Modified-Since'] == 'Mon, 25 May 2026 19:01:01 GMT'

    def test_force_refresh_skips_conditional_headers(self, scraper_class, tmp_path):
        """force_refresh=True must re-download even with stored validators."""
        url = 'https://www.football-data.co.uk/mmz4281/2425/E0.csv'
        with patch.object(scraper_class, '_get_browser'):
            scraper = scraper_class(
                leagues=['ENG-Premier League'],
                seasons=[2024],
                force_refresh=True,
            )
            scraper._session = MagicMock()
            scraper._http_meta_path = tmp_path / 'matchhistory_http_meta.json'
            scraper._http_meta = {url: {'etag': '"abc-123"'}}

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = b'Date,HomeTeam\n15/01/2024,Arsenal\n'
            mock_response.headers = {}
            scraper._session.get.return_value = mock_response

            scraper._fetch_csv_with_requests(url)

            assert mock_scraper_headers_empty(scraper._session.get.call_args.kwargs)
            scraper.close()

    def test_meta_committed_only_after_explicit_commit(self, mock_scraper):
        """Validators from a 200 land in the pending buffer and reach the
        on-disk store only via commit_http_meta() (i.e. after the Iceberg save
        succeeded) — a failed write must not poison the 304 short-circuit."""
        import json as _json

        url = 'http://test.com/data.csv'
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'Date,HomeTeam\n15/01/2024,Arsenal\n'
        mock_response.headers = {
            'ETag': '"abc-123"',
            'Last-Modified': 'Mon, 25 May 2026 19:01:01 GMT',
        }
        mock_scraper._session.get.return_value = mock_response

        mock_scraper._fetch_csv_with_requests(url)

        assert mock_scraper._pending_http_meta[url]['etag'] == '"abc-123"'
        assert not mock_scraper._http_meta_path.exists()

        mock_scraper.commit_http_meta()

        with open(mock_scraper._http_meta_path) as f:
            stored = _json.load(f)
        assert stored[url]['last_modified'] == 'Mon, 25 May 2026 19:01:01 GMT'
        assert mock_scraper._pending_http_meta == {}

    def test_decode_utf8_sig_strips_bom_and_keeps_accents(self, mock_scraper):
        """Modern files (season >= 2425): UTF-8 with BOM — the BOM must not
        leak into the first header and accented names must survive."""
        csv_bytes = '﻿Div,HomeTeam,Referee\nE0,Arsenal,José María\n'.encode('utf-8')
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = csv_bytes
        mock_response.headers = {}
        mock_scraper._session.get.return_value = mock_response

        df = mock_scraper._fetch_csv_with_requests('http://test.com/data.csv')

        assert 'Div' in df.columns
        assert df.iloc[0]['Referee'] == 'José María'

    def test_decode_latin1_fallback_for_old_files(self, mock_scraper):
        """Pre-2425 files are latin-1 (invalid as UTF-8) — fallback decodes."""
        csv_bytes = 'Date,HomeTeam,Referee\n15/01/2018,Arsenal,André\n'.encode('latin-1')
        assert b'\xe9' in csv_bytes  # invalid as UTF-8 → exercises the fallback

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = csv_bytes
        mock_response.headers = {}
        mock_scraper._session.get.return_value = mock_response

        df = mock_scraper._fetch_csv_with_requests('http://test.com/data.csv')

        assert df.iloc[0]['Referee'] == 'André'

    def test_scrape_all_skips_not_modified_and_commits_meta(self, mock_scraper):
        """scrape_all: NOT_MODIFIED leagues are skipped without failing, and
        HTTP meta is committed after a successful save."""
        from scrapers.matchhistory import NOT_MODIFIED

        mock_games = pd.DataFrame({
            'match_date': ['15/01/2024'],
            'home_team': ['Arsenal'],
            'away_team': ['Chelsea'],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })
        mock_scraper.leagues = ['ENG-Premier League', 'ESP-La Liga']
        mock_scraper._pending_http_meta = {'http://x': {'etag': '"e"'}}

        with patch.object(mock_scraper, 'read_games',
                          side_effect=[NOT_MODIFIED, mock_games]):
            with patch.object(mock_scraper, 'save_to_iceberg',
                              return_value='iceberg.bronze.test') as mock_save:
                results = mock_scraper.scrape_all()

        assert 'match_results' in results
        mock_save.assert_called_once()
        # meta committed after the save
        assert mock_scraper._pending_http_meta == {}
        assert mock_scraper._http_meta_path.exists()


def mock_scraper_headers_empty(kwargs) -> bool:
    """True if the session.get call carried no conditional headers."""
    headers = kwargs.get('headers') or {}
    return 'If-None-Match' not in headers and 'If-Modified-Since' not in headers


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
