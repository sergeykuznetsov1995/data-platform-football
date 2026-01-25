"""
Unit tests for FotMobSeleniumScraper.

Tests scraper logic with mocked HTTP responses.
"""

import json
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest


class TestFotMobSeleniumScraperUnit:
    """Unit tests for FotMobSeleniumScraper."""

    @pytest.fixture
    def scraper_class(self):
        """Get scraper class without instantiating."""
        from scrapers.fotmob_selenium_scraper import FotMobSeleniumScraper
        return FotMobSeleniumScraper

    @pytest.fixture
    def mock_scraper(self, scraper_class):
        """Create scraper with mocked browser."""
        with patch.object(scraper_class, '_get_browser'):
            with patch.object(scraper_class, '_obtain_cookies'):
                scraper = scraper_class(
                    leagues=['ENG-Premier League'],
                    seasons=[2024],
                    headless=True,
                )
                scraper._cookies_obtained = True
                scraper._session = MagicMock()
                yield scraper
                scraper.close()

    def test_init(self, scraper_class):
        """Test scraper initialization."""
        with patch.object(scraper_class, '_get_browser'):
            with patch.object(scraper_class, '_obtain_cookies'):
                scraper = scraper_class(
                    leagues=['ENG-Premier League'],
                    seasons=[2024],
                )
                assert scraper.SOURCE_NAME == 'fotmob'
                assert scraper.leagues == ['ENG-Premier League']
                assert scraper.seasons == [2024]
                scraper.close()

    def test_format_season(self, mock_scraper):
        """Test season formatting."""
        assert mock_scraper._format_season(2024) == '2024/2025'
        assert mock_scraper._format_season(2023) == '2023/2024'
        assert mock_scraper._format_season(2020) == '2020/2021'

    def test_league_ids(self, scraper_class):
        """Test league IDs configuration."""
        assert 'ENG-Premier League' in scraper_class.LEAGUE_IDS
        assert scraper_class.LEAGUE_IDS['ENG-Premier League'] == '47'
        assert scraper_class.LEAGUE_IDS['ESP-La Liga'] == '87'
        assert scraper_class.LEAGUE_IDS['GER-Bundesliga'] == '54'

    def test_fetch_api_json_success(self, mock_scraper):
        """Test successful API fetch."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': 'test'}

        mock_scraper._session.get.return_value = mock_response

        result = mock_scraper._fetch_api_json('test-endpoint', {'param': 'value'})

        assert result == {'data': 'test'}
        mock_scraper._session.get.assert_called()

    def test_fetch_api_json_403_refresh_cookies(self, mock_scraper):
        """Test cookie refresh on 403."""
        mock_response_403 = MagicMock()
        mock_response_403.status_code = 403

        mock_response_ok = MagicMock()
        mock_response_ok.status_code = 200
        mock_response_ok.json.return_value = {'success': True}

        # First call returns 403, second call succeeds
        mock_scraper._session.get.side_effect = [mock_response_403, mock_response_ok]

        with patch.object(mock_scraper, '_obtain_cookies') as mock_obtain:
            result = mock_scraper._fetch_api_json('test-endpoint')

            # Cookies should be refreshed
            mock_obtain.assert_called_once()
            assert result == {'success': True}

    def test_read_schedule_parses_data(self, mock_scraper):
        """Test schedule parsing from API response."""
        mock_data = {
            'fixtures': {
                'allMatches': [
                    {
                        'id': '123',
                        'status': {'utcTime': '2024-01-15T15:00:00Z', 'finished': True, 'scoreStr': '2 - 1'},
                        'home': {'name': 'Arsenal', 'id': '9825'},
                        'away': {'name': 'Chelsea', 'id': '8455'},
                        'round': '21',
                        'roundName': 21,
                    },
                    {
                        'id': '124',
                        'status': {'utcTime': '2024-01-16T15:00:00Z', 'finished': False},
                        'home': {'name': 'Liverpool', 'id': '8650'},
                        'away': {'name': 'Man City', 'id': '8456'},
                        'round': '21',
                        'roundName': 21,
                    },
                ]
            }
        }

        with patch.object(mock_scraper, '_get_league_data', return_value=mock_data):
            df = mock_scraper.read_schedule('ENG-Premier League', 2024)

            assert df is not None
            assert len(df) == 2
            assert 'match_id' in df.columns
            assert 'home_team' in df.columns
            assert 'away_team' in df.columns
            assert df.iloc[0]['home_team'] == 'Arsenal'
            assert df.iloc[0]['home_score'] == 2
            assert df.iloc[0]['is_finished'] == True

    def test_read_schedule_no_league(self, mock_scraper):
        """Test schedule with unknown league."""
        mock_scraper.leagues = []
        mock_scraper.seasons = []

        df = mock_scraper.read_schedule(None, None)

        assert df is None

    def test_read_team_stats_parses_data(self, mock_scraper):
        """Test team stats parsing from API response."""
        mock_data = {
            'table': [
                {
                    'data': {
                        'table': {
                            'all': [
                                {
                                    'id': '9825',
                                    'name': 'Arsenal',
                                    'idx': 1,
                                    'played': 20,
                                    'wins': 15,
                                    'draws': 3,
                                    'losses': 2,
                                    'scoresStr': '45-12',
                                    'goalConDiff': 33,
                                    'pts': 48,
                                },
                                {
                                    'id': '8650',
                                    'name': 'Liverpool',
                                    'idx': 2,
                                    'played': 20,
                                    'wins': 14,
                                    'draws': 4,
                                    'losses': 2,
                                    'scoresStr': '42-15',
                                    'goalConDiff': 27,
                                    'pts': 46,
                                },
                            ]
                        }
                    }
                }
            ]
        }

        with patch.object(mock_scraper, '_get_league_data', return_value=mock_data):
            df = mock_scraper.read_team_season_stats('ENG-Premier League', 2024)

            assert df is not None
            assert len(df) == 2
            assert 'team_name' in df.columns
            assert 'points' in df.columns
            assert df.iloc[0]['team_name'] == 'Arsenal'
            assert df.iloc[0]['points'] == 48

    def test_read_player_stats_parses_data(self, mock_scraper):
        """Test player stats parsing from API response."""
        mock_data = {
            'stats': {
                'players': [
                    {
                        'header': 'Top scorer',
                        'topThree': [
                            {
                                'id': 1001,
                                'name': 'Erling Haaland',
                                'teamId': 8456,
                                'teamName': 'Manchester City',
                                'value': 20,
                                'rank': 1,
                                'ccode': 'NOR',
                                'stat': {'name': 'goals', 'value': 20},
                            },
                            {
                                'id': 1002,
                                'name': 'Mohamed Salah',
                                'teamId': 8650,
                                'teamName': 'Liverpool',
                                'value': 15,
                                'rank': 2,
                                'ccode': 'EGY',
                                'stat': {'name': 'goals', 'value': 15},
                            },
                        ]
                    }
                ]
            }
        }

        with patch.object(mock_scraper, '_get_league_data', return_value=mock_data):
            df = mock_scraper.read_player_season_stats('goals', 'ENG-Premier League', 2024)

            assert df is not None
            assert len(df) == 2
            assert 'player_name' in df.columns
            assert 'stat_value' in df.columns
            assert df.iloc[0]['player_name'] == 'Erling Haaland'
            assert df.iloc[0]['stat_value'] == 20

    def test_scrape_all_combines_data(self, mock_scraper):
        """Test scrape_all combines all data types."""
        mock_schedule = pd.DataFrame({
            'match_id': ['123'],
            'home_team': ['Arsenal'],
            'away_team': ['Chelsea'],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        mock_team_stats = pd.DataFrame({
            'team_name': ['Arsenal'],
            'points': [48],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        mock_player_stats = pd.DataFrame({
            'player_name': ['Haaland'],
            'stat_value': [20],
            'league': ['ENG-Premier League'],
            'season': [2024],
        })

        with patch.object(mock_scraper, 'read_schedule', return_value=mock_schedule):
            with patch.object(mock_scraper, 'read_team_season_stats', return_value=mock_team_stats):
                with patch.object(mock_scraper, 'read_player_season_stats', return_value=mock_player_stats):
                    with patch.object(mock_scraper, 'save_to_iceberg', return_value='iceberg.bronze.test'):
                        results = mock_scraper.scrape_all()

                        assert 'schedule' in results
                        assert 'team_stats' in results
                        assert 'player_stats' in results

    def test_metadata_added(self, mock_scraper):
        """Test that metadata is added to DataFrames."""
        mock_data = {
            'fixtures': {
                'allMatches': [
                    {
                        'id': '123',
                        'status': {'utcTime': '2024-01-15T15:00:00Z', 'finished': True, 'scoreStr': '1 - 0'},
                        'home': {'name': 'Arsenal', 'id': '9825'},
                        'away': {'name': 'Chelsea', 'id': '8455'},
                        'round': '1',
                    },
                ]
            }
        }

        with patch.object(mock_scraper, '_get_league_data', return_value=mock_data):
            df = mock_scraper.read_schedule('ENG-Premier League', 2024)

            assert '_source' in df.columns
            assert '_entity_type' in df.columns
            assert '_ingested_at' in df.columns
            assert df.iloc[0]['_source'] == 'fotmob'

    def test_empty_response_handling(self, mock_scraper):
        """Test handling of empty API response."""
        with patch.object(mock_scraper, '_get_league_data', return_value=None):
            df = mock_scraper.read_schedule('ENG-Premier League', 2024)
            assert df is None

    def test_empty_matches_handling(self, mock_scraper):
        """Test handling of empty matches in response."""
        mock_data = {'fixtures': {'allMatches': []}}

        with patch.object(mock_scraper, '_get_league_data', return_value=mock_data):
            df = mock_scraper.read_schedule('ENG-Premier League', 2024)
            assert df is None


class TestFotMobLeagueMapping:
    """Test league ID mappings."""

    def test_major_leagues_present(self):
        """Test that all major leagues are mapped."""
        from scrapers.fotmob_selenium_scraper import FotMobSeleniumScraper

        major_leagues = [
            'ENG-Premier League',
            'ESP-La Liga',
            'GER-Bundesliga',
            'ITA-Serie A',
            'FRA-Ligue 1',
        ]

        for league in major_leagues:
            assert league in FotMobSeleniumScraper.LEAGUE_IDS
            assert FotMobSeleniumScraper.LEAGUE_IDS[league] is not None
