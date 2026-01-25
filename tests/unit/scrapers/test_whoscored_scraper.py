"""
Tests for WhoScoredScraper.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


class TestWhoScoredScraper:
    """Tests for WhoScoredScraper."""

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

            mock_iw_instance = MagicMock()
            mock_iw_instance.write_dataframe.return_value = 'iceberg.bronze.test'
            mock_iw.return_value = mock_iw_instance

            yield

    @pytest.fixture
    def scraper(self, mock_dependencies):
        """Create WhoScoredScraper instance."""
        from scrapers.whoscored_scraper import WhoScoredScraper

        return WhoScoredScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True
        )

    def test_init(self, scraper):
        """Test WhoScoredScraper initialization."""
        assert scraper.leagues == ['ENG-Premier League']
        assert scraper.seasons == [2024]
        assert scraper.headless is True

    def test_source_name(self, scraper):
        """Test source name is set correctly."""
        assert scraper.SOURCE_NAME == 'whoscored'

    def test_default_rate_limit(self, scraper):
        """Test conservative rate limit for WhoScored."""
        assert scraper.DEFAULT_RATE_LIMIT == 10

    def test_convert_coordinates(self, scraper):
        """Test coordinate conversion to SPADL format."""
        # WhoScored uses 0-100, SPADL uses 105x68 meters
        spadl_x, spadl_y = scraper._convert_coordinates(50, 50)

        assert spadl_x == 52.5  # 105 / 2
        assert spadl_y == 34.0  # 68 / 2

    def test_convert_coordinates_corners(self, scraper):
        """Test coordinate conversion at corners."""
        # Top-left
        x, y = scraper._convert_coordinates(0, 0)
        assert x == 0.0
        assert y == 0.0

        # Bottom-right
        x, y = scraper._convert_coordinates(100, 100)
        assert x == 105.0
        assert y == 68.0

    def test_event_type_mapping(self, scraper):
        """Test event type mapping to SPADL."""
        assert scraper.EVENT_TYPE_MAPPING['Pass'] == 'pass'
        assert scraper.EVENT_TYPE_MAPPING['Shot'] == 'shot'
        assert scraper.EVENT_TYPE_MAPPING['Tackle'] == 'tackle'
        assert scraper.EVENT_TYPE_MAPPING['Cross'] == 'cross'

    def test_event_to_spadl_pass(self, scraper):
        """Test converting pass event to SPADL."""
        event = {
            'id': 12345,
            'type': {'displayName': 'Pass'},
            'outcomeType': {'displayName': 'Successful'},
            'x': 50,
            'y': 50,
            'endX': 70,
            'endY': 40,
            'minute': 15,
            'second': 30,
            'period': {'value': 1},
            'teamId': 1,
            'playerId': 101,
            'playerName': 'Test Player',
            'qualifiers': [],
        }

        match_info = {
            'league': 'ENG-Premier League',
            'season': 2024,
            'match_id': 999,
            'home_team': 'Arsenal',
            'away_team': 'Chelsea',
            'home_team_id': 1,
            'away_team_id': 2,
        }

        spadl = scraper._event_to_spadl(event, match_info)

        assert spadl['action_type'] == 'pass'
        assert spadl['result'] == 'success'
        assert spadl['start_x'] == 52.5
        assert spadl['bodypart'] == 'foot'

    def test_event_to_spadl_header(self, scraper):
        """Test converting header event to SPADL."""
        event = {
            'id': 12346,
            'type': {'displayName': 'Shot'},
            'outcomeType': {'displayName': 'Successful'},
            'x': 90,
            'y': 50,
            'minute': 45,
            'second': 0,
            'period': {'value': 1},
            'teamId': 1,
            'playerId': 101,
            'playerName': 'Test Player',
            'qualifiers': [{'type': {'displayName': 'Head'}}],
            'isGoal': True,
        }

        match_info = {
            'league': 'ENG-Premier League',
            'season': 2024,
            'match_id': 999,
        }

        spadl = scraper._event_to_spadl(event, match_info)

        assert spadl['action_type'] == 'shot'
        assert spadl['bodypart'] == 'head'
        assert spadl['is_goal'] is True

    def test_event_to_spadl_penalty(self, scraper):
        """Test converting penalty event to SPADL."""
        event = {
            'id': 12347,
            'type': {'displayName': 'Shot'},
            'outcomeType': {'displayName': 'Successful'},
            'x': 90,
            'y': 50,
            'minute': 78,
            'second': 0,
            'period': {'value': 2},
            'teamId': 1,
            'playerId': 101,
            'qualifiers': [{'type': {'displayName': 'Penalty'}}],
            'isGoal': True,
        }

        match_info = {'league': 'Test', 'season': 2024, 'match_id': 1}

        spadl = scraper._event_to_spadl(event, match_info)

        assert spadl['action_type'] == 'shot_penalty'

    def test_league_slugs(self, scraper):
        """Test league URL slugs are defined."""
        assert 'ENG-Premier League' in scraper.LEAGUE_SLUGS
        assert scraper.LEAGUE_SLUGS['ENG-Premier League'] == 'England-Premier-League'


class TestSPADLDefinitions:
    """Tests for SPADL action definitions."""

    def test_spadl_actions_defined(self):
        """Test SPADL actions are defined."""
        from scrapers.whoscored_scraper import SPADL_ACTIONS

        assert 'pass' in SPADL_ACTIONS
        assert 'shot' in SPADL_ACTIONS
        assert 'tackle' in SPADL_ACTIONS
        assert 'dribble' in SPADL_ACTIONS
