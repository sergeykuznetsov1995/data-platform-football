"""
Tests for SofaScoreScraper.
"""

import pytest
from unittest.mock import MagicMock, patch


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
            from scrapers.sofascore import SofaScoreScraper
            return SofaScoreScraper(leagues=['ENG-Premier League'], seasons=[2024])

    def test_init(self, scraper):
        """Test SofaScoreScraper initialization."""
        assert scraper.SOURCE_NAME == 'sofascore'

    def test_rate_limit(self, scraper):
        """Test SofaScore rate limit."""
        assert scraper.DEFAULT_RATE_LIMIT == 20


class TestShotmapFlatten:
    """Pure-function tests for the shotmap payload flattener (#22)."""

    def _payload(self):
        """Minimal but realistic SofaScore shotmap response."""
        return {
            'shotmap': [
                {
                    'id': 101,
                    'player': {'id': 11111, 'name': 'Player A'},
                    'teamId': 1,
                    'isHome': True,
                    'time': 12,
                    'addedTime': 0,
                    'reversedPeriodCount': 1,
                    'shotType': 'rightFoot',
                    'situation': 'open-play',
                    'bodyPart': 'rightFoot',
                    'incidentType': 'goal',
                    'goalType': 'regular',
                    'playerCoordinates': {'x': 88.5, 'y': 50.0},
                    'goalMouthCoordinates': {'x': 100.0, 'y': 52.3},
                    'xg': 0.72,
                    'xgot': 0.84,
                },
                {
                    'id': 102,
                    'player': {'id': 22222, 'name': 'Player B'},
                    'teamId': 2,
                    'isHome': False,
                    'time': 45,
                    'addedTime': 2,
                    'reversedPeriodCount': 1,
                    'shotType': 'header',
                    'situation': 'corner',
                    'bodyPart': 'head',
                    'incidentType': 'save',
                    'goalType': None,
                    'playerCoordinates': {'x': 92.1, 'y': 48.5},
                    'goalMouthCoordinates': {'x': 100.0, 'y': 51.2},
                    'xg': 0.11,
                    'xgot': None,
                },
                # Missing id → composite fallback
                {
                    'player': {'id': 33333},
                    'teamId': 1,
                    'isHome': True,
                    'time': 78,
                    'shotType': 'leftFoot',
                    'incidentType': 'miss',
                    'xg': 0.04,
                },
            ]
        }

    def test_flatten_happy_path(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        rows = SofaScoreScraper._flatten_shotmap('14023925', self._payload())
        assert len(rows) == 3

        goal = rows[0]
        assert goal['match_id'] == '14023925'
        assert goal['shot_id'] == '101'
        assert goal['player_id'] == '11111'
        assert goal['team_id'] == 1
        assert goal['is_home'] is True
        assert goal['minute'] == 12
        assert goal['period'] == 1
        assert goal['shot_type'] == 'rightFoot'
        assert goal['situation'] == 'open-play'
        assert goal['body_part'] == 'rightFoot'
        assert goal['outcome'] == 'goal'
        assert goal['goal_type'] == 'regular'
        assert goal['x'] == 88.5
        assert goal['y'] == 50.0
        assert goal['xg'] == 0.72
        assert goal['xgot'] == 0.84

    def test_flatten_missing_id_falls_back_to_composite(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        rows = SofaScoreScraper._flatten_shotmap('14023925', self._payload())
        third = rows[2]
        # composite: match-time-player-addedTime
        assert third['shot_id'] == '14023925-78-33333-0'
        assert third['player_id'] == '33333'
        assert third['outcome'] == 'miss'
        # xgot absent → None
        assert third['xgot'] is None

    def test_flatten_handles_garbage(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        # Non-dict payload, missing shotmap key, non-list shotmap.
        assert SofaScoreScraper._flatten_shotmap('1', None) == []
        assert SofaScoreScraper._flatten_shotmap('1', {}) == []
        assert SofaScoreScraper._flatten_shotmap('1', {'shotmap': 'oops'}) == []
        assert SofaScoreScraper._flatten_shotmap('1', {'shotmap': [{}]}) == [
            # Empty dict still yields a row with mostly None values + composite shot_id
            {
                'match_id': '1',
                'shot_id': '1-NA-NA-0',
                'player_id': None,
                'team_id': None,
                'is_home': None,
                'minute': None,
                'added_time': None,
                'period': None,
                'shot_type': None,
                'situation': None,
                'body_part': None,
                'outcome': None,
                'goal_type': None,
                'x': None,
                'y': None,
                'goal_x': None,
                'goal_y': None,
                'xg': None,
                'xgot': None,
            }
        ]


class TestCamelToSnake:
    """Sanity tests for the snake_case normalizer used by #21/#23/#24."""

    def test_basic(self):
        from scrapers.sofascore.scraper import _camel_to_snake
        assert _camel_to_snake('goalsPrevented') == 'goals_prevented'
        assert _camel_to_snake('accuratePass') == 'accurate_pass'
        assert _camel_to_snake('expectedAssists') == 'expected_assists'

    def test_consecutive_capitals(self):
        from scrapers.sofascore.scraper import _camel_to_snake
        # XGOnTarget keeps the leading abbreviation intact, splits at the
        # first lowercase boundary.
        assert _camel_to_snake('XGOnTarget') == 'xg_on_target'

    def test_already_snake(self):
        from scrapers.sofascore.scraper import _camel_to_snake
        assert _camel_to_snake('already_snake') == 'already_snake'


class TestEventPlayerStatsFlatten:
    """Tests for the per-(match, player) Opta stats flattener (#21)."""

    def _payload(self):
        return {
            'player': {'id': 11111, 'name': 'Player A'},
            'team': {'id': 1, 'name': 'Team X'},
            'position': 'F',
            'extra': {
                'isHome': True,
                'captain': True,
                'substitute': False,
            },
            'statistics': {
                'rating': '7.8',
                'goalsPrevented': 0.42,
                'accuratePass': 35,
                'totalPass': 40,
                'expectedAssists': {'value': 0.21, 'previousValue': 0.10},
                # Pure dict without 'value' → None
                'noisyStruct': {'foo': 'bar'},
                'position': 'CF',  # Should be skipped (re-export)
            },
        }

    def test_flatten_happy_path(self):
        from scrapers.sofascore.scraper import SofaScoreScraper

        row = SofaScoreScraper._flatten_event_player_stats(
            '14023925', '11111', self._payload(),
        )
        assert row is not None
        assert row['match_id'] == '14023925'
        assert row['player_id'] == '11111'
        assert row['team_id'] == 1
        assert row['team_name'] == 'Team X'
        assert row['is_home'] is True
        assert row['captain'] is True
        assert row['substitute'] is False
        assert row['position'] == 'F'

        # snake_case auto-flatten
        assert row['rating'] == 7.8
        assert row['goals_prevented'] == 0.42
        assert row['accurate_pass'] == 35
        assert row['total_pass'] == 40
        # struct with `value` → unwrapped
        assert row['expected_assists'] == 0.21
        # struct without `value` → None
        assert row['noisy_struct'] is None
        # The 'position' key inside statistics is the re-export and must
        # not clobber the anchor column.
        assert row['position'] == 'F'

    def test_garbage_payload(self):
        from scrapers.sofascore.scraper import SofaScoreScraper
        assert SofaScoreScraper._flatten_event_player_stats('1', '1', None) is None
        # Empty payload still produces an anchor-only row (no stats).
        row = SofaScoreScraper._flatten_event_player_stats('1', '1', {})
        assert row is not None
        assert row['match_id'] == '1'
        assert row['player_id'] == '1'
