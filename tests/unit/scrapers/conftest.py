"""
Pytest fixtures for scraper tests.
"""

import os

# sys.path setup (project root + dags folder) is centralised in the root conftest.py.

import pandas as pd
import pytest
from datetime import date, datetime
from unittest.mock import MagicMock, patch


@pytest.fixture(autouse=True)
def set_trino_password(monkeypatch):
    """Set TRINO_PASSWORD for all tests that create TrinoTableManager."""
    monkeypatch.setenv('TRINO_PASSWORD', 'test_password')


@pytest.fixture
def sample_schedule_df():
    """Sample schedule DataFrame."""
    return pd.DataFrame({
        'league': ['ENG-Premier League'] * 3,
        'season': [2024, 2024, 2024],
        'match_date': [date(2024, 8, 17), date(2024, 8, 18), date(2024, 8, 19)],
        'home_team': ['Arsenal', 'Liverpool', 'Manchester City'],
        'away_team': ['Wolves', 'Ipswich', 'Chelsea'],
        'home_goals': [2, 2, 4],
        'away_goals': [0, 0, 1],
    })


@pytest.fixture
def sample_player_stats_df():
    """Sample player stats DataFrame."""
    return pd.DataFrame({
        'league': ['ENG-Premier League'] * 3,
        'season': [2024, 2024, 2024],
        'player': ['Erling Haaland', 'Mohamed Salah', 'Cole Palmer'],
        'team': ['Manchester City', 'Liverpool', 'Chelsea'],
        'position': ['FW', 'FW', 'MF'],
        'matches_played': [20, 20, 19],
        'goals': [15, 12, 14],
        'assists': [3, 8, 6],
        'xg': [13.5, 10.2, 11.8],
        'xa': [2.1, 7.5, 5.2],
    })


@pytest.fixture
def sample_shots_df():
    """Sample shots DataFrame."""
    return pd.DataFrame({
        'league': ['ENG-Premier League'] * 5,
        'season': [2024] * 5,
        'match_id': ['12345'] * 5,
        'player': ['Haaland', 'Haaland', 'De Bruyne', 'Salah', 'Salah'],
        'team': ['Man City', 'Man City', 'Man City', 'Liverpool', 'Liverpool'],
        'minute': [15, 45, 67, 23, 89],
        'x': [0.9, 0.85, 0.75, 0.88, 0.92],
        'y': [0.5, 0.45, 0.55, 0.48, 0.52],
        'xg': [0.76, 0.45, 0.12, 0.55, 0.82],
        'result': ['Goal', 'Saved', 'Blocked', 'Goal', 'Missed'],
    })


@pytest.fixture
def sample_spadl_events_df():
    """Sample SPADL events DataFrame."""
    return pd.DataFrame({
        'league': ['ENG-Premier League'] * 5,
        'season': [2024] * 5,
        'game_id': [12345] * 5,
        'period_id': [1, 1, 1, 2, 2],
        'time_seconds': [120, 350, 2400, 2800, 5200],
        'team_id': [1, 1, 2, 1, 2],
        'player_id': [101, 102, 201, 101, 202],
        'start_x': [35.0, 65.0, 20.0, 85.0, 45.0],
        'start_y': [34.0, 50.0, 40.0, 30.0, 60.0],
        'end_x': [65.0, 95.0, 50.0, 95.0, 75.0],
        'end_y': [50.0, 34.0, 55.0, 34.0, 40.0],
        'action_type': ['pass', 'shot', 'pass', 'shot', 'cross'],
        'result': ['success', 'success', 'success', 'fail', 'fail'],
        'bodypart': ['foot', 'foot', 'foot', 'head', 'foot'],
    })


@pytest.fixture
def mock_soccerdata():
    """Mock soccerdata library."""
    with patch.dict('sys.modules', {'soccerdata': MagicMock()}):
        yield


@pytest.fixture
def mock_iceberg_writer():
    """Mock IcebergWriter."""
    with patch('scrapers.base.iceberg_writer.IcebergWriter') as mock:
        instance = MagicMock()
        instance.write_dataframe.return_value = 'iceberg.bronze.test_table'
        mock.return_value = instance
        yield instance


@pytest.fixture
def mock_rate_limiter():
    """Mock RateLimiter that always allows requests."""
    with patch('scrapers.utils.rate_limiter.RateLimiter') as mock:
        instance = MagicMock()
        instance.acquire.return_value = True
        instance.try_acquire.return_value = True
        instance.available_tokens = 10.0
        mock.return_value = instance
        yield instance


@pytest.fixture
def mock_circuit_breaker():
    """Mock CircuitBreaker that always allows calls."""
    with patch('scrapers.utils.circuit_breaker.CircuitBreaker') as mock:
        instance = MagicMock()
        instance.call.side_effect = lambda func, *args, **kwargs: func(*args, **kwargs)
        instance.state = 'closed'
        mock.return_value = instance
        yield instance
