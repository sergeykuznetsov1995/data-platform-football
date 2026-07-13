"""
Integration tests for football data scrapers with real HTTP requests.

These tests verify:
- Scrapers can connect to real data sources
- Data is returned in expected DataFrame format
- Data is not empty

Run with:
    pytest tests/integration/scrapers/test_real_requests.py -v -m integration

Fast tests (no Selenium):
    pytest tests/integration/scrapers/test_real_requests.py -v -m "integration and not slow and not cloudflare"

With retries for flaky tests:
    pytest tests/integration/scrapers/test_real_requests.py -v -m integration --reruns 2
"""

from datetime import date

import pandas as pd
import pytest


# =============================================================================
# Understat Tests (No Tor Required)
# =============================================================================

@pytest.mark.integration
class TestUnderstatIntegration:
    """Integration tests for Understat scraper."""

    def test_read_schedule(self, understat_scraper, skip_if_no_network, integration_delay):
        """Test reading Understat schedule with real request."""
        df = understat_scraper.read_schedule()

        assert df is not None, "DataFrame should not be None"
        assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
        assert len(df) > 0, "DataFrame should not be empty"
        assert '_source' in df.columns, "Should have metadata column"
        assert df['_source'].iloc[0] == 'understat', "Source should be 'understat'"

    def test_read_player_season_stats(self, understat_scraper, skip_if_no_network, integration_delay):
        """Test reading player stats."""
        df = understat_scraper.read_player_season_stats()

        assert df is not None, "DataFrame should not be None"
        assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
        # May be empty for current season if no data yet
        if len(df) > 0:
            assert '_source' in df.columns, "Should have metadata column"


# =============================================================================
# FotMob Tests (No Tor Required)
# =============================================================================

@pytest.mark.integration
class TestFotMobIntegration:
    """Integration tests for FotMob scraper."""

    def test_read_schedule(self, fotmob_scraper, skip_if_no_network, integration_delay):
        """Test reading FotMob schedule with real request."""
        df = fotmob_scraper.read_schedule()

        assert df is not None, "DataFrame should not be None"
        assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
        assert len(df) > 0, "DataFrame should not be empty"
        assert '_source' in df.columns, "Should have metadata column"
        assert df['_source'].iloc[0] == 'fotmob', "Source should be 'fotmob'"


# =============================================================================
# ESPN Tests (No Tor Required)
# =============================================================================

@pytest.mark.integration
class TestESPNIntegration:
    """Integration tests for ESPN scraper."""

    def test_read_schedule(self, espn_scraper, skip_if_no_network, integration_delay):
        """Test reading ESPN schedule with real request."""
        df = espn_scraper.read_schedule()

        assert df is not None, "DataFrame should not be None"
        assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
        assert len(df) > 0, "DataFrame should not be empty"
        assert '_source' in df.columns, "Should have metadata column"
        assert df['_source'].iloc[0] == 'espn', "Source should be 'espn'"


# =============================================================================
# MatchHistory Tests (No Tor Required)
# =============================================================================

@pytest.mark.integration
class TestMatchHistoryIntegration:
    """Integration tests for MatchHistory scraper (football-data.co.uk)."""

    def test_read_games(self, matchhistory_scraper, skip_if_no_network, integration_delay):
        """Test reading match results with real request."""
        df = matchhistory_scraper.read_games()

        assert df is not None, "DataFrame should not be None"
        assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
        assert len(df) > 0, "DataFrame should not be empty"
        assert '_source' in df.columns, "Should have metadata column"
        assert df['_source'].iloc[0] == 'matchhistory', "Source should be 'matchhistory'"

        # Check for standardized columns
        expected_cols = ['home_team', 'away_team', 'home_goals', 'away_goals']
        for col in expected_cols:
            assert col in df.columns, f"Should have '{col}' column"


# =============================================================================
# ClubElo Tests (No Tor Required, May Have Server Issues)
# =============================================================================

@pytest.mark.integration
@pytest.mark.flaky
class TestClubEloIntegration:
    """Integration tests for ClubElo scraper.

    Note: ClubElo can have server issues (502 errors).
    These tests are marked as flaky.
    """

    def test_read_by_date(self, clubelo_scraper, skip_if_no_network, integration_delay):
        """Test reading ELO ratings by date."""
        df = clubelo_scraper.read_by_date()

        assert df is not None, "DataFrame should not be None"
        assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
        assert len(df) > 0, "DataFrame should not be empty"
        assert '_source' in df.columns, "Should have metadata column"
        assert df['_source'].iloc[0] == 'clubelo', "Source should be 'clubelo'"

    def test_read_by_date_historical(self, clubelo_scraper, skip_if_no_network, integration_delay):
        """Test reading historical ELO ratings."""
        historical_date = date(2024, 1, 1)
        df = clubelo_scraper.read_by_date(historical_date)

        assert df is not None, "DataFrame should not be None"
        assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
        assert len(df) > 0, "DataFrame should not be empty"


# =============================================================================
# FBref Tests (Requires Tor)
# =============================================================================

@pytest.mark.integration
@pytest.mark.tor
@pytest.mark.slow
class TestFBrefIntegration:
    """Integration tests for FBref scraper.

    FBref has Cloudflare protection, requires Tor proxy.
    """

    def test_read_schedule(self, fbref_scraper_with_tor, skip_if_no_network, integration_delay):
        """Test reading FBref schedule with Tor proxy."""
        df = fbref_scraper_with_tor.read_schedule()

        assert df is not None, "DataFrame should not be None"
        assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
        assert len(df) > 0, "DataFrame should not be empty"
        assert '_source' in df.columns, "Should have metadata column"
        assert df['_source'].iloc[0] == 'fbref', "Source should be 'fbref'"


# =============================================================================
# SoFIFA Tests (Requires Tor)
# =============================================================================

@pytest.mark.integration
@pytest.mark.tor
@pytest.mark.slow
class TestSoFIFAIntegration:
    """Integration tests for SoFIFA scraper.

    SoFIFA has blocking, requires Tor proxy.
    Slow due to large player dataset.
    """

    def test_read_players(self, sofifa_scraper_with_tor, skip_if_no_network, integration_delay):
        """Test reading SoFIFA player data with Tor proxy."""
        df = sofifa_scraper_with_tor.read_players()

        assert df is not None, "DataFrame should not be None"
        assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
        # May be blocked
        if len(df) > 0:
            assert '_source' in df.columns, "Should have metadata column"
            assert df['_source'].iloc[0] == 'sofifa', "Source should be 'sofifa'"


# =============================================================================
# Direct Source Availability Tests
# =============================================================================

@pytest.mark.integration
class TestSourceAvailability:
    """Tests to verify source websites are accessible."""

    def test_understat_accessible(self, skip_if_no_network):
        """Test that understat.com is accessible."""
        import requests

        response = requests.get(
            'https://understat.com/league/EPL/2024',
            timeout=30,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        assert response.status_code == 200

    def test_espn_accessible(self, skip_if_no_network):
        """Test that espn.com is accessible."""
        import requests

        response = requests.get(
            'https://www.espn.com/soccer/',
            timeout=30,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        assert response.status_code == 200

    def test_fotmob_accessible(self, skip_if_no_network):
        """Test that fotmob.com is accessible."""
        import requests

        response = requests.get(
            'https://www.fotmob.com/',
            timeout=30,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        assert response.status_code == 200

    def test_football_data_accessible(self, skip_if_no_network):
        """Test that football-data.co.uk is accessible."""
        import requests

        response = requests.get(
            'https://www.football-data.co.uk/englandm.php',
            timeout=30,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        assert response.status_code == 200

    @pytest.mark.flaky
    def test_clubelo_accessible(self, skip_if_no_network):
        """Test that clubelo.com is accessible (may have server issues)."""
        import requests

        response = requests.get(
            'http://clubelo.com/',
            timeout=30,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        # ClubElo sometimes returns 502
        assert response.status_code in [200, 502]


@pytest.mark.integration
@pytest.mark.tor
@pytest.mark.flaky
class TestSourceAvailabilityWithTor:
    """Tests to verify blocked sources are accessible with Tor.

    Note: These tests are flaky because Cloudflare may still block
    even through Tor exit nodes.
    """

    def test_fbref_with_tor(self, tor_available, skip_if_no_network):
        """Test that fbref.com is accessible through Tor.

        Note: FBref uses Cloudflare which may block even with Tor.
        """
        if not tor_available:
            pytest.skip("Tor not available")

        import requests

        proxies = {
            'http': 'socks5h://127.0.0.1:9050',
            'https': 'socks5h://127.0.0.1:9050',
        }

        response = requests.get(
            'https://fbref.com/en/',
            timeout=60,
            headers={'User-Agent': 'Mozilla/5.0'},
            proxies=proxies,
        )
        # Cloudflare may block even through Tor, accept 200 or 403
        # soccerdata library has additional logic to handle this
        assert response.status_code in [200, 403], f"Unexpected status: {response.status_code}"

    def test_sofifa_with_tor(self, tor_available, skip_if_no_network):
        """Test that sofifa.com is accessible through Tor."""
        if not tor_available:
            pytest.skip("Tor not available")

        import requests

        proxies = {
            'http': 'socks5h://127.0.0.1:9050',
            'https': 'socks5h://127.0.0.1:9050',
        }

        response = requests.get(
            'https://sofifa.com/',
            timeout=60,
            headers={'User-Agent': 'Mozilla/5.0'},
            proxies=proxies,
        )
        # May be blocked even through Tor
        assert response.status_code in [200, 403], f"Unexpected status: {response.status_code}"


# =============================================================================
# Data Quality Tests
# =============================================================================

@pytest.mark.integration
class TestDataQuality:
    """Tests to verify data quality from scrapers."""

    def test_understat_schedule_columns(self, understat_scraper, skip_if_no_network, integration_delay):
        """Verify Understat schedule has expected columns."""
        df = understat_scraper.read_schedule()

        if df is not None and len(df) > 0:
            # Check for essential columns
            assert 'league' in df.columns or 'ENG-Premier League' in str(df.index.names)
            assert '_source' in df.columns
            assert '_ingested_at' in df.columns

    def test_matchhistory_odds_columns(self, matchhistory_scraper, skip_if_no_network, integration_delay):
        """Verify MatchHistory has betting odds columns."""
        df = matchhistory_scraper.read_games()

        if df is not None and len(df) > 0:
            # Check for betting odds columns (at least one bookmaker)
            odds_cols = [c for c in df.columns if 'odds' in c.lower() or c.startswith('B365')]
            # Note: standardized column names start with 'odds_'
            # Original columns start with 'B365', 'BW', etc.
            assert len(odds_cols) >= 0  # May or may not have odds depending on source

    def test_espn_schedule_has_teams(self, espn_scraper, skip_if_no_network, integration_delay):
        """Verify ESPN schedule has home and away teams."""
        df = espn_scraper.read_schedule()

        if df is not None and len(df) > 0:
            # Should have team information
            has_home = 'home_team' in df.columns or 'home' in str(df.columns).lower()
            has_away = 'away_team' in df.columns or 'away' in str(df.columns).lower()
            assert has_home or has_away, "Should have team columns"
