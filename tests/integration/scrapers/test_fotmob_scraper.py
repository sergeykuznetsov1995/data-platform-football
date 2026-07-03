"""
Integration tests for FotMobScraper.

Tests actual HTTP requests to FotMob's public /api/data endpoints.
Requires network connectivity only (no browser / Cloudflare bypass).
"""

import pytest


@pytest.mark.integration
@pytest.mark.slow
class TestFotMobScraperIntegration:
    """Integration tests for FotMobScraper with real HTTP."""

    @pytest.fixture
    def scraper(self, network_available, minimal_leagues, minimal_seasons):
        """Create real FotMob scraper instance."""
        if not network_available:
            pytest.skip("No network connectivity")

        from scrapers.fotmob import FotMobScraper

        scraper = FotMobScraper(
            leagues=minimal_leagues,
            seasons=minimal_seasons,
        )
        yield scraper
        scraper.close()

    @pytest.mark.flaky
    def test_read_schedule_real(self, scraper, integration_delay):
        """Test reading schedule from real FotMob API."""
        df = scraper.read_schedule()

        assert df is not None, "Schedule should be returned"
        assert not df.empty, "Schedule should not be empty"
        assert 'match_id' in df.columns
        assert 'home_team' in df.columns
        assert 'away_team' in df.columns
        assert 'league' in df.columns
        assert len(df) > 10, "Should have multiple matches"

    @pytest.mark.flaky
    def test_read_team_stats_real(self, scraper, integration_delay):
        """Test reading team stats from real FotMob API."""
        df = scraper.read_team_season_stats()

        assert df is not None, "Team stats should be returned"
        assert not df.empty, "Team stats should not be empty"
        assert 'team_name' in df.columns
        assert 'points' in df.columns
        assert len(df) >= 18, "Should have at least 18 teams (Premier League has 20)"

    @pytest.mark.flaky
    def test_read_player_stats_real(self, scraper, integration_delay):
        """Test reading player stats from real FotMob API."""
        df = scraper.read_player_season_stats()

        assert df is not None, "Player stats should be returned"
        assert not df.empty, "Player stats should not be empty"
        assert 'participant_name' in df.columns
        assert 'stat_value' in df.columns

    @pytest.mark.flaky
    def test_session_created(self, scraper):
        """Test that a plain HTTP session is created (no cookies needed)."""
        session = scraper._get_session()

        assert session is not None
        assert scraper._session is not None

    @pytest.mark.flaky
    def test_api_request_works(self, scraper):
        """Test that API requests work after cookie setup."""
        data = scraper._get_league_data('ENG-Premier League', 2024)

        assert data is not None, "League data should be returned"
        assert isinstance(data, dict), "Data should be a dictionary"

    def test_unknown_league_returns_none(self, scraper):
        """Test that unknown league returns None."""
        df = scraper.read_schedule('UNKNOWN-League', 2024)
        assert df is None

    def test_scraper_stats_tracked(self, scraper, integration_delay):
        """Test that scraper stats are tracked."""
        # Make a request
        scraper.read_schedule()

        stats = scraper.get_stats()
        assert stats['successes'] > 0 or stats['failures'] > 0


@pytest.mark.integration
@pytest.mark.slow
class TestFotMobScraperMultiLeague:
    """Test scraping multiple leagues."""

    @pytest.fixture
    def multi_league_scraper(self, network_available):
        """Create scraper for multiple leagues."""
        if not network_available:
            pytest.skip("No network connectivity")

        from scrapers.fotmob import FotMobScraper

        scraper = FotMobScraper(
            leagues=['ENG-Premier League', 'ESP-La Liga'],
            seasons=[2024],
        )
        yield scraper
        scraper.close()

    @pytest.mark.flaky
    def test_scrape_multiple_leagues(self, multi_league_scraper, integration_delay):
        """Test scraping data from multiple leagues."""
        # Just get schedule for each league
        df_epl = multi_league_scraper.read_schedule('ENG-Premier League', 2024)
        df_laliga = multi_league_scraper.read_schedule('ESP-La Liga', 2024)

        assert df_epl is not None
        assert df_laliga is not None

        # Check leagues are correctly tagged
        assert all(df_epl['league'] == 'ENG-Premier League')
        assert all(df_laliga['league'] == 'ESP-La Liga')
