"""
Integration tests for MatchHistoryDirectScraper.

Tests actual HTTP requests to football-data.co.uk.
Requires network connectivity.
"""

import pytest


@pytest.mark.integration
@pytest.mark.slow
class TestMatchHistoryDirectIntegration:
    """Integration tests for MatchHistoryDirectScraper with real HTTP."""

    @pytest.fixture
    def scraper(self, network_available, minimal_leagues, minimal_seasons):
        """Create real MatchHistory scraper instance."""
        if not network_available:
            pytest.skip("No network connectivity")

        from scrapers.matchhistory_direct_scraper import MatchHistoryDirectScraper

        scraper = MatchHistoryDirectScraper(
            leagues=minimal_leagues,
            seasons=minimal_seasons,
            headless=True,
            use_xvfb=True,
        )
        yield scraper
        scraper.close()

    @pytest.mark.flaky
    def test_read_games_real(self, scraper, integration_delay):
        """Test reading games from real football-data.co.uk."""
        df = scraper.read_games()

        assert df is not None, "Games should be returned"
        assert not df.empty, "Games should not be empty"
        assert 'home_team' in df.columns
        assert 'away_team' in df.columns
        assert 'home_goals' in df.columns
        assert 'away_goals' in df.columns
        assert 'league' in df.columns
        assert len(df) > 50, "Should have multiple matches"

    @pytest.mark.flaky
    def test_odds_data_present(self, scraper, integration_delay):
        """Test that betting odds are present in data."""
        df = scraper.read_games()

        assert df is not None, "Games should be returned"

        # Check for odds columns
        odds_cols = [c for c in df.columns if 'odds_' in c]
        assert len(odds_cols) > 0, "Should have odds columns"

    @pytest.mark.flaky
    def test_calculate_odds_stats_real(self, scraper, integration_delay):
        """Test odds calculation on real data."""
        df = scraper.read_games()

        if df is None:
            pytest.skip("No data returned")

        df = scraper.calculate_odds_stats(df)

        # Check calculated columns exist
        if 'odds_home_avg' in df.columns:
            assert 'prob_home_implied' in df.columns
            assert df['prob_home_implied'].notnull().any()

    @pytest.mark.flaky
    def test_direct_request_works(self, scraper):
        """Test that direct HTTP request works."""
        url = scraper._get_csv_url('ENG-Premier League', 2024)
        df = scraper._fetch_csv_with_requests(url)

        # May return None if server blocks, but should not raise exception
        if df is not None:
            assert not df.empty

    def test_unknown_league_returns_none(self, scraper):
        """Test that unknown league returns None."""
        df = scraper.read_games('UNKNOWN-League', 2024)
        assert df is None

    def test_scraper_stats_tracked(self, scraper, integration_delay):
        """Test that scraper stats are tracked."""
        # Make a request
        scraper.read_games()

        stats = scraper.get_stats()
        assert stats['successes'] > 0 or stats['failures'] > 0


@pytest.mark.integration
@pytest.mark.slow
class TestMatchHistorySeleniumFallback:
    """Test Selenium fallback functionality."""

    @pytest.fixture
    def scraper_with_selenium(self, network_available, undetected_chrome_available):
        """Create scraper with Selenium available."""
        if not network_available:
            pytest.skip("No network connectivity")
        if not undetected_chrome_available:
            pytest.skip("undetected-chromedriver not available")

        from scrapers.matchhistory_direct_scraper import MatchHistoryDirectScraper

        scraper = MatchHistoryDirectScraper(
            leagues=['ENG-Premier League'],
            seasons=[2024],
            headless=True,
            use_xvfb=True,
        )
        yield scraper
        scraper.close()

    @pytest.mark.flaky
    @pytest.mark.cloudflare
    def test_selenium_fallback_works(self, scraper_with_selenium):
        """Test that Selenium fallback can fetch data."""
        url = scraper_with_selenium._get_csv_url('ENG-Premier League', 2024)

        # Force use of Selenium by directly calling the method
        df = scraper_with_selenium._fetch_csv_with_selenium(url)

        # Selenium should be able to fetch the data
        if df is not None:
            assert not df.empty
            assert 'HomeTeam' in df.columns or 'home_team' in df.columns


@pytest.mark.integration
@pytest.mark.slow
class TestMatchHistoryMultiLeague:
    """Test scraping multiple leagues."""

    @pytest.fixture
    def multi_league_scraper(self, network_available):
        """Create scraper for multiple leagues."""
        if not network_available:
            pytest.skip("No network connectivity")

        from scrapers.matchhistory_direct_scraper import MatchHistoryDirectScraper

        scraper = MatchHistoryDirectScraper(
            leagues=['ENG-Premier League', 'ESP-La Liga'],
            seasons=[2024],
            headless=True,
            use_xvfb=True,
        )
        yield scraper
        scraper.close()

    @pytest.mark.flaky
    def test_scrape_multiple_leagues(self, multi_league_scraper, integration_delay):
        """Test scraping data from multiple leagues."""
        df_epl = multi_league_scraper.read_games('ENG-Premier League', 2024)
        df_laliga = multi_league_scraper.read_games('ESP-La Liga', 2024)

        # At least one should work
        has_data = (df_epl is not None and not df_epl.empty) or \
                   (df_laliga is not None and not df_laliga.empty)
        assert has_data, "At least one league should return data"

        # Check leagues are correctly tagged
        if df_epl is not None:
            assert all(df_epl['league'] == 'ENG-Premier League')
        if df_laliga is not None:
            assert all(df_laliga['league'] == 'ESP-La Liga')
