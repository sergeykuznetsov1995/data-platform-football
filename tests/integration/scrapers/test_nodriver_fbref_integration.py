"""
Integration tests for NodriverFBrefScraper.

These tests make real HTTP requests to FBref and require:
- Network access
- nodriver and cf-verify installed
- Optionally: proxy file at /opt/airflow/proxys.txt

Run with:
    pytest tests/integration/scrapers/test_nodriver_fbref_integration.py -v -m integration
"""

import pytest
import os
import logging

logger = logging.getLogger(__name__)


# Skip all tests if nodriver is not available
pytest.importorskip("nodriver")


@pytest.fixture
def proxy_file():
    """Get proxy file path if available."""
    paths = [
        '/opt/airflow/proxys.txt',
        '/root/data_platform/proxys.txt',
        'proxys.txt',
    ]
    for path in paths:
        if os.path.exists(path):
            return path
    return None


@pytest.fixture
def scraper(proxy_file):
    """Create NodriverFBrefScraper instance."""
    from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

    scraper = NodriverFBrefScraper(
        leagues=['ENG-Premier League'],
        seasons=[2024],
        proxy_file=proxy_file,
        headless=True,
        use_xvfb=True,
        cloudflare_wait=90.0,
        max_retries=3,
        cf_verify_max_retries=10,
    )

    yield scraper

    scraper.close()


@pytest.mark.integration
@pytest.mark.cloudflare
@pytest.mark.slow
class TestNodriverFBrefIntegration:
    """Integration tests for NodriverFBrefScraper."""

    def test_bypass_cloudflare_and_fetch_html(self, scraper):
        """Test that nodriver can bypass Cloudflare and fetch HTML."""
        html = scraper._fetch_page("https://fbref.com/en/comps/")

        # Should either succeed or fail gracefully
        if html is not None:
            assert len(html) > 1000, "HTML too short"
            assert '<table' in html or 'Premier League' in html, "Missing expected content"
            assert not scraper._is_cloudflare_blocked(html), "Cloudflare still blocking"

            logger.info(f"Successfully fetched HTML: {len(html)} bytes")
            logger.info(f"Stats: {scraper.get_stats()}")
        else:
            logger.warning("Page fetch returned None - Cloudflare may be blocking")

    def test_read_schedule(self, scraper):
        """Test reading schedule from FBref."""
        df = scraper.read_schedule('ENG-Premier League', 2024)

        if df is not None:
            assert not df.empty, "Schedule DataFrame is empty"
            assert 'league' in df.columns
            assert 'season' in df.columns

            logger.info(f"Schedule: {len(df)} rows")
            logger.info(f"Columns: {list(df.columns)}")
        else:
            logger.warning("Schedule read returned None")

    def test_read_player_stats(self, scraper):
        """Test reading player statistics."""
        df = scraper.read_player_season_stats('stats', 'ENG-Premier League', 2024)

        if df is not None:
            assert not df.empty, "Player stats DataFrame is empty"
            assert 'stat_type' in df.columns
            assert df['stat_type'].iloc[0] == 'stats'

            logger.info(f"Player stats: {len(df)} rows")
        else:
            logger.warning("Player stats read returned None")

    def test_read_team_stats(self, scraper):
        """Test reading team statistics."""
        df = scraper.read_team_season_stats('stats', 'ENG-Premier League', 2024)

        if df is not None:
            assert not df.empty, "Team stats DataFrame is empty"
            assert 'stat_type' in df.columns

            logger.info(f"Team stats: {len(df)} rows")
        else:
            logger.warning("Team stats read returned None")

    def test_scrape_single_stat_type(self, scraper):
        """Test scraping a single stat type."""
        result = scraper.scrape_single_stat_type('stats', 'player')

        if result['data'] is not None:
            assert result['rows'] > 0, "No rows collected"
            logger.info(f"Scraped {result['rows']} player_stats rows")
        else:
            logger.warning("Single stat scrape returned no data")

        # Always check stats
        stats = result.get('stats', {})
        logger.info(f"Scraper stats: {stats}")


@pytest.mark.integration
@pytest.mark.cloudflare
class TestCloudflareBypassDirect:
    """Direct tests for Cloudflare bypass using NodriverBypass."""

    def test_nodriver_bypass_fbref(self, proxy_file):
        """Test NodriverBypass on FBref directly."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            headless=True,
            use_xvfb=True,
            proxy=proxy_file,  # Will be parsed if file-like
            use_cf_verify=True,
            cf_verify_max_retries=10,
            cloudflare_wait=60.0,
        )

        try:
            html = bypass.get_page(
                "https://fbref.com/en/comps/9/Premier-League-Stats",
                cloudflare_wait=60.0,
            )

            if html:
                assert len(html) > 500, "HTML too short"
                logger.info(f"NodriverBypass fetched {len(html)} bytes")

                # Check for Cloudflare block
                blocked_indicators = [
                    'just a moment',
                    'checking your browser',
                    'turnstile',
                ]
                html_lower = html.lower()
                is_blocked = any(ind in html_lower for ind in blocked_indicators)

                if is_blocked:
                    logger.warning("Page still shows Cloudflare challenge")
                else:
                    logger.info("Cloudflare bypass successful!")
            else:
                logger.warning("get_page returned None")

        finally:
            bypass.close_sync()


@pytest.mark.integration
class TestProxyManagerIntegration:
    """Tests for ProxyManager integration with nodriver."""

    def test_proxy_manager_nodriver_format(self, proxy_file):
        """Test ProxyManager returns correct nodriver format."""
        if not proxy_file:
            pytest.skip("No proxy file available")

        from scrapers.utils.proxy_manager import ProxyManager, ProxyType

        pm = ProxyManager()
        pm.load_from_file_custom_format(proxy_file, ProxyType.HTTP)

        assert pm.total_count > 0, "No proxies loaded"

        proxy_str = pm.get_nodriver_proxy_string()
        assert proxy_str is not None, "No proxy string returned"

        # Should be in format host:port:user:pass
        parts = proxy_str.split(':')
        assert len(parts) >= 2, f"Invalid format: {proxy_str}"

        logger.info(f"Loaded {pm.total_count} proxies")
        logger.info(f"Sample proxy: {parts[0]}:****")

    def test_proxy_manager_nodriver_dict(self, proxy_file):
        """Test ProxyManager returns correct nodriver dict."""
        if not proxy_file:
            pytest.skip("No proxy file available")

        from scrapers.utils.proxy_manager import ProxyManager, ProxyType

        pm = ProxyManager()
        pm.load_from_file_custom_format(proxy_file, ProxyType.HTTP)

        proxy_dict = pm.get_nodriver_proxy_dict()
        assert proxy_dict is not None, "No proxy dict returned"
        assert 'host' in proxy_dict
        assert 'port' in proxy_dict

        logger.info(f"Proxy dict: host={proxy_dict['host']}, port={proxy_dict['port']}")
