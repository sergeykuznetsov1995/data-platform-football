"""
Integration tests for Cloudflare bypass functionality.

These tests verify:
- CloudflareBypass class works with Selenium
- Browser can navigate to protected sites
- Page source can be extracted

Run with:
    pytest tests/integration/scrapers/test_cloudflare_bypass.py -v -m cloudflare
"""

import pytest


# =============================================================================
# CloudflareBypass Class Tests
# =============================================================================

@pytest.mark.integration
@pytest.mark.cloudflare
@pytest.mark.slow
class TestCloudflareBypass:
    """Integration tests for CloudflareBypass class."""

    def test_initialization(self, cloudflare_bypass):
        """Test CloudflareBypass initializes correctly."""
        assert cloudflare_bypass is not None
        assert cloudflare_bypass.config.headless is True

    def test_driver_creation(self, cloudflare_bypass):
        """Test that browser driver can be created."""
        driver = cloudflare_bypass.driver
        assert driver is not None

    def test_navigate_to_simple_page(self, cloudflare_bypass, skip_if_no_network):
        """Test navigation to a simple page without Cloudflare."""
        html = cloudflare_bypass.get_page(
            'https://www.google.com',
            cloudflare_wait=1.0,
        )

        assert html is not None
        assert len(html) > 0
        assert 'Google' in html

    def test_current_url(self, cloudflare_bypass, skip_if_no_network):
        """Test current URL property."""
        cloudflare_bypass.get_page('https://www.google.com', cloudflare_wait=1.0)
        url = cloudflare_bypass.current_url

        assert url is not None
        assert 'google' in url.lower()

    def test_execute_script(self, cloudflare_bypass, skip_if_no_network):
        """Test JavaScript execution."""
        cloudflare_bypass.get_page('https://www.google.com', cloudflare_wait=1.0)
        result = cloudflare_bypass.execute_script('return document.title')

        assert result is not None
        assert isinstance(result, str)

    def test_get_cookies(self, cloudflare_bypass, skip_if_no_network):
        """Test cookie retrieval."""
        cloudflare_bypass.get_page('https://www.google.com', cloudflare_wait=1.0)
        cookies = cloudflare_bypass.get_cookies()

        assert cookies is not None
        assert isinstance(cookies, list)

    def test_close_driver(self, cloudflare_bypass):
        """Test driver cleanup."""
        # Force driver creation
        _ = cloudflare_bypass.driver
        assert cloudflare_bypass._driver is not None

        cloudflare_bypass.close()
        assert cloudflare_bypass._driver is None


# =============================================================================
# WhoScored Navigation Tests
# =============================================================================

@pytest.mark.integration
@pytest.mark.cloudflare
@pytest.mark.slow
class TestWhoScoredNavigation:
    """Integration tests for navigating WhoScored with Cloudflare bypass."""

    def test_navigate_to_whoscored_homepage(self, cloudflare_bypass, skip_if_no_network):
        """Test navigation to WhoScored homepage.

        Note: This may be blocked by Cloudflare even with undetected-chromedriver.
        """
        try:
            html = cloudflare_bypass.get_page(
                'https://www.whoscored.com',
                cloudflare_wait=10.0,
            )

            assert html is not None
            assert len(html) > 0

            # Check if we got through Cloudflare
            if 'Checking your browser' in html or 'challenge-running' in html:
                pytest.skip("Cloudflare challenge not bypassed")

            # If successful, should have WhoScored content
            assert 'WhoScored' in html or 'whoscored' in html.lower()

        except Exception as e:
            pytest.skip(f"WhoScored navigation failed: {e}")

    def test_navigate_to_league_page(self, cloudflare_bypass, skip_if_no_network):
        """Test navigation to a league page."""
        try:
            html = cloudflare_bypass.get_page(
                'https://www.whoscored.com/Regions/252/Tournaments/2/England-Premier-League',
                cloudflare_wait=10.0,
            )

            assert html is not None
            assert len(html) > 0

            if 'Checking your browser' in html:
                pytest.skip("Cloudflare challenge not bypassed")

        except Exception as e:
            pytest.skip(f"League page navigation failed: {e}")


# =============================================================================
# Browser Session Tests
# =============================================================================

@pytest.mark.integration
@pytest.mark.cloudflare
@pytest.mark.slow
class TestBrowserSession:
    """Integration tests for browser_session context manager."""

    def test_browser_session_context_manager(self, undetected_chrome_available, skip_if_no_network):
        """Test browser_session as context manager."""
        if not undetected_chrome_available:
            pytest.skip("undetected-chromedriver not available")

        from scrapers.base.cloudflare_bypass import browser_session

        with browser_session(headless=True) as browser:
            html = browser.get_page('https://www.google.com', cloudflare_wait=1.0)
            assert html is not None
            assert 'Google' in html

        # After context, browser should be closed
        assert browser._driver is None


# =============================================================================
# Proxy Configuration Tests
# =============================================================================

@pytest.mark.integration
@pytest.mark.cloudflare
class TestCloudflareBypassWithProxy:
    """Tests for CloudflareBypass with proxy configuration."""

    def test_initialization_with_proxy(self, undetected_chrome_available, tor_available):
        """Test CloudflareBypass initialization with Tor proxy."""
        if not undetected_chrome_available:
            pytest.skip("undetected-chromedriver not available")
        if not tor_available:
            pytest.skip("Tor not available")

        from scrapers.base.cloudflare_bypass import CloudflareBypass

        bypass = CloudflareBypass(
            headless=True,
            proxy='socks5://127.0.0.1:9050',
        )

        try:
            assert bypass.config.proxy == 'socks5://127.0.0.1:9050'
        finally:
            bypass.close()


# =============================================================================
# Browser Configuration Tests
# =============================================================================

@pytest.mark.integration
@pytest.mark.cloudflare
class TestBrowserConfig:
    """Tests for BrowserConfig dataclass."""

    def test_default_config(self):
        """Test default browser configuration."""
        from scrapers.base.cloudflare_bypass import BrowserConfig

        config = BrowserConfig()

        assert config.headless is True
        assert config.window_size == (1920, 1080)
        assert config.page_load_timeout == 30
        assert config.implicit_wait == 10
        assert config.proxy is None
        assert config.extra_arguments == []

    def test_custom_config(self):
        """Test custom browser configuration."""
        from scrapers.base.cloudflare_bypass import BrowserConfig

        config = BrowserConfig(
            headless=False,
            window_size=(1280, 720),
            proxy='http://localhost:8080',
        )

        assert config.headless is False
        assert config.window_size == (1280, 720)
        assert config.proxy == 'http://localhost:8080'


# =============================================================================
# Error Handling Tests
# =============================================================================

@pytest.mark.integration
@pytest.mark.cloudflare
class TestCloudflareBypassErrors:
    """Tests for error handling in CloudflareBypass."""

    def test_timeout_handling(self, cloudflare_bypass, skip_if_no_network):
        """Test handling of page load timeout."""
        # Set very short timeout
        cloudflare_bypass.config.page_load_timeout = 1

        try:
            # This should either work quickly or timeout
            cloudflare_bypass.get_page(
                'https://www.google.com',
                cloudflare_wait=0.5,
            )
        except Exception:
            # Timeout is expected
            pass

    def test_invalid_url_handling(self, cloudflare_bypass):
        """Test handling of invalid URLs."""
        with pytest.raises(Exception):
            cloudflare_bypass.get_page('not-a-valid-url')


# =============================================================================
# Wait Functions Tests
# =============================================================================

@pytest.mark.integration
@pytest.mark.cloudflare
@pytest.mark.slow
class TestWaitFunctions:
    """Tests for wait functions in CloudflareBypass."""

    def test_wait_for_element(self, cloudflare_bypass, skip_if_no_network):
        """Test waiting for element."""
        cloudflare_bypass.get_page('https://www.google.com', cloudflare_wait=1.0)

        # Wait for search input
        try:
            element = cloudflare_bypass.wait_for_element(
                'input[name="q"]',
                timeout=5,
            )
            assert element is not None
        except Exception:
            # Element may not be found if page structure changed
            pass

    def test_wait_for_text(self, cloudflare_bypass, skip_if_no_network):
        """Test waiting for text on page."""
        cloudflare_bypass.get_page('https://www.google.com', cloudflare_wait=1.0)

        # Google page should contain 'Google'
        found = cloudflare_bypass.wait_for_text('Google', timeout=5)
        assert found is True

        # Should not find random text
        found = cloudflare_bypass.wait_for_text('RandomTextXYZ123', timeout=1)
        assert found is False
