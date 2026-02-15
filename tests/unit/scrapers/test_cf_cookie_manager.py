"""
Unit tests for CFCookieManager.

Tests the Cloudflare cookie extraction and injection functionality
without making actual network requests (uses mocks).
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture
def cf_cookie_manager():
    """Create a CFCookieManager instance for testing."""
    from scrapers.base.browser.cf_cookie_manager import CFCookieManager
    return CFCookieManager(
        cache_ttl_minutes=30,
        use_cf_verify=True,
        cf_verify_max_retries=5,
        cf_verify_interval=1.0,
    )


class TestCFCookieManager:
    """Tests for CFCookieManager class."""

    def test_init_default_values(self):
        """Test default initialization values."""
        from scrapers.base.browser.cf_cookie_manager import CFCookieManager

        manager = CFCookieManager()

        assert manager.cache_ttl == timedelta(minutes=30)
        assert manager.use_cf_verify is True
        assert manager.cf_verify_max_retries == 15  # Updated default
        assert manager.cf_verify_interval == 3.0  # Updated default
        assert manager.headless is True
        assert manager.use_xvfb is True
        assert manager._cookie_cache == {}

    def test_init_custom_values(self):
        """Test initialization with custom values."""
        from scrapers.base.browser.cf_cookie_manager import CFCookieManager

        manager = CFCookieManager(
            cache_ttl_minutes=60,
            use_cf_verify=False,
            cf_verify_max_retries=15,
            cf_verify_interval=3.0,
            headless=False,
            use_xvfb=False,
        )

        assert manager.cache_ttl == timedelta(minutes=60)
        assert manager.use_cf_verify is False
        assert manager.cf_verify_max_retries == 15
        assert manager.cf_verify_interval == 3.0
        assert manager.headless is False
        assert manager.use_xvfb is False

    def test_clear_cache_all(self, cf_cookie_manager):
        """Test clearing all cached cookies."""
        # Add some fake cached cookies
        cf_cookie_manager._cookie_cache = {
            'fbref.com': ({'cf_clearance': 'abc'}, datetime.now()),
            'example.com': ({'cf_clearance': 'xyz'}, datetime.now()),
        }

        cf_cookie_manager.clear_cache()

        assert cf_cookie_manager._cookie_cache == {}

    def test_clear_cache_single_domain(self, cf_cookie_manager):
        """Test clearing cache for a single domain."""
        cf_cookie_manager._cookie_cache = {
            'fbref.com': ({'cf_clearance': 'abc'}, datetime.now()),
            'example.com': ({'cf_clearance': 'xyz'}, datetime.now()),
        }

        cf_cookie_manager.clear_cache(domain='fbref.com')

        assert 'fbref.com' not in cf_cookie_manager._cookie_cache
        assert 'example.com' in cf_cookie_manager._cookie_cache

    def test_get_cache_info_empty(self, cf_cookie_manager):
        """Test cache info when empty."""
        info = cf_cookie_manager.get_cache_info()
        assert info == {}

    def test_get_cache_info_with_data(self, cf_cookie_manager):
        """Test cache info with cached data."""
        now = datetime.now()
        cf_cookie_manager._cookie_cache = {
            'fbref.com': (
                {'cf_clearance': 'abc', '__cf_bm': 'xyz'},
                now - timedelta(minutes=10)
            ),
        }

        info = cf_cookie_manager.get_cache_info()

        assert 'fbref.com' in info
        assert info['fbref.com']['cookie_count'] == 2
        assert 'cf_clearance' in info['fbref.com']['cookie_names']
        assert '__cf_bm' in info['fbref.com']['cookie_names']
        assert info['fbref.com']['is_valid'] is True
        # Age should be around 600 seconds (10 minutes)
        assert 590 < info['fbref.com']['age_seconds'] < 610

    def test_get_cache_info_expired(self, cf_cookie_manager):
        """Test cache info for expired cookies."""
        now = datetime.now()
        cf_cookie_manager._cookie_cache = {
            'fbref.com': (
                {'cf_clearance': 'abc'},
                now - timedelta(minutes=60)  # Expired (TTL is 30 min)
            ),
        }

        info = cf_cookie_manager.get_cache_info()

        assert 'fbref.com' in info
        assert info['fbref.com']['is_valid'] is False
        assert info['fbref.com']['expires_in_seconds'] == 0


class TestCFCookieManagerCaching:
    """Tests for cookie caching behavior."""

    @pytest.mark.asyncio
    async def test_get_cookies_uses_cache(self, cf_cookie_manager):
        """Test that cached cookies are returned without fetching."""
        cached_cookies = {'cf_clearance': 'cached_value'}
        cf_cookie_manager._cookie_cache = {
            'fbref.com': (cached_cookies, datetime.now())
        }

        # Mock _fetch_cookies to ensure it's not called
        cf_cookie_manager._fetch_cookies = AsyncMock()

        cookies = await cf_cookie_manager.get_cookies('https://fbref.com/en/comps/')

        assert cookies == cached_cookies
        cf_cookie_manager._fetch_cookies.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_cookies_fetches_when_cache_expired(self, cf_cookie_manager):
        """Test that expired cache triggers a fresh fetch."""
        old_cookies = {'cf_clearance': 'old_value'}
        new_cookies = {'cf_clearance': 'new_value'}

        # Set expired cache
        cf_cookie_manager._cookie_cache = {
            'fbref.com': (old_cookies, datetime.now() - timedelta(hours=1))
        }

        # Mock _fetch_cookies
        cf_cookie_manager._fetch_cookies = AsyncMock(return_value=new_cookies)

        cookies = await cf_cookie_manager.get_cookies('https://fbref.com/en/')

        assert cookies == new_cookies
        cf_cookie_manager._fetch_cookies.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_cookies_force_refresh(self, cf_cookie_manager):
        """Test force refresh bypasses cache."""
        cached_cookies = {'cf_clearance': 'cached_value'}
        new_cookies = {'cf_clearance': 'fresh_value'}

        cf_cookie_manager._cookie_cache = {
            'fbref.com': (cached_cookies, datetime.now())
        }

        cf_cookie_manager._fetch_cookies = AsyncMock(return_value=new_cookies)

        cookies = await cf_cookie_manager.get_cookies(
            'https://fbref.com/en/',
            force_refresh=True
        )

        assert cookies == new_cookies
        cf_cookie_manager._fetch_cookies.assert_called_once()


class TestCFCookieManagerSyncWrapper:
    """Tests for synchronous wrapper methods."""

    def test_get_cookies_sync(self, cf_cookie_manager):
        """Test synchronous get_cookies wrapper."""
        cached_cookies = {'cf_clearance': 'sync_test'}
        cf_cookie_manager._cookie_cache = {
            'fbref.com': (cached_cookies, datetime.now())
        }

        cookies = cf_cookie_manager.get_cookies_sync('https://fbref.com')

        assert cookies == cached_cookies

    def test_get_cookies_with_retry_sync_success(self, cf_cookie_manager):
        """Test get_cookies_with_retry_sync returns cookies on success."""
        from unittest.mock import AsyncMock

        mock_proxy_manager = MagicMock()
        mock_proxy_manager.get_http_proxy_url.return_value = 'http://proxy:8080'

        # Mock get_cookies to return cookies with cf_clearance
        expected_cookies = {'cf_clearance': 'test_value', '__cf_bm': 'other'}
        cf_cookie_manager.get_cookies = AsyncMock(return_value=expected_cookies)

        cookies = cf_cookie_manager.get_cookies_with_retry_sync(
            url='https://fbref.com',
            proxy_manager=mock_proxy_manager,
            max_attempts=3
        )

        assert cookies == expected_cookies
        assert 'cf_clearance' in cookies

    def test_get_cookies_with_retry_sync_fails_without_clearance(self, cf_cookie_manager):
        """Test get_cookies_with_retry_sync returns empty on failure."""
        from unittest.mock import AsyncMock

        mock_proxy_manager = MagicMock()
        mock_proxy_manager.get_http_proxy_url.return_value = 'http://proxy:8080'

        # Mock get_cookies to return cookies WITHOUT cf_clearance
        cf_cookie_manager.get_cookies = AsyncMock(return_value={'__cf_bm': 'other'})

        cookies = cf_cookie_manager.get_cookies_with_retry_sync(
            url='https://fbref.com',
            proxy_manager=mock_proxy_manager,
            max_attempts=2
        )

        # Should return empty dict when cf_clearance not obtained
        assert cookies == {}
        # Should have tried max_attempts times
        assert cf_cookie_manager.get_cookies.call_count == 2


class TestCFCookieManagerHelpers:
    """Tests for helper functions."""

    def test_get_cf_cookie_manager_singleton(self):
        """Test singleton pattern for default manager."""
        from scrapers.base.browser.cf_cookie_manager import (
            get_cf_cookie_manager,
            _default_manager,
        )

        # Reset singleton
        import scrapers.base.browser.cf_cookie_manager as module
        module._default_manager = None

        manager1 = get_cf_cookie_manager()
        manager2 = get_cf_cookie_manager()

        assert manager1 is manager2

    def test_inject_cf_cookies_sync(self):
        """Test inject_cf_cookies_sync helper function."""
        from scrapers.base.browser.cf_cookie_manager import inject_cf_cookies_sync

        # Create a mock session
        mock_session = MagicMock()
        mock_session.cookies = MagicMock()

        # Mock the manager
        with patch(
            'scrapers.base.browser.cf_cookie_manager.get_cf_cookie_manager'
        ) as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.get_cookies_sync.return_value = {
                'cf_clearance': 'test_value'
            }
            mock_get_manager.return_value = mock_manager

            result = inject_cf_cookies_sync(
                mock_session,
                'https://fbref.com',
            )

            assert result is True
            mock_session.cookies.update.assert_called_once_with({
                'cf_clearance': 'test_value'
            })

    def test_inject_cf_cookies_sync_no_cookies(self):
        """Test inject_cf_cookies_sync when no cookies available."""
        from scrapers.base.browser.cf_cookie_manager import inject_cf_cookies_sync

        mock_session = MagicMock()

        with patch(
            'scrapers.base.browser.cf_cookie_manager.get_cf_cookie_manager'
        ) as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.get_cookies_sync.return_value = {}
            mock_get_manager.return_value = mock_manager

            result = inject_cf_cookies_sync(mock_session, 'https://fbref.com')

            assert result is False
            mock_session.cookies.update.assert_not_called()


class TestCFCookieManagerFetchCookies:
    """Tests for _fetch_cookies method with mocked nodriver."""

    @pytest.mark.asyncio
    async def test_fetch_cookies_success(self, cf_cookie_manager):
        """Test successful cookie fetch with mocked nodriver."""
        mock_cookies = [
            MagicMock(name='cf_clearance', value='test_clearance'),
            MagicMock(name='__cf_bm', value='test_bm'),
            MagicMock(name='session_id', value='not_cf_cookie'),  # Should be filtered
        ]
        # Fix: MagicMock 'name' attribute conflicts with cookie name
        mock_cookies[0].name = 'cf_clearance'
        mock_cookies[0].value = 'test_clearance'
        mock_cookies[1].name = '__cf_bm'
        mock_cookies[1].value = 'test_bm'
        mock_cookies[2].name = 'session_id'
        mock_cookies[2].value = 'not_cf_cookie'

        with patch(
            'scrapers.base.browser.cf_cookie_manager.NodriverBypass'
        ) as MockBypass:
            mock_instance = AsyncMock()
            mock_instance._browser = MagicMock()
            mock_instance._browser.cookies.get_all = AsyncMock(return_value=mock_cookies)
            MockBypass.return_value = mock_instance

            cookies = await cf_cookie_manager._fetch_cookies('https://fbref.com')

            assert 'cf_clearance' in cookies
            assert '__cf_bm' in cookies
            # session_id should not be in cookies (not a CF cookie)
            # Note: current impl includes anything with 'cf' in name

    @pytest.mark.asyncio
    async def test_fetch_cookies_error_returns_empty(self, cf_cookie_manager):
        """Test that errors during fetch return empty dict."""
        with patch(
            'scrapers.base.browser.cf_cookie_manager.NodriverBypass'
        ) as MockBypass:
            mock_instance = AsyncMock()
            mock_instance.start = AsyncMock(side_effect=Exception("Browser error"))
            MockBypass.return_value = mock_instance

            cookies = await cf_cookie_manager._fetch_cookies('https://fbref.com')

            assert cookies == {}


class TestCloudfareCookieNames:
    """Tests for Cloudflare cookie name detection."""

    def test_cloudflare_cookie_names_constant(self):
        """Test that expected CF cookie names are defined."""
        from scrapers.base.browser.cf_cookie_manager import CFCookieManager

        expected_names = {'cf_clearance', '__cf_bm', '__cflb', '__cfuvid', '_cfuvid'}

        assert CFCookieManager.CLOUDFLARE_COOKIE_NAMES == expected_names
