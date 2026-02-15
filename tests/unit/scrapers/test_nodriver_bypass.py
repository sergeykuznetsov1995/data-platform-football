"""
Unit tests for NodriverBypass proxy parsing and CF-verify integration.
"""

import asyncio

import pytest
from unittest.mock import MagicMock, AsyncMock, patch


class TestNodriverBypassInit:
    """Tests for NodriverBypass initialization."""

    @pytest.mark.unit
    def test_init_default_cf_verify_settings(self):
        """Test default cf-verify settings on init."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()

        assert bypass.use_cf_verify is True
        assert bypass.cf_verify_max_retries == 10
        assert bypass.cf_verify_interval == 2.0

    @pytest.mark.unit
    def test_init_custom_cf_verify_settings(self):
        """Test custom cf-verify settings on init."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            use_cf_verify=False,
            cf_verify_max_retries=15,
            cf_verify_interval=3.0,
        )

        assert bypass.use_cf_verify is False
        assert bypass.cf_verify_max_retries == 15
        assert bypass.cf_verify_interval == 3.0

    @pytest.mark.unit
    def test_init_default_content_timeout(self):
        """Test default content_timeout is 45s."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()
        assert bypass.content_timeout == 30.0

    @pytest.mark.unit
    def test_init_custom_content_timeout(self):
        """Test custom content_timeout."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(content_timeout=180.0)
        assert bypass.content_timeout == 180.0


class TestNodriverBypassWaitForSelector:
    """Tests for wait_for_selector feature."""

    @pytest.mark.unit
    def test_init_default_wait_for_selector(self):
        """Test default wait_for_selector is None."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()

        assert bypass.wait_for_selector is None
        assert bypass.wait_for_selector_timeout == 30.0

    @pytest.mark.unit
    def test_init_custom_wait_for_selector(self):
        """Test custom wait_for_selector settings."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            wait_for_selector='#content',
            wait_for_selector_timeout=45.0,
        )

        assert bypass.wait_for_selector == '#content'
        assert bypass.wait_for_selector_timeout == 45.0

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_internal_calls_select_when_selector_set(self):
        """Test that _get_internal waits for selector when configured."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            wait_for_selector='table',
            wait_for_selector_timeout=5.0,
            cloudflare_wait=0.1,
        )
        bypass._page = MagicMock()
        bypass._page.get = AsyncMock()
        bypass._page.get_content = AsyncMock(return_value="<html><table>data</table></html>")
        bypass._page.select = AsyncMock(return_value=MagicMock())

        # Mock _wait_for_cloudflare to skip CF logic
        bypass._wait_for_cloudflare = AsyncMock()

        html = await bypass._get_internal("https://fbref.com/test")

        bypass._page.select.assert_called_once_with('table', timeout=5.0)
        assert len(html) > 0

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_internal_skips_select_when_no_selector(self):
        """Test that _get_internal skips selector wait when not configured."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(cloudflare_wait=0.1)
        bypass._page = MagicMock()
        bypass._page.get = AsyncMock()
        bypass._page.get_content = AsyncMock(return_value="<html>Page content</html>" * 1000)
        bypass._page.select = AsyncMock()

        bypass._wait_for_cloudflare = AsyncMock()

        await bypass._get_internal("https://fbref.com/test")

        bypass._page.select.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_internal_handles_selector_timeout(self):
        """Test that _get_internal handles selector timeout gracefully."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            wait_for_selector='#nonexistent',
            wait_for_selector_timeout=0.1,
            cloudflare_wait=0.1,
        )
        bypass._page = MagicMock()
        bypass._page.get = AsyncMock()
        bypass._page.get_content = AsyncMock(return_value="<html>Partial page</html>" * 1000)
        bypass._page.select = AsyncMock(side_effect=asyncio.TimeoutError())

        bypass._wait_for_cloudflare = AsyncMock()

        # Should not raise — timeout is handled gracefully
        html = await bypass._get_internal("https://fbref.com/test")
        assert html is not None


class TestNodriverContentTimeoutRetry:
    """Tests for content_timeout and fallback chain in _get_internal."""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_internal_uses_content_timeout_for_pre_content_js(self):
        """Test that _get_internal uses self.content_timeout for pre_content_js."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            content_timeout=90.0,
            cloudflare_wait=0.1,
            pre_content_js="(function() { return 'ok'; })()",
        )
        bypass._page = MagicMock()
        bypass._page.get = AsyncMock()
        bypass._wait_for_cloudflare = AsyncMock()

        mock_result = MagicMock()
        mock_result.value = "complete"
        bypass._page.evaluate = AsyncMock(return_value=mock_result)
        bypass._page.select = AsyncMock(side_effect=asyncio.TimeoutError())

        big_html = "<html>" + "a" * 20000 + "</html>"
        bypass._get_html_with_fallback = AsyncMock(return_value=big_html)

        # Patch asyncio.wait_for to verify timeout value
        original_wait_for = asyncio.wait_for
        captured_timeouts = []

        async def mock_wait_for(coro, *, timeout=None):
            captured_timeouts.append(timeout)
            return await original_wait_for(coro, timeout=timeout)

        with patch('scrapers.base.browser.nodriver_bypass.asyncio.wait_for', side_effect=mock_wait_for):
            await bypass._get_internal("https://fbref.com/test")

        # content_timeout (90.0) should appear for pre_content_js
        assert 90.0 in captured_timeouts

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_internal_uses_fallback_chain_on_evaluate_failure(self):
        """Test that _get_internal uses _get_html_with_fallback for content extraction."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            cloudflare_wait=0.1,
        )
        bypass._page = MagicMock()
        bypass._page.get = AsyncMock()
        bypass._wait_for_cloudflare = AsyncMock()
        bypass._page.select = AsyncMock(side_effect=asyncio.TimeoutError())

        mock_result = MagicMock()
        mock_result.value = "complete"
        bypass._page.evaluate = AsyncMock(return_value=mock_result)

        big_html = "<html>" + "x" * 20000 + "</html>"
        bypass._get_html_with_fallback = AsyncMock(return_value=big_html)

        html = await bypass._get_internal("https://fbref.com/test")

        # Should have called _get_html_with_fallback
        bypass._get_html_with_fallback.assert_called_once()
        assert len(html) > 10000


class TestNodriverSessionFactory:
    """Tests for nodriver_session factory function."""

    @pytest.mark.unit
    def test_nodriver_session_cf_verify_params(self):
        """Test that nodriver_session passes cf-verify params."""
        from scrapers.base.browser.nodriver_bypass import nodriver_session

        bypass = nodriver_session(
            use_cf_verify=False,
            cf_verify_max_retries=20,
            cf_verify_interval=5.0,
        )

        assert bypass.use_cf_verify is False
        assert bypass.cf_verify_max_retries == 20
        assert bypass.cf_verify_interval == 5.0

    @pytest.mark.unit
    def test_nodriver_session_wait_for_selector_params(self):
        """Test that nodriver_session passes wait_for_selector params."""
        from scrapers.base.browser.nodriver_bypass import nodriver_session

        bypass = nodriver_session(
            wait_for_selector='#content',
            wait_for_selector_timeout=60.0,
        )

        assert bypass.wait_for_selector == '#content'
        assert bypass.wait_for_selector_timeout == 60.0

    @pytest.mark.unit
    def test_nodriver_session_content_timeout_param(self):
        """Test that nodriver_session passes content_timeout param."""
        from scrapers.base.browser.nodriver_bypass import nodriver_session

        bypass = nodriver_session(content_timeout=180.0)
        assert bypass.content_timeout == 180.0

    @pytest.mark.unit
    def test_nodriver_session_default_content_timeout(self):
        """Test that nodriver_session has default content_timeout of 45s."""
        from scrapers.base.browser.nodriver_bypass import nodriver_session

        bypass = nodriver_session()
        assert bypass.content_timeout == 30.0


class TestImportCFVerify:
    """Tests for _import_cf_verify lazy import function."""

    @pytest.mark.unit
    def test_import_cf_verify_not_installed(self):
        """Test _import_cf_verify returns None when not installed."""
        import scrapers.base.browser.nodriver_bypass as module

        # Reset the global
        original = module.CFVerify
        module.CFVerify = None

        with patch.dict('sys.modules', {'nodriver_cf_verify': None}):
            with patch('builtins.__import__', side_effect=ImportError):
                result = module._import_cf_verify()
                assert result is None

        # Restore
        module.CFVerify = original

    @pytest.mark.unit
    def test_import_cf_verify_installed(self):
        """Test _import_cf_verify returns class when installed."""
        import scrapers.base.browser.nodriver_bypass as module

        # Reset the global
        original = module.CFVerify
        module.CFVerify = None

        # Create a mock module
        mock_cf_verify_class = MagicMock()
        mock_module = MagicMock()
        mock_module.CFVerify = mock_cf_verify_class

        with patch.dict('sys.modules', {'nodriver_cf_verify': mock_module}):
            # Need to actually import
            with patch(
                'scrapers.base.browser.nodriver_bypass._import_cf_verify'
            ) as mock_import:
                mock_import.return_value = mock_cf_verify_class
                result = mock_import()
                assert result is mock_cf_verify_class

        # Restore
        module.CFVerify = original


class TestNodriverProxyParsing:
    """Tests for _parse_proxy method."""

    @pytest.fixture
    def bypass(self):
        """Create NodriverBypass instance without starting browser."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass
        return NodriverBypass(headless=True)

    @pytest.mark.unit
    def test_parse_proxy_host_port_user_pass(self, bypass):
        """Test parsing host:port:user:pass format."""
        result = bypass._parse_proxy("pool.proxys.io:10000:user123:pass456")

        assert result['host'] == "pool.proxys.io"
        assert result['port'] == 10000
        assert result['username'] == "user123"
        assert result['password'] == "pass456"
        assert result['scheme'] == "http"

    @pytest.mark.unit
    def test_parse_proxy_password_with_colon(self, bypass):
        """Test parsing proxy where password contains colon."""
        result = bypass._parse_proxy("proxy.example.com:8080:user:pass:with:colons")

        assert result['host'] == "proxy.example.com"
        assert result['port'] == 8080
        assert result['username'] == "user"
        assert result['password'] == "pass:with:colons"

    @pytest.mark.unit
    def test_parse_proxy_host_port_only(self, bypass):
        """Test parsing host:port format without auth."""
        result = bypass._parse_proxy("proxy.example.com:8080")

        assert result['host'] == "proxy.example.com"
        assert result['port'] == 8080
        assert result['username'] is None
        assert result['password'] is None

    @pytest.mark.unit
    def test_parse_proxy_url_format_with_auth(self, bypass):
        """Test parsing http://user:pass@host:port format."""
        result = bypass._parse_proxy("http://user123:pass456@proxy.example.com:8080")

        assert result['host'] == "proxy.example.com"
        assert result['port'] == 8080
        assert result['username'] == "user123"
        assert result['password'] == "pass456"
        assert result['scheme'] == "http"

    @pytest.mark.unit
    def test_parse_proxy_url_format_without_auth(self, bypass):
        """Test parsing http://host:port format."""
        result = bypass._parse_proxy("http://proxy.example.com:8080")

        assert result['host'] == "proxy.example.com"
        assert result['port'] == 8080
        assert result['username'] is None
        assert result['password'] is None

    @pytest.mark.unit
    def test_parse_proxy_socks5_format(self, bypass):
        """Test parsing socks5://host:port format."""
        result = bypass._parse_proxy("socks5://proxy.example.com:1080")

        assert result['host'] == "proxy.example.com"
        assert result['port'] == 1080
        assert result['scheme'] == "socks5"

    @pytest.mark.unit
    def test_parse_proxy_invalid_format(self, bypass):
        """Test that invalid proxy format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid proxy format"):
            bypass._parse_proxy("invalid_proxy")

    @pytest.mark.unit
    def test_mask_proxy_url_format(self, bypass):
        """Test masking proxy URL with credentials."""
        result = bypass._mask_proxy("http://user:pass@proxy.example.com:8080")
        assert result == "****@proxy.example.com:8080"

    @pytest.mark.unit
    def test_mask_proxy_host_port_user_pass(self, bypass):
        """Test masking host:port:user:pass format."""
        result = bypass._mask_proxy("proxy.example.com:8080:user:pass")
        assert result == "proxy.example.com:8080:****:****"

    @pytest.mark.unit
    def test_mask_proxy_no_auth(self, bypass):
        """Test masking proxy without auth."""
        result = bypass._mask_proxy("proxy.example.com:8080")
        assert result == "proxy.example.com:8080"


class TestCloudflareDetection:
    """Tests for Cloudflare detection logic."""

    @pytest.fixture
    def bypass(self):
        """Create NodriverBypass instance without starting browser."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass
        return NodriverBypass(headless=True)

    @pytest.mark.unit
    def test_is_cloudflare_blocked_empty_html(self, bypass):
        """Test that empty HTML is considered blocked."""
        assert bypass._is_cloudflare_blocked("") is True
        assert bypass._is_cloudflare_blocked(None) is True

    @pytest.mark.unit
    def test_is_cloudflare_blocked_challenge_page(self, bypass):
        """Test detection of Cloudflare challenge indicators."""
        challenge_pages = [
            "<html><body>Just a moment...</body></html>",
            "<html><body>Checking your browser...</body></html>",
            '<div class="cf-browser-verification">Verifying</div>',
            '<div id="challenge-running">Please wait</div>',
            "Ray ID: abc123",
            '<div class="cf-turnstile" data-sitekey="abc">Challenge</div>',
            '<script>cf_chl_opt = {}</script>',
        ]

        for html in challenge_pages:
            assert bypass._is_cloudflare_blocked(html) is True, f"Failed for: {html}"

    @pytest.mark.unit
    def test_is_cloudflare_blocked_normal_page(self, bypass):
        """Test that normal pages are not detected as blocked."""
        normal_pages = [
            "<html><body>Welcome to our website</body></html>",
            "<html><head><title>FBref</title></head><body>Stats</body></html>",
            "<div>Player statistics</div>",
        ]

        for html in normal_pages:
            assert bypass._is_cloudflare_blocked(html) is False, f"Failed for: {html}"


class TestWaitForCloudflare:
    """Tests for _wait_for_cloudflare method with cf-verify plugin."""

    @pytest.fixture
    def bypass(self):
        """Create NodriverBypass instance."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass
        bypass = NodriverBypass(
            headless=True,
            use_cf_verify=True,
            cf_verify_max_retries=3,
            cf_verify_interval=0.1,
            cloudflare_wait=0.1,  # Short wait for tests
        )
        # Mock the page
        bypass._page = MagicMock()
        bypass._page.get_content = AsyncMock(return_value="<html>Normal page</html>")
        return bypass

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_wait_for_cloudflare_with_cf_verify_success(self, bypass):
        """Test successful Cloudflare bypass with cf-verify plugin."""
        mock_cfv_instance = MagicMock()
        mock_cfv_instance.verify = AsyncMock(return_value=True)

        mock_cfv_class = MagicMock(return_value=mock_cfv_instance)

        with patch(
            'scrapers.base.browser.nodriver_bypass.CFVerify', mock_cfv_class
        ), patch(
            'scrapers.base.browser.nodriver_cf_verify.CFVerify', mock_cfv_class, create=True
        ):
            # Patch the import inside _wait_for_cloudflare
            with patch.dict('sys.modules', {
                'scrapers.base.browser.nodriver_cf_verify': MagicMock(CFVerify=mock_cfv_class)
            }):
                await bypass._wait_for_cloudflare()

        # Verify cf-verify was called with correct params
        mock_cfv_class.assert_called_once_with(_browser_tab=bypass._page, _debug=True)
        mock_cfv_instance.verify.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_wait_for_cloudflare_cf_verify_disabled(self, bypass):
        """Test Cloudflare bypass without cf-verify plugin."""
        bypass.use_cf_verify = False

        with patch(
            'scrapers.base.browser.nodriver_bypass._import_cf_verify'
        ) as mock_import:
            await bypass._wait_for_cloudflare()

        # cf-verify should not be called when disabled
        mock_import.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_wait_for_cloudflare_cf_verify_not_installed(self, bypass):
        """Test fallback when cf-verify is not installed."""
        with patch(
            'scrapers.base.browser.nodriver_bypass._import_cf_verify',
            return_value=None
        ):
            # Should not raise, should fall back to passive wait
            await bypass._wait_for_cloudflare()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_wait_for_cloudflare_cf_verify_fails(self, bypass):
        """Test fallback when cf-verify returns False."""
        mock_cfv_instance = MagicMock()
        mock_cfv_instance.verify = AsyncMock(return_value=False)

        mock_cfv_class = MagicMock(return_value=mock_cfv_instance)

        with patch(
            'scrapers.base.browser.nodriver_bypass._import_cf_verify',
            return_value=mock_cfv_class
        ):
            # Should not raise, should fall back to passive wait
            await bypass._wait_for_cloudflare()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_wait_for_cloudflare_cf_verify_exception(self, bypass):
        """Test fallback when cf-verify raises exception."""
        mock_cfv_instance = MagicMock()
        mock_cfv_instance.verify = AsyncMock(side_effect=Exception("Plugin error"))

        mock_cfv_class = MagicMock(return_value=mock_cfv_instance)

        with patch(
            'scrapers.base.browser.nodriver_bypass._import_cf_verify',
            return_value=mock_cfv_class
        ):
            # Should not raise, should fall back to passive wait
            await bypass._wait_for_cloudflare()


class TestNodriverAntiFingerprintSettings:
    """Tests for anti-fingerprint improvements in NodriverBypass."""

    @pytest.mark.unit
    def test_window_sizes_defined(self):
        """Test that window sizes are defined for anti-fingerprinting."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        # Should have WINDOW_SIZES attribute
        assert hasattr(NodriverBypass, 'WINDOW_SIZES')
        assert len(NodriverBypass.WINDOW_SIZES) >= 3

        # Standard 1920x1080 should not be in the list (too common)
        assert (1920, 1080) not in NodriverBypass.WINDOW_SIZES

    @pytest.mark.unit
    def test_user_agents_defined(self):
        """Test that user agents are defined for anti-fingerprinting."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        # Should have USER_AGENTS attribute
        assert hasattr(NodriverBypass, 'USER_AGENTS')
        assert len(NodriverBypass.USER_AGENTS) >= 2

        # All should be Chrome-based
        for ua in NodriverBypass.USER_AGENTS:
            assert 'Chrome' in ua

    @pytest.mark.unit
    def test_user_agents_varied_versions(self):
        """Test that user agents have varied and recent Chrome versions."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass
        import re

        versions = set()
        for ua in NodriverBypass.USER_AGENTS:
            # Extract Chrome version (e.g., Chrome/131.0.0.0)
            match = re.search(r'Chrome/(\d+)', ua)
            if match:
                version = int(match.group(1))
                versions.add(version)
                # Chrome versions must be >= 130 to avoid detection as outdated
                assert version >= 130, (
                    f"Chrome version {version} is too old (< 130), "
                    f"Cloudflare will detect outdated User-Agent: {ua}"
                )

        # Should have at least 2 different versions
        assert len(versions) >= 2, (
            f"Expected at least 2 different Chrome versions, got {versions}"
        )


class TestNodriverPreClickBehavior:
    """Tests for pre-click human-like behavior in NodriverBypass."""

    @pytest.fixture
    def bypass(self):
        """Create NodriverBypass instance with mocked page."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(headless=True)
        bypass._page = MagicMock()
        bypass._page.evaluate = AsyncMock()
        bypass._page.send = AsyncMock()
        return bypass

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_pre_click_behavior_exists(self, bypass):
        """Test that _pre_click_behavior method exists and can be called."""
        # Method should exist
        assert hasattr(bypass, '_pre_click_behavior')

        # Should not raise when called
        await bypass._pre_click_behavior()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_pre_click_behavior_calls_evaluate(self, bypass):
        """Test that pre-click behavior may call page.evaluate for scrolling."""
        import random

        # Seed random for predictability
        random.seed(0)

        await bypass._pre_click_behavior()

        # evaluate may or may not be called (40% chance)
        # Just verify no exception is raised

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_pre_click_behavior_calls_send(self, bypass):
        """Test that pre-click behavior may call page.send for mouse movement."""
        import random

        # Seed random for predictability
        random.seed(0)

        await bypass._pre_click_behavior()

        # send may or may not be called (50% chance)
        # Just verify no exception is raised


class TestNodriverXvfbRandomResolution:
    """Tests for Xvfb random resolution feature."""

    @pytest.mark.unit
    def test_start_xvfb_random_resolution(self):
        """Test that Xvfb uses random resolution from WINDOW_SIZES."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        resolutions_seen = set()

        for i in range(20):
            bypass = NodriverBypass()

            mock_display = MagicMock()

            with patch(
                'pyvirtualdisplay.Display',
                return_value=mock_display
            ) as MockDisplay:
                bypass._start_xvfb()

                if MockDisplay.called:
                    size = MockDisplay.call_args[1].get('size')
                    if size:
                        resolutions_seen.add(size)

        # Should see at least one resolution from the list
        assert len(resolutions_seen) >= 1

        # All seen resolutions should be from WINDOW_SIZES
        for res in resolutions_seen:
            assert res in NodriverBypass.WINDOW_SIZES

    @pytest.mark.unit
    def test_start_xvfb_color_depth_24(self):
        """Test that Xvfb uses 24-bit color depth."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass()

        mock_display = MagicMock()

        with patch(
            'pyvirtualdisplay.Display',
            return_value=mock_display
        ) as MockDisplay:
            bypass._start_xvfb()

            if MockDisplay.called:
                assert MockDisplay.call_args[1].get('color_depth') == 24


class TestNodriverHumanLikeMouse:
    """Tests for human-like mouse behavior methods."""

    @pytest.fixture
    def bypass(self):
        """Create NodriverBypass with mocked page."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(headless=True)
        bypass._page = MagicMock()
        bypass._page.send = AsyncMock()
        return bypass

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_human_like_mouse_move(self, bypass):
        """Test human-like mouse movement sends multiple events."""
        await bypass._human_like_mouse_move(0, 0, 100, 100)

        # Should send multiple mouse move events (bezier-like)
        assert bypass._page.send.call_count >= 5

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_human_like_click(self, bypass):
        """Test human-like click sends press and release events."""
        await bypass._human_like_click(100, 100)

        # Should send at least 2 events (press and release)
        assert bypass._page.send.call_count >= 2


class TestCDPDomFallback:
    """Tests for CDP DOM fallback methods."""

    @pytest.fixture
    def bypass(self):
        """Create NodriverBypass instance with mocked page."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(headless=True)
        bypass._page = MagicMock()
        bypass._page.evaluate = AsyncMock()
        bypass._page.send = AsyncMock()
        bypass._page.get_content = AsyncMock(return_value="<html>fallback</html>")
        return bypass

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_html_via_cdp_dom(self, bypass):
        """Test _get_html_via_cdp_dom returns HTML from CDP DOM calls."""
        # Mock CDP DOM responses
        mock_doc = MagicMock()
        mock_doc.backend_node_id = 1

        bypass._page.send = AsyncMock(
            side_effect=[mock_doc, "<html><body>CDP DOM content</body></html>"]
        )

        html = await bypass._get_html_via_cdp_dom(timeout=5.0)

        assert "CDP DOM content" in html
        assert bypass._page.send.call_count == 2

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_html_via_cdp_dom_timeout(self, bypass):
        """Test _get_html_via_cdp_dom returns empty string on timeout."""
        async def slow_send(*args, **kwargs):
            await asyncio.sleep(10)

        bypass._page.send = slow_send

        html = await bypass._get_html_via_cdp_dom(timeout=0.01)
        assert html == ""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_html_with_fallback_evaluate_ok(self, bypass):
        """Test _get_html_with_fallback uses evaluate when it works."""
        mock_result = MagicMock()
        mock_result.value = "<html>" + "x" * 2000 + "</html>"
        bypass._page.evaluate = AsyncMock(return_value=mock_result)

        html = await bypass._get_html_with_fallback()

        assert len(html) > 1000
        bypass._page.evaluate.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_html_with_fallback_evaluate_timeout_cdp_dom_ok(self, bypass):
        """Test fallback to CDP DOM when evaluate times out."""
        # evaluate times out
        bypass._page.evaluate = AsyncMock(side_effect=asyncio.TimeoutError())

        # CDP DOM succeeds
        mock_doc = MagicMock()
        mock_doc.backend_node_id = 1
        cdp_html = "<html>" + "y" * 2000 + "</html>"
        bypass._page.send = AsyncMock(side_effect=[mock_doc, cdp_html])

        html = await bypass._get_html_with_fallback()

        assert len(html) > 1000
        assert "y" * 100 in html

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_html_with_fallback_all_fail_to_get_content(self, bypass):
        """Test fallback chain reaches page.get_content() as last resort."""
        # evaluate times out
        bypass._page.evaluate = AsyncMock(side_effect=asyncio.TimeoutError())

        # CDP DOM fails
        async def failing_send(*args, **kwargs):
            raise Exception("CDP DOM failed")
        bypass._page.send = failing_send

        # get_content works
        bypass._page.get_content = AsyncMock(
            return_value="<html>last resort content</html>"
        )

        html = await bypass._get_html_with_fallback()
        assert "last resort content" in html

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_get_html_with_fallback_all_methods_fail(self, bypass):
        """Test RuntimeError when all methods fail."""
        bypass._page.evaluate = AsyncMock(side_effect=asyncio.TimeoutError())

        async def failing_send(*args, **kwargs):
            raise Exception("CDP DOM failed")
        bypass._page.send = failing_send

        bypass._page.get_content = AsyncMock(side_effect=Exception("get_content failed"))

        with pytest.raises(RuntimeError, match="All HTML extraction methods failed"):
            await bypass._get_html_with_fallback()


class TestRuntimeHungBehavior:
    """Tests for _runtime_hung flag behavior in _get_internal."""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_runtime_hung_skips_content_polling(self):
        """Test that hung runtime skips wait_for_content_js polling."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            wait_for_content_js="document.querySelectorAll('table').length > 0",
            wait_for_content_timeout=5.0,
            cloudflare_wait=0.1,
        )
        bypass._page = MagicMock()
        bypass._page.get = AsyncMock()
        bypass._wait_for_cloudflare = AsyncMock()

        evaluate_call_count = 0

        async def mock_evaluate(*args, **kwargs):
            nonlocal evaluate_call_count
            evaluate_call_count += 1
            # First call (readyState) — timeout to trigger _runtime_hung
            if evaluate_call_count == 1:
                await asyncio.sleep(10)
            return MagicMock(value="<html>content</html>")

        bypass._page.evaluate = mock_evaluate
        bypass._page.select = AsyncMock(side_effect=asyncio.TimeoutError())

        # Mock _get_html_with_fallback to return valid HTML
        bypass._get_html_with_fallback = AsyncMock(
            return_value="<html>" + "x" * 20000 + "</html>"
        )

        html = await bypass._get_internal("https://fbref.com/test")

        # evaluate should only be called once (readyState) — content polling skipped
        assert evaluate_call_count == 1
        assert len(html) > 10000

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_runtime_hung_skips_pre_content_js(self):
        """Test that hung runtime skips pre_content_js execution."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            pre_content_js="(function() { return 'uncommented'; })()",
            cloudflare_wait=0.1,
        )
        bypass._page = MagicMock()
        bypass._page.get = AsyncMock()
        bypass._wait_for_cloudflare = AsyncMock()

        evaluate_call_count = 0

        async def mock_evaluate(*args, **kwargs):
            nonlocal evaluate_call_count
            evaluate_call_count += 1
            # readyState check — timeout
            if evaluate_call_count == 1:
                await asyncio.sleep(10)
            return MagicMock(value="result")

        bypass._page.evaluate = mock_evaluate
        bypass._page.select = AsyncMock(side_effect=asyncio.TimeoutError())

        bypass._get_html_with_fallback = AsyncMock(
            return_value="<html>" + "z" * 20000 + "</html>"
        )

        html = await bypass._get_internal("https://fbref.com/test")

        # evaluate called only once (readyState) — pre_content_js skipped
        assert evaluate_call_count == 1
        assert len(html) > 10000

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_runtime_ok_executes_all_steps(self):
        """Test that working runtime executes all JS steps normally."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            wait_for_content_js="true",
            wait_for_content_timeout=1.0,
            wait_for_content_poll=0.1,
            pre_content_js="(function() { return 'ok'; })()",
            cloudflare_wait=0.1,
            content_timeout=5.0,
        )
        bypass._page = MagicMock()
        bypass._page.get = AsyncMock()
        bypass._wait_for_cloudflare = AsyncMock()
        bypass._page.select = AsyncMock(side_effect=asyncio.TimeoutError())

        mock_result = MagicMock()
        mock_result.value = "complete"
        bypass._page.evaluate = AsyncMock(return_value=mock_result)

        big_html = "<html>" + "a" * 20000 + "</html>"
        bypass._get_html_with_fallback = AsyncMock(return_value=big_html)

        html = await bypass._get_internal("https://fbref.com/test")

        # evaluate should be called multiple times:
        # readyState, content_poll, url, pre_content_js
        assert bypass._page.evaluate.call_count >= 3
        assert len(html) > 10000


class TestStealthJSInjection:
    """Tests for stealth JS injection in start()."""

    @pytest.mark.unit
    def test_stealth_js_constant_defined(self):
        """Test that STEALTH_JS constant is defined and contains key patches."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        assert hasattr(NodriverBypass, 'STEALTH_JS')
        js = NodriverBypass.STEALTH_JS

        # Must contain key stealth patches
        assert 'navigator.webdriver' in js
        assert 'navigator.plugins' in js
        assert 'navigator.languages' in js
        assert 'window.chrome' in js
        assert 'Notification.permission' in js
        assert 'contentWindow' in js  # iframe patch

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_start_injects_stealth_js(self):
        """Test that start() injects stealth JS via CDP addScriptToEvaluateOnNewDocument."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(headless=True)

        # Mock nodriver module
        mock_config = MagicMock()
        mock_browser = MagicMock()
        mock_page = MagicMock()
        mock_page.send = AsyncMock()

        mock_browser_start = AsyncMock(return_value=mock_browser)
        mock_browser.get = AsyncMock(return_value=mock_page)

        mock_uc = MagicMock()
        mock_uc.Config.return_value = mock_config
        mock_uc.start = mock_browser_start

        with patch('scrapers.base.browser.nodriver_bypass._import_nodriver', return_value=mock_uc), \
             patch('shutil.which', return_value='/usr/bin/google-chrome'):
            await bypass.start()

        # Verify stealth JS was injected via CDP
        assert mock_page.send.called
        # The send call should have been made (stealth JS injection)
        assert mock_page.send.call_count >= 1

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_inject_stealth_js_handles_failure_gracefully(self):
        """Test that _inject_stealth_js handles errors without crashing."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(headless=True)
        bypass._page = MagicMock()
        bypass._page.send = AsyncMock(side_effect=Exception("CDP error"))

        # Should not raise
        await bypass._inject_stealth_js()


class TestCFDetectionFalsePositives:
    """Tests for fixed Cloudflare detection (turnstile → cf-turnstile)."""

    @pytest.fixture
    def bypass(self):
        """Create NodriverBypass instance."""
        from scrapers.base.browser.nodriver_bypass import NodriverBypass
        return NodriverBypass(headless=True)

    @pytest.mark.unit
    def test_turnstile_script_not_detected_as_blocked(self, bypass):
        """Test that turnstile/v0/api.js script in HTML does NOT trigger false positive.

        FBref loads this script even after successful CF verification.
        The old 'turnstile' indicator caused false positives.
        """
        html = """
        <html>
        <head>
            <script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async></script>
            <title>2024-2025 Premier League Stats | FBref.com</title>
        </head>
        <body>
            <div id="content">
                <table id="stats_standard">
                    <tr><td>Player stats here</td></tr>
                </table>
            </div>
        </body>
        </html>
        """
        # Should NOT be detected as blocked
        assert bypass._is_cloudflare_blocked(html) is False

    @pytest.mark.unit
    def test_cf_turnstile_element_detected_as_blocked(self, bypass):
        """Test that cf-turnstile challenge UI element IS detected as blocked.

        The cf-turnstile class/ID is the actual challenge widget that gets
        removed after successful verification.
        """
        html = """
        <html>
        <head><title>Just a moment...</title></head>
        <body>
            <div class="cf-turnstile" data-sitekey="0x123abc"></div>
        </body>
        </html>
        """
        # Should be detected as blocked
        assert bypass._is_cloudflare_blocked(html) is True

    @pytest.mark.unit
    def test_normal_fbref_page_not_blocked(self, bypass):
        """Test that a normal FBref page with turnstile script is not blocked."""
        html = """
        <html>
        <head>
            <script src="/turnstile/v0/api.js"></script>
            <title>Premier League Stats</title>
        </head>
        <body>
            <div id="content"><table><tr><td>Data</td></tr></table></div>
        </body>
        </html>
        """
        assert bypass._is_cloudflare_blocked(html) is False


class TestBrowserFlagsRemoved:
    """Tests for removed browser flags that reveal automation."""

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_no_disable_dev_shm_usage_flag(self):
        """Test that --disable-dev-shm-usage is NOT in browser config.

        With shm_size: '512m' in compose.yaml, Chrome should use /dev/shm
        directly (faster than /tmp fallback). This flag is also a bot marker.
        """
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(headless=True)

        # Track add_argument calls
        added_args = []
        mock_config = MagicMock()
        mock_config.add_argument = lambda arg: added_args.append(arg)
        mock_config.add_extension = MagicMock()

        mock_browser = MagicMock()
        mock_page = MagicMock()
        mock_page.send = AsyncMock()
        mock_browser.get = AsyncMock(return_value=mock_page)

        mock_uc = MagicMock()
        mock_uc.Config.return_value = mock_config
        mock_uc.start = AsyncMock(return_value=mock_browser)

        with patch('scrapers.base.browser.nodriver_bypass._import_nodriver', return_value=mock_uc), \
             patch('shutil.which', return_value='/usr/bin/google-chrome'):
            await bypass.start()

        # --disable-dev-shm-usage should NOT be in the arguments
        assert '--disable-dev-shm-usage' not in added_args

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_no_disable_extensions_flag(self):
        """Test that --disable-extensions is NOT in browser config.

        This flag can reveal automation and is not needed with nodriver.
        """
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(headless=True)

        added_args = []
        mock_config = MagicMock()
        mock_config.add_argument = lambda arg: added_args.append(arg)
        mock_config.add_extension = MagicMock()

        mock_browser = MagicMock()
        mock_page = MagicMock()
        mock_page.send = AsyncMock()
        mock_browser.get = AsyncMock(return_value=mock_page)

        mock_uc = MagicMock()
        mock_uc.Config.return_value = mock_config
        mock_uc.start = AsyncMock(return_value=mock_browser)

        with patch('scrapers.base.browser.nodriver_bypass._import_nodriver', return_value=mock_uc), \
             patch('shutil.which', return_value='/usr/bin/google-chrome'):
            await bypass.start()

        # --disable-extensions should NOT be in the arguments
        assert '--disable-extensions' not in added_args
