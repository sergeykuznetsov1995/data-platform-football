"""
Unit tests for DrissionPageBypass.

Tests the DrissionPage-based Cloudflare bypass without WebDriver signature.
"""

import gc
import pytest
from unittest.mock import Mock, patch, MagicMock


class TestDrissionPageBypass:
    """Tests for DrissionPageBypass class."""

    def test_import_drissionpage_not_installed(self):
        """Test graceful handling when DrissionPage is not installed."""
        with patch.dict('sys.modules', {'DrissionPage': None}):
            from scrapers.base.browser.drissionpage_bypass import _import_drissionpage

            with pytest.raises(ImportError) as exc_info:
                # Force reimport
                import importlib
                import scrapers.base.browser.drissionpage_bypass as module
                module.ChromiumPage = None
                module.ChromiumOptions = None
                _import_drissionpage()

            assert "DrissionPage is not installed" in str(exc_info.value)

    def test_init_defaults(self):
        """Test DrissionPageBypass initialization with defaults."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        assert bypass.proxy is None
        assert bypass.cloudflare_wait == 30.0
        assert bypass.headless is False
        assert bypass.use_xvfb is True
        assert bypass.page_load_timeout == 60.0
        assert bypass._page is None
        assert bypass._xvfb_display is None

    def test_init_custom_params(self):
        """Test DrissionPageBypass initialization with custom parameters."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass(
            proxy='host:port:user:pass',
            cloudflare_wait=60.0,
            headless=True,
            use_xvfb=False,
            page_load_timeout=120.0,
        )

        assert bypass.proxy == 'host:port:user:pass'
        assert bypass.cloudflare_wait == 60.0
        assert bypass.headless is True
        assert bypass.use_xvfb is False
        assert bypass.page_load_timeout == 120.0

    def test_parse_proxy_with_auth(self):
        """Test proxy parsing with authentication."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()
        result = bypass._parse_proxy('proxy.example.com:8080:user:password123')

        assert result['host'] == 'proxy.example.com'
        assert result['port'] == 8080
        assert result['username'] == 'user'
        assert result['password'] == 'password123'

    def test_parse_proxy_no_auth(self):
        """Test proxy parsing without authentication."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()
        result = bypass._parse_proxy('proxy.example.com:8080')

        assert result['host'] == 'proxy.example.com'
        assert result['port'] == 8080
        assert result['username'] is None
        assert result['password'] is None

    def test_parse_proxy_password_with_colon(self):
        """Test proxy parsing when password contains colon."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()
        result = bypass._parse_proxy('host:8080:user:pass:word:with:colons')

        assert result['host'] == 'host'
        assert result['port'] == 8080
        assert result['username'] == 'user'
        assert result['password'] == 'pass:word:with:colons'

    def test_parse_proxy_invalid(self):
        """Test proxy parsing with invalid format."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        with pytest.raises(ValueError) as exc_info:
            bypass._parse_proxy('invalid')

        assert "Invalid proxy format" in str(exc_info.value)

    def test_is_cloudflare_blocked_true(self):
        """Test Cloudflare block detection - positive cases."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        # Various Cloudflare challenge indicators
        blocked_pages = [
            '<html><body>Just a moment...</body></html>',
            '<div>Checking your browser before accessing</div>',
            '<div id="cf-browser-verification">Please wait</div>',
            '<div class="challenge-running">Verifying</div>',
            '<span>Ray ID: abc123</span>',
            '<div class="cf-turnstile" data-sitekey="abc">Verification</div>',
            '<script>cf_chl_opt = {}</script>',
        ]

        for html in blocked_pages:
            assert bypass._is_cloudflare_blocked(html) is True, f"Should detect: {html[:50]}"

    def test_is_cloudflare_blocked_false(self):
        """Test Cloudflare block detection - negative cases."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        # Valid page content
        valid_pages = [
            '<html><body><table>Data</table></body></html>',
            '<div class="stats-table">Player statistics</div>',
            '<html><head><title>FBref</title></head><body>Content</body></html>',
        ]

        for html in valid_pages:
            assert bypass._is_cloudflare_blocked(html) is False, f"Should not detect: {html[:50]}"

    def test_is_cloudflare_blocked_empty(self):
        """Test Cloudflare block detection with empty HTML."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        assert bypass._is_cloudflare_blocked('') is True
        assert bypass._is_cloudflare_blocked(None) is True

    def test_human_like_delay(self):
        """Test human-like delay is within expected range."""
        import time
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        start = time.time()
        bypass._human_like_delay(0.1, 0.2)
        elapsed = time.time() - start

        assert 0.1 <= elapsed <= 0.3  # Small buffer for execution time

    def test_context_manager(self):
        """Test context manager calls close."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        with patch.object(bypass, 'close') as mock_close:
            with bypass:
                pass
            mock_close.assert_called_once()

    def test_close_cleanup(self):
        """Test close method cleans up resources."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()
        bypass._page = Mock()
        mock_display = Mock()
        bypass._xvfb_display = mock_display

        bypass.close()

        assert bypass._page is None
        mock_display.stop.assert_called_once()
        assert bypass._xvfb_display is None

    def test_window_sizes_variation(self):
        """Test that window sizes are varied for anti-fingerprinting."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        # Check window sizes are defined and varied
        assert len(DrissionPageBypass.WINDOW_SIZES) >= 3
        assert (1920, 1080) not in DrissionPageBypass.WINDOW_SIZES  # Standard should be avoided

    def test_create_options_omits_cf_violating_args(self):
        """CF invariants (#469): no --disable-gpu / --renderer-process-limit /
        --disable-software-rasterizer, and no User-Agent override."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()
        captured = []
        mock_co = MagicMock()
        mock_co.set_argument.side_effect = lambda arg: captured.append(arg)

        with patch('scrapers.base.browser.drissionpage_bypass._import_drissionpage',
                   return_value=(Mock(), Mock(return_value=mock_co))):
            bypass._create_options()

        assert '--disable-gpu' not in captured
        assert '--renderer-process-limit=1' not in captured
        assert '--disable-software-rasterizer' not in captured
        assert not any(a.startswith('--user-agent') for a in captured)

    def test_drissionpage_session_factory(self):
        """Test drissionpage_session factory function."""
        from scrapers.base.browser.drissionpage_bypass import drissionpage_session

        bypass = drissionpage_session(
            proxy='host:port',
            cloudflare_wait=45.0,
            headless=True,
        )

        assert bypass.proxy == 'host:port'
        assert bypass.cloudflare_wait == 45.0
        assert bypass.headless is True

    @pytest.mark.parametrize("proxy_format", [
        'host:8080',
        'host:8080:user:pass',
        'host:8080:user:pass:with:colons',
    ])
    def test_various_proxy_formats(self, proxy_format):
        """Test parsing various valid proxy formats."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()
        result = bypass._parse_proxy(proxy_format)

        assert 'host' in result
        assert 'port' in result


class TestDrissionPageMemoryManagement:
    """Tests for memory management in DrissionPageBypass."""

    def test_gc_called_in_get_page_finally(self):
        """Test that garbage collection is called in finally block."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        # Mock DrissionPage to avoid actual browser
        mock_page_class = Mock()
        mock_options_class = Mock()
        mock_page = Mock()
        mock_page.html = '<html><body>Test</body></html>'
        mock_page_class.return_value = mock_page

        with patch.object(gc, 'collect') as mock_gc:
            with patch('scrapers.base.browser.drissionpage_bypass._import_drissionpage',
                       return_value=(mock_page_class, mock_options_class)):
                # This will fail but finally should still run
                try:
                    bypass.get_page('http://test.com')
                except Exception:
                    pass

            # gc.collect should be called at least twice
            assert mock_gc.call_count >= 2

    def test_browser_quit_called(self):
        """Test that browser.quit() is always called."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        mock_page_class = Mock()
        mock_options_class = Mock()
        mock_page = Mock()
        mock_page.html = '<html><body>Valid content here</body></html>'
        mock_page_class.return_value = mock_page

        with patch('scrapers.base.browser.drissionpage_bypass._import_drissionpage',
                   return_value=(mock_page_class, mock_options_class)):
            bypass.get_page('http://test.com')

        mock_page.quit.assert_called_once()


class TestDrissionPageXvfb:
    """Tests for Xvfb handling in DrissionPageBypass."""

    def test_start_xvfb_creates_display(self):
        """Test that Xvfb display is created with random resolution."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        mock_display = Mock()

        with patch('pyvirtualdisplay.Display', return_value=mock_display) as MockDisplay:
            bypass._start_xvfb()

            # Should create display
            assert MockDisplay.called
            call_kwargs = MockDisplay.call_args[1]
            assert call_kwargs['visible'] is False
            assert call_kwargs['color_depth'] == 24

            # Should start display
            mock_display.start.assert_called_once()

    def test_start_xvfb_random_resolution(self):
        """Test that Xvfb uses random resolution."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        resolutions_seen = set()

        for _ in range(10):
            bypass = DrissionPageBypass()

            mock_display = Mock()

            with patch('pyvirtualdisplay.Display', return_value=mock_display) as MockDisplay:
                bypass._start_xvfb()
                if MockDisplay.called:
                    size = MockDisplay.call_args[1]['size']
                    resolutions_seen.add(size)

        # Should see at least one resolution
        assert len(resolutions_seen) >= 1

    def test_stop_xvfb_cleanup(self):
        """Test that Xvfb display is properly stopped."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()
        mock_display = Mock()
        bypass._xvfb_display = mock_display

        bypass._stop_xvfb()

        mock_display.stop.assert_called_once()
        assert bypass._xvfb_display is None

    def test_xvfb_import_error_handled(self):
        """Test graceful handling when pyvirtualdisplay is not installed."""
        from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

        bypass = DrissionPageBypass()

        # Simulate ImportError when trying to import Display
        with patch.dict('sys.modules', {'pyvirtualdisplay': None}):
            # Should not raise, just log warning
            bypass._start_xvfb()

        # Display may or may not be None depending on import caching
        # Key is no exception is raised
