"""
DrissionPage Cloudflare Bypass
==============================

Cloudflare bypass using DrissionPage - a browser automation library
that does NOT use WebDriver protocol.

Key advantage over Selenium/nodriver:
- No WebDriver signature (navigator.webdriver = undefined)
- Uses CDP (Chrome DevTools Protocol) directly
- Cloudflare cannot detect it via standard WebDriver checks

Usage:
    bypass = DrissionPageBypass(proxy='host:port:user:pass')
    html = bypass.get_page('https://fbref.com/en/')

Requirements:
    - Python 3.9+
    - DrissionPage>=4.1.0
    - Chrome/Chromium browser installed
    - Xvfb for headless mode in Docker
"""

import gc
import logging
import os
import random
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Lazy import to avoid errors when DrissionPage is not installed
ChromiumPage = None
ChromiumOptions = None


def _import_drissionpage():
    """Lazy import DrissionPage to allow graceful degradation."""
    global ChromiumPage, ChromiumOptions
    if ChromiumPage is None:
        try:
            from DrissionPage import ChromiumPage as _ChromiumPage
            from DrissionPage import ChromiumOptions as _ChromiumOptions
            ChromiumPage = _ChromiumPage
            ChromiumOptions = _ChromiumOptions
        except ImportError as e:
            raise ImportError(
                "DrissionPage is not installed. Install with: pip install DrissionPage>=4.1.0"
            ) from e
    return ChromiumPage, ChromiumOptions


class DrissionPageBypass:
    """
    Cloudflare bypass using DrissionPage (no WebDriver signature).

    DrissionPage controls Chrome via CDP without the WebDriver protocol,
    which is the primary method Cloudflare uses to detect automation.

    Attributes:
        proxy: Proxy string (format: host:port:user:pass or host:port)
        cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
        headless: Run browser in headless mode (requires Xvfb in Docker)
    """

    # Window sizes for rotation (non-standard to avoid fingerprinting)
    WINDOW_SIZES = [
        (1366, 768),   # Common laptop
        (1536, 864),   # Scaled laptop
        (1440, 900),   # MacBook
        (1280, 800),   # Smaller laptop
        (1600, 900),   # Wide laptop
    ]

    def __init__(
        self,
        proxy: Optional[str] = None,
        cloudflare_wait: float = 30.0,
        headless: bool = False,
        use_xvfb: bool = True,
        page_load_timeout: float = 60.0,
    ):
        """
        Initialize DrissionPageBypass.

        Args:
            proxy: Proxy string (host:port:user:pass or host:port)
            cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
            headless: Run browser in headless mode
            use_xvfb: Use Xvfb virtual display (recommended for Docker)
            page_load_timeout: Maximum time to wait for page load (seconds)
        """
        self.proxy = proxy
        self.cloudflare_wait = cloudflare_wait
        self.headless = headless
        self.use_xvfb = use_xvfb
        self.page_load_timeout = page_load_timeout

        self._page = None
        self._xvfb_display = None

    def _start_xvfb(self):
        """Start Xvfb virtual display for headless mode."""
        if self._xvfb_display is not None:
            return

        try:
            from pyvirtualdisplay import Display

            # Random resolution to avoid fingerprinting
            width, height = random.choice(self.WINDOW_SIZES)

            self._xvfb_display = Display(
                visible=False,
                size=(width, height),
                color_depth=24,
            )
            self._xvfb_display.start()

            # Важно: установить DISPLAY для DrissionPage
            os.environ['DISPLAY'] = self._xvfb_display.new_display_var
            logger.info(f"Started Xvfb display: {width}x{height}, DISPLAY={os.environ['DISPLAY']}")
        except ImportError:
            logger.warning(
                "pyvirtualdisplay not available. Install with: pip install pyvirtualdisplay"
            )
        except Exception as e:
            logger.warning(f"Failed to start Xvfb: {e}")

    def _stop_xvfb(self):
        """Stop Xvfb virtual display."""
        if self._xvfb_display is not None:
            try:
                self._xvfb_display.stop()
                logger.debug("Stopped Xvfb display")
            except Exception as e:
                logger.warning(f"Error stopping Xvfb: {e}")
            finally:
                self._xvfb_display = None

    def _create_options(self):
        """Create ChromiumOptions with anti-fingerprint settings."""
        import shutil
        _ChromiumPage, _ChromiumOptions = _import_drissionpage()

        co = _ChromiumOptions()

        # Явно указать путь к браузеру (как в nodriver)
        browser_found = False
        for browser_name in ['google-chrome', 'chromium', 'chromium-browser', 'chrome']:
            browser_path = shutil.which(browser_name)
            if browser_path:
                co.set_browser_path(browser_path)
                logger.info(f"DrissionPage using browser: {browser_path}")
                browser_found = True
                break

        if not browser_found:
            logger.warning("No browser found in PATH for DrissionPage, will use default")

        # Headless mode
        if self.headless:
            co.headless(True)

        # Random window size
        width, height = random.choice(self.WINDOW_SIZES)
        co.set_argument(f'--window-size={width},{height}')

        # Anti-fingerprint arguments
        co.set_argument('--disable-blink-features=AutomationControlled')
        co.set_argument('--disable-infobars')
        co.set_argument('--disable-extensions')
        co.set_argument('--no-first-run')
        co.set_argument('--no-default-browser-check')

        # Do NOT override User-Agent — let the real Chromium 120 use its native UA.
        # Faking Chrome 131-133 UA on a Chromium 120 binary creates a version /
        # JA3/JA4 mismatch that Cloudflare catches. (#469)

        # Language settings (human-like)
        co.set_argument('--lang=en-US,en,ru-RU,ru')

        # Memory optimization for Docker (CRITICAL for OOM prevention)
        # Note: --single-process removed — Cloudflare detects it as bot marker
        co.set_argument('--disable-dev-shm-usage')
        co.set_argument('--no-sandbox')
        co.set_argument('--disable-background-networking')
        co.set_argument('--disable-sync')
        co.set_argument('--disable-translate')
        co.set_argument('--js-flags=--max-old-space-size=512')
        co.set_argument('--disable-background-timer-throttling')

        # Note: --disable-gpu, --renderer-process-limit=1 and --disable-software-rasterizer
        # intentionally NOT set — they create a unique browser fingerprint that Cloudflare
        # detects as a bot marker. WebGL fingerprinting needs GPU/SwiftShader enabled to
        # produce a "real" WebGL context (null context = bot tell). (#469)

        # Software WebGL via ANGLE (#574): with the mesa GL stack in the image, these
        # flags make Chromium 120 expose a real WebGL context under Xvfb (renderer
        # Mesa/llvmpipe) instead of null. --use-angle=swiftshader is the only ANGLE
        # backend that initializes here; --use-angle=gl fails EGL/X init → null context.
        co.set_argument('--use-gl=angle')
        co.set_argument('--use-angle=swiftshader')

        # Proxy configuration
        if self.proxy:
            proxy_info = self._parse_proxy(self.proxy)
            if proxy_info.get('username'):
                # DrissionPage does NOT support proxy with auth via --proxy-server
                # Using proxy without auth will result in "Proxy Authentication Required"
                # Skip proxy entirely - go direct without proxy
                logger.warning(
                    "DrissionPage: proxy requires auth but auth not supported, "
                    "proceeding WITHOUT proxy (direct connection)"
                )
                # Do NOT set --proxy-server when auth is required
            else:
                co.set_argument(f"--proxy-server={proxy_info['host']}:{proxy_info['port']}")
                logger.info(f"DrissionPage using proxy: {proxy_info['host']}:{proxy_info['port']}")

        return co

    def _parse_proxy(self, proxy: str) -> dict:
        """
        Parse proxy string into components.

        Supports formats:
        - host:port:user:pass (residential proxy format)
        - host:port (no auth)
        """
        parts = proxy.split(':')
        if len(parts) >= 4:
            return {
                'host': parts[0],
                'port': int(parts[1]),
                'username': parts[2],
                'password': ':'.join(parts[3:]),
            }
        elif len(parts) == 2:
            return {
                'host': parts[0],
                'port': int(parts[1]),
                'username': None,
                'password': None,
            }
        raise ValueError(f"Invalid proxy format: {proxy}")

    def _is_cloudflare_blocked(self, html: str) -> bool:
        """Check if page contains Cloudflare challenge."""
        if not html:
            return True

        cloudflare_indicators = [
            "just a moment",
            "checking your browser",
            "cf-browser-verification",
            "challenge-running",
            "ray id",
            "cf-turnstile",  # Class/ID of challenge UI element (removed after verification)
            "cf_chl_opt",
        ]

        html_lower = html.lower()
        return any(indicator in html_lower for indicator in cloudflare_indicators)

    def _human_like_delay(self, min_sec: float = 0.5, max_sec: float = 2.0):
        """Add human-like random delay."""
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)

    def _simulate_human_behavior(self, page):
        """Simulate human-like behavior on page."""
        try:
            # Random scroll
            if random.random() < 0.4:
                scroll_amount = random.randint(-100, 200)
                page.scroll.down(scroll_amount)
                self._human_like_delay(0.3, 0.8)

            # Random mouse movement (if supported)
            # DrissionPage moves mouse implicitly during interactions

            # "Thinking" pause
            self._human_like_delay(1.0, 3.0)

        except Exception as e:
            logger.debug(f"Error in human simulation: {e}")

    def _wait_for_cloudflare(self, page) -> bool:
        """
        Wait for Cloudflare challenge to complete.

        Returns:
            True if challenge passed, False if still blocked
        """
        logger.info("Waiting for Cloudflare challenge...")

        start_time = time.time()
        check_interval = 3.0

        while time.time() - start_time < self.cloudflare_wait:
            html = page.html
            if not self._is_cloudflare_blocked(html):
                elapsed = time.time() - start_time
                logger.info(f"Cloudflare challenge passed after {elapsed:.1f}s")
                return True

            # Try to find and click Turnstile checkbox
            try:
                # Look for Turnstile iframe
                iframes = page.eles('tag:iframe')
                for iframe in iframes:
                    src = iframe.attr('src') or ''
                    if 'challenges.cloudflare.com' in src or 'turnstile' in src.lower():
                        logger.info("Found Turnstile iframe, attempting click...")

                        # Simulate human behavior before clicking
                        self._simulate_human_behavior(page)

                        # Try to click the iframe (checkbox is inside)
                        try:
                            iframe.click()
                            self._human_like_delay(2.0, 4.0)
                        except Exception as e:
                            logger.debug(f"Could not click iframe: {e}")

                        break
            except Exception as e:
                logger.debug(f"Error finding Turnstile: {e}")

            elapsed = time.time() - start_time
            logger.debug(f"Cloudflare challenge in progress ({elapsed:.1f}s)...")
            time.sleep(check_interval)

        logger.warning(f"Cloudflare challenge timeout after {self.cloudflare_wait}s")
        return False

    def get_page(self, url: str) -> Optional[str]:
        """
        Fetch page HTML with Cloudflare bypass.

        Args:
            url: URL to fetch

        Returns:
            Page HTML or None if failed
        """
        _ChromiumPage, _ChromiumOptions = _import_drissionpage()
        page = None

        try:
            # Start Xvfb if needed
            if self.use_xvfb and not self.headless:
                self._start_xvfb()

            # Create browser options
            options = self._create_options()

            # Create page
            logger.info(f"DrissionPage navigating to: {url}")
            page = _ChromiumPage(options)

            # Set timeouts
            page.set.timeouts(
                base=self.page_load_timeout,
                page_load=self.page_load_timeout,
                script=30
            )

            # Navigate to URL
            page.get(url)

            # Wait for page to load
            page.wait.load_start()

            # Check for Cloudflare
            html = page.html
            if self._is_cloudflare_blocked(html):
                # Wait for challenge
                passed = self._wait_for_cloudflare(page)
                if not passed:
                    logger.warning("DrissionPage: Cloudflare challenge not passed")
                    return None

                # Get updated HTML after challenge
                html = page.html

            # Verify we have valid content
            if not html or len(html) < 1000:
                logger.warning(f"DrissionPage: Page content too small ({len(html) if html else 0} bytes)")
                return None

            logger.info(f"DrissionPage: Successfully fetched {len(html)} bytes")
            return html

        except Exception as e:
            logger.error(
                f"DrissionPage error ({type(e).__name__}): {e}",
                exc_info=True  # Traceback для отладки
            )
            return None

        finally:
            # CRITICAL: Always close browser and cleanup
            if page is not None:
                try:
                    page.quit()
                    logger.debug("DrissionPage browser closed")
                except Exception as e:
                    logger.warning(f"Error closing DrissionPage browser: {e}")

            # Stop Xvfb
            self._stop_xvfb()

            # Aggressive garbage collection
            gc.collect()
            gc.collect()

    def close(self):
        """Cleanup resources."""
        if self._page is not None:
            try:
                self._page.quit()
            except Exception:
                pass
            finally:
                self._page = None

        self._stop_xvfb()
        gc.collect()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False


def drissionpage_session(
    proxy: Optional[str] = None,
    cloudflare_wait: float = 30.0,
    headless: bool = False,
    use_xvfb: bool = True,
    **kwargs
) -> DrissionPageBypass:
    """
    Factory function for creating DrissionPageBypass instances.

    Usage:
        with drissionpage_session(proxy='host:port:user:pass') as bypass:
            html = bypass.get_page('https://fbref.com/en/')

    Args:
        proxy: Proxy string (host:port:user:pass or host:port)
        cloudflare_wait: Time to wait for Cloudflare challenge
        headless: Run browser in headless mode
        use_xvfb: Use Xvfb virtual display
        **kwargs: Additional arguments

    Returns:
        DrissionPageBypass instance
    """
    return DrissionPageBypass(
        proxy=proxy,
        cloudflare_wait=cloudflare_wait,
        headless=headless,
        use_xvfb=use_xvfb,
        **kwargs
    )
