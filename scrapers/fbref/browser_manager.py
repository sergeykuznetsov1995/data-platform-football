"""
FBref Browser Manager Mixin
============================

Browser lifecycle management for FBref scraper.
Handles browser creation, page fetching (nodriver / Selenium / curl),
cache management, and browser restart logic.

Xvfb Display Management:
    Xvfb is managed at the mixin level (_shared_xvfb_display) rather than
    per-NodriverBypass instance. This prevents "filedescriptor out of range
    in select()" errors that occur when Xvfb is repeatedly stopped/restarted
    (each restart accumulates Chrome FDs, eventually exceeding select()'s
    FD_SETSIZE=1024 limit).
"""

import asyncio
import gc
import logging
import os
import random
import time
from typing import Dict, Optional

from scrapers.base.browser import CloudflareBypass
from scrapers.base.browser.nodriver_stealth import WINDOW_SIZES
from scrapers.fbref.constants import FBREF_UNCOMMENT_TABLES_JS

logger = logging.getLogger(__name__)

# Chrome 120 User-Agent (must match container's Chromium for TLS fingerprint)
_CHROME120_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


class FBrefBrowserMixin:
    """
    Mixin providing browser lifecycle and page-fetching methods for FBrefScraper.

    Expects the host class to provide (via SeleniumScraper / BaseScraper):
        - self._proxy_manager
        - self.proxy
        - self.headless
        - self.use_xvfb
        - self.use_nodriver
        - self.nodriver_cloudflare_wait
        - self._browser
        - self._nodriver_browser
        - self._page_cache: Dict[str, str]
        - self._pages_fetched: int
        - self._rate_limiter
        - self._stats
        - self.MAX_PAGES_BEFORE_BROWSER_RESTART
        - self.MAX_CACHE_SIZE
    """

    # Shared Xvfb display (lives across browser restarts)
    _shared_xvfb_display = None

    # ------------------------------------------------------------------
    # Xvfb management (mixin level — survives browser restarts)
    # ------------------------------------------------------------------

    def _ensure_xvfb(self) -> bool:
        """
        Start shared Xvfb display if not already running.

        Xvfb is kept alive across browser restarts to avoid FD exhaustion.
        The display is only stopped in _stop_shared_xvfb() (called from close()).

        Returns:
            True if Xvfb is running, False otherwise.
        """
        if self._shared_xvfb_display is not None:
            return True

        try:
            from pyvirtualdisplay import Display

            width, height = random.choice(WINDOW_SIZES)
            display = Display(visible=False, size=(width, height), color_depth=24)
            display.start()
            FBrefBrowserMixin._shared_xvfb_display = display
            logger.info(f"Started shared Xvfb display: {width}x{height}")
            return True
        except ImportError:
            logger.warning("pyvirtualdisplay not available, using headless mode")
            return False
        except Exception as e:
            logger.warning(f"Failed to start shared Xvfb: {e}, using headless mode")
            return False

    def _stop_shared_xvfb(self) -> None:
        """Stop the shared Xvfb display (only at final cleanup)."""
        if FBrefBrowserMixin._shared_xvfb_display is not None:
            try:
                FBrefBrowserMixin._shared_xvfb_display.stop()
                logger.info("Stopped shared Xvfb display")
            except Exception as e:
                logger.warning(f"Error stopping shared Xvfb: {e}")
            finally:
                FBrefBrowserMixin._shared_xvfb_display = None

    # ------------------------------------------------------------------
    # Proxy helpers
    # ------------------------------------------------------------------

    def _get_proxy_url(self) -> Optional[str]:
        """Get proxy URL from manager or direct proxy setting."""
        if self._proxy_manager and self._proxy_manager.total_count > 0:
            proxy_obj = self._proxy_manager.get_proxy()
            if proxy_obj:
                logger.debug(
                    f"Using proxy for FBref: {proxy_obj.host}:{proxy_obj.port}"
                )
                return proxy_obj.url
        elif self.proxy:
            return self.proxy
        return None

    # ------------------------------------------------------------------
    # Browser creation
    # ------------------------------------------------------------------

    def _get_nodriver_browser(self):
        """Get nodriver browser with FBref-specific configuration.

        Xvfb is managed at the mixin level: we start it once here and pass
        use_xvfb=False to NodriverBypass so it doesn't try to manage its own
        Xvfb (which would cause FD exhaustion on repeated restarts).
        """
        if self._nodriver_browser is None:
            from scrapers.base.browser import get_nodriver_bypass
            NodriverBypass = get_nodriver_bypass()

            proxy_url = self._get_proxy_url()

            # Manage Xvfb at mixin level to avoid FD exhaustion
            xvfb_running = False
            if self.use_xvfb:
                xvfb_running = self._ensure_xvfb()

            self._nodriver_browser = NodriverBypass(
                headless=self.headless if not xvfb_running else False,
                use_xvfb=False,  # Mixin manages Xvfb, not NodriverBypass
                proxy=proxy_url,
                cloudflare_wait=self.nodriver_cloudflare_wait,
                page_load_timeout=20.0,
                max_retries=2,
                use_cf_verify=True,
                pre_content_js=FBREF_UNCOMMENT_TABLES_JS,
                content_timeout=30.0,
                slow_proxy_threshold=15.0,
            )
            logger.debug(
                f"Initialized nodriver browser (headless={self.headless}, "
                f"xvfb={xvfb_running}, "
                f"cloudflare_wait={self.nodriver_cloudflare_wait}s)"
            )
        return self._nodriver_browser

    def _get_browser(self) -> CloudflareBypass:
        """Get browser with FBref-specific configuration and proxy support."""
        if self._browser is None:
            proxy_url = self._get_proxy_url()

            self._browser = CloudflareBypass(
                headless=self.headless,
                use_xvfb=self.use_xvfb,
                proxy=proxy_url,
                page_load_timeout=120,
            )
        return self._browser

    # ------------------------------------------------------------------
    # HTTP session (curl_cffi with CF cookies from nodriver)
    # ------------------------------------------------------------------

    def _extract_cookies_from_nodriver(self) -> dict:
        """Extract all cookies from the running nodriver browser."""
        if self._nodriver_browser is None or self._nodriver_browser._browser is None:
            return {}

        try:
            import asyncio
            loop = self._nodriver_browser._get_or_create_loop()

            # Use wait_for to avoid hanging on pending nodriver tasks
            async def _get_cookies_with_timeout():
                return await asyncio.wait_for(
                    self._nodriver_browser._browser.cookies.get_all(),
                    timeout=10.0,
                )

            all_cookies = loop.run_until_complete(_get_cookies_with_timeout())

            cookies = {}
            for cookie in all_cookies:
                name = cookie.name if hasattr(cookie, 'name') else cookie.get('name', '')
                value = cookie.value if hasattr(cookie, 'value') else cookie.get('value', '')
                if name:
                    cookies[name] = value

            logger.debug(f"Extracted {len(cookies)} cookies from nodriver browser")
            return cookies

        except asyncio.TimeoutError:
            logger.debug("Timeout extracting cookies from nodriver (10s)")
            return {}
        except Exception as e:
            logger.debug(f"Could not extract cookies from nodriver: {e}")
            return {}

    def _create_http_session(self, cookies: dict):
        """Create curl_cffi session with chrome120 impersonation and cookies."""
        from curl_cffi.requests import Session

        session = Session(impersonate='chrome120')
        session.cookies.update(cookies)
        session.headers.update({
            'User-Agent': _CHROME120_UA,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
        })
        return session

    def _try_init_http_session(self) -> None:
        """Try to initialize HTTP session with cookies from nodriver browser."""
        cookies = self._extract_cookies_from_nodriver()
        if not cookies:
            return

        # Need at least cf_clearance for Cloudflare-protected pages
        has_cf = any('cf' in name.lower() for name in cookies)
        if not has_cf:
            logger.debug("No CF cookies found, HTTP session not initialized")
            return

        try:
            self._http_session = self._create_http_session(cookies)
            self._http_cookies_time = time.time()
            self._http_request_count = 0
            logger.info(
                f"HTTP session initialized with {len(cookies)} cookies "
                f"(cf_clearance present)"
            )
        except Exception as e:
            logger.warning(f"Failed to create HTTP session: {e}")
            self._http_session = None

    def _http_cookies_expired(self) -> bool:
        """Check if HTTP session cookies have expired (TTL or request count)."""
        if self._http_cookies_time is None:
            return True
        elapsed_minutes = (time.time() - self._http_cookies_time) / 60.0
        if elapsed_minutes >= self.HTTP_COOKIE_TTL_MINUTES:
            logger.info(f"HTTP cookies expired after {elapsed_minutes:.1f} minutes")
            return True
        if self._http_request_count >= self.HTTP_MAX_REQUESTS:
            logger.info(f"HTTP cookies expired after {self._http_request_count} requests")
            return True
        return False

    def _fetch_page_http(self, url: str) -> Optional[str]:
        """Fetch page via HTTP (curl_cffi) using CF cookies from nodriver."""
        if self._http_session is None:
            return None

        try:
            response = self._http_session.get(url, timeout=30)
            self._http_request_count += 1

            if response.status_code != 200:
                logger.debug(f"HTTP fetch got status {response.status_code} for {url}")
                return None

            html = response.text
            if not html:
                return None

            # Validate: must have tables (real FBref page)
            has_tables = '<table' in html
            has_cloudflare = any(cf in html.lower() for cf in [
                'just a moment', 'checking your browser',
                'cf-browser-verification', 'challenge-running'
            ])

            if has_cloudflare:
                logger.debug(f"HTTP fetch got Cloudflare challenge for {url}")
                return None

            if not has_tables and len(html) < 50000:
                has_comment_tables = '<!--' in html and '<table' in html
                if not has_comment_tables:
                    logger.debug(f"HTTP fetch got incomplete page for {url}")
                    return None

            logger.debug(
                f"HTTP fetch OK: {url} ({len(html)} bytes, "
                f"request #{self._http_request_count})"
            )
            return html

        except Exception as e:
            logger.debug(f"HTTP fetch failed for {url}: {e}")
            return None

    # ------------------------------------------------------------------
    # Page fetching
    # ------------------------------------------------------------------

    def _fetch_page(self, url: str, use_cache: bool = True) -> Optional[str]:
        """
        Fetch page HTML with caching support.

        On SlowProxyError, retries up to MAX_SLOW_PROXY_RETRIES times
        with browser restart (which triggers proxy rotation).

        Args:
            url: URL to fetch
            use_cache: Whether to use page cache

        Returns:
            Page HTML or None
        """
        if use_cache and url in self._page_cache:
            logger.debug(f"Using cached page: {url}")
            return self._page_cache[url]

        from scrapers.base.browser.nodriver_bypass import SlowProxyError

        for slow_retry in range(self.MAX_SLOW_PROXY_RETRIES):
            try:
                # Rate limiting
                self._rate_limiter.acquire()

                # For match pages — try HTTP first (faster, ~0.5s vs ~3-5s)
                if (
                    '/en/matches/' in url
                    and self.use_nodriver
                    and self._http_session is not None
                    and not self._http_cookies_expired()
                ):
                    html = self._fetch_page_http(url)
                    if html:
                        # Skip browser validation — HTTP validation already done
                        if use_cache:
                            self._page_cache[url] = html
                            self._manage_cache_size()
                        self._stats['successes'] += 1
                        self._consecutive_fetch_failures = 0
                        return html
                    else:
                        logger.info("HTTP fetch failed for match page, falling back to nodriver")
                        self._http_session = None  # Reset — cookies likely expired

                # Use nodriver if enabled
                if self.use_nodriver:
                    html = self._fetch_page_nodriver(url)
                else:
                    html = self._fetch_page_selenium(url)

                # Diagnostic logging
                if html:
                    html_len = len(html)
                    has_tables = '<table' in html
                    has_cloudflare = any(cf in html.lower() for cf in [
                        'just a moment', 'checking your browser',
                        'cf-browser-verification', 'challenge-running'
                    ])

                    logger.debug(
                        f"Page fetched: {url} | "
                        f"length={html_len}, has_tables={has_tables}, "
                        f"cloudflare_blocked={has_cloudflare}"
                    )

                    if has_cloudflare:
                        logger.warning(
                            f"Cloudflare challenge detected in response for {url}. "
                            f"HTML preview: {html[:500]}"
                        )
                        # Return None if page is still blocked
                        self._stats['failures'] += 1
                        return None

                    if not has_tables and html_len < 5000:
                        logger.warning(
                            f"Page appears incomplete or blocked: {url}. "
                            f"HTML preview: {html[:500]}"
                        )
                        self._stats['failures'] += 1
                        return None

                    # Detect pages that loaded but have no real content
                    # FBref pages with tables are typically >50KB; pages <50KB
                    # without visible tables should also have comment-embedded tables
                    if not has_tables and html_len < 50000:
                        has_comment_tables = '<!--' in html and '<table' in html
                        if not has_comment_tables:
                            logger.warning(
                                f"Page has no tables and no comment tables: {url}, "
                                f"len={html_len}. Likely incomplete load after browser issue."
                            )
                            self._stats['failures'] += 1
                            return None
                else:
                    logger.warning(f"Empty HTML returned for {url}")
                    self._stats['failures'] += 1
                    return None

                if use_cache:
                    self._page_cache[url] = html
                    self._manage_cache_size()

                self._stats['successes'] += 1
                self._consecutive_fetch_failures = 0
                self._maybe_restart_browser()

                # After successful nodriver fetch — init HTTP session if not yet
                if self.use_nodriver and self._http_session is None:
                    self._try_init_http_session()

                return html

            except SlowProxyError as e:
                self._stats['failures'] += 1
                self._close_browser()
                if slow_retry < self.MAX_SLOW_PROXY_RETRIES - 1:
                    wait = 2 * (slow_retry + 1)
                    logger.warning(
                        f"SlowProxyError ({slow_retry + 1}/{self.MAX_SLOW_PROXY_RETRIES}): "
                        f"{e} — retrying with new proxy in {wait}s"
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        f"All {self.MAX_SLOW_PROXY_RETRIES} proxy attempts failed for {url}"
                    )
                continue

            except Exception as e:
                self._stats['failures'] += 1
                self._consecutive_fetch_failures += 1
                if self._consecutive_fetch_failures >= self.MAX_CONSECUTIVE_FAILURES:
                    logger.warning(
                        f"{self._consecutive_fetch_failures} consecutive fetch failures "
                        f"— restarting browser for proxy rotation"
                    )
                    self._close_browser()
                    self._consecutive_fetch_failures = 0
                logger.error(f"Error fetching page {url}: {e}", exc_info=True)
                return None

        return None

    def _fetch_page_nodriver(self, url: str) -> Optional[str]:
        """Fetch page using nodriver (better Cloudflare bypass)."""
        from scrapers.base.browser.nodriver_bypass import SlowProxyError

        browser = self._get_nodriver_browser()

        logger.debug(f"Fetching page with nodriver: {url}")

        try:
            html = browser.get_page(
                url,
                wait_timeout=30,
                cloudflare_wait=self.nodriver_cloudflare_wait,
            )
        except SlowProxyError as e:
            logger.warning(f"Proxy timeout for {url}: {e} — forcing browser restart")
            self._close_browser()
            raise

        # get_page() already returns fully loaded HTML after Cloudflare bypass
        # No need to call page_source again (it can hang without timeout)
        if html:
            logger.debug(f"HTML received from nodriver, length={len(html)} bytes")
        else:
            logger.warning(f"No HTML received from nodriver for {url}")

        return html

    def _fetch_page_selenium(self, url: str) -> Optional[str]:
        """Fetch page using Selenium/undetected-chromedriver."""
        browser = self._get_browser()

        logger.debug(f"Fetching page: {url}")

        html = browser.get_page(
            url,
            wait_timeout=30,
            cloudflare_wait=30.0,
        )

        # Wait for dynamic content and get updated source
        time.sleep(2)
        html = browser.page_source

        return html

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear page cache and force garbage collection."""
        cache_size = len(self._page_cache)
        self._page_cache.clear()
        gc.collect()
        logger.info(f"Page cache cleared ({cache_size} pages), garbage collected")

    def _manage_cache_size(self) -> None:
        """Manage cache size to prevent memory issues."""
        if len(self._page_cache) > self.MAX_CACHE_SIZE:
            # Remove oldest entries (first half of cache)
            keys_to_remove = list(self._page_cache.keys())[:len(self._page_cache) // 2]
            for key in keys_to_remove:
                del self._page_cache[key]
            logger.info(f"Cache trimmed: removed {len(keys_to_remove)} old entries")

    # ------------------------------------------------------------------
    # Browser restart / close
    # ------------------------------------------------------------------

    def _maybe_restart_browser(self) -> None:
        """Restart browser if page limit reached to prevent memory leaks."""
        self._pages_fetched += 1
        if self._pages_fetched >= self.MAX_PAGES_BEFORE_BROWSER_RESTART:
            logger.info(
                f"Restarting browser after {self._pages_fetched} pages to prevent memory leaks"
            )
            self._close_browser()
            self._pages_fetched = 0
            gc.collect()

    def _close_browser(self) -> None:
        """Close browser and clean up resources.

        Note: Does NOT stop the shared Xvfb display — it survives across
        browser restarts. Call _stop_shared_xvfb() for final cleanup.
        """
        # Close Selenium browser
        if self._browser is not None:
            try:
                self._browser.close()
            except Exception as e:
                logger.warning(f"Error closing Selenium browser: {e}")
            self._browser = None

        # Close nodriver browser (Xvfb is managed separately)
        if self._nodriver_browser is not None:
            try:
                self._nodriver_browser.close_sync()
            except Exception as e:
                logger.warning(f"Error closing nodriver browser: {e}")
            self._nodriver_browser = None

        # Reset HTTP session (cookies are bound to the browser session)
        self._http_session = None

    def _close_all(self) -> None:
        """Close browser AND shared Xvfb (final cleanup)."""
        self._close_browser()
        self._stop_shared_xvfb()
