"""
Nodriver Cloudflare Bypass
==========================

Advanced Cloudflare bypass using nodriver - the official successor to
undetected-chromedriver from the same author.

Advantages over undetected-chromedriver:
- Better fingerprint evasion
- Async API (faster)
- Built-in CDP detection protection
- Actively maintained

Usage:
    # Async usage
    async with NodriverBypass(headless=True) as browser:
        html = await browser.get("https://fbref.com")

    # Sync wrapper for compatibility
    bypass = NodriverBypass(headless=True)
    html = bypass.get_sync("https://fbref.com")

Requirements:
    - Python 3.9+
    - nodriver>=0.32
    - Chrome/Chromium browser installed
"""

import asyncio
import logging
import random
from typing import Optional

logger = logging.getLogger(__name__)


class SlowProxyError(Exception):
    """Raised when proxy is too slow for effective CF bypass."""
    pass

# Lazy import to avoid errors when nodriver is not installed
nodriver = None
CFVerify = None


def _import_nodriver():
    """Lazy import nodriver to allow graceful degradation."""
    global nodriver
    if nodriver is None:
        try:
            import nodriver as uc
            nodriver = uc
        except ImportError as e:
            raise ImportError(
                "nodriver is not installed. Install it with: pip install nodriver>=0.32"
            ) from e
    return nodriver


def _import_cf_verify():
    """
    Lazy import nodriver-cf-verify plugin.

    Returns:
        CFVerify class or None if not installed
    """
    global CFVerify
    if CFVerify is None:
        try:
            # Try local module first (bundled with project)
            from scrapers.base.browser.nodriver_cf_verify import CFVerify as _CFVerify
            CFVerify = _CFVerify
            logger.debug("nodriver-cf-verify plugin loaded from local module")
        except ImportError:
            try:
                # Fallback to installed package
                from nodriver_cf_verify import CFVerify as _CFVerify
                CFVerify = _CFVerify
                logger.debug("nodriver-cf-verify plugin loaded from installed package")
            except ImportError:
                logger.debug("nodriver-cf-verify not available, Turnstile auto-click disabled")
                pass
    return CFVerify


class NodriverBypass:
    """
    Cloudflare bypass using nodriver (async).

    nodriver is the official successor to undetected-chromedriver,
    providing better fingerprint evasion and an async API.

    Attributes:
        headless: Run browser in headless mode (with stealth patches)
        use_xvfb: Use Xvfb virtual display to bypass headless detection
        proxy: Proxy URL (format: host:port or http://user:pass@host:port)
        cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
        page_load_timeout: Maximum time to wait for page load (seconds)
        max_retries: Maximum number of retries for Cloudflare bypass
    """

    def __init__(
        self,
        headless: bool = True,
        use_xvfb: bool = False,
        proxy: Optional[str] = None,
        cloudflare_wait: float = 90.0,
        page_load_timeout: float = 180.0,
        browser_args: Optional[list] = None,
        max_retries: int = 2,  # Reduced from 5: retry with same IP is useless for CF block
        use_cf_verify: bool = True,
        cf_verify_max_retries: int = 10,
        cf_verify_interval: float = 2.0,
        wait_for_selector: Optional[str] = None,
        wait_for_selector_timeout: float = 30.0,
        content_timeout: float = 30.0,
        pre_content_js: Optional[str] = None,
        wait_for_content_js: Optional[str] = None,
        wait_for_content_timeout: float = 120.0,
        wait_for_content_poll: float = 5.0,
        slow_proxy_threshold: float = 15.0,
    ):
        """
        Initialize NodriverBypass.

        Args:
            headless: Run browser in headless mode
            use_xvfb: Use Xvfb virtual display (bypasses headless detection)
            proxy: Proxy URL (host:port or http://user:pass@host:port)
            cloudflare_wait: Time to wait for Cloudflare challenge (seconds, default 90s)
            page_load_timeout: Maximum time for page load (seconds, default 180s)
            browser_args: Additional browser arguments
            max_retries: Maximum number of retries for Cloudflare bypass (default 2).
                         Note: Retrying with the same IP/proxy rarely helps with CF blocks.
                         Higher-level retry with proxy rotation should be used instead.
            use_cf_verify: Use nodriver-cf-verify plugin for active Turnstile bypass (default True)
            cf_verify_max_retries: Max retries for cf-verify plugin (default 10)
            cf_verify_interval: Interval between cf-verify retries in seconds (default 2.0)
            wait_for_selector: CSS selector to wait for after CF bypass (ensures JS rendered)
            wait_for_selector_timeout: Timeout for wait_for_selector in seconds (default 30)
            content_timeout: Timeout for content extraction in seconds (default 45).
            pre_content_js: JavaScript to execute before content extraction (e.g., uncomment tables).
            wait_for_content_js: JS expression that returns truthy when content is ready (polling).
                                 Example: "document.querySelectorAll('table').length > 0"
            wait_for_content_timeout: Max time to poll wait_for_content_js (default 120s).
            wait_for_content_poll: Poll interval for wait_for_content_js (default 5s).
        """
        self.headless = headless
        self.use_xvfb = use_xvfb
        self.proxy = proxy
        self.cloudflare_wait = cloudflare_wait
        self.page_load_timeout = page_load_timeout
        self.browser_args = browser_args or []
        self.max_retries = max_retries

        # nodriver-cf-verify plugin settings
        self.use_cf_verify = use_cf_verify
        self.cf_verify_max_retries = cf_verify_max_retries
        self.cf_verify_interval = cf_verify_interval

        # Post-CF-bypass DOM readiness settings
        self.wait_for_selector = wait_for_selector
        self.wait_for_selector_timeout = wait_for_selector_timeout

        # Content retrieval timeout
        self.content_timeout = content_timeout

        # JavaScript to execute before content extraction
        self.pre_content_js = pre_content_js

        # Content readiness polling
        self.wait_for_content_js = wait_for_content_js
        self.wait_for_content_timeout = wait_for_content_timeout
        self.wait_for_content_poll = wait_for_content_poll

        # Slow proxy detection: page.get() > threshold → SlowProxyError
        # Set to 0 to disable slow proxy detection
        self.slow_proxy_threshold = slow_proxy_threshold

        self._browser = None
        self._page = None
        self._loop = None
        self._xvfb_display = None

    # Stealth JS script injected via CDP Page.addScriptToEvaluateOnNewDocument.
    # Executes BEFORE any page scripts on every navigation.
    # Based on puppeteer-extra-plugin-stealth patches.
    STEALTH_JS = """
    // --- navigator.webdriver ---
    // Double insurance on top of nodriver's built-in patch
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });

    // --- navigator.plugins ---
    // Headless Chrome has empty plugins array — dead giveaway
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const plugins = [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
            ];
            plugins.length = 3;
            return plugins;
        },
        configurable: true
    });

    // --- navigator.languages ---
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true
    });

    // --- navigator.permissions.query ---
    // Headless returns 'denied' for notifications; real browsers return 'prompt'
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : originalQuery(parameters)
    );

    // --- window.chrome ---
    // Must exist with runtime property in real Chrome
    if (!window.chrome) {
        window.chrome = {};
    }
    if (!window.chrome.runtime) {
        window.chrome.runtime = {};
    }

    // --- Notification.permission ---
    if (Notification.permission === 'denied') {
        Object.defineProperty(Notification, 'permission', {
            get: () => 'default',
            configurable: true
        });
    }

    """

    # Window sizes for rotation (non-standard to avoid fingerprinting)
    WINDOW_SIZES = [
        (1366, 768),   # Common laptop
        (1536, 864),   # Scaled laptop
        (1440, 900),   # MacBook
        (1280, 800),   # Smaller laptop
        (1600, 900),   # Wide laptop
    ]

    # User agents for rotation (Chrome 131-133, актуальные для Q1 2026)
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
    ]

    def _start_xvfb(self):
        """Start Xvfb virtual display for non-headless mode with random resolution."""
        if self._xvfb_display is not None:
            return  # Already started

        try:
            from pyvirtualdisplay import Display

            # Random resolution to avoid fingerprinting (not standard 1920x1080)
            width, height = random.choice(self.WINDOW_SIZES)

            self._xvfb_display = Display(
                visible=False,
                size=(width, height),
                color_depth=24,
            )
            self._xvfb_display.start()
            logger.info(f"Started Xvfb virtual display: {width}x{height}")
        except ImportError:
            logger.warning(
                "pyvirtualdisplay not available, falling back to headless mode. "
                "Install with: pip install pyvirtualdisplay"
            )
        except Exception as e:
            logger.warning(f"Failed to start Xvfb: {e}, falling back to headless mode")
            self._xvfb_display = None

    def _stop_xvfb(self):
        """Stop Xvfb virtual display."""
        if self._xvfb_display is not None:
            try:
                self._xvfb_display.stop()
                logger.info("Stopped Xvfb virtual display")
            except Exception as e:
                logger.warning(f"Error stopping Xvfb: {e}")
            finally:
                self._xvfb_display = None

    def _cleanup_chrome_processes(self):
        """Kill orphaned Chrome/Chromium processes to prevent FD exhaustion."""
        import subprocess
        try:
            # Find zombie or orphaned chromium processes started by nodriver
            result = subprocess.run(
                ['pkill', '-f', '--user-data-dir=/tmp/uc_'],
                capture_output=True, timeout=5
            )
            if result.returncode == 0:
                logger.info("Cleaned up orphaned Chrome processes")
        except Exception as e:
            logger.debug(f"Chrome process cleanup: {e}")

        # Clean up leftover temp directories
        import glob
        import shutil
        for d in glob.glob('/tmp/uc_*'):
            try:
                shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass
        for d in glob.glob('/tmp/extension_*'):
            try:
                shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass

    async def start(self):
        """Start the browser asynchronously."""
        uc = _import_nodriver()

        config = uc.Config()

        # Find browser executable (nodriver may not find it automatically in Docker)
        import shutil
        for browser_name in ['google-chrome', 'chromium', 'chromium-browser', 'chrome']:
            browser_path = shutil.which(browser_name)
            if browser_path:
                config.browser_executable_path = browser_path
                logger.info(f"Using browser: {browser_path}")
                break
        else:
            logger.warning("No browser found in PATH, nodriver will try to find one")

        # If use_xvfb is enabled, start Xvfb and run in non-headless mode
        # This bypasses Cloudflare's headless browser detection
        if self.use_xvfb:
            self._start_xvfb()
            if self._xvfb_display is not None:
                config.headless = False  # Real browser in virtual display
                logger.info("Running nodriver in non-headless mode with Xvfb")
            else:
                config.headless = self.headless  # Fallback to headless
        else:
            config.headless = self.headless

        # Required for running as root in Docker containers
        # nodriver uses config.sandbox attribute instead of --no-sandbox argument
        config.sandbox = False

        # Add proxy if specified (with auth support via extension)
        if self.proxy:
            proxy_info = self._parse_proxy(self.proxy)

            if proxy_info.get('username'):
                # Create auth extension for proxy with credentials
                from scrapers.base.browser.proxy_extension import create_proxy_auth_extension
                extension_path = create_proxy_auth_extension(
                    proxy_host=proxy_info['host'],
                    proxy_port=proxy_info['port'],
                    proxy_user=proxy_info['username'],
                    proxy_pass=proxy_info['password'],
                )
                config.add_extension(extension_path)
                logger.info(
                    f"Loaded proxy auth extension for {proxy_info['host']}:{proxy_info['port']}"
                )
            else:
                # No auth - use --proxy-server argument
                config.add_argument(
                    f"--proxy-server={proxy_info['host']}:{proxy_info['port']}"
                )
                logger.info(
                    f"Nodriver using proxy: {proxy_info['host']}:{proxy_info['port']}"
                )

        # Anti-fingerprint settings - CRITICAL for Cloudflare bypass
        config.add_argument('--disable-blink-features=AutomationControlled')
        # Note: WebGL is NOT disabled — Cloudflare uses WebGL fingerprinting
        # to verify "real" browser. With --disable-gpu, WebGL falls back to
        # software rendering with minimal memory usage.
        # Note: --lang is not allowed in newer nodriver versions (use config.locale instead)
        # Language is set via Accept-Language header or browser preferences

        # Do NOT override User-Agent — let the real Chromium version (120) use its
        # native UA. Faking Chrome 131-133 UA with Chrome 120 TLS fingerprint
        # creates a detectable JA3/JA4 mismatch that Cloudflare catches.

        # Random window size (set via Xvfb, but also pass as argument)
        width, height = random.choice(self.WINDOW_SIZES)
        config.add_argument(f'--window-size={width},{height}')

        # Note: Memory optimization flags (--disable-gpu, --renderer-process-limit=1, etc.)
        # intentionally removed — they create a unique browser fingerprint that
        # Cloudflare detects as a bot marker. WebGL fingerprinting requires GPU enabled.

        # Add custom browser arguments
        for arg in self.browser_args:
            config.add_argument(arg)

        # Start browser
        logger.info(f"Starting nodriver browser (headless={self.headless})")
        self._browser = await uc.start(config)

        # Get initial page
        self._page = await self._browser.get("about:blank")

        # Re-enable stealth JS — the iframe contentWindow patch was removed,
        # remaining patches (navigator.webdriver, plugins, languages, chrome.runtime)
        # help mask headless fingerprint.
        await self._inject_stealth_js()

        logger.debug("Nodriver browser started successfully")

    async def _inject_stealth_js(self):
        """
        Inject stealth JavaScript via CDP Page.addScriptToEvaluateOnNewDocument.

        This ensures the stealth patches run BEFORE any page scripts on every
        navigation, hiding automation markers from Cloudflare Turnstile.
        """
        try:
            import nodriver.cdp.page as cdp_page

            await self._page.send(
                cdp_page.add_script_to_evaluate_on_new_document(source=self.STEALTH_JS)
            )
            logger.info("Stealth JS injected via addScriptToEvaluateOnNewDocument")
        except Exception as e:
            logger.warning(f"Failed to inject stealth JS: {e}")

    def _parse_proxy(self, proxy: str) -> dict:
        """
        Parse proxy string into components.

        Supports formats:
        - host:port:user:pass (common residential proxy format)
        - host:port (no auth)
        - http://user:pass@host:port (URL format)
        - http://host:port (URL format, no auth)
        - socks5://host:port

        Args:
            proxy: Proxy string in any supported format

        Returns:
            Dict with keys: host, port, username, password, scheme
        """
        # URL format (http://..., https://..., socks5://...)
        if '://' in proxy:
            from scrapers.base.browser.proxy_extension import parse_proxy_url
            return parse_proxy_url(proxy)

        # host:port:user:pass format (common for residential proxies)
        parts = proxy.split(':')
        if len(parts) >= 4:
            return {
                'host': parts[0],
                'port': int(parts[1]),
                'username': parts[2],
                'password': ':'.join(parts[3:]),  # Password may contain ':'
                'scheme': 'http',
            }
        elif len(parts) == 2:
            return {
                'host': parts[0],
                'port': int(parts[1]),
                'username': None,
                'password': None,
                'scheme': 'http',
            }

        raise ValueError(f"Invalid proxy format: {proxy}")

    def _mask_proxy(self, proxy: str) -> str:
        """Mask proxy credentials for logging."""
        # URL format with @
        if "@" in proxy:
            parts = proxy.split("@")
            return f"****@{parts[-1]}"
        # host:port:user:pass format
        parts = proxy.split(':')
        if len(parts) >= 4:
            return f"{parts[0]}:{parts[1]}:****:****"
        return proxy

    async def get(self, url: str, wait_for_cloudflare: bool = True) -> str:
        """
        Navigate to URL and get page HTML with retry logic.

        Args:
            url: URL to navigate to
            wait_for_cloudflare: Whether to wait for Cloudflare challenge

        Returns:
            Page HTML content
        """
        if self._page is None:
            await self.start()

        logger.info(f"Nodriver navigating to: {url}")

        html = ""
        last_error = None

        for attempt in range(self.max_retries):
            try:
                html = await self._get_internal(url, wait_for_cloudflare)

                # Check if Cloudflare bypass succeeded
                if not self._is_cloudflare_blocked(html):
                    logger.info(f"Successfully loaded page: {url} (attempt {attempt + 1})")
                    return html

                logger.warning(
                    f"Cloudflare still blocking (attempt {attempt + 1}/{self.max_retries})"
                )

            except SlowProxyError as e:
                logger.warning(f"Slow proxy detected on attempt {attempt + 1}: {e}")
                raise  # Propagate to caller for immediate proxy rotation
            except asyncio.TimeoutError as e:
                logger.warning(f"Timeout on attempt {attempt + 1}/{self.max_retries}: {e}")
                last_error = e
            except Exception as e:
                logger.warning(f"Error on attempt {attempt + 1}/{self.max_retries}: {e}")
                last_error = e

            # Wait before retry (exponential backoff)
            if attempt < self.max_retries - 1:
                wait_time = 5 * (attempt + 1)
                logger.info(f"Waiting {wait_time}s before retry...")
                await asyncio.sleep(wait_time)

        # Return last result even if blocked (let caller handle)
        if html:
            logger.warning(
                f"Cloudflare challenge not passed after {self.max_retries} attempts"
            )
            return html

        # If no HTML at all, raise the last error
        if last_error:
            raise last_error
        raise RuntimeError(f"Failed to load page after {self.max_retries} attempts")

    async def _get_html_via_cdp_dom(self, timeout: float = 30.0) -> str:
        """
        Get page HTML via CDP DOM.getOuterHTML — bypasses JS execution.

        This method works at the browser process level and does NOT require
        a functioning JS execution context. It's the reliable fallback when
        Runtime.evaluate hangs after Cloudflare bypass.

        Args:
            timeout: Timeout for each CDP call in seconds.

        Returns:
            Page HTML string, or empty string on failure.
        """
        try:
            import nodriver.cdp.dom as cdp_dom

            doc = await asyncio.wait_for(
                self._page.send(cdp_dom.get_document(-1, True)),
                timeout=timeout,
            )
            html = await asyncio.wait_for(
                self._page.send(
                    cdp_dom.get_outer_html(backend_node_id=doc.backend_node_id)
                ),
                timeout=timeout,
            )
            return html if isinstance(html, str) else str(html) if html else ""
        except Exception as e:
            logger.warning(f"CDP DOM.getOuterHTML failed: {e}")
            return ""

    async def _get_html_with_fallback(self) -> str:
        """
        Get page HTML with fallback chain:
        1. Runtime.evaluate (fast path, 15s timeout)
        2. CDP DOM.getOuterHTML (reliable, no JS needed)
        3. page.get_content() (nodriver built-in)

        Returns:
            Page HTML string.

        Raises:
            RuntimeError: If all methods fail.
        """
        # 1. Fast path: Runtime.evaluate
        try:
            logger.debug("Getting HTML via Runtime.evaluate (fast path)")
            result = await asyncio.wait_for(
                self._page.evaluate(
                    "document.documentElement.outerHTML",
                    return_by_value=True,
                ),
                timeout=15.0,
            )
            html = getattr(result, 'value', result) if hasattr(result, 'value') else result
            if not isinstance(html, str):
                html = str(html) if html else ""
            if html and len(html) >= 1000:
                logger.info(f"Got {len(html)} bytes via Runtime.evaluate")
                return html
            logger.warning(f"Runtime.evaluate returned small content ({len(html)} bytes)")
        except asyncio.TimeoutError:
            logger.warning(
                "Runtime.evaluate timed out after 15s, "
                "falling back to CDP DOM.getOuterHTML"
            )
        except Exception as e:
            logger.warning(f"Runtime.evaluate failed: {e}, falling back to CDP DOM")

        # 2. CDP DOM.getOuterHTML — no JS execution needed
        html = await self._get_html_via_cdp_dom(timeout=30.0)
        if html and len(html) >= 1000:
            logger.info(f"Got {len(html)} bytes via CDP DOM.getOuterHTML")
            return html

        # 3. Last resort: nodriver built-in get_content()
        try:
            logger.debug("Falling back to page.get_content()")
            html = await asyncio.wait_for(
                self._page.get_content(),
                timeout=30.0,
            )
            if html:
                logger.info(f"Got {len(html)} bytes via page.get_content()")
                return html
        except Exception as e:
            logger.warning(f"page.get_content() failed: {e}")

        raise RuntimeError("All HTML extraction methods failed")

    async def _get_html_hung_runtime(self) -> str:
        """
        Get HTML when JS runtime is hung. Short timeouts, skip evaluate.

        When Runtime.evaluate hangs (common after CF bypass through slow proxies),
        this method uses only CDP DOM and get_content() with 10s timeouts instead
        of the normal 15s+30s+30s=75s fallback chain.
        """
        html = await self._get_html_via_cdp_dom(timeout=10.0)
        if html and len(html) >= 1000:
            logger.info(f"[HUNG] Got {len(html)} bytes via CDP DOM")
            return html
        try:
            html = await asyncio.wait_for(self._page.get_content(), timeout=10.0)
            if html:
                logger.info(f"[HUNG] Got {len(html)} bytes via get_content()")
                return html
        except Exception as e:
            logger.warning(f"[HUNG] get_content() failed: {e}")
        raise RuntimeError("All HTML extraction methods failed (runtime hung)")

    async def _get_internal(self, url: str, wait_for_cloudflare: bool = True) -> str:
        """
        Internal method to navigate to URL and get page HTML (single attempt).

        Args:
            url: URL to navigate to
            wait_for_cloudflare: Whether to wait for Cloudflare challenge

        Returns:
            Page HTML content
        """
        try:
            # Navigate to URL with timeout and slow proxy detection
            logger.debug(f"[DIAG] Starting page.get() for {url}")
            _nav_start = asyncio.get_event_loop().time()
            try:
                await asyncio.wait_for(self._page.get(url), timeout=60)
            except asyncio.TimeoutError:
                logger.error(f"[DIAG] page.get() TIMED OUT after 60s for {url}")
                raise
            _nav_elapsed = asyncio.get_event_loop().time() - _nav_start
            logger.info(f"[DIAG] page.get() completed in {_nav_elapsed:.1f}s")
            if self.slow_proxy_threshold > 0 and _nav_elapsed > self.slow_proxy_threshold:
                raise SlowProxyError(
                    f"page.get() took {_nav_elapsed:.1f}s (>{self.slow_proxy_threshold}s threshold) — proxy too slow"
                )

            # Wait for Cloudflare challenge if enabled (with timeout)
            if wait_for_cloudflare:
                logger.debug(f"[DIAG] Starting _wait_for_cloudflare()")
                try:
                    await asyncio.wait_for(
                        self._wait_for_cloudflare(),
                        timeout=self.cloudflare_wait + 30  # cloudflare_wait + buffer
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"[DIAG] _wait_for_cloudflare() timed out, continuing anyway")
                logger.debug(f"[DIAG] _wait_for_cloudflare() completed")

            # Wait for specific DOM element to ensure JS has fully rendered
            if self.wait_for_selector:
                try:
                    logger.debug(
                        f"[DIAG] Waiting for selector '{self.wait_for_selector}' "
                        f"(timeout={self.wait_for_selector_timeout}s)"
                    )
                    await asyncio.wait_for(
                        self._page.select(self.wait_for_selector, timeout=self.wait_for_selector_timeout),
                        timeout=self.wait_for_selector_timeout + 5,
                    )
                    logger.info(f"DOM element '{self.wait_for_selector}' found")
                except asyncio.TimeoutError:
                    logger.warning(
                        f"Timeout waiting for selector '{self.wait_for_selector}' "
                        f"after {self.wait_for_selector_timeout}s"
                    )
                except Exception as e:
                    logger.warning(f"Error waiting for selector '{self.wait_for_selector}': {e}")

            # Wait for document.readyState === 'complete' (page fully loaded)
            # This is crucial for pages loaded through slow proxies where
            # HTML download may still be in progress after initial DOM elements appear.
            # If evaluate hangs here, we set _runtime_hung flag and skip JS-dependent steps.
            _runtime_hung = False
            readystate_timeout = 15.0  # 15s max for readyState check (reduced from 30s)
            try:
                logger.debug("[DIAG] Waiting for document.readyState === 'complete'...")
                for _rs_attempt in range(int(readystate_timeout / 2)):
                    rs = await asyncio.wait_for(
                        self._page.evaluate("document.readyState", return_by_value=True),
                        timeout=3,  # Reduced from 5s for faster hung detection
                    )
                    # rs may be a RemoteObject or a simple value
                    rs_value = getattr(rs, 'value', rs) if hasattr(rs, 'value') else rs
                    if rs_value == 'complete':
                        logger.info(f"document.readyState = 'complete'")
                        break
                    logger.debug(f"[DIAG] document.readyState = '{rs_value}', waiting...")
                    await asyncio.sleep(2)
                else:
                    logger.warning(
                        f"document.readyState not 'complete' after {readystate_timeout}s, continuing"
                    )
            except asyncio.TimeoutError:
                logger.warning(
                    "Runtime.evaluate timed out during readyState check, "
                    "marking runtime as hung — will use CDP DOM fallback"
                )
                _runtime_hung = True
            except Exception as e:
                logger.warning(f"Error checking readyState: {e}")

            # Brief wait for remaining JS
            await asyncio.sleep(2)
            logger.info("JS stabilization wait complete")

            # Poll for content readiness (e.g., wait for tables to appear in DOM)
            # SKIP when runtime is hung — evaluate calls will just timeout
            if self.wait_for_content_js and not _runtime_hung:
                content_ready = False
                poll_start = asyncio.get_event_loop().time()
                poll_end = poll_start + self.wait_for_content_timeout
                poll_count = 0

                while asyncio.get_event_loop().time() < poll_end:
                    poll_count += 1
                    try:
                        result = await asyncio.wait_for(
                            self._page.evaluate(self.wait_for_content_js, return_by_value=True),
                            timeout=10,
                        )
                        val = getattr(result, 'value', result) if hasattr(result, 'value') else result
                        if val:
                            elapsed = asyncio.get_event_loop().time() - poll_start
                            logger.info(
                                f"Content ready after {elapsed:.1f}s "
                                f"(poll #{poll_count}, result={val})"
                            )
                            content_ready = True
                            break
                    except asyncio.TimeoutError:
                        logger.debug(f"Content poll #{poll_count} timed out")
                    except Exception as e:
                        logger.debug(f"Content poll #{poll_count} error: {e}")

                    await asyncio.sleep(self.wait_for_content_poll)

                if not content_ready:
                    elapsed = asyncio.get_event_loop().time() - poll_start
                    logger.warning(
                        f"Content not ready after {elapsed:.1f}s ({poll_count} polls)"
                    )
            elif self.wait_for_content_js and _runtime_hung:
                logger.info("Skipping wait_for_content_js — runtime is hung")

            # Log current URL for diagnostics (skip if runtime hung)
            if not _runtime_hung:
                try:
                    current_url = await asyncio.wait_for(
                        self._page.evaluate("window.location.href", return_by_value=True),
                        timeout=5,
                    )
                    url_value = getattr(current_url, 'value', current_url) if hasattr(current_url, 'value') else current_url
                    logger.info(f"Current browser URL: {url_value}")
                except Exception as e:
                    logger.warning(f"Could not get current URL: {e}")

            # Execute pre-content JavaScript (e.g., uncomment FBref tables)
            # SKIP when runtime is hung — tables from HTML comments will be
            # extracted in Python via extract_tables_from_comments()
            if self.pre_content_js and not _runtime_hung:
                try:
                    logger.debug("[DIAG] Executing pre_content_js...")
                    result = await asyncio.wait_for(
                        self._page.evaluate(self.pre_content_js, return_by_value=True),
                        timeout=self.content_timeout,
                    )
                    result_value = getattr(result, 'value', result) if hasattr(result, 'value') else result
                    logger.info(f"pre_content_js result: {result_value}")
                    await asyncio.sleep(2)
                except asyncio.TimeoutError:
                    logger.warning(f"pre_content_js timed out after {self.content_timeout}s")
                except Exception as e:
                    logger.warning(f"pre_content_js error: {e}")
            elif self.pre_content_js and _runtime_hung:
                logger.info(
                    "Skipping pre_content_js — runtime is hung. "
                    "Tables from HTML comments will be extracted in Python."
                )

            # Get page HTML with fallback chain
            # When runtime is hung, use short-timeout method (20s max vs 75s)
            if _runtime_hung:
                html = await self._get_html_hung_runtime()
            else:
                html = await self._get_html_with_fallback()

            content_size = len(html) if html else 0
            if content_size < 10000:
                logger.warning(f"Content small ({content_size} bytes) after fallback chain")

            return html

        except asyncio.TimeoutError:
            logger.error(f"Timeout loading page: {url}")
            raise
        except Exception as e:
            logger.error(f"Error loading page {url}: {e}")
            raise

    async def _wait_for_cloudflare(self):
        """
        Wait for Cloudflare challenge with active bypass attempt.

        Strategy:
        1. Use CFVerify plugin to find and click Turnstile iframe
        2. Fallback to opencv-based template matching with human-like behavior
        3. Passive wait as last resort
        """
        logger.debug("Attempting Cloudflare Turnstile bypass...")

        # First try CFVerify plugin (clicks on iframe - proven to work)
        if self.use_cf_verify:
            try:
                from scrapers.base.browser.nodriver_cf_verify import CFVerify
                cf_verify = CFVerify(_browser_tab=self._page, _debug=True)

                logger.info("Using CFVerify plugin for Turnstile bypass...")
                result = await asyncio.wait_for(
                    cf_verify.verify(
                        _max_retries=self.cf_verify_max_retries,
                        _interval_between_retries=self.cf_verify_interval,
                        _reload_page_after_n_retries=5  # Reload after 5 failed clicks for fresh challenge token
                    ),
                    timeout=self.cloudflare_wait
                )

                if result:
                    logger.info("CFVerify successfully bypassed Cloudflare!")
                    await asyncio.sleep(2)  # Wait for page to fully load
                    return

                logger.warning("CFVerify returned False, trying opencv-based approach...")

            except asyncio.TimeoutError:
                logger.warning("CFVerify timed out, trying opencv-based approach...")
            except ImportError:
                logger.warning("CFVerify not available, trying opencv-based approach...")
            except Exception as e:
                logger.warning(f"CFVerify error: {e}, trying opencv-based approach...")

        # Fallback: try opencv template matching with human-like behavior
        for attempt in range(min(3, self.cf_verify_max_retries)):
            try:
                # Check if already passed
                html = await self._page.get_content()
                if not self._is_cloudflare_blocked(html):
                    logger.info("Cloudflare challenge already passed")
                    return

                logger.info(f"Opencv attempt {attempt + 1}/3: locating CF checkbox...")

                # Pre-click human behavior (scroll, mouse movement, thinking pause)
                await self._pre_click_behavior()

                # Find checkbox coordinates using opencv template matching
                coords = await asyncio.wait_for(
                    self._page.template_location(), timeout=15
                )
                if not coords:
                    logger.warning("CF checkbox not found in screenshot")
                    await asyncio.sleep(self.cf_verify_interval)
                    continue

                x, y = coords
                logger.info(f"Found CF checkbox at ({x}, {y}), performing human-like click...")

                # Human-like behavior: move mouse to checkbox area with some randomness
                start_x = x + random.randint(-200, 200)
                start_y = y + random.randint(-100, 100)

                # Move mouse towards target in steps (simulates natural movement)
                await self._human_like_mouse_move(start_x, start_y, x, y)

                # Small random offset for click position
                click_x = x + random.randint(-3, 3)
                click_y = y + random.randint(-3, 3)

                # Human-like click with delay between press and release
                await self._human_like_click(click_x, click_y)

                # Wait for Cloudflare to process the click
                await asyncio.sleep(5 + random.uniform(0, 3))

                # Check if bypass succeeded
                html = await self._page.get_content()
                if not self._is_cloudflare_blocked(html):
                    logger.info("Cloudflare bypassed with opencv human-like click!")
                    return

            except asyncio.TimeoutError:
                logger.warning(f"Opencv template_location timed out on attempt {attempt + 1}")
            except Exception as e:
                logger.warning(f"Opencv attempt {attempt + 1} failed: {e}")

            await asyncio.sleep(self.cf_verify_interval + random.uniform(1, 3))

        logger.warning("Active bypass methods exhausted, trying passive wait...")

        # Fallback: passive wait for Cloudflare challenge with early exit
        # Instead of waiting full cloudflare_wait, check every 5 seconds
        # This reduces wait time from 90s to actual challenge resolution time (~5-15s)
        logger.debug(f"Passive wait up to {self.cloudflare_wait}s for Cloudflare challenge (checking every 5s)...")

        check_interval = 5
        elapsed = 0

        # Phase 1: Wait up to cloudflare_wait with periodic checks
        while elapsed < self.cloudflare_wait:
            html = await self._page.get_content()

            if not self._is_cloudflare_blocked(html):
                logger.info(f"Cloudflare challenge passed after {elapsed}s (early exit)")
                return

            logger.debug(f"Cloudflare challenge in progress, waited {elapsed}s...")
            await asyncio.sleep(check_interval)
            elapsed += check_interval

        # Phase 2: Additional wait if still blocked (max 60s more)
        max_additional_wait = 60
        additional_elapsed = 0

        while additional_elapsed < max_additional_wait:
            html = await self._page.get_content()

            if not self._is_cloudflare_blocked(html):
                logger.info(f"Cloudflare challenge passed after {elapsed + additional_elapsed}s")
                return

            logger.debug(f"Still on Cloudflare challenge, waiting {check_interval}s more...")
            await asyncio.sleep(check_interval)
            additional_elapsed += check_interval

        logger.warning(
            f"Cloudflare wait timeout after {elapsed + additional_elapsed}s"
        )

    async def _pre_click_behavior(self):
        """
        Human-like behavior before clicking the Cloudflare checkbox.

        Simulates natural user behavior: random scroll, mouse movement, thinking pause.
        This makes the browser session appear more human-like to Cloudflare.
        """
        import nodriver.cdp.input_ as cdp_input

        # Random scroll (40% chance)
        if random.random() < 0.4:
            scroll_y = random.randint(-50, 100)
            await self._page.evaluate(f'window.scrollBy(0, {scroll_y})')
            await asyncio.sleep(random.uniform(0.3, 0.8))
            logger.debug(f"Pre-click: scrolled {scroll_y}px")

        # Random mouse movement (50% chance)
        if random.random() < 0.5:
            rand_x = random.randint(100, 800)
            rand_y = random.randint(100, 400)
            await self._page.send(
                cdp_input.dispatch_mouse_event(
                    type_="mouseMoved",
                    x=rand_x,
                    y=rand_y
                )
            )
            await asyncio.sleep(random.uniform(0.5, 1.5))
            logger.debug(f"Pre-click: mouse moved to ({rand_x}, {rand_y})")

        # "Thinking" pause (always)
        thinking_time = random.uniform(1.0, 3.0)
        await asyncio.sleep(thinking_time)
        logger.debug(f"Pre-click: thinking pause {thinking_time:.1f}s")

    async def _human_like_mouse_move(self, start_x: int, start_y: int, end_x: int, end_y: int):
        """
        Move mouse from start to end position in a human-like manner.

        Uses bezier-like movement with random delays to simulate natural mouse movement.
        """
        import nodriver.cdp.input_ as cdp_input

        steps = random.randint(5, 10)
        for i in range(steps + 1):
            # Progress from 0 to 1
            t = i / steps
            # Add slight curve using easing function
            eased_t = t * t * (3 - 2 * t)  # smoothstep

            # Current position with some random jitter
            curr_x = start_x + (end_x - start_x) * eased_t + random.uniform(-2, 2)
            curr_y = start_y + (end_y - start_y) * eased_t + random.uniform(-2, 2)

            await self._page.send(
                cdp_input.dispatch_mouse_event(
                    type_="mouseMoved",
                    x=curr_x,
                    y=curr_y
                )
            )
            # Variable delay between movements (50-150ms)
            await asyncio.sleep(random.uniform(0.05, 0.15))

    async def _human_like_click(self, x: int, y: int):
        """
        Perform a human-like mouse click with realistic timing.

        Humans have ~100-300ms delay between press and release.
        """
        import nodriver.cdp.input_ as cdp_input

        # Mouse press
        await self._page.send(
            cdp_input.dispatch_mouse_event(
                type_="mousePressed",
                x=x,
                y=y,
                button=cdp_input.MouseButton("left"),
                buttons=1,
                click_count=1
            )
        )

        # Human-like delay between press and release (100-300ms)
        await asyncio.sleep(random.uniform(0.1, 0.3))

        # Mouse release
        await self._page.send(
            cdp_input.dispatch_mouse_event(
                type_="mouseReleased",
                x=x,
                y=y,
                button=cdp_input.MouseButton("left"),
                buttons=1,
                click_count=1
            )
        )

        logger.debug(f"Human-like click at ({x}, {y}) completed")

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

    @property
    def page_source(self) -> str:
        """Get current page source synchronously (for compatibility)."""
        if self._page is None:
            return ""

        loop = self._get_or_create_loop()

        async def _get_with_timeout():
            try:
                result = await asyncio.wait_for(
                    self._page.evaluate(
                        "document.documentElement.outerHTML",
                        return_by_value=True,
                    ),
                    timeout=self.content_timeout,
                )
                html = getattr(result, 'value', result) if hasattr(result, 'value') else result
                return html if isinstance(html, str) else str(html) if html else ""
            except asyncio.TimeoutError:
                logger.warning(
                    f"page_source: evaluate timed out after {self.content_timeout}s, "
                    "trying CDP DOM fallback"
                )
                # Fallback to CDP DOM (no JS needed)
                html = await self._get_html_via_cdp_dom(timeout=30.0)
                if html:
                    return html
                # Last resort: nodriver built-in
                try:
                    return await asyncio.wait_for(
                        self._page.get_content(), timeout=30.0
                    )
                except Exception:
                    return ""

        return loop.run_until_complete(_get_with_timeout())

    async def close(self):
        """Close the browser asynchronously with aggressive memory cleanup."""
        if self._browser is not None:
            try:
                await self._browser.stop()
                logger.debug("Nodriver browser closed")
            except Exception as e:
                logger.warning(f"Error closing nodriver browser: {e}")
            finally:
                self._browser = None
                self._page = None
                # Aggressive garbage collection to free Chromium memory
                # Double collect handles circular references
                import gc
                gc.collect()
                gc.collect()

        # Stop Xvfb if running
        self._stop_xvfb()
        # Clean up orphaned Chrome processes and temp files
        self._cleanup_chrome_processes()

    def _get_or_create_loop(self) -> asyncio.AbstractEventLoop:
        """Get existing event loop or create a new one."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop - create new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop

    def restart_browser(self):
        """
        Restart the browser synchronously.

        This is useful when browser becomes unresponsive after Cloudflare bypass.
        """
        loop = self._get_or_create_loop()
        try:
            loop.run_until_complete(self.close())
        except Exception as e:
            logger.warning(f"Error during browser restart close: {e}")
        self._browser = None
        self._page = None
        # Aggressive garbage collection after restart
        import gc
        gc.collect()
        gc.collect()
        logger.info("Browser restarted (will reconnect on next request)")

    def get_sync(self, url: str, wait_for_cloudflare: bool = True) -> str:
        """
        Synchronous wrapper for get() - for compatibility with existing code.

        Opens browser, navigates to URL, gets HTML, and closes browser.

        Args:
            url: URL to navigate to
            wait_for_cloudflare: Whether to wait for Cloudflare challenge

        Returns:
            Page HTML content
        """
        loop = self._get_or_create_loop()
        return loop.run_until_complete(
            self._get_with_lifecycle(url, wait_for_cloudflare)
        )

    async def _get_with_lifecycle(
        self, url: str, wait_for_cloudflare: bool = True
    ) -> str:
        """Get page with automatic browser lifecycle management."""
        await self.start()
        try:
            return await self.get(url, wait_for_cloudflare)
        finally:
            await self.close()

    def get_page(
        self,
        url: str,
        wait_timeout: float = 30,
        cloudflare_wait: float = None,
    ) -> str:
        """
        Synchronous method compatible with CloudflareBypass interface.

        Args:
            url: URL to navigate to
            wait_timeout: Not used (for interface compatibility)
            cloudflare_wait: Time to wait for Cloudflare (overrides instance setting)

        Returns:
            Page HTML content
        """
        if cloudflare_wait is not None:
            original_wait = self.cloudflare_wait
            self.cloudflare_wait = cloudflare_wait

        try:
            loop = self._get_or_create_loop()

            # Start browser if not started
            if self._browser is None:
                loop.run_until_complete(self.start())

            # Navigate and get content
            html = loop.run_until_complete(self.get(url, wait_for_cloudflare=True))
            return html

        finally:
            if cloudflare_wait is not None:
                self.cloudflare_wait = original_wait

    def close_sync(self):
        """Close browser synchronously with memory cleanup."""
        if self._browser is not None:
            loop = self._get_or_create_loop()
            loop.run_until_complete(self.close())
            # Extra garbage collection for sync close
            import gc
            gc.collect()
        else:
            # Browser is None (crashed or never started), but Xvfb may still be running
            self._stop_xvfb()
            self._cleanup_chrome_processes()

    # Context manager support (sync)
    def __enter__(self):
        """Enter sync context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit sync context manager."""
        self.close_sync()
        return False

    # Async context manager support
    async def __aenter__(self):
        """Enter async context manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit async context manager."""
        await self.close()
        return False


def nodriver_session(
    headless: bool = True,
    use_xvfb: bool = False,
    proxy: Optional[str] = None,
    cloudflare_wait: float = 90.0,
    max_retries: int = 2,  # Reduced: retry with same IP is useless for CF block
    use_cf_verify: bool = True,
    cf_verify_max_retries: int = 10,
    cf_verify_interval: float = 2.0,
    wait_for_selector: Optional[str] = None,
    wait_for_selector_timeout: float = 30.0,
    content_timeout: float = 30.0,
    pre_content_js: Optional[str] = None,
    wait_for_content_js: Optional[str] = None,
    wait_for_content_timeout: float = 120.0,
    wait_for_content_poll: float = 5.0,
    **kwargs
) -> NodriverBypass:
    """
    Factory function for creating NodriverBypass instances.

    Usage:
        with nodriver_session(headless=True, use_xvfb=True) as browser:
            html = browser.get_page("https://fbref.com")

        # Or async
        async with nodriver_session(headless=True, use_xvfb=True) as browser:
            html = await browser.get("https://fbref.com")

    Returns:
        NodriverBypass instance
    """
    return NodriverBypass(
        headless=headless,
        use_xvfb=use_xvfb,
        proxy=proxy,
        cloudflare_wait=cloudflare_wait,
        max_retries=max_retries,
        use_cf_verify=use_cf_verify,
        cf_verify_max_retries=cf_verify_max_retries,
        cf_verify_interval=cf_verify_interval,
        wait_for_selector=wait_for_selector,
        wait_for_selector_timeout=wait_for_selector_timeout,
        content_timeout=content_timeout,
        pre_content_js=pre_content_js,
        wait_for_content_js=wait_for_content_js,
        wait_for_content_timeout=wait_for_content_timeout,
        wait_for_content_poll=wait_for_content_poll,
        **kwargs
    )
