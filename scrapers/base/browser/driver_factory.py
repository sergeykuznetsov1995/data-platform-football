"""
Driver Factory
==============

Factory functions for creating Selenium WebDriver instances.
Supports both undetected-chromedriver and standard Selenium drivers.
"""

import logging
import subprocess
from dataclasses import dataclass
from typing import List, Optional

from scrapers.base.browser.utils import find_chrome_binary
from scrapers.base.browser.proxy_extension import (
    create_proxy_auth_extension,
    parse_proxy_url,
)

logger = logging.getLogger(__name__)

# Try to import undetected-chromedriver
try:
    import undetected_chromedriver as uc
    UC_AVAILABLE = True
except ImportError:
    UC_AVAILABLE = False
    logger.debug("undetected-chromedriver not available")


@dataclass
class BrowserConfig:
    """Configuration for browser automation."""
    headless: bool = True
    use_xvfb: bool = False  # Use Xvfb for virtual display (bypasses headless detection)
    window_size: tuple = (1920, 1080)
    page_load_timeout: int = 30
    implicit_wait: int = 10
    user_agent: Optional[str] = None
    proxy: Optional[str] = None
    disable_images: bool = False
    disable_javascript: bool = False
    extra_arguments: List[str] = None

    def __post_init__(self):
        if self.extra_arguments is None:
            self.extra_arguments = []


class DriverFactory:
    """Factory for creating browser drivers."""

    def __init__(self, config: BrowserConfig):
        """
        Initialize driver factory.

        Args:
            config: Browser configuration
        """
        self.config = config
        self._proxy_extension_path: Optional[str] = None

    @property
    def proxy_extension_path(self) -> Optional[str]:
        """Get path to proxy extension (if created)."""
        return self._proxy_extension_path

    def create_driver(self):
        """
        Create browser driver instance.

        Returns:
            WebDriver instance (undetected or standard)
        """
        if UC_AVAILABLE:
            return self._create_undetected_driver()
        else:
            return self._create_standard_driver()

    def _create_undetected_driver(self):
        """Create undetected Chrome driver."""
        options = uc.ChromeOptions()

        # Auto-detect Chrome/Chromium binary
        options.binary_location = find_chrome_binary()

        # Use headless mode only if not using Xvfb
        if self.config.headless and not self.config.use_xvfb:
            options.add_argument('--headless=new')

        options.add_argument(
            f'--window-size={self.config.window_size[0]},{self.config.window_size[1]}'
        )
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-setuid-sandbox')

        # Note: --disable-gpu / --disable-software-rasterizer intentionally NOT set —
        # they null the WebGL context, which Cloudflare reads as a bot marker. WebGL
        # needs GPU/SwiftShader enabled to produce a "real" context. (#567)

        # Memory and stability optimizations
        options.add_argument('--disable-background-networking')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-sync')
        options.add_argument('--disable-translate')
        options.add_argument('--mute-audio')
        options.add_argument('--no-first-run')
        options.add_argument('--safebrowsing-disable-auto-update')
        options.add_argument('--js-flags=--max-old-space-size=1024')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--disable-accelerated-2d-canvas')
        options.add_argument('--disable-accelerated-video-decode')

        # Additional anti-detection options
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--allow-running-insecure-content')
        options.add_argument('--disable-web-security')
        options.add_argument('--lang=en-US,en')

        # Set realistic user agent if not provided
        if not self.config.user_agent:
            options.add_argument(
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            )
        else:
            options.add_argument(f'--user-agent={self.config.user_agent}')

        # Handle proxy settings
        if self.config.proxy:
            proxy_info = parse_proxy_url(self.config.proxy)

            if proxy_info['username'] and proxy_info['password']:
                # Proxy with authentication - use extension
                self._proxy_extension_path = create_proxy_auth_extension(
                    proxy_host=proxy_info['host'],
                    proxy_port=proxy_info['port'],
                    proxy_user=proxy_info['username'],
                    proxy_pass=proxy_info['password'],
                )
                options.add_extension(self._proxy_extension_path)
                logger.info(
                    f"Using proxy auth extension for "
                    f"{proxy_info['host']}:{proxy_info['port']}"
                )
            else:
                # Proxy without authentication - use command line
                proxy_url = (
                    f"{proxy_info['scheme']}://"
                    f"{proxy_info['host']}:{proxy_info['port']}"
                )
                options.add_argument(f'--proxy-server={proxy_url}')
                logger.info(f"Using proxy: {proxy_url}")
        else:
            options.add_argument('--no-proxy-server')

        for arg in self.config.extra_arguments:
            options.add_argument(arg)

        # Auto-detect Chrome version
        version_main = self._detect_chrome_version()

        # Find system chromedriver to avoid download issues
        chromedriver_path = self._find_chromedriver()

        # Create driver with use_subprocess for better detection evasion
        driver = uc.Chrome(
            options=options,
            version_main=version_main,
            use_subprocess=True,
            driver_executable_path=chromedriver_path,
        )
        driver.set_page_load_timeout(self.config.page_load_timeout)
        driver.implicitly_wait(self.config.implicit_wait)

        return driver

    def _create_standard_driver(self):
        """Create standard Chrome driver (fallback)."""
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options

        options = Options()

        if self.config.headless:
            options.add_argument('--headless=new')

        options.add_argument(
            f'--window-size={self.config.window_size[0]},{self.config.window_size[1]}'
        )
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        # Note: --disable-gpu intentionally NOT set — it nulls the WebGL context
        # that Cloudflare uses to verify a "real" browser (bot marker). (#567)
        options.add_argument('--disable-blink-features=AutomationControlled')

        # Handle proxy settings - bypass system proxy if no explicit proxy configured
        if self.config.proxy:
            options.add_argument(f'--proxy-server={self.config.proxy}')
        else:
            options.add_argument('--no-proxy-server')

        if self.config.user_agent:
            options.add_argument(f'--user-agent={self.config.user_agent}')

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(self.config.page_load_timeout)
        driver.implicitly_wait(self.config.implicit_wait)

        return driver

    def _detect_chrome_version(self) -> Optional[int]:
        """
        Detect Chrome version for undetected-chromedriver.

        Returns:
            Major Chrome version number or None
        """
        try:
            chrome_binary = find_chrome_binary()
            result = subprocess.run(
                [chrome_binary, '--version'],
                capture_output=True,
                text=True
            )
            version_str = result.stdout.strip().split()[-1]
            return int(version_str.split('.')[0])
        except Exception:
            return None

    def _find_chromedriver(self) -> Optional[str]:
        """
        Find chromedriver binary (prefer user-writable location for patching).

        undetected-chromedriver needs to patch the binary, so we need
        a writable copy. Prefer user's home directory over system paths.

        Returns:
            Path to chromedriver or None
        """
        import os
        import shutil

        # User-writable paths (preferred for undetected-chromedriver patching)
        home = os.path.expanduser('~')
        user_paths = [
            os.path.join(home, '.local', 'bin', 'chromedriver'),
            os.path.join(home, 'chromedriver'),
        ]

        # Check user paths first (writable for patching)
        for path in user_paths:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                logger.debug(f"Found writable chromedriver at: {path}")
                return path

        # System paths (fallback, may not be writable)
        system_paths = [
            '/usr/bin/chromedriver',
            '/usr/local/bin/chromedriver',
            '/opt/chromedriver/chromedriver',
        ]

        for path in system_paths:
            if os.path.isfile(path) and os.access(path, os.X_OK):
                # Try to copy to writable location
                user_chromedriver = os.path.join(home, '.local', 'bin', 'chromedriver')
                try:
                    os.makedirs(os.path.dirname(user_chromedriver), exist_ok=True)
                    shutil.copy2(path, user_chromedriver)
                    os.chmod(user_chromedriver, 0o755)
                    logger.info(f"Copied chromedriver to writable location: {user_chromedriver}")
                    return user_chromedriver
                except Exception as e:
                    logger.warning(f"Could not copy chromedriver: {e}")
                    return path

        # Try to find via which
        chromedriver = shutil.which('chromedriver')
        if chromedriver:
            logger.debug(f"Found chromedriver via which: {chromedriver}")
            return chromedriver

        logger.warning("System chromedriver not found, will download")
        return None
