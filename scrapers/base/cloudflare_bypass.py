"""
Cloudflare Bypass
=================

Selenium-based browser automation for bypassing Cloudflare protection.
Uses undetected-chromedriver to avoid bot detection.
"""

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Try to import selenium and undetected-chromedriver
try:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException,
        WebDriverException,
        NoSuchElementException,
    )
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("Selenium not available")

try:
    import undetected_chromedriver as uc
    UC_AVAILABLE = True
except ImportError:
    UC_AVAILABLE = False
    logger.warning("undetected-chromedriver not available")


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


class CloudflareBypass:
    """
    Browser automation for bypassing Cloudflare and other protections.

    Usage:
        with CloudflareBypass(headless=False) as browser:
            html = browser.get_page("https://whoscored.com/...")
            data = browser.execute_script("return window.__INITIAL_STATE__")
    """

    def __init__(
        self,
        headless: bool = True,
        use_xvfb: bool = False,
        proxy: Optional[str] = None,
        user_agent: Optional[str] = None,
        page_load_timeout: int = 30,
    ):
        """
        Initialize browser.

        Args:
            headless: Run browser in headless mode
            use_xvfb: Use Xvfb for virtual display (bypasses headless detection)
            proxy: Proxy server URL
            user_agent: Custom user agent string
            page_load_timeout: Page load timeout in seconds
        """
        if not SELENIUM_AVAILABLE:
            raise ImportError("Selenium is required for CloudflareBypass")

        self.config = BrowserConfig(
            headless=headless,
            use_xvfb=use_xvfb,
            proxy=proxy,
            user_agent=user_agent,
            page_load_timeout=page_load_timeout,
        )

        self._driver = None
        self._xvfb_display = None

    def _start_xvfb(self):
        """Start Xvfb virtual display if needed."""
        if self.config.use_xvfb and self._xvfb_display is None:
            try:
                from pyvirtualdisplay import Display
                self._xvfb_display = Display(visible=False, size=self.config.window_size)
                self._xvfb_display.start()
                logger.info("Started Xvfb virtual display")
            except ImportError:
                logger.warning("pyvirtualdisplay not available, using headless mode")
            except Exception as e:
                logger.warning(f"Failed to start Xvfb: {e}, using headless mode")

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

    def _create_driver(self):
        """Create browser driver instance."""
        # Start Xvfb if configured
        if self.config.use_xvfb:
            self._start_xvfb()

        if UC_AVAILABLE:
            return self._create_undetected_driver()
        else:
            return self._create_standard_driver()

    def _create_undetected_driver(self):
        """Create undetected Chrome driver."""
        options = uc.ChromeOptions()

        # Explicitly set Chrome binary to avoid snap Chromium conflicts
        options.binary_location = '/usr/bin/google-chrome'

        # Use headless mode only if not using Xvfb
        if self.config.headless and not self.config.use_xvfb:
            options.add_argument('--headless=new')

        options.add_argument(f'--window-size={self.config.window_size[0]},{self.config.window_size[1]}')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument('--disable-software-rasterizer')

        # Handle proxy settings - bypass system proxy if no explicit proxy configured
        if self.config.proxy:
            options.add_argument(f'--proxy-server={self.config.proxy}')
        else:
            options.add_argument('--no-proxy-server')

        if self.config.user_agent:
            options.add_argument(f'--user-agent={self.config.user_agent}')

        for arg in self.config.extra_arguments:
            options.add_argument(arg)

        # Auto-detect Chrome version
        try:
            import subprocess
            result = subprocess.run(['google-chrome', '--version'], capture_output=True, text=True)
            version_str = result.stdout.strip().split()[-1]
            version_main = int(version_str.split('.')[0])
        except Exception:
            version_main = None

        driver = uc.Chrome(options=options, version_main=version_main)
        driver.set_page_load_timeout(self.config.page_load_timeout)
        driver.implicitly_wait(self.config.implicit_wait)

        return driver

    def _create_standard_driver(self):
        """Create standard Chrome driver (fallback)."""
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options

        options = Options()

        if self.config.headless:
            options.add_argument('--headless=new')

        options.add_argument(f'--window-size={self.config.window_size[0]},{self.config.window_size[1]}')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
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

    @property
    def driver(self):
        """Get or create the browser driver."""
        if self._driver is None:
            self._driver = self._create_driver()
        return self._driver

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup driver."""
        self.close()
        return False

    def close(self) -> None:
        """Close the browser and Xvfb if running."""
        if self._driver is not None:
            try:
                self._driver.quit()
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")
            finally:
                self._driver = None

        # Stop Xvfb if running
        self._stop_xvfb()

    def get_page(
        self,
        url: str,
        wait_for_selector: Optional[str] = None,
        wait_timeout: int = 10,
        cloudflare_wait: float = 5.0,
    ) -> str:
        """
        Navigate to URL and return page source.

        Args:
            url: URL to navigate to
            wait_for_selector: CSS selector to wait for
            wait_timeout: Timeout for waiting
            cloudflare_wait: Time to wait for Cloudflare challenge

        Returns:
            Page HTML source
        """
        logger.debug(f"Navigating to: {url}")

        self.driver.get(url)

        # Wait for potential Cloudflare challenge
        self._wait_for_cloudflare(cloudflare_wait)

        # Wait for specific element if specified
        if wait_for_selector:
            self.wait_for_element(wait_for_selector, wait_timeout)

        return self.driver.page_source

    def _wait_for_cloudflare(self, wait_time: float) -> None:
        """Wait for Cloudflare challenge to complete."""
        # Check for common Cloudflare indicators
        cloudflare_selectors = [
            '#challenge-running',
            '#challenge-stage',
            '.cf-browser-verification',
            '#cf-challenge-running',
        ]

        start_time = time.time()

        while time.time() - start_time < wait_time:
            # Check if any Cloudflare element is present
            for selector in cloudflare_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element.is_displayed():
                        logger.debug("Cloudflare challenge detected, waiting...")
                        time.sleep(1)
                        break
                except NoSuchElementException:
                    continue
            else:
                # No Cloudflare elements found
                return

        logger.debug("Cloudflare wait completed")

    def wait_for_element(
        self,
        selector: str,
        timeout: int = 10,
        by: str = 'css'
    ) -> Any:
        """
        Wait for element to be present.

        Args:
            selector: Element selector
            timeout: Timeout in seconds
            by: Selector type ('css', 'xpath', 'id')

        Returns:
            WebElement if found

        Raises:
            TimeoutException: If element not found within timeout
        """
        by_map = {
            'css': By.CSS_SELECTOR,
            'xpath': By.XPATH,
            'id': By.ID,
            'class': By.CLASS_NAME,
            'tag': By.TAG_NAME,
        }

        locator = (by_map.get(by, By.CSS_SELECTOR), selector)

        wait = WebDriverWait(self.driver, timeout)
        return wait.until(EC.presence_of_element_located(locator))

    def wait_for_text(
        self,
        text: str,
        timeout: int = 10
    ) -> bool:
        """Wait for specific text to appear on page."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            if text in self.driver.page_source:
                return True
            time.sleep(0.5)

        return False

    def execute_script(self, script: str) -> Any:
        """Execute JavaScript and return result."""
        return self.driver.execute_script(script)

    def get_json_from_script(
        self,
        variable_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract JSON data from JavaScript variable.

        Args:
            variable_name: JavaScript variable name (e.g., '__INITIAL_STATE__')

        Returns:
            Parsed JSON data or None
        """
        try:
            import json
            script = f"return JSON.stringify(window.{variable_name})"
            result = self.execute_script(script)

            if result:
                return json.loads(result)
        except Exception as e:
            logger.error(f"Error extracting {variable_name}: {e}")

        return None

    def click_element(
        self,
        selector: str,
        by: str = 'css',
        wait: bool = True
    ) -> None:
        """Click on element."""
        by_map = {
            'css': By.CSS_SELECTOR,
            'xpath': By.XPATH,
            'id': By.ID,
        }

        locator = by_map.get(by, By.CSS_SELECTOR)

        if wait:
            element = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((locator, selector))
            )
        else:
            element = self.driver.find_element(locator, selector)

        element.click()

    def scroll_to_bottom(self, pause_time: float = 1.0) -> None:
        """Scroll to bottom of page (for infinite scroll)."""
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(pause_time)

            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_cookies(self) -> List[Dict[str, Any]]:
        """Get all cookies."""
        return self.driver.get_cookies()

    def add_cookie(self, cookie: Dict[str, Any]) -> None:
        """Add cookie to browser."""
        self.driver.add_cookie(cookie)

    def take_screenshot(self, filename: str) -> None:
        """Save screenshot to file."""
        self.driver.save_screenshot(filename)

    @property
    def current_url(self) -> str:
        """Get current URL."""
        return self.driver.current_url

    @property
    def page_source(self) -> str:
        """Get current page source."""
        return self.driver.page_source


@contextmanager
def browser_session(
    headless: bool = True,
    proxy: Optional[str] = None,
    **kwargs
):
    """
    Context manager for browser sessions.

    Usage:
        with browser_session(headless=False) as browser:
            html = browser.get_page(url)
    """
    browser = CloudflareBypass(headless=headless, proxy=proxy, **kwargs)
    try:
        yield browser
    finally:
        browser.close()
