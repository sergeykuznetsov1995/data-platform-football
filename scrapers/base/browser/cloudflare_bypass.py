"""
Cloudflare Bypass
=================

Selenium-based browser automation for bypassing Cloudflare protection.
Uses undetected-chromedriver to avoid bot detection.
"""

import logging
import os
import shutil
import time
from typing import Any, Dict, List, Optional

from scrapers.base.browser.driver_factory import BrowserConfig, DriverFactory

logger = logging.getLogger(__name__)

# Try to import selenium
try:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        TimeoutException,
        NoSuchElementException,
    )
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("Selenium not available")


class CloudflareBypass:
    """
    Browser automation for bypassing Cloudflare and other protections.

    Uses Selenium with undetected-chromedriver.

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
            raise ImportError(
                "Selenium is required for CloudflareBypass"
            )

        self.config = BrowserConfig(
            headless=headless,
            use_xvfb=use_xvfb,
            proxy=proxy,
            user_agent=user_agent,
            page_load_timeout=page_load_timeout,
        )

        self._driver = None
        self._driver_factory: Optional[DriverFactory] = None
        self._xvfb_display = None

    def _start_xvfb(self):
        """Start Xvfb virtual display if needed."""
        if self.config.use_xvfb and self._xvfb_display is None:
            try:
                from pyvirtualdisplay import Display
                self._xvfb_display = Display(
                    visible=False,
                    size=self.config.window_size
                )
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
        if self.config.use_xvfb:
            self._start_xvfb()

        self._driver_factory = DriverFactory(self.config)
        return self._driver_factory.create_driver()

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
        """Close the browser, Xvfb, and cleanup temp files."""
        if self._driver is not None:
            try:
                self._driver.quit()
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")
            finally:
                self._driver = None

        self._stop_xvfb()

        # Cleanup proxy extension temp directory
        if self._driver_factory and self._driver_factory.proxy_extension_path:
            try:
                extension_dir = os.path.dirname(
                    self._driver_factory.proxy_extension_path
                )
                if extension_dir and os.path.exists(extension_dir):
                    shutil.rmtree(extension_dir)
                    logger.debug(f"Cleaned up proxy extension: {extension_dir}")
            except Exception as e:
                logger.warning(f"Error cleaning up proxy extension: {e}")

    def get_page(
        self,
        url: str,
        wait_for_selector: Optional[str] = None,
        wait_timeout: int = 10,
        cloudflare_wait: float = 60.0,
    ) -> str:
        """
        Navigate to URL and return page source.

        Args:
            url: URL to navigate to
            wait_for_selector: CSS selector to wait for
            wait_timeout: Timeout for waiting
            cloudflare_wait: Time to wait for Cloudflare challenge (default 60s)

        Returns:
            Page HTML source
        """
        logger.debug(f"Navigating to: {url}")

        self.driver.get(url)

        # Wait for potential Cloudflare challenge
        self._wait_for_cloudflare(cloudflare_wait, url=url)

        # Wait for specific element if specified
        if wait_for_selector:
            self.wait_for_element(wait_for_selector, wait_timeout)

        return self.driver.page_source

    def _wait_for_cloudflare(self, wait_time: float, url: str = None) -> None:
        """
        Wait for Cloudflare challenge to complete automatically.

        Args:
            wait_time: Time to wait for automatic resolution
            url: Current page URL (unused, kept for backward compatibility)
        """
        cloudflare_selectors = [
            '#challenge-running',
            '#challenge-stage',
            '.cf-browser-verification',
            '#cf-challenge-running',
            '#cf-wrapper',
            '.ray_id',
        ]

        turnstile_selectors = [
            '.cf-turnstile',
            'iframe[src*="challenges.cloudflare.com"]',
            '#turnstile-wrapper',
        ]

        cloudflare_titles = [
            'just a moment',
            'please wait',
            'checking your browser',
            'attention required',
            'one more step',
        ]

        start_time = time.time()
        check_interval = 0.5

        while time.time() - start_time < wait_time:
            elapsed = time.time() - start_time

            try:
                title = self.driver.title.lower()
                is_cloudflare_page = any(
                    cf_title in title for cf_title in cloudflare_titles
                )

                if is_cloudflare_page:
                    logger.debug(
                        f"Cloudflare page detected (title: {self.driver.title}), "
                        f"waiting... ({elapsed:.1f}s)"
                    )
                    time.sleep(check_interval)
                    continue
            except Exception:
                pass

            cloudflare_found = False
            for selector in cloudflare_selectors + turnstile_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element.is_displayed():
                        logger.debug(
                            f"Cloudflare element detected: {selector}, waiting..."
                        )
                        cloudflare_found = True
                        break
                except NoSuchElementException:
                    continue

            if cloudflare_found:
                time.sleep(check_interval)
                continue

            logger.debug("Cloudflare challenge completed")
            return

        logger.warning(f"Cloudflare wait timeout after {wait_time}s")

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

    def get_cookies(self) -> List[Dict[str, Any]]:
        """Get all cookies."""
        return self.driver.get_cookies()

    def add_cookie(self, cookie: Dict[str, Any]) -> None:
        """Add cookie to browser."""
        self.driver.add_cookie(cookie)

    @property
    def current_url(self) -> str:
        """Get current URL."""
        return self.driver.current_url

    @property
    def page_source(self) -> str:
        """Get current page source."""
        return self.driver.page_source
