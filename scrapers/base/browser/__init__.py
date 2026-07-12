"""
Browser Automation Package
==========================

Provides browser automation utilities for web scraping with
Cloudflare bypass capabilities.

Modules:
- cloudflare_bypass: Main CloudflareBypass class (undetected-chromedriver)
- driver_factory: WebDriver creation and configuration
- proxy_extension: Chrome proxy authentication extension
- utils: Browser utility functions

Usage:
    from scrapers.base.browser import CloudflareBypass, browser_session

    # Selenium mode (default, using undetected-chromedriver)
    with CloudflareBypass(headless=False) as browser:
        html = browser.get_page("https://example.com")

    # Or using context manager
    with browser_session(headless=False) as browser:
        html = browser.get_page("https://example.com")

"""

from contextlib import contextmanager
from typing import Optional

from scrapers.base.browser.cloudflare_bypass import CloudflareBypass
from scrapers.base.browser.driver_factory import BrowserConfig, DriverFactory
from scrapers.base.browser.proxy_extension import (
    create_proxy_auth_extension,
    parse_proxy_url,
)
from scrapers.base.browser.utils import find_chrome_binary

__all__ = [
    'CloudflareBypass',
    'BrowserConfig',
    'DriverFactory',
    'browser_session',
    'create_proxy_auth_extension',
    'parse_proxy_url',
    'find_chrome_binary',
]


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
    browser = CloudflareBypass(
        headless=headless,
        proxy=proxy,
        **kwargs
    )
    try:
        yield browser
    finally:
        browser.close()
