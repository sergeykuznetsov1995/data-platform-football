"""
Browser Automation Package
==========================

Provides browser automation utilities for web scraping with
Cloudflare bypass capabilities.

Modules:
- cloudflare_bypass: Main CloudflareBypass class (undetected-chromedriver)
- nodriver_bypass: NodriverBypass class (nodriver - successor to uc)
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

    # Nodriver mode (better Cloudflare bypass)
    from scrapers.base.browser import NodriverBypass, nodriver_session

    with nodriver_session(headless=True) as browser:
        html = browser.get_page("https://fbref.com")

    # Async nodriver
    async with NodriverBypass(headless=True) as browser:
        html = await browser.get("https://fbref.com")
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

# Lazy import for nodriver (may not be installed)
NodriverBypass = None
nodriver_session = None


def _import_nodriver():
    """Lazy import nodriver components."""
    global NodriverBypass, nodriver_session
    if NodriverBypass is None:
        from scrapers.base.browser.nodriver_bypass import (
            NodriverBypass as _NodriverBypass,
            nodriver_session as _nodriver_session,
        )
        NodriverBypass = _NodriverBypass
        nodriver_session = _nodriver_session
    return NodriverBypass, nodriver_session


def get_nodriver_bypass():
    """Get NodriverBypass class (lazy import)."""
    cls, _ = _import_nodriver()
    return cls


def get_nodriver_session():
    """Get nodriver_session factory (lazy import)."""
    _, factory = _import_nodriver()
    return factory


__all__ = [
    'CloudflareBypass',
    'BrowserConfig',
    'DriverFactory',
    'browser_session',
    'create_proxy_auth_extension',
    'parse_proxy_url',
    'find_chrome_binary',
    # Nodriver (lazy import)
    'get_nodriver_bypass',
    'get_nodriver_session',
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
