"""
Cloudflare Bypass (Compatibility Module)
========================================

This module is maintained for backwards compatibility.
The actual implementation has been moved to scrapers.base.browser package.

For new code, import directly from:
    from scrapers.base.browser import CloudflareBypass, browser_session
"""

# Re-export everything from the new location for backwards compatibility
from scrapers.base.browser import (
    CloudflareBypass,
    BrowserConfig,
    DriverFactory,
    browser_session,
    create_proxy_auth_extension,
    parse_proxy_url,
    find_chrome_binary,
)

__all__ = [
    'CloudflareBypass',
    'BrowserConfig',
    'DriverFactory',
    'browser_session',
    'create_proxy_auth_extension',
    'parse_proxy_url',
    'find_chrome_binary',
]
