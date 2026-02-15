"""
Cloudflare Cookie Manager
=========================

Извлекает cf_clearance cookies через nodriver и инжектирует в HTTP сессии.
Рекомендация maintainer'a soccerdata (Issue #916).

Usage:
    # Async usage
    manager = CFCookieManager()
    cookies = await manager.get_cookies("https://fbref.com")
    session.cookies.update(cookies)

    # Sync usage
    manager = CFCookieManager()
    cookies = manager.get_cookies_sync("https://fbref.com")
    session.cookies.update(cookies)

Features:
    - Automatic Turnstile bypass via nodriver-cf-verify plugin
    - Cookie caching with TTL (default 30 minutes)
    - Proxy support for residential proxies
    - Fallback to passive wait if plugin not available
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class CFCookieManager:
    """
    Manages Cloudflare cookies for HTTP-based scrapers.

    Extracts cf_clearance and related cookies by loading pages with nodriver
    (which can bypass Cloudflare Turnstile) and then injects those cookies
    into regular HTTP sessions.

    This is useful for scrapers that use HTTP libraries (like soccerdata/curl_cffi)
    but encounter Cloudflare protection.

    Attributes:
        cache_ttl: How long to cache cookies before refreshing
        use_cf_verify: Whether to use nodriver-cf-verify plugin
    """

    # Cookie names to extract from browser
    CLOUDFLARE_COOKIE_NAMES = {
        'cf_clearance',
        '__cf_bm',
        '__cflb',
        '__cfuvid',
        '_cfuvid',
    }

    def __init__(
        self,
        cache_ttl_minutes: int = 30,
        use_cf_verify: bool = True,
        cf_verify_max_retries: int = 15,
        cf_verify_interval: float = 3.0,
        headless: bool = True,
        use_xvfb: bool = True,
    ):
        """
        Initialize CFCookieManager.

        Args:
            cache_ttl_minutes: How long to cache cookies (default 30 minutes)
            use_cf_verify: Use nodriver-cf-verify plugin for Turnstile bypass
            cf_verify_max_retries: Max retries for cf-verify plugin
            cf_verify_interval: Interval between cf-verify retries (seconds)
            headless: Run browser in headless mode
            use_xvfb: Use Xvfb virtual display
        """
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        self.use_cf_verify = use_cf_verify
        self.cf_verify_max_retries = cf_verify_max_retries
        self.cf_verify_interval = cf_verify_interval
        self.headless = headless
        self.use_xvfb = use_xvfb

        # domain -> (cookies_dict, timestamp)
        self._cookie_cache: Dict[str, tuple] = {}

    async def get_cookies(
        self,
        url: str,
        proxy: Optional[str] = None,
        force_refresh: bool = False,
    ) -> Dict[str, str]:
        """
        Get Cloudflare cookies for a URL.

        Uses cache if available and not expired. Otherwise fetches fresh
        cookies by loading the page in nodriver.

        Args:
            url: URL to get cookies for (e.g., "https://fbref.com")
            proxy: Optional proxy URL (host:port or host:port:user:pass)
            force_refresh: Force refresh even if cache is valid

        Returns:
            Dictionary of cookie name -> value for Cloudflare cookies
        """
        from urllib.parse import urlparse
        domain = urlparse(url).netloc

        # Check cache
        if not force_refresh and domain in self._cookie_cache:
            cookies, timestamp = self._cookie_cache[domain]
            if datetime.now() - timestamp < self.cache_ttl:
                logger.debug(f"Using cached CF cookies for {domain}")
                return cookies

        # Fetch new cookies
        logger.info(f"Fetching fresh CF cookies for {domain}...")
        cookies = await self._fetch_cookies(url, proxy)

        # Update cache
        self._cookie_cache[domain] = (cookies, datetime.now())

        return cookies

    async def _fetch_cookies(
        self,
        url: str,
        proxy: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Fetch fresh Cloudflare cookies using nodriver.

        Args:
            url: URL to load
            proxy: Optional proxy URL

        Returns:
            Dictionary of Cloudflare cookies
        """
        from scrapers.base.browser.nodriver_bypass import NodriverBypass

        bypass = NodriverBypass(
            headless=self.headless,
            use_xvfb=self.use_xvfb,
            proxy=proxy,
            cloudflare_wait=60.0,  # Increased for reliable Turnstile bypass
            use_cf_verify=self.use_cf_verify,
            cf_verify_max_retries=self.cf_verify_max_retries,
            cf_verify_interval=self.cf_verify_interval,
        )

        try:
            await bypass.start()

            # Navigate to URL (this triggers Cloudflare challenge handling)
            logger.debug(f"Loading {url} in nodriver for cookie extraction...")
            await bypass.get(url, wait_for_cloudflare=True)

            # Extract cookies from browser
            all_cookies = await bypass._browser.cookies.get_all()

            # Filter to Cloudflare-related cookies
            cf_cookies = {}
            for cookie in all_cookies:
                cookie_name = cookie.name if hasattr(cookie, 'name') else cookie.get('name', '')
                cookie_value = cookie.value if hasattr(cookie, 'value') else cookie.get('value', '')

                # Check if it's a Cloudflare cookie
                if cookie_name in self.CLOUDFLARE_COOKIE_NAMES or 'cf' in cookie_name.lower():
                    cf_cookies[cookie_name] = cookie_value

            logger.info(
                f"Extracted {len(cf_cookies)} CF cookies from {url}: "
                f"{list(cf_cookies.keys())}"
            )

            return cf_cookies

        except Exception as e:
            logger.error(f"Error fetching CF cookies: {e}")
            return {}

        finally:
            await bypass.close()

    def get_cookies_sync(
        self,
        url: str,
        proxy: Optional[str] = None,
        force_refresh: bool = False,
    ) -> Dict[str, str]:
        """
        Synchronous wrapper for get_cookies().

        Args:
            url: URL to get cookies for
            proxy: Optional proxy URL
            force_refresh: Force refresh even if cache is valid

        Returns:
            Dictionary of Cloudflare cookies
        """
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self.get_cookies(url, proxy, force_refresh)
            )
        finally:
            loop.close()

    async def get_cookies_with_retry(
        self,
        url: str,
        proxy_manager,
        max_attempts: int = 5,
    ) -> Dict[str, str]:
        """
        Попытаться получить CF cookies, перебирая разные прокси.

        Args:
            url: URL для получения cookies
            proxy_manager: ProxyManager с пулом прокси
            max_attempts: Максимум попыток с разными прокси

        Returns:
            Dictionary с CF cookies или пустой dict при неудаче
        """
        for attempt in range(max_attempts):
            proxy = proxy_manager.get_http_proxy_url() if proxy_manager else None
            logger.info(f"CF cookie attempt {attempt+1}/{max_attempts} with proxy")

            cookies = await self.get_cookies(url, proxy, force_refresh=True)

            if cookies and 'cf_clearance' in cookies:
                logger.info(f"Successfully got cf_clearance on attempt {attempt+1}")
                return cookies

            logger.warning(f"CF cookies attempt {attempt+1} failed, no cf_clearance")

        logger.error(f"Failed to get CF cookies after {max_attempts} attempts")
        return {}

    def get_cookies_with_retry_sync(
        self,
        url: str,
        proxy_manager,
        max_attempts: int = 5,
    ) -> Dict[str, str]:
        """Sync wrapper for get_cookies_with_retry."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self.get_cookies_with_retry(url, proxy_manager, max_attempts)
            )
        finally:
            loop.close()

    def clear_cache(self, domain: Optional[str] = None) -> None:
        """
        Clear cached cookies.

        Args:
            domain: Specific domain to clear, or None to clear all
        """
        if domain:
            self._cookie_cache.pop(domain, None)
            logger.debug(f"Cleared cookie cache for {domain}")
        else:
            self._cookie_cache.clear()
            logger.debug("Cleared all cookie cache")

    def get_cache_info(self) -> Dict[str, dict]:
        """
        Get information about cached cookies.

        Returns:
            Dictionary with domain -> cache info
        """
        info = {}
        now = datetime.now()

        for domain, (cookies, timestamp) in self._cookie_cache.items():
            age = now - timestamp
            expires_in = self.cache_ttl - age

            info[domain] = {
                'cookie_count': len(cookies),
                'cookie_names': list(cookies.keys()),
                'age_seconds': age.total_seconds(),
                'expires_in_seconds': max(0, expires_in.total_seconds()),
                'is_valid': expires_in.total_seconds() > 0,
            }

        return info


# Singleton instance for convenient reuse
_default_manager: Optional[CFCookieManager] = None


def get_cf_cookie_manager() -> CFCookieManager:
    """
    Get or create default CFCookieManager instance.

    Returns:
        CFCookieManager singleton instance
    """
    global _default_manager
    if _default_manager is None:
        _default_manager = CFCookieManager()
    return _default_manager


def inject_cf_cookies_sync(
    session,
    url: str,
    proxy: Optional[str] = None,
    force_refresh: bool = False,
) -> bool:
    """
    Convenience function to inject CF cookies into a requests session.

    Args:
        session: requests.Session or similar with .cookies.update() method
        url: URL to get cookies for
        proxy: Optional proxy URL
        force_refresh: Force refresh cookies

    Returns:
        True if cookies were injected, False otherwise
    """
    manager = get_cf_cookie_manager()
    cookies = manager.get_cookies_sync(url, proxy, force_refresh)

    if cookies:
        session.cookies.update(cookies)
        logger.info(f"Injected {len(cookies)} CF cookies into session")
        return True

    logger.warning("No CF cookies to inject")
    return False
