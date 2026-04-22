"""
SoccerdataFBrefScraper - FBref Scraper using soccerdata library with Tor support
================================================================================

Lightweight alternative to Selenium-based FBref scraper.
Uses the soccerdata library which makes HTTP requests with proper headers,
avoiding Cloudflare detection in most cases.

Key features:
- No Selenium/Chrome required (much lower memory footprint)
- Native Tor support via soccerdata's proxy parameter
- Built-in caching and rate limiting from soccerdata
- Fallback to residential proxies if Tor fails
- curl_cffi patch for Chrome TLS fingerprint impersonation

Usage:
    from scrapers.soccerdata_fbref import SoccerdataFBrefScraper

    # With Tor (recommended)
    scraper = SoccerdataFBrefScraper(
        leagues=['ENG-Premier League'],
        seasons=[2024],
        use_tor=True
    )
    results = scraper.scrape_all()

    # With residential proxies
    scraper = SoccerdataFBrefScraper(
        leagues=['ENG-Premier League'],
        seasons=[2024],
        use_tor=False,
        proxy_file='/path/to/proxys.txt'
    )
"""

import logging as _logging

_patch_logger = _logging.getLogger(__name__)


# Marker to prevent double patching
_SOCCERDATA_CURL_PATCHED = False

# Browser-like headers for better stealth (used by patch)
_BROWSER_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
}


def _patch_soccerdata_with_curl_cffi():
    """
    Patch soccerdata to use curl_cffi instead of tls_requests.

    curl_cffi provides real Chrome TLS fingerprint impersonation,
    which is much harder for Cloudflare to detect than tls_requests.

    This patch replaces both __init__ and _init_session methods
    in soccerdata's BaseRequestsReader to use curl_cffi Session
    that impersonates Chrome 120.

    The patch is applied only once, even if called multiple times.
    """
    global _SOCCERDATA_CURL_PATCHED

    if _SOCCERDATA_CURL_PATCHED:
        _patch_logger.debug("curl_cffi patch already applied, skipping")
        return True

    try:
        from curl_cffi.requests import Session as CurlSession
    except ImportError:
        _patch_logger.warning(
            "curl_cffi not installed. Soccerdata will use default tls_requests. "
            "For better Cloudflare bypass, install curl_cffi: pip install curl_cffi"
        )
        return False

    try:
        import soccerdata._common as sd_common
    except ImportError:
        _patch_logger.warning("soccerdata not installed, skipping patch")
        return False

    # Check if already patched (e.g., by previous import)
    if hasattr(sd_common.BaseRequestsReader, '_curl_cffi_patched'):
        _patch_logger.debug("soccerdata already patched by previous import")
        _SOCCERDATA_CURL_PATCHED = True
        return True

    # Store original methods
    original_init = sd_common.BaseRequestsReader.__init__
    original_init_session = sd_common.BaseRequestsReader._init_session

    def _create_curl_session(proxy_str=None):
        """Create a curl_cffi session with Chrome impersonation."""
        session = CurlSession(impersonate='chrome120')
        session.headers.update(_BROWSER_HEADERS)

        if proxy_str:
            if proxy_str == 'tor':
                proxy_str = 'socks5h://localhost:9050'
            session.proxies = {'http': proxy_str, 'https': proxy_str}
            _patch_logger.debug(f"curl_cffi session using proxy: {proxy_str[:30]}...")

        return session

    def patched_init(self, *args, **kwargs):
        """Patched __init__ that replaces session with curl_cffi."""
        original_init(self, *args, **kwargs)

        # Get proxy from the callable self.proxy() if available
        proxy_str = None
        proxy_attr = getattr(self, 'proxy', None)
        if proxy_attr and callable(proxy_attr):
            try:
                proxy_str = proxy_attr()
            except Exception:
                pass
        elif proxy_attr and isinstance(proxy_attr, str):
            proxy_str = proxy_attr

        # Replace the session with curl_cffi
        self._session = _create_curl_session(proxy_str)
        _patch_logger.debug("Created curl_cffi session with Chrome 120 impersonation")

    def patched_init_session(self, headers=None):
        """Patched _init_session that returns curl_cffi session instead of tls_requests."""
        # Get proxy from the callable self.proxy() if available
        proxy_str = None
        proxy_attr = getattr(self, 'proxy', None)
        if proxy_attr and callable(proxy_attr):
            try:
                proxy_str = proxy_attr()
            except Exception:
                pass
        elif proxy_attr and isinstance(proxy_attr, str):
            proxy_str = proxy_attr

        session = _create_curl_session(proxy_str)

        # Apply additional headers if provided
        if headers:
            session.headers.update(headers)

        _patch_logger.debug("Re-created curl_cffi session (retry scenario)")
        return session

    # Apply patches to both methods
    sd_common.BaseRequestsReader.__init__ = patched_init
    sd_common.BaseRequestsReader._init_session = patched_init_session
    sd_common.BaseRequestsReader._curl_cffi_patched = True
    _SOCCERDATA_CURL_PATCHED = True

    _patch_logger.info("Successfully applied curl_cffi patch to soccerdata (__init__ and _init_session)")
    return True


def is_curl_cffi_patched() -> bool:
    """Check if curl_cffi patch is active."""
    return _SOCCERDATA_CURL_PATCHED


# Apply patch at module load time
_CURL_CFFI_PATCHED = _patch_soccerdata_with_curl_cffi()

import gc
import logging
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from scrapers.base.base_scraper import SoccerdataScraper
from scrapers.fbref.constants import (
    PLAYER_STAT_TYPES,
    TEAM_STAT_TYPES,
    KEEPER_STAT_TYPES,
)

logger = logging.getLogger(__name__)


class SoccerdataFBrefScraper(SoccerdataScraper):
    """
    FBref scraper using soccerdata library with Tor/proxy support.

    This scraper is a lightweight alternative to the Selenium-based FBrefScraper.
    It uses the soccerdata library which makes direct HTTP requests with proper
    headers, avoiding Cloudflare detection in many cases.

    Advantages over Selenium:
    - Much lower memory footprint (no Chrome browser)
    - Faster execution
    - Native Tor support
    - Built-in caching

    Limitations:
    - May not work if Cloudflare implements stricter JS challenges
    - Some advanced features may not be available

    Attributes:
        SOURCE_NAME: Source identifier for metadata
        DEFAULT_RATE_LIMIT: Requests per minute
    """

    SOURCE_NAME = 'fbref'
    DEFAULT_RATE_LIMIT = 12  # Conservative rate limit

    # Stat types for compatibility with FBrefScraper
    PLAYER_STAT_TYPES = PLAYER_STAT_TYPES
    TEAM_STAT_TYPES = TEAM_STAT_TYPES
    KEEPER_STAT_TYPES = KEEPER_STAT_TYPES

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        use_tor: bool = True,
        tor_host: str = 'tor',
        tor_port: int = 9050,
        proxy_file: Optional[str] = None,
        no_cache: bool = False,
        **kwargs
    ):
        """
        Initialize SoccerdataFBrefScraper.

        Args:
            leagues: List of leagues to scrape (soccerdata format)
                     e.g., ['ENG-Premier League', 'ESP-La Liga']
            seasons: List of seasons (ending year, e.g., [2024] for 2023-24)
            use_tor: Use Tor SOCKS5 proxy for anonymization
            tor_host: Tor service hostname (default: 'tor' for Docker)
            tor_port: Tor SOCKS5 port (default: 9050)
            proxy_file: Path to file with proxies (format: host:port:user:pass)
                       Used as fallback if Tor fails
            no_cache: Disable soccerdata caching
            **kwargs: Additional arguments for SoccerdataScraper
        """
        # Extract our custom kwargs before passing to parent
        self._use_nodriver_fallback: bool = kwargs.pop('use_nodriver_fallback', True)
        self._use_cf_cookie_injection: bool = kwargs.pop('use_cf_cookie_injection', True)

        super().__init__(
            leagues=leagues,
            seasons=seasons,
            no_cache=no_cache,
            proxy_file=proxy_file,
            **kwargs
        )

        self.use_tor = use_tor
        self.tor_host = tor_host
        self.tor_port = tor_port

        # Sticky session settings for residential proxies
        # Reduced from 10 to 5 to avoid FBref rate limiting/banning
        self._sticky_proxy: Optional[str] = None
        self._sticky_requests_count: int = 0
        self._max_sticky_requests: int = 5  # Rotate after N requests (was 10, reduced for FBref)

        # Nodriver fallback settings
        self._nodriver_instance = None

        # CF cookie injection settings (fallback for HTTP scraper)
        self._cf_cookie_manager = None
        self._cf_cookies_injected: bool = False

        # Configure proxy for soccerdata
        self._sd_proxy = self._configure_proxy()

        # Reader instance (lazy initialization)
        self._reader = None

        logger.info(
            f"Initialized SoccerdataFBrefScraper: "
            f"leagues={leagues}, seasons={seasons}, "
            f"use_tor={use_tor}, proxy={self._sd_proxy}"
        )

    def _configure_proxy(self) -> Optional[str]:
        """
        Configure proxy string for soccerdata.

        Priority:
        1. Residential proxies from proxy_file (HTTP format for best compatibility)
        2. Tor proxy if enabled
        3. Single proxy URL if set
        4. No proxy

        Returns:
            Proxy string for soccerdata (e.g., 'http://user:pass@host:port')
        """
        # Priority 1: Residential proxies (HTTP format)
        if self._proxy_manager and self._proxy_manager.total_count > 0:
            http_proxy = self._proxy_manager.get_http_proxy_url()
            if http_proxy:
                proxy_obj = self._proxy_manager.get_proxy()
                if proxy_obj:
                    logger.info(
                        f"Using residential proxy: {proxy_obj.host}:{proxy_obj.port} "
                        f"(available: {self._proxy_manager.available_count}/{self._proxy_manager.total_count})"
                    )
                return http_proxy

        # Priority 2: Tor proxy
        if self.use_tor:
            # soccerdata accepts 'tor' as a special value
            # which internally uses socks5h://localhost:9050
            # For Docker, we need to specify the host explicitly
            if self.tor_host != 'localhost':
                return f'socks5h://{self.tor_host}:{self.tor_port}'
            return 'tor'

        # Priority 3: Single proxy URL
        if self.proxy:
            return self.proxy

        return None

    def _get_sticky_proxy(self) -> Optional[str]:
        """
        Get sticky proxy for maintaining session across multiple requests.

        Keeps the same proxy for _max_sticky_requests requests to maintain
        IP consistency and reduce ban probability.

        Returns:
            HTTP proxy URL string or None
        """
        if not self._proxy_manager or self._proxy_manager.total_count == 0:
            return None

        # If we have a sticky proxy and haven't exceeded limit, use it
        if self._sticky_proxy and self._sticky_requests_count < self._max_sticky_requests:
            self._sticky_requests_count += 1
            logger.debug(
                f"Using sticky proxy (request {self._sticky_requests_count}/{self._max_sticky_requests})"
            )
            return self._sticky_proxy

        # Rotate to new proxy
        http_proxy = self._proxy_manager.get_http_proxy_url()
        if http_proxy:
            self._sticky_proxy = http_proxy
            self._sticky_requests_count = 1
            proxy_obj = self._proxy_manager.get_proxy()
            if proxy_obj:
                logger.info(
                    f"New sticky proxy: {proxy_obj.host}:{proxy_obj.port} "
                    f"(available: {self._proxy_manager.available_count}/{self._proxy_manager.total_count})"
                )
            return self._sticky_proxy

        return None

    def _reset_sticky_proxy(self) -> None:
        """Force rotation to new proxy on next request (e.g., after 403)."""
        logger.debug("Resetting sticky proxy due to error")
        self._sticky_proxy = None
        self._sticky_requests_count = 0

    def _get_reader(self):
        """
        Get soccerdata FBref reader instance.

        Returns:
            soccerdata.FBref instance
        """
        if self._reader is None:
            try:
                import soccerdata as sd
            except ImportError:
                raise ImportError(
                    "soccerdata library is required. "
                    "Install with: pip install soccerdata"
                )

            reader_kwargs = {
                'leagues': self.leagues,
                'seasons': self.seasons,
                'no_cache': self.no_cache,
            }

            if self._sd_proxy:
                reader_kwargs['proxy'] = self._sd_proxy

            self._reader = sd.FBref(**reader_kwargs)
            logger.info(f"Created soccerdata FBref reader with proxy={self._sd_proxy}")

        return self._reader

    def _reset_reader(self) -> None:
        """Reset reader instance (e.g., after proxy rotation)."""
        self._reader = None

    def _rotate_proxy(self) -> bool:
        """
        Rotate to next proxy if available.

        Returns:
            True if rotation successful, False otherwise
        """
        if self._proxy_manager and self._proxy_manager.available_count > 0:
            http_proxy = self._proxy_manager.get_http_proxy_url()
            if http_proxy:
                self._sd_proxy = http_proxy
                self._reset_reader()
                proxy_obj = self._proxy_manager.get_proxy()
                if proxy_obj:
                    logger.info(
                        f"Rotated to proxy: {proxy_obj.host}:{proxy_obj.port} "
                        f"(available: {self._proxy_manager.available_count}/{self._proxy_manager.total_count})"
                    )
                return True
        return False

    def _record_proxy_result(self, success: bool) -> None:
        """
        Record proxy result for current proxy.

        Args:
            success: Whether the request was successful
        """
        if self._proxy_manager:
            # Get the last used proxy (current one)
            for proxy in self._proxy_manager._proxies:
                if not proxy.is_banned:
                    self._proxy_manager.record_result(proxy, success)
                    break

    def _classify_error(self, error: Exception) -> str:
        """
        Classify exception into error type.

        Checks both the exception message and the original cause (if chained).

        Args:
            error: Exception to classify

        Returns:
            Error type: 'rate_limit', 'forbidden', 'cloudflare', 'timeout', 'unknown'
        """
        # Build error string from exception and its cause chain
        error_parts = [str(error)]
        cause = getattr(error, '__cause__', None) or getattr(error, '__context__', None)
        while cause:
            error_parts.append(str(cause))
            cause = getattr(cause, '__cause__', None) or getattr(cause, '__context__', None)

        # Also include exception type name for better classification
        error_parts.append(type(error).__name__)

        error_str = ' '.join(error_parts).lower()

        # Rate limiting - need to wait longer
        if '429' in error_str or 'rate limit' in error_str or 'too many' in error_str:
            return 'rate_limit'

        # Forbidden - switch proxy (check for HTTP 403 in various formats)
        # Note: soccerdata wraps 403 errors as "Could not download" ConnectionError
        # without preserving the original exception, so we treat download failures
        # on FBref as forbidden (Cloudflare 403)
        if any(x in error_str for x in [
            '403', 'forbidden', 'access denied',
            'http error 403', 'status_code=403',
            'could not download',  # soccerdata wrapper for 403
        ]):
            return 'forbidden'

        # Cloudflare challenge - try nodriver
        if any(x in error_str for x in [
            'cloudflare', 'challenge', 'captcha', 'turnstile',
            'checking your browser', 'just a moment'
        ]):
            return 'cloudflare'

        # Timeout
        if 'timeout' in error_str or 'timed out' in error_str:
            return 'timeout'

        return 'unknown'

    def _get_delay_for_error_type(
        self,
        error_type: str,
        base_delay: float,
        attempt: int,
        max_delay: float
    ) -> float:
        """
        Calculate appropriate delay based on error type.

        Args:
            error_type: Type of error
            base_delay: Base delay in seconds
            attempt: Current attempt number (0-indexed)
            max_delay: Maximum delay cap

        Returns:
            Delay in seconds
        """
        import random

        # Rate limit errors need longer delays
        if error_type == 'rate_limit':
            exp_delay = min(base_delay * (3 ** attempt), max_delay * 2)
            jitter = random.uniform(5.0, 15.0)
            return exp_delay + jitter

        # Cloudflare errors - moderate delay before nodriver fallback
        if error_type == 'cloudflare':
            return base_delay + random.uniform(2.0, 5.0)

        # Forbidden - standard exponential backoff
        if error_type == 'forbidden':
            exp_delay = min(base_delay * (2 ** attempt), max_delay)
            jitter = random.uniform(2.0, 8.0)
            return exp_delay + jitter

        # Default exponential backoff
        exp_delay = min(base_delay * (2 ** attempt), max_delay)
        jitter = random.uniform(2.0, 8.0)
        return exp_delay + jitter

    def _record_proxy_result_with_type(self, success: bool, error_type: Optional[str] = None) -> None:
        """
        Record proxy result with error type classification.

        Args:
            success: Whether the request was successful
            error_type: Type of error if failed
        """
        if not self._proxy_manager:
            return

        # Find current proxy
        for proxy in self._proxy_manager._proxies:
            if not proxy.is_banned:
                self._proxy_manager.record_result(
                    proxy,
                    success,
                    error_type=error_type
                )
                break

    def _inject_cf_cookies(self) -> bool:
        """
        Inject Cloudflare cookies into soccerdata session.

        Uses nodriver to solve Cloudflare Turnstile and extract cookies,
        then injects them into the current HTTP session.

        Returns:
            True if cookies were injected successfully
        """
        if not self._use_cf_cookie_injection:
            return False

        try:
            from scrapers.base.browser.cf_cookie_manager import CFCookieManager

            if self._cf_cookie_manager is None:
                self._cf_cookie_manager = CFCookieManager(
                    cache_ttl_minutes=25,
                    use_cf_verify=True,
                    cf_verify_max_retries=15,
                    cf_verify_interval=3.0,
                )

            # Get current proxy for nodriver
            proxy = self._sticky_proxy or self._sd_proxy

            # Get cookies using nodriver + cf-verify
            cookies = self._cf_cookie_manager.get_cookies_sync(
                url="https://fbref.com/en/",
                proxy=proxy,
                force_refresh=True,
            )

            if not cookies:
                logger.warning("No CF cookies obtained from nodriver")
                return False

            # Inject cookies into soccerdata reader session
            reader = self._get_reader()
            if hasattr(reader, '_session') and reader._session is not None:
                reader._session.cookies.update(cookies)
                logger.info(
                    f"Injected {len(cookies)} CF cookies into soccerdata session: "
                    f"{list(cookies.keys())}"
                )
                self._cf_cookies_injected = True
                return True
            else:
                logger.warning("Cannot inject cookies: reader._session not available")
                return False

        except ImportError as e:
            logger.warning(f"CF cookie injection not available: {e}")
            return False
        except Exception as e:
            logger.error(f"Error injecting CF cookies: {e}")
            return False

    def _inject_cf_cookies_from_xcom(self, cookies: Dict[str, str]) -> bool:
        """
        Inject pre-warmed CF cookies from Airflow XCom.

        Args:
            cookies: Dict с CF cookies из prewarm task

        Returns:
            True если успешно, False иначе
        """
        if not cookies or 'cf_clearance' not in cookies:
            logger.warning("No valid CF cookies provided from XCom")
            return False

        try:
            reader = self._get_reader()
            if hasattr(reader, '_session') and reader._session is not None:
                reader._session.cookies.update(cookies)
                logger.info(
                    f"Injected pre-warmed CF cookies from XCom: "
                    f"{list(cookies.keys())}"
                )
                self._cf_cookies_injected = True
                return True
            else:
                logger.warning("Cannot inject XCom cookies: reader._session not available")
                return False
        except Exception as e:
            logger.error(f"Error injecting XCom cookies: {e}")
            return False

    def _safe_call_with_retry(
        self,
        method_name: str,
        max_retries: int = 5,
        base_delay: float = 15.0,
        *args,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        Call soccerdata method with sticky proxy and intelligent retry on 403 errors.

        Uses sticky sessions to maintain same proxy for multiple requests,
        with human-like delays and jitter to avoid detection.

        Features:
        - Error type classification (rate_limit, forbidden, cloudflare)
        - Adaptive delays based on error type
        - Automatic fallback to nodriver for Cloudflare blocks
        - CF cookie injection after 2 failed attempts

        Args:
            method_name: Name of the reader method
            max_retries: Maximum retry attempts
            base_delay: Base delay between retries (exponential backoff, default 15s)
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            DataFrame or None if all retries exhausted
        """
        import random

        last_error = None
        max_delay = 60.0  # Cap exponential backoff
        cloudflare_failures = 0
        cf_cookie_injection_attempted = False

        for attempt in range(max_retries):
            try:
                # Use sticky proxy if available
                sticky_proxy = self._get_sticky_proxy()
                if sticky_proxy and self._sd_proxy != sticky_proxy:
                    self._sd_proxy = sticky_proxy
                    self._reset_reader()

                reader = self._get_reader()
                if not hasattr(reader, method_name):
                    logger.error(f"Reader has no method: {method_name}")
                    return None

                method = getattr(reader, method_name)

                # Execute with rate limiting
                start_time = time.time()
                self._rate_limiter.acquire()
                result = method(*args, **kwargs)
                response_time = time.time() - start_time

                # Success - record it with response time
                self._record_proxy_result_with_type(success=True)
                self._stats['successes'] += 1

                logger.info(
                    f"Request {method_name} succeeded in {response_time:.2f}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )

                # Add post-request delay (human-like behavior)
                post_delay = random.uniform(3.0, 8.0)
                logger.debug(f"Post-request delay: {post_delay:.1f}s")
                time.sleep(post_delay)

                return result

            except Exception as e:
                last_error = e
                error_type = self._classify_error(e)

                logger.warning(
                    f"Request {method_name} failed (attempt {attempt + 1}/{max_retries}): "
                    f"error_type={error_type}, error={e}"
                )

                # Record failure with error type
                self._record_proxy_result_with_type(success=False, error_type=error_type)
                self._stats['failures'] += 1

                # Track Cloudflare failures for nodriver fallback
                if error_type in ('cloudflare', 'forbidden'):
                    cloudflare_failures += 1

                # Try CF cookie injection after 2 failed attempts
                if (
                    self._use_cf_cookie_injection
                    and not cf_cookie_injection_attempted
                    and cloudflare_failures >= 2
                    and error_type in ('cloudflare', 'forbidden')
                ):
                    logger.info(
                        f"Multiple Cloudflare blocks ({cloudflare_failures}), "
                        f"attempting CF cookie injection..."
                    )
                    cf_cookie_injection_attempted = True

                    if self._inject_cf_cookies():
                        logger.info("CF cookies injected, retrying request...")
                        # Reset reader to use new cookies
                        self._reset_reader()
                        # Short delay before retry with new cookies
                        time.sleep(3)
                        continue

                # Check if should try nodriver fallback (after cookie injection failed)
                if (
                    self._use_nodriver_fallback
                    and error_type == 'cloudflare'
                    and cloudflare_failures >= 3
                ):
                    logger.info(
                        f"Multiple Cloudflare blocks ({cloudflare_failures}), "
                        f"nodriver fallback may be needed..."
                    )
                    # Nodriver fallback will be handled by caller if needed
                    # For now, continue with retries but signal the issue
                    self._stats['cloudflare_blocks'] = self._stats.get('cloudflare_blocks', 0) + 1

                # Force rotation on blocking errors
                if error_type in ('forbidden', 'cloudflare', 'rate_limit'):
                    self._reset_sticky_proxy()

                    # Try rotating proxy
                    if self._rotate_proxy():
                        delay = self._get_delay_for_error_type(
                            error_type, base_delay, attempt, max_delay
                        )
                        logger.info(f"Waiting {delay:.1f}s before retry with new proxy...")
                        time.sleep(delay)
                    else:
                        # No more proxies available
                        if self._proxy_manager and self._proxy_manager.available_count == 0:
                            logger.warning("All proxies exhausted, unbanning and retrying...")
                            self._proxy_manager.unban_all()
                            self._rotate_proxy()
                            # Longer delay when recycling proxies
                            time.sleep(base_delay * 3)
                        else:
                            time.sleep(base_delay)
                else:
                    # Non-blocking error (timeout, connection, unknown)
                    delay = self._get_delay_for_error_type(
                        error_type, base_delay, attempt, max_delay
                    )
                    logger.info(f"Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)

        # Provide helpful error message for common issues
        error_type = self._classify_error(last_error) if last_error else 'unknown'
        if error_type == 'forbidden':
            logger.error(
                f"All retries exhausted for {method_name}: FBref returned 403 Forbidden. "
                f"This is likely due to Cloudflare Turnstile CAPTCHA protection. "
                f"Options: 1) Integrate CAPTCHA-solving service (2captcha, anti-captcha), "
                f"2) Use alternative data source (Understat, FotMob), "
                f"3) Wait and retry later. Error: {last_error}"
            )
        else:
            logger.error(f"All retries exhausted for {method_name}: {last_error}")
        return None

    def read_schedule(
        self,
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read match schedule/fixtures using soccerdata.

        Args:
            league: League name (uses first configured if not specified)
            season: Season year (uses first configured if not specified)

        Returns:
            DataFrame with schedule data
        """
        try:
            # Use retry with proxy rotation for resilience against 403 errors
            df = self._safe_call_with_retry('read_schedule', max_retries=5, base_delay=5.0)

            if df is None or df.empty:
                logger.warning("No schedule data returned from soccerdata")
                return None

            # Reset MultiIndex if present
            if isinstance(df.index, pd.MultiIndex):
                df = df.reset_index()

            # Add metadata
            df = self._add_metadata(df, 'schedule')

            logger.info(f"Read {len(df)} schedule entries via soccerdata")
            return df

        except Exception as e:
            logger.error(f"Error reading schedule: {e}")
            return None

    def read_team_season_stats(
        self,
        stat_type: str = 'standard',
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read team/squad statistics using soccerdata.

        Args:
            stat_type: Type of statistics (standard, shooting, passing, etc.)
            league: League name
            season: Season year

        Returns:
            DataFrame with team stats
        """
        # Map stat_type to soccerdata method
        stat_type_map = {
            'stats': 'standard',
            'standard': 'standard',
            'shooting': 'shooting',
            'passing': 'passing',
            'passing_types': 'passing_types',
            'gca': 'goal_shot_creation',
            'defense': 'defense',
            'possession': 'possession',
            'playingtime': 'playing_time',
            'misc': 'misc',
        }

        sd_stat_type = stat_type_map.get(stat_type, stat_type)

        try:
            # Use retry with proxy rotation for resilience against 403 errors
            df = self._safe_call_with_retry(
                'read_team_season_stats',
                max_retries=5,
                base_delay=5.0,
                stat_type=sd_stat_type
            )

            if df is None or df.empty:
                logger.warning(f"No team stats ({stat_type}) returned from soccerdata")
                return None

            # Reset MultiIndex if present
            if isinstance(df.index, pd.MultiIndex):
                df = df.reset_index()

            # Flatten MultiIndex columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(str(c) for c in col).strip('_') for col in df.columns]

            # Add metadata
            df['stat_type'] = stat_type
            df = self._add_metadata(df, f'team_stats_{stat_type}')

            logger.info(f"Read {len(df)} team stat entries ({stat_type}) via soccerdata")
            return df

        except Exception as e:
            logger.error(f"Error reading team stats ({stat_type}): {e}")
            return None

    def read_player_season_stats(
        self,
        stat_type: str = 'standard',
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read player statistics using soccerdata.

        Args:
            stat_type: Type of statistics (standard, shooting, passing, etc.)
            league: League name
            season: Season year

        Returns:
            DataFrame with player stats
        """
        # Map stat_type to soccerdata method
        stat_type_map = {
            'stats': 'standard',
            'standard': 'standard',
            'shooting': 'shooting',
            'passing': 'passing',
            'passing_types': 'passing_types',
            'gca': 'goal_shot_creation',
            'defense': 'defense',
            'possession': 'possession',
            'playingtime': 'playing_time',
            'misc': 'misc',
        }

        sd_stat_type = stat_type_map.get(stat_type, stat_type)

        try:
            # Use retry with proxy rotation for resilience against 403 errors
            df = self._safe_call_with_retry(
                'read_player_season_stats',
                max_retries=5,
                base_delay=5.0,
                stat_type=sd_stat_type
            )

            if df is None or df.empty:
                logger.warning(f"No player stats ({stat_type}) returned from soccerdata")
                return None

            # Reset MultiIndex if present
            if isinstance(df.index, pd.MultiIndex):
                df = df.reset_index()

            # Flatten MultiIndex columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(str(c) for c in col).strip('_') for col in df.columns]

            # Add metadata
            df['stat_type'] = stat_type
            df = self._add_metadata(df, f'player_stats_{stat_type}')

            logger.info(f"Read {len(df)} player stat entries ({stat_type}) via soccerdata")
            return df

        except Exception as e:
            logger.error(f"Error reading player stats ({stat_type}): {e}")
            return None

    def read_keeper_stats(
        self,
        stat_type: str = 'keeper',
        league: str = None,
        season: int = None
    ) -> Optional[pd.DataFrame]:
        """
        Read goalkeeper statistics using soccerdata.

        Args:
            stat_type: Type of keeper statistics (keeper, keeper_adv)
            league: League name
            season: Season year

        Returns:
            DataFrame with goalkeeper stats
        """
        # Map to soccerdata method
        sd_stat_type = 'keepers_adv' if stat_type == 'keeper_adv' else 'keepers'

        try:
            # Use retry with proxy rotation for resilience against 403 errors
            df = self._safe_call_with_retry(
                'read_player_season_stats',
                max_retries=5,
                base_delay=5.0,
                stat_type=sd_stat_type
            )

            if df is None or df.empty:
                logger.warning(f"No keeper stats ({stat_type}) returned from soccerdata")
                return None

            # Reset MultiIndex if present
            if isinstance(df.index, pd.MultiIndex):
                df = df.reset_index()

            # Flatten MultiIndex columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(str(c) for c in col).strip('_') for col in df.columns]

            # Add metadata
            df['stat_type'] = stat_type
            df = self._add_metadata(df, f'keeper_stats_{stat_type}')

            logger.info(f"Read {len(df)} keeper stat entries ({stat_type}) via soccerdata")
            return df

        except Exception as e:
            logger.error(f"Error reading keeper stats ({stat_type}): {e}")
            return None

    def scrape_single_stat_type(
        self,
        stat_type: str,
        data_category: str,
    ) -> Dict[str, str]:
        """
        Memory-efficient: scrape single stat_type for all leagues/seasons.

        Args:
            stat_type: One of PLAYER_STAT_TYPES, TEAM_STAT_TYPES, or KEEPER_STAT_TYPES
            data_category: One of 'player', 'team', or 'keeper'

        Returns:
            Dictionary mapping '{data_category}_{stat_type}' to Iceberg table path
        """
        logger.info(
            f"Starting soccerdata single stat_type scrape: category={data_category}, "
            f"stat_type={stat_type}, leagues={self.leagues}, seasons={self.seasons}"
        )

        try:
            df = None

            if data_category == 'player':
                df = self.read_player_season_stats(stat_type)
            elif data_category == 'team':
                df = self.read_team_season_stats(stat_type)
            elif data_category == 'keeper':
                df = self.read_keeper_stats(stat_type)
            else:
                logger.error(f"Unknown data_category: {data_category}")
                return {}

            if df is None or df.empty:
                logger.warning(f"No data collected for {data_category}_{stat_type}")
                return {}

            # Add league/season columns if not present
            if 'league' not in df.columns and len(self.leagues) == 1:
                df['league'] = self.leagues[0]
            if 'season' not in df.columns and len(self.seasons) == 1:
                df['season'] = self.seasons[0]

            table_name = f'fbref_{data_category}_{stat_type}'
            table_path = self.save_to_iceberg(
                df=df,
                table_name=table_name,
                partition_cols=['league', 'season'] if 'league' in df.columns and 'season' in df.columns else None,
            )

            key = f'{data_category}_{stat_type}'
            logger.info(
                f"Saved {len(df)} rows to {table_name} via soccerdata"
            )

            return {key: table_path}

        except Exception as e:
            logger.error(f"Error in scrape_single_stat_type: {e}")
            return {}

    def scrape_match_data(
        self,
        data_type: str,
        max_matches: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Memory-efficient: scrape match-level data for all leagues/seasons.

        Args:
            data_type: One of 'schedule', 'shot_events', 'match_events', 'lineups'
            max_matches: Maximum number of matches (not used for soccerdata)

        Returns:
            Dictionary mapping data_type to Iceberg table path
        """
        logger.info(
            f"Starting soccerdata match data scrape: type={data_type}, "
            f"leagues={self.leagues}, seasons={self.seasons}"
        )

        results = {}

        if data_type == 'schedule':
            df = self.read_schedule()
            if df is not None and not df.empty:
                # Add league/season columns if not present
                if 'league' not in df.columns and len(self.leagues) == 1:
                    df['league'] = self.leagues[0]
                if 'season' not in df.columns and len(self.seasons) == 1:
                    df['season'] = self.seasons[0]

                _has_partitions = 'league' in df.columns and 'season' in df.columns
                table_path = self.save_to_iceberg(
                    df=df,
                    table_name='fbref_schedule',
                    partition_cols=['league', 'season'] if _has_partitions else None,
                    replace_partitions=['league', 'season'] if _has_partitions else None,
                )
                results['schedule'] = table_path
                logger.info(f"Saved {len(df)} schedule rows via soccerdata")
        else:
            # For shot_events, match_events, lineups - not directly supported by soccerdata
            # Fall back to message for now
            logger.warning(
                f"Match data type '{data_type}' not directly supported by soccerdata. "
                f"Use Selenium scraper for detailed match data."
            )

        return results

    def scrape_all(
        self,
        include_extended_stats: bool = True,
        include_keeper_stats: bool = True,
        include_team_stats_extended: bool = True,
        **kwargs  # Accept but ignore Selenium-specific kwargs
    ) -> Dict[str, str]:
        """
        Scrape all FBref data using soccerdata library.

        Note: This method only collects season-level statistics.
        For match-level data (shot_events, match_events, lineups),
        use the Selenium-based FBrefScraper.

        Args:
            include_extended_stats: Collect extended player stats (all stat_types)
            include_keeper_stats: Collect goalkeeper statistics
            include_team_stats_extended: Collect extended team stats
            **kwargs: Additional arguments (ignored for compatibility)

        Returns:
            Dictionary mapping data type to Iceberg table path
        """
        logger.info(
            f"Starting soccerdata FBref scrape: leagues={self.leagues}, "
            f"seasons={self.seasons}, extended={include_extended_stats}, "
            f"keeper={include_keeper_stats}"
        )

        results = {}

        try:
            # Schedule
            schedule_df = self.read_schedule()
            if schedule_df is not None and not schedule_df.empty:
                _sched_has_partitions = (
                    'league' in schedule_df.columns
                    and 'season' in schedule_df.columns
                )
                table_path = self.save_to_iceberg(
                    df=schedule_df,
                    table_name='fbref_schedule',
                    partition_cols=['league', 'season'] if _sched_has_partitions else None,
                    replace_partitions=['league', 'season'] if _sched_has_partitions else None,
                )
                results['schedule'] = table_path

            time.sleep(3)

            # Team stats (standard)
            team_df = self.read_team_season_stats('stats')
            if team_df is not None and not team_df.empty:
                table_path = self.save_to_iceberg(
                    df=team_df,
                    table_name='fbref_team_stats',
                    partition_cols=['league', 'season'] if 'league' in team_df.columns else None,
                )
                results['team_stats'] = table_path

            time.sleep(3)

            # Extended team stats
            if include_team_stats_extended:
                for stat_type in TEAM_STAT_TYPES:
                    df = self.read_team_season_stats(stat_type)
                    if df is not None and not df.empty:
                        table_path = self.save_to_iceberg(
                            df=df,
                            table_name=f'fbref_team_{stat_type}',
                            partition_cols=['league', 'season'] if 'league' in df.columns else None,
                        )
                        results[f'team_{stat_type}'] = table_path
                    time.sleep(3)
                    gc.collect()

            # Player stats (standard)
            player_df = self.read_player_season_stats('stats')
            if player_df is not None and not player_df.empty:
                table_path = self.save_to_iceberg(
                    df=player_df,
                    table_name='fbref_player_stats',
                    partition_cols=['league', 'season'] if 'league' in player_df.columns else None,
                )
                results['player_stats'] = table_path

            time.sleep(3)

            # Extended player stats
            if include_extended_stats:
                for stat_type in PLAYER_STAT_TYPES:
                    df = self.read_player_season_stats(stat_type)
                    if df is not None and not df.empty:
                        table_path = self.save_to_iceberg(
                            df=df,
                            table_name=f'fbref_player_{stat_type}',
                            partition_cols=['league', 'season'] if 'league' in df.columns else None,
                        )
                        results[f'player_{stat_type}'] = table_path
                    time.sleep(3)
                    gc.collect()

            # Keeper stats
            if include_keeper_stats:
                for stat_type in KEEPER_STAT_TYPES:
                    df = self.read_keeper_stats(stat_type)
                    if df is not None and not df.empty:
                        table_path = self.save_to_iceberg(
                            df=df,
                            table_name=f'fbref_keeper_{stat_type}',
                            partition_cols=['league', 'season'] if 'league' in df.columns else None,
                        )
                        results[f'keeper_{stat_type}'] = table_path
                    time.sleep(3)
                    gc.collect()

        except Exception as e:
            logger.error(f"Error in scrape_all: {e}")

        logger.info(f"Soccerdata FBref scrape complete: {list(results.keys())}")
        return results

    def close(self) -> None:
        """Cleanup resources with explicit memory management."""
        # Clear reader reference
        self._reader = None

        # Cleanup nodriver instance if exists
        if self._nodriver_instance is not None:
            try:
                self._nodriver_instance.close_sync()
            except Exception as e:
                logger.warning(f"Error closing nodriver: {e}")
            finally:
                self._nodriver_instance = None

        # Cleanup CF cookie manager
        if self._cf_cookie_manager is not None:
            try:
                self._cf_cookie_manager.clear_cache()
            except Exception:
                pass
            self._cf_cookie_manager = None

        # Clear proxy manager references to allow GC
        if self._proxy_manager is not None:
            try:
                self._proxy_manager._proxies.clear()
            except Exception:
                pass

        # Reset sticky proxy state
        self._sticky_proxy = None
        self._sticky_requests_count = 0

        # Force garbage collection to release memory
        gc.collect()

        super().close()

    def get_proxy_stats(self) -> Dict[str, Any]:
        """
        Get detailed proxy statistics.

        Returns:
            Dictionary with proxy pool statistics
        """
        if not self._proxy_manager:
            return {'message': 'No proxy manager configured'}

        stats = self._proxy_manager.get_stats()
        stats['scraper_stats'] = {
            'successes': self._stats.get('successes', 0),
            'failures': self._stats.get('failures', 0),
            'cloudflare_blocks': self._stats.get('cloudflare_blocks', 0),
        }
        return stats
