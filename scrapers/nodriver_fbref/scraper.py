"""
NodriverFBrefScraper
====================

FBref scraper using nodriver for Cloudflare Turnstile bypass.

This is the primary scraper for FBref data. It uses:
- DrissionPage as primary bypass (no WebDriver signature)
- nodriver (successor to undetected-chromedriver) as fallback
- cf-verify plugin for automatic Turnstile checkbox clicking
- Residential proxies for IP rotation
- Xvfb for headless detection bypass

Fallback chain:
1. DrissionPage (no WebDriver) - highest success rate
2. Nodriver with enhanced bypass
3. Rotate proxy and retry

Usage:
    scraper = NodriverFBrefScraper(
        leagues=['ENG-Premier League'],
        seasons=[2024],
        proxy_file='proxys.txt',
    )
    result = scraper.scrape_single_stat_type('stats', 'player')

NOTE: soccerdata and curl_cffi do NOT work for FBref as of 2025-2026.
      Cloudflare Turnstile requires JavaScript execution.
"""

import asyncio
import gc
import logging
import time
from collections import Counter
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from scrapers.base.browser.nodriver_bypass import NodriverBypass, SlowProxyError
from scrapers.fbref.constants import (
    BASE_URL,
    FBREF_UNCOMMENT_TABLES_JS,
    LEAGUE_IDS,
    PLAYER_STAT_TYPES,
    TEAM_STAT_TYPES,
    KEEPER_STAT_TYPES,
    DEFAULT_RATE_LIMIT,
)
from scrapers.fbref.url_builder import (
    format_season,
    get_schedule_url,
    get_stats_url,
)
from scrapers.fbref.html_parser import (
    extract_tables_from_comments,
    parse_table,
    find_schedule_table,
    find_team_stats_table,
    find_player_stats_table,
    parse_shots_table,
    parse_lineup_table,
    parse_events_from_scorebox,
    diagnose_html_structure,
)
from scrapers.base.base_scraper import BaseScraper
from scrapers.utils.proxy_manager import ProxyManager, ProxyType

logger = logging.getLogger(__name__)


class NodriverFBrefScraper(BaseScraper):
    """
    FBref scraper using nodriver for Cloudflare bypass.

    This scraper is designed specifically for FBref which uses Cloudflare
    Turnstile CAPTCHA. It uses:
    - nodriver for browser automation (async, better fingerprint evasion)
    - cf-verify plugin for automatic Turnstile checkbox clicking
    - Residential proxies for IP rotation
    - Xvfb for headless detection bypass
    - Memory-efficient single stat_type collection

    Key differences from FBrefScraper:
    - Uses only nodriver (no Selenium fallback)
    - Async-first architecture
    - Better memory management
    - Aggressive garbage collection
    """

    SOURCE_NAME = 'fbref'
    DEFAULT_RATE_LIMIT = DEFAULT_RATE_LIMIT

    # Memory management - reduced to prevent OOM on limited RAM systems
    MAX_PAGES_BEFORE_RESTART = 10  # Was 30, restart more often to free memory
    MIN_REQUEST_INTERVAL = 5.0  # seconds between requests

    # nodriver settings
    CLOUDFLARE_WAIT = 30.0
    PAGE_LOAD_TIMEOUT = 180.0
    CONTENT_TIMEOUT = 45.0  # Timeout per evaluate call (increased for CDP DOM fallback)
    MAX_RETRIES = 2  # Per-proxy retries (reduced from 5, proxy rotation handles CF blocks)
    MAX_CF_RETRIES = 6  # Retries with different proxies for CF bypass
    CF_VERIFY_MAX_RETRIES = 6
    CF_VERIFY_INTERVAL = 2.5

    def __init__(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[int]] = None,
        proxy_file: Optional[str] = None,
        headless: bool = True,
        use_xvfb: bool = True,
        output_dir: str = '/data/bronze/fbref',
        cloudflare_wait: float = CLOUDFLARE_WAIT,
        max_retries: int = MAX_RETRIES,
        cf_verify_max_retries: int = CF_VERIFY_MAX_RETRIES,
        content_timeout: float = CONTENT_TIMEOUT,
        cf_cookies_file: Optional[str] = None,
    ):
        """
        Initialize NodriverFBrefScraper.

        Args:
            leagues: List of leagues to scrape (e.g., ['ENG-Premier League'])
            seasons: List of seasons (e.g., [2024] for 2024-2025)
            proxy_file: Path to proxy file (format: host:port:user:pass)
            headless: Run browser in headless mode (with Xvfb bypass)
            use_xvfb: Use Xvfb virtual display for headless detection bypass
            output_dir: Directory for output files
            cloudflare_wait: Time to wait for Cloudflare challenge (seconds)
            max_retries: Maximum retry attempts for page loads
            cf_verify_max_retries: Maximum retries for cf-verify plugin
            content_timeout: Timeout for content extraction (seconds, default 45).
        """
        # proxy_file=None in super — nodriver manages its own ProxyManager
        # with weighted strategy, min_success_rate=0.3, cooldown_seconds=30.0
        super().__init__(
            leagues=leagues or ['ENG-Premier League'],
            seasons=seasons or [2024],
            proxy_file=None,
            rate_limit=self.DEFAULT_RATE_LIMIT,
        )

        self.proxy_file = proxy_file
        self.headless = headless
        self.use_xvfb = use_xvfb
        self.output_dir = output_dir
        self.cloudflare_wait = cloudflare_wait
        self.max_retries = max_retries
        self.cf_verify_max_retries = cf_verify_max_retries
        self.content_timeout = content_timeout
        # Issue #118: inter-process CF cookie cache file (set by run_fbref_scraper
        # from --cf-cookies-file); forwarded to each NodriverBypass instance.
        self.cf_cookies_file = cf_cookies_file

        # Initialize proxy manager (nodriver-specific: weighted strategy)
        if proxy_file:
            self._init_proxy_manager(proxy_file)

        # Browser instance (lazy init)
        self._browser: Optional[NodriverBypass] = None
        self._pages_fetched = 0
        self._last_request_time = 0.0

        # Extend BaseScraper stats with nodriver-specific keys
        self._stats.update({
            'cloudflare_blocked': 0,
            'proxy_rotations': 0,
        })

        # Issue #131: real-traffic accumulator base — preserves proxy bytes /
        # CF counters across browser restarts (each new NodriverBypass resets
        # its own counters to 0). Flushed in _close_browser() before the
        # browser is torn down; read back in _update_real_traffic_stats().
        # Mirrors FBrefBrowserMixin (scrapers/fbref/browser_manager.py).
        self._real_traffic_base_bytes: int = 0
        self._real_traffic_base_requests: int = 0
        self._real_traffic_base_bytes_by_rtype: Counter = Counter()
        self._real_traffic_base_requests_by_rtype: Counter = Counter()
        self._cf_challenge_attempts_base: int = 0
        self._cf_challenges_passed_base: int = 0
        self._cf_challenges_failed_base: int = 0
        self._restart_reasons_base: Counter = Counter()
        self._resource_type_cache_misses_base: int = 0

        logger.info(
            f"NodriverFBrefScraper initialized: "
            f"leagues={self.leagues}, seasons={self.seasons}, "
            f"proxy_file={proxy_file}, headless={headless}, use_xvfb={use_xvfb}"
        )

    def _init_proxy_manager(self, proxy_file: str) -> None:
        """Initialize proxy manager from file."""
        self._proxy_manager = ProxyManager(
            rotation_strategy='weighted',
            min_success_rate=0.3,
            cooldown_seconds=30.0,
        )
        count = self._proxy_manager.load_from_file_custom_format(
            proxy_file, ProxyType.HTTP
        )
        logger.info(f"Loaded {count} proxies from {proxy_file}")

    def _get_proxy_string(self) -> Optional[str]:
        """Get next proxy in nodriver format (host:port:user:pass)."""
        if self._proxy_manager and self._proxy_manager.available_count > 0:
            proxy_str = self._proxy_manager.get_nodriver_proxy_string()
            if proxy_str:
                self._stats['proxy_rotations'] += 1
                logger.debug(f"Using proxy: {proxy_str.split(':')[0]}:****")
                return proxy_str
        return None

    def _create_browser(
        self,
        slow_proxy_threshold: float = 45.0,
        max_retries: int = None,
        wait_for_selector_timeout: float = 60.0,
    ) -> NodriverBypass:
        """Create new nodriver browser instance."""
        proxy_str = self._get_proxy_string()

        browser = NodriverBypass(
            headless=self.headless,
            use_xvfb=self.use_xvfb,
            proxy=proxy_str,
            cloudflare_wait=self.cloudflare_wait,
            page_load_timeout=self.PAGE_LOAD_TIMEOUT,
            max_retries=max_retries if max_retries is not None else self.max_retries,
            use_cf_verify=True,
            cf_verify_max_retries=self.cf_verify_max_retries,
            cf_verify_interval=self.CF_VERIFY_INTERVAL,
            wait_for_selector='#content',
            wait_for_selector_timeout=wait_for_selector_timeout,
            content_timeout=self.content_timeout,
            # wait_for_content_js removed: polling via evaluate hangs when Runtime
            # is unresponsive after CF bypass. Tables from HTML comments are extracted
            # in Python by extract_tables_from_comments() regardless.
            pre_content_js=FBREF_UNCOMMENT_TABLES_JS,
            slow_proxy_threshold=slow_proxy_threshold,
            cf_cookies_file=self.cf_cookies_file,
        )

        logger.debug(
            f"Created nodriver browser: "
            f"headless={self.headless}, use_xvfb={self.use_xvfb}, "
            f"cloudflare_wait={self.cloudflare_wait}s, "
            f"content_timeout={self.content_timeout}s, "
            f"cf_verify_retries={self.cf_verify_max_retries}"
        )

        return browser

    def _get_browser(self) -> NodriverBypass:
        """Get or create browser instance."""
        if self._browser is None:
            self._browser = self._create_browser()
        return self._browser

    def _flush_browser_traffic(self) -> None:
        """Accumulate live browser traffic counters into the persistent base.

        Issue #131: each NodriverBypass instance tracks proxy bytes / CF
        challenges only for its own lifetime and resets on restart. Call this
        BEFORE tearing the browser down (in _close_browser) so the numbers
        survive restarts and reach the runner diagnostics. Mirrors the flush
        block in scrapers/fbref/browser_manager.py::_close_browser.
        """
        if self._browser is None or not hasattr(self._browser, 'get_real_traffic_stats'):
            return
        try:
            real = self._browser.get_real_traffic_stats()
            self._real_traffic_base_bytes += real.get('real_bytes_downloaded', 0)
            self._real_traffic_base_requests += real.get('real_requests_count', 0)
            for k, v in (real.get('real_bytes_by_resource_type') or {}).items():
                self._real_traffic_base_bytes_by_rtype[k] += v
            for k, v in (real.get('real_requests_by_resource_type') or {}).items():
                self._real_traffic_base_requests_by_rtype[k] += v
            self._cf_challenge_attempts_base += int(real.get('cf_challenge_attempts', 0) or 0)
            self._cf_challenges_passed_base += int(real.get('cf_challenges_passed', 0) or 0)
            self._cf_challenges_failed_base += int(real.get('cf_challenges_failed', 0) or 0)
            self._resource_type_cache_misses_base += int(
                real.get('resource_type_cache_misses', 0) or 0
            )
            for k, v in (real.get('restart_reasons') or {}).items():
                self._restart_reasons_base[k] += v
            if real.get('real_bytes_downloaded', 0) > 0:
                logger.info(
                    f"Session proxy traffic: "
                    f"{real['real_bytes_downloaded'] / 1024 / 1024:.1f} MB "
                    f"over {real.get('real_requests_count', 0)} requests "
                    f"(total accumulated: "
                    f"{self._real_traffic_base_bytes / 1024 / 1024:.1f} MB)"
                )
        except Exception as e:
            logger.debug(f"Could not flush traffic stats: {e}")

    def _update_real_traffic_stats(self) -> None:
        """Merge accumulated base + live browser counters into self._stats.

        Issue #131: the runner's _get_traffic_diagnostics() calls this hook
        (via hasattr) to pull a fresh snapshot before writing the traffic JSON.
        When called mid-session the browser is still alive (base may be 0), so
        we add the current session's counters on top of the accumulated base.
        Direct mirror of FBrefBrowserMixin._sync_real_traffic_stats().
        """
        session: Dict[str, Any] = {}
        if self._browser is not None and hasattr(self._browser, 'get_real_traffic_stats'):
            try:
                session = self._browser.get_real_traffic_stats()
            except Exception:
                session = {}

        self._stats['real_bytes_downloaded'] = (
            self._real_traffic_base_bytes + session.get('real_bytes_downloaded', 0)
        )
        self._stats['real_requests_count'] = (
            self._real_traffic_base_requests + session.get('real_requests_count', 0)
        )
        bytes_by_rtype = dict(self._real_traffic_base_bytes_by_rtype)
        reqs_by_rtype = dict(self._real_traffic_base_requests_by_rtype)
        for k, v in (session.get('real_bytes_by_resource_type') or {}).items():
            bytes_by_rtype[k] = bytes_by_rtype.get(k, 0) + v
        for k, v in (session.get('real_requests_by_resource_type') or {}).items():
            reqs_by_rtype[k] = reqs_by_rtype.get(k, 0) + v
        self._stats['real_bytes_by_resource_type'] = bytes_by_rtype
        self._stats['real_requests_by_resource_type'] = reqs_by_rtype
        self._stats['cf_challenge_attempts'] = (
            self._cf_challenge_attempts_base
            + int(session.get('cf_challenge_attempts', 0) or 0)
        )
        self._stats['cf_challenges_passed'] = (
            self._cf_challenges_passed_base
            + int(session.get('cf_challenges_passed', 0) or 0)
        )
        self._stats['cf_challenges_failed'] = (
            self._cf_challenges_failed_base
            + int(session.get('cf_challenges_failed', 0) or 0)
        )
        restart_reasons = dict(self._restart_reasons_base)
        for k, v in (session.get('restart_reasons') or {}).items():
            restart_reasons[k] = restart_reasons.get(k, 0) + v
        self._stats['restart_reasons'] = restart_reasons
        self._stats['resource_type_cache_misses'] = (
            self._resource_type_cache_misses_base
            + int(session.get('resource_type_cache_misses', 0) or 0)
        )

    def _close_browser(self) -> None:
        """Close browser and cleanup with aggressive memory release."""
        if self._browser is not None:
            # Issue #131: snapshot proxy-traffic counters into the persistent
            # base before the browser (and its counters) go away.
            self._flush_browser_traffic()
            try:
                self._browser.close_sync()
                logger.info("Closed nodriver browser")
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
            finally:
                self._browser = None
                # Aggressive garbage collection to free Chromium memory
                # Double collect handles circular references
                gc.collect()
                gc.collect()

    def _restart_browser(self) -> None:
        """Restart browser with new proxy."""
        logger.info("Restarting browser with new proxy...")
        self._close_browser()
        gc.collect()
        self._pages_fetched = 0
        # Next _get_browser() will create new instance

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.MIN_REQUEST_INTERVAL:
            sleep_time = self.MIN_REQUEST_INTERVAL - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def _maybe_restart_browser(self) -> None:
        """Restart browser if page limit reached."""
        self._pages_fetched += 1
        if self._pages_fetched >= self.MAX_PAGES_BEFORE_RESTART:
            logger.info(
                f"Page limit reached ({self._pages_fetched}), restarting browser"
            )
            self._restart_browser()

    def _fetch_with_drissionpage(self, url: str) -> Optional[str]:
        """
        Try to fetch page using DrissionPage (no WebDriver signature).

        DrissionPage is the primary bypass method because it doesn't use
        the WebDriver protocol that Cloudflare detects.

        Args:
            url: URL to fetch

        Returns:
            Page HTML or None if failed
        """
        try:
            from scrapers.base.browser.drissionpage_bypass import DrissionPageBypass

            proxy_str = self._get_proxy_string()

            bypass = DrissionPageBypass(
                proxy=proxy_str,
                cloudflare_wait=self.cloudflare_wait,
                headless=self.headless,
                use_xvfb=self.use_xvfb,
                page_load_timeout=self.PAGE_LOAD_TIMEOUT,
            )

            logger.debug(f"DrissionPage attempt: {url}")
            html = bypass.get_page(url)

            if html and not self._is_cloudflare_blocked(html):
                if '<table' in html or len(html) > 5000:
                    logger.debug(f"DrissionPage success: {url} ({len(html)} bytes)")
                    self._stats['successes'] += 1

                    # Record success for proxy
                    if self._proxy_manager:
                        proxy = self._proxy_manager.get_current_proxy()
                        if proxy:
                            self._proxy_manager.record_result(proxy, success=True)

                    return html

            logger.warning("DrissionPage: Cloudflare blocked or invalid content")
            return None

        except ImportError as e:
            logger.warning(f"DrissionPage not installed: {e}")
            return None
        except Exception as e:
            logger.error(
                f"DrissionPage failed ({type(e).__name__}): {e}",
                exc_info=True  # Добавляем traceback для отладки
            )
            return None

    def _fetch_page(self, url: str, max_cf_retries: int = MAX_CF_RETRIES) -> Optional[str]:
        """
        Fetch page HTML with Cloudflare bypass and proxy rotation.

        Fallback chain:
        1. DrissionPage (no WebDriver signature) - highest success rate
        2. Nodriver with enhanced bypass
        3. Rotate proxy and retry

        When Cloudflare blocks the request, this method will restart the browser
        with a new proxy and retry. This is critical because Cloudflare remembers
        IP addresses and repeated failures from the same IP only make it worse.

        Args:
            url: URL to fetch
            max_cf_retries: Max retries with different proxies for CF bypass

        Returns:
            Page HTML or None if failed
        """
        # Check if proxy requires authentication
        proxy_requires_auth = False
        if self._proxy_manager and self._proxy_manager.available_count > 0:
            current_proxy = self._proxy_manager.get_current_proxy()
            if current_proxy and current_proxy.username:
                proxy_requires_auth = True

        # Phase 1: Try DrissionPage first ONLY if no auth required
        # DrissionPage does NOT support authenticated proxies - it will go direct!
        drissionpage_available = True
        if proxy_requires_auth:
            logger.debug(
                "Skipping DrissionPage: proxy requires authentication "
                "(DrissionPage does not support proxy auth)"
            )
            drissionpage_available = False
        else:
            try:
                html = self._fetch_with_drissionpage(url)
                if html:
                    logger.debug("DrissionPage SUCCESS")
                    return html
                logger.warning("DrissionPage returned None, falling back to nodriver")
            except ImportError:
                drissionpage_available = False
                logger.warning("DrissionPage not available, using nodriver only")

        if drissionpage_available:
            logger.debug("DrissionPage failed, falling back to nodriver...")

        # Phase 2: Fallback to nodriver with retry
        for cf_attempt in range(max_cf_retries):
            self._rate_limit()

            browser = self._get_browser()

            logger.info(f"Nodriver attempt: {url} (CF attempt {cf_attempt + 1}/{max_cf_retries})")

            try:
                html = browser.get_page(url, cloudflare_wait=self.cloudflare_wait)

                if not html:
                    logger.warning(f"Empty HTML from {url}")
                    self._stats['failures'] += 1
                    # Restart with new proxy and retry
                    self._restart_browser()
                    time.sleep(5)  # Wait before retry with new proxy
                    continue

                # Check for Cloudflare block
                if self._is_cloudflare_blocked(html):
                    logger.warning(
                        f"Cloudflare still blocking: {url} "
                        f"(CF attempt {cf_attempt + 1}/{max_cf_retries})"
                    )
                    self._stats['cloudflare_blocked'] += 1

                    # Record failure for proxy
                    if self._proxy_manager:
                        proxy = self._proxy_manager.get_current_proxy()
                        if proxy:
                            self._proxy_manager.record_result(
                                proxy, success=False, error_type='cloudflare'
                            )

                    # Restart with new proxy and retry (KEY: don't return None immediately)
                    self._restart_browser()
                    time.sleep(5)  # Wait before retry with new proxy
                    continue

                # Check for valid content — FBref stats pages are typically >500KB
                # with full JS-rendered tables. A 237KB page means JS hasn't
                # finished rendering (tables hidden in HTML comments).
                MIN_FBREF_PAGE_SIZE = 500_000
                if '<table' not in html and len(html) < MIN_FBREF_PAGE_SIZE:
                    logger.warning(
                        f"Page likely incomplete: {url} "
                        f"(len={len(html)}, no <table> tags, expected >{MIN_FBREF_PAGE_SIZE})"
                    )
                    # Save diagnostic dump
                    try:
                        diag_path = f'/tmp/fbref_incomplete_cf{cf_attempt}.html'
                        with open(diag_path, 'w', encoding='utf-8') as f:
                            f.write(html)
                        logger.debug(f"Saved incomplete page to {diag_path}")
                        logger.debug(f"Page start: {html[:300]}")
                    except Exception as e:
                        logger.warning(f"Failed to save diagnostic page: {e}")
                    self._stats['failures'] += 1
                    # Restart with new proxy and retry
                    self._restart_browser()
                    time.sleep(5)
                    continue

                # Success - valid content
                logger.info(f"Successfully fetched: {url} (len={len(html)})")
                self._stats['successes'] += 1

                # Record success for proxy
                if self._proxy_manager:
                    proxy = self._proxy_manager.get_current_proxy()
                    if proxy:
                        self._proxy_manager.record_result(proxy, success=True)

                self._maybe_restart_browser()
                return html

            except SlowProxyError as e:
                logger.warning(f"Slow proxy on CF attempt {cf_attempt + 1}/{max_cf_retries}: {e}")
                # Record failure for proxy with 'slow' error type
                if self._proxy_manager:
                    proxy = self._proxy_manager.get_current_proxy()
                    if proxy:
                        self._proxy_manager.record_result(
                            proxy, success=False, error_type='slow'
                        )
                # On last attempt: retry with slow proxy detection DISABLED
                # Better to proceed through a slow proxy than fail entirely
                if cf_attempt >= max_cf_retries - 1:
                    logger.warning(
                        "Last attempt — disabling slow proxy detection, "
                        "proceeding with best available proxy"
                    )
                    self._restart_browser()
                    # max_retries=1: don't waste time on internal retries with slow proxy
                    # wait_for_selector_timeout=30: halve selector wait since proxy is slow
                    self._browser = self._create_browser(
                        slow_proxy_threshold=0,
                        max_retries=1,
                        wait_for_selector_timeout=30.0,
                    )
                    try:
                        html = self._browser.get_page(url, cloudflare_wait=self.cloudflare_wait)
                        if html and not self._is_cloudflare_blocked(html):
                            logger.info(f"Success on last attempt (slow proxy): {url} (len={len(html)})")
                            self._stats['successes'] += 1
                            return html
                    except Exception as e2:
                        logger.error(f"Last attempt with slow proxy also failed: {e2}")
                    continue
                # Quick restart — proxy rotation, not CF recovery
                self._restart_browser()
                time.sleep(2)  # Shorter than 5s — just need new proxy
                continue

            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                self._stats['failures'] += 1

                # Record failure for proxy
                if self._proxy_manager:
                    proxy = self._proxy_manager.get_current_proxy()
                    if proxy:
                        self._proxy_manager.record_result(
                            proxy, success=False, error_type='unknown'
                        )

                # Restart browser and try again with new proxy
                self._restart_browser()
                time.sleep(5)
                continue

        logger.error(f"Failed to fetch {url} after DrissionPage + {max_cf_retries} nodriver attempts")
        return None

    def _is_cloudflare_blocked(self, html: str) -> bool:
        """Check if page is blocked by Cloudflare."""
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

    # =========================================================================
    # Data Collection Methods
    # =========================================================================

    def read_schedule(
        self,
        league: str,
        season: int
    ) -> Optional[pd.DataFrame]:
        """Read match schedule for league/season."""
        url = get_schedule_url(league, season)
        html = self._fetch_page(url)

        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        # Diagnostic logging
        diagnosis = diagnose_html_structure(soup)
        logger.info(
            f"Schedule HTML diagnosis: tables={diagnosis['total_tables']}, "
            f"comments={diagnosis['comment_count']}, "
            f"cloudflare={diagnosis['cloudflare_indicators']}"
        )

        if diagnosis['cloudflare_indicators']:
            logger.error(f"Cloudflare block in parsed HTML")
            return None

        comment_tables = extract_tables_from_comments(soup)
        season_str = format_season(season)
        league_info = LEAGUE_IDS.get(league, {})
        comp_id = league_info.get('comp_id', '9')

        df = find_schedule_table(soup, comment_tables, season_str, comp_id)

        if df is None or df.empty:
            logger.warning(
                f"No schedule found for {league} {season}. "
                f"Tables: {diagnosis['table_ids'][:5]}"
            )
            return None

        # match_url is populated inside find_schedule_table → parse_table
        # (mapped before row filtering so URLs stay aligned with fixtures, #241).
        if 'match_url' not in df.columns or not df['match_url'].notna().any():
            logger.warning(f"No match URLs extracted from schedule HTML for {league} {season}")

        df['league'] = league
        df['season'] = season
        df = self._add_metadata(df, 'schedule')

        logger.info(f"Parsed {len(df)} schedule entries for {league} {season}")
        return df

    def read_player_season_stats(
        self,
        stat_type: str,
        league: str,
        season: int
    ) -> Optional[pd.DataFrame]:
        """Read player statistics for league/season."""
        url = get_stats_url(league, season, stat_type, for_squads=False)
        html = self._fetch_page(url)

        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')

        # Diagnostic logging for HTML structure
        diagnosis = diagnose_html_structure(soup)
        logger.info(
            f"Player stats HTML diagnosis: tables={diagnosis['total_tables']}, "
            f"table_ids={diagnosis['table_ids']}, "
            f"has_stats={diagnosis['has_stats_tables']}, "
            f"comments={diagnosis['comment_count']}"
        )

        if diagnosis['cloudflare_indicators']:
            logger.error(f"Cloudflare block detected in parsed HTML: {diagnosis['cloudflare_indicators']}")
            return None

        comment_tables = extract_tables_from_comments(soup)

        df = find_player_stats_table(soup, comment_tables, stat_type)

        if df is None or df.empty:
            # Save HTML for debugging
            debug_file = f"/tmp/fbref_debug_{league.replace(' ', '_')}_{season}_{stat_type}.html"
            try:
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(html)
                logger.warning(
                    f"No player stats for {league} {season} ({stat_type}). "
                    f"Debug HTML saved to: {debug_file}"
                )
            except Exception as e:
                logger.warning(
                    f"No player stats for {league} {season} ({stat_type}). "
                    f"Failed to save debug HTML: {e}"
                )
            return None

        # Clean player names
        if 'Player' in df.columns:
            df['Player'] = df['Player'].astype(str).str.replace(
                r'^\d+\s*', '', regex=True
            )

        df['league'] = league
        df['season'] = season
        df['stat_type'] = stat_type
        df = self._add_metadata(df, f'player_{stat_type}')

        logger.info(f"Parsed {len(df)} player stats for {league} {season} ({stat_type})")
        return df

    def read_team_season_stats(
        self,
        stat_type: str,
        league: str,
        season: int
    ) -> Optional[pd.DataFrame]:
        """Read team/squad statistics for league/season."""
        url = get_stats_url(league, season, stat_type, for_squads=True)
        html = self._fetch_page(url)

        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        df = find_team_stats_table(soup, comment_tables, stat_type)

        if df is None or df.empty:
            logger.warning(f"No team stats for {league} {season} ({stat_type})")
            return None

        df['league'] = league
        df['season'] = season
        df['stat_type'] = stat_type
        df = self._add_metadata(df, f'team_{stat_type}')

        logger.info(f"Parsed {len(df)} team stats for {league} {season} ({stat_type})")
        return df

    def read_keeper_stats(
        self,
        stat_type: str,
        league: str,
        season: int
    ) -> Optional[pd.DataFrame]:
        """Read goalkeeper statistics for league/season."""
        url = get_stats_url(league, season, stat_type, for_squads=False)
        html = self._fetch_page(url)

        if not html:
            return None

        soup = BeautifulSoup(html, 'html.parser')
        comment_tables = extract_tables_from_comments(soup)

        df = find_player_stats_table(soup, comment_tables, stat_type)

        if df is None or df.empty:
            logger.warning(f"No keeper stats for {league} {season} ({stat_type})")
            return None

        # Clean player names
        if 'Player' in df.columns:
            df['Player'] = df['Player'].astype(str).str.replace(
                r'^\d+\s*', '', regex=True
            )

        df['league'] = league
        df['season'] = season
        df['stat_type'] = stat_type
        df = self._add_metadata(df, f'keeper_{stat_type}')

        logger.info(f"Parsed {len(df)} keeper stats for {league} {season} ({stat_type})")
        return df

    # =========================================================================
    # Memory-Efficient Scraping Methods
    # =========================================================================

    def scrape_single_stat_type(
        self,
        stat_type: str,
        data_category: str,
    ) -> Dict[str, str]:
        """
        Scrape single stat_type for all configured leagues/seasons.

        Memory-efficient method that collects only one stat_type at a time.
        Saves results to Iceberg table.

        Args:
            stat_type: One of PLAYER_STAT_TYPES, TEAM_STAT_TYPES, KEEPER_STAT_TYPES
            data_category: One of 'player', 'team', 'keeper'

        Returns:
            Dictionary mapping entity name to Iceberg table path,
            e.g., {'player_stats': 'iceberg.bronze.fbref_player_stats'}
        """
        logger.info(
            f"Starting scrape: {data_category}_{stat_type} "
            f"for {self.leagues} x {self.seasons}"
        )

        all_data = []

        for league in self.leagues:
            for season in self.seasons:
                try:
                    df = None

                    if data_category == 'player':
                        df = self.read_player_season_stats(stat_type, league, season)
                    elif data_category == 'team':
                        df = self.read_team_season_stats(stat_type, league, season)
                    elif data_category == 'keeper':
                        df = self.read_keeper_stats(stat_type, league, season)
                    else:
                        logger.error(f"Unknown data_category: {data_category}")
                        continue

                    if df is not None and not df.empty:
                        all_data.append(df)
                        logger.info(
                            f"Collected {len(df)} rows: "
                            f"{data_category}_{stat_type} ({league}, {season})"
                        )

                except Exception as e:
                    logger.error(
                        f"Error scraping {data_category}_{stat_type} "
                        f"for {league} {season}: {e}"
                    )
                    continue

            # Memory cleanup after each league
            gc.collect()

        result: Dict[str, str] = {}

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            table_name = f"fbref_{data_category}_{stat_type}"
            # #536: this nodriver path is the production single_stat default
            # (create_single_stat_task scraper_type='nodriver'). Without
            # replace_partitions the weekly DAG appends a full copy of each
            # (league, season) every run (45-50x bloat). Mirrors scrape_all
            # (#468) and the selenium FBrefDataReaderMixin path.
            table_path = self.save_to_iceberg(
                combined_df, table_name, partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            entity_key = f"{data_category}_{stat_type}"
            result[entity_key] = table_path
            logger.info(
                f"Completed {entity_key}: {len(combined_df)} total rows → {table_path}"
            )

        return result

    def scrape_schedule(self) -> Dict[str, str]:
        """
        Scrape schedules for all configured leagues/seasons.

        Returns:
            Dictionary mapping entity name to Iceberg table path,
            e.g., {'schedule': 'iceberg.bronze.fbref_schedule'}
        """
        logger.info(f"Scraping schedules for {self.leagues} x {self.seasons}")

        all_data = []

        for league in self.leagues:
            for season in self.seasons:
                try:
                    df = self.read_schedule(league, season)
                    if df is not None and not df.empty:
                        all_data.append(df)
                except Exception as e:
                    logger.error(f"Error scraping schedule {league} {season}: {e}")
                    continue

            gc.collect()

        result: Dict[str, str] = {}

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)

            # JSON fallback for match_all_data (Trino-independent)
            for league in self.leagues:
                for season in self.seasons:
                    league_df = combined_df[
                        (combined_df['league'] == league)
                        & (combined_df['season'] == season)
                    ]
                    if not league_df.empty:
                        safe_league = league.replace(' ', '_').replace('-', '_')
                        path = f'/tmp/fbref_schedule_{safe_league}_{season}.json'
                        league_df.to_json(path, orient='records', date_format='iso')
                        logger.info(
                            f"Schedule JSON fallback: {path} ({len(league_df)} rows)"
                        )

            table_path = self.save_to_iceberg(
                combined_df, 'fbref_schedule',
                partition_cols=['league', 'season'],
                replace_partitions=['league', 'season'],
            )
            result['schedule'] = table_path

        return result

    def scrape_all(self) -> Dict[str, str]:
        """
        Scrape all data for configured leagues and seasons.

        Collects schedule, player stats, team stats, and keeper stats
        sequentially to manage memory. Each step saves to Iceberg.

        Returns:
            Dictionary mapping data type to Iceberg table path.
        """
        all_results: Dict[str, str] = {}

        # Schedule
        logger.info("Collecting schedule...")
        all_results.update(self.scrape_schedule())

        # Player stats
        for stat_type in PLAYER_STAT_TYPES:
            logger.info(f"Collecting player_{stat_type}...")
            try:
                all_results.update(self.scrape_single_stat_type(stat_type, 'player'))
            except Exception as e:
                logger.error(f"Error collecting player_{stat_type}: {e}")

        # Team stats
        for stat_type in TEAM_STAT_TYPES:
            logger.info(f"Collecting team_{stat_type}...")
            try:
                all_results.update(self.scrape_single_stat_type(stat_type, 'team'))
            except Exception as e:
                logger.error(f"Error collecting team_{stat_type}: {e}")

        # Keeper stats
        for stat_type in KEEPER_STAT_TYPES:
            logger.info(f"Collecting keeper_{stat_type}...")
            try:
                all_results.update(self.scrape_single_stat_type(stat_type, 'keeper'))
            except Exception as e:
                logger.error(f"Error collecting keeper_{stat_type}: {e}")

        logger.info(f"Full scrape completed: {len(all_results)} tables saved")
        return all_results

    def close(self) -> None:
        """Close browser and cleanup resources."""
        self._close_browser()
        gc.collect()
        logger.info(f"NodriverFBrefScraper closed. Stats: {self._stats}")
        super().close()

    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics."""
        stats = super().get_stats()
        if self._proxy_manager:
            stats['proxy_stats'] = self._proxy_manager.get_stats()
        return stats
