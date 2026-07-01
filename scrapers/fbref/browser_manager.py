"""
FBref Browser Manager Mixin
============================

Browser lifecycle management for FBref scraper.
Handles browser creation, page fetching (nodriver / Selenium / curl),
cache management, and browser restart logic.

Xvfb Display Management:
    Xvfb is managed at the mixin level (_shared_xvfb_display) rather than
    per-NodriverBypass instance. This prevents "filedescriptor out of range
    in select()" errors that occur when Xvfb is repeatedly stopped/restarted
    (each restart accumulates Chrome FDs, eventually exceeding select()'s
    FD_SETSIZE=1024 limit).
"""

import asyncio
import gc
import logging
import os
import random
import re
import time
from typing import Dict, Optional

from scrapers.base.browser import CloudflareBypass
from scrapers.base.browser.nodriver_stealth import WINDOW_SIZES
from scrapers.fbref.constants import FBREF_UNCOMMENT_TABLES_JS

logger = logging.getLogger(__name__)

# Match pages must contain at least one stats_*_summary table — without it
# parse_player_match_stats_tables silently returns None. Matches the raw HTML
# (tables may be in DOM or inside HTML comments).
_MATCH_SUMMARY_RE = re.compile(
    r'<table[^>]*\bid="stats_[a-f0-9]+_summary[a-z_]*"'
)

# Chrome 120 User-Agent (must match container's Chromium for TLS fingerprint)
_CHROME120_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _content_type_to_resource_type(ct: str) -> str:
    """Map HTTP Content-Type header to CDP-style resource_type label.

    Mirrors NodriverBypass._rtype_name() labels so HTTP and CDP counters
    can be merged into total_proxy_*_by_resource_type for unified audit
    (issue #124).
    """
    mime = (ct or '').split(';', 1)[0].strip().lower()
    if not mime:
        return 'Other'
    if mime == 'text/html':
        return 'Document'
    if mime == 'application/json':
        return 'XHR'
    if mime == 'text/css':
        return 'Stylesheet'
    if mime in ('application/javascript', 'text/javascript'):
        return 'Script'
    if mime.startswith('image/'):
        return 'Image'
    if mime.startswith('font/') or mime.startswith('application/font-'):
        return 'Font'
    return 'Other'


class FBrefBrowserMixin:
    """
    Mixin providing browser lifecycle and page-fetching methods for FBrefScraper.

    Expects the host class to provide (via SeleniumScraper / BaseScraper):
        - self._proxy_manager
        - self.proxy
        - self.headless
        - self.use_xvfb
        - self.use_nodriver
        - self.nodriver_cloudflare_wait
        - self._browser
        - self._nodriver_browser
        - self._page_cache: Dict[str, str]
        - self._pages_fetched: int
        - self._rate_limiter
        - self._stats
        - self.MAX_PAGES_BEFORE_BROWSER_RESTART
        - self.MAX_CACHE_SIZE
    """

    # Shared Xvfb display (lives across browser restarts)
    _shared_xvfb_display = None

    # ------------------------------------------------------------------
    # Xvfb management (mixin level — survives browser restarts)
    # ------------------------------------------------------------------

    def _ensure_xvfb(self) -> bool:
        """
        Start shared Xvfb display if not already running.

        Xvfb is kept alive across browser restarts to avoid FD exhaustion.
        The display is only stopped in _stop_shared_xvfb() (called from close()).

        Returns:
            True if Xvfb is running, False otherwise.
        """
        if self._shared_xvfb_display is not None:
            return True

        try:
            from pyvirtualdisplay import Display

            width, height = random.choice(WINDOW_SIZES)
            display = Display(visible=False, size=(width, height), color_depth=24)
            display.start()
            FBrefBrowserMixin._shared_xvfb_display = display
            logger.info(f"Started shared Xvfb display: {width}x{height}")
            return True
        except ImportError:
            logger.warning("pyvirtualdisplay not available, using headless mode")
            return False
        except Exception as e:
            logger.warning(f"Failed to start shared Xvfb: {e}, using headless mode")
            return False

    def _stop_shared_xvfb(self) -> None:
        """Stop the shared Xvfb display (only at final cleanup)."""
        if FBrefBrowserMixin._shared_xvfb_display is not None:
            try:
                FBrefBrowserMixin._shared_xvfb_display.stop()
                logger.info("Stopped shared Xvfb display")
            except Exception as e:
                logger.warning(f"Error stopping shared Xvfb: {e}")
            finally:
                FBrefBrowserMixin._shared_xvfb_display = None

    # ------------------------------------------------------------------
    # Proxy helpers
    # ------------------------------------------------------------------

    def _get_proxy_url(self) -> Optional[str]:
        """Get proxy URL from manager or direct proxy setting."""
        if self._proxy_manager and self._proxy_manager.total_count > 0:
            proxy_obj = self._proxy_manager.get_proxy()
            if proxy_obj:
                self._current_proxy_obj = proxy_obj
                logger.debug(
                    f"Using proxy for FBref: {proxy_obj.host}:{proxy_obj.port}"
                )
                return proxy_obj.url
        elif self.proxy:
            return self.proxy
        return None

    def _get_current_nodriver_proxy_url(self) -> Optional[str]:
        """Get the proxy URL currently used by the nodriver browser instance."""
        if self._nodriver_browser is not None and self._nodriver_browser.proxy:
            return self._nodriver_browser.proxy
        return None

    # ------------------------------------------------------------------
    # Browser creation
    # ------------------------------------------------------------------

    def _get_nodriver_browser(self):
        """Get nodriver browser with FBref-specific configuration.

        Xvfb is managed at the mixin level: we start it once here and pass
        use_xvfb=False to NodriverBypass so it doesn't try to manage its own
        Xvfb (which would cause FD exhaustion on repeated restarts).
        """
        if self._nodriver_browser is None:
            from scrapers.base.browser import get_nodriver_bypass
            NodriverBypass = get_nodriver_bypass()

            proxy_url = self._get_proxy_url()

            # Manage Xvfb at mixin level to avoid FD exhaustion
            xvfb_running = False
            if self.use_xvfb:
                xvfb_running = self._ensure_xvfb()

            self._nodriver_browser = NodriverBypass(
                headless=self.headless if not xvfb_running else False,
                use_xvfb=False,  # Mixin manages Xvfb, not NodriverBypass
                proxy=proxy_url,
                cloudflare_wait=self.nodriver_cloudflare_wait,
                page_load_timeout=10.0,      # 40→20→10: dead proxy detected in 10s
                max_retries=1,               # 2→1: retry with same proxy useless for CF block
                use_cf_verify=True,
                pre_content_js=FBREF_UNCOMMENT_TABLES_JS,
                content_timeout=30.0,
                slow_proxy_threshold=self.SLOW_PROXY_THRESHOLD,  # env-tunable (issue #624); default 15s (normal load 5-8s)
            )
            logger.debug(
                f"Initialized nodriver browser (headless={self.headless}, "
                f"xvfb={xvfb_running}, "
                f"cloudflare_wait={self.nodriver_cloudflare_wait}s)"
            )
        return self._nodriver_browser

    def _get_browser(self) -> CloudflareBypass:
        """Get browser with FBref-specific configuration and proxy support."""
        if self._browser is None:
            proxy_url = self._get_proxy_url()

            self._browser = CloudflareBypass(
                headless=self.headless,
                use_xvfb=self.use_xvfb,
                proxy=proxy_url,
                page_load_timeout=120,
            )
        return self._browser

    # ------------------------------------------------------------------
    # HTTP session (curl_cffi with CF cookies from nodriver)
    # ------------------------------------------------------------------

    @staticmethod
    def _cdp_get_cookies_raw(urls=None):
        """Custom CDP generator returning RAW cookie dicts.

        Mirrors nodriver.cdp.network.get_cookies() but returns json['cookies']
        as plain dicts, bypassing Cookie.from_json which raises TypeError on
        Chromium 120 responses in nodriver 0.48.1 (the unhandled exception in
        Connection._listener corrupts the event loop and the next page.get()
        hangs ~40s). Repro: scripts/research/repro_nodriver_cookies_hang.py
        (Method D, 2026-05-23) — see docs/research/fbref-scraper-speedup.md.
        """
        params: dict = {}
        if urls is not None:
            params['urls'] = list(urls)
        json = yield {'method': 'Network.getCookies', 'params': params}
        if isinstance(json, dict):
            return json.get('cookies', [])
        return []

    def _extract_cookies_from_nodriver(self) -> dict:
        """Extract all cookies from the running nodriver browser via raw CDP.

        Uses _cdp_get_cookies_raw to bypass the broken Cookie.from_json parser
        in nodriver 0.48.1. Returns {name: value} dict with all cookies (CF
        + session). Returns {} on any failure — caller should treat as 'no
        HTTP fast-path available' and stick to nodriver.
        """
        if self._nodriver_browser is None or self._nodriver_browser._page is None:
            logger.info("Cannot extract cookies: nodriver browser/page not running")
            return {}

        try:
            loop = self._nodriver_browser._get_or_create_loop()

            async def _get_cookies_with_timeout():
                return await asyncio.wait_for(
                    self._nodriver_browser._page.send(
                        self._cdp_get_cookies_raw(urls=["https://fbref.com/"])
                    ),
                    timeout=5.0,
                )

            raw_cookies = loop.run_until_complete(_get_cookies_with_timeout())

            cookies: dict = {}
            cf_cookie_names = []
            for c in raw_cookies or []:
                if not isinstance(c, dict):
                    continue
                name = c.get('name', '')
                value = c.get('value', '')
                if name:
                    cookies[name] = value
                    if 'cf' in name.lower():
                        cf_cookie_names.append(name)

            logger.info(
                f"Extracted {len(cookies)} cookies from nodriver via raw CDP "
                f"(CF cookies: {cf_cookie_names or 'none'})"
            )
            return cookies

        except asyncio.TimeoutError:
            logger.warning("Timeout extracting cookies from nodriver (5s)")
            return {}
        except Exception as e:
            logger.warning(f"Could not extract cookies from nodriver: {e}")
            return {}

    def _create_http_session(self, cookies: dict, proxy_url: Optional[str] = None):
        """Create curl_cffi session with chrome120 impersonation, cookies and proxy."""
        from curl_cffi.requests import Session

        session = Session(impersonate='chrome120')
        session.cookies.update(cookies)
        session.headers.update({
            'User-Agent': _CHROME120_UA,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
        })

        if proxy_url:
            session.proxies = {
                'http': proxy_url,
                'https': proxy_url,
            }
            logger.info(f"HTTP session using proxy: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")

        return session

    def _try_init_http_session(self) -> None:
        """Try to initialize HTTP session with cookies and proxy from nodriver browser."""
        cookies = self._extract_cookies_from_nodriver()
        if not cookies:
            return

        # Need at least cf_clearance for Cloudflare-protected pages
        cf_names = [name for name in cookies if 'cf' in name.lower()]
        if not cf_names:
            logger.info("No CF cookies found, HTTP session not initialized")
            return

        # Get proxy from nodriver — CF cookies are IP-bound
        proxy_url = self._get_current_nodriver_proxy_url()

        try:
            self._http_session = self._create_http_session(cookies, proxy_url=proxy_url)
            self._http_cookies_time = time.time()
            self._http_request_count = 0
            # Issue #624: remember the proxy this session is bound to so a later
            # fallback diag can flag proxy-mismatch (drift vs current nodriver proxy).
            self._http_proxy_minted = self._sanitize_proxy_url(proxy_url)
            cf_clearance_preview = cookies.get('cf_clearance', '')[:16]
            logger.info(
                f"HTTP session initialized with {len(cookies)} cookies "
                f"(CF: {cf_names}, cf_clearance={cf_clearance_preview!r}..., "
                f"proxy: {self._sanitize_proxy_url(proxy_url) or 'none'}, "
                f"impersonate=chrome120)"
            )
        except Exception as e:
            logger.warning(f"Failed to create HTTP session: {e}")
            self._http_session = None
            self._http_proxy_minted = None

    def _http_cookies_expired(self) -> bool:
        """Check if HTTP session cookies have expired (TTL or request count)."""
        if self._http_cookies_time is None:
            return True
        elapsed_minutes = (time.time() - self._http_cookies_time) / 60.0
        if elapsed_minutes >= self.HTTP_COOKIE_TTL_MINUTES:
            logger.info(f"HTTP cookies expired after {elapsed_minutes:.1f} minutes")
            return True
        if self._http_request_count >= self.HTTP_MAX_REQUESTS:
            logger.info(f"HTTP cookies expired after {self._http_request_count} requests")
            return True
        return False

    @staticmethod
    def _sanitize_proxy_url(proxy_url: Optional[str]) -> Optional[str]:
        """Strip credentials from proxy URL for safe logging (host:port only)."""
        if not proxy_url:
            return None
        return proxy_url.split('@', 1)[-1] if '@' in proxy_url else proxy_url

    def _record_http_diag(
        self,
        url: str,
        status: Optional[int],
        html_len: int,
        reason: str,
        headers=None,
        html_preview: str = "",
        exception: str = "",
    ) -> None:
        """Record HTTP fast-path failure detail. Logs INFO for first 3, JSON-collects all.

        Used by issue #65 diagnostics. The `http_fetch_diag` list lands in the
        bench JSON via scripts/research/bench_fbref_fetch.py so root cause
        (TLS fingerprint / proxy mismatch / cookie expiration) can be triaged
        offline.
        """
        cookies_age_min = None
        if self._http_cookies_time is not None:
            cookies_age_min = round((time.time() - self._http_cookies_time) / 60.0, 2)

        hdrs = headers or {}
        record = {
            "url": url,
            "status": status,
            "html_len": html_len,
            "reason": reason,
            "server": hdrs.get("Server") or hdrs.get("server"),
            "cf_ray": hdrs.get("cf-ray") or hdrs.get("CF-RAY"),
            "cf_mitigated": hdrs.get("cf-mitigated") or hdrs.get("CF-Mitigated"),
            "content_encoding": hdrs.get("Content-Encoding") or hdrs.get("content-encoding"),
            "content_length": hdrs.get("Content-Length") or hdrs.get("content-length"),
            "cookies_age_min": cookies_age_min,
            "request_n": self._http_request_count,
            # Issue #624: proxy the nodriver browser is currently on. cf_clearance
            # is IP-bound; if this drifts from the proxy the curl session was
            # minted on (`proxy_minted`), the fallback is a proxy-mismatch (not
            # TLS / expiry). Compare proxy != proxy_minted to detect the drift.
            "proxy": self._sanitize_proxy_url(self._get_current_nodriver_proxy_url()),
            "proxy_minted": getattr(self, "_http_proxy_minted", None),
        }
        if html_preview:
            record["html_preview"] = html_preview[:500]
        if exception:
            record["exception"] = exception

        self._stats.setdefault("http_fetch_diag", []).append(record)

        logged = self._stats.setdefault("http_fetch_diag_logged", 0)
        if logged < 3:
            self._stats["http_fetch_diag_logged"] = logged + 1
            logger.info(
                f"[http-fast-path] FAIL reason={reason} url={url} "
                f"status={status} cf_mitigated={record['cf_mitigated']} "
                f"cf_ray={record['cf_ray']} server={record['server']} "
                f"len={html_len} cookies_age_min={cookies_age_min} "
                f"req_n={self._http_request_count}"
            )
            if html_preview:
                logger.info(f"[http-fast-path] preview: {html_preview[:300]!r}")
            if exception:
                logger.info(f"[http-fast-path] exception: {exception}")

    def _fetch_page_http(self, url: str) -> Optional[str]:
        """Fetch page via HTTP (curl_cffi) using CF cookies from nodriver."""
        if self._http_session is None:
            return None

        try:
            response = self._http_session.get(url, timeout=30)
            self._http_request_count += 1

            # Issue #124: account proxy bytes by resource_type for HTTP path.
            # Counted before status check — proxy spent bytes regardless of
            # response code. Uses decoded body length to match the legacy
            # bytes_downloaded metric (acceptance: Document ≈ html_mb).
            size = len(response.text or '')
            rtype = _content_type_to_resource_type(
                response.headers.get('content-type', '') or ''
            )
            self._stats['http_bytes_downloaded'] = (
                self._stats.get('http_bytes_downloaded', 0) + size
            )
            self._stats['http_requests_count'] = (
                self._stats.get('http_requests_count', 0) + 1
            )
            bytes_by_rt = self._stats.setdefault(
                'http_bytes_by_resource_type', {}
            )
            reqs_by_rt = self._stats.setdefault(
                'http_requests_by_resource_type', {}
            )
            bytes_by_rt[rtype] = bytes_by_rt.get(rtype, 0) + size
            reqs_by_rt[rtype] = reqs_by_rt.get(rtype, 0) + 1

            if response.status_code != 200:
                self._record_http_diag(
                    url, response.status_code, len(response.text or ""),
                    reason="non_200",
                    headers=response.headers,
                    html_preview=(response.text or "")[:500],
                )
                return None

            html = response.text
            if not html:
                self._record_http_diag(
                    url, response.status_code, 0,
                    reason="empty_body",
                    headers=response.headers,
                )
                return None

            # Validate: must have tables (real FBref page)
            has_tables = '<table' in html
            has_cloudflare = any(cf in html.lower() for cf in [
                'just a moment', 'checking your browser',
                'cf-browser-verification', 'challenge-running'
            ])

            if has_cloudflare:
                self._record_http_diag(
                    url, response.status_code, len(html),
                    reason="cf_challenge_in_body",
                    headers=response.headers,
                    html_preview=html[:500],
                )
                return None

            if not has_tables and len(html) < 50000:
                has_comment_tables = '<!--' in html and '<table' in html
                if not has_comment_tables:
                    self._record_http_diag(
                        url, response.status_code, len(html),
                        reason="incomplete_no_tables",
                        headers=response.headers,
                        html_preview=html[:500],
                    )
                    return None

            logger.debug(
                f"HTTP fetch OK: {url} ({len(html)} bytes, "
                f"request #{self._http_request_count})"
            )
            return html

        except Exception as e:
            self._record_http_diag(
                url, status=None, html_len=0,
                reason="exception",
                exception=f"{type(e).__name__}: {e}",
            )
            return None

    # ------------------------------------------------------------------
    # Page fetching
    # ------------------------------------------------------------------

    def _track_download(self, html_len: int, page_type: str = 'other') -> None:
        """Track downloaded bytes and page count in _stats.

        Syncs real proxy traffic stats from nodriver_browser (CDP
        Network.loadingFinished tracks actual bytes received through proxy,
        including CSS/JS/images that HTML-size tracking doesn't see).
        """
        self._stats['bytes_downloaded'] += html_len
        self._stats['pages_downloaded'] += 1
        self._stats['bytes_by_page_type'][page_type] = (
            self._stats['bytes_by_page_type'].get(page_type, 0) + html_len
        )
        self._sync_real_traffic_stats()

    def _sync_real_traffic_stats(self) -> None:
        """Sync real proxy traffic from nodriver browser to scraper stats.

        nodriver browser accumulates bytes within a single session. On
        browser restart the counter resets, so we flush current values into
        `_real_traffic_base` before restart. This method reads current
        session value and adds it to the persisted base.
        """
        if not (self._nodriver_browser is not None
                and hasattr(self._nodriver_browser, 'get_real_traffic_stats')):
            return
        try:
            real = self._nodriver_browser.get_real_traffic_stats()
            session_bytes = real.get('real_bytes_downloaded', 0)
            session_reqs = real.get('real_requests_count', 0)
            self._stats['real_bytes_downloaded'] = (
                self._real_traffic_base_bytes + session_bytes
            )
            self._stats['real_requests_count'] = (
                self._real_traffic_base_requests + session_reqs
            )
            # Issue #44: expose per-resource-type + CF + restart breakdown.
            # We add current-session counters on top of accumulated base so
            # mid-run reads (e.g. progress logs) reflect the live total.
            bytes_by_rtype = dict(self._real_traffic_base_bytes_by_rtype)
            reqs_by_rtype = dict(self._real_traffic_base_requests_by_rtype)
            for k, v in (real.get('real_bytes_by_resource_type') or {}).items():
                bytes_by_rtype[k] = bytes_by_rtype.get(k, 0) + v
            for k, v in (real.get('real_requests_by_resource_type') or {}).items():
                reqs_by_rtype[k] = reqs_by_rtype.get(k, 0) + v
            self._stats['real_bytes_by_resource_type'] = bytes_by_rtype
            self._stats['real_requests_by_resource_type'] = reqs_by_rtype
            # Issue #616: per-URL breakdown + top-consumer / first-third summary.
            bytes_by_url = dict(self._real_traffic_base_bytes_by_url)
            reqs_by_url = dict(self._real_traffic_base_requests_by_url)
            for k, v in (real.get('real_bytes_by_url') or {}).items():
                bytes_by_url[k] = bytes_by_url.get(k, 0) + v
            for k, v in (real.get('real_requests_by_url') or {}).items():
                reqs_by_url[k] = reqs_by_url.get(k, 0) + v
            self._stats['real_bytes_by_url'] = bytes_by_url
            self._stats['real_requests_by_url'] = reqs_by_url
            from scrapers.base.browser.nodriver_bypass import _summarise_url_traffic
            self._stats.update(_summarise_url_traffic(bytes_by_url, reqs_by_url))
            self._stats['cf_challenge_attempts'] = (
                self._cf_challenge_attempts_base
                + int(real.get('cf_challenge_attempts', 0) or 0)
            )
            self._stats['cf_challenges_passed'] = (
                self._cf_challenges_passed_base
                + int(real.get('cf_challenges_passed', 0) or 0)
            )
            self._stats['cf_challenges_failed'] = (
                self._cf_challenges_failed_base
                + int(real.get('cf_challenges_failed', 0) or 0)
            )
            restart_reasons = dict(self._restart_reasons_base)
            for k, v in (real.get('restart_reasons') or {}).items():
                restart_reasons[k] = restart_reasons.get(k, 0) + v
            self._stats['restart_reasons'] = restart_reasons
            # Issue #116: surface CDP cache-miss counter for hit-rate analysis.
            self._stats['resource_type_cache_misses'] = (
                self._resource_type_cache_misses_base
                + int(real.get('resource_type_cache_misses', 0) or 0)
            )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Camoufox transport (#CF-2026-07) — Turnstile bypass via anti-detect FF
    # ------------------------------------------------------------------

    def _camoufox_proxy_provider(self):
        """Zero-arg callable yielding a Playwright proxy dict from the proxy
        manager (rotates each call), or None when no proxy is configured."""
        def _next():
            if self._proxy_manager and self._proxy_manager.total_count > 0:
                p = self._proxy_manager.get_proxy()
                if p:
                    self._current_proxy_obj = p
                    return {
                        "server": f"http://{p.host}:{p.port}",
                        "username": p.username,
                        "password": p.password,
                    }
            if self.proxy:
                return {"server": self.proxy}
            return None
        return _next

    def _get_camoufox_transport(self):
        """Lazily build the warm CamoufoxFbrefTransport (one Firefox session
        reused across the whole scrape; cf_clearance persists across pages)."""
        if self._camoufox_transport is None:
            from scrapers.fbref.camoufox_fetch import CamoufoxFbrefTransport
            self._camoufox_transport = CamoufoxFbrefTransport(
                proxy_provider=self._camoufox_proxy_provider(),
                geoip=True,
                headless=getattr(self, 'headless', True),
                humanize=True,
                block_resources=True,
            )
        return self._camoufox_transport

    def _fetch_page_camoufox(
        self, url: str, use_cache: bool = True, page_type: str = 'other'
    ) -> Optional[str]:
        """Fetch one FBref page via Camoufox (Turnstile solve + resource block).

        Mirrors _fetch_page's caching, rate-limiting and stats bookkeeping so
        the traffic guard / diagnostics see the same keys as the nodriver path.
        """
        self._rate_limiter.acquire()
        transport = self._get_camoufox_transport()
        html = transport.fetch(url)

        # Merge transport proxy-byte + CF counters into _stats regardless of
        # outcome (a failed page still cost bytes / a CF attempt).
        ts = transport.traffic_stats()
        self._stats['real_bytes_downloaded'] = ts['real_bytes_downloaded']
        self._stats['real_requests_count'] = ts['real_requests_count']
        self._stats['real_bytes_by_resource_type'] = ts['real_bytes_by_resource_type']
        self._stats['cf_challenge_attempts'] = ts['cf_challenge_attempts']
        self._stats['cf_challenges_passed'] = ts['cf_challenges_passed']
        self._stats['cf_challenges_failed'] = ts['cf_challenges_failed']

        if not html:
            self._stats['failures'] = self._stats.get('failures', 0) + 1
            self._consecutive_fetch_failures += 1
            logger.warning(f"Camoufox transport returned no HTML for {url}")
            return None

        self._consecutive_fetch_failures = 0
        self._stats['successes'] = self._stats.get('successes', 0) + 1
        if use_cache:
            self._page_cache[url] = html
            self._manage_cache_size()
        self._track_download(len(html), page_type)
        return html

    def _fetch_page(self, url: str, use_cache: bool = True, page_type: str = 'other') -> Optional[str]:
        """
        Fetch page HTML with caching support.

        On SlowProxyError, retries up to MAX_SLOW_PROXY_RETRIES times
        with browser restart (which triggers proxy rotation).

        Args:
            url: URL to fetch
            use_cache: Whether to use page cache
            page_type: Page category for traffic tracking (schedule, player_stat, etc.)

        Returns:
            Page HTML or None
        """
        if use_cache and url in self._page_cache:
            logger.debug(f"Using cached page: {url}")
            return self._page_cache[url]

        # Camoufox transport (#CF-2026-07): the whole nodriver + curl_cffi
        # fast-path below is dead against fbref's current Cloudflare managed
        # interstitial. When FBREF_TRANSPORT=camoufox, fetch through the
        # anti-detect Firefox Turnstile solver instead.
        if getattr(self, 'fbref_transport', 'nodriver') == 'camoufox':
            return self._fetch_page_camoufox(url, use_cache, page_type)

        from scrapers.base.browser.nodriver_bypass import SlowProxyError

        for slow_retry in range(self.MAX_SLOW_PROXY_RETRIES):
            try:
                # Rate limiting
                self._rate_limiter.acquire()

                # HTTP fast-path: if we already have a live cf_clearance from a
                # previous nodriver fetch, try curl_cffi first (~1-2s vs ~8-15s).
                # Falls back to nodriver on CF challenge or incomplete HTML.
                html = None
                if (
                    self.use_nodriver
                    and self._http_session is not None
                    and not self._http_cookies_expired()
                ):
                    html = self._fetch_page_http(url)
                    if html is not None:
                        self._stats.setdefault('http_fetch_ok', 0)
                        self._stats['http_fetch_ok'] += 1
                        self._http_consecutive_fallbacks = 0
                    else:
                        self._stats.setdefault('http_fetch_fallback', 0)
                        self._stats['http_fetch_fallback'] += 1
                        # Issue #624: the curl session keeps its OWN proxy across
                        # nodriver restarts, so the proxy-manager never bans it on
                        # failure. A run of fallbacks means that pinned proxy is
                        # dead — drop the session so the next nodriver fetch
                        # re-mints cf_clearance on the current (healthy) proxy.
                        # Bounds the dead-proxy streak; reset the counter so we
                        # don't re-trip before the re-mint lands.
                        self._http_consecutive_fallbacks += 1
                        if self._http_consecutive_fallbacks >= self.HTTP_MAX_FALLBACKS_BEFORE_REMINT:
                            self._http_session = None
                            self._http_proxy_minted = None
                            self._http_consecutive_fallbacks = 0
                            logger.info(
                                "[http-fast-path] %d consecutive fallbacks — dropping "
                                "curl session to re-mint on next nodriver fetch",
                                self.HTTP_MAX_FALLBACKS_BEFORE_REMINT,
                            )

                # Fallback / first-time path: nodriver (or selenium)
                if html is None:
                    if self.use_nodriver:
                        html = self._fetch_page_nodriver(url)
                    else:
                        html = self._fetch_page_selenium(url)

                # Diagnostic logging
                if html:
                    html_len = len(html)
                    has_tables = '<table' in html
                    has_cloudflare = any(cf in html.lower() for cf in [
                        'just a moment', 'checking your browser',
                        'cf-browser-verification', 'challenge-running'
                    ])

                    logger.debug(
                        f"Page fetched: {url} | "
                        f"length={html_len}, has_tables={has_tables}, "
                        f"cloudflare_blocked={has_cloudflare}"
                    )


                    if has_cloudflare:
                        logger.warning(
                            f"Cloudflare challenge detected in response for {url}. "
                            f"HTML preview: {html[:500]}"
                        )
                        # Return None if page is still blocked
                        self._stats['failures'] += 1
                        return None

                    if not has_tables and html_len < 5000:
                        logger.warning(
                            f"Page appears incomplete or blocked: {url}. "
                            f"HTML preview: {html[:500]}"
                        )
                        self._stats['failures'] += 1
                        return None

                    # Detect pages that loaded but have no real content
                    # FBref pages with tables are typically >50KB; pages <50KB
                    # without visible tables should also have comment-embedded tables
                    if not has_tables and html_len < 50000:
                        has_comment_tables = '<!--' in html and '<table' in html
                        if not has_comment_tables:
                            logger.warning(
                                f"Page has no tables and no comment tables: {url}, "
                                f"len={html_len}. Likely incomplete load after browser issue."
                            )
                            self._stats['failures'] += 1
                            return None

                    # Match pages must contain at least one stats_*_summary table.
                    # Without it, parse_player_match_stats_tables silently returns
                    # None while lineups parse fine — data loss is invisible.
                    # We saw ~5% of match fetches return 200KB truncated HTML with
                    # lineup tables but no summary tables.
                    if page_type == 'match':
                        if not _MATCH_SUMMARY_RE.search(html):
                            proxy_desc = (
                                f"{self._current_proxy_obj.host}:{self._current_proxy_obj.port}"
                                if self._current_proxy_obj else 'no-proxy'
                            )
                            logger.warning(
                                f"Match page missing stats_*_summary table: {url}, "
                                f"len={html_len}, proxy={proxy_desc}. "
                                f"Treating as incomplete load."
                            )
                            self._stats['failures'] += 1
                            return None
                else:
                    logger.warning(f"Empty HTML returned for {url}")
                    self._stats['failures'] += 1
                    return None

                # Track successful download
                self._track_download(html_len, page_type)
                logger.info(
                    f"Fetched {url}: {html_len:,} bytes "
                    f"(total: {self._stats['bytes_downloaded']/1024/1024:.1f} MB)"
                )

                if use_cache:
                    self._page_cache[url] = html
                    self._manage_cache_size()

                self._stats['successes'] += 1
                self._consecutive_fetch_failures = 0
                if self._proxy_manager and self._current_proxy_obj:
                    self._proxy_manager.record_result(
                        self._current_proxy_obj, success=True,
                    )
                self._maybe_restart_browser()

                # Lazy-init HTTP fast-path after first successful nodriver fetch.
                # Raw CDP extraction (_cdp_get_cookies_raw) bypasses the broken
                # Cookie.from_json in nodriver 0.48.1 so the event loop is not
                # corrupted. Subsequent _fetch_page() calls will try HTTP first
                # and fall back to nodriver on CF challenge.
                if self._http_session is None:
                    self._try_init_http_session()

                return html

            except SlowProxyError as e:
                self._stats['failures'] += 1
                dead_proxy_desc = (
                    f"{self._current_proxy_obj.host}:{self._current_proxy_obj.port}"
                    if self._current_proxy_obj else 'no-proxy'
                )
                if self._proxy_manager and self._current_proxy_obj:
                    self._proxy_manager.record_result(
                        self._current_proxy_obj, success=False,
                        error_type='timeout',
                    )
                logger.warning(
                    f"SlowProxyError on {url}: proxy={dead_proxy_desc}, "
                    f"attempt={slow_retry + 1}/{self.MAX_SLOW_PROXY_RETRIES}"
                )

                # Try changing proxy via CDP without browser restart
                proxy_changed = self._try_change_proxy_nodriver()

                if proxy_changed:
                    logger.info(
                        f"SlowProxyError ({slow_retry + 1}/{self.MAX_SLOW_PROXY_RETRIES}): "
                        f"{e} — proxy changed via CDP, retrying immediately"
                    )
                else:
                    # Issue #624: keep the curl fast-path session — it is bound to
                    # its OWN proxy and serves matches independently of which proxy
                    # the nodriver browser rotates to. Dropping it here re-minted
                    # cf_clearance from a full CF cold-start every restart (the
                    # bad-day ~2 MB/match amplifier). FBREF_RESET_HTTP_ON_RESTART=1
                    # restores the old reset-on-restart behaviour.
                    self._close_browser(
                        reset_http=bool(self.RESET_HTTP_ON_RESTART), reason='slow_proxy'
                    )
                    if slow_retry < self.MAX_SLOW_PROXY_RETRIES - 1:
                        wait = 2 * (slow_retry + 1)
                        logger.warning(
                            f"SlowProxyError ({slow_retry + 1}/{self.MAX_SLOW_PROXY_RETRIES}): "
                            f"{e} — retrying with new proxy in {wait}s"
                        )
                        time.sleep(wait)
                    else:
                        logger.error(
                            f"All {self.MAX_SLOW_PROXY_RETRIES} proxy attempts failed for {url}"
                        )
                continue

            except Exception as e:
                self._stats['failures'] += 1
                self._consecutive_fetch_failures += 1
                if self._consecutive_fetch_failures >= self.MAX_CONSECUTIVE_FAILURES:
                    logger.warning(
                        f"{self._consecutive_fetch_failures} consecutive fetch failures "
                        f"— restarting browser for proxy rotation"
                    )
                    # Issue #624: keep the curl fast-path session (see slow_proxy
                    # branch) — it survives nodriver restarts on its own proxy.
                    self._close_browser(
                        reset_http=bool(self.RESET_HTTP_ON_RESTART),
                        reason='consecutive_failures',
                    )
                    self._consecutive_fetch_failures = 0
                logger.error(f"Error fetching page {url}: {e}", exc_info=True)
                return None

        return None

    def _fetch_page_nodriver(self, url: str) -> Optional[str]:
        """Fetch page using nodriver (better Cloudflare bypass)."""
        from scrapers.base.browser.nodriver_bypass import SlowProxyError

        browser = self._get_nodriver_browser()

        logger.debug(f"Fetching page with nodriver: {url}")

        try:
            html = browser.get_page(
                url,
                wait_timeout=30,
                cloudflare_wait=self.nodriver_cloudflare_wait,
            )
        except SlowProxyError as e:
            logger.warning(f"Proxy timeout for {url}: {e}")
            raise

        # get_page() already returns fully loaded HTML after Cloudflare bypass
        # No need to call page_source again (it can hang without timeout)
        if html:
            logger.debug(f"HTML received from nodriver, length={len(html)} bytes")
        else:
            logger.warning(f"No HTML received from nodriver for {url}")

        return html

    def _fetch_page_selenium(self, url: str) -> Optional[str]:
        """Fetch page using Selenium/undetected-chromedriver."""
        browser = self._get_browser()

        logger.debug(f"Fetching page: {url}")

        html = browser.get_page(
            url,
            wait_timeout=30,
            cloudflare_wait=30.0,
        )

        # Wait for dynamic content and get updated source
        time.sleep(2)
        html = browser.page_source

        return html

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear page cache and force garbage collection."""
        cache_size = len(self._page_cache)
        self._page_cache.clear()
        gc.collect()
        logger.info(f"Page cache cleared ({cache_size} pages), garbage collected")

    def _manage_cache_size(self) -> None:
        """Manage cache size to prevent memory issues."""
        if len(self._page_cache) > self.MAX_CACHE_SIZE:
            # Remove oldest entries (first half of cache)
            keys_to_remove = list(self._page_cache.keys())[:len(self._page_cache) // 2]
            for key in keys_to_remove:
                del self._page_cache[key]
            logger.info(f"Cache trimmed: removed {len(keys_to_remove)} old entries")

    # ------------------------------------------------------------------
    # Dynamic proxy change (without browser restart)
    # ------------------------------------------------------------------

    def _try_change_proxy_nodriver(self) -> bool:
        """Try to change proxy via CDP without restarting the browser.

        Gets a new proxy from the proxy manager and updates the proxy extension
        via CDP. Saves ~10s per rotation (no browser restart + no CF bypass).

        Returns:
            True if proxy changed successfully, False if browser restart needed.
        """
        if not self._nodriver_browser or not self._proxy_manager:
            logger.warning(
                f"CDP proxy change unavailable: "
                f"browser={'alive' if self._nodriver_browser else 'None'}, "
                f"proxy_manager={'yes' if self._proxy_manager else 'None'}"
            )
            return False

        proxy_obj = self._proxy_manager.get_proxy()
        if not proxy_obj:
            logger.debug("No available proxy for CDP change")
            return False

        new_proxy_url = proxy_obj.url
        success = self._nodriver_browser.change_proxy_sync(new_proxy_url)

        if success:
            self._current_proxy_obj = proxy_obj
            # Reset HTTP session since proxy changed (cf_clearance is IP-bound)
            self._http_session = None
            self._http_proxy_minted = None
            logger.info(
                f"Proxy changed via CDP to {proxy_obj.host}:{proxy_obj.port} "
                f"(no browser restart)"
            )
        else:
            logger.warning("CDP proxy change failed, will restart browser")
        return success

    # ------------------------------------------------------------------
    # Browser restart / close
    # ------------------------------------------------------------------

    def _maybe_restart_browser(self) -> None:
        """Restart browser if page limit reached to prevent memory leaks."""
        self._pages_fetched += 1
        if self._pages_fetched >= self.MAX_PAGES_BEFORE_BROWSER_RESTART:
            logger.info(
                f"Restarting browser after {self._pages_fetched} pages to prevent memory leaks"
            )
            # Keep HTTP session — same proxy, cookies still valid
            self._close_browser(reset_http=False, reason='page_limit')
            self._pages_fetched = 0
            gc.collect()

    def _close_browser(self, reset_http: bool = True, reason: str = 'explicit') -> None:
        """Close browser and clean up resources.

        Args:
            reset_http: If True, also reset the HTTP session. Default-False at
                the slow_proxy / consecutive_failures / page_limit restart sites
                (issue #624): the curl fast-path session is bound to its OWN
                proxy and survives nodriver restarts independently — a fallback
                run re-mints it, not the restart. True only on explicit/Selenium
                close or when FBREF_RESET_HTTP_ON_RESTART=1.
            reason: Why the browser is being closed — passed through to
                NodriverBypass.close_sync() for the restart_reasons counter
                (issue #44). Accepted: 'slow_proxy', 'consecutive_failures',
                'page_limit', 'retry_failed_matches', 'explicit'.

        Note: Does NOT stop the shared Xvfb display — it survives across
        browser restarts. Call _stop_shared_xvfb() for final cleanup.
        """
        # Close Selenium browser
        if self._browser is not None:
            try:
                self._browser.close()
            except Exception as e:
                logger.warning(f"Error closing Selenium browser: {e}")
            self._browser = None

        # Close nodriver browser (Xvfb is managed separately)
        if self._nodriver_browser is not None:
            # Flush session traffic stats to persistent base BEFORE closing
            # (nodriver browser counters reset on restart).
            try:
                real = self._nodriver_browser.get_real_traffic_stats()
                self._real_traffic_base_bytes += real.get('real_bytes_downloaded', 0)
                self._real_traffic_base_requests += real.get('real_requests_count', 0)
                # Issue #44: flush per-resource-type + CF + restart counters
                # so they survive the browser teardown. The NodriverBypass
                # instance's own _restart_reasons already includes the
                # current close (incremented in close()), so this captures it.
                for k, v in (real.get('real_bytes_by_resource_type') or {}).items():
                    self._real_traffic_base_bytes_by_rtype[k] += v
                for k, v in (real.get('real_requests_by_resource_type') or {}).items():
                    self._real_traffic_base_requests_by_rtype[k] += v
                # Issue #616: flush per-URL counters so they survive teardown.
                for k, v in (real.get('real_bytes_by_url') or {}).items():
                    self._real_traffic_base_bytes_by_url[k] += v
                for k, v in (real.get('real_requests_by_url') or {}).items():
                    self._real_traffic_base_requests_by_url[k] += v
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
                        f"over {real['real_requests_count']} requests "
                        f"(total accumulated: "
                        f"{self._real_traffic_base_bytes / 1024 / 1024:.1f} MB) "
                        f"reason={reason}"
                    )
            except Exception as e:
                logger.debug(f"Could not flush traffic stats: {e}")

            try:
                self._nodriver_browser.close_sync(reason=reason)
            except Exception as e:
                logger.warning(f"Error closing nodriver browser: {e}")
            self._nodriver_browser = None

            # After browser close, stats in _stats reflect accumulated base only
            self._stats['real_bytes_downloaded'] = self._real_traffic_base_bytes
            self._stats['real_requests_count'] = self._real_traffic_base_requests
            # Issue #44: same for per-resource-type + CF + restart counters.
            self._stats['real_bytes_by_resource_type'] = dict(
                self._real_traffic_base_bytes_by_rtype
            )
            self._stats['real_requests_by_resource_type'] = dict(
                self._real_traffic_base_requests_by_rtype
            )
            # Issue #616: surface accumulated per-URL breakdown + summary.
            self._stats['real_bytes_by_url'] = dict(self._real_traffic_base_bytes_by_url)
            self._stats['real_requests_by_url'] = dict(
                self._real_traffic_base_requests_by_url
            )
            from scrapers.base.browser.nodriver_bypass import _summarise_url_traffic
            self._stats.update(_summarise_url_traffic(
                self._real_traffic_base_bytes_by_url,
                self._real_traffic_base_requests_by_url,
            ))
            self._stats['cf_challenge_attempts'] = self._cf_challenge_attempts_base
            self._stats['cf_challenges_passed'] = self._cf_challenges_passed_base
            self._stats['cf_challenges_failed'] = self._cf_challenges_failed_base
            self._stats['restart_reasons'] = dict(self._restart_reasons_base)
            self._stats['resource_type_cache_misses'] = self._resource_type_cache_misses_base

        # Reset HTTP session only when the caller asks (explicit/Selenium close,
        # or FBREF_RESET_HTTP_ON_RESTART=1). Issue #624: proxy-rotation restarts
        # (slow_proxy / consecutive_failures) now pass reset_http=False — the curl
        # session is pinned to its own proxy and re-minted by the fallback counter.
        if reset_http:
            self._http_session = None
            self._http_proxy_minted = None

    def _close_all(self) -> None:
        """Close browser AND shared Xvfb (final cleanup)."""
        self._close_browser()
        # Camoufox transport is a warm session that lives for the whole scrape
        # (it rotates proxies internally); tear it down only at final cleanup.
        if getattr(self, '_camoufox_transport', None) is not None:
            try:
                self._camoufox_transport.close()
            except Exception as e:  # noqa: BLE001 — teardown is best-effort
                logger.warning(f"Error closing Camoufox transport: {e}")
            self._camoufox_transport = None
        self._stop_shared_xvfb()
