"""
Camoufox transport for FBref (Cloudflare Turnstile bypass).
====================================================================

FBref now serves a Cloudflare **managed interstitial** ("Just a moment…")
that nodriver + Chromium 149 cannot pass — Cloudflare detects the automated
Chromium regardless of proxy, WebGL renderer, click, or stealth (diagnosed
2026-07-01). The project's SofaScore transport already beats Turnstile with
**Camoufox** (anti-detect Firefox, C++-level fingerprint spoofing + geoip);
the same engine passes FBref's harder interstitial when driven with
``humanize=True`` and an explicit click on the Turnstile checkbox
(live-proven on 3/3 residential exit IPs).

This module wraps a warm Camoufox session:
- one Firefox instance per session, cf_clearance reused across pages
- Turnstile solver: locate the challenges.cloudflare.com iframe and click
  its checkbox, poll until the real page (a ``<table>``) appears
- resource blocking (image/media/font/stylesheet + analytics) to cut proxy
  bytes, always passing Cloudflare/Turnstile assets so the challenge renders
- rx+tx byte accounting from ``request.sizes()`` for the traffic guard

Requirements (already in the image, do NOT bump):
- camoufox 0.4.11 + ``python -m camoufox fetch``
- playwright < 1.60 (1.60 crashes Camoufox on page errors — camoufox#617)
- a residential proxy (Turnstile 403s on a datacenter IP)
"""

import logging
import os
import time
from collections import Counter
from typing import Callable, Dict, Optional

from scrapers.fbref.constants import FBREF_UNCOMMENT_TABLES_JS

logger = logging.getLogger(__name__)


# Non-essential resource types aborted to cut proxy bytes. FBref is static
# HTML parsed server-side (BeautifulSoup + comment extraction), so it never
# needs images/media/fonts/CSS — mirrors nodriver_bypass.BLOCKED_URL_PATTERNS.
_BLOCK_RESOURCE_TYPES = frozenset({"image", "media", "font", "stylesheet"})

# Cloudflare / Turnstile assets — ALWAYS pass (even css/font/img the challenge
# iframe pulls), or the bare type-block starves a visible challenge and breaks
# the bypass. Checked BEFORE the type block.
_ALLOW_URL_SUBSTRINGS = (
    "challenges.cloudflare.com",
    "turnstile.cloudflare.com",
    "/cdn-cgi/challenge-platform/",
)

# Analytics / tracking hosts aborted regardless of type. Do NOT add
# challenges/turnstile.cloudflare.com here — they are required for the bypass.
_BLOCK_URL_SUBSTRINGS = (
    # FBref autocomplete cache (~1.65 MB, top per-URL consumer in the #616
    # audit). CDP-pattern blocking missed it (activated after first load);
    # Playwright route() is registered before the first navigation, so the
    # block is effective here.
    "fbref.com/short/inc/",
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "googlesyndication.com", "googleadservices.com", "googletagservices.com",
    "facebook.net", "facebook.com/tr", "twitter.com/i/", "platform.twitter.com",
    "amazon-adsystem.com", "adsafeprotected.com", "adsrvr.org",
    "scorecardresearch.com", "quantserve.com", "cloudflareinsights.com",
    "newrelic.com", "nr-data.net", "hotjar.com", "segment.io", "mixpanel.com",
    "snap.licdn.com", "bat.bing.com", "hs-scripts.com", "hs-analytics.net",
)

# Cloudflare interstitial markers (mirror nodriver_bypass._is_cloudflare_blocked).
_CF_MARKERS = (
    "just a moment", "checking your browser", "cf-browser-verification",
    "challenge-running", "cf_chl_opt",
)


def should_block_request(
    resource_type: str, url: str, block_scripts: bool = False
) -> bool:
    """True → abort this request to save proxy bytes.

    Never blocks document/xhr/fetch (FBref's page itself), nor any
    Cloudflare/Turnstile asset — the allow-guard runs before the type block.
    ``block_scripts=True`` (FBREF_CAMOUFOX_BLOCK_SCRIPTS=1, experimental)
    additionally aborts script resources: the Python parser reads tables from
    HTML comments server-side, so FBref's own JS is not needed for content,
    and challenge JS is covered by the allow-list.
    """
    low = url.lower()
    if any(s in low for s in _ALLOW_URL_SUBSTRINGS):
        return False
    if resource_type in _BLOCK_RESOURCE_TYPES:
        return True
    if block_scripts and resource_type == "script":
        return True
    return any(s in low for s in _BLOCK_URL_SUBSTRINGS)


def is_cloudflare_blocked(html: str, title: str = "") -> bool:
    """True while the page is still the Cloudflare challenge, not real content."""
    if not html:
        return True
    blob = f"{title}\n{html}".lower()
    return any(m in blob for m in _CF_MARKERS)


def _navigation_error_type(exc: Exception) -> str:
    """Classify page.goto failures without immediately banning good proxies."""
    message = f"{type(exc).__name__}: {exc}".lower()
    hard_proxy_markers = (
        'err_proxy_connection_failed', 'err_tunnel_connection_failed',
    )
    if any(marker in message for marker in hard_proxy_markers):
        return 'timeout'
    transient_network_markers = (
        'timeout', 'timed out', 'err_connection_reset',
        'err_connection_timed_out',
    )
    if any(marker in message for marker in transient_network_markers):
        return 'network'
    return 'browser_error'


class CamoufoxFbrefTransport:
    """Warm Camoufox session that fetches FBref HTML through a Turnstile solve.

    Usage::

        transport = CamoufoxFbrefTransport(proxy_provider=pm_next_proxy)
        html = transport.fetch(url)   # solves Turnstile on the first page
        html2 = transport.fetch(url2) # reuses cf_clearance, no challenge
        transport.close()

    ``proxy_provider`` is a zero-arg callable returning a Playwright proxy dict
    (``{"server","username","password"}``) or ``None``. It is called once per
    (re)start so a solve-timeout can rotate to a fresh exit IP.
    """

    # Turnstile solve budget per page (seconds) and poll cadence.
    CF_SOLVE_TIMEOUT_S = 45.0
    POLL_INTERVAL_S = 1.5
    # How many polls to wait before the first checkbox click (let the widget
    # render); then re-click at most CLICK_ATTEMPTS times.
    CLICK_AFTER_POLLS = 2
    CLICK_ATTEMPTS = 3
    # Browser restarts (each on a fresh proxy) before giving up on a page.
    MAX_PROXY_ROTATIONS = 3

    def __init__(
        self,
        proxy_provider: Optional[Callable[[], Optional[dict]]] = None,
        proxy: Optional[dict] = None,
        proxy_result_callback: Optional[
            Callable[[bool, Optional[str]], None]
        ] = None,
        geoip: bool = True,
        headless: bool = True,
        humanize: bool = True,
        block_resources: bool = True,
        nav_timeout_ms: int = 90000,
    ):
        # Either a rotating provider or a single fixed proxy dict.
        if proxy_provider is None and proxy is not None:
            proxy_provider = lambda: proxy  # noqa: E731
        self._proxy_provider = proxy_provider
        self._proxy_result_callback = proxy_result_callback
        self._geoip = geoip
        self._headless = headless
        self._humanize = humanize
        self._block_resources = block_resources
        self._nav_timeout_ms = nav_timeout_ms

        self._cm = None
        self._browser = None
        self._page = None
        self._proxy = None

        # rx+tx byte accounting (#842 pattern) — survives restarts.
        self._bytes_total = 0
        self._bytes_by_type: Counter = Counter()
        self._blocked_count = 0
        self._requests_count = 0
        # CF solve counters (feed the traffic guard / diagnostics).
        self.cf_challenge_attempts = 0
        self.cf_challenges_passed = 0
        self.cf_challenges_failed = 0
        self._last_solve_failure: Optional[str] = None
        # Env-tunables: experimental script blocking + page-count restart
        # (Firefox memory creep on 380+-page backfills). A restart costs a
        # full CF cold-start (~4 MB measured) — keep the limit high.
        self._block_scripts = os.environ.get(
            "FBREF_CAMOUFOX_BLOCK_SCRIPTS", "").strip() == "1"
        try:
            self._max_pages_per_session = int(
                os.environ.get("FBREF_CAMOUFOX_MAX_PAGES") or 200)
        except ValueError:
            self._max_pages_per_session = 200
        self._pages_this_session = 0

    def _record_proxy_result(
        self, success: bool, error_type: Optional[str] = None
    ) -> None:
        """Notify the owner while the attempted proxy is still current."""
        if self._proxy_result_callback is None:
            return
        try:
            self._proxy_result_callback(success, error_type)
        except Exception:  # noqa: BLE001 — diagnostics must not break fetches
            logger.debug("proxy result callback failed", exc_info=True)

    # -- lifecycle -------------------------------------------------------- #
    def _start(self) -> None:
        from camoufox.sync_api import Camoufox  # lazy: heavy (Firefox)

        self._proxy = self._proxy_provider() if self._proxy_provider else None
        if not self._proxy:
            logger.warning(
                "CamoufoxFbrefTransport starting WITHOUT a proxy — Turnstile "
                "403s on a datacenter IP; the fetch will not solve."
            )
        kwargs = {"headless": self._headless, "humanize": self._humanize}
        if self._proxy:
            kwargs["proxy"] = self._proxy
            kwargs["geoip"] = self._geoip  # locale/timezone matched to exit IP
        self._cm = Camoufox(**kwargs)
        self._browser = self._cm.__enter__()
        self._page = self._browser.new_page()
        if self._block_resources:
            self._page.route("**/*", self._maybe_block)
        self._page.on("requestfinished", self._on_request_finished)
        self._pages_this_session = 0
        server = (self._proxy or {}).get("server", "direct")
        logger.info("Camoufox session started (proxy=%s, humanize=%s)",
                    server, self._humanize)

    def _stop(self) -> None:
        if self._cm is not None:
            try:
                self._cm.__exit__(None, None, None)
            except Exception:  # noqa: BLE001 — teardown is best-effort
                logger.warning("Camoufox teardown failed", exc_info=True)
        self._cm = self._browser = self._page = None

    def _restart(self) -> None:
        """Tear down and start fresh on the next proxy (rotation on failure)."""
        self._stop()
        self._start()

    def close(self) -> None:
        top = ", ".join(f"{t}={b // 1024}KB"
                        for t, b in self._bytes_by_type.most_common(4))
        logger.info("Camoufox fbref session total=%.1fMB blocked=%d top=[%s]",
                    self._bytes_total / 1_048_576, self._blocked_count, top)
        self._stop()

    def __enter__(self) -> "CamoufoxFbrefTransport":
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    # -- byte accounting / blocking --------------------------------------- #
    def _maybe_block(self, route) -> None:
        try:
            req = route.request
            if should_block_request(req.resource_type, req.url,
                                    self._block_scripts):
                self._blocked_count += 1
                route.abort()
                return
            route.continue_()
        except Exception:  # noqa: BLE001 — routing must not break the fetch
            try:
                route.continue_()
            except Exception:
                pass

    def _on_request_finished(self, req) -> None:
        try:
            s = req.sizes()
            n = (s.get("responseBodySize", 0) + s.get("responseHeadersSize", 0)
                 + s.get("requestBodySize", 0) + s.get("requestHeadersSize", 0))
            self._bytes_by_type[req.resource_type] += n
            self._bytes_total += n
            self._requests_count += 1
        except Exception:  # noqa: BLE001 — sizes() can race on teardown
            pass

    # -- Turnstile solve -------------------------------------------------- #
    def _challenge_frame_present(self) -> bool:
        """True when the challenges.cloudflare.com iframe is on the page."""
        try:
            return any(
                "challenges.cloudflare.com" in (f.url or "")
                for f in self._page.frames
            )
        except Exception:  # noqa: BLE001 — frames can detach mid-nav
            return False

    def _click_turnstile(self) -> bool:
        """Click the Turnstile checkbox inside the CF iframe. Returns True if a
        click was dispatched."""
        try:
            for frame in self._page.frames:
                if "challenges.cloudflare.com" in (frame.url or ""):
                    box = frame.locator("body").bounding_box()
                    if box:
                        # Checkbox sits ~30px from the widget's left edge,
                        # vertically centered.
                        self._page.mouse.click(
                            box["x"] + 30, box["y"] + box["height"] / 2
                        )
                        return True
                    break
        except Exception as e:  # noqa: BLE001 — frame can detach mid-solve
            logger.debug("Turnstile click skipped: %s", e)
        return False

    def _solve_current_page(self) -> Optional[str]:
        """Poll the current page until the real content appears, clicking the
        Turnstile checkbox as needed. Returns HTML or None.

        Success is keyed on a real ``<table>`` in the DOM: every FBref page we
        fetch (schedule / stats / match) has one, and the Cloudflare
        interstitial has none — this is the signal the live 3/3 solve relied
        on. A bare title check is NOT used: mid-navigation the title briefly
        goes empty and would spuriously read as "solved" (returning the 27 KB
        challenge shell instead of the ~500 KB page).
        """
        self._last_solve_failure = None
        deadline = time.time() + self.CF_SOLVE_TIMEOUT_S
        clicks = 0
        polls = 0
        # A challenge "attempt" is counted only when a CF shell is actually
        # observed (iframe or interstitial markers) — a plain warm-page nav
        # is NOT an attempt, so passed/attempts stays a real solve rate.
        challenge_seen = False
        while time.time() < deadline:
            time.sleep(self.POLL_INTERVAL_S)
            polls += 1
            if not challenge_seen and self._challenge_frame_present():
                challenge_seen = True
                self.cf_challenge_attempts += 1
            try:
                has_table = self._page.evaluate(
                    "!!document.querySelector('table')")
            except Exception as e:  # noqa: BLE001 — navigation mid-poll
                logger.debug("poll eval failed (navigating?): %s", e)
                continue
            if has_table:
                # Uncomment FBref's comment-wrapped tables before snapshotting
                # (belt-and-suspenders — the Python parser also handles them).
                try:
                    self._page.evaluate(FBREF_UNCOMMENT_TABLES_JS)
                except Exception:  # noqa: BLE001
                    pass
                html = self._page.content()
                # Sanity: never return a challenge shell as success.
                if not is_cloudflare_blocked(html):
                    if challenge_seen:
                        self.cf_challenges_passed += 1
                    return html
                if not challenge_seen:
                    # Shell markers without the iframe still mean a challenge.
                    challenge_seen = True
                    self.cf_challenge_attempts += 1
            if polls >= self.CLICK_AFTER_POLLS and clicks < self.CLICK_ATTEMPTS:
                if self._click_turnstile():
                    clicks += 1
        if challenge_seen:
            self.cf_challenges_failed += 1
            self._last_solve_failure = 'cloudflare'
        else:
            self._last_solve_failure = 'page_contract'
        return None

    def _lifecycle_action(self, action, label: str) -> bool:
        """Start/restart under the same bounded failure policy as navigation."""
        try:
            action()
            return True
        except Exception as exc:  # noqa: BLE001 — browser process boundary
            error_type = _navigation_error_type(exc)
            logger.warning("Camoufox %s failed: %s", label, exc)
            # Only failures with a network/proxy signature affect proxy
            # health. Local browser crashes must not burn a healthy exit IP.
            if error_type in {'timeout', 'network'}:
                self._record_proxy_result(False, error_type)
            self._stop()
            return False

    # -- public fetch ----------------------------------------------------- #
    def fetch(self, url: str) -> Optional[str]:
        """Navigate to ``url``, solve Turnstile if present, return page HTML.

        Reuses the warm session (cf_clearance) across calls; on a solve-timeout
        it restarts on a fresh proxy up to ``MAX_PROXY_ROTATIONS`` times.
        """
        if (
            self._page is not None
            and self._pages_this_session >= self._max_pages_per_session
        ):
            logger.info(
                "Camoufox session page limit reached (%d) — restarting to cap "
                "Firefox memory", self._pages_this_session)
            self._lifecycle_action(self._restart, 'page-limit restart')

        for attempt in range(self.MAX_PROXY_ROTATIONS + 1):
            if self._page is None:
                if not self._lifecycle_action(self._start, 'start'):
                    continue
            try:
                self._page.goto(
                    url, wait_until="domcontentloaded",
                    timeout=self._nav_timeout_ms,
                )
            except Exception as e:  # noqa: BLE001 — browser/network boundary
                logger.warning("Camoufox goto failed (attempt %d): %s",
                               attempt + 1, e)
                error_type = _navigation_error_type(e)
                if error_type in {'timeout', 'network'}:
                    self._record_proxy_result(False, error_type)
                if attempt < self.MAX_PROXY_ROTATIONS:
                    self._lifecycle_action(self._restart, 'restart')
                continue

            # CF challenge counters live in _solve_current_page — only navs
            # that actually surfaced a challenge shell count as attempts.
            html = self._solve_current_page()
            if html is not None:
                self._pages_this_session += 1
                self._record_proxy_result(True)
                logger.info("Camoufox fetched %s (%d bytes html)", url, len(html))
                return html

            failure_type = self._last_solve_failure or 'page_contract'
            logger.warning(
                "Camoufox page did not satisfy %s for %s (attempt %d/%d)",
                failure_type, url, attempt + 1,
                self.MAX_PROXY_ROTATIONS + 1)
            if failure_type == 'cloudflare':
                self._record_proxy_result(False, failure_type)
            if attempt < self.MAX_PROXY_ROTATIONS:
                self._lifecycle_action(self._restart, 'restart')

        return None

    # -- clearance export (HTTP fast-path) --------------------------------- #
    def get_clearance(self) -> Optional[dict]:
        """Export cf_clearance + fingerprint facts for the curl_cffi fast-path.

        Returns ``{'cookies': {name: value}, 'user_agent': str,
        'proxy': <playwright proxy dict or None>}`` from the live Firefox
        session, or ``None`` when there is no session / no cf_clearance yet.
        cf_clearance is bound to the exit IP and the browser fingerprint —
        the caller must reuse the same proxy and a Firefox impersonation.
        """
        if self._page is None:
            return None
        try:
            cookies = {
                c.get("name"): c.get("value")
                for c in self._page.context.cookies("https://fbref.com/")
                if c.get("name")
            }
            if "cf_clearance" not in cookies:
                return None
            user_agent = self._page.evaluate("navigator.userAgent")
            return {
                "cookies": cookies,
                "user_agent": user_agent,
                "proxy": self._proxy,
            }
        except Exception as e:  # noqa: BLE001 — session may be tearing down
            logger.debug("get_clearance failed: %s", e)
            return None

    # -- traffic stats ---------------------------------------------------- #
    def traffic_stats(self) -> Dict[str, object]:
        """Snapshot for the scraper's _stats / traffic guard."""
        return {
            "real_bytes_downloaded": self._bytes_total,
            "real_requests_count": self._requests_count,
            "real_bytes_by_resource_type": dict(self._bytes_by_type),
            "blocked_count": self._blocked_count,
            "cf_challenge_attempts": self.cf_challenge_attempts,
            "cf_challenges_passed": self.cf_challenges_passed,
            "cf_challenges_failed": self.cf_challenges_failed,
        }
