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


def should_block_request(resource_type: str, url: str) -> bool:
    """True → abort this request to save proxy bytes.

    Never blocks document/script/xhr/fetch (FBref's page + any challenge JS),
    nor any Cloudflare/Turnstile asset. Both guards run before the type block.
    """
    low = url.lower()
    if any(s in low for s in _ALLOW_URL_SUBSTRINGS):
        return False
    if resource_type in _BLOCK_RESOURCE_TYPES:
        return True
    return any(s in low for s in _BLOCK_URL_SUBSTRINGS)


def is_cloudflare_blocked(html: str, title: str = "") -> bool:
    """True while the page is still the Cloudflare challenge, not real content."""
    if not html:
        return True
    blob = f"{title}\n{html}".lower()
    return any(m in blob for m in _CF_MARKERS)


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
            if should_block_request(req.resource_type, req.url):
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
        deadline = time.time() + self.CF_SOLVE_TIMEOUT_S
        clicks = 0
        polls = 0
        while time.time() < deadline:
            time.sleep(self.POLL_INTERVAL_S)
            polls += 1
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
                    return html
            if polls >= self.CLICK_AFTER_POLLS and clicks < self.CLICK_ATTEMPTS:
                if self._click_turnstile():
                    clicks += 1
        return None

    # -- public fetch ----------------------------------------------------- #
    def fetch(self, url: str) -> Optional[str]:
        """Navigate to ``url``, solve Turnstile if present, return page HTML.

        Reuses the warm session (cf_clearance) across calls; on a solve-timeout
        it restarts on a fresh proxy up to ``MAX_PROXY_ROTATIONS`` times.
        """
        if self._page is None:
            self._start()

        for attempt in range(self.MAX_PROXY_ROTATIONS + 1):
            self.cf_challenge_attempts += 1
            try:
                self._page.goto(
                    url, wait_until="domcontentloaded",
                    timeout=self._nav_timeout_ms,
                )
            except Exception as e:  # noqa: BLE001 — dead proxy / nav timeout
                logger.warning("Camoufox goto failed (attempt %d): %s",
                               attempt + 1, e)
                self.cf_challenges_failed += 1
                if attempt < self.MAX_PROXY_ROTATIONS:
                    self._restart()
                continue

            html = self._solve_current_page()
            if html is not None:
                self.cf_challenges_passed += 1
                logger.info("Camoufox fetched %s (%d bytes html)", url, len(html))
                return html

            logger.warning(
                "Camoufox could not solve Turnstile for %s (attempt %d/%d)",
                url, attempt + 1, self.MAX_PROXY_ROTATIONS + 1)
            self.cf_challenges_failed += 1
            if attempt < self.MAX_PROXY_ROTATIONS:
                self._restart()

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
