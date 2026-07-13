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

import asyncio
import logging
import os
import threading
import time
from collections import Counter
from contextlib import contextmanager
from typing import Callable, Dict, Optional

from scrapers.fbref.constants import FBREF_UNCOMMENT_TABLES_JS

logger = logging.getLogger(__name__)


# Non-essential resource types aborted to cut proxy bytes. FBref is static
# HTML parsed server-side (BeautifulSoup + comment extraction), so it never
# needs images/media/fonts/CSS; blocking them materially reduces proxy spend.
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

# Cloudflare interstitial markers shared by the raw-response contract.
_CF_MARKERS = (
    "just a moment", "checking your browser", "cf-browser-verification",
    "challenge-running", "cf_chl_opt",
)

# A response body is reserved from Content-Length before the browser may read
# it.  This fixed allowance covers request/response headers and framing, which
# Playwright only reports exactly after completion.  The allowance is per
# in-flight request and is released when that request finishes or fails.
BROWSER_REQUEST_FIXED_OVERHEAD_BYTES = 64 * 1024

# Cloudflare answers over HTTP/2, and chunked HTTP/1.1 responses carry no
# Content-Length at all, so an undeclared body is the norm on the clearance
# path rather than an attack.  Such a response reserves this ceiling before the
# browser may read it and settles to the observed size on completion; a body
# that outgrows the reservation still aborts the session at completion, and a
# reservation that does not fit the remaining budget aborts it before transport.
BROWSER_UNDECLARED_BODY_RESERVATION_BYTES = 512 * 1024


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
    # Camoufox's launch has no timeout of its own.  On an exit IP that accepts
    # the connection but never answers, it waits forever inside Playwright's
    # event loop, and no navigation timeout applies because navigation never
    # starts — a production fetch wave then hangs until its own kill deadline.
    # A healthy launch (Firefox start plus the geoip lookup through the proxy)
    # completes in seconds.
    BROWSER_START_TIMEOUT_S = 120.0
    # Playwright's own timeouts (nav_timeout_ms, and the poll deadline in
    # _solve_current_page) only fire while its driver still answers.  A proxy
    # that stalls mid-navigation can wedge the driver connection itself: goto
    # never returns, its timeout never fires, and the solve loop's evaluate()
    # blocks before it can check its deadline.  Bound the whole attempt —
    # navigation plus solve — with a deadline of our own; killing the browser
    # is what makes the blocked call raise, back into the rotation path.
    BROWSER_ATTEMPT_TIMEOUT_S = 240.0

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
        max_network_requests: Optional[int] = None,
        max_network_bytes: Optional[int] = None,
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
        if max_network_requests is not None and int(max_network_requests) <= 0:
            raise ValueError("max_network_requests must be positive")
        if max_network_bytes is not None and int(max_network_bytes) <= 0:
            raise ValueError("max_network_bytes must be positive")
        self._max_network_requests = (
            None if max_network_requests is None else int(max_network_requests)
        )
        self._max_network_bytes = (
            None if max_network_bytes is None else int(max_network_bytes)
        )

        self._cm = None
        self._browser = None
        self._page = None
        self._proxy = None

        # rx+tx byte accounting (#842 pattern) — survives restarts.
        self._bytes_total = 0
        self._bytes_by_type: Counter = Counter()
        self._blocked_count = 0
        self._requests_count = 0
        self._network_requests_started = 0
        self._browser_start_attempts = 0
        self._browser_watchdog_kills = 0
        self._navigation_attempts = 0
        self._budget_blocked_count = 0
        self._inflight_byte_reservations: Dict[int, int] = {}
        # Playwright/Firefox may hand a *different* Request wrapper to the
        # response/finished callbacks than the one seen by route(), so the
        # reservation is also indexed by request URL to survive re-wrapping.
        self._inflight_request_urls: Dict[int, str] = {}
        self._inflight_ids_by_url: Dict[str, list] = {}
        self._inflight_reserved_bytes = 0
        self._unobserved_reserved_bytes = 0
        self._byte_budget_exhausted = False
        self._byte_budget_failure: Optional[str] = None
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
        self._browser_start_attempts += 1
        from camoufox.sync_api import Camoufox  # lazy: heavy (Firefox)

        # Recover from a poisoned thread. camoufox's ``Camoufox.__exit__`` calls
        # an UNGUARDED ``browser.close()`` before playwright's event-loop
        # teardown; when a prior session's browser is dead/hung (e.g. a failed
        # Turnstile solve), that close() raises and playwright never closes its
        # sync loop, leaving it bound-and-running on this thread. playwright's
        # sync ``__enter__`` then hits ``get_running_loop().is_running()`` and
        # raises "Sync API inside the asyncio loop", which cascades to every
        # remaining fetch in the process (observed poisoning combined_season_stats
        # after a single failed Turnstile). Detaching the stale running-loop flag
        # here lets this start build a fresh loop. No-op on the normal path
        # (no running loop -> RuntimeError -> pass).
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            logger.warning(
                "Stale running asyncio loop detected before Camoufox start "
                "(prior teardown left it bound); detaching so the session can "
                "start clean."
            )
            asyncio.events._set_running_loop(None)

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
        # The launch itself is unbounded (see BROWSER_START_TIMEOUT_S), so kill
        # the browser it spawned if it overruns: Playwright then raises out of
        # the launch and the caller rotates onto a fresh exit IP.
        with self._browser_deadline(self.BROWSER_START_TIMEOUT_S, "start"):
            self._cm = Camoufox(**kwargs)
            self._browser = self._cm.__enter__()
            self._page = self._browser.new_page()
        if (
            self._block_resources
            or self._max_network_requests is not None
            or self._max_network_bytes is not None
        ):
            self._page.route("**/*", self._maybe_block)
        self._page.on("response", self._on_response)
        self._page.on("requestfinished", self._on_request_finished)
        self._page.on("requestfailed", self._on_request_failed)
        self._pages_this_session = 0
        server = (self._proxy or {}).get("server", "direct")
        logger.info("Camoufox session started (proxy=%s, humanize=%s)",
                    server, self._humanize)

    @contextmanager
    def _browser_deadline(self, timeout_s: float, phase: str):
        """Bound a blocking Playwright call: on overrun, kill the browser."""
        watchdog = threading.Timer(
            timeout_s, self._kill_browser_processes, args=(phase, timeout_s)
        )
        watchdog.daemon = True
        watchdog.start()
        try:
            yield
        finally:
            watchdog.cancel()

    def _kill_browser_processes(
        self, phase: str = "start", timeout_s: Optional[float] = None
    ) -> None:
        """Kill the browser this transport spawned, and only the browser.

        Runs on a watchdog thread, so it must not touch Playwright objects:
        the browser's death is what makes the blocked call raise. Playwright's
        node driver is deliberately spared — killing it severs the connection
        every later session in this process depends on, and every subsequent
        navigation then failed instantly with NS_ERROR_FAILURE (observed in
        production: three fresh proxies "failed" 150ms after launch).
        """
        import psutil

        try:
            children = psutil.Process().children(recursive=True)
        except psutil.Error:  # the process tree can vanish mid-walk
            return
        killed = 0
        for child in children:
            try:
                name = (child.name() or "").casefold()
                if "camoufox" in name or "firefox" in name:
                    child.kill()
                    killed += 1
            except psutil.Error:
                continue
        if killed:
            self._browser_watchdog_kills += 1
            logger.warning(
                "Camoufox %s exceeded %.0fs (stalled exit IP?) — killed %d "
                "browser process(es) to force a proxy rotation",
                phase,
                timeout_s if timeout_s is not None
                else self.BROWSER_START_TIMEOUT_S,
                killed,
            )

    def _stop(self) -> None:
        if self._cm is not None:
            try:
                self._cm.__exit__(None, None, None)
            except Exception:  # noqa: BLE001 — teardown is best-effort
                logger.warning("Camoufox teardown failed", exc_info=True)
        # Any request that produced no finished/failed callback may have
        # crossed an unknown fraction of its reservation. Charge the full
        # remainder before allowing a restart to reuse capacity.
        self._unobserved_reserved_bytes += self._inflight_reserved_bytes
        self._cm = self._browser = self._page = None
        self._inflight_byte_reservations.clear()
        self._inflight_reserved_bytes = 0

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
    @staticmethod
    def _request_url(req) -> Optional[str]:
        try:
            url = req.url
        except Exception:  # noqa: BLE001 — detached requests have no url
            return None
        return str(url) if url else None

    def _track_reservation(self, req, reservation: int) -> None:
        key = id(req)
        self._inflight_byte_reservations[key] = reservation
        self._inflight_reserved_bytes += reservation
        url = self._request_url(req)
        if url is not None:
            self._inflight_request_urls[key] = url
            self._inflight_ids_by_url.setdefault(url, []).append(key)

    def _forget_reservation(self, key: int) -> int:
        reserved = self._inflight_byte_reservations.pop(key, 0)
        url = self._inflight_request_urls.pop(key, None)
        if url is not None:
            keys = self._inflight_ids_by_url.get(url)
            if keys:
                try:
                    keys.remove(key)
                except ValueError:
                    pass
                if not keys:
                    self._inflight_ids_by_url.pop(url, None)
        return reserved

    def _reservation_key(self, req) -> Optional[int]:
        """Reservation key for ``req``, re-keying a re-wrapped request by URL.

        The route callback and the response/finished callbacks can receive
        different Python wrappers for the same in-flight request, so identity
        alone loses the reservation and would fail the session closed.
        """
        key = id(req)
        if key in self._inflight_byte_reservations:
            return key
        url = self._request_url(req)
        if url is None:
            return None
        keys = self._inflight_ids_by_url.get(url)
        if not keys:
            return None
        stale_key = keys[0]
        reserved = self._inflight_byte_reservations.pop(stale_key, 0)
        self._inflight_request_urls.pop(stale_key, None)
        keys.pop(0)
        if not keys:
            self._inflight_ids_by_url.pop(url, None)
        self._inflight_byte_reservations[key] = reserved
        self._inflight_request_urls[key] = url
        self._inflight_ids_by_url.setdefault(url, []).append(key)
        return key

    def _release_byte_reservation(self, req) -> int:
        key = self._reservation_key(req)
        if key is None:
            return 0
        reserved = self._forget_reservation(key)
        self._inflight_reserved_bytes = max(
            0, self._inflight_reserved_bytes - reserved
        )
        return reserved

    @staticmethod
    def _observed_request_bytes(req) -> int:
        sizes = req.sizes()
        names = (
            "responseBodySize",
            "responseHeadersSize",
            "requestBodySize",
            "requestHeadersSize",
        )
        values = [int(sizes.get(name, 0) or 0) for name in names]
        if any(value < 0 for value in values):
            raise ValueError("Playwright returned a negative request size")
        return sum(values)

    def _record_observed_request_bytes(self, req, size: int) -> None:
        observed = max(0, int(size))
        self._bytes_by_type[req.resource_type] += observed
        self._bytes_total += observed

    def _charge_failed_request(self, req, reserved: int) -> None:
        try:
            observed = self._observed_request_bytes(req)
        except Exception:  # noqa: BLE001 — failed requests often lack sizes
            observed = 0
        if observed > 0:
            self._record_observed_request_bytes(req, observed)
        else:
            self._unobserved_reserved_bytes += max(0, int(reserved))

        if (
            self._max_network_bytes is not None
            and self._bytes_total
            + self._unobserved_reserved_bytes
            + self._inflight_reserved_bytes
            > self._max_network_bytes
        ):
            self._abort_session_for_byte_budget(
                "failed_request_consumed_byte_cap"
            )

    def _abort_session_for_byte_budget(self, reason: str) -> None:
        if self._byte_budget_exhausted:
            return
        self._byte_budget_exhausted = True
        self._byte_budget_failure = reason
        self._budget_blocked_count += 1
        self._last_solve_failure = "byte_budget"
        logger.error("Camoufox byte budget aborted the session: %s", reason)
        # Charge every in-flight reservation in full: those requests may have
        # crossed an unknown fraction of the wire, and the cap must stay
        # conservative even though the browser is still up for a moment.
        self._unobserved_reserved_bytes += self._inflight_reserved_bytes
        self._inflight_byte_reservations.clear()
        self._inflight_reserved_bytes = 0
        # The browser is NOT closed here.  This runs inside a Playwright event
        # callback (`response` / `requestfinished`), and the sync API deadlocks
        # if the browser is torn down from one: the callback waits for the
        # close, the close waits for the callback to return, and the wave hangs
        # forever holding its leases.  `route()` refuses every further request
        # while the flag is set, and `fetch()` closes the session on its way out.

    def _maybe_block(self, route) -> None:
        req = None
        try:
            req = route.request
            if (
                self._block_resources
                and should_block_request(
                    req.resource_type, req.url, self._block_scripts
                )
            ):
                self._blocked_count += 1
                route.abort()
                return
            request_cap_reached = (
                self._max_network_requests is not None
                and self._network_requests_started >= self._max_network_requests
            )
            byte_cap_reached = (
                self._max_network_bytes is not None
                and (
                    self._byte_budget_exhausted
                    or self._bytes_total
                    + self._unobserved_reserved_bytes
                    + self._inflight_reserved_bytes
                    + BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
                    > self._max_network_bytes
                )
            )
            if request_cap_reached or byte_cap_reached:
                self._blocked_count += 1
                self._budget_blocked_count += 1
                route.abort()
                return
            # Count before handing the request to Playwright. Failed and
            # interrupted requests still consumed one global network attempt.
            self._network_requests_started += 1
            if self._max_network_bytes is not None:
                self._track_reservation(
                    req, BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
                )
            route.continue_()
        except Exception:  # noqa: BLE001 — fail closed at the proxy boundary
            if req is not None:
                reserved = self._release_byte_reservation(req)
                if reserved:
                    self._charge_failed_request(req, reserved)
            try:
                self._blocked_count += 1
                route.abort()
            except Exception:
                pass

    @staticmethod
    def _response_headers(response) -> Dict[str, str]:
        try:
            raw = response.headers
            if callable(raw):
                raw = raw()
            return {
                str(name).casefold(): str(value)
                for name, value in dict(raw or {}).items()
            }
        except Exception:  # noqa: BLE001 — fail closed in _on_response
            return {}

    def _adopt_unrouted_request(self, req) -> Optional[int]:
        """Charge an in-flight request that ``route()`` never reserved.

        Server redirects are not re-routed by Playwright, and Firefox can hand
        the response callbacks a request the route callback never saw. Such a
        request is still real proxy traffic, so it consumes a request slot and
        the same fixed overhead as a routed one, enforced against the same
        caps. Only a cap breach aborts the session — unlike ``route()`` there
        is no way to refuse the transfer before transport.
        """
        if (
            self._max_network_requests is not None
            and self._network_requests_started >= self._max_network_requests
        ):
            self._abort_session_for_byte_budget("unrouted_request_over_request_cap")
            return None
        reservation = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        projected = (
            self._bytes_total
            + self._unobserved_reserved_bytes
            + self._inflight_reserved_bytes
            + reservation
        )
        if projected > self._max_network_bytes:
            self._abort_session_for_byte_budget("unrouted_request_over_byte_cap")
            return None
        self._network_requests_started += 1
        self._track_reservation(req, reservation)
        return reservation

    def _on_response(self, response) -> None:
        """Reserve the declared body before any response can cross the cap."""

        if self._max_network_bytes is None or self._byte_budget_exhausted:
            return
        req = getattr(response, "request", None)
        if req is None:
            self._abort_session_for_byte_budget("response_without_request")
            return
        reservation_key = self._reservation_key(req)
        if reservation_key is None:
            fixed = self._adopt_unrouted_request(req)
            if fixed is None:
                return  # a cap breach already aborted the session
            reservation_key = id(req)
        else:
            fixed = self._inflight_byte_reservations[reservation_key]

        headers = self._response_headers(response)
        transfer_encoding = headers.get("transfer-encoding", "")
        chunked = "chunked" in {
            token.strip().casefold()
            for token in transfer_encoding.split(",")
        }
        raw_length = headers.get("content-length", "").strip()
        declared = (
            not chunked
            and raw_length
            and raw_length.isascii()
            and raw_length.isdigit()
        )
        body_reservation = (
            int(raw_length)
            if declared
            else BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
        )
        projected = (
            self._bytes_total
            + self._unobserved_reserved_bytes
            + self._inflight_reserved_bytes
            + body_reservation
        )
        if projected > self._max_network_bytes:
            self._abort_session_for_byte_budget(
                f"declared_content_length_exceeds_cap:{body_reservation}"
                if declared
                else f"undeclared_body_exceeds_cap:{body_reservation}"
            )
            return

        self._inflight_byte_reservations[reservation_key] = (
            fixed + body_reservation
        )
        self._inflight_reserved_bytes += body_reservation

    def _on_request_finished(self, req) -> None:
        reserved = self._release_byte_reservation(req)
        try:
            n = self._observed_request_bytes(req)
            self._requests_count += 1
            if self._max_network_bytes is not None and n == 0:
                self._unobserved_reserved_bytes += max(0, int(reserved))
                return
            self._record_observed_request_bytes(req, n)
            if (
                self._max_network_bytes is not None
                and (
                    n > reserved
                    or self._bytes_total
                    + self._unobserved_reserved_bytes
                    + self._inflight_reserved_bytes
                    > self._max_network_bytes
                )
            ):
                self._abort_session_for_byte_budget(
                    f"completed_size_exceeded_reservation:{n}>{reserved}"
                )
        except Exception:  # noqa: BLE001 — sizes() can race on teardown
            self._unobserved_reserved_bytes += max(0, int(reserved))

    def _on_request_failed(self, req) -> None:
        reserved = self._release_byte_reservation(req)
        self._charge_failed_request(req, reserved)

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
        if self._byte_budget_exhausted:
            return None
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
                # FBrefFetcher uses Camoufox only for its clearance bootstrap,
                # so every navigation here is one exact bootstrap attempt.
                # Count before goto: timeouts and interrupted navigations still
                # consumed a browser/proxy attempt.
                self._navigation_attempts += 1
                with self._browser_deadline(
                    self.BROWSER_ATTEMPT_TIMEOUT_S, "navigation"
                ):
                    self._page.goto(
                        url, wait_until="domcontentloaded",
                        timeout=self._nav_timeout_ms,
                    )
            except Exception as e:  # noqa: BLE001 — browser/network boundary
                logger.warning("Camoufox goto failed (attempt %d): %s",
                               attempt + 1, e)
                error_type = _navigation_error_type(e)
                if self._byte_budget_exhausted:
                    break
                if error_type in {'timeout', 'network'}:
                    self._record_proxy_result(False, error_type)
                if attempt < self.MAX_PROXY_ROTATIONS:
                    self._lifecycle_action(self._restart, 'restart')
                continue

            if self._byte_budget_exhausted:
                break

            # CF challenge counters live in _solve_current_page — only navs
            # that actually surfaced a challenge shell count as attempts.
            try:
                with self._browser_deadline(
                    self.BROWSER_ATTEMPT_TIMEOUT_S, "challenge solve"
                ):
                    html = self._solve_current_page()
            except Exception as e:  # noqa: BLE001 — killed browser raises here
                logger.warning("Camoufox solve failed (attempt %d): %s",
                               attempt + 1, e)
                if self._byte_budget_exhausted:
                    break
                if attempt < self.MAX_PROXY_ROTATIONS:
                    self._lifecycle_action(self._restart, 'restart')
                continue
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

        if self._byte_budget_exhausted and self._cm is not None:
            # The cap fired inside a Playwright callback, which may not close
            # the browser (see _abort_session_for_byte_budget).  Do it here,
            # back on the main thread, so no parallel response keeps spending
            # proxy bytes after the budget failed.
            self._stop()
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
            "real_requests_count": max(
                self._requests_count, self._network_requests_started
            ),
            "completed_requests_count": self._requests_count,
            "browser_bootstrap_attempts": max(
                self._browser_start_attempts, self._navigation_attempts
            ),
            "browser_start_attempts": self._browser_start_attempts,
            "browser_watchdog_kills": self._browser_watchdog_kills,
            "browser_navigation_attempts": self._navigation_attempts,
            "budget_blocked_count": self._budget_blocked_count,
            "inflight_reserved_bytes": self._inflight_reserved_bytes,
            "unobserved_reserved_bytes": self._unobserved_reserved_bytes,
            "budget_unobserved_bytes": (
                self._unobserved_reserved_bytes
                + self._inflight_reserved_bytes
            ),
            "budget_bytes_consumed": (
                self._bytes_total
                + self._unobserved_reserved_bytes
                + self._inflight_reserved_bytes
            ),
            "byte_budget_exhausted": self._byte_budget_exhausted,
            "byte_budget_failure": self._byte_budget_failure,
            "real_bytes_by_resource_type": dict(self._bytes_by_type),
            "blocked_count": self._blocked_count,
            "cf_challenge_attempts": self.cf_challenge_attempts,
            "cf_challenges_passed": self.cf_challenges_passed,
            "cf_challenges_failed": self.cf_challenges_failed,
        }
