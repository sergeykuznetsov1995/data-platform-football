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
import ipaddress
import json
import logging
import os
import threading
import time
from collections import Counter
from contextlib import contextmanager
from typing import Callable, Dict, Optional
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

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

# Every request reserves its full body ceiling in route() before transport.
# This fixed allowance covers request/response headers and framing, which
# Playwright only reports exactly after completion.  The allowance is per
# in-flight request and is released when that request finishes or fails.
BROWSER_REQUEST_FIXED_OVERHEAD_BYTES = 64 * 1024

# Cloudflare answers over HTTP/2, and chunked HTTP/1.1 responses carry no
# Content-Length at all, so an undeclared body is the norm on the clearance
# path rather than an attack. A declared smaller body shrinks the reservation
# after response headers; an undeclared body keeps the ceiling. A body that
# outgrows it aborts the session, and a ceiling that does not fit the remaining
# budget aborts in route() before any socket is opened.
BROWSER_UNDECLARED_BODY_RESERVATION_BYTES = 512 * 1024
UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES = (
    BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
)

# Firefox is kept at zero automatic redirects because it does not re-enter
# context.route() for server-redirect hops. The bootstrap may still follow one
# proved same-origin HTTPS Location by issuing a second explicit page.goto();
# that request passes the normal request/byte admission guard before transport.
MANUAL_SERVER_REDIRECT_HOPS = 1
_SERVER_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})

# Camoufox 0.4.11 resolves ``geoip=True`` before Playwright exists.  Its helper
# follows redirects and can try six public-IP services, so routing cannot admit
# those paid requests.  Production performs exactly one bounded request through
# the sticky lease, with redirects and transport retries disabled, and charges
# it before opening the socket.  A failed lookup is still one spent request.
GEOIP_REQUEST_RESERVATION = 1
GEOIP_LOOKUP_URL = "https://api.ipify.org"
GEOIP_RESPONSE_LIMIT_BYTES = 64
GEOIP_CONNECT_TIMEOUT_SECONDS = 3.0
GEOIP_READ_TIMEOUT_SECONDS = 3.0
GEOIP_BYTE_RESERVATION_BYTES = (
    BROWSER_REQUEST_FIXED_OVERHEAD_BYTES + GEOIP_RESPONSE_LIMIT_BYTES
)

# These Firefox transports can create a proxy connection without producing a
# Playwright Request (for example ``<link rel=preconnect>`` or a 103 Early
# Hints preconnect). A metered session disables them at the browser boundary;
# normal document/fetch/script traffic continues through context.route().
FIREFOX_METERED_NETWORK_PREFS = {
    # WebRTC creates ICE/STUN/DTLS sockets below Playwright routing. FBref's
    # static clearance page does not need it, so disable it before any source
    # document can run.
    "media.peerconnection.enabled": False,
    "network.http.redirection-limit": 0,
    "network.http.speculative-parallel-limit": 0,
    "network.preconnect": False,
    "network.early-hints.enabled": False,
    "network.early-hints.preconnect.enabled": False,
    "network.prefetch-next": False,
    "network.dns.disablePrefetch": True,
    "network.predictor.enabled": False,
}

# Network constructors that can open sockets outside context.route(). Keep the
# Python tuple as the exact, testable contract and derive the injected source
# from it so a new browser API cannot be added to one side only.
NETWORK_API_BLOCKED_CONSTRUCTORS = (
    "WebSocket",
    "WebTransport",
    "Worker",
    "SharedWorker",
    "RTCPeerConnection",
)

# Camoufox runs Playwright init scripts in an isolated world, so changing its
# ``globalThis.WebSocket`` does not affect page JavaScript.  This wrapper adds a
# short privileged script element at document start; ``bypass_csp=True`` on the
# browser context makes the guard deterministic even on a strict source CSP.
# The element reports synchronous installation through a temporary data
# attribute and is removed before source scripts run.
_NETWORK_API_DENY_MAIN_WORLD = r"""
(() => {
  const sentinel = "__fbrefNetworkApiGuard_v1_7bb0a73d";
  const markReady = () => {
    if (document.currentScript) {
      document.currentScript.dataset.fbrefNetworkGuard = "ready";
    }
  };
  if (globalThis[sentinel] === true) {
    markReady();
    return;
  }
  const deny = (name) => {
    throw new DOMException(`${name} is disabled`, "SecurityError");
  };
  for (const name of __FBREF_BLOCKED_NETWORK_CONSTRUCTORS__) {
    const nativeConstructor = globalThis[name];
    if (typeof nativeConstructor === "function") {
      const blockedConstructor = new Proxy(nativeConstructor, {
        apply() {
          deny(name);
        },
        construct() {
          deny(name);
        },
      });
      Object.defineProperty(globalThis, name, {
        value: blockedConstructor,
        writable: false,
        configurable: false,
      });
      // A Proxy forwards ``prototype``. Without replacing this link, page
      // code can recover the unguarded target with
      // ``new WebSocket.prototype.constructor(url)`` and open a socket before
      // Playwright can meter it.
      Object.defineProperty(nativeConstructor.prototype, "constructor", {
        value: blockedConstructor,
        writable: false,
        configurable: false,
      });
    }
  }
  const serviceWorkerContainer = navigator.serviceWorker;
  const serviceWorkerPrototype = serviceWorkerContainer
    ? Object.getPrototypeOf(serviceWorkerContainer)
    : null;
  const nativeRegister = serviceWorkerPrototype?.register;
  if (typeof nativeRegister === "function") {
    const blockedRegister = new Proxy(nativeRegister, {
      apply() {
        return Promise.reject(
          new DOMException("ServiceWorker is disabled", "SecurityError")
        );
      },
    });
    Object.defineProperty(serviceWorkerPrototype, "register", {
      value: blockedRegister,
      writable: false,
      configurable: false,
    });
  }
  Object.defineProperty(globalThis, sentinel, {
    value: true,
    enumerable: false,
    writable: false,
    configurable: false,
  });
  markReady();
})();
""".replace(
    "__FBREF_BLOCKED_NETWORK_CONSTRUCTORS__",
    json.dumps(NETWORK_API_BLOCKED_CONSTRUCTORS),
)
NETWORK_API_BLOCK_INIT_SCRIPT = """
(() => {
  const guard = document.createElement("script");
  guard.textContent = %s;
  (document.documentElement || document).appendChild(guard);
  const ready = guard.dataset.fbrefNetworkGuard === "ready";
  guard.remove();
  if (!ready) {
    throw new Error("FBref main-world network guard did not install");
  }
  return ready;
})();
""" % json.dumps(_NETWORK_API_DENY_MAIN_WORLD)


def _proxy_url_with_credentials(proxy: Optional[dict]) -> Optional[str]:
    if not proxy or not str(proxy.get("server") or "").strip():
        return None
    parsed = urlsplit(str(proxy["server"]).strip())
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Camoufox geo-IP proxy URL is invalid")
    username = str(proxy.get("username") or "")
    password = str(proxy.get("password") or "")
    if username != "lease" or not password:
        raise ValueError("Camoufox geo-IP requires its authenticated lease proxy")
    credentials = ""
    if username:
        credentials = quote(username, safe="")
        if password:
            credentials += ":" + quote(password, safe="")
        credentials += "@"
    host = parsed.hostname
    if ":" in host:
        host = f"[{host}]"
    netloc = credentials + host
    if parsed.port is not None:
        netloc += f":{parsed.port}"
    return urlunsplit((parsed.scheme, netloc, "", "", ""))


def resolve_geoip_without_redirects(proxy: Optional[dict]) -> str:
    """Resolve one exit IP using exactly one bounded, non-redirecting attempt."""

    import requests
    from requests.adapters import HTTPAdapter

    proxy_url = _proxy_url_with_credentials(proxy)
    if proxy_url is None:
        raise RuntimeError("Camoufox paid geo-IP lookup requires its lease proxy")
    proxies = {"http": proxy_url, "https": proxy_url}
    session = requests.Session()
    session.trust_env = False
    session.mount("http://", HTTPAdapter(max_retries=0))
    session.mount("https://", HTTPAdapter(max_retries=0))
    try:
        response = session.get(
            GEOIP_LOOKUP_URL,
            proxies=proxies,
            timeout=(
                GEOIP_CONNECT_TIMEOUT_SECONDS,
                GEOIP_READ_TIMEOUT_SECONDS,
            ),
            allow_redirects=False,
            headers={"Connection": "close", "Accept": "text/plain"},
            stream=True,
        )
        try:
            if int(response.status_code) != 200:
                raise RuntimeError("Camoufox geo-IP lookup returned non-200")
            raw_length = str(response.headers.get("content-length") or "").strip()
            if raw_length and (
                not raw_length.isascii()
                or not raw_length.isdigit()
                or int(raw_length) > GEOIP_RESPONSE_LIMIT_BYTES
            ):
                raise RuntimeError("Camoufox geo-IP response is oversized")
            payload = response.raw.read(
                GEOIP_RESPONSE_LIMIT_BYTES + 1,
                decode_content=True,
            )
            if len(payload) > GEOIP_RESPONSE_LIMIT_BYTES:
                raise RuntimeError("Camoufox geo-IP response is oversized")
            try:
                value = payload.decode("ascii").strip()
            except UnicodeDecodeError as exc:
                raise RuntimeError("Camoufox geo-IP response is not ASCII") from exc
            try:
                return str(ipaddress.ip_address(value))
            except ValueError as exc:
                raise RuntimeError("Camoufox geo-IP response is invalid") from exc
        finally:
            response.close()
    except requests.RequestException as exc:
        raise RuntimeError("Camoufox geo-IP lookup failed") from exc
    finally:
        session.close()


def require_camoufox_geoip_database() -> None:
    """Prove Camoufox will use its local GeoLite DB, never download at runtime."""

    try:
        from camoufox.locale import MMDB_FILE
    except (ImportError, AttributeError) as exc:
        raise RuntimeError("Camoufox local GeoLite database is unavailable") from exc
    try:
        ready = MMDB_FILE.is_file() and MMDB_FILE.stat().st_size > 0
    except OSError as exc:
        raise RuntimeError("Camoufox local GeoLite database is unavailable") from exc
    if not ready:
        raise RuntimeError("Camoufox local GeoLite database is unavailable")


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
    if "ns_error_redirect_loop" in message:
        # Native redirects are deliberately disabled so they cannot skip the
        # request-admission route. This is a policy stop, not a bad proxy.
        return "redirect_blocked"
    hard_proxy_markers = (
        'err_proxy_connection_failed', 'err_tunnel_connection_failed',
    )
    if any(marker in message for marker in hard_proxy_markers):
        return 'timeout'
    transient_network_markers = (
        'timeout', 'timed out', 'err_connection_reset',
        'err_connection_timed_out', 'ns_error_proxy_bad_gateway',
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
        geoip_resolver: Callable[[Optional[dict]], str] = (
            resolve_geoip_without_redirects
        ),
        geoip_database_check: Callable[[], None] = (
            require_camoufox_geoip_database
        ),
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
        self._geoip_resolver = geoip_resolver
        self._geoip_database_check = geoip_database_check

        self._cm = None
        self._browser = None
        self._context = None
        self._page = None
        self._proxy = None

        # rx+tx byte accounting (#842 pattern) — survives restarts.
        self._bytes_total = 0
        self._bytes_by_type: Counter = Counter()
        self._blocked_count = 0
        self._requests_count = 0
        self._network_requests_started = 0
        self._billed_traffic: Dict[str, object] = {}
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
        # route.abort() produces a normal Playwright requestfailed event. Keep
        # those request identities so that callback is not mistaken for HTTP
        # traffic that bypassed route admission. URL queues cover Playwright
        # re-wrapping and preserve multiplicity for duplicate URLs.
        self._intentional_abort_request_urls: Dict[int, Optional[str]] = {}
        self._intentional_abort_ids_by_url: Dict[str, list] = {}
        # An unrouted response is charged immediately; remember it until its
        # terminal callback so requestfinished/requestfailed cannot charge the
        # same escaped request a second time.
        self._unrouted_response_request_urls: Dict[int, Optional[str]] = {}
        self._unrouted_response_ids_by_url: Dict[str, list] = {}
        # Firefox can emit requestfailed just before the response callback for
        # the same routed request. Keep that settled identity until the late
        # response arrives so it cannot be mistaken for a routing bypass.
        self._settled_failed_request_urls: Dict[int, Optional[str]] = {}
        self._settled_failed_ids_by_url: Dict[str, list] = {}
        self._settled_failed_request_guids: Dict[int, str] = {}
        self._responded_request_urls: Dict[int, Optional[str]] = {}
        self._responded_ids_by_url: Dict[str, list] = {}
        self._navigation_source_url: Optional[str] = None
        self._pending_manual_redirect_url: Optional[str] = None
        # An __exit__ failure is a permanent phase-boundary failure. The
        # Camoufox context-manager handle stays available for a cleanup retry,
        # but this transport may never start another browser afterwards.
        self._browser_finalize_error: Optional[Exception] = None
        self._inflight_reserved_bytes = 0
        self._unobserved_reserved_bytes = 0
        self._byte_budget_exhausted = False
        self._byte_budget_failure: Optional[str] = None
        self._request_budget_exhausted = False
        self._redirect_blocked = False
        self._geoip_lookup_failed = False
        self._network_policy_failed = False
        self._network_policy_failure: Optional[str] = None
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
        if self._browser_finalize_error is not None:
            raise RuntimeError(
                "Camoufox browser finalization previously failed; reuse disabled"
            ) from self._browser_finalize_error
        if self._geoip_lookup_failed:
            raise RuntimeError(
                "Camoufox geo-IP lookup already failed; automatic retry disabled"
            )
        self._browser_start_attempts += 1

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

        if self._geoip is True:
            # Prove this before acquiring a paid lease. Passing an explicit IP
            # later makes Camoufox skip public_ip() and use this local DB only.
            self._geoip_database_check()

        self._proxy = self._proxy_provider() if self._proxy_provider else None
        if not self._proxy:
            logger.warning(
                "CamoufoxFbrefTransport starting WITHOUT a proxy — Turnstile "
                "403s on a datacenter IP; the fetch will not solve."
            )
        geoip = self._geoip
        if geoip is True:
            if self._max_network_requests is not None and not self._proxy:
                self._request_budget_exhausted = True
                self._budget_blocked_count += 1
                raise RuntimeError(
                    "Camoufox bounded geo-IP requires its paid lease proxy"
                )
            projected = (
                self._network_requests_started + GEOIP_REQUEST_RESERVATION
            )
            projected_bytes = (
                self._bytes_total
                + self._unobserved_reserved_bytes
                + self._inflight_reserved_bytes
                + GEOIP_BYTE_RESERVATION_BYTES
            )
            if (
                self._max_network_requests is not None
                and projected > self._max_network_requests
            ):
                self._request_budget_exhausted = True
                self._budget_blocked_count += 1
                raise RuntimeError(
                    "Camoufox geo-IP request admission exceeds request cap"
                )
            if (
                self._max_network_bytes is not None
                and projected_bytes > self._max_network_bytes
            ):
                self._byte_budget_exhausted = True
                self._byte_budget_failure = "geoip_admission_exceeds_byte_cap"
                self._budget_blocked_count += 1
                raise RuntimeError(
                    "Camoufox geo-IP byte admission exceeds byte cap"
                )
            # Charge the one allowed lookup and its bounded response before the
            # resolver can open its paid socket. Failed lookups are not refunded.
            self._network_requests_started = projected
            self._unobserved_reserved_bytes += GEOIP_BYTE_RESERVATION_BYTES
            try:
                geoip = self._geoip_resolver(self._proxy)
            except Exception:
                # A lookup failure already spent its one paid request. Do not
                # rotate leases and silently repeat it inside this transport.
                self._geoip_lookup_failed = True
                raise

        from camoufox.sync_api import Camoufox  # lazy: heavy (Firefox)

        kwargs = {"headless": self._headless, "humanize": self._humanize}
        if self._proxy:
            kwargs["proxy"] = self._proxy
            kwargs["geoip"] = geoip  # locale/timezone matched to exit IP
        elif geoip:
            kwargs["geoip"] = geoip
        if (
            self._max_network_requests is not None
            or self._max_network_bytes is not None
        ):
            # Firefox otherwise follows an HTTP 3xx without re-entering the
            # route handler.  Zero makes that navigation fail closed; JS/meta
            # navigations are new requests and are admitted by _maybe_block.
            kwargs["firefox_user_prefs"] = FIREFOX_METERED_NETWORK_PREFS.copy()
        # The launch itself is unbounded (see BROWSER_START_TIMEOUT_S), so kill
        # the browser it spawned if it overruns: Playwright then raises out of
        # the launch and the caller rotates onto a fresh exit IP.
        with self._browser_deadline(self.BROWSER_START_TIMEOUT_S, "start"):
            self._cm = Camoufox(**kwargs)
            self._browser = self._cm.__enter__()
            # Service-worker script fetches and WebSocket handshakes bypass
            # ordinary Playwright routing. Install the main-world deny guard
            # before creating any page; bypass_csp is needed only so this
            # browser-owned guard cannot be suppressed by a source CSP.
            self._context = self._browser.new_context(
                service_workers="block",
                bypass_csp=True,
            )
            self._context.add_init_script(
                script=NETWORK_API_BLOCK_INIT_SCRIPT
            )
            if (
                self._block_resources
                or self._max_network_requests is not None
                or self._max_network_bytes is not None
            ):
                # Context routing covers every page in the browser context.
                # Native redirects are disabled above, so every allowed HTTP
                # request must pass admission before transport.
                self._context.route("**/*", self._maybe_block)
            # Playwright's WebSocket close happens after the handshake on
            # Firefox 135; retain it only as defense after the main-world
            # constructor guard has already prevented content sockets.
            self._context.route_web_socket(
                "**/*",
                self._on_unexpected_websocket,
            )
            # Context events account popups and every other page in this
            # context; page-only callbacks leave their traffic unobserved.
            self._context.on("response", self._on_response)
            self._context.on("requestfinished", self._on_request_finished)
            self._context.on("requestfailed", self._on_request_failed)
            self._page = self._context.new_page()
            if self._page.evaluate(NETWORK_API_BLOCK_INIT_SCRIPT) is not True:
                raise RuntimeError(
                    "FBref main-world network guard is unavailable"
                )
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
        cm = self._cm
        prior_finalize_error = self._browser_finalize_error
        if cm is not None:
            # A browser close cancels admitted requests. Remember their
            # identities before __exit__ so its requestfailed callbacks remain
            # distinguishable from routing bypasses.
            self._remember_all_inflight_as_intentional_aborts()
            try:
                cm.__exit__(None, None, None)
            except Exception as exc:  # noqa: BLE001 — lifecycle boundary
                # Never turn a failed lifecycle boundary into success. Retain
                # the context-manager handle so a later close can retry its
                # cleanup, but make the browser/page unusable immediately.
                if self._browser_finalize_error is None:
                    self._browser_finalize_error = exc
                logger.error("Camoufox teardown failed", exc_info=True)
                try:
                    self._kill_browser_processes("teardown", None)
                except Exception:  # noqa: BLE001 — preserve __exit__ failure
                    logger.exception("Camoufox emergency teardown kill failed")
                self._clear_inflight_tracking(charge_unobserved=True)
                self._browser = self._context = self._page = None
                raise self._browser_finalize_error
        # Any request that produced no finished/failed callback may have
        # crossed an unknown fraction of its reservation. Charge the full
        # remainder before allowing a restart to reuse capacity.
        self._clear_inflight_tracking(charge_unobserved=True)
        self._cm = self._browser = self._context = self._page = None
        self._clear_request_callback_tracking()
        # A retry may finish the physical cleanup, but the original failed
        # boundary must still reach FBrefFetcher. It is what prevents lease
        # extension and construction of the unmetered HTTP phase.
        if prior_finalize_error is not None:
            raise prior_finalize_error

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

    def _remember_intentional_abort(self, req) -> None:
        """Remember one pre-network route abort until requestfailed arrives."""

        self._remember_request_marker(
            req,
            self._intentional_abort_request_urls,
            self._intentional_abort_ids_by_url,
        )

    def _remember_request_marker(
        self,
        req,
        request_urls: Dict[int, Optional[str]],
        ids_by_url: Dict[str, list],
    ) -> None:
        key = id(req)
        if key in request_urls:
            return
        url = self._request_url(req)
        # ``None`` is a real marker value: an exact wrapper callback can still
        # be recognized when a detached Request no longer exposes its URL.
        request_urls[key] = url
        if url is not None:
            ids_by_url.setdefault(url, []).append(key)

    def _remember_all_inflight_as_intentional_aborts(self) -> None:
        for key in self._inflight_byte_reservations:
            if key in self._intentional_abort_request_urls:
                continue
            url = self._inflight_request_urls.get(key)
            self._intentional_abort_request_urls[key] = url
            if url is not None:
                self._intentional_abort_ids_by_url.setdefault(url, []).append(key)

    @staticmethod
    def _forget_request_marker(
        key: int,
        request_urls: Dict[int, Optional[str]],
        ids_by_url: Dict[str, list],
    ) -> None:
        url = request_urls.pop(key, None)
        if url is None:
            return
        keys = ids_by_url.get(url)
        if keys:
            try:
                keys.remove(key)
            except ValueError:
                pass
            if not keys:
                ids_by_url.pop(url, None)

    def _consume_request_marker(
        self,
        req,
        request_urls: Dict[int, Optional[str]],
        ids_by_url: Dict[str, list],
    ) -> bool:
        key = id(req)
        if key in request_urls:
            self._forget_request_marker(key, request_urls, ids_by_url)
            return True
        url = self._request_url(req)
        keys = ids_by_url.get(url or "")
        if not keys:
            return False
        self._forget_request_marker(keys[0], request_urls, ids_by_url)
        return True

    def _consume_intentional_abort(self, req) -> bool:
        """Consume an abort marker, tolerating a re-wrapped Request object."""

        return self._consume_request_marker(
            req,
            self._intentional_abort_request_urls,
            self._intentional_abort_ids_by_url,
        )

    def _remember_unrouted_response(self, req) -> None:
        self._remember_request_marker(
            req,
            self._unrouted_response_request_urls,
            self._unrouted_response_ids_by_url,
        )

    def _consume_unrouted_response(self, req) -> bool:
        return self._consume_request_marker(
            req,
            self._unrouted_response_request_urls,
            self._unrouted_response_ids_by_url,
        )

    def _remember_settled_failed_request(self, req) -> None:
        self._remember_request_marker(
            req,
            self._settled_failed_request_urls,
            self._settled_failed_ids_by_url,
        )
        key = id(req)
        guid = self._request_guid(req)
        if guid is not None and key not in self._settled_failed_request_guids:
            self._settled_failed_request_guids[key] = guid

    @staticmethod
    def _request_guid(req) -> Optional[str]:
        """Return Playwright's stable request identity when it is available."""

        try:
            guid = getattr(getattr(req, "_impl_obj", None), "_guid", None)
        except Exception:  # noqa: BLE001 — detached request diagnostics
            return None
        if not isinstance(guid, str) or not guid.startswith("request@"):
            return None
        return guid

    def _settled_failed_marker_key(
        self,
        req,
    ) -> Optional[int]:
        key = id(req)
        guid = self._request_guid(req)
        if key in self._settled_failed_request_urls:
            known_guid = self._settled_failed_request_guids.get(key)
            # Guard against CPython reusing an old wrapper id for a new
            # Playwright request after the original wrapper was collected.
            return key if known_guid == guid else None
        if guid is not None:
            current_url = self._request_url(req)
            for known_key, known_guid in self._settled_failed_request_guids.items():
                if known_guid != guid:
                    continue
                known_url = self._settled_failed_request_urls.get(known_key)
                if known_url is not None and current_url != known_url:
                    return None
                return known_key
        return None

    def _forget_settled_failed_marker(self, key: int) -> None:
        self._settled_failed_request_guids.pop(key, None)
        self._forget_request_marker(
            key,
            self._settled_failed_request_urls,
            self._settled_failed_ids_by_url,
        )

    def _consume_settled_failed_request(self, req) -> bool:
        key = self._settled_failed_marker_key(req)
        if key is None:
            return False
        self._forget_settled_failed_marker(key)
        return True

    def _is_duplicate_settled_failure(self, req) -> bool:
        """Recognize only the same routed request, never merely the same URL."""

        return self._settled_failed_marker_key(req) is not None

    def _remember_responded_request(self, req) -> None:
        self._remember_request_marker(
            req,
            self._responded_request_urls,
            self._responded_ids_by_url,
        )

    def _consume_responded_request(self, req) -> bool:
        return self._consume_request_marker(
            req,
            self._responded_request_urls,
            self._responded_ids_by_url,
        )

    def _clear_request_callback_tracking(self) -> None:
        self._intentional_abort_request_urls.clear()
        self._intentional_abort_ids_by_url.clear()
        self._unrouted_response_request_urls.clear()
        self._unrouted_response_ids_by_url.clear()
        self._settled_failed_request_urls.clear()
        self._settled_failed_ids_by_url.clear()
        self._settled_failed_request_guids.clear()
        self._responded_request_urls.clear()
        self._responded_ids_by_url.clear()

    def _track_reservation(self, req, reservation: int) -> None:
        key = id(req)
        self._inflight_byte_reservations[key] = reservation
        self._inflight_reserved_bytes += reservation
        url = self._request_url(req)
        if url is not None:
            self._inflight_request_urls[key] = url
            self._inflight_ids_by_url.setdefault(url, []).append(key)

    def _clear_inflight_tracking(self, *, charge_unobserved: bool) -> None:
        """Atomically discard every index for the current in-flight set."""

        if charge_unobserved:
            self._unobserved_reserved_bytes += self._inflight_reserved_bytes
        self._inflight_byte_reservations.clear()
        self._inflight_request_urls.clear()
        self._inflight_ids_by_url.clear()
        self._inflight_reserved_bytes = 0

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
        self._remember_all_inflight_as_intentional_aborts()
        self._clear_inflight_tracking(charge_unobserved=True)
        # The browser is NOT closed here.  This runs inside a Playwright event
        # callback (`response` / `requestfinished`), and the sync API deadlocks
        # if the browser is torn down from one: the callback waits for the
        # close, the close waits for the callback to return, and the wave hangs
        # forever holding its leases.  `route()` refuses every further request
        # while the flag is set, and `fetch()` closes the session on its way out.

    def _on_unexpected_websocket(self, websocket) -> None:
        """Fail closed if content bypassed the main-world WebSocket guard.

        Firefox reports this callback after the handshake may have reached the
        proxy. Treat it as an accounting tripwire, not as a free blocker: one
        request and a conservative unknown-body ceiling stay permanently spent.
        """

        self._latch_unexpected_network("unexpected_websocket_handshake")
        logger.error(
            "Camoufox unexpected WebSocket handshake aborted the session"
        )
        try:
            websocket.close(
                code=1008, reason="FBref websocket traffic is disabled"
            )
        except Exception:  # noqa: BLE001 — the tripwire stays latched
            logger.debug("WebSocket tripwire close failed", exc_info=True)

    def _latch_unexpected_network(self, reason: str) -> None:
        """Permanently charge and reject traffic outside context routing."""

        self._network_requests_started += 1
        self._unobserved_reserved_bytes += (
            UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES
        )
        self._network_policy_failed = True
        self._network_policy_failure = reason
        self._last_solve_failure = "network_policy"
        self._blocked_count += 1
        self._budget_blocked_count += 1

    def _hard_network_failure_latched(self) -> bool:
        return bool(
            self._byte_budget_exhausted
            or self._request_budget_exhausted
            or self._network_policy_failed
        )

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
                self._remember_intentional_abort(req)
                route.abort()
                return
            request_cap_reached = (
                self._max_network_requests is not None
                and self._network_requests_started >= self._max_network_requests
            )
            admission_reservation = (
                BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
                + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
            )
            byte_cap_reached = (
                self._max_network_bytes is not None
                and (
                    self._byte_budget_exhausted
                    or self._bytes_total
                    + self._unobserved_reserved_bytes
                    + self._inflight_reserved_bytes
                    + admission_reservation
                    > self._max_network_bytes
                )
            )
            if (
                request_cap_reached
                or byte_cap_reached
                or self._network_policy_failed
            ):
                self._blocked_count += 1
                byte_was_exhausted = self._byte_budget_exhausted
                if byte_cap_reached:
                    self._abort_session_for_byte_budget(
                        "request_admission_exceeds_byte_cap"
                    )
                if not byte_cap_reached or byte_was_exhausted:
                    self._budget_blocked_count += 1
                if request_cap_reached:
                    self._request_budget_exhausted = True
                self._remember_intentional_abort(req)
                route.abort()
                return
            # Count before handing the request to Playwright. Failed and
            # interrupted requests still consumed one global network attempt.
            self._network_requests_started += 1
            self._track_reservation(
                req,
                admission_reservation
                if self._max_network_bytes is not None
                else 0,
            )
            route.continue_()
        except Exception:  # noqa: BLE001 — fail closed at the proxy boundary
            if req is not None:
                reserved = self._release_byte_reservation(req)
                if reserved:
                    self._charge_failed_request(req, reserved)
                self._remember_intentional_abort(req)
            try:
                self._blocked_count += 1
                route.abort()
            except Exception:
                pass

    @staticmethod
    def _response_status(response) -> Optional[int]:
        try:
            raw = response.status
            raw = raw() if callable(raw) else raw
            status = int(raw)
        except Exception:  # noqa: BLE001 — detached response is not proof
            return None
        return status if 100 <= status <= 599 else None

    @staticmethod
    def _is_navigation_request(req) -> bool:
        try:
            value = req.is_navigation_request
            value = value() if callable(value) else value
        except Exception:  # noqa: BLE001 — detached request is not navigation
            return False
        return value is True

    @staticmethod
    def _normalized_https_url(value: object) -> Optional[str]:
        try:
            parsed = urlsplit(str(value or ""))
            port = parsed.port
        except (TypeError, ValueError):
            return None
        if (
            parsed.scheme.casefold() != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or port not in (None, 443)
        ):
            return None
        host = parsed.hostname.casefold()
        return urlunsplit(
            ("https", host, parsed.path or "/", parsed.query, "")
        )

    def _manual_redirect_target(
        self,
        source_url: object,
        location: object,
    ) -> Optional[str]:
        source = self._normalized_https_url(source_url)
        raw_location = str(location or "").strip()
        if source is None or not raw_location:
            return None
        target = self._normalized_https_url(urljoin(source, raw_location))
        if target is None:
            return None
        if urlsplit(source).netloc != urlsplit(target).netloc:
            return None
        return target

    def _capture_manual_redirect(
        self,
        response,
        req,
        headers: Dict[str, str],
    ) -> None:
        source = self._navigation_source_url
        if source is None or not self._is_navigation_request(req):
            return
        if self._normalized_https_url(self._request_url(req)) != source:
            return
        if self._response_status(response) not in _SERVER_REDIRECT_STATUSES:
            return
        self._pending_manual_redirect_url = self._manual_redirect_target(
            source,
            headers.get("location"),
        )

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

    def _reject_unrouted_request(self) -> None:
        """Reject a response that appeared without route admission.

        Native redirects are disabled and every context page is routed before
        creation. A missing reservation is therefore a transport-policy bypass,
        never an HTTP request that may be silently adopted.
        """

        self._latch_unexpected_network("unrouted_http_response")

    def _on_response(self, response) -> None:
        """Reserve the declared body before any response can cross the cap."""

        if self._byte_budget_exhausted:
            # A prior hard abort charged and cleared every known in-flight
            # reservation. Late callbacks belong to those requests and must
            # not be misclassified or counted a second time as unrouted.
            return
        req = getattr(response, "request", None)
        if req is None:
            self._latch_unexpected_network("response_without_request")
            return
        reservation_key = self._reservation_key(req)
        if reservation_key is None:
            if self._consume_settled_failed_request(req):
                headers = self._response_headers(response)
                self._capture_manual_redirect(response, req, headers)
                return
            self._remember_unrouted_response(req)
            self._reject_unrouted_request()
            return
        headers = self._response_headers(response)
        self._remember_responded_request(req)
        self._capture_manual_redirect(response, req, headers)
        if (
            self._network_policy_failed
            or self._max_network_bytes is None
            or self._byte_budget_exhausted
        ):
            return
        admitted = self._inflight_byte_reservations[reservation_key]

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
        desired = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES + body_reservation
        if desired > admitted:
            self._abort_session_for_byte_budget(
                f"declared_body_exceeds_admission_reservation:{body_reservation}"
            )
            return
        self._inflight_byte_reservations[reservation_key] = desired
        self._inflight_reserved_bytes -= admitted - desired

    def _on_request_finished(self, req) -> None:
        if self._byte_budget_exhausted:
            self._consume_intentional_abort(req)
            self._consume_unrouted_response(req)
            return
        reservation_key = self._reservation_key(req)
        if reservation_key is None:
            if (
                self._consume_intentional_abort(req)
                or self._consume_unrouted_response(req)
            ):
                return
            self._latch_unexpected_network("unrouted_http_completion")
            return
        reserved = self._release_byte_reservation(req)
        self._consume_responded_request(req)
        self._consume_intentional_abort(req)
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
        try:
            failure = getattr(req, "failure", None)
            failure = failure() if callable(failure) else failure
        except Exception:  # noqa: BLE001 — detached request diagnostics
            failure = None
        redirect_blocked = (
            "ns_error_redirect_loop" in str(failure or "").casefold()
        )
        if redirect_blocked:
            self._redirect_blocked = True
        if self._byte_budget_exhausted:
            # The hard abort already charged and cleared every admitted
            # reservation. Its late cancellation callbacks are not new traffic.
            self._consume_intentional_abort(req)
            self._consume_unrouted_response(req)
            return
        reservation_key = self._reservation_key(req)
        if reservation_key is not None:
            if not self._consume_responded_request(req):
                self._remember_settled_failed_request(req)
            reserved = self._release_byte_reservation(req)
            self._consume_intentional_abort(req)
            self._charge_failed_request(req, reserved)
            return
        if self._consume_intentional_abort(req):
            # Browser-owned cancellation happened before another socket could
            # be opened. Any original routed request was counted at admission.
            return
        if self._consume_unrouted_response(req):
            return
        if self._is_duplicate_settled_failure(req):
            # Firefox can report the same routed transport failure twice. The
            # first callback already charged and settled its reservation. Keep
            # the marker for a possible late response (notably a blocked 3xx)
            # instead of misclassifying the duplicate as a routing bypass.
            return
        if redirect_blocked:
            return
        # A requestfailed event with no route reservation and no intentional
        # abort marker proves that an HTTP transport escaped context.route().
        # Charge it once, conservatively, and make the session terminal.
        self._latch_unexpected_network("unrouted_http_failure")

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
            if self._browser_finalize_error is not None:
                # A failed __exit__ is not a rotatable proxy problem. Let the
                # fetcher observe the phase-boundary failure and refuse HTTP.
                raise
            # Only failures with a network/proxy signature affect proxy
            # health. Local browser crashes must not burn a healthy exit IP.
            if error_type in {'timeout', 'network'}:
                self._record_proxy_result(False, error_type)
            self._stop()
            return False

    def _navigate_with_one_manual_redirect(self, url: str) -> None:
        """Navigate with one explicit, routed server-redirect continuation."""

        current_url = url
        for hop in range(MANUAL_SERVER_REDIRECT_HOPS + 1):
            self._pending_manual_redirect_url = None
            self._navigation_source_url = self._normalized_https_url(
                current_url
            )
            self._navigation_attempts += 1
            try:
                with self._browser_deadline(
                    self.BROWSER_ATTEMPT_TIMEOUT_S, "navigation"
                ):
                    self._page.goto(
                        current_url,
                        wait_until="domcontentloaded",
                        timeout=self._nav_timeout_ms,
                    )
            except Exception as exc:
                error_type = _navigation_error_type(exc)
                redirect_url = self._pending_manual_redirect_url
                self._pending_manual_redirect_url = None
                if (
                    (self._redirect_blocked or error_type == "redirect_blocked")
                    and redirect_url is not None
                    and hop < MANUAL_SERVER_REDIRECT_HOPS
                    and not self._hard_network_failure_latched()
                ):
                    # network.http.redirection-limit=0 stopped Firefox before
                    # the hop. The explicit continuation is a fresh route(),
                    # so its request and byte caps are checked before transport.
                    self._redirect_blocked = False
                    current_url = redirect_url
                    logger.info("Camoufox following one admitted server redirect")
                    continue
                raise
            finally:
                self._navigation_source_url = None
            self._pending_manual_redirect_url = None
            return

    # -- public fetch ----------------------------------------------------- #
    def fetch(self, url: str) -> Optional[str]:
        """Navigate to ``url``, solve Turnstile if present, return page HTML.

        Reuses the warm session (cf_clearance) across calls; on a solve-timeout
        it restarts on a fresh proxy up to ``MAX_PROXY_ROTATIONS`` times.
        """
        if self._hard_network_failure_latched():
            return None
        if self._geoip_lookup_failed:
            return None
        self._redirect_blocked = False
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
                    if (
                        self._hard_network_failure_latched()
                        or self._geoip_lookup_failed
                    ):
                        break
                    continue
            try:
                # FBrefFetcher uses Camoufox only for its clearance bootstrap,
                # so this is one bootstrap plus at most one explicit, routed
                # same-origin server-redirect continuation.
                self._navigate_with_one_manual_redirect(url)
            except Exception as e:  # noqa: BLE001 — browser/network boundary
                logger.warning("Camoufox goto failed (attempt %d): %s",
                               attempt + 1, e)
                error_type = _navigation_error_type(e)
                if self._hard_network_failure_latched():
                    break
                if self._redirect_blocked or error_type == "redirect_blocked":
                    self._redirect_blocked = True
                    self._last_solve_failure = "redirect_blocked"
                    break
                if error_type in {'timeout', 'network'}:
                    self._record_proxy_result(False, error_type)
                if attempt < self.MAX_PROXY_ROTATIONS:
                    self._lifecycle_action(self._restart, 'restart')
                continue

            if self._hard_network_failure_latched():
                break
            if self._redirect_blocked:
                self._last_solve_failure = "redirect_blocked"
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
                if self._redirect_blocked:
                    self._last_solve_failure = "redirect_blocked"
                    break
                if self._hard_network_failure_latched():
                    break
                if attempt < self.MAX_PROXY_ROTATIONS:
                    self._lifecycle_action(self._restart, 'restart')
                continue
            if self._hard_network_failure_latched():
                break
            if self._redirect_blocked:
                self._last_solve_failure = "redirect_blocked"
                break
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
            if self._redirect_blocked:
                self._last_solve_failure = "redirect_blocked"
                break
            if failure_type == 'cloudflare':
                self._record_proxy_result(False, failure_type)
            if attempt < self.MAX_PROXY_ROTATIONS:
                self._lifecycle_action(self._restart, 'restart')

        if self._hard_network_failure_latched() and self._cm is not None:
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
        if (
            self._page is None
            or self._hard_network_failure_latched()
            or self._redirect_blocked
            or self._geoip_lookup_failed
        ):
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
    def traffic_delta(self) -> Dict[str, object]:
        """Traffic since the previous call — one bootstrap, billed exactly once.

        The counters are cumulative for the whole session, and the caller
        charges what it reads to the target that triggered the bootstrap.
        Charging the running totals made every later target of a wave pay again
        for the same browser traffic: a wave with a failing clearance billed 140
        requests for ~20 real ones and exhausted the run's request budget.
        """
        stats = self.traffic_stats()
        baseline = self._billed_traffic
        delta: Dict[str, object] = {}
        for key, value in stats.items():
            if isinstance(value, int) and not isinstance(value, bool):
                # Not every number is a counter: the in-flight reservations are
                # a gauge that falls when requests settle, so a plain difference
                # can go negative and bill a fetch a negative byte count.
                delta[key] = max(0, value - int(baseline.get(key, 0) or 0))
            else:
                delta[key] = value
        by_type = dict(stats.get("real_bytes_by_resource_type") or {})
        billed_by_type = dict(baseline.get("real_bytes_by_resource_type") or {})
        delta["real_bytes_by_resource_type"] = {
            name: max(0, total - int(billed_by_type.get(name, 0) or 0))
            for name, total in by_type.items()
        }
        self._billed_traffic = stats
        return delta

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
            "request_budget_exhausted": self._request_budget_exhausted,
            "network_policy_failed": self._network_policy_failed,
            "network_policy_failure": self._network_policy_failure,
            "geoip_lookup_failed": self._geoip_lookup_failed,
            "redirect_blocked": self._redirect_blocked,
            "real_bytes_by_resource_type": dict(self._bytes_by_type),
            "blocked_count": self._blocked_count,
            "cf_challenge_attempts": self.cf_challenge_attempts,
            "cf_challenges_passed": self.cf_challenges_passed,
            "cf_challenges_failed": self.cf_challenges_failed,
        }
