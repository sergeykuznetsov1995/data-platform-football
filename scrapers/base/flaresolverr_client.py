"""
FlareSolverr Client
===================

Lightweight HTTP client for the FlareSolverr REST API. Used by Cloudflare-protected
scrapers (e.g. WhoScored events) to fetch HTML through a long-lived browser session
that holds CF state inside the FlareSolverr container.
"""

import logging
import uuid
from collections import Counter
from typing import Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class FlareSolverrError(Exception):
    """Generic FlareSolverr operation error."""
    pass


class FlareSolverrTimeout(FlareSolverrError):
    """Raised when the FlareSolverr endpoint times out or is unreachable."""
    pass


class FlareSolverrCFChallengeFailed(FlareSolverrError):
    """Raised when FlareSolverr cannot solve a Cloudflare/Turnstile challenge."""
    pass


class FlareSolverrTabCrashed(FlareSolverrError):
    """Raised when FlareSolverr's internal Chromium tab crashes.

    Distinct from a CF challenge failure: the fix is to recreate the session
    (fresh browser tab), not to re-solve a challenge. SoFIFA's SPA pages crash
    the Chromium 142 tab unpredictably during long iteration — callers rotate
    the session on this error (see ``FlareSolverrSoFIFAReader._fs_get_with_recovery``).
    """
    pass


class FlareSolverrErrorPage(FlareSolverrError):
    """Raised when FlareSolverr returns a Chromium net-error page, not site HTML.

    The browser renders its own error page (HTTP 200) for transport failures
    such as ``ERR_NO_SUPPORTED_PROXIES``. Callers must raise rather than cache
    it, or soccerdata's ``read_*(max_age=…)`` would reuse the poisoned cache
    for days (#655).
    """
    pass


_CF_MARKERS = ("cloudflare", "challenge", "turnstile")

#: Substrings in a FlareSolverr error message that indicate the Chromium tab
#: crashed (vs. a CF challenge). Kept narrow to avoid mislabelling other errors.
_TAB_CRASH_MARKERS = ("tab crashed", "target crashed", "page crashed", "renderer")

#: Markers in returned HTML that flag a Chromium net-error page rather than site
#: content. FlareSolverr serves these as HTTP 200 (e.g. ERR_NO_SUPPORTED_PROXIES
#: when proxy creds are embedded in the URL, #647). ``chrome-error://`` is the
#: error page's base URL and ``neterror`` its template id/class — both confirmed
#: present in the live poisoned page (#655) and absent from real sofifa/whoscored
#: HTML, so they reliably catch any ERR_* code with zero false positives.
_CHROMIUM_ERROR_MARKERS = (
    "chrome-error://",
    "neterror",
    "ERR_NO_SUPPORTED_PROXIES",
)


def is_chromium_error_page(html: str) -> bool:
    """True if ``html`` is a Chromium network-error page, not real site content."""
    return any(marker in html for marker in _CHROMIUM_ERROR_MARKERS)


def _proxy_payload(proxy_url: str) -> dict:
    """Build the FlareSolverr ``proxy`` object from a proxy URL.

    Chromium's ``--proxy-server`` rejects credentials embedded in the URL
    (``http://user:pass@host:port`` → ``ERR_NO_SUPPORTED_PROXIES``: every
    proxied fetch silently returns a browser error page with HTTP 200, so
    no exception is raised and reactive rotation never fires — see #647).
    FlareSolverr takes auth as separate ``username`` / ``password`` fields,
    so split them out of the netloc and pass a credential-free URL.
    """
    parts = urlsplit(proxy_url)
    if not (parts.username or parts.password):
        return {"url": proxy_url}
    netloc = parts.hostname or ""
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    clean_url = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    return {
        "url": clean_url,
        "username": parts.username or "",
        "password": parts.password or "",
    }


def _normalise_url_key(url: str) -> str:
    """Collapse a URL to ``host/path`` (query + fragment dropped).

    Groups repeated calls to one endpoint under a single key so the per-URL
    traffic counter (issue #616) stays bounded regardless of cache-busting
    query params (``?d=`` on WhoScored data, ``?r=&set=true`` on SoFIFA).
    Kept local so this lightweight client never imports a browser runtime.
    Returns '' for falsy input.
    """
    if not url:
        return ""
    try:
        parts = urlsplit(url)
        if parts.netloc:
            return f"{parts.netloc}{parts.path}"
        return url.split("?", 1)[0].split("#", 1)[0]
    except Exception:
        return url.split("?", 1)[0]


def describe_proxy_mode(proxy_url: Optional[str]) -> str:
    """Human-readable FlareSolverr proxy mode for logs (issue #616).

    Makes the proxy decision visible at session start so an accidental
    re-enable (via ``PROXY_FILTER_URL`` or a non-empty proxy-file) shows up
    in logs instead of being silent. Never echoes residential credentials —
    a residential URL carries ``user:pass`` so we only name the mode.
    """
    if not proxy_url:
        return "PROXY-LESS (FlareSolverr solves CF directly)"
    if "proxy_filter" in proxy_url:
        return "via ad-tech filter (#652)"
    return "via residential proxy"


class FlareSolverrClient:
    """HTTP wrapper around FlareSolverr `/v1` and `/health` endpoints."""

    #: Number of top per-URL consumers surfaced by get_traffic_stats().
    _TOP_URLS_N = 25

    def __init__(
        self,
        url: str = "http://flaresolverr:8191",
        default_timeout: float = 90.0,
        default_max_timeout_ms: int = 60_000,
    ) -> None:
        self.url = url.rstrip("/")
        self.default_timeout = default_timeout
        self.default_max_timeout_ms = default_max_timeout_ms
        self._session: Optional[requests.Session] = None
        self._auto_session_id: Optional[str] = None
        # Traffic-audit counters (issue #616). ``fs_response_bytes`` is the
        # payload FlareSolverr returns to us (rendered HTML + JSON envelope) —
        # a LOWER BOUND on residential-proxy traffic, NOT the proxy MB itself:
        # Camoufox downloads images/CSS/JS/XHR through the proxy and returns
        # only the rendered HTML. ``sessions_created`` ≈ CF cold-starts, the
        # real traffic driver (each new session re-solves the CF challenge).
        self._fs_response_bytes = 0
        self._requests = 0
        self._bytes_by_url: Counter = Counter()
        self._requests_by_url: Counter = Counter()
        self._sessions_created = 0
        self._cf_challenge_failures = 0
        self._last_post_bytes = 0

    @property
    def session(self) -> requests.Session:
        """Lazily build a requests Session with retry on network errors."""
        if self._session is None:
            session = requests.Session()
            retry = Retry(
                total=2,
                backoff_factor=1.5,
                status_forcelist=(502, 503, 504),
                allowed_methods=frozenset(["GET", "POST"]),
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._session = session
        return self._session

    @session.setter
    def session(self, value: requests.Session) -> None:
        self._session = value

    @session.deleter
    def session(self) -> None:
        self._session = None

    def _post(self, payload: dict, timeout: Optional[float] = None) -> dict:
        """POST to /v1 and translate transport/protocol errors to typed exceptions."""
        endpoint = f"{self.url}/v1"
        cmd = payload.get("cmd", "?")
        logger.debug(f"FlareSolverr POST {endpoint} cmd={cmd}")

        try:
            response = self.session.post(
                endpoint,
                json=payload,
                timeout=timeout or self.default_timeout,
            )
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"FlareSolverr transport error (cmd={cmd}): {e}")
            raise FlareSolverrTimeout(f"FlareSolverr unreachable: {e}") from e
        except requests.exceptions.RequestException as e:
            logger.warning(f"FlareSolverr request error (cmd={cmd}): {e}")
            raise FlareSolverrError(f"FlareSolverr request failed: {e}") from e

        if not response.ok:
            body = response.text[:300]
            logger.warning(
                f"FlareSolverr HTTP {response.status_code} (cmd={cmd}): {body}"
            )
            lower = body.lower()
            if "challenge" in lower or "cloudflare" in lower or "turnstile" in lower:
                self._cf_challenge_failures += 1
                raise FlareSolverrCFChallengeFailed(
                    f"FlareSolverr HTTP {response.status_code}: {body}"
                )
            raise FlareSolverrError(
                f"FlareSolverr HTTP {response.status_code}: {body}"
            )

        try:
            data = response.json()
        except ValueError as e:
            raise FlareSolverrError(f"FlareSolverr returned non-JSON: {e}") from e

        if data.get("status") == "error":
            message = str(data.get("message", "unknown error"))
            lowered = message.lower()
            if any(marker in lowered for marker in _TAB_CRASH_MARKERS):
                logger.warning(f"FlareSolverr tab crashed (cmd={cmd}): {message}")
                raise FlareSolverrTabCrashed(message)
            if any(marker in lowered for marker in _CF_MARKERS):
                self._cf_challenge_failures += 1
                logger.warning(f"FlareSolverr CF challenge failed (cmd={cmd}): {message}")
                raise FlareSolverrCFChallengeFailed(message)
            logger.warning(f"FlareSolverr error (cmd={cmd}): {message}")
            raise FlareSolverrError(message)

        # Count only successful responses: bytes FlareSolverr returned to us.
        # ``_last_post_bytes`` bridges to get() for per-URL attribution.
        self._last_post_bytes = len(response.content or b"")
        self._fs_response_bytes += self._last_post_bytes
        return data

    def health(self) -> bool:
        """Return True if /health responds 200, False on any error."""
        try:
            response = self.session.get(
                f"{self.url}/health",
                timeout=self.default_timeout,
            )
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def create_session(self, session_id: str, proxy_url: Optional[str] = None) -> None:
        """Create a named FlareSolverr browser session, optionally bound to a proxy."""
        payload: dict = {"cmd": "sessions.create", "session": session_id}
        if proxy_url:
            payload["proxy"] = _proxy_payload(proxy_url)
        self._post(payload)
        # A fresh session re-solves the Cloudflare challenge → a cold-start.
        # Counting these is the cheapest signal for the dominant traffic driver
        # (issue #616): SoFIFA rotates every 4 requests, WhoScored every 8–10.
        self._sessions_created += 1
        logger.info(
            f"FlareSolverr session created: {session_id}"
            f"{' (proxy=' + proxy_url + ')' if proxy_url else ''}"
        )

    def destroy_session(self, session_id: str) -> None:
        """Destroy a session; idempotent — never raises if the session is gone."""
        payload = {"cmd": "sessions.destroy", "session": session_id}
        try:
            self._post(payload)
            logger.info(f"FlareSolverr session destroyed: {session_id}")
        except FlareSolverrError as e:
            logger.debug(f"FlareSolverr destroy_session({session_id}) ignored: {e}")

    def list_sessions(self) -> list[str]:
        """List active FlareSolverr session IDs."""
        data = self._post({"cmd": "sessions.list"})
        return list(data.get("sessions", []))

    def get(
        self,
        url: str,
        session_id: str,
        max_timeout_ms: Optional[int] = None,
        return_only_cookies: bool = False,
    ) -> dict:
        """GET via FlareSolverr; returns the `solution` subdict (html, cookies, ...)."""
        payload: dict = {
            "cmd": "request.get",
            "url": url,
            "session": session_id,
            "maxTimeout": max_timeout_ms or self.default_max_timeout_ms,
        }
        if return_only_cookies:
            payload["returnOnlyCookies"] = True

        timeout = (max_timeout_ms / 1000.0 + 30.0) if max_timeout_ms else self.default_timeout
        data = self._post(payload, timeout=timeout)
        # Per-URL traffic attribution (issue #616). Only successful fetches
        # reach here (_post raises on CF / error), so a failed page is never
        # booked as a request.
        self._requests += 1
        url_key = _normalise_url_key(url)
        if url_key:
            self._bytes_by_url[url_key] += self._last_post_bytes
            self._requests_by_url[url_key] += 1
        solution = data.get("solution") or {}
        return {
            "html": solution.get("response", ""),
            "cookies": solution.get("cookies", []),
            "userAgent": solution.get("userAgent", ""),
            "status": solution.get("status", 0),
        }

    def get_traffic_stats(self) -> dict:
        """Per-scrape proxy-traffic audit summary (issue #616).

        ``fs_response_*`` is the FlareSolverr payload returned to us — a LOWER
        BOUND on residential-proxy bytes, not the proxy MB itself (sub-resources
        are fetched by Camoufox and never returned). The true per-match proxy
        MB is measured at the container/proxy level on the VM; see
        ``docs/research/flaresolverr-proxy-traffic-audit.md``. ``sessions_created``
        ≈ CF cold-starts, the dominant traffic driver.
        """
        top = sorted(
            self._bytes_by_url.items(), key=lambda kv: kv[1], reverse=True
        )[: self._TOP_URLS_N]
        return {
            "fs_response_bytes": self._fs_response_bytes,
            "fs_response_mb": round(self._fs_response_bytes / 1024 / 1024, 4),
            "requests": self._requests,
            "sessions_created": self._sessions_created,
            "cf_challenge_failures": self._cf_challenge_failures,
            "top_traffic_urls": [
                {
                    "url": key,
                    "bytes": size,
                    "mb": round(size / 1024 / 1024, 4),
                    "requests": int(self._requests_by_url.get(key, 0)),
                }
                for key, size in top
            ],
        }

    def __enter__(self) -> Tuple["FlareSolverrClient", str]:
        session_id = f"fs-{uuid.uuid4().hex[:8]}"
        self.create_session(session_id)
        self._auto_session_id = session_id
        return self, session_id

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._auto_session_id is not None:
            try:
                self.destroy_session(self._auto_session_id)
            except Exception as e:
                logger.warning(
                    f"FlareSolverr __exit__ destroy_session failed "
                    f"({self._auto_session_id}): {e}"
                )
            finally:
                self._auto_session_id = None
        return False
